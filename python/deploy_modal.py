#!/usr/bin/env python3
"""
Deploy hospital-loader to Modal, shard hospital entries across parallel workers.

Each worker runs `hospital-loader single` for each entry in its shard,
then returns the JSONL log output. Results are merged into a single JSONL file.

Usage:
    modal run python/deploy_modal.py --file cms-hpt.jsonl --out-dir s3://hospital-mrf/20260306/
    modal run python/deploy_modal.py --file cms-hpt.jsonl --out-dir s3://hospital-mrf/output/ --shards 50
"""

import json
import os
import sys
import time
from datetime import datetime

import modal


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    task_id = os.environ.get("MODAL_TASK_ID", "")
    if not task_id:
        import socket
        task_id = socket.gethostname()
    task_id = task_id[-8:]
    prefix = f"[ID|{task_id}] " if task_id else ""
    print(f"{ts} {prefix}{msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Module-level CLI parsing for params that must be known at decorator time.
# ---------------------------------------------------------------------------
def _cli_arg(name, default, type_fn=str):
    flag = f"--{name}"
    for i, arg in enumerate(sys.argv):
        if arg == flag and i + 1 < len(sys.argv):
            return type_fn(sys.argv[i + 1])
        if arg.startswith(f"{flag}="):
            return type_fn(arg.split("=", 1)[1])
    return default


_MEMORY = _cli_arg("memory", 4096, int)
_CPU = _cli_arg("cpu", 2, int)
_TIMEOUT = _cli_arg("timeout", 7200, int)
_CLOUD = _cli_arg("cloud", "aws")
_REGION = _cli_arg("region", "us-east-1")

# ---------------------------------------------------------------------------
# Modal app setup
# ---------------------------------------------------------------------------
app = modal.App("hospital-loader")

BINARY = "hospital-loader-linux-amd64"

def _build_binary():
    """Cross-compile the Go binary for Linux amd64 if needed."""
    import subprocess as sp
    log(f"Building {BINARY}...")
    result = sp.run(
        ["go", "build", "-o", BINARY, "./cmd/hospital-loader"],
        env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"},
    )
    if result.returncode != 0:
        print("Go build failed", file=sys.stderr)
        sys.exit(1)
    log(f"Built {BINARY}")

if not os.environ.get("MODAL_IS_REMOTE"):
    _build_binary()

image = (
    modal.Image.debian_slim()
    .apt_install("ca-certificates", "unzip")
    .add_local_file(BINARY, "/hospital-loader", copy=True)
    .run_commands("chmod +x /hospital-loader")
)


@app.function(
    image=image,
    cpu=_CPU,
    memory=_MEMORY,
    timeout=_TIMEOUT,
    cloud=_CLOUD,
    region=_REGION,
    secrets=[modal.Secret.from_name("aws-secret")]
)
def run_shard(
    shard_index: int,
    entries: list[dict],
    out_dir: str,
    batch_size: int,
    skip_payer: bool,
):
    import subprocess as sp

    work_dir = f"/tmp/shard-{shard_index}"
    os.makedirs(work_dir, exist_ok=True)
    log_path = os.path.join(work_dir, "log.jsonl")

    total = len(entries)
    succeeded = 0
    failed = 0

    for i, entry in enumerate(entries):
        url = entry.get("mrf-url", "")
        name = entry.get("location-name", "unknown")

        if not url:
            log(f"shard {shard_index}: skip entry {i+1}/{total} (no mrf-url): {name}")
            failed += 1
            continue

        log(f"shard {shard_index}: processing {i+1}/{total}: {name}")

        cmd = [
            "/hospital-loader", "single",
            "--file", url,
            "--out", out_dir,
            "--batch", str(batch_size),
            f"--skip-payer-charges={str(skip_payer).lower()}",
            "--log", log_path,
        ]

        proc = sp.run(cmd, capture_output=True, text=True)

        if proc.returncode != 0:
            log(f"shard {shard_index}: FAILED {name}: {proc.stderr[-500:] if proc.stderr else 'no stderr'}")
            failed += 1
        else:
            succeeded += 1
            log(f"shard {shard_index}: completed {name}")

    log(f"shard {shard_index}: done ({succeeded} succeeded, {failed} failed)")

    if os.path.exists(log_path):
        with open(log_path, "rb") as f:
            return f.read()
    return b""


def read_entries(path: str, limit: int = 0) -> list[dict]:
    """Read hospital entries from a JSONL file."""
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
            if limit and len(entries) >= limit:
                break
    return entries


def dedup_entries(entries: list[dict]) -> list[dict]:
    """Deduplicate entries by mrf-url."""
    seen = {}
    unique = []
    dupes = 0
    for entry in entries:
        url = entry.get("mrf-url", "")
        if not url:
            unique.append(entry)
            continue
        if url in seen:
            dupes += 1
            continue
        seen[url] = entry.get("location-name", "")
        unique.append(entry)
    if dupes:
        log(f"removed {dupes} duplicate URLs")
    return unique


def shard_entries(entries: list[dict], n: int) -> list[list[dict]]:
    """Split entries into n roughly-equal shards via round-robin."""
    shards: list[list[dict]] = [[] for _ in range(n)]
    for i, entry in enumerate(entries):
        shards[i % n].append(entry)
    return [s for s in shards if s]


@app.local_entrypoint()
def main(
    file: str,
    out_dir: str,
    shards: int = 100,
    batch: int = 10000,
    skip_payer: bool = True,
    limit: int = 0,
    output: str = "",
):
    entries = read_entries(file, limit)
    entries = dedup_entries(entries)

    entry_shards = shard_entries(entries, shards)

    log(f"Input: {file} ({len(entries)} entries across {len(entry_shards)} shards)")
    log(f"Output dir: {out_dir}")
    log(f"Infra: {_CPU} CPU, {_MEMORY} MB memory, {_CLOUD}/{_REGION}")
    log(f"Batch size: {batch}, skip payer charges: {skip_payer}")

    start = time.time()

    try:
        shard_outputs = list(run_shard.starmap(
            [
                (i, shard, out_dir, batch, skip_payer)
                for i, shard in enumerate(entry_shards)
            ]
        ))
    except Exception as e:
        log(f"Processing failed: {e}")
        sys.exit(1)

    wall_time = time.time() - start

    # Merge all log JSONL outputs into a single file.
    if output:
        output_path = output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"hospital-loader-log-{timestamp}.jsonl"

    total_entries = 0
    succeeded = 0
    failed = 0

    with open(output_path, "w") as out:
        for data in shard_outputs:
            if not data:
                continue
            text = data.decode("utf-8", errors="replace")
            for line in text.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue
                out.write(line + "\n")
                total_entries += 1
                try:
                    entry = json.loads(line)
                    if entry.get("success"):
                        succeeded += 1
                    else:
                        failed += 1
                except json.JSONDecodeError:
                    pass

    log(f"Complete: {succeeded} succeeded, {failed} failed, {total_entries} log entries in {wall_time:.1f}s")
    log(f"Log saved to {output_path}")
