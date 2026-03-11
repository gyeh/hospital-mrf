#!/usr/bin/env python3
"""
Retry failed entries from a hospital-loader log file.

Reads a JSONL log (output of 3_deploy_modal.py or the Go CLI batch command),
extracts failed entries, and re-runs them through `hospital-loader batch`.
The S3 output directory is inferred from successful entries in the log.
Results are merged: original successes + retry results.

Usage:
    python python/4_retry_failures.py --log hospital-loader-log-20260311_145459.jsonl
    python python/4_retry_failures.py --log hospital-loader-log-20260311_145459.jsonl --out-dir s3://hospital-mrf/20260311/
    python python/4_retry_failures.py --log hospital-loader-log-20260311_145459.jsonl --parallel 4
    python python/4_retry_failures.py --log hospital-loader-log-20260311_145459.jsonl --lookup hospitals/cms-hpt.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts} {msg}", file=sys.stderr, flush=True)


def read_log(path: str) -> list[dict]:
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
    return entries


def infer_out_dir(entries: list[dict]) -> str | None:
    """Extract S3 output directory from successful entries."""
    for entry in entries:
        output_file = entry.get("output_file", "")
        if output_file.startswith("s3://"):
            # e.g. s3://hospital-mrf/20260311/hospital_name.parquet -> s3://hospital-mrf/20260311/
            return output_file.rsplit("/", 1)[0] + "/"
    return None


def build_url_to_name(lookup_path: str) -> dict[str, str]:
    """Build URL -> location-name map from original cms-hpt.jsonl."""
    mapping = {}
    with open(lookup_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            url = entry.get("mrf-url", "")
            name = entry.get("location-name", "")
            if url and name:
                mapping[url] = name
    return mapping


def main():
    parser = argparse.ArgumentParser(description="Retry failed hospital-loader entries")
    parser.add_argument("--log", required=True, help="Input JSONL log file from a previous run")
    parser.add_argument("--out-dir", default="", help="S3 output directory (inferred from log if not set)")
    parser.add_argument("--lookup", default="hospitals/cms-hpt.jsonl", help="Original JSONL with location-name mappings")
    parser.add_argument("--parallel", type=int, default=4, help="Number of parallel workers for batch")
    parser.add_argument("--batch", type=int, default=10000, help="Parquet batch size")
    parser.add_argument("--output", default="", help="Output merged JSONL path (default: auto-generated)")
    parser.add_argument("--binary", default="./hospital-loader", help="Path to hospital-loader binary")
    args = parser.parse_args()

    # Read the log file.
    log_entries = read_log(args.log)
    successes = [e for e in log_entries if e.get("success")]
    failures = [e for e in log_entries if not e.get("success")]

    log(f"Log: {args.log} ({len(successes)} succeeded, {len(failures)} failed)")

    if not failures:
        log("No failures to retry.")
        return

    # Determine S3 output directory.
    out_dir = args.out_dir or infer_out_dir(log_entries)
    if not out_dir:
        log("ERROR: Could not infer S3 output directory. Pass --out-dir explicitly.")
        sys.exit(1)
    log(f"Output dir: {out_dir}")

    # Build URL -> location-name lookup from the original input file.
    url_to_name = {}
    if os.path.exists(args.lookup):
        url_to_name = build_url_to_name(args.lookup)
        log(f"Loaded {len(url_to_name)} URL->name mappings from {args.lookup}")

    # Build retry input JSONL. Use hospital_name from log entry, fall back to
    # lookup file, then to "unknown".
    retry_entries = []
    for entry in failures:
        url = entry.get("url", "")
        if not url:
            continue
        name = entry.get("hospital_name", "") or url_to_name.get(url, "unknown")
        retry_entries.append({"mrf-url": url, "location-name": name})

    log(f"Retrying {len(retry_entries)} failed entries")

    # Write temporary JSONL input for `hospital-loader batch`.
    retry_input = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", prefix="retry-", delete=False
    )
    retry_log = tempfile.NamedTemporaryFile(
        suffix=".jsonl", prefix="retry-log-", delete=False
    )
    retry_log.close()

    for entry in retry_entries:
        retry_input.write(json.dumps(entry) + "\n")
    retry_input.close()

    # Run hospital-loader batch on the retry input.
    cmd = [
        args.binary, "batch",
        "--input", retry_input.name,
        "--out-dir", out_dir,
        "--log", retry_log.name,
        "--batch", str(args.batch),
        "--parallel", str(args.parallel),
        "--skip-payer-charges=true",
    ]

    log(f"Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd)

    if proc.returncode != 0:
        log(f"WARNING: hospital-loader batch exited with code {proc.returncode}")

    # Read retry results.
    retry_results = read_log(retry_log.name)
    retry_succeeded = [e for e in retry_results if e.get("success")]
    retry_failed = [e for e in retry_results if not e.get("success")]
    log(f"Retry results: {len(retry_succeeded)} succeeded, {len(retry_failed)} failed")

    # Merge: original successes + retry results.
    # Index retry results by URL so we can replace the original failures.
    retry_by_url = {}
    for entry in retry_results:
        url = entry.get("url", "")
        if url:
            retry_by_url[url] = entry

    merged = []
    for entry in log_entries:
        if entry.get("success"):
            merged.append(entry)
        else:
            url = entry.get("url", "")
            if url in retry_by_url:
                merged.append(retry_by_url[url])
            else:
                merged.append(entry)

    # Write merged output.
    if args.output:
        output_path = args.output
    else:
        base = os.path.splitext(os.path.basename(args.log))[0]
        output_path = f"{base}-retried.jsonl"

    with open(output_path, "w") as f:
        for entry in merged:
            f.write(json.dumps(entry) + "\n")

    final_succeeded = sum(1 for e in merged if e.get("success"))
    final_failed = sum(1 for e in merged if not e.get("success"))

    log(f"Merged: {final_succeeded} succeeded, {final_failed} failed (was {len(failures)} failed)")
    log(f"Saved to {output_path}")

    # Cleanup temp files.
    os.unlink(retry_input.name)
    os.unlink(retry_log.name)


if __name__ == "__main__":
    main()
