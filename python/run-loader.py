#!/usr/bin/env python3
"""Read a cms-hpt.jsonl file and run hospital-loader on each entry's mrf-url.

Usage:
    python3 run-loader.py [--input cms-hpt.jsonl] [--limit N] [--loader ./hospital-loader]
                          [--out-dir output/] [--log hospital-loader-log.jsonl]
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime


def run_loader(loader_bin, url, out_path, log_path):
    """Run hospital-loader CLI, streaming output in real-time. Returns success bool."""
    cmd = [
        loader_bin,
        "-file", url,
        "-out", out_path,
        "-log", log_path,
    ]
    result = subprocess.run(cmd, timeout=600)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Run hospital-loader on cms-hpt.jsonl entries")
    parser.add_argument("--input", default="cms-hpt.jsonl", help="Input JSONL file")
    parser.add_argument("--limit", type=int, default=0, help="Max entries to process (0 = all)")
    parser.add_argument("--loader", default="./hospital-loader", help="Path to hospital-loader binary")
    parser.add_argument("--out-dir", default="output", help="Output directory for parquet files")
    default_log = f"hospital-loader-log-{datetime.now().strftime('%Y%m%d-%H%M%S')}.jsonl"
    parser.add_argument("--log", default=default_log, help="JSONL log file path")
    args = parser.parse_args()

    if not os.path.exists(args.loader):
        print(f"Error: hospital-loader binary not found at {args.loader}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.out_dir, exist_ok=True)

    entries = []
    with open(args.input, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
            if args.limit and len(entries) >= args.limit:
                break

    print(f"Loaded {len(entries)} entries from {args.input}")
    print(f"Output dir: {args.out_dir}")
    print(f"Log file:   {args.log}")
    print()

    succeeded = 0
    failed = 0

    for i, entry in enumerate(entries):
        url = entry.get("mrf-url", "")
        name = entry.get("location-name", "unknown")
        if not url:
            print(f"[{i + 1}/{len(entries)}] SKIP {name}: no mrf-url")
            failed += 1
            continue

        print(f"[{i + 1}/{len(entries)}] {name}")
        print(f"  URL: {url}")

        # Determine output filename from location name
        safe_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in name)
        safe_name = safe_name.strip().replace(" ", "_")
        out_path = os.path.join(args.out_dir, f"{safe_name}.parquet")

        try:
            ok = run_loader(args.loader, url, out_path, args.log)

            if ok:
                size_mb = os.path.getsize(out_path) / 1024 / 1024 if os.path.exists(out_path) else 0
                print(f"  OK ({size_mb:.1f} MB)")
                succeeded += 1
            else:
                print(f"  FAILED")
                failed += 1

        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1

        print()

    print(f"Done: {succeeded} succeeded, {failed} failed out of {len(entries)}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
