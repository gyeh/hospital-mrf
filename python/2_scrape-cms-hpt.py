#!/usr/bin/env python3
"""Fetch cms-hpt.txt from each hospital's domain and extract MRF entries.

For each hospital in mrf_links_2026-02-27.json:
  1. Try {top_url}/cms-hpt.txt (path-based base from the MRF file URL)
  2. If not found, try {domain_root}/cms-hpt.txt (scheme + host only)

Parsed entries are written as JSONL with: location-name, source-page-url, mrf-url.
"""

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from urllib.parse import urlparse

import requests
from curl_cffi import requests as cffi_requests

INPUT_FILE = "/Users/gyeh/Code/hospital-mrf/hospitals/mrf_links_2026-02-27.json"
OUTPUT_FILE = "cms-hpt.jsonl"
FAILED_FILE = "failed-cms-hpt.jsonl"

USER_AGENTS = [
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "curl/8.7.1",
]


def get_base_url(url):
    """Extract scheme://host/ from a full URL."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/"


def parse_cms_hpt(text):
    """Parse cms-hpt.txt key-value format into a list of entry dicts."""
    entries = []
    current = {}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            if current:
                entries.append(current)
                current = {}
            continue

        if ": " in line:
            key, _, value = line.partition(": ")
            key = key.strip().lower()
            current[key] = value.strip()

    if current:
        entries.append(current)

    return entries


def _is_valid_cms_hpt(text):
    return text and "location-name" in text[:2000]


def try_fetch_cms_hpt(url):
    """Try to GET cms-hpt.txt at the given URL. Returns text or None.

    Tries multiple strategies to bypass WAF/bot-detection:
      1. requests with browser User-Agent
      2. requests with curl User-Agent
      3. curl_cffi with Chrome TLS fingerprint (bypasses Akamai etc.)
    """
    for ua in USER_AGENTS:
        try:
            resp = requests.get(
                url, timeout=20, allow_redirects=True,
                headers={"User-Agent": ua, "Accept": "*/*"},
            )
            if resp.status_code == 200 and _is_valid_cms_hpt(resp.text):
                return resp.text
        except Exception:
            pass

    # Fallback: curl_cffi with real Chrome TLS fingerprint
    try:
        resp = cffi_requests.get(url, timeout=20, allow_redirects=True, impersonate="chrome")
        if resp.status_code == 200 and _is_valid_cms_hpt(resp.text):
            return resp.text
    except Exception:
        pass

    return None


def fetch_for_domain(domain_url):
    """Try cms-hpt.txt at the given base URL. Returns (url, text) or None."""
    cms_url = domain_url.rstrip("/") + "/cms-hpt.txt"
    text = try_fetch_cms_hpt(cms_url)
    if text:
        return cms_url, text
    return None


def fetch_domains_concurrent(domains, label=""):
    """Fetch cms-hpt.txt for a list of domains. Returns dict of domain -> (cms_url, text)."""
    found = {}
    total = len(domains)
    done = 0

    if label:
        print(f"\n{label}: checking {total} domains ...")

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fetch_for_domain, d): d for d in domains}

        for future in as_completed(futures):
            domain = futures[future]
            result = future.result()
            done += 1

            if result:
                found[domain] = result

            if done % 100 == 0 or done == total:
                print(f"  {done}/{total} checked, {len(found)} found")

    return found


def write_entries(found, output_file):
    """Parse cms-hpt.txt results and write JSONL. Returns entry count."""
    total = 0
    with open(output_file, "w", encoding="utf-8") as out:
        for domain, (cms_url, text) in sorted(found.items()):
            for entry in parse_cms_hpt(text):
                mrf = entry.get("mrf-url", "")
                if not mrf:
                    continue
                record = {
                    "location-name": entry.get("location-name", ""),
                    "source-page-url": entry.get("source-page-url", ""),
                    "mrf-url": mrf,
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                total += 1
    return total


def main():
    with open(INPUT_FILE) as f:
        hospitals = json.load(f)

    print(f"Loaded {len(hospitals)} entries from {INPUT_FILE}")

    # Map each URL-domain to its hospital entries
    url_domain_to_hospitals = {}
    for entry in hospitals:
        domain = get_base_url(entry["url"])
        url_domain_to_hospitals.setdefault(domain, []).append(entry)

    url_domains = list(url_domain_to_hospitals.keys())
    print(f"Unique URL domains: {len(url_domains)}")

    # --- Pass 1: try cms-hpt.txt on URL domains ---
    found = fetch_domains_concurrent(url_domains, label="Pass 1 (url domains)")
    print(f"Pass 1: cms-hpt.txt found on {len(found)}/{len(url_domains)} domains")

    # --- Pass 2: for failures, try top_url domains as backup ---
    failed_url_domains = set(url_domains) - set(found.keys())

    # Collect unique top_url domains for failed entries, excluding
    # domains already found in pass 1
    backup_domain_to_hospitals = {}
    for url_domain in failed_url_domains:
        for entry in url_domain_to_hospitals[url_domain]:
            top_url = entry.get("top_url", "")
            if not top_url or not top_url.startswith("http"):
                continue
            top_domain = get_base_url(top_url)
            # Skip if already resolved in pass 1 or same as the URL domain
            if top_domain in found or top_domain == url_domain:
                continue
            backup_domain_to_hospitals.setdefault(top_domain, []).append(entry)

    backup_domains = list(backup_domain_to_hospitals.keys())

    if backup_domains:
        backup_found = fetch_domains_concurrent(
            backup_domains, label="Pass 2 (top_url backup)"
        )
        print(f"Pass 2: cms-hpt.txt found on {len(backup_found)}/{len(backup_domains)} backup domains")
        found.update(backup_found)
    else:
        print("\nPass 2: no backup domains to check")

    print(f"\nTotal domains with cms-hpt.txt: {len(found)}")

    # Write successful entries
    total_entries = write_entries(found, OUTPUT_FILE)
    print(f"Wrote {total_entries} entries to {OUTPUT_FILE}")

    # Write failed entries — hospitals where neither url nor top_url had cms-hpt.txt
    all_found_domains = set(found.keys())
    failed_count = 0
    with open(FAILED_FILE, "w", encoding="utf-8") as out:
        for url_domain in sorted(failed_url_domains):
            for entry in url_domain_to_hospitals[url_domain]:
                # Check if this entry's top_url was resolved in pass 2
                top_url = entry.get("top_url", "")
                top_domain = get_base_url(top_url) if top_url.startswith("http") else ""
                if top_domain in all_found_domains:
                    continue

                record = {
                    "system": entry["system"],
                    "hospital": entry["hospital"],
                    "url": entry["url"],
                    "top_url": top_url,
                    "count": entry["count"],
                    "state": entry["state"],
                    "domain": url_domain,
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                failed_count += 1

    print(f"Wrote {failed_count} entries to {FAILED_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
