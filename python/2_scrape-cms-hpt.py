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


# CMS TXT file specification (per 45 CFR 180.50(d)(6) and CMS TXT generator):
# https://cmsgov.github.io/hpt-tool/txt-generator/
#
# Format: key: value (one per line), entries separated by blank lines.
# Required fields per entry:
#   location-name, source-page-url, mrf-url, contact-name, contact-email
VALID_KEYS = {"location-name", "source-page-url", "mrf-url", "contact-name", "contact-email"}
REQUIRED_KEYS = {"location-name", "mrf-url"}


class CMSParseError(RuntimeError):
    """Raised when a cms-hpt.txt file cannot be parsed or has no valid entries."""
    pass


def parse_cms_hpt(text, cms_url="<unknown>"):
    """Parse cms-hpt.txt per CMS specification.

    Skips entries missing required fields (with warnings).
    Raises CMSParseError if zero valid entries are found.
    Returns (entries, warnings) where warnings is a list of strings.
    """
    entries = []
    warnings = []
    current = {}
    entry_start_line = 0

    lines = text.splitlines()
    for lineno, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()

        if not line:
            # Blank line = entry separator. But some hospitals put blank lines
            # between every field. Only flush if we have both required fields,
            # otherwise keep accumulating into the current entry.
            if current and not (REQUIRED_KEYS - set(current.keys())):
                _collect_entry(current, entry_start_line, cms_url, entries, warnings)
                current = {}
            continue

        # Skip non-key:value lines (comments, decorative lines like ****).
        # A valid key:value line must have a colon, and the key portion must
        # contain only lowercase letters and hyphens (per CMS spec keys).
        if ":" not in line:
            # If we have accumulated fields but hit a non-key line, it might
            # be a new "section" in a malformed file. Flush if complete.
            if current and not (REQUIRED_KEYS - set(current.keys())):
                _collect_entry(current, entry_start_line, cms_url, entries, warnings)
                current = {}
            continue

        key, _, value = line.partition(":")
        key = key.strip().lower()

        # Skip lines where the "key" doesn't look like a CMS field name
        # (e.g. decorative text, prose sentences with colons, URLs).
        if not key or not all(c.isalpha() or c == "-" for c in key):
            continue

        if key not in VALID_KEYS:
            continue

        # If we see a key that's already in current, this is a new entry.
        # (e.g. "location-name" appearing again means new hospital.)
        if key in current:
            _collect_entry(current, entry_start_line, cms_url, entries, warnings)
            current = {}

        if not current:
            entry_start_line = lineno

        current[key] = value.strip()

    if current:
        _collect_entry(current, entry_start_line, cms_url, entries, warnings)

    if not entries:
        raise CMSParseError(
            f"no valid entries found in {cms_url}\n"
            f"  file length: {len(text)} bytes, {len(lines)} lines\n"
            f"  first 500 chars: {text[:500]!r}"
        )

    return entries, warnings


def _collect_entry(entry, start_line, cms_url, entries, warnings):
    """Validate and collect a single entry; append warning if invalid."""
    missing = REQUIRED_KEYS - set(entry.keys())
    if missing:
        warnings.append(
            f"line {start_line} in {cms_url}: "
            f"skipping entry missing {missing}: {entry}"
        )
        return
    entries.append(entry)


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
    """Parse cms-hpt.txt results and write JSONL.

    Returns (entry_count, all_warnings, fatal_errors).
    fatal_errors: domains where zero entries could be parsed.
    all_warnings: entries skipped due to missing required fields.
    """
    total = 0
    all_warnings = []
    fatal_errors = []
    with open(output_file, "w", encoding="utf-8") as out:
        for domain, (cms_url, text) in sorted(found.items()):
            try:
                entries, warnings = parse_cms_hpt(text, cms_url)
            except CMSParseError as e:
                fatal_errors.append((domain, cms_url, str(e)))
                continue

            all_warnings.extend(warnings)

            for entry in entries:
                record = {
                    "location-name": entry.get("location-name", ""),
                    "source-page-url": entry.get("source-page-url", ""),
                    "mrf-url": entry["mrf-url"],
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                total += 1
    return total, all_warnings, fatal_errors


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
    total_entries, all_warnings, fatal_errors = write_entries(found, OUTPUT_FILE)
    print(f"Wrote {total_entries} entries to {OUTPUT_FILE}")

    if all_warnings:
        print(f"\nSkipped entries ({len(all_warnings)} entries with missing required fields):")
        for w in all_warnings:
            print(f"  WARN: {w}")

    if fatal_errors:
        print(f"\nFatal parse errors ({len(fatal_errors)} domains with zero valid entries):")
        for domain, cms_url, err in fatal_errors:
            print(f"  ERROR: {err}")
        # Move parse-error domains out of found so they appear in failed output.
        error_domains = {domain for domain, _, _ in fatal_errors}
        found = {k: v for k, v in found.items() if k not in error_domains}

    # --- Pass 3: for hospitals with no cms-hpt.txt, verify the MRF URL exists ---
    # Collect entries that are still unresolved.
    all_found_domains = set(found.keys())
    unresolved = []
    for url_domain in failed_url_domains:
        for entry in url_domain_to_hospitals[url_domain]:
            top_url = entry.get("top_url", "")
            top_domain = get_base_url(top_url) if top_url.startswith("http") else ""
            if top_domain in all_found_domains:
                continue
            unresolved.append(entry)

    # Also include entries from parse-error domains.
    for url_domain in (set(url_domain_to_hospitals.keys()) - all_found_domains - failed_url_domains):
        for entry in url_domain_to_hospitals[url_domain]:
            unresolved.append(entry)

    # Deduplicate by URL.
    seen_urls = set()
    unique_unresolved = []
    for entry in unresolved:
        url = entry["url"]
        if url not in seen_urls:
            seen_urls.add(url)
            unique_unresolved.append(entry)

    fallback_count = 0
    failed_count = 0
    if unique_unresolved:
        print(f"\nPass 3 (verify MRF URLs): checking {len(unique_unresolved)} URLs ...")
        verified, not_found = _verify_mrf_urls(unique_unresolved)
        fallback_count = len(verified)
        failed_count = len(not_found)
        print(f"Pass 3: {fallback_count} verified, {failed_count} not found")

        # Append verified entries to the output file.
        with open(OUTPUT_FILE, "a", encoding="utf-8") as out:
            for entry in verified:
                record = {
                    "location-name": entry["hospital"],
                    "source-page-url": "",
                    "mrf-url": entry["url"],
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                total_entries += 1

        # Write truly failed entries.
        with open(FAILED_FILE, "w", encoding="utf-8") as out:
            for entry in not_found:
                record = {
                    "system": entry["system"],
                    "hospital": entry["hospital"],
                    "url": entry["url"],
                    "top_url": entry.get("top_url", ""),
                    "count": entry["count"],
                    "state": entry["state"],
                    "domain": get_base_url(entry["url"]),
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
    else:
        # No unresolved entries — write empty failed file.
        with open(FAILED_FILE, "w") as out:
            pass

    print(f"\nTotal: {total_entries} entries to {OUTPUT_FILE}, {failed_count} to {FAILED_FILE}")
    return 0


def _check_url_exists(entry):
    """HEAD/GET check if an MRF URL is reachable. Returns (entry, exists)."""
    url = entry["url"]
    for ua in USER_AGENTS:
        try:
            resp = requests.head(
                url, timeout=15, allow_redirects=True,
                headers={"User-Agent": ua},
            )
            if resp.status_code == 200:
                return entry, True
            # Some servers don't support HEAD; try GET with range.
            if resp.status_code in (403, 405):
                resp = requests.get(
                    url, timeout=15, allow_redirects=True, stream=True,
                    headers={"User-Agent": ua, "Range": "bytes=0-0"},
                )
                if resp.status_code in (200, 206):
                    return entry, True
        except Exception:
            continue

    # Fallback: curl_cffi
    try:
        resp = cffi_requests.head(url, timeout=15, allow_redirects=True, impersonate="chrome")
        if resp.status_code == 200:
            return entry, True
    except Exception:
        pass

    return entry, False


def _verify_mrf_urls(entries):
    """Verify MRF URLs exist in parallel. Returns (verified, not_found)."""
    verified = []
    not_found = []
    total = len(entries)
    done = 0

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_check_url_exists, e): e for e in entries}
        for future in as_completed(futures):
            entry, exists = future.result()
            done += 1
            if exists:
                verified.append(entry)
            else:
                not_found.append(entry)
            if done % 50 == 0 or done == total:
                print(f"  {done}/{total} checked, {len(verified)} verified")

    return verified, not_found


if __name__ == "__main__":
    sys.exit(main())
