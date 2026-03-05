#!/usr/bin/env python3
"""Search hospital price transparency data by zip code and billing code.

Loads hospital MRF metadata from a JSONL log file, finds nearby hospitals
using haversine distance, then queries their S3 parquet files via DuckDB
for matching billing codes.
"""

import argparse
import json
import math
import sys
import time

import duckdb

# ── Zip code data (embedded subset not needed — we use the web/data file) ──

ZIPCODES: dict[str, tuple[float, float]] = {}

# ── Code type → parquet column name mapping ──

CODE_TYPE_COLUMNS = {
    "CPT": "cpt_code",
    "HCPCS": "hcpcs_code",
    "MS-DRG": "ms_drg_code",
    "NDC": "ndc_code",
    "RC": "rc_code",
    "ICD": "icd_code",
    "DRG": "drg_code",
    "CDM": "cdm_code",
    "LOCAL": "local_code",
    "APC": "apc_code",
    "EAPG": "eapg_code",
    "HIPPS": "hipps_code",
    "CDT": "cdt_code",
    "R-DRG": "r_drg_code",
    "S-DRG": "s_drg_code",
    "APS-DRG": "aps_drg_code",
    "AP-DRG": "ap_drg_code",
    "APR-DRG": "apr_drg_code",
    "TRIS-DRG": "tris_drg_code",
}


# ── Data types ──

class Hospital:
    __slots__ = (
        "name", "address", "lat", "lon", "output_file",
        "last_updated_on", "license_state",
    )

    def __init__(self, name, address, lat, lon, output_file, last_updated_on, license_state):
        self.name = name
        self.address = address
        self.lat = lat
        self.lon = lon
        self.output_file = output_file
        self.last_updated_on = last_updated_on
        self.license_state = license_state


# ── Haversine ──

_EARTH_RADIUS_MILES = 3958.8


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_MILES * math.asin(math.sqrt(a))


# ── Load data ──

def load_zipcodes(path: str) -> None:
    """Load zip code → (lat, lon) lookup from JSON file."""
    global ZIPCODES
    with open(path) as f:
        data = json.load(f)
    ZIPCODES = {k: (v["lat"], v["lon"]) for k, v in data.items()}
    print(f"Loaded {len(ZIPCODES)} zip codes", file=sys.stderr)


def load_hospitals(jsonl_path: str) -> list[Hospital]:
    """Load hospitals with valid geocodes and output files from JSONL."""
    hospitals = []
    with open(jsonl_path) as f:
        for line in f:
            entry = json.loads(line)
            if not entry.get("success") or not entry.get("output_file"):
                continue
            geocodes = entry.get("geocodes") or []
            geo = next((g for g in geocodes if g.get("matched")), None)
            if not geo:
                continue
            address = ""
            addrs = entry.get("hospital_addresses") or []
            if addrs:
                address = addrs[0]
            hospitals.append(Hospital(
                name=entry["hospital_name"],
                address=address,
                lat=geo["latitude"],
                lon=geo["longitude"],
                output_file=entry["output_file"],
                last_updated_on=entry.get("last_updated_on", ""),
                license_state=entry.get("license_state"),
            ))
    print(f"Loaded {len(hospitals)} hospitals with geocodes", file=sys.stderr)
    return hospitals


# ── Search ──

def find_nearby(
    hospitals: list[Hospital],
    lat: float,
    lon: float,
    radius_miles: float = 40.0,
    limit: int = 30,
) -> list[tuple[Hospital, float]]:
    """Find hospitals within radius, sorted by distance."""
    results = []
    for h in hospitals:
        dist = haversine(lat, lon, h.lat, h.lon)
        if dist <= radius_miles:
            results.append((h, round(dist, 1)))
    results.sort(key=lambda x: x[1])
    return results[:limit]


def query_prices(
    nearby: list[tuple[Hospital, float]],
    code_type: str,
    code_value: str,
) -> None:
    """Query all nearby parquet files in a single DuckDB query and print results."""
    column = CODE_TYPE_COLUMNS[code_type]

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("CALL load_aws_credentials();")
    con.execute("SET s3_region = 'us-east-1';")

    # Build a lookup from output_file → (hospital, distance)
    # Deduplicate files — multiple nearby entries can share the same parquet file
    file_to_hospitals: dict[str, list[tuple[Hospital, float]]] = {}
    for hospital, distance in nearby:
        file_to_hospitals.setdefault(hospital.output_file, []).append((hospital, distance))

    s3_paths = list(file_to_hospitals.keys())

    print(f"\nSearching {len(s3_paths)} parquet files ({len(nearby)} hospital locations) for {code_type} = {code_value}\n")

    t0 = time.perf_counter()
    try:
        rows = con.execute(
            f"""
            SELECT
                filename,
                description,
                setting,
                payer_name,
                plan_name,
                gross_charge,
                discounted_cash,
                negotiated_dollar,
                negotiated_percentage,
                min_charge,
                max_charge,
                methodology
            FROM read_parquet($1, filename=true)
            WHERE {column} = $2
            ORDER BY filename, gross_charge DESC NULLS LAST
            """,
            [s3_paths, code_value],
        ).fetchall()
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"  [ERROR] Query failed after {elapsed:.2f}s: {e}", file=sys.stderr)
        con.close()
        return

    elapsed = time.perf_counter() - t0
    print(f"Query completed in {elapsed:.2f}s — {len(rows)} total rows\n")
    print("=" * 100)

    if not rows:
        print(f"\nNo results found for {code_type} = {code_value} at any nearby hospital.")
        con.close()
        return

    # Group results by file
    rows_by_file: dict[str, list] = {}
    for row in rows:
        rows_by_file.setdefault(row[0], []).append(row[1:])

    for s3_path, charge_rows in rows_by_file.items():
        for hospital, distance in file_to_hospitals[s3_path]:
            print(f"\n{hospital.name}")
            print(f"  {hospital.address}")
            if hospital.license_state:
                print(f"  State: {hospital.license_state}", end="")
            print(f"  |  Distance: {distance} mi  |  Updated: {hospital.last_updated_on}")
            print(f"  File: {s3_path}")
            print(f"  {len(charge_rows)} matching charge line(s):")
            print()

            print(f"  {'Description':<50} {'Setting':<12} {'Gross':>10} {'Cash':>10} {'Negotiated':>10} {'Payer':<25} {'Plan':<20}")
            print(f"  {'-'*50} {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*25} {'-'*20}")

            for r in charge_rows:
                desc = (r[0] or "")[:50]
                setting = (r[1] or "")[:12]
                payer = (r[2] or "")[:25]
                plan = (r[3] or "")[:20]
                gross = f"${r[4]:,.2f}" if r[4] is not None else ""
                cash = f"${r[5]:,.2f}" if r[5] is not None else ""
                neg = f"${r[6]:,.2f}" if r[6] is not None else ""
                print(f"  {desc:<50} {setting:<12} {gross:>10} {cash:>10} {neg:>10} {payer:<25} {plan:<20}")

    print()
    con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Search hospital price transparency data by zip code and billing code.",
    )
    parser.add_argument("zip_code", help="5-digit US zip code")
    parser.add_argument("code_type", choices=sorted(CODE_TYPE_COLUMNS.keys()),
                        help="Billing code type (e.g. CPT, HCPCS, MS-DRG)")
    parser.add_argument("code_value", help="Billing code value (e.g. 99213)")
    parser.add_argument("--jsonl", required=True,
                        help="Path to hospital JSONL log file")
    parser.add_argument("--zipcodes", default="web/data/zipcodes.json",
                        help="Path to zipcodes.json (default: web/data/zipcodes.json)")
    parser.add_argument("--radius", type=float, default=40.0,
                        help="Search radius in miles (default: 40)")
    parser.add_argument("--limit", type=int, default=30,
                        help="Max number of hospitals to query (default: 30)")

    args = parser.parse_args()

    zip_code = args.zip_code.strip().zfill(5)
    if not zip_code.isdigit() or len(zip_code) != 5:
        print(f"Error: invalid zip code '{args.zip_code}'", file=sys.stderr)
        sys.exit(1)

    load_zipcodes(args.zipcodes)

    if zip_code not in ZIPCODES:
        print(f"Error: zip code {zip_code} not found", file=sys.stderr)
        sys.exit(1)

    center_lat, center_lon = ZIPCODES[zip_code]
    print(f"Zip {zip_code}: ({center_lat}, {center_lon})", file=sys.stderr)

    hospitals = load_hospitals(args.jsonl)
    nearby = find_nearby(hospitals, center_lat, center_lon, args.radius, args.limit)

    if not nearby:
        print(f"No hospitals found within {args.radius} miles of {zip_code}")
        sys.exit(0)

    print(f"Found {len(nearby)} hospitals within {args.radius} mi", file=sys.stderr)

    query_prices(nearby, args.code_type, args.code_value)


if __name__ == "__main__":
    main()
