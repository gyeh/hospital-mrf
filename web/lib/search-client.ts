import { HospitalRecord, SearchResult, SearchResponse } from "./types";
import { haversineDistance } from "./haversine";

interface RawGeocode {
  address: string;
  matched: boolean;
  latitude: number;
  longitude: number;
}

interface RawEntry {
  success: boolean;
  output_file?: string;
  hospital_name: string;
  cms_hpt_location_name?: string;
  hospital_addresses: string[] | null;
  license_number: string | null;
  license_state: string | null;
  type_2_npis: string[] | null;
  last_updated_on: string;
  geocodes?: RawGeocode[];
}

let zipcodesCache: Record<string, { lat: number; lon: number }> | null = null;
let hospitalsCache: HospitalRecord[] | null = null;

async function loadZipcodes(): Promise<
  Record<string, { lat: number; lon: number }>
> {
  if (zipcodesCache) return zipcodesCache;
  const res = await fetch("/data/zipcodes.json");
  if (!res.ok) throw new Error("Failed to load zip code data");
  zipcodesCache = await res.json();
  return zipcodesCache!;
}

async function loadHospitals(): Promise<HospitalRecord[]> {
  if (hospitalsCache) return hospitalsCache;
  const res = await fetch("/data/hospitals.jsonl");
  if (!res.ok) throw new Error("Failed to load hospital data");
  const text = await res.text();
  const lines = text.trim().split("\n");

  const records: HospitalRecord[] = [];
  for (const line of lines) {
    const entry: RawEntry = JSON.parse(line);
    if (!entry.success || !entry.output_file) continue;
    if (!entry.geocodes || entry.geocodes.length === 0) continue;

    const geo = entry.geocodes.find((g) => g.matched);
    if (!geo) continue;

    const address =
      entry.hospital_addresses && entry.hospital_addresses.length > 0
        ? entry.hospital_addresses[0]
        : "";

    records.push({
      hospitalName: entry.cms_hpt_location_name || entry.hospital_name,
      address,
      lat: geo.latitude,
      lon: geo.longitude,
      outputFile: entry.output_file,
      lastUpdatedOn: entry.last_updated_on,
      licenseNumber: entry.license_number,
      licenseState: entry.license_state,
      npis: entry.type_2_npis ?? [],
    });
  }

  hospitalsCache = records;
  return records;
}

function lookupZip(
  zipcodes: Record<string, { lat: number; lon: number }>,
  zip: string
): { lat: number; lon: number } | null {
  const normalized = zip.trim().padStart(5, "0");
  return zipcodes[normalized] ?? null;
}

function searchNearby(
  hospitals: HospitalRecord[],
  lat: number,
  lon: number,
  radiusMiles = 40,
  limit = 30
): SearchResult[] {
  const results: SearchResult[] = [];
  for (const h of hospitals) {
    const dist = haversineDistance(lat, lon, h.lat, h.lon);
    if (dist <= radiusMiles) {
      results.push({ ...h, distanceMiles: Math.round(dist * 10) / 10 });
    }
  }
  results.sort((a, b) => a.distanceMiles - b.distanceMiles);
  return results.slice(0, limit);
}

const VALID_CODE_TYPES = new Set([
  "CPT",
  "HCPCS",
  "MS-DRG",
  "NDC",
  "RC",
  "ICD",
  "DRG",
  "CDM",
  "LOCAL",
  "APC",
  "EAPG",
  "HIPPS",
  "CDT",
  "R-DRG",
  "S-DRG",
  "APS-DRG",
  "AP-DRG",
  "APR-DRG",
  "TRIS-DRG",
]);

export function hospitalSlug(outputFile: string): string {
  const filename = outputFile.split("/").pop() ?? "";
  return filename.replace(/\.parquet$/, "");
}

export async function findHospitalBySlug(
  slug: string
): Promise<HospitalRecord | null> {
  const hospitals = await loadHospitals();
  return hospitals.find((h) => hospitalSlug(h.outputFile) === slug) ?? null;
}

export async function clientSearch(
  zipCode: string,
  codeType: string,
  codeValue: string,
  radius?: number,
  limit?: number
): Promise<SearchResponse> {
  if (!zipCode || !/^\d{5}$/.test(zipCode.trim())) {
    throw new Error("zipCode is required and must be 5 digits");
  }
  if (!codeType || !VALID_CODE_TYPES.has(codeType)) {
    throw new Error(
      `codeType must be one of: ${[...VALID_CODE_TYPES].join(", ")}`
    );
  }
  if (!codeValue || codeValue.trim().length === 0) {
    throw new Error("codeValue is required");
  }

  const [zipcodes, hospitals] = await Promise.all([
    loadZipcodes(),
    loadHospitals(),
  ]);

  const coords = lookupZip(zipcodes, zipCode.trim());
  if (!coords) {
    throw new Error(`Zip code ${zipCode} not found`);
  }

  const results = searchNearby(
    hospitals,
    coords.lat,
    coords.lon,
    radius ?? 40,
    limit ?? 300
  );

  return {
    results,
    center: coords,
    zipCode: zipCode.trim(),
    totalHospitals: hospitals.length,
  };
}
