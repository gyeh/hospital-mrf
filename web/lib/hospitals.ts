import fs from "fs";
import path from "path";
import { HospitalRecord, SearchResult } from "./types";
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
  hospital_addresses: string[] | null;
  license_number: string | null;
  license_state: string | null;
  type_2_npis: string[] | null;
  last_updated_on: string;
  geocodes?: RawGeocode[];
}

let hospitals: HospitalRecord[] | null = null;

function loadHospitals(): HospitalRecord[] {
  if (hospitals) return hospitals;

  const filePath = path.join(process.cwd(), "data", "hospitals.jsonl");
  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.trim().split("\n");

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
      hospitalName: entry.hospital_name,
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

  hospitals = records;
  console.log(`Loaded ${records.length} hospitals with geocodes`);
  return records;
}

export function searchNearby(
  lat: number,
  lon: number,
  radiusMiles = 40,
  limit = 30
): SearchResult[] {
  const all = loadHospitals();

  const results: SearchResult[] = [];
  for (const h of all) {
    const dist = haversineDistance(lat, lon, h.lat, h.lon);
    if (dist <= radiusMiles) {
      results.push({ ...h, distanceMiles: Math.round(dist * 10) / 10 });
    }
  }

  results.sort((a, b) => a.distanceMiles - b.distanceMiles);
  return results.slice(0, limit);
}

export function getTotalHospitals(): number {
  return loadHospitals().length;
}
