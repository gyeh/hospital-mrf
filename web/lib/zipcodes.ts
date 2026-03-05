import zipData from "@/data/zipcodes.json";

const zipcodes = zipData as Record<string, { lat: number; lon: number }>;

export function lookupZip(zip: string): { lat: number; lon: number } | null {
  const normalized = zip.trim().padStart(5, "0");
  return zipcodes[normalized] ?? null;
}
