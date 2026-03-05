import { NextResponse } from "next/server";
import { lookupZip } from "@/lib/zipcodes";
import { searchNearby, getTotalHospitals } from "@/lib/hospitals";
import { SearchRequest, SearchResponse } from "@/lib/types";

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

export async function POST(request: Request) {
  let body: SearchRequest;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const { zipCode, codeType, codeValue, radius, limit } = body;

  if (!zipCode || !/^\d{5}$/.test(zipCode.trim())) {
    return NextResponse.json(
      { error: "zipCode is required and must be 5 digits" },
      { status: 400 }
    );
  }

  if (!codeType || !VALID_CODE_TYPES.has(codeType)) {
    return NextResponse.json(
      { error: `codeType must be one of: ${[...VALID_CODE_TYPES].join(", ")}` },
      { status: 400 }
    );
  }

  if (!codeValue || codeValue.trim().length === 0) {
    return NextResponse.json(
      { error: "codeValue is required" },
      { status: 400 }
    );
  }

  const coords = lookupZip(zipCode.trim());
  if (!coords) {
    return NextResponse.json(
      { error: `Zip code ${zipCode} not found` },
      { status: 404 }
    );
  }

  const results = searchNearby(
    coords.lat,
    coords.lon,
    radius ?? 40,
    limit ?? 30
  );

  const response: SearchResponse = {
    results,
    center: coords,
    zipCode: zipCode.trim(),
    totalHospitals: getTotalHospitals(),
  };

  return NextResponse.json(response);
}
