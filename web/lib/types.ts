export interface HospitalRecord {
  hospitalName: string;
  address: string;
  lat: number;
  lon: number;
  outputFile: string;
  lastUpdatedOn: string;
  licenseNumber: string | null;
  licenseState: string | null;
  npis: string[];
}

export interface SearchRequest {
  zipCode: string;
  codeType: string;
  codeValue: string;
  radius?: number;
  limit?: number;
}

export interface SearchResult extends HospitalRecord {
  distanceMiles: number;
}

export interface SearchResponse {
  results: SearchResult[];
  center: { lat: number; lon: number };
  zipCode: string;
  totalHospitals: number;
}

export type { ChargeRow, PriceQueryResult } from "./duckdb";
