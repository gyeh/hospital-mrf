"use client";

import { SearchResult } from "@/lib/types";
import { PriceQueryResult } from "@/lib/duckdb";
import HospitalCard from "./HospitalCard";

interface Props {
  results: SearchResult[];
  codeType: string;
  codeValue: string;
  zipCode: string;
  hoveredHospital: string | null;
  onHoverHospital: (name: string | null) => void;
  priceData: PriceQueryResult | null;
  priceLoading: boolean;
  priceError: string | null;
}

export default function ResultsList({
  results,
  codeType,
  codeValue,
  zipCode,
  hoveredHospital,
  onHoverHospital,
  priceData,
}: Props) {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-baseline justify-between">
        <h2 className="text-sm font-semibold text-warm-900">
          {results.length} hospital{results.length !== 1 ? "s" : ""} with
          pricing
        </h2>
        <span className="text-xs text-warm-500">
          {codeType}: {codeValue}
        </span>
      </div>

      {priceData && (
        <div className="rounded-lg border border-green-200 bg-green-50 px-4 py-2.5 text-sm text-green-700">
          {priceData.totalRows} charge{priceData.totalRows !== 1 ? "s" : ""}{" "}
          found
          <span className="ml-2 text-green-600">
            ({(priceData.durationMs / 1000).toFixed(1)}s)
          </span>
        </div>
      )}

      <div className="flex flex-col gap-2">
        {results.map((hospital) => (
          <HospitalCard
            key={hospital.hospitalName + hospital.address}
            hospital={hospital}
            zipCode={zipCode}
            isHovered={hoveredHospital === hospital.hospitalName}
            onMouseEnter={() => onHoverHospital(hospital.hospitalName)}
            onMouseLeave={() => onHoverHospital(null)}
            charges={priceData?.chargesByHospital[hospital.hospitalName]}
          />
        ))}
      </div>
    </div>
  );
}
