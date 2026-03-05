"use client";

import { SearchResult } from "@/lib/types";
import HospitalCard from "./HospitalCard";

interface Props {
  results: SearchResult[];
  codeType: string;
  codeValue: string;
  hoveredHospital: string | null;
  onHoverHospital: (name: string | null) => void;
}

export default function ResultsList({
  results,
  codeType,
  codeValue,
  hoveredHospital,
  onHoverHospital,
}: Props) {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-baseline justify-between">
        <h2 className="text-sm font-semibold text-warm-900">
          {results.length} hospital{results.length !== 1 ? "s" : ""} found
        </h2>
        <span className="text-xs text-warm-500">
          Searching {codeType}: {codeValue}
        </span>
      </div>
      <div className="flex max-h-[500px] flex-col gap-2 overflow-y-auto pr-1">
        {results.map((hospital) => (
          <HospitalCard
            key={hospital.hospitalName + hospital.address}
            hospital={hospital}
            isHovered={hoveredHospital === hospital.hospitalName}
            onMouseEnter={() => onHoverHospital(hospital.hospitalName)}
            onMouseLeave={() => onHoverHospital(null)}
          />
        ))}
        {results.length === 0 && (
          <p className="py-8 text-center text-sm text-warm-500">
            No hospitals found in this area. Try a larger radius or different zip
            code.
          </p>
        )}
      </div>
    </div>
  );
}
