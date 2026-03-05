"use client";

import { SearchResult } from "@/lib/types";

interface Props {
  hospital: SearchResult;
  isHovered: boolean;
  onMouseEnter: () => void;
  onMouseLeave: () => void;
}

export default function HospitalCard({
  hospital,
  isHovered,
  onMouseEnter,
  onMouseLeave,
}: Props) {
  return (
    <div
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      className={`rounded-xl border bg-white p-4 shadow-sm transition-all ${
        isHovered
          ? "border-blue-400 shadow-md ring-1 ring-blue-200"
          : "border-warm-200"
      }`}
    >
      <div className="flex items-start justify-between gap-2">
        <h3 className="text-sm font-semibold leading-tight text-warm-900">
          {hospital.hospitalName}
        </h3>
        <span className="shrink-0 text-xs font-medium text-blue-600">
          {hospital.distanceMiles} mi
        </span>
      </div>
      <p className="mt-1 text-xs text-warm-600">{hospital.address}</p>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        {hospital.licenseState && (
          <span className="rounded-md bg-warm-100 px-2 py-0.5 text-xs font-medium text-warm-700">
            {hospital.licenseState}
          </span>
        )}
        {hospital.lastUpdatedOn && (
          <span className="text-xs text-warm-500">
            Updated {hospital.lastUpdatedOn}
          </span>
        )}
      </div>
    </div>
  );
}
