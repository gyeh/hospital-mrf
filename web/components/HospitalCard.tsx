"use client";

import { useState } from "react";
import Link from "next/link";
import { SearchResult } from "@/lib/types";
import { ChargeRow } from "@/lib/duckdb";
import { hospitalSlug } from "@/lib/search-client";

interface Props {
  hospital: SearchResult;
  isHovered: boolean;
  onMouseEnter: () => void;
  onMouseLeave: () => void;
  charges?: ChargeRow[];
}

function formatCurrency(value: number | null): string {
  if (value == null) return "";
  return `$${value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

const INITIAL_ROWS = 5;

export default function HospitalCard({
  hospital,
  isHovered,
  onMouseEnter,
  onMouseLeave,
  charges,
}: Props) {
  const [expanded, setExpanded] = useState(false);
  const displayCharges =
    charges && !expanded ? charges.slice(0, INITIAL_ROWS) : charges;

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
        <h3 className="text-sm font-semibold leading-tight">
          <Link
            href={`/hospital/${hospitalSlug(hospital.outputFile)}`}
            className="text-blue-700 hover:text-blue-900 hover:underline"
          >
            {hospital.hospitalName}
          </Link>
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

      {/* Price data table */}
      {charges && charges.length > 0 && (
        <div className="mt-3 border-t border-warm-100 pt-3">
          <p className="mb-2 text-xs font-medium text-warm-700">
            {charges.length} charge{charges.length !== 1 ? "s" : ""}
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead>
                <tr className="border-b border-warm-100 text-warm-500">
                  <th className="pb-1 pr-3 font-medium">Description</th>
                  <th className="pb-1 pr-3 font-medium">Setting</th>
                  <th className="pb-1 pr-3 text-right font-medium">Gross</th>
                  <th className="pb-1 pr-3 text-right font-medium">Cash</th>
                  <th className="pb-1 pr-3 text-right font-medium">Neg. $</th>
                  <th className="pb-1 font-medium">Payer / Plan</th>
                </tr>
              </thead>
              <tbody className="text-warm-700">
                {displayCharges!.map((c, i) => (
                  <tr
                    key={i}
                    className="border-b border-warm-50 last:border-0"
                  >
                    <td
                      className="max-w-[180px] truncate py-1 pr-3"
                      title={c.description}
                    >
                      {c.description}
                    </td>
                    <td className="whitespace-nowrap py-1 pr-3">{c.setting}</td>
                    <td className="whitespace-nowrap py-1 pr-3 text-right">
                      {formatCurrency(c.grossCharge)}
                    </td>
                    <td className="whitespace-nowrap py-1 pr-3 text-right">
                      {formatCurrency(c.discountedCash)}
                    </td>
                    <td className="whitespace-nowrap py-1 pr-3 text-right">
                      {formatCurrency(c.negotiatedDollar)}
                    </td>
                    <td
                      className="max-w-[160px] truncate py-1"
                      title={
                        c.payerName
                          ? `${c.payerName}${c.planName ? ` / ${c.planName}` : ""}`
                          : ""
                      }
                    >
                      {c.payerName}
                      {c.planName ? ` / ${c.planName}` : ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {charges.length > INITIAL_ROWS && (
            <button
              onClick={() => setExpanded(!expanded)}
              className="mt-2 text-xs font-medium text-blue-600 hover:text-blue-700"
            >
              {expanded
                ? "Show less"
                : `Show all ${charges.length} charges`}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
