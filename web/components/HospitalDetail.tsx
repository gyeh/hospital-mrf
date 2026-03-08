"use client";

import { useState, useEffect, useRef, FormEvent } from "react";
import { HospitalRecord } from "@/lib/types";
import { ChargeRow, queryPrices } from "@/lib/duckdb";
import { findHospitalBySlug } from "@/lib/search-client";
import { CODE_TYPES } from "./SearchForm";

function formatCurrency(value: number | null): string {
  if (value == null) return "";
  return `$${value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export default function HospitalDetail({ slug }: { slug: string }) {
  const [hospital, setHospital] = useState<HospitalRecord | null>(null);
  const [notFound, setNotFound] = useState(false);

  const [codeType, setCodeType] = useState("CPT");
  const [codeValue, setCodeValue] = useState("");

  const [charges, setCharges] = useState<ChargeRow[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [durationMs, setDurationMs] = useState(0);

  const [progress, setProgress] = useState(0);
  const progressInterval = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    findHospitalBySlug(slug).then((h) => {
      if (h) setHospital(h);
      else setNotFound(true);
    });
  }, [slug]);

  function startProgress() {
    setProgress(0);
    if (progressInterval.current) clearInterval(progressInterval.current);
    const start = Date.now();
    progressInterval.current = setInterval(() => {
      const elapsed = (Date.now() - start) / 1000;
      const fast = 40 * (1 - Math.exp(-elapsed / 1.2));
      const slow = 50 * (1 - Math.exp(-elapsed / 8));
      setProgress(Math.min(fast + slow, 92));
    }, 100);
  }

  function stopProgress(): Promise<void> {
    return new Promise((resolve) => {
      if (progressInterval.current) {
        clearInterval(progressInterval.current);
        progressInterval.current = null;
      }
      setProgress(100);
      setTimeout(() => {
        setProgress(0);
        resolve();
      }, 500);
    });
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    if (!hospital) return;
    setLoading(true);
    setError(null);
    setCharges(null);
    startProgress();

    try {
      const result = await queryPrices(
        [{ hospitalName: hospital.hospitalName, outputFile: hospital.outputFile }],
        codeType,
        codeValue
      );
      setCharges(result.chargesByHospital[hospital.hospitalName] ?? []);
      setDurationMs(result.durationMs);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query failed");
    } finally {
      await stopProgress();
      setLoading(false);
    }
  }

  if (notFound) {
    return (
      <div className="rounded-xl border border-warm-200 bg-white py-12 text-center shadow-sm">
        <p className="text-sm text-warm-500">Hospital not found.</p>
      </div>
    );
  }

  if (!hospital) {
    return (
      <div className="rounded-xl border border-warm-200 bg-white py-12 text-center shadow-sm">
        <p className="text-sm text-warm-500">Loading hospital...</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Hospital info */}
      <div className="rounded-xl border border-warm-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-warm-900">
          {hospital.hospitalName}
        </h2>
        <p className="mt-1 text-sm text-warm-600">{hospital.address}</p>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          {hospital.licenseState && (
            <span className="rounded-md bg-warm-100 px-2 py-0.5 text-xs font-medium text-warm-700">
              {hospital.licenseState}
            </span>
          )}
          {hospital.lastUpdatedOn && (
            <span className="text-xs text-warm-500">
              MRF updated {hospital.lastUpdatedOn}
            </span>
          )}
        </div>
      </div>

      {/* Search form */}
      <form
        onSubmit={handleSubmit}
        className="flex flex-wrap items-end gap-3 rounded-xl border border-warm-200 bg-white p-4 shadow-sm"
      >
        <div className="flex flex-col gap-1">
          <label htmlFor="codeType" className="text-xs font-medium text-warm-600">
            Code Type
          </label>
          <select
            id="codeType"
            value={codeType}
            onChange={(e) => setCodeType(e.target.value)}
            className="h-10 rounded-lg border border-warm-300 bg-warm-50 px-3 pr-8 text-sm text-warm-900 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            {CODE_TYPES.map((ct) => (
              <option key={ct} value={ct}>
                {ct}
              </option>
            ))}
          </select>
        </div>

        <div className="flex flex-col gap-1">
          <label htmlFor="codeValue" className="text-xs font-medium text-warm-600">
            Code Value
          </label>
          <input
            id="codeValue"
            type="text"
            placeholder="99213"
            value={codeValue}
            onChange={(e) => setCodeValue(e.target.value)}
            className="h-10 w-32 rounded-lg border border-warm-300 bg-warm-50 px-3 text-sm text-warm-900 placeholder:text-warm-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            required
          />
        </div>

        <button
          type="submit"
          disabled={loading}
          className="h-10 rounded-lg bg-blue-600 px-5 text-sm font-medium text-white transition-colors hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? "Searching..." : "Search"}
        </button>
      </form>

      {error && (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Progress bar */}
      {loading && (
        <div className="rounded-xl border border-warm-200 bg-white px-6 py-8 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <span className="text-sm font-medium text-warm-700">
              Querying hospital prices...
            </span>
            <span className="text-xs tabular-nums text-warm-400">
              {Math.round(progress)}%
            </span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-warm-100">
            <div
              className="h-full rounded-full bg-blue-600 transition-all duration-200 ease-out"
              style={{ width: `${progress}%` }}
            />
          </div>
        </div>
      )}

      {/* Results */}
      {charges && !loading && (
        <div className="rounded-xl border border-warm-200 bg-white p-4 shadow-sm">
          <div className="mb-4 rounded-lg bg-green-50 px-4 py-2 text-sm text-green-700">
            {charges.length} charge{charges.length !== 1 ? "s" : ""} found ({(durationMs / 1000).toFixed(1)}s)
          </div>

          {charges.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-warm-200 text-warm-500">
                    <th className="pb-2 pr-4 font-medium">Description</th>
                    <th className="pb-2 pr-4 font-medium">Setting</th>
                    <th className="pb-2 pr-4 text-right font-medium">Gross</th>
                    <th className="pb-2 text-right font-medium">Cash</th>
                  </tr>
                </thead>
                <tbody className="text-warm-700">
                  {charges.map((c, i) => (
                    <tr
                      key={i}
                      className="border-b border-warm-50 last:border-0"
                    >
                      <td className="py-2 pr-4" title={c.description}>
                        {c.description}
                      </td>
                      <td className="whitespace-nowrap py-2 pr-4">
                        {c.setting}
                      </td>
                      <td className="whitespace-nowrap py-2 pr-4 text-right">
                        {formatCurrency(c.grossCharge)}
                      </td>
                      <td className="whitespace-nowrap py-2 text-right">
                        {formatCurrency(c.discountedCash)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-sm text-warm-500">
              No charges found for {codeType} {codeValue} at this hospital.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
