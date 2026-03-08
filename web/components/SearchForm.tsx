"use client";

import { useState, useEffect, useRef, FormEvent } from "react";
import { SearchResponse } from "@/lib/types";
import { clientSearch } from "@/lib/search-client";
import { PriceQueryResult, queryPrices } from "@/lib/duckdb";
import ResultsMap from "./ResultsMap";
import ResultsList from "./ResultsList";

const CODE_TYPES = [
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
];

export default function SearchForm() {
  const [zipCode, setZipCode] = useState("");
  const [codeType, setCodeType] = useState("CPT");
  const [codeValue, setCodeValue] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<SearchResponse | null>(null);
  const [hoveredHospital, setHoveredHospital] = useState<string | null>(null);

  const [priceData, setPriceData] = useState<PriceQueryResult | null>(null);
  const [priceLoading, setPriceLoading] = useState(false);
  const [priceError, setPriceError] = useState<string | null>(null);
  const [progress, setProgress] = useState(0);
  const progressInterval = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    fetch("https://ipapi.co/json/")
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (
          data?.country_code === "US" &&
          data?.postal &&
          /^\d{5}$/.test(data.postal)
        ) {
          setZipCode((prev) => (prev === "" ? data.postal : prev));
        }
      })
      .catch(() => {});
  }, []);

  function startProgress() {
    setProgress(0);
    if (progressInterval.current) clearInterval(progressInterval.current);
    const start = Date.now();
    progressInterval.current = setInterval(() => {
      const elapsed = (Date.now() - start) / 1000;
      // Fast to ~40% in 2s, then slow approach toward 90% over ~14s
      const fast = 40 * (1 - Math.exp(-elapsed / 1.2));
      const slow = 50 * (1 - Math.exp(-elapsed / 8));
      setProgress(Math.min(fast + slow, 92));
    }, 100);
  }

  function stopProgress() {
    if (progressInterval.current) {
      clearInterval(progressInterval.current);
      progressInterval.current = null;
    }
    setProgress(100);
    // Reset after the bar fills
    setTimeout(() => setProgress(0), 400);
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setData(null);
    setPriceData(null);
    setPriceError(null);
    startProgress();

    try {
      const json = await clientSearch(zipCode, codeType, codeValue);
      setData(json);

      // Run DuckDB price query on the returned hospital parquet files
      if (json.results.length > 0) {
        setPriceLoading(true);
        try {
          const result = await queryPrices(json.results, codeType, codeValue);
          setPriceData(result);
        } catch (err) {
          setPriceError(
            err instanceof Error ? err.message : "Price query failed"
          );
        } finally {
          setPriceLoading(false);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setLoading(false);
      stopProgress();
    }
  }

  return (
    <div className="space-y-6">
      <form
        onSubmit={handleSubmit}
        className="flex flex-wrap items-end gap-3 rounded-xl border border-warm-200 bg-white p-4 shadow-sm"
      >
        <div className="flex flex-col gap-1">
          <label
            htmlFor="zipCode"
            className="text-xs font-medium text-warm-600"
          >
            Zip Code
          </label>
          <input
            id="zipCode"
            type="text"
            inputMode="numeric"
            maxLength={5}
            placeholder="10001"
            value={zipCode}
            onChange={(e) =>
              setZipCode(e.target.value.replace(/\D/g, "").slice(0, 5))
            }
            className="h-10 w-28 rounded-lg border border-warm-300 bg-warm-50 px-3 text-sm text-warm-900 placeholder:text-warm-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            required
          />
        </div>

        <div className="flex flex-col gap-1">
          <label
            htmlFor="codeType"
            className="text-xs font-medium text-warm-600"
          >
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
          <label
            htmlFor="codeValue"
            className="text-xs font-medium text-warm-600"
          >
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
          disabled={loading || priceLoading || zipCode.length !== 5}
          className="h-10 rounded-lg bg-blue-600 px-5 text-sm font-medium text-white transition-colors hover:bg-blue-700 disabled:opacity-50"
        >
          {loading || priceLoading ? "Searching..." : "Search"}
        </button>
      </form>

      {error && (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Loading progress bar */}
      {(loading || priceLoading) && (
        <div className="rounded-xl border border-warm-200 bg-white px-6 py-8 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <span className="text-sm font-medium text-warm-700">
              {loading
                ? "Finding nearby hospitals..."
                : `Querying prices across ${data?.results.length ?? 0} hospital files...`}
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

      {/* Only show results after prices have loaded; filter to hospitals with charges */}
      {data && priceData && !priceLoading && (() => {
        const filteredResults = data.results.filter(
          (h) => (priceData.chargesByHospital[h.hospitalName]?.length ?? 0) > 0
        );
        return filteredResults.length > 0 ? (
          <div className="grid gap-6 lg:grid-cols-2">
            <ResultsMap
              results={filteredResults}
              center={data.center}
              hoveredHospital={hoveredHospital}
              onHoverHospital={setHoveredHospital}
            />
            <ResultsList
              results={filteredResults}
              codeType={codeType}
              codeValue={codeValue}
              hoveredHospital={hoveredHospital}
              onHoverHospital={setHoveredHospital}
              priceData={priceData}
              priceLoading={false}
              priceError={priceError}
            />
          </div>
        ) : (
          <div className="rounded-xl border border-warm-200 bg-white py-12 text-center shadow-sm">
            <p className="text-sm text-warm-500">
              No hospitals found with pricing for {codeType} {codeValue} in this area.
            </p>
          </div>
        );
      })()}

      {data && priceError && !priceLoading && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700">
          Price query failed: {priceError}
        </div>
      )}
    </div>
  );
}
