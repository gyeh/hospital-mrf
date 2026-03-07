"use client";

import { useState, useEffect, FormEvent } from "react";
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

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setData(null);
    setPriceData(null);
    setPriceError(null);

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

      {/* Loading indicator while prices are being queried */}
      {(loading || priceLoading) && (
        <div className="flex items-center justify-center gap-3 rounded-xl border border-warm-200 bg-white py-12 shadow-sm">
          <svg
            className="h-5 w-5 animate-spin text-blue-600"
            viewBox="0 0 24 24"
            fill="none"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
          <span className="text-sm text-warm-600">
            {loading
              ? "Finding nearby hospitals..."
              : `Querying prices across ${data?.results.length ?? 0} hospital files...`}
          </span>
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
