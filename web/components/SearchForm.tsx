"use client";

import { useState, FormEvent } from "react";
import { SearchResponse } from "@/lib/types";
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

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setData(null);

    try {
      const res = await fetch("/api/search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ zipCode, codeType, codeValue }),
      });
      const json = await res.json();
      if (!res.ok) {
        setError(json.error || "Search failed");
        return;
      }
      setData(json);
    } catch {
      setError("Network error — please try again");
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
          disabled={loading || zipCode.length !== 5}
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

      {data && (
        <div className="grid gap-6 lg:grid-cols-2">
          <ResultsMap
            results={data.results}
            center={data.center}
            hoveredHospital={hoveredHospital}
            onHoverHospital={setHoveredHospital}
          />
          <ResultsList
            results={data.results}
            codeType={codeType}
            codeValue={codeValue}
            hoveredHospital={hoveredHospital}
            onHoverHospital={setHoveredHospital}
          />
        </div>
      )}
    </div>
  );
}
