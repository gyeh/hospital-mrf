"use client";

import { useMemo, useCallback, useState } from "react";
import Map, { Marker, Popup, NavigationControl } from "react-map-gl/mapbox";
import "mapbox-gl/dist/mapbox-gl.css";
import { SearchResult } from "@/lib/types";

const MAPBOX_TOKEN = process.env.NEXT_PUBLIC_MAPBOX_TOKEN ?? "";

interface Props {
  results: SearchResult[];
  center: { lat: number; lon: number };
  hoveredHospital: string | null;
  onHoverHospital: (name: string | null) => void;
}

export default function ResultsMap({
  results,
  center,
  hoveredHospital,
  onHoverHospital,
}: Props) {
  const [selectedHospital, setSelectedHospital] =
    useState<SearchResult | null>(null);

  const bounds = useMemo(() => {
    if (results.length === 0) return undefined;
    const lats = [center.lat, ...results.map((r) => r.lat)];
    const lons = [center.lon, ...results.map((r) => r.lon)];
    return [
      [Math.min(...lons), Math.min(...lats)],
      [Math.max(...lons), Math.max(...lats)],
    ] as [[number, number], [number, number]];
  }, [results, center]);

  const handleMarkerClick = useCallback(
    (hospital: SearchResult) => {
      setSelectedHospital(
        selectedHospital?.hospitalName === hospital.hospitalName
          ? null
          : hospital
      );
    },
    [selectedHospital]
  );

  return (
    <div className="overflow-hidden rounded-xl border border-warm-200 shadow-sm">
      <Map
        mapboxAccessToken={MAPBOX_TOKEN}
        initialViewState={{
          longitude: center.lon,
          latitude: center.lat,
          zoom: 10,
          bounds,
          fitBoundsOptions: { padding: 80 },
        }}
        style={{ width: "100%", height: 500 }}
        mapStyle="mapbox://styles/mapbox/streets-v12"
        attributionControl={false}
      >
        <NavigationControl position="top-right" />

        {/* Zip code center marker */}
        <Marker longitude={center.lon} latitude={center.lat} anchor="center">
          <div className="h-3.5 w-3.5 rounded-full border-2 border-white bg-blue-600 shadow-md" />
        </Marker>

        {/* Hospital markers */}
        {results.map((hospital) => {
          const isHovered = hoveredHospital === hospital.hospitalName;
          return (
            <Marker
              key={hospital.hospitalName + hospital.address}
              longitude={hospital.lon}
              latitude={hospital.lat}
              anchor="bottom"
              onClick={(e) => {
                e.originalEvent.stopPropagation();
                handleMarkerClick(hospital);
              }}
            >
              <div
                onMouseEnter={() => onHoverHospital(hospital.hospitalName)}
                onMouseLeave={() => onHoverHospital(null)}
                className="cursor-pointer transition-transform"
                style={{ transform: isHovered ? "scale(1.3)" : "scale(1)" }}
              >
                <svg
                  width="28"
                  height="36"
                  viewBox="0 0 28 36"
                  fill="none"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path
                    d="M14 0C6.268 0 0 6.268 0 14c0 10.5 14 22 14 22s14-11.5 14-22C28 6.268 21.732 0 14 0z"
                    fill={isHovered ? "#2563eb" : "#dc2626"}
                  />
                  <rect
                    x="12"
                    y="7"
                    width="4"
                    height="12"
                    rx="1"
                    fill="white"
                  />
                  <rect
                    x="8"
                    y="11"
                    width="12"
                    height="4"
                    rx="1"
                    fill="white"
                  />
                </svg>
              </div>
            </Marker>
          );
        })}

        {/* Popup */}
        {selectedHospital && (
          <Popup
            longitude={selectedHospital.lon}
            latitude={selectedHospital.lat}
            anchor="bottom"
            offset={36}
            closeOnClick={false}
            onClose={() => setSelectedHospital(null)}
          >
            <div className="max-w-[220px] space-y-1 p-1">
              <p className="text-sm font-semibold leading-tight text-warm-900">
                {selectedHospital.hospitalName}
              </p>
              <p className="text-xs text-warm-600">
                {selectedHospital.address}
              </p>
              <p className="text-xs font-medium text-blue-600">
                {selectedHospital.distanceMiles} mi away
              </p>
            </div>
          </Popup>
        )}
      </Map>
    </div>
  );
}
