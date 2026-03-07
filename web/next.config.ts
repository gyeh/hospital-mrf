import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  webpack: (config, { isServer }) => {
    // DuckDB-WASM requires async WASM support
    config.experiments = {
      ...config.experiments,
      asyncWebAssembly: true,
    };

    // Prevent webpack from bundling duckdb-wasm on the server
    if (isServer) {
      config.externals = config.externals || [];
      (config.externals as string[]).push("@duckdb/duckdb-wasm");
    }

    return config;
  },
};

export default nextConfig;
