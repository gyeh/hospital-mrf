/**
 * Client-side DuckDB-WASM utility for querying hospital price transparency
 * parquet files directly from S3 via HTTP range requests.
 *
 * Requires the S3 bucket to have CORS configured for browser access:
 *   AllowedMethods: GET, HEAD
 *   AllowedOrigins: * (or your domain)
 *   ExposeHeaders: Content-Length, Content-Range, Accept-Ranges
 */

import * as duckdb from "@duckdb/duckdb-wasm";

const CODE_TYPE_COLUMNS: Record<string, string> = {
  CPT: "cpt_code",
  HCPCS: "hcpcs_code",
  "MS-DRG": "ms_drg_code",
  NDC: "ndc_code",
  RC: "rc_code",
  ICD: "icd_code",
  DRG: "drg_code",
  CDM: "cdm_code",
  LOCAL: "local_code",
  APC: "apc_code",
  EAPG: "eapg_code",
  HIPPS: "hipps_code",
  CDT: "cdt_code",
  "R-DRG": "r_drg_code",
  "S-DRG": "s_drg_code",
  "APS-DRG": "aps_drg_code",
  "AP-DRG": "ap_drg_code",
  "APR-DRG": "apr_drg_code",
  "TRIS-DRG": "tris_drg_code",
};

function s3ToHttps(s3Path: string): string {
  const match = s3Path.match(/^s3:\/\/([^/]+)\/(.+)$/);
  if (!match) throw new Error(`Invalid S3 path: ${s3Path}`);
  return `https://${match[1]}.s3.us-east-1.amazonaws.com/${match[2]}`;
}

export interface ChargeRow {
  description: string;
  setting: string;
  payerName: string;
  planName: string;
  grossCharge: number | null;
  discountedCash: number | null;
  negotiatedDollar: number | null;
  negotiatedPercentage: number | null;
  minCharge: number | null;
  maxCharge: number | null;
  methodology: string;
}

export interface PriceQueryResult {
  chargesByHospital: Record<string, ChargeRow[]>;
  durationMs: number;
  totalRows: number;
}

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;

async function getDB(): Promise<duckdb.AsyncDuckDB> {
  if (dbPromise) return dbPromise;

  dbPromise = (async () => {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], {
        type: "text/javascript",
      })
    );

    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);
    return db;
  })();

  return dbPromise;
}

export async function queryPrices(
  hospitals: { hospitalName: string; outputFile: string }[],
  codeType: string,
  codeValue: string
): Promise<PriceQueryResult> {
  const column = CODE_TYPE_COLUMNS[codeType];
  if (!column) throw new Error(`Unknown code type: ${codeType}`);

  // Map HTTPS URL -> hospital names (dedup files shared across locations)
  const urlToHospitalNames = new Map<string, string[]>();
  for (const h of hospitals) {
    if (!h.outputFile) continue;
    const url = s3ToHttps(h.outputFile);
    const names = urlToHospitalNames.get(url) ?? [];
    names.push(h.hospitalName);
    urlToHospitalNames.set(url, names);
  }

  const uniqueUrls = [...urlToHospitalNames.keys()];
  if (uniqueUrls.length === 0) {
    return { chargesByHospital: {}, durationMs: 0, totalRows: 0 };
  }

  const db = await getDB();
  const conn = await db.connect();

  try {
    const urlList = uniqueUrls.map((u) => `'${u}'`).join(", ");
    const escapedValue = codeValue.replace(/'/g, "''");

    const sql = `
      SELECT
        filename,
        description,
        setting,
        payer_name,
        plan_name,
        gross_charge,
        discounted_cash,
        negotiated_dollar,
        negotiated_percentage,
        min_charge,
        max_charge,
        methodology
      FROM read_parquet([${urlList}], filename=true)
      WHERE ${column} = '${escapedValue}'
      ORDER BY filename, gross_charge DESC NULLS LAST
    `;

    const t0 = performance.now();
    const table = await conn.query(sql);
    const durationMs = Math.round(performance.now() - t0);

    const chargesByHospital: Record<string, ChargeRow[]> = {};
    const numRows = table.numRows;

    for (let i = 0; i < numRows; i++) {
      const row = table.get(i);
      if (!row) continue;

      const filename = String(row["filename"] ?? "");
      const charge: ChargeRow = {
        description: String(row["description"] ?? ""),
        setting: String(row["setting"] ?? ""),
        payerName: String(row["payer_name"] ?? ""),
        planName: String(row["plan_name"] ?? ""),
        grossCharge: row["gross_charge"] ?? null,
        discountedCash: row["discounted_cash"] ?? null,
        negotiatedDollar: row["negotiated_dollar"] ?? null,
        negotiatedPercentage: row["negotiated_percentage"] ?? null,
        minCharge: row["min_charge"] ?? null,
        maxCharge: row["max_charge"] ?? null,
        methodology: String(row["methodology"] ?? ""),
      };

      const hospitalNames = urlToHospitalNames.get(filename) ?? [];
      for (const name of hospitalNames) {
        if (!chargesByHospital[name]) chargesByHospital[name] = [];
        chargesByHospital[name].push(charge);
      }
    }

    return { chargesByHospital, durationMs, totalRows: numRows };
  } finally {
    await conn.close();
  }
}
