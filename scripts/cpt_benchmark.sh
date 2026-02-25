#!/usr/bin/env bash
set -euo pipefail

# cpt_benchmark.sh — Run CPT point queries across all hospital parquet files
# in S3 and report latency + bytes downloaded for each query.
#
# Usage: ./scripts/cpt_benchmark.sh

PREAMBLE=$(cat <<'SQL'
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-east-1';
CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);
SQL
)

FILES=$(cat <<'SQL'
read_parquet([
  's3://hospital-mrf/111631788_Jamaica-Hospital_standardcharges.parquet',
  's3://hospital-mrf/131624096_mount-sinai-hospital_standardcharges__1_.parquet',
  's3://hospital-mrf/131624096_mount-sinai-queens_standardcharges.parquet',
  's3://hospital-mrf/131740114_montefiore-medical-center_standardcharges.parquet',
  's3://hospital-mrf/131974191_bronxcare_standardcharges.parquet',
  's3://hospital-mrf/132655001-1033124961_nyc-health-and-hospitals_standardcharges.parquet',
  's3://hospital-mrf/132655001-1073535027_nyc-health-and-hospitals_bellevue_standardcharges.parquet',
  's3://hospital-mrf/132655001-1073535027_nyc-health-and-hospitals_standardcharges.parquet',
  's3://hospital-mrf/132655001-1801803903_nyc-health-and-hospitals_standardcharges.parquet',
  's3://hospital-mrf/132655001_elmhurst-hospital_standardcharges.parquet',
  's3://hospital-mrf/132655001_kings-county-hospital_standardcharges.parquet',
  's3://hospital-mrf/132997301_mount-sinai-morningside_standardcharges__1_.parquet',
  's3://hospital-mrf/132997301_mount-sinai-morningside_standardcharges__2_.parquet',
  's3://hospital-mrf/133957095_newyork-presbyterian-hospital_standardcharges.parquet',
  's3://hospital-mrf/133971298_nyu-langone-tisch_standardcharges.parquet',
  's3://hospital-mrf/135562304_new-york-eye-and-ear-infirmary-of-mount-sinai_standardcharges.parquet',
  's3://hospital-mrf/135564934_mount-sinai-behavioral-health-center_standardcharges.parquet',
  's3://hospital-mrf/135564934_mount-sinai-brooklyn_standardcharges__1_.parquet',
  's3://hospital-mrf/Lenox_Hill_Hospital_StandardCharges.parquet'
], filename=true, hive_partitioning=false)
SQL
)

HOSPITAL_MACRO="CREATE MACRO hospital(f) AS replace(replace(regexp_extract(f, '[^/]+\$'), '_standardcharges', ''), '.parquet', '');"

# ── Total dataset size ──────────────────────────────────────────────
echo "========================================"
echo "  Dataset: 19 hospital parquet files in s3://hospital-mrf/"
echo "========================================"
duckdb -c "
${PREAMBLE}
SELECT
  ROUND(SUM(total_compressed_size)/1024.0/1024.0, 2) as total_compressed_mb,
  COUNT(DISTINCT row_group_id || '-' || file_name) as total_row_groups,
  SUM(num_values) / COUNT(DISTINCT row_group_id || '-' || file_name) as total_rows_approx
FROM parquet_metadata([
  's3://hospital-mrf/111631788_Jamaica-Hospital_standardcharges.parquet',
  's3://hospital-mrf/131624096_mount-sinai-hospital_standardcharges__1_.parquet',
  's3://hospital-mrf/131624096_mount-sinai-queens_standardcharges.parquet',
  's3://hospital-mrf/131740114_montefiore-medical-center_standardcharges.parquet',
  's3://hospital-mrf/131974191_bronxcare_standardcharges.parquet',
  's3://hospital-mrf/132655001-1033124961_nyc-health-and-hospitals_standardcharges.parquet',
  's3://hospital-mrf/132655001-1073535027_nyc-health-and-hospitals_bellevue_standardcharges.parquet',
  's3://hospital-mrf/132655001-1073535027_nyc-health-and-hospitals_standardcharges.parquet',
  's3://hospital-mrf/132655001-1801803903_nyc-health-and-hospitals_standardcharges.parquet',
  's3://hospital-mrf/132655001_elmhurst-hospital_standardcharges.parquet',
  's3://hospital-mrf/132655001_kings-county-hospital_standardcharges.parquet',
  's3://hospital-mrf/132997301_mount-sinai-morningside_standardcharges__1_.parquet',
  's3://hospital-mrf/132997301_mount-sinai-morningside_standardcharges__2_.parquet',
  's3://hospital-mrf/133957095_newyork-presbyterian-hospital_standardcharges.parquet',
  's3://hospital-mrf/133971298_nyu-langone-tisch_standardcharges.parquet',
  's3://hospital-mrf/135562304_new-york-eye-and-ear-infirmary-of-mount-sinai_standardcharges.parquet',
  's3://hospital-mrf/135564934_mount-sinai-behavioral-health-center_standardcharges.parquet',
  's3://hospital-mrf/135564934_mount-sinai-brooklyn_standardcharges__1_.parquet',
  's3://hospital-mrf/Lenox_Hill_Hospital_StandardCharges.parquet'
]);
" 2>&1 | grep -v Success
echo ""

# ── Query definitions ───────────────────────────────────────────────
declare -a QUERY_NAMES
declare -a QUERY_SQLS

QUERY_NAMES+=("Q1: CPT 99213 — Office visit (level 3)")
QUERY_SQLS+=("SELECT hospital(filename) as hospital, cpt_code, LEFT(description,40) as description, gross_charge, discounted_cash as cash_price, setting FROM ${FILES} WHERE cpt_code = '99213' ORDER BY gross_charge DESC NULLS LAST LIMIT 15")

QUERY_NAMES+=("Q2: CPT 36415 — Venipuncture / blood draw")
QUERY_SQLS+=("SELECT hospital(filename) as hospital, cpt_code, LEFT(description,40) as description, gross_charge, discounted_cash as cash_price FROM ${FILES} WHERE cpt_code = '36415' ORDER BY gross_charge DESC NULLS LAST LIMIT 15")

QUERY_NAMES+=("Q3: CPT 27447 — Total knee replacement")
QUERY_SQLS+=("SELECT hospital(filename) as hospital, cpt_code, LEFT(description,40) as description, gross_charge, discounted_cash as cash_price FROM ${FILES} WHERE cpt_code = '27447' ORDER BY gross_charge DESC NULLS LAST LIMIT 15")

QUERY_NAMES+=("Q4: CPT 99285 — ER visit (highest severity)")
QUERY_SQLS+=("SELECT hospital(filename) as hospital, cpt_code, LEFT(description,40) as description, gross_charge, discounted_cash as cash_price FROM ${FILES} WHERE cpt_code = '99285' ORDER BY gross_charge DESC NULLS LAST LIMIT 15")

QUERY_NAMES+=("Q5: Multi-CPT imaging (71046 chest xray, 74177 CT abdomen, 70553 MRI brain)")
QUERY_SQLS+=("SELECT hospital(filename) as hospital, cpt_code, LEFT(description,40) as description, gross_charge, discounted_cash as cash_price FROM ${FILES} WHERE cpt_code IN ('71046','74177','70553') ORDER BY cpt_code, gross_charge DESC NULLS LAST LIMIT 20")

QUERY_NAMES+=("Q6: Aggregation — avg gross charge per hospital for CPT 99213")
QUERY_SQLS+=("SELECT hospital(filename) as hospital, COUNT(*) as rows, ROUND(AVG(gross_charge),2) as avg_gross, ROUND(MIN(gross_charge),2) as min_gross, ROUND(MAX(gross_charge),2) as max_gross FROM ${FILES} WHERE cpt_code = '99213' GROUP BY hospital ORDER BY avg_gross DESC")

QUERY_NAMES+=("Q7: CPT 43239 — Upper GI endoscopy w/ biopsy (with payer)")
QUERY_SQLS+=("SELECT hospital(filename) as hospital, cpt_code, LEFT(description,35) as description, gross_charge, LEFT(payer_name,25) as payer, LEFT(plan_name,25) as plan FROM ${FILES} WHERE cpt_code = '43239' ORDER BY gross_charge DESC NULLS LAST LIMIT 15")

# ── Summary table arrays ────────────────────────────────────────────
declare -a SUMMARY_NAMES
declare -a SUMMARY_DOWNLOADED
declare -a SUMMARY_REQUESTS
declare -a SUMMARY_LATENCY

# ── Run each query in its own DuckDB process (clean HTTP state) ─────
for i in "${!QUERY_NAMES[@]}"; do
    name="${QUERY_NAMES[$i]}"
    sql="${QUERY_SQLS[$i]}"

    echo "========================================"
    echo "  ${name}"
    echo "========================================"

    # Run EXPLAIN ANALYZE in a fresh process to get HTTP stats + timing
    ea_output=$(duckdb -c "
${PREAMBLE}
${HOSPITAL_MACRO}
EXPLAIN ANALYZE ${sql};
" 2>&1)

    # Parse HTTP stats from EXPLAIN ANALYZE output
    downloaded=$(echo "$ea_output" | grep -o 'in: [0-9.]*[[:space:]]*[A-Za-z]*' | head -1 | sed 's/in: //')
    head_reqs=$(echo "$ea_output" | grep '#HEAD' | grep -o '[0-9]*' | head -1)
    get_reqs=$(echo "$ea_output" | grep '#GET' | grep -o '[0-9]*' | head -1)
    total_time=$(echo "$ea_output" | grep 'Total Time' | grep -o '[0-9.]*s' | head -1)
    files_read=$(echo "$ea_output" | grep 'Total Files Read' | grep -o '[0-9]*' | head -1)

    echo "  Downloaded:  ${downloaded:-N/A}"
    echo "  HTTP reqs:   ${head_reqs:-0} HEAD + ${get_reqs:-0} GET"
    echo "  Files read:  ${files_read:-N/A} / 19"
    echo "  Latency:     ${total_time:-N/A}"
    echo ""

    # Run the actual query for results display
    duckdb -c "
${PREAMBLE}
${HOSPITAL_MACRO}
${sql};
" 2>&1 | grep -v Success
    echo ""

    # Collect for summary
    SUMMARY_NAMES+=("$name")
    SUMMARY_DOWNLOADED+=("${downloaded:-N/A}")
    SUMMARY_REQUESTS+=("${head_reqs:-0}H+${get_reqs:-0}G")
    SUMMARY_LATENCY+=("${total_time:-N/A}")
done

# ── Summary table ───────────────────────────────────────────────────
echo "========================================"
echo "  SUMMARY"
echo "========================================"
printf "%-62s  %14s  %8s  %8s\n" "Query" "Downloaded" "Reqs" "Latency"
printf "%-62s  %14s  %8s  %8s\n" "-----" "----------" "----" "-------"
for i in "${!SUMMARY_NAMES[@]}"; do
    printf "%-62s  %14s  %8s  %8s\n" \
        "${SUMMARY_NAMES[$i]}" \
        "${SUMMARY_DOWNLOADED[$i]}" \
        "${SUMMARY_REQUESTS[$i]}" \
        "${SUMMARY_LATENCY[$i]}"
done
