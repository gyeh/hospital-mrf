-- cpt_benchmark.sql
-- Point queries for CPT codes across all hospital parquet files in S3.
-- Run: duckdb < scripts/cpt_benchmark.sql

INSTALL httpfs;
LOAD httpfs;
SET s3_region = 'us-east-1';
CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

CREATE VIEW all_charges AS
SELECT * FROM read_parquet([
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
], filename=true, hive_partitioning=false);

-- Helper: short hospital name from filename
CREATE MACRO hospital(f) AS
  replace(replace(regexp_extract(f, '[^/]+$'), '_standardcharges', ''), '.parquet', '');

.timer on

-- ============================================================
.print '=== Q1: CPT 99213 — Office visit (level 3) ==='
-- ============================================================
SELECT
    hospital(filename) as hospital,
    cpt_code,
    LEFT(description, 40) as description,
    gross_charge,
    discounted_cash as cash_price,
    setting
FROM all_charges
WHERE cpt_code = '99213'
ORDER BY gross_charge DESC NULLS LAST
LIMIT 15;

-- ============================================================
.print '=== Q2: CPT 36415 — Venipuncture / blood draw ==='
-- ============================================================
SELECT
    hospital(filename) as hospital,
    cpt_code,
    LEFT(description, 40) as description,
    gross_charge,
    discounted_cash as cash_price
FROM all_charges
WHERE cpt_code = '36415'
ORDER BY gross_charge DESC NULLS LAST
LIMIT 15;

-- ============================================================
.print '=== Q3: CPT 27447 — Total knee replacement ==='
-- ============================================================
SELECT
    hospital(filename) as hospital,
    cpt_code,
    LEFT(description, 40) as description,
    gross_charge,
    discounted_cash as cash_price
FROM all_charges
WHERE cpt_code = '27447'
ORDER BY gross_charge DESC NULLS LAST
LIMIT 15;

-- ============================================================
.print '=== Q4: CPT 99285 — ER visit (highest severity) ==='
-- ============================================================
SELECT
    hospital(filename) as hospital,
    cpt_code,
    LEFT(description, 40) as description,
    gross_charge,
    discounted_cash as cash_price
FROM all_charges
WHERE cpt_code = '99285'
ORDER BY gross_charge DESC NULLS LAST
LIMIT 15;

-- ============================================================
.print '=== Q5: Multi-CPT imaging (71046 chest xray, 74177 CT abdomen, 70553 MRI brain) ==='
-- ============================================================
SELECT
    hospital(filename) as hospital,
    cpt_code,
    LEFT(description, 40) as description,
    gross_charge,
    discounted_cash as cash_price
FROM all_charges
WHERE cpt_code IN ('71046', '74177', '70553')
ORDER BY cpt_code, gross_charge DESC NULLS LAST
LIMIT 20;

-- ============================================================
.print '=== Q6: Aggregation — avg gross charge per hospital for CPT 99213 ==='
-- ============================================================
SELECT
    hospital(filename) as hospital,
    COUNT(*) as rows,
    ROUND(AVG(gross_charge), 2) as avg_gross,
    ROUND(MIN(gross_charge), 2) as min_gross,
    ROUND(MAX(gross_charge), 2) as max_gross
FROM all_charges
WHERE cpt_code = '99213'
GROUP BY hospital
ORDER BY avg_gross DESC;

-- ============================================================
.print '=== Q7: CPT 43239 — Upper GI endoscopy w/ biopsy (with payer) ==='
-- ============================================================
SELECT
    hospital(filename) as hospital,
    cpt_code,
    LEFT(description, 35) as description,
    gross_charge,
    LEFT(payer_name, 25) as payer,
    LEFT(plan_name, 25) as plan
FROM all_charges
WHERE cpt_code = '43239'
ORDER BY gross_charge DESC NULLS LAST
LIMIT 15;
