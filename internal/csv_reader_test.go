package internal

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// writeTallCSV creates a Tall-format CSV test file.
// Includes both payer_name/plan_name AND standard_charge|negotiated_dollar
// columns — the combination that previously triggered a non-deterministic
// Wide detection bug (fixed in detectFormat).
func writeTallCSV(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tall.csv")

	content := `hospital_name,last_updated_on,version,hospital_location,hospital_address
Test General Hospital,2024-01-15,2.0.0,"New York, NY","123 Main St, New York, NY 10001"
description,setting,code|1,code|1|type,code|2,code|2|type,standard_charge|gross,standard_charge|discounted_cash,standard_charge|min,standard_charge|max,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|methodology,drug_unit_of_measurement,drug_type_of_measurement,additional_generic_notes,modifiers
ECHOCARDIOGRAM COMPLETE,outpatient,93306,CPT,G0389,HCPCS,1500.00,750.00,500.00,2000.00,Aetna,Aetna PPO,900.00,fee_schedule,,,,
ECHOCARDIOGRAM COMPLETE,outpatient,93306,CPT,G0389,HCPCS,1500.00,750.00,500.00,2000.00,UnitedHealthcare,UHC Choice Plus,1100.00,case_rate,,,,
ACETAMINOPHEN 500MG TABLET,inpatient,00456-0422-01,NDC,,,15.50,8.25,5.00,20.00,Cigna,Cigna Open Access,10.00,fee_schedule,500.0,ME,Oral tablet only,
HEART TRANSPLANT WITH MCC,inpatient,001,MS-DRG,,,500000.00,250000.00,200000.00,750000.00,,,,,,,,26 59
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write tall CSV: %v", err)
	}
	return path
}

// writeWideCSV creates a Wide-format CSV test file.
func writeWideCSV(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "wide.csv")

	content := `hospital_name,last_updated_on,version,hospital_location,hospital_address
Wide Test Hospital,2024-06-01,2.0.0,Brooklyn NY,456 Oak Ave Brooklyn NY 11201
description,setting,code|1,code|1|type,standard_charge|gross,standard_charge|discounted_cash,standard_charge|min,standard_charge|max,standard_charge|Aetna|PPO|negotiated_dollar,standard_charge|Aetna|PPO|methodology,standard_charge|UHC|Choice_Plus|negotiated_dollar,standard_charge|UHC|Choice_Plus|methodology
X-RAY CHEST,outpatient,71046,CPT,250.00,125.00,80.00,300.00,150.00,fee_schedule,175.00,case_rate
MRI BRAIN,inpatient,70553,CPT,3500.00,1750.00,1200.00,4000.00,2200.00,per_diem,,
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write wide CSV: %v", err)
	}
	return path
}

// csvToParquet reads a CSV file via CSVReader, writes all rows to a parquet file
// via ChargeWriter, and returns the parquet path and collected rows.
func csvToParquet(t *testing.T, csvPath string) (string, []HospitalChargeRow) {
	t.Helper()

	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader(%s): %v", csvPath, err)
	}
	defer reader.Close()

	var allRows []HospitalChargeRow
	for {
		rows, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("CSVReader.Next: %v", err)
		}
		allRows = append(allRows, rows...)
	}

	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "output.parquet")
	w, err := NewChargeWriter(parquetPath)
	if err != nil {
		t.Fatalf("NewChargeWriter: %v", err)
	}
	if _, err := w.Write(allRows); err != nil {
		t.Fatalf("ChargeWriter.Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ChargeWriter.Close: %v", err)
	}

	return parquetPath, allRows
}

// readParquet reads all HospitalChargeRow records from a parquet file.
func readParquet(t *testing.T, path string) []HospitalChargeRow {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[HospitalChargeRow](f)
	defer reader.Close()

	rows := make([]HospitalChargeRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read parquet: %v", err)
	}
	return rows[:n]
}

func strPtr(s string) *string   { return &s }
func f64Ptr(f float64) *float64 { return &f }

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.01
}

func assertStrPtrEq(t *testing.T, label string, got, want *string) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("%s = %q, want nil", label, *got)
		}
		return
	}
	if got == nil {
		t.Errorf("%s = nil, want %q", label, *want)
		return
	}
	if *got != *want {
		t.Errorf("%s = %q, want %q", label, *got, *want)
	}
}

func assertF64PtrEq(t *testing.T, label string, got, want *float64) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("%s = %f, want nil", label, *got)
		}
		return
	}
	if got == nil {
		t.Errorf("%s = nil, want %f", label, *want)
		return
	}
	if !approxEqual(*got, *want) {
		t.Errorf("%s = %f, want %f", label, *got, *want)
	}
}

func TestCSVReaderTallToParquet(t *testing.T) {
	csvPath := writeTallCSV(t)
	parquetPath, csvRows := csvToParquet(t, csvPath)
	pqRows := readParquet(t, parquetPath)

	// Tall CSV has 4 data rows → 4 HospitalChargeRows (1 per CSV row)
	if len(csvRows) != 4 {
		t.Fatalf("CSV produced %d rows, want 4", len(csvRows))
	}
	if len(pqRows) != 4 {
		t.Fatalf("parquet has %d rows, want 4", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "tall" {
		t.Errorf("format = %q, want %q", reader.Format(), "tall")
	}

	// ── Verify hospital metadata propagated to all rows ──────────────
	for i, row := range pqRows {
		if row.HospitalName != "Test General Hospital" {
			t.Errorf("row[%d].HospitalName = %q, want %q", i, row.HospitalName, "Test General Hospital")
		}
		if row.LastUpdatedOn != "2024-01-15" {
			t.Errorf("row[%d].LastUpdatedOn = %q, want %q", i, row.LastUpdatedOn, "2024-01-15")
		}
		if row.Version != "2.0.0" {
			t.Errorf("row[%d].Version = %q, want %q", i, row.Version, "2.0.0")
		}
		if row.HospitalAddress != "123 Main St, New York, NY 10001" {
			t.Errorf("row[%d].HospitalAddress = %q", i, row.HospitalAddress)
		}
	}

	// ── Row 0: ECHOCARDIOGRAM / Aetna ────────────────────────────────
	r := pqRows[0]
	if r.Description != "ECHOCARDIOGRAM COMPLETE" {
		t.Errorf("row[0].Description = %q", r.Description)
	}
	if r.Setting != "outpatient" {
		t.Errorf("row[0].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[0].CPTCode", r.CPTCode, strPtr("93306"))
	assertStrPtrEq(t, "row[0].HCPCSCode", r.HCPCSCode, strPtr("G0389"))
	assertF64PtrEq(t, "row[0].GrossCharge", r.GrossCharge, f64Ptr(1500.00))
	assertF64PtrEq(t, "row[0].DiscountedCash", r.DiscountedCash, f64Ptr(750.00))
	assertF64PtrEq(t, "row[0].MinCharge", r.MinCharge, f64Ptr(500.00))
	assertF64PtrEq(t, "row[0].MaxCharge", r.MaxCharge, f64Ptr(2000.00))
	assertStrPtrEq(t, "row[0].PayerName", r.PayerName, strPtr("Aetna"))
	assertStrPtrEq(t, "row[0].PlanName", r.PlanName, strPtr("Aetna PPO"))
	assertF64PtrEq(t, "row[0].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(900.00))
	assertStrPtrEq(t, "row[0].Methodology", r.Methodology, strPtr("fee_schedule"))

	// ── Row 1: ECHOCARDIOGRAM / UHC ──────────────────────────────────
	r = pqRows[1]
	assertStrPtrEq(t, "row[1].PayerName", r.PayerName, strPtr("UnitedHealthcare"))
	assertStrPtrEq(t, "row[1].PlanName", r.PlanName, strPtr("UHC Choice Plus"))
	assertF64PtrEq(t, "row[1].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(1100.00))
	assertStrPtrEq(t, "row[1].Methodology", r.Methodology, strPtr("case_rate"))
	// Same codes as row 0
	assertStrPtrEq(t, "row[1].CPTCode", r.CPTCode, strPtr("93306"))
	assertStrPtrEq(t, "row[1].HCPCSCode", r.HCPCSCode, strPtr("G0389"))

	// ── Row 2: ACETAMINOPHEN / drug info ─────────────────────────────
	r = pqRows[2]
	if r.Description != "ACETAMINOPHEN 500MG TABLET" {
		t.Errorf("row[2].Description = %q", r.Description)
	}
	if r.Setting != "inpatient" {
		t.Errorf("row[2].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[2].NDCCode", r.NDCCode, strPtr("00456-0422-01"))
	assertStrPtrEq(t, "row[2].CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "row[2].GrossCharge", r.GrossCharge, f64Ptr(15.50))
	assertF64PtrEq(t, "row[2].DiscountedCash", r.DiscountedCash, f64Ptr(8.25))
	assertF64PtrEq(t, "row[2].DrugUnitOfMeasurement", r.DrugUnitOfMeasurement, f64Ptr(500.0))
	assertStrPtrEq(t, "row[2].DrugTypeOfMeasurement", r.DrugTypeOfMeasurement, strPtr("ME"))
	assertStrPtrEq(t, "row[2].AdditionalGenericNotes", r.AdditionalGenericNotes, strPtr("Oral tablet only"))
	assertStrPtrEq(t, "row[2].PayerName", r.PayerName, strPtr("Cigna"))
	assertStrPtrEq(t, "row[2].PlanName", r.PlanName, strPtr("Cigna Open Access"))
	assertF64PtrEq(t, "row[2].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(10.00))
	assertStrPtrEq(t, "row[2].Methodology", r.Methodology, strPtr("fee_schedule"))

	// ── Row 3: HEART TRANSPLANT / no payer, with modifiers ───────────
	r = pqRows[3]
	if r.Description != "HEART TRANSPLANT WITH MCC" {
		t.Errorf("row[3].Description = %q", r.Description)
	}
	assertStrPtrEq(t, "row[3].MSDRGCode", r.MSDRGCode, strPtr("001"))
	assertStrPtrEq(t, "row[3].PayerName", r.PayerName, nil)
	assertStrPtrEq(t, "row[3].PlanName", r.PlanName, nil)
	assertF64PtrEq(t, "row[3].GrossCharge", r.GrossCharge, f64Ptr(500000.00))
	assertF64PtrEq(t, "row[3].DiscountedCash", r.DiscountedCash, f64Ptr(250000.00))
	assertStrPtrEq(t, "row[3].Modifiers", r.Modifiers, strPtr("26 59"))

	// ── Round-trip: CSV rows match parquet rows field by field ────────
	for i := range csvRows {
		csv := csvRows[i]
		pq := pqRows[i]
		if csv.Description != pq.Description {
			t.Errorf("row[%d] Description mismatch: csv=%q pq=%q", i, csv.Description, pq.Description)
		}
		if csv.Setting != pq.Setting {
			t.Errorf("row[%d] Setting mismatch: csv=%q pq=%q", i, csv.Setting, pq.Setting)
		}
		assertStrPtrEq(t, "roundtrip CPTCode", pq.CPTCode, csv.CPTCode)
		assertStrPtrEq(t, "roundtrip HCPCSCode", pq.HCPCSCode, csv.HCPCSCode)
		assertStrPtrEq(t, "roundtrip MSDRGCode", pq.MSDRGCode, csv.MSDRGCode)
		assertStrPtrEq(t, "roundtrip NDCCode", pq.NDCCode, csv.NDCCode)
		assertStrPtrEq(t, "roundtrip PayerName", pq.PayerName, csv.PayerName)
		assertStrPtrEq(t, "roundtrip PlanName", pq.PlanName, csv.PlanName)
		assertF64PtrEq(t, "roundtrip GrossCharge", pq.GrossCharge, csv.GrossCharge)
		assertF64PtrEq(t, "roundtrip DiscountedCash", pq.DiscountedCash, csv.DiscountedCash)
		assertF64PtrEq(t, "roundtrip MinCharge", pq.MinCharge, csv.MinCharge)
		assertF64PtrEq(t, "roundtrip MaxCharge", pq.MaxCharge, csv.MaxCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", pq.NegotiatedDollar, csv.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", pq.Methodology, csv.Methodology)
		assertStrPtrEq(t, "roundtrip Modifiers", pq.Modifiers, csv.Modifiers)
		assertF64PtrEq(t, "roundtrip DrugUnit", pq.DrugUnitOfMeasurement, csv.DrugUnitOfMeasurement)
		assertStrPtrEq(t, "roundtrip DrugType", pq.DrugTypeOfMeasurement, csv.DrugTypeOfMeasurement)
	}
}

func TestCSVReaderWideToParquet(t *testing.T) {
	csvPath := writeWideCSV(t)
	parquetPath, csvRows := csvToParquet(t, csvPath)
	pqRows := readParquet(t, parquetPath)

	// Wide CSV: 2 data rows, each with 2 payer columns.
	// Row 1 (X-RAY): both Aetna and UHC have data → 2 rows
	// Row 2 (MRI): Aetna has data, UHC has no dollar/method → 1 row (Aetna only)
	if len(csvRows) != 3 {
		t.Fatalf("CSV produced %d rows, want 3", len(csvRows))
	}
	if len(pqRows) != 3 {
		t.Fatalf("parquet has %d rows, want 3", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "wide" {
		t.Errorf("format = %q, want %q", reader.Format(), "wide")
	}
	if reader.PayerPlanCount() != 2 {
		t.Errorf("PayerPlanCount = %d, want 2", reader.PayerPlanCount())
	}

	// ── Hospital metadata ────────────────────────────────────────────
	for i, row := range pqRows {
		if row.HospitalName != "Wide Test Hospital" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.Version != "2.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
	}

	// ── Row 0: X-RAY / Aetna ────────────────────────────────────────
	r := pqRows[0]
	if r.Description != "X-RAY CHEST" {
		t.Errorf("row[0].Description = %q", r.Description)
	}
	if r.Setting != "outpatient" {
		t.Errorf("row[0].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[0].CPTCode", r.CPTCode, strPtr("71046"))
	assertF64PtrEq(t, "row[0].GrossCharge", r.GrossCharge, f64Ptr(250.00))
	assertF64PtrEq(t, "row[0].DiscountedCash", r.DiscountedCash, f64Ptr(125.00))
	// Wide format replaces underscores with spaces in payer/plan names
	assertStrPtrEq(t, "row[0].PayerName", r.PayerName, strPtr("Aetna"))
	assertStrPtrEq(t, "row[0].PlanName", r.PlanName, strPtr("PPO"))
	assertF64PtrEq(t, "row[0].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(150.00))
	assertStrPtrEq(t, "row[0].Methodology", r.Methodology, strPtr("fee_schedule"))

	// ── Row 1: X-RAY / UHC ──────────────────────────────────────────
	r = pqRows[1]
	if r.Description != "X-RAY CHEST" {
		t.Errorf("row[1].Description = %q", r.Description)
	}
	assertStrPtrEq(t, "row[1].PayerName", r.PayerName, strPtr("UHC"))
	assertStrPtrEq(t, "row[1].PlanName", r.PlanName, strPtr("Choice Plus"))
	assertF64PtrEq(t, "row[1].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(175.00))
	assertStrPtrEq(t, "row[1].Methodology", r.Methodology, strPtr("case_rate"))
	// Same base charges as row 0
	assertF64PtrEq(t, "row[1].GrossCharge", r.GrossCharge, f64Ptr(250.00))

	// ── Row 2: MRI / Aetna only (UHC has no data) ───────────────────
	r = pqRows[2]
	if r.Description != "MRI BRAIN" {
		t.Errorf("row[2].Description = %q", r.Description)
	}
	if r.Setting != "inpatient" {
		t.Errorf("row[2].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[2].CPTCode", r.CPTCode, strPtr("70553"))
	assertStrPtrEq(t, "row[2].PayerName", r.PayerName, strPtr("Aetna"))
	assertStrPtrEq(t, "row[2].PlanName", r.PlanName, strPtr("PPO"))
	assertF64PtrEq(t, "row[2].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(2200.00))
	assertStrPtrEq(t, "row[2].Methodology", r.Methodology, strPtr("per_diem"))
	assertF64PtrEq(t, "row[2].GrossCharge", r.GrossCharge, f64Ptr(3500.00))

	// ── Round-trip integrity ─────────────────────────────────────────
	for i := range csvRows {
		csv := csvRows[i]
		pq := pqRows[i]
		if csv.Description != pq.Description {
			t.Errorf("row[%d] Description mismatch: csv=%q pq=%q", i, csv.Description, pq.Description)
		}
		assertStrPtrEq(t, "roundtrip PayerName", pq.PayerName, csv.PayerName)
		assertF64PtrEq(t, "roundtrip GrossCharge", pq.GrossCharge, csv.GrossCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", pq.NegotiatedDollar, csv.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", pq.Methodology, csv.Methodology)
	}
}
