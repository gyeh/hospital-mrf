package internal

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// jsonToParquet reads a JSON file via JSONReader, writes all rows to a parquet
// file via ChargeWriter, and returns the parquet path and collected rows.
func jsonToParquet(t *testing.T, jsonPath string) (string, []HospitalChargeRow) {
	t.Helper()

	reader, err := NewJSONReader(jsonPath)
	if err != nil {
		t.Fatalf("NewJSONReader(%s): %v", jsonPath, err)
	}
	defer reader.Close()

	var allRows []HospitalChargeRow
	for {
		rows, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("JSONReader.Next: %v", err)
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

// readParquetJSON reads all HospitalChargeRow records from a parquet file.
// Named differently to avoid collision with readParquet in csv_reader_test.go.
func readParquetJSON(t *testing.T, path string) []HospitalChargeRow {
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

func TestJSONReaderV2ToParquet(t *testing.T) {
	jsonPath := filepath.Join("testdata", "test_v2.json")
	parquetPath, jsonRows := jsonToParquet(t, jsonPath)
	pqRows := readParquetJSON(t, parquetPath)

	// V2 JSON: 3 items
	//   Item 1 (X-RAY): 1 charge setting × 2 payers = 2 rows
	//   Item 2 (IBUPROFEN): 1 charge setting × 1 payer = 1 row
	//   Item 3 (KNEE): 1 charge setting × 0 payers = 1 row (gross/discounted only)
	// Total: 4 rows
	if len(jsonRows) != 4 {
		t.Fatalf("JSON produced %d rows, want 4", len(jsonRows))
	}
	if len(pqRows) != 4 {
		t.Fatalf("parquet has %d rows, want 4", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewJSONReader(jsonPath)
	if err != nil {
		t.Fatalf("NewJSONReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "json-v2" {
		t.Errorf("format = %q, want %q", reader.Format(), "json-v2")
	}

	// ── Verify hospital metadata (same for all rows) ─────────────────
	for i, row := range pqRows {
		if row.HospitalName != "Test Community Hospital" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.LastUpdatedOn != "2024-06-01" {
			t.Errorf("row[%d].LastUpdatedOn = %q", i, row.LastUpdatedOn)
		}
		if row.Version != "2.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
		// hospital_location is joined with "; "
		if row.HospitalLocation != "Test Community Hospital; 456 Oak Ave, Brooklyn, NY 11201" {
			t.Errorf("row[%d].HospitalLocation = %q", i, row.HospitalLocation)
		}
		// hospital_address is joined with "; "
		if row.HospitalAddress != "456 Oak Ave; Brooklyn, NY 11201" {
			t.Errorf("row[%d].HospitalAddress = %q", i, row.HospitalAddress)
		}
		assertStrPtrEq(t, "LicenseNumber", row.LicenseNumber, strPtr("H-99887"))
		assertStrPtrEq(t, "LicenseState", row.LicenseState, strPtr("NY"))
		if !row.Affirmation {
			t.Errorf("row[%d].Affirmation = false, want true", i)
		}
	}

	// ── Row 0: X-RAY / Aetna ────────────────────────────────────────
	r := pqRows[0]
	if r.Description != "X-RAY CHEST 2 VIEWS" {
		t.Errorf("row[0].Description = %q", r.Description)
	}
	if r.Setting != "outpatient" {
		t.Errorf("row[0].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[0].CPTCode", r.CPTCode, strPtr("71046"))
	assertStrPtrEq(t, "row[0].RCCode", r.RCCode, strPtr("0324"))
	// V2 gross_charges as comma-formatted string "1,250.00"
	assertF64PtrEq(t, "row[0].GrossCharge", r.GrossCharge, f64Ptr(1250.00))
	assertF64PtrEq(t, "row[0].DiscountedCash", r.DiscountedCash, f64Ptr(625.00))
	assertF64PtrEq(t, "row[0].MinCharge", r.MinCharge, f64Ptr(400.00))
	assertF64PtrEq(t, "row[0].MaxCharge", r.MaxCharge, f64Ptr(1800.00))
	assertStrPtrEq(t, "row[0].Modifiers", r.Modifiers, strPtr("26|TC"))
	assertStrPtrEq(t, "row[0].AdditionalGenericNotes", r.AdditionalGenericNotes, strPtr("Includes interpretation"))
	assertStrPtrEq(t, "row[0].PayerName", r.PayerName, strPtr("Aetna"))
	assertStrPtrEq(t, "row[0].PlanName", r.PlanName, strPtr("Aetna PPO"))
	assertF64PtrEq(t, "row[0].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(800.00))
	assertStrPtrEq(t, "row[0].Methodology", r.Methodology, strPtr("fee_schedule"))
	assertStrPtrEq(t, "row[0].AdditionalPayerNotes", r.AdditionalPayerNotes, strPtr("Network rate"))

	// ── Row 1: X-RAY / Blue Cross (percentage + estimated) ──────────
	r = pqRows[1]
	if r.Description != "X-RAY CHEST 2 VIEWS" {
		t.Errorf("row[1].Description = %q", r.Description)
	}
	assertStrPtrEq(t, "row[1].PayerName", r.PayerName, strPtr("Blue Cross"))
	assertStrPtrEq(t, "row[1].PlanName", r.PlanName, strPtr("BC Standard"))
	assertStrPtrEq(t, "row[1].Methodology", r.Methodology, strPtr("percent_of_total_billed_charges"))
	assertF64PtrEq(t, "row[1].NegotiatedPercentage", r.NegotiatedPercentage, f64Ptr(75.5))
	assertF64PtrEq(t, "row[1].EstimatedAmount", r.EstimatedAmount, f64Ptr(943.75))
	assertF64PtrEq(t, "row[1].NegotiatedDollar", r.NegotiatedDollar, nil)
	// Same base charges as row 0
	assertF64PtrEq(t, "row[1].GrossCharge", r.GrossCharge, f64Ptr(1250.00))
	assertStrPtrEq(t, "row[1].Modifiers", r.Modifiers, strPtr("26|TC"))

	// ── Row 2: IBUPROFEN / drug info + algorithm ─────────────────────
	r = pqRows[2]
	if r.Description != "IBUPROFEN 200MG TABLET" {
		t.Errorf("row[2].Description = %q", r.Description)
	}
	if r.Setting != "inpatient" {
		t.Errorf("row[2].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[2].NDCCode", r.NDCCode, strPtr("00573-0150-20"))
	assertStrPtrEq(t, "row[2].HCPCSCode", r.HCPCSCode, strPtr("J3490"))
	assertStrPtrEq(t, "row[2].CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "row[2].DrugUnitOfMeasurement", r.DrugUnitOfMeasurement, f64Ptr(200.0))
	assertStrPtrEq(t, "row[2].DrugTypeOfMeasurement", r.DrugTypeOfMeasurement, strPtr("ME"))
	assertF64PtrEq(t, "row[2].GrossCharge", r.GrossCharge, f64Ptr(12.50))
	assertF64PtrEq(t, "row[2].DiscountedCash", r.DiscountedCash, f64Ptr(6.25))
	assertStrPtrEq(t, "row[2].PayerName", r.PayerName, strPtr("UnitedHealthcare"))
	assertStrPtrEq(t, "row[2].PlanName", r.PlanName, strPtr("UHC Choice Plus"))
	assertF64PtrEq(t, "row[2].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(8.00))
	assertStrPtrEq(t, "row[2].Methodology", r.Methodology, strPtr("per_diem"))
	assertStrPtrEq(t, "row[2].NegotiatedAlgorithm", r.NegotiatedAlgorithm, strPtr("per diem rate table v3"))

	// ── Row 3: KNEE REPLACEMENT / no payer, gross_charges string ─────
	r = pqRows[3]
	if r.Description != "KNEE REPLACEMENT" {
		t.Errorf("row[3].Description = %q", r.Description)
	}
	assertStrPtrEq(t, "row[3].MSDRGCode", r.MSDRGCode, strPtr("470"))
	assertStrPtrEq(t, "row[3].PayerName", r.PayerName, nil)
	assertStrPtrEq(t, "row[3].PlanName", r.PlanName, nil)
	// V2 gross_charges as comma-formatted string "45,000.00"
	assertF64PtrEq(t, "row[3].GrossCharge", r.GrossCharge, f64Ptr(45000.00))
	assertF64PtrEq(t, "row[3].DiscountedCash", r.DiscountedCash, f64Ptr(22500.00))
	assertF64PtrEq(t, "row[3].MinCharge", r.MinCharge, f64Ptr(20000.00))
	assertF64PtrEq(t, "row[3].MaxCharge", r.MaxCharge, f64Ptr(65000.00))

	// ── Round-trip: JSON rows match parquet rows ─────────────────────
	for i := range jsonRows {
		j := jsonRows[i]
		p := pqRows[i]
		if j.Description != p.Description {
			t.Errorf("row[%d] Description mismatch: json=%q pq=%q", i, j.Description, p.Description)
		}
		if j.Setting != p.Setting {
			t.Errorf("row[%d] Setting mismatch: json=%q pq=%q", i, j.Setting, p.Setting)
		}
		assertStrPtrEq(t, "roundtrip PayerName", p.PayerName, j.PayerName)
		assertStrPtrEq(t, "roundtrip PlanName", p.PlanName, j.PlanName)
		assertF64PtrEq(t, "roundtrip GrossCharge", p.GrossCharge, j.GrossCharge)
		assertF64PtrEq(t, "roundtrip DiscountedCash", p.DiscountedCash, j.DiscountedCash)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", p.NegotiatedDollar, j.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", p.Methodology, j.Methodology)
		assertF64PtrEq(t, "roundtrip DrugUnit", p.DrugUnitOfMeasurement, j.DrugUnitOfMeasurement)
		assertStrPtrEq(t, "roundtrip DrugType", p.DrugTypeOfMeasurement, j.DrugTypeOfMeasurement)
	}
}

func TestJSONReaderV3ToParquet(t *testing.T) {
	jsonPath := filepath.Join("testdata", "test_v3.json")
	parquetPath, jsonRows := jsonToParquet(t, jsonPath)
	pqRows := readParquetJSON(t, parquetPath)

	// V3 JSON: 2 items
	//   Item 1 (MRI): 1 payer = 1 row
	//   Item 2 (ER VISIT): 0 payers = 1 row
	// Total: 2 rows
	if len(jsonRows) != 2 {
		t.Fatalf("JSON produced %d rows, want 2", len(jsonRows))
	}
	if len(pqRows) != 2 {
		t.Fatalf("parquet has %d rows, want 2", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewJSONReader(jsonPath)
	if err != nil {
		t.Fatalf("NewJSONReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "json-v3" {
		t.Errorf("format = %q, want %q", reader.Format(), "json-v3")
	}

	// ── Verify V3 metadata ───────────────────────────────────────────
	for i, row := range pqRows {
		if row.HospitalName != "Metro Health Center" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.LastUpdatedOn != "2025-01-15" {
			t.Errorf("row[%d].LastUpdatedOn = %q", i, row.LastUpdatedOn)
		}
		if row.Version != "3.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
		// V3 uses location_name instead of hospital_location
		if row.HospitalLocation != "Metro Health Center; 789 Elm St, Manhattan, NY 10010" {
			t.Errorf("row[%d].HospitalLocation = %q", i, row.HospitalLocation)
		}
		assertStrPtrEq(t, "LicenseNumber", row.LicenseNumber, strPtr("MHC-5544"))
		assertStrPtrEq(t, "LicenseState", row.LicenseState, strPtr("NY"))
		// V3 uses attestation instead of affirmation
		if !row.Affirmation {
			t.Errorf("row[%d].Affirmation = false, want true", i)
		}
	}

	// ── Row 0: MRI BRAIN / Cigna ─────────────────────────────────────
	r := pqRows[0]
	if r.Description != "MRI BRAIN WITHOUT CONTRAST" {
		t.Errorf("row[0].Description = %q", r.Description)
	}
	if r.Setting != "outpatient" {
		t.Errorf("row[0].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[0].CPTCode", r.CPTCode, strPtr("70551"))
	// V3 uses gross_charge (number) instead of gross_charges (string)
	assertF64PtrEq(t, "row[0].GrossCharge", r.GrossCharge, f64Ptr(3500.00))
	assertF64PtrEq(t, "row[0].DiscountedCash", r.DiscountedCash, f64Ptr(1750.00))
	assertF64PtrEq(t, "row[0].MinCharge", r.MinCharge, f64Ptr(1200.00))
	assertF64PtrEq(t, "row[0].MaxCharge", r.MaxCharge, f64Ptr(4500.00))
	assertStrPtrEq(t, "row[0].PayerName", r.PayerName, strPtr("Cigna"))
	assertStrPtrEq(t, "row[0].PlanName", r.PlanName, strPtr("Cigna Open Access"))
	assertF64PtrEq(t, "row[0].NegotiatedDollar", r.NegotiatedDollar, f64Ptr(2200.00))
	assertStrPtrEq(t, "row[0].Methodology", r.Methodology, strPtr("case_rate"))

	// ── Row 1: ER VISIT / no payer ───────────────────────────────────
	r = pqRows[1]
	if r.Description != "EMERGENCY ROOM VISIT LEVEL 3" {
		t.Errorf("row[1].Description = %q", r.Description)
	}
	if r.Setting != "outpatient" {
		t.Errorf("row[1].Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "row[1].CPTCode", r.CPTCode, strPtr("99283"))
	assertStrPtrEq(t, "row[1].RCCode", r.RCCode, strPtr("0450"))
	assertF64PtrEq(t, "row[1].GrossCharge", r.GrossCharge, f64Ptr(950.00))
	assertF64PtrEq(t, "row[1].DiscountedCash", r.DiscountedCash, f64Ptr(475.00))
	assertF64PtrEq(t, "row[1].MinCharge", r.MinCharge, nil)
	assertF64PtrEq(t, "row[1].MaxCharge", r.MaxCharge, nil)
	assertStrPtrEq(t, "row[1].PayerName", r.PayerName, nil)
	assertStrPtrEq(t, "row[1].PlanName", r.PlanName, nil)

	// ── Round-trip integrity ─────────────────────────────────────────
	for i := range jsonRows {
		j := jsonRows[i]
		p := pqRows[i]
		if j.Description != p.Description {
			t.Errorf("row[%d] Description mismatch: json=%q pq=%q", i, j.Description, p.Description)
		}
		assertStrPtrEq(t, "roundtrip PayerName", p.PayerName, j.PayerName)
		assertF64PtrEq(t, "roundtrip GrossCharge", p.GrossCharge, j.GrossCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", p.NegotiatedDollar, j.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", p.Methodology, j.Methodology)
	}
}
