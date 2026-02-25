package internal

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

const (
	// RowsPerGroup controls how many rows go into each Parquet row group.
	// Smaller row groups = more granular predicate pushdown over the network
	// (engines skip entire row groups whose min/max stats don't match).
	// 50K rows yields ~4 row groups for a typical 210K-row hospital file.
	RowsPerGroup = 50000

	// bloomBitsPerValue controls bloom filter sizing. 10 bits/value ≈ 1%
	// false positive rate — a good trade-off between filter size and accuracy.
	bloomBitsPerValue = 10
)

// ChargeWriter writes HospitalChargeRow records to a Parquet file configured
// for fast analytical queries and small file size.
//
// Writer configuration rationale:
//
//   - Zstd(3): ~20-30% smaller than Snappy with acceptable write overhead.
//
//   - Rows sorted by cpt_code ascending: clusters rows by the most common
//     query predicate, producing tight per-row-group min/max statistics so
//     engines can skip row groups that can't match.
//
//   - 50K rows per row group: for a 210K-row file this yields ~4 row groups.
//     More row groups = finer-grained predicate pushdown over the network.
//
//   - Bloom filters on all 19 code columns plus payer_name/plan_name: lets
//     engines definitively rule out a row group with a small read, even when
//     min/max ranges overlap.
//
//   - 8KB page size with statistics: enables page-level filtering within row
//     groups (DuckDB 0.9+, Spark 3.3+).
type ChargeWriter struct {
	file   *os.File
	writer *parquet.GenericWriter[HospitalChargeRow]
	rows   []HospitalChargeRow
}

// NewChargeWriter creates a Parquet writer optimized for analytical queries.
func NewChargeWriter(filename string) (*ChargeWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[HospitalChargeRow](file,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
		parquet.PageBufferSize(8*1024),
		parquet.DataPageStatistics(true),
		parquet.CreatedBy("pricetool", "1.0", ""),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(bloomBitsPerValue, "cpt_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "hcpcs_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ms_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ndc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "rc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "icd_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cdm_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "local_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "apc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "eapg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "hipps_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cdt_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "r_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "s_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "aps_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ap_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "apr_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "tris_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "payer_name"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "plan_name"),
		),
	)

	return &ChargeWriter{
		file:   file,
		writer: writer,
	}, nil
}

// Write buffers rows. Rows are sorted and flushed on Close.
func (w *ChargeWriter) Write(rows []HospitalChargeRow) (int, error) {
	w.rows = append(w.rows, rows...)
	return len(rows), nil
}

// Close sorts all buffered rows by cpt_code, writes them in fixed-size row
// groups (flushing after each group to force row group boundaries), and closes.
func (w *ChargeWriter) Close() error {
	slices.SortFunc(w.rows, func(a, b HospitalChargeRow) int {
		return cmpOptStr(a.CPTCode, b.CPTCode)
	})

	for i := 0; i < len(w.rows); i += RowsPerGroup {
		end := i + RowsPerGroup
		if end > len(w.rows) {
			end = len(w.rows)
		}
		if _, err := w.writer.Write(w.rows[i:end]); err != nil {
			w.file.Close()
			return fmt.Errorf("write parquet rows: %w", err)
		}
		if err := w.writer.Flush(); err != nil {
			w.file.Close()
			return fmt.Errorf("flush row group: %w", err)
		}
	}

	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return w.file.Close()
}

// Count returns the total number of rows buffered.
func (w *ChargeWriter) Count() int {
	return len(w.rows)
}

// cmpOptStr compares two optional strings, with nil (null) sorting first.
func cmpOptStr(a, b *string) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return strings.Compare(*a, *b)
}
