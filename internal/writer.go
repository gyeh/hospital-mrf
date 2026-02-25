package internal

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// ChargeWriter writes HospitalChargeRow records to a Parquet file configured
// for fast analytical queries and small file size.
//
// Writer configuration rationale:
//
//	Zstd(3): ~20-30% smaller files than Snappy with acceptable write overhead.
//	Good decode speed for query engines.
//
//	64MB row groups: balances row-group-level min/max skip (smaller = more
//	granular skip) against compression ratio (larger = better dictionary reuse).
//	For typical hospital files (1-10M rows), this yields 5-50 row groups —
//	enough for effective predicate pushdown.
//
//	Column page size 8KB: enables page-level filtering within row groups
//	when the engine supports column indexes (DuckDB 0.9+, Spark 3.3+).
//
//	Statistics on every column: row-group min/max stored for all columns,
//	enabling skip on any predicate.
//
// For best query performance, sort input rows by (description, payer_name)
// before writing. This maximizes row-group skip for the two most common
// filter patterns: "find charges for procedure X" and "find charges by payer".
type ChargeWriter struct {
	file   *os.File
	writer *parquet.GenericWriter[HospitalChargeRow]
	count  int
}

// NewChargeWriter creates a Parquet writer optimized for analytical queries.
func NewChargeWriter(filename string) (*ChargeWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[HospitalChargeRow](file,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
		parquet.PageBufferSize(8*1024),        // 8KB pages for page-level filtering
		parquet.WriteBufferSize(64*1024*1024), // 64MB row groups
		parquet.DataPageStatistics(true),      // page-level min/max for column indexes
		parquet.CreatedBy("pricetool", "1.0", ""),
	)

	return &ChargeWriter{
		file:   file,
		writer: writer,
	}, nil
}

// Write writes a batch of rows. Callers should batch rows (e.g. 10K at a time)
// to amortize write overhead.
func (w *ChargeWriter) Write(rows []HospitalChargeRow) (int, error) {
	n, err := w.writer.Write(rows)
	w.count += n
	if err != nil {
		return n, fmt.Errorf("write parquet rows: %w", err)
	}
	return n, nil
}

// Close flushes the final row group and closes the file.
func (w *ChargeWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return w.file.Close()
}

// Count returns the total number of rows written.
func (w *ChargeWriter) Count() int {
	return w.count
}
