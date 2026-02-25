package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"pricetool/internal"
	"strings"
	"time"
)

// chargeReader is the common interface for CSV and JSON readers.
type chargeReader interface {
	Next() ([]internal.HospitalChargeRow, error)
	Format() string
	Close() error
}

func main() {
	inputFile := flag.String("file", "", "Input file (CSV/JSON for Parquet mode, Parquet for PG mode)")
	outputFile := flag.String("out", "", "Output Parquet file")
	batchSize := flag.Int("batch", 10000, "Batch size (default: 10000 for Parquet)")
	skipPayerCharges := flag.Bool("skip-payer-charges", true, "Skip parsing/uploading payer_charges (default: true)")
	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  CSV/JSON → Parquet: hospital_loader -file input.csv [-out output.parquet] [-batch N]\n")
		fmt.Fprintf(os.Stderr, "                      hospital_loader -file input.json [-out output.parquet] [-batch N]\n")
		os.Exit(1)
	}

	if *outputFile == "" {
		base := strings.TrimSuffix(filepath.Base(*inputFile), filepath.Ext(*inputFile))
		*outputFile = base + ".parquet"
	}
	if err := convert(*inputFile, *outputFile, *batchSize, *skipPayerCharges); err != nil {
		log.Fatal(err)
	}
}

func convert(inputPath, outputPath string, batchSize int, skipPayerCharges bool) error {
	start := time.Now()

	isJSON := strings.EqualFold(filepath.Ext(inputPath), ".json")

	var reader chargeReader
	var csvReader *internal.CSVReader
	var jsonReader *internal.JSONReader
	var err error

	if isJSON {
		jsonReader, err = internal.NewJSONReader(inputPath)
		if err != nil {
			return fmt.Errorf("open JSON: %w", err)
		}
		jsonReader.SkipPayerCharges = skipPayerCharges
		reader = jsonReader
	} else {
		csvReader, err = internal.NewCSVReader(inputPath)
		if err != nil {
			return fmt.Errorf("open CSV: %w", err)
		}
		csvReader.SkipPayerCharges = skipPayerCharges
		reader = csvReader
	}
	defer reader.Close()

	writer, err := internal.NewChargeWriter(outputPath)
	if err != nil {
		return fmt.Errorf("create Parquet: %w", err)
	}

	fi, _ := os.Stat(inputPath)
	fileSize := int64(0)
	if fi != nil {
		fileSize = fi.Size()
	}

	fmt.Printf("Input:   %s\n", inputPath)
	fmt.Printf("Output:  %s\n", outputPath)
	fmt.Printf("Format:  %s\n", reader.Format())
	if csvReader != nil && csvReader.Format() == "wide" {
		fmt.Printf("Payers:  %d payer/plan combinations\n", csvReader.PayerPlanCount())
	}
	if fileSize > 0 {
		fmt.Printf("Size:    %.1f MB\n", float64(fileSize)/1024/1024)
	}
	fmt.Println()

	inputLabel := "CSV rows"
	if isJSON {
		inputLabel = "JSON items"
	}

	batch := make([]internal.HospitalChargeRow, 0, batchSize)
	var totalRows int
	var inputCount int64
	lastLog := time.Now()

	for {
		rows, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isJSON {
				return fmt.Errorf("read JSON item %d: %w", jsonReader.ItemNum()+1, err)
			}
			return fmt.Errorf("read CSV row %d: %w", csvReader.RowNum(), err)
		}

		inputCount++
		batch = append(batch, rows...)

		if len(batch) >= batchSize {
			if _, err := writer.Write(batch); err != nil {
				return fmt.Errorf("write Parquet batch: %w", err)
			}
			totalRows += len(batch)
			batch = batch[:0]
		}

		if time.Since(lastLog) >= 5*time.Second {
			elapsed := time.Since(start).Seconds()
			fmt.Printf("  progress: %d %s → %d Parquet rows (%.0f rows/s)\n",
				inputCount, inputLabel, totalRows+len(batch), float64(totalRows+len(batch))/elapsed)
			lastLog = time.Now()
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		if _, err := writer.Write(batch); err != nil {
			return fmt.Errorf("write final Parquet batch: %w", err)
		}
		totalRows += len(batch)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close Parquet: %w", err)
	}

	elapsed := time.Since(start)
	outFi, _ := os.Stat(outputPath)
	outSize := int64(0)
	if outFi != nil {
		outSize = outFi.Size()
	}

	fmt.Println()
	fmt.Printf("Done in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  %-14s %d\n", inputLabel+":", inputCount)
	fmt.Printf("  Parquet rows: %d\n", totalRows)
	fmt.Printf("  Throughput:   %.0f rows/s\n", float64(totalRows)/elapsed.Seconds())
	if fileSize > 0 && outSize > 0 {
		fmt.Printf("  Input size:   %.1f MB\n", float64(fileSize)/1024/1024)
		fmt.Printf("  Output size:  %.1f MB (%.1fx compression)\n",
			float64(outSize)/1024/1024, float64(fileSize)/float64(outSize))
	}

	return nil
}
