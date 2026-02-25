package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"pricetool/internal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
		fmt.Fprintf(os.Stderr, "                      hospital_loader -file https://example.com/charges.csv [-out output.parquet]\n")
		os.Exit(1)
	}

	// If input is a URL, download to a temp file first.
	inputDisplay := *inputFile
	if isURL(*inputFile) {
		localPath, cleanup, err := downloadURL(*inputFile)
		if err != nil {
			log.Fatalf("download %s: %v", *inputFile, err)
		}
		defer cleanup()
		*inputFile = localPath
	}

	if *outputFile == "" {
		base := strings.TrimSuffix(filepath.Base(inputDisplay), filepath.Ext(inputDisplay))
		// For URLs, extract filename from the URL path
		if isURL(inputDisplay) {
			if u, err := url.Parse(inputDisplay); err == nil {
				base = strings.TrimSuffix(path.Base(u.Path), path.Ext(u.Path))
			}
		}
		*outputFile = base + ".parquet"
	}

	s3Dest := ""
	localOut := *outputFile
	if strings.HasPrefix(*outputFile, "s3://") {
		s3Dest = *outputFile
		f, err := os.CreateTemp("", "hospital-loader-*.parquet")
		if err != nil {
			log.Fatalf("create temp file: %v", err)
		}
		localOut = f.Name()
		f.Close()
		// Ensure temp file is cleaned up on exit and signals.
		defer os.Remove(localOut)
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			os.Remove(localOut)
			os.Exit(1)
		}()
	}

	displayOut := *outputFile
	if err := convert(*inputFile, inputDisplay, localOut, displayOut, *batchSize, *skipPayerCharges); err != nil {
		log.Fatal(err)
	}

	if s3Dest != "" {
		if err := uploadToS3(context.Background(), localOut, s3Dest); err != nil {
			log.Fatal(err)
		}
	}
}

func convert(inputPath, inputDisplay, outputPath, displayPath string, batchSize int, skipPayerCharges bool) error {
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

	fmt.Printf("Input:   %s\n", inputDisplay)
	fmt.Printf("Output:  %s\n", displayPath)
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

// parseS3URI splits "s3://bucket/key/path" into bucket and key.
func parseS3URI(uri string) (bucket, key string, err error) {
	uri = strings.TrimPrefix(uri, "s3://")
	parts := strings.SplitN(uri, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid S3 URI: must be s3://bucket/key")
	}
	return parts[0], parts[1], nil
}

func uploadToS3(ctx context.Context, localPath, s3URI string) error {
	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		return err
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local file for upload: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat local file: %w", err)
	}

	fmt.Printf("\nUploading %.1f MB to %s ...\n", float64(fi.Size())/1024/1024, s3URI)
	start := time.Now()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("S3 PutObject: %w", err)
	}

	fmt.Printf("Uploaded in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// downloadURL downloads a URL to a temp file, preserving the original file
// extension so format detection works. Returns the temp file path and a
// cleanup function that removes the temp file.
func downloadURL(rawURL string) (localPath string, cleanup func(), err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", nil, fmt.Errorf("parse URL: %w", err)
	}

	ext := path.Ext(u.Path)
	if ext == "" {
		ext = ".csv" // default assumption
	}

	f, err := os.CreateTemp("", "hospital-loader-*"+ext)
	if err != nil {
		return "", nil, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := f.Name()

	cleanupFn := func() { os.Remove(tmpPath) }

	// Also clean up on signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		os.Remove(tmpPath)
		os.Exit(1)
	}()

	fmt.Printf("Downloading %s ...\n", rawURL)
	start := time.Now()

	resp, err := http.Get(rawURL)
	if err != nil {
		f.Close()
		cleanupFn()
		return "", nil, fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		f.Close()
		cleanupFn()
		return "", nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	n, err := io.Copy(f, resp.Body)
	if err != nil {
		f.Close()
		cleanupFn()
		return "", nil, fmt.Errorf("download: %w", err)
	}

	if err := f.Close(); err != nil {
		cleanupFn()
		return "", nil, fmt.Errorf("close temp file: %w", err)
	}

	fmt.Printf("Downloaded %.1f MB in %s\n\n", float64(n)/1024/1024, time.Since(start).Round(time.Millisecond))
	return tmpPath, cleanupFn, nil
}
