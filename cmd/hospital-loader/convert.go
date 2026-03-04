package main

import (
	"archive/zip"
	"context"
	"encoding/json"
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

type geocodeResult struct {
	Address   string  `json:"address"`
	Matched   bool    `json:"matched"`
	MatchType string  `json:"match_type,omitempty"`
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
}

type logEntry struct {
	Success           bool     `json:"success"`
	InputFormat       string   `json:"input_format"`
	URL               string   `json:"url"`
	StartTime         string   `json:"start_time"`
	DurationSeconds   float64  `json:"duration_seconds"`
	Error             string   `json:"error,omitempty"`
	OutputFile        string   `json:"output_file,omitempty"`
	HospitalName      string   `json:"hospital_name"`
	LocationNames     []string `json:"location_names"`
	HospitalAddresses []string `json:"hospital_addresses"`
	LicenseNumber     *string  `json:"license_number"`
	LicenseState      *string  `json:"license_state"`
	Type2NPIs         []string `json:"type_2_npis"`
	LastUpdatedOn     string          `json:"last_updated_on"`
	SchemaVersion     string          `json:"schema_version"`
	Geocodes          []geocodeResult `json:"geocodes,omitempty"`
}

// processEntry handles a single file conversion: URL download, convert, log.
// Both single and batch subcommands call this.
func processEntry(logPrefix, inputFile, outputFile, logFile string, batchSize int, skipPayerCharges bool) error {
	startTime := time.Now()
	inputDisplay := inputFile
	var meta internal.RunMeta
	var processErr error

	// Always write a log entry when we're done, regardless of success/failure.
	defer func() {
		inputFormat := "csv"
		localExt := filepath.Ext(inputFile)
		if isURL(inputFile) {
			if u, err := url.Parse(inputFile); err == nil {
				localExt = path.Ext(u.Path)
			}
		}
		if strings.EqualFold(localExt, ".json") {
			inputFormat = "json"
		}

		entry := logEntry{
			Success:           processErr == nil,
			InputFormat:       inputFormat,
			URL:               inputDisplay,
			StartTime:         startTime.Format(time.RFC3339),
			DurationSeconds:   time.Since(startTime).Seconds(),
			HospitalName:      meta.HospitalName,
			LocationNames:     meta.LocationNames,
			HospitalAddresses: meta.HospitalAddresses,
			LicenseNumber:     meta.LicenseNumber,
			LicenseState:      meta.LicenseState,
			Type2NPIs:         meta.Type2NPIs,
			LastUpdatedOn:     meta.LastUpdatedOn,
			SchemaVersion:     meta.Version,
		}
		if processErr != nil {
			entry.Error = processErr.Error()
		}
		if processErr == nil && outputFile != "" {
			if strings.HasPrefix(outputFile, "s3://") {
				entry.OutputFile = outputFile
			} else if abs, err := filepath.Abs(outputFile); err == nil {
				entry.OutputFile = abs
			} else {
				entry.OutputFile = outputFile
			}
		}

		if err := appendLogEntry(logFile, &entry); err != nil {
			log.Printf("warning: failed to write log entry: %v", err)
		}
	}()

	// If input is a URL, download to a temp file first.
	localInput := inputFile
	if isURL(inputFile) {
		localPath, cleanup, err := downloadURL(logPrefix, inputFile)
		if err != nil {
			processErr = fmt.Errorf("download %s: %w", inputFile, err)
			return processErr
		}
		defer cleanup()
		localInput = localPath
	}

	if outputFile == "" {
		base := strings.TrimSuffix(filepath.Base(inputDisplay), filepath.Ext(inputDisplay))
		// For URLs, extract filename from the URL path
		if isURL(inputDisplay) {
			if u, err := url.Parse(inputDisplay); err == nil {
				base = strings.TrimSuffix(path.Base(u.Path), path.Ext(u.Path))
			}
		}
		outputFile = base + ".parquet"
	}

	s3Dest := ""
	localOut := outputFile
	if strings.HasPrefix(outputFile, "s3://") {
		s3Dest = outputFile
		f, err := os.CreateTemp("", "hospital-loader-*.parquet")
		if err != nil {
			processErr = fmt.Errorf("create temp file: %v", err)
			return processErr
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

	displayOut := outputFile
	meta, processErr = convert(logPrefix, localInput, inputDisplay, localOut, displayOut, batchSize, skipPayerCharges)
	if processErr != nil {
		return processErr
	}

	if s3Dest != "" {
		if err := uploadToS3(logPrefix, context.Background(), localOut, s3Dest); err != nil {
			processErr = err
			return processErr
		}
	}

	return nil
}

func convert(logPrefix, inputPath, inputDisplay, outputPath, displayPath string, batchSize int, skipPayerCharges bool) (internal.RunMeta, error) {
	start := time.Now()
	var meta internal.RunMeta

	isJSON := strings.EqualFold(filepath.Ext(inputPath), ".json")

	var reader chargeReader
	var csvReader *internal.CSVReader
	var jsonReader *internal.JSONReader
	var err error

	if isJSON {
		jsonReader, err = internal.NewJSONReader(inputPath)
		if err != nil {
			return meta, fmt.Errorf("open JSON: %w", err)
		}
		jsonReader.SkipPayerCharges = skipPayerCharges
		reader = jsonReader
		meta = jsonReader.Meta()
	} else {
		csvReader, err = internal.NewCSVReader(inputPath)
		if err != nil {
			return meta, fmt.Errorf("open CSV: %w", err)
		}
		csvReader.SkipPayerCharges = skipPayerCharges
		reader = csvReader
		meta = csvReader.Meta()
	}
	defer reader.Close()

	writer, err := internal.NewChargeWriter(outputPath)
	if err != nil {
		return meta, fmt.Errorf("create Parquet: %w", err)
	}

	fi, _ := os.Stat(inputPath)
	fileSize := int64(0)
	if fi != nil {
		fileSize = fi.Size()
	}

	pprintf(logPrefix, "Input:   %s\n", inputDisplay)
	pprintf(logPrefix, "Output:  %s\n", displayPath)
	pprintf(logPrefix, "Format:  %s\n", reader.Format())
	if csvReader != nil && csvReader.Format() == "wide" {
		pprintf(logPrefix, "Payers:  %d payer/plan combinations\n", csvReader.PayerPlanCount())
	}
	if fileSize > 0 {
		pprintf(logPrefix, "Size:    %.1f MB\n", float64(fileSize)/1024/1024)
	}
	pprintln(logPrefix)

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
				return meta, fmt.Errorf("read JSON item %d: %w", jsonReader.ItemNum()+1, err)
			}
			return meta, fmt.Errorf("read CSV row %d: %w", csvReader.RowNum(), err)
		}

		inputCount++
		batch = append(batch, rows...)

		if len(batch) >= batchSize {
			if _, err := writer.Write(batch); err != nil {
				return meta, fmt.Errorf("write Parquet batch: %w", err)
			}
			totalRows += len(batch)
			batch = batch[:0]
		}

		if time.Since(lastLog) >= 5*time.Second {
			elapsed := time.Since(start).Seconds()
			pprintf(logPrefix, "  progress: %d %s → %d Parquet rows (%.0f rows/s)\n",
				inputCount, inputLabel, totalRows+len(batch), float64(totalRows+len(batch))/elapsed)
			lastLog = time.Now()
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		if _, err := writer.Write(batch); err != nil {
			return meta, fmt.Errorf("write final Parquet batch: %w", err)
		}
		totalRows += len(batch)
	}

	if err := writer.Close(); err != nil {
		return meta, fmt.Errorf("close Parquet: %w", err)
	}

	elapsed := time.Since(start)
	outFi, _ := os.Stat(outputPath)
	outSize := int64(0)
	if outFi != nil {
		outSize = outFi.Size()
	}

	pprintln(logPrefix)
	pprintf(logPrefix, "Done in %s\n", elapsed.Round(time.Millisecond))
	pprintf(logPrefix, "  %-14s %d\n", inputLabel+":", inputCount)
	pprintf(logPrefix, "  Parquet rows: %d\n", totalRows)
	pprintf(logPrefix, "  Throughput:   %.0f rows/s\n", float64(totalRows)/elapsed.Seconds())
	if fileSize > 0 && outSize > 0 {
		pprintf(logPrefix, "  Input size:   %.1f MB\n", float64(fileSize)/1024/1024)
		pprintf(logPrefix, "  Output size:  %.1f MB (%.1fx compression)\n",
			float64(outSize)/1024/1024, float64(fileSize)/float64(outSize))
	}

	return meta, nil
}

func appendLogEntry(path string, entry *logEntry) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}
	data = append(data, '\n')

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write log entry: %w", err)
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

func uploadToS3(logPrefix string, ctx context.Context, localPath, s3URI string) error {
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

	pprintf(logPrefix, "\nUploading %.1f MB to %s ...\n", float64(fi.Size())/1024/1024, s3URI)
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

	pprintf(logPrefix, "Uploaded in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// downloadURL downloads a URL to a temp file, preserving the original file
// extension so format detection works. Returns the temp file path and a
// cleanup function that removes the temp file.
func downloadURL(logPrefix, rawURL string) (localPath string, cleanup func(), err error) {
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

	pprintf(logPrefix, "Downloading %s ...\n", rawURL)
	start := time.Now()

	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		f.Close()
		cleanupFn()
		return "", nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")

	resp, err := http.DefaultClient.Do(req)
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

	pprintf(logPrefix, "Downloaded %.1f MB in %s\n\n", float64(n)/1024/1024, time.Since(start).Round(time.Millisecond))

	// If the downloaded file is a zip, extract the first CSV/JSON from it.
	if strings.HasSuffix(strings.ToLower(tmpPath), ".zip") {
		extracted, err := extractZip(tmpPath)
		if err != nil {
			cleanupFn()
			return "", nil, fmt.Errorf("extract zip: %w", err)
		}
		// Clean up the zip file, return the extracted file instead.
		os.Remove(tmpPath)
		extractedCleanup := func() { os.Remove(extracted) }
		pprintf(logPrefix, "Extracted %s (%.1f MB)\n\n", filepath.Base(extracted), float64(fileSize(extracted))/1024/1024)
		return extracted, extractedCleanup, nil
	}

	return tmpPath, cleanupFn, nil
}

// extractZip opens a zip file and extracts the first CSV or JSON file to a
// temp file. Returns the path to the extracted file.
func extractZip(zipPath string) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	// Find first CSV or JSON file in the archive.
	var target *zip.File
	for _, f := range r.File {
		lower := strings.ToLower(f.Name)
		if strings.HasSuffix(lower, ".csv") || strings.HasSuffix(lower, ".json") {
			target = f
			break
		}
	}
	if target == nil {
		// Fall back to first file.
		if len(r.File) == 0 {
			return "", fmt.Errorf("empty zip archive")
		}
		target = r.File[0]
	}

	ext := filepath.Ext(target.Name)
	if ext == "" {
		ext = ".csv"
	}

	rc, err := target.Open()
	if err != nil {
		return "", fmt.Errorf("open %s in zip: %w", target.Name, err)
	}
	defer rc.Close()

	tmp, err := os.CreateTemp("", "hospital-loader-*"+ext)
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(tmp, rc); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("extract %s: %w", target.Name, err)
	}
	tmp.Close()

	return tmp.Name(), nil
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
