package internal

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// chargeReader is the common interface for CSV and JSON readers.
type chargeReader interface {
	Next() ([]HospitalChargeRow, error)
	Format() string
	Close() error
}

type geocodeResult struct {
	Address   string  `json:"address"`
	Matched   bool    `json:"matched"`
	MatchType string  `json:"match_type,omitempty"`
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
	Source    string  `json:"source,omitempty"`
}

type logEntry struct {
	Success           bool            `json:"success"`
	InputFormat       string          `json:"input_format"`
	URL               string          `json:"url"`
	StartTime         string          `json:"start_time"`
	DurationSeconds   float64         `json:"duration_seconds"`
	Error             string          `json:"error,omitempty"`
	OutputFile        string          `json:"output_file,omitempty"`
	HospitalName      string          `json:"hospital_name"`
	LocationNames     []string        `json:"location_names"`
	HospitalAddresses []string        `json:"hospital_addresses"`
	LicenseNumber     *string         `json:"license_number"`
	LicenseState      *string         `json:"license_state"`
	Type2NPIs         []string        `json:"type_2_npis"`
	LastUpdatedOn     string          `json:"last_updated_on"`
	SchemaVersion     string          `json:"schema_version"`
	Geocodes          []geocodeResult `json:"geocodes,omitempty"`
}

// ProcessEntry handles a single file conversion: URL download, convert, log.
// Both single and batch subcommands call this.
//
// outputFile can be:
//   - A specific file path: "output.parquet"
//   - An S3 URI: "s3://bucket/key.parquet"
//   - A directory (local or S3, ending in "/"): "out/" or "s3://bucket/prefix/"
//   - Empty: output to current directory with metadata-derived name
//
// When outputFile is empty or a directory, the filename is derived from
// hospital metadata: {hospital_name}-{license_number}-{last_updated_on}.parquet
func ProcessEntry(logger *slog.Logger, inputFile, outputFile, logFile string, batchSize int, skipPayerCharges bool) error {
	startTime := time.Now()
	inputDisplay := inputFile
	var meta RunMeta
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
			logger.Warn("failed to write log entry", "error", err)
		}
	}()

	// If input is a URL, download to a temp file first.
	localInput := inputFile
	if isURL(inputFile) {
		localPath, cleanup, err := downloadURL(logger, inputFile)
		if err != nil {
			processErr = fmt.Errorf("download %s: %w", inputFile, err)
			return processErr
		}
		defer cleanup()
		localInput = localPath
	}

	// Determine if output is a directory (filename will be derived from metadata).
	outputIsDir := outputFile == "" || strings.HasSuffix(outputFile, "/")
	isS3 := strings.HasPrefix(outputFile, "s3://")

	// Always write to a temp file when the final name isn't known yet,
	// or when uploading to S3.
	var tempFile string
	s3Dest := ""
	localOut := outputFile

	if isS3 || outputIsDir {
		f, err := os.CreateTemp("", "hospital-loader-*.parquet")
		if err != nil {
			processErr = fmt.Errorf("create temp file: %v", err)
			return processErr
		}
		tempFile = f.Name()
		f.Close()
		localOut = tempFile
		defer func() {
			if tempFile != "" {
				os.Remove(tempFile)
			}
		}()
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			if tempFile != "" {
				os.Remove(tempFile)
			}
			os.Exit(1)
		}()
		if isS3 {
			s3Dest = outputFile
		}
	}

	displayOut := outputFile
	meta, processErr = convert(logger, localInput, inputDisplay, localOut, displayOut, batchSize, skipPayerCharges)
	if processErr != nil {
		return processErr
	}

	// Resolve the final output filename from metadata.
	if outputIsDir {
		filename := buildOutputFilename(meta)
		if isS3 {
			outputFile = strings.TrimSuffix(outputFile, "/") + "/" + filename
			s3Dest = outputFile
		} else {
			dir := strings.TrimSuffix(outputFile, "/")
			if dir == "" {
				dir = "."
			}
			finalPath := filepath.Join(dir, filename)
			if err := os.Rename(localOut, finalPath); err != nil {
				processErr = fmt.Errorf("rename output: %w", err)
				return processErr
			}
			tempFile = "" // renamed successfully, don't clean up
			outputFile = finalPath
		}
		logger.Info("output file resolved", "path", outputFile)
	}

	if s3Dest != "" {
		if err := uploadToS3(logger, context.Background(), localOut, s3Dest); err != nil {
			processErr = err
			return processErr
		}
	}

	return nil
}

func convert(logger *slog.Logger, inputPath, inputDisplay, outputPath, displayPath string, batchSize int, skipPayerCharges bool) (RunMeta, error) {
	start := time.Now()
	var meta RunMeta

	isJSON := strings.EqualFold(filepath.Ext(inputPath), ".json")

	var reader chargeReader
	var csvReader *CSVReader
	var jsonReader *JSONReader
	var err error

	if isJSON {
		jsonReader, err = NewJSONReader(inputPath)
		if err != nil {
			return meta, fmt.Errorf("open JSON: %w", err)
		}
		jsonReader.SkipPayerCharges = skipPayerCharges
		reader = jsonReader
		meta = jsonReader.Meta()
	} else {
		csvReader, err = NewCSVReader(inputPath)
		if err != nil {
			return meta, fmt.Errorf("open CSV: %w", err)
		}
		csvReader.SkipPayerCharges = skipPayerCharges
		reader = csvReader
		meta = csvReader.Meta()
	}
	defer reader.Close()

	writer, err := NewChargeWriter(outputPath)
	if err != nil {
		return meta, fmt.Errorf("create Parquet: %w", err)
	}

	fi, _ := os.Stat(inputPath)
	inputSize := int64(0)
	if fi != nil {
		inputSize = fi.Size()
	}

	// Log conversion start as a single line with all metadata.
	attrs := []any{
		"input", inputDisplay,
		"output", displayPath,
		"format", reader.Format(),
	}
	if csvReader != nil && csvReader.Format() == "wide" {
		attrs = append(attrs, "payers", csvReader.PayerPlanCount())
	}
	if inputSize > 0 {
		attrs = append(attrs, "size_mb", fmt.Sprintf("%.1f", float64(inputSize)/1024/1024))
	}
	logger.Info("converting", attrs...)

	inputLabel := "CSV rows"
	if isJSON {
		inputLabel = "JSON items"
	}

	batch := make([]HospitalChargeRow, 0, batchSize)
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
			cur := totalRows + len(batch)
			logger.Debug("progress",
				"input_type", inputLabel, "input_count", inputCount,
				"parquet_rows", cur,
				"rows_per_sec", fmt.Sprintf("%.0f", float64(cur)/elapsed))
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

	// Log completion as a single line with all stats.
	doneAttrs := []any{
		"duration", elapsed.Round(time.Millisecond).String(),
		"input_type", inputLabel,
		"input_count", inputCount,
		"parquet_rows", totalRows,
		"rows_per_sec", fmt.Sprintf("%.0f", float64(totalRows)/elapsed.Seconds()),
	}
	if inputSize > 0 && outSize > 0 {
		doneAttrs = append(doneAttrs,
			"input_size_mb", fmt.Sprintf("%.1f", float64(inputSize)/1024/1024),
			"output_size_mb", fmt.Sprintf("%.1f", float64(outSize)/1024/1024),
			"compression", fmt.Sprintf("%.1fx", float64(inputSize)/float64(outSize)),
		)
	}
	logger.Info("done", doneAttrs...)

	return meta, nil
}

// buildOutputFilename builds a parquet filename from hospital metadata:
// {hospital_name}-{license_number}-{last_updated_on}.parquet
// Missing parts are omitted.
func buildOutputFilename(meta RunMeta) string {
	var parts []string

	name := sanitizeFilename(meta.HospitalName)
	if name == "" {
		name = "unknown"
	}
	parts = append(parts, name)

	if meta.LicenseNumber != nil && *meta.LicenseNumber != "" {
		parts = append(parts, sanitizeFilename(*meta.LicenseNumber))
	}

	if meta.LastUpdatedOn != "" {
		parts = append(parts, sanitizeFilename(meta.LastUpdatedOn))
	}

	return strings.Join(parts, "-") + ".parquet"
}

// sanitizeFilename replaces characters that are unsafe in filenames with
// underscores and collapses whitespace.
func sanitizeFilename(name string) string {
	var b strings.Builder
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == ' ' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	return strings.ToLower(strings.TrimSpace(strings.ReplaceAll(b.String(), " ", "_")))
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

func uploadToS3(logger *slog.Logger, ctx context.Context, localPath, s3URI string) error {
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

	logger.Debug("uploading",
		"size_mb", fmt.Sprintf("%.1f", float64(fi.Size())/1024/1024),
		"dest", s3URI)
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

	logger.Debug("uploaded", "duration", time.Since(start).Round(time.Millisecond).String())
	return nil
}

// DownloadFromS3 downloads an S3 object to a local temp file.
// Returns the temp file path and a cleanup function.
func DownloadFromS3(ctx context.Context, s3URI string) (string, func(), error) {
	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		return "", nil, err
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return "", nil, fmt.Errorf("S3 GetObject: %w", err)
	}
	defer resp.Body.Close()

	tmp, err := os.CreateTemp("", "s3-download-*.jsonl")
	if err != nil {
		return "", nil, fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", nil, fmt.Errorf("download S3 object: %w", err)
	}
	tmp.Close()

	cleanup := func() { os.Remove(tmp.Name()) }
	return tmp.Name(), cleanup, nil
}

// UploadToS3 uploads a local file to S3.
func UploadToS3(ctx context.Context, localPath, s3URI string) error {
	return uploadToS3(slog.Default(), ctx, localPath, s3URI)
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// downloadURL downloads a URL to a temp file, preserving the original file
// extension so format detection works. Returns the temp file path and a
// cleanup function that removes the temp file.
func downloadURL(logger *slog.Logger, rawURL string) (localPath string, cleanup func(), err error) {
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

	logger.Info("downloading", "url", rawURL)
	start := time.Now()

	const maxAttempts = 3
	var n int64
	var lastErr error

	for attempt := range maxAttempts {
		if attempt > 0 {
			backoff := time.Duration(1<<(attempt-1)) * 2 * time.Second // 2s, 4s
			logger.Info("retrying download", "attempt", attempt+1, "backoff", backoff.String(), "error", lastErr)
			time.Sleep(backoff)
			// Truncate the file for a fresh write.
			if err := f.Truncate(0); err != nil {
				f.Close()
				cleanupFn()
				return "", nil, fmt.Errorf("truncate temp file: %w", err)
			}
			if _, err := f.Seek(0, 0); err != nil {
				f.Close()
				cleanupFn()
				return "", nil, fmt.Errorf("seek temp file: %w", err)
			}
		}

		n, lastErr = doDownload(f, rawURL)
		if lastErr == nil {
			break
		}
	}

	if lastErr != nil {
		f.Close()
		cleanupFn()
		return "", nil, lastErr
	}

	if err := f.Close(); err != nil {
		cleanupFn()
		return "", nil, fmt.Errorf("close temp file: %w", err)
	}

	dlDuration := time.Since(start)
	dlSpeedMBs := float64(n) / 1024 / 1024 / dlDuration.Seconds()
	logger.Info("downloaded",
		"size_mb", fmt.Sprintf("%.1f", float64(n)/1024/1024),
		"duration", dlDuration.Round(time.Millisecond).String(),
		"speed_mb_s", fmt.Sprintf("%.1f", dlSpeedMBs))

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
		logger.Info("extracted",
			"file", filepath.Base(extracted),
			"size_mb", fmt.Sprintf("%.1f", float64(fileSize(extracted))/1024/1024))
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

// doDownload performs a single HTTP GET and writes the response body to w.
func doDownload(w io.Writer, rawURL string) (int64, error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	n, err := io.Copy(w, resp.Body)
	if err != nil {
		return n, fmt.Errorf("download: %w", err)
	}
	return n, nil
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
