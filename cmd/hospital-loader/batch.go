package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

type jsonlEntry struct {
	LocationName  string `json:"location-name"`
	MRFUrl        string `json:"mrf-url"`
	SourcePageURL string `json:"source-page-url"`
}

var batchCmd = &cobra.Command{
	Use:   "batch",
	Short: "Process multiple hospitals from a JSONL file",
	Long: `Read a JSONL file containing hospital entries and convert each one to Parquet.

Each line in the JSONL file should have "mrf-url" and "location-name" fields.

Examples:
  hospital-loader batch --input cms-hpt.jsonl
  hospital-loader batch --input cms-hpt.jsonl --limit 5 --out-dir output/`,
	Run: func(cmd *cobra.Command, args []string) {
		input, _ := cmd.Flags().GetString("input")
		limit, _ := cmd.Flags().GetInt("limit")
		outDir, _ := cmd.Flags().GetString("out-dir")
		logPath, _ := cmd.Flags().GetString("log")
		batch, _ := cmd.Flags().GetInt("batch")
		skipPayer, _ := cmd.Flags().GetBool("skip-payer-charges")
		parallel, _ := cmd.Flags().GetInt("parallel")

		entries, err := readJSONL(input, limit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", input, err)
			os.Exit(1)
		}

		if err := os.MkdirAll(outDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
			os.Exit(1)
		}

		// Deduplicate entries by URL.
		seenURLs := make(map[string]string) // url -> first location-name
		var unique []jsonlEntry
		var duplicates int
		for _, entry := range entries {
			if entry.MRFUrl == "" {
				unique = append(unique, entry)
				continue
			}
			if firstName, ok := seenURLs[entry.MRFUrl]; ok {
				name := entry.LocationName
				if name == "" {
					name = "unknown"
				}
				fmt.Printf("SKIP %s: duplicate URL (same as %s)\n", name, firstName)
				duplicates++
				continue
			}
			seenURLs[entry.MRFUrl] = entry.LocationName
			unique = append(unique, entry)
		}
		if duplicates > 0 {
			fmt.Println()
		}

		fmt.Printf("Loaded %d entries from %s (%d duplicates removed)\n", len(unique), input, duplicates)
		fmt.Printf("Output dir: %s\n", outDir)
		fmt.Printf("Log file:   %s\n", logPath)
		if parallel > 1 {
			fmt.Printf("Parallel:   %d workers\n", parallel)
		}
		fmt.Println()

		var succeeded, failed atomic.Int64

		if parallel <= 1 {
			// Sequential processing.
			for i, entry := range unique {
				ok := processBatchEntry(entry, i, len(unique), outDir, logPath, batch, skipPayer)
				if ok {
					succeeded.Add(1)
				} else {
					failed.Add(1)
				}
			}
		} else {
			// Parallel processing with worker pool.
			type work struct {
				entry jsonlEntry
				index int
			}
			ch := make(chan work)
			var wg sync.WaitGroup

			for range parallel {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for w := range ch {
						ok := processBatchEntry(w.entry, w.index, len(unique), outDir, logPath, batch, skipPayer)
						if ok {
							succeeded.Add(1)
						} else {
							failed.Add(1)
						}
					}
				}()
			}

			for i, entry := range unique {
				ch <- work{entry: entry, index: i}
			}
			close(ch)
			wg.Wait()
		}

		s, f := succeeded.Load(), failed.Load()
		fmt.Printf("Done: %d succeeded, %d failed, %d duplicates skipped out of %d total\n",
			s, f, duplicates, int64(len(unique))+int64(duplicates))
		if f > 0 {
			os.Exit(1)
		}
	},
}

func init() {
	batchCmd.Flags().String("input", "cms-hpt.jsonl", "JSONL file with hospital entries")
	batchCmd.Flags().Int("limit", 0, "Max entries to process (0 = all)")
	batchCmd.Flags().String("out-dir", "output", "Output directory for Parquet files")
	defaultLog := fmt.Sprintf("hospital-loader-log-%s.jsonl", time.Now().Format("20060102-150405"))
	batchCmd.Flags().String("log", defaultLog, "JSONL log file path")
	batchCmd.Flags().Int("batch", 10000, "Batch size for Parquet writes")
	batchCmd.Flags().Bool("skip-payer-charges", true, "Skip payer-specific negotiated rates")
	defaultParallel := runtime.NumCPU() - 1
	if defaultParallel < 1 {
		defaultParallel = 1
	}
	batchCmd.Flags().Int("parallel", defaultParallel, "Number of parallel workers")
}

// processBatchEntry processes a single entry and prints status. Returns true on success.
func processBatchEntry(entry jsonlEntry, index, total int, outDir, logPath string, batchSize int, skipPayer bool) bool {
	name := entry.LocationName
	if name == "" {
		name = "unknown"
	}
	url := entry.MRFUrl

	if url == "" {
		fmt.Printf("[%d/%d] SKIP %s: no mrf-url\n\n", index+1, total, name)
		return false
	}

	fmt.Printf("[%d/%d] %s\n  URL: %s\n", index+1, total, name, url)

	safeName := sanitizeFilename(name)
	outPath := filepath.Join(outDir, safeName+".parquet")

	err := processEntry(url, outPath, logPath, batchSize, skipPayer)
	if err == nil {
		fi, statErr := os.Stat(outPath)
		if statErr == nil {
			fmt.Printf("  OK (%.1f MB)\n\n", float64(fi.Size())/1024/1024)
		} else {
			fmt.Printf("  OK\n\n")
		}
		return true
	}

	fmt.Printf("  FAILED: %v\n\n", err)
	return false
}

func readJSONL(path string, limit int) ([]jsonlEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []jsonlEntry
	scanner := bufio.NewScanner(f)
	// Allow large lines (some JSONL entries can be long)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry jsonlEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("parse JSONL line: %w", err)
		}
		entries = append(entries, entry)
		if limit > 0 && len(entries) >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

// sanitizeFilename replaces non-alphanumeric characters (except - _ and space)
// with underscores, then replaces spaces with underscores.
func sanitizeFilename(name string) string {
	var b strings.Builder
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == ' ' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	return strings.TrimSpace(strings.ReplaceAll(b.String(), " ", "_"))
}
