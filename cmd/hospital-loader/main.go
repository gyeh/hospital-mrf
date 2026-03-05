package main

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "hospital-loader",
	Short: "Convert hospital price transparency files to Parquet",
	Long: `hospital-loader converts CMS Machine-Readable Format files (CSV/JSON)
into query-optimized Parquet files.

Use "hospital-loader single" to convert a single file, or
"hospital-loader batch" to process multiple hospitals from a JSONL file.`,
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	rootCmd.AddCommand(singleCmd)
	rootCmd.AddCommand(batchCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
