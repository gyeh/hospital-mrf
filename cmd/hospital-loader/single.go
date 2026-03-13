package main

import (
	"log/slog"
	"os"
	"pricetool/internal"

	"github.com/spf13/cobra"
)

var singleCmd = &cobra.Command{
	Use:   "single",
	Short: "Convert a single CSV/JSON file to Parquet",
	Long: `Convert a single hospital price transparency file (CSV or JSON) to Parquet.

Examples:
  hospital-loader single --file input.csv
  hospital-loader single --file input.json --out output.parquet
  hospital-loader single --file https://example.com/charges.csv`,
	Run: func(cmd *cobra.Command, args []string) {
		file, _ := cmd.Flags().GetString("file")
		out, _ := cmd.Flags().GetString("out")
		batch, _ := cmd.Flags().GetInt("batch")
		skipPayer, _ := cmd.Flags().GetBool("skip-payer-charges")
		logPath, _ := cmd.Flags().GetString("log")
		hospitalName, _ := cmd.Flags().GetString("hospitalName")

		if file == "" {
			slog.Error("--file is required")
			cmd.Usage()
			os.Exit(1)
		}

		if err := internal.ProcessEntry(slog.Default(), file, out, logPath, batch, skipPayer, hospitalName); err != nil {
			slog.Error("conversion failed", "error", err)
			os.Exit(1)
		}

		if err := internal.GeocodeLogFile(logPath); err != nil {
			slog.Warn("geocoding failed", "error", err)
		}
	},
}

func init() {
	singleCmd.Flags().String("file", "", "Input file path or URL (required)")
	singleCmd.Flags().String("out", "", "Output Parquet file (default: derived from input)")
	singleCmd.Flags().Int("batch", 10000, "Batch size for Parquet writes")
	singleCmd.Flags().Bool("skip-payer-charges", true, "Skip payer-specific negotiated rates")
	singleCmd.Flags().String("log", "hospital-loader-log.jsonl", "JSONL log file path")
	singleCmd.Flags().String("hospitalName", "", "CMS HPT location name for log entry")
}
