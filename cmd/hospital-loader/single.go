package main

import (
	"fmt"
	"os"

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

		if file == "" {
			fmt.Fprintln(os.Stderr, "Error: --file is required")
			cmd.Usage()
			os.Exit(1)
		}

		if err := processEntry(file, out, logPath, batch, skipPayer); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if err := geocodeLogFile(logPath); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: geocoding failed: %v\n", err)
		}
	},
}

func init() {
	singleCmd.Flags().String("file", "", "Input file path or URL (required)")
	singleCmd.Flags().String("out", "", "Output Parquet file (default: derived from input)")
	singleCmd.Flags().Int("batch", 10000, "Batch size for Parquet writes")
	singleCmd.Flags().Bool("skip-payer-charges", true, "Skip payer-specific negotiated rates")
	singleCmd.Flags().String("log", "hospital-loader-log.jsonl", "JSONL log file path")
}
