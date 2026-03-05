package main

import (
	"context"
	"log/slog"
	"os"
	"pricetool/internal"
	"strings"

	"github.com/spf13/cobra"
)

var geocodeCmd = &cobra.Command{
	Use:   "geocode",
	Short: "Geocode hospital addresses in a JSONL log file",
	Long: `Geocode hospital addresses found in a JSONL log file using the US Census
batch geocoder. The log file can be a local path or an S3 URI.

Examples:
  hospital-loader geocode --log hospital-loader-log.jsonl
  hospital-loader geocode --log s3://hospital-mrf/logs/run.jsonl`,
	Run: func(cmd *cobra.Command, args []string) {
		logPath, _ := cmd.Flags().GetString("log")

		if logPath == "" {
			slog.Error("--log is required")
			cmd.Usage()
			os.Exit(1)
		}

		if strings.HasPrefix(logPath, "s3://") {
			geocodeS3(logPath)
		} else {
			geocodeLocal(logPath)
		}
	},
}

func init() {
	geocodeCmd.Flags().String("log", "", "JSONL log file path or S3 URI (required)")
}

func geocodeLocal(logPath string) {
	if err := internal.GeocodeLogFile(logPath); err != nil {
		slog.Error("geocoding failed", "error", err)
		os.Exit(1)
	}
	slog.Info("geocoding complete", "file", logPath)
}

func geocodeS3(s3URI string) {
	ctx := context.Background()

	slog.Info("downloading log file from S3", "uri", s3URI)
	localPath, cleanup, err := internal.DownloadFromS3(ctx, s3URI)
	if err != nil {
		slog.Error("failed to download from S3", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	if err := internal.GeocodeLogFile(localPath); err != nil {
		slog.Error("geocoding failed", "error", err)
		os.Exit(1)
	}

	slog.Info("uploading geocoded log file to S3", "uri", s3URI)
	if err := internal.UploadToS3(ctx, localPath, s3URI); err != nil {
		slog.Error("failed to upload to S3", "error", err)
		os.Exit(1)
	}

	slog.Info("geocoding complete", "file", s3URI)
}
