#!/usr/bin/env bash
set -euo pipefail

LOADER="$(dirname "$0")/../hospital-loader"
MRF_DIR="$(dirname "$0")/../testdata/mrf"
S3_BUCKET="s3://hospital-mrf"

failed=0
total=0

for file in "$MRF_DIR"/*.csv "$MRF_DIR"/*.json; do
    [ -f "$file" ] || continue
    total=$((total + 1))

    basename="$(basename "$file")"
    # Sanitize filename for S3 key: strip spaces and parens
    s3_key=$(echo "$basename" | tr ' ()' '___')
    s3_key="${s3_key%.*}.parquet"

    echo "========================================"
    echo "[$total] $basename"
    echo "  -> $S3_BUCKET/$s3_key"
    echo "========================================"

    if AWS_REGION=us-east-1 "$LOADER" -file "$file" -out "$S3_BUCKET/$s3_key"; then
        echo ""
    else
        echo "FAILED: $basename"
        failed=$((failed + 1))
    fi
    echo ""
done

echo "========================================"
echo "Complete: $((total - failed))/$total succeeded"
if [ "$failed" -gt 0 ]; then
    echo "  $failed failed"
    exit 1
fi
