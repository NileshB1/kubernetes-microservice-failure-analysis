#!/bin/bash
# ============================================================
# Dataset Download Helper
# ============================================================
# Wraps the Python download script. Use this from Docker
# or any shell environment.
#
# Usage:
#   bash scripts/download_dataset.sh              # download + validate
#   bash scripts/download_dataset.sh --validate-only  # check existing
#   bash scripts/download_dataset.sh --force      # re-download
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "============================================================"
echo " Dataset Setup"
echo "============================================================"
echo ""

# Default to ./data if not set
DATA_DIR="${DATA_DIR:-./data}"

# Run the Python downloader
python scripts/download_kaggle_dataset.py \
    --data-dir "$DATA_DIR" \
    "$@"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "Dataset ready. Run the pipeline:"
    echo "  docker-compose run pipeline /app/scripts/run_pipeline.sh"
    echo "  or locally:"
    echo "  python -m modules.ingestion"
    echo ""
fi

exit $EXIT_CODE
