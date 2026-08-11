#!/bin/bash
# ============================================================
# End-to-End Pipeline Runner
# ============================================================
# Runs all six modules in sequence:
#   1. Ingestion        — generates sample data & uploads to MinIO
#   2. Preprocessing    — cleans, joins, feature-engineers via Spark
#   3. Cross-Service    — RQ1: failure propagation analysis
#   4. Failure Detection— RQ2: anomaly & failure pattern detection
#   5. Scalability      — RQ3: Spark scalability experiments
#   6. Visualization    — generates all plots
# ============================================================
set -e

# ============================================================
# Flag Parsing
# ============================================================
LOG_LEVEL="${LOG_LEVEL:-INFO}"
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --debug)
            LOG_LEVEL="DEBUG"
            ;;
        --dry-run)
            DRY_RUN=true
            ;;
        --help|-h)
            echo "Usage: $0 [--debug] [--dry-run] [--help]"
            echo ""
            echo "  --debug     Set log level to DEBUG for verbose output"
            echo "              (console shows DEBUG+, file always gets DEBUG)"
            echo "  --dry-run   Print what would be executed without running"
            echo "  --help      Show this help message"
            exit 0
            ;;
    esac
done

export LOG_LEVEL

OUTPUT_DIR="${OUTPUT_DIR:-/output}"
DATA_DIR="${DATA_DIR:-/data}"
SAMPLE_ROWS="${SAMPLE_DATA_ROWS:-100000}"
BUCKET="${MINIO_BUCKET:-microservice-logs}"

echo ""
echo "##############################################################"
echo "#  DISTRIBUTED ANALYSIS PIPELINE                             #"
echo "#  Kubernetes Microservice Logs → Failure Detection          #"
echo "##############################################################"
echo ""
echo "Configuration:"
echo "  Spark Master:    ${SPARK_MASTER_URL:-spark://spark-master:7077}"
echo "  MinIO:           ${MINIO_ENDPOINT:-http://minio:9000}"
echo "  Bucket:          ${BUCKET}"
echo "  PostgreSQL:      ${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-microservice_analysis}"
echo "  Sample Rows:     ${SAMPLE_ROWS}"
echo "  Output Dir:      ${OUTPUT_DIR}"
echo "  Log Level:       ${LOG_LEVEL}"
if [ "$DRY_RUN" = true ]; then
    echo "  Mode:            DRY-RUN (no execution)"
fi
echo ""

# ============================================================
# Module 1: Data Ingestion
# ============================================================
echo "============================================================"
echo " MODULE 1/6: DATA INGESTION"
echo "============================================================"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY-RUN] python -m modules.ingestion"
else
    python -m modules.ingestion
fi
echo ""
echo "✓ Module 1 complete."
echo ""

# ============================================================
# Module 2: Data Preprocessing
# ============================================================
echo "============================================================"
echo " MODULE 2/6: DATA PREPROCESSING"
echo "============================================================"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY-RUN] python -m modules.preprocessing"
else
    python -m modules.preprocessing
fi
echo ""
echo "✓ Module 2 complete."
echo ""

# ============================================================
# Module 3: Cross-Service Failure Analysis (RQ1)
# ============================================================
echo "============================================================"
echo " MODULE 3/6: CROSS-SERVICE FAILURE PROPAGATION (RQ1)"
echo "============================================================"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY-RUN] python -m modules.cross_service_analysis"
else
    python -m modules.cross_service_analysis
fi
echo ""
echo "✓ Module 3 complete."
echo ""

# ============================================================
# Module 4: Failure / Anomaly Detection (RQ2)
# ============================================================
echo "============================================================"
echo " MODULE 4/6: FAILURE & ANOMALY DETECTION (RQ2)"
echo "============================================================"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY-RUN] python -m modules.failure_detection"
else
    python -m modules.failure_detection
fi
echo ""
echo "✓ Module 4 complete."
echo ""

# ============================================================
# Module 5: Scalability Analysis (RQ3)
# ============================================================
echo "============================================================"
echo " MODULE 5/6: SPARK SCALABILITY ANALYSIS (RQ3)"
echo "============================================================"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY-RUN] python -m modules.scalability_analysis"
else
    python -m modules.scalability_analysis
fi
echo ""
echo "✓ Module 5 complete."
echo ""

# ============================================================
# Module 6: Visualization
# ============================================================
echo "============================================================"
echo " MODULE 6/6: RESULTS & VISUALIZATION"
echo "============================================================"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY-RUN] python -m modules.visualization"
else
    python -m modules.visualization
fi
echo ""
echo "✓ Module 6 complete."
echo ""

# ============================================================
# Summary
# ============================================================
echo ""
echo "##############################################################"
echo "#  PIPELINE COMPLETE                                         #"
echo "##############################################################"
echo ""
echo "Output files:"
ls -lh "${OUTPUT_DIR}"/*.png 2>/dev/null || echo "  (no PNG files found)"
echo ""
echo "MinIO Console:  http://localhost:9001  (minioadmin / minioadmin)"
echo "Spark UI:       http://localhost:8080"
echo "PostgreSQL:     localhost:5432  (sparkuser / sparkpass / microservice_analysis)"
echo ""
echo "Tables in PostgreSQL:"
echo "  - processed_telemetry"
echo "  - cross_service_pairs"
echo "  - propagation_chains"
echo "  - error_correlations"
echo "  - anomaly_scores"
echo "  - failure_patterns"
echo "  - scalability_metrics"
echo ""
echo "Done."
