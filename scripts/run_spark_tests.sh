#!/bin/bash
# ============================================================
# Full Test Suite Runner — Spark + Non-Spark (Docker)
# ============================================================
# Runs ALL project tests inside the Docker pipeline container.
# Requires Java + PySpark (provided by the bitnami/spark base image).
#
# Spark tests use local[2] mode — no external Spark cluster needed.
# Non-Spark tests (local_seeder, db_adapter, ingestion) also run.
#
# Usage (inside container):
#   /app/scripts/run_spark_tests.sh            # All tests, verbose
#   /app/scripts/run_spark_tests.sh --quiet    # CI-friendly output
#   /app/scripts/run_spark_tests.sh --help     # Show help
#
# From host (via make):
#   make test-docker
#
# From host (manual):
#   docker compose run --rm --no-deps pipeline /app/scripts/run_spark_tests.sh
# ============================================================
set -e

# ── Defaults ───────────────────────────────────────────────
QUIET=false
PYTEST_ARGS=(-v --tb=short)

# ── Flag Parsing ───────────────────────────────────────────
for arg in "$@"; do
    case "$arg" in
        --quiet|-q)
            QUIET=true
            PYTEST_ARGS=(-q --no-header --tb=line)
            ;;
        --help|-h)
            echo "Usage: $0 [--quiet] [--help]"
            echo ""
            echo "  --quiet, -q  Minimal output (CI-friendly)"
            echo "  --help, -h   Show this help"
            echo ""
            echo "Runs ALL project tests (Spark + non-Spark) inside Docker."
            echo "Requires: Java + PySpark (provided by Docker image)."
            exit 0
            ;;
        *)
            echo "Unknown flag: $arg (use --help for usage)"
            exit 2
            ;;
    esac
done

# ── Working directory ──────────────────────────────────────
cd /app

# ── Banner ─────────────────────────────────────────────────
if [ "$QUIET" = false ]; then
    echo ""
    echo "============================================================"
    echo "  FULL TEST SUITE (Spark + Non-Spark)"
    echo "  Project: K8s Microservice Failure Analysis"
    echo "============================================================"
    echo ""
    echo "Spark:      local[2] (in-process, no external cluster)"
    echo "Java:       $(java -version 2>&1 | head -1)"
    echo "PySpark:    $(python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'N/A')"
    echo ""

    # Count tests
    TOTAL=$(python -m pytest tests/ --collect-only -q 2>/dev/null | tail -1 | grep -oP '\d+(?= tests)')
    echo "Test files:"
    echo "  • tests/test_local_seeder.py          (62 tests)"
    echo "  • tests/test_db_adapter.py            (40 tests)"
    echo "  • tests/test_ingestion.py             (25 tests)"
    echo "  • tests/test_preprocessing.py         (Spark — data cleaning & features)"
    echo "  • tests/test_cross_service_analysis.py (Spark — RQ1 propagation)"
    echo "  • tests/test_failure_detection.py     (Spark — RQ2 anomaly detection)"
    echo "  • tests/test_scalability_analysis.py  (Spark — RQ3 benchmarks)"
    echo ""
fi

# ── Run ─────────────────────────────────────────────────────
set +e
python -m pytest tests/ "${PYTEST_ARGS[@]}" -p no:asyncio
EXIT_CODE=$?
set -e

# ── Summary ─────────────────────────────────────────────────
if [ "$QUIET" = false ]; then
    echo ""
    if [ "$EXIT_CODE" -eq 0 ]; then
        echo "============================================================"
        echo "  ✓ ALL TESTS PASSED"
        echo "============================================================"
    else
        echo "============================================================"
        echo "  ✗ TESTS FAILED (exit code: $EXIT_CODE)"
        echo "============================================================"
    fi
    echo ""
fi

exit $EXIT_CODE
