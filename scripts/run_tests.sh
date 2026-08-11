#!/bin/bash
# ============================================================
# CI-Friendly Test Runner — Non-Spark Tests
# ============================================================
# Runs the 127 non-Spark unit tests that don't require Java,
# PySpark, Docker, or any external services. Suitable for:
#   - GitHub Actions / CI pipelines
#   - Local pre-commit checks
#   - Quick smoke tests before Docker builds
#
# Usage:
#   ./scripts/run_tests.sh              # Run all non-Spark tests
#   ./scripts/run_tests.sh --coverage   # Run with coverage report
#   ./scripts/run_tests.sh --quiet      # Minimal output (CI log-friendly)
#   ./scripts/run_tests.sh --help       # Show this help
#
# Test files run (127 tests total):
#   tests/test_local_seeder.py   —  62 tests (SQLite seeder schema & data)
#   tests/test_db_adapter.py     —  40 tests (DB adapter SQLite + PG mocks)
#   tests/test_ingestion.py      —  25 tests (CSV generation, config, MinIO)
# ============================================================
set -e

# ── Defaults ───────────────────────────────────────────────
COVERAGE=false
QUIET=false
PYTEST_ARGS=()

# ── Flag Parsing ───────────────────────────────────────────
for arg in "$@"; do
    case "$arg" in
        --coverage)
            COVERAGE=true
            ;;
        --quiet|-q)
            QUIET=true
            ;;
        --help|-h)
            echo "Usage: $0 [--coverage] [--quiet] [--help]"
            echo ""
            echo "  --coverage   Generate HTML + terminal coverage report"
            echo "  --quiet, -q  Minimal output (pass/fail only, CI-friendly)"
            echo "  --help, -h   Show this help message"
            echo ""
            echo "Runs 127 non-Spark tests across 3 test files."
            echo "No Java, PySpark, Docker, or PostgreSQL required."
            echo ""
            echo "For full coverage including Spark tests, use:"
            echo "  make test-docker"
            exit 0
            ;;
        *)
            echo "Unknown flag: $arg (use --help for usage)"
            exit 2
            ;;
    esac
done

# ── Locate project root ────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ── Banner ─────────────────────────────────────────────────
if [ "$QUIET" = false ]; then
    echo ""
    echo "============================================================"
    echo "  NON-SPARK TEST SUITE"
    echo "  Project: K8s Microservice Failure Analysis"
    echo "============================================================"
    echo ""
    echo "Test files:"
    echo "  • tests/test_local_seeder.py   (62 tests)"
    echo "  • tests/test_db_adapter.py     (40 tests)"
    echo "  • tests/test_ingestion.py      (25 tests)"
    echo "  ─────────────────────────────"
    echo "  Total:                         127 tests"
    echo ""
fi

# ── Build pytest args ──────────────────────────────────────
if [ "$QUIET" = true ]; then
    PYTEST_ARGS+=(-q --no-header --tb=line)
else
    PYTEST_ARGS+=(-v --tb=short)
fi

# Coverage
if [ "$COVERAGE" = true ]; then
    PYTEST_ARGS+=(--cov=modules --cov-report=term-missing --cov-report=html)
    if [ "$QUIET" = false ]; then
        echo "Coverage: enabled (terminal + htmlcov/)"
        echo ""
    fi
fi

# ── Run ─────────────────────────────────────────────────────
# Explicit file list avoids collecting Spark test files,
# which would hang trying to import pyspark.
set +e  # capture exit code manually
python -m pytest \
    tests/test_local_seeder.py \
    tests/test_db_adapter.py \
    tests/test_ingestion.py \
    "${PYTEST_ARGS[@]}"

EXIT_CODE=$?
set -e

# ── Summary ─────────────────────────────────────────────────
if [ "$QUIET" = false ]; then
    echo ""
    if [ "$EXIT_CODE" -eq 0 ]; then
        echo "============================================================"
        echo "  ✓ ALL 127 TESTS PASSED"
        echo "============================================================"
    else
        echo "============================================================"
        echo "  ✗ TESTS FAILED (exit code: $EXIT_CODE)"
        echo "============================================================"
    fi
    echo ""

    if [ "$COVERAGE" = true ] && [ "$EXIT_CODE" -eq 0 ]; then
        echo "Coverage report: htmlcov/index.html"
        echo ""
    fi
fi

exit $EXIT_CODE
