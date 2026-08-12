#!/bin/bash

# CI-Friendly Test Runner - Non-Spark Tests


# Defaults
COVERAGE=false
QUIET=false
PYTEST_ARGS=()

#Flag Parsing
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
            echo " --coverage   Generate HTML + terminal coverage report"
            echo " --quiet, -q  Minimal output (pass/fail only, CI-friendly)"
            echo " --help, -h   Show this help message"
            echo "Runs the non-Spark test suite (no Java, Spark, Docker, or PostgreSQL)."
            echo "For full coverage including Spark tests, use:"
            echo " make test-docker"
            exit 0
            ;;
        *)
            echo "Unknown flag: $arg (use --help for usage)"
            exit 2
            ;;
    esac
done

#locate project roo
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"


if [ "$QUIET" = false ]; then

    echo " NON-SPARK TEST SUITE "
    echo "Project: K8s Microservice Failure Analysis"

    echo ""
    echo "Test files:"
    echo " - tests/test_local_seeder.py"
    echo " - tests/test_db_adapter.py "
    echo "tests/test_db_adapter_resilience.py"
    echo "  -tests/test_settings.py"
    echo "- tests/test_ingestion.py"
    echo "- tests/test_report_generator.py"
    echo " - tests/test_dataset_acquisition.py"
    echo ""
fi


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

#run

python -m pytest \
    tests/test_local_seeder.py \
    tests/test_db_adapter.py \
    tests/test_db_adapter_resilience.py \
    tests/test_settings.py \
    tests/test_ingestion.py \
    tests/test_report_generator.py \
    tests/test_dataset_acquisition.py \
    "${PYTEST_ARGS[@]}"

EXIT_CODE=$?
set -e


if [ "$QUIET" = false ]; then
    echo ""
    if [ "$EXIT_CODE" -eq 0 ]; then
        echo " All NON-SPARK TESTS PASSED"
    else
        echo " Some test cases failed....."
    fi
    echo ""

    if [ "$COVERAGE" = true ] && [ "$EXIT_CODE" -eq 0 ]; then
        echo "Coverage report: htmlcov/index.html"
        echo ""
    fi
fi

exit $EXIT_CODE
