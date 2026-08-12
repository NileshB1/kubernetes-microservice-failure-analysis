#!/bin/bash

# Full Test Suite Runner - Spark + Non-Spark (Docker)

set -e

# -- Defaults -----------------------------------------------
QUIET=false
PYTEST_ARGS=(-v --tb=short)

# -- Flag Parsing -------------------------------------------
for arg in "$@"; do
    case "$arg" in
        --quiet|-q)
            QUIET=true
            PYTEST_ARGS=(-q --no-header --tb=line)
            ;;
        --help|-h)
            echo "Usage: $0 [--quiet] [--help]"
            echo " --quiet, -q  Minimal output (CI-friendly)"
            echo " --help, -h   Show this help"
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

# --Working directory 
cd /app

#-- Banner
if [ "$QUIET" = false ]; then
    echo ""
    
    echo " FULL TEST SUITE (Spark + Non-Spark)"
    echo " Project: K8s Microservice Failure Analysis"
    echo "Spark: local[2] (in-process, no external cluster)"
    echo "Java: $(java -version 2>&1 | head -1)"
    echo "PySpark: $(python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'N/A')"
    echo ""

    # Discover rather than hard-code, so this stays correct as files are added.
    echo "Collecting:"
    python -m pytest tests/ --collect-only -q 2>/dev/null | tail -1
    echo ""
fi

# --Run
set +e
python -m pytest tests/ "${PYTEST_ARGS[@]}" -p no:asyncio
EXIT_CODE=$?
set -e

# -- Summary
if [ "$QUIET" = false ]; then
    echo ""
    if [ "$EXIT_CODE" -eq 0 ]; then
        echo "All TESTS PASSED"
    else
        echo "Test failed...."
    fi
fi

exit $EXIT_CODE
