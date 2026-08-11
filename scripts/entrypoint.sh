#!/bin/bash
# ============================================================
# Docker Entrypoint
# ============================================================
set -e

echo "============================================================"
echo " Distributed Analysis of Kubernetes Microservice Logs"
echo " Docker Container — Ready"
echo "============================================================"
echo ""

# Wait for services to be ready (using bash built-ins only)
echo "Waiting for Spark Master at ${SPARK_MASTER_URL:-spark://spark-master:7077}..."
for i in $(seq 1 30); do
    if timeout 2 bash -c "echo > /dev/tcp/spark-master/7077" 2>/dev/null; then
        echo "  ✓ Spark Master is ready"
        break
    fi
    echo "  ... waiting ($i/30)"
    sleep 3
done

echo "Waiting for PostgreSQL..."
for i in $(seq 1 30); do
    if pg_isready -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-sparkuser}" 2>/dev/null; then
        echo "  ✓ PostgreSQL is ready"
        break
    fi
    echo "  ... waiting ($i/30)"
    sleep 3
done

echo "Waiting for MinIO..."
for i in $(seq 1 20); do
    if timeout 2 bash -c "echo > /dev/tcp/minio/9000" 2>/dev/null; then
        echo "  ✓ MinIO is ready"
        break
    fi
    echo "  ... waiting ($i/20)"
    sleep 2
done

echo ""
echo "All services ready. Executing command: $@"
echo ""

exec "$@"
