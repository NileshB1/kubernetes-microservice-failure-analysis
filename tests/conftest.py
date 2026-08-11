# ============================================================
# Pytest Shared Fixtures — conftest.py
# ============================================================
# Provides:
#   - spark:           Local SparkSession (master="local[2]")
#   - sample_raw_dfs:  Dict of 5 DataFrames simulating raw MinIO CSV reads
#   - unified_df:      Joined & feature-engineered DataFrame (output of
#                      preprocessing.join_datasets + engineer_features)
#   - timeseries_df:   Output of failure_detection.compute_error_rate_timeseries
# ============================================================

import os
import sys
import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F


# ============================================================
# Ensure PySpark uses the correct Python executable on all platforms
# ============================================================
def _configure_pyspark_env():
    """Set environment variables so PySpark workers find the right Python, Java, and Spark home."""
    # Use the current Python executable so Spark workers use the same interpreter
    python_exe = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exe)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exe)

    # Override stale SPARK_HOME if it's missing or points to a non-existent path.
    # Use the PySpark package directory which bundles its own Spark distribution.
    import pyspark
    pyspark_home = os.path.dirname(pyspark.__file__)
    current_spark_home = os.environ.get("SPARK_HOME", "")
    if not current_spark_home or not os.path.isdir(current_spark_home):
        os.environ["SPARK_HOME"] = pyspark_home

    # Ensure JAVA_HOME is set so PySpark can launch the JVM gateway.
    # On Windows, PySpark's java_gateway spawns a 'java' subprocess and needs
    # it on PATH or JAVA_HOME set.  We search common locations.
    if "JAVA_HOME" not in os.environ:
        candidates = [
            "C:/Program Files/Java/jdk-17",
            "C:/Program Files/Java/jdk-21",
            "C:/Program Files/Java/jdk-11",
            "C:/Program Files/Common Files/Oracle/Java/javapath",
        ]
        for candidate in candidates:
            java_exe = os.path.join(candidate, "bin", "java.exe")
            if os.path.isfile(java_exe):
                os.environ["JAVA_HOME"] = candidate
                break
    # Also ensure Java 'bin' is on PATH so subprocess.Popen can find it
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home:
        java_bin = os.path.join(java_home, "bin")
        existing_path = os.environ.get("PATH", "")
        if java_bin not in existing_path:
            os.environ["PATH"] = java_bin + os.pathsep + existing_path


_configure_pyspark_env()


# ============================================================
# Spark Session Fixture
# ============================================================
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped Spark session using local mode with 2 threads.
    All tests share this single session for speed.

    Configures Python and Java paths explicitly so Spark workers
    and the JVM gateway find the correct interpreters on all platforms.
    """
    python_exe = sys.executable

    # Re-assert JAVA_HOME inside the fixture (pytest may run with a
    # different environment than module-import time).
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        java_bin = os.path.join(java_home, "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-spark")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        # Explicit Python path for Spark workers
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        # Python 3.14 + PySpark 4.x compat: disable daemon worker to avoid
        # EOFException crashes on Windows.
        .config("spark.pyspark.use.daemon", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ============================================================
# Sample Raw DataFrames (matching the 5-CSV schema)
# ============================================================
@pytest.fixture(scope="session")
def sample_raw_dfs(spark) -> dict[str, DataFrame]:
    """
    Build 5 DataFrames matching the raw CSV schemas from ingestion.
    Uses known, deterministic values for predictable assertions.

    Returns:
      dict with keys: trace_service_name, trace_response_times,
                      trace_request_times, resource_usage, status_codes
    """
    # --- trace_service_name ---
    service_rows = [
        Row(trace_id="trace-001", service_name="frontend", span_id="span-001",
            parent_span_id="span-000", namespace="production", pod_id="pod-00001",
            node_id="node-001"),
        Row(trace_id="trace-001", service_name="auth-service", span_id="span-002",
            parent_span_id="span-001", namespace="production", pod_id="pod-00002",
            node_id="node-002"),
        Row(trace_id="trace-001", service_name="user-service", span_id="span-003",
            parent_span_id="span-002", namespace="production", pod_id="pod-00003",
            node_id="node-003"),
        Row(trace_id="trace-002", service_name="frontend", span_id="span-004",
            parent_span_id="span-003", namespace="staging", pod_id="pod-00001",
            node_id="node-001"),
        # Duplicate span_id (should be deduped)
        Row(trace_id="trace-002", service_name="frontend", span_id="span-004",
            parent_span_id="span-003", namespace="staging", pod_id="pod-00001",
            node_id="node-001"),
        # Missing parent_span_id (should be filtered out)
        Row(trace_id="trace-003", service_name="payment-service", span_id="span-005",
            parent_span_id="", namespace="production", pod_id="pod-00004",
            node_id="node-004"),
        # Null span_id (should be filtered out)
        Row(trace_id="trace-004", service_name="cart-service", span_id=None,
            parent_span_id="span-006", namespace="canary", pod_id="pod-00005",
            node_id="node-005"),
    ]
    service_df = spark.createDataFrame(service_rows)

    # --- trace_response_times ---
    response_rows = [
        Row(span_id="span-001", response_time_ms=45.5, wait_time_ms=10.0,
            processing_time_ms=30.0, network_latency_ms=5.5),
        Row(span_id="span-002", response_time_ms=2500.0, wait_time_ms=200.0,
            processing_time_ms=2200.0, network_latency_ms=100.0),
        Row(span_id="span-003", response_time_ms=120.0, wait_time_ms=20.0,
            processing_time_ms=90.0, network_latency_ms=10.0),
        Row(span_id="span-004", response_time_ms=80.0, wait_time_ms=15.0,
            processing_time_ms=60.0, network_latency_ms=5.0),
        # Negative response_time (should be filtered out)
        Row(span_id="span-005", response_time_ms=-5.0, wait_time_ms=1.0,
            processing_time_ms=-2.0, network_latency_ms=0.0),
    ]
    response_df = spark.createDataFrame(response_rows)

    # --- trace_request_times ---
    ts_base = datetime(2024, 1, 1, 10, 0, 0)
    request_rows = [
        Row(span_id="span-001", start_time=(ts_base).isoformat(),
            end_time=(ts_base + timedelta(seconds=0.045)).isoformat(),
            duration_ms=45.0, http_method="GET", endpoint="/api/v1/frontend/get"),
        Row(span_id="span-002", start_time=(ts_base + timedelta(seconds=1)).isoformat(),
            end_time=(ts_base + timedelta(seconds=3)).isoformat(),
            duration_ms=2000.0, http_method="POST", endpoint="/api/v1/auth-service/login"),
        Row(span_id="span-003", start_time=(ts_base + timedelta(seconds=2)).isoformat(),
            end_time=(ts_base + timedelta(seconds=2, milliseconds=120)).isoformat(),
            duration_ms=120.0, http_method="GET", endpoint="/api/v1/user-service/profile"),
        Row(span_id="span-004", start_time=(ts_base + timedelta(seconds=3)).isoformat(),
            end_time=(ts_base + timedelta(seconds=3, milliseconds=80)).isoformat(),
            duration_ms=80.0, http_method="GET", endpoint="/api/v1/frontend/status"),
        # Null start_time (should be filtered)
        Row(span_id="span-005", start_time=None, end_time=None,
            duration_ms=0.0, http_method="DELETE", endpoint="/api/v1/admin/cleanup"),
    ]
    request_df = spark.createDataFrame(request_rows)

    # --- resource_usage ---
    resource_rows = [
        Row(pod_id="pod-00001", timestamp=ts_base.isoformat(),
            cpu_usage_mcores=350.0, memory_usage_mb=512.0,
            network_rx_bytes=1000000, network_tx_bytes=500000,
            disk_io_read_bytes=100000, disk_io_write_bytes=50000),
        Row(pod_id="pod-00002", timestamp=ts_base.isoformat(),
            cpu_usage_mcores=800.0, memory_usage_mb=1024.0,
            network_rx_bytes=2000000, network_tx_bytes=1000000,
            disk_io_read_bytes=200000, disk_io_write_bytes=100000),
        Row(pod_id="pod-00003", timestamp=ts_base.isoformat(),
            cpu_usage_mcores=200.0, memory_usage_mb=256.0,
            network_rx_bytes=500000, network_tx_bytes=250000,
            disk_io_read_bytes=50000, disk_io_write_bytes=25000),
        # Negative CPU (should be filtered)
        Row(pod_id="pod-00004", timestamp=ts_base.isoformat(),
            cpu_usage_mcores=-10.0, memory_usage_mb=100.0,
            network_rx_bytes=0, network_tx_bytes=0,
            disk_io_read_bytes=0, disk_io_write_bytes=0),
        # Duplicate pod+timestamp (should be deduped)
        Row(pod_id="pod-00001", timestamp=ts_base.isoformat(),
            cpu_usage_mcores=360.0, memory_usage_mb=520.0,
            network_rx_bytes=1100000, network_tx_bytes=550000,
            disk_io_read_bytes=110000, disk_io_write_bytes=55000),
    ]
    resource_df = spark.createDataFrame(resource_rows)

    # --- status_codes ---
    status_rows = [
        Row(span_id="span-001", status_code=200, error_message="", is_error=0),
        Row(span_id="span-002", status_code=500, error_message="Internal server error", is_error=1),
        Row(span_id="span-003", status_code=200, error_message="", is_error=0),
        Row(span_id="span-004", status_code=503, error_message="Service unavailable", is_error=1),
        # Duplicate span_id (should be deduped)
        Row(span_id="span-002", status_code=500, error_message="Internal server error", is_error=1),
    ]
    status_df = spark.createDataFrame(status_rows)

    return {
        "trace_service_name": service_df,
        "trace_response_times": response_df,
        "trace_request_times": request_df,
        "resource_usage": resource_df,
        "status_codes": status_df,
    }


# ============================================================
# Unified DataFrame (post-preprocessing, for failure_detection tests)
# ============================================================
@pytest.fixture(scope="session")
def unified_df(spark) -> DataFrame:
    """
    Build a small but realistic unified telemetry DataFrame with
    engineered features already applied.

    Contains 10 rows across 3 services over a 30-minute window,
    with known failure/latency patterns for predictable assertions.
    """
    ts_base = datetime(2024, 1, 1, 10, 0, 0)
    rows = [
        # service=frontend — normal behaviour
        {"span_id": "u-001", "trace_id": "t-001", "service_name": "frontend",
         "start_time_ts": ts_base,
         "response_time_ms": 45.0, "status_code": 200,
         "is_failure": 0, "is_latency_spike": 0,
         "cpu_usage_mcores": 300.0, "memory_usage_mb": 512.0},
        {"span_id": "u-002", "trace_id": "t-002", "service_name": "frontend",
         "start_time_ts": ts_base + timedelta(minutes=5),
         "response_time_ms": 55.0, "status_code": 200,
         "is_failure": 0, "is_latency_spike": 0,
         "cpu_usage_mcores": 320.0, "memory_usage_mb": 520.0},
        {"span_id": "u-003", "trace_id": "t-003", "service_name": "frontend",
         "start_time_ts": ts_base + timedelta(minutes=10),
         "response_time_ms": 2100.0, "status_code": 500,
         "is_failure": 1, "is_latency_spike": 1,
         "cpu_usage_mcores": 950.0, "memory_usage_mb": 1024.0},
        {"span_id": "u-004", "trace_id": "t-004", "service_name": "frontend",
         "start_time_ts": ts_base + timedelta(minutes=15),
         "response_time_ms": 60.0, "status_code": 200,
         "is_failure": 0, "is_latency_spike": 0,
         "cpu_usage_mcores": 310.0, "memory_usage_mb": 515.0},

        # service=auth-service — mostly failures
        {"span_id": "u-005", "trace_id": "t-005", "service_name": "auth-service",
         "start_time_ts": ts_base + timedelta(minutes=2),
         "response_time_ms": 3000.0, "status_code": 502,
         "is_failure": 1, "is_latency_spike": 1,
         "cpu_usage_mcores": 1200.0, "memory_usage_mb": 2048.0},
        {"span_id": "u-006", "trace_id": "t-006", "service_name": "auth-service",
         "start_time_ts": ts_base + timedelta(minutes=7),
         "response_time_ms": 3200.0, "status_code": 503,
         "is_failure": 1, "is_latency_spike": 1,
         "cpu_usage_mcores": 1300.0, "memory_usage_mb": 2100.0},
        {"span_id": "u-007", "trace_id": "t-007", "service_name": "auth-service",
         "start_time_ts": ts_base + timedelta(minutes=12),
         "response_time_ms": 3500.0, "status_code": 500,
         "is_failure": 1, "is_latency_spike": 1,
         "cpu_usage_mcores": 1400.0, "memory_usage_mb": 2200.0},

        # service=user-service — mixed
        {"span_id": "u-008", "trace_id": "t-008", "service_name": "user-service",
         "start_time_ts": ts_base + timedelta(minutes=3),
         "response_time_ms": 90.0, "status_code": 200,
         "is_failure": 0, "is_latency_spike": 0,
         "cpu_usage_mcores": 250.0, "memory_usage_mb": 300.0},
        {"span_id": "u-009", "trace_id": "t-009", "service_name": "user-service",
         "start_time_ts": ts_base + timedelta(minutes=8),
         "response_time_ms": 150.0, "status_code": 404,
         "is_failure": 0, "is_latency_spike": 0,
         "cpu_usage_mcores": 260.0, "memory_usage_mb": 310.0},
        {"span_id": "u-010", "trace_id": "t-010", "service_name": "user-service",
         "start_time_ts": ts_base + timedelta(minutes=13),
         "response_time_ms": 2500.0, "status_code": 500,
         "is_failure": 1, "is_latency_spike": 1,
         "cpu_usage_mcores": 900.0, "memory_usage_mb": 1500.0},
    ]
    return spark.createDataFrame([Row(**r) for r in rows])


# ============================================================
# Time Series DataFrame (for anomaly detection tests)
# ============================================================
@pytest.fixture(scope="session")
def timeseries_df(spark, unified_df) -> DataFrame:
    """
    Pre-computed time series from unified_df using 5-minute windows.
    This fixture avoids re-computing the timeseries for every test.
    """
    from modules.failure_detection import compute_error_rate_timeseries
    return compute_error_rate_timeseries(unified_df, window_minutes=5)
