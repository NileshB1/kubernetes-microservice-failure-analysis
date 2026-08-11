
# Source Adapter - real telemetry to the pipeline's canonical schema


from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from modules.shared_utils import setup_logging

logger = setup_logging("source_adapter")

# Kubernetes names a pod "<deployment>-<replicaset-hash>-<pod-suffix>".
# Stripping the last two segments recovers the service name.
POD_TO_SERVICE = r"-[^-]+-[^-]+$"


#Nezha's Duration column is microseconds.
MICROS_PER_MS = 1000.0
NANOS_PER_SEC = 1_000_000_000.0

CANONICAL_DATASETS = (
    "trace_service_name", "trace_response_times",
    "trace_request_times", "resource_usage", "status_codes",
)

# Which benchmark application each capture ran against - a real property
# of the source, carried through as the namespace
CAPTURE_NAMESPACES = {"2022-08-22": "hipstershop", "2022-08-23": "hipstershop", "2023-01-29": "trainticket",
    "2023-01-30": "trainticket"}

DEFAULT_DEGRADATION_PERCENTILE = 0.99



# Reading the source
def resolve_input_paths(base_path: str, roots: tuple[str, ...], pattern: str) -> list[str]:
    """
    Turn a per-root glob into something Spark can actually read
    """
    if base_path.startswith(("s3a://", "s3://", "hdfs://")):
        return [f"{base_path.rstrip('/')}/{root}/{pattern}" for root in roots]

    from pathlib import Path

    resolved: list[str] = []
    root_dir = Path(base_path)
    for root in roots:
        resolved.extend(str(p) for p in sorted(root_dir.glob(f"{root}/{pattern}")))

    if not resolved:
        raise FileNotFoundError(
            f"No files matched {pattern!r} under {base_path} for roots {roots}. "
            "Run: python -m modules.dataset_acquisition --data-dir " + base_path
        )
    return resolved


def _capture_date_from_path() -> F.Column:
    """
    Recover the capture date from the file path Spark read each row from
    """
    return F.regexp_extract(F.input_file_name(), r"/(\d{4}-\d{2}-\d{2})/", 1)


def read_source_traces(spark: SparkSession, base_path: str, roots: tuple[str, ...]) -> DataFrame:
    """
    Read every trace CSV under the given measurement roots
    """
    paths = resolve_input_paths(base_path, roots, "*/trace/*.csv")
    logger.info("Reading traces from %d path(s)", len(paths))

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(paths)
        .withColumn("capture_date", _capture_date_from_path())
        .withColumn(
            "capture_root",
            F.when(F.input_file_name().contains("rca_data"), F.lit("fault")).otherwise(F.lit("baseline")),
        )
    )

    return (
        df.select(
            F.col("TraceID").alias("trace_id"), F.col("SpanID").alias("span_id"),
            F.col("ParentID").alias("parent_span_id"),
            F.col("PodName").alias("pod_id"),  F.regexp_replace(F.col("PodName"), POD_TO_SERVICE, "").alias("service_name"),
            F.col("OperationName").alias("endpoint"), F.col("StartTimeUnixNano").cast("double").alias("start_ns"),
            F.col("EndTimeUnixNano").cast("double").alias("end_ns"),
            (F.col("Duration").cast("double") / F.lit(MICROS_PER_MS)).alias("duration_ms"),
            "capture_date", "capture_root",
        )
        .filter(F.col("span_id").isNotNull() & F.col("duration_ms").isNotNull())
        .filter(F.col("duration_ms") >= 0)
    )


def read_source_metrics(spark: SparkSession, base_path: str, roots: tuple[str, ...]) -> DataFrame:
    """Read every per-pod resource metric CSV under the given roots."""
    paths = resolve_input_paths(base_path, roots, "*/metric/*_metric.csv")
    logger.info("Reading pod metrics from %d path(s)", len(paths))

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(paths)

    return (
        df.select(
            F.col("PodName").alias("pod_id"), F.col("TimeStamp").cast("long").alias("epoch_sec"),
            F.col("`CpuUsage(m)`").cast("double").alias("cpu_usage_mcores"), F.col("`MemoryUsage(Mi)`").cast("double").alias("memory_usage_mb"),
            F.col("NetworkReceiveBytes").cast("double").alias("network_rx_bytes"),
            F.col("NetworkTransmitBytes").cast("double").alias("network_tx_bytes"),
        )
        .filter(F.col("pod_id").isNotNull() & F.col("epoch_sec").isNotNull())
        .filter(F.col("cpu_usage_mcores") >= 0)
    )


# ------------------------------------------------------------
# Learning what "normal" looks like
# ------------------------------------------------------------
def add_self_time(traces: DataFrame) -> DataFrame:
    """
    Add exclusive (self) time: the span's own duration, minus its children's
    """
    # The grouping key is renamed before the join. Both sides otherwise
    # carry a column called parent_span_id and, because they derive from
    # the same DataFrame, Spark cannot tell the two apart - the join then
    # fails as ambiguous rather than picking one.
    child_time = (
        traces.groupBy("parent_span_id")
        .agg(F.sum("duration_ms").alias("child_duration_ms"))
        .withColumnRenamed("parent_span_id", "_child_of")
    )

    return (
        traces.join(child_time, traces["span_id"] == child_time["_child_of"], how="left")
        .drop("_child_of")
        .withColumn(
            "self_time_ms",
            F.greatest(
                F.col("duration_ms") - F.coalesce(F.col("child_duration_ms"), F.lit(0.0)),
                F.lit(0.0),
            ),
        )
        .drop("child_duration_ms")
    )


def learn_latency_baseline(
    traces: DataFrame,
    percentile: float = DEFAULT_DEGRADATION_PERCENTILE,
) -> DataFrame:
    """
    Compute a per-operation latency ceiling from the fault-free capture
    """
    baseline = traces.filter(F.col("capture_root") == "baseline")

    profile = baseline.groupBy("endpoint").agg(
        F.expr(f"percentile_approx(self_time_ms, {percentile})").alias("baseline_p99_ms"),
        F.count("*").alias("baseline_samples"),
    )

    # An operation seen only a handful of times in the baseline has no
    # trustworthy percentile; those fall back to the global ceiling.
    global_ceiling = baseline.agg(
        F.expr(f"percentile_approx(self_time_ms, {percentile})").alias("global_p99")
    ).collect()[0]["global_p99"]

    logger.info("Baseline latency ceiling (global p%.0f): %.2f ms", percentile * 100, global_ceiling or 0.0)

    return profile.withColumn(
        "baseline_p99_ms",
        F.when(
            (F.col("baseline_samples") >= 30) & F.col("baseline_p99_ms").isNotNull(),
            F.col("baseline_p99_ms")).otherwise(F.lit(global_ceiling)),
    )



# Canonical datasets

def build_canonical_datasets(
    spark: SparkSession,
    base_path: str,  roots: tuple[str, ...] = ("construct_data", "rca_data"),
    percentile: float = DEFAULT_DEGRADATION_PERCENTILE,
) -> dict[str, DataFrame]:
    """
    Map the real source into the five canonical datasets
    """
    traces = add_self_time(read_source_traces(spark, base_path, roots))
    metrics = read_source_metrics(spark, base_path, roots)

    # The baseline profile is small (one row per operation) and is joined
    # against every span, so broadcasting avoids a shuffle of the 1.5M-row side
    baseline = learn_latency_baseline(traces, percentile)
    traces = traces.join(F.broadcast(baseline), on="endpoint", how="left")

    namespace_map = F.create_map([F.lit(x) for pair in CAPTURE_NAMESPACES.items() for x in pair])

    traces = traces.withColumn(
        "is_degraded",
        F.when(F.col("baseline_p99_ms").isNotNull() & (F.col("self_time_ms") > F.col("baseline_p99_ms")),
            1).otherwise(0),
    )

    trace_service_name = traces.select(
        "trace_id", "service_name",
        "span_id", "parent_span_id",
        F.coalesce(namespace_map[F.col("capture_date")], F.lit("unknown")).alias("namespace"),
        "pod_id",
        # Nezha's trace files do not carry the node; the log files do, but
        # those are not required for the analysis, so this stays honest.
        F.lit(None).cast("string").alias("node_id"),
    )

    trace_response_times = traces.select(
        "span_id", F.col("duration_ms").alias("response_time_ms"),
        F.lit(None).cast("double").alias("wait_time_ms"),
        # Nezha reports a single span duration, not a wait/processing
        # split, so processing time is the measured duration itself.
        F.col("duration_ms").alias("processing_time_ms"),
        F.lit(None).cast("double").alias("network_latency_ms"),
    )

    trace_request_times = traces.select(
        "span_id", F.date_format(
            (F.col("start_ns") / F.lit(NANOS_PER_SEC)).cast("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"
        ).alias("start_time"),
        F.date_format(
            (F.col("end_ns") / F.lit(NANOS_PER_SEC)).cast("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"
        ).alias("end_time"),
        F.col("duration_ms"),
        # The gRPC spans that make up most of this capture carry no HTTP
        # method; leaving it NULL is truthful.
        F.lit(None).cast("string").alias("http_method"),
        "endpoint",
    )

    # A degraded span is recorded as a server-side failure so that the
    # existing analysis, which keys off status_code, sees it. 200 means the
    # span completed within its operation's fault-free envelope
    status_codes = traces.select(
        "span_id",
        F.when(F.col("is_degraded") == 1, F.lit(503)).otherwise(F.lit(200)).alias("status_code"),
        F.when(
            F.col("is_degraded") == 1,
            F.concat(
                F.lit("Self time "), F.round(F.col("self_time_ms"), 2).cast("string"),
                F.lit(" ms exceeded the fault-free p99 of "),
                F.round(F.col("baseline_p99_ms"), 2).cast("string"),
                F.lit(" ms for this operation")
            ),
        )
        .otherwise(F.lit(""))
        .alias("error_message"), F.col("is_degraded").alias("is_error"),
    )

    resource_usage = metrics.select(
        "pod_id",
        F.date_format(F.col("epoch_sec").cast("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
        "cpu_usage_mcores", "memory_usage_mb",
        F.col("network_rx_bytes").cast("int").alias("network_rx_bytes"),
        F.col("network_tx_bytes").cast("int").alias("network_tx_bytes"),
        F.lit(None).cast("int").alias("disk_io_read_bytes"),
        F.lit(None).cast("int").alias("disk_io_write_bytes"),
    )

    return {
        "trace_service_name": trace_service_name, "trace_response_times": trace_response_times,
        "trace_request_times": trace_request_times,
        "resource_usage": resource_usage, "status_codes": status_codes
    }


def build_fault_ground_truth(spark: SparkSession, labels: list[dict]) -> DataFrame:
    """
    Turn the fault list into a Spark DataFrame for evaluation
    """
    schema = (
        "capture_date string, inject_time string, inject_timestamp long, "
        "inject_pod string, inject_type string"
    )
    if not labels:
        logger.warning("No ground-truth fault labels available; evaluation will be skipped.")
        return spark.createDataFrame([], schema)

    df = spark.createDataFrame(labels, schema)
    return df.withColumn("inject_service", F.regexp_replace(F.col("inject_pod"), POD_TO_SERVICE, ""))


def write_canonical_datasets(datasets: dict[str, DataFrame], output_path: str) -> dict[str, int]:
    """
    Write the canonical datasets as CSV where the pipeline expects them
    """
    counts: dict[str, int] = {}
    for name, df in datasets.items():
        destination = f"{output_path.rstrip('/')}/{name}.csv"
        logger.info("Writing %s ....", destination)
        (df.coalesce(1).write.mode("overwrite").option("header", "true").csv(destination))
        counts[name] = df.count()
        logger.info("  %s: %s rows", name, f"{counts[name]:,}")
    return counts
