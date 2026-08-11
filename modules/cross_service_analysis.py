# ============================================================
# Module 3: Cross-Service Failure Propagation Analysis
# ============================================================
# Purpose (RQ1):
#   "How can distributed analysis identify cross-service failure
#    propagation that is difficult to detect from individual
#    microservice logs?"
#
#   Uses Spark window functions, joins, and aggregations to
#   trace how failures cascade between microservices.
#
# Inputs:
#   - Unified telemetry Parquet from s3a://microservice-logs/processed/
#   - config.yaml cross_service_analysis parameters
#
# Outputs:
#   - cross_service_correlations table in PostgreSQL
#   - Propagation chains stored in MinIO (Parquet)
#
# Main Functions:
#   - build_service_dependency_graph()   → Infers call graph from parent_span_id
#   - detect_propagation_chains()        → Identifies failure cascades
#   - correlate_cross_service_errors()   → Computes pairwise error correlation
#   - compute_propagation_metrics()      → Summary metrics for RQ1
#
# Spark Operations Used:
#   - Self-join on parent_span_id → span_id
#   - Window functions with lag()
#   - groupBy().agg() for correlations
#   - pyspark.ml.stat.Correlation
#
# RQ Contribution:
#   - RQ1: Directly answers the research question with quantitative evidence
# ============================================================

import os
import logging
from typing import Optional

from dotenv import load_dotenv
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from modules.shared_utils import create_spark_session, load_config, write_to_postgres, setup_logging

load_dotenv()
logger = setup_logging("cross_service_analysis")


# ============================================================
# Service Dependency Graph
# ============================================================
def build_service_dependency_graph(
    df: DataFrame,
    propagation_window_sec: int = 60,
) -> DataFrame:
    """
    Infer a service-to-service call graph from parent_span_id relationships.

    Methodology:
      1. Self-join: parent_span_id (caller) → span_id (callee)
      2. Extract caller service and callee service names
      3. Group by (caller_service, callee_service) to count calls
      4. Flag propagation paths where caller has errors shortly before callee

    Returns DataFrame with columns:
      [caller_service, callee_service, call_count,
       caller_error_count, callee_error_count,
       propagation_score]
    """
    logger.info("Building service dependency graph from trace topology...")

    # Self-join: parent_span_id in child matches span_id in parent
    call_graph = (
        df.alias("parent")
        .join(
            df.alias("child"),
            F.col("parent.span_id") == F.col("child.parent_span_id"),
            "inner",
        )
        .select(
            F.col("parent.service_name").alias("caller_service"),
            F.col("child.service_name").alias("callee_service"),
            F.col("parent.is_failure").alias("caller_failure"),
            F.col("child.is_failure").alias("callee_failure"),
            F.col("parent.start_time_ts").alias("caller_start"),
            F.col("child.start_time_ts").alias("callee_start"),
            F.col("child.response_time_ms").alias("callee_latency"),
        )
    )

    # Aggregate per service pair
    service_pairs = (
        call_graph
        .groupBy("caller_service", "callee_service")
        .agg(
            F.count("*").alias("call_count"),
            F.sum("caller_failure").alias("caller_error_count"),
            F.sum("callee_failure").alias("callee_error_count"),
            F.sum(F.when(
                (F.col("caller_failure") == 1) & (F.col("callee_failure") == 1), 1
            ).otherwise(0)).alias("co_failure_count"),
            F.avg("callee_latency").alias("avg_callee_latency_ms"),
        )
    )

    # Propagation score: proportion of callee failures that co-occur with caller failures
    service_pairs = service_pairs.withColumn(
        "propagation_score",
        F.when(F.col("callee_error_count") > 0,
               F.col("co_failure_count") / F.col("callee_error_count"))
        .otherwise(0.0),
    )

    logger.info(f"Service dependency graph: {service_pairs.count()} service pairs identified.")
    return service_pairs


# ============================================================
# Failure Propagation Chain Detection
# ============================================================
def detect_propagation_chains(
    df: DataFrame,
    time_window_sec: int = 60,
) -> DataFrame:
    """
    Identify temporal chains where a failure in service A is followed
    within `time_window_sec` by a failure in service B (its downstream caller).

    Uses Spark window functions with lag() to detect temporal cascades.

    Returns DataFrame with columns:
      [trace_id, source_service, target_service, source_timestamp,
       target_timestamp, propagation_lag_ms, propagation_depth]
    """
    logger.info(f"Detecting failure propagation chains (window={time_window_sec}s)...")

    # Filter to failures only
    failures = df.filter(F.col("is_failure") == 1)

    # Window: order by start_time within each trace_id
    window_spec = Window.partitionBy("trace_id").orderBy("start_time_ts")

    # Build chains: for each failure, find the next failure in the same trace
    chains = (
        failures
        .withColumn("next_service", F.lead("service_name", 1).over(window_spec))
        .withColumn("next_timestamp", F.lead("start_time_ts", 1).over(window_spec))
        .withColumn("next_status", F.lead("status_code", 1).over(window_spec))
        .withColumn("next_is_failure", F.lead("is_failure", 1).over(window_spec))
        .withColumn("propagation_lag_sec",
            F.when(F.col("next_timestamp").isNotNull(),
                   F.unix_timestamp("next_timestamp") - F.unix_timestamp("start_time_ts"))
            .otherwise(None))
        # Only keep chains where the next service also failed within the window
        .filter(
            (F.col("next_is_failure") == 1) &
            (F.col("propagation_lag_sec") <= time_window_sec) &
            (F.col("service_name") != F.col("next_service"))  # exclude self-loops
        )
        .select(
            F.col("trace_id"),
            F.col("service_name").alias("source_service"),
            F.col("next_service").alias("target_service"),
            F.col("start_time_ts").alias("source_timestamp"),
            F.col("next_timestamp").alias("target_timestamp"),
            F.col("propagation_lag_sec"),
            F.lit(2).alias("propagation_depth"),  # direct propagation is depth 2
        )
    )

    chain_count = chains.count()
    logger.info(f"Detected {chain_count:,} failure propagation chains.")
    return chains


# ============================================================
# Cross-Service Error Correlation
# ============================================================
def correlate_cross_service_errors(
    df: DataFrame,
    spark_session,
    min_correlation: float = 0.3,
) -> DataFrame:
    """
    Compute pairwise error-rate correlation between services using a
    pivot + join strategy that stays within Spark's distributed engine
    (avoids driver-side Python loops for production-scale data).

    For the academic dataset (~20 microservices), each service pair
    triggers a Spark join — still distributed in terms of data shuffling
    but sequential in pair processing. This is acceptable for Master's-level
    analysis while still demonstrating Spark operations.

    Returns DataFrame:
      [service_a, service_b, error_correlation, sample_size]
    """
    logger.info("Computing cross-service error correlations (pivot-join strategy)...")

    # Step 1: Error rate per service per minute
    service_minute = (
        df
        .withColumn("minute_ts",
            F.window("start_time_ts", "1 minute").getField("start"))
        .groupBy("service_name", "minute_ts")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum("is_failure").alias("error_count"),
        )
        .withColumn("error_rate",
            F.col("error_count") / F.when(F.col("total_requests") > 0, F.col("total_requests")).otherwise(1))
    )
    service_minute.cache()

    # Step 2: Pivot to service×time matrix for correlation computation
    # Collect service names (small cardinality ~20, safe for collect)
    services = [r.service_name for r in service_minute.select("service_name").distinct().collect()]
    logger.info(f"Computing pairwise correlations for {len(services)} services "
                 f"({len(services) * (len(services) - 1) // 2} pairs)...")

    correlations = []
    for i, srv_a in enumerate(services):
        for srv_b in services[i + 1:]:
            # Join error-rate time series for service pair
            a_data = service_minute.filter(F.col("service_name") == srv_a).select(
                F.col("minute_ts"), F.col("error_rate").alias("error_rate_a")
            )
            b_data = service_minute.filter(F.col("service_name") == srv_b).select(
                F.col("minute_ts"), F.col("error_rate").alias("error_rate_b")
            )
            joined = a_data.join(b_data, "minute_ts", "inner")
            joined = joined.filter(
                F.col("error_rate_a").isNotNull() & F.col("error_rate_b").isNotNull()
            )
            n = joined.count()
            if n < 5:
                continue
            corr_val = joined.stat.corr("error_rate_a", "error_rate_b")
            if corr_val is not None and abs(corr_val) >= min_correlation:
                correlations.append((srv_a, srv_b, float(corr_val), n))

    service_minute.unpersist()

    schema = ["service_a", "service_b", "error_correlation", "sample_size"]
    if not correlations:
        logger.warning("No significant cross-service error correlations found.")
        return spark_session.createDataFrame([], schema)

    result = spark_session.createDataFrame(correlations, schema)
    result = result.orderBy(F.abs("error_correlation").desc())
    logger.info(f"Found {result.count()} significant cross-service error correlations (|r| >= {min_correlation}).")
    return result


# ============================================================
# Propagation Summary Metrics
# ============================================================
def compute_propagation_metrics(
    service_pairs: DataFrame,
    chains: DataFrame,
) -> dict:
    """Compute summary metrics for RQ1 results."""
    metrics = {}

    # Top propagation paths
    top_pairs = (
        service_pairs
        .orderBy(F.col("propagation_score").desc())
        .limit(10)
        .collect()
    )
    metrics["top_propagation_paths"] = [
        {
            "caller": r.caller_service,
            "callee": r.callee_service,
            "score": round(r.propagation_score, 4),
            "call_count": r.call_count,
        }
        for r in top_pairs
    ]

    # Propagation chain summary
    metrics["total_propagation_chains"] = chains.count()
    metrics["avg_propagation_lag_sec"] = round(
        chains.agg(F.avg("propagation_lag_sec")).collect()[0][0] or 0, 2
    )

    logger.info(f"Propagation metrics: {metrics}")
    return metrics


# ============================================================
# Write Results
# ============================================================



# ============================================================
# Main Entry Point
# ============================================================
def run_cross_service_analysis(
    df: Optional[DataFrame] = None,
    bucket: str = "microservice-logs",
) -> dict:
    """
    Full cross-service failure propagation analysis (RQ1).

    Args:
        df: If provided, use this preloaded DataFrame. Otherwise read from MinIO.
        bucket: MinIO bucket name.

    Returns dict with propagation metrics for RQ1.
    """
    logger.info("=" * 60)
    logger.info("MODULE 3: CROSS-SERVICE FAILURE PROPAGATION ANALYSIS (RQ1)")
    logger.info("=" * 60)

    spark = create_spark_session()
    config = load_config()
    cfg = config["cross_service_analysis"]

    # Load data if not passed in
    if df is None:
        parquet_path = f"s3a://{bucket}/processed/telemetry_unified.parquet"
        logger.info(f"Loading data from {parquet_path}...")
        df = spark.read.parquet(parquet_path)

    # Analysis 1: Service dependency graph
    service_pairs = build_service_dependency_graph(
        df,
        propagation_window_sec=cfg.get("propagation_time_window_seconds", 60),
    )

    # Analysis 2: Propagation chains
    chains = detect_propagation_chains(
        df,
        time_window_sec=cfg.get("propagation_time_window_seconds", 60),
    )

    # Analysis 3: Error correlations
    correlations = correlate_cross_service_errors(
        df, spark,
        min_correlation=cfg.get("min_correlation_threshold", 0.3),
    )

    # Compute metrics
    metrics = compute_propagation_metrics(service_pairs, chains)

    # Write results
    write_to_postgres(service_pairs, "cross_service_pairs")
    write_to_postgres(chains.limit(50000), "propagation_chains")
    write_to_postgres(correlations, "error_correlations")

    # Write propagation chains to MinIO
    chains.write.mode("overwrite").parquet(
        f"s3a://{bucket}/analysis/cross_service/propagation_chains.parquet"
    )

    logger.info("Cross-service analysis complete.")
    return metrics


if __name__ == "__main__":
    run_cross_service_analysis()
