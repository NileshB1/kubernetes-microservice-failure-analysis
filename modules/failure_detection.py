# ============================================================
# Module 4: Abnormal Failure Pattern Detection
# ============================================================
# Purpose (RQ2):
#   "How effectively can distributed processing identify abnormal
#    failure patterns across concurrent Kubernetes microservice
#    workloads?"
#
#   Applies statistical anomaly detection using z-score thresholds
#   on error rates, latency distributions, and resource usage
#   across services and time windows.
#
# Inputs:
#   - Unified telemetry Parquet from s3a://microservice-logs/processed/
#   - config.yaml failure_detection parameters
#
# Outputs:
#   - anomaly_scores table in PostgreSQL
#   - failure_patterns table in PostgreSQL
#   - Anomaly results in MinIO (Parquet)
#
# Main Functions:
#   - compute_error_rate_timeseries()    → Per-service per-minute error rates
#   - detect_error_rate_anomalies()      → Z-score based anomaly detection
#   - detect_latency_anomalies()         → Latency distribution anomalies
#   - detect_resource_anomalies()        → CPU/memory usage anomalies
#   - cluster_failure_patterns()         → Group similar failure patterns
#   - compute_rq2_summary()              → Summary metrics for RQ2
#
# Spark Operations Used:
#   - GroupBy + window aggregations
#   - F.stddev(), F.mean() for z-score
#   - Spark window functions for rolling metrics
#   - DataFrame statistics (approxPercentile)
#
# RQ Contribution:
#   - RQ2: Quantifies ability to identify abnormal failure patterns
# ============================================================

import os
import logging
from typing import Optional

from dotenv import load_dotenv
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from modules.shared_utils import create_spark_session, load_config, write_to_postgres, setup_logging

load_dotenv()
logger = setup_logging("failure_detection")


# ============================================================
# Error Rate Time Series
# ============================================================
def compute_error_rate_timeseries(df: DataFrame, window_minutes: int = 1) -> DataFrame:
    """
    Compute per-service per-time-bucket error rates.

    Returns DataFrame:
      [service_name, time_bucket, total_requests, error_count, error_rate]
    """
    logger.info(f"Computing error-rate time series (window={window_minutes} min)...")

    ts = (
        df
        .withColumn("time_bucket",
            F.window("start_time_ts", f"{window_minutes} minutes").getField("start"))
        .groupBy("service_name", "time_bucket")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum("is_failure").alias("error_count"),
            F.avg("response_time_ms").alias("avg_latency_ms"),
            F.avg("cpu_usage_mcores").alias("avg_cpu_mcores"),
            F.avg("memory_usage_mb").alias("avg_memory_mb"),
        )
        .withColumn("error_rate",
            F.col("error_count") / F.when(F.col("total_requests") > 0, F.col("total_requests")).otherwise(1))
    )

    logger.info(f"Error rate time series: {ts.count():,} rows.")
    return ts


# ============================================================
# Z-Score Anomaly Detection
# ============================================================
def detect_error_rate_anomalies(
    ts: DataFrame,
    zscore_threshold: float = 3.0,
) -> DataFrame:
    """
    Detect anomalies in per-service error rates using z-score method.

    For each service:
      1. Compute mean and stddev of error_rate across time
      2. Flag time buckets where |z-score| > threshold

    Returns DataFrame with anomaly flags.
    """
    logger.info(f"Detecting error-rate anomalies (z-score > {zscore_threshold})...")

    # Compute per-service baseline statistics
    baseline = (
        ts
        .groupBy("service_name")
        .agg(
            F.mean("error_rate").alias("mean_error_rate"),
            F.stddev("error_rate").alias("std_error_rate"),
        )
    )

    # Join baseline to time series and compute z-score
    anomalies = (
        ts.join(baseline, "service_name", "left")
        .withColumn("zscore_error_rate",
            F.when(F.col("std_error_rate") > 0,
                   (F.col("error_rate") - F.col("mean_error_rate")) / F.col("std_error_rate"))
            .otherwise(0.0))
        .withColumn("is_anomaly_error",
            F.when(F.abs("zscore_error_rate") > zscore_threshold, 1).otherwise(0))
    )

    anomaly_count = anomalies.filter(F.col("is_anomaly_error") == 1).count()
    logger.info(f"  Found {anomaly_count:,} error-rate anomaly time buckets.")
    return anomalies


def detect_latency_anomalies(
    ts: DataFrame,
    zscore_threshold: float = 3.0,
) -> DataFrame:
    """
    Detect latency anomalies using the same z-score methodology.
    """
    logger.info(f"Detecting latency anomalies (z-score > {zscore_threshold})...")

    baseline = (
        ts
        .groupBy("service_name")
        .agg(
            F.mean("avg_latency_ms").alias("mean_latency"),
            F.stddev("avg_latency_ms").alias("std_latency"),
        )
    )

    anomalies = (
        ts.join(baseline, "service_name", "left")
        .withColumn("zscore_latency",
            F.when(F.col("std_latency") > 0,
                   (F.col("avg_latency_ms") - F.col("mean_latency")) / F.col("std_latency"))
            .otherwise(0.0))
        .withColumn("is_anomaly_latency",
            F.when(F.abs("zscore_latency") > zscore_threshold, 1).otherwise(0))
    )

    anomaly_count = anomalies.filter(F.col("is_anomaly_latency") == 1).count()
    logger.info(f"  Found {anomaly_count:,} latency anomaly time buckets.")
    return anomalies


def detect_resource_anomalies(
    ts: DataFrame,
    zscore_threshold: float = 3.0,
) -> DataFrame:
    """
    Detect resource usage anomalies (CPU/memory spikes).
    """
    logger.info(f"Detecting resource anomalies (z-score > {zscore_threshold})...")

    baseline = (
        ts
        .groupBy("service_name")
        .agg(
            F.mean("avg_cpu_mcores").alias("mean_cpu"),
            F.stddev("avg_cpu_mcores").alias("std_cpu"),
            F.mean("avg_memory_mb").alias("mean_mem"),
            F.stddev("avg_memory_mb").alias("std_mem"),
        )
    )

    anomalies = (
        ts.join(baseline, "service_name", "left")
        .withColumn("zscore_cpu",
            F.when(F.col("std_cpu") > 0,
                   (F.col("avg_cpu_mcores") - F.col("mean_cpu")) / F.col("std_cpu"))
            .otherwise(0.0))
        .withColumn("zscore_memory",
            F.when(F.col("std_mem") > 0,
                   (F.col("avg_memory_mb") - F.col("mean_mem")) / F.col("std_mem"))
            .otherwise(0.0))
        .withColumn("is_anomaly_resource",
            F.when(
                (F.abs("zscore_cpu") > zscore_threshold) |
                (F.abs("zscore_memory") > zscore_threshold), 1
            ).otherwise(0))
    )

    anomaly_count = anomalies.filter(F.col("is_anomaly_resource") == 1).count()
    logger.info(f"  Found {anomaly_count:,} resource anomaly time buckets.")
    return anomalies


# ============================================================
# Anomaly Unification
# ============================================================
def unify_anomalies(
    error_anomalies: DataFrame,
    latency_anomalies: DataFrame,
    resource_anomalies: DataFrame,
) -> DataFrame:
    """
    Combine all anomaly types into a single anomaly score per service-time bucket.

    Anomaly score = normalized sum of individual anomaly flags.
    """
    logger.info("Unifying anomaly signals...")

    # Join all anomaly types
    unified = (
        error_anomalies
        .select("service_name", "time_bucket", "is_anomaly_error", "zscore_error_rate")
        .join(
            latency_anomalies.select("service_name", "time_bucket", "is_anomaly_latency", "zscore_latency"),
            ["service_name", "time_bucket"],
            "outer",
        )
        .join(
            resource_anomalies.select("service_name", "time_bucket", "is_anomaly_resource", "zscore_cpu", "zscore_memory"),
            ["service_name", "time_bucket"],
            "outer",
        )
    )

    # Fill nulls (from outer joins)
    unified = unified.fillna(0, subset=[
        "is_anomaly_error", "is_anomaly_latency", "is_anomaly_resource",
        "zscore_error_rate", "zscore_latency", "zscore_cpu", "zscore_memory",
    ])

    # Composite anomaly score (0–3)
    unified = unified.withColumn(
        "anomaly_score",
        F.col("is_anomaly_error") + F.col("is_anomaly_latency") + F.col("is_anomaly_resource"),
    )

    # Overall anomaly flag: at least 2 out of 3 signals
    unified = unified.withColumn(
        "is_anomaly_overall",
        F.when(F.col("anomaly_score") >= 2, 1).otherwise(0),
    )

    logger.info(f"Unified anomalies: {unified.filter('is_anomaly_overall = 1').count():,} multi-signal anomalies.")
    return unified


# ============================================================
# Failure Pattern Clustering
# ============================================================
def cluster_failure_patterns(unified: DataFrame) -> DataFrame:
    """
    Identify distinct failure patterns by grouping anomalies
    by their signal combination (error, latency, resource).

    Pattern types:
      - "latency_spike"    — only latency anomaly
      - "error_surge"      — only error anomaly
      - "resource_pressure" — only resource anomaly
      - "cascading_failure" — error + latency
      - "resource_exhaustion" — latency + resource
      - "full_failure"     — all three signals
    """
    logger.info("Clustering failure patterns...")

    patterns = (
        unified
        .withColumn("pattern_type",
            F.when(F.col("anomaly_score") == 3, "full_failure")
             .when((F.col("is_anomaly_error") == 1) & (F.col("is_anomaly_latency") == 1), "cascading_failure")
             .when((F.col("is_anomaly_latency") == 1) & (F.col("is_anomaly_resource") == 1), "resource_exhaustion")
             .when((F.col("is_anomaly_error") == 1) & (F.col("is_anomaly_resource") == 1), "error_resource_link")
             .when(F.col("is_anomaly_error") == 1, "error_surge")
             .when(F.col("is_anomaly_latency") == 1, "latency_spike")
             .when(F.col("is_anomaly_resource") == 1, "resource_pressure")
             .otherwise("normal")
        )
        .groupBy("service_name", "pattern_type")
        .agg(
            F.count("*").alias("occurrence_count"),
            F.avg("anomaly_score").alias("avg_severity"),
        )
        .filter(F.col("pattern_type") != "normal")
    )

    logger.info(f"Identified failure patterns across {patterns.select('service_name').distinct().count()} services.")
    return patterns


# ============================================================
# RQ2 Summary Metrics
# ============================================================
def compute_rq2_summary(unified: DataFrame, patterns: DataFrame) -> dict:
    """Compute summary metrics for RQ2."""
    total_anomalies = unified.filter(F.col("is_anomaly_overall") == 1).count()
    total_buckets = unified.count()

    metrics = {
        "total_time_buckets_analyzed": total_buckets,
        "total_anomalies_detected": total_anomalies,
        "anomaly_rate_pct": round(100 * total_anomalies / max(total_buckets, 1), 2),
        "top_anomalous_services": (
            unified
            .filter(F.col("is_anomaly_overall") == 1)
            .groupBy("service_name")
            .agg(F.count("*").alias("anomaly_count"))
            .orderBy(F.col("anomaly_count").desc())
            .limit(5)
            .rdd.map(lambda r: {"service": r.service_name, "count": r.anomaly_count})
            .collect()
        ),
        "pattern_distribution": (
            patterns
            .groupBy("pattern_type")
            .agg(F.sum("occurrence_count").alias("total"))
            .orderBy(F.col("total").desc())
            .rdd.map(lambda r: {"pattern": r.pattern_type, "count": r.total})
            .collect()
        ),
    }
    logger.info(f"RQ2 Summary: {metrics['anomaly_rate_pct']}% anomaly rate, {len(metrics['pattern_distribution'])} pattern types.")
    return metrics


# ============================================================
# Write Results
# ============================================================



# ============================================================
# Main Entry Point
# ============================================================
def run_failure_detection(
    df: Optional[DataFrame] = None,
    bucket: str = "microservice-logs",
) -> dict:
    """
    Full failure/anomaly detection pipeline (RQ2).

    Args:
        df: Preloaded unified telemetry DataFrame, or None to load from MinIO.
        bucket: MinIO bucket name.

    Returns dict with RQ2 summary metrics.
    """
    logger.info("=" * 60)
    logger.info("MODULE 4: ABNORMAL FAILURE PATTERN DETECTION (RQ2)")
    logger.info("=" * 60)

    spark = create_spark_session()
    config = load_config()
    cfg = config["failure_detection"]

    if df is None:
        parquet_path = f"s3a://{bucket}/processed/telemetry_unified.parquet"
        logger.info(f"Loading data from {parquet_path}...")
        df = spark.read.parquet(parquet_path)

    z_thresh = cfg.get("anomaly_zscore_threshold", 3.0)

    # Step 1: Error rate time series
    ts = compute_error_rate_timeseries(df, window_minutes=cfg.get("rolling_window_minutes", 15))

    # Step 2: Detect anomalies
    error_anomalies = detect_error_rate_anomalies(ts, zscore_threshold=z_thresh)
    latency_anomalies = detect_latency_anomalies(ts, zscore_threshold=z_thresh)
    resource_anomalies = detect_resource_anomalies(ts, zscore_threshold=z_thresh)

    # Step 3: Unify
    unified = unify_anomalies(error_anomalies, latency_anomalies, resource_anomalies)

    # Step 4: Cluster patterns
    patterns = cluster_failure_patterns(unified)

    # Step 5: Compute metrics
    metrics = compute_rq2_summary(unified, patterns)

    # Step 6: Write to PostgreSQL
    write_to_postgres(unified.select(
        "service_name", "time_bucket", "is_anomaly_error",
        "is_anomaly_latency", "is_anomaly_resource",
        "anomaly_score", "is_anomaly_overall",
    ), "anomaly_scores")

    write_to_postgres(patterns, "failure_patterns")

    # Write to MinIO
    unified.write.mode("overwrite").parquet(
        f"s3a://{bucket}/analysis/failure_detection/anomalies.parquet"
    )

    logger.info("Failure detection pipeline complete.")
    return metrics


if __name__ == "__main__":
    run_failure_detection()
