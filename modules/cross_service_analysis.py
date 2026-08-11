
# Module 3: Cross-Service Failure Propagation Analysis


from dotenv import load_dotenv
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from modules.shared_utils import create_spark_session, load_config, setup_logging, write_to_postgres

load_dotenv()
logger = setup_logging("cross_service_analysis")


# Service Dependency Graph

def build_service_dependency_graph(df: DataFrame,  propagation_window_sec: int = 60):
    """
    Infer a service-to-service call graph from parent_span_id relationships.
    """
    logger.info(f"#### Building service dependency graph from trace topology....")

    # Self-join: parent_span_id in child matches span_id in parent
    call_graph = (
        df.alias("parent").join(df.alias("child"),
            F.col("parent.span_id") == F.col("child.parent_span_id"),"inner")
        .select(
            F.col("parent.service_name").alias("caller_service"), F.col("child.service_name").alias("callee_service"),
            F.col("parent.is_failure").alias("caller_failure"),  F.col("child.is_failure").alias("callee_failure"),
            F.col("parent.start_time_ts").alias("caller_start"),  F.col("child.start_time_ts").alias("callee_start"),
            F.col("child.response_time_ms").alias("callee_latency")
        )
    )

    # Aggregate per service pair
    service_pairs = call_graph.groupBy("caller_service", "callee_service").agg(
        F.count("*").alias("call_count"), F.sum("caller_failure").alias("caller_error_count"),
        F.sum("callee_failure").alias("callee_error_count"),
        F.sum(F.when((F.col("caller_failure") == 1) & (F.col("callee_failure") == 1), 1).otherwise(0)).alias(
            "co_failure_count"
        ),
        F.avg("callee_latency").alias("avg_callee_latency_ms"),
    )

    # Propagation score: proportion of callee failures that co-occur with caller failures
    service_pairs = service_pairs.withColumn(
        "propagation_score",
        F.when(
            F.col("callee_error_count") > 0, F.col("co_failure_count") / F.col("callee_error_count")
        ).otherwise(0.0),
    )

    logger.info(f"Service dependency graph: {service_pairs.count()} service pairs identified.")
    return service_pairs



#failure Propagation Chain Detection

def detect_propagation_chains(df: DataFrame, time_window_sec: int = 60):
    """
    Identify temporal chains where a failure in service A is followed
    within `time_window_sec` 
    """
    logger.info(f"#Detecting failure propagation chains ....")

    # Filter to failures only
    failures = df.filter(F.col("is_failure") == 1)

    # Window: order by start_time within each trace_id
    window_spec = Window.partitionBy("trace_id").orderBy("start_time_ts")

    # Build chains: for each failure, find the next failure in the same trace
    chains = (
        failures.withColumn("next_service", F.lead("service_name", 1).over(window_spec)).withColumn("next_timestamp", F.lead("start_time_ts", 1).over(window_spec))
        .withColumn("next_status", F.lead("status_code", 1).over(window_spec)).withColumn("next_is_failure", F.lead("is_failure", 1).over(window_spec))
        .withColumn(
            "propagation_lag_sec",
            F.when(
                F.col("next_timestamp").isNotNull(),
                F.unix_timestamp("next_timestamp") - F.unix_timestamp("start_time_ts"),
            ).otherwise(None)
        )
        # Only keep chains where the next service also failed within the window
        .filter(
            (F.col("next_is_failure") == 1) & (F.col("propagation_lag_sec") <= time_window_sec)
            & (F.col("service_name") != F.col("next_service"))  # exclude self-loops
        )
        .select(
            F.col("trace_id"),
            F.col("service_name").alias("source_service"), F.col("next_service").alias("target_service"),
            F.col("start_time_ts").alias("source_timestamp"), F.col("next_timestamp").alias("target_timestamp"),
            F.col("propagation_lag_sec"),  F.lit(2).alias("propagation_depth"),  # direct propagation is depth 2
        )
    )

    chain_count = chains.count()
    logger.info(f"#### Detected {chain_count:,} failure propagation chains.....")
    return chains



#cross-Service Error Correlation

def correlate_cross_service_errors(
    df: DataFrame, spark_session=None, min_correlation: float = 0.3,
    min_samples: int = 5):
    """
    Compute pairwise error-rate correlation between every pair of services
    """
    del spark_session  # Output is derived from `df`, so no session handle is needed.

    logger.info("#### Computing cross-service error correlations....")

    # Step 1: Error rate per service per minute.
    service_minute = (
        df.withColumn("minute_ts", F.window("start_time_ts", "1 minute").getField("start"))
        .groupBy("service_name", "minute_ts")
        .agg(F.count("*").alias("total_requests"), F.sum("is_failure").alias("error_count"))
        .withColumn(
            "error_rate", F.col("error_count") / F.when(F.col("total_requests") > 0, F.col("total_requests")).otherwise(1),
        )
        .select("service_name", "minute_ts", "error_rate")
    )

    # Step 2: Self-join on the time bucket to align both services' series.
    left = service_minute.select(
        F.col("service_name").alias("service_a"),
        F.col("minute_ts"), F.col("error_rate").alias("error_rate_a"),
    )
    right = service_minute.select(
        F.col("service_name").alias("service_b"),
        F.col("minute_ts"), F.col("error_rate").alias("error_rate_b"),
    )

    # Steps 3 & 4: one aggregation collects the sufficient statistics for every
    # pair
    paired = (
        left.join(right, "minute_ts", "inner")
        .filter(F.col("service_a") < F.col("service_b"))
        .groupBy("service_a", "service_b")
        .agg(
            F.count("*").alias("sample_size"), F.sum("error_rate_a").alias("sum_a"),
            F.sum("error_rate_b").alias("sum_b"),
            F.sum(F.col("error_rate_a")*F.col("error_rate_b")).alias("sum_ab"),
            F.sum(F.col("error_rate_a") * F.col("error_rate_a")).alias("sum_aa"),
            F.sum(F.col("error_rate_b") * F.col("error_rate_b")).alias("sum_bb")
        ).filter(F.col("sample_size") >= min_samples)
    )

    n = F.col("sample_size")
    covariance = n * F.col("sum_ab") - F.col("sum_a") * F.col("sum_b")
    # TODO: add check for constant series
    variance_a = F.greatest(n * F.col("sum_aa") - F.col("sum_a") * F.col("sum_a"), F.lit(0.0))
    variance_b = F.greatest(n * F.col("sum_bb") - F.col("sum_b") * F.col("sum_b"), F.lit(0.0))

    result = (paired.withColumn(
            "error_correlation",
            F.try_divide(covariance, F.sqrt(variance_a * variance_b)),
        )
        .filter(F.col("error_correlation").isNotNull())
        # Clamp to the mathematically valid range; accumulated float error can
        # otherwise push a perfect correlation a hair past +/-1.
        .withColumn(
            "error_correlation",
            F.greatest(F.lit(-1.0), F.least(F.lit(1.0), F.col("error_correlation"))),
        ).filter(F.abs(F.col("error_correlation")) >= min_correlation)
        .select("service_a", "service_b", "error_correlation", "sample_size")
        .orderBy(F.abs(F.col("error_correlation")).desc())
    )

    logger.info(f"Cross-service error correlations computed")
    return result



# Propagation Summary Metrics

def compute_propagation_metrics(
    service_pairs: DataFrame, chains: DataFrame,
):
    """Compute summary metrics for RQ1 results."""
    metrics = {}

    # Top propagation paths
    top_pairs = service_pairs.orderBy(F.col("propagation_score").desc()).limit(10).collect()
    metrics["top_propagation_paths"] = [
        { "caller": r.caller_service, "callee": r.callee_service,
            "score": round(r.propagation_score, 4), "call_count": r.call_count
        }
        for r in top_pairs
    ]

    # Propagation chain summary
    metrics["total_propagation_chains"] = chains.count()
    metrics["avg_propagation_lag_sec"] = round(
        chains.agg(F.avg("propagation_lag_sec")).collect()[0][0] or 0, 2
    )

    logger.info(f"#### Propagation metrics: {metrics}")
    return metrics


#main
def run_cross_service_analysis(
    df: DataFrame | None = None,
    bucket: str = "microservice-logs",
) -> dict:
    """
    Full cross-service failure propagation analysis (RQ1)
    """
    logger.info("=" * 40)
    logger.info("MODULE 3: CROSS SERVICE FAILURE PROPAGATION ANALYSIS (RQ1)")
    logger.info("=" * 55)

    spark = create_spark_session()
    config = load_config()
    cfg = config["cross_service_analysis"]

    # Load data if not passed in
    if df is None:
        parquet_path = f"s3a://{bucket}/processed/telemetry_unified.parquet"
        logger.info(f"Loading data from {parquet_path}....")
        df = spark.read.parquet(parquet_path)

    # Analysis 1: Service dependency graph
    service_pairs = build_service_dependency_graph(
        df,  propagation_window_sec=cfg.get("propagation_time_window_seconds", 60),
    )

    # Analysis 2: Propagation chains
    chains = detect_propagation_chains(
        df, time_window_sec=cfg.get("propagation_time_window_seconds", 60))

    # Analysis 3: Error correlations
    correlations = correlate_cross_service_errors(
        df, spark,
        min_correlation=cfg.get("min_correlation_threshold", 0.3)
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

#main method
if __name__ == "__main__":
    run_cross_service_analysis()
