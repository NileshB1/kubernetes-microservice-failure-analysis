
#module 5- Spark Scalability Analysis


import os
import time

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from modules.shared_utils import create_spark_session, load_config, setup_logging, write_to_postgres

load_dotenv()
logger = setup_logging("scalability")



# Data Generation at Scale
SERVICE_NAMES = [
    "frontend", "auth-service", "user-service", "order-service",
    "payment-service", "inventory-service",
    "notification-service", "shipping-service",
    "catalog-service", "cart-service",
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
HTTP_STATUSES = [200, 201, 204, 301, 400, 401, 403, 404, 408, 500, 502, 503, 504]
POD_IDS = [f"pod-{i:05d}" for i in range(1, 201)]


def generate_scaled_dataset(spark, target_rows: int, bucket: str) -> str:
    """
    Generate a scaled dataset using Spark's range() 
    """
    logger.info(f"####Generating scaled dataset {target_rows:,} rows....")

    num_partitions = min(16, max(1, target_rows // 100_000))

    # Generate with range (distributed) and add columns via selectExpr
    df = spark.range(target_rows, numPartitions=num_partitions).select(
        F.concat(F.lit("span-"), F.lpad(F.col("id").cast("string"), 12, "0")).alias("span_id"),
        F.concat(F.lit("trace-"), F.lpad((F.col("id") % 5000).cast("string"), 6, "0")).alias("trace_id"))
    # Add service_name (randomly distributed)
    services_array = F.array([F.lit(s) for s in SERVICE_NAMES])
    df = df.withColumn("service_name", services_array.getItem((F.rand() * len(SERVICE_NAMES)).cast("int")))

    # Add numeric columns with random distributions
    df = df.withColumn("response_time_ms", F.abs(F.round(F.rand() * 300 + F.randn() * 60 + 30, 2)))
    df = df.withColumn(
        "status_code",
        F.when(F.rand() < 0.08,
            F.array([F.lit(500), F.lit(502), F.lit(503), F.lit(504)]).getItem((F.rand() * 4).cast("int")),
        ).otherwise(F.lit(200)),
    )
    df = df.withColumn("is_failure", F.when(F.col("status_code") >= 500, 1).otherwise(0))
    df = df.withColumn(
        "pod_id", F.concat(F.lit("pod-"), F.lpad((F.rand() * 200).cast("int").cast("string"), 5, "0"))
    )
    df = df.withColumn("cpu_usage_mcores", F.abs(F.round(F.rand() * 1000 + F.randn() * 300 + 100, 2)))
    df = df.withColumn("memory_usage_mb", F.abs(F.round(F.rand() * 2000 + F.randn() * 500 + 200, 2)))
    df = df.withColumn("start_time",
        (F.lit("2024-01-01 00:00:00").cast("timestamp")
            + F.expr("make_interval(0, 0, 0, 0, 0, 0, cast(rand() * 86400 as int))")
        ).cast("string"),
    )

    df = df.repartition(num_partitions)

    output_path = f"s3a://{bucket}/scalability/data_{target_rows}.parquet"
    logger.info(f"Writing scaled dataset to {output_path} ...")
    df.write.mode("overwrite").parquet(output_path)

    actual_count = spark.read.parquet(output_path).count()
    logger.info(f"  [ok] Written {actual_count:,} rows to {output_path}")
    return output_path


#benchmark Runner

def run_benchmark(
    spark: SparkSession, df: DataFrame,
    label: str):
    """
    Run a subset of the analysis pipeline against a DataFrame and
    record timing for key Spark operations
    """
    # Count once: df.count() is a full pass, and the benchmark below is
    # what we actually want to measure.....
    input_rows = df.count()
    logger.info(f"  Benchmarking [{label}] ({input_rows:,} rows)...")

    results = {"label": label, "input_rows": input_rows}

    # Operation 1: GroupBy + Aggregation
    t0 = time.time()
    _ = (
        df.groupBy("service_name")
        .agg(F.count("*").alias("total"),  F.sum("is_failure").alias("errors"),
            F.avg("response_time_ms").alias("avg_latency"), F.stddev("response_time_ms").alias("std_latency")
        ).collect()
    )
    results["groupby_agg_sec"] = round(time.time() - t0, 3)
    logger.info(f"    GroupBy+Agg:  {results['groupby_agg_sec']:.3f}s")

    #operation 2=> Window Function (lag)
    t0 = time.time()
    window_spec = Window.partitionBy("trace_id").orderBy("start_time")
    _ = (
        df.withColumn("next_service", F.lead("service_name", 1).over(window_spec))
        .filter(F.col("next_service").isNotNull()).count()
    )
    results["window_fn_sec"] = round(time.time() - t0, 3)
    logger.info(f"    Window fn:    {results['window_fn_sec']:.3f}s")

    # Operation 3: Join bounded
    t0 = time.time()
    sample = df.sample(fraction=0.01, seed=42).select("trace_id", "service_name")
    _ = (
        df.alias("full").join(sample.alias("s"), F.col("full.trace_id") == F.col("s.trace_id"), "inner")
        .filter(F.col("full.service_name") != F.col("s.service_name")).count()
    )
    results["join_sec"] = round(time.time() - t0, 3)
    logger.info(f"Join: {results['join_sec']:.3f}s")

    #operation 4: Full shuffle
    t0 = time.time()
    _ = df.repartition(16, "service_name").count()
    results["shuffle_sec"] = round(time.time() - t0, 3)
    logger.info(f"Shuffle:  {results['shuffle_sec']:.3f}s")

    # Total
    results["total_sec"] = round(results["groupby_agg_sec"] + results["window_fn_sec"] + results["join_sec"] + results["shuffle_sec"],
        3)
    logger.info(f"TOTAL: {results['total_sec']:.3f}s")

    return results



# Scalability Experiment Suite

def run_scalability_experiments(bucket: str = "microservice-logs",  data_sizes: list[int] | None = None,
    repetitions: int = 1
):
    """
    Run the full scalability experiment suite
    """
    logger.info("=" * 45)
    logger.info("MODULE 5=> SPARK SCALABILITY ANALYSIS (RQ3......)")
    logger.info("=" * 55)

    config = load_config()
    cfg = config["scalability"]

    if data_sizes is None:
        # Check env var first (set by docker-compose), fall back to config.yaml
        env_sizes = os.getenv("SCALABILITY_DATA_SIZES", "")
        if env_sizes:
            data_sizes = [int(x.strip()) for x in env_sizes.split(",")]
            logger.info(f"Using SCALABILITY_DATA_SIZES from env: {data_sizes}")
        else:
            data_sizes = cfg.get("data_sizes", [100_000, 500_000, 1_000_000])
    if repetitions is None:
        repetitions = cfg.get("repetitions", 1)

    spark = create_spark_session()
    all_results = []

    for size in data_sizes:
        logger.info(f"\n{'=' * 45}")
        logger.info(f"Data Size: {size:,} rows")
        logger.info(f"{'=' * 45}")

        # Generate data
        data_path = generate_scaled_dataset(spark, size, bucket)

        # Run benchmarks
        for rep in range(repetitions):
            logger.info(f"  Repetition {rep + 1}/{repetitions}....")
            df = spark.read.parquet(data_path)
            result = run_benchmark(spark, df, f"{size:,}_r{rep+1}")
            result["data_size"] = size
            result["repetition"] = rep + 1

            # Compute derived metrics
            if result["total_sec"] > 0:
                result["throughput_rows_per_sec"] = round(result["input_rows"] / result["total_sec"], 1)
            else:
                result["throughput_rows_per_sec"] = 0.0

            all_results.append(result)

    # Compute speed-up (relative to smallest data size)
    if all_results:
        baseline = all_results[0]["total_sec"]
        if baseline > 0:
            for r in all_results:
                r["speedup_vs_baseline"] = round(baseline / max(r["total_sec"], 0.001), 3)
                r["baseline_size"] = all_results[0]["data_size"]
                r["baseline_time_sec"] = baseline

    logger.info(f"\nScalability experiments complete: {len(all_results)} results.....")
    return all_results



#scalability Metrics Computation
def compute_scalability_metrics(results: list[dict]) -> dict:
    """
    Aggregate scalability results into RQ3 summary metrics
    """
    logger.info("#### Computing scalability metrics for RQ3....")

    if not results:
        return {}

    # Group by data size
    by_size = {}
    for r in results:
        size = r["data_size"]
        if size not in by_size:
            by_size[size] = []
        by_size[size].append(r)

    sizes = sorted(by_size.keys())
    baseline_total = sum(r["total_sec"] for r in by_size[sizes[0]]) / len(by_size[sizes[0]])

    metrics = {
        "num_data_sizes": len(sizes), "num_repetitions_per_size": len(results) // len(sizes),
        "baseline_rows": sizes[0],  "baseline_total_sec": round(baseline_total, 3),
        "scaling_results": []
    }

    for size in sizes:
        avg_total = sum(r["total_sec"] for r in by_size[size]) / len(by_size[size])
        avg_throughput = sum(r["throughput_rows_per_sec"] for r in by_size[size]) / len(by_size[size])

        data_ratio = size / sizes[0]
        speedup = baseline_total / avg_total if avg_total > 0 else 0
        efficiency = speedup / data_ratio if data_ratio > 0 else 0

        metrics["scaling_results"].append(
            {
                "data_size": size, "avg_total_time_sec": round(avg_total, 3),
                "avg_throughput_rows_per_sec": round(avg_throughput, 1),
                "data_ratio": round(data_ratio, 2), "speedup": round(speedup, 3),
                "scalability_efficiency": round(efficiency, 3),
            }
        )

    # Check if sub-linear scaling
    largest = metrics["scaling_results"][-1]
    if largest["scalability_efficiency"] < 0.8:
        metrics["scaling_characteristic"] = "sub-linear"
    elif largest["scalability_efficiency"] < 1.1:
        metrics["scaling_characteristic"] = "near-linear"
    else:
        metrics["scaling_characteristic"] = "super-linear"

    logger.info(f"Scaling characteristic: {metrics['scaling_characteristic']}")
    for sr in metrics["scaling_results"]:
        logger.info(f"{sr['data_size']:>12,} rows | {sr['avg_total_time_sec']:>8.3f}s | "
            f"speedup={sr['speedup']:.3f} | efficiency={sr['scalability_efficiency']:.3f}")

    return metrics



#write Results
def write_scalability_results(
    spark_session, results: list[dict], table_name: str, mode: str = "overwrite"
) -> None:
    """Write list of dicts as a Spark DataFrame to PostgreSQL"""
    if not results:
        logger.warning(f"No results to write for {table_name}")
        return

    df = spark_session.createDataFrame(results)
    write_to_postgres(df, table_name, mode=mode)



#main
def run_scalability_analysis(
    bucket: str = "microservice-logs",
    data_sizes: list[int] | None = None,
    repetitions: int = 1,
) -> dict:
    """
    Full scalability analysis pipeline (RQ3)"""
    # Run experiments (creates its own SparkSession internally)
    results = run_scalability_experiments(bucket=bucket, data_sizes=data_sizes, repetitions=repetitions)

    # Create session only for writing results
    spark = create_spark_session()

    # Compute metrics
    metrics = compute_scalability_metrics(results)

    # Write raw results to PostgreSQL
    write_scalability_results(spark, results, "scalability_metrics")

    logger.info("Scalability analysis complete......ha ha")
    return metrics


if __name__ == "__main__":
    run_scalability_analysis()
