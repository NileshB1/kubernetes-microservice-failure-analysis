# ============================================================
# Module 2: Data Preprocessing
# ============================================================
# Purpose:
#   Reads raw CSV telemetry from MinIO via Spark, performs
#   distributed data cleaning, transformation, joins, and
#   outputs clean Parquet files back to MinIO.
#
# Inputs:
#   - Raw CSVs from s3a://microservice-logs/raw/
#   - config.yaml preprocessing parameters
#
# Outputs:
#   - Cleaned Parquet files in s3a://microservice-logs/processed/
#   - Raw telemetry loaded into PostgreSQL `raw_telemetry`
#   - Processed telemetry loaded into PostgreSQL `processed_telemetry`
#
# Main Functions:
#   - read_raw_datasets()       → Reads all CSVs from MinIO as Spark DFs
#   - clean_and_validate()      → Drops nulls, validates timestamps, dedupes
#   - join_datasets()           → Joins all 5 CSVs into unified telemetry DF
#   - engineer_features()       → Creates derived features (error flags, latency buckets)
#   - write_processed_data()    → Writes Parquet to MinIO
#   - write_to_postgres()       → Writes DataFrames to PostgreSQL
#
# Spark Operations Used:
#   - spark.read.csv() / .option()
#   - DataFrame.join() (inner / left)
#   - DataFrame.filter(), .dropDuplicates()
#   - DataFrame.withColumn(), .when(), .otherwise()
#   - .groupBy().agg()
#   - .write.parquet()
#   - .write.jdbc() (PostgreSQL)
#
# RQ Contribution:
#   - RQ1, RQ2, RQ3: Preprocessed, joined data is input for all analyses
# ============================================================

import os
import logging
from typing import Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

from modules.shared_utils import create_spark_session, load_config, write_to_postgres, setup_logging

load_dotenv()
logger = setup_logging("preprocessing")


# ============================================================
# Data Loading
# ============================================================
def read_raw_datasets(spark: SparkSession, bucket: str, prefix: str = "raw/") -> dict[str, DataFrame]:
    """
    Read all raw CSV files from MinIO into Spark DataFrames.

    Returns dict: filename (without .csv) → DataFrame
    """
    base_path = f"s3a://{bucket}/{prefix}"
    logger.info(f"Reading raw datasets from {base_path} ...")

    schemas = {
        "trace_service_name": StructType([
            StructField("trace_id", StringType(), True),
            StructField("service_name", StringType(), True),
            StructField("span_id", StringType(), True),
            StructField("parent_span_id", StringType(), True),
            StructField("namespace", StringType(), True),
            StructField("pod_id", StringType(), True),
            StructField("node_id", StringType(), True),
        ]),
        "trace_response_times": StructType([
            StructField("span_id", StringType(), True),
            StructField("response_time_ms", DoubleType(), True),
            StructField("wait_time_ms", DoubleType(), True),
            StructField("processing_time_ms", DoubleType(), True),
            StructField("network_latency_ms", DoubleType(), True),
        ]),
        "trace_request_times": StructType([
            StructField("span_id", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("duration_ms", DoubleType(), True),
            StructField("http_method", StringType(), True),
            StructField("endpoint", StringType(), True),
        ]),
        "resource_usage": StructType([
            StructField("pod_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("cpu_usage_mcores", DoubleType(), True),
            StructField("memory_usage_mb", DoubleType(), True),
            StructField("network_rx_bytes", IntegerType(), True),
            StructField("network_tx_bytes", IntegerType(), True),
            StructField("disk_io_read_bytes", IntegerType(), True),
            StructField("disk_io_write_bytes", IntegerType(), True),
        ]),
        "status_codes": StructType([
            StructField("span_id", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("error_message", StringType(), True),
            StructField("is_error", IntegerType(), True),
        ]),
    }

    dfs = {}
    for name, schema in schemas.items():
        file_path = f"{base_path}{name}.csv"
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(schema)
            .csv(file_path)
        )
        dfs[name] = df
        logger.info(f"  ✓ Loaded {name}: {df.count():,} rows, {len(df.columns)} columns")

    return dfs


# ============================================================
# Cleaning & Validation
# ============================================================
def clean_and_validate(dfs: dict[str, DataFrame], null_threshold: float = 0.5) -> dict[str, DataFrame]:
    """
    Clean individual DataFrames:
      - Drop columns exceeding null_threshold
      - Drop rows with nulls in key columns
      - Validate numeric ranges (latency > 0, etc.)
      - Deduplicate on key columns
    """
    cleaned = {}

    # Clean trace_service_name
    df = dfs["trace_service_name"]
    df = df.dropDuplicates(["span_id"])
    df = df.filter(F.col("span_id").isNotNull() & F.col("service_name").isNotNull())
    df = df.filter(F.col("parent_span_id") != "")
    cleaned["trace_service_name"] = df

    # Clean trace_response_times — ensure positive latencies
    df = dfs["trace_response_times"]
    df = df.dropDuplicates(["span_id"])
    df = df.filter(F.col("span_id").isNotNull())
    df = df.filter(
        (F.col("response_time_ms") >= 0) &
        (F.col("processing_time_ms") >= 0)
    )
    cleaned["trace_response_times"] = df

    # Clean trace_request_times — parse timestamps
    df = dfs["trace_request_times"]
    df = df.dropDuplicates(["span_id"])
    df = df.filter(F.col("span_id").isNotNull())
    df = df.withColumn("start_time_ts", F.to_timestamp("start_time"))
    df = df.withColumn("end_time_ts", F.to_timestamp("end_time"))
    df = df.filter(F.col("start_time_ts").isNotNull())
    cleaned["trace_request_times"] = df

    # Clean resource_usage
    df = dfs["resource_usage"]
    df = df.dropDuplicates(["pod_id", "timestamp"])
    df = df.withColumn("ts", F.to_timestamp("timestamp"))
    df = df.filter(F.col("cpu_usage_mcores") >= 0)
    cleaned["resource_usage"] = df

    # Clean status_codes
    df = dfs["status_codes"]
    df = df.dropDuplicates(["span_id"])
    df = df.filter(F.col("span_id").isNotNull())
    cleaned["status_codes"] = df

    logger.info("Cleaning & validation complete for all datasets.")
    return cleaned


# ============================================================
# Joining & Feature Engineering
# ============================================================
def join_datasets(dfs: dict[str, DataFrame]) -> DataFrame:
    """
    Join all datasets into a unified telemetry DataFrame.
    Primary key: span_id

    Join strategy:
      trace_service_name (left)
        ⋈ trace_response_times (on span_id)
        ⋈ trace_request_times (on span_id)
        ⋈ status_codes (on span_id)

    resource_usage is joined on pod_id as a left-join (enrichment).
    """
    logger.info("Joining datasets into unified telemetry...")

    base = dfs["trace_service_name"]
    base = base.join(dfs["trace_response_times"], on="span_id", how="inner")
    base = base.join(dfs["trace_request_times"], on="span_id", how="inner")
    base = base.join(dfs["status_codes"], on="span_id", how="inner")

    # resource_usage enrichment via pod_id
    base = base.join(
        dfs["resource_usage"].select("pod_id", "cpu_usage_mcores", "memory_usage_mb"),
        on="pod_id",
        how="left",
    )

    logger.info(f"Unified telemetry: {base.count():,} rows, {len(base.columns)} columns")
    return base


def engineer_features(df: DataFrame) -> DataFrame:
    """
    Add derived features for downstream analysis:

    - is_failure:       Boolean — status_code >= 500
    - is_latency_spike: Boolean — response_time_ms > 2000
    - latency_bucket:   String — low/medium/high/critical
    - error_category:   String — client_error / server_error / success
    - hour_of_day:      Integer
    - cpu_memory_ratio: Float
    """
    logger.info("Engineering features...")

    df = df.withColumn("is_failure", F.when(F.col("status_code") >= 500, 1).otherwise(0))
    df = df.withColumn("is_latency_spike", F.when(F.col("response_time_ms") > 2000, 1).otherwise(0))

    df = df.withColumn("latency_bucket",
        F.when(F.col("response_time_ms") < 100, "low")
         .when(F.col("response_time_ms") < 500, "medium")
         .when(F.col("response_time_ms") < 2000, "high")
         .otherwise("critical")
    )

    df = df.withColumn("error_category",
        F.when(F.col("status_code") >= 500, "server_error")
         .when(F.col("status_code") >= 400, "client_error")
         .otherwise("success")
    )

    df = df.withColumn("hour_of_day", F.hour("start_time_ts"))

    df = df.withColumn("cpu_memory_ratio",
        F.when(F.col("memory_usage_mb") > 0,
               F.col("cpu_usage_mcores") / F.col("memory_usage_mb"))
        .otherwise(0.0)
    )

    return df


# ============================================================
# Output
# ============================================================
def write_processed_data(df: DataFrame, bucket: str, prefix: str = "processed/") -> str:
    """Write the unified, feature-engineered DataFrame as Parquet to MinIO."""
    output_path = f"s3a://{bucket}/{prefix}telemetry_unified.parquet"
    logger.info(f"Writing processed data to {output_path} ...")

    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"  ✓ Written to {output_path}")
    return output_path





# ============================================================
# Main Entry Point
# ============================================================
def run_preprocessing_pipeline(
    bucket: str = "microservice-logs",
    raw_prefix: str = "raw/",
    processed_prefix: str = "processed/",
) -> DataFrame:
    """
    Full preprocessing pipeline:
      1. Create Spark session
      2. Read raw CSVs from MinIO
      3. Clean & validate
      4. Join into unified telemetry
      5. Engineer features
      6. Write Parquet to MinIO
      7. Write to PostgreSQL

    Returns the unified DataFrame for downstream modules.
    """
    logger.info("=" * 60)
    logger.info("MODULE 2: DATA PREPROCESSING")
    logger.info("=" * 60)

    spark = create_spark_session()
    config = load_config()

    # Step 1: Read
    raw_dfs = read_raw_datasets(spark, bucket, raw_prefix)

    # Step 2: Clean
    cleaned_dfs = clean_and_validate(raw_dfs, config["preprocessing"].get("null_threshold", 0.5))

    # Step 3: Join
    unified = join_datasets(cleaned_dfs)

    # Step 4: Feature Engineering
    unified = engineer_features(unified)

    # Cache for downstream use
    unified.cache()
    logger.info(f"Final processed dataset: {unified.count():,} rows")

    # Step 5: Write to MinIO
    write_processed_data(unified, bucket, processed_prefix)

    # Step 6: Write to PostgreSQL (sample for quick SQL queries)
    sample_for_db = unified.sample(fraction=0.1, seed=42)
    write_to_postgres(sample_for_db, "processed_telemetry", mode="overwrite")

    logger.info("Preprocessing pipeline complete.")
    return unified


if __name__ == "__main__":
    run_preprocessing_pipeline()
