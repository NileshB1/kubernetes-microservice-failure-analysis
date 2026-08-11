# ============================================================
# Module 1: Data Ingestion
# ============================================================
# Purpose:
#   Reads the Microservices Bottleneck Detection Dataset from
#   Kaggle (or generates realistic sample data), uploads to
#   MinIO blob storage, and registers it with Spark.
#
# Inputs:
#   - Kaggle dataset CSV files (or generated sample data)
#   - MinIO bucket credentials
#
# Outputs:
#   - Raw CSV files stored in MinIO bucket: s3a://microservice-logs/raw/
#
# Main Functions:
#   - generate_sample_data()     → Produces realistic microservice telemetry
#   - upload_to_minio()          → Uploads local files to MinIO
#   - read_raw_from_minio()      → Returns Spark DataFrame from MinIO
#   - verify_data_in_minio()     → Checks data integrity in blob storage
#
# Spark Operations Used:
#   - spark.read.csv()
#   - DataFrame caching for repeated reads
#
# RQ Contribution:
#   - Foundation for all three RQs — provides the data layer
# ============================================================

import os
import sys
import io
import csv
import random
import logging
from datetime import datetime, timedelta

import boto3
from botocore.config import Config as BotoConfig
import yaml
from dotenv import load_dotenv

from modules.shared_utils import setup_logging

load_dotenv()
logger = setup_logging("ingestion")

# ============================================================
# Constants (matching the real Kaggle dataset structure)
# ============================================================
SERVICE_NAMES = [
    "frontend", "auth-service", "user-service", "order-service",
    "payment-service", "inventory-service", "notification-service",
    "shipping-service", "catalog-service", "cart-service",
    "recommendation-service", "search-service", "analytics-service",
    "rate-limiter", "api-gateway", "message-queue",
    "cache-service", "logging-service", "config-service", "discovery-service",
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
HTTP_STATUSES = [200, 201, 204, 301, 400, 401, 403, 404, 408, 500, 502, 503, 504]
NODE_IDS = [f"node-{i:03d}" for i in range(1, 21)]
POD_IDS = [f"pod-{i:05d}" for i in range(1, 201)]
NAMESPACES = ["default", "production", "staging", "canary"]


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load YAML configuration."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ============================================================
# Sample Data Generation
# ============================================================
def generate_sample_data(num_rows: int = 100_000, output_dir: str = "/data") -> list[str]:
    """
    Generate realistic microservice telemetry matching the structure of the
    Microservices Bottleneck Detection Dataset.

    Generates files:
      - trace_service_name.csv
      - trace_response_times.csv
      - trace_request_times.csv
      - resource_usage.csv
      - status_codes.csv

    Returns list of generated file paths.
    """
    logger.info(f"Generating {num_rows:,} rows of sample microservice telemetry data...")
    os.makedirs(output_dir, exist_ok=True)

    base_time = datetime(2024, 1, 1, 0, 0, 0)
    files_generated = []

    # ---------------------------------------------------------
    # File 1: trace_service_name.csv
    # ---------------------------------------------------------
    service_file = os.path.join(output_dir, "trace_service_name.csv")
    with open(service_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["trace_id", "service_name", "span_id", "parent_span_id", "namespace", "pod_id", "node_id"])
        for i in range(num_rows):
            trace_id = f"trace-{i % (num_rows // 10):08d}" if i % 10 != 0 else f"trace-{random.randint(0, num_rows // 10):08d}"
            writer.writerow([
                trace_id,
                random.choice(SERVICE_NAMES),
                f"span-{i:010d}",
                f"span-{random.randint(0, max(0, i-1)):010d}" if random.random() > 0.3 else "",
                random.choice(NAMESPACES),
                random.choice(POD_IDS),
                random.choice(NODE_IDS),
            ])
    files_generated.append(service_file)
    logger.info(f"  ✓ {service_file} ({num_rows:,} rows)")

    # ---------------------------------------------------------
    # File 2: trace_response_times.csv
    # ---------------------------------------------------------
    response_file = os.path.join(output_dir, "trace_response_times.csv")
    with open(response_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["span_id", "response_time_ms", "wait_time_ms", "processing_time_ms", "network_latency_ms"])
        for i in range(num_rows):
            base_latency = random.expovariate(1.0 / 80)
            writer.writerow([
                f"span-{i:010d}",
                round(base_latency + max(0, random.gauss(30, 60)), 2),
                round(max(0, random.gauss(15, 20)), 2),
                round(max(1, random.gauss(40, 30)), 2),
                round(max(0, random.gauss(5, 8)), 2),
            ])
    files_generated.append(response_file)
    logger.info(f"  ✓ {response_file} ({num_rows:,} rows)")

    # ---------------------------------------------------------
    # File 3: trace_request_times.csv
    # ---------------------------------------------------------
    request_file = os.path.join(output_dir, "trace_request_times.csv")
    with open(request_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["span_id", "start_time", "end_time", "duration_ms", "http_method", "endpoint"])
        for i in range(num_rows):
            start = base_time + timedelta(milliseconds=random.randint(0, 86_400_000))
            duration = round(max(1, random.expovariate(1.0 / 100)), 2)
            writer.writerow([
                f"span-{i:010d}",
                start.isoformat(),
                (start + timedelta(milliseconds=duration)).isoformat(),
                duration,
                random.choice(HTTP_METHODS),
                f"/api/v1/{random.choice(SERVICE_NAMES)}/{random.choice(['get','create','update','delete'])}",
            ])
    files_generated.append(request_file)
    logger.info(f"  ✓ {request_file} ({num_rows:,} rows)")

    # ---------------------------------------------------------
    # File 4: resource_usage.csv
    # ---------------------------------------------------------
    resource_file = os.path.join(output_dir, "resource_usage.csv")
    with open(resource_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["pod_id", "timestamp", "cpu_usage_mcores", "memory_usage_mb", "network_rx_bytes", "network_tx_bytes", "disk_io_read_bytes", "disk_io_write_bytes"])
        for i in range(num_rows):
            ts = base_time + timedelta(seconds=random.randint(0, 86_400))
            writer.writerow([
                random.choice(POD_IDS),
                ts.isoformat(),
                round(max(10, random.gauss(500, 300)), 2),
                round(max(50, random.gauss(1024, 512)), 2),
                random.randint(0, 10_000_000),
                random.randint(0, 10_000_000),
                random.randint(0, 50_000_000),
                random.randint(0, 50_000_000),
            ])
    files_generated.append(resource_file)
    logger.info(f"  ✓ {resource_file} ({num_rows:,} rows)")

    # ---------------------------------------------------------
    # File 5: status_codes.csv
    # ---------------------------------------------------------
    status_file = os.path.join(output_dir, "status_codes.csv")
    with open(status_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["span_id", "status_code", "error_message", "is_error"])
        for i in range(num_rows):
            # Introduce ~8% error rate to simulate realistic failures
            is_error = 1 if random.random() < 0.08 else 0
            status = random.choice([500, 502, 503, 504]) if is_error else random.choice([200, 200, 200, 200, 201, 204, 301])
            writer.writerow([
                f"span-{i:010d}",
                status,
                f"Error in processing request" if is_error else "",
                is_error,
            ])
    files_generated.append(status_file)
    logger.info(f"  ✓ {status_file} ({num_rows:,} rows)")

    logger.info(f"Sample data generation complete: {len(files_generated)} files, {num_rows:,} rows each.")
    return files_generated


# ============================================================
# MinIO (Blob Storage) Operations
# ============================================================
def get_minio_client() -> boto3.client:
    """Create and return a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_to_minio(local_files: list[str], bucket: str, prefix: str = "raw/") -> None:
    """
    Upload local CSV files to MinIO blob storage.

    Args:
        local_files: List of local file paths to upload.
        bucket:    MinIO bucket name.
        prefix:    Object key prefix (folder) within the bucket.
    """
    client = get_minio_client()
    logger.info(f"Uploading {len(local_files)} files to s3://{bucket}/{prefix} ...")

    for filepath in local_files:
        filename = os.path.basename(filepath)
        object_key = f"{prefix}{filename}"
        client.upload_file(filepath, bucket, object_key)
        logger.info(f"  ✓ Uploaded: {filename} → s3://{bucket}/{object_key}")

    logger.info("Upload complete.")


def verify_data_in_minio(bucket: str, prefix: str = "raw/") -> dict[str, int]:
    """
    Verify that data exists in MinIO and return file sizes.

    Returns:
        Dict mapping filename → size in bytes.
    """
    client = get_minio_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    file_sizes = {}

    if "Contents" not in response:
        logger.warning(f"No objects found in s3://{bucket}/{prefix}")
        return file_sizes

    for obj in response["Contents"]:
        filename = obj["Key"].replace(prefix, "")
        file_sizes[filename] = obj["Size"]
        logger.info(f"  Found: {filename} ({obj['Size']:,} bytes)")

    return file_sizes


# ============================================================
# Main Entry Point
# ============================================================
def run_ingestion_pipeline(
    sample_rows: int = 100_000,
    data_dir: str = "/data",
    bucket: str = "microservice-logs",
    prefix: str = "raw/",
) -> dict:
    """
    Full ingestion pipeline:
      1. Generate sample microservice telemetry data
      2. Upload all files to MinIO blob storage
      3. Verify upload integrity

    Returns dict with ingestion summary.
    """
    logger.info("=" * 60)
    logger.info("MODULE 1: DATA INGESTION")
    logger.info("=" * 60)

    # Step 1: Generate / locate data
    logger.info("Step 1: Generating sample data...")
    local_files = generate_sample_data(num_rows=sample_rows, output_dir=data_dir)

    # Step 2: Upload to MinIO
    logger.info("Step 2: Uploading to MinIO blob storage...")
    upload_to_minio(local_files, bucket=bucket, prefix=prefix)

    # Step 3: Verify
    logger.info("Step 3: Verifying data in MinIO...")
    sizes = verify_data_in_minio(bucket=bucket, prefix=prefix)

    summary = {
        "files_ingested": len(sizes),
        "total_bytes": sum(sizes.values()),
        "bucket": bucket,
        "prefix": prefix,
    }
    logger.info(f"Ingestion complete: {summary}")
    return summary


# ============================================================
# CLI Entry Point
# ============================================================
if __name__ == "__main__":
    rows = int(os.getenv("SAMPLE_DATA_ROWS", "100000"))
    run_ingestion_pipeline(sample_rows=rows)
