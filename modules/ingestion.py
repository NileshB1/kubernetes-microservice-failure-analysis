
# Module 1: Data Ingestion
#
# Main Functions:
# - run_ingestion_pipeline() -> Acquire, upload, normalise
# - upload_tree_to_minio() -> Uploads preserving directory structure
# -normalise_source_in_blob() -> Spark: source in blob -> canonical in blob
# - generate_sample_data() -> Offline fallback telemetry
# - verify_data_in_minio() -> Checks data integrity in blob storage


import csv
import os
import random
from datetime import datetime, timedelta

import boto3
import yaml
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv

from modules.shared_utils import setup_logging

load_dotenv()
logger = setup_logging("ingestion")


# Constants (matching the real Kaggle dataset structure)

SERVICE_NAMES = [
    "frontend", "auth-service", "user-service",
    "order-service", "payment-service", "inventory-service", "notification-service",
    "shipping-service", "catalog-service",
    "cart-service", "recommendation-service", "search-service",
    "analytics-service", "rate-limiter", "api-gateway", "message-queue",
    "cache-service", "logging-service", "config-service","discovery-service",
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
HTTP_STATUSES = [200, 201, 204, 301, 400, 401, 403, 404, 408, 500, 502, 503, 504]
NODE_IDS = [f"node-{i:03d}" for i in range(1, 21)]
POD_IDS = [f"pod-{i:05d}" for i in range(1, 201)]
NAMESPACES = ["default", "production", "staging", "canary"]


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load YAML configuration."""
    with open(config_path) as f:
        return yaml.safe_load(f)



# Sample Data Generation

def generate_sample_data(num_rows: int = 100_000, output_dir: str = "/data") -> list[str]:
    """
    Generate realistic microservice telemetry matching the structure of the
    Microservices Bottleneck Detection Dataset.

    Generates files:
      - trace_service_name.csv, - trace_response_times.csv
      - trace_request_times.csv, - resource_usage.csv
      - status_codes.csv

    Returns list of generated file paths.
    """
    logger.info(f"Generating {num_rows:,} rows of sample microservice telemetry data...")
    os.makedirs(output_dir, exist_ok=True)

    base_time = datetime(2024, 1, 1, 0, 0, 0)
    files_generated = []


    # File 1: trace_service_name.csv

    service_file = os.path.join(output_dir, "trace_service_name.csv")
    with open(service_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["trace_id", "service_name", "span_id", "parent_span_id", "namespace", "pod_id", "node_id"]
        )
        for i in range(num_rows):
            trace_id = (
                f"trace-{i % (num_rows // 10):08d}"
                if i % 10 != 0
                else f"trace-{random.randint(0, num_rows // 10):08d}"
            )
            writer.writerow(
                [
                    trace_id,
                    random.choice(SERVICE_NAMES),  f"span-{i:010d}",
                    f"span-{random.randint(0, max(0, i-1)):010d}" if random.random() > 0.3 else "",
                    random.choice(NAMESPACES), random.choice(POD_IDS), random.choice(NODE_IDS),
                ]
            )
    files_generated.append(service_file)
    logger.info(f"  [ok] {service_file} ({num_rows:,} rows)")

    
    # File 2: trace_response_times.csv
    
    response_file = os.path.join(output_dir, "trace_response_times.csv")
    with open(response_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["span_id", "response_time_ms", "wait_time_ms", "processing_time_ms", "network_latency_ms"]
        )
        for i in range(num_rows):
            base_latency = random.expovariate(1.0 / 80)
            writer.writerow(
                [
                    f"span-{i:010d}",  round(base_latency + max(0, random.gauss(30, 60)), 2),
                    round(max(0, random.gauss(15, 20)), 2), round(max(1, random.gauss(40, 30)), 2),
                    round(max(0, random.gauss(5, 8)), 2),
                ]
            )
    files_generated.append(response_file)
    logger.info(f"  [ok] {response_file} ({num_rows:,} rows)")

    # File 3: trace_request_times.csv
    request_file = os.path.join(output_dir, "trace_request_times.csv")
    with open(request_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["span_id", "start_time", "end_time", "duration_ms", "http_method", "endpoint"])
        for i in range(num_rows):
            start = base_time + timedelta(milliseconds=random.randint(0, 86_400_000))
            duration = round(max(1, random.expovariate(1.0 / 100)), 2)
            writer.writerow(
                [
                    f"span-{i:010d}",    start.isoformat(),
                    (start + timedelta(milliseconds=duration)).isoformat(),
                    duration,  random.choice(HTTP_METHODS),
                    f"/api/v1/{random.choice(SERVICE_NAMES)}/{random.choice(['get','create','update','delete'])}",
                ]
            )
    files_generated.append(request_file)
    logger.info(f"  [ok] {request_file} ({num_rows:,} rows)")


    # File 4: resource_usage.csv
    resource_file = os.path.join(output_dir, "resource_usage.csv")
    with open(resource_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "pod_id", "timestamp",
                "cpu_usage_mcores", "memory_usage_mb",  "network_rx_bytes",
                "network_tx_bytes", "disk_io_read_bytes",
                "disk_io_write_bytes",
            ]
        )
        for _ in range(num_rows):
            ts = base_time + timedelta(seconds=random.randint(0, 86_400))
            writer.writerow(
                [
                    random.choice(POD_IDS),
                    ts.isoformat(), round(max(10, random.gauss(500, 300)), 2),
                    round(max(50, random.gauss(1024, 512)), 2),
                    random.randint(0, 10_000_000), random.randint(0, 10_000_000),
                    random.randint(0, 50_000_000),
                    random.randint(0, 50_000_000)
                ]
            )
    files_generated.append(resource_file)
    logger.info(f"  [ok] {resource_file} ({num_rows:,} rows)")

    
    # File 5: status_codes.csv
    status_file = os.path.join(output_dir, "status_codes.csv")
    with open(status_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["span_id", "status_code", "error_message", "is_error"])
        for i in range(num_rows):
            # Introduce ~8% error rate to simulate realistic failures
            is_error = 1 if random.random() < 0.08 else 0
            status = (
                random.choice([500, 502, 503, 504])
                if is_error
                else random.choice([200, 200, 200, 200, 201, 204, 301])
            )
            writer.writerow(
                [
                    f"span-{i:010d}",
                    status, "Error in processing request" if is_error else "",
                    is_error,
                ]
            )
    files_generated.append(status_file)
    logger.info(f"  [ok] {status_file} ({num_rows:,} rows)")

    logger.info(f"Sample data generation complete: {len(files_generated)} files, {num_rows:,} rows each.")
    return files_generated



# MinIO (Blob Storage) Operations

def get_minio_client() -> boto3.client:
    """Create and return a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3", endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=BotoConfig(signature_version="s3v4"), region_name="us-east-1",
    )


def upload_to_minio(local_files: list[str], bucket: str, prefix: str = "raw/") -> None:
    """
    Upload local CSV files to MinIO blob storage, flattened into prefix

    """
    client = get_minio_client()
    logger.info(f"Uploading {len(local_files)} files to s3://{bucket}/{prefix} ...")

    for filepath in local_files:
        filename = os.path.basename(filepath)
        object_key = f"{prefix}{filename}"
        client.upload_file(filepath, bucket, object_key)
        logger.info(f"Uploaded: {filename} -> s3://{bucket}/{object_key}")

    logger.info("Upload complete.")


def upload_tree_to_minio(local_root: str, bucket: str, prefix: str = "source/") -> int:
    """
    Upload a directory tree to blob storage, preserving its structure
    """
    from pathlib import Path

    client = get_minio_client()
    root = Path(local_root)
    files = sorted(p for p in root.rglob("*") if p.is_file())

    if not files:
        raise FileNotFoundError(f"No files to upload under {root}")

    logger.info(f"Uploading {len(files)} source files to s3://{bucket}/{prefix} ....")
    for index, path in enumerate(files, start=1):
        # as_posix keeps forward slashes in the object key on Windows.
        relative = path.relative_to(root).as_posix()
        client.upload_file(str(path), bucket, f"{prefix}{relative}")
        if index % 25 == 0 or index == len(files):
            logger.info(f"  {index}/{len(files)} objects")

    logger.info("Source upload complete.....")
    return len(files)


def verify_data_in_minio(bucket: str, prefix: str = "raw/") -> dict[str, int]:
    """
    Verify that data exists in MinIO and return file sizes....
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



# Main Entry Point

SOURCE_PREFIX = "source/"


def run_ingestion_pipeline(
    data_dir: str = "/data",  bucket: str = "microservice-logs",
    prefix: str = "raw/",  capture_dates: tuple[str, ...] | None = None,
    synthetic: bool = False, sample_rows: int = 100_000
):
    """
    Get the real source dataset into blob storage and normalise it
    """
    logger.info("=" * 60)
    logger.info("MODULE 1: DATA INGESTION")
    logger.info("=" * 60)

    if synthetic:
        return _run_synthetic_ingestion(sample_rows, data_dir, bucket, prefix)

    from modules.dataset_acquisition import (
        BASELINE_ROOT, FAULT_ROOT,
        dataset_is_present, download_dataset,
        download_fault_lists, validate_local_dataset
    )

    # Step 1:Acquire the real datase
    if dataset_is_present(data_dir):
        logger.info("Step 1: Source dataset already present in %s", data_dir)
    else:
        logger.info("Step 1: Acquiring source dataset via the GitHub API ....")
        download_dataset(
            data_dir=data_dir, kinds=("trace", "metric"), 
            capture_dates=capture_dates, roots=(BASELINE_ROOT, FAULT_ROOT)
        )
        download_fault_lists(data_dir, capture_dates=capture_dates)

    validate_local_dataset(data_dir)

    # Step 2: the source tree goes to blob storage untouched
    logger.info("Step 2: Uploading source dataset to blob storage ...")
    objects = upload_tree_to_minio(data_dir, bucket=bucket, prefix=SOURCE_PREFIX)

    # Step 3: Spark reads the source back out of blob and normalises it
    logger.info("Step 3: Normalising source into the canonical schema (Spark) ....")
    counts = normalise_source_in_blob(bucket=bucket, prefix=prefix)

    sizes = verify_data_in_minio(bucket=bucket, prefix=prefix)
    summary = {
        "data_source": "nezha-real", "source_objects": objects,
        "canonical_rows": counts, "files_ingested": len(sizes),
        "bucket": bucket,  "prefix": prefix,
    }
    logger.info(f"ingestion completed, summary is: {summary}")
    return summary


def normalise_source_in_blob(
    bucket: str = "microservice-logs",
    prefix: str = "raw/", roots: tuple[str, ...] = ("construct_data", "rca_data")
):
    """
    Spark job: source in blob storage -> canonical datasets in blob storage
    """
    from modules.dataset_acquisition import load_fault_labels
    from modules.shared_utils import create_spark_session, write_to_postgres
    from modules.source_adapter import (
        build_canonical_datasets, build_fault_ground_truth,
        write_canonical_datasets
    )

    spark = create_spark_session("SourceNormalisation")
    source_path = f"s3a://{bucket}/{SOURCE_PREFIX.rstrip('/')}"
    output_path = f"s3a://{bucket}/{prefix.rstrip('/')}"

    datasets = build_canonical_datasets(spark, source_path, roots=roots)
    counts = write_canonical_datasets(datasets, output_path)

    ground_truth = build_fault_ground_truth(spark, load_fault_labels(os.getenv("DATA_DIR", "/data")))
    if ground_truth.count():
        write_to_postgres(ground_truth, "fault_injections")

    return counts


def _run_synthetic_ingestion(sample_rows: int, data_dir: str, bucket: str, prefix: str) -> dict:
    """Offline fallback: generate telemetry instead of acquiring it."""
    logger.warning(
        "Running with SYNTHETIC data. This path exists for offline demos and tests. "
        "the analysis results it produces are not measurements."
    )
    local_files = generate_sample_data(num_rows=sample_rows, output_dir=data_dir)
    upload_to_minio(local_files, bucket=bucket, prefix=prefix)
    sizes = verify_data_in_minio(bucket=bucket, prefix=prefix)

    summary = {
        "data_source": "synthetic",  "files_ingested": len(sizes),
        "total_bytes": sum(sizes.values()),
        "bucket": bucket, "prefix": prefix,
    }
    logger.info(f"Ingestion complete: {summary}")
    return summary



# CLI Entry Point
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Module 1 - data ingestion")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "/data"))
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "microservice-logs"))
    parser.add_argument(
        "--dates",  nargs="*",
        # Falls back to DATASET_CAPTURE_DATES so Compose can bound the
        # download without overriding the command line.
        default=[d for d in os.getenv("DATASET_CAPTURE_DATES", "").split(",") if d] or None,
        help="Restrict acquisition to these capture dates "
        "(default: DATASET_CAPTURE_DATES, else every published date)",
    )
    parser.add_argument(
        "--synthetic", action="store_true", help="Generate telemetry instead of acquiring the real dataset"
    )
    parser.add_argument(
        "--sample-rows",
        type=int, default=int(os.getenv("SAMPLE_DATA_ROWS", "100000")),
        help="Row count for --synthetic mode"
    )
    args = parser.parse_args()

    run_ingestion_pipeline(
        data_dir=args.data_dir, bucket=args.bucket,
        capture_dates=tuple(args.dates) if args.dates else None,
        synthetic=args.synthetic, sample_rows=args.sample_rows,
    )
