#!/usr/bin/env python3

# Kaggle Dataset Downloader + Schema Validator



import argparse
import csv
import os
import shutil
import sys


# Expected schemas - must match preprocessing.read_raw_datasets()

EXPECTED_SCHEMAS: dict[str, dict] = {
    "trace_service_name.csv": {
        "columns": [
            "trace_id", "service_name",  "span_id", "parent_span_id",
            "namespace",  "pod_id", "node_id"
        ],
        "key_column": "span_id",
        "description": "Trace topology: which service handled each span",
    },
    "trace_response_times.csv": {
        "columns": [
            "span_id",  "response_time_ms",  "wait_time_ms", "processing_time_ms",
            "network_latency_ms"
        ],
        "key_column": "span_id",
    },
    "trace_request_times.csv": {
        "columns": [
            "span_id", "start_time",  "end_time", "duration_ms",
            "http_method",  "endpoint" ],
        "key_column": "span_id",
    },
    "resource_usage.csv": {
        "columns": [
            "pod_id", "timestamp",
            "cpu_usage_mcores",  "memory_usage_mb", "network_rx_bytes",
            "network_tx_bytes", "disk_io_read_bytes",
            "disk_io_write_bytes"
        ],
        "key_column": "pod_id",
    },
    "status_codes.csv": {
        "columns": [
            "span_id", "status_code", "error_message",
            "is_error"
        ],
        "key_column": "span_id",
    },
}

# Dataset identifier on Kaggle
KAGGLE_DATASET = "gagansomashekar/microservices-bottleneck-detection-dataset"



# Schema Validation

def validate_csv_schema(filepath: str, expected: dict) -> dict:
    """
    Validate a single CSV file against its expected schema.

    Returns:
        {
            "file": str,  "valid": bool,   "row_count": int,  "missing_columns": list[str],
            "extra_columns": list[str], "sample_row": list | None,
            "error": str | None,
        }
    """
    result = {"file": os.path.basename(filepath),
        "valid": False,  "row_count": 0,
        "missing_columns": [], "extra_columns": [],
        "sample_row": None, "error": None }

    if not os.path.isfile(filepath):
        result["error"] = f"File not found: {filepath}"
        return result

    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                result["error"] = "Empty file (no header row)"
                return result

            # Normalise: strip BOM, whitespace
            header = [h.strip().lstrip("\ufeff") for h in header]
            expected_cols = expected["columns"]

            result["missing_columns"] = [c for c in expected_cols if c not in header]
            result["extra_columns"] = [c for c in header if c not in expected_cols]

            # Count rows and grab a sample
            row_count = 0
            sample = None
            for row in reader:
                if row_count == 0:
                    sample = row
                row_count += 1

            result["row_count"] = row_count
            result["sample_row"] = sample

            # Valid if ALL expected columns are present (extra columns are OK)
            result["valid"] = len(result["missing_columns"]) == 0

    except Exception as e:
        result["error"] = str(e)

    return result


def validate_all_csvs(data_dir: str) -> tuple[bool, list[dict]]:
    """Validate all expected CSV files in a directory. Returns (all_valid, results)."""
    results = []
    all_valid = True

    for filename, schema in EXPECTED_SCHEMAS.items():
        filepath = os.path.join(data_dir, filename)
        result = validate_csv_schema(filepath, schema)
        results.append(result)
        if not result["valid"]:
            all_valid = False

    return all_valid, results


def print_validation_report(results: list[dict]) -> None:
    """Pretty-print the validation results."""
    print()
    print("=" * 45)
    print("  SCHEMA VALIDATION REPORT")
    print("=" * 55)

    for r in results:
        status = "[PASS]" if r["valid"] else "[FAIL]"
        print(f"\n  [{status}]  {r['file']}")
        print(f"Rows: {r['row_count']:,}")

        if r["error"]:
            print(f"Error occured: {r['error']}")

        if r["missing_columns"]:
            print(f" Missing columns: {', '.join(r['missing_columns'])}")

        if r["extra_columns"]:
            print(f" Extra columns:   {', '.join(r['extra_columns'])}")

        if r["sample_row"]:
            sample = ", ".join(str(v)[:40] for v in r["sample_row"][:5])
            print(f"           Sample: {sample}...")

    total = len(results)
    passed = sum(1 for r in results if r["valid"])
    print(f"\n  {'-' * 55}")
    print(f"  Result: {passed}/{total} files pass schema validation")
    print("=" * 65)
    print()



# Kaggle Download - via kagglehub (preferred, no API key)

def download_via_kagglehub(data_dir: str) -> bool:
    """
    Download using kagglehub. Returns True on success.
    kagglehub requires no authentication for public datasets.
    """
    try:
        import kagglehub
    except ImportError:
        return False

    print(f"Downloading dataset via kagglehub: {KAGGLE_DATASET}")
    print("(This may take a few minutes - the dataset is large)")
    print()

    try:
        download_path = kagglehub.dataset_download(KAGGLE_DATASET)
        print(f"  Downloaded to: {download_path}")
    except Exception as e:
        print(f"  [ERR] Download failed: {e}")
        print("  Try the Kaggle API method instead (see --help).")
        return False

    # Copy CSV files to data_dir
    return _copy_csv_files(download_path, data_dir)



# Kaggle Download - via kaggle CLI / API (requires kaggle.json)

def download_via_kaggle_api(data_dir: str) -> bool:
    """
    Download using the kaggle CLI. Returns True on success
    """
    import subprocess

    # Check if kaggle CLI is available
    try:
        result = subprocess.run(
            ["kaggle", "--version"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return False
    except FileNotFoundError:
        return False

    # Check if authenticated
    kaggle_json = os.path.expanduser("~/.kaggle/kaggle.json")
    if not os.path.isfile(kaggle_json):
        print("Error kaggle CLI found but no ~/.kaggle/kaggle.json")
        print("-> Sign in at https://www.kaggle.com")
        print(" => Go to Settings -> API -> Create New Token ")
        print("-> Place the downloaded kaggle.json at ~/.kaggle/")
        return False

    print(f"Downloading dataset via Kaggle API: {KAGGLE_DATASET}")
    print()

    # Download to a temp location
    tmp_dir = os.path.join(data_dir, ".kaggle_tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        subprocess.run(
            [
                "kaggle", "datasets",  "download",  KAGGLE_DATASET,
                "-p",    tmp_dir, "--unzip",
            ],
            check=True,
        )
        print("  [OK] Download complete")
    except subprocess.CalledProcessError as e:
        print(f"  [ERR] Download failed: {e}")
        return False

    return _copy_csv_files(tmp_dir, data_dir)



# Manual fallback - print instructions

def print_manual_instructions(data_dir: str) -> None:
    """Print step-by-step manual download instructions."""
    print()
    print("=" * 45)
    print("  MANUAL DOWNLOAD INSTRUCTIONS")
    print("=" * 55)
    print()
    print("Neither kagglehub nor the Kaggle CLI is available.")
    print()
    print("  To download the dataset manually:")
    print()
    print(f" 1. Visit: https://www.kaggle.com/datasets/{KAGGLE_DATASET}")
    print(" 2. Click 'Download' (you'll need a free Kaggle account)")
    print("  3. Extract the ZIP archive")
    print(f"4. Copy these 5 CSV files into: {data_dir}/")
    print()
    for filename, schema in EXPECTED_SCHEMAS.items():
        print(f"     * {filename}  ({', '.join(schema['columns'][:4])}...)")
    print()
    print("  5. Re-run: python scripts/download_kaggle_dataset.py --validate-only")
    print()
    print(" Alternatively, install kagglehub:")
    print("pip install kagglehub")
    print("python scripts/download_kaggle_dataset.py")
    print()
    print("=" * 45)



# Helpers

def _copy_csv_files(src_dir: str, dst_dir: str) -> bool:
    """Copy CSV files from src_dir to dst_dir. Returns True if any were copied."""
    os.makedirs(dst_dir, exist_ok=True)
    copied = 0

    for filename in EXPECTED_SCHEMAS:
        # Search recursively for the file (kagglehub may nest it)
        found = _find_file(src_dir, filename)
        if found:
            dst = os.path.join(dst_dir, filename)
            shutil.copy2(found, dst)
            size_mb = os.path.getsize(dst) / (1024 * 1024)
            print(f"  [OK] {filename}  ({size_mb:.1f} MB)")
            copied += 1

    if copied == 0:
        print(f"  [ERR] No CSV files found in {src_dir}")
        print(f"  Contents: {os.listdir(src_dir)[:20]}")
        return False

    print(f"  -> {copied} files copied to {dst_dir}/")
    return True


def _find_file(root: str, filename: str) -> str | None:
    """Recursively search for a file by name under root."""
    for dirpath, _, filenames in os.walk(root):
        if filename in filenames:
            return os.path.join(dirpath, filename)
    return None



# Main

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download and validate the Kaggle Microservices Bottleneck Detection Dataset",
    )
    parser.add_argument(
        "--data-dir", default="./data",
        help="Directory to store/check CSV files (default: ./data)",
    )
    parser.add_argument(
        "--validate-only", action="store_true",
        help="Only validate existing CSV files, do not download",
    )
    parser.add_argument(
        "--force",  action="store_true",
        help="Re-download even if files already exist",
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    os.makedirs(data_dir, exist_ok=True)

    print()
    print("=" * 45)
    print(" Kaggle Dataset Downloader")
    print(f" Dataset: {KAGGLE_DATASET}")
    print(f"Target: {data_dir}/")
    print("=" * 45)

    #step 1 Check if files already exist
    existing = [f for f in EXPECTED_SCHEMAS if os.path.isfile(os.path.join(data_dir, f))]
    if existing and not args.force and not args.validate_only:
        print(f"\n  {len(existing)} CSV files already exist in {data_dir}/")
        print("  Run with --force to re-download, or --validate-only to check them.")
        print()
        _, results = validate_all_csvs(data_dir)
        print_validation_report(results)
        return 0

    # Step 2 - Download (unless validate-only)
    if not args.validate_only:
        print("\n  Attempting download....")
        downloaded = False

        # Try kagglehub first (simplest, no auth)
        print("\n  [Method 1] Trying kagglehub (no API key needed)....")
        downloaded = download_via_kagglehub(data_dir)

        # Fall back to kaggle CLI
        if not downloaded:
            print("\n  [Method 2] Trying Kaggle CLI (requires kaggle.json)....")
            downloaded = download_via_kaggle_api(data_dir)

        # Manual instructions if both fail
        if not downloaded:
            print_manual_instructions(data_dir)
            return 1

    # Step 3 - Validate schemas
    all_valid, results = validate_all_csvs(data_dir)
    print_validation_report(results)

    if not all_valid:
        print("[WARN] Some files do not match the expected schema.")
        print(" The pipeline's sample data generator will be used as fallback.")
        print(" Check the column names above against the expected schemas")
        return 2

    # Step 4 - Summary
    total_rows = sum(r["row_count"] for r in results)
    print("[OK] Dataset is ready and schema-validated.")
    print(f"   {len(results)} files, {total_rows:,} total rows.")
    print()
    print("Next: run the ingestion pipeline to upload to MinIO:")
    print("  python -m modules.ingestion")
    print()
    return 0

#main method
if __name__ == "__main__":
    sys.exit(main())
