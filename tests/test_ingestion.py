# ============================================================
# Unit Tests — Module 1: Data Ingestion
# ============================================================
# Tests: generate_sample_data, load_config, get_minio_client
# ============================================================

import os
import csv
import tempfile
import pytest

from modules.ingestion import (
    generate_sample_data,
    load_config,
    get_minio_client,
)


# ============================================================
# generate_sample_data
# ============================================================
class TestGenerateSampleData:
    """Tests for the generate_sample_data() function."""

    @pytest.fixture(scope="class")
    def generated_files(self, tmp_path_factory):
        """Generate a small 100-row dataset into a temp directory (shared across class)."""
        tmpdir = tmp_path_factory.mktemp("ingestion_data")
        files = generate_sample_data(num_rows=100, output_dir=str(tmpdir))
        return files, str(tmpdir)

    def test_returns_five_files(self, generated_files):
        """Should return exactly 5 file paths."""
        files, _ = generated_files
        assert len(files) == 5, f"Expected 5 files, got {len(files)}"

    def test_all_files_exist_on_disk(self, generated_files):
        """Every returned path should point to an existing file."""
        files, _ = generated_files
        for f in files:
            assert os.path.isfile(f), f"File does not exist: {f}"

    def test_all_files_have_csv_extension(self, generated_files):
        """All generated files should have .csv extension."""
        files, _ = generated_files
        for f in files:
            assert f.endswith(".csv"), f"Not a CSV: {f}"

    def test_trace_service_name_has_correct_columns(self, generated_files):
        """First file must have the expected header columns."""
        files, _ = generated_files
        service_file = [f for f in files if "trace_service_name" in f][0]
        with open(service_file, "r") as fh:
            reader = csv.reader(fh)
            header = next(reader)
        expected = ["trace_id", "service_name", "span_id", "parent_span_id",
                    "namespace", "pod_id", "node_id"]
        assert header == expected

    def test_trace_response_times_has_correct_columns(self, generated_files):
        """Second file must have the expected header columns."""
        files, _ = generated_files
        response_file = [f for f in files if "trace_response_times" in f][0]
        with open(response_file, "r") as fh:
            reader = csv.reader(fh)
            header = next(reader)
        expected = ["span_id", "response_time_ms", "wait_time_ms",
                    "processing_time_ms", "network_latency_ms"]
        assert header == expected

    def test_trace_request_times_has_correct_columns(self, generated_files):
        """Third file must have the expected header columns."""
        files, _ = generated_files
        request_file = [f for f in files if "trace_request_times" in f][0]
        with open(request_file, "r") as fh:
            reader = csv.reader(fh)
            header = next(reader)
        expected = ["span_id", "start_time", "end_time", "duration_ms",
                    "http_method", "endpoint"]
        assert header == expected

    def test_resource_usage_has_correct_columns(self, generated_files):
        """Fourth file must have the expected header columns."""
        files, _ = generated_files
        resource_file = [f for f in files if "resource_usage" in f][0]
        with open(resource_file, "r") as fh:
            reader = csv.reader(fh)
            header = next(reader)
        expected = ["pod_id", "timestamp", "cpu_usage_mcores", "memory_usage_mb",
                    "network_rx_bytes", "network_tx_bytes",
                    "disk_io_read_bytes", "disk_io_write_bytes"]
        assert header == expected

    def test_status_codes_has_correct_columns(self, generated_files):
        """Fifth file must have the expected header columns."""
        files, _ = generated_files
        status_file = [f for f in files if "status_codes" in f][0]
        with open(status_file, "r") as fh:
            reader = csv.reader(fh)
            header = next(reader)
        expected = ["span_id", "status_code", "error_message", "is_error"]
        assert header == expected

    def test_generates_exact_row_count(self, generated_files):
        """Each file should have exactly num_rows data rows (+ 1 header)."""
        files, _ = generated_files
        for f in files:
            with open(f, "r") as fh:
                reader = csv.reader(fh)
                header = next(reader)  # skip header
                row_count = sum(1 for _ in reader)
            assert row_count == 100, f"{os.path.basename(f)} has {row_count} rows, expected 100"

    def test_span_ids_are_sequential(self, generated_files):
        """Span IDs in service_name file should be span-0000000000 through span-N."""
        files, _ = generated_files
        service_file = [f for f in files if "trace_service_name" in f][0]
        with open(service_file, "r") as fh:
            reader = csv.reader(fh)
            next(reader)  # header
            first_span = next(reader)[2]  # span_id is column index 2
        assert first_span == "span-0000000000"

    def test_empty_parent_span_ids_exist(self, generated_files):
        """~30% of rows should have empty parent_span_id (root spans)."""
        files, _ = generated_files
        service_file = [f for f in files if "trace_service_name" in f][0]
        empty_count = 0
        with open(service_file, "r") as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                if row[3] == "":  # parent_span_id is column index 3
                    empty_count += 1
        assert empty_count > 0, "No empty parent_span_ids — root spans missing"

    def test_status_codes_has_failures(self, generated_files):
        """~8% of status codes should be 5xx errors."""
        files, _ = generated_files
        status_file = [f for f in files if "status_codes" in f][0]
        error_count = 0
        with open(status_file, "r") as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                if int(row[3]) == 1:  # is_error column
                    error_count += 1
        assert error_count > 0, "No errors generated — failure simulation broken"

    def test_csv_values_are_valid(self, generated_files):
        """Spot-check: all rows in all files should have the correct number of columns."""
        files, _ = generated_files
        for f in files:
            with open(f, "r") as fh:
                reader = csv.reader(fh)
                header = next(reader)
                expected_cols = len(header)
                for i, row in enumerate(reader, start=1):
                    assert len(row) == expected_cols, (
                        f"{os.path.basename(f)} row {i}: expected {expected_cols} cols, "
                        f"got {len(row)}"
                    )

    def test_numeric_values_are_positive(self, generated_files):
        """All response_time_ms values should be positive floats."""
        files, _ = generated_files
        response_file = [f for f in files if "trace_response_times" in f][0]
        with open(response_file, "r") as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                rt = float(row[1])  # response_time_ms is col index 1
                assert rt > 0, f"Non-positive response_time_ms: {rt}"

    def test_creates_output_directory(self):
        """Should create output_dir if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_dir = os.path.join(tmpdir, "nested", "data")
            assert not os.path.exists(nested_dir)
            files = generate_sample_data(num_rows=10, output_dir=nested_dir)
            assert os.path.isdir(nested_dir)
            assert len(files) == 5

    def test_single_row(self):
        """Should handle num_rows=1 without errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            files = generate_sample_data(num_rows=1, output_dir=tmpdir)
            assert len(files) == 5
            for f in files:
                with open(f, "r") as fh:
                    reader = csv.reader(fh)
                    next(reader)
                    rows = list(reader)
                    assert len(rows) == 1

    def test_large_row_count(self):
        """Should handle num_rows=1000 without errors (quick smoke test)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            files = generate_sample_data(num_rows=1000, output_dir=tmpdir)
            service_file = [f for f in files if "trace_service_name" in f][0]
            with open(service_file, "r") as fh:
                reader = csv.reader(fh)
                next(reader)
                row_count = sum(1 for _ in reader)
            assert row_count == 1000


# ============================================================
# load_config
# ============================================================
class TestLoadConfig:
    """Tests for the load_config() function."""

    def test_loads_valid_yaml(self):
        """Should parse config/config.yaml without error."""
        config = load_config("config/config.yaml")
        assert isinstance(config, dict)
        assert len(config) > 0

    def test_contains_expected_sections(self):
        """Config must contain sections for each pipeline module."""
        config = load_config("config/config.yaml")
        expected_sections = [
            "preprocessing",
            "cross_service_analysis",
            "failure_detection",
            "scalability",
        ]
        for section in expected_sections:
            assert section in config, f"Missing config section: {section}"

    def test_scalability_has_data_sizes(self):
        """Scalability config must include data_sizes list."""
        config = load_config("config/config.yaml")
        scal_cfg = config.get("scalability", {})
        assert "data_sizes" in scal_cfg
        assert isinstance(scal_cfg["data_sizes"], list)
        assert len(scal_cfg["data_sizes"]) > 0

    def test_cross_service_has_window_param(self):
        """Cross-service config must have propagation_time_window_seconds."""
        config = load_config("config/config.yaml")
        cs_cfg = config.get("cross_service_analysis", {})
        assert "propagation_time_window_seconds" in cs_cfg

    def test_raises_on_missing_file(self):
        """Should raise FileNotFoundError for non-existent config path."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent/config.yaml")


# ============================================================
# get_minio_client
# ============================================================
class TestGetMinioClient:
    """Tests for the get_minio_client() function."""

    def test_returns_boto3_client(self):
        """Should return an S3 client object."""
        client = get_minio_client()
        # boto3 S3 client exposes the meta endpoint_resolver
        assert hasattr(client, "meta")

    def test_client_uses_s3_service(self):
        """Client should be for the 's3' service."""
        client = get_minio_client()
        assert client.meta.service_model.service_name == "s3"

    def test_multiple_calls_return_independent_clients(self):
        """Each call should create a fresh client (not a singleton)."""
        c1 = get_minio_client()
        c2 = get_minio_client()
        assert c1 is not c2
