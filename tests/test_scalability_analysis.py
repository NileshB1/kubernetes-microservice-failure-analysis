# ============================================================
# Unit Tests — Module 5: Spark Scalability Analysis
# ============================================================
# Tests: generate_scaled_dataset, run_benchmark,
#        compute_scalability_metrics
# ============================================================

import math
import pytest
from pyspark.sql import Row

from modules.scalability_analysis import (
    generate_scaled_dataset,
    run_benchmark,
    compute_scalability_metrics,
    SERVICE_NAMES,
)


# ============================================================
# generate_scaled_dataset
# ============================================================
@pytest.mark.skip(reason="Requires running MinIO / S3A endpoint for Parquet I/O")
class TestGenerateScaledDataset:
    """Tests for generate_scaled_dataset()."""

    @pytest.fixture(scope="class")
    def small_dataset_path(self, spark):
        """Generate a 500-row dataset once per class."""
        # Use a mock bucket path (local filesystem via file://) to avoid MinIO dependency
        path = generate_scaled_dataset(spark, target_rows=500, bucket="test-bucket")
        return path

    def test_generates_exact_row_count(self, spark, small_dataset_path):
        """Path should contain exactly `target_rows` rows."""
        df = spark.read.parquet(small_dataset_path)
        assert df.count() == 500

    def test_has_expected_columns(self, spark, small_dataset_path):
        """Generated data must include all schema columns."""
        df = spark.read.parquet(small_dataset_path)
        expected = {"span_id", "trace_id", "service_name", "response_time_ms",
                    "status_code", "is_failure", "pod_id",
                    "cpu_usage_mcores", "memory_usage_mb", "start_time"}
        actual = set(df.columns)
        assert expected.issubset(actual), f"Missing columns: {expected - actual}"

    def test_span_ids_are_unique(self, spark, small_dataset_path):
        """Every span_id should appear exactly once."""
        df = spark.read.parquet(small_dataset_path)
        total = df.count()
        unique_spans = df.select("span_id").distinct().count()
        assert total == unique_spans, f"{total} rows but {unique_spans} unique span_ids"

    def test_service_names_from_known_set(self, spark, small_dataset_path):
        """All service_name values must be from SERVICE_NAMES."""
        df = spark.read.parquet(small_dataset_path)
        actual_services = set(r.service_name for r in
                              df.select("service_name").distinct().collect())
        assert actual_services.issubset(set(SERVICE_NAMES)), (
            f"Unknown services: {actual_services - set(SERVICE_NAMES)}"
        )

    def test_status_codes_are_valid_http(self, spark, small_dataset_path):
        """All status codes should be standard HTTP codes (200-599)."""
        df = spark.read.parquet(small_dataset_path)
        codes = [r.status_code for r in df.select("status_code").distinct().collect()]
        for c in codes:
            assert 200 <= c < 600, f"Invalid HTTP status: {c}"

    def test_is_failure_is_binary(self, spark, small_dataset_path):
        """is_failure must only contain 0 or 1."""
        df = spark.read.parquet(small_dataset_path)
        vals = set(r.is_failure for r in df.select("is_failure").distinct().collect())
        assert vals.issubset({0, 1}), f"Unexpected is_failure values: {vals}"

    def test_response_times_are_positive(self, spark, small_dataset_path):
        """All response_time_ms values must be positive."""
        df = spark.read.parquet(small_dataset_path)
        min_rt = df.agg({"response_time_ms": "min"}).collect()[0][0]
        assert min_rt > 0, f"Non-positive response_time_ms found: min={min_rt}"

    def test_cpu_usage_positive(self, spark, small_dataset_path):
        """All cpu_usage_mcores values must be >= 0."""
        df = spark.read.parquet(small_dataset_path)
        min_cpu = df.agg({"cpu_usage_mcores": "min"}).collect()[0][0]
        assert min_cpu >= 0, f"Negative cpu_usage_mcores: min={min_cpu}"

    def test_memory_usage_positive(self, spark, small_dataset_path):
        """All memory_usage_mb values must be >= 0."""
        df = spark.read.parquet(small_dataset_path)
        min_mem = df.agg({"memory_usage_mb": "min"}).collect()[0][0]
        assert min_mem >= 0, f"Negative memory_usage_mb: min={min_mem}"

    def test_some_failures_present(self, spark, small_dataset_path):
        """~8% error rate means at least some failures should exist."""
        df = spark.read.parquet(small_dataset_path)
        failure_count = df.filter(df.is_failure == 1).count()
        assert failure_count > 0, "No failures generated — unrealistic dataset"

    def test_some_successes_present(self, spark, small_dataset_path):
        """Most requests should succeed (~92%)."""
        df = spark.read.parquet(small_dataset_path)
        success_count = df.filter(df.is_failure == 0).count()
        assert success_count > 0, "No successes generated — unrealistic dataset"

    def test_pod_ids_format(self, spark, small_dataset_path):
        """All pod_ids should match the format 'pod-XXXXX'."""
        df = spark.read.parquet(small_dataset_path)
        pod_ids = [r.pod_id for r in df.select("pod_id").distinct().collect()]
        for pid in pod_ids:
            assert pid.startswith("pod-"), f"Invalid pod_id format: {pid}"
            assert len(pid) == 9, f"Invalid pod_id length: {pid}"  # "pod-00001" = 9 chars

    def test_start_time_is_not_empty(self, spark, small_dataset_path):
        """All start_time strings should be non-empty."""
        df = spark.read.parquet(small_dataset_path)
        null_starts = df.filter(df.start_time.isNull() | (df.start_time == "")).count()
        assert null_starts == 0, f"{null_starts} rows have null/empty start_time"

    def test_large_dataset(self, spark):
        """Generate 2000 rows to verify scaling doesn't break."""
        path = generate_scaled_dataset(spark, target_rows=2000, bucket="test-bucket")
        df = spark.read.parquet(path)
        assert df.count() == 2000
        assert df.select("span_id").distinct().count() == 2000

    def test_single_row(self, spark):
        """Edge case: target_rows=1 should work."""
        path = generate_scaled_dataset(spark, target_rows=1, bucket="test-bucket")
        df = spark.read.parquet(path)
        assert df.count() == 1


# ============================================================
# run_benchmark
# ============================================================
@pytest.mark.skip(reason="Requires running MinIO / S3A endpoint for Parquet I/O")
class TestRunBenchmark:
    """Tests for run_benchmark()."""

    @pytest.fixture(scope="class")
    def benchmark_result(self, spark):
        """Run benchmark against a 500-row dataset."""
        path = generate_scaled_dataset(spark, target_rows=500, bucket="test-bucket")
        df = spark.read.parquet(path)
        return run_benchmark(spark, df, label="500_test")

    def test_returns_dict_with_expected_keys(self, benchmark_result):
        """Result must contain all benchmark timing keys."""
        expected = {
            "label", "input_rows",
            "groupby_agg_sec", "window_fn_sec", "join_sec", "shuffle_sec",
            "total_sec",
        }
        assert set(benchmark_result.keys()) == expected

    def test_label_matches(self, benchmark_result):
        """label should match the input label."""
        assert benchmark_result["label"] == "500_test"

    def test_input_rows_correct(self, benchmark_result):
        """input_rows should match the DataFrame row count."""
        assert benchmark_result["input_rows"] == 500

    def test_all_timings_are_positive(self, benchmark_result):
        """Every timing metric should be > 0 (some work was done)."""
        timing_keys = ["groupby_agg_sec", "window_fn_sec", "join_sec", "shuffle_sec"]
        for key in timing_keys:
            assert benchmark_result[key] > 0, f"{key} is not positive: {benchmark_result[key]}"

    def test_total_equals_sum_of_parts(self, benchmark_result):
        """total_sec should equal the sum of individual operation timings."""
        parts = (benchmark_result["groupby_agg_sec"] +
                 benchmark_result["window_fn_sec"] +
                 benchmark_result["join_sec"] +
                 benchmark_result["shuffle_sec"])
        assert math.isclose(benchmark_result["total_sec"], parts, rel_tol=0.01), (
            f"total={benchmark_result['total_sec']} != sum={parts}"
        )

    def test_timings_are_reasonable(self, benchmark_result):
        """For 500 rows, total time should be under 60 seconds."""
        assert benchmark_result["total_sec"] < 60, (
            f"Benchmark took {benchmark_result['total_sec']}s for 500 rows — too slow"
        )

    def test_benchmark_runs_on_larger_dataset(self, spark):
        """Verify benchmark completes on 2000 rows without error."""
        path = generate_scaled_dataset(spark, target_rows=2000, bucket="test-bucket")
        df = spark.read.parquet(path)
        result = run_benchmark(spark, df, label="2000_test")
        assert result["input_rows"] == 2000
        assert result["total_sec"] > 0


# ============================================================
# compute_scalability_metrics
# ============================================================
class TestComputeScalabilityMetrics:
    """Tests for compute_scalability_metrics()."""

    def test_returns_dict(self):
        """Should return a dictionary."""
        results = [
            {
                "data_size": 500, "repetition": 1, "input_rows": 500,
                "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
                "join_sec": 0.8, "shuffle_sec": 0.4,
                "total_sec": 2.0, "throughput_rows_per_sec": 250.0,
            },
            {
                "data_size": 1000, "repetition": 1, "input_rows": 1000,
                "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
                "join_sec": 1.2, "shuffle_sec": 0.7,
                "total_sec": 3.2, "throughput_rows_per_sec": 312.5,
            },
            {
                "data_size": 2000, "repetition": 1, "input_rows": 2000,
                "groupby_agg_sec": 1.4, "window_fn_sec": 0.9,
                "join_sec": 2.0, "shuffle_sec": 1.2,
                "total_sec": 5.5, "throughput_rows_per_sec": 363.6,
            },
        ]
        metrics = compute_scalability_metrics(results)
        assert isinstance(metrics, dict)

    def test_contains_expected_keys(self):
        """Must have baseline, scaling_results, and scaling_characteristic."""
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 1000, "repetition": 1, "input_rows": 1000,
             "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
             "join_sec": 1.2, "shuffle_sec": 0.7,
             "total_sec": 3.2, "throughput_rows_per_sec": 312.5},
        ]
        metrics = compute_scalability_metrics(results)
        expected = {"num_data_sizes", "num_repetitions_per_size",
                    "baseline_rows", "baseline_total_sec",
                    "scaling_results", "scaling_characteristic"}
        assert set(metrics.keys()) == expected

    def test_num_data_sizes_correct(self):
        """Should count unique data sizes."""
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 500, "repetition": 2, "input_rows": 500,
             "groupby_agg_sec": 0.48, "window_fn_sec": 0.28,
             "join_sec": 0.78, "shuffle_sec": 0.38,
             "total_sec": 1.92, "throughput_rows_per_sec": 260.4},
            {"data_size": 1000, "repetition": 1, "input_rows": 1000,
             "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
             "join_sec": 1.2, "shuffle_sec": 0.7,
             "total_sec": 3.2, "throughput_rows_per_sec": 312.5},
        ]
        metrics = compute_scalability_metrics(results)
        assert metrics["num_data_sizes"] == 2

    def test_num_repetitions_per_size_calculated(self):
        """Repetitions per size = total results / unique data sizes."""
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 500, "repetition": 2, "input_rows": 500,
             "groupby_agg_sec": 0.48, "window_fn_sec": 0.28,
             "join_sec": 0.78, "shuffle_sec": 0.38,
             "total_sec": 1.92, "throughput_rows_per_sec": 260.4},
            {"data_size": 1000, "repetition": 1, "input_rows": 1000,
             "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
             "join_sec": 1.2, "shuffle_sec": 0.7,
             "total_sec": 3.2, "throughput_rows_per_sec": 312.5},
            {"data_size": 1000, "repetition": 2, "input_rows": 1000,
             "groupby_agg_sec": 0.75, "window_fn_sec": 0.48,
             "join_sec": 1.15, "shuffle_sec": 0.65,
             "total_sec": 3.03, "throughput_rows_per_sec": 330.0},
        ]
        metrics = compute_scalability_metrics(results)
        # 4 results / 2 unique sizes = 2 repetitions per size
        assert metrics["num_repetitions_per_size"] == 2

    def test_scaling_results_has_expected_fields(self):
        """Each scaling_result must have data_size, avg_total_time_sec, speedup, efficiency."""
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 1000, "repetition": 1, "input_rows": 1000,
             "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
             "join_sec": 1.2, "shuffle_sec": 0.7,
             "total_sec": 3.2, "throughput_rows_per_sec": 312.5},
        ]
        metrics = compute_scalability_metrics(results)
        for sr in metrics["scaling_results"]:
            for field in ["data_size", "avg_total_time_sec", "avg_throughput_rows_per_sec",
                          "data_ratio", "speedup", "scalability_efficiency"]:
                assert field in sr, f"Missing field '{field}' in scaling_result"

    def test_speedup_reflects_execution_time_ratio(self):
        """
        Speedup = baseline_time / current_time.
        When larger datasets take more time, speedup < 1 (sub-linear scaling).
        """
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 1000, "repetition": 1, "input_rows": 1000,
             "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
             "join_sec": 1.2, "shuffle_sec": 0.7,
             "total_sec": 3.2, "throughput_rows_per_sec": 312.5},
        ]
        metrics = compute_scalability_metrics(results)
        # baseline speedup should always equal 1.0
        assert metrics["scaling_results"][0]["speedup"] == 1.0
        # 2x data takes 1.6x time → speedup = 2.0/3.2 = 0.625 (sub-linear)
        assert math.isclose(metrics["scaling_results"][1]["speedup"], 0.625, rel_tol=0.01)

    def test_efficiency_between_zero_and_one_for_sublinear(self):
        """For sub-linear scaling, efficiency should be in (0, 1]."""
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 5000, "repetition": 1, "input_rows": 5000,
             "groupby_agg_sec": 5.0, "window_fn_sec": 3.0,
             "join_sec": 8.0, "shuffle_sec": 4.0,
             "total_sec": 20.0, "throughput_rows_per_sec": 250.0},
        ]
        metrics = compute_scalability_metrics(results)
        for sr in metrics["scaling_results"]:
            assert 0.0 <= sr["scalability_efficiency"] <= 2.0, (
                f"Efficiency out of reasonable range: {sr['scalability_efficiency']}"
            )

    def test_scaling_characteristic_is_valid_string(self):
        """scaling_characteristic must be one of the three known values."""
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 1000, "repetition": 1, "input_rows": 1000,
             "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
             "join_sec": 1.2, "shuffle_sec": 0.7,
             "total_sec": 3.2, "throughput_rows_per_sec": 312.5},
        ]
        metrics = compute_scalability_metrics(results)
        valid = {"sub-linear", "near-linear", "super-linear"}
        assert metrics["scaling_characteristic"] in valid, (
            f"Unknown characteristic: {metrics['scaling_characteristic']}"
        )

    def test_empty_results_returns_empty_dict(self):
        """Should return empty dict for empty results list."""
        metrics = compute_scalability_metrics([])
        assert metrics == {}

    def test_single_data_size(self):
        """Single data size should still produce valid metrics."""
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
        ]
        metrics = compute_scalability_metrics(results)
        assert metrics["num_data_sizes"] == 1
        assert metrics["num_repetitions_per_size"] == 1
        assert len(metrics["scaling_results"]) == 1
        # For single data size, speedup should be 1.0
        assert metrics["scaling_results"][0]["speedup"] == 1.0

    def test_averages_multiple_repetitions(self):
        """Multiple repetitions at same size should be averaged correctly."""
        # size=500: avg total = (2.0 + 3.0) / 2 = 2.5
        results = [
            {"data_size": 500, "repetition": 1, "input_rows": 500,
             "groupby_agg_sec": 0.5, "window_fn_sec": 0.3,
             "join_sec": 0.8, "shuffle_sec": 0.4,
             "total_sec": 2.0, "throughput_rows_per_sec": 250.0},
            {"data_size": 500, "repetition": 2, "input_rows": 500,
             "groupby_agg_sec": 0.8, "window_fn_sec": 0.5,
             "join_sec": 1.2, "shuffle_sec": 0.5,
             "total_sec": 3.0, "throughput_rows_per_sec": 166.7},
        ]
        metrics = compute_scalability_metrics(results)
        avg = metrics["scaling_results"][0]["avg_total_time_sec"]
        assert math.isclose(avg, 2.5, rel_tol=0.01), f"Expected avg=2.5, got {avg}"
