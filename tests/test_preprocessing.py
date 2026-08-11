# ============================================================
# Unit Tests — Module 2: Data Preprocessing
# ============================================================
# Tests: clean_and_validate, join_datasets, engineer_features
# ============================================================

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from modules.preprocessing import (
    clean_and_validate,
    join_datasets,
    engineer_features,
)


# ============================================================
# clean_and_validate
# ============================================================
class TestCleanAndValidate:
    """Tests for the clean_and_validate() function."""

    def test_deduplicates_span_ids(self, sample_raw_dfs):
        """Duplicate span_ids should be removed (only first kept)."""
        cleaned = clean_and_validate(sample_raw_dfs)

        service_df = cleaned["trace_service_name"]
        span_ids = [r.span_id for r in service_df.select("span_id").collect()]

        # span-004 appears twice in raw, should appear once after dedup
        assert span_ids.count("span-004") == 1

    def test_filters_empty_parent_span_ids(self, sample_raw_dfs):
        """Rows with empty parent_span_id should be removed."""
        cleaned = clean_and_validate(sample_raw_dfs)

        service_df = cleaned["trace_service_name"]
        # span-005 has parent_span_id="", should be filtered out
        span_ids = [r.span_id for r in service_df.select("span_id").collect()]
        assert "span-005" not in span_ids

    def test_filters_null_span_ids(self, sample_raw_dfs):
        """Rows with null span_ids should be removed."""
        cleaned = clean_and_validate(sample_raw_dfs)

        service_df = cleaned["trace_service_name"]
        span_ids = [r.span_id for r in service_df.select("span_id").collect()]
        # The null-span_id row should be filtered out
        assert None not in span_ids

    def test_filters_negative_latencies(self, sample_raw_dfs):
        """Rows with negative response_time_ms or processing_time_ms should be removed."""
        cleaned = clean_and_validate(sample_raw_dfs)

        response_df = cleaned["trace_response_times"]
        # span-005 has negative response_time_ms
        span_ids = [r.span_id for r in response_df.select("span_id").collect()]
        assert "span-005" not in span_ids

    def test_parses_timestamps(self, sample_raw_dfs):
        """Request times should have parsed start_time_ts and end_time_ts columns."""
        cleaned = clean_and_validate(sample_raw_dfs)

        request_df = cleaned["trace_request_times"]
        columns = request_df.columns
        assert "start_time_ts" in columns
        assert "end_time_ts" in columns

        # Verify the timestamp was parsed (not null)
        timestamps = request_df.select("start_time_ts").filter(
            F.col("start_time_ts").isNotNull()
        ).count()
        assert timestamps > 0

    def test_filters_null_timestamps(self, sample_raw_dfs):
        """Rows with null start_time_ts should be removed."""
        cleaned = clean_and_validate(sample_raw_dfs)

        request_df = cleaned["trace_request_times"]
        # span-005 has null start_time
        span_ids = [r.span_id for r in request_df.select("span_id").collect()]
        assert "span-005" not in span_ids

    def test_filters_negative_cpu(self, sample_raw_dfs):
        """Rows with negative CPU usage should be removed."""
        cleaned = clean_and_validate(sample_raw_dfs)

        resource_df = cleaned["resource_usage"]
        cpu_values = [r.cpu_usage_mcores for r in resource_df.select("cpu_usage_mcores").collect()]
        assert all(v >= 0 for v in cpu_values)

    def test_returns_all_five_datasets(self, sample_raw_dfs):
        """Should return dict with all 5 expected keys."""
        cleaned = clean_and_validate(sample_raw_dfs)

        expected_keys = {
            "trace_service_name", "trace_response_times",
            "trace_request_times", "resource_usage", "status_codes",
        }
        assert set(cleaned.keys()) == expected_keys

    def test_preserves_valid_rows_count(self, sample_raw_dfs):
        """After cleaning, we should have fewer rows than raw but > 0."""
        cleaned = clean_and_validate(sample_raw_dfs)

        for name, df in cleaned.items():
            assert df.count() > 0, f"{name} is empty after cleaning"

    def test_handles_empty_dataframes(self, spark):
        """Should not crash when given empty DataFrames."""
        dfs = {
            "trace_service_name": spark.createDataFrame(
                [], "trace_id string, service_name string, span_id string, parent_span_id string, namespace string, pod_id string, node_id string"
            ),
            "trace_response_times": spark.createDataFrame(
                [], "span_id string, response_time_ms double, wait_time_ms double, processing_time_ms double, network_latency_ms double"
            ),
            "trace_request_times": spark.createDataFrame(
                [], "span_id string, start_time string, end_time string, duration_ms double, http_method string, endpoint string"
            ),
            "resource_usage": spark.createDataFrame(
                [], "pod_id string, timestamp string, cpu_usage_mcores double, memory_usage_mb double, network_rx_bytes int, network_tx_bytes int, disk_io_read_bytes int, disk_io_write_bytes int"
            ),
            "status_codes": spark.createDataFrame(
                [], "span_id string, status_code int, error_message string, is_error int"
            ),
        }
        cleaned = clean_and_validate(dfs)
        # Should not crash — all DataFrames should have 0 rows
        for name, df in cleaned.items():
            assert df.count() == 0, f"{name} should be empty"


# ============================================================
# join_datasets
# ============================================================
class TestJoinDatasets:
    """Tests for the join_datasets() function."""

    def test_inner_join_only_keeps_common_span_ids(self, spark, sample_raw_dfs):
        """
        After cleaning, only span_ids present in ALL 4 core datasets
        (service, response, request, status) should survive the inner joins.
        """
        from modules.preprocessing import clean_and_validate
        cleaned = clean_and_validate(sample_raw_dfs)
        joined = join_datasets(cleaned)

        # Known: span-001, span-002, span-003, span-004 exist in all core datasets
        # span-005 has no valid timestamp, null in response, empty parent
        span_ids = [r.span_id for r in joined.select("span_id").distinct().collect()]

        assert "span-001" in span_ids
        assert "span-002" in span_ids
        assert "span-003" in span_ids
        assert "span-004" in span_ids

    def test_result_has_expected_columns(self, spark, sample_raw_dfs):
        """Joined DataFrame should contain columns from all source datasets."""
        from modules.preprocessing import clean_and_validate
        cleaned = clean_and_validate(sample_raw_dfs)
        joined = join_datasets(cleaned)

        columns = set(joined.columns)
        expected_subset = {
            "span_id", "trace_id", "service_name", "pod_id",
            "response_time_ms", "start_time_ts", "status_code",
            "cpu_usage_mcores", "memory_usage_mb",
        }
        assert expected_subset.issubset(columns), f"Missing columns: {expected_subset - columns}"

    def test_left_join_brings_resource_data(self, spark, sample_raw_dfs):
        """Resource columns (cpu, memory) should appear even if pod not in resource_usage."""
        from modules.preprocessing import clean_and_validate
        cleaned = clean_and_validate(sample_raw_dfs)
        joined = join_datasets(cleaned)

        assert "cpu_usage_mcores" in joined.columns
        assert "memory_usage_mb" in joined.columns


# ============================================================
# engineer_features
# ============================================================
class TestEngineerFeatures:
    """Tests for the engineer_features() function."""

    @pytest.fixture(scope="class")
    def featured_df(self, spark, sample_raw_dfs):
        """Build a joined + feature-engineered DataFrame once per class."""
        from modules.preprocessing import clean_and_validate, join_datasets
        cleaned = clean_and_validate(sample_raw_dfs)
        joined = join_datasets(cleaned)
        return engineer_features(joined)

    def test_is_failure_flag(self, featured_df):
        """Status >= 500 should set is_failure=1, others = 0."""
        rows = featured_df.select("status_code", "is_failure").collect()
        for r in rows:
            if r.status_code >= 500:
                assert r.is_failure == 1, f"status {r.status_code} → is_failure should be 1"
            else:
                assert r.is_failure == 0, f"status {r.status_code} → is_failure should be 0"

    def test_is_latency_spike_flag(self, featured_df):
        """Response time > 2000 should set is_latency_spike=1."""
        rows = featured_df.select("response_time_ms", "is_latency_spike").collect()
        for r in rows:
            if r.response_time_ms > 2000:
                assert r.is_latency_spike == 1, f"latency {r.response_time_ms} → spike should be 1"
            else:
                assert r.is_latency_spike == 0

    def test_latency_bucket_values(self, featured_df):
        """Latency buckets should be one of: low, medium, high, critical."""
        valid_buckets = {"low", "medium", "high", "critical"}
        buckets = [r.latency_bucket for r in featured_df.select("latency_bucket").distinct().collect()]
        for b in buckets:
            assert b in valid_buckets, f"Invalid latency bucket: {b}"

    def test_latency_bucket_boundaries(self, featured_df):
        """Check specific boundary values for latency buckets."""
        rows = featured_df.select("response_time_ms", "latency_bucket").collect()
        for r in rows:
            if r.response_time_ms < 100:
                assert r.latency_bucket == "low", f"{r.response_time_ms}ms should be 'low'"
            elif r.response_time_ms < 500:
                assert r.latency_bucket == "medium", f"{r.response_time_ms}ms should be 'medium'"
            elif r.response_time_ms < 2000:
                assert r.latency_bucket == "high", f"{r.response_time_ms}ms should be 'high'"
            else:
                assert r.latency_bucket == "critical", f"{r.response_time_ms}ms should be 'critical'"

    def test_error_category_values(self, featured_df):
        """Error categories should be one of: success, client_error, server_error."""
        valid_categories = {"success", "client_error", "server_error"}
        cats = [r.error_category for r in featured_df.select("error_category").distinct().collect()]
        for c in cats:
            assert c in valid_categories, f"Invalid error category: {c}"

    def test_error_category_boundaries(self, featured_df):
        """Check 400-499 → client_error, 500+ → server_error, else → success."""
        rows = featured_df.select("status_code", "error_category").collect()
        for r in rows:
            if r.status_code >= 500:
                assert r.error_category == "server_error", f"status {r.status_code}"
            elif r.status_code >= 400:
                assert r.error_category == "client_error", f"status {r.status_code}"
            else:
                assert r.error_category == "success", f"status {r.status_code}"

    def test_hour_of_day_extracted(self, featured_df):
        """hour_of_day should be an integer 0–23."""
        hours = [r.hour_of_day for r in featured_df.select("hour_of_day").collect()]
        for h in hours:
            assert isinstance(h, int) or h is None
            if h is not None:
                assert 0 <= h <= 23, f"hour_of_day out of range: {h}"

    def test_cpu_memory_ratio(self, featured_df):
        """cpu_memory_ratio should be >= 0 and = cpu/memory when memory > 0."""
        rows = featured_df.select("cpu_usage_mcores", "memory_usage_mb", "cpu_memory_ratio").collect()
        for r in rows:
            assert r.cpu_memory_ratio >= 0, f"Negative ratio: {r.cpu_memory_ratio}"
            if r.memory_usage_mb is not None and r.memory_usage_mb > 0 and r.cpu_usage_mcores is not None:
                expected = r.cpu_usage_mcores / r.memory_usage_mb
                assert abs(r.cpu_memory_ratio - expected) < 0.001

    def test_all_feature_columns_present(self, featured_df):
        """All 6 engineered feature columns should exist."""
        expected_features = {
            "is_failure", "is_latency_spike", "latency_bucket",
            "error_category", "hour_of_day", "cpu_memory_ratio",
        }
        assert expected_features.issubset(set(featured_df.columns))
