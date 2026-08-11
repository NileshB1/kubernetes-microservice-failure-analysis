# ============================================================
# Unit Tests — Module 4: Failure / Anomaly Detection
# ============================================================
# Tests: compute_error_rate_timeseries, detect_error/latency/resource
#        anomalies, unify_anomalies, cluster_failure_patterns,
#        compute_rq2_summary
# ============================================================

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from modules.failure_detection import (
    compute_error_rate_timeseries,
    detect_error_rate_anomalies,
    detect_latency_anomalies,
    detect_resource_anomalies,
    unify_anomalies,
    cluster_failure_patterns,
    compute_rq2_summary,
)


# ============================================================
# compute_error_rate_timeseries
# ============================================================
class TestComputeErrorRateTimeseries:
    """Tests for compute_error_rate_timeseries()."""

    def test_output_has_required_columns(self, timeseries_df):
        """Time series must contain expected aggregation columns."""
        required = {"service_name", "time_bucket", "total_requests",
                     "error_count", "error_rate", "avg_latency_ms",
                     "avg_cpu_mcores", "avg_memory_mb"}
        assert required.issubset(set(timeseries_df.columns))

    def test_error_rate_between_zero_and_one(self, timeseries_df):
        """Error rate must always be in [0, 1]."""
        rows = timeseries_df.select("error_rate").collect()
        for r in rows:
            assert 0.0 <= r.error_rate <= 1.0, f"error_rate={r.error_rate}"

    def test_total_requests_matches_error_count_bound(self, timeseries_df):
        """error_count ≤ total_requests for every bucket."""
        rows = timeseries_df.select("total_requests", "error_count").collect()
        for r in rows:
            assert r.error_count <= r.total_requests, (
                f"error_count ({r.error_count}) > total ({r.total_requests})"
            )

    def test_service_names_preserved(self, timeseries_df, unified_df):
        """All services from input should appear in output."""
        input_services = set(r.service_name for r in
                             unified_df.select("service_name").distinct().collect())
        output_services = set(r.service_name for r in
                              timeseries_df.select("service_name").distinct().collect())
        assert input_services == output_services

    def test_time_bucket_is_not_null(self, timeseries_df):
        """Every row should have a valid time_bucket."""
        null_count = timeseries_df.filter(F.col("time_bucket").isNull()).count()
        assert null_count == 0

    def test_handles_empty_input(self, spark):
        """Should return an empty DataFrame with correct schema for empty input."""
        empty = spark.createDataFrame(
            [], "span_id string, service_name string, start_time_ts timestamp, "
                "response_time_ms double, is_failure int, "
                "cpu_usage_mcores double, memory_usage_mb double"
        )
        result = compute_error_rate_timeseries(empty, window_minutes=1)
        assert result.count() == 0
        assert "error_rate" in result.columns


# ============================================================
# detect_error_rate_anomalies
# ============================================================
class TestDetectErrorRateAnomalies:
    """Tests for detect_error_rate_anomalies()."""

    @pytest.fixture(scope="class")
    def error_anomalies(self, timeseries_df):
        return detect_error_rate_anomalies(timeseries_df, zscore_threshold=3.0)

    def test_adds_zscore_and_flag_columns(self, error_anomalies):
        """Must append zscore_error_rate and is_anomaly_error columns."""
        assert "zscore_error_rate" in error_anomalies.columns
        assert "is_anomaly_error" in error_anomalies.columns

    def test_is_anomaly_error_is_binary(self, error_anomalies):
        """is_anomaly_error must be 0 or 1 only."""
        vals = set(r.is_anomaly_error for r in
                   error_anomalies.select("is_anomaly_error").distinct().collect())
        assert vals.issubset({0, 1}), f"Unexpected values: {vals}"

    def test_zscore_is_finite(self, error_anomalies):
        """All z-scores should be finite numbers (no inf or nan)."""
        import math
        rows = error_anomalies.select("zscore_error_rate").collect()
        for r in rows:
            assert math.isfinite(r.zscore_error_rate), f"Non-finite zscore: {r.zscore_error_rate}"

    def test_anomalies_flag_respected(self, error_anomalies):
        """When is_anomaly_error=1, |zscore| must be > threshold."""
        anomalies = error_anomalies.filter(F.col("is_anomaly_error") == 1).collect()
        for r in anomalies:
            assert abs(r.zscore_error_rate) > 3.0, (
                f"Flagged anomaly has |zscore|={abs(r.zscore_error_rate):.4f}"
            )

    def test_lower_threshold_detects_more(self, timeseries_df):
        """Lower zscore threshold should produce more anomalies."""
        strict = detect_error_rate_anomalies(timeseries_df, zscore_threshold=3.0)
        loose = detect_error_rate_anomalies(timeseries_df, zscore_threshold=1.0)
        strict_count = strict.filter(F.col("is_anomaly_error") == 1).count()
        loose_count = loose.filter(F.col("is_anomaly_error") == 1).count()
        assert loose_count >= strict_count


# ============================================================
# detect_latency_anomalies
# ============================================================
class TestDetectLatencyAnomalies:
    """Tests for detect_latency_anomalies()."""

    @pytest.fixture(scope="class")
    def latency_anomalies(self, timeseries_df):
        return detect_latency_anomalies(timeseries_df, zscore_threshold=3.0)

    def test_adds_latency_columns(self, latency_anomalies):
        """Must append zscore_latency and is_anomaly_latency."""
        assert "zscore_latency" in latency_anomalies.columns
        assert "is_anomaly_latency" in latency_anomalies.columns

    def test_is_anomaly_latency_is_binary(self, latency_anomalies):
        """is_anomaly_latency must be 0 or 1."""
        vals = set(r.is_anomaly_latency for r in
                   latency_anomalies.select("is_anomaly_latency").distinct().collect())
        assert vals.issubset({0, 1}), f"Unexpected values: {vals}"

    def test_service_with_high_latency_gets_anomalies(self, unified_df, timeseries_df):
        """
        auth-service has consistently high latency (3000-3500ms).
        With 3 data points, stddev will be low, so one should
        be flagged as anomalous relative to baseline.
        """
        anomalies = detect_latency_anomalies(timeseries_df, zscore_threshold=2.0)
        auth_anomalies = anomalies.filter(
            (F.col("service_name") == "auth-service") &
            (F.col("is_anomaly_latency") == 1)
        ).count()
        # At least one auth-service bucket should be anomalous at z=2.0
        assert auth_anomalies > 0, "High-latency service not detected"


# ============================================================
# detect_resource_anomalies
# ============================================================
class TestDetectResourceAnomalies:
    """Tests for detect_resource_anomalies()."""

    @pytest.fixture(scope="class")
    def resource_anomalies(self, timeseries_df):
        return detect_resource_anomalies(timeseries_df, zscore_threshold=3.0)

    def test_adds_resource_columns(self, resource_anomalies):
        """Must append zscore_cpu, zscore_memory, is_anomaly_resource."""
        assert "zscore_cpu" in resource_anomalies.columns
        assert "zscore_memory" in resource_anomalies.columns
        assert "is_anomaly_resource" in resource_anomalies.columns

    def test_is_anomaly_resource_is_binary(self, resource_anomalies):
        """is_anomaly_resource must be 0 or 1."""
        vals = set(r.is_anomaly_resource for r in
                   resource_anomalies.select("is_anomaly_resource").distinct().collect())
        assert vals.issubset({0, 1})

    def test_cpu_or_memory_spike_triggers_flag(self, resource_anomalies):
        """At least one of CPU or memory must be anomalous when is_anomaly_resource=1."""
        flagged = resource_anomalies.filter(F.col("is_anomaly_resource") == 1)
        if flagged.count() > 0:
            for r in flagged.select("zscore_cpu", "zscore_memory").collect():
                assert abs(r.zscore_cpu) > 3.0 or abs(r.zscore_memory) > 3.0, (
                    f"Resource anomaly flagged with no zscore > 3: cpu={r.zscore_cpu}, mem={r.zscore_memory}"
                )


# ============================================================
# unify_anomalies
# ============================================================
class TestUnifyAnomalies:
    """Tests for unify_anomalies()."""

    @pytest.fixture(scope="class")
    def unified(self, timeseries_df):
        err = detect_error_rate_anomalies(timeseries_df, zscore_threshold=2.0)
        lat = detect_latency_anomalies(timeseries_df, zscore_threshold=2.0)
        res = detect_resource_anomalies(timeseries_df, zscore_threshold=2.0)
        return unify_anomalies(err, lat, res)

    def test_anomaly_score_in_range(self, unified):
        """anomaly_score must be in [0, 3]."""
        scores = [r.anomaly_score for r in unified.select("anomaly_score").collect()]
        for s in scores:
            assert 0 <= s <= 3, f"anomaly_score={s} out of [0,3]"

    def test_is_anomaly_overall_binary(self, unified):
        """is_anomaly_overall must be 0 or 1."""
        vals = set(r.is_anomaly_overall for r in
                   unified.select("is_anomaly_overall").distinct().collect())
        assert vals.issubset({0, 1})

    def test_overall_anomaly_requires_score_at_least_2(self, unified):
        """When is_anomaly_overall=1, anomaly_score must be >= 2."""
        overall = unified.filter(F.col("is_anomaly_overall") == 1).collect()
        for r in overall:
            assert r.anomaly_score >= 2, (
                f"Overall anomaly with score={r.anomaly_score}"
            )

    def test_no_null_values_in_flags(self, unified):
        """All flag columns should be non-null after unification."""
        flag_cols = ["is_anomaly_error", "is_anomaly_latency",
                     "is_anomaly_resource", "anomaly_score", "is_anomaly_overall"]
        for col_name in flag_cols:
            null_count = unified.filter(F.col(col_name).isNull()).count()
            assert null_count == 0, f"{col_name} has {null_count} nulls"

    def test_contains_all_services(self, unified, timeseries_df):
        """All original service-time buckets should appear (outer join preserves all)."""
        ts_count = timeseries_df.count()
        unified_count = unified.count()
        # Outer join should preserve or expand rows
        assert unified_count >= ts_count


# ============================================================
# cluster_failure_patterns
# ============================================================
class TestClusterFailurePatterns:
    """Tests for cluster_failure_patterns()."""

    @pytest.fixture(scope="class")
    def patterns(self, timeseries_df):
        err = detect_error_rate_anomalies(timeseries_df, zscore_threshold=2.0)
        lat = detect_latency_anomalies(timeseries_df, zscore_threshold=2.0)
        res = detect_resource_anomalies(timeseries_df, zscore_threshold=2.0)
        unified = unify_anomalies(err, lat, res)
        return cluster_failure_patterns(unified)

    def test_excludes_normal_patterns(self, patterns):
        """The 'normal' pattern_type should not appear in output."""
        pattern_types = set(r.pattern_type for r in
                           patterns.select("pattern_type").distinct().collect())
        assert "normal" not in pattern_types, "Normal patterns should be filtered out"

    def test_pattern_types_are_valid(self, patterns):
        """All pattern types must be from the predefined set."""
        valid = {"latency_spike", "error_surge", "resource_pressure",
                 "cascading_failure", "resource_exhaustion",
                 "error_resource_link", "full_failure"}
        pattern_types = set(r.pattern_type for r in
                           patterns.select("pattern_type").distinct().collect())
        assert pattern_types.issubset(valid), f"Unknown patterns: {pattern_types - valid}"

    def test_occurrence_count_positive(self, patterns):
        """Every pattern should have occurrence_count > 0."""
        for r in patterns.collect():
            assert r.occurrence_count > 0, (
                f"Zero occurrence for {r.service_name}/{r.pattern_type}"
            )

    def test_avg_severity_in_range(self, patterns):
        """avg_severity should be between 1 and 3."""
        for r in patterns.collect():
            assert 1.0 <= r.avg_severity <= 3.0, (
                f"avg_severity={r.avg_severity} out of [1,3]"
            )


# ============================================================
# compute_rq2_summary
# ============================================================
class TestComputeRq2Summary:
    """Tests for compute_rq2_summary()."""

    @pytest.fixture(scope="class")
    def summary(self, timeseries_df):
        err = detect_error_rate_anomalies(timeseries_df, zscore_threshold=2.0)
        lat = detect_latency_anomalies(timeseries_df, zscore_threshold=2.0)
        res = detect_resource_anomalies(timeseries_df, zscore_threshold=2.0)
        unified = unify_anomalies(err, lat, res)
        patterns = cluster_failure_patterns(unified)
        return compute_rq2_summary(unified, patterns)

    def test_returns_dict_with_expected_keys(self, summary):
        """Summary should contain all required metric keys."""
        expected = {"total_time_buckets_analyzed", "total_anomalies_detected",
                    "anomaly_rate_pct", "top_anomalous_services",
                    "pattern_distribution"}
        assert set(summary.keys()) == expected

    def test_anomaly_rate_percentage_reasonable(self, summary):
        """Anomaly rate should be a percentage between 0 and 100."""
        assert 0.0 <= summary["anomaly_rate_pct"] <= 100.0

    def test_total_anomalies_not_exceeding_total(self, summary):
        """Detected anomalies should not exceed total buckets."""
        assert summary["total_anomalies_detected"] <= summary["total_time_buckets_analyzed"]

    def test_top_anomalous_services_is_list(self, summary):
        """top_anomalous_services should be a list of dicts with service/count."""
        assert isinstance(summary["top_anomalous_services"], list)
        for item in summary["top_anomalous_services"]:
            assert "service" in item
            assert "count" in item
            assert item["count"] > 0

    def test_pattern_distribution_is_list(self, summary):
        """pattern_distribution should be a list of dicts with pattern/count."""
        assert isinstance(summary["pattern_distribution"], list)
        for item in summary["pattern_distribution"]:
            assert "pattern" in item
            assert "count" in item
            assert item["count"] > 0

    def test_auth_service_is_most_anomalous(self, summary):
        """
        auth-service has 3/3 requests failing with high latency —
        should appear as a top anomalous service.
        """
        top_services = [item["service"] for item in summary["top_anomalous_services"]]
        assert "auth-service" in top_services, (
            f"auth-service should be top anomalous, got: {top_services}"
        )
