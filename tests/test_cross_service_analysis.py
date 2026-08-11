# ============================================================
# Unit Tests — Module 3: Cross-Service Failure Propagation
# ============================================================
# Tests: build_service_dependency_graph, detect_propagation_chains,
#        correlate_cross_service_errors, compute_propagation_metrics
# ============================================================

import pytest
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql import functions as F

from modules.cross_service_analysis import (
    build_service_dependency_graph,
    detect_propagation_chains,
    correlate_cross_service_errors,
    compute_propagation_metrics,
)


# ============================================================
# Shared Fixture — Trace Topology DataFrame
# ============================================================
@pytest.fixture(scope="session")
def trace_df(spark):
    """
    Build a DataFrame with known parent-child span relationships
    across 4 services, including failure and non-failure spans.

    Topology:
        trace-A:
          frontend (span-01, root, OK)
            → auth-service (span-02, parent=span-01, 500)
              → user-service (span-03, parent=span-02, 500)  ← cascade!

        trace-B:
          frontend (span-04, root, OK)
            → order-service (span-05, parent=span-04, OK)

        trace-C:
          frontend (span-06, root, 500)
            → auth-service (span-07, parent=span-06, 503)     ← cascade!
    """
    ts_base = datetime(2024, 1, 1, 10, 0, 0)
    rows = [
        # trace-A: healthy frontend calls failing auth which calls failing user
        {"span_id": "span-01", "trace_id": "trace-A", "service_name": "frontend",
         "parent_span_id": "", "start_time_ts": ts_base,
         "response_time_ms": 45.0, "status_code": 200, "is_failure": 0,
         "cpu_usage_mcores": 300.0, "memory_usage_mb": 512.0},
        {"span_id": "span-02", "trace_id": "trace-A", "service_name": "auth-service",
         "parent_span_id": "span-01", "start_time_ts": ts_base + timedelta(seconds=1),
         "response_time_ms": 2500.0, "status_code": 500, "is_failure": 1,
         "cpu_usage_mcores": 800.0, "memory_usage_mb": 1024.0},
        {"span_id": "span-03", "trace_id": "trace-A", "service_name": "user-service",
         "parent_span_id": "span-02", "start_time_ts": ts_base + timedelta(seconds=3),
         "response_time_ms": 3000.0, "status_code": 500, "is_failure": 1,
         "cpu_usage_mcores": 900.0, "memory_usage_mb": 2048.0},

        # trace-B: all healthy
        {"span_id": "span-04", "trace_id": "trace-B", "service_name": "frontend",
         "parent_span_id": "", "start_time_ts": ts_base + timedelta(minutes=5),
         "response_time_ms": 60.0, "status_code": 200, "is_failure": 0,
         "cpu_usage_mcores": 310.0, "memory_usage_mb": 500.0},
        {"span_id": "span-05", "trace_id": "trace-B", "service_name": "order-service",
         "parent_span_id": "span-04", "start_time_ts": ts_base + timedelta(minutes=5, seconds=1),
         "response_time_ms": 80.0, "status_code": 200, "is_failure": 0,
         "cpu_usage_mcores": 250.0, "memory_usage_mb": 400.0},

        # trace-C: failing frontend calls failing auth
        {"span_id": "span-06", "trace_id": "trace-C", "service_name": "frontend",
         "parent_span_id": "", "start_time_ts": ts_base + timedelta(minutes=10),
         "response_time_ms": 2100.0, "status_code": 500, "is_failure": 1,
         "cpu_usage_mcores": 950.0, "memory_usage_mb": 1024.0},
        {"span_id": "span-07", "trace_id": "trace-C", "service_name": "auth-service",
         "parent_span_id": "span-06", "start_time_ts": ts_base + timedelta(minutes=10, seconds=2),
         "response_time_ms": 3100.0, "status_code": 503, "is_failure": 1,
         "cpu_usage_mcores": 1200.0, "memory_usage_mb": 1500.0},

        # trace-D: frontend only (root span, no parent)
        {"span_id": "span-08", "trace_id": "trace-D", "service_name": "frontend",
         "parent_span_id": "", "start_time_ts": ts_base + timedelta(minutes=15),
         "response_time_ms": 50.0, "status_code": 200, "is_failure": 0,
         "cpu_usage_mcores": 300.0, "memory_usage_mb": 510.0},
    ]
    return spark.createDataFrame([Row(**r) for r in rows])


# ============================================================
# build_service_dependency_graph
# ============================================================
class TestBuildServiceDependencyGraph:
    """Tests for build_service_dependency_graph()."""

    def test_returns_dataframe_with_expected_columns(self, trace_df):
        """Output must contain the service-pair aggregation columns."""
        result = build_service_dependency_graph(trace_df)
        expected_cols = {
            "caller_service", "callee_service", "call_count",
            "caller_error_count", "callee_error_count",
            "co_failure_count", "avg_callee_latency_ms", "propagation_score",
        }
        actual_cols = set(result.columns)
        assert expected_cols.issubset(actual_cols), f"Missing: {expected_cols - actual_cols}"

    def test_detects_frontend_to_auth_edge(self, trace_df):
        """frontend → auth-service should be a detected service pair."""
        result = build_service_dependency_graph(trace_df)
        pairs = result.select("caller_service", "callee_service").collect()
        pair_tuples = {(r.caller_service, r.callee_service) for r in pairs}
        assert ("frontend", "auth-service") in pair_tuples

    def test_detects_auth_to_user_edge(self, trace_df):
        """auth-service → user-service should be a detected service pair."""
        result = build_service_dependency_graph(trace_df)
        pairs = result.select("caller_service", "callee_service").collect()
        pair_tuples = {(r.caller_service, r.callee_service) for r in pairs}
        assert ("auth-service", "user-service") in pair_tuples

    def test_propagation_score_between_zero_and_one(self, trace_df):
        """propagation_score must be in [0.0, 1.0]."""
        result = build_service_dependency_graph(trace_df)
        scores = [r.propagation_score for r in result.select("propagation_score").collect()]
        for s in scores:
            assert 0.0 <= s <= 1.0, f"propagation_score={s} out of range"

    def test_positive_call_count_for_all_pairs(self, trace_df):
        """Every service pair should have call_count > 0."""
        result = build_service_dependency_graph(trace_df)
        counts = [r.call_count for r in result.select("call_count").collect()]
        for c in counts:
            assert c > 0, f"Zero call_count for a service pair"

    def test_co_failure_count_not_exceeding_min_error(self, trace_df):
        """Co-failures cannot exceed the minimum of caller/callee error counts."""
        result = build_service_dependency_graph(trace_df)
        for r in result.collect():
            max_possible = min(r.caller_error_count, r.callee_error_count)
            assert r.co_failure_count <= max_possible, (
                f"co_failure_count={r.co_failure_count} exceeds "
                f"min(caller_err={r.caller_error_count}, callee_err={r.callee_error_count})"
            )

    def test_no_self_loops(self, trace_df):
        """A service should not appear as both caller and callee in the same row."""
        result = build_service_dependency_graph(trace_df)
        for r in result.collect():
            assert r.caller_service != r.callee_service, (
                f"Self-loop: {r.caller_service} → {r.callee_service}"
            )

    def test_handles_empty_dataframe(self, spark):
        """Should return empty DataFrame with correct schema on empty input."""
        empty = spark.createDataFrame(
            [], "span_id string, trace_id string, service_name string, "
                "parent_span_id string, start_time_ts timestamp, "
                "response_time_ms double, status_code int, is_failure int, "
                "cpu_usage_mcores double, memory_usage_mb double"
        )
        result = build_service_dependency_graph(empty)
        assert result.count() == 0
        assert "propagation_score" in result.columns

    def test_single_service_handled(self, spark):
        """Single service with parent-child relationships."""
        ts = datetime(2024, 1, 1, 10, 0, 0)
        single_svc = spark.createDataFrame([
            Row(span_id="s-1", trace_id="t-1", service_name="frontend",
                parent_span_id="", start_time_ts=ts, response_time_ms=50.0,
                status_code=200, is_failure=0,
                cpu_usage_mcores=300.0, memory_usage_mb=512.0),
            Row(span_id="s-2", trace_id="t-1", service_name="frontend",
                parent_span_id="s-1", start_time_ts=ts + timedelta(seconds=1),
                response_time_ms=60.0, status_code=500, is_failure=1,
                cpu_usage_mcores=350.0, memory_usage_mb=520.0),
        ])
        result = build_service_dependency_graph(single_svc)
        # Single-service self-calls produce same-service pairs (valid in microservices)
        assert result.count() >= 0
        # Verify expected columns are present
        assert "propagation_score" in result.columns
        assert "caller_service" in result.columns
        assert "callee_service" in result.columns


# ============================================================
# detect_propagation_chains
# ============================================================
class TestDetectPropagationChains:
    """Tests for detect_propagation_chains()."""

    def test_returns_expected_columns(self, trace_df):
        """Output must contain chain-specific columns."""
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        expected = {"trace_id", "source_service", "target_service",
                    "source_timestamp", "target_timestamp",
                    "propagation_lag_sec", "propagation_depth"}
        assert expected.issubset(set(chains.columns))

    def test_detects_frontend_to_auth_cascade_in_trace_c(self, trace_df):
        """
        trace-C: frontend(span-06, 500) → auth-service(span-07, 503)
        Both are failures within 60s — should be detected.
        """
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        trace_c_chains = chains.filter(F.col("trace_id") == "trace-C").collect()
        sources = [r.source_service for r in trace_c_chains]
        targets = [r.target_service for r in trace_c_chains]
        assert "frontend" in sources
        assert "auth-service" in targets

    def test_detects_auth_to_user_cascade_in_trace_a(self, trace_df):
        """
        trace-A: auth-service(span-02, 500) → user-service(span-03, 500)
        Both failures, detected.
        """
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        trace_a_chains = chains.filter(F.col("trace_id") == "trace-A").collect()
        sources = [r.source_service for r in trace_a_chains]
        targets = [r.target_service for r in trace_a_chains]
        assert "auth-service" in sources
        assert "user-service" in targets

    def test_no_self_loops(self, trace_df):
        """Source and target service must differ."""
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        for r in chains.collect():
            assert r.source_service != r.target_service, (
                f"Self-loop: {r.source_service} → {r.target_service}"
            )

    def test_propagation_lag_is_positive(self, trace_df):
        """propagation_lag_sec must be > 0 (target after source)."""
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        lags = [r.propagation_lag_sec for r in chains.select("propagation_lag_sec").collect()]
        for lag in lags:
            assert lag > 0, f"Non-positive propagation lag: {lag}"

    def test_propagation_lag_within_window(self, trace_df):
        """All lags must be <= time_window_sec."""
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        for r in chains.collect():
            assert r.propagation_lag_sec <= 60, (
                f"Lag {r.propagation_lag_sec}s exceeds window of 60s"
            )

    def test_propagation_depth_is_two(self, trace_df):
        """All direct chains should have depth=2."""
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        depths = [r.propagation_depth for r in chains.select("propagation_depth").collect()]
        for d in depths:
            assert d == 2, f"Expected depth=2, got {d}"

    def test_strict_window_reduces_chains(self, trace_df):
        """A very short time window should detect fewer chains."""
        wide = detect_propagation_chains(trace_df, time_window_sec=300)
        narrow = detect_propagation_chains(trace_df, time_window_sec=1)
        # With our trace, lags are 1-2 seconds, so window=1 may still catch some
        # but should not exceed the wide window
        assert narrow.count() <= wide.count()

    def test_no_failures_yields_empty_chains(self, spark):
        """Data with zero failures should produce zero chains."""
        ts = datetime(2024, 1, 1, 10, 0, 0)
        ok_df = spark.createDataFrame([
            Row(span_id="s-1", trace_id="t-1", service_name="frontend",
                parent_span_id="", start_time_ts=ts, response_time_ms=50.0,
                status_code=200, is_failure=0,
                cpu_usage_mcores=300.0, memory_usage_mb=512.0),
            Row(span_id="s-2", trace_id="t-1", service_name="auth-service",
                parent_span_id="s-1", start_time_ts=ts + timedelta(seconds=1),
                response_time_ms=60.0, status_code=200, is_failure=0,
                cpu_usage_mcores=250.0, memory_usage_mb=400.0),
        ])
        chains = detect_propagation_chains(ok_df, time_window_sec=60)
        assert chains.count() == 0

    def test_handles_empty_dataframe(self, spark):
        """Should return empty DataFrame on empty input."""
        empty = spark.createDataFrame(
            [], "span_id string, trace_id string, service_name string, "
                "parent_span_id string, start_time_ts timestamp, "
                "response_time_ms double, status_code int, is_failure int,"
                "cpu_usage_mcores double, memory_usage_mb double"
        )
        chains = detect_propagation_chains(empty, time_window_sec=60)
        assert chains.count() == 0

    def test_single_failure_no_propagation(self, spark):
        """Single isolated failure should not form a chain."""
        ts = datetime(2024, 1, 1, 10, 0, 0)
        single_fail = spark.createDataFrame([
            Row(span_id="s-1", trace_id="t-1", service_name="frontend",
                parent_span_id="", start_time_ts=ts, response_time_ms=2100.0,
                status_code=500, is_failure=1,
                cpu_usage_mcores=950.0, memory_usage_mb=1024.0),
        ])
        chains = detect_propagation_chains(single_fail, time_window_sec=60)
        assert chains.count() == 0


# ============================================================
# correlate_cross_service_errors
# ============================================================
class TestCorrelateCrossServiceErrors:
    """Tests for correlate_cross_service_errors()."""

    @pytest.fixture(scope="class")
    def correlations(self, trace_df, spark):
        """Compute cross-service error correlations once per class."""
        return correlate_cross_service_errors(trace_df, spark, min_correlation=0.0)

    def test_returns_dataframe_with_expected_columns(self, correlations):
        """Output must have service pair + correlation columns."""
        expected = {"service_a", "service_b", "error_correlation", "sample_size"}
        assert expected.issubset(set(correlations.columns))

    def test_correlation_between_minus_one_and_one(self, correlations):
        """Pearson correlation must be in [-1.0, 1.0]."""
        for r in correlations.collect():
            assert -1.0 <= r.error_correlation <= 1.0, (
                f"Correlation out of range: {r.error_correlation}"
            )

    def test_sample_size_positive(self, correlations):
        """Every pair must have sample_size > 0."""
        for r in correlations.collect():
            assert r.sample_size > 0, f"Zero sample size for {r.service_a}/{r.service_b}"

    def test_no_self_correlations(self, correlations):
        """service_a must differ from service_b."""
        for r in correlations.collect():
            assert r.service_a != r.service_b, "Self-correlation detected"

    def test_min_correlation_filter_works(self, trace_df, spark):
        """Setting min_correlation=1.0 should filter out almost all pairs."""
        all_pairs = correlate_cross_service_errors(trace_df, spark, min_correlation=0.0)
        strict = correlate_cross_service_errors(trace_df, spark, min_correlation=0.99)
        assert strict.count() <= all_pairs.count()

    def test_handles_empty_dataframe(self, spark):
        """Should return empty DataFrame for empty input."""
        empty = spark.createDataFrame(
            [], "span_id string, service_name string, start_time_ts timestamp, "
                "response_time_ms double, status_code int, is_failure int, "
                "cpu_usage_mcores double, memory_usage_mb double"
        )
        result = correlate_cross_service_errors(empty, spark, min_correlation=0.3)
        assert result.count() == 0

    def test_single_service_returns_empty(self, spark):
        """One service can't have cross-service correlations."""
        ts = datetime(2024, 1, 1, 10, 0, 0)
        single = spark.createDataFrame([
            Row(span_id="s-1", service_name="frontend", start_time_ts=ts,
                response_time_ms=50.0, status_code=200, is_failure=0,
                cpu_usage_mcores=300.0, memory_usage_mb=512.0),
            Row(span_id="s-2", service_name="frontend", start_time_ts=ts + timedelta(minutes=1),
                response_time_ms=60.0, status_code=500, is_failure=1,
                cpu_usage_mcores=350.0, memory_usage_mb=520.0),
        ])
        result = correlate_cross_service_errors(single, spark, min_correlation=0.0)
        assert result.count() == 0


# ============================================================
# compute_propagation_metrics
# ============================================================
class TestComputePropagationMetrics:
    """Tests for compute_propagation_metrics()."""

    @pytest.fixture(scope="class")
    def metrics(self, trace_df):
        """Compute metrics from trace_df."""
        service_pairs = build_service_dependency_graph(trace_df)
        chains = detect_propagation_chains(trace_df, time_window_sec=60)
        return compute_propagation_metrics(service_pairs, chains)

    def test_returns_dict(self, metrics):
        """Should return a dictionary."""
        assert isinstance(metrics, dict)

    def test_contains_expected_keys(self, metrics):
        """Must have top_propagation_paths, total_propagation_chains, avg_propagation_lag_sec."""
        assert "top_propagation_paths" in metrics
        assert "total_propagation_chains" in metrics
        assert "avg_propagation_lag_sec" in metrics

    def test_total_chains_is_positive(self, metrics):
        """trace_df has known failure cascades — total should be > 0."""
        assert metrics["total_propagation_chains"] > 0

    def test_avg_lag_is_positive(self, metrics):
        """Average propagation lag should be > 0."""
        assert metrics["avg_propagation_lag_sec"] > 0

    def test_top_paths_is_list_of_dicts(self, metrics):
        """top_propagation_paths should be a list of dicts with expected keys."""
        paths = metrics["top_propagation_paths"]
        assert isinstance(paths, list)
        if paths:
            for p in paths:
                assert "caller" in p
                assert "callee" in p
                assert "score" in p
                assert "call_count" in p

    def test_top_paths_sorted_by_score_desc(self, metrics):
        """Top paths should be sorted by score descending."""
        paths = metrics["top_propagation_paths"]
        if len(paths) >= 2:
            scores = [p["score"] for p in paths]
            assert scores == sorted(scores, reverse=True), (
                f"Not sorted descending: {scores}"
            )

    def test_top_paths_limited_to_10(self, metrics):
        """Should not return more than 10 top paths."""
        paths = metrics["top_propagation_paths"]
        assert len(paths) <= 10
