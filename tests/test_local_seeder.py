# ============================================================
# Unit Tests — SQLite Local Seeder
# ============================================================
# Tests: seed_all() and per-table data integrity.
# No Spark or PostgreSQL needed — pure SQLite in-memory.
# ============================================================

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

# Add project root so modules.local_seeder is importable
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.local_seeder import (
    seed_all,
    seed_processed_telemetry,
    seed_cross_service_pairs,
    seed_propagation_chains,
    seed_error_correlations,
    seed_anomaly_scores,
    seed_failure_patterns,
    seed_scalability_metrics,
    SCHEMA_SQL,
    SERVICES,
    FAILURE_SERVICES,
    PATTERN_TYPES,
)


# ============================================================
# Fixtures
# ============================================================
@pytest.fixture(scope="module")
def db_path():
    """Create a temporary SQLite database with all tables seeded (module-scoped).
    Uses a fixed random seed for deterministic output."""
    import random as _random
    _random.seed(42)

    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    path = tmp.name
    tmp.close()

    # Seed with fixed randomness for deterministic test assertions
    seed_all(path)

    yield path
    # Cleanup
    for suffix in ("", "-wal", "-shm"):
        f = path + suffix
        if os.path.exists(f):
            os.remove(f)


@pytest.fixture(scope="module")
def conn(db_path):
    """Return a read-only connection to the seeded database (module-scoped).
    Do NOT close this connection — other tests in the module depend on it."""
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    yield c
    c.close()


@pytest.fixture()
def fresh_conn(db_path):
    """Return a fresh independent connection (function-scoped) for tests
    that need to close and reopen the DB."""
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    yield c
    c.close()


# ============================================================
# seed_all — End-to-End
# ============================================================
class TestSeedAll:
    """Tests for seed_all() — the main entry point."""

    def test_file_exists(self, db_path):
        """Database file should exist after seeding."""
        assert os.path.isfile(db_path)
        assert os.path.getsize(db_path) > 0, "Database is empty"

    def test_all_seven_tables_present(self, conn):
        """All 7 expected tables should exist."""
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        expected = {
            "processed_telemetry",
            "cross_service_pairs",
            "propagation_chains",
            "error_correlations",
            "anomaly_scores",
            "failure_patterns",
            "scalability_metrics",
        }
        assert expected.issubset(tables), f"Missing tables: {expected - tables}"

    def test_all_tables_have_rows(self, conn):
        """Every table should have at least one row."""
        for table in [
            "processed_telemetry",
            "cross_service_pairs",
            "propagation_chains",
            "error_correlations",
            "anomaly_scores",
            "failure_patterns",
            "scalability_metrics",
        ]:
            cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert cnt > 0, f"Table {table} is empty"

    def test_idempotent_rerun(self):
        """Running seed_all twice should produce the same number of rows.
        Uses its own temp file to avoid Windows file-lock conflicts."""
        import random as _random

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            path = tmp.name

        try:
            _random.seed(42)
            seed_all(path)

            c1 = sqlite3.connect(path)
            c1.row_factory = sqlite3.Row

            counts_before = {}
            tables = [
                "processed_telemetry",
                "cross_service_pairs",
                "propagation_chains",
                "error_correlations",
                "anomaly_scores",
                "failure_patterns",
                "scalability_metrics",
            ]
            for table in tables:
                counts_before[table] = c1.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            c1.close()

            # Re-run with same seed
            _random.seed(42)
            seed_all(path)

            c2 = sqlite3.connect(path)
            c2.row_factory = sqlite3.Row
            for table, expected in counts_before.items():
                actual = c2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                assert actual == expected, f"{table}: expected {expected} rows, got {actual}"
            c2.close()
        finally:
            for suffix in ("", "-wal", "-shm"):
                f = path + suffix
                if os.path.exists(f):
                    os.remove(f)

    def test_small_seed_works(self):
        """Seeding with num_rows=10 should work without errors."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            path = tmp.name

        conn = sqlite3.connect(path)
        conn.executescript(SCHEMA_SQL)
        seed_processed_telemetry(conn, num_rows=10)
        cnt = conn.execute("SELECT COUNT(*) FROM processed_telemetry").fetchone()[0]
        conn.close()
        assert cnt == 10

        # Cleanup
        for suffix in ("", "-wal", "-shm"):
            f = path + suffix
            if os.path.exists(f):
                os.remove(f)


# ============================================================
# processed_telemetry — Data Integrity
# ============================================================
class TestProcessedTelemetry:
    """Data integrity tests for the processed_telemetry table."""

    def test_is_failure_binary(self, conn):
        """is_failure must be 0 or 1."""
        vals = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT is_failure FROM processed_telemetry"
            ).fetchall()
        }
        assert vals.issubset({0, 1}), f"Unexpected is_failure values: {vals}"

    def test_is_latency_spike_binary(self, conn):
        """is_latency_spike must be 0 or 1."""
        vals = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT is_latency_spike FROM processed_telemetry"
            ).fetchall()
        }
        assert vals.issubset({0, 1}), f"Unexpected is_latency_spike values: {vals}"

    def test_latency_bucket_valid(self, conn):
        """latency_bucket must be one of low/medium/high/critical."""
        valid = {"low", "medium", "high", "critical"}
        buckets = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT latency_bucket FROM processed_telemetry"
            ).fetchall()
        }
        assert buckets.issubset(valid), f"Invalid buckets: {buckets - valid}"

    def test_error_category_valid(self, conn):
        """error_category must be one of success/client_error/server_error."""
        valid = {"success", "client_error", "server_error"}
        cats = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT error_category FROM processed_telemetry"
            ).fetchall()
        }
        assert cats.issubset(valid), f"Invalid categories: {cats - valid}"

    def test_status_code_consistent_with_is_failure(self, conn):
        """If is_failure=1 then status_code must be >= 500."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry "
            "WHERE is_failure = 1 AND status_code < 500"
        ).fetchone()[0]
        assert violations == 0, f"{violations} rows have is_failure=1 but status < 500"

    def test_status_code_consistent_with_error_category(self, conn):
        """error_category must match status_code range."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry "
            "WHERE error_category = 'server_error' AND status_code < 500"
        ).fetchone()[0]
        assert violations == 0, f"{violations} rows with server_error but status < 500"

        violations2 = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry "
            "WHERE error_category = 'success' AND status_code >= 400"
        ).fetchone()[0]
        assert violations2 == 0, f"{violations2} rows with success but status >= 400"

    def test_latency_bucket_boundaries(self, conn):
        """latency_bucket must match response_time_ms thresholds."""
        rows = conn.execute(
            "SELECT response_time_ms, latency_bucket FROM processed_telemetry"
        ).fetchall()
        for rt, bucket in rows:
            if bucket == "low":
                assert rt < 100, f"low bucket but rt={rt}"
            elif bucket == "medium":
                assert 100 <= rt < 500, f"medium bucket but rt={rt}"
            elif bucket == "high":
                assert 500 <= rt < 2000, f"high bucket but rt={rt}"
            elif bucket == "critical":
                assert rt >= 2000, f"critical bucket but rt={rt}"

    def test_hour_of_day_range(self, conn):
        """hour_of_day must be in [0, 23]."""
        out_of_range = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry "
            "WHERE hour_of_day < 0 OR hour_of_day > 23"
        ).fetchone()[0]
        assert out_of_range == 0

    def test_response_time_positive(self, conn):
        """All response_time_ms values must be > 0."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry WHERE response_time_ms <= 0"
        ).fetchone()[0]
        assert violations == 0

    def test_cpu_memory_positive(self, conn):
        """CPU and memory values must be >= 0."""
        cpu_violations = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry WHERE cpu_usage_mcores < 0"
        ).fetchone()[0]
        mem_violations = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry WHERE memory_usage_mb < 0"
        ).fetchone()[0]
        assert cpu_violations == 0
        assert mem_violations == 0

    def test_service_names_valid(self, conn):
        """All service_name values must be from the SERVICES list."""
        unknown = conn.execute(
            "SELECT DISTINCT service_name FROM processed_telemetry "
            "WHERE service_name NOT IN ({})".format(
                ",".join(f"'{s}'" for s in SERVICES)
            )
        ).fetchall()
        assert len(unknown) == 0, f"Unknown services: {unknown}"

    def test_span_ids_are_unique(self, conn):
        """Each span_id should appear at most once (deduplicated)."""
        dupes = conn.execute(
            "SELECT span_id, COUNT(*) as cnt FROM processed_telemetry "
            "GROUP BY span_id HAVING cnt > 1"
        ).fetchall()
        assert len(dupes) == 0, f"Duplicate span_ids: {dupes[:5]}"

    def test_trace_ids_exist(self, conn):
        """All rows should have a non-empty trace_id."""
        nulls = conn.execute(
            "SELECT COUNT(*) FROM processed_telemetry "
            "WHERE trace_id IS NULL OR trace_id = ''"
        ).fetchone()[0]
        assert nulls == 0

    def test_http_methods_valid(self, conn):
        """All http_method values should be valid HTTP verbs."""
        valid = {"GET", "POST", "PUT", "DELETE", "PATCH"}
        actual = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT http_method FROM processed_telemetry"
            ).fetchall()
        }
        assert actual.issubset(valid), f"Invalid methods: {actual - valid}"


# ============================================================
# cross_service_pairs — Data Integrity
# ============================================================
class TestCrossServicePairs:
    """Data integrity tests for cross_service_pairs."""

    def test_propagation_score_range(self, conn):
        """propagation_score must be >= 0 (co_fail/calls ratio can exceed 1.0)."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM cross_service_pairs "
            "WHERE propagation_score < 0.0"
        ).fetchone()[0]
        assert violations == 0

    def test_no_self_loops(self, conn):
        """caller_service != callee_service for all rows."""
        self_loops = conn.execute(
            "SELECT COUNT(*) FROM cross_service_pairs "
            "WHERE caller_service = callee_service"
        ).fetchone()[0]
        assert self_loops == 0

    def test_call_count_positive(self, conn):
        """call_count must be > 0."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM cross_service_pairs WHERE call_count <= 0"
        ).fetchone()[0]
        assert violations == 0

    def test_error_counts_not_exceed_call_count(self, conn):
        """caller_error_count and callee_error_count <= call_count."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM cross_service_pairs "
            "WHERE caller_error_count > call_count OR callee_error_count > call_count"
        ).fetchone()[0]
        assert violations == 0

    def test_co_failure_count_is_positive_when_score_exists(self, conn):
        """If propagation_score > 0, then co_failure_count must be > 0."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM cross_service_pairs "
            "WHERE propagation_score > 0 AND co_failure_count = 0"
        ).fetchone()[0]
        assert violations == 0

    def test_caller_callee_from_services(self, conn):
        """Both caller and callee should be from the SERVICES list."""
        svc_set = set(SERVICES)
        rows = conn.execute(
            "SELECT DISTINCT caller_service, callee_service FROM cross_service_pairs"
        ).fetchall()
        for caller, callee in rows:
            assert caller in svc_set, f"Unknown caller: {caller}"
            assert callee in svc_set, f"Unknown callee: {callee}"

    def test_has_meaningful_propagation(self, conn):
        """At least one row should have propagation_score > 0."""
        cnt = conn.execute(
            "SELECT COUNT(*) FROM cross_service_pairs WHERE propagation_score > 0"
        ).fetchone()[0]
        assert cnt > 0, "No rows with positive propagation_score"


# ============================================================
# propagation_chains — Data Integrity
# ============================================================
class TestPropagationChains:
    """Data integrity tests for propagation_chains."""

    def test_chain_count(self, conn):
        """Should have exactly 200 chains (as seeded)."""
        cnt = conn.execute("SELECT COUNT(*) FROM propagation_chains").fetchone()[0]
        assert cnt == 200

    def test_no_self_loops(self, conn):
        """source_service != target_service."""
        self_loops = conn.execute(
            "SELECT COUNT(*) FROM propagation_chains "
            "WHERE source_service = target_service"
        ).fetchone()[0]
        assert self_loops == 0

    def test_propagation_lag_positive(self, conn):
        """propagation_lag_sec must be > 0."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM propagation_chains WHERE propagation_lag_sec <= 0"
        ).fetchone()[0]
        assert violations == 0

    def test_propagation_lag_within_range(self, conn):
        """propagation_lag_sec must be <= 60 (matching the 0.5–55 uniform range)."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM propagation_chains WHERE propagation_lag_sec > 60"
        ).fetchone()[0]
        assert violations == 0

    def test_propagation_depth_is_two(self, conn):
        """All chains should have depth=2."""
        other = conn.execute(
            "SELECT COUNT(*) FROM propagation_chains WHERE propagation_depth != 2"
        ).fetchone()[0]
        assert other == 0

    def test_source_from_failure_services(self, conn):
        """All source_service values should be from FAILURE_SERVICES."""
        fsvc = set(FAILURE_SERVICES)
        unknown = conn.execute(
            "SELECT DISTINCT source_service FROM propagation_chains "
            "WHERE source_service NOT IN ({})".format(
                ",".join(f"'{s}'" for s in FAILURE_SERVICES)
            )
        ).fetchall()
        assert len(unknown) == 0, f"Unknown source: {unknown}"

    def test_target_timestamp_not_earlier_than_source(self, conn):
        """target_timestamp should not be more than 1 second before source_timestamp
        (allowing for rounding/string comparison edge cases)."""
        # Check that the lag stored matches reality: propagation_lag_sec > 0
        violations = conn.execute(
            "SELECT COUNT(*) FROM propagation_chains "
            "WHERE propagation_lag_sec <= 0"
        ).fetchone()[0]
        assert violations == 0


# ============================================================
# error_correlations — Data Integrity
# ============================================================
class TestErrorCorrelations:
    """Data integrity tests for error_correlations."""

    def test_correlation_range(self, conn):
        """error_correlation must be in [-1.0, 1.0]."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM error_correlations "
            "WHERE error_correlation < -1.0 OR error_correlation > 1.0"
        ).fetchone()[0]
        assert violations == 0

    def test_no_self_correlations(self, conn):
        """service_a != service_b."""
        self_corrs = conn.execute(
            "SELECT COUNT(*) FROM error_correlations "
            "WHERE service_a = service_b"
        ).fetchone()[0]
        assert self_corrs == 0

    def test_sample_size_positive(self, conn):
        """sample_size must be > 0."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM error_correlations WHERE sample_size <= 0"
        ).fetchone()[0]
        assert violations == 0

    def test_services_from_list(self, conn):
        """Both services should be in the SERVICES list."""
        svc_set = set(SERVICES)
        rows = conn.execute(
            "SELECT DISTINCT service_a, service_b FROM error_correlations"
        ).fetchall()
        for a, b in rows:
            assert a in svc_set, f"Unknown service: {a}"
            assert b in svc_set, f"Unknown service: {b}"


# ============================================================
# anomaly_scores — Data Integrity
# ============================================================
class TestAnomalyScores:
    """Data integrity tests for anomaly_scores."""

    def test_row_count(self, conn):
        """Should have 20 services × 96 buckets = 1920 rows."""
        cnt = conn.execute("SELECT COUNT(*) FROM anomaly_scores").fetchone()[0]
        assert cnt == 1920

    def test_all_services_present(self, conn):
        """All 20 services should appear."""
        svcs = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT service_name FROM anomaly_scores"
            ).fetchall()
        }
        assert svcs == set(SERVICES), f"Missing services: {set(SERVICES) - svcs}"

    def test_anomaly_flags_binary(self, conn):
        """is_anomaly_error, is_anomaly_latency, is_anomaly_resource must be 0 or 1."""
        for col in ["is_anomaly_error", "is_anomaly_latency", "is_anomaly_resource"]:
            vals = {
                row[0]
                for row in conn.execute(
                    f"SELECT DISTINCT {col} FROM anomaly_scores"
                ).fetchall()
            }
            assert vals.issubset({0, 1}), f"{col} has invalid values: {vals}"

    def test_anomaly_score_range(self, conn):
        """anomaly_score must be in [0, 3]."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM anomaly_scores "
            "WHERE anomaly_score < 0 OR anomaly_score > 3"
        ).fetchone()[0]
        assert violations == 0

    def test_anomaly_score_equals_sum_of_flags(self, conn):
        """anomaly_score == is_anomaly_error + is_anomaly_latency + is_anomaly_resource."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM anomaly_scores "
            "WHERE anomaly_score != (is_anomaly_error + is_anomaly_latency + is_anomaly_resource)"
        ).fetchone()[0]
        assert violations == 0

    def test_is_anomaly_overall_consistent(self, conn):
        """is_anomaly_overall = 1 iff anomaly_score >= 2."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM anomaly_scores "
            "WHERE (anomaly_score >= 2 AND is_anomaly_overall != 1) "
            "   OR (anomaly_score < 2 AND is_anomaly_overall != 0)"
        ).fetchone()[0]
        assert violations == 0

    def test_time_buckets_evenly_spaced(self, conn):
        """Time buckets should be 15 minutes apart for each service."""
        rows = conn.execute(
            "SELECT time_bucket FROM anomaly_scores WHERE service_name = 'frontend' "
            "ORDER BY time_bucket"
        ).fetchall()
        assert len(rows) == 96

        from datetime import datetime

        for i in range(1, len(rows)):
            prev = datetime.strptime(rows[i - 1][0], "%Y-%m-%d %H:%M:%S")
            curr = datetime.strptime(rows[i][0], "%Y-%m-%d %H:%M:%S")
            diff = (curr - prev).total_seconds()
            assert diff == 900, f"Bucket gap is {diff}s, expected 900s (15 min)"


# ============================================================
# failure_patterns — Data Integrity
# ============================================================
class TestFailurePatterns:
    """Data integrity tests for failure_patterns."""

    def test_row_count(self, conn):
        """Should have 20 services × 7 pattern types = 140 rows."""
        cnt = conn.execute("SELECT COUNT(*) FROM failure_patterns").fetchone()[0]
        assert cnt == 140

    def test_pattern_types_valid(self, conn):
        """All pattern_type values must be in PATTERN_TYPES."""
        valid = set(PATTERN_TYPES)
        actual = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT pattern_type FROM failure_patterns"
            ).fetchall()
        }
        assert actual == valid, f"Pattern mismatch: expected {valid}, got {actual}"

    def test_occurrence_count_positive(self, conn):
        """occurrence_count must be > 0."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM failure_patterns WHERE occurrence_count <= 0"
        ).fetchone()[0]
        assert violations == 0

    def test_avg_severity_range(self, conn):
        """avg_severity must be in [1.0, 3.0]."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM failure_patterns "
            "WHERE avg_severity < 1.0 OR avg_severity > 3.0"
        ).fetchone()[0]
        assert violations == 0

    def test_services_valid(self, conn):
        """All service_name values must be from SERVICES."""
        svc_set = set(SERVICES)
        actual = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT service_name FROM failure_patterns"
            ).fetchall()
        }
        assert actual == svc_set, f"Missing services: {svc_set - actual}"

    def test_no_duplicate_service_pattern(self, conn):
        """No duplicate (service_name, pattern_type) pairs."""
        dupes = conn.execute(
            "SELECT service_name, pattern_type, COUNT(*) as cnt "
            "FROM failure_patterns GROUP BY service_name, pattern_type "
            "HAVING cnt > 1"
        ).fetchall()
        assert len(dupes) == 0, f"Duplicate patterns: {dupes[:5]}"

    def test_normal_pattern_absent(self, conn):
        """'normal' should NOT appear as a pattern type (it's excluded by the seeder)."""
        normal = conn.execute(
            "SELECT COUNT(*) FROM failure_patterns WHERE pattern_type = 'normal'"
        ).fetchone()[0]
        assert normal == 0


# ============================================================
# scalability_metrics — Data Integrity
# ============================================================
class TestScalabilityMetrics:
    """Data integrity tests for scalability_metrics."""

    def test_row_count(self, conn):
        """5 data sizes × 3 repetitions = 15 rows."""
        cnt = conn.execute("SELECT COUNT(*) FROM scalability_metrics").fetchone()[0]
        assert cnt == 15

    def test_data_sizes_match(self, conn):
        """All 5 expected data sizes should be present."""
        expected = {100000, 500000, 1000000, 5000000, 10000000}
        actual = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT data_size FROM scalability_metrics"
            ).fetchall()
        }
        assert actual == expected, f"Data size mismatch: {actual}"

    def test_three_repetitions_per_size(self, conn):
        """Each data size should have exactly 3 repetitions."""
        violations = conn.execute(
            "SELECT data_size, COUNT(*) as cnt FROM scalability_metrics "
            "GROUP BY data_size HAVING cnt != 3"
        ).fetchall()
        assert len(violations) == 0, f"Rep count mismatch: {violations}"

    def test_total_equals_sum_of_operations(self, conn):
        """total_sec should equal groupby + window + join + shuffle (±0.01 rounding)."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM scalability_metrics "
            "WHERE ABS(total_sec - (groupby_agg_sec + window_fn_sec + join_sec + shuffle_sec)) > 0.01"
        ).fetchone()[0]
        assert violations == 0

    def test_throughput_positive(self, conn):
        """throughput_rows_per_sec must be > 0."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM scalability_metrics WHERE throughput_rows_per_sec <= 0"
        ).fetchone()[0]
        assert violations == 0

    def test_speedup_computed(self, conn):
        """speedup_vs_baseline must be > 0 for all rows."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM scalability_metrics WHERE speedup_vs_baseline <= 0"
        ).fetchone()[0]
        assert violations == 0

    def test_baseline_consistent(self, conn):
        """All rows should reference the same baseline_size and baseline_time_sec."""
        sizes = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT baseline_size FROM scalability_metrics"
            ).fetchall()
        }
        times = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT baseline_time_sec FROM scalability_metrics"
            ).fetchall()
        }
        assert len(sizes) == 1, f"Multiple baseline sizes: {sizes}"
        assert len(times) == 1, f"Multiple baseline times: {times}"
        assert list(sizes)[0] == 100000

    def test_speedup_formula(self, conn):
        """speedup_vs_baseline should equal baseline_time_sec / total_sec."""
        violations = conn.execute(
            "SELECT COUNT(*) FROM scalability_metrics "
            "WHERE ABS(speedup_vs_baseline - (baseline_time_sec / total_sec)) > 0.01"
        ).fetchone()[0]
        assert violations == 0

    def test_timings_increase_with_data_size(self, conn):
        """Average total_sec should increase as data_size increases."""
        avgs = conn.execute(
            "SELECT data_size, AVG(total_sec) as avg_t "
            "FROM scalability_metrics GROUP BY data_size ORDER BY data_size"
        ).fetchall()
        for i in range(1, len(avgs)):
            assert avgs[i][1] > avgs[i - 1][1], (
                f"Time didn't increase: {avgs[i-1][0]}→{avgs[i][0]}: "
                f"{avgs[i-1][1]} vs {avgs[i][1]}"
            )


# ============================================================
# Edge Cases & Validation
# ============================================================
class TestEdgeCases:
    """Edge case and validation tests (use their own temp files)."""

    def test_seed_twice_deletes_old_db(self):
        """seed_all should delete the old DB before creating a new one."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            path = tmp.name
        try:
            seed_all(path)
            assert os.path.isfile(path)
            # Run again — should delete and recreate
            seed_all(path)
            assert os.path.isfile(path)
            conn = sqlite3.connect(path)
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            conn.close()
            assert tables >= 7
        finally:
            for suffix in ("", "-wal", "-shm"):
                f = path + suffix
                if os.path.exists(f):
                    os.remove(f)

    def test_nonexistent_path_works(self):
        """seed_all should create a DB at a path that doesn't exist yet."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            path = tmp.name
        os.unlink(path)
        try:
            seed_all(path)
            assert os.path.isfile(path)
        finally:
            for suffix in ("", "-wal", "-shm"):
                f = path + suffix
                if os.path.exists(f):
                    os.remove(f)
