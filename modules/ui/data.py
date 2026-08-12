
# Dashboard Data Access


from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import streamlit as st

from modules.db_adapter import KNOWN_TABLES, get_table_names, health, placeholder, run_query

CACHE_TTL = 30


def _in_clause(values: Sequence[str]) -> str:
    """Build a placeholder list matching the active backend's paramstyle."""
    return ", ".join(placeholder() for _ in values)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Cached read. Params must be a tuple so the cache key stays hashable."""
    return run_query(sql, params or None)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def db_health() -> dict:
    """Backend, reachability, and per-table row counts."""
    return health()


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def available_tables() -> set[str]:
    """Application tables that currently exist and are readable."""
    return set(get_table_names()) & set(KNOWN_TABLES)


def has_data(table: str) -> bool:
    """True when a table exists and holds at least one row."""
    if table not in available_tables():
        return False
    return int(db_health().get("tables", {}).get(table, 0)) > 0



# Shared dimensions

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def service_names() -> list[str]:
    """Every service seen in processed telemetry, alphabetically."""
    df = query("SELECT DISTINCT service_name FROM processed_telemetry ORDER BY service_name")
    if df.empty:
        return []
    return df["service_name"].dropna().tolist()


def _service_filter(services: Sequence[str], column: str = "service_name") -> tuple[str, tuple]:
    """
    Return a SQL fragment and params restricting `column` to `services`.

    An empty selection means "no filter" rather than "match nothing" -
    that is what a cleared filter control means to a reader.
    """
    if not services:
        return "", ()
    return f" AND {column} IN ({_in_clause(services)})", tuple(services)



# Overview

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def telemetry_summary(services: tuple[str, ...] = ()) -> dict:
    """Headline counts across processed telemetry for the selected services."""
    clause, params = _service_filter(services)
    df = query(
        "SELECT COUNT(*) AS total_records, "
        "  SUM(is_failure) AS failures, "
        "    AVG(response_time_ms) AS avg_latency, "
        "  COUNT(DISTINCT service_name) AS services, "
        "  COUNT(DISTINCT trace_id) AS traces "
        f"FROM processed_telemetry WHERE 1=1{clause}",
        params,
    )
    if df.empty or df.iloc[0]["total_records"] in (None, 0):
        return {
            "total_records": 0,  "failures": 0,
            "avg_latency": None,   "services": 0,
            "traces": 0, "error_rate": None,
        }

    row = df.iloc[0]
    total = int(row["total_records"] or 0)
    failures = int(row["failures"] or 0)
    return {
        "total_records": total, "failures": failures,
        "avg_latency": float(row["avg_latency"]) if row["avg_latency"] is not None else None,
        "services": int(row["services"] or 0),
        "traces": int(row["traces"] or 0),
        "error_rate": (failures / total * 100) if total else None,
    }


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def error_rate_by_hour(services: tuple[str, ...] = ()) -> pd.DataFrame:
    """Requests and failures per hour of day - the shape of the workload."""
    clause, params = _service_filter(services)
    return query(
        "SELECT hour_of_day, COUNT(*) AS requests, SUM(is_failure) AS failures "
        f"FROM processed_telemetry WHERE hour_of_day IS NOT NULL{clause} "
        "GROUP BY hour_of_day ORDER BY hour_of_day",
        params,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def service_health_table(services: tuple[str, ...] = ()) -> pd.DataFrame:
    """Per-service request volume, error rate, and latency."""
    clause, params = _service_filter(services)
    return query(
        "SELECT service_name, "
        " COUNT(*) AS requests, "
        "  SUM(is_failure) AS failures, "
        "   AVG(response_time_ms) AS avg_latency_ms, "
        " AVG(cpu_usage_mcores) AS avg_cpu_mcores, "
        "  AVG(memory_usage_mb) AS avg_memory_mb "
        f"FROM processed_telemetry WHERE 1=1{clause} "
        "GROUP BY service_name ORDER BY COUNT(*) DESC",
        params,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def latency_distribution(services: tuple[str, ...] = ()) -> pd.DataFrame:
    """Request counts per latency bucket."""
    clause, params = _service_filter(services)
    return query(
        "SELECT latency_bucket, COUNT(*) AS requests "
        f"FROM processed_telemetry WHERE latency_bucket IS NOT NULL{clause} "
        "GROUP BY latency_bucket",
        params,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def error_categories(services: tuple[str, ...] = ()) -> pd.DataFrame:
    """Outcome mix - success, client error, server error."""
    clause, params = _service_filter(services)
    return query(
        "SELECT error_category, COUNT(*) AS requests "
        f"FROM processed_telemetry WHERE error_category IS NOT NULL{clause} "
        "GROUP BY error_category ORDER BY COUNT(*) DESC",
        params,
    )


# RQ1 - cross-service propagation

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def propagation_pairs(min_score: float = 0.0, limit: int = 400) -> pd.DataFrame:
    """Caller->callee pairs ranked by propagation score."""
    return query(
        "SELECT caller_service, callee_service, call_count, caller_error_count, "
        "   callee_error_count, co_failure_count, avg_callee_latency_ms, "
        "  propagation_score "
        f"FROM cross_service_pairs WHERE propagation_score >= {placeholder()} "
        f"ORDER BY propagation_score DESC LIMIT {placeholder()}",
        (min_score, limit),
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def propagation_chains(limit: int = 15) -> pd.DataFrame:
    """Most frequently observed source->target failure cascades."""
    return query(
        "SELECT source_service, target_service, COUNT(*) AS chain_count, "
        "    AVG(propagation_lag_sec) AS avg_lag_sec "
        "FROM propagation_chains GROUP BY source_service, target_service "
        f"ORDER BY COUNT(*) DESC LIMIT {placeholder()}",
        (limit,),
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def propagation_lag_profile() -> pd.DataFrame:
    """How long a failure takes to reach the next service."""
    return query(
        "SELECT source_service, propagation_lag_sec "
        "FROM propagation_chains WHERE propagation_lag_sec IS NOT NULL"
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def error_correlations(limit: int = 15) -> pd.DataFrame:
    """Service pairs whose error rates move together (or oppose)."""
    return query(
        "SELECT service_a, service_b, error_correlation, sample_size "
        f"FROM error_correlations ORDER BY ABS(error_correlation) DESC LIMIT {placeholder()}",
        (limit,),
    )


# RQ2 - anomalies

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def anomaly_summary(services: tuple[str, ...] = ()) -> dict:
    """Counts for each anomaly signal across all analysed time buckets."""
    clause, params = _service_filter(services)
    df = query(
        "SELECT COUNT(*) AS total_buckets, "
        "  SUM(is_anomaly_overall) AS overall, "
        "    SUM(is_anomaly_error) AS error_signal, "
        "  SUM(is_anomaly_latency) AS latency_signal, "
        "   SUM(is_anomaly_resource) AS resource_signal "
        f"FROM anomaly_scores WHERE 1=1{clause}",
        params,
    )
    if df.empty:
        return {}
    row = df.iloc[0]
    total = int(row["total_buckets"] or 0)
    overall = int(row["overall"] or 0)
    return {
        "total_buckets": total,   "overall": overall,
        "error_signal": int(row["error_signal"] or 0),
        "latency_signal": int(row["latency_signal"] or 0),
        "resource_signal": int(row["resource_signal"] or 0),
        "anomaly_rate": (overall / total * 100) if total else None,
    }


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def slo_breach_summary(services: tuple[str, ...] = ()) -> pd.DataFrame:
    """
    Split each service's flags into "regressing" and "chronically bad"
    """
    clause, params = _service_filter(services)
    return query(
        "SELECT service_name, "
        "  SUM(is_error_rate_slo_breach) AS error_slo_breaches, "
        "    SUM(is_latency_slo_breach) AS latency_slo_breaches, "
        "   SUM(is_anomaly_overall) AS overall_anomalies, "
        "   COUNT(*) AS buckets "
        f"FROM anomaly_scores WHERE 1=1{clause} "
        "GROUP BY service_name ORDER BY SUM(is_anomaly_overall) DESC",
        params,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def anomalies_by_service(services: tuple[str, ...] = (), limit: int = 15) -> pd.DataFrame:
    """Services ranked by multi-signal anomaly count."""
    clause, params = _service_filter(services)
    return query(
        "SELECT service_name, COUNT(*) AS anomaly_count "
        f"FROM anomaly_scores WHERE is_anomaly_overall = 1{clause} "
        f"GROUP BY service_name ORDER BY COUNT(*) DESC LIMIT {placeholder()}",
        params + (limit,),
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def anomaly_timeline(services: tuple[str, ...] = ()) -> pd.DataFrame:
    """Anomalies per time bucket - when the system was in trouble."""
    clause, params = _service_filter(services)
    return query(
        "SELECT time_bucket, "
        "  SUM(is_anomaly_error) AS error_signal, "
        "   SUM(is_anomaly_latency) AS latency_signal, "
        "  SUM(is_anomaly_resource) AS resource_signal, "
        "   SUM(is_anomaly_overall) AS multi_signal "
        f"FROM anomaly_scores WHERE 1=1{clause} "
        "GROUP BY time_bucket ORDER BY time_bucket",
        params,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def failure_patterns(services: tuple[str, ...] = ()) -> pd.DataFrame:
    """Occurrences of each failure-pattern type."""
    clause, params = _service_filter(services)
    return query(
        "SELECT pattern_type, SUM(occurrence_count) AS occurrences, "
        "   AVG(avg_severity) AS avg_severity "
        f"FROM failure_patterns WHERE 1=1{clause} "
        "GROUP BY pattern_type ORDER BY SUM(occurrence_count) DESC",
        params,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def failure_patterns_detail(services: tuple[str, ...] = (), limit: int = 200) -> pd.DataFrame:
    """Per-service pattern breakdown."""
    clause, params = _service_filter(services)
    return query(
        "SELECT service_name, pattern_type, occurrence_count, avg_severity "
        f"FROM failure_patterns WHERE 1=1{clause} "
        f"ORDER BY occurrence_count DESC LIMIT {placeholder()}",
        params + (limit,),
    )


# ------------------------------------------------------------
# RQ3 - scalability
# ------------------------------------------------------------
@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def scalability_by_size() -> pd.DataFrame:
    """Mean timings per data size, averaged across repetitions."""
    return query(
        "SELECT data_size, "
        "  AVG(total_sec) AS total_sec, "
        "    AVG(groupby_agg_sec) AS groupby_agg_sec, "
        "  AVG(window_fn_sec) AS window_fn_sec,  AVG(join_sec) AS join_sec, "
        "    AVG(shuffle_sec) AS shuffle_sec, "
        " AVG(throughput_rows_per_sec) AS throughput, "
        "  COUNT(*) AS repetitions "
        "FROM scalability_metrics GROUP BY data_size ORDER BY data_size"
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def scalability_raw() -> pd.DataFrame:
    """Every individual benchmark run."""
    return query(
        "SELECT label, data_size, repetition, groupby_agg_sec, window_fn_sec, "
        "    join_sec, shuffle_sec, total_sec, throughput_rows_per_sec "
        "FROM scalability_metrics ORDER BY data_size, repetition"
    )


def scalability_efficiency(by_size: pd.DataFrame) -> pd.DataFrame:
    """
    Derive scaling efficiency from the per-size timings
    """
    if by_size.empty:
        return pd.DataFrame()

    frame = by_size.copy()
    baseline_size = float(frame["data_size"].iloc[0])
    baseline_time = float(frame["total_sec"].iloc[0])

    frame["size_ratio"] = frame["data_size"] / baseline_size
    frame["time_ratio"] = frame["total_sec"] / baseline_time if baseline_time else float("nan")
    frame["efficiency"] = frame["size_ratio"] / frame["time_ratio"].replace(0, float("nan"))
    return frame
