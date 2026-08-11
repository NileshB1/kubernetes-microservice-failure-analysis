# ============================================================
# SQLite Seeder — Standalone sample data generator
# ============================================================
# Creates a SQLite database with the same schema as the
# PostgreSQL tables and populates it with realistic sample data.
#
# Called by run_streamlit.py when --local flag is used.
# Zero external dependencies (only Python stdlib + numpy for random).
# ============================================================

import os
import random
import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("local_seeder")

# --- Constants (mirror production SERVICE_NAMES) ---
SERVICES = [
    "frontend", "auth-service", "user-service", "order-service",
    "payment-service", "inventory-service", "notification-service",
    "shipping-service", "catalog-service", "cart-service",
    "recommendation-service", "search-service", "analytics-service",
    "rate-limiter", "api-gateway", "message-queue",
    "cache-service", "logging-service", "config-service", "discovery-service",
]

FAILURE_SERVICES = ["auth-service", "payment-service", "frontend", "order-service"]

PATTERN_TYPES = [
    "cascading_failure", "error_surge", "latency_spike",
    "resource_pressure", "full_failure", "resource_exhaustion",
    "error_resource_link",
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
ENDPOINTS = ["get", "create", "update", "delete", "search", "login", "checkout"]
NAMESPACES = ["production", "staging", "canary", "default"]
NODES = [f"node-{i:03d}" for i in range(1, 21)]
PODS = [f"pod-{i:05d}" for i in range(1, 201)]


# ============================================================
# Schema DDL (SQLite-compatible)
# ============================================================

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS processed_telemetry (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id        TEXT,
    service_name    TEXT,
    span_id         TEXT,
    parent_span_id  TEXT,
    namespace       TEXT,
    pod_id          TEXT,
    node_id         TEXT,
    response_time_ms REAL,
    wait_time_ms    REAL,
    processing_time_ms REAL,
    network_latency_ms REAL,
    start_time_ts   TEXT,
    end_time_ts     TEXT,
    duration_ms     REAL,
    http_method     TEXT,
    endpoint        TEXT,
    status_code     INTEGER,
    error_message   TEXT,
    is_failure      INTEGER,
    is_latency_spike INTEGER,
    latency_bucket  TEXT,
    error_category  TEXT,
    hour_of_day     INTEGER,
    cpu_usage_mcores REAL,
    memory_usage_mb REAL,
    cpu_memory_ratio REAL,
    processed_at    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cross_service_pairs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    caller_service    TEXT,
    callee_service    TEXT,
    call_count        INTEGER,
    caller_error_count INTEGER,
    callee_error_count INTEGER,
    co_failure_count  INTEGER,
    avg_callee_latency_ms REAL,
    propagation_score REAL,
    analyzed_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS propagation_chains (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id          TEXT,
    source_service    TEXT,
    target_service    TEXT,
    source_timestamp  TEXT,
    target_timestamp  TEXT,
    propagation_lag_sec REAL,
    propagation_depth INTEGER,
    detected_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS error_correlations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    service_a         TEXT,
    service_b         TEXT,
    error_correlation REAL,
    sample_size       INTEGER,
    computed_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS anomaly_scores (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name      TEXT,
    time_bucket       TEXT,
    is_anomaly_error  INTEGER,
    is_anomaly_latency INTEGER,
    is_anomaly_resource INTEGER,
    anomaly_score     INTEGER,
    is_anomaly_overall INTEGER,
    computed_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS failure_patterns (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name      TEXT,
    pattern_type      TEXT,
    occurrence_count  INTEGER,
    avg_severity      REAL,
    computed_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS scalability_metrics (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    label             TEXT,
    input_rows        INTEGER,
    groupby_agg_sec   REAL,
    window_fn_sec     REAL,
    join_sec          REAL,
    shuffle_sec       REAL,
    total_sec         REAL,
    data_size         INTEGER,
    repetition        INTEGER,
    throughput_rows_per_sec REAL,
    speedup_vs_baseline REAL,
    baseline_size     INTEGER,
    baseline_time_sec REAL,
    recorded_at       TEXT DEFAULT (datetime('now'))
);
"""


def _fmt_ts(dt: datetime) -> str:
    """Format datetime as ISO-8601 string for SQLite TEXT column."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# Seed Functions
# ============================================================

def seed_processed_telemetry(conn: sqlite3.Connection, num_rows: int = 5000):
    """Insert realistic processed telemetry."""
    logger.info(f"Seeding {num_rows:,} processed_telemetry rows...")

    base_time = datetime(2024, 1, 1, 0, 0, 0)
    rows = []

    for i in range(num_rows):
        svc = random.choice(SERVICES)
        is_fail = 1 if (svc in FAILURE_SERVICES and random.random() < 0.12) else 0
        status = random.choice([500, 502, 503, 504]) if is_fail else random.choice(
            [200, 200, 200, 200, 201, 204, 301]
        )
        rt = round(max(5, abs(random.gauss(80, 60))), 2)
        is_spike = 1 if rt > 2000 else 0

        rows.append((
            f"trace-{i % 500:06d}",
            svc,
            f"span-{i:010d}",
            f"span-{max(0, random.randint(0, max(0, i - 1))):010d}" if random.random() > 0.3 else "",
            random.choice(NAMESPACES),
            random.choice(PODS),
            random.choice(NODES),
            rt,
            round(max(0, random.gauss(15, 20)), 2),
            round(max(1, random.gauss(40, 30)), 2),
            round(max(0, random.gauss(5, 8)), 2),
            _fmt_ts(base_time + timedelta(seconds=random.randint(0, 86400))),
            _fmt_ts(base_time + timedelta(seconds=random.randint(0, 86400) + random.randint(1, 500))),
            round(max(1, random.expovariate(1.0 / 100)), 2),
            random.choice(HTTP_METHODS),
            f"/api/v1/{svc}/{random.choice(ENDPOINTS)}",
            status,
            "Internal server error" if is_fail else "",
            is_fail,
            is_spike,
            "low" if rt < 100 else ("medium" if rt < 500 else ("high" if rt < 2000 else "critical")),
            "server_error" if status >= 500 else ("client_error" if status >= 400 else "success"),
            random.randint(0, 23),
            round(max(10, random.gauss(500, 300)), 2),
            round(max(50, random.gauss(1024, 512)), 2),
            round(random.uniform(0.1, 2.0), 4),
        ))

    conn.executemany(
        """INSERT INTO processed_telemetry
        (trace_id, service_name, span_id, parent_span_id, namespace, pod_id, node_id,
         response_time_ms, wait_time_ms, processing_time_ms, network_latency_ms,
         start_time_ts, end_time_ts, duration_ms, http_method, endpoint,
         status_code, error_message, is_failure, is_latency_spike,
         latency_bucket, error_category, hour_of_day,
         cpu_usage_mcores, memory_usage_mb, cpu_memory_ratio)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    logger.info(f"  OK  {num_rows:,} telemetry rows.")


def seed_cross_service_pairs(conn: sqlite3.Connection):
    """Insert cross-service propagation pairs."""
    logger.info("Seeding cross_service_pairs...")
    pairs = []
    for caller in SERVICES:
        for callee in SERVICES:
            if caller == callee:
                continue
            if random.random() < 0.30:
                call_count = random.randint(100, 10000)
                caller_err = int(call_count * random.uniform(0.01, 0.15))
                callee_err = int(call_count * random.uniform(0.01, 0.20))
                co_fail = min(caller_err, callee_err) + int(
                    random.uniform(0, min(caller_err, callee_err) * 0.5)
                )
                propagation_score = round(co_fail / max(callee_err, 1), 4)
                avg_lat = round(random.uniform(50, 2500), 2)
                pairs.append(
                    (caller, callee, call_count, caller_err, callee_err, co_fail, avg_lat, propagation_score)
                )

    conn.executemany(
        """INSERT INTO cross_service_pairs
        (caller_service, callee_service, call_count, caller_error_count,
         callee_error_count, co_failure_count, avg_callee_latency_ms, propagation_score)
        VALUES (?,?,?,?,?,?,?,?)""",
        pairs,
    )
    conn.commit()
    logger.info(f"  OK  {len(pairs)} cross-service pairs.")


def seed_propagation_chains(conn: sqlite3.Connection):
    """Insert propagation chains."""
    logger.info("Seeding propagation_chains...")
    chains = []
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    for _ in range(200):
        src = random.choice(FAILURE_SERVICES)
        tgt = random.choice([s for s in SERVICES if s != src])
        trace_id = f"trace-{random.randint(0, 499):06d}"
        source_ts = base_time + timedelta(seconds=random.randint(0, 86000))
        lag = round(random.uniform(0.5, 55), 2)
        target_ts = source_ts + timedelta(seconds=lag)
        chains.append((trace_id, src, tgt, _fmt_ts(source_ts), _fmt_ts(target_ts), lag, 2))

    conn.executemany(
        """INSERT INTO propagation_chains
        (trace_id, source_service, target_service,
         source_timestamp, target_timestamp, propagation_lag_sec, propagation_depth)
        VALUES (?,?,?,?,?,?,?)""",
        chains,
    )
    conn.commit()
    logger.info(f"  OK  {len(chains)} propagation chains.")


def seed_error_correlations(conn: sqlite3.Connection):
    """Insert error correlations."""
    logger.info("Seeding error_correlations...")
    corrs = []
    for i, srv_a in enumerate(SERVICES):
        for srv_b in SERVICES[i + 1 :]:
            if random.random() < 0.35:
                corr_val = round(random.uniform(-0.8, 0.9), 4)
                corrs.append((srv_a, srv_b, corr_val, random.randint(100, 5000)))

    conn.executemany(
        "INSERT INTO error_correlations (service_a, service_b, error_correlation, sample_size) "
        "VALUES (?,?,?,?)",
        corrs,
    )
    conn.commit()
    logger.info(f"  OK  {len(corrs)} error correlations.")


def seed_anomaly_scores(conn: sqlite3.Connection):
    """Insert anomaly scores."""
    logger.info("Seeding anomaly_scores...")
    scores = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    buckets_per_service = 96  # 24h at 15-min intervals

    for svc in SERVICES:
        for b in range(buckets_per_service):
            tb = base_time + timedelta(minutes=b * 15)
            is_err = 1 if (svc in FAILURE_SERVICES and random.random() < 0.08) else 0
            is_lat = 1 if random.random() < 0.06 else 0
            is_res = 1 if random.random() < 0.04 else 0
            anomaly_score = is_err + is_lat + is_res
            is_overall = 1 if anomaly_score >= 2 else 0
            scores.append((svc, _fmt_ts(tb), is_err, is_lat, is_res, anomaly_score, is_overall))

    conn.executemany(
        """INSERT INTO anomaly_scores
        (service_name, time_bucket, is_anomaly_error, is_anomaly_latency,
         is_anomaly_resource, anomaly_score, is_anomaly_overall)
        VALUES (?,?,?,?,?,?,?)""",
        scores,
    )
    conn.commit()
    logger.info(f"  OK  {len(scores)} anomaly scores ({buckets_per_service * len(SERVICES)} buckets).")


def seed_failure_patterns(conn: sqlite3.Connection):
    """Insert failure patterns."""
    logger.info("Seeding failure_patterns...")
    patterns = []
    for svc in SERVICES:
        for ptype in PATTERN_TYPES:
            occurrences = random.randint(1, 150)
            severity = round(random.uniform(1.0, 3.0), 2)
            patterns.append((svc, ptype, occurrences, severity))

    conn.executemany(
        "INSERT INTO failure_patterns (service_name, pattern_type, occurrence_count, avg_severity) "
        "VALUES (?,?,?,?)",
        patterns,
    )
    conn.commit()
    logger.info(f"  OK  {len(patterns)} failure patterns.")


def seed_scalability_metrics(conn: sqlite3.Connection):
    """Insert scalability benchmark data."""
    logger.info("Seeding scalability_metrics...")
    data_sizes = [100000, 500000, 1000000, 5000000, 10000000]
    metrics = []

    for size in data_sizes:
        for rep in range(1, 4):
            groupby = round(0.5 + (size / 200000) * random.uniform(0.9, 1.1), 3)
            window_fn = round(0.3 + (size / 150000) * random.uniform(0.9, 1.1), 3)
            join_op = round(0.8 + (size / 100000) * random.uniform(0.9, 1.1), 3)
            shuffle = round(1.0 + (size / 80000) * random.uniform(0.9, 1.1), 3)
            total = round(groupby + window_fn + join_op + shuffle, 3)
            throughput = round(size / max(total, 0.001), 1)
            metrics.append(
                (f"{size:,}_r{rep}", size, groupby, window_fn, join_op, shuffle, total, size, rep, throughput)
            )

    # Compute speedups
    baseline_total = metrics[0][6]
    updated = []
    for m in metrics:
        label, input_rows, gb, wf, jo, sh, total, ds, rep, tp = m
        speedup = round(baseline_total / max(total, 0.001), 3)
        updated.append(
            (label, input_rows, gb, wf, jo, sh, total, ds, rep, tp, speedup, data_sizes[0], baseline_total)
        )

    conn.executemany(
        """INSERT INTO scalability_metrics
        (label, input_rows, groupby_agg_sec, window_fn_sec, join_sec, shuffle_sec,
         total_sec, data_size, repetition, throughput_rows_per_sec,
         speedup_vs_baseline, baseline_size, baseline_time_sec)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        updated,
    )
    conn.commit()
    logger.info(f"  OK  {len(updated)} scalability metrics.")


# ============================================================
# Main Entry Point
# ============================================================

def seed_all(db_path: str):
    """Create a SQLite database, build schema, and populate all tables."""
    logger.info(f"Creating SQLite database at {db_path} ...")

    # Clean up old DB so re-runs are idempotent
    if os.path.exists(db_path):
        os.remove(db_path)
        logger.info("  Removed existing database.")

    conn = sqlite3.connect(db_path)

    # Performance tweaks for bulk inserts
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-20000")  # 20 MB cache

    conn.executescript(SCHEMA_SQL)
    logger.info("  Schema created (7 tables).")

    seed_processed_telemetry(conn, num_rows=5000)
    seed_cross_service_pairs(conn)
    seed_propagation_chains(conn)
    seed_error_correlations(conn)
    seed_anomaly_scores(conn)
    seed_failure_patterns(conn)
    seed_scalability_metrics(conn)

    # Verify
    tables = [
        "processed_telemetry", "cross_service_pairs", "propagation_chains",
        "error_correlations", "anomaly_scores", "failure_patterns",
        "scalability_metrics",
    ]
    for t in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        logger.info(f"  ✓ {t}: {cnt:,} rows")

    conn.close()
    logger.info(f"SQLite database ready: {db_path} ({os.path.getsize(db_path):,} bytes)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dashboard.db",
    )
    seed_all(db_path)
