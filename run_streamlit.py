#!/usr/bin/env python3
# ============================================================
# run_streamlit.py — End-to-End Streamlit Launcher
# ============================================================
# Usage:
#   python run_streamlit.py                    # default: port 8501
#   python run_streamlit.py --port 8502        # custom port
#   python run_streamlit.py --seed-only        # only seed data, don't launch UI
#   python run_streamlit.py --browser          # auto-open browser
#
# What it does:
#   1. Checks connectivity to PostgreSQL
#   2. Seeds sample telemetry + analysis data into PostgreSQL
#      (bypasses Spark/MinIO — purely for dashboard demo)
#   3. Generates placeholder plots in output/
#   4. Launches streamlit run modules/dashboard.py
#
# Requirements:
#   - PostgreSQL running (local or Docker)
#   - Python packages: streamlit, psycopg2-binary, numpy, etc.
#   - Ready in ~10 seconds with sample data
# ============================================================

import argparse
import logging
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

# Add project root to path so modules/ is importable
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_streamlit")


# ============================================================
# Colour helpers for terminal output
# ============================================================
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


def banner():
    print()
    print(f"{CYAN}{'=' * 60}{RESET}")
    print(f"{CYAN}  K8s Microservice Failure Analysis — Streamlit Dashboard{RESET}")
    print(f"{CYAN}{'=' * 60}{RESET}")
    print()


# ============================================================
# PostgreSQL Connection Check
# ============================================================
def check_postgres(host, port, user, password, dbname):
    """Try to connect to PostgreSQL; return True if successful."""
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
        )
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"PostgreSQL not reachable at {host}:{port}: {e}")
        return False


# ============================================================
# Schema Initialisation (CREATE TABLE IF NOT EXISTS)
# ============================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS processed_telemetry (
    id              SERIAL PRIMARY KEY,
    trace_id        VARCHAR(64),
    service_name    VARCHAR(128),
    span_id         VARCHAR(64),
    parent_span_id  VARCHAR(64),
    namespace       VARCHAR(64),
    pod_id          VARCHAR(64),
    node_id         VARCHAR(64),
    response_time_ms DOUBLE PRECISION,
    wait_time_ms    DOUBLE PRECISION,
    processing_time_ms DOUBLE PRECISION,
    network_latency_ms DOUBLE PRECISION,
    start_time_ts   TIMESTAMP,
    end_time_ts     TIMESTAMP,
    duration_ms     DOUBLE PRECISION,
    http_method     VARCHAR(16),
    endpoint        VARCHAR(256),
    status_code     INTEGER,
    error_message   TEXT,
    is_failure      INTEGER,
    is_latency_spike INTEGER,
    latency_bucket  VARCHAR(16),
    error_category  VARCHAR(32),
    hour_of_day     INTEGER,
    cpu_usage_mcores DOUBLE PRECISION,
    memory_usage_mb DOUBLE PRECISION,
    cpu_memory_ratio DOUBLE PRECISION,
    processed_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cross_service_pairs (
    id                SERIAL PRIMARY KEY,
    caller_service    VARCHAR(128),
    callee_service    VARCHAR(128),
    call_count        BIGINT,
    caller_error_count BIGINT,
    callee_error_count BIGINT,
    co_failure_count  BIGINT,
    avg_callee_latency_ms DOUBLE PRECISION,
    propagation_score DOUBLE PRECISION,
    analyzed_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS propagation_chains (
    id                SERIAL PRIMARY KEY,
    trace_id          VARCHAR(64),
    source_service    VARCHAR(128),
    target_service    VARCHAR(128),
    source_timestamp  TIMESTAMP,
    target_timestamp  TIMESTAMP,
    propagation_lag_sec DOUBLE PRECISION,
    propagation_depth INTEGER,
    detected_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS error_correlations (
    id                SERIAL PRIMARY KEY,
    service_a         VARCHAR(128),
    service_b         VARCHAR(128),
    error_correlation DOUBLE PRECISION,
    sample_size       BIGINT,
    computed_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomaly_scores (
    id                SERIAL PRIMARY KEY,
    service_name      VARCHAR(128),
    time_bucket       TIMESTAMP,
    is_anomaly_error  INTEGER,
    is_anomaly_latency INTEGER,
    is_anomaly_resource INTEGER,
    anomaly_score     INTEGER,
    is_anomaly_overall INTEGER,
    computed_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS failure_patterns (
    id                SERIAL PRIMARY KEY,
    service_name      VARCHAR(128),
    pattern_type      VARCHAR(64),
    occurrence_count  BIGINT,
    avg_severity      DOUBLE PRECISION,
    computed_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scalability_metrics (
    id                SERIAL PRIMARY KEY,
    label             VARCHAR(128),
    input_rows        BIGINT,
    groupby_agg_sec   DOUBLE PRECISION,
    window_fn_sec     DOUBLE PRECISION,
    join_sec          DOUBLE PRECISION,
    shuffle_sec       DOUBLE PRECISION,
    total_sec         DOUBLE PRECISION,
    data_size         BIGINT,
    repetition        INTEGER,
    throughput_rows_per_sec DOUBLE PRECISION,
    speedup_vs_baseline DOUBLE PRECISION,
    baseline_size     BIGINT,
    baseline_time_sec DOUBLE PRECISION,
    recorded_at       TIMESTAMP DEFAULT NOW()
);
"""


def init_schema(conn):
    """Create all tables if they don't exist, truncate existing data for clean re-seed."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)

        # Truncate tables before re-seeding for idempotent runs
        tables_to_clear = [
            "processed_telemetry", "cross_service_pairs", "propagation_chains",
            "error_correlations", "anomaly_scores", "failure_patterns",
            "scalability_metrics",
        ]
        for table in tables_to_clear:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")

    conn.commit()
    logger.info("Schema initialised & tables cleared for fresh seed.")


# ============================================================
# Sample Data Generation (bypasses Spark/MinIO — direct SQL)
# ============================================================
SERVICES = [
    "frontend", "auth-service", "user-service", "order-service",
    "payment-service", "inventory-service", "notification-service",
    "shipping-service", "catalog-service", "cart-service",
    "recommendation-service", "search-service", "analytics-service",
    "rate-limiter", "api-gateway", "message-queue",
    "cache-service", "logging-service", "config-service", "discovery-service",
]

FAILURE_SERVICES = ["auth-service", "payment-service", "frontend", "order-service"]
STABLE_SERVICES = ["cache-service", "config-service", "discovery-service", "logging-service"]

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


def seed_processed_telemetry(conn, num_rows=5000):
    """Insert realistic processed telemetry directly into PostgreSQL."""
    logger.info(f"Seeding {num_rows:,} processed telemetry rows...")

    base_time = datetime(2024, 1, 1, 0, 0, 0)
    rows = []

    for i in range(num_rows):
        svc = random.choice(SERVICES)
        is_fail = 1 if (svc in FAILURE_SERVICES and random.random() < 0.12) else 0
        status = random.choice([500, 502, 503, 504]) if is_fail else random.choice([200, 200, 200, 200, 201, 204, 301])
        rt = round(max(5, abs(random.gauss(80, 60))), 2)
        is_spike = 1 if rt > 2000 else 0

        rows.append((
            f"trace-{i % 500:06d}",
            svc,
            f"span-{i:010d}",
            f"span-{max(0, random.randint(0, max(0, i-1))):010d}" if random.random() > 0.3 else "",
            random.choice(NAMESPACES),
            random.choice(PODS),
            random.choice(NODES),
            rt,
            round(max(0, random.gauss(15, 20)), 2),
            round(max(1, random.gauss(40, 30)), 2),
            round(max(0, random.gauss(5, 8)), 2),
            base_time + timedelta(seconds=random.randint(0, 86400)),
            base_time + timedelta(seconds=random.randint(0, 86400) + random.randint(1, 500)),
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

    sql = """
        INSERT INTO processed_telemetry
        (trace_id, service_name, span_id, parent_span_id, namespace, pod_id, node_id,
         response_time_ms, wait_time_ms, processing_time_ms, network_latency_ms,
         start_time_ts, end_time_ts, duration_ms, http_method, endpoint,
         status_code, error_message, is_failure, is_latency_spike,
         latency_bucket, error_category, hour_of_day,
         cpu_usage_mcores, memory_usage_mb, cpu_memory_ratio)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    logger.info(f"  ✓ Inserted {num_rows:,} telemetry rows.")


def seed_cross_service_pairs(conn):
    """Insert sample cross-service propagation data."""
    logger.info("Seeding cross_service_pairs...")
    pairs = []
    for caller in SERVICES:
        for callee in SERVICES:
            if caller == callee:
                continue
            # ~30% of pairs have meaningful propagation
            if random.random() < 0.30:
                call_count = random.randint(100, 10000)
                caller_err = int(call_count * random.uniform(0.01, 0.15))
                callee_err = int(call_count * random.uniform(0.01, 0.20))
                co_fail = min(caller_err, callee_err) + int(random.uniform(0, min(caller_err, callee_err) * 0.5))
                propagation_score = round(co_fail / max(callee_err, 1), 4)
                avg_lat = round(random.uniform(50, 2500), 2)
                pairs.append((caller, callee, call_count, caller_err, callee_err, co_fail, avg_lat, propagation_score))

    sql = """
        INSERT INTO cross_service_pairs
        (caller_service, callee_service, call_count, caller_error_count,
         callee_error_count, co_failure_count, avg_callee_latency_ms, propagation_score)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, pairs)
    conn.commit()
    logger.info(f"  ✓ Inserted {len(pairs)} cross-service pairs.")


def seed_propagation_chains(conn):
    """Insert sample propagation chains."""
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
        chains.append((trace_id, src, tgt, source_ts, target_ts, lag, 2))

    sql = """
        INSERT INTO propagation_chains
        (trace_id, source_service, target_service,
         source_timestamp, target_timestamp, propagation_lag_sec, propagation_depth)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, chains)
    conn.commit()
    logger.info(f"  ✓ Inserted {len(chains)} propagation chains.")


def seed_error_correlations(conn):
    """Insert sample error correlations."""
    logger.info("Seeding error_correlations...")
    corrs = []
    for i, srv_a in enumerate(SERVICES):
        for srv_b in SERVICES[i + 1:]:
            if random.random() < 0.35:
                corr_val = round(random.uniform(-0.8, 0.9), 4)
                corrs.append((srv_a, srv_b, corr_val, random.randint(100, 5000)))

    sql = """
        INSERT INTO error_correlations (service_a, service_b, error_correlation, sample_size)
        VALUES (%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, corrs)
    conn.commit()
    logger.info(f"  ✓ Inserted {len(corrs)} error correlations.")


def seed_anomaly_scores(conn):
    """Insert sample anomaly scores."""
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
            scores.append((svc, tb, is_err, is_lat, is_res, anomaly_score, is_overall))

    sql = """
        INSERT INTO anomaly_scores
        (service_name, time_bucket, is_anomaly_error, is_anomaly_latency,
         is_anomaly_resource, anomaly_score, is_anomaly_overall)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, scores)
    conn.commit()
    logger.info(f"  ✓ Inserted {len(scores)} anomaly scores ({buckets_per_service * len(SERVICES)} buckets).")


def seed_failure_patterns(conn):
    """Insert sample failure patterns."""
    logger.info("Seeding failure_patterns...")
    patterns = []
    for svc in SERVICES:
        for ptype in PATTERN_TYPES:
            occurrences = random.randint(1, 150)
            severity = round(random.uniform(1.0, 3.0), 2)
            patterns.append((svc, ptype, occurrences, severity))

    sql = """
        INSERT INTO failure_patterns
        (service_name, pattern_type, occurrence_count, avg_severity)
        VALUES (%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, patterns)
    conn.commit()
    logger.info(f"  ✓ Inserted {len(patterns)} failure patterns.")


def seed_scalability_metrics(conn):
    """Insert sample scalability benchmark data."""
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

            metrics.append((
                f"{size:,}_r{rep}",
                size,
                groupby,
                window_fn,
                join_op,
                shuffle,
                total,
                size,
                rep,
                throughput,
                None,  # speedup computed below
                data_sizes[0],
                None,  # baseline_time filled below
            ))

    # Compute speedups relative to first entry's total_sec
    baseline_total = metrics[0][6]
    updated = []
    for m in metrics:
        label, input_rows, gb, wf, jo, sh, total, ds, rep, tp, _, bl_size, _ = m
        speedup = round(baseline_total / max(total, 0.001), 3)
        updated.append((label, input_rows, gb, wf, jo, sh, total, ds, rep, tp, speedup, bl_size, baseline_total))

    sql = """
        INSERT INTO scalability_metrics
        (label, input_rows, groupby_agg_sec, window_fn_sec, join_sec, shuffle_sec,
         total_sec, data_size, repetition, throughput_rows_per_sec,
         speedup_vs_baseline, baseline_size, baseline_time_sec)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, updated)
    conn.commit()
    logger.info(f"  ✓ Inserted {len(updated)} scalability metrics.")


# ============================================================
# Full Seeding
# ============================================================
def seed_all(conn):
    """Seed all tables with sample data."""
    seed_processed_telemetry(conn, num_rows=5000)
    seed_cross_service_pairs(conn)
    seed_propagation_chains(conn)
    seed_error_correlations(conn)
    seed_anomaly_scores(conn)
    seed_failure_patterns(conn)
    seed_scalability_metrics(conn)

    # Verify
    with conn.cursor() as cur:
        tables = [
            "processed_telemetry",
            "cross_service_pairs",
            "propagation_chains",
            "error_correlations",
            "anomaly_scores",
            "failure_patterns",
            "scalability_metrics",
        ]
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            cnt = cur.fetchone()[0]
            status = f"{GREEN}✓{RESET}" if cnt > 0 else f"{RED}✗{RESET}"
            logger.info(f"  {status} {t}: {cnt:,} rows")


# ============================================================
# Placeholder Plot Generation
# ============================================================
def generate_plots(project_root=PROJECT_ROOT):
    """Run the placeholder plot generator script."""
    logger.info("Generating placeholder plots...")
    script = os.path.join(project_root, "scripts", "generate_placeholders.py")
    if not os.path.exists(script):
        logger.warning(f"Placeholder script not found: {script}")
        return
    try:
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            logger.info(f"  {GREEN}✓{RESET} Placeholder plots generated.")
        else:
            logger.warning(f"Placeholder script exited with {result.returncode}: {result.stderr[:300]}")
    except Exception as e:
        logger.warning(f"Could not generate placeholders: {e}")


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Streamlit Dashboard Launcher")
    parser.add_argument("--port", type=int, default=8501, help="Streamlit server port (default: 8501)")
    parser.add_argument("--seed-only", action="store_true", help="Only seed data & plots, don't launch UI")
    parser.add_argument("--browser", action="store_true", help="Auto-open browser on launch")
    parser.add_argument("--skip-seed", action="store_true", help="Skip data seeding (use existing data)")
    parser.add_argument("--local", action="store_true", help="Use SQLite (zero deps) instead of PostgreSQL")
    parser.add_argument("--pg-host", default=None, help="PostgreSQL host")
    parser.add_argument("--pg-port", default=None, help="PostgreSQL port")
    parser.add_argument("--pg-user", default=None, help="PostgreSQL user")
    parser.add_argument("--pg-pass", default=None, help="PostgreSQL password")
    parser.add_argument("--pg-db", default=None, help="PostgreSQL database")
    args = parser.parse_args()

    banner()

    # ---- Resolve PostgreSQL credentials ----
    load_dotenv()

    pg_host = args.pg_host or os.getenv("POSTGRES_HOST", "localhost")
    pg_port = int(args.pg_port or os.getenv("POSTGRES_PORT", "5432"))
    pg_user = args.pg_user or os.getenv("POSTGRES_USER", "sparkuser")
    pg_pass = args.pg_pass or os.getenv("POSTGRES_PASSWORD", "sparkpass")
    pg_db = args.pg_db or os.getenv("POSTGRES_DB", "microservice_analysis")

    os.environ["POSTGRES_HOST"] = pg_host
    os.environ["POSTGRES_PORT"] = str(pg_port)
    os.environ["POSTGRES_USER"] = pg_user
    os.environ["POSTGRES_PASSWORD"] = pg_pass
    os.environ["POSTGRES_DB"] = pg_db

    # Set OUTPUT_DIR so the dashboard finds generated plots locally
    os.environ["OUTPUT_DIR"] = os.path.join(PROJECT_ROOT, "output")

    # ---- SQLite (local) mode ----
    if args.local:
        print(f"{BOLD}{CYAN}Mode: SQLite (local) — zero external dependencies{RESET}\n")

        os.environ["DB_MODE"] = "sqlite"
        db_path = os.path.join(PROJECT_ROOT, "dashboard.db")
        os.environ["SQLITE_DB_PATH"] = db_path

        # Always re-seed in local mode
        if not args.skip_seed:
            print(f"{BOLD}Step 1/3:{RESET} Creating SQLite database & seeding data...")
            from modules.local_seeder import seed_all as seed_sqlite
            seed_sqlite(db_path)
        else:
            print(f"{BOLD}Step 1/3:{RESET} {YELLOW}⊘{RESET} Skipped seeding (--skip-seed).")
            if not os.path.exists(db_path):
                print(f"{RED}  ✗ SQLite database not found at {db_path}{RESET}")
                print(f"  Run without --skip-seed first to create it.")
                sys.exit(1)

        # Generate plots
        print(f"{BOLD}Step 2/3:{RESET} Generating placeholder plots...")
        generate_plots()

        if args.seed_only:
            print()
            print(f"{GREEN}{'=' * 60}{RESET}")
            print(f"{GREEN}  SQLite DB ready: {db_path}{RESET}")
            print(f"{GREEN}{'=' * 60}{RESET}")
            return

        # Launch
        print(f"{BOLD}Step 3/3:{RESET} Launching Streamlit dashboard...")
        print()
        print(f"  {GREEN}Database: SQLite ({db_path}){RESET}")
        print(f"  {GREEN}Dashboard: http://localhost:{args.port}{RESET}")
        print(f"  {CYAN}Press Ctrl+C to stop{RESET}")
        print()
    else:
        # ---- PostgreSQL mode ----
        print(f"{BOLD}Step 1/4:{RESET} Checking PostgreSQL at {pg_host}:{pg_port}...")
        if not check_postgres(pg_host, pg_port, pg_user, pg_pass, pg_db):
            print()
            print(f"{RED}{'=' * 60}{RESET}")
            print(f"{RED}  Cannot connect to PostgreSQL{RESET}")
            print(f"{RED}    Host: {pg_host}:{pg_port}{RESET}")
            print(f"{RED}    User: {pg_user}{RESET}")
            print(f"{RED}    DB:   {pg_db}{RESET}")
            print()
            print(f"{YELLOW}  Quick fixes:{RESET}")
            print(f"    {BOLD}docker compose up -d postgres{RESET}")
            print(f"    {BOLD}python run_streamlit.py --local{RESET}  (zero-deps SQLite mode)")
            print()
            print(f"{YELLOW}  Or pass custom credentials:{RESET}")
            print(f"    python run_streamlit.py --pg-host <host> --pg-user <user> --pg-pass <pass> --pg-db <db>")
            print(f"{RED}{'=' * 60}{RESET}")
            sys.exit(1)

        print(f"  {GREEN}PostgreSQL is reachable.{RESET}")

        # Seed
        print(f"{BOLD}Step 2/4:{RESET} Seeding database...")
        if args.skip_seed:
            print(f"  {YELLOW}Skipped (--skip-seed). Using existing data.{RESET}")
        else:
            import psycopg2
            conn = psycopg2.connect(
                host=pg_host, port=pg_port,
                user=pg_user, password=pg_pass, dbname=pg_db,
            )
            try:
                init_schema(conn)
                seed_all(conn)
            finally:
                conn.close()

        # Generate plots
        print(f"{BOLD}Step 3/4:{RESET} Generating placeholder plots...")
        generate_plots()

        if args.seed_only:
            print()
            print(f"{GREEN}{'=' * 60}{RESET}")
            print(f"{GREEN}  Data seeded & plots generated. Done.{RESET}")
            print(f"{GREEN}{'=' * 60}{RESET}")
            return

        print(f"{BOLD}Step 4/4:{RESET} Launching Streamlit dashboard...")
        print()
        print(f"  {GREEN}Dashboard: http://localhost:{args.port}{RESET}")
        print(f"  {CYAN}Press Ctrl+C to stop{RESET}")
        print()

    # Build streamlit command
    dashboard_path = os.path.join(PROJECT_ROOT, "modules", "dashboard.py")

    cmd = [
        sys.executable, "-m", "streamlit", "run",
        dashboard_path,
        "--server.port", str(args.port),
        "--server.address", "0.0.0.0",
        "--browser.serverAddress", "localhost",
    ]

    if not args.browser:
        cmd.append("--server.headless")
        cmd.append("true")

    # Launch streamlit (blocks until user presses Ctrl+C)
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print()
        print(f"{GREEN}Dashboard stopped.{RESET}")
    except subprocess.CalledProcessError as e:
        print(f"{RED}Dashboard exited with error code {e.returncode}.{RESET}")
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()
