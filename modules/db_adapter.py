
# Database Adapter - PostgreSQL / SQLite Abstraction Layer


from __future__ import annotations

import logging
import os
import re

import sqlite3
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("db_adapter")

DB_MODE = os.getenv("DB_MODE", "postgres").lower()
SQLITE_PATH = os.getenv(
    "SQLITE_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dashboard.db"),
)

# Connection retry policy (PostgreSQL only - SQLite is a local file and a
# failure there is not transient).
MAX_RETRIES = int(os.getenv("POSTGRES_MAX_RETRIES", "5"))
RETRY_BACKOFF_SEC = float(os.getenv("POSTGRES_RETRY_BACKOFF", "0.5"))
CONNECT_TIMEOUT_SEC = int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10"))

# A table name can never be a bound parameter
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# The tables this application owns - used by callers that want to check
# whether a pipeline stage has produced its output yet.
KNOWN_TABLES: tuple[str, ...] = (
    "raw_telemetry",    "processed_telemetry",
    "cross_service_pairs",  "propagation_chains",
    "error_correlations", "anomaly_scores",
    "failure_patterns", "scalability_metrics",
)


class DatabaseError(RuntimeError):
    """Raised when the database is unreachable or a query fails."""



# Connections
def get_connection():
    """
    Return a DB-API 2.0 connection
    """
    if DB_MODE == "sqlite":
        return _connect_sqlite()
    return _connect_postgres()


def _connect_sqlite() -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(SQLITE_PATH)
    except sqlite3.Error as exc:
        raise DatabaseError(f"Could not open SQLite database at {SQLITE_PATH}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    return conn


def _connect_postgres():
    """Connect to PostgreSQL, retrying transient failures with backoff."""
    import psycopg2

    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),  port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "sparkuser"),   password=os.getenv("POSTGRES_PASSWORD", "sparkpass"),
                dbname=os.getenv("POSTGRES_DB", "microservice_analysis"),
                connect_timeout=CONNECT_TIMEOUT_SEC,
            )
        except psycopg2.OperationalError as exc:
            # OperationalError covers "not accepting connections yet"
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            delay = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
            logger.warning(
                "PostgreSQL connection attempt %d/%d failed (%s); retrying in %.1fs",
                attempt,       MAX_RETRIES,
                exc.__class__.__name__,  delay,
            )
            time.sleep(delay)

    raise DatabaseError(
        f"Not able to connect to PostgreSQL after {MAX_RETRIES} attempts: {last_error}"
    ) from last_error


@contextmanager
def connection() -> Iterator[Any]:
    """
    Context-managed connection that always closes, even on error

    """
    conn = get_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - closing must never mask the real error
            logger.debug("Exception while closing DB connection: ", exc_info=True)



# Queries

def run_query_strict(query: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
    """
    Run a SQL query and return a DataFrame, raising on failure.

    """
    with connection() as conn:
        try:
            return pd.read_sql(query, conn, params=params)
        except Exception as exc:
            raise DatabaseError(f"Query failed: {exc}") from exc


def run_query(query: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
    """
    Run a SQL query and return a DataFrame, or an empty one on failure.

    """
    try:
        return run_query_strict(query, params)
    except DatabaseError as exc:
        logger.warning("%s", exc)
        return pd.DataFrame()


def placeholder() -> str:
    """Return the parameter placeholder for the active backend."""
    return "?" if DB_MODE == "sqlite" else "%s"



# Introspection

def get_table_names() -> list[str]:
    """Return the tables present in the database, sorted."""
    if DB_MODE == "sqlite":
        df = run_query(
            "SELECT name FROM sqlite_master WHERE type='table' " "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        column = "name"
    else:
        df = run_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        column = "table_name"

    if df.empty or column not in df.columns:
        return []
    return df[column].tolist()


def get_table_row_count(table_name: str) -> int:
    """
    Return the row count for a table, or 0 if it is missing or unreadable.

    """
    if not _SAFE_IDENTIFIER.match(table_name):
        logger.warning("Refusing to query unsafe table identifier: %r", table_name)
        return 0

    # SQLite is quoted because seeded tables may collide with keywords;
    # PostgreSQL is left bare so the lookup stays case-insensitive.
    sql = (
        f'SELECT COUNT(*) AS cnt FROM "{table_name}"'
        if DB_MODE == "sqlite"
        else f"SELECT COUNT(*) AS cnt FROM {table_name}"
    )
    df = run_query(sql)
    if df.empty:
        return 0
    return int(df.iloc[0, 0])


def check_connection() -> bool:
    """Return True if the database is reachable and answers a trivial query."""
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
        return True
    except Exception:  # noqa: BLE001 - this is a health probe; any failure means down
        logger.debug("Database health check failed", exc_info=True)
        return False


def health() -> dict[str, Any]:
    """
    Return a structured health report for the dashboard status panel.

    """
    backend = "SQLite" if DB_MODE == "sqlite" else "PostgreSQL"
    location = (
        SQLITE_PATH
        if DB_MODE == "sqlite"
        else (
            f"{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}"
            f"/{os.getenv('POSTGRES_DB', 'microservice_analysis')}"
        )
    )

    if not check_connection():
        return {
            "backend": backend,    "location": location,
            "connected": False, "tables": {}, "total_rows": 0,
        }

    tables = {name: get_table_row_count(name) for name in get_table_names()}
    return { "backend": backend,   "location": location,
        "connected": True,  "tables": tables,
        "total_rows": sum(tables.values()) }
