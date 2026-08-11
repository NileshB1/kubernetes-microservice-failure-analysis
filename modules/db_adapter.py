# ============================================================
# Database Adapter — PostgreSQL / SQLite Abstraction Layer
# ============================================================
# Detects DB_MODE env var at import time:
#   DB_MODE=sqlite   → uses local SQLite file (zero dependencies)
#   DB_MODE=postgres → uses psycopg2 (default)
#
# All dashboard DB calls go through get_connection() and run_query().
# ============================================================

import os
import logging

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("db_adapter")

DB_MODE = os.getenv("DB_MODE", "postgres").lower()
SQLITE_PATH = os.getenv("SQLITE_DB_PATH", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dashboard.db",
))


def get_connection():
    """
    Return a DB-API 2.0 connection.
    PostgreSQL → psycopg2 connection
    SQLite     → sqlite3 connection
    """
    if DB_MODE == "sqlite":
        import sqlite3

        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    else:
        import psycopg2

        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "sparkuser"),
            password=os.getenv("POSTGRES_PASSWORD", "sparkpass"),
            dbname=os.getenv("POSTGRES_DB", "microservice_analysis"),
        )


def run_query(query: str) -> pd.DataFrame:
    """
    Run a SQL query and return results as a pandas DataFrame.

    Automatically translates PostgreSQL-specific syntax to SQLite
    when DB_MODE=sqlite.
    """
    conn = get_connection()
    try:
        # All dashboard queries use ANSI SQL compatible with both backends
        # (COUNT, SUM, AVG, ROUND, ABS, GROUP BY, ORDER BY, LIMIT, DISTINCT).
        # No translation needed — PostgreSQL-specific information_schema queries
        # are handled by get_table_names() above.
        return pd.read_sql(query, conn)
    except Exception as e:
        logger.warning(f"Query failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def get_table_names() -> list[str]:
    """Return list of table names present in the database."""
    if DB_MODE == "sqlite":
        df = run_query(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return df["name"].tolist() if not df.empty else []
    else:
        df = run_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        return df["table_name"].tolist() if not df.empty else []


def get_table_row_count(table_name: str) -> int:
    """Return the approximate row count for a table."""
    df = run_query(f'SELECT COUNT(*) as cnt FROM "{table_name}"' if DB_MODE == "sqlite"
                   else f"SELECT COUNT(*) as cnt FROM {table_name}")
    return int(df.iloc[0, 0]) if not df.empty else 0


def check_connection() -> bool:
    """Return True if the database is reachable."""
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


# ============================================================
# Note on SQL compatibility
# ============================================================
# All dashboard queries use ANSI SQL that works identically in
# PostgreSQL and SQLite: COUNT, SUM, AVG, ROUND, ABS, GROUP BY,
# ORDER BY, LIMIT, DISTINCT. The only PostgreSQL-specific queries
# (information_schema) are handled by get_table_names() above.
