# ============================================================
# Unit Tests — Database Adapter (db_adapter.py)
# ============================================================
# Tests: get_connection, run_query, get_table_names,
#        get_table_row_count, check_connection, DB_MODE routing.
#
# SQLite tests run without any external deps.
# PostgreSQL tests are mocked (no live PG server needed).
# ============================================================

import importlib
import os
import sqlite3
import tempfile
from unittest import mock

import pandas as pd
import pytest


# ============================================================
# Helpers
# ============================================================
def _reload_adapter(env_overrides=None):
    """Reload db_adapter with custom env vars, return the fresh module."""
    if env_overrides:
        for k, v in env_overrides.items():
            os.environ[k] = v

    # Force re-import
    if "modules.db_adapter" in importlib.sys.modules:
        del importlib.sys.modules["modules.db_adapter"]

    import modules.db_adapter as adapter
    return adapter


@pytest.fixture()
def sqlite_db():
    """Create a temporary SQLite database with a test table and some rows."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    path = tmp.name
    tmp.close()

    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value REAL)")
    conn.execute("INSERT INTO test_table (name, value) VALUES ('alpha', 1.0)")
    conn.execute("INSERT INTO test_table (name, value) VALUES ('beta', 2.5)")
    conn.execute("INSERT INTO test_table (name, value) VALUES ('gamma', 3.0)")
    conn.commit()
    conn.close()

    yield path

    # Cleanup
    for suffix in ("", "-wal", "-shm"):
        f = path + suffix
        if os.path.exists(f):
            os.remove(f)


@pytest.fixture()
def adapter_sqlite(sqlite_db, monkeypatch):
    """Reload db_adapter in SQLite mode pointing at our test DB."""
    monkeypatch.setenv("DB_MODE", "sqlite")
    monkeypatch.setenv("SQLITE_DB_PATH", sqlite_db)
    return _reload_adapter({"DB_MODE": "sqlite", "SQLITE_DB_PATH": sqlite_db})


# ============================================================
# DB_MODE Detection
# ============================================================
class TestDBModeDetection:
    """Tests that DB_MODE correctly reads from environment."""

    def test_defaults_to_postgres(self, monkeypatch):
        """Without DB_MODE set, should default to 'postgres'."""
        monkeypatch.delenv("DB_MODE", raising=False)
        adapter = _reload_adapter()
        assert adapter.DB_MODE == "postgres"

    def test_explicit_postgres(self, monkeypatch):
        """DB_MODE=postgres should yield 'postgres'."""
        monkeypatch.setenv("DB_MODE", "postgres")
        adapter = _reload_adapter({"DB_MODE": "postgres"})
        assert adapter.DB_MODE == "postgres"

    def test_explicit_sqlite(self, monkeypatch):
        """DB_MODE=sqlite should yield 'sqlite'."""
        monkeypatch.setenv("DB_MODE", "sqlite")
        adapter = _reload_adapter({"DB_MODE": "sqlite"})
        assert adapter.DB_MODE == "sqlite"

    def test_case_insensitive(self, monkeypatch):
        """DB_MODE should be lowercased (SQLITE → sqlite)."""
        monkeypatch.setenv("DB_MODE", "SQLITE")
        adapter = _reload_adapter({"DB_MODE": "SQLITE"})
        assert adapter.DB_MODE == "sqlite"

    def test_unknown_value_preserved(self, monkeypatch):
        """An unknown value like 'mysql' should be passed through as-is."""
        monkeypatch.setenv("DB_MODE", "mysql")
        adapter = _reload_adapter({"DB_MODE": "mysql"})
        assert adapter.DB_MODE == "mysql"


# ============================================================
# SQLite Connection
# ============================================================
class TestSQLiteConnection:
    """Tests for get_connection() in SQLite mode."""

    def test_returns_sqlite_connection(self, adapter_sqlite):
        """get_connection() should return a sqlite3.Connection."""
        conn = adapter_sqlite.get_connection()
        assert isinstance(conn, sqlite3.Connection)
        conn.close()

    def test_connection_is_usable(self, adapter_sqlite):
        """The returned connection should be able to execute queries."""
        conn = adapter_sqlite.get_connection()
        result = conn.execute("SELECT 1").fetchone()
        conn.close()
        assert result[0] == 1

    def test_row_factory_is_set(self, adapter_sqlite):
        """Connection should have row_factory = sqlite3.Row for named access."""
        conn = adapter_sqlite.get_connection()
        conn.execute("SELECT 1 AS val")
        # row_factory check: rows should be subscriptable by name
        row = conn.execute("SELECT 1 AS val").fetchone()
        conn.close()
        assert row["val"] == 1

    def test_multiple_connections_are_independent(self, adapter_sqlite):
        """Each call to get_connection() should return a new connection."""
        c1 = adapter_sqlite.get_connection()
        c2 = adapter_sqlite.get_connection()
        assert c1 is not c2
        c1.close()
        c2.close()


# ============================================================
# run_query — SQLite
# ============================================================
class TestRunQuerySQLite:
    """Tests for run_query() in SQLite mode."""

    def test_returns_dataframe(self, adapter_sqlite):
        """run_query should return a pandas DataFrame."""
        df = adapter_sqlite.run_query("SELECT * FROM test_table")
        assert isinstance(df, pd.DataFrame)

    def test_returns_correct_rows(self, adapter_sqlite):
        """Should return all 3 rows from the test table."""
        df = adapter_sqlite.run_query("SELECT * FROM test_table")
        assert len(df) == 3

    def test_returns_correct_columns(self, adapter_sqlite):
        """Should include id, name, value columns."""
        df = adapter_sqlite.run_query("SELECT * FROM test_table")
        assert set(df.columns) == {"id", "name", "value"}

    def test_where_clause(self, adapter_sqlite):
        """Filtered queries should work."""
        df = adapter_sqlite.run_query(
            "SELECT * FROM test_table WHERE value > 2.0"
        )
        assert len(df) == 2
        assert set(df["name"]) == {"beta", "gamma"}

    def test_aggregation(self, adapter_sqlite):
        """Aggregate queries (COUNT, SUM, AVG) should work."""
        df = adapter_sqlite.run_query(
            "SELECT COUNT(*) as cnt, SUM(value) as total, AVG(value) as avg_val "
            "FROM test_table"
        )
        assert df["cnt"].iloc[0] == 3
        assert df["total"].iloc[0] == pytest.approx(6.5)
        assert df["avg_val"].iloc[0] == pytest.approx(2.166, abs=0.01)

    def test_empty_result(self, adapter_sqlite):
        """Query matching no rows should return empty DataFrame (not crash)."""
        df = adapter_sqlite.run_query(
            "SELECT * FROM test_table WHERE value > 999"
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_invalid_query_returns_empty_df(self, adapter_sqlite):
        """A syntax error should return an empty DataFrame, not raise."""
        df = adapter_sqlite.run_query("INVALID SQL SYNTAX !!!")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_closes_connection_after_query(self, adapter_sqlite):
        """Each run_query should open and close its own connection."""
        # Run a query, then verify we can delete the DB file
        # (if a connection were left open, Windows would block deletion)
        adapter_sqlite.run_query("SELECT 1")
        # No assertion needed — if connection leaked, cleanup would fail

    def test_order_by(self, adapter_sqlite):
        """ORDER BY should work."""
        df = adapter_sqlite.run_query(
            "SELECT name FROM test_table ORDER BY name DESC"
        )
        names = df["name"].tolist()
        assert names == ["gamma", "beta", "alpha"]

    def test_limit(self, adapter_sqlite):
        """LIMIT should work."""
        df = adapter_sqlite.run_query(
            "SELECT * FROM test_table LIMIT 1"
        )
        assert len(df) == 1


# ============================================================
# get_table_names — SQLite
# ============================================================
class TestGetTableNamesSQLite:
    """Tests for get_table_names() in SQLite mode."""

    def test_returns_list(self, adapter_sqlite):
        """Should return a list of strings."""
        tables = adapter_sqlite.get_table_names()
        assert isinstance(tables, list)

    def test_includes_test_table(self, adapter_sqlite):
        """Should include 'test_table'."""
        tables = adapter_sqlite.get_table_names()
        assert "test_table" in tables

    def test_excludes_sqlite_internal(self, adapter_sqlite):
        """Should NOT include sqlite_* internal tables."""
        tables = adapter_sqlite.get_table_names()
        for t in tables:
            assert not t.startswith("sqlite_"), f"Leaked internal table: {t}"

    def test_empty_db_returns_empty_list(self, monkeypatch):
        """An empty SQLite DB should return []."""
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        path = tmp.name
        tmp.close()

        monkeypatch.setenv("DB_MODE", "sqlite")
        monkeypatch.setenv("SQLITE_DB_PATH", path)
        adapter = _reload_adapter({"DB_MODE": "sqlite", "SQLITE_DB_PATH": path})

        tables = adapter.get_table_names()
        assert tables == []

        for suffix in ("", "-wal", "-shm"):
            f = path + suffix
            if os.path.exists(f):
                os.remove(f)


# ============================================================
# get_table_row_count — SQLite
# ============================================================
class TestGetTableRowCountSQLite:
    """Tests for get_table_row_count() in SQLite mode."""

    def test_returns_int(self, adapter_sqlite):
        """Should return an integer."""
        cnt = adapter_sqlite.get_table_row_count("test_table")
        assert isinstance(cnt, int)

    def test_correct_count(self, adapter_sqlite):
        """Should return 3 for our test table."""
        cnt = adapter_sqlite.get_table_row_count("test_table")
        assert cnt == 3

    def test_nonexistent_table_returns_zero(self, adapter_sqlite):
        """Querying a nonexistent table should return 0 (via empty DataFrame)."""
        cnt = adapter_sqlite.get_table_row_count("nonexistent_table")
        assert cnt == 0


# ============================================================
# check_connection — SQLite
# ============================================================
class TestCheckConnectionSQLite:
    """Tests for check_connection() in SQLite mode."""

    def test_returns_true_for_valid_db(self, adapter_sqlite):
        """Should return True when the SQLite DB exists."""
        assert adapter_sqlite.check_connection() is True

    def test_returns_false_for_missing_db(self, monkeypatch):
        """Should return False when the SQLite file doesn't exist."""
        monkeypatch.setenv("DB_MODE", "sqlite")
        monkeypatch.setenv("SQLITE_DB_PATH", "/nonexistent/path/db.sqlite")
        adapter = _reload_adapter({
            "DB_MODE": "sqlite",
            "SQLITE_DB_PATH": "/nonexistent/path/db.sqlite",
        })
        assert adapter.check_connection() is False


# ============================================================
# PostgreSQL — Mocked Tests
# ============================================================
class TestPostgreSQLMocked:
    """Tests for PostgreSQL path (mocked — no live PG needed)."""

    def test_get_connection_uses_psycopg2(self, monkeypatch):
        """When DB_MODE=postgres, get_connection should call psycopg2.connect."""
        monkeypatch.setenv("DB_MODE", "postgres")
        adapter = _reload_adapter({"DB_MODE": "postgres"})

        with mock.patch("psycopg2.connect") as mock_connect:
            mock_connect.return_value = mock.MagicMock()
            conn = adapter.get_connection()
            mock_connect.assert_called_once()
            conn.close()

    def test_get_connection_passes_env_vars(self, monkeypatch):
        """psycopg2.connect should receive env var values."""
        monkeypatch.setenv("DB_MODE", "postgres")
        monkeypatch.setenv("POSTGRES_HOST", "testhost")
        monkeypatch.setenv("POSTGRES_PORT", "9999")
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTGRES_DB", "testdb")

        adapter = _reload_adapter({
            "DB_MODE": "postgres",
            "POSTGRES_HOST": "testhost",
            "POSTGRES_PORT": "9999",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_DB": "testdb",
        })

        with mock.patch("psycopg2.connect") as mock_connect:
            mock_connect.return_value = mock.MagicMock()
            adapter.get_connection()
            call_kwargs = mock_connect.call_args.kwargs
            assert call_kwargs["host"] == "testhost"
            assert call_kwargs["port"] == 9999
            assert call_kwargs["user"] == "testuser"
            assert call_kwargs["password"] == "testpass"
            assert call_kwargs["dbname"] == "testdb"

    def test_get_table_names_postgres(self, monkeypatch):
        """get_table_names in PG mode queries information_schema."""
        monkeypatch.setenv("DB_MODE", "postgres")
        adapter = _reload_adapter({"DB_MODE": "postgres"})

        with mock.patch.object(adapter, "run_query") as mock_query:
            mock_query.return_value = pd.DataFrame(
                {"table_name": ["processed_telemetry", "anomaly_scores"]}
            )
            tables = adapter.get_table_names()
            assert tables == ["processed_telemetry", "anomaly_scores"]
            # Verify it used information_schema query
            call_sql = mock_query.call_args[0][0]
            assert "information_schema.tables" in call_sql

    def test_get_table_row_count_postgres(self, monkeypatch):
        """get_table_row_count in PG mode uses unquoted table name."""
        monkeypatch.setenv("DB_MODE", "postgres")
        adapter = _reload_adapter({"DB_MODE": "postgres"})

        with mock.patch.object(adapter, "run_query") as mock_query:
            mock_query.return_value = pd.DataFrame({"cnt": [42]})
            cnt = adapter.get_table_row_count("some_table")
            assert cnt == 42
            call_sql = mock_query.call_args[0][0]
            assert "FROM some_table" in call_sql

    def test_check_connection_postgres_success(self, monkeypatch):
        """check_connection should return True when PG is reachable."""
        monkeypatch.setenv("DB_MODE", "postgres")
        adapter = _reload_adapter({"DB_MODE": "postgres"})

        with mock.patch.object(adapter, "get_connection") as mock_conn:
            mock_conn.return_value = mock.MagicMock()
            assert adapter.check_connection() is True
            mock_conn.return_value.close.assert_called_once()

    def test_check_connection_postgres_failure(self, monkeypatch):
        """check_connection should return False when PG is unreachable."""
        monkeypatch.setenv("DB_MODE", "postgres")
        adapter = _reload_adapter({"DB_MODE": "postgres"})

        with mock.patch.object(adapter, "get_connection") as mock_conn:
            mock_conn.side_effect = Exception("Connection refused")
            assert adapter.check_connection() is False


# ============================================================
# SQLite_PATH
# ============================================================
class TestSQLitePath:
    """Tests for SQLITE_PATH module constant."""

    def test_default_path(self, monkeypatch):
        """Default SQLITE_PATH should be <project_root>/dashboard.db."""
        monkeypatch.setenv("DB_MODE", "sqlite")
        monkeypatch.delenv("SQLITE_DB_PATH", raising=False)
        adapter = _reload_adapter()

        # Should end with dashboard.db
        assert adapter.SQLITE_PATH.endswith("dashboard.db")

    def test_custom_path(self, monkeypatch):
        """SQLITE_PATH should respect the env var."""
        custom = "/tmp/my_custom.db"
        monkeypatch.setenv("DB_MODE", "sqlite")
        monkeypatch.setenv("SQLITE_DB_PATH", custom)
        adapter = _reload_adapter({
            "DB_MODE": "sqlite",
            "SQLITE_DB_PATH": custom,
        })
        assert adapter.SQLITE_PATH == custom


# ============================================================
# Edge Cases
# ============================================================
class TestEdgeCases:
    """Edge case and resilience tests."""

    def test_run_query_never_raises(self, adapter_sqlite):
        """run_query should handle any error gracefully (return empty DF)."""
        # Malformed SQL
        df = adapter_sqlite.run_query("NOT VALID SQL AT ALL")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_table_row_count_handles_null_count(self, adapter_sqlite, monkeypatch):
        """If COUNT(*) returns None, should return 0."""
        with mock.patch.object(adapter_sqlite, "run_query") as mock_query:
            mock_query.return_value = pd.DataFrame()
            cnt = adapter_sqlite.get_table_row_count("test_table")
            assert cnt == 0

    def test_sqlite_connection_with_wal_mode(self, sqlite_db, monkeypatch):
        """Connecting to a WAL-mode DB should work."""
        # Enable WAL on the test DB
        conn = sqlite3.connect(sqlite_db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.close()

        monkeypatch.setenv("DB_MODE", "sqlite")
        monkeypatch.setenv("SQLITE_DB_PATH", sqlite_db)
        adapter = _reload_adapter({"DB_MODE": "sqlite", "SQLITE_DB_PATH": sqlite_db})

        assert adapter.check_connection() is True
        df = adapter.run_query("SELECT COUNT(*) as cnt FROM test_table")
        assert df["cnt"].iloc[0] == 3

    def test_sqlite_path_created_by_seeder(self):
        """The default SQLITE_PATH should be a plausible file path."""
        # Don't reload — just check the default path is reasonable
        import modules.db_adapter as adapter
        # Clean up if a reload happened during other tests
        assert adapter.SQLITE_PATH.endswith(".db")
        assert len(adapter.SQLITE_PATH) > 10  # not empty
