
# Module 7: Spark SQL Analysis


from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from modules.settings import PROJECT_ROOT
from modules.shared_utils import create_spark_session, setup_logging, write_to_postgres

logger = setup_logging("spark_sql_analysis")

QUERY_FILE = PROJECT_ROOT / "sql" / "analysis_queries.sql"

# Each statement in the file is introduced by "-- @name <identifier>".
QUERY_HEADER = re.compile(r"^--\s*@name\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", re.MULTILINE)


# Queries that read the ground-truth view; skipped when no labels exist.

REQUIRES_GROUND_TRUTH = frozenset({"ground_truth_evaluation"})


def load_queries(query_file: Path | str = QUERY_FILE):
    """
    Parse the SQL file into {name: statement}.
    """
    path = Path(query_file)
    if not path.exists():
        raise FileNotFoundError(f"#### Spark SQL query file not found: {path}")

    text = path.read_text(encoding="utf-8")
    matches = list(QUERY_HEADER.finditer(text))
    if not matches:
        raise ValueError(f"No '-- @name' markers found in {path}")

    queries: dict[str, str] = {}
    for index, match in enumerate(matches):
        start = match.end()
        end=matches[index + 1].start() if index + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if body:
            queries[match.group(1)] = body

    logger.info("#### Loaded %d Spark SQL queries from %s", len(queries), path.name)
    return queries


def register_views(telemetry: DataFrame, ground_truth: DataFrame | None = None) -> None:
    """Expose the DataFrames to Spark SQL under stable view names."""
    telemetry.createOrReplaceTempView("telemetry")
    logger.info("Registered view: telemetry......")

    if ground_truth is not None:
        ground_truth.createOrReplaceTempView("ground_truth")
        logger.info("Registered view: ground_truth")


def run_queries(spark: SparkSession, queries: dict[str, str], has_ground_truth: bool):
    """
    Execute each query, returning {name: result}
    """
    results: dict[str, DataFrame] = {}

    for name, statement in queries.items():
        if name in REQUIRES_GROUND_TRUTH and not has_ground_truth:
            logger.warning("Skipping %s - no ground-truth labels available", name)
            continue
        try:
            results[name] = spark.sql(statement)
            logger.info("  [ok] %s", name)
        except Exception as exc:  # noqa: BLE001 - one bad query must not sink the rest
            logger.error("  [x] %s failed: %s", name, exc)

    return results


def run_spark_sql_analysis(
    telemetry: DataFrame, bucket: str = "microservice-logs",
    persist: bool = True
):
    """
    Run the Spark SQL analysis layer end to end
    """
    logger.info("=" * 40)
    logger.info("moodule 7: SPARK SQL ANALYSIS")
    logger.info("=" * 40)

    spark = create_spark_session("SparkSQLAnalysis")

    if telemetry is None:
        parquet_path = f"s3a://{bucket}/processed/telemetry_unified.parquet"
        logger.info("Loading telemetry from %s ...", parquet_path)
        telemetry = spark.read.parquet(parquet_path)

    ground_truth = _read_ground_truth(spark)
    has_ground_truth = ground_truth is not None and ground_truth.count() > 0

    register_views(telemetry, ground_truth)
    results = run_queries(spark, load_queries(), has_ground_truth)

    counts: dict[str, int] = {}
    for name, df in results.items():
        counts[name] = df.count()
        logger.info("%s: %s rows", name, f"{counts[name]:,}")
        if persist:
            write_to_postgres(df, name)

    logger.info("#### Spark SQL analysis complete: %s", counts)
    return counts


def _read_ground_truth(spark: SparkSession) -> DataFrame | None:
    """
    Read the injected-fault labels back from PostgreSQL
    """
    from modules.shared_utils import get_jdbc_props, get_jdbc_url

    try:
        return spark.read.jdbc(url=get_jdbc_url(), table="fault_injections", properties=get_jdbc_props())
    except Exception as exc:  # noqa: BLE001 - absence is expected, not fatal
        logger.warning("Ground-truth labels unavailable (%s); evaluation query will be skipped.", exc)
        return None

#main
if __name__ == "__main__":
    run_spark_sql_analysis()
