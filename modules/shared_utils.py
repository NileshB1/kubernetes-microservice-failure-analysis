
# Shared Utilities


import contextlib
import logging
import os
import sys

import yaml
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

load_dotenv()

# Centralised Logging Setup

_logging_initialized = False


def setup_logging(name: str, log_file: str = "/output/pipeline.log") -> logging.Logger:
    """
    Configure logging once and return a logger for the given module name.


    Format: 2024-01-15 10:23:45,123 [INFO] ingestion: Generating data...
    """
    global _logging_initialized

    if not _logging_initialized:
        fmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)-5s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Log messages contain arrows and check marks. A Windows console

        for stream in (sys.stdout, sys.stderr):
            # Not a real stream (captured in tests, redirected in Docker)?
            with contextlib.suppress(AttributeError, OSError, ValueError):
                stream.reconfigure(encoding="utf-8", errors="replace")

        # Console handler (stdout, unbuffered) - level from LOG_LEVEL env var
        log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
        console_level = getattr(logging, log_level_name, logging.INFO)
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(console_level)
        console.setFormatter(fmt)

        # File handler (persistent log)
        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            fh.setLevel(logging.DEBUG)  # file gets everything
            fh.setFormatter(fmt)
        except OSError:
            fh = None  # file logging is best-effort

        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        root.handlers.clear()
        root.addHandler(console)
        if fh:
            root.addHandler(fh)

        _logging_initialized = True

    return logging.getLogger(name)


logger = setup_logging("shared_utils")


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load YAML configuration file."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def prepare_local_spark_env() -> None:
    """
    Make PySpark launchable outside the Docker image
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # PySpark bundles its own Spark distribution; prefer it over a stale
    # SPARK_HOME pointing at a directory that no longer exists.
    import pyspark

    bundled_home = os.path.dirname(pyspark.__file__)
    if not os.path.isdir(os.environ.get("SPARK_HOME", "")):
        os.environ["SPARK_HOME"] = bundled_home

    if "JAVA_HOME" not in os.environ:
        for candidate in (
            "C:/Program Files/Java/jdk-17",    "C:/Program Files/Java/jdk-21",
            "C:/Program Files/Java/jdk-11", "/usr/lib/jvm/java-17-openjdk-amd64",
            "/usr/lib/jvm/java-11-openjdk-amd64",
        ):
            if os.path.isfile(os.path.join(candidate, "bin", "java.exe")) or os.path.isfile(
                os.path.join(candidate, "bin", "java")
            ):
                os.environ["JAVA_HOME"] = candidate
                break

    java_home = os.environ.get("JAVA_HOME", "")
    if java_home:
        java_bin = os.path.join(java_home, "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")


def create_spark_session(app_name: str = "MicroserviceAnalysis") -> SparkSession:
    """
    Create a Spark session connected to the Spark master.

    """
    prepare_local_spark_env()
    master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")


        # MinIO / S3A configuration
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key",os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_postgres(
    df: DataFrame,  table_name: str,
    mode: str = "overwrite",   host: str | None = None,
    port: str | None = None,  db: str | None = None,
    user: str | None = None,  password: str | None = None,
) -> None:
    """
    Write a Spark DataFrame to a PostgreSQL table via JDBC.

    """
    jdbc_url = (
        f"jdbc:postgresql://{host or os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{port or os.getenv('POSTGRES_PORT', '5432')}/"
        f"{db or os.getenv('POSTGRES_DB', 'microservice_analysis')}"
    )
    props = {
        "user": user or os.getenv("POSTGRES_USER", "sparkuser"),
        "password": password or os.getenv("POSTGRES_PASSWORD", "sparkpass"),
        "driver": "org.postgresql.Driver",
    }

    logger.info(f"Writing {table_name} to PostgreSQL ({mode}) .....")
    df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=props)
    logger.info(f" Written {df.count():,} rows to table: {table_name}")


def get_jdbc_url() -> str:
    """Get the JDBC URL for PostgreSQL."""
    return (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'microservice_analysis')}"
    )


def get_jdbc_props() -> dict:
    """Get JDBC connection properties for PostgreSQL."""
    return {
        "user": os.getenv("POSTGRES_USER", "sparkuser"),
        "password": os.getenv("POSTGRES_PASSWORD", "sparkpass"),  "driver": "org.postgresql.Driver",
    }
