
# Settings - Typed, Validated Configuration

# One place that reads the environment....

from __future__ import annotations

import os

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Credentials that ship in the repo for local convenience. 
INSECURE_DEFAULT_CREDENTIALS = frozenset(
    {"sparkpass", "minioadmin", "postgres",  "password", "changeme" }
)


class ConfigurationError(RuntimeError):
    """Raised when settings are missing or unsafe for the current environment"""


# Env parsing helpers

def _env_str(name: str, default: str):
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _env_int(name: str, default: int):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer, but got {raw!r}") from exc


def _env_float(name: str, default: float):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Secret(str):
    """A string that refuses to reveal itself in logs and tracebacks."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "'***redacted***'"

    def reveal(self) -> str:
        """Return the raw value. Call this only when handing it to a client."""
        return str.__str__(self)



# Setting groups

@dataclass(frozen=True)
class PostgresSettings:
    host: str = "postgres"
    port: int = 5432
    user: str = "sparkuser"
    password: Secret = field(default_factory=lambda: Secret("sparkpass"))

    database: str = "microservice_analysis"
    connect_timeout_sec: int = 10
    max_retries: int = 5
    retry_backoff_sec: float = 0.5

    @classmethod
    def from_env(cls) -> PostgresSettings:
        return cls(
            host=_env_str("POSTGRES_HOST", "postgres"),   port=_env_int("POSTGRES_PORT", 5432),
            user=_env_str("POSTGRES_USER", "sparkuser"),  password=Secret(_env_str("POSTGRES_PASSWORD", "sparkpass")),
            database=_env_str("POSTGRES_DB", "microservice_analysis"),
            connect_timeout_sec=_env_int("POSTGRES_CONNECT_TIMEOUT", 10),
            max_retries=_env_int("POSTGRES_MAX_RETRIES", 5),  retry_backoff_sec=_env_float("POSTGRES_RETRY_BACKOFF", 0.5)
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def dsn(self, redacted: bool = True) -> str:
        secret = "***" if redacted else self.password.reveal()
        return f"postgresql://{self.user}:{secret}@{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class MinIOSettings:
    endpoint: str = "http://minio:9000"
    access_key: str = "minioadmin"
    secret_key: Secret = field(default_factory=lambda: Secret("minioadmin"))
    bucket: str = "microservice-logs"
    secure: bool = False

    @classmethod
    def from_env(cls) -> MinIOSettings:
        endpoint = _env_str("MINIO_ENDPOINT", "http://minio:9000")
        return cls(
            endpoint=endpoint,   access_key=_env_str("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=Secret(_env_str("MINIO_SECRET_KEY", "minioadmin")),
            bucket=_env_str("MINIO_BUCKET", "microservice-logs"),
            secure=endpoint.startswith("https://"),
        )


@dataclass(frozen=True)
class SparkSettings:
    master_url: str = "spark://spark-master:7077"
    master_host: str = "spark-master"
    master_web_port: int = 8080
    driver_memory: str = "1g"
    executor_memory: str = "1g"
    shuffle_partitions: int = 8

    @classmethod
    def from_env(cls) -> SparkSettings:
        return cls(
            master_url=_env_str("SPARK_MASTER_URL", "spark://spark-master:7077"),
            master_host=_env_str("SPARK_MASTER_HOST", "spark-master"),  master_web_port=_env_int("SPARK_MASTER_WEBUI_PORT", 8080),
            driver_memory=_env_str("SPARK_DRIVER_MEMORY", "1g"),  executor_memory=_env_str("SPARK_EXECUTOR_MEMORY", "1g"),
            shuffle_partitions=_env_int("SPARK_SHUFFLE_PARTITIONS", 8),
        )

    @property
    def rest_api_base(self) -> str:
        return f"http://{self.master_host}:{self.master_web_port}/api/v1"

    @property
    def web_ui_url(self) -> str:
        return f"http://{self.master_host}:{self.master_web_port}"


@dataclass(frozen=True)
class AppSettings:
    env: str = "development"
    db_mode: str = "postgres"
    sqlite_path: Path = PROJECT_ROOT / "dashboard.db"
    output_dir: Path = Path("/output")
    log_level: str = "INFO"
    log_file: Path = Path("/output/pipeline.log")
    log_json: bool = False
    sample_data_rows: int = 100_000
    dashboard_cache_ttl_sec: int = 30

    @classmethod
    def from_env(cls) -> AppSettings:
        default_output = PROJECT_ROOT / "output" if not Path("/output").exists() else Path("/output")
        output_dir = Path(_env_str("OUTPUT_DIR", str(default_output)))
        return cls(
            env=_env_str("APP_ENV", "development").lower(),  db_mode=_env_str("DB_MODE", "postgres").lower(),
            sqlite_path=Path(_env_str("SQLITE_DB_PATH", str(PROJECT_ROOT / "dashboard.db"))),
            output_dir=output_dir,    log_level=_env_str("LOG_LEVEL", "INFO").upper(),
            log_file=Path(_env_str("LOG_FILE", str(output_dir / "pipeline.log"))),
            log_json=_env_bool("LOG_JSON", False),
            sample_data_rows=_env_int("SAMPLE_DATA_ROWS", 100_000),  dashboard_cache_ttl_sec=_env_int("DASHBOARD_CACHE_TTL", 30),
        )

    @property
    def is_production(self) -> bool:
        return self.env in {"production", "prod"}


# Root settings object
@dataclass(frozen=True)
class Settings:
    app: AppSettings
    postgres: PostgresSettings
    minio: MinIOSettings
    spark: SparkSettings

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            app=AppSettings.from_env(),
            postgres=PostgresSettings.from_env(),
            minio=MinIOSettings.from_env(),    spark=SparkSettings.from_env(),
        )

    # -- validation --------------------------------------------------
    def problems(self) -> list[str]:
        """
        Return every configuration problem found, worst first"""
        issues: list[str] = []

        if self.app.db_mode not in {"postgres", "sqlite"}:
            issues.append(f"DB_MODE must be 'postgres' or 'sqlite', got {self.app.db_mode!r}")

        if self.app.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            issues.append(f"LOG_LEVEL is not a valid level: {self.app.log_level!r}")


        if not 1 <= self.postgres.port <= 65535:
            issues.append(f"POSTGRES_PORT out of range: {self.postgres.port}")


        if not self.minio.endpoint.startswith(("http://", "https://")):
            issues.append(f"MINIO_ENDPOINT must include a scheme, got {self.minio.endpoint!r}")

        if self.app.sample_data_rows <= 0:
            issues.append(f"SAMPLE_DATA_ROWS must be positive, got {self.app.sample_data_rows}")

        if self.app.is_production:
            issues.extend(self._production_problems())

        return issues

    def _production_problems(self) -> list[str]:
        """Checks that only apply once APP_ENV says this is a real deployment."""
        issues: list[str] = []

        if self.postgres.password.reveal() in INSECURE_DEFAULT_CREDENTIALS:
            issues.append(
                "POSTGRES_PASSWORD is still a development default  "
                "Set a real secret before running in production "
            )
        if self.minio.secret_key.reveal() in INSECURE_DEFAULT_CREDENTIALS:
            issues.append(
                "MINIO_SECRET_KEY is still a development default. "
                "Set a real secret before running in production."
            )
        if not self.minio.secure:
            issues.append("MINIO_ENDPOINT uses plaintext http:// in production; use https://.")
        return issues

    def validate(self) -> None:
        """Raise ConfigurationError listing every problem, or return quietly """
        issues = self.problems()
        if issues:
            bullets = "\n".join(f"  - {issue}" for issue in issues)
            raise ConfigurationError(f"{len(issues)} configuration problem(s) found:\n{bullets}")

    # -- presentation ------------------------------------------------
    def describe(self) -> dict[str, Any]:
        """A redacted summary safe to log or render in the dashboard."""
        return {
            "environment": self.app.env,  "database_mode": self.app.db_mode,
            "postgres": f"{self.postgres.host}:{self.postgres.port}/{self.postgres.database}",
            "sqlite_path": str(self.app.sqlite_path),
            "minio_endpoint": self.minio.endpoint,
            "minio_bucket": self.minio.bucket,  "spark_master": self.spark.master_url,
            "output_dir": str(self.app.output_dir),  "log_level": self.app.log_level,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the process-wide settings, built once from the environment.

    """
    return Settings.from_env()


# YAML pipeline configuration

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


@lru_cache(maxsize=4)
def load_pipeline_config(config_path: str | None = None) -> dict[str, Any]:
    """
    Load config/config.yaml - the tunable analysis parameters.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if not path.exists():
        raise ConfigurationError(f"Pipeline config not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh)

    if not isinstance(loaded, dict):
        raise ConfigurationError(f"Pipeline config must be a mapping: {path}")

    return loaded
