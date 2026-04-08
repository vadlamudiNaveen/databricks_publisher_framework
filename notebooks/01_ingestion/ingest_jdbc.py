"""Production-oriented generic JDBC ingestion adapter."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_REQUIRED_JDBC_PROFILE_KEYS = {"url", "driver", "fetchsize"}
_PARTITION_OPTION_KEYS = {"numPartitions", "partitionColumn", "lowerBound", "upperBound"}
_ALLOWED_EXTRA_OPTION_KEYS = {
    "query",
    "prepareQuery",
    "queryTimeout",
    "sessionInitStatement",
    "pushDownPredicate",
    "pushDownAggregate",
    "pushDownLimit",
    "pushDownOffset",
    "pushDownTableSample",
    "truncate",
    "customSchema",
    "isolationLevel",
    "batchsize",
    "fetchsize",
    "numPartitions",
    "partitionColumn",
    "lowerBound",
    "upperBound",
    "queryTimeout",
    "sessionInitStatement",
    "query",
    "prepareQuery",
}


class JdbcIngestionConfigError(ValueError):
    """Raised when JDBC ingestion configuration is invalid."""


class JdbcIngestionExecutionError(RuntimeError):
    """Raised when JDBC ingestion execution fails."""


def _require_non_empty(value: Any, field_name: str) -> str:
    if value is None:
        raise JdbcIngestionConfigError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise JdbcIngestionConfigError(f"{field_name} is required")
    return text


def _to_spark_option(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (str, int, float)):
        return str(value)
    raise JdbcIngestionConfigError(
        f"Unsupported Spark option value type for JDBC option: {type(value).__name__}"
    )


def _validate_unknown_extra_options(extra_options: Mapping[str, Any] | None) -> None:
    if not extra_options:
        return

    unknown = sorted(set(extra_options) - _ALLOWED_EXTRA_OPTION_KEYS)
    if unknown:
        raise JdbcIngestionConfigError(
            f"Unsupported JDBC extra_options keys: {unknown}. "
            f"Allowed keys are: {sorted(_ALLOWED_EXTRA_OPTION_KEYS)}"
        )


def _resolve_env_value(env_var_name: str | None, field_name: str) -> str | None:
    if not env_var_name:
        return None
    env_name = _require_non_empty(env_var_name, field_name)
    value = os.getenv(env_name, "").strip()
    if not value:
        raise JdbcIngestionConfigError(
            f"Environment variable '{env_name}' is required for {field_name}"
        )
    return value


@dataclass(frozen=True)
class JdbcProfile:
    url: str
    driver: str
    fetchsize: int
    user_env: str | None = None
    password_env: str | None = None

    @classmethod
    def from_dict(cls, jdbc_profile: Mapping[str, Any]) -> "JdbcProfile":
        missing = [key for key in sorted(_REQUIRED_JDBC_PROFILE_KEYS) if not jdbc_profile.get(key)]
        if missing:
            raise JdbcIngestionConfigError(f"Missing JDBC profile fields: {missing}")

        fetchsize = int(jdbc_profile["fetchsize"])
        if fetchsize <= 0:
            raise JdbcIngestionConfigError("jdbc_profile.fetchsize must be > 0")

        return cls(
            url=_require_non_empty(jdbc_profile.get("url"), "jdbc_profile.url"),
            driver=_require_non_empty(jdbc_profile.get("driver"), "jdbc_profile.driver"),
            fetchsize=fetchsize,
            user_env=str(jdbc_profile.get("user_env")).strip() if jdbc_profile.get("user_env") else None,
            password_env=str(jdbc_profile.get("password_env")).strip() if jdbc_profile.get("password_env") else None,
        )


@dataclass(frozen=True)
class SourceConfig:
    jdbc_table: str
    source_system: str | None = None
    source_entity: str | None = None

    @classmethod
    def from_dict(cls, source: Mapping[str, Any]) -> "SourceConfig":
        return cls(
            jdbc_table=_require_non_empty(source.get("jdbc_table"), "source.jdbc_table"),
            source_system=str(source.get("source_system")).strip() if source.get("source_system") else None,
            source_entity=str(source.get("source_entity")).strip() if source.get("source_entity") else None,
        )

    @property
    def source_id(self) -> str:
        system = self.source_system or "unknown_system"
        entity = self.source_entity or "unknown_entity"
        return f"{system}.{entity}"


@dataclass(frozen=True)
class ParallelReadOptions:
    enabled: bool
    options: dict[str, str]

    @classmethod
    def from_extra_options(cls, extra_options: Mapping[str, Any] | None) -> "ParallelReadOptions":
        opts = dict(extra_options or {})
        present = _PARTITION_OPTION_KEYS.intersection(opts)

        if not present:
            return cls(enabled=False, options={})

        if present != _PARTITION_OPTION_KEYS:
            missing = sorted(_PARTITION_OPTION_KEYS - present)
            raise JdbcIngestionConfigError(
                "Parallel JDBC read requires all partition options together. "
                f"Missing keys: {missing}"
            )

        num_partitions = int(opts["numPartitions"])
        if num_partitions <= 0:
            raise JdbcIngestionConfigError("extra_options.numPartitions must be > 0")

        partition_column = _require_non_empty(
            opts["partitionColumn"], "extra_options.partitionColumn"
        )

        lower_bound = opts["lowerBound"]
        upper_bound = opts["upperBound"]

        return cls(
            enabled=True,
            options={
                "numPartitions": _to_spark_option(num_partitions),
                "partitionColumn": partition_column,
                "lowerBound": _to_spark_option(lower_bound),
                "upperBound": _to_spark_option(upper_bound),
            },
        )


@dataclass(frozen=True)
class ResolvedJdbcConfig:
    profile: JdbcProfile
    source: SourceConfig
    options: dict[str, str]


def build_jdbc_options(
    jdbc_profile: Mapping[str, Any],
    source: Mapping[str, Any],
    extra_options: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    _validate_unknown_extra_options(extra_options)

    profile = JdbcProfile.from_dict(jdbc_profile)
    source_cfg = SourceConfig.from_dict(source)
    parallel = ParallelReadOptions.from_extra_options(extra_options)

    options: dict[str, str] = {
        "url": profile.url,
        "dbtable": source_cfg.jdbc_table,
        "driver": profile.driver,
        "fetchsize": _to_spark_option(profile.fetchsize),
    }

    user = _resolve_env_value(profile.user_env, "jdbc_profile.user_env")
    password = _resolve_env_value(profile.password_env, "jdbc_profile.password_env")

    if user is not None:
        options["user"] = user
    if password is not None:
        options["password"] = password

    if parallel.enabled:
        options.update(parallel.options)

    for key, value in (extra_options or {}).items():
        if key in _PARTITION_OPTION_KEYS:
            continue
        if value is None:
            continue
        options[key] = _to_spark_option(value)

    return options


def resolve_jdbc_config(
    jdbc_profile: Mapping[str, Any],
    source: Mapping[str, Any],
    extra_options: Mapping[str, Any] | None = None,
) -> ResolvedJdbcConfig:
    profile = JdbcProfile.from_dict(jdbc_profile)
    source_cfg = SourceConfig.from_dict(source)
    options = build_jdbc_options(jdbc_profile=jdbc_profile, source=source, extra_options=extra_options)
    return ResolvedJdbcConfig(profile=profile, source=source_cfg, options=options)


def ingest_jdbc_batch(
    spark: SparkSession,
    jdbc_profile: Mapping[str, Any],
    source: Mapping[str, Any],
    extra_options: Mapping[str, Any] | None = None,
) -> DataFrame:
    resolved = resolve_jdbc_config(
        jdbc_profile=jdbc_profile,
        source=source,
        extra_options=extra_options,
    )

    logger.info(
        "Starting JDBC ingestion source_id=%s dbtable=%s url=%s driver=%s",
        resolved.source.source_id,
        resolved.source.jdbc_table,
        resolved.profile.url,
        resolved.profile.driver,
    )

    try:
        reader = spark.read.format("jdbc").options(**resolved.options)
        df = reader.load()

        logger.info(
            "Built JDBC DataFrame source_id=%s dbtable=%s",
            resolved.source.source_id,
            resolved.source.jdbc_table,
        )
        return df

    except Exception as exc:
        raise JdbcIngestionExecutionError(
            f"JDBC ingestion failed for source_id={resolved.source.source_id} "
            f"dbtable={resolved.source.jdbc_table} url={resolved.profile.url}: {exc}"
        ) from exc