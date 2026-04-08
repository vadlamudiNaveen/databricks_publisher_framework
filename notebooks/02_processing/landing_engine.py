"""Production-oriented landing engine: preserve source records and add technical metadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_ALLOWED_WRITE_MODES = {"append", "overwrite", "error", "ignore"}
_ALLOWED_TABLE_TYPES = {"managed", "external"}


class LandingConfigError(ValueError):
    """Raised when landing configuration is invalid."""


class LandingExecutionError(RuntimeError):
    """Raised when landing execution fails."""


def _require_non_empty(value: Any, field_name: str) -> str:
    if value is None:
        raise LandingConfigError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise LandingConfigError(f"{field_name} is required")
    return text


def _normalize_bool(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "false"}:
            return text
    raise LandingConfigError(f"Expected boolean-compatible value, got: {value!r}")


@dataclass(frozen=True)
class LandingWriteConfig:
    table_name: str
    table_type: str = "managed"
    external_path: str | None = None
    archive_table: str | None = None
    retention_hours: int | None = None
    merge_schema: bool = True
    mode: str = "append"

    @classmethod
    def build(
        cls,
        table_name: str,
        table_type: str = "managed",
        external_path: str | None = None,
        archive_table: str | None = None,
        retention_days: int | None = None,
        retention_hours: int | None = None,
        merge_schema: bool = True,
        mode: str = "append",
    ) -> "LandingWriteConfig":
        resolved_table_name = _require_non_empty(table_name, "table_name")
        resolved_table_type = _require_non_empty(table_type, "table_type").lower()
        if resolved_table_type not in _ALLOWED_TABLE_TYPES:
            raise LandingConfigError(
                f"Unsupported table_type '{resolved_table_type}'. Supported values: {sorted(_ALLOWED_TABLE_TYPES)}"
            )
        resolved_external_path = (
            _require_non_empty(external_path, "external_path") if external_path else None
        )
        if resolved_table_type == "external" and not resolved_external_path:
            raise LandingConfigError("external_path is required when table_type=external")

        resolved_archive_table = (
            _require_non_empty(archive_table, "archive_table") if archive_table else None
        )
        resolved_mode = _require_non_empty(mode, "mode").lower()

        if resolved_mode not in _ALLOWED_WRITE_MODES:
            raise LandingConfigError(
                f"Unsupported write mode '{resolved_mode}'. Supported modes: {sorted(_ALLOWED_WRITE_MODES)}"
            )

        if retention_days is not None and retention_hours is not None:
            raise LandingConfigError(
                "Provide only one of retention_days or retention_hours, not both"
            )

        resolved_retention_hours: int | None = None
        if retention_days is not None:
            if int(retention_days) <= 0:
                raise LandingConfigError("retention_days must be > 0")
            resolved_retention_hours = int(retention_days) * 24

        if retention_hours is not None:
            if int(retention_hours) <= 0:
                raise LandingConfigError("retention_hours must be > 0")
            resolved_retention_hours = int(retention_hours)

        return cls(
            table_name=resolved_table_name,
            table_type=resolved_table_type,
            external_path=resolved_external_path,
            archive_table=resolved_archive_table,
            retention_hours=resolved_retention_hours,
            merge_schema=bool(merge_schema),
            mode=resolved_mode,
        )


def add_landing_metadata(
    df: DataFrame,
    source_system: str,
    source_entity: str,
    load_id: str,
) -> DataFrame:
    resolved_source_system = _require_non_empty(source_system, "source_system")
    resolved_source_entity = _require_non_empty(source_entity, "source_entity")
    resolved_load_id = _require_non_empty(load_id, "load_id")

    return (
        df.withColumn("source_system", F.lit(resolved_source_system))
        .withColumn("source_entity", F.lit(resolved_source_entity))
        .withColumn("load_id", F.lit(resolved_load_id))
        .withColumn("ingest_ts", F.current_timestamp())
    )


def _write_delta_table(
    df: DataFrame,
    table_name: str,
    mode: str,
    merge_schema: bool,
    table_type: str,
    external_path: str | None,
) -> None:
    writer = (
        df.write.mode(mode)
        .format("delta")
        .option("mergeSchema", _normalize_bool(merge_schema))
    )
    if table_type == "external" and external_path:
        writer = writer.option("path", external_path)
    writer.saveAsTable(table_name)


def _vacuum_table(
    spark: SparkSession,
    table_name: str,
    retention_hours: int,
) -> None:
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")


def write_landing(
    df: DataFrame,
    table_name: str,
    table_type: str = "managed",
    external_path: str | None = None,
    archive_table: str | None = None,
    retention_days: int | None = None,
    retention_hours: int | None = None,
    merge_schema: bool = True,
    mode: str = "append",
) -> None:
    config = LandingWriteConfig.build(
        table_name=table_name,
        table_type=table_type,
        external_path=external_path,
        archive_table=archive_table,
        retention_days=retention_days,
        retention_hours=retention_hours,
        merge_schema=merge_schema,
        mode=mode,
    )

    logger.info(
        "Writing landing table=%s archive_table=%s mode=%s merge_schema=%s",
        config.table_name,
        config.archive_table,
        config.mode,
        config.merge_schema,
    )

    try:
        _write_delta_table(
            df=df,
            table_name=config.table_name,
            mode=config.mode,
            merge_schema=config.merge_schema,
            table_type=config.table_type,
            external_path=config.external_path,
        )

        if config.archive_table:
            _write_delta_table(
                df=df,
                table_name=config.archive_table,
                mode="append",
                merge_schema=config.merge_schema,
                table_type="managed",
                external_path=None,
            )

            if config.retention_hours is not None:
                spark = SparkSession.getActiveSession()
                if spark is None:
                    raise LandingExecutionError(
                        "No active SparkSession found for archive VACUUM operation"
                    )
                _vacuum_table(
                    spark=spark,
                    table_name=config.archive_table,
                    retention_hours=config.retention_hours,
                )

        logger.info(
            "Completed landing write table=%s archive_table=%s",
            config.table_name,
            config.archive_table,
        )

    except Exception as exc:
        raise LandingExecutionError(
            f"Landing write failed for table={config.table_name} "
            f"archive_table={config.archive_table}: {exc}"
        ) from exc