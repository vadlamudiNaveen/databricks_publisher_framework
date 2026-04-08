"""Production-oriented batch file ingestion adapter.

This adapter is intended for Bronze RAW ingestion where files can be loaded
either with their native Spark reader format or as raw payload using
``binaryFile`` for heterogeneous/mixed content.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_FORMAT_MAP = {
    "json": "json",
    "jsonl": "json",
    "binaryfile": "binaryFile",
    "json_jsonl_mixed": "binaryFile",
    "csv": "csv",
    "parquet": "parquet",
    "avro": "avro",
    "orc": "orc",
    "text": "text",
}

_ALLOWED_CONTROL_KEYS = {
    "incremental_start_timestamp",
    "fail_on_empty",
    "file_ingest_mode",
    "image_field",
    "attachment_field",
    "attachment_dest_dir",
    "copy_attachments",
}

_ALLOWED_READER_OPTION_KEYS = {
    "header",
    "delimiter",
    "sep",
    "quote",
    "escape",
    "encoding",
    "multiLine",
    "inferSchema",
    "recursiveFileLookup",
    "pathGlobFilter",
    "modifiedBefore",
    "modifiedAfter",
    "mergeSchema",
}


class BatchFileIngestionConfigError(ValueError):
    """Raised when batch file ingestion configuration is invalid."""


def _require_non_empty(value: Any, field_name: str) -> str:
    if value is None:
        raise BatchFileIngestionConfigError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise BatchFileIngestionConfigError(f"{field_name} is required")
    return text


def _to_spark_option(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (str, int, float)):
        return str(value)
    raise BatchFileIngestionConfigError(
        f"Unsupported Spark option value type: {type(value).__name__}"
    )


def _normalize_source_format(source_format: str) -> str:
    normalized = source_format.strip().lower()
    if normalized not in _FORMAT_MAP:
        raise BatchFileIngestionConfigError(
            f"Unsupported source_format '{source_format}'. "
            f"Supported formats: {sorted(_FORMAT_MAP)}"
        )
    return normalized


def _validate_unknown_source_options(source_options: Mapping[str, Any] | None) -> None:
    if not source_options:
        return

    allowed = _ALLOWED_CONTROL_KEYS | _ALLOWED_READER_OPTION_KEYS
    unknown = sorted(set(source_options) - allowed)
    if unknown:
        raise BatchFileIngestionConfigError(
            f"Unsupported source_options keys: {unknown}. "
            f"Allowed keys are: {sorted(allowed)}"
        )


@dataclass(frozen=True)
class SourceConfig:
    source_path: str
    source_format: str
    load_type: str
    source_system: str | None = None
    source_entity: str | None = None

    @classmethod
    def from_dict(cls, source: Mapping[str, Any], global_config: Mapping[str, Any]) -> "SourceConfig":
        source_path = _require_non_empty(source.get("source_path"), "source.source_path")

        file_defaults = global_config.get("connections", {}).get("file_defaults", {})
        default_format = str(file_defaults.get("format", "binaryFile")).strip()

        raw_source_format = str(source.get("source_format", default_format)).strip()
        normalized_source_format = _normalize_source_format(raw_source_format)

        load_type = str(source.get("load_type", "incremental")).strip().lower()
        if load_type not in {"full", "incremental"}:
            raise BatchFileIngestionConfigError(
                f"Unsupported load_type '{load_type}'. Supported: ['full', 'incremental']"
            )

        return cls(
            source_path=source_path,
            source_format=normalized_source_format,
            load_type=load_type,
            source_system=str(source.get("source_system")).strip() if source.get("source_system") else None,
            source_entity=str(source.get("source_entity")).strip() if source.get("source_entity") else None,
        )

    @property
    def spark_format(self) -> str:
        return _FORMAT_MAP[self.source_format]

    @property
    def source_id(self) -> str:
        system = self.source_system or "unknown_system"
        entity = self.source_entity or "unknown_entity"
        return f"{system}.{entity}"


@dataclass(frozen=True)
class ControlOptions:
    incremental_start_timestamp: str | None
    fail_on_empty: bool
    image_field: str | None
    attachment_field: str | None
    attachment_dest_dir: str | None
    copy_attachments: bool

    @classmethod
    def from_source_options(cls, source_options: Mapping[str, Any] | None) -> "ControlOptions":
        opts = source_options or {}

        return cls(
            incremental_start_timestamp=(
                str(opts.get("incremental_start_timestamp")).strip()
                if opts.get("incremental_start_timestamp")
                else None
            ),
            fail_on_empty=bool(opts.get("fail_on_empty", True)),
            image_field=str(opts.get("image_field")).strip() if opts.get("image_field") else None,
            attachment_field=str(opts.get("attachment_field")).strip() if opts.get("attachment_field") else None,
            attachment_dest_dir=(
                str(opts.get("attachment_dest_dir")).strip()
                if opts.get("attachment_dest_dir")
                else None
            ),
            copy_attachments=bool(opts.get("copy_attachments", False)),
        )


@dataclass(frozen=True)
class ResolvedBatchConfig:
    source: SourceConfig
    reader_options: dict[str, str]
    control: ControlOptions


def resolve_batch_file_config(
    source: Mapping[str, Any],
    global_config: Mapping[str, Any],
    source_options: Mapping[str, Any] | None = None,
) -> ResolvedBatchConfig:
    _validate_unknown_source_options(source_options)

    source_cfg = SourceConfig.from_dict(source, global_config)
    control = ControlOptions.from_source_options(source_options)

    reader_options: dict[str, str] = {}
    for key, value in (source_options or {}).items():
        if key in _ALLOWED_READER_OPTION_KEYS and value is not None:
            reader_options[key] = _to_spark_option(value)

    return ResolvedBatchConfig(
        source=source_cfg,
        reader_options=reader_options,
        control=control,
    )


def _apply_incremental_filter(df: DataFrame, resolved: ResolvedBatchConfig) -> DataFrame:
    """Apply incremental file modification-time filter where supported."""
    if resolved.source.load_type != "incremental":
        return df

    incremental_start = resolved.control.incremental_start_timestamp
    if not incremental_start:
        return df

    if "modificationTime" not in df.columns:
        logger.warning(
            "Skipping incremental_start_timestamp filter for source_id=%s because "
            "column 'modificationTime' is not available for spark_format=%s",
            resolved.source.source_id,
            resolved.source.spark_format,
        )
        return df

    return df.filter(
        F.col("modificationTime") > F.to_timestamp(F.lit(incremental_start))
    )


def ingest_file_batch(
    spark: SparkSession,
    source: Mapping[str, Any],
    global_config: Mapping[str, Any],
    source_options: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Read files from source_path as a Spark batch DataFrame.

    Notes
    -----
    - This function intentionally does not trigger Spark actions.
    - Empty detection should be done by the orchestrator after ingestion.
    - Attachment copying is not performed here; keep ingestion side-effect free.
    """
    resolved = resolve_batch_file_config(
        source=source,
        global_config=global_config,
        source_options=source_options,
    )

    logger.info(
        "Starting batch file ingestion source_id=%s source_path=%s source_format=%s spark_format=%s load_type=%s",
        resolved.source.source_id,
        resolved.source.source_path,
        resolved.source.source_format,
        resolved.source.spark_format,
        resolved.source.load_type,
    )

    reader = spark.read.format(resolved.source.spark_format).options(**resolved.reader_options)
    df = reader.load(resolved.source.source_path)
    df = _apply_incremental_filter(df, resolved)

    logger.info(
        "Built batch DataFrame for source_id=%s source_path=%s",
        resolved.source.source_id,
        resolved.source.source_path,
    )

    return df


def collect_attachment_references(
    df: DataFrame,
    field_name: str,
    max_rows: int = 10000,
) -> list[str]:
    """Collect distinct non-empty attachment/image references from a column.

    This is intentionally separate from ingestion because it triggers a Spark action
    and performs driver-side collection.
    """
    if field_name not in df.columns:
        raise BatchFileIngestionConfigError(
            f"Field '{field_name}' does not exist in DataFrame columns"
        )

    values = (
        df.select(field_name)
        .where(F.col(field_name).isNotNull())
        .distinct()
        .limit(max_rows)
        .collect()
    )

    return [row[field_name] for row in values if row[field_name]]


def copy_attachment_references(
    df: DataFrame,
    field_name: str,
    dest_dir: str,
    handle_attachment_file,
    max_rows: int = 10000,
    fail_on_error: bool = False,
) -> list[tuple[str, str]]:
    """Copy referenced attachment/image files to a destination directory.

    This is a side-effecting helper and should be called explicitly by the
    orchestrator, not during generic ingestion.
    """
    copied: list[tuple[str, str]] = []
    references = collect_attachment_references(df, field_name=field_name, max_rows=max_rows)

    logger.info(
        "Copying attachment references field_name=%s count=%s dest_dir=%s",
        field_name,
        len(references),
        dest_dir,
    )

    for ref in references:
        try:
            new_path = handle_attachment_file(ref, dest_dir)
            copied.append((ref, new_path))
        except Exception as exc:
            logger.exception(
                "Failed to copy attachment reference field_name=%s reference=%s dest_dir=%s",
                field_name,
                ref,
                dest_dir,
            )
            if fail_on_error:
                raise

    return copied