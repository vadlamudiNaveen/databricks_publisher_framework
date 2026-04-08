"""Production-oriented file ingestion adapter for Databricks Auto Loader.

Design goals:
- Fail early on bad configuration
- Keep read/write/control options separate
- Be easy to test
- Be hard to misuse
- Add operational metadata for observability
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


# -----------------------------
# Exceptions
# -----------------------------


class AutoLoaderConfigError(ValueError):
    """Raised when the Auto Loader configuration is invalid."""


class AutoLoaderExecutionError(RuntimeError):
    """Raised when Auto Loader execution fails."""


# -----------------------------
# Constants
# -----------------------------


SUPPORTED_FILE_FORMATS = {"json", "csv", "parquet", "avro", "orc", "text", "binaryFile"}
SUPPORTED_SCHEMA_EVOLUTION_MODES = {"addNewColumns", "failOnNewColumns", "none", "rescue"}

# Allowed source_options split by purpose.
ALLOWED_READ_OPTION_KEYS = {
    "cloudFiles.useNotifications",
    "cloudFiles.includeExistingFiles",
    "cloudFiles.maxFilesPerTrigger",
    "cloudFiles.maxBytesPerTrigger",
    "cloudFiles.partitionColumns",
    "cloudFiles.schemaHints",
    "cloudFiles.validateOptions",
    "cloudFiles.allowOverwrites",
    "cloudFiles.useIncrementalListing",
    "cloudFiles.backfillInterval",
    "recursiveFileLookup",
    "header",
    "delimiter",
    "multiLine",
    "encoding",
    "escape",
    "quote",
    "sep",
    "inferSchema",
}

ALLOWED_WRITE_OPTION_KEYS = {
    "mergeSchema",
    "queryName",
    "trigger_once",
    "trigger_available_now",
    "outputMode",
}

ALLOWED_CONTROL_OPTION_KEYS = {
    "schema_ddl",
    "add_metadata_columns",
    "metadata_source_name",
}

DEFAULT_OUTPUT_MODE = "append"


# -----------------------------
# Small helpers
# -----------------------------


def _require_non_empty(value: Any, field_name: str) -> str:
    """Return a stripped non-empty string or raise."""
    if value is None:
        raise AutoLoaderConfigError(f"{field_name} is required")

    text = str(value).strip()
    if not text:
        raise AutoLoaderConfigError(f"{field_name} is required")

    return text


def _to_spark_option(value: Any) -> str:
    """Normalize Python values to Spark option strings."""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float, str)):
        return str(value)
    raise AutoLoaderConfigError(f"Unsupported Spark option value type: {type(value).__name__}")


def _sanitize_path_part(value: str) -> str:
    """Sanitize a path segment to reduce accidental path issues."""
    sanitized = value.strip()
    for old, new in (("/", "_"), ("\\", "_"), (" ", "_"), (":", "_")):
        sanitized = sanitized.replace(old, new)

    if not sanitized:
        raise AutoLoaderConfigError("Path segment cannot be empty after sanitization")

    return sanitized


def _validate_unknown_option_keys(source_options: Mapping[str, Any] | None) -> None:
    """Reject unsupported option keys early."""
    if not source_options:
        return

    allowed = (
        ALLOWED_READ_OPTION_KEYS
        | ALLOWED_WRITE_OPTION_KEYS
        | ALLOWED_CONTROL_OPTION_KEYS
    )

    unknown = sorted(set(source_options) - allowed)
    if unknown:
        raise AutoLoaderConfigError(
            f"Unsupported source_options keys: {unknown}. "
            f"Allowed keys are: {sorted(allowed)}"
        )


def _validate_file_format(file_format: str) -> None:
    if file_format not in SUPPORTED_FILE_FORMATS:
        raise AutoLoaderConfigError(
            f"Unsupported file format '{file_format}'. "
            f"Supported formats: {sorted(SUPPORTED_FILE_FORMATS)}"
        )


def _validate_schema_evolution_mode(mode: str) -> None:
    if mode not in SUPPORTED_SCHEMA_EVOLUTION_MODES:
        raise AutoLoaderConfigError(
            f"Unsupported schema evolution mode '{mode}'. "
            f"Supported modes: {sorted(SUPPORTED_SCHEMA_EVOLUTION_MODES)}"
        )


# -----------------------------
# Dataclasses
# -----------------------------


@dataclass(frozen=True)
class DatabricksConfig:
    checkpoint_root: str
    schema_tracking_root: str

    @classmethod
    def from_global_config(cls, global_config: Mapping[str, Any]) -> "DatabricksConfig":
        dbx_cfg = global_config.get("databricks", {})
        return cls(
            checkpoint_root=_require_non_empty(
                dbx_cfg.get("checkpoint_root"),
                "global_config.databricks.checkpoint_root",
            ),
            schema_tracking_root=_require_non_empty(
                dbx_cfg.get("schema_tracking_root"),
                "global_config.databricks.schema_tracking_root",
            ),
        )


@dataclass(frozen=True)
class FileDefaults:
    format: str
    infer_column_types: bool
    schema_evolution_mode: str

    @classmethod
    def from_global_config(cls, global_config: Mapping[str, Any]) -> "FileDefaults":
        file_defaults = global_config.get("connections", {}).get("file_defaults")
        if not file_defaults:
            raise AutoLoaderConfigError(
                "global_config.connections.file_defaults must be configured"
            )

        format_value = _require_non_empty(
            file_defaults.get("format"),
            "global_config.connections.file_defaults.format",
        )
        _validate_file_format(format_value)

        schema_evolution_mode = _require_non_empty(
            file_defaults.get("schema_evolution_mode"),
            "global_config.connections.file_defaults.schema_evolution_mode",
        )
        _validate_schema_evolution_mode(schema_evolution_mode)

        if "infer_column_types" not in file_defaults:
            raise AutoLoaderConfigError(
                "global_config.connections.file_defaults.infer_column_types is required"
            )

        return cls(
            format=format_value,
            infer_column_types=bool(file_defaults["infer_column_types"]),
            schema_evolution_mode=schema_evolution_mode,
        )


@dataclass(frozen=True)
class SourceConfig:
    tenant: str
    product_name: str
    source_system: str
    source_entity: str
    source_path: str
    landing_table: str | None
    source_format: str | None

    @classmethod
    def from_dict(cls, source: Mapping[str, Any]) -> "SourceConfig":
        source_format = source.get("source_format")
        source_format_text = str(source_format).strip() if source_format else None
        if source_format_text:
            _validate_file_format(source_format_text)

        landing_table = source.get("landing_table")
        landing_table_text = str(landing_table).strip() if landing_table else None

        return cls(
            tenant=_require_non_empty(source.get("tenant"), "source.tenant"),
            product_name=_require_non_empty(source.get("product_name"), "source.product_name"),
            source_system=_require_non_empty(source.get("source_system"), "source.source_system"),
            source_entity=_require_non_empty(source.get("source_entity"), "source.source_entity"),
            source_path=_require_non_empty(source.get("source_path"), "source.source_path"),
            landing_table=landing_table_text,
            source_format=source_format_text,
        )

    @property
    def source_id(self) -> str:
        return f"{self.source_system}.{self.source_entity}"

    @property
    def tracking_suffix(self) -> str:
        return "/".join(
            [
                _sanitize_path_part(self.tenant),
                _sanitize_path_part(self.product_name),
                _sanitize_path_part(self.source_system),
                _sanitize_path_part(self.source_entity),
            ]
        )


@dataclass(frozen=True)
class ControlOptions:
    schema_ddl: str | None
    add_metadata_columns: bool
    metadata_source_name: str | None

    @classmethod
    def from_source_options(cls, source_options: Mapping[str, Any] | None) -> "ControlOptions":
        source_options = source_options or {}
        schema_ddl = source_options.get("schema_ddl")
        metadata_source_name = source_options.get("metadata_source_name")

        return cls(
            schema_ddl=str(schema_ddl).strip() if schema_ddl else None,
            add_metadata_columns=bool(source_options.get("add_metadata_columns", True)),
            metadata_source_name=str(metadata_source_name).strip() if metadata_source_name else None,
        )


@dataclass(frozen=True)
class ResolvedAutoLoaderConfig:
    source: SourceConfig
    dbx: DatabricksConfig
    file_defaults: FileDefaults
    schema_location: str
    checkpoint_location: str
    read_options: dict[str, str]
    write_options: dict[str, str]
    control_options: ControlOptions
    query_name: str | None
    output_mode: str
    trigger_available_now: bool
    trigger_once: bool


# -----------------------------
# Config resolution
# -----------------------------


def _build_tracking_paths(dbx_cfg: DatabricksConfig, source_cfg: SourceConfig) -> tuple[str, str]:
    """Build schema and checkpoint paths for this source."""
    schema_location = f"{dbx_cfg.schema_tracking_root.rstrip('/')}/{source_cfg.tracking_suffix}"
    checkpoint_location = f"{dbx_cfg.checkpoint_root.rstrip('/')}/{source_cfg.tracking_suffix}"
    return schema_location, checkpoint_location


def resolve_autoloader_config(
    source: Mapping[str, Any],
    global_config: Mapping[str, Any],
    source_options: Mapping[str, Any] | None = None,
) -> ResolvedAutoLoaderConfig:
    """Resolve and validate all config for Auto Loader."""
    _validate_unknown_option_keys(source_options)

    source_cfg = SourceConfig.from_dict(source)
    dbx_cfg = DatabricksConfig.from_global_config(global_config)
    file_defaults = FileDefaults.from_global_config(global_config)
    control_options = ControlOptions.from_source_options(source_options)

    schema_location, checkpoint_location = _build_tracking_paths(dbx_cfg, source_cfg)

    file_format = source_cfg.source_format or file_defaults.format
    _validate_file_format(file_format)

    read_options: dict[str, str] = {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.inferColumnTypes": _to_spark_option(file_defaults.infer_column_types),
        "cloudFiles.schemaEvolutionMode": file_defaults.schema_evolution_mode,
    }

    write_options: dict[str, str] = {
        "checkpointLocation": checkpoint_location,
    }

    query_name: str | None = None
    output_mode = DEFAULT_OUTPUT_MODE
    trigger_available_now = True
    trigger_once = False

    for key, value in (source_options or {}).items():
        if value is None:
            continue

        if key in ALLOWED_READ_OPTION_KEYS:
            read_options[key] = _to_spark_option(value)
        elif key == "mergeSchema":
            write_options[key] = _to_spark_option(value)
        elif key == "queryName":
            query_name = str(value).strip()
        elif key == "outputMode":
            output_mode = str(value).strip()
        elif key == "trigger_available_now":
            trigger_available_now = bool(value)
        elif key == "trigger_once":
            trigger_once = bool(value)

    if trigger_available_now and trigger_once:
        raise AutoLoaderConfigError(
            "Only one of trigger_available_now or trigger_once may be true"
        )

    if output_mode not in {"append", "complete", "update"}:
        raise AutoLoaderConfigError(
            f"Unsupported output mode '{output_mode}'. Supported: ['append', 'complete', 'update']"
        )

    if "mergeSchema" not in write_options:
        write_options["mergeSchema"] = _to_spark_option(True)

    return ResolvedAutoLoaderConfig(
        source=source_cfg,
        dbx=dbx_cfg,
        file_defaults=file_defaults,
        schema_location=schema_location,
        checkpoint_location=checkpoint_location,
        read_options=read_options,
        write_options=write_options,
        control_options=control_options,
        query_name=query_name,
        output_mode=output_mode,
        trigger_available_now=trigger_available_now,
        trigger_once=trigger_once,
    )


# -----------------------------
# Stream build / enrichment
# -----------------------------


def _apply_optional_schema(reader, schema_ddl: str | None):
    """Apply explicit schema if present. Accepts DDL string (Databricks-only) or None."""
    if schema_ddl:
        # Databricks runtime supports DDL string directly
        try:
            return reader.schema(schema_ddl)
        except Exception as exc:
            raise AutoLoaderConfigError(f"Failed to apply schema DDL: {exc}")
    return reader


def _enrich_with_metadata(df: DataFrame, resolved: ResolvedAutoLoaderConfig) -> DataFrame:
    """Add operational metadata columns for bronze/landing ingestion."""
    if not resolved.control_options.add_metadata_columns:
        return df

    metadata_source_name = (
        resolved.control_options.metadata_source_name
        or resolved.source.source_id
    )

    return (
        df.withColumn("_ingestion_ts", current_timestamp())
          .withColumn("_source_file", input_file_name())
          .withColumn("_source_path", lit(resolved.source.source_path))
          .withColumn("_source_id", lit(resolved.source.source_id))
          .withColumn("_metadata_source_name", lit(metadata_source_name))
    )


def ingest_file_stream(
    spark: SparkSession,
    source: Mapping[str, Any],
    global_config: Mapping[str, Any],
    source_options: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Create and return an Auto Loader streaming DataFrame."""
    resolved = resolve_autoloader_config(
        source=source,
        global_config=global_config,
        source_options=source_options,
    )

    logger.info(
        "Starting Auto Loader read for source_id=%s source_path=%s schema_location=%s",
        resolved.source.source_id,
        resolved.source.source_path,
        resolved.schema_location,
    )

    reader = spark.readStream.format("cloudFiles").options(**resolved.read_options)
    reader = _apply_optional_schema(reader, resolved.control_options.schema_ddl)

    df = reader.load(resolved.source.source_path)
    df = _enrich_with_metadata(df, resolved)

    return df


# -----------------------------
# Stream write / execution
# -----------------------------


def _build_writer(streaming_df: DataFrame, resolved: ResolvedAutoLoaderConfig):
    """Build the writeStream writer with validated options."""
    writer = (
        streaming_df.writeStream
        .format("delta")
        .outputMode(resolved.output_mode)
        .options(**resolved.write_options)
    )

    if resolved.query_name:
        writer = writer.queryName(resolved.query_name)

    if resolved.trigger_available_now:
        writer = writer.trigger(availableNow=True)
    elif resolved.trigger_once:
        writer = writer.trigger(once=True)

    return writer


def write_file_stream_to_landing(
    streaming_df: DataFrame,
    source: Mapping[str, Any],
    global_config: Mapping[str, Any],
    source_options: Mapping[str, Any] | None = None,
) -> None:
    """Write a streaming DataFrame to the configured landing Delta table."""
    resolved = resolve_autoloader_config(
        source=source,
        global_config=global_config,
        source_options=source_options,
    )

    if not resolved.source.landing_table:
        raise AutoLoaderConfigError("source.landing_table is required for Auto Loader writes")

    logger.info(
        "Starting Auto Loader write for source_id=%s landing_table=%s checkpoint_location=%s query_name=%s",
        resolved.source.source_id,
        resolved.source.landing_table,
        resolved.checkpoint_location,
        resolved.query_name,
    )

    try:
        writer = _build_writer(streaming_df, resolved)
        query: StreamingQuery = writer.toTable(resolved.source.landing_table)
        query.awaitTermination()

        exception = query.exception()
        if exception is not None:
            raise AutoLoaderExecutionError(
                f"Streaming query ended with exception for source_id={resolved.source.source_id} "
                f"landing_table={resolved.source.landing_table}: {exception}"
            )

        logger.info(
            "Completed Auto Loader write for source_id=%s landing_table=%s",
            resolved.source.source_id,
            resolved.source.landing_table,
        )

    except Exception as exc:
        raise AutoLoaderExecutionError(
            f"Auto Loader write failed for source_id={resolved.source.source_id} "
            f"source_path={resolved.source.source_path} "
            f"landing_table={resolved.source.landing_table} "
            f"checkpoint_location={resolved.checkpoint_location}: {exc}"
        ) from exc


def run_file_ingestion(
    spark: SparkSession,
    source: Mapping[str, Any],
    global_config: Mapping[str, Any],
    source_options: Mapping[str, Any] | None = None,
) -> None:
    """Convenience entrypoint: build the stream and write it to the landing table."""
    streaming_df = ingest_file_stream(
        spark=spark,
        source=source,
        global_config=global_config,
        source_options=source_options,
    )

    write_file_stream_to_landing(
        streaming_df=streaming_df,
        source=source,
        global_config=global_config,
        source_options=source_options,
    )