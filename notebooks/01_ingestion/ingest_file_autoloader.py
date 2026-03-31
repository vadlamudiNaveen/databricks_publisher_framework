"""File ingestion adapter for Databricks Auto Loader."""

from __future__ import annotations


def _build_tracking_paths(global_config: dict, source: dict) -> tuple[str, str]:
    dbx_cfg = global_config.get("databricks", {})
    checkpoint_root = dbx_cfg.get("checkpoint_root")
    schema_root = dbx_cfg.get("schema_tracking_root")
    tenant = source.get("tenant")
    product = source.get("product_name")
    source_system = source.get("source_system")
    source_entity = source.get("source_entity")

    required = {
        "checkpoint_root": checkpoint_root,
        "schema_tracking_root": schema_root,
        "tenant": tenant,
        "product_name": product,
        "source_system": source_system,
        "source_entity": source_entity,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing tracking path configuration: {missing}")

    path_suffix = f"{tenant}/{product}/{source_system}/{source_entity}"
    checkpoint_location = f"{checkpoint_root.rstrip('/')}/{path_suffix}"
    schema_location = f"{schema_root.rstrip('/')}/{path_suffix}"
    return schema_location, checkpoint_location


def build_autoloader_options(source: dict, global_config: dict, source_options: dict | None = None) -> dict:
    """Build the full Auto Loader option dict for a given source.

    Extra keys from ``source_options`` are merged last so per-source overrides
    win over framework defaults.  The caller is responsible for separating
    streaming options (``cloudFiles.*``) from write options (``checkpointLocation``).
    """
    source_path = source.get("source_path")
    if not source_path:
        raise ValueError("source_path is required for FILE source types")

    schema_location, checkpoint_location = _build_tracking_paths(global_config, source)
    file_defaults = global_config.get("connections", {}).get("file_defaults")
    if not file_defaults:
        raise ValueError("connections.file_defaults must be configured in global config")

    required_defaults = {"format", "infer_column_types", "schema_evolution_mode"}
    missing_defaults = required_defaults - set(file_defaults)
    if missing_defaults:
        raise ValueError(f"file_defaults must define: {sorted(missing_defaults)}")

    format_value = source.get("source_format") or file_defaults["format"]
    infer_types = str(file_defaults["infer_column_types"]).lower()
    schema_evolution_mode = file_defaults["schema_evolution_mode"]

    options: dict = {
        "cloudFiles.format": format_value,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.inferColumnTypes": infer_types,
        "cloudFiles.schemaEvolutionMode": schema_evolution_mode,
        "path": source_path,
        "checkpointLocation": checkpoint_location,
    }

    # Per-source overrides from source_options_json (last-write wins).
    for k, v in (source_options or {}).items():
        if isinstance(v, (str, int, float, bool)):
            options[k] = str(v)

    return options


def ingest_file_stream(spark, source: dict, global_config: dict, source_options: dict | None = None):
    """Start an Auto Loader structured streaming read and return the streaming DataFrame.

    The returned DataFrame is *not* yet writing; callers must call
    ``write_file_stream_to_landing`` or attach their own write stream.
    """
    options = build_autoloader_options(source=source, global_config=global_config, source_options=source_options)

    # Separate cloudFiles read options from write-side options.
    read_options = {k: v for k, v in options.items() if k not in {"path", "checkpointLocation"}}

    reader = spark.readStream.format("cloudFiles").options(**read_options)

    # Optional explicit schema — set schema_ddl in source_options_json to skip inference.
    schema_ddl = (source_options or {}).get("schema_ddl", "")
    if schema_ddl:
        from pyspark.sql.types import StructType
        reader = reader.schema(StructType.fromDDL(schema_ddl))

    return reader.load(options["path"])


def write_file_stream_to_landing(
    spark,
    streaming_df,
    source: dict,
    global_config: dict,
    source_options: dict | None = None,
) -> None:
    """Write a streaming DataFrame to the landing Delta table.

    Uses ``availableNow=True`` so the query processes all available files then
    stops — equivalent to a micro-batch full-catch-up, friendly for scheduled jobs.

    Raises ``RuntimeError`` if the streaming query terminates with an exception,
    wrapping the cause with the source identifier for easier debugging.
    """
    options = build_autoloader_options(source=source, global_config=global_config, source_options=source_options)
    checkpoint_location = options["checkpointLocation"]
    landing_table = source.get("landing_table", "")
    if not landing_table:
        raise ValueError("landing_table is required in source metadata for Auto Loader write")

    merge_schema = str((source_options or {}).get("mergeSchema", "true")).lower()

    try:
        query = (
            streaming_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", merge_schema)
            .trigger(availableNow=True)
            .toTable(landing_table)
        )
        query.awaitTermination()
        if query.exception():
            raise RuntimeError(query.exception())
    except Exception as exc:
        source_id = f"{source.get('source_system', '?')}.{source.get('source_entity', '?')}"
        raise RuntimeError(
            f"Auto Loader streaming write failed for {source_id}: {exc}"
        ) from exc
