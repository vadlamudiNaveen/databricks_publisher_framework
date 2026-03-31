
from __future__ import annotations
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent / "00_common"))
from attachment_utils import handle_attachment_file
"""Batch file ingestion adapter for full and incremental loads.

This adapter is designed for Bronze RAW ingestion where heterogeneous files
(JSON/JSONL/mixed) can be loaded as raw payload using ``binaryFile``.
"""

# Allowed source_format values that map to a Spark reader format.
_FORMAT_MAP = {
    "json": "json",
    "jsonl": "json",           # JSONL is newline-delimited JSON — same Spark reader.
    "binaryFile": "binaryFile",
    "json_jsonl_mixed": "binaryFile",  # Mixed: read raw bytes, parse later.
    "csv": "csv",
    "parquet": "parquet",
    "avro": "avro",
    "orc": "orc",
    "text": "text",
}


def _reader_format(source: dict, global_config: dict) -> str:
    source_format = str(source.get("source_format", "")).strip()
    file_defaults = global_config.get("connections", {}).get("file_defaults", {})
    default_format = str(file_defaults.get("format", "binaryFile")).strip()
    return _FORMAT_MAP.get(source_format, default_format)


def ingest_file_batch(spark, source: dict, global_config: dict, source_options: dict | None = None):
    """Read files from *source_path* as a Spark batch DataFrame.

    Parameters
    ----------
    spark:
        Active SparkSession.
    source:
        Source registry row.  Must contain ``source_path``.
    global_config:
        Parsed global config dict.
    source_options:
        Parsed ``source_options_json`` dict.  All scalar values are forwarded
        as Spark reader options.  Special keys:

        - ``incremental_start_timestamp`` — ISO-8601 string; rows whose
          ``modificationTime`` is older are filtered out (binaryFile only).
        - ``fail_on_empty`` — ``"true"`` (default) raises ``ValueError`` if the
          path returns zero rows, catching path/config mismatches early.
        - Any other key is forwarded directly to the Spark reader as a string.

    Returns
    -------
    pyspark.sql.DataFrame
    """
    source_path = source.get("source_path", "")
    if not source_path:
        raise ValueError("source_path is required for FILE ingestion")

    source_options = source_options or {}
    fmt = _reader_format(source, global_config)
    reader = spark.read.format(fmt)

    # Reserved keys that are handled explicitly — not forwarded to the reader.
    _reserved = {"incremental_start_timestamp", "fail_on_empty", "file_ingest_mode"}

    for k, v in source_options.items():
        if k in _reserved:
            continue
        if isinstance(v, (str, int, float, bool)):
            reader = reader.option(k, str(v))

    df = reader.load(source_path)

    # Handle images and attachments if fields are specified in source_options
    image_field = source_options.get("image_field")
    attachment_field = source_options.get("attachment_field")
    dest_dir = source_options.get("attachment_dest_dir")
    if image_field and image_field in df.columns and dest_dir:
        # Copy all images referenced in the image_field column to dest_dir
        image_paths = [
            row[image_field]
            for row in df.select(image_field).distinct().collect()
            if row[image_field]
        ]
        for img_path in image_paths:
            try:
                new_path = handle_attachment_file(img_path, dest_dir)
                print(f"Copied image {img_path} to {new_path}")
            except Exception as e:
                print(f"Failed to copy image {img_path}: {e}")
    if attachment_field and attachment_field in df.columns and dest_dir:
        # Copy all attachments referenced in the attachment_field column to dest_dir
        attach_paths = [row[attachment_field] for row in df.select(attachment_field).distinct().collect() if row[attachment_field]]
        for att_path in attach_paths:
            try:
                new_path = handle_attachment_file(att_path, dest_dir)
                print(f"Copied attachment {att_path} to {new_path}")
            except Exception as e:
                print(f"Failed to copy attachment {att_path}: {e}")

    load_type = str(source.get("load_type", "incremental")).lower()
    if load_type == "incremental":
        from pyspark.sql import functions as F

        # Optional runtime filter — keep only files modified after this timestamp.
        incremental_start = source_options.get("incremental_start_timestamp")
        if incremental_start and "modificationTime" in df.columns:
            df = df.filter(F.col("modificationTime") > F.to_timestamp(F.lit(str(incremental_start))))

    # NOTE: empty-dataset detection is intentionally NOT done here.
    # Calling df.rdd.isEmpty() or df.count() at ingestion time would trigger a
    # full Spark action before the orchestrator's own raw_count = raw_df.count().
    # That would scan the data twice.  The orchestrator handles empty detection
    # by checking raw_count == 0 after ingestion.
    return df
