"""Silver publish engine with append and merge modes."""

from __future__ import annotations

import re
import uuid

# Only allow simple identifiers as merge keys (column names or schema.table.col).
# This prevents SQL injection through metadata-driven merge_key values.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _validate_identifier(value: str, field_name: str) -> str:
    """Raise ValueError if *value* is not a safe SQL identifier."""
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(
            f"{field_name} must be a valid SQL identifier (letters, digits, underscores, dots). "
            f"Got: {value!r}"
        )
    return value


def publish_append(
    df,
    silver_table: str,
    partition_columns: list[str] | None = None,
    table_type: str = "managed",
    external_path: str | None = None,
):
    resolved_table_type = str(table_type).strip().lower() if table_type else "managed"
    if resolved_table_type not in {"managed", "external"}:
        raise ValueError("table_type must be 'managed' or 'external'")
    if resolved_table_type == "external" and not str(external_path or "").strip():
        raise ValueError("external_path is required when table_type=external")

    writer = df.write.mode("append").format("delta").option("mergeSchema", "true")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    if resolved_table_type == "external" and external_path:
        writer = writer.option("path", external_path)
    writer.saveAsTable(silver_table)


def publish_merge(spark, df, silver_table: str, merge_key: str):
    _validate_identifier(merge_key, "merge_key")
    _validate_identifier(silver_table, "silver_table")

    # Use a unique view name per merge operation to prevent cross-source
    # temp view collisions when multiple sources run in the same Spark session.
    view_name = f"_stg_silver_{uuid.uuid4().hex[:12]}"
    df.createOrReplaceTempView(view_name)
    try:
        spark.sql(
            f"""
            MERGE INTO {silver_table} AS tgt
            USING {view_name} AS src
            ON tgt.{merge_key} = src.{merge_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(view_name)


def apply_post_publish_actions(spark, silver_table: str, zorder_columns: list[str] | None = None):
    _validate_identifier(silver_table, "silver_table")
    if zorder_columns:
        safe_cols = [_validate_identifier(c, "zorder_column") for c in zorder_columns]
        cols = ", ".join(safe_cols)
        spark.sql(f"OPTIMIZE {silver_table} ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE {silver_table}")
