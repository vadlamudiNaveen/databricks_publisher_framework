"""Audit logger for pipeline execution metrics."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from importlib import import_module
from typing import Any


def _to_json_text(payload: Any) -> str:
    return json.dumps(payload, default=str)


def _spark_functions():
    return import_module("pyspark.sql.functions")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def write_pipeline_audit(spark, table_name: str, run_rows: list[tuple]):
    """Write pipeline run audit rows.

    Each tuple must have exactly 9 elements matching the schema::

        (pipeline_name, run_id, source_system, source_entity,
         rows_in, rows_out, status, error_message, run_ts)

    Pass ``None`` for ``run_ts`` to auto-fill with the current UTC timestamp.
    """
    F = _spark_functions()
    now = _utc_now()

    # Auto-fill run_ts (last element) if caller passes None.
    filled_rows = [
        row[:8] + (now,) if len(row) >= 9 and row[8] is None else row
        for row in run_rows
    ]

    schema = (
        "pipeline_name string, run_id string, source_system string, source_entity string, "
        "rows_in long, rows_out long, status string, error_message string, run_ts timestamp"
    )
    spark.createDataFrame(filled_rows, schema=schema).write.mode("append").format("delta").saveAsTable(table_name)


def write_dq_rule_results(spark, table_name: str, run_id: str, source_system: str, source_entity: str, dq_df):
    F = _spark_functions()

    summary_df = (
        dq_df.groupBy("dq_status", "dq_failed_rule")
        .agg(F.count("*").alias("row_count"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_system", F.lit(source_system))
        .withColumn("source_entity", F.lit(source_entity))
        .withColumn("run_ts", F.current_timestamp())
        .select("run_id", "source_system", "source_entity", "dq_status", "dq_failed_rule", "row_count", "run_ts")
    )
    summary_df.write.mode("append").format("delta").saveAsTable(table_name)


def write_reject_rows(reject_df, table_name: str, run_id: str, source_system: str, source_entity: str):
    """Write rejected rows to the rejects audit table.

    Each row is serialized to ``payload_json`` (all original columns as a JSON
    string) so the rejects table always has a stable, predictable schema that
    does not drift as source schemas evolve.  The standard metadata columns
    (run_id, source_system, source_entity, rejected_ts, dq_status,
    dq_failed_rule) are also persisted as first-class columns for easy querying.

    DDL expected::

        run_id STRING, source_system STRING, source_entity STRING,
        rejected_ts TIMESTAMP, dq_status STRING, dq_failed_rule STRING,
        payload_json STRING
    """
    F = _spark_functions()

    # Identify the standard DQ annotation columns added by the framework;
    # everything else forms the source payload.
    _framework_cols = {"dq_status", "dq_failed_rule", "bronze_dq_status", "bronze_dq_failed_check",
                       "source_system", "source_entity", "load_id", "ingest_ts"}
    payload_cols = [c for c in reject_df.columns if c not in _framework_cols]

    # Build a JSON string from the payload columns only.
    map_expr = F.to_json(F.struct(*[F.col(c) for c in payload_cols]))

    (
        reject_df
        .withColumn("run_id", F.lit(run_id))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_entity", F.lit(source_entity))
        .withColumn("rejected_ts", F.current_timestamp())
        .withColumn("payload_json", map_expr)
        .select(
            F.col("run_id"),
            F.col("_source_system").alias("source_system"),
            F.col("_source_entity").alias("source_entity"),
            F.col("rejected_ts"),
            F.col("dq_status"),
            F.col("dq_failed_rule"),
            F.col("payload_json"),
        )
        .write.mode("append")
        .format("delta")
        .saveAsTable(table_name)
    )


def write_bronze_dq_results(
    spark,
    table_name: str,
    run_id: str,
    source_system: str,
    source_entity: str,
    checks: list[dict[str, Any]],
    config_errors: list[str] | None = None,
):
    F = _spark_functions()

    rows = []
    for item in checks:
        rows.append(
            (
                run_id,
                source_system,
                source_entity,
                str(item.get("check_name", "UNKNOWN")),
                str(item.get("status", "UNKNOWN")),
                _to_json_text(item.get("details", {})),
            )
        )

    for err in config_errors or []:
        rows.append((run_id, source_system, source_entity, "bronze_dq_config", "FAIL", _to_json_text({"error": err})))

    if not rows:
        rows.append((run_id, source_system, source_entity, "bronze_dq_config", "PASS", _to_json_text({"message": "No bronze DQ checks configured"})))

    schema = (
        "run_id string, source_system string, source_entity string, "
        "validation_type string, validation_status string, error_details string"
    )
    (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("timestamp", F.current_timestamp())
        .write.mode("append")
        .format("delta")
        .saveAsTable(table_name)
    )


def write_validation_events(
    spark,
    table_name: str,
    run_id: str,
    source_system: str,
    source_entity: str,
    events: list[dict[str, Any]],
):
    F = _spark_functions()

    rows = []
    for event in events:
        rows.append(
            (
                run_id,
                source_system,
                source_entity,
                str(event.get("event", "UNKNOWN")),
                str(event.get("status", "INFO")),
                _to_json_text(event.get("payload", {})),
            )
        )

    if not rows:
        return

    schema = "run_id string, source_system string, source_entity string, event_name string, event_status string, event_payload string"
    (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("event_ts", F.current_timestamp())
        .write.mode("append")
        .format("delta")
        .saveAsTable(table_name)
    )
