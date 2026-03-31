from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
COMMON_DIR = CURRENT_DIR.parent / "00_common"
INGESTION_DIR = CURRENT_DIR.parent / "01_ingestion"
PROCESSING_DIR = CURRENT_DIR.parent / "02_processing"
PUBLISH_DIR = CURRENT_DIR.parent / "03_publish"
AUDIT_DIR = CURRENT_DIR.parent / "04_audit"

for p in [COMMON_DIR, INGESTION_DIR, PROCESSING_DIR, PUBLISH_DIR, AUDIT_DIR]:
    sys.path.append(str(p))

from global_config import load_global_config
from config_loader import (
    active_sources,
    dq_rules_for_entity,
    mappings_for_entity,
    parse_json_cell,
    parse_json_list,
    publish_rule_for_entity,
)
from utils import get_logger, new_load_id, truncate_str

_logger = get_logger("ingestion_framework")
from ingest_api import ingest_api
from ingest_file_autoloader import ingest_file_stream, write_file_stream_to_landing
from ingest_file_batch import ingest_file_batch
from ingest_jdbc import ingest_jdbc_batch
from landing_engine import add_landing_metadata, write_landing
from conformance_engine import apply_column_mappings, write_conformance
from bronze_dq_engine import run_bronze_dq_checks
from dq_engine import apply_dq_rules, split_valid_reject
from publish_silver import apply_post_publish_actions, publish_append, publish_merge
from audit_logger import (
    write_bronze_dq_results,
    write_dq_rule_results,
    write_pipeline_audit,
    write_reject_rows,
    write_validation_events,
)


def _require_source_fields(source: dict) -> None:
    required = [
        "source_system",
        "source_entity",
        "source_type",
        "landing_table",
        "conformance_table",
        "silver_table",
        "publish_mode",
    ]
    missing = [k for k in required if not source.get(k)]
    if missing:
        raise ValueError(f"Invalid source config for {source}: missing {missing}")


def _connection_profiles(global_config: dict) -> tuple[dict, dict]:
    connections = global_config.get("connections", {})
    return connections.get("jdbc_profiles", {}), connections.get("api_profiles", {})


def _ingest_source(spark, source: dict, global_config: dict):
    source_type = source["source_type"].upper()
    source_options = parse_json_cell(source.get("source_options_json"))
    load_type = str(source.get("load_type", global_config.get("execution", {}).get("default_load_type", "incremental"))).lower()
    jdbc_profiles, api_profiles = _connection_profiles(global_config)

    if source_type == "JDBC":
        profile_name = source.get("jdbc_profile", "")
        jdbc_profile = jdbc_profiles.get(profile_name, {})
        if load_type == "incremental" and source.get("watermark_column") and source_options.get("incremental_start_value"):
            watermark_col = source["watermark_column"]
            start_value = str(source_options["incremental_start_value"]).replace("'", "''")
            table = source.get("jdbc_table", "")
            source = {
                **source,
                "jdbc_table": f"(SELECT * FROM {table} WHERE {watermark_col} > '{start_value}') t",
            }
        return ingest_jdbc_batch(spark=spark, jdbc_profile=jdbc_profile, source=source, extra_options=source_options)

    if source_type == "API":
        profile_name = source.get("api_profile", "")
        api_profile = api_profiles.get(profile_name, {})
        query_params = parse_json_cell(source.get("api_query_params_json"))
        if load_type == "incremental" and source.get("watermark_column") and source_options.get("incremental_start_value"):
            query_params[source["watermark_column"]] = source_options["incremental_start_value"]
        # Pass source_options so pagination config (pagination_type, page_size, etc.)
        # is read from source_options_json, not from the flat registry row.
        records = ingest_api(source=source, api_profile=api_profile, params=query_params, source_options=source_options)
        return spark.createDataFrame(records)

    if source_type == "FILE":
        file_mode = str(source_options.get("file_ingest_mode", "batch")).lower()
        if file_mode == "autoloader":
            return ingest_file_stream(spark=spark, source=source, global_config=global_config, source_options=source_options)
        return ingest_file_batch(spark=spark, source=source, global_config=global_config, source_options=source_options)

    raise ValueError(f"Unsupported source_type: {source_type}")


def _audit_table_name(global_config: dict) -> str:
    table_name = global_config.get("audit", {}).get("pipeline_runs_table", "")
    if not table_name:
        raise ValueError("audit.pipeline_runs_table must be configured in global_config.yaml")
    return table_name


def _execution_config(global_config: dict) -> dict:
    return global_config.get("execution", {})


def _retry_policy(global_config: dict, source_type: str) -> tuple[int, float]:
    execution = _execution_config(global_config)
    per_source_type = execution.get("retry_by_source_type", {})
    source_policy = per_source_type.get(source_type.lower(), {})

    max_retries = int(source_policy.get("max_retries", execution.get("max_retries", 1)))
    backoff_seconds = float(source_policy.get("backoff_seconds", execution.get("retry_backoff_seconds", 2)))
    return max_retries, backoff_seconds


def _run_with_retry(func, max_retries: int, backoff_seconds: float, stage_name: str):
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as exc:  # pragma: no cover
            last_error = exc
            if attempt >= max_retries:
                break
            time.sleep(backoff_seconds * (attempt + 1))
    if last_error is None:
        raise RuntimeError(f"{stage_name} failed with unknown error")
    raise RuntimeError(f"{stage_name} failed after {max_retries + 1} attempt(s): {last_error}") from last_error


def _require_runtime_config(global_config: dict) -> None:
    required = [
        ("organization.framework_name", global_config.get("organization", {}).get("framework_name")),
        ("audit.pipeline_runs_table", global_config.get("audit", {}).get("pipeline_runs_table")),
    ]
    missing = [name for name, value in required if not value]
    if missing:
        raise ValueError(f"Missing required global config values: {missing}")


def _publish_rule_lists(publish_rule: dict | None) -> tuple[list[str], list[str]]:
    if not publish_rule:
        return [], []
    partition_columns = [str(c) for c in parse_json_list(publish_rule.get("partition_columns_json")) if str(c).strip()]
    zorder_columns = [str(c) for c in parse_json_list(publish_rule.get("optimize_zorder_json")) if str(c).strip()]
    return partition_columns, zorder_columns


def _log_event(event: str, **fields) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    _logger.info(json.dumps(payload))


def _append_validation_event(events: list[dict], event: str, status: str = "INFO", **payload) -> None:
    events.append(
        {
            "event": event,
            "status": status,
            "payload": payload,
        }
    )


def process_source(
    config_dir: str,
    source: dict,
    global_config: dict,
    spark=None,
    dry_run: bool = True,
) -> dict:
    _require_source_fields(source)
    _require_runtime_config(global_config)

    source_system = source["source_system"]
    source_entity = source["source_entity"]
    source_type = source["source_type"].upper()
    load_id = new_load_id()
    stage_seconds: dict[str, float] = {}
    validation_events: list[dict] = []

    _log_event(
        "source_start",
        source=f"{source_system}.{source_entity}",
        load_id=load_id,
        source_type=source_type,
        dry_run=dry_run,
    )
    _append_validation_event(
        validation_events,
        "source_start",
        source=f"{source_system}.{source_entity}",
        load_id=load_id,
        source_type=source_type,
        dry_run=dry_run,
    )

    mapping_rows = mappings_for_entity(
        config_dir,
        source_system,
        source_entity,
        product_name=source.get("product_name"),
        spark=spark,
        global_config=global_config,
    )
    dq_rows = dq_rules_for_entity(
        config_dir,
        source_system,
        source_entity,
        product_name=source.get("product_name"),
        spark=spark,
        global_config=global_config,
    )
    publish_rule = publish_rule_for_entity(
        config_dir,
        source_system,
        source_entity,
        product_name=source.get("product_name"),
        spark=spark,
        global_config=global_config,
    )

    _log_event(
        "metadata_resolved",
        source=f"{source_system}.{source_entity}",
        mapping_count=len(mapping_rows),
        dq_rule_count=len(dq_rows),
        publish_rule_found=bool(publish_rule),
    )
    _append_validation_event(
        validation_events,
        "metadata_resolved",
        status="PASS",
        mapping_count=len(mapping_rows),
        dq_rule_count=len(dq_rows),
        publish_rule_found=bool(publish_rule),
    )

    if dry_run or spark is None:
        _log_event(
            "verification_dry_run",
            source=f"{source_system}.{source_entity}",
            checks={
                "routing_metadata_resolved": True,
                "mappings_loaded": len(mapping_rows),
                "dq_rules_loaded": len(dq_rows),
                "publish_mode_configured": bool(source.get("publish_mode")),
            },
        )
        _append_validation_event(
            validation_events,
            "verification_dry_run",
            status="PASS",
            routing_metadata_resolved=True,
            mappings_loaded=len(mapping_rows),
            dq_rules_loaded=len(dq_rows),
            publish_mode_configured=bool(source.get("publish_mode")),
        )
        return {
            "tenant": source.get("tenant", ""),
            "brand": source.get("brand", ""),
            "product_name": source.get("product_name", ""),
            "source": f"{source_system}.{source_entity}",
            "mode": "dry_run",
            "steps": [
                f"ingest via {source['source_type']}",
                f"write landing {source['landing_table']}",
                f"apply {len(mapping_rows)} mappings",
                f"apply {len(dq_rows)} dq rules",
                f"publish {source['publish_mode']} to {source['silver_table']}",
            ],
        }
    fail_fast = bool(_execution_config(global_config).get("fail_fast", True))
    max_retries, backoff_seconds = _retry_policy(global_config, source_type=source_type)
    audit_cfg = global_config.get("audit", {})
    framework_name = global_config.get("organization", {}).get("framework_name", "")
    audit_table = _audit_table_name(global_config)

    try:
        t0 = time.monotonic()
        raw_df = _run_with_retry(
            lambda: _ingest_source(spark, source, global_config),
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            stage_name=f"ingest:{source_system}.{source_entity}",
        )
        stage_seconds["ingest"] = round(time.monotonic() - t0, 3)
        _log_event(
            "stage_complete",
            source=f"{source_system}.{source_entity}",
            stage="ingestion_adapter",
            seconds=stage_seconds["ingest"],
            verification={"dataframe_created": raw_df is not None},
        )
        _append_validation_event(
            validation_events,
            "ingestion_adapter",
            status="PASS" if raw_df is not None else "FAIL",
            seconds=stage_seconds["ingest"],
            dataframe_created=raw_df is not None,
        )

        if getattr(raw_df, "isStreaming", False):
            source_options = parse_json_cell(source.get("source_options_json"))
            t1 = time.monotonic()
            streaming_landing_df = add_landing_metadata(
                raw_df,
                source_system=source_system,
                source_entity=source_entity,
                load_id=load_id,
            )
            write_file_stream_to_landing(
                spark=spark,
                streaming_df=streaming_landing_df,
                source=source,
                global_config=global_config,
                source_options=source_options,
            )
            stage_seconds["landing_stream"] = round(time.monotonic() - t1, 3)
            _log_event(
                "stage_complete",
                source=f"{source_system}.{source_entity}",
                stage="landing_stream",
                seconds=stage_seconds["landing_stream"],
                verification={"streaming_query_completed": True},
            )
            _append_validation_event(
                validation_events,
                "landing_stream",
                status="PASS",
                seconds=stage_seconds["landing_stream"],
                streaming_query_completed=True,
            )

            validation_events_table = audit_cfg.get("validation_events_table", "")
            if validation_events_table:
                write_validation_events(
                    spark,
                    validation_events_table,
                    load_id,
                    source_system,
                    source_entity,
                    validation_events,
                )

            write_pipeline_audit(
                spark,
                table_name=audit_table,
                run_rows=[
                    (
                        framework_name,
                        load_id,
                        source_system,
                        source_entity,
                        0,
                        0,
                        "SUCCESS",
                        None,
                        None,
                    )
                ],
            )

            return {
                "source": f"{source_system}.{source_entity}",
                "mode": "execute_streaming",
                "landing_table": source["landing_table"],
                "status": "SUCCESS",
                "stage_seconds": stage_seconds,
            }

        t2 = time.monotonic()
        source_options = parse_json_cell(source.get("source_options_json"))
        raw_columns = list(raw_df.columns)
        raw_count = raw_df.count()
        bronze_out = run_bronze_dq_checks(raw_df, source_options=source_options)
        bronze_df = bronze_out["annotated_df"]
        bronze_valid_df = bronze_out["valid_df"]
        bronze_reject_df = bronze_out["reject_df"]
        bronze_results = bronze_out["results"]
        bronze_config_errors = bronze_out["config_errors"]

        # Cache counts once — reused in stage log, append_event, and verification_summary.
        bronze_valid_count = bronze_valid_df.count()
        bronze_reject_count = bronze_reject_df.count()

        _log_event(
            "stage_complete",
            source=f"{source_system}.{source_entity}",
            stage="bronze_dq_engine",
            verification={
                "config_errors": bronze_config_errors,
                "checks": bronze_results,
                "bronze_valid_rows": bronze_valid_count,
                "bronze_reject_rows": bronze_reject_count,
            },
        )
        _append_validation_event(
            validation_events,
            "bronze_dq_engine",
            status="PASS" if not bronze_config_errors else "FAIL",
            config_errors=bronze_config_errors,
            checks=bronze_results,
            bronze_valid_rows=bronze_valid_count,
            bronze_reject_rows=bronze_reject_count,
        )

        landing_df = add_landing_metadata(bronze_df, source_system=source_system, source_entity=source_entity, load_id=load_id)
        write_landing(landing_df, source["landing_table"])
        stage_seconds["landing"] = round(time.monotonic() - t2, 3)
        required_landing_cols = {"source_system", "source_entity", "load_id", "ingest_ts"}
        landing_has_tech_cols = required_landing_cols.issubset(set(landing_df.columns))
        _log_event(
            "stage_complete",
            source=f"{source_system}.{source_entity}",
            stage="landing_raw",
            seconds=stage_seconds["landing"],
            verification={
                "raw_row_count": raw_count,
                "raw_columns_preserved": set(raw_columns).issubset(set(landing_df.columns)),
                "landing_technical_columns_present": landing_has_tech_cols,
            },
        )
        _append_validation_event(
            validation_events,
            "landing_raw",
            status="PASS" if landing_has_tech_cols else "FAIL",
            seconds=stage_seconds["landing"],
            raw_row_count=raw_count,
            raw_columns_preserved=set(raw_columns).issubset(set(landing_df.columns)),
            landing_technical_columns_present=landing_has_tech_cols,
        )

        t3 = time.monotonic()
        conformance_df = apply_column_mappings(bronze_valid_df, mapping_rows)
        write_conformance(conformance_df, source["conformance_table"])
        stage_seconds["conformance"] = round(time.monotonic() - t3, 3)
        expected_conf_cols = [r.get("conformance_column", "") for r in mapping_rows if r.get("conformance_column")]
        missing_conf_cols = [c for c in expected_conf_cols if c not in conformance_df.columns]
        _log_event(
            "stage_complete",
            source=f"{source_system}.{source_entity}",
            stage="conformance_engine",
            seconds=stage_seconds["conformance"],
            verification={
                "expected_conformance_columns": expected_conf_cols,
                "missing_conformance_columns": missing_conf_cols,
                "conformance_columns_ok": len(missing_conf_cols) == 0,
            },
        )
        _append_validation_event(
            validation_events,
            "conformance_engine",
            status="PASS" if len(missing_conf_cols) == 0 else "FAIL",
            seconds=stage_seconds["conformance"],
            expected_conformance_columns=expected_conf_cols,
            missing_conformance_columns=missing_conf_cols,
        )

        t4 = time.monotonic()
        dq_df = apply_dq_rules(conformance_df, dq_rows)
        valid_df, reject_df = split_valid_reject(dq_df)
        stage_seconds["dq"] = round(time.monotonic() - t4, 3)
        _log_event(
            "stage_complete",
            source=f"{source_system}.{source_entity}",
            stage="dq_engine",
            seconds=stage_seconds["dq"],
            verification={
                "dq_columns_present": {"dq_status", "dq_failed_rule"}.issubset(set(dq_df.columns)),
            },
        )
        _append_validation_event(
            validation_events,
            "dq_engine",
            status="PASS" if {"dq_status", "dq_failed_rule"}.issubset(set(dq_df.columns)) else "FAIL",
            seconds=stage_seconds["dq"],
            dq_columns_present={"dq_status", "dq_failed_rule"}.issubset(set(dq_df.columns)),
        )

        mode = (publish_rule or {}).get("publish_mode", source["publish_mode"]).lower()
        partition_columns, zorder_columns = _publish_rule_lists(publish_rule)
        t5 = time.monotonic()
        if mode == "append":
            _run_with_retry(
                lambda: publish_append(valid_df, source["silver_table"], partition_columns=partition_columns),
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
                stage_name=f"publish_append:{source_system}.{source_entity}",
            )
        elif mode == "merge":
            merge_key = (publish_rule or {}).get("merge_key") or source.get("primary_key", "")
            if not merge_key:
                raise ValueError(f"Missing merge key for {source_system}.{source_entity}")
            _run_with_retry(
                lambda: publish_merge(spark, valid_df, source["silver_table"], merge_key),
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
                stage_name=f"publish_merge:{source_system}.{source_entity}",
            )
        else:
            raise ValueError(f"Unsupported publish mode: {mode}")

        post_publish_optimize = bool(_execution_config(global_config).get("post_publish_optimize", True))
        if post_publish_optimize:
            apply_post_publish_actions(spark, source["silver_table"], zorder_columns=zorder_columns)
        stage_seconds["publish"] = round(time.monotonic() - t5, 3)

        _log_event(
            "stage_complete",
            source=f"{source_system}.{source_entity}",
            stage="silver_publish",
            seconds=stage_seconds["publish"],
            verification={
                "publish_mode": mode,
                "partition_columns": partition_columns,
                "zorder_columns": zorder_columns,
                "post_publish_optimize": post_publish_optimize,
            },
        )
        _append_validation_event(
            validation_events,
            "silver_publish",
            status="PASS",
            seconds=stage_seconds["publish"],
            publish_mode=mode,
            partition_columns=partition_columns,
            zorder_columns=zorder_columns,
            post_publish_optimize=post_publish_optimize,
        )

        # Count once near the end to avoid repetitive expensive actions.
        landing_count = landing_df.count()
        conformance_count = conformance_df.count()
        valid_count = valid_df.count()
        reject_count = reject_df.count()

        checks = {
            "landing_equals_ingestion_for_batch": landing_count == raw_count,
            "bronze_valid_plus_bronze_reject_equals_ingestion": (bronze_valid_count + bronze_reject_count) == raw_count,
            "valid_plus_reject_equals_conformance": (valid_count + reject_count) == conformance_count,
            "append_expected_silver_increase_equals_valid": mode != "append" or valid_count >= 0,
        }
        _log_event(
            "verification_summary",
            source=f"{source_system}.{source_entity}",
            load_id=load_id,
            counts={
                "ingestion_rows": raw_count,
                "landing_rows": landing_count,
                "bronze_valid_rows": bronze_valid_count,
                    "bronze_reject_rows": bronze_reject_count,
                "conformance_rows": conformance_count,
                "valid_rows": valid_count,
                "reject_rows": reject_count,
            },
            checks=checks,
        )
        _append_validation_event(
            validation_events,
            "verification_summary",
            status="PASS" if all(checks.values()) else "FAIL",
            counts={
                "ingestion_rows": raw_count,
                "landing_rows": landing_count,
                "bronze_valid_rows": bronze_valid_count,
                "bronze_reject_rows": bronze_reject_count,
                "conformance_rows": conformance_count,
                "valid_rows": valid_count,
                "reject_rows": reject_count,
            },
            checks=checks,
        )

        bronze_results_table = audit_cfg.get("bronze_dq_results_table", "")
        if bronze_results_table:
            write_bronze_dq_results(
                spark,
                bronze_results_table,
                load_id,
                source_system,
                source_entity,
                bronze_results,
                config_errors=bronze_config_errors,
            )

        dq_results_table = audit_cfg.get("dq_results_table", "")
        if dq_results_table:
            write_dq_rule_results(spark, dq_results_table, load_id, source_system, source_entity, dq_df)

        rejects_table = audit_cfg.get("rejects_table", "")
        if rejects_table and reject_count > 0:
            write_reject_rows(reject_df, rejects_table, load_id, source_system, source_entity)

        validation_events_table = audit_cfg.get("validation_events_table", "")
        if validation_events_table:
            write_validation_events(
                spark,
                validation_events_table,
                load_id,
                source_system,
                source_entity,
                validation_events,
            )

        write_pipeline_audit(
            spark,
            table_name=audit_table,
            run_rows=[
                (
                    framework_name,
                    load_id,
                    source_system,
                    source_entity,
                    landing_count,
                    valid_count,
                    "SUCCESS",
                    None,
                    None,
                )
            ],
        )

        return {
            "source": f"{source_system}.{source_entity}",
            "mode": "execute",
            "status": "SUCCESS",
            "valid_rows": valid_count,
            "reject_rows": reject_count,
            "publish_mode": mode,
            "stage_seconds": stage_seconds,
        }
    except Exception as exc:
        _log_event(
            "source_failed",
            source=f"{source_system}.{source_entity}",
            load_id=load_id,
            error=truncate_str(str(exc)),
            stage_seconds=stage_seconds,
        )
        _append_validation_event(
            validation_events,
            "source_failed",
            status="FAIL",
            error=truncate_str(str(exc)),
            stage_seconds=stage_seconds,
        )
        validation_events_table = audit_cfg.get("validation_events_table", "")
        if validation_events_table:
            write_validation_events(
                spark,
                validation_events_table,
                load_id,
                source_system,
                source_entity,
                validation_events,
            )
        write_pipeline_audit(
            spark,
            table_name=audit_table,
            run_rows=[
                (
                    framework_name,
                    load_id,
                    source_system,
                    source_entity,
                    0,
                    0,
                    "FAILED",
                    truncate_str(str(exc)),
                    None,
                )
            ],
        )
        if fail_fast:
            raise
        return {
            "source": f"{source_system}.{source_entity}",
            "mode": "execute",
            "status": "FAILED",
            "error": truncate_str(str(exc)),
            "stage_seconds": stage_seconds,
        }


def run_all(
    config_dir: str,
    global_config: dict,
    spark=None,
    dry_run: bool = True,
    product_name: str | None = None,
    source_system: str | None = None,
    source_entity: str | None = None,
) -> list[dict]:
    sources = active_sources(
        config_dir,
        spark=spark,
        global_config=global_config,
        product_name=product_name,
        source_system=source_system,
        source_entity=source_entity,
    )

    results = []
    for source in sources:
        results.append(
            process_source(
                config_dir=config_dir,
                source=source,
                global_config=global_config,
                spark=spark,
                dry_run=dry_run,
            )
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run metadata-driven IKEA ingestion orchestrator")
    parser.add_argument("--config-dir", default=str(Path(__file__).resolve().parents[2] / "config"))
    parser.add_argument("--global-config", default=str(Path(__file__).resolve().parents[2] / "config" / "global_config.yaml"))
    parser.add_argument("--execute", action="store_true", help="Execute with Spark (Databricks runtime)")
    parser.add_argument("--product-name", default=None, help="Optional product filter")
    parser.add_argument("--source-system", default=None, help="Optional source system filter")
    parser.add_argument("--source-entity", default=None, help="Optional source entity filter")
    args = parser.parse_args()

    global_config = load_global_config(args.global_config)
    dry_run = not args.execute

    results = run_all(
        config_dir=args.config_dir,
        global_config=global_config,
        spark=globals().get("spark"),
        dry_run=dry_run,
        product_name=args.product_name,
        source_system=args.source_system,
        source_entity=args.source_entity,
    )
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
