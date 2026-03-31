"""Bronze-layer data quality engine.

This module validates raw/landing payloads before conformance using optional
metadata embedded in source_options_json under the `bronze_dq` key.
"""

from __future__ import annotations

from typing import Any


def bronze_dq_config(source_options: dict[str, Any] | None) -> dict[str, Any]:
    cfg = (source_options or {}).get("bronze_dq", {})
    return cfg if isinstance(cfg, dict) else {}


def validate_bronze_dq_config(cfg: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    list_keys = ["required_columns", "expected_columns", "non_nullable_columns", "logical_checks"]
    for key in list_keys:
        if key in cfg and not isinstance(cfg[key], list):
            errors.append(f"bronze_dq.{key} must be a list")

    dict_keys = ["expected_types", "regex_patterns", "allowed_values", "length_constraints", "range_constraints", "null_thresholds"]
    for key in dict_keys:
        if key in cfg and not isinstance(cfg[key], dict):
            errors.append(f"bronze_dq.{key} must be an object")

    uniqueness = cfg.get("uniqueness_constraints", [])
    if uniqueness and not isinstance(uniqueness, list):
        errors.append("bronze_dq.uniqueness_constraints must be a list")

    freshness = cfg.get("freshness", {})
    if freshness and not isinstance(freshness, dict):
        errors.append("bronze_dq.freshness must be an object")

    volume = cfg.get("volume", {})
    if volume and not isinstance(volume, dict):
        errors.append("bronze_dq.volume must be an object")

    for col, threshold in (cfg.get("null_thresholds", {}) or {}).items():
        try:
            v = float(threshold)
        except (TypeError, ValueError):
            errors.append(f"bronze_dq.null_thresholds.{col} must be numeric")
            continue
        if v < 0 or v > 1:
            errors.append(f"bronze_dq.null_thresholds.{col} must be between 0 and 1")

    return errors


def dataset_schema_checks(df, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    cols = set(df.columns)
    required = set(cfg.get("required_columns", []))
    missing_required = sorted(required - cols)
    results.append(
        {
            "check_name": "required_columns",
            "status": "PASS" if not missing_required else "FAIL",
            "details": {"missing_columns": missing_required},
        }
    )

    expected = set(cfg.get("expected_columns", []))
    if expected:
        extra = sorted(cols - expected)
        results.append(
            {
                "check_name": "extra_columns",
                "status": "PASS" if not extra else "FAIL",
                "details": {"extra_columns": extra},
            }
        )

    expected_types = cfg.get("expected_types", {}) or {}
    type_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    type_mismatch = {}
    for col, expected_type in expected_types.items():
        actual = type_map.get(col)
        if actual and str(actual).lower() != str(expected_type).lower():
            type_mismatch[col] = {"expected": expected_type, "actual": actual}
    results.append(
        {
            "check_name": "data_types",
            "status": "PASS" if not type_mismatch else "FAIL",
            "details": {"mismatch": type_mismatch},
        }
    )

    return results


def row_level_checks(df, cfg: dict[str, Any]):
    from pyspark.sql import functions as F

    result = df.withColumn("bronze_dq_status", F.lit("PASS")).withColumn("bronze_dq_failed_check", F.lit(None).cast("string"))

    for col in cfg.get("non_nullable_columns", []):
        if col in df.columns:
            failed = F.col(col).isNull()
            result = result.withColumn("bronze_dq_status", F.when(failed, F.lit("FAIL")).otherwise(F.col("bronze_dq_status"))).withColumn(
                "bronze_dq_failed_check",
                F.when(failed & F.col("bronze_dq_failed_check").isNull(), F.lit(f"non_null:{col}")).otherwise(F.col("bronze_dq_failed_check")),
            )

    for col, values in (cfg.get("allowed_values", {}) or {}).items():
        if col in df.columns and isinstance(values, list) and values:
            failed = ~F.col(col).isin([v for v in values])
            result = result.withColumn("bronze_dq_status", F.when(failed, F.lit("FAIL")).otherwise(F.col("bronze_dq_status"))).withColumn(
                "bronze_dq_failed_check",
                F.when(failed & F.col("bronze_dq_failed_check").isNull(), F.lit(f"allowed_values:{col}")).otherwise(F.col("bronze_dq_failed_check")),
            )

    for col, pattern in (cfg.get("regex_patterns", {}) or {}).items():
        if col in df.columns and pattern:
            failed = (~F.col(col).rlike(str(pattern))) & F.col(col).isNotNull()
            result = result.withColumn("bronze_dq_status", F.when(failed, F.lit("FAIL")).otherwise(F.col("bronze_dq_status"))).withColumn(
                "bronze_dq_failed_check",
                F.when(failed & F.col("bronze_dq_failed_check").isNull(), F.lit(f"regex:{col}")).otherwise(F.col("bronze_dq_failed_check")),
            )

    for col, spec in (cfg.get("length_constraints", {}) or {}).items():
        if col not in df.columns or not isinstance(spec, dict):
            continue
        min_len = spec.get("min")
        max_len = spec.get("max")
        failed = F.lit(False)
        if min_len is not None:
            failed = failed | (F.length(F.col(col)) < F.lit(int(min_len)))
        if max_len is not None:
            failed = failed | (F.length(F.col(col)) > F.lit(int(max_len)))
        result = result.withColumn("bronze_dq_status", F.when(failed, F.lit("FAIL")).otherwise(F.col("bronze_dq_status"))).withColumn(
            "bronze_dq_failed_check",
            F.when(failed & F.col("bronze_dq_failed_check").isNull(), F.lit(f"length:{col}")).otherwise(F.col("bronze_dq_failed_check")),
        )

    for col, spec in (cfg.get("range_constraints", {}) or {}).items():
        if col not in df.columns or not isinstance(spec, dict):
            continue
        min_val = spec.get("min")
        max_val = spec.get("max")
        cast_col = F.col(col).cast("double")
        failed = F.lit(False)
        if min_val is not None:
            failed = failed | (cast_col < F.lit(float(min_val)))
        if max_val is not None:
            failed = failed | (cast_col > F.lit(float(max_val)))
        result = result.withColumn("bronze_dq_status", F.when(failed, F.lit("FAIL")).otherwise(F.col("bronze_dq_status"))).withColumn(
            "bronze_dq_failed_check",
            F.when(failed & F.col("bronze_dq_failed_check").isNull(), F.lit(f"range:{col}")).otherwise(F.col("bronze_dq_failed_check")),
        )

    for expr in cfg.get("logical_checks", []):
        if not expr:
            continue
        failed = ~F.expr(str(expr))
        result = result.withColumn("bronze_dq_status", F.when(failed, F.lit("FAIL")).otherwise(F.col("bronze_dq_status"))).withColumn(
            "bronze_dq_failed_check",
            F.when(failed & F.col("bronze_dq_failed_check").isNull(), F.lit(f"logical:{expr}")).otherwise(F.col("bronze_dq_failed_check")),
        )

    valid_df = result.filter("bronze_dq_status = 'PASS'")
    reject_df = result.filter("bronze_dq_status = 'FAIL'")
    return result, valid_df, reject_df


def dataset_quality_checks(df, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    results: list[dict[str, Any]] = []
    total_rows = df.count()

    for col, threshold in (cfg.get("null_thresholds", {}) or {}).items():
        if col not in df.columns:
            continue
        null_rows = df.filter(F.col(col).isNull()).count()
        ratio = (null_rows / total_rows) if total_rows else 0
        max_ratio = float(threshold)
        results.append(
            {
                "check_name": f"null_threshold:{col}",
                "status": "PASS" if ratio <= max_ratio else "FAIL",
                "details": {"ratio": ratio, "max_ratio": max_ratio},
            }
        )

    for entry in (cfg.get("uniqueness_constraints", []) or []):
        cols = entry if isinstance(entry, list) else [entry]
        cols = [c for c in cols if c in df.columns]
        if not cols:
            continue
        distinct = df.select(*cols).distinct().count()
        dupes = max(total_rows - distinct, 0)
        results.append(
            {
                "check_name": f"uniqueness:{','.join(cols)}",
                "status": "PASS" if dupes == 0 else "FAIL",
                "details": {"duplicate_rows": dupes},
            }
        )

    freshness = cfg.get("freshness", {}) or {}
    freshness_col = freshness.get("column")
    max_age_hours = freshness.get("max_age_hours")
    if freshness_col in df.columns and max_age_hours is not None:
        latest_ts = df.select(F.max(F.col(freshness_col)).alias("latest_ts")).collect()[0]["latest_ts"]
        age_hours = None
        if latest_ts is not None:
            age_hours = df.select((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.lit(latest_ts))) / 3600.0).collect()[0][0]
        status = "PASS" if age_hours is not None and age_hours <= float(max_age_hours) else "FAIL"
        results.append(
            {
                "check_name": f"freshness:{freshness_col}",
                "status": status,
                "details": {"age_hours": age_hours, "max_age_hours": float(max_age_hours)},
            }
        )

    volume = cfg.get("volume", {}) or {}
    min_rows = volume.get("min_rows")
    max_rows = volume.get("max_rows")
    if min_rows is not None or max_rows is not None:
        status = True
        if min_rows is not None:
            status = status and total_rows >= int(min_rows)
        if max_rows is not None:
            status = status and total_rows <= int(max_rows)
        results.append(
            {
                "check_name": "volume",
                "status": "PASS" if status else "FAIL",
                "details": {"row_count": total_rows, "min_rows": min_rows, "max_rows": max_rows},
            }
        )

    return results


def run_bronze_dq_checks(df, source_options: dict[str, Any] | None) -> dict[str, Any]:
    cfg = bronze_dq_config(source_options)
    cfg_errors = validate_bronze_dq_config(cfg)

    schema_results = dataset_schema_checks(df, cfg)
    annotated_df, valid_df, reject_df = row_level_checks(df, cfg)
    dataset_results = dataset_quality_checks(annotated_df, cfg)

    return {
        "config_errors": cfg_errors,
        "results": schema_results + dataset_results,
        "annotated_df": annotated_df,
        "valid_df": valid_df,
        "reject_df": reject_df,
    }