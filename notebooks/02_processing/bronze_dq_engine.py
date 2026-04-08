"""Production-oriented Bronze-layer data quality engine."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_ALLOWED_TOP_LEVEL_KEYS = {
    "required_columns",
    "expected_columns",
    "non_nullable_columns",
    "logical_checks",
    "expected_types",
    "regex_patterns",
    "allowed_values",
    "length_constraints",
    "range_constraints",
    "null_thresholds",
    "uniqueness_constraints",
    "freshness",
    "volume",
}

_ALLOWED_CHECK_STATUSES = {"PASS", "FAIL"}


class BronzeDQConfigError(ValueError):
    """Raised when bronze DQ configuration is invalid."""


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _require_dict(value: Any, field_name: str, errors: list[str]) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        errors.append(f"{field_name} must be an object")
        return {}
    return value


def _require_list(value: Any, field_name: str, errors: list[str]) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{field_name} must be a list")
        return []
    return value


def bronze_dq_config(source_options: Mapping[str, Any] | None) -> dict[str, Any]:
    cfg = (source_options or {}).get("bronze_dq", {})
    return cfg if isinstance(cfg, dict) else {}


def validate_bronze_dq_config(cfg: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []

    unknown_keys = sorted(set(cfg) - _ALLOWED_TOP_LEVEL_KEYS)
    if unknown_keys:
        errors.append(f"bronze_dq contains unsupported keys: {unknown_keys}")

    list_keys = [
        "required_columns",
        "expected_columns",
        "non_nullable_columns",
        "logical_checks",
    ]
    for key in list_keys:
        _require_list(cfg.get(key), f"bronze_dq.{key}", errors)

    dict_keys = [
        "expected_types",
        "regex_patterns",
        "allowed_values",
        "length_constraints",
        "range_constraints",
        "null_thresholds",
        "freshness",
        "volume",
    ]
    for key in dict_keys:
        _require_dict(cfg.get(key), f"bronze_dq.{key}", errors)

    uniqueness = cfg.get("uniqueness_constraints")
    if uniqueness is not None and not isinstance(uniqueness, list):
        errors.append("bronze_dq.uniqueness_constraints must be a list")

    for col, threshold in (_as_dict(cfg.get("null_thresholds"))).items():
        try:
            v = float(threshold)
        except (TypeError, ValueError):
            errors.append(f"bronze_dq.null_thresholds.{col} must be numeric")
            continue
        if v < 0 or v > 1:
            errors.append(f"bronze_dq.null_thresholds.{col} must be between 0 and 1")

    for col, spec in (_as_dict(cfg.get("length_constraints"))).items():
        if not isinstance(spec, dict):
            errors.append(f"bronze_dq.length_constraints.{col} must be an object")
            continue
        min_len = spec.get("min")
        max_len = spec.get("max")
        if min_len is not None:
            try:
                int(min_len)
            except (TypeError, ValueError):
                errors.append(f"bronze_dq.length_constraints.{col}.min must be an integer")
        if max_len is not None:
            try:
                int(max_len)
            except (TypeError, ValueError):
                errors.append(f"bronze_dq.length_constraints.{col}.max must be an integer")

    for col, spec in (_as_dict(cfg.get("range_constraints"))).items():
        if not isinstance(spec, dict):
            errors.append(f"bronze_dq.range_constraints.{col} must be an object")
            continue
        min_val = spec.get("min")
        max_val = spec.get("max")
        if min_val is not None:
            try:
                float(min_val)
            except (TypeError, ValueError):
                errors.append(f"bronze_dq.range_constraints.{col}.min must be numeric")
        if max_val is not None:
            try:
                float(max_val)
            except (TypeError, ValueError):
                errors.append(f"bronze_dq.range_constraints.{col}.max must be numeric")

    freshness = _as_dict(cfg.get("freshness"))
    if freshness:
        if "column" in freshness and freshness["column"] is not None and not isinstance(freshness["column"], str):
            errors.append("bronze_dq.freshness.column must be a string")
        if "max_age_hours" in freshness and freshness["max_age_hours"] is not None:
            try:
                v = float(freshness["max_age_hours"])
                if v < 0:
                    errors.append("bronze_dq.freshness.max_age_hours must be >= 0")
            except (TypeError, ValueError):
                errors.append("bronze_dq.freshness.max_age_hours must be numeric")

    volume = _as_dict(cfg.get("volume"))
    if volume:
        if "min_rows" in volume and volume["min_rows"] is not None:
            try:
                int(volume["min_rows"])
            except (TypeError, ValueError):
                errors.append("bronze_dq.volume.min_rows must be an integer")
        if "max_rows" in volume and volume["max_rows"] is not None:
            try:
                int(volume["max_rows"])
            except (TypeError, ValueError):
                errors.append("bronze_dq.volume.max_rows must be an integer")

    for col, values in (_as_dict(cfg.get("allowed_values"))).items():
        if not isinstance(values, list):
            errors.append(f"bronze_dq.allowed_values.{col} must be a list")

    return errors


@dataclass(frozen=True)
class BronzeDQRunResult:
    config_errors: list[str]
    results: list[dict[str, Any]]
    annotated_df: DataFrame
    valid_df: DataFrame
    reject_df: DataFrame


def _result(check_name: str, status: str, details: dict[str, Any]) -> dict[str, Any]:
    if status not in _ALLOWED_CHECK_STATUSES:
        raise BronzeDQConfigError(f"Invalid check status: {status}")
    return {
        "check_name": check_name,
        "status": status,
        "details": details,
    }


def dataset_schema_checks(df: DataFrame, cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    cols = set(df.columns)

    required = {
        str(col).strip()
        for col in _as_list(cfg.get("required_columns"))
        if str(col).strip()
    }
    missing_required = sorted(required - cols)
    results.append(
        _result(
            check_name="required_columns",
            status="PASS" if not missing_required else "FAIL",
            details={"missing_columns": missing_required},
        )
    )

    expected = {
        str(col).strip()
        for col in _as_list(cfg.get("expected_columns"))
        if str(col).strip()
    }
    if expected:
        extra = sorted(cols - expected)
        missing_expected = sorted(expected - cols)
        results.append(
            _result(
                check_name="expected_columns",
                status="PASS" if not extra and not missing_expected else "FAIL",
                details={
                    "extra_columns": extra,
                    "missing_expected_columns": missing_expected,
                },
            )
        )

    expected_types = _as_dict(cfg.get("expected_types"))
    type_map = {field.name: field.dataType.simpleString().lower() for field in df.schema.fields}
    type_mismatch: dict[str, dict[str, str]] = {}
    missing_type_columns: list[str] = []

    for col, expected_type in expected_types.items():
        actual = type_map.get(str(col))
        if actual is None:
            missing_type_columns.append(str(col))
            continue
        expected_type_str = str(expected_type).strip().lower()
        if expected_type_str and actual != expected_type_str:
            type_mismatch[str(col)] = {
                "expected": expected_type_str,
                "actual": actual,
            }

    results.append(
        _result(
            check_name="data_types",
            status="PASS" if not type_mismatch and not missing_type_columns else "FAIL",
            details={
                "mismatch": type_mismatch,
                "missing_columns": sorted(missing_type_columns),
            },
        )
    )

    return results


def _append_failure(
    df: DataFrame,
    failed_condition,
    failure_label: str,
) -> DataFrame:
    return (
        df.withColumn(
            "bronze_dq_status",
            F.when(failed_condition, F.lit("FAIL")).otherwise(F.col("bronze_dq_status")),
        )
        .withColumn(
            "bronze_dq_failed_check",
            F.when(
                failed_condition & F.col("bronze_dq_failed_check").isNull(),
                F.lit(failure_label),
            ).otherwise(F.col("bronze_dq_failed_check")),
        )
    )


def row_level_checks(df: DataFrame, cfg: Mapping[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    result = (
        df.withColumn("bronze_dq_status", F.lit("PASS"))
        .withColumn("bronze_dq_failed_check", F.lit(None).cast("string"))
    )

    for col in _as_list(cfg.get("non_nullable_columns")):
        col_name = str(col).strip()
        if col_name and col_name in df.columns:
            failed = F.col(col_name).isNull()
            result = _append_failure(result, failed, f"non_null:{col_name}")

    for col, values in _as_dict(cfg.get("allowed_values")).items():
        col_name = str(col).strip()
        if col_name in df.columns and isinstance(values, list) and values:
            failed = F.col(col_name).isNotNull() & (~F.col(col_name).isin(values))
            result = _append_failure(result, failed, f"allowed_values:{col_name}")

    for col, pattern in _as_dict(cfg.get("regex_patterns")).items():
        col_name = str(col).strip()
        if col_name in df.columns and pattern:
            failed = F.col(col_name).isNotNull() & (~F.col(col_name).cast("string").rlike(str(pattern)))
            result = _append_failure(result, failed, f"regex:{col_name}")

    for col, spec in _as_dict(cfg.get("length_constraints")).items():
        col_name = str(col).strip()
        if col_name not in df.columns or not isinstance(spec, dict):
            continue

        min_len = spec.get("min")
        max_len = spec.get("max")

        failed = F.lit(False)
        str_col = F.col(col_name).cast("string")

        if min_len is not None:
            failed = failed | (F.col(col_name).isNotNull() & (F.length(str_col) < F.lit(int(min_len))))
        if max_len is not None:
            failed = failed | (F.col(col_name).isNotNull() & (F.length(str_col) > F.lit(int(max_len))))

        result = _append_failure(result, failed, f"length:{col_name}")

    for col, spec in _as_dict(cfg.get("range_constraints")).items():
        col_name = str(col).strip()
        if col_name not in df.columns or not isinstance(spec, dict):
            continue

        min_val = spec.get("min")
        max_val = spec.get("max")
        cast_col = F.col(col_name).cast("double")

        failed = F.lit(False)
        if min_val is not None:
            failed = failed | (F.col(col_name).isNotNull() & (cast_col < F.lit(float(min_val))))
        if max_val is not None:
            failed = failed | (F.col(col_name).isNotNull() & (cast_col > F.lit(float(max_val))))

        result = _append_failure(result, failed, f"range:{col_name}")

    for expr in _as_list(cfg.get("logical_checks")):
        expr_str = str(expr).strip()
        if not expr_str:
            continue
        failed = ~(F.expr(expr_str))
        result = _append_failure(result, failed, f"logical:{expr_str}")

    valid_df = result.filter(F.col("bronze_dq_status") == F.lit("PASS"))
    reject_df = result.filter(F.col("bronze_dq_status") == F.lit("FAIL"))
    return result, valid_df, reject_df


def dataset_quality_checks(df: DataFrame, cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    metrics = df.agg(
        F.count(F.lit(1)).alias("_total_rows"),
        F.sum(F.when(F.col("bronze_dq_status") == "FAIL", 1).otherwise(0)).alias("_failed_rows"),
    ).collect()[0]

    total_rows = int(metrics["_total_rows"] or 0)
    failed_rows = int(metrics["_failed_rows"] or 0)
    passed_rows = total_rows - failed_rows

    results.append(
        _result(
            check_name="row_level_summary",
            status="PASS" if failed_rows == 0 else "FAIL",
            details={
                "total_rows": total_rows,
                "passed_rows": passed_rows,
                "failed_rows": failed_rows,
            },
        )
    )

    null_thresholds = _as_dict(cfg.get("null_thresholds"))
    if null_thresholds:
        agg_exprs = []
        threshold_columns: list[str] = []
        threshold_values: dict[str, float] = {}

        for col, threshold in null_thresholds.items():
            col_name = str(col).strip()
            if col_name not in df.columns:
                continue
            threshold_columns.append(col_name)
            threshold_values[col_name] = float(threshold)
            agg_exprs.append(
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"null_count__{col_name}")
            )

        if agg_exprs:
            agg_row = df.agg(*agg_exprs).collect()[0]
            for col_name in threshold_columns:
                null_rows = int(agg_row[f"null_count__{col_name}"] or 0)
                ratio = (null_rows / total_rows) if total_rows else 0.0
                max_ratio = threshold_values[col_name]
                results.append(
                    _result(
                        check_name=f"null_threshold:{col_name}",
                        status="PASS" if ratio <= max_ratio else "FAIL",
                        details={
                            "null_rows": null_rows,
                            "ratio": ratio,
                            "max_ratio": max_ratio,
                        },
                    )
                )

    for entry in _as_list(cfg.get("uniqueness_constraints")):
        cols = entry if isinstance(entry, list) else [entry]
        col_names = [str(c).strip() for c in cols if str(c).strip() in df.columns]
        if not col_names:
            continue

        distinct_count = df.select(*col_names).distinct().count()
        duplicate_rows = max(total_rows - distinct_count, 0)

        results.append(
            _result(
                check_name=f"uniqueness:{','.join(col_names)}",
                status="PASS" if duplicate_rows == 0 else "FAIL",
                details={
                    "columns": col_names,
                    "duplicate_rows": duplicate_rows,
                },
            )
        )

    freshness = _as_dict(cfg.get("freshness"))
    freshness_col = str(freshness.get("column")).strip() if freshness.get("column") else None
    max_age_hours = freshness.get("max_age_hours")

    if freshness_col and freshness_col in df.columns and max_age_hours is not None:
        freshness_row = df.select(
            F.max(F.col(freshness_col)).alias("latest_ts")
        ).collect()[0]
        latest_ts = freshness_row["latest_ts"]

        age_hours = None
        if latest_ts is not None:
            age_hours_row = df.select(
                (
                    F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.lit(latest_ts))
                ).cast("double") / F.lit(3600.0)
            ).limit(1).collect()[0]
            age_hours = float(age_hours_row[0]) if age_hours_row[0] is not None else None

        max_age_hours_float = float(max_age_hours)
        status = "PASS" if age_hours is not None and age_hours <= max_age_hours_float else "FAIL"

        results.append(
            _result(
                check_name=f"freshness:{freshness_col}",
                status=status,
                details={
                    "latest_ts": latest_ts.isoformat() if latest_ts is not None and hasattr(latest_ts, "isoformat") else latest_ts,
                    "age_hours": age_hours,
                    "max_age_hours": max_age_hours_float,
                },
            )
        )

    volume = _as_dict(cfg.get("volume"))
    min_rows = volume.get("min_rows")
    max_rows = volume.get("max_rows")

    if min_rows is not None or max_rows is not None:
        status = True
        min_rows_int = int(min_rows) if min_rows is not None else None
        max_rows_int = int(max_rows) if max_rows is not None else None

        if min_rows_int is not None:
            status = status and total_rows >= min_rows_int
        if max_rows_int is not None:
            status = status and total_rows <= max_rows_int

        results.append(
            _result(
                check_name="volume",
                status="PASS" if status else "FAIL",
                details={
                    "row_count": total_rows,
                    "min_rows": min_rows_int,
                    "max_rows": max_rows_int,
                },
            )
        )

    return results


def run_bronze_dq_checks(
    df: DataFrame,
    source_options: Mapping[str, Any] | None,
) -> dict[str, Any]:
    cfg = bronze_dq_config(source_options)
    cfg_errors = validate_bronze_dq_config(cfg)

    if cfg_errors:
        annotated_df = (
            df.withColumn("bronze_dq_status", F.lit("FAIL"))
            .withColumn("bronze_dq_failed_check", F.lit("invalid_bronze_dq_config"))
        )
        valid_df = annotated_df.filter(F.lit(False))
        reject_df = annotated_df

        return {
            "config_errors": cfg_errors,
            "results": [
                _result(
                    check_name="bronze_dq_config",
                    status="FAIL",
                    details={"errors": cfg_errors},
                )
            ],
            "annotated_df": annotated_df,
            "valid_df": valid_df,
            "reject_df": reject_df,
        }

    logger.info("Running bronze DQ checks")

    schema_results = dataset_schema_checks(df, cfg)
    annotated_df, valid_df, reject_df = row_level_checks(df, cfg)
    dataset_results = dataset_quality_checks(annotated_df, cfg)

    return {
        "config_errors": [],
        "results": schema_results + dataset_results,
        "annotated_df": annotated_df,
        "valid_df": valid_df,
        "reject_df": reject_df,
    }