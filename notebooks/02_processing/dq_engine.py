"""Production-oriented data quality engine driven by rule metadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

logger = logging.getLogger(__name__)

_ALLOWED_RULE_SEVERITIES = {"ERROR", "WARN"}
_ALLOWED_RULE_STATUSES = {"PASS", "FAIL"}
_REQUIRED_RULE_KEYS = {"rule_expression", "rule_name"}


class DQConfigError(ValueError):
    """Raised when DQ rule configuration is invalid."""


class DQExecutionError(RuntimeError):
    """Raised when DQ rule execution fails."""


def _require_non_empty(value: Any, field_name: str) -> str:
    if value is None:
        raise DQConfigError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise DQConfigError(f"{field_name} is required")
    return text


@dataclass(frozen=True)
class DQRule:
    rule_name: str
    rule_expression: str
    severity: str
    is_active: bool
    ordinal: int

    @classmethod
    def from_row(cls, row: Mapping[str, Any], ordinal: int) -> "DQRule | None":
        is_active_raw = row.get("is_active", True)

        if isinstance(is_active_raw, str):
            is_active = is_active_raw.strip().lower() not in {"false", "0", "n", "no"}
        else:
            is_active = bool(is_active_raw)

        if not is_active:
            return None

        rule_expression = row.get("rule_expression")
        rule_name = row.get("rule_name")

        if rule_expression is None and rule_name is None:
            return None

        rule_name_text = _require_non_empty(rule_name, f"dq_rows[{ordinal}].rule_name")
        rule_expression_text = _require_non_empty(
            rule_expression,
            f"dq_rows[{ordinal}].rule_expression",
        )

        severity = str(row.get("severity", "ERROR")).strip().upper()
        if severity not in _ALLOWED_RULE_SEVERITIES:
            raise DQConfigError(
                f"dq_rows[{ordinal}].severity must be one of {sorted(_ALLOWED_RULE_SEVERITIES)}"
            )

        return cls(
            rule_name=rule_name_text,
            rule_expression=rule_expression_text,
            severity=severity,
            is_active=True,
            ordinal=ordinal,
        )


def validate_dq_rows(dq_rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    seen_rule_names: set[str] = set()

    for idx, row in enumerate(dq_rows):
        if not isinstance(row, dict):
            errors.append(f"dq_rows[{idx}] must be an object")
            continue

        is_active_raw = row.get("is_active", True)
        if isinstance(is_active_raw, str):
            is_active = is_active_raw.strip().lower() not in {"false", "0", "n", "no"}
        else:
            is_active = bool(is_active_raw)

        if not is_active:
            continue

        missing = [key for key in _REQUIRED_RULE_KEYS if not row.get(key)]
        if missing:
            errors.append(f"dq_rows[{idx}] missing required fields: {missing}")
            continue

        rule_name = str(row["rule_name"]).strip()
        if rule_name in seen_rule_names:
            errors.append(f"Duplicate rule_name found: {rule_name}")
        else:
            seen_rule_names.add(rule_name)

        severity = str(row.get("severity", "ERROR")).strip().upper()
        if severity not in _ALLOWED_RULE_SEVERITIES:
            errors.append(
                f"dq_rows[{idx}].severity must be one of {sorted(_ALLOWED_RULE_SEVERITIES)}"
            )

    return errors


def _build_rules(dq_rows: list[dict[str, Any]]) -> list[DQRule]:
    rules: list[DQRule] = []

    for idx, row in enumerate(dq_rows):
        if not isinstance(row, dict):
            raise DQConfigError(f"dq_rows[{idx}] must be an object")

        rule = DQRule.from_row(row, ordinal=idx)
        if rule is not None:
            rules.append(rule)

    rule_names = [rule.rule_name for rule in rules]
    duplicates = sorted({name for name in rule_names if rule_names.count(name) > 1})
    if duplicates:
        raise DQConfigError(f"Duplicate rule_name values are not allowed: {duplicates}")

    return rules


def _append_failed_rule_name(
    current_failed_rule_col,
    failed_condition,
    rule_name: str,
):
    return F.when(
        failed_condition,
        F.when(current_failed_rule_col.isNull(), F.lit(rule_name)).otherwise(
            F.concat(current_failed_rule_col, F.lit(","), F.lit(rule_name))
        ),
    ).otherwise(current_failed_rule_col)


def _primary_key_columns(primary_key: str | None) -> list[str]:
    if not primary_key:
        return []
    cols = [c.strip() for c in str(primary_key).split(",") if c.strip()]
    # Preserve order while de-duplicating.
    return list(dict.fromkeys(cols))


def apply_dq_rules(
    df: DataFrame,
    dq_rows: list[dict[str, Any]],
    primary_key: str | None = None,
) -> DataFrame:
    validation_errors = validate_dq_rows(dq_rows)
    if validation_errors:
        raise DQConfigError(f"Invalid DQ rule metadata: {validation_errors}")

    rules = _build_rules(dq_rows)

    if not rules:
        logger.warning("No active valid DQ rules found. Returning DataFrame with default PASS status.")
        return (
            df.withColumn("dq_status", F.lit("PASS"))
            .withColumn("dq_failed_rule", F.lit(None).cast("string"))
            .withColumn("dq_warn_rule", F.lit(None).cast("string"))
        )

    result = (
        df.withColumn("dq_status", F.lit("PASS"))
        .withColumn("dq_failed_rule", F.lit(None).cast("string"))
        .withColumn("dq_warn_rule", F.lit(None).cast("string"))
    )

    try:
        pk_cols = _primary_key_columns(primary_key)
        if pk_cols:
            missing_pk_cols = [c for c in pk_cols if c not in result.columns]
            if missing_pk_cols:
                raise DQConfigError(
                    f"primary_key columns not found in DataFrame: {missing_pk_cols}"
                )

            # PK non-null check (all PK columns must be non-null).
            pk_null_failed = F.lit(False)
            for c in pk_cols:
                pk_null_failed = pk_null_failed | F.col(c).isNull()

            result = (
                result.withColumn(
                    "dq_status",
                    F.when(pk_null_failed, F.lit("FAIL")).otherwise(F.col("dq_status")),
                )
                .withColumn(
                    "dq_failed_rule",
                    _append_failed_rule_name(
                        F.col("dq_failed_rule"),
                        pk_null_failed,
                        "primary_key_not_null",
                    ),
                )
            )

            # PK uniqueness check (composite-safe).
            dup_window = Window.partitionBy(*[F.col(c) for c in pk_cols])
            result = result.withColumn("__pk_dup_count", F.count(F.lit(1)).over(dup_window))
            pk_unique_failed = F.col("__pk_dup_count") > F.lit(1)

            result = (
                result.withColumn(
                    "dq_status",
                    F.when(pk_unique_failed, F.lit("FAIL")).otherwise(F.col("dq_status")),
                )
                .withColumn(
                    "dq_failed_rule",
                    _append_failed_rule_name(
                        F.col("dq_failed_rule"),
                        pk_unique_failed,
                        "primary_key_unique",
                    ),
                )
                .drop("__pk_dup_count")
            )

        for rule in rules:
            failed_condition = ~(F.expr(rule.rule_expression))

            if rule.severity == "ERROR":
                result = (
                    result.withColumn(
                        "dq_status",
                        F.when(failed_condition, F.lit("FAIL")).otherwise(F.col("dq_status")),
                    )
                    .withColumn(
                        "dq_failed_rule",
                        _append_failed_rule_name(
                            F.col("dq_failed_rule"),
                            failed_condition,
                            rule.rule_name,
                        ),
                    )
                )
            elif rule.severity == "WARN":
                result = result.withColumn(
                    "dq_warn_rule",
                    _append_failed_rule_name(
                        F.col("dq_warn_rule"),
                        failed_condition,
                        rule.rule_name,
                    ),
                )

        logger.info("Applied %s DQ rules", len(rules))
        return result

    except Exception as exc:
        raise DQExecutionError(
            f"Failed to apply DQ rules. One or more rule_expression values may be invalid. Original error: {exc}"
        ) from exc


def split_valid_reject(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    required_cols = {"dq_status"}
    missing = required_cols - set(df.columns)
    if missing:
        raise DQConfigError(
            f"Cannot split valid/reject DataFrame because required columns are missing: {sorted(missing)}"
        )

    valid_df = df.filter(F.col("dq_status") == F.lit("PASS"))
    reject_df = df.filter(F.col("dq_status") == F.lit("FAIL"))
    return valid_df, reject_df