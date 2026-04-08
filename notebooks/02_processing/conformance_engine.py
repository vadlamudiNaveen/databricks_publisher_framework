"""Production-oriented conformance engine driven by mapping metadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_FRAMEWORK_COLS = frozenset(
    {
        "source_system",
        "source_entity",
        "load_id",
        "ingest_ts",
        "bronze_dq_status",
        "bronze_dq_failed_check",
        "dq_status",
        "dq_failed_rule",
    }
)

_REQUIRED_MAPPING_KEYS = {"transform_expression", "conformance_column"}
_ALLOWED_WRITE_MODES = {"append", "overwrite", "error", "ignore"}
_ALLOWED_TABLE_TYPES = {"managed", "external"}


class ConformanceConfigError(ValueError):
    """Raised when conformance configuration is invalid."""


class ConformanceExecutionError(RuntimeError):
    """Raised when conformance execution fails."""


def _require_non_empty(value: Any, field_name: str) -> str:
    if value is None:
        raise ConformanceConfigError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise ConformanceConfigError(f"{field_name} is required")
    return text


def _normalize_bool(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "false"}:
            return text
    raise ConformanceConfigError(f"Expected boolean-compatible value, got: {value!r}")


@dataclass(frozen=True)
class MappingRule:
    transform_expression: str
    conformance_column: str
    ordinal: int
    source_column: str | None = None
    is_active: bool = True

    @classmethod
    def from_row(cls, row: Mapping[str, Any], ordinal: int) -> "MappingRule | None":
        is_active_raw = row.get("is_active", True)

        if isinstance(is_active_raw, str):
            is_active = is_active_raw.strip().lower() not in {"false", "0", "n", "no"}
        else:
            is_active = bool(is_active_raw)

        if not is_active:
            return None

        transform_expression = row.get("transform_expression")
        conformance_column = row.get("conformance_column")

        if transform_expression is None and conformance_column is None:
            return None

        if not transform_expression or not str(transform_expression).strip():
            raise ConformanceConfigError(
                f"mapping_rows[{ordinal}].transform_expression is required for active mappings"
            )

        if not conformance_column or not str(conformance_column).strip():
            raise ConformanceConfigError(
                f"mapping_rows[{ordinal}].conformance_column is required for active mappings"
            )

        return cls(
            transform_expression=str(transform_expression).strip(),
            conformance_column=str(conformance_column).strip(),
            ordinal=ordinal,
            source_column=str(row.get("source_column")).strip() if row.get("source_column") else None,
            is_active=True,
        )


def validate_mapping_rows(mapping_rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    seen_output_columns: set[str] = set()

    for idx, row in enumerate(mapping_rows):
        if not isinstance(row, dict):
            errors.append(f"mapping_rows[{idx}] must be an object")
            continue

        is_active_raw = row.get("is_active", True)
        if isinstance(is_active_raw, str):
            is_active = is_active_raw.strip().lower() not in {"false", "0", "n", "no"}
        else:
            is_active = bool(is_active_raw)

        if not is_active:
            continue

        missing = [key for key in _REQUIRED_MAPPING_KEYS if not row.get(key)]
        if missing:
            errors.append(f"mapping_rows[{idx}] missing required fields: {missing}")
            continue

        output_col = str(row["conformance_column"]).strip()
        if output_col in seen_output_columns:
            errors.append(f"Duplicate conformance_column found: {output_col}")
        else:
            seen_output_columns.add(output_col)

    return errors


def _build_mapping_rules(mapping_rows: list[dict[str, Any]]) -> list[MappingRule]:
    rules: list[MappingRule] = []

    for idx, row in enumerate(mapping_rows):
        if not isinstance(row, dict):
            raise ConformanceConfigError(f"mapping_rows[{idx}] must be an object")

        rule = MappingRule.from_row(row, ordinal=idx)
        if rule is not None:
            rules.append(rule)

    output_cols = [rule.conformance_column for rule in rules]
    duplicates = sorted({col for col in output_cols if output_cols.count(col) > 1})
    if duplicates:
        raise ConformanceConfigError(
            f"Duplicate conformance_column values are not allowed: {duplicates}"
        )

    return rules


def apply_column_mappings(
    df: DataFrame,
    mapping_rows: list[dict[str, Any]],
    passthrough_on_empty: bool = True,
    framework_cols: set[str] | frozenset[str] | None = None,
) -> DataFrame:
    framework_cols = framework_cols or _FRAMEWORK_COLS

    validation_errors = validate_mapping_rows(mapping_rows)
    if validation_errors:
        raise ConformanceConfigError(
            f"Invalid mapping metadata: {validation_errors}"
        )

    rules = _build_mapping_rules(mapping_rows)

    if not rules:
        if not passthrough_on_empty:
            raise ConformanceConfigError(
                "No active valid mapping rows found and passthrough_on_empty=False"
            )

        passthrough_cols = [c for c in df.columns if c not in framework_cols]

        logger.warning(
            "No active valid mapping rows found. Returning passthrough DataFrame with framework columns removed."
        )

        return df.select(*passthrough_cols) if passthrough_cols else df

    try:
        expressions = [
            F.expr(rule.transform_expression).alias(rule.conformance_column)
            for rule in rules
        ]
        result_df = df.select(*expressions)

        logger.info(
            "Applied %s conformance mapping rules",
            len(rules),
        )

        return result_df

    except Exception as exc:
        raise ConformanceExecutionError(
            "Conformance select failed. One or more transform_expression values are invalid. "
            f"Original error: {exc}"
        ) from exc


def write_conformance(
    df: DataFrame,
    table_name: str,
    table_type: str = "managed",
    external_path: str | None = None,
    mode: str = "append",
    merge_schema: bool = True,
) -> None:
    resolved_table_name = _require_non_empty(table_name, "table_name")
    resolved_table_type = _require_non_empty(table_type, "table_type").lower()
    resolved_mode = _require_non_empty(mode, "mode").lower()

    if resolved_table_type not in _ALLOWED_TABLE_TYPES:
        raise ConformanceConfigError(
            f"Unsupported table_type '{resolved_table_type}'. Supported values: {sorted(_ALLOWED_TABLE_TYPES)}"
        )
    resolved_external_path = (
        _require_non_empty(external_path, "external_path") if external_path else None
    )
    if resolved_table_type == "external" and not resolved_external_path:
        raise ConformanceConfigError("external_path is required when table_type=external")

    if resolved_mode not in _ALLOWED_WRITE_MODES:
        raise ConformanceConfigError(
            f"Unsupported write mode '{resolved_mode}'. Supported modes: {sorted(_ALLOWED_WRITE_MODES)}"
        )

    try:
        writer = (
            df.write.mode(resolved_mode)
            .format("delta")
            .option("mergeSchema", _normalize_bool(merge_schema))
        )
        if resolved_table_type == "external" and resolved_external_path:
            writer = writer.option("path", resolved_external_path)
        writer.saveAsTable(resolved_table_name)

        logger.info(
            "Wrote conformance DataFrame to table=%s mode=%s mergeSchema=%s",
            resolved_table_name,
            resolved_mode,
            merge_schema,
        )

    except Exception as exc:
        raise ConformanceExecutionError(
            f"Failed to write conformance DataFrame to table={resolved_table_name}: {exc}"
        ) from exc