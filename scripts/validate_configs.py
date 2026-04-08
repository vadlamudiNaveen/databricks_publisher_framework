"""Validate control CSV files for required headers and basic values."""

from __future__ import annotations

import csv
import json
from pathlib import Path

REQUIRED_HEADERS = {
    "source_registry.csv": {
        "tenant",
        "brand",
        "product_name",
        "source_system",
        "source_entity",
        "source_type",
        "load_type",
        "landing_table",
        "conformance_table",
        "silver_table",
        "publish_mode",
        "is_active",
    },
    "column_mapping.csv": {
        "tenant",
        "brand",
        "product_name",
        "source_system",
        "source_entity",
        "conformance_column",
        "transform_expression",
        "is_active",
    },
    "dq_rules.csv": {
        "tenant",
        "brand",
        "product_name",
        "source_system",
        "source_entity",
        "rule_name",
        "rule_expression",
        "is_active",
    },
    "publish_rules.csv": {
        "tenant",
        "brand",
        "product_name",
        "source_system",
        "source_entity",
        "publish_mode",
        "silver_table",
    },
}


def validate_csv_headers(config_dir: Path) -> list[str]:
    errors: list[str] = []

    for file_name, required in REQUIRED_HEADERS.items():
        file_path = config_dir / file_name
        if not file_path.exists():
            errors.append(f"Missing config file: {file_name}")
            continue

        with file_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = set(reader.fieldnames or [])
            missing = sorted(required - header)
            if missing:
                errors.append(f"{file_name}: missing headers {missing}")

            for index, row in enumerate(reader, start=2):
                if file_name == "source_registry.csv":
                    if row.get("is_active", "").lower() not in {"true", "false"}:
                        errors.append(f"{file_name}:{index}: is_active must be true|false")
                    if row.get("source_type", "").upper() not in {"FILE", "JDBC", "API", "CUSTOM"}:
                        errors.append(f"{file_name}:{index}: source_type must be FILE|JDBC|API|CUSTOM")
                    if row.get("load_type", "").lower() not in {"full", "incremental"}:
                        errors.append(f"{file_name}:{index}: load_type must be full|incremental")
                    if row.get("publish_mode", "").lower() not in {"append", "merge"}:
                        errors.append(f"{file_name}:{index}: publish_mode must be append|merge")

                    for layer in ["landing", "conformance", "silver"]:
                        table_type = row.get(f"{layer}_table_type", "managed").strip().lower() or "managed"
                        if table_type not in {"managed", "external"}:
                            errors.append(
                                f"{file_name}:{index}: {layer}_table_type must be managed|external"
                            )
                        table_path = row.get(f"{layer}_table_path", "").strip()
                        if table_type == "external" and not table_path:
                            errors.append(
                                f"{file_name}:{index}: {layer}_table_path is required when {layer}_table_type=external"
                            )
                    if row.get("source_type", "").upper() == "FILE" and not row.get("source_path", "").strip():
                        errors.append(f"{file_name}:{index}: source_path is required for FILE sources")
                    if row.get("source_type", "").upper() == "JDBC" and not row.get("jdbc_table", "").strip():
                        errors.append(f"{file_name}:{index}: jdbc_table is required for JDBC sources")
                    if row.get("source_type", "").upper() == "API" and not row.get("api_endpoint", "").strip():
                        errors.append(f"{file_name}:{index}: api_endpoint is required for API sources")
                    source_options = row.get("source_options_json", "").strip()
                    if source_options:
                        try:
                            parsed_options = json.loads(source_options)
                            if not isinstance(parsed_options, dict):
                                errors.append(f"{file_name}:{index}: source_options_json must be a JSON object")
                        except json.JSONDecodeError as ex:
                            errors.append(f"{file_name}:{index}: invalid source_options_json ({ex.msg})")
                if file_name == "publish_rules.csv" and row.get("publish_mode", "").lower() not in {"append", "merge"}:
                    errors.append(f"{file_name}:{index}: publish_mode must be append|merge")
                if file_name == "publish_rules.csv":
                    mode = row.get("publish_mode", "").lower()
                    if mode == "merge" and not row.get("merge_key", "").strip():
                        errors.append(f"{file_name}:{index}: merge_key is required when publish_mode=merge")
                    for field_name in ["optimize_zorder_json", "partition_columns_json"]:
                        value = row.get(field_name, "").strip()
                        if not value:
                            continue
                        try:
                            parsed = json.loads(value)
                            if not isinstance(parsed, list):
                                errors.append(f"{file_name}:{index}: {field_name} must be a JSON array")
                        except json.JSONDecodeError as ex:
                            errors.append(f"{file_name}:{index}: invalid {field_name} ({ex.msg})")

    return errors


def main() -> int:
    config_dir = Path(__file__).resolve().parents[1] / "config"
    errors = validate_csv_headers(config_dir)
    if errors:
        for e in errors:
            print(f"ERROR: {e}")
        return 1
    print("Config validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
