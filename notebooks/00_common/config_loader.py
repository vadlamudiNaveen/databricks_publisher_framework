"""Control metadata loaders for CSV or Databricks table-backed configuration."""

from __future__ import annotations

import csv
import json
import os
import re
import threading
import warnings
from pathlib import Path
from typing import Any

# Supports both ${VAR} and ${VAR:default} — consistent with global_config.py.
_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::([^}]*))?\}")

# Module-level cache: absolute CSV path → resolved rows.
# Avoids re-reading the same file on every per-entity metadata call.
# _CSV_LOCK serialises concurrent reads/writes so the cache is safe when
# multiple threads call load_csv_config() at the same time.
_CSV_CACHE: dict[str, list[dict[str, str]]] = {}
_CSV_LOCK = threading.Lock()


def clear_config_cache() -> None:
    """Clear the in-memory CSV row cache.

    Useful in tests (call in setUp/teardown) or when you need hot-reload
    of config files without restarting the Python process.
    """
    with _CSV_LOCK:
        _CSV_CACHE.clear()


def _substitute_env(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        default_value = match.group(2) if match.group(2) is not None else ""
        return os.getenv(key, default_value)

    return _ENV_PATTERN.sub(repl, value)


def _resolve_row_env(row: dict[str, str]) -> dict[str, str]:
    return {k: _substitute_env(v) if isinstance(v, str) else v for k, v in row.items()}


def load_csv_config(file_path: str) -> list[dict[str, str]]:
    abs_path = str(Path(file_path).resolve())
    with _CSV_LOCK:
        if abs_path in _CSV_CACHE:
            return _CSV_CACHE[abs_path]
    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            rows = [_resolve_row_env(row) for row in csv.DictReader(f)]
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Framework control CSV not found: {abs_path}. "
            "Ensure the file exists in your config directory."
        ) from None
    with _CSV_LOCK:
        _CSV_CACHE[abs_path] = rows
    return rows

def get_lifecycle_log_table(config: dict) -> str:
    return config.get("lifecycle_log_table", "audit.entity_lifecycle_log")


def _to_row_dicts_from_table(spark, table_name: str) -> list[dict[str, str]]:
    rows = spark.table(table_name).collect()
    out: list[dict[str, str]] = []
    for row in rows:
        out.append({k: "" if v is None else str(v) for k, v in row.asDict().items()})
    return out


def _metadata_mode(global_config: dict[str, Any] | None) -> str:
    return str((global_config or {}).get("metadata", {}).get("mode", "csv")).lower()


def _metadata_csv_root(config_dir: str, global_config: dict[str, Any] | None) -> Path:
    csv_root = (global_config or {}).get("metadata", {}).get("csv_root", "")
    if csv_root:
        return Path(config_dir).resolve().parent / str(csv_root)
    return Path(config_dir)


def _table_name_for(global_config: dict[str, Any], key: str) -> str:
    return str(global_config.get("metadata", {}).get("tables", {}).get(key, ""))


def load_control_rows(
    config_dir: str,
    control_name: str,
    spark=None,
    global_config: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    mode = _metadata_mode(global_config)

    if mode == "table":
        if spark is None:
            raise ValueError("Metadata mode is table but spark session is not provided")
        table_name = _table_name_for(global_config or {}, control_name)
        if not table_name:
            raise ValueError(f"Missing table name config for {control_name}")
        return _to_row_dicts_from_table(spark, table_name)

    csv_root = _metadata_csv_root(config_dir, global_config)
    csv_path = csv_root / f"{control_name}.csv"
    return load_csv_config(str(csv_path))


def _active_flag(row: dict[str, str]) -> bool:
    return row.get("is_active", "").strip().lower() == "true"


def _matches_filters(row: dict[str, str], filters: dict[str, str | None]) -> bool:
    for key, value in filters.items():
        if value and row.get(key, "").strip().lower() != value.strip().lower():
            return False
    return True


def active_sources(
    config_dir: str,
    spark=None,
    global_config: dict[str, Any] | None = None,
    product_name: str | None = None,
    source_system: str | None = None,
    source_entity: str | None = None,
) -> list[dict[str, str]]:
    rows = load_control_rows(config_dir, "source_registry", spark=spark, global_config=global_config)
    return [
        r
        for r in rows
        if _active_flag(r)
        and _matches_filters(
            r,
            {
                "product_name": product_name,
                "source_system": source_system,
                "source_entity": source_entity,
            },
        )
    ]


def mappings_for_entity(
    config_dir: str,
    source_system: str,
    source_entity: str,
    product_name: str | None = None,
    spark=None,
    global_config: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    rows = load_control_rows(config_dir, "column_mapping", spark=spark, global_config=global_config)
    return [
        r
        for r in rows
        if _active_flag(r)
        and r.get("source_system", "").lower() == source_system.lower()
        and r.get("source_entity", "").lower() == source_entity.lower()
        and (not product_name or r.get("product_name", "").lower() == product_name.lower())
    ]


def dq_rules_for_entity(
    config_dir: str,
    source_system: str,
    source_entity: str,
    product_name: str | None = None,
    spark=None,
    global_config: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    rows = load_control_rows(config_dir, "dq_rules", spark=spark, global_config=global_config)
    return [
        r
        for r in rows
        if _active_flag(r)
        and r.get("source_system", "").lower() == source_system.lower()
        and r.get("source_entity", "").lower() == source_entity.lower()
        and (not product_name or r.get("product_name", "").lower() == product_name.lower())
    ]


def publish_rule_for_entity(
    config_dir: str,
    source_system: str,
    source_entity: str,
    product_name: str | None = None,
    spark=None,
    global_config: dict[str, Any] | None = None,
) -> dict[str, str] | None:
    rows = load_control_rows(config_dir, "publish_rules", spark=spark, global_config=global_config)
    for row in rows:
        if (
            _active_flag(row)
            and
            row.get("source_system", "").lower() == source_system.lower()
            and row.get("source_entity", "").lower() == source_entity.lower()
            and (not product_name or row.get("product_name", "").lower() == product_name.lower())
        ):
            return row
    return None


def parse_json_cell(value: str | None) -> dict[str, Any]:
    if not value or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
        warnings.warn(f"parse_json_cell: expected a JSON object, got {type(parsed).__name__}. Returning {{}}.")
    except json.JSONDecodeError as exc:
        warnings.warn(f"parse_json_cell: invalid JSON ({exc}). Value: {value!r:.120}. Returning {{}}.")
    return {}


def parse_json_list(value: str | None) -> list[Any]:
    if not value or not value.strip():
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
        warnings.warn(f"parse_json_list: expected a JSON array, got {type(parsed).__name__}. Returning [].")
    except json.JSONDecodeError as exc:
        warnings.warn(f"parse_json_list: invalid JSON ({exc}). Value: {value!r:.120}. Returning [].")
    return []
