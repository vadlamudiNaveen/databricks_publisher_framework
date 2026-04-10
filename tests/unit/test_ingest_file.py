"""Tests for ingest_file_batch.py and ingest_file_autoloader.py — pure-Python parts."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "01_ingestion"))

from ingest_file_batch import _FORMAT_MAP, resolve_batch_file_config
from ingest_file_autoloader import _build_tracking_paths, resolve_autoloader_config


# ─── resolve_batch_file_config ───────────────────────────────────────────────


def _global_cfg(fmt="binaryFile"):
    return {"connections": {"file_defaults": {"format": fmt}}}


def test_resolve_batch_file_config_json():
    resolved = resolve_batch_file_config(
        {"source_path": "abfss://raw@storage/a", "source_format": "json", "load_type": "incremental"},
        _global_cfg(),
    )
    assert resolved.source.spark_format == "json"


def test_resolve_batch_file_config_jsonl_maps_to_json():
    resolved = resolve_batch_file_config(
        {"source_path": "abfss://raw@storage/a", "source_format": "jsonl", "load_type": "incremental"},
        _global_cfg(),
    )
    assert resolved.source.spark_format == "json"


def test_resolve_batch_file_config_mixed_maps_to_binary():
    resolved = resolve_batch_file_config(
        {"source_path": "abfss://raw@storage/a", "source_format": "json_jsonl_mixed", "load_type": "incremental"},
        _global_cfg(),
    )
    assert resolved.source.spark_format == "binaryFile"


def test_resolve_batch_file_config_parquet():
    resolved = resolve_batch_file_config(
        {"source_path": "abfss://raw@storage/a", "source_format": "parquet", "load_type": "incremental"},
        _global_cfg(),
    )
    assert resolved.source.spark_format == "parquet"


def test_resolve_batch_file_config_csv():
    resolved = resolve_batch_file_config(
        {"source_path": "abfss://raw@storage/a", "source_format": "csv", "load_type": "incremental"},
        _global_cfg(),
    )
    assert resolved.source.spark_format == "csv"


def test_resolve_batch_file_config_falls_back_to_global_default():
    resolved = resolve_batch_file_config(
        {"source_path": "abfss://raw@storage/a", "source_format": "", "load_type": "incremental"},
        _global_cfg("json"),
    )
    assert resolved.source.spark_format == "json"


def test_resolve_batch_file_config_unknown_raises():
    with pytest.raises(ValueError, match="Unsupported source_format"):
        resolve_batch_file_config(
            {"source_path": "abfss://raw@storage/a", "source_format": "xlsx", "load_type": "incremental"},
            _global_cfg("binaryFile"),
        )


def test_format_map_covers_all_common_formats():
    # _FORMAT_MAP uses lowercase keys (normalized format names)
    for fmt in ("json", "jsonl", "binaryfile", "json_jsonl_mixed", "csv", "parquet", "avro", "orc", "text"):
        assert fmt in _FORMAT_MAP


# ─── _build_tracking_paths ───────────────────────────────────────────────────


def _tracking_cfg():
    return {
        "databricks": {
            "checkpoint_root": "abfss://bronze@storage/checkpoints",
            "schema_tracking_root": "abfss://bronze@storage/schema",
        }
    }


def _tracking_source():
    return {
        "tenant": "ikea",
        "product_name": "connect",
        "source_system": "cemc",
        "source_entity": "orders",
    }


def test_tracking_paths_happy_path():
    from ingest_file_autoloader import DatabricksConfig, SourceConfig as AutoLoaderSourceConfig
    dbx_cfg = DatabricksConfig.from_global_config(_tracking_cfg())
    source_cfg = AutoLoaderSourceConfig.from_dict({**_tracking_source(), "source_path": "abfss://raw@storage/data"})
    schema_loc, checkpoint_loc = _build_tracking_paths(dbx_cfg, source_cfg)
    assert schema_loc == "abfss://bronze@storage/schema/ikea/connect/cemc/orders"
    assert checkpoint_loc == "abfss://bronze@storage/checkpoints/ikea/connect/cemc/orders"


def test_tracking_paths_raises_on_missing_source_field():
    from ingest_file_autoloader import DatabricksConfig, SourceConfig as AutoLoaderSourceConfig
    dbx_cfg = DatabricksConfig.from_global_config(_tracking_cfg())
    # tenant field is required for tracking_suffix - test that it raises
    with pytest.raises((ValueError, KeyError), match="tenant|Missing tracking path"):
        AutoLoaderSourceConfig.from_dict({**_tracking_source(), "tenant": "", "source_path": "abfss://raw@storage/data"})


def test_tracking_paths_raises_on_missing_config():
    from ingest_file_autoloader import DatabricksConfig
    # DatabricksConfig requires schema_tracking_root and checkpoint_root from global config
    with pytest.raises((ValueError, KeyError), match="scheme_tracking_root|checkpoint_root|Missing"):
        DatabricksConfig.from_global_config({})


# ─── build_autoloader_options ────────────────────────────────────────────────


def _autoloader_global_cfg():
    return {
        **_tracking_cfg(),
        "connections": {
            "file_defaults": {
                "format": "json",
                "infer_column_types": "true",
                "schema_evolution_mode": "addNewColumns",
            }
        },
    }


def _autoloader_source():
    return {
        **_tracking_source(),
        "source_path": "abfss://raw@storage/orders/",
    }


def test_build_autoloader_options_happy_path():
    resolved = resolve_autoloader_config(_autoloader_source(), _autoloader_global_cfg())
    assert resolved.read_options["cloudFiles.format"] == "json"
    assert resolved.read_options["cloudFiles.inferColumnTypes"] == "true"
    assert resolved.source.source_path == "abfss://raw@storage/orders/"
    assert "checkpointLocation" in resolved.write_options
    assert "cloudFiles.schemaLocation" in resolved.read_options


def test_build_autoloader_options_source_format_overrides_default():
    source = {**_autoloader_source(), "source_format": "parquet"}
    resolved = resolve_autoloader_config(source, _autoloader_global_cfg())
    assert resolved.read_options["cloudFiles.format"] == "parquet"


def test_build_autoloader_options_raises_on_missing_source_path():
    source = {**_autoloader_source(), "source_path": ""}
    with pytest.raises(ValueError, match="source.source_path is required"):
        resolve_autoloader_config(source, _autoloader_global_cfg())


def test_build_autoloader_options_raises_on_missing_file_defaults():
    cfg = {**_tracking_cfg()}  # No "connections" key at all.
    with pytest.raises(ValueError, match="connections.file_defaults must be configured"):
        resolve_autoloader_config(_autoloader_source(), cfg)


def test_build_autoloader_options_raises_on_incomplete_file_defaults():
    cfg = {
        **_tracking_cfg(),
        "connections": {"file_defaults": {"format": "json"}},  # missing required keys
    }
    with pytest.raises(ValueError, match="schema_evolution_mode is required"):
        resolve_autoloader_config(_autoloader_source(), cfg)


def test_build_autoloader_options_source_options_override():
    resolved = resolve_autoloader_config(
        _autoloader_source(),
        _autoloader_global_cfg(),
        source_options={"cloudFiles.maxFilesPerTrigger": "100"},
    )
    assert resolved.read_options["cloudFiles.maxFilesPerTrigger"] == "100"
