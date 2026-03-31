"""Tests for ingest_file_batch.py and ingest_file_autoloader.py — pure-Python parts."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "01_ingestion"))

from ingest_file_batch import _FORMAT_MAP, _reader_format
from ingest_file_autoloader import _build_tracking_paths, build_autoloader_options


# ─── _reader_format ──────────────────────────────────────────────────────────


def _global_cfg(fmt="binaryFile"):
    return {"connections": {"file_defaults": {"format": fmt}}}


def test_reader_format_json():
    assert _reader_format({"source_format": "json"}, _global_cfg()) == "json"


def test_reader_format_jsonl_maps_to_json():
    assert _reader_format({"source_format": "jsonl"}, _global_cfg()) == "json"


def test_reader_format_mixed_maps_to_binary():
    assert _reader_format({"source_format": "json_jsonl_mixed"}, _global_cfg()) == "binaryFile"


def test_reader_format_parquet():
    assert _reader_format({"source_format": "parquet"}, _global_cfg()) == "parquet"


def test_reader_format_csv():
    assert _reader_format({"source_format": "csv"}, _global_cfg()) == "csv"


def test_reader_format_falls_back_to_global_default():
    assert _reader_format({"source_format": ""}, _global_cfg("json")) == "json"


def test_reader_format_unknown_falls_back_to_global_default():
    assert _reader_format({"source_format": "xlsx"}, _global_cfg("binaryFile")) == "binaryFile"


def test_format_map_covers_all_common_formats():
    for fmt in ("json", "jsonl", "binaryFile", "json_jsonl_mixed", "csv", "parquet", "avro", "orc", "text"):
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
    schema_loc, checkpoint_loc = _build_tracking_paths(_tracking_cfg(), _tracking_source())
    assert schema_loc == "abfss://bronze@storage/schema/ikea/connect/cemc/orders"
    assert checkpoint_loc == "abfss://bronze@storage/checkpoints/ikea/connect/cemc/orders"


def test_tracking_paths_raises_on_missing_source_field():
    source = {**_tracking_source(), "tenant": ""}
    with pytest.raises(ValueError, match="Missing tracking path configuration"):
        _build_tracking_paths(_tracking_cfg(), source)


def test_tracking_paths_raises_on_missing_config():
    with pytest.raises(ValueError, match="Missing tracking path configuration"):
        _build_tracking_paths({}, _tracking_source())


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
    opts = build_autoloader_options(_autoloader_source(), _autoloader_global_cfg())
    assert opts["cloudFiles.format"] == "json"
    assert opts["cloudFiles.inferColumnTypes"] == "true"
    assert opts["path"] == "abfss://raw@storage/orders/"
    assert "checkpointLocation" in opts
    assert "cloudFiles.schemaLocation" in opts


def test_build_autoloader_options_source_format_overrides_default():
    source = {**_autoloader_source(), "source_format": "parquet"}
    opts = build_autoloader_options(source, _autoloader_global_cfg())
    assert opts["cloudFiles.format"] == "parquet"


def test_build_autoloader_options_raises_on_missing_source_path():
    source = {**_autoloader_source(), "source_path": ""}
    with pytest.raises(ValueError, match="source_path is required"):
        build_autoloader_options(source, _autoloader_global_cfg())


def test_build_autoloader_options_raises_on_missing_file_defaults():
    cfg = {**_tracking_cfg()}  # No "connections" key at all.
    with pytest.raises(ValueError, match="connections.file_defaults must be configured"):
        build_autoloader_options(_autoloader_source(), cfg)


def test_build_autoloader_options_raises_on_incomplete_file_defaults():
    cfg = {
        **_tracking_cfg(),
        "connections": {"file_defaults": {"format": "json"}},  # missing 2 required keys
    }
    with pytest.raises(ValueError, match="file_defaults must define"):
        build_autoloader_options(_autoloader_source(), cfg)


def test_build_autoloader_options_source_options_override():
    opts = build_autoloader_options(
        _autoloader_source(),
        _autoloader_global_cfg(),
        source_options={"cloudFiles.maxFilesPerTrigger": "100"},
    )
    assert opts["cloudFiles.maxFilesPerTrigger"] == "100"
