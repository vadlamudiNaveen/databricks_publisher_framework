"""Tests for bronze_dq_engine.py (pure-Python config and logic)."""
from unittest.mock import MagicMock
import pytest

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "02_processing"))

import bronze_dq_engine as dq

def test_bronze_dq_config_none():
    assert dq.bronze_dq_config(None) == {}

def test_bronze_dq_config_dict():
    opts = {"bronze_dq": {"foo": 1}}
    assert dq.bronze_dq_config(opts) == {"foo": 1}

def test_validate_bronze_dq_config_types():
    cfg = {
        "required_columns": "notalist",
        "expected_types": [],
        "uniqueness_constraints": {},
        "freshness": [],
        "volume": [],
        "null_thresholds": {"col": "notanumber"},
    }
    errors = dq.validate_bronze_dq_config(cfg)
    assert any("must be a list" in e for e in errors)
    assert any("must be an object" in e for e in errors)
    assert any("must be numeric" in e for e in errors)

def test_validate_bronze_dq_config_null_threshold_range():
    cfg = {"null_thresholds": {"col": 1.5}}
    errors = dq.validate_bronze_dq_config(cfg)
    assert any("between 0 and 1" in e for e in errors)

def test_dataset_schema_checks_required_and_types():
    df = MagicMock()
    df.columns = ["a", "b"]
    df.schema.fields = [MagicMock(name="a", dataType=MagicMock(simpleString=lambda: "string")),
                       MagicMock(name="b", dataType=MagicMock(simpleString=lambda: "int"))]
    cfg = {"required_columns": ["a", "c"], "expected_types": {"a": "string", "b": "int", "c": "int"}}
    results = dq.dataset_schema_checks(df, cfg)
    assert results[0]["status"] == "FAIL"
    assert results[2]["status"] == "PASS"

def test_row_level_checks_non_nullable_and_allowed_values():
    df = MagicMock()
    df.columns = ["x", "y"]
    # Only check that the function runs and returns 3 values (annotated, valid, reject)
    cfg = {"non_nullable_columns": ["x"], "allowed_values": {"y": [1, 2]}}
    result = dq.row_level_checks(df, cfg)
    assert isinstance(result, tuple) and len(result) == 3

def test_dataset_quality_checks_null_threshold_and_volume():
    df = MagicMock()
    df.columns = ["a"]
    df.count.return_value = 10
    df.filter.return_value.count.return_value = 2
    cfg = {"null_thresholds": {"a": 0.5}, "volume": {"min_rows": 5, "max_rows": 20}}
    results = dq.dataset_quality_checks(df, cfg)
    assert any(r["check_name"].startswith("null_threshold") for r in results)
    assert any(r["check_name"] == "volume" for r in results)

def test_run_bronze_dq_checks_integration():
    df = MagicMock()
    df.columns = ["a"]
    df.count.return_value = 1
    df.filter.return_value.count.return_value = 0
    df.select.return_value = df
    df.distinct.return_value = df
    df.collect.return_value = [{"latest_ts": None}]
    df.schema.fields = []
    cfg = {"required_columns": ["a"]}
    opts = {"bronze_dq": cfg}
    result = dq.run_bronze_dq_checks(df, opts)
    assert "config_errors" in result
    assert "results" in result
    assert "annotated_df" in result
    assert "valid_df" in result
    assert "reject_df" in result
