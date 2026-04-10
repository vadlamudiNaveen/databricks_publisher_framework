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

@pytest.mark.skip(reason="Requires Spark context; run in Databricks environment")
def test_dataset_schema_checks_required_and_types(spark_session):
    df = spark_session.createDataFrame([("a_val", 1)], schema="a string, b int")
    cfg = {"required_columns": ["a", "c"], "expected_types": {"a": "string", "b": "int", "c": "int"}}
    results = dq.dataset_schema_checks(df, cfg)
    assert len(results) >= 1
    assert any(r["status"] == "FAIL" for r in results)  # Missing column "c" should fail

@pytest.mark.skip(reason="Requires Spark context; run in Databricks environment")
def test_row_level_checks_non_nullable_and_allowed_values(spark_session):
    df = spark_session.createDataFrame([("val", 1), (None, 2)], schema="x string, y int")
    cfg = {"non_nullable_columns": ["x"], "allowed_values": {"y": [1, 2]}}
    result = dq.row_level_checks(df, cfg)
    assert isinstance(result, tuple) and len(result) == 3

@pytest.mark.skip(reason="Requires Spark context; run in Databricks environment")
def test_dataset_quality_checks_null_threshold_and_volume(spark_session):
    df = spark_session.createDataFrame([(1,), (None,), (3,)], schema="a int")
    cfg = {"null_thresholds": {"a": 0.5}, "volume": {"min_rows": 1, "max_rows": 10}}
    results = dq.dataset_quality_checks(df, cfg)
    assert len(results) >= 1

@pytest.mark.skip(reason="Requires Spark context; run in Databricks environment")
def test_run_bronze_dq_checks_integration(spark_session):
    df = spark_session.createDataFrame([("val",)], schema="a string")
    cfg = {"required_columns": ["a"]}
    opts = {"bronze_dq": cfg}
    result = dq.run_bronze_dq_checks(df, opts)
    assert "config_errors" in result
    assert "results" in result
    assert "annotated_df" in result
    assert "valid_df" in result
    assert "reject_df" in result
