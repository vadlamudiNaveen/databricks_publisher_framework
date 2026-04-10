"""Tests for conformance_engine and dq_engine using local PySpark."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "02_processing"))

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

from conformance_engine import apply_column_mappings, ConformanceConfigError
from dq_engine import apply_dq_rules, split_valid_reject, DQConfigError


@pytest.fixture(scope="module")
def spark():
    if sys.version_info >= (3, 12):
        pytest.skip("PySpark 3.5.x Spark tests are unsupported on Python 3.12+ in local mode")
    try:
        # SparkSession.builder is a classproperty; assigning to a typed variable
        # avoids the Pylance attribute-chain resolution error.
        builder: SparkSession.Builder = SparkSession.builder  # type: ignore[misc]
        return (
            builder
            .config("spark.master", "local[1]")
            .config("spark.app.name", "test_processing")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception:
        pytest.skip("PySpark/Java not available in this environment — skipping Spark tests")


# ─── apply_column_mappings ───────────────────────────────────────────────────


def test_apply_column_mappings_renames_column(spark):
    df = spark.createDataFrame([{"src_name": "Alice", "src_age": 30}])
    mappings = [
        {"transform_expression": "src_name", "conformance_column": "full_name", "is_active": "true"},
        {"transform_expression": "CAST(src_age AS STRING)", "conformance_column": "age_str", "is_active": "true"},
    ]
    result = apply_column_mappings(df, mappings)
    assert "full_name" in result.columns
    assert "age_str" in result.columns
    assert "src_name" not in result.columns


def test_apply_column_mappings_empty_mappings_warns_and_strips_framework_cols(spark, caplog):
    from pyspark.sql import functions as F

    df = (
        spark.createDataFrame([{"order_id": "1", "amount": 100.0}])
        .withColumn("bronze_dq_status", F.lit("PASS"))
        .withColumn("bronze_dq_failed_check", F.lit(None).cast("string"))
        .withColumn("source_system", F.lit("test"))
    )

    with caplog.at_level("WARNING"):
        result = apply_column_mappings(df, [])

    assert any("no active valid mapping rows found" in m.lower() for m in caplog.messages)
    # Framework technical columns must NOT be in the result.
    assert "bronze_dq_status" not in result.columns
    assert "source_system" not in result.columns
    # Source data columns should be preserved.
    assert "order_id" in result.columns


def test_apply_column_mappings_skips_rows_without_expression(spark):
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    mappings = [
        {"transform_expression": "a", "conformance_column": "col_a"},
        {"transform_expression": "", "conformance_column": "col_b"},   # invalid — skipped
        {"transform_expression": None, "conformance_column": "col_c"}, # invalid — skipped
    ]
    with pytest.raises(ConformanceConfigError):
        apply_column_mappings(df, mappings)


# ─── apply_dq_rules / split_valid_reject ────────────────────────────────────


def test_apply_dq_rules_marks_passing_rows(spark):
    df = spark.createDataFrame([{"id": 1, "amount": 100}])
    rules = [{"rule_name": "amount_positive", "rule_expression": "amount > 0"}]
    result = apply_dq_rules(df, rules)
    row = result.collect()[0]
    assert row["dq_status"] == "PASS"
    assert row["dq_failed_rule"] is None


def test_apply_dq_rules_marks_failing_rows(spark):
    df = spark.createDataFrame([{"id": 1, "amount": -5}])
    rules = [{"rule_name": "amount_positive", "rule_expression": "amount > 0"}]
    result = apply_dq_rules(df, rules)
    row = result.collect()[0]
    assert row["dq_status"] == "FAIL"
    assert row["dq_failed_rule"] == "amount_positive"


def test_apply_dq_rules_accumulates_all_failing_rules(spark):
    """A row failing multiple rules must list ALL of them, not just the first."""
    df = spark.createDataFrame([{"id": 1, "amount": -5, "status": "UNKNOWN"}])
    rules = [
        {"rule_name": "amount_positive", "rule_expression": "amount > 0"},
        {"rule_name": "status_valid", "rule_expression": "status IN ('OPEN', 'CLOSED')"},
    ]
    result = apply_dq_rules(df, rules)
    row = result.collect()[0]
    assert row["dq_status"] == "FAIL"
    assert "amount_positive" in row["dq_failed_rule"]
    assert "status_valid" in row["dq_failed_rule"]


def test_apply_dq_rules_skips_empty_expression(spark):
    df = spark.createDataFrame([{"id": 1}])
    rules = [{"rule_name": "empty_rule", "rule_expression": ""}]
    with pytest.raises(DQConfigError):
        apply_dq_rules(df, rules)


def test_split_valid_reject_separates_correctly(spark):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("dq_status", StringType(), True),
        StructField("dq_failed_rule", StringType(), True),
    ])
    df = spark.createDataFrame([
        (1, "PASS", None),
        (2, "FAIL", "rule_x"),
        (3, "PASS", None),
    ], schema=schema)
    valid, reject = split_valid_reject(df)
    assert valid.count() == 2
    assert reject.count() == 1
    assert reject.collect()[0]["id"] == 2


def test_apply_dq_rules_primary_key_not_null(spark):
    df = spark.createDataFrame([
        {"id": 1, "amount": 10},
        {"id": None, "amount": 20},
    ])
    result = apply_dq_rules(df, dq_rows=[], primary_key="id")
    rows = {r["amount"]: r for r in result.collect()}
    assert rows[10]["dq_status"] == "PASS"
    assert rows[20]["dq_status"] == "FAIL"
    assert "primary_key_not_null" in (rows[20]["dq_failed_rule"] or "")


def test_apply_dq_rules_primary_key_unique(spark):
    df = spark.createDataFrame([
        {"id": 1, "amount": 10},
        {"id": 1, "amount": 20},
        {"id": 2, "amount": 30},
    ])
    result = apply_dq_rules(df, dq_rows=[], primary_key="id")
    rows = sorted(result.collect(), key=lambda r: r["amount"])
    assert rows[0]["dq_status"] == "FAIL"
    assert rows[1]["dq_status"] == "FAIL"
    assert rows[2]["dq_status"] == "PASS"
    assert "primary_key_unique" in (rows[0]["dq_failed_rule"] or "")
    assert "primary_key_unique" in (rows[1]["dq_failed_rule"] or "")
