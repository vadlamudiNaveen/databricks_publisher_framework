"""Tests for publish_silver.py — pure-Python / mock-based."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "03_publish"))

from publish_silver import _validate_identifier, publish_merge


# ─── _validate_identifier ────────────────────────────────────────────────────


def test_validate_identifier_accepts_simple_column():
    assert _validate_identifier("order_id", "merge_key") == "order_id"


def test_validate_identifier_accepts_schema_qualified():
    assert _validate_identifier("main.silver.orders", "silver_table") == "main.silver.orders"


def test_validate_identifier_rejects_sql_injection():
    with pytest.raises(ValueError, match="valid SQL identifier"):
        _validate_identifier("id; DROP TABLE orders", "merge_key")


def test_validate_identifier_rejects_spaces():
    with pytest.raises(ValueError, match="valid SQL identifier"):
        _validate_identifier("order id", "merge_key")


def test_validate_identifier_rejects_empty():
    with pytest.raises(ValueError, match="valid SQL identifier"):
        _validate_identifier("", "merge_key")


# ─── publish_merge ───────────────────────────────────────────────────────────


def test_publish_merge_uses_unique_temp_view():
    """Each call to publish_merge must create a DIFFERENT temp view name."""
    spark = MagicMock()
    df = MagicMock()

    view_names = []

    def capture_create_view(name):
        view_names.append(name)

    df.createOrReplaceTempView.side_effect = capture_create_view

    publish_merge(spark, df, "main.silver.orders", "order_id")
    publish_merge(spark, df, "main.silver.orders", "order_id")

    assert len(view_names) == 2
    assert view_names[0] != view_names[1], "Two sequential merges used the same temp view name — collision risk!"


def test_publish_merge_drops_temp_view_after_merge():
    """Temp view must be cleaned up even on success."""
    spark = MagicMock()
    df = MagicMock()
    captured_view = []

    def capture_view(name):
        captured_view.append(name)

    df.createOrReplaceTempView.side_effect = capture_view

    publish_merge(spark, df, "main.silver.orders", "order_id")

    spark.catalog.dropTempView.assert_called_once_with(captured_view[0])


def test_publish_merge_drops_temp_view_on_sql_error():
    """Temp view must be cleaned up even when the MERGE SQL fails."""
    spark = MagicMock()
    df = MagicMock()
    spark.sql.side_effect = RuntimeError("MERGE failed")
    captured_view = []

    df.createOrReplaceTempView.side_effect = lambda name: captured_view.append(name)

    with pytest.raises(RuntimeError, match="MERGE failed"):
        publish_merge(spark, df, "main.silver.orders", "order_id")

    spark.catalog.dropTempView.assert_called_once_with(captured_view[0])


def test_publish_merge_rejects_injection_in_merge_key():
    spark = MagicMock()
    df = MagicMock()
    with pytest.raises(ValueError, match="valid SQL identifier"):
        publish_merge(spark, df, "main.silver.orders", "id OR 1=1")
