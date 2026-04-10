"""Tests for ingest_jdbc.py — pure-Python logic, no Spark required."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "01_ingestion"))

from ingest_jdbc import JdbcIngestionConfigError, ParallelReadOptions, build_jdbc_options


# ─── build_jdbc_options ──────────────────────────────────────────────────────


def _valid_profile():
    return {"url": "jdbc:postgresql://host/db", "driver": "org.postgresql.Driver", "fetchsize": "1000"}


def test_build_jdbc_options_happy_path():
    options = build_jdbc_options(_valid_profile(), {"jdbc_table": "schema.orders"})
    assert options["url"] == "jdbc:postgresql://host/db"
    assert options["dbtable"] == "schema.orders"
    assert options["driver"] == "org.postgresql.Driver"
    assert options["fetchsize"] == "1000"


def test_build_jdbc_options_strips_empty_credentials():
    profile = {**_valid_profile(), "user_env": "MISSING_ENV_XYZ", "password_env": "MISSING_PASS_XYZ"}
    options = build_jdbc_options(profile, {"jdbc_table": "t"})
    # Env vars don't exist → empty strings → must be stripped from options dict.
    assert "user" not in options
    assert "password" not in options


def test_build_jdbc_options_includes_credentials_when_env_set(monkeypatch):
    monkeypatch.setenv("DB_USER", "alice")
    monkeypatch.setenv("DB_PASS", "secret")
    profile = {**_valid_profile(), "user_env": "DB_USER", "password_env": "DB_PASS"}
    options = build_jdbc_options(profile, {"jdbc_table": "t"})
    assert options["user"] == "alice"
    assert options["password"] == "secret"


def test_build_jdbc_options_raises_on_missing_profile_fields():
    with pytest.raises(ValueError, match="Missing JDBC profile fields"):
        build_jdbc_options({"url": "jdbc:x"}, {"jdbc_table": "t"})


def test_build_jdbc_options_raises_when_no_jdbc_table():
    with pytest.raises(ValueError, match="jdbc_table is required"):
        build_jdbc_options(_valid_profile(), {"jdbc_table": ""})


# ─── ParallelReadOptions.from_extra_options ──────────────────────────────────


def test_parallel_read_all_four_keys_wired():
    extra = {"numPartitions": 8, "partitionColumn": "id", "lowerBound": 1, "upperBound": 1_000_000}
    result = ParallelReadOptions.from_extra_options(extra)
    assert result.enabled is True
    assert result.options["numPartitions"] == "8"
    assert result.options["partitionColumn"] == "id"
    assert result.options["lowerBound"] == "1"
    assert result.options["upperBound"] == "1000000"


def test_parallel_read_partial_keys_raise_validation_error():
    extra = {"numPartitions": 8, "partitionColumn": "id", "lowerBound": 1}
    with pytest.raises(JdbcIngestionConfigError, match="requires all partition options"):
        ParallelReadOptions.from_extra_options(extra)


def test_parallel_read_no_partition_keys_unchanged():
    result = ParallelReadOptions.from_extra_options({"queryTimeout": "30"})
    assert result.enabled is False
    assert result.options == {}
