"""Tests for utils.py — get_logger, truncate_str, row_hash, require_keys."""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "00_common"))

from utils import get_logger, require_keys, row_hash, truncate_str


# ─── truncate_str ────────────────────────────────────────────────────────────


def test_truncate_str_short_string_unchanged():
    assert truncate_str("hello") == "hello"


def test_truncate_str_long_string_cut():
    result = truncate_str("x" * 600)
    assert len(result) == 500


def test_truncate_str_custom_max():
    result = truncate_str("hello world", max_chars=5)
    assert result == "hello"


def test_truncate_str_replaces_newlines():
    result = truncate_str("line1\nline2\nline3")
    assert "\n" not in result
    assert "line1 line2 line3" == result


def test_truncate_str_strips_whitespace():
    assert truncate_str("  hello  ") == "hello"


# ─── get_logger ──────────────────────────────────────────────────────────────


def test_get_logger_returns_logger_with_correct_name():
    logger = get_logger("test.ingestion")
    assert logger.name == "test.ingestion"


def test_get_logger_idempotent_no_duplicate_handlers():
    logger1 = get_logger("test.dedup")
    handler_count = len(logger1.handlers)
    logger2 = get_logger("test.dedup")  # Second call — same logger.
    assert len(logger2.handlers) == handler_count


def test_get_logger_emits_json(capsys):
    logger = get_logger("test.json_output")
    logger.info("test message")
    out = capsys.readouterr().out
    # Must be valid JSON.
    payload = json.loads(out.strip())
    assert payload["message"] == "test message"
    assert payload["level"] == "INFO"
    assert "ts" in payload


def test_get_logger_default_name():
    logger = get_logger()
    assert logger.name == "ingestion_framework"


# ─── row_hash ────────────────────────────────────────────────────────────────


def test_row_hash_is_stable():
    payload = {"id": 1, "name": "Alice"}
    assert row_hash(payload) == row_hash(payload)


def test_row_hash_is_order_independent():
    a = {"z": 2, "a": 1}
    b = {"a": 1, "z": 2}
    assert row_hash(a) == row_hash(b)


def test_row_hash_differs_for_different_payloads():
    assert row_hash({"id": 1}) != row_hash({"id": 2})


def test_row_hash_is_sha256_length():
    h = row_hash({"x": 1})
    assert len(h) == 64  # SHA-256 hex digest is always 64 chars.


# ─── require_keys ────────────────────────────────────────────────────────────


def test_require_keys_passes_when_all_present():
    require_keys({"a": 1, "b": 2}, ["a", "b"])  # No exception.


def test_require_keys_raises_with_missing_keys():
    with pytest.raises(ValueError, match="Missing required keys"):
        require_keys({"a": 1}, ["a", "b", "c"])


def test_require_keys_empty_required_list_passes():
    require_keys({}, [])  # No exception.
