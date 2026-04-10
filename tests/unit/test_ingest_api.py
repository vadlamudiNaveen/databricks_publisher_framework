"""Tests for ingest_api.py — covers all pure-Python logic without network calls."""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "01_ingestion"))

from ingest_api import (
    ApiIngestionConfigError,
    ApiIngestionExecutionError,
    ApiProfile,
    SourceConfig,
    _extract_records,
    _parse_response_json,
    _resolve_api_url,
    _resolve_auth_headers_and_auth,
    ingest_api,
)


# ─── _resolve_api_url ────────────────────────────────────────────────────────


def test_resolve_api_url_uses_absolute_endpoint():
    source = SourceConfig.from_dict({"api_endpoint": "https://api.example.com/data"})
    profile = ApiProfile.from_dict({"timeout_seconds": 10})
    assert _resolve_api_url(profile, source) == "https://api.example.com/data"


def test_resolve_api_url_joins_base_and_relative():
    profile = ApiProfile.from_dict({"base_url": "https://api.example.com", "timeout_seconds": 10})
    source = SourceConfig.from_dict({"api_endpoint": "/v1/items"})
    assert _resolve_api_url(profile, source) == "https://api.example.com/v1/items"


def test_resolve_api_url_raises_when_no_endpoint():
    with pytest.raises(ApiIngestionConfigError, match="source.api_endpoint is required"):
        SourceConfig.from_dict({"api_endpoint": ""})


def test_resolve_api_url_raises_when_relative_and_no_base():
    profile = ApiProfile.from_dict({"timeout_seconds": 10})
    source = SourceConfig.from_dict({"api_endpoint": "/v1/items"})
    with pytest.raises(ApiIngestionConfigError, match="api_profile.base_url is required"):
        _resolve_api_url(profile, source)


# ─── _resolve_auth_headers_and_auth ─────────────────────────────────────────


def _profile(auth_type: str, **extra):
    return ApiProfile.from_dict({"timeout_seconds": 10, "auth_type": auth_type, **extra})


def test_resolve_headers_bearer(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "abc123")
    headers, auth = _resolve_auth_headers_and_auth(_profile("bearer", token_env="MY_TOKEN"))
    assert headers["Authorization"] == "Bearer abc123"
    assert auth is None


def test_resolve_headers_bearer_raises_when_env_missing(monkeypatch):
    monkeypatch.delenv("MISSING_TOKEN", raising=False)
    with pytest.raises(ApiIngestionConfigError, match="required for bearer auth"):
        _resolve_auth_headers_and_auth(_profile("bearer", token_env="MISSING_TOKEN"))


def test_resolve_headers_api_key(monkeypatch):
    monkeypatch.setenv("MY_API_KEY", "key999")
    headers, auth = _resolve_auth_headers_and_auth(
        _profile("api_key", api_key_env="MY_API_KEY", api_key_header="X-Custom-Key")
    )
    assert headers["X-Custom-Key"] == "key999"
    assert auth is None


def test_resolve_headers_api_key_default_header_name(monkeypatch):
    monkeypatch.setenv("MY_API_KEY", "keyXYZ")
    headers, _ = _resolve_auth_headers_and_auth(_profile("api_key", api_key_env="MY_API_KEY"))
    assert headers["X-API-Key"] == "keyXYZ"


def test_resolve_headers_basic(monkeypatch):
    monkeypatch.setenv("DB_USER", "alice")
    monkeypatch.setenv("DB_PASS", "s3cr3t")
    headers, auth = _resolve_auth_headers_and_auth(
        _profile("basic", user_env="DB_USER", password_env="DB_PASS")
    )
    assert "Authorization" not in headers
    assert auth is not None


def test_resolve_headers_none_auth_type():
    headers, auth = _resolve_auth_headers_and_auth(_profile("none"))
    assert "Authorization" not in headers
    assert auth is None


def test_resolve_headers_merges_extra_headers(monkeypatch):
    monkeypatch.setenv("TOK", "t1")
    extra = {"X-Trace-Id": "trace-abc"}
    headers, _ = _resolve_auth_headers_and_auth(
        _profile("bearer", token_env="TOK"), headers=extra
    )
    assert headers["X-Trace-Id"] == "trace-abc"
    assert "Authorization" in headers


def test_resolve_headers_caller_header_not_overwritten(monkeypatch):
    monkeypatch.setenv("TOK", "t1")
    extra = {"Authorization": "Bearer caller-token"}
    headers, _ = _resolve_auth_headers_and_auth(
        _profile("bearer", token_env="TOK"), headers=extra
    )
    assert headers["Authorization"] == "Bearer caller-token"


# ─── _extract_records ────────────────────────────────────────────────────────


def test_extract_records_plain_list():
    payload = [{"id": 1}, {"id": 2}]
    assert _extract_records(payload, {}) == payload


def test_extract_records_data_envelope():
    payload = {"data": [{"id": 1}], "total": 1}
    assert _extract_records(payload, {}) == [{"id": 1}]


def test_extract_records_items_envelope():
    payload = {"items": [{"id": 2}]}
    assert _extract_records(payload, {}) == [{"id": 2}]


def test_extract_records_custom_key_override():
    payload = {"content": [{"x": 1}], "data": [{"x": 2}]}
    # "content" key override wins over auto-detect
    source = {"response_data_key": "content"}
    assert _extract_records(payload, source) == [{"x": 1}]


def test_extract_records_scalar_dict_wrapped():
    payload = {"id": 1, "name": "test"}
    assert _extract_records(payload, {}) == [{"id": 1, "name": "test"}]


def test_extract_records_empty_input():
    assert _extract_records([], {}) == []


# ─── _parse_response_json ────────────────────────────────────────────────────


def test_parse_response_json_accepts_json_content_type():
    mock_resp = MagicMock()
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json.return_value = {"key": "value"}
    result = _parse_response_json(mock_resp)
    assert result == {"key": "value"}


def test_parse_response_json_rejects_html():
    mock_resp = MagicMock()
    mock_resp.headers = {"Content-Type": "text/html"}
    mock_resp.text = "<html>Error</html>"
    with pytest.raises(ApiIngestionExecutionError, match="Expected JSON response"):
        _parse_response_json(mock_resp)


# ─── ingest_api (integration via mock) ───────────────────────────────────────


def test_ingest_api_single_page_no_pagination():
    profile = {"timeout_seconds": 10, "base_url": "https://api.test"}
    source = {"api_endpoint": "/items"}

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json.return_value = [{"id": 1}, {"id": 2}]

    with patch("ingest_api.requests.Session") as MockSession:
        session_instance = MockSession.return_value.__enter__.return_value
        session_instance.get.return_value = mock_resp
        mock_resp.raise_for_status = MagicMock()

        records = ingest_api(source=source, api_profile=profile)

    assert records == [{"id": 1}, {"id": 2}]


def test_ingest_api_raises_when_no_timeout():
    with pytest.raises(ApiIngestionConfigError, match="timeout_seconds must be configured"):
        ingest_api(source={"api_endpoint": "https://a.b/c"}, api_profile={})


def test_ingest_api_pagination_reads_from_source_options_not_source():
    """Pagination config must come from source_options, NOT the flat source registry row."""
    profile = {"timeout_seconds": 5, "base_url": "https://api.test"}
    # Pagination keys on the source registry row — these must be IGNORED.
    source = {"api_endpoint": "/items", "pagination_type": "cursor"}

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json.return_value = [{"id": 1}]  # Plain list, no cursor key

    with patch("ingest_api.requests.Session") as MockSession:
        session = MockSession.return_value.__enter__.return_value
        session.get.return_value = mock_resp
        mock_resp.raise_for_status = MagicMock()

        # No source_options → pagination_type defaults to "none" → single page.
        records = ingest_api(source=source, api_profile=profile, source_options=None)

    # Should return records from exactly 1 call (no pagination loop).
    assert records == [{"id": 1}]
    assert session.get.call_count == 1
