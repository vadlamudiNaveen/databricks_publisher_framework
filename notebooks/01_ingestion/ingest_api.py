"""Generic API ingestion adapter."""

from __future__ import annotations

import os
import time

import requests

# Retry on transient server/rate-limit errors.
_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# Common envelope keys different APIs use to wrap record arrays.
# Checked in order; first match wins.
_DATA_ENVELOPE_KEYS = ["data", "items", "results", "records", "value", "content"]


def _resolve_api_url(api_profile: dict, source: dict) -> str:
    endpoint = source.get("api_endpoint", "")
    base_url = api_profile.get("base_url", "")
    if not endpoint:
        raise ValueError("api_endpoint is required in source metadata")
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    if not base_url:
        raise ValueError("base_url is required in API profile for relative api_endpoint values")
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _resolve_headers(api_profile: dict, headers: dict | None = None) -> dict:
    """Build request headers including authentication.

    Supported auth_type values in the API profile:
    - ``bearer``  — reads token from the env var named by ``token_env``
    - ``api_key`` — reads key from env var ``api_key_env``; header name from
                    ``api_key_header`` (default: ``X-API-Key``)
    - ``basic``   — reads username from ``user_env`` and password from ``password_env``
    - ``none``    — no auth header added
    """
    final_headers = dict(headers or {})
    auth_type = str(api_profile.get("auth_type", "none")).lower()

    if auth_type == "bearer":
        token_env = api_profile.get("token_env", "")
        token = os.getenv(token_env, "") if token_env else ""
        if token:
            final_headers.setdefault("Authorization", f"Bearer {token}")

    elif auth_type == "api_key":
        key_env = api_profile.get("api_key_env", "")
        key_header = api_profile.get("api_key_header", "X-API-Key")
        api_key = os.getenv(key_env, "") if key_env else ""
        if api_key:
            final_headers.setdefault(key_header, api_key)

    elif auth_type == "basic":
        import base64
        user_env = api_profile.get("user_env", "")
        password_env = api_profile.get("password_env", "")
        user = os.getenv(user_env, "") if user_env else ""
        password = os.getenv(password_env, "") if password_env else ""
        if user and password:
            token = base64.b64encode(f"{user}:{password}".encode()).decode()
            final_headers.setdefault("Authorization", f"Basic {token}")

    return final_headers


def _extract_records(payload: object, source: dict) -> list[dict]:
    """Extract a flat list of records from the API response payload.

    Supports:
    - Plain list response: ``[{...}, {...}]``
    - Enveloped response using a configurable or auto-detected key:
      ``{"data": [...]}`` / ``{"items": [...]}`` / ``{"results": [...]}`` etc.
    - Single-record response: ``{...}`` → wrapped in a list.

    Set ``response_data_key`` in ``source_options_json`` to pin the envelope
    key for a specific source (e.g. ``"response_data_key": "content"``).
    """
    if isinstance(payload, list):
        return payload  # type: ignore[return-value]

    if isinstance(payload, dict):
        # Allow config-level override first.
        override_key = source.get("response_data_key", "")
        if override_key and isinstance(payload.get(override_key), list):
            return payload[override_key]  # type: ignore[return-value]
        # Auto-detect common envelope keys.
        for key in _DATA_ENVELOPE_KEYS:
            if isinstance(payload.get(key), list):
                return payload[key]  # type: ignore[return-value]
        # Scalar envelope — wrap the whole dict.
        return [payload]

    return []


def _parse_response_json(response: requests.Response) -> object:
    """Parse response body as JSON with a clear error if content-type is wrong."""
    content_type = response.headers.get("Content-Type", "")
    if "json" not in content_type and "javascript" not in content_type:
        snippet = response.text[:200]
        raise ValueError(
            f"Expected JSON response but got Content-Type: {content_type!r}. "
            f"Response snippet: {snippet!r}"
        )
    return response.json()


def _fetch_page(
    session: requests.Session,
    url: str,
    headers: dict,
    params: dict,
    timeout: int,
) -> requests.Response:
    response = session.get(url, headers=headers, params=params, timeout=timeout)
    return response


def ingest_api(
    source: dict,
    api_profile: dict,
    headers: dict | None = None,
    params: dict | None = None,
    source_options: dict | None = None,
) -> list[dict]:
    """Ingest records from a REST API endpoint with retry and pagination support.

    Pagination is driven by ``source_options`` (parsed from ``source_options_json``):

    .. code-block:: json

        {
          "pagination_type": "cursor",
          "next_page_key":   "next_page_token",
          "page_size_param": "limit",
          "page_size":       500,
          "max_pages":       50
        }

    ``pagination_type`` values supported:
    - ``cursor``  — follows ``next_page_key`` in response body until null/absent
    - ``offset``  — increments ``offset`` param by ``page_size`` until empty page
    - ``none``    — single request (default)
    """
    if "timeout_seconds" not in api_profile:
        raise ValueError("timeout_seconds must be configured in API profile")

    opts = source_options or {}
    endpoint = _resolve_api_url(api_profile, source)
    timeout = int(api_profile["timeout_seconds"])
    max_retries = int(api_profile.get("max_retries", 2))
    backoff_seconds = float(api_profile.get("retry_backoff_seconds", 2))
    final_headers = _resolve_headers(api_profile, headers=headers)

    # All pagination config must come from source_options_json, not from the
    # source registry row — they are operational options, not registry fields.
    pagination_type = str(opts.get("pagination_type", "none")).lower()
    next_page_key = str(opts.get("next_page_key", "next_page_token"))
    page_size_param = str(opts.get("page_size_param", "limit"))
    page_size = int(opts.get("page_size", 500))
    max_pages = int(opts.get("max_pages", 100))

    current_params = dict(params or {})
    if pagination_type in {"cursor", "offset"} and page_size_param:
        current_params[page_size_param] = page_size

    all_records: list[dict] = []
    offset = 0

    with requests.Session() as session:
        for page_num in range(max_pages):
            if pagination_type == "offset":
                current_params["offset"] = offset

            # Retry loop for transient failures.
            last_error: Exception | None = None
            response: requests.Response | None = None
            for attempt in range(max_retries + 1):
                try:
                    response = _fetch_page(session, endpoint, final_headers, current_params, timeout)
                    if response.status_code in _RETRY_STATUS_CODES and attempt < max_retries:
                        time.sleep(backoff_seconds * (attempt + 1))
                        continue
                    response.raise_for_status()
                    break
                except requests.RequestException as exc:
                    last_error = exc
                    if attempt >= max_retries:
                        raise RuntimeError(
                            f"API request failed after {max_retries + 1} attempt(s): {exc}"
                        ) from exc
                    time.sleep(backoff_seconds * (attempt + 1))

            if response is None:
                raise RuntimeError("API ingestion failed with unknown error")

            payload = _parse_response_json(response)
            records = _extract_records(payload, source)
            all_records.extend(records)

            # Determine whether to fetch the next page.
            if pagination_type == "cursor":
                next_token = payload.get(next_page_key) if isinstance(payload, dict) else None  # type: ignore[union-attr]
                if not next_token:
                    break
                current_params[next_page_key] = next_token
            elif pagination_type == "offset":
                if not records:
                    break  # Empty page → last page reached.
                offset += len(records)
            else:
                break  # No pagination — single request.

    return all_records
