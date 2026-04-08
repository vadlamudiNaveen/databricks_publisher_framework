"""Production-oriented generic API ingestion adapter.

Features:
- Config-driven endpoint resolution
- Config-driven auth
- Retry support with backoff and Retry-After handling
- GET and POST support
- Cursor and offset pagination
- Strict validation
- Structured logging
- Better error context
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Mapping

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
_DATA_ENVELOPE_KEYS = ["data", "items", "results", "records", "value", "content"]
_SUPPORTED_HTTP_METHODS = {"GET", "POST"}
_SUPPORTED_AUTH_TYPES = {"none", "bearer", "api_key", "basic"}
_SUPPORTED_PAGINATION_TYPES = {"none", "cursor", "offset"}


class ApiIngestionConfigError(ValueError):
    """Raised when API ingestion configuration is invalid."""


class ApiIngestionExecutionError(RuntimeError):
    """Raised when API ingestion execution fails."""


def _require_non_empty(value: Any, field_name: str) -> str:
    if value is None:
        raise ApiIngestionConfigError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise ApiIngestionConfigError(f"{field_name} is required")
    return text


def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return repr(value)


def _get_nested_value(payload: Any, path: str) -> Any:
    """Get nested value from dict using dot notation, e.g. 'data.items'."""
    current = payload
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


@dataclass(frozen=True)
class ApiProfile:
    base_url: str | None
    auth_type: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    http_method: str

    token_env: str | None = None
    api_key_env: str | None = None
    api_key_header: str | None = None
    user_env: str | None = None
    password_env: str | None = None

    @classmethod
    def from_dict(cls, api_profile: Mapping[str, Any]) -> "ApiProfile":
        if "timeout_seconds" not in api_profile:
            raise ApiIngestionConfigError("api_profile.timeout_seconds must be configured")

        auth_type = str(api_profile.get("auth_type", "none")).strip().lower()
        if auth_type not in _SUPPORTED_AUTH_TYPES:
            raise ApiIngestionConfigError(
                f"Unsupported auth_type '{auth_type}'. Supported: {sorted(_SUPPORTED_AUTH_TYPES)}"
            )

        http_method = str(api_profile.get("http_method", "GET")).strip().upper()
        if http_method not in _SUPPORTED_HTTP_METHODS:
            raise ApiIngestionConfigError(
                f"Unsupported http_method '{http_method}'. Supported: {sorted(_SUPPORTED_HTTP_METHODS)}"
            )

        timeout_seconds = int(api_profile["timeout_seconds"])
        if timeout_seconds <= 0:
            raise ApiIngestionConfigError("api_profile.timeout_seconds must be > 0")

        max_retries = int(api_profile.get("max_retries", 2))
        if max_retries < 0:
            raise ApiIngestionConfigError("api_profile.max_retries must be >= 0")

        retry_backoff_seconds = float(api_profile.get("retry_backoff_seconds", 2.0))
        if retry_backoff_seconds < 0:
            raise ApiIngestionConfigError("api_profile.retry_backoff_seconds must be >= 0")

        base_url = api_profile.get("base_url")
        base_url_text = str(base_url).strip() if base_url else None

        return cls(
            base_url=base_url_text,
            auth_type=auth_type,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            http_method=http_method,
            token_env=str(api_profile.get("token_env")).strip() if api_profile.get("token_env") else None,
            api_key_env=str(api_profile.get("api_key_env")).strip() if api_profile.get("api_key_env") else None,
            api_key_header=str(api_profile.get("api_key_header", "X-API-Key")).strip(),
            user_env=str(api_profile.get("user_env")).strip() if api_profile.get("user_env") else None,
            password_env=str(api_profile.get("password_env")).strip() if api_profile.get("password_env") else None,
        )


@dataclass(frozen=True)
class SourceConfig:
    api_endpoint: str
    source_system: str | None = None
    source_entity: str | None = None
    response_data_key: str | None = None

    @classmethod
    def from_dict(cls, source: Mapping[str, Any]) -> "SourceConfig":
        response_data_key = source.get("response_data_key")
        return cls(
            api_endpoint=_require_non_empty(source.get("api_endpoint"), "source.api_endpoint"),
            source_system=str(source.get("source_system")).strip() if source.get("source_system") else None,
            source_entity=str(source.get("source_entity")).strip() if source.get("source_entity") else None,
            response_data_key=str(response_data_key).strip() if response_data_key else None,
        )

    @property
    def source_id(self) -> str:
        system = self.source_system or "unknown_system"
        entity = self.source_entity or "unknown_entity"
        return f"{system}.{entity}"


@dataclass(frozen=True)
class PaginationConfig:
    pagination_type: str
    next_page_key: str
    next_page_param: str
    page_size_param: str
    page_size: int
    max_pages: int
    offset_param: str

    @classmethod
    def from_source_options(cls, source_options: Mapping[str, Any] | None) -> "PaginationConfig":
        opts = source_options or {}
        pagination_type = str(opts.get("pagination_type", "none")).strip().lower()
        if pagination_type not in _SUPPORTED_PAGINATION_TYPES:
            raise ApiIngestionConfigError(
                f"Unsupported pagination_type '{pagination_type}'. Supported: {sorted(_SUPPORTED_PAGINATION_TYPES)}"
            )

        page_size = int(opts.get("page_size", 500))
        if page_size <= 0:
            raise ApiIngestionConfigError("source_options.page_size must be > 0")

        max_pages = int(opts.get("max_pages", 100))
        if max_pages <= 0:
            raise ApiIngestionConfigError("source_options.max_pages must be > 0")

        return cls(
            pagination_type=pagination_type,
            next_page_key=str(opts.get("next_page_key", "next_page_token")).strip(),
            next_page_param=str(opts.get("next_page_param", "page_token")).strip(),
            page_size_param=str(opts.get("page_size_param", "limit")).strip(),
            page_size=page_size,
            max_pages=max_pages,
            offset_param=str(opts.get("offset_param", "offset")).strip(),
        )


@dataclass(frozen=True)
class RequestOptions:
    body: dict[str, Any] | None
    envelope_key: str | None

    @classmethod
    def from_source_and_options(
        cls,
        source: Mapping[str, Any],
        source_options: Mapping[str, Any] | None,
    ) -> "RequestOptions":
        opts = source_options or {}
        body = opts.get("request_body")
        if body is not None and not isinstance(body, dict):
            raise ApiIngestionConfigError("source_options.request_body must be a dict when provided")

        envelope_key = source.get("response_data_key") or opts.get("response_data_key")
        envelope_key_text = str(envelope_key).strip() if envelope_key else None

        return cls(
            body=body,
            envelope_key=envelope_key_text,
        )


def _resolve_api_url(api_profile: ApiProfile, source: SourceConfig) -> str:
    endpoint = source.api_endpoint

    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint

    if not api_profile.base_url:
        raise ApiIngestionConfigError(
            "api_profile.base_url is required when source.api_endpoint is relative"
        )

    return f"{api_profile.base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _resolve_auth_headers_and_auth(
    api_profile: ApiProfile,
    headers: Mapping[str, Any] | None = None,
) -> tuple[dict[str, str], HTTPBasicAuth | None]:
    final_headers = {str(k): str(v) for k, v in (headers or {}).items()}
    auth = None

    if api_profile.auth_type == "none":
        return final_headers, None

    if api_profile.auth_type == "bearer":
        token_env = _require_non_empty(api_profile.token_env, "api_profile.token_env")
        token = os.getenv(token_env, "").strip()
        if not token:
            raise ApiIngestionConfigError(
                f"Environment variable '{token_env}' is required for bearer auth"
            )
        final_headers.setdefault("Authorization", f"Bearer {token}")
        return final_headers, None

    if api_profile.auth_type == "api_key":
        key_env = _require_non_empty(api_profile.api_key_env, "api_profile.api_key_env")
        key_header = _require_non_empty(api_profile.api_key_header, "api_profile.api_key_header")
        api_key = os.getenv(key_env, "").strip()
        if not api_key:
            raise ApiIngestionConfigError(
                f"Environment variable '{key_env}' is required for api_key auth"
            )
        final_headers.setdefault(key_header, api_key)
        return final_headers, None

    if api_profile.auth_type == "basic":
        user_env = _require_non_empty(api_profile.user_env, "api_profile.user_env")
        password_env = _require_non_empty(api_profile.password_env, "api_profile.password_env")
        user = os.getenv(user_env, "").strip()
        password = os.getenv(password_env, "").strip()
        if not user or not password:
            raise ApiIngestionConfigError(
                f"Environment variables '{user_env}' and '{password_env}' are required for basic auth"
            )
        auth = HTTPBasicAuth(user, password)
        return final_headers, auth

    raise ApiIngestionConfigError(f"Unsupported auth_type '{api_profile.auth_type}'")


def _extract_records(payload: Any, envelope_key: str | None = None) -> list[dict[str, Any]]:
    """Extract a flat list of dict records from the API payload."""
    if isinstance(payload, list):
        if all(isinstance(item, dict) for item in payload):
            return list(payload)
        return [{"value": item} for item in payload]

    if isinstance(payload, dict):
        if envelope_key:
            nested = _get_nested_value(payload, envelope_key)
            if nested is None:
                raise ApiIngestionExecutionError(
                    f"Configured response_data_key '{envelope_key}' not found in API response"
                )
            if isinstance(nested, list):
                if all(isinstance(item, dict) for item in nested):
                    return list(nested)
                return [{"value": item} for item in nested]
            if isinstance(nested, dict):
                return [nested]
            return [{"value": nested}]

        for key in _DATA_ENVELOPE_KEYS:
            value = payload.get(key)
            if isinstance(value, list):
                if all(isinstance(item, dict) for item in value):
                    return list(value)
                return [{"value": item} for item in value]

        return [payload]

    return []


def _parse_response_json(response: requests.Response) -> Any:
    content_type = response.headers.get("Content-Type", "")
    if "json" not in content_type.lower() and "javascript" not in content_type.lower():
        snippet = response.text[:500]
        raise ApiIngestionExecutionError(
            f"Expected JSON response but got Content-Type={content_type!r}. "
            f"Response snippet={snippet!r}"
        )

    try:
        return response.json()
    except ValueError as exc:
        snippet = response.text[:500]
        raise ApiIngestionExecutionError(
            f"Response advertised JSON but could not be parsed. Snippet={snippet!r}"
        ) from exc


def _sleep_before_retry(response: requests.Response | None, base_backoff_seconds: float, attempt: int) -> None:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                time.sleep(float(retry_after))
                return
            except ValueError:
                pass

    time.sleep(base_backoff_seconds * (attempt + 1))


def _send_request(
    session: requests.Session,
    method: str,
    url: str,
    headers: Mapping[str, str],
    params: Mapping[str, Any],
    json_body: Mapping[str, Any] | None,
    timeout: int,
    auth: HTTPBasicAuth | None,
) -> requests.Response:
    if method == "GET":
        return session.get(url, headers=headers, params=params, timeout=timeout, auth=auth)
    if method == "POST":
        return session.post(url, headers=headers, params=params, json=json_body, timeout=timeout, auth=auth)

    raise ApiIngestionConfigError(f"Unsupported HTTP method '{method}'")


def ingest_api(
    source: Mapping[str, Any],
    api_profile: Mapping[str, Any],
    headers: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
    source_options: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Ingest records from a REST API endpoint with retry and pagination support."""
    profile = ApiProfile.from_dict(api_profile)
    source_cfg = SourceConfig.from_dict(source)
    pagination = PaginationConfig.from_source_options(source_options)
    request_options = RequestOptions.from_source_and_options(source, source_options)

    endpoint = _resolve_api_url(profile, source_cfg)
    final_headers, auth = _resolve_auth_headers_and_auth(profile, headers=headers)

    current_params: dict[str, Any] = dict(params or {})
    if pagination.pagination_type in {"cursor", "offset"} and pagination.page_size_param:
        current_params[pagination.page_size_param] = pagination.page_size

    all_records: list[dict[str, Any]] = []
    offset = 0
    seen_tokens: set[str] = set()
    started_at = time.time()

    logger.info(
        "Starting API ingestion source_id=%s method=%s endpoint=%s pagination_type=%s",
        source_cfg.source_id,
        profile.http_method,
        endpoint,
        pagination.pagination_type,
    )

    with requests.Session() as session:
        for page_num in range(1, pagination.max_pages + 1):
            if pagination.pagination_type == "offset":
                current_params[pagination.offset_param] = offset

            response: requests.Response | None = None

            for attempt in range(profile.max_retries + 1):
                try:
                    logger.debug(
                        "API request source_id=%s page_num=%s attempt=%s params=%s",
                        source_cfg.source_id,
                        page_num,
                        attempt + 1,
                        _safe_json_dumps(current_params),
                    )

                    response = _send_request(
                        session=session,
                        method=profile.http_method,
                        url=endpoint,
                        headers=final_headers,
                        params=current_params,
                        json_body=request_options.body,
                        timeout=profile.timeout_seconds,
                        auth=auth,
                    )

                    if response.status_code in _RETRY_STATUS_CODES and attempt < profile.max_retries:
                        logger.warning(
                            "Retryable API response source_id=%s page_num=%s status_code=%s attempt=%s",
                            source_cfg.source_id,
                            page_num,
                            response.status_code,
                            attempt + 1,
                        )
                        _sleep_before_retry(response, profile.retry_backoff_seconds, attempt)
                        continue

                    response.raise_for_status()
                    break

                except requests.RequestException as exc:
                    if attempt >= profile.max_retries:
                        raise ApiIngestionExecutionError(
                            f"API request failed source_id={source_cfg.source_id} "
                            f"endpoint={endpoint} page_num={page_num} "
                            f"after {profile.max_retries + 1} attempt(s): {exc}"
                        ) from exc

                    logger.warning(
                        "Transient API request failure source_id=%s page_num=%s attempt=%s error=%s",
                        source_cfg.source_id,
                        page_num,
                        attempt + 1,
                        str(exc),
                    )
                    _sleep_before_retry(response, profile.retry_backoff_seconds, attempt)

            if response is None:
                raise ApiIngestionExecutionError(
                    f"API ingestion failed with unknown error for source_id={source_cfg.source_id}"
                )

            payload = _parse_response_json(response)
            records = _extract_records(payload, envelope_key=request_options.envelope_key)
            all_records.extend(records)

            logger.info(
                "Fetched API page source_id=%s page_num=%s records=%s total_records=%s",
                source_cfg.source_id,
                page_num,
                len(records),
                len(all_records),
            )

            if pagination.pagination_type == "cursor":
                next_token = None
                if isinstance(payload, dict):
                    next_token = _get_nested_value(payload, pagination.next_page_key)

                if not next_token:
                    break

                next_token_str = str(next_token)
                if next_token_str in seen_tokens:
                    raise ApiIngestionExecutionError(
                        f"Detected repeated cursor token for source_id={source_cfg.source_id}. "
                        f"Token={next_token_str!r}"
                    )

                seen_tokens.add(next_token_str)
                current_params[pagination.next_page_param] = next_token

            elif pagination.pagination_type == "offset":
                if not records:
                    break
                offset += len(records)

            else:
                break

    elapsed = round(time.time() - started_at, 3)
    logger.info(
        "Completed API ingestion source_id=%s endpoint=%s total_records=%s elapsed_seconds=%s",
        source_cfg.source_id,
        endpoint,
        len(all_records),
        elapsed,
    )

    return all_records