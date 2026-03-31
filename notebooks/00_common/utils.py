"""Common utility helpers for Databricks ingestion framework."""

from __future__ import annotations

import hashlib
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_load_id() -> str:
    return str(uuid.uuid4())


def row_hash(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def require_keys(payload: Dict[str, Any], required: list[str]) -> None:
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f"Missing required keys: {missing}")


def truncate_str(value: str, max_chars: int = 500) -> str:
    """Truncate a string to *max_chars*, replacing newlines for single-line output."""
    msg = str(value).replace("\n", " ").strip()
    return msg[:max_chars]


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = "ingestion_framework") -> logging.Logger:
    """Return a logger that writes structured JSON lines to stdout.

    Safe to call multiple times with the same name — returns the existing
    logger without adding duplicate handlers.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger
