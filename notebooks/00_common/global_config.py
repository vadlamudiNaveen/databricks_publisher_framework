"""Global configuration loader with environment variable substitution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::([^}]*))?\}")


def _substitute_env(value: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        key = match.group(1)
        default_value = match.group(2) if match.group(2) is not None else ""
        return os.getenv(key, default_value)

    return _ENV_PATTERN.sub(replacer, value)


def _resolve_obj(obj: Any) -> Any:
    if isinstance(obj, str):
        return _substitute_env(obj)
    if isinstance(obj, dict):
        return {k: _resolve_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_obj(v) for v in obj]
    return obj


def load_global_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Global config not found: {path}. "
            "Set --global-config to the correct path or check your deployment."
        )
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in global config {path}: {exc}") from exc
    return _resolve_obj(payload)


def require_config_keys(config: dict, required_dotted_paths: list[str]) -> None:
    """Raise ValueError listing every dotted key path that is missing or empty.

    Example::

        require_config_keys(cfg, ["audit.pipeline_runs_table", "organization.framework_name"])
    """
    missing: list[str] = []
    for dotted in required_dotted_paths:
        parts = dotted.split(".")
        node: Any = config
        for part in parts:
            if not isinstance(node, dict) or part not in node:
                missing.append(dotted)
                break
            node = node[part]
        else:
            if not node:  # catches empty string / None / 0 / []
                missing.append(dotted)
    if missing:
        raise ValueError(f"Missing required global config values: {missing}")
