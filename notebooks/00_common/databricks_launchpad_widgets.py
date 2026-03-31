"""Databricks UI widget helper driven by config/widget_options.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# dbutils compatibility shim
# ---------------------------------------------------------------------------
# In a live Databricks notebook, `dbutils` is injected as a notebook global.
# Outside Databricks (local dev, unit tests) it doesn't exist, so we provide
# a no-op stub so this module can always be imported and tested safely.
# ---------------------------------------------------------------------------

try:
    dbutils  # type: ignore[name-defined]  # noqa: F821
except NameError:

    class _NoOpWidgets:  # pragma: no cover
        """Stub that silently ignores widget creation outside Databricks."""

        _values: dict[str, str] = {}

        def dropdown(self, name: str, default: str, choices: list[str], label: str = "") -> None:
            self._values[name] = default

        def get(self, name: str) -> str:
            return self._values.get(name, "")

        def removeAll(self) -> None:
            self._values.clear()

    class _NoOpDbutils:
        widgets = _NoOpWidgets()

    dbutils = _NoOpDbutils()  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# YAML helpers
# ---------------------------------------------------------------------------


def load_widget_options(widget_file: str) -> dict:
    path = Path(widget_file)
    if not path.exists():
        raise FileNotFoundError(f"Widget options file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    return payload.get("widget_options", {})


def _choices(options: dict, key: str) -> list[str]:
    values = options.get(key, [])
    return [str(v) for v in values] if isinstance(values, list) else []


def _first(choices: list[str], fallback: str = "") -> str:
    """Return the first choice as the dropdown default, or *fallback* if empty."""
    return choices[0] if choices else fallback


# ---------------------------------------------------------------------------
# Widget creation
# ---------------------------------------------------------------------------


def create_launchpad_widgets(widget_file: str | None = None) -> dict[str, Any]:
    """Create Databricks dropdown widgets driven by widget_options.yaml.

    Defaults for each widget are taken from the **first value** in the
    corresponding YAML list, so updating widget_options.yaml automatically
    refreshes the defaults without touching this file.

    Returns the parsed ``widget_options`` dict so callers can inspect choices.
    Calling this function more than once is safe — existing widgets are removed
    first to avoid Databricks duplicate-widget errors.
    """
    if widget_file is None:
        widget_file = str(Path(__file__).resolve().parents[2] / "config" / "widget_options.yaml")

    options = load_widget_options(widget_file)

    # Remove any previously registered widgets to prevent duplicate errors
    # when the notebook cell is re-run.
    try:
        dbutils.widgets.removeAll()
    except Exception:  # noqa: BLE001
        pass

    env_choices = _choices(options, "environments")
    bool_choices = _choices(options, "boolean_values")
    folder_choices = _choices(options, "metadata_folder_name")
    mount_choices = _choices(options, "metadata_base_mount_point")
    paths_choices = _choices(options, "paths_config_path")
    source_choices = _choices(options, "ikea_dropzone_mounts")

    dbutils.widgets.dropdown("environment", _first(env_choices, "dev"), env_choices, "Environment")
    dbutils.widgets.dropdown("full_load", _first(bool_choices, "False"), bool_choices, "Full Load")
    dbutils.widgets.dropdown("metadata_folder", _first(folder_choices), folder_choices, "Metadata Folder")
    dbutils.widgets.dropdown("metadata_mount", _first(mount_choices), mount_choices, "Metadata Base Mount")
    dbutils.widgets.dropdown("paths_config_path", _first(paths_choices), paths_choices, "Paths Config")
    dbutils.widgets.dropdown("source_mount", _first(source_choices), source_choices, "Source Mount")

    return options


# ---------------------------------------------------------------------------
# Widget value reader
# ---------------------------------------------------------------------------

_WIDGET_NAMES = [
    "environment",
    "full_load",
    "metadata_folder",
    "metadata_mount",
    "paths_config_path",
    "source_mount",
]


def get_widget_values() -> dict[str, str]:
    """Read all launchpad widget values and return them as a plain dict.

    Call this after ``create_launchpad_widgets()`` to get the user-selected
    values at notebook runtime.

    Example::

        create_launchpad_widgets()
        cfg = get_widget_values()
        env = cfg["environment"]   # "dev" | "qa" | "prod"
    """
    return {name: dbutils.widgets.get(name) for name in _WIDGET_NAMES}

