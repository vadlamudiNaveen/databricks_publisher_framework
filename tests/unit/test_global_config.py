from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2] / "notebooks" / "00_common"))

from global_config import load_global_config, require_config_keys


def test_load_global_config_resolves_env(monkeypatch, tmp_path):
    monkeypatch.setenv("IKEA_ENV", "dev")
    p = tmp_path / "global_config.yaml"
    p.write_text("organization:\n  environment: ${IKEA_ENV}\n", encoding="utf-8")

    cfg = load_global_config(str(p))
    assert cfg["organization"]["environment"] == "dev"


def test_load_global_config_env_default_used_when_var_missing(monkeypatch, tmp_path):
    monkeypatch.delenv("MISSING_VAR_XYZ", raising=False)
    p = tmp_path / "global_config.yaml"
    p.write_text("org:\n  env: ${MISSING_VAR_XYZ:prod}\n", encoding="utf-8")
    cfg = load_global_config(str(p))
    assert cfg["org"]["env"] == "prod"


def test_load_global_config_raises_file_not_found():
    with pytest.raises(FileNotFoundError, match="Global config not found"):
        load_global_config("/tmp/does_not_exist_xyz_abc.yaml")


def test_load_global_config_raises_on_invalid_yaml(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("key: [\nunclosed bracket\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid YAML"):
        load_global_config(str(p))


def test_load_global_config_empty_file_returns_dict(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("", encoding="utf-8")
    cfg = load_global_config(str(p))
    # Empty YAML file returns config with lifecycle_log_table default
    assert "lifecycle_log_table" in cfg
    assert cfg["lifecycle_log_table"] == "audit.entity_lifecycle_log"


# ─── require_config_keys ────────────────────────────────────────────────────


def test_require_config_keys_passes_when_all_present():
    cfg = {"audit": {"pipeline_runs_table": "main.audit.runs"}, "organization": {"framework_name": "ikea"}}
    require_config_keys(cfg, ["audit.pipeline_runs_table", "organization.framework_name"])  # No exception.


def test_require_config_keys_raises_on_missing_nested_key():
    cfg = {"audit": {}}
    with pytest.raises(ValueError, match="audit.pipeline_runs_table"):
        require_config_keys(cfg, ["audit.pipeline_runs_table"])


def test_require_config_keys_raises_on_empty_value():
    cfg = {"audit": {"pipeline_runs_table": ""}}
    with pytest.raises(ValueError, match="audit.pipeline_runs_table"):
        require_config_keys(cfg, ["audit.pipeline_runs_table"])


def test_require_config_keys_raises_on_missing_top_level_key():
    with pytest.raises(ValueError, match="organization.framework_name"):
        require_config_keys({}, ["organization.framework_name"])


def test_require_config_keys_lists_all_missing():
    cfg = {}
    with pytest.raises(ValueError) as exc_info:
        require_config_keys(cfg, ["a.b", "c.d", "e.f"])
    msg = str(exc_info.value)
    assert "a.b" in msg
    assert "c.d" in msg
    assert "e.f" in msg
