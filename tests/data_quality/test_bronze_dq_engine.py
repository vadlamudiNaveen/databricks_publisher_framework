from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "02_processing"))

from bronze_dq_engine import bronze_dq_config, validate_bronze_dq_config


def test_bronze_dq_config_defaults_to_empty_dict():
    assert bronze_dq_config(None) == {}
    assert bronze_dq_config({}) == {}
    assert bronze_dq_config({"bronze_dq": "bad"}) == {}


def test_validate_bronze_dq_config_detects_invalid_shapes():
    cfg = {
        "required_columns": "id",
        "expected_types": [],
        "null_thresholds": {"email": 2},
        "volume": "bad",
    }

    errors = validate_bronze_dq_config(cfg)
    assert any("required_columns must be a list" in e for e in errors)
    assert any("expected_types must be an object" in e for e in errors)
    assert any("null_thresholds.email must be between 0 and 1" in e for e in errors)
    assert any("volume must be an object" in e for e in errors)


def test_validate_bronze_dq_config_allows_supported_shapes():
    cfg = {
        "required_columns": ["id", "status"],
        "expected_columns": ["id", "status", "email"],
        "non_nullable_columns": ["id"],
        "expected_types": {"id": "string", "status": "string"},
        "regex_patterns": {"email": "^[^@]+@[^@]+$"},
        "allowed_values": {"status": ["OPEN", "CLOSED"]},
        "length_constraints": {"id": {"min": 1, "max": 20}},
        "range_constraints": {"amount": {"min": 0}},
        "null_thresholds": {"email": 0.1},
        "freshness": {"column": "event_ts", "max_age_hours": 24},
        "volume": {"min_rows": 1, "max_rows": 1000},
        "logical_checks": ["start_date <= end_date"],
        "uniqueness_constraints": ["id"],
    }

    assert validate_bronze_dq_config(cfg) == []