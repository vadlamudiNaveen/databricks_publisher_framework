from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[2] / "scripts"))

from validate_configs import validate_csv_headers


def test_validate_csv_headers_passes_for_current_config():
    config_dir = Path(__file__).resolve().parents[2] / "config"
    assert validate_csv_headers(config_dir) == []


def test_validate_csv_headers_detects_invalid_json_and_missing_merge_key(tmp_path):
    cfg = tmp_path
    (cfg / "source_registry.csv").write_text(
        "tenant,brand,product_name,source_system,source_entity,source_type,load_type,landing_table,conformance_table,silver_table,publish_mode,is_active,source_path,source_options_json\n"
        "ikea,ikea,p1,s1,e1,FILE,incremental,l,c,s,append,true,/mnt/path,not-json\n",
        encoding="utf-8",
    )
    (cfg / "column_mapping.csv").write_text(
        "tenant,brand,product_name,source_system,source_entity,conformance_column,transform_expression,is_active\n"
        "ikea,ikea,p1,s1,e1,c1,col1,true\n",
        encoding="utf-8",
    )
    (cfg / "dq_rules.csv").write_text(
        "tenant,brand,product_name,source_system,source_entity,rule_name,rule_expression,is_active\n"
        "ikea,ikea,p1,s1,e1,r1,col1 is not null,true\n",
        encoding="utf-8",
    )
    (cfg / "publish_rules.csv").write_text(
        "tenant,brand,product_name,source_system,source_entity,silver_table,publish_mode,merge_key\n"
        "ikea,ikea,p1,s1,e1,main.silver.tbl,merge,\n",
        encoding="utf-8",
    )

    errors = validate_csv_headers(cfg)
    assert any("invalid source_options_json" in e for e in errors)
    assert any("merge_key is required" in e for e in errors)
