from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[2] / "notebooks" / "00_common"))

from config_loader import active_sources, load_csv_config, parse_json_list


def test_load_csv_config_reads_rows(tmp_path):
    p = tmp_path / "sample.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")
    rows = load_csv_config(str(p))
    assert rows == [{"a": "1", "b": "2"}]


def test_active_sources_filters_product(tmp_path):
    cfg_dir = tmp_path
    source_registry = cfg_dir / "source_registry.csv"
    source_registry.write_text(
        "source_system,source_entity,is_active,product_name\n"
        "hip,customer,true,customer360\n"
        "erp,product,true,product360\n",
        encoding="utf-8",
    )

    rows = active_sources(str(cfg_dir), product_name="customer360")
    assert len(rows) == 1
    assert rows[0]["source_system"] == "hip"


def test_parse_json_list_handles_array_and_invalid():
    assert parse_json_list('["a", "b"]') == ["a", "b"]
    assert parse_json_list('{"a":1}') == []
    assert parse_json_list("not-json") == []
