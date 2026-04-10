"""Tests for ingest_file_batch.py dataclasses and helpers."""
from unittest.mock import MagicMock
import pytest
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "01_ingestion"))

import ingest_file_batch as batch

def test_source_config_valid():
    source = {"source_path": "/data", "source_format": "json", "load_type": "full"}
    global_config = {"connections": {"file_defaults": {"format": "json"}}}
    cfg = batch.SourceConfig.from_dict(source, global_config)
    assert cfg.source_path == "/data"
    assert cfg.source_format == "json"
    assert cfg.load_type == "full"

def test_source_config_invalid_format():
    source = {"source_path": "/data", "source_format": "xlsx"}
    global_config = {"connections": {"file_defaults": {"format": "json"}}}
    with pytest.raises(batch.BatchFileIngestionConfigError):
        batch.SourceConfig.from_dict(source, global_config)

def test_control_options_defaults():
    opts = batch.ControlOptions.from_source_options(None)
    assert opts.fail_on_empty is True
    assert opts.copy_attachments is False

def test_resolve_batch_file_config_valid():
    source = {"source_path": "/data", "source_format": "json"}
    global_config = {"connections": {"file_defaults": {"format": "json"}}}
    opts = {"header": True, "delimiter": ","}
    resolved = batch.resolve_batch_file_config(source, global_config, opts)
    assert resolved.source.source_format == "json"
    assert resolved.reader_options["header"] == "true"
    assert resolved.reader_options["delimiter"] == ","

def test_resolve_batch_file_config_invalid_option():
    source = {"source_path": "/data", "source_format": "json"}
    global_config = {"connections": {"file_defaults": {"format": "json"}}}
    opts = {"bad_option": 1}
    with pytest.raises(batch.BatchFileIngestionConfigError):
        batch.resolve_batch_file_config(source, global_config, opts)

def test_apply_incremental_filter_skips_if_not_incremental():
    df = MagicMock()
    resolved = MagicMock()
    resolved.source.load_type = "full"
    result = batch._apply_incremental_filter(df, resolved)
    assert result is df

def test_apply_incremental_filter_skips_if_no_column():
    df = MagicMock()
    df.columns = ["a"]
    resolved = MagicMock()
    resolved.source.load_type = "incremental"
    resolved.control.incremental_start_timestamp = "2024-01-01T00:00:00"
    resolved.source.source_id = "sys.entity"
    resolved.source.spark_format = "json"
    result = batch._apply_incremental_filter(df, resolved)
    assert result is df

def test_collect_attachment_references_field_missing():
    df = MagicMock()
    df.columns = ["a"]
    with pytest.raises(batch.BatchFileIngestionConfigError):
        batch.collect_attachment_references(df, "missing")

@pytest.mark.skip(reason="Requires active PySpark SparkContext - test in Databricks notebook instead")
def test_collect_attachment_references_success():
    df = MagicMock()
    df.columns = ["a"]
    df.select.return_value.where.return_value.distinct.return_value.limit.return_value.collect.return_value = [
        {"a": "file1"}, {"a": "file2"}, {"a": None}
    ]
    refs = batch.collect_attachment_references(df, "a")
    assert refs == ["file1", "file2"]

@pytest.mark.skip(reason="Requires active PySpark SparkContext - test in Databricks notebook instead")
def test_copy_attachment_references_success():
    df = MagicMock()
    df.columns = ["a"]
    df.select.return_value.where.return_value.distinct.return_value.limit.return_value.collect.return_value = [
        {"a": "file1"}, {"a": "file2"}
    ]
    def handler(ref, dest):
        return f"{dest}/{ref}"
    copied = batch.copy_attachment_references(df, "a", "/tmp", handler)
    assert copied == [("file1", "/tmp/file1"), ("file2", "/tmp/file2")]

def test_copy_attachment_references_fail_on_error():
    df = MagicMock()
    df.columns = ["a"]
    df.select.return_value.where.return_value.distinct.return_value.limit.return_value.collect.return_value = [
        {"a": "file1"}, {"a": "file2"}
    ]
    def handler(ref, dest):
        if ref == "file2":
            raise Exception("fail")
        return f"{dest}/{ref}"
    with pytest.raises(Exception):
        batch.copy_attachment_references(df, "a", "/tmp", handler, fail_on_error=True)
