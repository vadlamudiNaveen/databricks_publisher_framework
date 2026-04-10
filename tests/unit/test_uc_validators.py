"""Tests for UC validators across landing_engine, conformance_engine, and publish_silver."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Import validators from all three engines
sys.path.append(str(ROOT / "notebooks" / "02_processing"))
sys.path.append(str(ROOT / "notebooks" / "03_publish"))

from landing_engine import (
    _validate_uc_table_name as landing_validate_uc,
    _validate_cloud_uri as landing_validate_uri,
    LandingConfigError,
)
from conformance_engine import (
    _validate_uc_table_name as conformance_validate_uc,
    _validate_cloud_uri as conformance_validate_uri,
    ConformanceConfigError,
)
from publish_silver import (
    _validate_uc_table_name as publish_validate_uc,
    _validate_cloud_uri as publish_validate_uri,
)


# ─── _validate_uc_table_name Tests ───────────────────────────────────────────


class TestValidateUcTableName:
    """Test UC table name validation consistency across all engines."""

    def test_landing_accepts_three_part_name(self):
        result = landing_validate_uc("main.bronze.customers", "table_name")
        assert result == "main.bronze.customers"

    def test_conformance_accepts_three_part_name(self):
        result = conformance_validate_uc("main.bronze.customers", "table_name")
        assert result == "main.bronze.customers"

    def test_publish_accepts_three_part_name(self):
        result = publish_validate_uc("main.silver.orders", "silver_table")
        assert result == "main.silver.orders"

    def test_landing_rejects_one_part_name(self):
        with pytest.raises(LandingConfigError, match="three-part name"):
            landing_validate_uc("customers", "table_name")

    def test_conformance_rejects_one_part_name(self):
        with pytest.raises(ConformanceConfigError, match="three-part name"):
            conformance_validate_uc("customers", "table_name")

    def test_publish_rejects_one_part_name(self):
        with pytest.raises(ValueError, match="three-part name"):
            publish_validate_uc("customers", "silver_table")

    def test_landing_rejects_two_part_name(self):
        with pytest.raises(LandingConfigError, match="three-part name"):
            landing_validate_uc("bronze.customers", "table_name")

    def test_conformance_rejects_two_part_name(self):
        with pytest.raises(ConformanceConfigError, match="three-part name"):
            conformance_validate_uc("bronze.customers", "table_name")

    def test_publish_rejects_two_part_name(self):
        with pytest.raises(ValueError, match="three-part name"):
            publish_validate_uc("bronze.customers", "silver_table")

    def test_landing_rejects_four_part_name(self):
        with pytest.raises(LandingConfigError, match="three-part name"):
            landing_validate_uc("main.bronze.schema.customers", "table_name")

    def test_landing_rejects_invalid_identifier_start(self):
        """Identifiers cannot start with digits."""
        with pytest.raises(LandingConfigError, match="three-part name"):
            landing_validate_uc("main.9bronze.customers", "table_name")

    def test_landing_rejects_special_characters(self):
        """Identifiers cannot contain special chars like hyphens."""
        with pytest.raises(LandingConfigError, match="three-part name"):
            landing_validate_uc("main.bronze-dev.customers", "table_name")

    def test_landing_rejects_spaces_in_identifier(self):
        with pytest.raises(LandingConfigError, match="three-part name"):
            landing_validate_uc("main.bronze dev.customers", "table_name")

    def test_landing_rejects_empty_string(self):
        with pytest.raises(LandingConfigError, match="required"):
            landing_validate_uc("", "table_name")

    def test_landing_rejects_none(self):
        with pytest.raises(LandingConfigError, match="required"):
            landing_validate_uc(None, "table_name")

    def test_landing_rejects_whitespace_only(self):
        with pytest.raises(LandingConfigError, match="required"):
            landing_validate_uc("   ", "table_name")

    def test_landing_accepts_underscores_and_numbers(self):
        """Valid identifiers can contain underscores and numbers (after first char)."""
        result = landing_validate_uc("main.bronze_2.cust_orders_v1", "table_name")
        assert result == "main.bronze_2.cust_orders_v1"

    def test_publish_rejects_sql_injection_pattern(self):
        """Ensure SQL injection attempts are blocked."""
        with pytest.raises(ValueError):
            publish_validate_uc("main.silver.orders; DROP TABLE main.silver.orders", "table")


# ─── _validate_cloud_uri Tests ───────────────────────────────────────────────


class TestValidateCloudUri:
    """Test cloud URI validation consistency across landing and conformance engines."""

    def test_landing_accepts_abfss_uri(self):
        uri = "abfss://rngpub@adlsdnapdevsilver.dfs.core.windows.net/eng511/landing"
        result = landing_validate_uri(uri, "external_path")
        assert result == uri

    def test_conformance_accepts_abfss_uri(self):
        uri = "abfss://rngpub@adlsdnapdevsilver.dfs.core.windows.net/eng511/conformance"
        result = conformance_validate_uri(uri, "external_path")
        assert result == uri

    def test_landing_accepts_s3_uri(self):
        result = landing_validate_uri("s3://my-bucket/data/path", "external_path")
        assert result == "s3://my-bucket/data/path"

    def test_conformance_accepts_gs_uri(self):
        result = conformance_validate_uri("gs://my-bucket/data", "external_path")
        assert result == "gs://my-bucket/data"

    def test_landing_rejects_mount_path(self):
        """Legacy mount paths (starting with /) are not cloud URIs."""
        with pytest.raises(LandingConfigError, match="cloud URI"):
            landing_validate_uri("/mnt/data/bronze", "external_path")

    def test_conformance_rejects_mount_path(self):
        with pytest.raises(ConformanceConfigError, match="cloud URI"):
            conformance_validate_uri("/mnt/data/bronze", "external_path")

    def test_landing_rejects_relative_path(self):
        """Relative paths are not cloud URIs."""
        with pytest.raises(LandingConfigError, match="cloud URI"):
            landing_validate_uri("data/bronze", "external_path")

    def test_landing_rejects_hdfs_path(self):
        """HDFS paths are not in the allowed cloud URI prefixes."""
        with pytest.raises(LandingConfigError, match="cloud URI"):
            landing_validate_uri("hdfs://namenode/data", "external_path")

    def test_landing_rejects_empty_string(self):
        with pytest.raises(LandingConfigError, match="required"):
            landing_validate_uri("", "external_path")

    def test_landing_rejects_none(self):
        with pytest.raises(LandingConfigError, match="required"):
            landing_validate_uri(None, "external_path")

    def test_landing_rejects_whitespace_only(self):
        with pytest.raises(LandingConfigError, match="required"):
            landing_validate_uri("   ", "external_path")

    def test_landing_case_insensitive_prefix_check(self):
        """Prefix check should be case-insensitive."""
        result = landing_validate_uri("ABFSS://bucket/path", "external_path")
        assert result == "ABFSS://bucket/path"

    def test_conformance_case_insensitive_prefix_check(self):
        result = conformance_validate_uri("S3://bucket/path", "external_path")
        assert result == "S3://bucket/path"

    def test_landing_rejects_local_filesystem_path(self):
        with pytest.raises(LandingConfigError, match="cloud URI"):
            landing_validate_uri("/var/data", "external_path")

    def test_conformance_rejects_file_protocol(self):
        """file:// is not a supported cloud URI."""
        with pytest.raises(ConformanceConfigError, match="cloud URI"):
            conformance_validate_uri("file:///data/local", "external_path")


# ─── Integration Tests: Config Builders ──────────────────────────────────────


class TestLandingConfigBuilderValidation:
    """Test that landing_engine.LandingWriteConfig.build validates correctly."""

    def test_landing_config_build_requires_three_part_table_name(self):
        from landing_engine import LandingWriteConfig
        with pytest.raises(LandingConfigError, match="three-part"):
            LandingWriteConfig.build(table_name="bronze.customers")

    def test_landing_config_build_requires_cloud_uri_for_external_table(self):
        from landing_engine import LandingWriteConfig
        with pytest.raises(LandingConfigError, match="cloud URI"):
            LandingWriteConfig.build(
                table_name="main.bronze.customers",
                table_type="external",
                external_path="/mnt/invalid",  # Not a cloud URI
            )

    def test_landing_config_build_accepts_valid_external_config(self):
        from landing_engine import LandingWriteConfig
        config = LandingWriteConfig.build(
            table_name="main.bronze.customers",
            table_type="external",
            external_path="abfss://bucket/path",
        )
        assert config.table_name == "main.bronze.customers"
        assert config.external_path == "abfss://bucket/path"

    def test_landing_config_build_archive_table_must_be_three_part(self):
        from landing_engine import LandingWriteConfig
        with pytest.raises(LandingConfigError, match="three-part"):
            LandingWriteConfig.build(
                table_name="main.bronze.customers",
                archive_table="bronze.archive",  # Only two-part
            )

    def test_landing_config_build_archive_table_accepts_valid_name(self):
        from landing_engine import LandingWriteConfig
        config = LandingWriteConfig.build(
            table_name="main.bronze.customers",
            archive_table="main.bronze.archive",
        )
        assert config.archive_table == "main.bronze.archive"


class TestConformanceEngineValidation:
    """Test that conformance_engine validates UC names in mapping rules."""

    def test_conformance_apply_column_mappings_with_valid_config(self):
        from conformance_engine import apply_column_mappings

        # This is a pure Python test (no Spark needed)
        mappings = [
            {"transform_expression": "src_col", "conformance_column": "dest_col", "is_active": "true"}
        ]
        # Just ensure no exception is raised during mapping rule validation
        # (actual DataFrame operations would require Spark)
        from conformance_engine import _build_mapping_rules
        rules = _build_mapping_rules(mappings)
        assert len(rules) == 1
        assert rules[0].conformance_column == "dest_col"


class TestPublishSilverValidation:
    """Test publish_silver validators in isolation."""

    def test_publish_merge_rejects_invalid_table_name(self):
        from publish_silver import publish_merge_with_target_policy
        from unittest.mock import MagicMock

        spark = MagicMock()
        df = MagicMock()
        
        with pytest.raises(ValueError, match="three-part"):
            publish_merge_with_target_policy(spark, df, "silver.orders", "order_id")

    def test_publish_merge_rejects_invalid_merge_key(self):
        from publish_silver import publish_merge_with_target_policy
        from unittest.mock import MagicMock

        spark = MagicMock()
        df = MagicMock()
        
        with pytest.raises(ValueError, match="valid SQL identifier"):
            publish_merge_with_target_policy(
                spark, df, "main.silver.orders", "id OR 1=1"  # SQL injection
            )

    def test_publish_merge_rejects_invalid_external_path(self):
        from publish_silver import publish_merge_with_target_policy
        from unittest.mock import MagicMock

        spark = MagicMock()
        df = MagicMock()
        
        with pytest.raises(ValueError, match="cloud URI"):
            publish_merge_with_target_policy(
                spark, df, "main.silver.orders", "order_id",
                table_type="external",
                external_path="/mnt/invalid"
            )

    def test_publish_append_rejects_invalid_table_name(self):
        from publish_silver import publish_append
        from unittest.mock import MagicMock

        df = MagicMock()
        
        with pytest.raises(ValueError, match="three-part"):
            publish_append(df, "silver.orders")

    def test_publish_append_accepts_valid_config(self):
        from publish_silver import publish_append
        from unittest.mock import MagicMock

        df = MagicMock()
        
        # This should not raise; it will fail on Spark operations but validates config
        try:
            publish_append(df, "main.silver.orders")
        except Exception as e:
            # We expect it to fail on Spark write, not on validation
            assert "three-part" not in str(e)
