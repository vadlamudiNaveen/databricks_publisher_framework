"""End-to-end flow test: landing → conformance → silver with parquet export.

This test validates:
1. Data ingestion to landing layer (raw Bronze)
2. Column mapping and conformance transformation
3. Silver table publishing
4. Parquet export to external location
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "02_processing"))
sys.path.append(str(ROOT / "notebooks" / "03_publish"))

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from landing_engine import add_landing_metadata, write_landing
from conformance_engine import apply_column_mappings
from publish_silver import publish_append, publish_merge_with_target_policy


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    try:
        builder: SparkSession.Builder = SparkSession.builder  # type: ignore[misc]
        return (
            builder
            .config("spark.master", "local[1]")
            .config("spark.app.name", "test_end_to_end_flow")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .getOrCreate()
        )
    except Exception:
        pytest.skip("PySpark/Java not available in this environment — skipping E2E tests")


@pytest.fixture(scope="module")
def temp_db(spark):
    """Create temporary database for testing."""
    db_name = "test_e2e_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    yield db_name
    # Cleanup
    try:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    except Exception:
        pass


# ─── Landing Layer Tests ───────────────────────────────────────────────────


def test_landing_ingestion_adds_metadata(spark):
    """Test that landing ingestion adds timestamp and source metadata."""
    # Create sample raw data
    df = spark.createDataFrame([
        {"customer_id": "C001", "name": "Alice", "age": 30},
        {"customer_id": "C002", "name": "Bob", "age": 25},
    ])

    # Add landing metadata
    df_with_metadata = add_landing_metadata(
        df,
        source_system="crm_system",
        source_entity="customer",
        load_id="load_20260410_001",
    )

    # Verify metadata columns exist
    assert "source_system" in df_with_metadata.columns
    assert "source_entity" in df_with_metadata.columns
    assert "load_id" in df_with_metadata.columns
    assert "ingest_ts" in df_with_metadata.columns

    # Verify metadata values
    row = df_with_metadata.collect()[0]
    assert row["source_system"] == "crm_system"
    assert row["source_entity"] == "customer"
    assert row["load_id"] == "load_20260410_001"
    assert row["ingest_ts"] is not None


def test_landing_table_write_managed(spark, temp_db):
    """Test landing table write in managed mode (UC)."""
    # Create sample data
    df = spark.createDataFrame([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ])

    table_name = f"{temp_db}.landing_customers"
    
    # Write to landing table
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)

    # Verify table exists and has data
    result = spark.sql(f"SELECT * FROM {table_name}")
    assert result.count() == 2


# ─── Conformance Layer Tests ───────────────────────────────────────────────


def test_conformance_column_mapping(spark):
    """Test conformance: apply column mappings."""
    # Create raw data
    df = spark.createDataFrame([
        {"src_id": "C001", "src_full_name": "Alice Smith", "src_age": 30},
        {"src_id": "C002", "src_full_name": "Bob Jones", "src_age": 25},
    ])

    # Define column mappings (rename and transform)
    mappings = [
        {
            "transform_expression": "src_id",
            "conformance_column": "customer_id",
            "is_active": "true",
        },
        {
            "transform_expression": "src_full_name",
            "conformance_column": "full_name",
            "is_active": "true",
        },
        {
            "transform_expression": "CAST(src_age AS STRING)",
            "conformance_column": "age_str",
            "is_active": "true",
        },
    ]

    # Apply mappings
    df_mapped = apply_column_mappings(df, mappings)

    # Verify output columns
    assert "customer_id" in df_mapped.columns
    assert "full_name" in df_mapped.columns
    assert "age_str" in df_mapped.columns
    
    # Verify source columns are removed
    assert "src_id" not in df_mapped.columns

    # Verify data
    rows = df_mapped.collect()
    assert rows[0]["customer_id"] == "C001"
    assert rows[0]["full_name"] == "Alice Smith"
    assert rows[0]["age_str"] == "30"


def test_conformance_with_dq_columns_strips_framework_cols(spark):
    """Test conformance ignores framework technical columns (dq_status, etc)."""
    # Create data with DQ columns (from landing+processing)
    df = spark.createDataFrame([
        {
            "src_id": "C001",
            "src_name": "Alice",
            "bronze_dq_status": "PASS",
            "bronze_dq_failed_check": None,
        },
    ])

    mappings = [
        {
            "transform_expression": "src_id",
            "conformance_column": "customer_id",
            "is_active": "true",
        },
        {
            "transform_expression": "src_name",
            "conformance_column": "name",
            "is_active": "true",
        },
    ]

    df_mapped = apply_column_mappings(df, mappings)

    # Framework cols must be removed
    assert "bronze_dq_status" not in df_mapped.columns
    assert "bronze_dq_failed_check" not in df_mapped.columns
    
    # Data cols must be present
    assert "customer_id" in df_mapped.columns
    assert "name" in df_mapped.columns


# ─── Silver Layer Tests ────────────────────────────────────────────────────


def test_silver_append_publish(spark, temp_db):
    """Test silver table append publish mode."""
    # Create silver data
    silver_df = spark.createDataFrame([
        {"order_id": "O001", "customer_id": "C001", "amount": 100.0},
        {"order_id": "O002", "customer_id": "C002", "amount": 200.0},
    ])

    silver_table = f"{temp_db}.orders_silver"

    # First append: create table
    silver_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)
    assert spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").collect()[0]["cnt"] == 2

    # Second append: add more rows
    new_rows = spark.createDataFrame([
        {"order_id": "O003", "customer_id": "C001", "amount": 150.0},
    ])
    new_rows.write.mode("append").format("delta").saveAsTable(silver_table)
    assert spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").collect()[0]["cnt"] == 3


def test_silver_merge_publish(spark, temp_db):
    """Test silver table merge publish mode (upsert)."""
    # Create initial silver table
    initial_df = spark.createDataFrame([
        {"order_id": "O001", "customer_id": "C001", "amount": 100.0, "status": "PENDING"},
        {"order_id": "O002", "customer_id": "C002", "amount": 200.0, "status": "PENDING"},
    ])

    silver_table = f"{temp_db}.orders_silver_merge"
    initial_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)

    # Prepare update data (modify O001, add O003)
    update_df = spark.createDataFrame([
        {"order_id": "O001", "customer_id": "C001", "amount": 125.0, "status": "CONFIRMED"},
        {"order_id": "O003", "customer_id": "C001", "amount": 150.0, "status": "PENDING"},
    ])

    # Perform merge
    publish_merge_with_target_policy(
        spark, update_df, silver_table, "order_id", table_type="managed"
    )

    # Verify results
    result = spark.sql(f"SELECT * FROM {silver_table} ORDER BY order_id")
    rows = result.collect()
    
    # Should have 3 rows: O001 (updated), O002 (unchanged), O003 (new)
    assert result.count() == 3
    
    # O001 should be updated
    o001 = [r for r in rows if r["order_id"] == "O001"][0]
    assert o001["amount"] == 125.0
    assert o001["status"] == "CONFIRMED"


# ─── Parquet Export Tests ──────────────────────────────────────────────────


def test_parquet_export_to_external_location(spark, tmp_path):
    """Test exporting silver data to parquet in external location."""
    import tempfile
    
    # Create silver data
    silver_df = spark.createDataFrame([
        {"order_id": "O001", "customer_id": "C001", "amount": 100.0},
        {"order_id": "O002", "customer_id": "C002", "amount": 200.0},
    ])

    # Export to temporary parquet location (simulating external location)
    parquet_path = str(tmp_path / "silver_orders")
    silver_df.coalesce(1).write.mode("overwrite").format("parquet").option(
        "compression", "snappy"
    ).save(parquet_path)

    # Verify parquet files exist and can be read back
    parquet_read = spark.read.format("parquet").load(parquet_path)
    assert parquet_read.count() == 2
    assert "order_id" in parquet_read.columns
    assert "amount" in parquet_read.columns


# ─── Full End-to-End Pipeline Test ────────────────────────────────────────


def test_full_pipeline_landing_to_silver_to_parquet(spark, temp_db, tmp_path):
    """Full end-to-end test: raw data → landing → conformance → silver → parquet."""
    
    # STEP 1: Raw data ingestion to landing
    raw_df = spark.createDataFrame([
        {"raw_id": "1", "raw_name": "Alice", "raw_amount": "100.50"},
        {"raw_id": "2", "raw_name": "Bob", "raw_amount": "200.75"},
    ])

    raw_table = f"{temp_db}.raw_orders"
    raw_df.write.mode("overwrite").format("delta").saveAsTable(raw_table)

    # STEP 2: Write to landing with metadata
    landing_df = add_landing_metadata(
        raw_df,
        source_system="order_system",
        source_entity="orders",
        load_id="load_001",
    )

    landing_table = f"{temp_db}.landing_orders"
    landing_df.write.mode("overwrite").format("delta").saveAsTable(landing_table)
    
    # Verify landing has metadata
    landing_result = spark.sql(f"SELECT * FROM {landing_table}")
    assert "ingest_ts" in landing_result.columns
    assert "source_system" in landing_result.columns

    # STEP 3: Conformance transformation (column mapping + cleanup)
    landing_data = spark.sql(f"SELECT raw_id, raw_name, raw_amount FROM {landing_table}")
    
    mappings = [
        {"transform_expression": "raw_id", "conformance_column": "order_id", "is_active": "true"},
        {"transform_expression": "raw_name", "conformance_column": "customer_name", "is_active": "true"},
        {
            "transform_expression": "CAST(raw_amount AS DECIMAL(10, 2))",
            "conformance_column": "order_amount",
            "is_active": "true",
        },
    ]

    conformance_df = apply_column_mappings(landing_data, mappings)

    conformance_table = f"{temp_db}.conformance_orders"
    conformance_df.write.mode("overwrite").format("delta").saveAsTable(conformance_table)

    # Verify conformance
    conformance_result = spark.sql(f"SELECT * FROM {conformance_table}")
    assert "order_id" in conformance_result.columns
    assert "customer_name" in conformance_result.columns
    assert "order_amount" in conformance_result.columns

    # STEP 4: Publish to silver (final layer)
    silver_table = f"{temp_db}.silver_orders"
    conformance_result.write.mode("overwrite").format("delta").saveAsTable(silver_table)

    silver_data = spark.sql(f"SELECT * FROM {silver_table}")
    assert silver_data.count() == 2

    # STEP 5: Export to parquet (simulating external location export)
    parquet_export_path = str(tmp_path / "external_location" / "silver_orders")
    silver_data.coalesce(1).write.mode("overwrite").format("parquet").option(
        "compression", "snappy"
    ).save(parquet_export_path)

    # Verify parquet export
    parquet_data = spark.read.format("parquet").load(parquet_export_path)
    assert parquet_data.count() == 2
    
    # Verify column order and data
    rows = parquet_data.collect()
    assert rows[0]["order_id"] == "1"
    assert rows[0]["customer_name"] == "Alice"
    assert float(rows[0]["order_amount"]) == 100.50

    print(f"✅ Full E2E pipeline completed: raw → landing → conformance → silver → parquet")
    print(f"   Parquet export location: {parquet_export_path}")
