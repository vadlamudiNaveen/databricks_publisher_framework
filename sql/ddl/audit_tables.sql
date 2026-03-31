-- Audit tables for Databricks Ingestion Framework
-- Default catalog: main  |  Audit schema: audit  |  DQ schema: dq
-- To change catalog, update the USE CATALOG statement below.
-- To change schemas, find-replace 'audit.' and 'dq.' in this file.

CREATE CATALOG IF NOT EXISTS main;
USE CATALOG main;

CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS dq;

-- 1) Pipeline run-level audit (aligned with notebooks/04_audit/audit_logger.py)
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
  pipeline_name STRING,
  run_id STRING,
  source_system STRING,
  source_entity STRING,
  rows_in BIGINT,
  rows_out BIGINT,
  status STRING,
  error_message STRING,
  run_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);

-- 2) Standard DQ summary results (from dq_engine)
CREATE TABLE IF NOT EXISTS audit.dq_rule_results (
  run_id STRING,
  source_system STRING,
  source_entity STRING,
  dq_status STRING,
  dq_failed_rule STRING,
  row_count BIGINT,
  run_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);

-- 3) Bronze DQ check results (from bronze_dq_engine)
CREATE TABLE IF NOT EXISTS audit.bronze_dq_results (
  run_id STRING,
  source_system STRING,
  source_entity STRING,
  validation_type STRING,
  validation_status STRING,
  error_details STRING,
  timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);

-- 4) Structured validation events (stage-level verification logs)
CREATE TABLE IF NOT EXISTS audit.validation_events (
  run_id STRING,
  source_system STRING,
  source_entity STRING,
  event_name STRING,
  event_status STRING,
  event_payload STRING,
  event_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);

-- 5) Reject rows table (aligned with config/global_config.yaml -> audit.rejects_table)
-- NOTE: write_reject_rows appends all reject_df columns + run_id/source_system/source_entity/rejected_ts.
-- Keep this table schema-flexible by enabling schema evolution when writing if needed.
CREATE TABLE IF NOT EXISTS dq.rejects (
  run_id STRING,
  source_system STRING,
  source_entity STRING,
  rejected_ts TIMESTAMP,
  dq_status STRING,
  dq_failed_rule STRING,
  payload_json STRING
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);
