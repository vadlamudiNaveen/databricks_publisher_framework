-- Control table DDL
-- Default target: main.control
-- If needed, replace main/control with your target catalog/schema.

-- CREATE SCHEMA IF NOT EXISTS main.control;

/*
Run the statements below in Databricks SQL (Delta syntax).

CREATE TABLE IF NOT EXISTS control.source_registry (
  tenant STRING,
  brand STRING,
  region STRING,
  product_area STRING,
  product_name STRING,
  source_system STRING,
  source_entity STRING,
  source_type STRING,
  source_format STRING,
  source_path STRING,
  jdbc_profile STRING,
  jdbc_table STRING,
  api_profile STRING,
  api_endpoint STRING,
  api_method STRING,
  api_query_params_json STRING,
  load_type STRING,
  watermark_column STRING,
  landing_table STRING,
  conformance_table STRING,
  silver_table STRING,
  primary_key STRING,
  publish_mode STRING,
  is_active BOOLEAN,
  source_options_json STRING,
  pre_landing_transform_notebook STRING,
  post_conformance_transform_notebook STRING,
  custom_publish_notebook STRING,
  scheduler_name STRING,
  schedule_cron STRING,
  retention_days INT,
  sttm_profile STRING,
  landing_table_type STRING,
  landing_table_path STRING,
  conformance_table_type STRING,
  conformance_table_path STRING,
  silver_table_type STRING,
  silver_table_path STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS control.column_mapping (
  tenant STRING,
  brand STRING,
  product_name STRING,
  source_system STRING,
  source_entity STRING,
  source_column STRING,
  landing_column STRING,
  conformance_column STRING,
  silver_column STRING,
  source_type_name STRING,
  target_type_name STRING,
  nullable_flag BOOLEAN,
  default_value STRING,
  transform_expression STRING,
  is_active BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS control.dq_rules (
  tenant STRING,
  brand STRING,
  product_name STRING,
  source_system STRING,
  source_entity STRING,
  column_name STRING,
  rule_name STRING,
  rule_type STRING,
  rule_expression STRING,
  severity STRING,
  on_violation STRING,
  is_active BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS control.publish_rules (
  tenant STRING,
  brand STRING,
  product_name STRING,
  source_system STRING,
  source_entity STRING,
  silver_table STRING,
  publish_mode STRING,
  merge_key STRING,
  sequence_column STRING,
  delete_handling STRING,
  schema_evolution_flag BOOLEAN,
  optimize_zorder_json STRING,
  partition_columns_json STRING
) USING DELTA;
*/
