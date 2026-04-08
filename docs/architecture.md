# Architecture

## Pattern
Metadata-driven medallion-style framework:
1. Ingestion adapters by source type (FILE, JDBC, API)
2. Landing raw table (1:1 source)
3. Conformance from mappings
4. DQ evaluation from rules
5. Silver publish with append or merge
6. Audit logging

## IKEA Global Config Model
- All runtime values are externalized in config/global_config.yaml and environment variables.
- Connection settings are profile-driven (jdbc_profiles, api_profiles, file_defaults).
- Metadata can be loaded from CSV or Databricks control tables without code changes.
- Framework code is reusable across product domains and entities by metadata only.

## Key Databricks Features
- Auto Loader for file ingestion
- Delta tables for Landing/Conformance/Silver
- Lakeflow pipeline for recurring runtime orchestration
- Databricks Jobs for one-time setup orchestration (`initialize_framework` and `setup_wizard`)
- Unity Catalog for governance

## Execution Model
- Recurring runtime: run `notebooks/05_orchestration/framework_orchestrator.py` via Lakeflow pipeline.
- One-time setup: run `notebooks/05_orchestration/initialize_framework.py` and `notebooks/05_orchestration/setup_wizard.py` via Databricks Jobs or notebook execution.
- Setup jobs template is available at `pipelines/databricks_setup_jobs.json`.
