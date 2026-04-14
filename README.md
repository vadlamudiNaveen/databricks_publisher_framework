# Metadata-Driven Databricks Ingestion Framework

Onboard and run data pipelines in Databricks by editing metadata files, not framework code.

This README is written for a fresher in data engineering who wants to run the framework end-to-end without external help.

## Start Here First

If you are opening this project for the first time, do these 3 things:

1. Read docs/DATABRICKS_QUICKSTART.md once.
2. Run the copy-paste command block in "First Run (Databricks)" below.
3. Use "Verify After Run" SQL to confirm Bronze and Silver outputs.

## What This Framework Does

The framework processes source data in this flow:

1. Ingestion (read source files/JDBC/API)
2. Landing/Bronze write
3. Conformance transformation
4. DQ checks
5. Silver publish
6. Audit logging

You control behavior using metadata in config files.

## Prerequisites

Before running commands, ensure:

1. Python 3.10+ is installed.
2. Databricks CLI is installed and authenticated.
3. You are in repository root.
4. You have Unity Catalog permissions for target catalog/schemas.

Quick auth check:

```bash
databricks current-user me
```

## Key Files You Will Edit

1. config/global_config.yaml: environment and runtime config
2. config/source_registry.csv: source-level setup, source path, formats
3. config/column_mapping.csv: conformance mappings
4. config/dq_rules.csv: DQ rules
5. config/publish_rules.csv: silver publish behavior

## First Run (Databricks)

Run these commands in this exact order:

```bash
# 1) Validate metadata
python scripts/validate_configs.py

# 2) Generate notebook artifacts from Python modules
python3 scripts/generate_notebooks_ipynb.py

# 3) Deploy bundle to Databricks
databricks bundle deploy -t dev

# 4) One-time initialization (create/sync schemas and control metadata)
databricks bundle run -t dev framework_initialize_infrastructure_once

# 5) One-time setup validation
databricks bundle run -t dev framework_setup_wizard_once

# 6) Run orchestrator
databricks bundle run -t dev framework_orchestrator_runtime --no-wait
```

Track run status:

```bash
databricks jobs get-run <run_id> --output json
```

## How To Update Source Path Or Source Files

Use this when your raw data folder changes or you onboard a new file source.

### Step 1: Update source path metadata

Open config/source_registry.csv and update:

1. source_path
2. source_format (json/jsonl/delta/csv/parquet)
3. source_options_json (for recursive lookup, glob filters, autoloader)

Example source_path change:

- Before: ${RAW_CONNECT_ROOT:abfss://.../eng511/raw_data/connect/}
- After: ${RAW_CONNECT_ROOT:abfss://.../eng511/raw_data/connect_new/}

### Step 2: Optional env var override

Instead of hardcoding a full path in CSV, use env variable override:

```bash
export RAW_CONNECT_ROOT="abfss://rngpub@adlsdnapdevbronze.dfs.core.windows.net/eng511/raw_data/connect_new/"
```

### Step 3: Validate and execute

```bash
python scripts/validate_configs.py
python3 scripts/generate_notebooks_ipynb.py
databricks bundle deploy -t dev
databricks bundle run -t dev framework_initialize_infrastructure_once
databricks bundle run -t dev framework_orchestrator_runtime --no-wait
```

### Step 4: Test only one source (recommended)

```bash
python notebooks/05_orchestration/framework_orchestrator.py \
  --product-name connect \
  --source-system cemc \
  --source-entity countryriskdet
```

## Local Dry-Run (No Spark Writes)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

python scripts/validate_configs.py
python notebooks/05_orchestration/framework_orchestrator.py
```

## Verify After Run

Run these in Databricks SQL after orchestrator finishes.

```sql
-- Bronze table count
SELECT COUNT(*) AS bronze_count
FROM system.information_schema.tables
WHERE table_schema = 'bronze_dev'
  AND table_catalog = 'eng511_development_bronze';

-- Silver table count
SELECT COUNT(*) AS silver_count
FROM system.information_schema.tables
WHERE table_schema = 'silver'
  AND table_catalog = 'eng511_development_silver';

-- Bronze table list with row estimates
SELECT table_name, num_rows
FROM system.information_schema.tables
WHERE table_schema = 'bronze_dev'
  AND table_catalog = 'eng511_development_bronze'
ORDER BY table_name;

-- Silver table list with row estimates
SELECT table_name, num_rows
FROM system.information_schema.tables
WHERE table_schema = 'silver'
  AND table_catalog = 'eng511_development_silver'
ORDER BY table_name;
```

If num_rows is null, run direct COUNT(*) for specific tables.

## Common Problems And Fixes

1. Missing env variables
Set values from config/.env.example before deployment.

2. Table/schema not found
Run initialize and setup jobs once before orchestrator runtime.

3. Path not found
Check source_path in config/source_registry.csv and env var overrides.

4. Wrong output layer (for example silver under bronze catalog)
Check bundle variables in databricks.yml and resources/jobs.yml.

## Advanced Topics

Use these docs after you complete your first successful run:

1. docs/DATABRICKS_QUICKSTART.md
2. docs/DATABRICKS_SETUP_FULL.md
3. docs/onboarding_guide.md
4. docs/runbook.md
5. docs/architecture.md
6. docs/framework_reference.md
7. docs/adr/ADR-001-auto-loader-vs-dlt.md
8. docs/adr/ADR-003-orchestration-parallelism.md

## Design Principles

1. No hardcoded environment values
2. Metadata-driven behavior
3. Reusable framework engines
4. Audit and DQ visibility by default
