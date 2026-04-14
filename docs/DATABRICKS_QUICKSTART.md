# Databricks Quick Start

This is the fastest path to get the framework running in Databricks with automated setup.

## 0. Databricks Asset Bundles (Recommended)

This repository now includes Databricks Asset Bundles files:
- `databricks.yml`
- `resources/jobs.yml`
- `resources/pipelines.yml`

Note: default bundle deployment path currently includes jobs only. The pipeline resource in `resources/pipelines.yml` requires Unity Catalog `CREATE TABLE` privileges on target schemas.

Target isolation defaults are preconfigured by bundle variables.
Use the values in databricks.yml for your environment.

Use bundle commands from the repository root:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

For direct CLI job triggers, use positional job ID syntax (Databricks CLI v0.296+):

```bash
databricks jobs run-now 193010758254845 -o json
```

Run one-time setup jobs:

```bash
databricks bundle run -t dev framework_initialize_infrastructure_once
databricks bundle run -t dev framework_setup_wizard_once
```

Run recurring orchestrator job:

```bash
databricks bundle run -t dev framework_orchestrator_runtime
```

Track run status:

```bash
databricks jobs get-run <run_id> --output json
```

Promote and run in prod target:

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod framework_orchestrator_runtime
```

## 1. Update Configuration

Edit these files:
- `config/global_config.yaml`
- `config/source_registry.csv`

Minimum required values:
- Databricks catalog/schema names
- checkpoint and schema tracking roots
- source paths / API/JDBC profile references

### Update A Source File Path

When a source path changes, update it in config/source_registry.csv.

1. Locate source row by product_name, source_system, source_entity.
2. Update source_path.
3. Verify source_format (json/jsonl/delta/csv/parquet).
4. Verify source_options_json (recursiveFileLookup/pathGlobFilter/file_ingest_mode).

Example:

- Before: ${RAW_CONNECT_ROOT:abfss://.../eng511/raw_data/connect/}
- After:  ${RAW_CONNECT_ROOT:abfss://.../eng511/raw_data/connect_new/}

Optional env override pattern:

```bash
export RAW_CONNECT_ROOT="abfss://rngpub@adlsdnapdevbronze.dfs.core.windows.net/eng511/raw_data/connect_new/"
```

Optional source metadata now supported in `source_registry.csv`:
- `scheduler_name`
- `schedule_cron`
- `retention_days`
- `sttm_profile`

## 2. Import Notebooks

Generate notebook artifacts if needed:

```bash
python scripts/generate_notebooks_ipynb.py
```

Import the `notebooks_ipynb/` folder into your Databricks workspace.

## 3. Run One-Time Setup

Use one of these options:

### Option A: Run setup wizard notebook

Run `notebooks/05_orchestration/setup_wizard` once.

### Option B: Create dedicated setup jobs

Use `pipelines/databricks_setup_jobs.json` to create one-time jobs for:
- `notebooks/05_orchestration/initialize_framework`
- `notebooks/05_orchestration/setup_wizard`

## 4. Run Recurring Pipeline

For recurring production runs, execute only:
- `notebooks/05_orchestration/framework_orchestrator`

`pipelines/lakeflow_pipeline.json` is already aligned to this runtime model.

## 5. Execute End-To-End (Copy/Paste)

```bash
python scripts/validate_configs.py
python3 scripts/generate_notebooks_ipynb.py
databricks bundle deploy -t dev
databricks bundle run -t dev framework_initialize_infrastructure_once
databricks bundle run -t dev framework_setup_wizard_once
databricks bundle run -t dev framework_orchestrator_runtime --no-wait
```

## 6. Verify

Expected outcomes:
- Catalog and schemas exist
- control and audit tables exist
- dry-run succeeds
- runtime pipeline executes active sources

Verification SQL:

```sql
SELECT COUNT(*) AS bronze_count
FROM system.information_schema.tables
WHERE table_schema = 'bronze_dev'
	AND table_catalog = 'eng511_development_bronze';

SELECT COUNT(*) AS silver_count
FROM system.information_schema.tables
WHERE table_schema = 'silver'
	AND table_catalog = 'eng511_development_silver';
```

## Operational Model

Purpose of the three orchestration notebooks:
- `initialize_framework.py`: infra provisioning only
- `setup_wizard.py`: guided setup + validation
- `framework_orchestrator.py`: recurring runtime processing
