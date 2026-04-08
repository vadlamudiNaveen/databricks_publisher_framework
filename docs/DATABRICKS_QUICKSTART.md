# Databricks Quick Start

This is the fastest path to get the framework running in Databricks with automated setup.

## 1. Update Configuration

Edit these files:
- `config/global_config.yaml`
- `config/source_registry.csv`

Minimum required values:
- Databricks catalog/schema names
- checkpoint and schema tracking roots
- source paths / API/JDBC profile references

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

## 5. Verify

Expected outcomes:
- Catalog and schemas exist
- control and audit tables exist
- dry-run succeeds
- runtime pipeline executes active sources

## Operational Model

Purpose of the three orchestration notebooks:
- `initialize_framework.py`: infra provisioning only
- `setup_wizard.py`: guided setup + validation
- `framework_orchestrator.py`: recurring runtime processing
