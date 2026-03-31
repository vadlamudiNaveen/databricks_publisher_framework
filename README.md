# IKEA Databricks Ingestion Framework


**What is this?**

This framework helps you move and process data from different sources (like files, databases, or APIs) into Databricks in a repeatable, automated way. It uses a "metadata-driven" approach, which means you control what happens by editing configuration files (not by writing lots of new code). This makes it easy to onboard new data sources or change behavior by just updating a spreadsheet or YAML file.

**For those new to Databricks or metadata-driven pipelines:**
- **Databricks** is a cloud platform for big data analytics and machine learning, built on Apache Spark. It lets you run data processing jobs at scale.
- **Metadata-driven** means you describe your data sources, rules, and processing steps in config files (like CSV or YAML), not in code. The framework reads these files and does the work for you.
- **Why is this useful?** You can add new data sources, change rules, or update processing logic without needing to be a Python or Spark expert—just update the configs and rerun the pipeline.

This framework provides reusable engines for different data types (files, databases, APIs), so you don't have to reinvent the wheel for each new source. It is designed to be easy to extend, test, and operate, even for teams with mixed technical backgrounds.

## Goals
- Configuration-first onboarding
- Global config-first runtime (no hardcoded environment values)
- Reusable ingestion, conformance, DQ, publish, and audit engines
- 1:1 Landing/Bronze raw preservation
- Minimal source-specific custom code
- Multi-product, multi-entity onboarding with shared framework code

## Repository Layout
- `config/`: metadata control files
- `config/global_config.yaml`: centralized runtime and Databricks connection configuration
- `config/.env.example`: environment variable template for all deploy-specific values
- `notebooks/`: reusable Python modules matching Databricks notebook responsibilities
- `sql/`: DDL and merge templates
- `pipelines/`: Lakeflow pipeline configuration placeholder
- `scripts/`: utility scripts (metadata validation)
- `tests/`: unit, integration, and DQ tests
- `docs/`: architecture, onboarding, runbook

## Quick Start
1. Create schemas, control tables, and audit tables using scripts in `sql/ddl/`.
2. Set environment values from `config/.env.example`.
3. Update `config/global_config.yaml` for your environment and metadata mode (`csv` or `table`).
4. Load CSVs from `config/` into control tables if using table mode.
5. Configure pipeline behavior in `pipelines/lakeflow_pipeline.json`.
6. Execute dry-run orchestration: `python notebooks/05_orchestration/framework_orchestrator.py`.
7. In Databricks runtime, run with Spark enabled: `python notebooks/05_orchestration/framework_orchestrator.py --execute`.

## Run And Verify

### Local Verification (No Spark Required)
1. Activate environment:
	- `source .venv/bin/activate`
2. Run unit tests:
	- `python -m pytest -q tests/unit`
	- Verifies core module behavior (config loaders, validators, utilities).
3. Run full test suite:
	- `python -m pytest -q tests`
	- Verifies unit + integration placeholders + DQ tests in this repository.
4. Validate metadata files:
	- `python scripts/validate_configs.py`
	- Checks required headers and semantic constraints (source type fields, JSON columns, merge keys).
5. Run orchestrator dry-run:
	- `python notebooks/05_orchestration/framework_orchestrator.py`
	- Produces a JSON execution plan from active metadata without Spark writes.
6. Run orchestrator dry-run for one source:
	- `python notebooks/05_orchestration/framework_orchestrator.py --product-name connect --source-system cemc --source-entity countryriskdet`
	- Useful to validate a single source onboarding before full runs.

### Databricks Execution (Spark Required)
- Command:
  - `python notebooks/05_orchestration/framework_orchestrator.py --execute`
- What it does:
  - Ingests active sources, writes landing/conformance, runs DQ, publishes to silver, and writes audit rows.

## Running the Orchestrator (CLI Options)

### Default: Run All File Path Sources (Dry Run)

```
python notebooks/05_orchestration/framework_orchestrator.py
```

### Run for a Specific Source

You can run the orchestrator for a specific source system and/or entity using:

```
python notebooks/05_orchestration/framework_orchestrator.py --source-system cemc --source-entity countryriskdet
```

You can also filter by product name:

```
python notebooks/05_orchestration/framework_orchestrator.py --product-name connect
```

### Execute with Spark (Databricks runtime)

```
python notebooks/05_orchestration/framework_orchestrator.py --execute
```

## Archiving Data in the Bronze Layer

The bronze (landing) layer is written using `add_landing_metadata()` and `write_landing()` in `landing_engine.py`. To support archiving, you can:

- Add `archive_mode` and `archive_table` (or path) to your config.
- Update `write_landing()` to optionally write to both the main and archive destinations based on config or a command-line flag.

This allows you to control archiving location and behavior without code changes. See `notebooks/02_processing/landing_engine.py` for extension points.

## Unity Catalog Prerequisites For `--execute`
`--execute` assumes target catalog/schemas/tables already exist and the runtime identity can read/write them.

### 1) Required Environment Variables
Set these before execution (from `config/.env.example`):
- `UC_CATALOG`
- `UC_BRONZE_SCHEMA`
- `UC_SILVER_SCHEMA`
- `UC_AUDIT_SCHEMA`
- `CHECKPOINT_ROOT`
- `SCHEMA_TRACKING_ROOT`

### 2) Required Global Config Values
Ensure these are set in `config/global_config.yaml`:
- `databricks.catalog`
- `databricks.bronze_schema`
- `databricks.silver_schema`
- `databricks.audit_schema`
- `audit.pipeline_runs_table`
- `audit.dq_results_table`
- `audit.rejects_table`

### 3) Required UC Objects
Create these before run:
1. Catalog: `UC_CATALOG`
2. Schemas:
	- `UC_BRONZE_SCHEMA`
	- `UC_SILVER_SCHEMA`
	- `UC_AUDIT_SCHEMA`
	- Control schema used by metadata tables if running `metadata.mode: table` (for example `control`).
3. Tables:
	- Landing/conformance/silver tables referenced in `config/source_registry.csv`
	- Audit table from `audit.pipeline_runs_table`
	- DQ results table from `audit.dq_results_table`
	- Rejects table from `audit.rejects_table`
	- Control tables (`source_registry`, `column_mapping`, `dq_rules`, `publish_rules`) if using table mode

### 4) Permissions
The Databricks job principal/service principal must have:
1. `USE CATALOG` on target catalog
2. `USE SCHEMA`, `CREATE TABLE`, `SELECT`, `INSERT`, `UPDATE`, `DELETE` on target schemas/tables as needed
3. Read/write access to checkpoint and schema tracking storage paths

## Full And Incremental Loads
- Use `load_type` in `config/source_registry.csv` with values `full` or `incremental`.
- FILE sources:
	- `full`: batch ingest of full path.
	- `incremental`: batch ingest with optional `incremental_start_timestamp` in `source_options_json`, or Auto Loader mode for continuous ingestion.
- JDBC/API sources:
	- `incremental`: uses `watermark_column` + `incremental_start_value` from `source_options_json`.
	- `full`: reads full source.

## Environment Parameterization
- `IKEA_ENV` defaults to `dev` via `config/global_config.yaml` (`${IKEA_ENV:dev}`).
- Allowed environments: dev, qa, prod.
- All environment-specific values (catalog/schema/paths/hosts/secrets) come from env variables and config files.

## Databricks Launchpad Widgets
- Widget options are centralized in `config/widget_options.yaml`.
- Notebook helper for widget creation: `notebooks/00_common/databricks_launchpad_widgets.py`.
- Default widget environment is set to `dev`.

## ENG511 Dev Dropzone Sources Onboarded
- `/mnt/dropzone/connect/cemccountryriskdet` (mixed JSON/JSONL)
- `/mnt/dropzone/connect/cemcitemmatplantrep` (mixed JSON/JSONL)
- `/mnt/dropzone/connect/cemcmssrepreceiver` (mixed JSON/JSONL, schema evolution)
- `/mnt/dropzone/connect/cemcmtrlftsdetail` (mixed JSON/JSONL)
- `/mnt/dropzone/pia/commondimensionsprd` (JSONL)
- `/mnt/dropzone/pia/itemsummarypublicprd` (complex JSON)

## Raw File Analysis
- Analysis helper: `notebooks/06_analysis/raw_json_jsonl_analysis.py`
- Use this to profile and sample raw payload from mounted dropzone paths before downstream conformance.

## Global Configuration Principles
- No environment-specific values are hardcoded in code.
- All connection details, catalogs, schemas, checkpoints, and profile settings are loaded from `config/global_config.yaml`.
- Source-specific behavior is controlled by metadata rows in `source_registry.csv`, `column_mapping.csv`, `dq_rules.csv`, and `publish_rules.csv`.
- Configuration placeholders like `${UC_CATALOG}` are resolved from environment variables at runtime.

## CI Checks
- GitHub Actions workflow: `.github/workflows/ci.yml`
- Checks executed:
	- `ruff check .`
	- `python scripts/validate_configs.py`
	- `pytest -q tests`

## Processing Flow
1. Source routing from `source_registry`
2. Ingestion adapter (Auto Loader/JDBC/API)
3. Landing write with technical metadata
4. Conformance from column mapping
5. DQ checks from rules metadata
6. Silver publish (append/merge)
7. Audit logging

## Notes
- Code is written as reusable Python modules and can be imported in Databricks notebooks/jobs.
- Add new IKEA products/entities by metadata only for standard ingestion patterns.
- Use custom hooks only for exceptional source-specific processing.

## Troubleshooting & Common Issues

- **Missing Environment Variables:** Ensure all required variables are set from `config/.env.example` before running any scripts.
- **Schema/Table Not Found:** Double-check that all required schemas and tables are created as per the prerequisites section.
- **Permission Errors:** Verify the Databricks principal has the necessary permissions on catalogs, schemas, and storage paths.
- **Config Validation Fails:** Run `python scripts/validate_configs.py` and review error messages for missing headers or invalid values.
- **PySpark/Spark Errors:** For Spark-related issues, ensure you are running in a Databricks or Spark-enabled environment when required.
- **Test Failures:** Run `pytest -v` for detailed output and check the `tests/` directory for test coverage and expected behaviors.

## Custom Hooks: Extending the Framework

For most sources, onboarding is metadata-driven. If you need to implement custom logic (e.g., special parsing, enrichment, or non-standard ingestion):

1. Create a new Python module in the appropriate `notebooks/` subfolder (e.g., `01_ingestion/` or `02_processing/`).
2. Implement your custom logic as a function or class.
3. Reference your custom hook in the relevant metadata (e.g., add a `custom_hook` column in `source_registry.csv` or use a config flag).
4. Document the hook in `docs/onboarding_guide.md` for future maintainers.

See the `notebooks/` folder for examples and patterns.

## Data Lineage, Monitoring & Change Tracking

- **Audit Tables:** All pipeline runs, DQ results, and rejects are logged in dedicated audit tables for traceability.
- **Log Table for Lifecycle Tracking:** A dedicated log table should be maintained to track changes and events throughout the lifecycle of each data entity, including onboarding, updates, and archival. This enables full traceability and auditability of all changes.
- **Execution Plan:** The orchestrator dry-run produces a JSON plan, which can be used for lineage tracking and debugging.
- **Monitoring:** Integrate with Databricks job monitoring and alerting for production pipelines. Consider extending audit logging for more granular lineage if needed.

## Handling Images and Attachments

The framework can be extended to support images and file attachments as part of the ingested data. To do this:
- Store image or attachment file paths or binary data in the source data or as references in the metadata.
- Update ingestion and processing logic to handle binary data or file references, ensuring files are stored in accessible locations (e.g., cloud storage, object store).
- Document the handling of images/attachments in the onboarding guide and update relevant config files to include these fields.

## Onboarding Tips for New Users

1. **Familiarize with Databricks basics:** Understand clusters, jobs, and workspace navigation.
2. **Read the onboarding guide:** See `docs/onboarding_guide.md` for step-by-step instructions.
3. **Start with local verification:** Run unit tests and config validation before attempting full pipeline runs.
4. **Use sample configs:** Begin with provided examples in `config/` and modify as needed.
5. **Ask for help:** If stuck, review the troubleshooting section or reach out to the maintainers listed in the repository.
