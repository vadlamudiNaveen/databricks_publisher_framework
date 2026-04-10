# Metadata-Driven Databricks Ingestion Framework

This document is the definitive technical reference for the **IKEA Metadata-Driven Ingestion Framework** — a reusable, configuration-first pipeline that ingests data from Files, JDBC databases, and REST APIs into a Delta lakehouse across Landing → Conformance → Silver layers.

## Overview

Instead of writing custom notebooks per source, sources are described in four CSV (or Delta control table) metadata files. The orchestrator reads those files and routes every source through the same set of shared engine modules.

### Quick Stats
- **4 metadata files** — `source_registry`, `column_mapping`, `dq_rules`, `publish_rules`
- **3 supported source types** — FILE (batch + Auto Loader streaming), JDBC, REST API
- **5 pipeline stages** — Ingest → Landing → Conformance → DQ → Silver
- **Full auditability** — every run writes to `audit.pipeline_runs`, `audit.dq_rule_results`, `dq.rejects`
- **Zero code required** for standard sources — metadata only
- **Exception hooks** — plug-in notebooks for non-standard transforms

---

## Architecture: 5-Stage Pipeline

```
┌──────────────────────────────────────────────────────────────────────┐
│  IKEA Metadata-Driven Ingestion Framework — End-to-End Pipeline      │
└──────────────────────────────────────────────────────────────────────┘

1️⃣  INGEST
    └─ FILE:  batch reader (_FORMAT_MAP) or Auto Loader streaming
    └─ JDBC:  Spark JDBC reader with optional parallel partitioned read
    └─ API:   requests.Session with bearer / api_key / basic auth
    └─ All modes add source_options_json config (pagination, schema hints, etc.)

2️⃣  LANDING (Bronze Raw — 1:1 Copy)
    └─ add_landing_metadata() appends:
         source_system, source_entity, load_id, ingest_ts
    └─ write_landing() → Delta append, mergeSchema=true
    └─ Source data is NEVER modified — Landing is the immutable replay point

3️⃣  CONFORMANCE (Bronze Standardised)
    └─ Bronze DQ checks run first (run_bronze_dq_checks)
    └─ apply_column_mappings() → rename, cast, transform per column_mapping.csv
    └─ Framework technical cols (_FRAMEWORK_COLS) stripped before silver
    └─ write_conformance() → Delta append, mergeSchema=true

4️⃣  DATA QUALITY
    └─ apply_dq_rules() evaluates ALL rules — rows marked dq_status=PASS/FAIL
    └─ ALL failing rule names accumulated (comma-separated) in dq_failed_rule
    └─ split_valid_reject() → valid_df + reject_df
    └─ write_dq_rule_results() logs summary counts
    └─ write_reject_rows() serialises payload to payload_json STRING

5️⃣  SILVER PUBLISH
    └─ publish_append()  — events / high-volume sources (mode: append)
    └─ publish_merge()   — master data / SCD (mode: merge)
         UUID temp view per merge (prevents cross-source collisions)
         SQL injection guard on merge_key and silver_table identifiers
    └─ apply_post_publish_actions() → OPTIMIZE [ZORDER BY (...)]
    └─ Audit rows written to audit.pipeline_runs
```

---

## Module Inventory

### `notebooks/00_common/` — Shared Utilities

| Module | Purpose |
|---|---|
| `global_config.py` | Loads `global_config.yaml`; resolves `${VAR:default}` env substitutions; `require_config_keys()` validator |
| `config_loader.py` | Reads CSV metadata files; module-level `_CSV_CACHE` (read-once per process); `${VAR:default}` env support; `clear_config_cache()` for tests |
| `utils.py` | `new_load_id()` UUID; `truncate_str(value, max_chars=500)`; `get_logger(name)` with JSON-structured output |
| `databricks_launchpad_widgets.py` | Databricks widget registration with YAML-driven defaults; local `dbutils` shim for testing |

### `notebooks/01_ingestion/` — Source Adapters

| Module | Key Functions | Source Type |
|---|---|---|
| `ingest_file_batch.py` | `ingest_file_batch()` | FILE — all formats via `_FORMAT_MAP` |
| `ingest_file_autoloader.py` | `ingest_file_stream()`, `write_file_stream_to_landing()` | FILE — Auto Loader |
| `ingest_jdbc.py` | `ingest_jdbc_batch()`, `_apply_parallel_read_options()` | JDBC |
| `ingest_api.py` | `ingest_api()`, `_extract_records()`, multi-auth, pagination | REST API |

### `notebooks/02_processing/` — Transform & DQ Engines

| Module | Key Functions |
|---|---|
| `landing_engine.py` | `add_landing_metadata()`, `write_landing()` |
| `conformance_engine.py` | `apply_column_mappings()`, `write_conformance()` |
| `dq_engine.py` | `apply_dq_rules()`, `split_valid_reject()` |
| `bronze_dq_engine.py` | `run_bronze_dq_checks()` — schema, volume, completeness, freshness, dedup |

### `notebooks/03_publish/`

| Module | Key Functions |
|---|---|
| `publish_silver.py` | `publish_append()`, `publish_merge()`, `apply_post_publish_actions()` |

### `notebooks/04_audit/`

| Module | Key Functions |
|---|---|
| `audit_logger.py` | `write_pipeline_audit()`, `write_dq_rule_results()`, `write_reject_rows()`, `write_validation_events()`, `write_bronze_dq_results()` |

### `notebooks/05_orchestration/`

| Module | Purpose |
|---|---|
| `framework_orchestrator.py` | Master runtime coordinator — reads all metadata, calls all engines, handles retry, writes audit |
| `initialize_framework.py` | One-time infrastructure provisioning for catalog, schemas, control and audit tables |
| `setup_wizard.py` | One-time setup workflow that verifies environment and runs end-to-end setup checks |

Runtime model:
- Recurring pipeline execution should run `framework_orchestrator.py`.
- One-time setup can run via `setup_wizard.py` or the job template in `pipelines/databricks_setup_jobs.json`.

---

## Metadata Reference

### 1. `config/source_registry.csv`

The primary control table. One row per source entity.

| Column | Required | Example | Notes |
|---|---|---|---|
| `source_system` | ✅ | `cemc` | Logical system name |
| `source_entity` | ✅ | `countryriskdet` | Entity / table name |
| `source_type` | ✅ | `FILE` / `JDBC` / `API` | Case-insensitive |
| `source_format` | FILE only | `json_jsonl_mixed` | See format map below |
| `source_path` | FILE / API | `abfss://raw@<storage-account>.dfs.core.windows.net/connect/...` | Cloud landing path |
| `jdbc_profile` | JDBC only | `ikea_erp` | Key in `global_config.yaml > connections.jdbc_profiles` |
| `jdbc_table` | JDBC only | `dbo.orders` | Table or subquery |
| `api_profile` | API only | `connect_api` | Key in `global_config.yaml > connections.api_profiles` |
| `api_endpoint` | API only | `/v1/data` | Relative path appended to base URL |
| `api_method` | API only | `GET` | HTTP verb |
| `load_type` | ✅ | `incremental` / `full` | Controls watermark injection |
| `watermark_column` | incremental | `file_modification_ts` | Used for JDBC predicate push-down |
| `landing_table` | ✅ | `${UC_CATALOG}.${UC_BRONZE_SCHEMA}.cemc_countryriskdet_landing` | Supports env var substitution |
| `conformance_table` | ✅ | `${UC_CATALOG}.${UC_BRONZE_SCHEMA}.cemc_countryriskdet_conformance` | |
| `silver_table` | ✅ | `${UC_CATALOG}.${UC_SILVER_SCHEMA}.cemc_countryriskdet` | |
| `primary_key` | ✅ | `record_id` | Used by DQ uniqueness rules |
| `publish_mode` | ✅ | `append` / `merge` | Drives silver write strategy |
| `is_active` | ✅ | `true` | Set to `false` to skip without deleting |
| `source_options_json` | optional | `{"file_ingest_mode":"batch","recursiveFileLookup":"true"}` | Passed to ingest adapter; also carries pagination settings for API |
| `pre_landing_transform_notebook` | optional | `path/to/notebook` | Called after ingest, before landing write |
| `post_conformance_transform_notebook` | optional | `path/to/notebook` | Called after conformance, before DQ |
| `custom_publish_notebook` | optional | `path/to/notebook` | Fully replaces publish stage |
| `scheduler_name` | optional | `daily_connect_job` | Scheduler metadata label per source |
| `schedule_cron` | optional | `0 0 2 * * ?` | Cron metadata per source |
| `retention_days` | optional | `30` | Retention metadata per source |
| `sttm_profile` | optional | `standard_transfer` | Transfer profile metadata per source |

**Supported FILE formats** (via `_FORMAT_MAP`):

| `source_format` | Spark reader format | Notes |
|---|---|---|
| `json` | `json` | Single-line or multiLine=true via source_options |
| `jsonl` | `json` | Newline-delimited |
| `json_jsonl_mixed` | `json` | Auto-detects per file |
| `csv` | `csv` | |
| `parquet` | `parquet` | |
| `avro` | `avro` | Requires spark-avro jar |
| `orc` | `orc` | |
| `text` | `text` | |
| `binary` | `binaryFile` | |

---

### 2. `config/column_mapping.csv`

One row per column transformation. Drives the entire Conformance stage.

| Column | Required | Example | Notes |
|---|---|---|---|
| `source_system` | ✅ | `hip` | Must match source_registry |
| `source_entity` | ✅ | `customer` | |
| `source_column` | ✅ | `Cust_ID` | Original column name in source data |
| `conformance_column` | ✅ | `customer_id` | Output column name written to conformance/silver |
| `transform_expression` | ✅ | `trim(Cust_ID)` | Valid Spark SQL expression. Aliased to `conformance_column` |
| `target_type_name` | optional | `date` | Informational; casting done inside `transform_expression` |
| `nullable_flag` | optional | `false` | Informational; enforced via DQ rules |
| `is_active` | ✅ | `true` | Inactive rows are skipped |

**Expression examples**:
```
trim(Cust_ID)                           → whitespace removal
to_date(DOB,'yyyy-MM-dd')              → string to date
cast(PROD_ID as bigint)                → type cast
upper(status)                          → normalise casing
coalesce(amount, 0)                    → null-safe default
date_format(order_ts,'yyyy-MM-dd')     → format extraction
```

> **Note**: If no active mapping rows exist for a source entity, `apply_column_mappings()` returns the source DataFrame with framework technical columns (`source_system`, `source_entity`, `load_id`, `ingest_ts`, `dq_status`, `dq_failed_rule`) stripped. A `warnings.warn` is emitted to surface the misconfiguration.

---

### 3. `config/dq_rules.csv`

One row per quality rule. Applied by `apply_dq_rules()` in order.

| Column | Required | Example | Notes |
|---|---|---|---|
| `source_system` | ✅ | `hip` | |
| `source_entity` | ✅ | `customer` | |
| `column_name` | ✅ | `customer_id` | Informational label |
| `rule_name` | ✅ | `NOT_NULL` | Appears in `dq_failed_rule`; should be unique per entity |
| `rule_type` | optional | `expression` | Informational |
| `rule_expression` | ✅ | `customer_id IS NOT NULL` | Valid Spark SQL passed to `F.expr()` |
| `severity` | optional | `high` / `medium` | Informational in current version |
| `on_violation` | optional | `reject` | Informational; all violations produce FAIL status |
| `is_active` | ✅ | `true` | |

**How DQ works**:
1. Every row starts as `dq_status = 'PASS'`, `dq_failed_rule = NULL`
2. For each active rule: `failed_condition = NOT(rule_expression)`
3. If failed: `dq_status` → `'FAIL'`; `dq_failed_rule` accumulates the `rule_name` (comma-separated if multiple rules fail)
4. `split_valid_reject()` splits the DataFrame — valid rows go to Silver, failing rows go to `dq.rejects`

**Rule expression examples**:
```sql
customer_id IS NOT NULL
date_of_birth IS NULL OR date_of_birth <= current_date()
status IN ('OPEN','CLOSED','PENDING')
amount > 0
length(postcode) = 5
record_id IS NOT NULL AND length(trim(record_id)) > 0
```

---

### 4. `config/publish_rules.csv`

One row per source entity, controls how silver is written.

| Column | Required | Example | Notes |
|---|---|---|---|
| `source_system` | ✅ | `hip` | |
| `source_entity` | ✅ | `customer` | |
| `silver_table` | ✅ | `${UC_CATALOG}.${UC_SILVER_SCHEMA}.customer` | |
| `publish_mode` | ✅ | `merge` / `append` | |
| `merge_key` | merge only | `customer_id` | Must be a safe SQL identifier (letters, digits, `_`, `.` only) |
| `sequence_column` | optional | `ingest_ts` | Used for ordering in merge scenarios |
| `delete_handling` | optional | `soft_delete` / `ignore` | Informational |
| `schema_evolution_flag` | optional | `true` | |
| `optimize_zorder_json` | optional | `["customer_id"]` | JSON array of columns for ZORDER |
| `partition_columns_json` | optional | `["ingest_date"]` | JSON array of partition columns |

---

## Configuration: `config/global_config.yaml`

All runtime settings live here. All values support `${ENV_VAR}` and `${ENV_VAR:default}` substitution.

### Key Sections

```yaml
organization:
  client_name: IKEA
  environment: ${IKEA_ENV:dev}          # dev | qa | prod

execution:
  default_mode: dry_run                  # dry_run | live
  fail_fast: true                        # Abort pipeline on first source error
  max_retries: 2
  retry_backoff_seconds: 2
  post_publish_optimize: true            # Run OPTIMIZE after each silver write
  retry_by_source_type:
    api:
      max_retries: 3
      backoff_seconds: 3

databricks:
  catalog: ${UC_CATALOG}
  bronze_schema: ${UC_BRONZE_SCHEMA}
  silver_schema: ${UC_SILVER_SCHEMA}
  audit_schema: ${UC_AUDIT_SCHEMA}
  checkpoint_root: ${CHECKPOINT_ROOT}    # Auto Loader checkpoint base path
  schema_tracking_root: ${SCHEMA_TRACKING_ROOT}

metadata:
  mode: csv                              # csv | table
  csv_root: config                       # Relative to project root
  tables:                                # Used when mode=table
    source_registry: ${UC_CATALOG}.control.source_registry
    column_mapping:  ${UC_CATALOG}.control.column_mapping
    dq_rules:        ${UC_CATALOG}.control.dq_rules
    publish_rules:   ${UC_CATALOG}.control.publish_rules

connections:
  jdbc_profiles:
    ikea_erp:
      url: ${IKEA_ERP_JDBC_URL}
      driver: ${IKEA_ERP_JDBC_DRIVER}
      user_env: IKEA_ERP_JDBC_USER       # Env var that holds the username
      password_env: IKEA_ERP_JDBC_PASSWORD
      fetchsize: 10000
  api_profiles:
    connect_api:
      base_url: ${CONNECT_API_BASE_URL}
      auth_type: bearer                  # bearer | api_key | basic
      token_env: CONNECT_API_TOKEN

audit:
  pipeline_runs_table: main.audit.pipeline_runs
  dq_results_table:    main.audit.dq_rule_results
  rejects_table:       main.dq.rejects
  bronze_dq_table:     main.audit.bronze_dq_results
  validation_events_table: main.audit.validation_events
```

### Environment Variables Required at Runtime

| Variable | Purpose | Example |
|---|---|---|
| `IKEA_ENV` | Environment selector | `dev` / `qa` / `prod` |
| `UC_CATALOG` | Unity Catalog catalog | `main` |
| `UC_BRONZE_SCHEMA` | Bronze schema name | `bronze` |
| `UC_SILVER_SCHEMA` | Silver schema name | `silver` |
| `UC_AUDIT_SCHEMA` | Audit schema name | `audit` |
| `CHECKPOINT_ROOT` | Auto Loader checkpoint path | `abfss://framework@<storage-account>.dfs.core.windows.net/checkpoints` |
| `SCHEMA_TRACKING_ROOT` | Auto Loader schema inference path | `abfss://framework@<storage-account>.dfs.core.windows.net/schema_tracking` |
| `DATABRICKS_HOST` | Workspace URL | `https://adb-xxx.azuredatabricks.net` |
| `IKEA_ERP_JDBC_URL` | JDBC connection string | `jdbc:sqlserver://...` |
| `IKEA_ERP_JDBC_USER` | JDBC username | — |
| `IKEA_ERP_JDBC_PASSWORD` | JDBC password | — |
| `CONNECT_API_TOKEN` | Bearer token | — |

> Set in Databricks job environment variables, cluster environment, or a local `.env` file (excluded from git by `.gitignore`).

---

## Security Controls

### SQL Injection Guard

All values that flow into `MERGE INTO` or `OPTIMIZE` SQL statements are validated before use:

```python
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")

_validate_identifier(merge_key, "merge_key")       # Raises ValueError if unsafe
_validate_identifier(silver_table, "silver_table") # Raised before any SQL is built
```

Any metadata value that does not match the safe identifier pattern (letters, digits, underscores, dots only) raises a `ValueError` immediately — no SQL is executed.

### Temp View Isolation

Each `publish_merge()` call creates a UUID-suffixed temp view:

```python
view_name = f"_stg_silver_{uuid.uuid4().hex[:12]}"
```

This prevents temp view name collisions when multiple source entities run in the same Spark session. The view is always dropped in a `finally` block regardless of success or failure.

### Credential Handling

- Credentials are **never** stored in YAML or CSV files.
- Connections reference `user_env` / `password_env` / `token_env` — environment variable *names* whose values are resolved at runtime via `os.getenv()`.
- No credentials in git: `.gitignore` excludes `.env*`, `secrets.*`, `*.key`.

---

## Audit Tables

All tables are created by `sql/ddl/audit_tables.sql` under `USE CATALOG main`.

### `audit.pipeline_runs`

One row per pipeline execution per source entity.

| Column | Type | Notes |
|---|---|---|
| `pipeline_name` | STRING | Orchestrator-assigned name |
| `run_id` | STRING | UUID for this run |
| `source_system` | STRING | |
| `source_entity` | STRING | |
| `rows_in` | BIGINT | Rows read from source |
| `rows_out` | BIGINT | Rows written to silver |
| `status` | STRING | `SUCCESS` / `FAILED` |
| `error_message` | STRING | Truncated to 500 chars via `truncate_str()` |
| `run_ts` | TIMESTAMP | Auto-filled with UTC now if not provided |

### `audit.dq_rule_results`

Aggregate DQ summary — row counts per status per rule.

| Column | Type | Notes |
|---|---|---|
| `run_id` | STRING | |
| `source_system` | STRING | |
| `source_entity` | STRING | |
| `dq_status` | STRING | `PASS` / `FAIL` |
| `dq_failed_rule` | STRING | Comma-separated rule names that failed |
| `row_count` | BIGINT | Number of rows with this status/rule combination |
| `run_ts` | TIMESTAMP | |

### `dq.rejects`

Full rejected row payload for investigation.

| Column | Type | Notes |
|---|---|---|
| `run_id` | STRING | |
| `source_system` | STRING | |
| `source_entity` | STRING | |
| `rejected_ts` | TIMESTAMP | |
| `dq_status` | STRING | Always `FAIL` |
| `dq_failed_rule` | STRING | All failing rule names |
| `payload_json` | STRING | All source columns serialised via `to_json(struct(*))` |

> `payload_json` keeps the rejects table schema stable regardless of source schema changes.

### `audit.bronze_dq_results`

Bronze-layer technical validation events (schema checks, volume checks, etc.) from `bronze_dq_engine`.

### `audit.validation_events`

Stage-level verification log (arbitrary events with `event_name`, `event_status`, `event_payload`).

---

## Ingestion Mode Details

### FILE — Batch

```python
# source_options_json controls reader behaviour
{"file_ingest_mode": "batch", "recursiveFileLookup": "true"}
{"file_ingest_mode": "batch", "multiLine": "true"}
```

Reserved keys (`file_ingest_mode`, `schema_ddl`) are consumed by the adapter and not forwarded to Spark. All other keys are passed as `.option(k, v)` to the reader.

### FILE — Auto Loader (Streaming)

```python
{"file_ingest_mode": "autoloader", "cloudFiles.schemaEvolutionMode": "rescue"}
```

- Checkpoint path: `{checkpoint_root}/{source_system}/{source_entity}`
- Schema tracking: `{schema_tracking_root}/{source_system}/{source_entity}`
- Output mode: `append`
- `mergeSchema: true` on write

### JDBC

Supports parallel read when all 4 partition keys are present together in `source_options_json`:

```json
{
  "numPartitions": "8",
  "partitionColumn": "order_id",
  "lowerBound": "1",
  "upperBound": "10000000"
}
```

All 4 must be present — if any is missing, the read falls back to single-partition (safe default).

Incremental loads inject a `WHERE watermark_column > 'value'` pushdown predicate when `load_type=incremental` and `incremental_start_value` is set in `source_options_json`.

### REST API

**Authentication** (set `auth_type` in the API profile):

| `auth_type` | Header set |
|---|---|
| `bearer` | `Authorization: Bearer <TOKEN>` |
| `api_key` | `X-Api-Key: <TOKEN>` |
| `basic` | `Authorization: Basic <base64(user:pass)>` |

**Envelope auto-detection** — `_extract_records()` looks for these keys (in order) and unwraps the records list automatically: `data`, `items`, `results`, `records`, `value`, `content`. Falls back to the raw response if none found.

**Pagination** — configured via `source_options_json` (not the flat registry row):

```json
{
  "pagination_type": "cursor",
  "page_size": 100,
  "cursor_key": "next_page_token",
  "max_pages": 50
}
```

```json
{
  "pagination_type": "offset",
  "page_size": 200,
  "max_pages": 100
}
```

---

## Onboarding a New Source

### Standard Path (no custom code needed)

**Step 1 — Validate configs exist**:
```bash
source .venv/bin/activate
python scripts/validate_configs.py
```

**Step 2 — Add row to `config/source_registry.csv`**:
```
ikea,ikea,eu,risk,connect,new_system,new_entity,FILE,json,abfss://raw@<storage-account>.dfs.core.windows.net/new_system/new_entity,,,,,,,incremental,file_modification_ts,${UC_CATALOG}.${UC_BRONZE_SCHEMA}.new_system_new_entity_landing,${UC_CATALOG}.${UC_BRONZE_SCHEMA}.new_system_new_entity_conformance,${UC_CATALOG}.${UC_SILVER_SCHEMA}.new_system_new_entity,record_id,append,true,"{""file_ingest_mode"":""batch""}",,,
```

**Step 3 — Add column mappings to `config/column_mapping.csv`**:
```
ikea,ikea,product_area,new_system,new_entity,SrcCol,src_col,target_col,target_col,string,string,true,,trim(SrcCol),true
```

**Step 4 — Add DQ rules to `config/dq_rules.csv`**:
```
ikea,ikea,product_area,new_system,new_entity,record_id,RECORD_ID_NOT_NULL,expression,record_id IS NOT NULL,high,reject,true
```

**Step 5 — Add publish rule to `config/publish_rules.csv`** (merge example):
```
ikea,ikea,product_area,new_system,new_entity,${UC_CATALOG}.${UC_SILVER_SCHEMA}.new_entity,merge,record_id,ingest_ts,ignore,true,"[""record_id""]","[]"
```

**Step 6 — Validate and run**:
```bash
python scripts/validate_configs.py

python notebooks/05_orchestration/framework_orchestrator.py \
  --product-name connect \
  --source-system new_system \
  --source-entity new_entity
```

### Exception Path (custom transform needed)

Add the notebook path to the appropriate hook column in `source_registry.csv`. The hook notebook receives:
- `spark` — active SparkSession
- `df` — DataFrame at that stage
- `source_config` — the source registry row as a dict

| Hook column | When called | Use for |
|---|---|---|
| `pre_landing_transform_notebook` | After ingest, before landing write | Decryption, file-level parsing, schema corrections |
| `post_conformance_transform_notebook` | After conformance, before DQ | Complex enrichment, lookup joins |
| `custom_publish_notebook` | Instead of standard publish | Custom SCD logic, multi-table writes |

---

## Running the Orchestrator

### Dry Run (default — no writes)
```bash
python notebooks/05_orchestration/framework_orchestrator.py
```

### Run all active sources for a product
```bash
python notebooks/05_orchestration/framework_orchestrator.py \
  --product-name connect
```

### Run a single source entity
```bash
python notebooks/05_orchestration/framework_orchestrator.py \
  --product-name connect \
  --source-system cemc \
  --source-entity countryriskdet
```

### Run tests
```bash
python -m pytest -q tests/unit tests/data_quality
# → 101 passed, 8 skipped
```

---

## Monitoring & Observability

### Pipeline Health
```sql
-- Recent run status summary
SELECT
  date_trunc('hour', run_ts)  AS run_hour,
  source_system, source_entity, status,
  COUNT(*)       AS run_count,
  SUM(rows_in)   AS total_in,
  SUM(rows_out)  AS total_out
FROM audit.pipeline_runs
WHERE run_ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1,2,3,4
ORDER BY run_hour DESC;
```

### DQ Failure Rate
```sql
SELECT
  source_system, source_entity,
  dq_failed_rule,
  SUM(row_count) AS failed_rows,
  MAX(run_ts)    AS last_seen
FROM audit.dq_rule_results
WHERE dq_status = 'FAIL'
GROUP BY 1,2,3
ORDER BY failed_rows DESC;
```

### Investigating Rejected Rows
```sql
SELECT
  run_id,
  dq_failed_rule,
  payload_json,
  rejected_ts
FROM dq.rejects
WHERE source_system = 'hip'
  AND source_entity = 'customer'
  AND rejected_ts >= current_timestamp() - INTERVAL 1 DAY
ORDER BY rejected_ts DESC
LIMIT 100;
```

### Overall Quality Gate (per run)
Use `sql/monitoring/audit_validation_queries.sql` — Q9 provides a full `PASS/FAIL` gate joining `pipeline_runs`, `dq_rule_results`, and `bronze_dq_results`.

---

## Failure Handling & Replay

### Recovery Steps
1. Query `audit.pipeline_runs` to find the failed `run_id`, `source_entity`, and `error_message`.
2. Fix the root cause (bad metadata row, missing credential, network issue).
3. Replay from **Landing** — Landing is immutable and retained. Never re-extract from source unless Landing rows are missing.
4. Re-run the orchestrator with `--source-system` / `--source-entity` scoped to the affected entity.

### Retry Configuration
Per-source-type retry is configured in `global_config.yaml`:
```yaml
execution:
  max_retries: 2
  retry_backoff_seconds: 2
  retry_by_source_type:
    api:
      max_retries: 3
      backoff_seconds: 3
    jdbc:
      max_retries: 2
      backoff_seconds: 2
    file:
      max_retries: 1
      backoff_seconds: 2
```

### Common Failures

| Symptom | Cause | Fix |
|---|---|---|
| `Source not found or inactive` | `is_active` ≠ `true` in source_registry | Set `is_active=true` or check spelling |
| `Missing required columns` in audit | Mandatory source_registry columns blank | Fill `source_type`, `landing_table`, `conformance_table`, `silver_table`, `publish_mode` |
| `conformance_engine: DataFrame select failed` | Bad `transform_expression` in column_mapping | Test expression with `spark.sql("SELECT your_expr FROM table").show()` |
| `merge_key must be a valid SQL identifier` | Merge key contains special characters | Use only letters, digits, `_`, `.` in merge keys |
| High DQ reject rate | Rule expressions too strict or data schema changed | Review `dq.rejects.payload_json`; adjust rule_expression or update source schema |
| Auto Loader not picking up new files | Wrong checkpoint or schema tracking path | Check `CHECKPOINT_ROOT` env var; delete corrupt checkpoint to reset |
| JDBC timeout or partial read | No partition config on large table | Add `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound` to `source_options_json` |

---

## Follow-up Tickets (Non-blockers)

| # | Item | Effort |
|---|---|---|
| 1 | Wire `get_logger()` into `_log_event()` in orchestrator (currently uses `print(json.dumps(...))`) | S |
| 2 | Add `threading.Lock` to `_CSV_CACHE` in `config_loader.py` for multi-threaded use | S |
| 3 | GitHub Actions CI job for Spark tests (8 tests currently skip without Java) | M |

---

## Unity Catalog Metadata Registration

Unity Catalog (UC) is the governance layer for this framework. Registering table-level and column-level metadata makes every layer (Landing, Conformance, Silver, Audit) discoverable, governed, and compliant.

### What the Framework Registers

After each successful Silver write, `apply_post_publish_actions()` runs `OPTIMIZE`. You can extend the post-publish step to also call the helpers below. They generate standard `COMMENT ON TABLE`, `COMMENT ON COLUMN`, and `ALTER TABLE SET TBLPROPERTIES` statements for any Delta table.

---

### Table-Level Comments

Attach a human-readable description to any layer table:

```sql
-- Landing table
COMMENT ON TABLE main.bronze.cemc_countryriskdet_landing IS
  'Landing (raw) copy of CEMC Country Risk Detail from drop zone.
  Source: abfss://raw@<storage-account>.dfs.core.windows.net/connect/cemccountryriskdet | Format: json_jsonl_mixed
   Preserves 1:1 source data — do not modify. Replay downstream from here.';

-- Conformance table
COMMENT ON TABLE main.bronze.cemc_countryriskdet_conformance IS
  'CEMC Country Risk Detail after schema normalisation via column_mapping.csv.
   Loaded by: conformance_engine.apply_column_mappings()';

-- Silver table
COMMENT ON TABLE main.silver.cemc_countryriskdet IS
  'Production-ready CEMC Country Risk Detail.
   publish_mode: append | optimised with ZORDER BY (record_id)';

-- Audit tables
COMMENT ON TABLE main.audit.pipeline_runs IS
  'One row per pipeline execution per source entity.
   Written by: audit_logger.write_pipeline_audit()';

COMMENT ON TABLE main.dq.rejects IS
  'Rejected rows from DQ evaluation. payload_json holds all source columns
   as a serialised JSON string for schema-stable storage.
   Written by: audit_logger.write_reject_rows()';
```

---

### Column-Level Comments

Document the framework technical columns added at Landing:

```sql
-- Technical metadata columns added by landing_engine.add_landing_metadata()
COMMENT ON COLUMN main.bronze.cemc_countryriskdet_landing.source_system
  IS 'Logical source system name from source_registry.csv (e.g. cemc)';

COMMENT ON COLUMN main.bronze.cemc_countryriskdet_landing.source_entity
  IS 'Logical entity name from source_registry.csv (e.g. countryriskdet)';

COMMENT ON COLUMN main.bronze.cemc_countryriskdet_landing.load_id
  IS 'UUID assigned per orchestrator run — links all rows from the same load across layers';

COMMENT ON COLUMN main.bronze.cemc_countryriskdet_landing.ingest_ts
  IS 'Timestamp when this row was written to Landing by the framework';

-- DQ annotation columns added by dq_engine.apply_dq_rules()
COMMENT ON COLUMN main.dq.rejects.dq_status
  IS 'Always FAIL for rows in this table';

COMMENT ON COLUMN main.dq.rejects.dq_failed_rule
  IS 'Comma-separated list of all rule_name values that the row violated';

COMMENT ON COLUMN main.dq.rejects.payload_json
  IS 'All source columns serialised via to_json(struct(*)) — schema stable regardless of source changes';
```

---

### Table Properties

Use `TBLPROPERTIES` to tag every table for governance automation, retention policies, and data classification:

```sql
-- Landing table
ALTER TABLE main.bronze.cemc_countryriskdet_landing SET TBLPROPERTIES (
  'layer'               = 'landing',
  'classification'      = 'bronze_raw',
  'source_system'       = 'cemc',
  'source_entity'       = 'countryriskdet',
  'product_area'        = 'risk',
  'data_owner'          = 'data-engineering',
  'retention_days'      = '365',
  'pii_flag'            = 'false',
  'framework_version'   = '1.0',
  'last_onboarded_date' = '2026-03-27'
);

-- Silver table
ALTER TABLE main.silver.cemc_countryriskdet SET TBLPROPERTIES (
  'layer'               = 'silver',
  'classification'      = 'silver_curated',
  'source_system'       = 'cemc',
  'source_entity'       = 'countryriskdet',
  'publish_mode'        = 'append',
  'data_owner'          = 'data-engineering',
  'retention_days'      = '1825',
  'pii_flag'            = 'false',
  'framework_version'   = '1.0'
);
```

**Standard property keys used across all framework tables**:

| Property | Values | Purpose |
|---|---|---|
| `layer` | `landing` / `conformance` / `silver` / `audit` | Medallion layer |
| `classification` | `bronze_raw` / `bronze_standardised` / `silver_curated` | Data quality tier |
| `source_system` | e.g. `cemc` | Traceability to source |
| `source_entity` | e.g. `countryriskdet` | Traceability to entity |
| `product_area` | e.g. `risk` / `supply` / `master` | Business domain from source_registry |
| `data_owner` | e.g. `data-engineering` | Governance owner |
| `retention_days` | e.g. `365` | Data lifecycle policy |
| `pii_flag` | `true` / `false` | PII classification for compliance |
| `framework_version` | e.g. `1.0` | Framework version that created the table |
| `last_onboarded_date` | `yyyy-MM-dd` | When the source was first registered |

---

### Reusable Registration Helper (PySpark)

Add this to a post-onboarding notebook or call from `apply_post_publish_actions()`:

```python
def register_uc_metadata(
    spark,
    table_name: str,
    source_system: str,
    source_entity: str,
    layer: str,
    product_area: str,
    publish_mode: str = "",
    pii_flag: bool = False,
    retention_days: int = 365,
) -> None:
    """Register table comment and governance properties in Unity Catalog."""
    layer_labels = {
        "landing":     ("bronze_raw",           "1:1 raw copy from source drop zone"),
        "conformance": ("bronze_standardised",   "Schema-normalised via column_mapping.csv"),
        "silver":      ("silver_curated",        f"Production-ready data | publish_mode: {publish_mode}"),
        "audit":       ("audit",                 "Framework audit and DQ logging table"),
    }
    classification, description = layer_labels.get(layer, ("unknown", ""))

    # Table comment
    spark.sql(f"""
        COMMENT ON TABLE {table_name} IS
        '{description} | source_system={source_system} | source_entity={source_entity}'
    """)

    # Technical metadata column comments (landing and conformance layers)
    if layer in ("landing", "conformance"):
        for col, comment in {
            "source_system": "Logical source system name from source_registry.csv",
            "source_entity":  "Logical entity name from source_registry.csv",
            "load_id":        "UUID per orchestrator run — links rows from the same load across layers",
            "ingest_ts":      "Timestamp when this row was written to the table by the framework",
        }.items():
            try:
                spark.sql(f"COMMENT ON COLUMN {table_name}.{col} IS '{comment}'")
            except Exception:
                pass  # Column may not exist on every table; skip silently

    # Table properties
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'layer'             = '{layer}',
            'classification'    = '{classification}',
            'source_system'     = '{source_system}',
            'source_entity'     = '{source_entity}',
            'product_area'      = '{product_area}',
            'data_owner'        = 'data-engineering',
            'retention_days'    = '{retention_days}',
            'pii_flag'          = '{"true" if pii_flag else "false"}',
            'framework_version' = '1.0'
        )
    """)
```

**Call it for each layer after table creation**:
```python
for layer, tbl in [
    ("landing",     source["landing_table"]),
    ("conformance", source["conformance_table"]),
    ("silver",      source["silver_table"]),
]:
    register_uc_metadata(
        spark,
        table_name=tbl,
        source_system=source["source_system"],
        source_entity=source["source_entity"],
        layer=layer,
        product_area=source.get("product_area", ""),
        publish_mode=source.get("publish_mode", ""),
    )
```

---

### Querying UC Metadata

**View table properties**:
```sql
SHOW TBLPROPERTIES main.silver.cemc_countryriskdet;
```

**View full table detail including comments**:
```sql
DESCRIBE TABLE EXTENDED main.silver.cemc_countryriskdet;
```

**Find all framework tables by layer**:
```sql
SELECT
  table_catalog,
  table_schema,
  table_name,
  comment
FROM system.information_schema.tables
WHERE table_catalog = 'main'
  AND table_name LIKE '%countryriskdet%'
ORDER BY table_schema, table_name;
```

**Find all tables tagged with a specific source_system** (requires Delta table properties scan):
```sql
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE table_catalog = 'main'
ORDER BY table_schema, table_name;
-- Then inspect properties via: SHOW TBLPROPERTIES <table>
```

**Find all tables where PII flag is set**:
```sql
-- In Databricks SQL — use information_schema with property filters
SELECT table_name, property_value
FROM system.information_schema.table_properties
WHERE catalog_name  = 'main'
  AND property_key  = 'pii_flag'
  AND property_value = 'true';
```

---

### Alert Thresholds for Governance

| Issue | Classification | Action |
|---|---|---|
| Table missing `data_owner` property | Warning | Check registration step ran after onboarding |
| `pii_flag = true` table without column-level masking | Critical | Apply dynamic data masking in UC before production |
| `retention_days` not set | Warning | Default UC behaviour applies — set explicitly |
| Silver table missing `layer = silver` property | Warning | Re-run `register_uc_metadata()` for that table |

---

## File Inventory

```
config/
  global_config.yaml          Runtime settings, connection profiles, env var substitution
  source_registry.csv         One row per source entity
  column_mapping.csv          Field-level transform expressions
  dq_rules.csv                Quality rules evaluated per row
  publish_rules.csv           Silver write strategy (append / merge)

notebooks/
  00_common/
    global_config.py          YAML loader + require_config_keys()
    config_loader.py          CSV reader with cache + env substitution
    utils.py                  truncate_str, get_logger, new_load_id
    databricks_launchpad_widgets.py  Widget registration + local shim
  01_ingestion/
    ingest_file_batch.py      Batch file reader (9 formats)
    ingest_file_autoloader.py Auto Loader streaming reader
    ingest_jdbc.py            JDBC reader with parallel partition support
    ingest_api.py             REST API client (3 auth types, pagination)
  02_processing/
    landing_engine.py         Metadata append + Delta write
    conformance_engine.py     Mapping-driven column transforms
    dq_engine.py              Rule evaluation + pass/fail accumulation
    bronze_dq_engine.py       Technical DQ checks before conformance
  03_publish/
    publish_silver.py         Append and merge with SQL injection guard
  04_audit/
    audit_logger.py           All audit write functions
  05_orchestration/
    framework_orchestrator.py Master coordinator

sql/
  ddl/
    audit_tables.sql          CREATE TABLE statements for all audit tables
    control_tables.sql        CREATE TABLE statements for control tables (Delta-backed)
    create_tables.sql         Landing / Conformance / Silver DDL templates
  merge/
    merge_templates.sql       Reference MERGE templates
  monitoring/
    audit_validation_queries.sql  9 monitoring queries (Q1–Q9)

scripts/
  validate_configs.py         Pre-flight config validation
  generate_notebooks_ipynb.py Convert .py notebooks to .ipynb

tests/
  unit/
    test_config_loader.py
    test_global_config.py
    test_ingest_api.py        16 tests
    test_ingest_jdbc.py       8 tests
    test_ingest_file.py       14 tests
    test_utils.py             16 tests
    test_publish_silver.py    6 tests
    test_processing_engines.py  8 Spark tests (skip without Java)
    test_audit_logger.py
    test_validate_configs.py
  data_quality/
    test_dq_rules.py
  integration/
    test_framework_flow.py
```
