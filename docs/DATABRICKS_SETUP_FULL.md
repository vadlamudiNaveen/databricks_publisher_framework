# Databricks Setup & Execution Guide

Complete checklist for running the ingestion framework in Databricks.

---

## 1. Cluster Setup

### Prerequisites
- Databricks workspace with at least one SQL/Compute cluster
- Cluster runtime: **Databricks Runtime 14.0+** (supports Python 3.11+)
- Cluster libraries installed (see Section 5)

### Cluster Configuration
1. Create or select an existing cluster
2. Go to **Libraries** tab
3. Install the following:
   - **JAR Dependencies** (if using JDBC sources):
     - For Oracle: `com.oracle.database.jdbc:ojdbc11:21.9.0.0`
     - For SQL Server: `com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11`
     - For PostgreSQL: `org.postgresql:postgresql:42.6.0`

4. **Python Dependencies:**
   - Install via **Cluster → Libraries → Install New → PyPI**
   - Package: `pyspark==3.5.1`
   - Package: `PyYAML==6.0.2`
   - Package: `requests==2.32.3`

---

## 2. Unity Catalog Prerequisites

The framework includes an automated initialization module that creates all required UC infrastructure.

### Automated UC Initialization (Recommended)

Run this in a Databricks notebook to automatically create catalog, schemas, and all tables:

```python
# Cell 1: Setup paths
%python
import sys
framework_root = "/Workspace/Users/your-email/ingestion-framework"
sys.path.append(f"{framework_root}/notebooks/05_orchestration")
sys.path.append(f"{framework_root}/notebooks/00_common")

# Cell 2: Initialize framework infrastructure
%python
from initialize_framework import initialize_framework

config_path = f"{framework_root}/config/global_config.yaml"
success = initialize_framework(config_path)
print(f"Framework initialization: {'✓ SUCCESS' if success else '✗ FAILED'}")
```

Alternative one-time setup path:
- Use `pipelines/databricks_setup_jobs.json` to create Databricks Jobs for:
  - `notebooks/05_orchestration/initialize_framework`
  - `notebooks/05_orchestration/setup_wizard`

**What gets created:**
- ✓ Catalog (e.g., `main`)
- ✓ Schemas: `bronze`, `silver`, `audit`, `control`
- ✓ Control tables: `source_registry`, `column_mapping`, `dq_rules`, `publish_rules`
- ✓ Audit tables: `pipeline_runs`, `dq_rule_results`, `bronze_dq_results`, `rejects`, `validation_events`

### Manual UC Creation (Alternative)

If you prefer manual SQL or need to customize, run these scripts in Databricks SQL editor:

#### Create Catalog & Schemas
```sql
CREATE CATALOG IF NOT EXISTS main;

CREATE SCHEMA IF NOT EXISTS main.bronze;
CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.audit;
CREATE SCHEMA IF NOT EXISTS main.control;
```

#### Create Control Tables
```sql
USE CATALOG main;
USE SCHEMA control;

CREATE TABLE IF NOT EXISTS source_registry (
    tenant STRING, brand STRING, region STRING, product_area STRING,
    product_name STRING, source_system STRING, source_entity STRING,
    source_type STRING, source_format STRING, source_path STRING,
    jdbc_profile STRING, jdbc_table STRING,
    api_profile STRING, api_endpoint STRING, api_method STRING,
    api_query_params_json STRING,
    load_type STRING, watermark_column STRING,
    landing_table STRING, conformance_table STRING, silver_table STRING,
    primary_key STRING, publish_mode STRING, is_active BOOLEAN,
    source_options_json STRING,
    pre_landing_transform_notebook STRING,
    post_conformance_transform_notebook STRING,
    custom_publish_notebook STRING,
    scheduler_name STRING,
    schedule_cron STRING,
    retention_days INT,
    sttm_profile STRING,
    landing_table_type STRING, landing_table_path STRING,
    conformance_table_type STRING, conformance_table_path STRING,
    silver_table_type STRING, silver_table_path STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS column_mapping (
    product_name STRING,
    source_system STRING,
    source_entity STRING,
    source_column STRING,
    conformance_column STRING,
    conformance_data_type STRING,
    column_description STRING,
    is_active BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS dq_rules (
    product_name STRING,
    source_system STRING,
    source_entity STRING,
    rule_name STRING,
    rule_type STRING,
    rule_sql STRING,
    column_name STRING,
    expected_value STRING,
    comparator STRING,
    severity STRING,
    is_active BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS publish_rules (
    product_name STRING,
    source_system STRING,
    source_entity STRING,
    publish_mode STRING,
    merge_key STRING,
    partition_columns_json STRING,
    optimize_zorder_json STRING
) USING DELTA;
```

#### Create Audit Tables
```sql
USE CATALOG main;
USE SCHEMA audit;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    framework_name STRING,
    load_id STRING,
    source_system STRING,
    source_entity STRING,
    landing_row_count BIGINT,
    valid_row_count BIGINT,
    status STRING,
    error_message STRING,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS dq_rule_results (
    load_id STRING,
    source_system STRING,
    source_entity STRING,
    rule_name STRING,
    failed_row_count BIGINT,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze_dq_results (
    load_id STRING,
    source_system STRING,
    source_entity STRING,
    check_name STRING,
    result STRING,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS rejects (
    load_id STRING,
    source_system STRING,
    source_entity STRING,
    rejected_payload STRING,
    dq_failed_rule STRING,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS validation_events (
    load_id STRING,
    source_system STRING,
    source_entity STRING,
    event_name STRING,
    event_status STRING,
    event_payload STRING,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING DELTA;
```

### Create Landing/Conformance/Silver Tables
For each source in `source_registry`, create target tables:

```sql
USE CATALOG main;
USE SCHEMA bronze;

-- Landing tables (raw data)
CREATE TABLE IF NOT EXISTS connect_countryriskdet_landing (
    source_system STRING,
    source_entity STRING,
    load_id STRING,
    ingest_ts TIMESTAMP,
    raw_payload STRING
) USING DELTA;

-- Conformance tables (cleansed data with standard columns)
CREATE TABLE IF NOT EXISTS connect_countryriskdet_conformance (
    record_id STRING,
    country_code STRING,
    risk_level STRING,
    load_id STRING,
    ingest_ts TIMESTAMP
) USING DELTA;

USE SCHEMA silver;

-- Silver tables (business-ready)
CREATE TABLE IF NOT EXISTS connect_countryriskdet (
    record_id STRING,
    country_code STRING,
    risk_level STRING,
    load_id STRING,
    ingest_ts TIMESTAMP,
    processed_ts TIMESTAMP
) USING DELTA;
```

---

## 3. External Location Setup (if using external tables)

### Create External Locations
Run from Databricks SQL:
```sql
-- Create external location for landing zone
CREATE EXTERNAL LOCATION IF NOT EXISTS landing_dropzone
    URL 'abfss://connect@storage.dfs.core.windows.net/landing'
    WITH (CREDENTIAL `storage-credential-name`);

-- Grant permissions
GRANT CREATE, READ_METADATA, WRITE_METADATA ON EXTERNAL LOCATION landing_dropzone TO `group-or-user`;
```

### Create Storage Credential
```sql
CREATE STORAGE CREDENTIAL IF NOT EXISTS storage-credential-name
    USING (
        PROVIDER = 'ADLS'
        DIRECTORY_ID = '<tenant-id>',
        APPLICATION_ID = '<app-id>',
        CLIENT_SECRET = '<secret>'
    );
```

---

## 4. Environment Variables Setup

### Option A: Databricks Secrets (Recommended for Production)

1. Create a secret scope:
```bash
databricks secrets create-scope --scope ingestion-framework
```

2. Add secrets:
```bash
databricks secrets put --scope ingestion-framework --key UC_CATALOG --string-value main
databricks secrets put --scope ingestion-framework --key UC_BRONZE_SCHEMA --string-value bronze
databricks secrets put --scope ingestion-framework --key UC_SILVER_SCHEMA --string-value silver
databricks secrets put --scope ingestion-framework --key UC_AUDIT_SCHEMA --string-value audit
databricks secrets put --scope ingestion-framework --key CHECKPOINT_ROOT --string-value /Volumes/main/system/checkpoints
databricks secrets put --scope ingestion-framework --key SCHEMA_TRACKING_ROOT --string-value /Volumes/main/system/schema_tracking
databricks secrets put --scope ingestion-framework --key ENVIRONMENT --string-value dev
```

3. For JDBC/API credentials:
```bash
databricks secrets put --scope ingestion-framework --key ORACLE_DEV_HOST --string-value your-host
databricks secrets put --scope ingestion-framework --key ORACLE_DEV_USER --string-value your-user
databricks secrets put --scope ingestion-framework --key ORACLE_DEV_PASSWORD --string-value your-password
```

### Option B: Job Parameters (Development)

In Databricks Job configuration:
```json
{
  "tasks": [{
    "environment_variables": {
      "UC_CATALOG": "main",
      "UC_BRONZE_SCHEMA": "bronze",
      "UC_SILVER_SCHEMA": "silver",
      "UC_AUDIT_SCHEMA": "audit",
      "CHECKPOINT_ROOT": "/Volumes/main/system/checkpoints",
      "SCHEMA_TRACKING_ROOT": "/Volumes/main/system/schema_tracking",
      "ENVIRONMENT": "dev"
    }
  }]
}
```

---

## 5. Configuration Files Update

### Update global_config.yaml
Replace placeholders with actual values:

```yaml
organization:
  client_name: your-organization
  environment: dev
  tenant: your-tenant

databricks:
  host: https://your-workspace.cloud.databricks.com
  workspace_id: "1234567890123456"
  catalog: main
  bronze_schema: bronze
  silver_schema: silver
  audit_schema: audit
  checkpoint_root: /Volumes/main/system/checkpoints
  schema_tracking_root: /Volumes/main/system/schema_tracking

metadata:
  mode: csv  # or "table" if using UC tables
  csv_root: /Workspace/config  # path in Databricks Workspace
  tables:
    source_registry: main.control.source_registry
    column_mapping: main.control.column_mapping
    dq_rules: main.control.dq_rules
    publish_rules: main.control.publish_rules

connections:
  jdbc_profiles:
    oracle_dev:
      url: jdbc:oracle:thin:@//your-host:1521/your-service
      driver: oracle.jdbc.OracleDriver
      user_env: ORACLE_DEV_USER
      password_env: ORACLE_DEV_PASSWORD
```

### Update source_registry.csv
Ensure paths are accessible from Databricks:

```
product_name,source_system,source_entity,source_type,source_path,landing_table,conformance_table,silver_table,is_active
connect,cemc,countryriskdet,FILE,/mnt/dropzone/connect/cemccountryriskdet,main.bronze.connect_countryriskdet_landing,main.bronze.connect_countryriskdet_conformance,main.silver.connect_countryriskdet,true
```

---

## 6. Databricks Notebook Setup

### Import Notebooks
1. In Databricks UI: **Workspace → Import**
2. Upload entire `notebooks_ipynb/` folder
3. Import directory structure maintains organization:
   - `notebooks_ipynb/00_common/`
   - `notebooks_ipynb/01_ingestion/`
   - `notebooks_ipynb/02_processing/`
   - `notebooks_ipynb/03_publish/`
   - `notebooks_ipynb/04_audit/`
   - `notebooks_ipynb/05_orchestration/`

### Create Main Orchestrator Notebook
Create a new notebook in Databricks to call the orchestrator:

```python
# Cell 1: Setup paths and configuration
%python
import sys
from pathlib import Path

# Add framework modules to path
framework_root = "/Workspace/Users/your-email/ingestion-framework"
sys.path.append(f"{framework_root}/notebooks/00_common")
sys.path.append(f"{framework_root}/notebooks/01_ingestion")
sys.path.append(f"{framework_root}/notebooks/02_processing")
sys.path.append(f"{framework_root}/notebooks/03_publish")
sys.path.append(f"{framework_root}/notebooks/04_audit")

# Cell 2: Load configuration
%python
from global_config import load_global_config
import os

config_path = f"{framework_root}/config/global_config.yaml"
global_config = load_global_config(config_path)
print("✓ Global config loaded")

# Cell 3: Run orchestrator (DRY RUN)
%python
from framework_orchestrator import run_all

results = run_all(
    config_dir=f"{framework_root}/config",
    global_config=global_config,
    spark=spark,
    dry_run=True,
    pk_check_summary=True
)

print(f"Dry run completed: {len(results)} sources processed")
import json
print(json.dumps(results, indent=2))

# Cell 4: Run orchestrator (EXECUTE)
%python
results = run_all(
    config_dir=f"{framework_root}/config",
    global_config=global_config,
    spark=spark,
    dry_run=False,  # Execute mode
    pk_check_summary=True
)

print(f"Execution completed: {len(results)} sources processed")
print(json.dumps(results, indent=2))
```

---

## 7. File Paths & Mounts

### Mount dropzone paths (if needed)
```python
# In a Databricks notebook cell
dbutils.fs.mount(
    source="abfss://dropzone@yourstorage.dfs.core.windows.net/",
    mount_point="/mnt/dropzone",
    extra_configs={"fs.azure.account.auth.type": "CustomAuth",
                   "fs.azure.account.custom.auth.login.provider.class": "com.databricks.adl.oauth.CustomAdlGen2OAuthProvider"}
)
```

Or use Unity Catalog volumes:
```sql
CREATE VOLUME IF NOT EXISTS main.system.dropzone;
```

Then update source_registry paths to: `/Volumes/main/system/dropzone/...`

---

## 8. First Run Checklist

Before running orchestrator for first time:

- [ ] Cluster created & libraries installed
- [ ] Run `initialize_framework.py` to create UC objects (catalog/schemas/tables)
  - Or manually run SQL scripts from [Section 2](#2-unity-catalog-prerequisites)
- [ ] Environment variables set (via Secrets or Job config)
- [ ] global_config.yaml updated with actual values
- [ ] Source registry paths point to valid locations
- [ ] Landing/conformance/silver target tables created (or auto-created by framework)
- [ ] JDBC drivers installed (if needed for JDBC sources)
- [ ] Databricks file paths/mounts accessible
- [ ] Test data available in source paths (e.g., /mnt/dropzone/...)
- [ ] DQ rules exist in config/dq_rules.csv
- [ ] Column mappings exist in config/column_mapping.csv

---

## 9. Test Execution Steps

### Step 1: Dry-Run Single Source
```python
from framework_orchestrator import run_all

results = run_all(
    config_dir="/Workspace/config",
    global_config=global_config,
    spark=spark,
    dry_run=True,
    product_name="connect",
    source_system="cemc",
    source_entity="countryriskdet",
    pk_check_summary=True
)

print(json.dumps(results, indent=2))
```

**Expected output:**
```json
[
  {
    "source": "cemc.countryriskdet",
    "mode": "dry_run",
    "steps": [
      "ingest via FILE",
      "write landing main.bronze.connect_countryriskdet_landing",
      "apply 0 mappings",
      "apply 0 dq rules",
      "publish append to main.silver.connect_countryriskdet"
    ],
    "pk_check_summary": {
      "primary_key_configured": true,
      "primary_key_columns": ["record_id"],
      "primary_key_checks": ["primary_key_not_null", "primary_key_unique"]
    }
  }
]
```

### Step 2: Execute Single Source
```python
results = run_all(
    config_dir="/Workspace/config",
    global_config=global_config,
    spark=spark,
    dry_run=False,  # Execute!
    product_name="connect",
    source_system="cemc",
    source_entity="countryriskdet",
    pk_check_summary=True
)
```

**Expected output:**
```json
[
  {
    "source": "cemc.countryriskdet",
    "mode": "execute",
    "status": "SUCCESS",
    "valid_rows": 1000,
    "reject_rows": 5,
    "publish_mode": "append",
    "stage_seconds": {
      "ingest": 2.5,
      "landing": 1.2,
      "conformance": 0.8,
      "dq": 1.5,
      "publish": 2.1
    },
    "pk_check_summary": {
      "primary_key_configured": true,
      "primary_key_columns": ["record_id"],
      "primary_key_checks": ["primary_key_not_null", "primary_key_unique"],
      "primary_key_failure_counts": {
        "primary_key_not_null": 0,
        "primary_key_unique": 0
      }
    }
  }
]
```

### Step 3: Verify Data in Audit Tables
```sql
SELECT * FROM main.audit.pipeline_runs ORDER BY run_timestamp DESC LIMIT 5;
SELECT * FROM main.audit.dq_rule_results ORDER BY run_timestamp DESC LIMIT 10;
```

---

## 10. Production Job Setup

Create a Databricks Job in UI or via API:

```json
{
  "name": "ingestion-framework-orchestrator",
  "tasks": [{
    "task_key": "run_orchestrator",
    "notebook_task": {
      "notebook_path": "/Users/your-email/ingestion-orchestrator"
    },
    "new_cluster": {
      "spark_version": "14.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 4,
      "aws_attributes": {
        "availability": "ON_DEMAND"
      }
    },
    "environment_variables": {
      "UC_CATALOG": "main",
      "UC_BRONZE_SCHEMA": "bronze",
      "UC_SILVER_SCHEMA": "silver",
      "UC_AUDIT_SCHEMA": "audit",
      "CHECKPOINT_ROOT": "/Volumes/main/system/checkpoints",
      "SCHEMA_TRACKING_ROOT": "/Volumes/main/system/schema_tracking",
      "ENVIRONMENT": "prod"
    },
    "timeout_seconds": 3600,
    "max_retries": 2
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 8 * * ? *",
    "timezone_id": "UTC"
  }
}
```

---

## 11. Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| **Module not found** | Path not set in Databricks notebook | Add sys.path.append for notebooks directories |
| **UC tables not found** | Catalog/schema permissions | Grant `USE CATALOG`, `USE SCHEMA` to job principal |
| **JDBC connection failed** | Driver not installed or credentials wrong | Install JAR via cluster libs, verify secrets |
| **Empty results []** | No active sources matched filters | Verify `is_active=true` in source_registry |
| **Permission denied** on paths | Storage access issue | Verify mount or UC volume permissions |
| **DQ checks not running** | Missing dq_rules.csv or source mappings | Load CSV to control table or ensure CSV accessible |

---

## 12. Health Check Commands

Run these to verify setup:

```python
# Check config loaded
from global_config import load_global_config
cfg = load_global_config("/Workspace/config/global_config.yaml")
print(f"✓ Catalog: {cfg['databricks']['catalog']}")
print(f"✓ Bronze Schema: {cfg['databricks']['bronze_schema']}")

# Check active sources
from config_loader import active_sources
sources = active_sources("/Workspace/config", spark=spark, global_config=cfg)
print(f"✓ Active sources: {len(sources)}")
for s in sources:
    print(f"  - {s['source_system']}.{s['source_entity']}")

# Check table accessibility
spark.sql("SELECT COUNT(*) FROM main.control.source_registry").show()
spark.sql("SELECT COUNT(*) FROM main.audit.pipeline_runs").show()
```

---

## Next Steps

1. Update all configuration values in `global_config.yaml`
2. Create UC objects using scripts in `sql/ddl/`
3. Import notebooks from `notebooks_ipynb/`
4. Create test notebook to verify connectivity
5. Run dry-run for single source
6. Execute full orchestration
7. Monitor via audit tables
8. Set up production job with schedule

For detailed documentation, see:
- [README.md](../README.md) - Framework overview
- [architecture.md](architecture.md) - Design decisions
- [runbook.md](runbook.md) - Operational procedures
