# Databricks Ingestion Framework - Setup Checklist

## ✅ What's FULLY AUTOMATED (Nothing to Do)

The `setup_wizard.py` automatically handles ALL of this:

- ✅ Create Databricks catalog (e.g., `main`)
- ✅ Create schemas (`bronze`, `silver`, `audit`, `control`)
- ✅ Create control tables:
  - `source_registry`
  - `column_mapping`
  - `dq_rules`
  - `publish_rules`
- ✅ Create audit tables:
  - `pipeline_runs`
  - `dq_rule_results`
  - `bronze_dq_results`
  - `rejects`
  - `validation_events`
- ✅ Create sample landing/conformance/silver tables (for demo)
- ✅ Load sample metadata
- ✅ Run orchestrator dry-run test
- ✅ Validate configuration
- ✅ Verify Spark cluster
- ✅ Verify Python libraries

---

## 🔧 What YOU Need to Configure (Customize These Values)

### 1. Global Configuration (`config/global_config.yaml`)

| What | Example | Your Value |
|------|---------|-----------|
| Organization name | `your-organization` | _________ |
| Tenant name | `your-tenant` | _________ |
| Databricks host | `https://xyz.cloud.databricks.com` | _________ |
| Workspace ID | `1234567890123456` | _________ |
| UC catalog | `main` | _________ |
| Bronze schema | `bronze` | _________ |
| Silver schema | `silver` | _________ |
| Audit schema | `audit` | _________ |

### 2. Environment Variables (Set in Databricks Workspace)

Set these via **Databricks Secrets** or **Job Configuration**:

| Variable | Example | Your Value |
|----------|---------|-----------|
| `UC_CATALOG` | `main` | _________ |
| `UC_BRONZE_SCHEMA` | `bronze` | _________ |
| `UC_SILVER_SCHEMA` | `silver` | _________ |
| `UC_AUDIT_SCHEMA` | `audit` | _________ |
| `CHECKPOINT_ROOT` | `/Volumes/main/system/checkpoints` | _________ |
| `SCHEMA_TRACKING_ROOT` | `/Volumes/main/system/schema_tracking` | _________ |
| `ENVIRONMENT` | `dev` | _________ |

### 3. Connection Details (for JDBC/API sources)

If using databases or APIs, configure profiles in `config/global_config.yaml`:

```yaml
connections:
  jdbc_profiles:
    oracle_dev:
      url: jdbc:oracle:thin:@//YOUR-HOST:1521/YOUR-SERVICE  # ← Your DB host
      user_env: ORACLE_DEV_USER
      password_env: ORACLE_DEV_PASSWORD
  api_profiles:
    my_api:
      base_url: https://api.your-domain.com  # ← Your API URL
      auth_type: bearer
      token_env: API_TOKEN
```

Then set credentials as env vars:
```
ORACLE_DEV_HOST = your-db-host
ORACLE_DEV_USER = your-db-user
ORACLE_DEV_PASSWORD = your-db-password
API_TOKEN = your-api-token
```

### 4. Data Source Paths (`config/source_registry.csv`)

Define your data locations:

```csv
product_name,source_system,source_entity,source_type,source_path,landing_table,is_active
connect,cemc,countryriskdet,FILE,/mnt/dropzone/connect/cemccountryriskdet,main.bronze.connect_countryriskdet_landing,true
```

| Field | Example | Your Value |
|-------|---------|-----------|
| `source_path` | `/mnt/dropzone/connect/cemccountryriskdet` | _________ |
| `landing_table` | `main.bronze.connect_countryriskdet_landing` | _________ |
| `conformance_table` | `main.bronze.connect_countryriskdet_conformance` | _________ |
| `silver_table` | `main.silver.connect_countryriskdet` | _________ |

Optional orchestration metadata per source:

| Field | Example | Purpose |
|-------|---------|---------|
| `scheduler_name` | `daily_connect_job` | Logical scheduler/job name |
| `schedule_cron` | `0 0 2 * * ?` | Cron schedule metadata |
| `retention_days` | `30` | Retention policy metadata |
| `sttm_profile` | `standard_transfer` | Transfer profile metadata |

### 5. Optional: Column Mappings (`config/column_mapping.csv`)

If you need to rename/transform columns:

```csv
product_name,source_system,source_entity,source_column,conformance_column
connect,cemc,countryriskdet,raw_id,record_id
connect,cemc,countryriskdet,risk_score,risk_level
```

### 6. Optional: Data Quality Rules (`config/dq_rules.csv`)

Define DQ checks:

```csv
product_name,source_system,source_entity,rule_name,rule_type,column_name
connect,cemc,countryriskdet,check_id_not_null,not_null,record_id
connect,cemc,countryriskdet,check_id_unique,unique,record_id
```

---

## ⚙️ What Needs Infrastructure Setup (One-Time)

| Task | How | Can Be Automated |
|------|-----|------------------|
| Create Databricks cluster | Databricks UI → Create Cluster | No - needs manual setup |
| Install cluster libraries | Cluster → Libraries → PyPI packages | Yes - list provided |
| Install JDBC drivers | Cluster → Libraries → Maven (if needed) | Yes - list provided |
| Create Databricks workspace | Databricks admin setup | No - organizational task |
| Create UC metastore | Databricks admin setup | No - one-time workspace item |
| Set up secrets scope | Databricks CLI or UI | No - one-time admin task |

---

## 🚀 The Setup Process (In Databricks)

### Step 1: Update Configuration (5 mins)
Edit `config/global_config.yaml` with your values

### Step 2: Set Environment Variables (2 mins)
Create Databricks Secrets scope or Job env vars

### Step 3: Import Notebooks (1 min)
Drag-drop `notebooks_ipynb/` folder into Databricks Workspace

### Step 4: Run Setup Wizard (2 mins)
```python
from setup_wizard import run_complete_setup
success = run_complete_setup("/Workspace/path/to/config/global_config.yaml")
```

Alternative:
- Use `pipelines/databricks_setup_jobs.json` to create two one-time setup jobs:
  - `framework_initialize_infrastructure_once`
  - `framework_setup_wizard_once`

**That's it!** Everything else is automated ✅

---

## ❌ What You DON'T Need to Do

- ❌ Manually run SQL to create catalog/schemas
- ❌ Manually run SQL to create tables
- ❌ Manually create audit infrastructure
- ❌ Manually configure permissions
- ❌ Manually import each notebook individually
- ❌ Manually configure Spark
- ❌ Manually install Python packages in notebook
- ❌ Manually load sample metadata
- ❌ Manually run test commands

**All of this is handled by `setup_wizard.py`**

---

## Verification Checklist

After running setup wizard, verify:

- [ ] Databricks cluster is running & connected
- [ ] `config/global_config.yaml` has your actual values
- [ ] Environment variables set in Databricks workspace
- [ ] Notebooks imported to workspace
- [ ] Setup wizard ran successfully
- [ ] Dry-run test passed (returned plan, not error)
- [ ] Can query audit tables: `SELECT * FROM main.audit.pipeline_runs`

---

## Troubleshooting

### "Module not found"
→ Ensure all notebooks are imported and sys.path.append includes directories

### "Table already exists"
→ This is fine - setup is idempotent, can re-run safely

### "Permission denied on catalog"
→ Check job principal has `USE CATALOG`, `USE SCHEMA` grants on UC objects

### "JDBC connection failed"
→ Verify JDBC driver installed via Cluster Libraries + credentials in env vars

### "Empty results []"
→ Check `is_active=true` in `config/source_registry.csv`

---

## Summary

```
Time to Production:
- Configuration: 5 mins (edit YAML + set env vars)
- Automation: 2 mins (run setup wizard)
- Testing: 1 min (run test command)
= 8 mins TOTAL ✅
```

**Only 3 things you need to provide:**
1. ✏️ Your Databricks workspace details
2. ✏️ Your data source locations/credentials
3. ✏️ Your UC catalog/schema names

**Recurring pipeline should run only:**
- `notebooks/05_orchestration/framework_orchestrator.py`

**One-time setup should run:**
- `notebooks/05_orchestration/initialize_framework.py`
- `notebooks/05_orchestration/setup_wizard.py`

**Everything else is automated!**
