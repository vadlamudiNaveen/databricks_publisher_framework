# Framework Features & Capabilities

**Version: 2.0.0 (10/10 Edition)**

This document outlines all major features and how to use them.

## Table of Contents
1. [Core Features](#core-features)
2. [Advanced Features](#advanced-features)
3. [Operational Features](#operational-features)
4. [Developer Features](#developer-features)

---

## Core Features

### 1. Metadata-Driven Configuration
Control everything via CSVs or Databricks tables—zero code changes needed.

**Files:**
- `config/source_registry.csv` - Data source definitions
- `config/column_mapping.csv` - Schema transformations
- `config/dq_rules.csv` - Data quality rules
- `config/publish_rules.csv` - Silver layer configuration

**Keys:** `schemas/source_registry.schema.json` etc. validate your configs

### 2. Multi-Source Ingestion
Ingest from diverse sources with adapters:
- **FILE**: CSV, Parquet, JSON, JSONL (batch or Auto Loader streaming)
- **JDBC**: Oracle, PostgreSQL, MySQL, etc.
- **API**: REST with pagination and rate limits
- **Custom**: Write your own plugin adapter

### 3. Medallion Architecture
Clean data layer separation:
- **Landing**: Raw 1:1 copy from source (with technical metadata)
- **Conformance**: Schema-mapped, conformed data types
- **Silver**: Business-ready, deduplicated, quality-assured data

### 4. Data Quality Checks
Validate data at multiple stages:
- Bronze DQ (landing layer): Required columns, null checks, basic validations
- Conformance DQ: Business rule checks, allowed values, ranges
- Rejection tracking: Capture failed rows for analysis

---

## Advanced Features

### 1. Error Recovery & Transactions
**Module:** `notebooks/00_common/transaction_manager.py`

Automatic checkpoint creation at each pipeline stage:

```python
from transaction_manager import TransactionManager

mgr = TransactionManager(
    checkpoint_root="abfss://framework@<storage-account>.dfs.core.windows.net/checkpoints",
    transaction_id="load_20250801_001"
)

txn = mgr.start_transaction(
    load_id="L001",
    product_name="sales",
    source_system="crm",
    source_entity="accounts"
)

# Save checkpoint after each successful stage
mgr.save_checkpoint("ingestion", 1, {"rows": 1000})
mgr.save_checkpoint("conformance", 2, {"errors": 0})

# On failure
mgr.mark_stage_failed("publish", "Insert error", recovery_action="RETRY")

# On success
mgr.mark_transaction_success()
```

**Benefits:**
- Resume from last checkpoint on failure
- No duplicate processing
- Complete audit trail

### 2. Monitoring & Alerting
**Module:** `notebooks/00_common/alert_manager.py`

Real-time alerts for pipeline health:

```python
from alert_manager import AlertManager, AlertSeverity

mgr = AlertManager(spark=spark)

# Check DQ failure rate
alert = mgr.check_dq_failure_rate(
    product_name="sales",
    source_system="crm",
    source_entity="accounts",
    load_id="L001",
    total_rows=10000,
    failed_rows=1500,  # 15% > 10% threshold
    threshold_percent=10.0
)

# Check performance
alert = mgr.check_performance_degradation(
    product_name="sales",
    source_system="crm",
    source_entity="accounts",
    load_id="L001",
    duration_seconds=180,
    baseline_seconds=60  # 3x slower
)

# Write to table
mgr.write_alerts_to_table(
    catalog="my_catalog",
    schema="audit",
    table_name="alerts"
)
```

**Supported Alerts:**
- DQ failure rate threshold
- Retry exhaustion
- Performance degradation
- Custom business alerts

### 3. Secrets Management
**Module:** `notebooks/00_common/secrets_manager.py`

Secure credential handling with pluggable backends:

```python
from secrets_manager import configure_secrets_from_global_config

# Configure from global_config.yaml
mgr = configure_secrets_from_global_config(global_config, spark=spark)

# Retrieve secrets
db_password = mgr.get_required_secret("ORACLE_DB_PASSWORD")
api_token = mgr.get_secret("API_TOKEN", default="")
```

**Backends:**
- Environment variables (default)
- Databricks Secrets API (enterprise)
- Custom (implement SecretsBackend class)

**Configuration:**
```yaml
# global_config.yaml
secrets:
  backend: databricks_secrets
  databricks_scope: default
```

### 4. Plugin Architecture
**Module:** `notebooks/00_common/adapter_registry.py`

Extend framework with custom ingestion adapters:

```python
from adapter_registry import IngestAdapter, AdapterRegistry

class KafkaAdapter(IngestAdapter):
    SOURCE_TYPE = "KAFKA"
    
    def ingest(self, spark=None):
        # Your Kafka ingestion logic
        return spark.readStream.format("kafka").load(...)
    
    def validate(self):
        # Validate configuration
        return True, []

# Register adapter
AdapterRegistry.register("KAFKA", KafkaAdapter)

# Use it
adapter = AdapterRegistry.get_or_create_adapter(
    "KAFKA",
    source={"broker": "localhost:9092", "topic": "orders"},
)
df = adapter.ingest(spark=spark)
```

### 5. Query Optimization
**Module:** `notebooks/00_common/optimization_advisor.py`

Get table optimization recommendations:

```python
from optimization_advisor import OptimizationAdvisor

# Get partitioning recommendation
rec = OptimizationAdvisor.recommend_partitioning(
    table_name="my_catalog.silver.orders",
    column_cardinalities={"region": 50, "date": 1000, "product_id": 10000},
    total_rows=100000000
)
# Output: Recommend partitioning by 'region'

# Get Z-order recommendation
rec = OptimizationAdvisor.recommend_zorder(
    table_name="my_catalog.silver.orders",
    high_cardinality_columns=["customer_id", "product_id"]
)
# Output: Run OPTIMIZE ... ZORDER BY (customer_id, product_id)
```

### 6. Parallel Processing
**Module:** `notebooks/05_orchestration/framework_orchestrator.py`

Process multiple sources concurrently:

```bash
# Sequential (stable, default)
python framework_orchestrator.py --execute

# Parallel with 4 workers
python framework_orchestrator.py --execute --parallel 4

# Sequential (explicit)
python framework_orchestrator.py --execute --serial
```

**Performance:**
- Sequential N sources: N × avg_time
- Parallel (4 workers): ~N/4 × avg_time (typical)

---

## Operational Features

### 1. Terraform Infrastructure Provisioning
**Directory:** `terraform/`

Automatically create and manage Databricks resources:

```bash
cd terraform
terraform init
terraform plan -var-file="environments/dev.tfvars"
terraform apply
```

**Provisioned Resources:**
- Unity Catalog
- Bronze, Silver, Audit, Control schemas
- All required audit tables
- Permissions for service principals

### 2. Monitoring Queries
**Directory:** `sql/monitoring/`

Pre-built SQL queries for operational visibility:

```sql
-- Pipeline health summary
SELECT source_system, COUNT(*) as runs, 
  SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) as successes
FROM audit.pipeline_runs
WHERE run_timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAY
GROUP BY source_system;

-- DQ failure rate
SELECT source_system, SUM(failed_row_count) as rejections
FROM audit.dq_results
WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
GROUP BY source_system;

-- Critical alerts
SELECT * FROM audit.alerts
WHERE severity = 'CRITICAL'
ORDER BY timestamp DESC LIMIT 10;
```

### 3. Audit & Compliance
All actions logged to centralized audit tables:
- `audit.pipeline_runs` - All orchestrator executions
- `audit.dq_results` - Data quality check results
- `audit.rejects` - Rejected rows with reasons
- `audit.alerts` - All system alerts
- `audit.performance_metrics` - Query execution metrics

---

## Developer Features

### 1. JSON Schema Validation
**Directory:** `schemas/`

Strict validation of all metadata:

```bash
# Validate configs against schemas
python scripts/validate_configs.py

# Checks:
# - Required columns present
# - Data types correct
# - Cross-reference integrity
# - Semantic constraints
```

### 2. Architecture Decision Records (ADRs)
**Directory:** `docs/adr/`

Why key decisions were made:
- ADR-001: Auto Loader vs. DLT
- ADR-002: CSV vs. Table metadata
- ADR-003: Sequential vs. Parallel
- ADR-004: Environment parameterization
- ADR-005: Secrets management

### 3. Examples & Walkthroughs
**Directory:** `docs/examples/`

Complete working examples:
- CSV file ingestion (simple)
- JDBC database (medium)
- REST API (advanced)

Each includes sample data, configs, and expected outputs.

### 4. Type Hints & IDE Support
Framework includes optional type hint annotations for IDE autocompletion and type checking.

### 5. Test Suite
```bash
# Unit tests
pytest tests/unit -v

# Integration tests
pytest tests/integration -v

# All tests
pytest tests -v --cov=notebooks
```

---

## Configuration Reference

### global_config.yaml
```yaml
organization:
  client_name: YOUR_ORG
  environment: ${ENVIRONMENT:dev}  # dev, qa, prod

execution:
  default_mode: dry_run            # dry_run or execute
  max_retries: 2
  retry_by_source_type:
    api: { max_retries: 3, backoff_seconds: 3 }
    jdbc: { max_retries: 2, backoff_seconds: 2 }
    file: { max_retries: 1, backoff_seconds: 2 }

databricks:
  host: ${DATABRICKS_HOST}
  catalog: ${UC_CATALOG}
  bronze_schema: bronze
  silver_schema: silver
  audit_schema: audit

secrets:
  backend: env_vars  # or: databricks_secrets
  databricks_scope: default

metadata:
  mode: csv  # or: table
  csv_root: config

connections:
  jdbc_profiles:
    oracle_prod:
      url: ${ORACLE_URL}
      driver: oracle.jdbc.OracleDriver
      user_env: ORACLE_USER
      password_env: ORACLE_PASSWORD

audit:
  pipeline_runs_table: audit.pipeline_runs
  dq_results_table: audit.dq_results
  rejects_table: audit.rejects
```

---

## Quick Tips

### Dry-Run vs. Execute
```bash
# Plan execution without writing
python framework_orchestrator.py
# or
python framework_orchestrator.py --execute=false

# Actually execute
python framework_orchestrator.py --execute
```

### Filter Execution
```bash
# One product
python framework_orchestrator.py--product-name SALES --execute

# One source
python framework_orchestrator.py --source-system CRM --source-entity ACCOUNTS --execute

# Multiple filters (AND logic)
python framework_orchestrator.py --product-name SALES --source-system CRM --execute
```

### Debug Failures
```bash
# With verbose logging
export LOGLEVEL=DEBUG
python framework_orchestrator.py --execute

# Check transaction checkpoints
ls -la /dbfs/Volumes/<catalog>/<schema>/framework/checkpoints/<transaction_id>/

# Query audit tables
SELECT * FROM audit.pipeline_runs WHERE status='FAILED' ORDER BY run_timestamp DESC;
SELECT * FROM audit.alerts WHERE severity='CRITICAL' ORDER BY timestamp DESC;
```

---

**For more details, see:**
- [Architecture Guide](docs/architecture.md)
- [Onboarding Guide](docs/onboarding_guide.md)
- [Runbook](docs/runbook.md)
- [ADRs](docs/adr/)
- [Examples](docs/examples/)
