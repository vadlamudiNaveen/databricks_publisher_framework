terraform {
  required_version = ">= 1.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Unity Catalog
resource "databricks_catalog" "main" {
  name            = var.catalog_name
  comment         = "Ingestion framework catalog for ${var.environment}"
  storage_root    = var.storage_root
  force_destroy   = var.force_destroy
}

# Schemas
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = var.bronze_schema_name
  comment      = "Bronze (landing) raw data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = var.silver_schema_name
  comment      = "Silver (conformance) processed data"
}

resource "databricks_schema" "audit" {
  catalog_name = databricks_catalog.main.name
  name         = var.audit_schema_name
  comment      = "Audit logging and pipeline metrics"
}

resource "databricks_schema" "control" {
  catalog_name = databricks_catalog.main.name
  name         = var.control_schema_name
  comment      = "Control tables for metadata (if using table mode)"
}

# Audit tables
resource "databricks_sql_table" "pipeline_runs" {
  catalog_name  = databricks_catalog.main.name
  schema_name   = databricks_schema.audit.name
  name          = "pipeline_runs"
  table_type    = "MANAGED"
  comment       = "Pipeline execution audit trail"

  column {
    name      = "framework_name"
    type      = "STRING"
    comment   = "Framework identifier"
  }

  column {
    name      = "load_id"
    type      = "STRING"
    comment   = "Unique load execution ID"
  }

  column {
    name      = "source_system"
    type      = "STRING"
    comment   = "Source system name"
  }

  column {
    name      = "source_entity"
    type      = "STRING"
    comment   = "Source entity name"
  }

  column {
    name      = "rows_ingested"
    type      = "BIGINT"
    comment   = "Number of rows ingested"
  }

  column {
    name      = "rows_rejected"
    type      = "BIGINT"
    comment   = "Number of rows rejected by DQ"
  }

  column {
    name      = "status"
    type      = "STRING"
    comment   = "SUCCESS, FAILED, PARTIAL"
  }

  column {
    name      = "error_message"
    type      = "STRING"
    comment   = "Error details if failed"
  }

  column {
    name      = "run_timestamp"
    type      = "TIMESTAMP"
    comment   = "When run started"
  }
}

# DQ results table
resource "databricks_sql_table" "dq_results" {
  catalog_name  = databricks_catalog.main.name
  schema_name   = databricks_schema.audit.name
  name          = "dq_results"
  table_type    = "MANAGED"
  comment       = "Data quality check results"

  column {
    name      = "load_id"
    type      = "STRING"
  }

  column {
    name      = "source_system"
    type      = "STRING"
  }

  column {
    name      = "source_entity"
    type      = "STRING"
  }

  column {
    name      = "rule_name"
    type      = "STRING"
  }

  column {
    name      = "rule_type"
    type      = "STRING"
  }

  column {
    name      = "status"
    type      = "STRING"
    comment   = "PASS or FAIL"
  }

  column {
    name      = "failed_row_count"
    type      = "BIGINT"
  }

  column {
    name      = "checked_at"
    type      = "TIMESTAMP"
  }
}

# Rejects table
resource "databricks_sql_table" "rejects" {
  catalog_name  = databricks_catalog.main.name
  schema_name   = databricks_schema.audit.name
  name          = "rejects"
  table_type    = "MANAGED"
  comment       = "Rejected rows from DQ failures"

  column {
    name      = "load_id"
    type      = "STRING"
  }

  column {
    name      = "source_system"
    type      = "STRING"
  }

  column {
    name      = "source_entity"
    type      = "STRING"
  }

  column {
    name      = "rejection_reason"
    type      = "STRING"
  }

  column {
    name      = "raw_record"
    type      = "STRING"
    comment   = "Full rejected record as JSON"
  }

  column {
    name      = "rejected_at"
    type      = "TIMESTAMP"
  }
}

# Alerts table
resource "databricks_sql_table" "alerts" {
  catalog_name  = databricks_catalog.main.name
  schema_name   = databricks_schema.audit.name
  name          = "alerts"
  table_type    = "MANAGED"
  comment       = "Pipeline alerts (performance, DQ, retries)"

  column {
    name      = "alert_id"
    type      = "STRING"
    comment   = "Unique alert ID"
  }

  column {
    name      = "severity"
    type      = "STRING"
    comment   = "INFO, WARNING, CRITICAL, ERROR"
  }

  column {
    name      = "alert_type"
    type      = "STRING"
    comment   = "DQ_FAILURE_RATE, RETRY_EXHAUSTION, PERFORMANCE_DEGRADATION"
  }

  column {
    name      = "title"
    type      = "STRING"
  }

  column {
    name      = "message"
    type      = "STRING"
  }

  column {
    name      = "load_id"
    type      = "STRING"
  }

  column {
    name      = "source_system"
    type      = "STRING"
  }

  column {
    name      = "source_entity"
    type      = "STRING"
  }

  column {
    name      = "timestamp"
    type      = "TIMESTAMP"
  }

  column {
    name      = "metadata"
    type      = "STRING"
    comment   = "JSON-encoded extra details"
  }
}

# Performance metrics table
resource "databricks_sql_table" "performance_metrics" {
  catalog_name  = databricks_catalog.main.name
  schema_name   = databricks_schema.audit.name
  name          = "performance_metrics"
  table_type    = "MANAGED"
  comment       = "Query and stage execution metrics"

  column {
    name      = "query_name"
    type      = "STRING"
  }

  column {
    name      = "duration_seconds"
    type      = "DOUBLE"
  }

  column {
    name      = "rows_processed"
    type      = "BIGINT"
  }

  column {
    name      = "bytes_processed"
    type      = "BIGINT"
  }

  column {
    name      = "throughput_mbps"
    type      = "DOUBLE"
  }

  column {
    name      = "recorded_at"
    type      = "TIMESTAMP"
  }
}

output "catalog_fqn" {
  value       = databricks_catalog.main.name
  description = "Fully qualified catalog name"
}

output "schemas" {
  value = {
    bronze   = databricks_schema.bronze.name
    silver   = databricks_schema.silver.name
    audit    = databricks_schema.audit.name
    control  = databricks_schema.control.name
  }
  description = "Schema names"
}

output "audit_tables" {
  value = {
    pipeline_runs        = databricks_sql_table.pipeline_runs.name
    dq_results           = databricks_sql_table.dq_results.name
    rejects              = databricks_sql_table.rejects.name
    alerts               = databricks_sql_table.alerts.name
    performance_metrics  = databricks_sql_table.performance_metrics.name
  }
  description = "Audit table FQNs"
}
