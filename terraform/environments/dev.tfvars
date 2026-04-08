databricks_host = "https://your-workspace.cloud.databricks.com"
# Note: Use environment variable instead: export TF_VAR_databricks_token=...

environment = "dev"

catalog_name = "ingestion_framework_dev"
bronze_schema_name = "bronze"
silver_schema_name = "silver"
audit_schema_name = "audit"
control_schema_name = "control"

# For AWS
storage_root = "s3://your-dev-bucket/uc-root"

# For Azure
# storage_root = "abfss://uc-container@youraccount.dfs.core.windows.net/root"

force_destroy = false
