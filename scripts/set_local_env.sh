#!/bin/zsh
# Set required environment variables for local dry run of the Databricks ingestion framework
export UC_CATALOG=local_catalog
export UC_BRONZE_SCHEMA=bronze
export UC_SILVER_SCHEMA=silver
export UC_AUDIT_SCHEMA=audit
export IKEA_ENV=dev
export IKEA_TENANT=ikea
export DATABRICKS_HOST=localhost
export DATABRICKS_WORKSPACE_ID=1
export CHECKPOINT_ROOT=/tmp/checkpoints
export SCHEMA_TRACKING_ROOT=/tmp/schema_tracking

echo "Environment variables set for local dry run."
