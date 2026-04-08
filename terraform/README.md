# Terraform IaC for Databricks Ingestion Framework

This directory contains Terraform modules to provision all required Databricks resources (catalogs, schemas, tables, permissions).

## Prerequisites

1. Install Terraform: https://www.terraform.io/downloads.html
2. Install Databricks Terraform Provider: https://registry.terraform.io/providers/databricks/databricks/latest/docs
3. Configure Databricks authentication (via environment variables or config file)

## Quick Start

```bash
cd terraform

# Initialize Terraform state
terraform init

# Plan infrastructure for dev environment
terraform plan -var-file="environments/dev.tfvars" -out=dev.plan

# Apply to dev
terraform apply dev.plan

# Repeat for qa, prod
terraform plan -var-file="environments/qa.tfvars" -out=qa.plan
terraform apply qa.plan
```

## Directory Structure

```
terraform/
├── main.tf                    # Main configuration
├── variables.tf               # Input variables
├── outputs.tf                 # Output values
├── environments/
│   ├── dev.tfvars            # Dev environment values
│   ├── qa.tfvars             # QA environment values
│   └── prod.tfvars           # Prod environment values
├── modules/
│   ├── catalog/              # UC Catalog creation
│   ├── schemas/              # Schema creation
│   ├── tables/               # Table creation
│   └── permissions/          # IAM role permissions
└── scripts/
    ├── generate_from_csv.py  # Auto-generate Terraform from CSV configs
    └── validate.sh           # Validate Terraform
```

## Modules

### Catalog Module
Creates Unity Catalog and storage location.

### Schemas Module
Creates schemas (bronze, silver, audit, control) within catalog.

### Tables Module
Auto-generates table DDL from `source_registry.csv` and creates them.

### Permissions Module
Sets up RBAC for data engineers, analysts, and orchestration service principal.

## Environment Variables

Required environment variables for Databricks authentication:

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-pat-token
```

Or configure in `~/.databrickscfg`:

```
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = your-pat-token
```

## Features

✅ Multi-environment support (dev/qa/prod)
✅ Auto-generate tables from CSV metadata
✅ RBAC with service principals
✅ Drift detection and remediation
✅ Destroy entire environment (danger zone!)

## Notes

- State is stored locally (`.tfstate`). For production, use remote state (S3, Azure Blob, Terraform Cloud)
- Always run `terraform plan` before `apply`
- Use approval gates in CI/CD before applying prod changes
- Keep `.tfvars` files with secrets in `git-crypt` or similar

## Support

See `terraform/README.md` for detailed module documentation.
