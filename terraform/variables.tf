variable "databricks_host" {
  description = "Databricks workspace host URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks workspace token"
  type        = string
  sensitive   = true
}

variable "environment" {
  description = "Environment name (dev, qa, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "Environment must be dev, qa, or prod."
  }
}

variable "catalog_name" {
  description = "Unity Catalog name"
  type        = string
  default     = "ingestion_framework"
}

variable "bronze_schema_name" {
  description = "Bronze schema name"
  type        = string
  default     = "bronze"
}

variable "silver_schema_name" {
  description = "Silver schema name"
  type        = string
  default     = "silver"
}

variable "audit_schema_name" {
  description = "Audit schema name"
  type        = string
  default     = "audit"
}

variable "control_schema_name" {
  description = "Control schema name"
  type        = string
  default     = "control"
}

variable "storage_root" {
  description = "Cloud storage root path for Unity Catalog (e.g., s3://my-bucket/uc-root, abfss://container@account.dfs.core.windows.net/uc-root)"
  type        = string
}

variable "force_destroy" {
  description = "Allow Terraform to destroy catalogs/schemas (danger!)"
  type        = bool
  default     = false
}
