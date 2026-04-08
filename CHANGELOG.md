# CHANGELOG

## [v2.0.0] - 2025-04-08

### Major Features

#### Architecture & Design
- ✅ Removed all IKEA-specific references - framework now fully generic
- ✅ Added comprehensive metadata JSON schemas with strict validation
- ✅ Created Architecture Decision Records (ADRs) for all key decisions
- ✅ Added plugin architecture for custom ingestion adapters

#### Error Recovery & Transaction Management
- ✅ Implemented `TransactionManager` for checkpoint-based recovery
- ✅ Added automatic rollback support for multi-stage failures
- ✅ Resume capability from last successful checkpoint
- ✅ Complete transaction audit trail

#### Monitoring & Observability
- ✅ Implemented `AlertManager` for real-time pipeline alerts
- ✅ Added alert types: DQ failure rate, retry exhaustion, performance degradation
- ✅ Created `audit.alerts` table for centralized alert logging
- ✅ Added `PerformanceMonitor` for query execution metrics
- ✅ Pre-built monitoring SQL queries in `sql/monitoring/`

#### Operational Capabilities
- ✅ Implemented `SecretsManager` with pluggable backends (env vars, Databricks Secrets)
- ✅ Created `OptimizationAdvisor` for automatic table optimization recommendations
- ✅ Added Terraform IaC templates for infrastructure provisioning
- ✅ Created multi-environment (dev/qa/prod) Terraform configurations

#### Developer Experience
- ✅ Implemented `AdapterRegistry` for plugin-based source adapters
- ✅ Created `ParallelExecutor` for concurrent source processing
- ✅ Added `EnhancedOrchestrator` with parallel processing support
- ✅ Created comprehensive examples in `docs/examples/`
- ✅ Added detailed features guide in `docs/FEATURES.md`

### Documentation Improvements
- ✅ Generalized README to remove organization-specific references
- ✅ Added metadata schema documentation with JSON schema files
- ✅ Created Architecture Decision Records (ADRs) directory
- ✅ Added example walkthroughs for common patterns
- ✅ Created comprehensive features guide (`FEATURES.md`)
- ✅ Added best practices and deployment guide

### Infrastructure-as-Code
- ✅ Terraform module for UC Catalog creation
- ✅ Terraform module for schema provisioning
- ✅ Terraform module for audit table creation
- ✅ Multi-environment support (dev.tfvars, qa.tfvars, prod.tfvars)
- ✅ Auto-generated table DDL from metadata

### New Modules
- `notebooks/00_common/transaction_manager.py` - Error recovery
- `notebooks/00_common/alert_manager.py` - Monitoring & alerting
- `notebooks/00_common/secrets_manager.py` - Secrets management
- `notebooks/00_common/adapter_registry.py` - Plugin architecture
- `notebooks/00_common/parallel_executor.py` - Parallel processing
- `notebooks/00_common/optimization_advisor.py` - Query optimization
- `notebooks/05_orchestration/framework_orchestrator.py` - Consolidated orchestration (sequential + parallel)

### New Configuration Files
- `schemas/source_registry.schema.json` - Source metadata schema
- `schemas/column_mapping.schema.json` - Column mapping schema
- `schemas/dq_rules.schema.json` - DQ rules schema
- `schemas/publish_rules.schema.json` - Publish rules schema
- `schemas/source_options.schema.json` - Source options schema

### New Documentation
- `docs/adr/ADR-001-auto-loader-vs-dlt.md`
- `docs/adr/ADR-002-metadata-storage.md`
- `docs/adr/ADR-003-orchestration-parallelism.md`
- `docs/adr/ADR-004-environment-parameterization.md`
- `docs/adr/ADR-005-secrets-management.md`
- `docs/examples/README.md` - Example walkthroughs
- `docs/FEATURES.md` - Comprehensive features guide
- `sql/monitoring/monitoring_queries.sql` - Pre-built SQL queries
- `terraform/README.md` - IaC documentation
- `terraform/main.tf` - Terraform configuration
- `terraform/variables.tf` - Terraform variables
- `terraform/environments/dev.tfvars` - Dev environment values

### Breaking Changes
- None (backward compatible with v1.x)

### Deprecations
- None

### Known Issues
- Parallel processing (`--parallel`) is opt-in (v1.5+) and requires testing before production use
- DLT integration planned for v3.0

### Migration Guide
Existing v1.x installations can upgrade without breaking changes:
1. Update config files to use `ENVIRONMENT` instead of `IKEA_ENV`
2. Optionally adopt new features (parallel, secrets, terraform)
3. Review ADRs for design rationale

### Roadmap (Future Versions)

**v2.1** (Next Quarter)
- Type hints on all public APIs (currently optional)
- Improved test coverage (target: 95%+)
- CLI enhancements with rich terminal UI
- Shell completions (bash/zsh)

**v2.5** (H2 2025)
- Delta Live Tables integration
- HashiCorp Vault secrets backend
- Advanced dependency graph orchestration
- Kafka and Snowflake adapters

**v3.0** (2026)
- Full DLT migration as default
- gRPC-based remote adapter support
- Advanced time-travel and lineage tracking

### Contributors
- Framework Team

### Acknowledgments
Thanks to all who provided feedback and improvements!

---

### Upgrade Instructions

1. **From v1.x to v2.0:**
   ```bash
   git pull origin main
   python -m pytest tests/ -v  # Validate installation
   
   # Update configs
   sed -i 's/IKEA_ENV/ENVIRONMENT/g' config/global_config.yaml
   ```

2. **New Features:**
   - Parallel processing: `python framework_orchestrator.py --execute --parallel 4`
   - Terraform provisioning: `cd terraform && terraform apply`
   - Query optimization: Use `notebooks/00_common/optimization_advisor.py`

3. **Monitoring:**
   - Query `audit.alerts` for pipeline health
   - Run SQL from `sql/monitoring/monitoring_queries.sql`

---

For detailed information, see:
- [Features Guide](docs/FEATURES.md)
- [Architecture Guide](docs/architecture.md)
- [ADR Index](docs/adr/)
- [Examples](docs/examples/)
