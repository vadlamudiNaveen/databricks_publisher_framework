# ADR-002: CSV Metadata vs. Databricks Control Tables

## Status
Accepted

## Context
Framework needed to decide where to store control metadata (source registry, column mappings, DQ rules):
1. **CSV files** - Version-controlled, portable, local development friendly
2. **Databricks Tables** - Runtime flexibility, change tracking, no deployment needed

## Decision
Adopted **CSV as default** with **table mode as option** for runtime flexibility.

## Rationale
- **Pro CSV:**
  - Version control integration (Git)
  - No Databricks runtime required for local validation
  - Familiar to non-technical users (Excel)
  - Simple CI/CD integration
  - Portable across environments
  
- **Con CSV:**
  - Schema changes require code redeploy
  - No runtime change tracking
  - Single source of truth must be in Git

- **Pro Table Mode:**
  - Runtime metadata updates without code redeploy
  - Audit trail of changes
  - Central governance in Databricks
  
- **Con Table Mode:**
  - Requires Databricks cluster for local testing
  - Changes may not be version-controlled
  - Risk of config drift between environments

## Consequences
- Both modes supported via `metadata.mode: csv | table` config
- CSV is recommended for most teams
- Tables recommended for advanced governance scenarios
- Migration utilities provided to convert between modes

## Recommendations
- Use **CSV** for: Teams with Git-centric DevOps, frequent schema changes
- Use **Table** for: Large enterprises with central data governance, runtime flexibility needed

## Related Decisions
- ADR-004: Environment Parameterization
