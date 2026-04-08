# ADR-004: Environment Parameterization Strategy

## Status
Accepted

## Context
Framework needed to support dev/qa/prod environments without code duplication:
1. **Environment variables** - Runtime injection, no code changes
2. **Config files** - Version-controlled, portable
3. **Hybrid** - Both env vars + config files

## Decision
Adopted **hybrid approach**: environment variables for secrets/deployment-specific values, config files for business logic.

## Rationale
- **Environment Variables:**
  - Best for: Database credentials, cloud storage paths, hosts, API tokens
  - Pattern: `${VAR_NAME:default_value}`
  - Resolution: At runtime, supports `.env` files
  
- **Config Files:**
  - Best for: Retry policies, load type, publish strategy
  - Pattern: Checked into Git
  - Resolution: At load time

- **Separation of Concerns:**
  - Secrets never in code/config files (security)
  - Business logic in code/config (auditability)
  - Deployment specifics in env vars (flexibility)

## Consequences
- `.env.example` provides template for required variables
- `global_config.yaml` provides business defaults
- Developers: Copy `.env.example` to `.env`, fill in local values
- DevOps: Set environment variables in deployment (Databricks Jobs, GitHub Actions)

## Configuration Hierarchy (lowest to highest priority)
1. Code defaults
2. global_config.yaml
3. environment variables

## Example Substitution Patterns
```yaml
databricks:
  host: ${DATABRICKS_HOST}
  catalog: ${UC_CATALOG:default_catalog}
  bronze_schema: ${UC_BRONZE_SCHEMA:bronze}
```

## Related Decisions
- ADR-005: Secrets Management Integration
