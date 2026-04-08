# ADR-005: Secrets Management Strategy

## Status
Accepted

## Context
Framework needs to securely manage credentials (JDBC passwords, API tokens, DB URLs):
1. **Environment Variables** - Simple, widely supported
2. **Databricks Secrets API** - Enterprise-grade, audited
3. **HashiCorp Vault** - Complex, overkill for most users

## Decision
Adopted **pluggable secrets backend**: support both environment variables (default) and Databricks Secrets (optional).

## Rationale
- **Environment Variables:**
  - Simplest for local dev and small deployments
  - Works everywhere (no Databricks required)
  - Con: Not audited, potential for logging leaks
  
- **Databricks Secrets:**
  - Enterprise-grade security (encrypted, audited)
  - Secret rotation support
  - Scoped access control
  - Con: Requires Databricks workspace
  
- **Hybrid Pluggable Approach:**
  - Both modes supported via `secrets.backend: env_vars | databricks_secrets`
  - Teams choose what fits their security posture
  - Easy to add custom backends (Vault, AWS Secrets Manager, etc.)

## Consequences
- Default: Environment variables (no breaking changes)
- Opt-in: Databricks Secrets via `global_config.yaml`
- Migration: Secrets are completely abstracted (can switch at upgrade)

## Implementation
```yaml
# global_config.yaml
secrets:
  backend: databricks_secrets  # or: env_vars (default)
  databricks_scope: default    # Databricks Secrets scope name
```

## Recommendations
- **Development:** Use environment variables (simplicity)
- **Production:** Use Databricks Secrets API or HashiCorp Vault (security)
- Never log or print secrets (framework masks them automatically)

## Related Decisions
- ADR-004: Environment Parameterization Strategy
