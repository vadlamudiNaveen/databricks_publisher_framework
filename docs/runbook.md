# Runbook

## Common Operations
- Start pipeline run for active sources.
- Validate row counts in landing, conformance, and silver tables.
- Review DQ rejects and audit results.
- Validate metadata before runs: python scripts/validate_configs.py.

## Failure Handling
1. Identify failed stage from audit.pipeline_runs.
2. Fix global configuration profile or source metadata issue.
3. Re-run from Landing where possible.

## Replay Guidance
- Keep Landing immutable and retained.
- Reprocess downstream layers from Landing for deterministic replay.
