# Runbook

## Common Operations
- Start pipeline run for active sources.
- Validate row counts in landing, conformance, and silver tables.
- Review DQ rejects and audit results.
- Validate metadata before runs: python scripts/validate_configs.py.

## External Location Ownership
1. New external locations must be created by the platform/IDNAP team with metastore admin permissions.
2. Data engineering team provides request details: storage account, container, environment, read_only flag, and principal list.
3. Platform team returns created external location names and grants.

## Managed vs External Table Guidance
- Landing (bronze raw): external is allowed and recommended when raw data must remain in governed storage paths.
- Conformance/Silver: managed is default unless there is a clear governance or interoperability need for external.
- Audit and control tables: managed by default.
- If table_type=external in source metadata, corresponding *_table_path is mandatory.

## Failure Handling
1. Identify failed stage from audit.pipeline_runs.
2. Fix global configuration profile or source metadata issue.
3. Re-run from Landing where possible.

## Replay Guidance
- Keep Landing immutable and retained.
- Reprocess downstream layers from Landing for deterministic replay.

## Optional Silver Seed Task
- Runtime job includes an optional seed_silver_test_data task that runs after post-pipeline verification.
- Default behavior is disabled (seed_silver_execute=false) to avoid accidental test writes.
- Control variables in bundle config or per run:
- seed_silver_execute
- seed_silver_source_table
- seed_silver_target_table
- seed_silver_row_limit
- seed_silver_mode

## UC-first Snapshot Export
- Use notebooks/00_common/uc_export_utils.py for table snapshot exports to cloud URIs.
- Legacy DBFS mount paths are rejected by design.
- Supported export formats: delta, parquet, csv, json.
