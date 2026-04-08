# ADR-001: Auto Loader vs. Delta Live Tables

## Status
Accepted

## Context
The framework needed to decide between two ingestion patterns:
1. **Auto Loader** - Lower-level, fine-grained control over checkpoints, transformations
2. **Delta Live Table (DLT)** - Higher-level orchestration, built-in retries, visual DAG

## Decision
Adopted **Auto Loader** as primary ingestion mechanism for FILE sources, with future migration path to DLT.

## Rationale
- **Pro Auto Loader:**
  - Fine-grained control over schema evolution and error handling
  - Works well for mixed file formats (JSON + JSONL)
  - Can be embedded in orchestrator without external dependencies
  - Lower cognitive overhead for teams new to Databricks
  
- **Con Auto Loader:**
  - More manual checkpoint management
  - No built-in visual lineage in Databricks UI
  - Requires orchestration framework (this framework!)

- **Pro DLT:**
  - Automatic retries, idempotency, lineage
  - Visual DAG in Databricks UI
  - Native quality checks
  
- **Con DLT:**
  - Less flexible for mixed file formats
  - Requires Databricks workspace (not portable)
  - Different mental model vs. Spark DataFrame API

## Consequences
- Framework remains portable and works in custom Spark environments
- Teams must manage checkpoints explicitly (documented in runbook)
- Future: Can migrate to DLT if Databricks becomes mandatory
- ADR-007 discusses DLT integration roadmap

## Related Decisions
- ADR-007: Delta Live Tables Migration Path
