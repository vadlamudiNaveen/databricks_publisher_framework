# ADR-003: Orchestration: Sequential vs. Parallel Processing

## Status
Accepted (Parallel as Future)

## Context
Initial release used sequential processing (one source at a time). Need to decide:
1. **Sequential** - Simple, stable, easy to debug
2. **Parallel** - Better throughput, complex error handling

## Decision
Adopted **parallel processing** as the future default (v2.0+), sequential remains baseline for stability.

## Rationale
- **Pro Sequential:**
  - Simpler error handling and debugging
  - Lower resource contention on cluster
  - Stable baseline for teams learning framework
  
- **Con Sequential:**
  - N sources = N × avg_time latency (poor throughput with many sources)
  - Cluster idle time between source runs

- **Pro Parallel:**
  - ~4x throughput with 4 workers (typical)
  - Better cluster resource utilization
  - Faster overall pipeline runtime
  
- **Con Parallel:**
  - Complex error aggregation
  - Requires thread-safe logging and state management
  - Harder to debug (interleaved logs)

## Consequences
- v1.0: Sequential (stable, proven)
- v1.5: Parallel with `--parallel <n>` flag (opt-in)
- v2.0: Parallel as default, sequential via `--serial` flag

## Implementation Notes
- Use `ThreadPoolExecutor` for Python thread-based parallelism
- Audit logging already thread-safe (mutex in config_loader)
- Each thread gets its own Spark session context
- Dependency graph support in v2.0 for ordered execution

## Related Decisions
- ADR-006: Delta Live Tables Integration (alternative orchestration)
