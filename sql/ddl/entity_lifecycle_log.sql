-- Entity lifecycle log table for full change tracking
CREATE TABLE IF NOT EXISTS audit.entity_lifecycle_log (
  entity_id STRING,
  event_type STRING,
  event_details STRING,
  user STRING,
  event_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);