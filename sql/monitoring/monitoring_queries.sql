-- SQL monitoring queries for ingestion framework
-- Use these to track pipeline health, performance, and anomalies

-- 1. Pipeline Run Summary (Last 7 days)
SELECT
  framework_name,
  source_system,
  source_entity,
  COUNT(*) as total_runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
  AVG(rows_ingested) as avg_rows_ingested,
  MAX(run_timestamp) as last_run
FROM {catalog}.{audit_schema}.pipeline_runs
WHERE run_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY framework_name, source_system, source_entity
ORDER BY last_run DESC;

-- 2. Data Quality Failure Rate (Last 24 hours)
SELECT
  source_system,
  source_entity,
  rule_name,
  rule_type,
  COUNT(*) as total_checks,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_checks,
  SUM(failed_row_count) as total_failed_rows,
  ROUND(100.0 * SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
FROM {catalog}.{audit_schema}.dq_results
WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY source_system, source_entity, rule_name, rule_type
HAVING SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) > 0
ORDER BY failure_rate_pct DESC;

-- 3. Critical Alerts (Last 7 days)
SELECT
  alert_id,
  severity,
  alert_type,
  title,
  source_system,
  source_entity,
  timestamp,
  message
FROM {catalog}.{audit_schema}.alerts
WHERE severity IN ('CRITICAL', 'ERROR')
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
ORDER BY timestamp DESC;

-- 4. Performance Degradation - Slow Queries
SELECT
  query_name,
  COUNT(*) as executions,
  MIN(duration_seconds) as min_seconds,
  AVG(duration_seconds) as avg_seconds,
  MAX(duration_seconds) as max_seconds,
  AVG(throughput_mbps) as avg_throughput_mbps,
  MAX(recorded_at) as last_recorded
FROM {catalog}.{audit_schema}.performance_metrics
WHERE recorded_at >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY query_name
HAVING AVG(duration_seconds) > 30  -- Flag queries over 30 seconds
ORDER BY avg_seconds DESC;

-- 5. Rejection Summary - Data Quality Issues
SELECT
  source_system,
  source_entity,
  rejection_reason,
  COUNT(*) as rejected_count,
  MAX(rejected_at) as most_recent,
  MIN(rejected_at) as oldest
FROM {catalog}.{audit_schema}.rejects
WHERE rejected_at >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY source_system, source_entity, rejection_reason
ORDER BY rejected_count DESC;

-- 6. Load Success Rate by Source (trending over 30 days)
SELECT
  source_system,
  source_entity,
  DATE(run_timestamp) as run_date,
  COUNT(*) as runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
  ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate_pct
FROM {catalog}.{audit_schema}.pipeline_runs
WHERE run_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY source_system, source_entity, DATE(run_timestamp)
ORDER BY run_date DESC, source_system, source_entity;

-- 7. Retry Exhaustion Events (Last 7 days)
SELECT
  alert_id,
  title,
  source_system,
  source_entity,
  timestamp,
  message
FROM {catalog}.{audit_schema}.alerts
WHERE alert_type = 'RETRY_EXHAUSTION'
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
ORDER BY timestamp DESC;

-- 8. Storage and Row Count Trends
-- (Run periodically to track data growth)
SELECT
  source_system,
  source_entity,
  MAX(rows_ingested) as current_row_count,
  SUM(rows_ingested) as cumulative_ingested,
  COUNT(*) as total_loads,
  MAX(run_timestamp) as last_load_time
FROM {catalog}.{audit_schema}.pipeline_runs
GROUP BY source_system, source_entity
ORDER BY cumulative_ingested DESC;

-- 9. Failed Loads Details (for root cause analysis)
SELECT
  load_id,
  source_system,
  source_entity,
  status,
  error_message,
  rows_ingested,
  rows_rejected,
  run_timestamp
FROM {catalog}.{audit_schema}.pipeline_runs
WHERE status = 'FAILED'
  AND run_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
ORDER BY run_timestamp DESC;

-- 10. Alert Severity Distribution (trend)
SELECT
  severity,
  COUNT(*) as alert_count,
  DATE(timestamp) as alert_date
FROM {catalog}.{audit_schema}.alerts
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY severity, DATE(timestamp)
ORDER BY alert_date DESC, alert_count DESC;
