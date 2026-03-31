-- Validation query pack for PASS/FAIL monitoring dashboards
-- Default catalog: main  |  Audit schema: audit  |  DQ schema: dq
-- To change catalog, update the USE CATALOG statement below.
-- To change schemas, find-replace 'audit.' and 'dq.' in this file.

USE CATALOG main;

-- Q1: Recent pipeline run health
SELECT
  date_trunc('hour', run_ts) AS run_hour,
  source_system,
  source_entity,
  status,
  COUNT(*) AS run_count,
  SUM(rows_in) AS total_rows_in,
  SUM(rows_out) AS total_rows_out
FROM audit.pipeline_runs
WHERE run_ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1,2,3,4
ORDER BY run_hour DESC, source_system, source_entity;

-- Q2: Latest failed runs with reason
SELECT
  run_ts,
  run_id,
  source_system,
  source_entity,
  status,
  error_message
FROM audit.pipeline_runs
WHERE status = 'FAILED'
ORDER BY run_ts DESC
LIMIT 100;

-- Q3: DQ fail rate by source (standard DQ)
WITH dq AS (
  SELECT
    run_id,
    source_system,
    source_entity,
    SUM(CASE WHEN dq_status = 'PASS' THEN row_count ELSE 0 END) AS pass_rows,
    SUM(CASE WHEN dq_status = 'FAIL' THEN row_count ELSE 0 END) AS fail_rows
  FROM audit.dq_rule_results
  WHERE run_ts >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY 1,2,3
)
SELECT
  run_id,
  source_system,
  source_entity,
  pass_rows,
  fail_rows,
  CASE WHEN pass_rows + fail_rows = 0 THEN 0 ELSE fail_rows / (pass_rows + fail_rows) END AS fail_rate
FROM dq
ORDER BY fail_rate DESC, run_id DESC;

-- Q4: Top failing DQ rules
SELECT
  source_system,
  source_entity,
  dq_failed_rule,
  SUM(row_count) AS failed_rows,
  MAX(run_ts) AS last_seen_ts
FROM audit.dq_rule_results
WHERE dq_status = 'FAIL'
GROUP BY 1,2,3
ORDER BY failed_rows DESC;

-- Q5: Bronze DQ validation status summary
SELECT
  date_trunc('hour', timestamp) AS event_hour,
  source_system,
  source_entity,
  validation_type,
  validation_status,
  COUNT(*) AS checks
FROM audit.bronze_dq_results
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1,2,3,4,5
ORDER BY event_hour DESC, source_system, source_entity;

-- Q6: Bronze DQ failures with details
SELECT
  timestamp,
  run_id,
  source_system,
  source_entity,
  validation_type,
  validation_status,
  error_details
FROM audit.bronze_dq_results
WHERE validation_status = 'FAIL'
ORDER BY timestamp DESC
LIMIT 200;

-- Q7: Validation events timeline (stage-level)
SELECT
  event_ts,
  run_id,
  source_system,
  source_entity,
  event_name,
  event_status,
  event_payload
FROM audit.validation_events
WHERE event_ts >= current_timestamp() - INTERVAL 3 DAYS
ORDER BY event_ts DESC;

-- Q8: Reject volume trend
SELECT
  date_trunc('hour', rejected_ts) AS reject_hour,
  source_system,
  source_entity,
  COUNT(*) AS reject_rows
FROM dq.rejects
WHERE rejected_ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1,2,3
ORDER BY reject_hour DESC, source_system, source_entity;

-- Q9: Run-level quality gate view (PASS/FAIL)
WITH runs AS (
  SELECT
    run_id,
    source_system,
    source_entity,
    rows_in,
    rows_out,
    status,
    run_ts
  FROM audit.pipeline_runs
),
dq AS (
  SELECT
    run_id,
    source_system,
    source_entity,
    SUM(CASE WHEN dq_status = 'FAIL' THEN row_count ELSE 0 END) AS dq_fail_rows
  FROM audit.dq_rule_results
  GROUP BY 1,2,3
),
bronze AS (
  SELECT
    run_id,
    source_system,
    source_entity,
    SUM(CASE WHEN validation_status = 'FAIL' THEN 1 ELSE 0 END) AS bronze_fail_checks
  FROM audit.bronze_dq_results
  GROUP BY 1,2,3
)
SELECT
  r.run_ts,
  r.run_id,
  r.source_system,
  r.source_entity,
  r.status AS pipeline_status,
  COALESCE(d.dq_fail_rows, 0) AS dq_fail_rows,
  COALESCE(b.bronze_fail_checks, 0) AS bronze_fail_checks,
  CASE
    WHEN r.status = 'FAILED' THEN 'FAIL'
    WHEN COALESCE(d.dq_fail_rows, 0) > 0 THEN 'FAIL'
    WHEN COALESCE(b.bronze_fail_checks, 0) > 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS overall_quality_gate
FROM runs r
LEFT JOIN dq d
  ON r.run_id = d.run_id AND r.source_system = d.source_system AND r.source_entity = d.source_entity
LEFT JOIN bronze b
  ON r.run_id = b.run_id AND r.source_system = b.source_system AND r.source_entity = b.source_entity
ORDER BY r.run_ts DESC;
