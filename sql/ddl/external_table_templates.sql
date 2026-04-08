-- External table templates for landing/conformance/silver
-- Replace ALL <...> tokens with real values before execution.
-- Example:
-- <catalog> = main
-- <bronze_schema> = bronze
-- <silver_schema> = silver
-- <entity> = connect_countryriskdet

-- Landing external table
-- CREATE TABLE IF NOT EXISTS <catalog>.<bronze_schema>.<entity>_landing
-- USING DELTA
-- LOCATION '<landing_table_path>';

-- Conformance external table
-- CREATE TABLE IF NOT EXISTS <catalog>.<bronze_schema>.<entity>_conformance
-- USING DELTA
-- LOCATION '<conformance_table_path>';

-- Silver external table
-- CREATE TABLE IF NOT EXISTS <catalog>.<silver_schema>.<entity>
-- USING DELTA
-- LOCATION '<silver_table_path>';

-- Merge-ready silver table (optional clustering/partition)
-- CREATE TABLE IF NOT EXISTS <catalog>.<silver_schema>.<entity>
-- USING DELTA
-- PARTITIONED BY (<partition_col>)
-- LOCATION '<silver_table_path>';

-- Optional managed table variant (if you decide to use managed instead of external)
-- CREATE TABLE IF NOT EXISTS <catalog>.<bronze_schema>.<entity>_landing USING DELTA;
-- CREATE TABLE IF NOT EXISTS <catalog>.<bronze_schema>.<entity>_conformance USING DELTA;
-- CREATE TABLE IF NOT EXISTS <catalog>.<silver_schema>.<entity> USING DELTA;
