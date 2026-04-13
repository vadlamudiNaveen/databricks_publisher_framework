-- Verification Script: Check Control Tables Have Data
-- Run this in Databricks SQL Editor to verify framework initialization

-- 1. Check row counts in all control tables
SELECT 'source_registry' as table_name, COUNT(*) as row_count 
FROM eng511_development_bronze.control.source_registry
UNION ALL
SELECT 'column_mapping' as table_name, COUNT(*) as row_count 
FROM eng511_development_bronze.control.column_mapping
UNION ALL
SELECT 'dq_rules' as table_name, COUNT(*) as row_count 
FROM eng511_development_bronze.control.dq_rules
UNION ALL
SELECT 'publish_rules' as table_name, COUNT(*) as row_count 
FROM eng511_development_bronze.control.publish_rules;

-- 2. Sample data from source_registry (should show at least one row)
SELECT product_name, source_system, source_entity, is_active, load_type 
FROM eng511_development_bronze.control.source_registry 
LIMIT 5;

-- 3. Sample data from column_mapping (should show at least one row)
SELECT product_name, source_system, source_entity, conformance_column, transform_expression 
FROM eng511_development_bronze.control.column_mapping 
LIMIT 5;

-- 4. Detailed table statistics (run each separately in Databricks SQL)
DESCRIBE DETAIL eng511_development_bronze.control.source_registry;

DESCRIBE DETAIL eng511_development_bronze.control.column_mapping;

DESCRIBE DETAIL eng511_development_bronze.control.dq_rules;

DESCRIBE DETAIL eng511_development_bronze.control.publish_rules;
