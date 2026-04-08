# End-to-End Examples

This directory contains complete, working examples for common ingestion patterns.

## Examples Included

1. **example-01-csv-batch-files** - Ingest CSV files from cloud storage (simple)
2. **example-02-jdbc-oracle** - Pull data from Oracle database with incremental watermarks (medium)
3. **example-03-rest-api-polling** - Consume REST API with pagination (advanced)

## Quick Start: CSV Batch Files

### Step 1: Add source registry entry

**config/source_registry.csv** (add this row):
```csv
product_name,source_system,source_entity,source_type,connection_profile,landing_table,conformance_table,silver_table,load_type,active,file_path,file_format,source_options_json,dq_enabled,publish_strategy,merge_keys,landing_table_type,landing_table_path,conformance_table_type,conformance_table_path,silver_table_type,silver_table_path
SALES,ECOMMERCE,ORDERS,FILE,,my_catalog.bronze.orders_landing,my_catalog.bronze.orders_conform,my_catalog.silver.orders,full,true,s3://mybucket/orders/*.csv,csv,{},true,append,,managed,,managed,,managed,
```

### Step 2: Define column mappings

**config/column_mapping.csv** (add these rows):
```csv
product_name,source_system,source_entity,landing_column,conformance_column,conformance_datatype,nullable,default_value,transformation_sql,column_order
SALES,ECOMMERCE,ORDERS,order_id,order_id,bigint,false,,, 1
SALES,ECOMMERCE,ORDERS,customer_id,customer_id,string,false,,, 2
SALES,ECOMMERCE,ORDERS,order_date,order_date,date,true,,CAST(order_date AS DATE), 3
SALES,ECOMMERCE,ORDERS,order_amount,order_amount,double,true,0,, 4
SALES,ECOMMERCE,ORDERS,status,status,string,false,,UPPER(status), 5
```

### Step 3: Define DQ rules

**config/dq_rules.csv** (add these rows):
```csv
product_name,source_system,source_entity,rule_name,rule_type,layer,column,condition,threshold_percent,blocking,enabled
SALES,ECOMMERCE,ORDERS,orders_required_cols,required_columns,landing,order_id;customer_id;order_date,,0,true,true
SALES,ECOMMERCE,ORDERS,orders_not_null,non_nullable,conformance,order_id,order_id IS NOT NULL,0,true,true
SALES,ECOMMERCE,ORDERS,status_allowed,allowed_values,conformance,status,status IN ('PENDING','SHIPPED','DELIVERED'),10,false,true
```

### Step 4: Run orchestrator

```bash
# Dry run to validate
python notebooks/05_orchestration/framework_orchestrator.py --product-name SALES --source-system ECOMMERCE --source-entity ORDERS

# Execute (requires Spark/Databricks)
python notebooks/05_orchestration/framework_orchestrator.py --product-name SALES --source-system ECOMMERCE --source-entity ORDERS --execute
```

## Detailed Examples

See individual example folders for:
- Complete config files
- Sample data
- Expected outputs
- Customization options

