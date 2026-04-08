# Onboarding Guide

## Standard Onboarding Steps
1. Set environment values from config/.env.example.
2. Configure runtime settings and connection profiles in config/global_config.yaml.
3. Ensure Unity Catalog external locations and grants are ready for the target environment (dev/tst/prd) when using external tables.
4. Add source row to config/source_registry.csv for the target product/entity.
5. Set table storage metadata per layer in source registry:
	- landing_table_type / conformance_table_type / silver_table_type: managed|external
	- landing_table_path / conformance_table_path / silver_table_path: required when table_type=external
6. For FILE sources, set source_path to the approved external location or volume path.
7. Validate source-specific requirements:
	- FILE: source_path required
	- JDBC: jdbc_table required
	- API: api_endpoint required
8. Add field mappings to config/column_mapping.csv.
9. Add quality checks to config/dq_rules.csv.
10. Add publish behavior to config/publish_rules.csv.
11. Run python scripts/validate_configs.py.
12. Activate source and run orchestrator.

## External Location Ticket Checklist
- Storage account(s): dev/tst/prd
- Container(s): per domain (for example connect, pace, pia)
- Read-only: false when write access is needed
- Grant model: platform team grants read/write/create external table to pipeline principals
- Confirm ABFSS URL mapping before onboarding

## Exception Hooks
Use these only when required:
- pre_landing_transform_notebook
- post_conformance_transform_notebook
- custom_publish_notebook
