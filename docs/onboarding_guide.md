# Onboarding Guide

## Standard Onboarding Steps
1. Set environment values from config/.env.example.
2. Configure runtime settings and connection profiles in config/global_config.yaml.
3. Add source row to config/source_registry.csv for the target IKEA product/entity.
4. Add field mappings to config/column_mapping.csv.
5. Add quality checks to config/dq_rules.csv.
6. Add publish behavior to config/publish_rules.csv.
7. Run python scripts/validate_configs.py.
8. Activate source and run orchestrator.

## Exception Hooks
Use these only when required:
- pre_landing_transform_notebook
- post_conformance_transform_notebook
- custom_publish_notebook
