#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-dev}"
CLUSTER_ID="${2:-0408-062709-xu0nb836}"

# Raw data roots (override these as needed before running).
export RAW_CONNECT_ROOT="${RAW_CONNECT_ROOT:-abfss://rngpub@adlsdnapdevbronze.dfs.core.windows.net/eng511/raw_data/connect/}"
export RAW_ITEMSUMMARY_ROOT="${RAW_ITEMSUMMARY_ROOT:-abfss://rngpub@adlsdnapdevbronze.dfs.core.windows.net/eng511/raw_data/pia/itemsummarypublicprd/}"
export RAW_PIA_COMMONDIM_ITEM_ROOT="${RAW_PIA_COMMONDIM_ITEM_ROOT:-abfss://rngpub@adlsdnapdevbronze.dfs.core.windows.net/eng511/raw_data/pia/commondimensions/item}"
export RAW_PIA_COMMONDIM_VALUEBAG_ROOT="${RAW_PIA_COMMONDIM_VALUEBAG_ROOT:-abfss://rngpub@adlsdnapdevbronze.dfs.core.windows.net/eng511/raw_data/pia/commondimensions/valuebag}"
export RAW_PIA_COMMONDIM_BAS_ROOT="${RAW_PIA_COMMONDIM_BAS_ROOT:-abfss://rngpub@adlsdnapdevbronze.dfs.core.windows.net/eng511/raw_data/pia/commondimensions/bas}"

# POC tuning knobs
export POC_INCREMENTAL_START_TS="${POC_INCREMENTAL_START_TS:-2026-04-01T00:00:00Z}"

echo "[1/6] Generating notebook artifacts"
python3 scripts/generate_notebooks_ipynb.py

echo "[2/6] Configuring jobs for cluster $CLUSTER_ID"
python3 scripts/configure_jobs_cluster.py "$CLUSTER_ID" resources/jobs.yml

echo "[3/6] Deploying bundle"
databricks bundle deploy -t "$TARGET"

USER_NAME="$(databricks current-user me --output json | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')"
WORKSPACE_NOTEBOOK_ROOT="/Workspace/Users/${USER_NAME}/.bundle/databricks-ingestion-framework/${TARGET}/notebooks_ipynb"

echo "[4/6] Importing notebooks_ipynb to workspace notebook objects"
databricks workspace import-dir --overwrite notebooks_ipynb "$WORKSPACE_NOTEBOOK_ROOT"

echo "[5/6] Running initialization + setup"
databricks bundle run -t "$TARGET" framework_initialize_infrastructure_once
databricks bundle run -t "$TARGET" framework_setup_wizard_once

echo "[6/6] Starting orchestrator (async)"
databricks bundle run -t "$TARGET" framework_orchestrator_runtime --no-wait

echo "Done. Orchestrator started."
