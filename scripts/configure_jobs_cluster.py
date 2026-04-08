#!/usr/bin/env python3
"""
Auto-configure jobs to use existing cluster or create ephemeral job clusters.

Checks if the provided cluster exists and is running. If yes, uses it.
If no or not provided, falls back to creating fresh job clusters.
"""

import json
import subprocess
import sys
from pathlib import Path


def get_cluster_status(cluster_id: str) -> dict | None:
    """Get cluster status from Databricks API via CLI."""
    try:
        result = subprocess.run(
            ["databricks", "clusters", "get", "--cluster-id", cluster_id, "--output", "json"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        
        # Fallback: parse cluster list if get fails
        result = subprocess.run(
            ["databricks", "clusters", "list", "--output", "json"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            clusters = json.loads(result.stdout)
            # Response is a list, not a dict
            if isinstance(clusters, list):
                for cluster in clusters:
                    if cluster.get("cluster_id") == cluster_id:
                        return cluster
        return None
    except Exception as e:
        print(f"⚠ Failed to check cluster status: {e}", file=sys.stderr)
        return None


def should_use_existing_cluster(cluster_id: str | None) -> bool:
    """Check if we should use existing cluster (exists and is running/idle)."""
    if not cluster_id or not cluster_id.strip():
        return False

    cluster_info = get_cluster_status(cluster_id)
    if not cluster_info:
        print(f"⚠ Cluster {cluster_id} not found—will use job clusters", file=sys.stderr)
        return False

    state = cluster_info.get("state", "")
    # Running, Idle, or Pending are all valid for job execution
    if state in ("RUNNING", "IDLE", "PENDING"):
        print(f"✓ Using existing cluster {cluster_id} (state: {state})")
        return True
    else:
        print(
            f"⚠ Cluster {cluster_id} is {state}—will create job clusters instead",
            file=sys.stderr,
        )
        return False


def generate_jobs_yaml(use_existing: bool, existing_cluster_id: str | None) -> str:
    """Generate jobs.yml with appropriate cluster config."""
    if use_existing and existing_cluster_id:
        return _generate_with_existing_cluster(existing_cluster_id)
    else:
        return _generate_with_job_clusters()


def _generate_with_existing_cluster(cluster_id: str) -> str:
    """Generate jobs config using existing cluster (no ephemeral clusters)."""
    return f"""resources:
  jobs:
    framework_initialize_infrastructure_once:
      name: framework_initialize_infrastructure_once_${{bundle.target}}
      max_concurrent_runs: 1
      tasks:
        - task_key: initialize_framework
          existing_cluster_id: "{cluster_id}"
          notebook_task:
            notebook_path: ${{workspace.root_path}}/notebooks_ipynb/05_orchestration/initialize_framework
            base_parameters:
              global-config: ${{var.global_config_path}}

    framework_setup_wizard_once:
      name: framework_setup_wizard_once_${{bundle.target}}
      max_concurrent_runs: 1
      tasks:
        - task_key: setup_wizard
          existing_cluster_id: "{cluster_id}"
          notebook_task:
            notebook_path: ${{workspace.root_path}}/notebooks_ipynb/05_orchestration/setup_wizard
            base_parameters:
              global-config: ${{var.global_config_path}}

    framework_orchestrator_runtime:
      name: framework_orchestrator_runtime_${{bundle.target}}
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: "0 0/30 * * * ?"
        timezone_id: UTC
        pause_status: PAUSED
      tasks:
        - task_key: framework_orchestrator_execute
          existing_cluster_id: "{cluster_id}"
          notebook_task:
            notebook_path: ${{workspace.root_path}}/notebooks_ipynb/05_orchestration/framework_orchestrator
            base_parameters:
              execute: "true"
              config-dir: ${{var.config_dir_path}}
              global-config: ${{var.global_config_path}}
              parallel: "4"
              pk-check-summary: "true"
"""


def _generate_with_job_clusters() -> str:
    """Generate jobs config using ephemeral job clusters (fresh start + cleanup)."""
    return """resources:
  jobs:
    framework_initialize_infrastructure_once:
      name: framework_initialize_infrastructure_once_${bundle.target}
      max_concurrent_runs: 1
      job_clusters:
        - job_cluster_key: framework_setup_cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            spark_env_vars:
              ENVIRONMENT: ${bundle.target}
              UC_CATALOG: ${var.uc_catalog}
              UC_BRONZE_SCHEMA: ${var.uc_bronze_schema}
              UC_SILVER_SCHEMA: ${var.uc_silver_schema}
              UC_AUDIT_SCHEMA: ${var.uc_audit_schema}
              UC_CONTROL_SCHEMA: ${var.uc_control_schema}
            autoscale:
              min_workers: ${var.min_workers}
              max_workers: ${var.max_workers}
      tasks:
        - task_key: initialize_framework
          job_cluster_key: framework_setup_cluster
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks_ipynb/05_orchestration/initialize_framework
            base_parameters:
              global-config: ${var.global_config_path}

    framework_setup_wizard_once:
      name: framework_setup_wizard_once_${bundle.target}
      max_concurrent_runs: 1
      job_clusters:
        - job_cluster_key: framework_setup_cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            spark_env_vars:
              ENVIRONMENT: ${bundle.target}
              UC_CATALOG: ${var.uc_catalog}
              UC_BRONZE_SCHEMA: ${var.uc_bronze_schema}
              UC_SILVER_SCHEMA: ${var.uc_silver_schema}
              UC_AUDIT_SCHEMA: ${var.uc_audit_schema}
              UC_CONTROL_SCHEMA: ${var.uc_control_schema}
            autoscale:
              min_workers: ${var.min_workers}
              max_workers: ${var.max_workers}
      tasks:
        - task_key: setup_wizard
          job_cluster_key: framework_setup_cluster
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks_ipynb/05_orchestration/setup_wizard
            base_parameters:
              global-config: ${var.global_config_path}

    framework_orchestrator_runtime:
      name: framework_orchestrator_runtime_${bundle.target}
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: "0 0/30 * * * ?"
        timezone_id: UTC
        pause_status: PAUSED
      job_clusters:
        - job_cluster_key: framework_runtime_cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            spark_env_vars:
              ENVIRONMENT: ${bundle.target}
              UC_CATALOG: ${var.uc_catalog}
              UC_BRONZE_SCHEMA: ${var.uc_bronze_schema}
              UC_SILVER_SCHEMA: ${var.uc_silver_schema}
              UC_AUDIT_SCHEMA: ${var.uc_audit_schema}
              UC_CONTROL_SCHEMA: ${var.uc_control_schema}
            autoscale:
              min_workers: ${var.min_workers}
              max_workers: ${var.max_workers}
      tasks:
        - task_key: framework_orchestrator_execute
          job_cluster_key: framework_runtime_cluster
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks_ipynb/05_orchestration/framework_orchestrator
            base_parameters:
              execute: "true"
              config-dir: ${var.config_dir_path}
              global-config: ${var.global_config_path}
              parallel: "4"
              pk-check-summary: "true"
"""


def main():
    """Auto-detect cluster and generate appropriate jobs config."""
    cluster_id = sys.argv[1] if len(sys.argv) > 1 else None
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("resources/jobs.yml")

    use_existing = should_use_existing_cluster(cluster_id)
    jobs_yaml = generate_jobs_yaml(use_existing, cluster_id if use_existing else None)

    output_path.write_text(jobs_yaml)
    print(f"✓ Generated {output_path}")

    if use_existing:
        print(f"  Mode: Existing cluster {cluster_id} (fast, always charges)")
    else:
        print("  Mode: Ephemeral job clusters (slower startup, charges only during run)")


if __name__ == "__main__":
    main()
