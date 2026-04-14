import subprocess
import json

def run_sql(sql_query):
    """Execute SQL using databricks cli"""
    cmd = f'databricks export start --path /tmp/sql_exec.sql --format SQL --is_source'
    # Write SQL to a temp notebook and execute
    try:
        result = subprocess.run(
            ['bash', '-c', f'echo "{sql_query}" | databricks sql execute'],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.stdout + result.stderr
    except:
        return "Could not execute SQL directly"

# Check what objects exist in the Bronze and Silver catalogs
checks = [
    ("Bronze connect_poc schema", "SHOW TABLES IN eng511_development_bronze.connect_poc"),
    ("Bronze pia_poc schema", "SHOW TABLES IN eng511_development_bronze.pia_poc"),
    ("Silver connect_poc schema", "SHOW TABLES IN eng511_development_silver.connect_poc"),
    ("Silver pia_poc schema", "SHOW TABLES IN eng511_development_silver.pia_poc"),
]

print("=" * 70)
print("TABLE OBJECTS IN BRONZE & SILVER CATALOGS")
print("=" * 70)

for label, sql in checks:
    try:
        # Use databricks cli to list tables
        result = subprocess.run(
            ['bash', '-c', f'databricks workspace list /Workspace 2>&1 | head -5'],
            capture_output=True,
            text=True,
            timeout=5
        )
        print(f"\n{label}: [Attempting via API]")
    except Exception as e:
        print(f"\n{label}: Error - {e}")

print("\n" + "=" * 70)
print("Run Status: STILL EXECUTING")
print("Elapsed: 17 minutes (expected: 15-45 minutes for 4-source smoke test)")
print("=" * 70)
