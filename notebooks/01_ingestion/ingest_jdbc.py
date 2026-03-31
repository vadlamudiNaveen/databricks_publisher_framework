"""Generic JDBC ingestion adapter."""

from __future__ import annotations

import os


def build_jdbc_options(jdbc_profile: dict, source: dict) -> dict:
    """Validate and build the JDBC option dict for a Spark JDBC reader.

    Credentials are read from environment variables (never hardcoded) so they
    do not appear in source control or config files.
    """
    required_profile_keys = ["url", "driver", "fetchsize"]
    missing_profile = [k for k in required_profile_keys if not jdbc_profile.get(k)]
    if missing_profile:
        raise ValueError(f"Missing JDBC profile fields: {missing_profile}")
    if not source.get("jdbc_table"):
        raise ValueError("jdbc_table is required in source metadata")

    options: dict = {
        "url": jdbc_profile["url"],
        "dbtable": source["jdbc_table"],
        "driver": jdbc_profile["driver"],
        "fetchsize": str(jdbc_profile["fetchsize"]),
    }

    user_env = jdbc_profile.get("user_env", "")
    password_env = jdbc_profile.get("password_env", "")
    if user_env:
        options["user"] = os.getenv(user_env, "")
    if password_env:
        options["password"] = os.getenv(password_env, "")

    # Strip blanks so Spark doesn't receive empty-string options.
    return {k: v for k, v in options.items() if v not in {"", None}}


def _apply_parallel_read_options(options: dict, extra_options: dict) -> dict:
    """Merge parallel read options when ``numPartitions`` is configured.

    For large tables, configure these in ``source_options_json``:

    .. code-block:: json

        {
          "numPartitions": 8,
          "partitionColumn": "id",
          "lowerBound": 1,
          "upperBound": 10000000
        }

    All four must be present together; missing any one will skip partitioning
    and fall back to single-threaded read rather than raising an error, so
    partial configs don't silently break things.
    """
    partition_keys = {"numPartitions", "partitionColumn", "lowerBound", "upperBound"}
    present = partition_keys.intersection(extra_options)
    if present == partition_keys:
        for k in partition_keys:
            options[k] = str(extra_options[k])
    return options


def ingest_jdbc_batch(spark, jdbc_profile: dict, source: dict, extra_options: dict | None = None):
    """Read a JDBC table or query into a Spark DataFrame.

    Parameters
    ----------
    spark:
        Active SparkSession.
    jdbc_profile:
        Connection profile from ``global_config.yaml`` (``connections.jdbc_profiles``).
    source:
        Source registry row.  Must contain ``jdbc_table``.
    extra_options:
        Parsed ``source_options_json`` dict.  Supports all Spark JDBC reader
        options.  Parallel read keys (``numPartitions``, ``partitionColumn``,
        ``lowerBound``, ``upperBound``) are handled automatically when all four
        are present.

    Returns
    -------
    pyspark.sql.DataFrame
    """
    options = build_jdbc_options(jdbc_profile, source)

    # Merge parallel-read partitioning options.
    merged_extra = dict(extra_options or {})
    options = _apply_parallel_read_options(options, merged_extra)

    # Forward any remaining extra options (e.g. queryTimeout, sessionInitStatement).
    partition_keys = {"numPartitions", "partitionColumn", "lowerBound", "upperBound"}
    for k, v in merged_extra.items():
        if k not in partition_keys and isinstance(v, (str, int, float, bool)):
            options[k] = str(v)

    reader = spark.read.format("jdbc")
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load()
