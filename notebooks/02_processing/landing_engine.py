"""Landing engine: preserve source records and add technical metadata."""

from __future__ import annotations


def add_landing_metadata(df, source_system: str, source_entity: str, load_id: str):
    from pyspark.sql import functions as F

    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("source_entity", F.lit(source_entity))
        .withColumn("load_id", F.lit(load_id))
        .withColumn("ingest_ts", F.current_timestamp())
    )


from typing import Optional
def write_landing(
    df,
    table_name: str,
    archive_table: Optional[str] = None,
    retention_days: Optional[int] = None
):
    """
    Write DataFrame to landing table and optionally to an archive table with retention policy.
    Args:
        df: DataFrame to write
        table_name: Main landing table name
        archive_table: Optional archive table name
        retention_days: Optional retention period in days for archive
    """
    if not table_name or not table_name.strip():
        raise ValueError("landing table_name must not be empty")
    (
        df.write.mode("append")
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
    if archive_table and archive_table.strip():
        (
            df.write.mode("append")
            .format("delta")
            .option("mergeSchema", "true")
            .saveAsTable(archive_table)
        )
        # Optionally enforce retention by vacuuming old data
        if retention_days is not None:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                spark.sql(f"VACUUM {archive_table} RETAIN {retention_days} HOURS")
