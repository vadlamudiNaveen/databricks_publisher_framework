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


def write_landing(df, table_name: str):
    if not table_name or not table_name.strip():
        raise ValueError("landing table_name must not be empty")
    (
        df.write.mode("append")
        .format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
