"""Sample analysis notebook script for raw JSON/JSONL files in IKEA dropzones."""

from __future__ import annotations


def analyze_raw_files(spark, source_path: str, sample_limit: int = 1000):
    # For mixed JSON/JSONL, binaryFile keeps payload safe in raw bronze.
    df = spark.read.format("binaryFile").load(source_path)

    profile = (
        df.selectExpr(
            "count(*) as file_count",
            "min(modificationTime) as min_modification_time",
            "max(modificationTime) as max_modification_time",
            "sum(length(content)) as total_bytes",
        )
    )

    sample = df.select("path", "modificationTime", "length", "content").limit(sample_limit)
    return profile, sample
