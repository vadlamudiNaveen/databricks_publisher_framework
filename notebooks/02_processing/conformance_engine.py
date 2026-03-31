"""Conformance engine driven by mapping metadata."""

from __future__ import annotations

import warnings

# Technical columns added by the framework that should never leak into silver.
_FRAMEWORK_COLS = frozenset({
    "source_system", "source_entity", "load_id", "ingest_ts",
    "bronze_dq_status", "bronze_dq_failed_check",
    "dq_status", "dq_failed_rule",
})


def apply_column_mappings(df, mapping_rows: list[dict]):
    """Apply transform expressions from mapping metadata and return a conformance DataFrame.

    Each active mapping row must have:
    - ``transform_expression``: a valid Spark SQL expression (e.g. ``CAST(src_col AS STRING)``)
    - ``conformance_column``:   the output column name in the conformance/silver table

    If no valid mapping rows are present, the function returns the input DataFrame
    minus all known framework technical columns, with a warning.  This prevents
    bronze metadata columns from silently appearing in silver tables when mappings
    are accidentally empty.

    Raises ``ValueError`` if the DataFrame select fails due to a bad expression in
    the mapping metadata — this surfaces CSV configuration errors as clear messages
    rather than cryptic Spark AnalysisExceptions.
    """
    from pyspark.sql import functions as F

    expressions = []
    for row in mapping_rows:
        expr_text = row.get("transform_expression")
        output_col = row.get("conformance_column")
        if expr_text and output_col:
            expressions.append(F.expr(expr_text).alias(output_col))

    if not expressions:
        warnings.warn(
            "apply_column_mappings: no valid mapping rows found. "
            "Returning input DataFrame with framework technical columns removed. "
            "Verify that column_mapping.csv has active rows for this source entity.",
            stacklevel=2,
        )
        passthrough_cols = [c for c in df.columns if c not in _FRAMEWORK_COLS]
        return df.select(*passthrough_cols) if passthrough_cols else df

    try:
        return df.select(*expressions)
    except Exception as exc:
        raise ValueError(
            f"conformance_engine: DataFrame select failed — one or more transform_expression "
            f"values in column_mapping.csv are invalid. Original error: {exc}"
        ) from exc


def write_conformance(df, table_name: str):
    df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(table_name)
