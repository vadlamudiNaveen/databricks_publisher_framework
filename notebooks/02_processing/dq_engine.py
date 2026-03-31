"""Data quality engine driven by rule metadata."""

from __future__ import annotations


def apply_dq_rules(df, dq_rows: list[dict]):
    """Apply all DQ rules and annotate each row with status and ALL failing rules.

    A row gets ``dq_status = 'FAIL'`` if it violates *any* rule.  All violated
    rule names are accumulated in ``dq_failed_rule`` as a comma-separated list
    so analysts can see every failure, not just the first one discovered.
    """
    from pyspark.sql import functions as F

    result = df.withColumn("dq_status", F.lit("PASS")).withColumn("dq_failed_rule", F.lit(None).cast("string"))

    for row in dq_rows:
        rule_expr = row.get("rule_expression")
        rule_name = row.get("rule_name", "UNKNOWN")
        if not rule_expr:
            continue

        failed_condition = ~F.expr(rule_expr)
        result = result.withColumn(
            "dq_status",
            F.when(failed_condition, F.lit("FAIL")).otherwise(F.col("dq_status")),
        ).withColumn(
            # Accumulate ALL failing rule names, not just the first.
            "dq_failed_rule",
            F.when(
                failed_condition,
                F.when(F.col("dq_failed_rule").isNull(), F.lit(rule_name)).otherwise(
                    F.concat(F.col("dq_failed_rule"), F.lit(","), F.lit(rule_name))
                ),
            ).otherwise(F.col("dq_failed_rule")),
        )

    return result


def split_valid_reject(df):
    valid_df = df.filter("dq_status = 'PASS'")
    reject_df = df.filter("dq_status = 'FAIL'")
    return valid_df, reject_df
