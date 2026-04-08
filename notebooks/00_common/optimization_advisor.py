"""Query optimization and performance monitoring utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class OptimizationRecommendation:
    """Represents a query optimization recommendation."""
    recommendation_type: str
    description: str
    expected_improvement: str
    implementation: str
    priority: str  # HIGH, MEDIUM, LOW


class OptimizationAdvisor:
    """
    Analyzes table metadata and provides optimization recommendations.
    
    Recommendations include:
    - Partitioning strategy
    - Z-order clustering
    - Data statistics
    - Query patterns
    """
    
    @staticmethod
    def recommend_partitioning(
        table_name: str,
        column_cardinalities: dict[str, int],
        total_rows: int,
    ) -> Optional[OptimizationRecommendation]:
        """
        Recommend partitioning strategy.
        
        Args:
            table_name: Table name
            column_cardinalities: Dict of column -> cardinality
            total_rows: Total row count
            
        Returns:
            Optimization recommendation or None
        """
        # Find low-cardinality columns suitable for partitioning
        low_card_cols = [
            col for col, card in column_cardinalities.items()
            if 2 <= card <= 1000  # Ideal partition cardinality range
        ]
        
        if not low_card_cols:
            return None
        
        primary_partition = low_card_cols[0]
        return OptimizationRecommendation(
            recommendation_type="PARTITIONING",
            description=f"Add partitioning by {primary_partition}",
            expected_improvement="2-5x faster queries on partitioned column",
            implementation=f"""
ALTER TABLE {table_name}
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE {table_name}_partitioned
USING DELTA
PARTITIONED BY ({primary_partition})
AS SELECT * FROM {table_name};

-- Then swap tables
ALTER TABLE {table_name} RENAME TO {table_name}_old;
ALTER TABLE {table_name}_partitioned RENAME TO {table_name};
""",
            priority="HIGH",
        )
    
    @staticmethod
    def recommend_zorder(
        table_name: str,
        high_cardinality_columns: list[str],
    ) -> Optional[OptimizationRecommendation]:
        """
        Recommend Z-order optimization.
        
        Args:
            table_name: Table name
            high_cardinality_columns: List of high-cardinality columns
            
        Returns:
            Optimization recommendation or None
        """
        if not high_cardinality_columns:
            return None
        
        zorder_cols = ",".join(high_cardinality_columns[:3])  # Z-order up to 3 columns
        return OptimizationRecommendation(
            recommendation_type="Z_ORDER",
            description=f"Apply Z-order clustering on {zorder_cols}",
            expected_improvement="2-3x faster on filter queries",
            implementation=f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})",
            priority="MEDIUM",
        )
    
    @staticmethod
    def recommend_statistics(
        table_name: str,
    ) -> OptimizationRecommendation:
        """Recommend collecting table statistics."""
        return OptimizationRecommendation(
            recommendation_type="STATISTICS",
            description="Collect and update table statistics",
            expected_improvement="Better query planner decisions",
            implementation=f"ANALYZE TABLE {table_name} COMPUTE STATISTICS",
            priority="MEDIUM",
        )


class PerformanceMonitor:
    """
    Monitors query execution times and identifies performance issues.
    """
    
    def __init__(self, spark=None):
        """Initialize PerformanceMonitor."""
        self.spark = spark
        self.metrics: list[dict] = []
    
    def record_execution(
        self,
        query_name: str,
        duration_seconds: float,
        rows_processed: int,
        bytes_processed: int,
    ) -> None:
        """Record query execution metrics."""
        throughput_mbps = (bytes_processed / (1024 ** 2)) / max(duration_seconds, 0.001)
        self.metrics.append({
            "query_name": query_name,
            "duration_seconds": duration_seconds,
            "rows_processed": rows_processed,
            "bytes_processed": bytes_processed,
            "throughput_mbps": throughput_mbps,
        })
        
        if duration_seconds > 60:  # Log slow queries
            logger.warning(f"Slow query detected: {query_name} took {duration_seconds:.1f}s")
    
    def get_slow_queries(self, threshold_seconds: float = 30.0) -> list[dict]:
        """Get queries that exceeded duration threshold."""
        return [
            m for m in self.metrics
            if m["duration_seconds"] > threshold_seconds
        ]
    
    def write_metrics_to_table(
        self,
        catalog: str,
        schema: str,
        table_name: str,
    ) -> int:
        """Write performance metrics to Databricks table."""
        if not self.spark or not self.metrics:
            return 0
        
        try:
            df = self.spark.createDataFrame(self.metrics)
            table_fqn = f"{catalog}.{schema}.{table_name}"
            df.write.mode("append").insertInto(table_fqn)
            logger.info(f"Wrote {len(self.metrics)} performance metrics to {table_fqn}")
            return len(self.metrics)
        except Exception as e:
            logger.error(f"Failed to write metrics: {e}")
            return 0
