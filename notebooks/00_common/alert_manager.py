"""Monitoring and alerting system for pipeline execution."""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"


@dataclass
class Alert:
    """Represents a pipeline alert."""
    alert_id: str
    severity: str  # AlertSeverity value
    alert_type: str
    title: str
    message: str
    product_name: str
    source_system: str
    source_entity: str
    load_id: str
    timestamp: str
    metadata: dict[str, Any]
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AlertManager:
    """
    Manages alerting for pipeline execution.
    
    Responsibilities:
    - Track pipeline events and metrics
    - Generate alerts based on thresholds
    - Send alerts to external systems
    - Maintain alert history
    """
    
    def __init__(self, spark=None):
        """
        Initialize AlertManager.
        
        Args:
            spark: Spark session for writing to audit tables (optional, for Databricks)
        """
        self.spark = spark
        self.alerts: list[Alert] = []
    
    def create_alert(
        self,
        alert_type: str,
        severity: AlertSeverity,
        title: str,
        message: str,
        product_name: str,
        source_system: str,
        source_entity: str,
        load_id: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Alert:
        """Create a new alert."""
        alert_id = f"{source_system}_{source_entity}_{datetime.now(timezone.utc).timestamp()}"
        alert = Alert(
            alert_id=alert_id,
            severity=severity.value,
            alert_type=alert_type,
            title=title,
            message=message,
            product_name=product_name,
            source_system=source_system,
            source_entity=source_entity,
            load_id=load_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            metadata=metadata or {},
        )
        self.alerts.append(alert)
        logger.log(
            logging.WARNING if severity == AlertSeverity.WARNING else logging.ERROR,
            f"[{severity.value}] {title}: {message}"
        )
        return alert
    
    def check_dq_failure_rate(
        self,
        product_name: str,
        source_system: str,
        source_entity: str,
        load_id: str,
        total_rows: int,
        failed_rows: int,
        threshold_percent: float = 10.0,
    ) -> Optional[Alert]:
        """Generate alert if DQ failure rate exceeds threshold."""
        if total_rows == 0:
            return None
        
        failure_rate = (failed_rows / total_rows) * 100
        if failure_rate > threshold_percent:
            severity = AlertSeverity.CRITICAL if failure_rate > 50 else AlertSeverity.WARNING
            return self.create_alert(
                alert_type="DQ_FAILURE_RATE",
                severity=severity,
                title=f"High DQ failure rate: {failure_rate:.1f}%",
                message=f"{failed_rows} of {total_rows} rows failed DQ checks",
                product_name=product_name,
                source_system=source_system,
                source_entity=source_entity,
                load_id=load_id,
                metadata={
                    "total_rows": total_rows,
                    "failed_rows": failed_rows,
                    "failure_rate": failure_rate,
                    "threshold": threshold_percent,
                },
            )
        return None
    
    def check_retry_exhaustion(
        self,
        product_name: str,
        source_system: str,
        source_entity: str,
        load_id: str,
        retry_count: int,
        max_retries: int,
        error_message: str,
    ) -> Optional[Alert]:
        """Generate critical alert if retries exhausted."""
        if retry_count >= max_retries:
            return self.create_alert(
                alert_type="RETRY_EXHAUSTION",
                severity=AlertSeverity.CRITICAL,
                title=f"Retries exhausted for {source_entity}",
                message=f"Failed after {max_retries} retries. Last error: {error_message[:100]}",
                product_name=product_name,
                source_system=source_system,
                source_entity=source_entity,
                load_id=load_id,
                metadata={
                    "retry_count": retry_count,
                    "max_retries": max_retries,
                    "error": error_message,
                },
            )
        return None
    
    def check_performance_degradation(
        self,
        product_name: str,
        source_system: str,
        source_entity: str,
        load_id: str,
        duration_seconds: float,
        baseline_seconds: float = 300.0,
    ) -> Optional[Alert]:
        """Generate warning if load duration exceeds baseline."""
        if duration_seconds > baseline_seconds * 2:  # 2x baseline
            return self.create_alert(
                alert_type="PERFORMANCE_DEGRADATION",
                severity=AlertSeverity.WARNING,
                title=f"Slow load: {duration_seconds:.1f}s (baseline: {baseline_seconds:.1f}s)",
                message=f"Load took {duration_seconds/baseline_seconds:.1f}x longer than expected",
                product_name=product_name,
                source_system=source_system,
                source_entity=source_entity,
                load_id=load_id,
                metadata={
                    "duration_seconds": duration_seconds,
                    "baseline_seconds": baseline_seconds,
                    "multiplier": duration_seconds / baseline_seconds,
                },
            )
        return None
    
    def write_alerts_to_table(
        self,
        catalog: str,
        schema: str,
        table_name: str,
    ) -> int:
        """
        Write all alerts to Databricks table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table_name: Table name
            
        Returns:
            Number of alerts written
        """
        if not self.spark or not self.alerts:
            return 0
        
        try:
            alert_dicts = [a.to_dict() for a in self.alerts]
            df = self.spark.createDataFrame(alert_dicts)
            table_fqn = f"{catalog}.{schema}.{table_name}"
            df.write.mode("append").insertInto(table_fqn)
            logger.info(f"Wrote {len(self.alerts)} alerts to {table_fqn}")
            return len(self.alerts)
        except Exception as e:
            logger.error(f"Failed to write alerts: {e}")
            return 0
    
    def get_alerts(
        self,
        severity: Optional[AlertSeverity] = None,
    ) -> list[Alert]:
        """Get all alerts, optionally filtered by severity."""
        if severity:
            return [a for a in self.alerts if a.severity == severity.value]
        return self.alerts
    
    def has_critical_alerts(self) -> bool:
        """Check if any critical alerts exist."""
        return any(a.severity == AlertSeverity.CRITICAL.value for a in self.alerts)
