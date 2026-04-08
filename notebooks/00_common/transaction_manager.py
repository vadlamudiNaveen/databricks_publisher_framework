"""Transaction management and error recovery for the ingestion framework."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TransactionState(Enum):
    """Enum for transaction state tracking."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    CHECKPOINT_SAVED = "CHECKPOINT_SAVED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


@dataclass
class StageCheckpoint:
    """Represents a checkpoint at a specific pipeline stage."""
    stage_name: str
    stage_sequence: int
    state: str  # TransactionState value
    timestamp: str
    details: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TransactionRecord:
    """Complete transaction record for a pipeline run."""
    transaction_id: str
    load_id: str
    product_name: str
    source_system: str
    source_entity: str
    started_at: str
    ended_at: Optional[str] = None
    overall_state: str = TransactionState.PENDING.value
    checkpoints: list[StageCheckpoint] = field(default_factory=list)
    error_message: Optional[str] = None
    recovery_action: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        checkpoint_dicts = [c.to_dict() for c in self.checkpoints]
        return {
            "transaction_id": self.transaction_id,
            "load_id": self.load_id,
            "product_name": self.product_name,
            "source_system": self.source_system,
            "source_entity": self.source_entity,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "overall_state": self.overall_state,
            "checkpoints": checkpoint_dicts,
            "error_message": self.error_message,
            "recovery_action": self.recovery_action,
            "metadata": self.metadata,
        }


class TransactionManager:
    """
    Manages transaction lifecycle and checkpoints for error recovery.
    
    Responsibilities:
    - Track pipeline stages and state transitions
    - Create/restore checkpoints
    - Handle rollback scenarios
    - Provide resume capability
    """
    
    def __init__(self, checkpoint_root: str, transaction_id: str):
        """
        Initialize TransactionManager.
        
        Args:
            checkpoint_root: Root directory for storing checkpoints
            transaction_id: Unique ID for this transaction
        """
        self.checkpoint_root = Path(checkpoint_root)
        self.checkpoint_root.mkdir(parents=True, exist_ok=True)
        self.transaction_id = transaction_id
        self.checkpoint_dir = self.checkpoint_root / transaction_id
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.transaction: Optional[TransactionRecord] = None
        self.stage_checkpoints: dict[str, StageCheckpoint] = {}
        
    def start_transaction(
        self,
        load_id: str,
        product_name: str,
        source_system: str,
        source_entity: str,
    ) -> TransactionRecord:
        """Start a new transaction."""
        now = datetime.now(timezone.utc).isoformat()
        self.transaction = TransactionRecord(
            transaction_id=self.transaction_id,
            load_id=load_id,
            product_name=product_name,
            source_system=source_system,
            source_entity=source_entity,
            started_at=now,
            overall_state=TransactionState.IN_PROGRESS.value,
        )
        self._save_transaction()
        logger.info(f"Started transaction {self.transaction_id}")
        return self.transaction
    
    def save_checkpoint(
        self,
        stage_name: str,
        stage_sequence: int,
        details: Optional[dict[str, Any]] = None,
    ) -> StageCheckpoint:
        """Save a checkpoint at a specific stage."""
        now = datetime.now(timezone.utc).isoformat()
        checkpoint = StageCheckpoint(
            stage_name=stage_name,
            stage_sequence=stage_sequence,
            state=TransactionState.CHECKPOINT_SAVED.value,
            timestamp=now,
            details=details or {},
        )
        self.stage_checkpoints[stage_name] = checkpoint
        if self.transaction:
            self.transaction.checkpoints.append(checkpoint)
            self._save_transaction()
        
        # Save checkpoint to disk for recovery
        checkpoint_file = self.checkpoint_dir / f"{stage_name}.json"
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)
        
        logger.info(f"Saved checkpoint: {stage_name} (seq={stage_sequence})")
        return checkpoint
    
    def get_last_successful_checkpoint(self) -> Optional[StageCheckpoint]:
        """Get the most recent successful checkpoint."""
        if not self.transaction or not self.transaction.checkpoints:
            return None
        # Return the last checkpoint
        return self.transaction.checkpoints[-1]
    
    def mark_stage_success(self, stage_name: str) -> None:
        """Mark a stage as successfully completed."""
        if stage_name in self.stage_checkpoints:
            self.stage_checkpoints[stage_name].state = TransactionState.SUCCESS.value
            self._save_transaction()
            logger.info(f"Marked stage {stage_name} as SUCCESS")
    
    def mark_stage_failed(
        self,
        stage_name: str,
        error_message: str,
        recovery_action: Optional[str] = None,
    ) -> None:
        """Mark a stage as failed with error details."""
        if self.transaction:
            self.transaction.overall_state = TransactionState.FAILED.value
            self.transaction.error_message = error_message
            self.transaction.recovery_action = recovery_action or "MANUAL_INTERVENTION_REQUIRED"
            self._save_transaction()
        
        if stage_name in self.stage_checkpoints:
            self.stage_checkpoints[stage_name].state = TransactionState.FAILED.value
        
        logger.error(f"Stage {stage_name} failed: {error_message}")
    
    def mark_transaction_success(self) -> None:
        """Mark entire transaction as successful."""
        if self.transaction:
            now = datetime.now(timezone.utc).isoformat()
            self.transaction.overall_state = TransactionState.SUCCESS.value
            self.transaction.ended_at = now
            self._save_transaction()
            logger.info(f"Transaction {self.transaction_id} completed successfully")
    
    def mark_transaction_rolled_back(self) -> None:
        """Mark transaction as rolled back."""
        if self.transaction:
            now = datetime.now(timezone.utc).isoformat()
            self.transaction.overall_state = TransactionState.ROLLED_BACK.value
            self.transaction.ended_at = now
            self._save_transaction()
            logger.warning(f"Transaction {self.transaction_id} rolled back")
    
    def _save_transaction(self) -> None:
        """Persist transaction state to disk."""
        if not self.transaction:
            return
        
        tx_file = self.checkpoint_dir / "transaction.json"
        with open(tx_file, "w") as f:
            json.dump(self.transaction.to_dict(), f, indent=2)
    
    def get_transaction_state(self) -> Optional[TransactionRecord]:
        """Get the current transaction state."""
        return self.transaction
    
    def cleanup_old_checkpoints(self, keep_count: int = 5) -> None:
        """Clean up old checkpoint directories (keep recent N)."""
        checkpoint_files = sorted(self.checkpoint_root.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
        for old_dir in checkpoint_files[keep_count:]:
            if old_dir.is_dir():
                import shutil
                shutil.rmtree(old_dir)
                logger.info(f"Cleaned up old checkpoint: {old_dir}")
