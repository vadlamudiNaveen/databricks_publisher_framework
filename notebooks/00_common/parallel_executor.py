"""Parallel execution engine for orchestrating multiple sources concurrently."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional, Any

logger = logging.getLogger(__name__)


class ParallelExecutor:
    """
    Manages parallel execution of independent tasks with dependency tracking.
    
    Features:
    - Configurable worker pool size
    - Graceful error handling
    - Progress tracking
    - Timeout support
    """
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize ParallelExecutor.
        
        Args:
            max_workers: Maximum number of concurrent workers
        """
        self.max_workers = max(1, min(max_workers, 16))  # Clamp between 1-16
        self.results: dict[str, Any] = {}
        self.errors: dict[str, Exception] = {}
    
    def execute_tasks(
        self,
        tasks: dict[str, tuple[Callable, tuple, dict]],
        timeout_seconds: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Execute multiple tasks in parallel.
        
        Args:
            tasks: Dict of task_id -> (callable, args_tuple, kwargs_dict)
            timeout_seconds: Timeout per task
            
        Returns:
            Dict of task_id -> result
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(func, *args, **kwargs): task_id
                for task_id, (func, args, kwargs) in tasks.items()
            }
            
            # Process completed tasks as they finish
            completed = 0
            for future in as_completed(future_to_task, timeout=timeout_seconds):
                task_id = future_to_task[future]
                try:
                    result = future.result(timeout=timeout_seconds)
                    self.results[task_id] = result
                    completed += 1
                    logger.info(f"Task {task_id} completed ({completed}/{len(tasks)})")
                except Exception as e:
                    self.errors[task_id] = e
                    logger.error(f"Task {task_id} failed: {e}")
        
        return self.results
    
    def get_results(self) -> tuple[dict[str, Any], dict[str, Exception]]:
        """Get all results and errors."""
        return self.results, self.errors
    
    def has_errors(self) -> bool:
        """Check if any errors occurred."""
        return len(self.errors) > 0
