"""Plugin architecture for extensible ingestion adapters."""

from __future__ import annotations

import importlib
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

logger = logging.getLogger(__name__)


class IngestAdapter(ABC):
    """
    Abstract base class for all ingestion adapters.
    
    Subclasses must implement the ingest() method.
    """
    
    def __init__(self, source: dict[str, Any], source_options: Optional[dict] = None):
        """
        Initialize adapter.
        
        Args:
            source: Source configuration from metadata
            source_options: Source-specific options
        """
        self.source = source
        self.source_options = source_options or {}
    
    @abstractmethod
    def ingest(self, spark=None) -> Any:
        """
        Ingest data from source.
        
        Args:
            spark: Spark session (optional)
            
        Returns:
            PySpark DataFrame or iterator of records
        """
        pass
    
    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate adapter configuration.
        
        Returns:
            (is_valid, error_messages)
        """
        return True, []


class AdapterRegistry:
    """
    Central registry for managing ingestion adapters.
    
    Allows dynamic discovery and instantiation of adapters.
    """
    
    _adapters: dict[str, type[IngestAdapter]] = {}
    
    @classmethod
    def register(cls, source_type: str, adapter_class: type[IngestAdapter]) -> None:
        """Register an adapter."""
        cls._adapters[source_type.upper()] = adapter_class
        logger.info(f"Registered adapter: {source_type}")
    
    @classmethod
    def get_adapter(cls, source_type: str) -> Optional[type[IngestAdapter]]:
        """Get an adapter by source type."""
        return cls._adapters.get(source_type.upper())
    
    @classmethod
    def list_adapters(cls) -> list[str]:
        """List all registered adapters."""
        return list(cls._adapters.keys())
    
    @classmethod
    def get_or_create_adapter(
        cls,
        source_type: str,
        source: dict,
        source_options: Optional[dict] = None,
    ) -> Optional[IngestAdapter]:
        """Get or instantiate an adapter."""
        adapter_class = cls.get_adapter(source_type)
        if not adapter_class:
            logger.warning(f"No adapter found for source_type: {source_type}")
            return None
        return adapter_class(source=source, source_options=source_options)


def auto_discover_adapters(adapter_module: str = "notebooks.01_ingestion.adapters") -> None:
    """
    Auto-discover and register adapters from a module.
    
    Args:
        adapter_module: Module path to search for adapters
    """
    try:
        module = importlib.import_module(adapter_module)
        for item_name in dir(module):
            item =getattr(module, item_name)
            if (
                isinstance(item, type)
                and issubclass(item, IngestAdapter)
                and item is not IngestAdapter
                and hasattr(item, "SOURCE_TYPE")
            ):
                source_type = getattr(item, "SOURCE_TYPE")
                AdapterRegistry.register(source_type, item)
                logger.info(f"Auto-discovered adapter: {source_type}")
    except Exception as e:
        logger.warning(f"Failed to auto-discover adapters: {e}")
