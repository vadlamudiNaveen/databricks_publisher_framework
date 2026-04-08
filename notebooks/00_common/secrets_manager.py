"""Secrets management integration with Databricks Secrets API."""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class SecretsBackend(ABC):
    """Abstract base class for secrets backends."""
    
    @abstractmethod
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieve a secret."""
        pass


class EnvironmentSecretsBackend(SecretsBackend):
    """Secrets from environment variables."""
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from environment variable."""
        return os.getenv(key, default)


class DatabricksSecretsBackend(SecretsBackend):
    """
    Secrets from Databricks Secrets API.
    
    Requires spark context and Databricks Secrets Scope configured.
    """
    
    def __init__(self, scope: str = "default", spark=None):
        """
        Initialize Databricks Secrets Backend.
        
        Args:
            scope: Secret scope name (default: "default")
            spark: Spark session for API access
        """
        self.scope = scope
        self.spark = spark
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from Databricks Secrets API."""
        if not self.spark:
            logger.warning("Spark session not available, falling back to environment variable")
            return os.getenv(key, default)
        
        try:
            # Use Databricks Secrets API via dbutils
            dbutils = self.spark.sparkContext._jvm.com.databricks.python.PythonUtils.getDBUtils(self.spark.sparkContext)
            secret = dbutils.secrets.get(scope=self.scope, key=key)
            logger.debug(f"Retrieved secret from Databricks Secrets: {self.scope}.{key}")
            return secret
        except Exception as e:
            logger.warning(f"Failed to retrieve secret from Databricks: {e}, using default")
            return default


class SecretsManager:
    """
    Central secrets manager with pluggable backends.
    
    Supports environment variables, Databricks Secrets, and custom backends.
    """
    
    def __init__(self, backend: Optional[SecretsBackend] = None):
        """
        Initialize SecretsManager.
        
        Args:
            backend: Secrets backend (default: environment variables)
        """
        self.backend = backend or EnvironmentSecretsBackend()
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Retrieve a secret.
        
        Args:
            key: Secret key/name
            default: Default value if secret not found
            
        Returns:
            Secret value or default
        """
        secret = self.backend.get_secret(key, default)
        if not secret and not default:
            logger.warning(f"Secret not found: {key}")
        return secret
    
    def get_required_secret(self, key: str) -> str:
        """
        Retrieve a required secret.
        
        Raises:
            ValueError: If secret not found
        """
        secret = self.get_secret(key)
        if not secret:
            raise ValueError(f"Required secret not found: {key}")
        return secret


def configure_secrets_from_global_config(
    global_config: dict,
    spark=None,
) -> SecretsManager:
    """
    Configure SecretsManager from global_config.
    
    Args:
        global_config: Global configuration dict
        spark: Spark session (required for Databricks backend)
        
    Returns:
        Configured SecretsManager
    """
    secrets_config = global_config.get("secrets", {})
    backend_type = secrets_config.get("backend", "env_vars").lower()
    
    if backend_type == "databricks_secrets":
        scope = secrets_config.get("databricks_scope", "default")
        backend = DatabricksSecretsBackend(scope=scope, spark=spark)
        logger.info(f"Using Databricks Secrets backend (scope={scope})")
    else:
        backend = EnvironmentSecretsBackend()
        logger.info("Using environment variables backend")
    
    return SecretsManager(backend=backend)
