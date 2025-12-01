"""Spark Thrift Server adapter for Datus Agent."""

from .config import SparkThriftConfig
from .connector import SparkThriftConnector

__version__ = "0.1.0"
__all__ = ["SparkThriftConnector", "SparkThriftConfig", "register"]


def register():
    """
    Register Spark Thrift connector with Datus registry.

    Call this function explicitly to register the connector with Datus.
    """
    try:
        from datus.tools.db_tools import connector_registry
        connector_registry.register("spark_thrift", SparkThriftConnector)
    except ImportError as e:
        import warnings
        warnings.warn(f"Failed to register spark_thrift connector: {e}")
    except Exception as e:
        import warnings
        warnings.warn(f"Error registering spark_thrift connector: {e}")


# Note: Auto-registration is disabled to avoid circular imports.
# Call register() explicitly after import if needed.