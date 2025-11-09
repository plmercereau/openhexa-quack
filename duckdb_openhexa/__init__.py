"""
DuckDB OpenHexa - Custom dialect with per-user connection pooling.

This package provides a duckdb_oh:// dialect that is completely isolated from
standard duckdb:// connections, ensuring no interference between the two.
"""

import logging

logger = logging.getLogger(__name__)


# Register duckdb_oh:// dialect with SQLAlchemy
def _register_dialect():
    """Register duckdb_oh as a custom SQLAlchemy dialect using our custom Dialect class."""
    try:
        from sqlalchemy.dialects import registry
        from duckdb_openhexa.engine import DuckDBPlusEngineSpec  # noqa: F401
        
        # Register our custom dialect class (not the standard duckdb_engine.Dialect)
        # This ensures duckdb_oh:// uses DuckDBOpenHexaDialect while duckdb:// uses standard Dialect
        registry.register("duckdb_oh", "duckdb_openhexa.dialect", "DuckDBOpenHexaDialect")
        logger.info("✓ Registered isolated duckdb_oh:// dialect with per-user pooling and UDFs")
    except Exception as e:
        logger.error(f"Failed to register duckdb_oh dialect: {e}", exc_info=True)

_register_dialect()
