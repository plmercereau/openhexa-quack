"""Custom Superset configuration for DuckDB integration."""

# Patch duckdb-engine BEFORE importing it to use connection pool
# This ensures HTTP cache persistence across SQLAlchemy sessions
try:
    from duckdb_openhexa.connection_pool import patch_duckdb_engine_dialect
    patch_duckdb_engine_dialect()
except Exception:
    # If patching fails, continue without it
    pass

# Register DuckDB SQLAlchemy dialect for Superset
import duckdb_engine  # noqa: F401

# Load duckdb_openhexa package to make functions available
import duckdb_openhexa  # noqa: F401

# Allow DuckDB connections
PREVENT_UNSAFE_DB_CONNECTIONS = False

