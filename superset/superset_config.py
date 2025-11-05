"""Custom Superset configuration for DuckDB integration."""

# Register DuckDB SQLAlchemy dialect for Superset
import duckdb_engine  # noqa: F401

# Load duckdb_openhexa package to make functions available
import duckdb_openhexa  # noqa: F401

# Allow DuckDB connections
PREVENT_UNSAFE_DB_CONNECTIONS = False

