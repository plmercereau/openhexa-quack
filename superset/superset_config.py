"""Custom Superset configuration for DuckDB integration."""

# Load duckdb_openhexa package (applies monkey-patch)
import duckdb_openhexa  # noqa: F401

# Allow DuckDB connections
PREVENT_UNSAFE_DB_CONNECTIONS = False

