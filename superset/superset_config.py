"""Custom Superset configuration for DuckDB integration."""

import sys

# Load from volume mount instead of site-packages
if '/app' not in sys.path:
    sys.path.insert(0, '/app')

# Import our custom connector - this does everything:
# - Registers duckdb_oh:// dialect with SQLAlchemy
# - Monkey-patches duckdb.connect() for per-user connection pooling
# - Auto-registers UDFs (get_dataset_file_url, openhexa_dataset_files)
# - Enables HTTP request caching
import duckdb_openhexa  # noqa: F401

# Allow DuckDB connections
PREVENT_UNSAFE_DB_CONNECTIONS = False
