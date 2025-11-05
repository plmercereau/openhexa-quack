"""DuckDB Extension - provides GraphQL querying as table/scalar functions."""

import logging
import re

import duckdb

from duckdb_openhexa.functions import openhexa_dataset_files, get_dataset_file_url

__all__ = [
    "openhexa_dataset_files",
    "get_dataset_file_url",
    "register_functions",
]
__version__ = "0.1.0"

logger = logging.getLogger(__name__)


def register_functions(conn: duckdb.DuckDBPyConnection) -> None:
    """Register custom function on a DuckDB connection."""
    try:
        conn.create_function("get_dataset_file_url", get_dataset_file_url)
        logger.info("✓ Registered get_dataset_file_url")
    except Exception as e:
        logger.warning(f"Could not register functions: {e}")


# Lightweight query interception for table functions
_original_execute = duckdb.DuckDBPyConnection.execute
_function_pattern = re.compile(r"openhexa_dataset_files\(([^)]*)\)", re.IGNORECASE)


def _patched_execute(self, query, *args, **kwargs):
    """Intercept queries containing openhexa_dataset_files() and replace with temp view."""
    # Register scalar function if needed
    try:
        self.create_function("get_dataset_file_url", get_dataset_file_url)
    except:
        pass
    
    # Check if query contains our table function
    if isinstance(query, str) and "openhexa_dataset_files" in query.lower():
        match = _function_pattern.search(query)
        if match:
            # Extract parameter (workspace) if provided
            param = match.group(1).strip().strip("'\"") if match.group(1) else None
            workspace = param if param else None
            
            # Call function and register result
            df = openhexa_dataset_files(workspace=workspace)
            view_name = "_openhexa_temp_view"
            self.register(view_name, df)
            
            # Replace function call with view name
            modified_query = _function_pattern.sub(view_name, query)
            logger.debug(f"Replaced openhexa_dataset_files with temp view")
            return _original_execute(self, modified_query, *args, **kwargs)
    
    return _original_execute(self, query, *args, **kwargs)


# Apply monkey-patch
duckdb.DuckDBPyConnection.execute = _patched_execute
logger.debug("Enabled automatic table function handling")
