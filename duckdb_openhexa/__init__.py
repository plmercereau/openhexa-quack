"""DuckDB Extension - provides GraphQL querying as table functions."""

import logging
import re
from typing import Any, Optional

import duckdb
import duckdb_engine  # noqa: F401
import pandas as pd

from duckdb_openhexa.client import OpenHexaGraphQLClient
from duckdb_openhexa.table_functions import openhexa_dataset_files, get_dataset_file_url

__all__ = [
    "openhexa_dataset_files",
    "get_dataset_file_url",
    "register_functions",
]
__version__ = "0.1.0"

logger = logging.getLogger(__name__)


def register_functions(conn: duckdb.DuckDBPyConnection) -> None:
    """Register custom scalar functions on a DuckDB connection."""
    try:
        conn.create_function("get_dataset_file_url", get_dataset_file_url)
        logger.info("Registered get_dataset_file_url UDF")
    except Exception as e:
        logger.error(f"Failed to register UDF: {e}", exc_info=True)
        raise


# Compile regex patterns once for performance
_FUNCTION_PATTERN = re.compile(
    r"openhexa_dataset_files\(['\"]?([^'\")\s]+)?['\"]?\)", re.IGNORECASE
)
_FUNCTION_NAME_PATTERN = re.compile(r"openhexa_dataset_files\([^)]*\)", re.IGNORECASE)

# Empty DataFrame schema for consistency
_EMPTY_COLUMNS = ["workspace", "dataset", "version", "filename", "file_id"]

# Store original execute method
_original_duckdb_execute = duckdb.DuckDBPyConnection.execute

# Track which connections have registered the UDF
_udf_registered_connections = set()


def _register_udf_if_needed(conn: duckdb.DuckDBPyConnection) -> None:
    """Register scalar UDF on first use per connection."""
    conn_id = id(conn)
    if conn_id in _udf_registered_connections:
        return
    
    try:
        conn.create_function("get_dataset_file_url", get_dataset_file_url)
        logger.debug("Registered get_dataset_file_url UDF")
    except Exception as e:
        logger.error(f"Failed to register UDF: {e}", exc_info=True)
    finally:
        _udf_registered_connections.add(conn_id)


def _extract_workspace(query: str) -> Optional[str]:
    """Extract workspace parameter from openhexa_dataset_files() call."""
    match = _FUNCTION_PATTERN.search(query)
    return match.group(1) if match and match.group(1) else None


def _fetch_and_register_data(conn: duckdb.DuckDBPyConnection, workspace: Optional[str]) -> str:
    """Fetch data from GraphQL and register as temporary view."""
    client = OpenHexaGraphQLClient()
    datasets = client.query_datasets(workspace=workspace)
    
    df = pd.DataFrame(datasets) if datasets else pd.DataFrame(columns=_EMPTY_COLUMNS)
    view_name = f"_openhexa_data_{abs(hash(workspace or 'all')) % 100000}"
    
    conn.register(view_name, df)
    logger.info(f"✓ Registered {len(df)} rows as {view_name}" + 
                (f" (workspace={workspace})" if workspace else ""))
    
    return view_name


def _patched_duckdb_execute(
    self: duckdb.DuckDBPyConnection, query: str, *args: Any, **kwargs: Any
) -> Any:
    """Intercept DuckDB queries and replace openhexa_dataset_files() with registered DataFrame."""
    _register_udf_if_needed(self)
    
    # Only intercept queries that contain openhexa_dataset_files
    if not isinstance(query, str) or "openhexa_dataset_files" not in query.lower():
        return _original_duckdb_execute(self, query, *args, **kwargs)

    try:
        workspace = _extract_workspace(query)
        view_name = _fetch_and_register_data(self, workspace)
        rewritten_query = _FUNCTION_NAME_PATTERN.sub(view_name, query)
        
        return _original_duckdb_execute(self, rewritten_query, *args, **kwargs)

    except Exception as e:
        logger.error(f"Error processing openhexa_dataset_files(): {e}", exc_info=True)
        raise


# Apply the monkey-patch
duckdb.DuckDBPyConnection.execute = _patched_duckdb_execute
logger.debug("DuckDB execute method patched for openhexa_dataset_files() interception")
