"""DuckDB table functions for GraphQL queries."""

import pandas as pd
from typing import Optional

from duckdb_openhexa.client import OpenHexaGraphQLClient

# Schema for empty DataFrame (must match GraphQL query results)
_EMPTY_SCHEMA = ["workspace", "dataset", "version", "filename", "file_path"]

# Global GraphQL client instance (shared across all function calls)
# This avoids creating a new client for each query while still benefiting from module-level cache
_graphql_client = OpenHexaGraphQLClient()


def openhexa_dataset_files(workspace: Optional[str] = None) -> pd.DataFrame:
    """Query datasets from OpenHexa GraphQL API, optionally filtered by workspace."""
    # Use global GraphQL client instance
    datasets = _graphql_client.query_datasets(workspace=workspace)

    # Return DataFrame with consistent schema
    return pd.DataFrame(datasets) if datasets else pd.DataFrame(columns=_EMPTY_SCHEMA)


def get_dataset_file_url(file_path: str) -> Optional[str]:
    """Get download URL for a dataset file by path (cached with TTL).

    Results are cached in memory with a TTL of GCS_SIGNED_BUCKET_CACHE_TTL_MINUTES (default: 9 minutes).
    Cache entries automatically expire and are refreshed from the API when stale.
    """
    # Use global GraphQL client instance (with global cache)
    return _graphql_client.query_file_download_url(file_path)
