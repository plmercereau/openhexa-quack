"""DuckDB table functions for GraphQL queries."""

import pandas as pd
from typing import Optional

from duckdb_openhexa.client import OpenHexaGraphQLClient

# Schema for empty DataFrame (must match GraphQL query results)
_EMPTY_SCHEMA = ["workspace", "dataset", "version", "filename", "file_id"]


def openhexa_dataset_files(workspace: Optional[str] = None) -> pd.DataFrame:
    """Query datasets from OpenHexa GraphQL API, optionally filtered by workspace."""
    # Create GraphQL client
    client = OpenHexaGraphQLClient()

    # Query datasets
    datasets = client.query_datasets(workspace=workspace)

    # Return DataFrame with consistent schema
    return pd.DataFrame(datasets) if datasets else pd.DataFrame(columns=_EMPTY_SCHEMA)


def get_dataset_file_url(file_id: str) -> Optional[str]:
    """Get download URL for a dataset file by ID."""
    # Create GraphQL client
    client = OpenHexaGraphQLClient()

    # Query the file download URL
    return client.query_file_download_url(file_id)
