"""GraphQL client for querying OpenHexa API."""

import logging
import os
from typing import Any, Dict, List, Optional

from gql import Client, gql
from gql.transport.exceptions import TransportQueryError
from gql.transport.requests import RequestsHTTPTransport

logger = logging.getLogger(__name__)


class OpenHexaGraphQLClient:
    """Client for querying OpenHexa GraphQL API with authentication."""

    def __init__(self, url: Optional[str] = None):
        """Initialize the GraphQL client with optional URL."""
        self.url = url or os.getenv("OPENHEXA_GRAPHQL_URL", "https://app.openhexa.org/graphql/")
        self.api_token = os.getenv("OPENHEXA_API_TOKEN")

        if not self.api_token:
            logger.warning("OPENHEXA_API_TOKEN not set - queries may fail")

    def _get_transport(self) -> RequestsHTTPTransport:
        """Create a transport with authentication headers."""
        headers = {}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        return RequestsHTTPTransport(
            url=self.url, headers=headers, verify=True, retries=3, timeout=30
        )

    def query_datasets(self, workspace: Optional[str] = None) -> List[Dict[str, str]]:
        """Query datasets from OpenHexa, optionally filtered by workspace."""
        query_string = """
        query GetDatasets($query: String!, $perPage: Int!) {
            datasets(query: $query, perPage: $perPage) {
                totalPages
                items {
                    id
                    slug
                    name
                    workspace {
                        slug
                    }
                    versions {
                        items {
                            name
                            files {
                                items {
                                    id
                                    filename
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        try:
            transport = self._get_transport()
            client = Client(transport=transport, fetch_schema_from_transport=False)
            query = gql(query_string)
            result = client.execute(query, variable_values={"query": "", "perPage": 1000})
            
            records = self._flatten_datasets(result, workspace)
            logger.info(
                f"Retrieved {len(records)} dataset files"
                + (f" from workspace '{workspace}'" if workspace else "")
            )
            return records

        except TransportQueryError as e:
            logger.error(f"GraphQL query failed: {e.errors if hasattr(e, 'errors') else e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error querying datasets: {e}", exc_info=True)
            return []

    def _flatten_datasets(
        self, result: Dict[str, Any], workspace_filter: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """Flatten the nested GraphQL response into a list of records."""
        records = []
        datasets = result.get("datasets", {}).get("items", [])

        for dataset in datasets:
            dataset_slug = dataset.get("slug", "")
            workspace_data = dataset.get("workspace", {})
            workspace_slug = workspace_data.get("slug", "") if workspace_data else ""

            # Filter by workspace if specified
            if workspace_filter and workspace_slug != workspace_filter:
                continue

            versions = dataset.get("versions", {}).get("items", [])

            for version in versions:
                version_name = version.get("name", "")
                files = version.get("files", {}).get("items", [])

                for file_item in files:
                    records.append(
                        {
                            "workspace": workspace_slug,
                            "dataset": dataset_slug,
                            "version": version_name,
                            "filename": file_item.get("filename", ""),
                            "file_id": file_item.get("id", ""),
                        }
                    )

        return records

    def query_file_download_url(self, file_id: str) -> Optional[str]:
        """Query download URL for a specific dataset file."""
        query_string = """
        query GetFileDownloadUrl($fileId: ID!) {
            datasetVersionFile(id: $fileId) {
                downloadUrl
            }
        }
        """

        try:
            transport = self._get_transport()
            client = Client(transport=transport, fetch_schema_from_transport=False)
            query = gql(query_string)
            result = client.execute(query, variable_values={"fileId": file_id})
            
            file_data = result.get("datasetVersionFile")

            if file_data:
                download_url = file_data.get("downloadUrl")
                logger.info(f"Retrieved download URL for file {file_id}")
                return download_url
            else:
                logger.warning(f"File {file_id} not found")
                return None

        except TransportQueryError as e:
            logger.error(
                f"GraphQL query failed for file {file_id}: {e.errors if hasattr(e, 'errors') else e}"
            )
            return None
        except Exception as e:
            logger.error(f"Unexpected error querying file {file_id}: {e}", exc_info=True)
            return None
