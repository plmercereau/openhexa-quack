"""GraphQL client for querying OpenHexa API."""

import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from glom import glom
from gql import Client, gql
from gql.transport.exceptions import TransportQueryError
from gql.transport.requests import RequestsHTTPTransport

logger = logging.getLogger(__name__)

# Global in-memory cache for download URLs
# Cache stores tuples of (url, timestamp) for TTL enforcement
_download_url_cache: Dict[str, Tuple[Optional[str], float]] = {}
_CACHE_SIZE = int(os.getenv("OPENHEXA_CACHE_SIZE", "1000"))
_CACHE_TTL_SECONDS = int(os.getenv("GCS_SIGNED_BUCKET_CACHE_TTL_MINUTES", "9")) * 60
logger.info(
    f"Global download URL cache configured with max size {_CACHE_SIZE} and TTL {_CACHE_TTL_SECONDS}s"
)


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

        transport = RequestsHTTPTransport(
            url=self.url,
            headers=headers,
            verify=True,
            retries=3,
            timeout=30,
        )

        return transport

    def query_datasets(self, workspace: Optional[str] = None) -> List[Dict[str, str]]:
        """Query datasets from OpenHexa, optionally filtered by workspace (NOT cached - always fresh)."""
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
                            "file_path": f"{workspace_slug}/{dataset_slug}/{version_name}/{file_item.get('filename', '')}",
                        }
                    )

        return records

    # def query_file_download_url(self, workspace_slug: str, dataset_slug: str, version: str, filename: str) -> Optional[str]:
    def query_file_download_url(self, file_path: str) -> Optional[str]:
        """Query download URL for a specific dataset file (cached in global memory with TTL).

        Uses a global dict cache shared across all client instances.
        Cache entries expire after GCS_SIGNED_BUCKET_CACHE_TTL_MINUTES (default: 9 minutes).
        Cache size can be configured via OPENHEXA_CACHE_SIZE env var (default: 1000).
        """
        # Check global cache first and verify TTL
        if file_path in _download_url_cache:
            cached_url, cached_time = _download_url_cache[file_path]
            age_seconds = time.time() - cached_time

            if age_seconds < _CACHE_TTL_SECONDS:
                logger.info(
                    f"CACHE HIT for {file_path} (age: {age_seconds:.1f}s, cache size: {len(_download_url_cache)})"
                )
                return cached_url
            else:
                # Cache entry expired
                logger.info(
                    f"CACHE EXPIRED for {file_path} (age: {age_seconds:.1f}s > TTL: {_CACHE_TTL_SECONDS}s)"
                )
                del _download_url_cache[file_path]

        # Parse file path: workspace/dataset/version/filename
        parts = file_path.split("/")
        if len(parts) < 4:
            raise ValueError(
                f"Invalid file path format. Expected 'workspace/dataset/version/filename', got '{file_path}' "
                f"(only {len(parts)} parts found)"
            )
        workspace_slug, dataset_slug, version, filename = parts[0], parts[1], parts[2], "/".join(parts[3:])

        query_string = """
        query GetFileDownloadUrl($workspaceSlug: String!, $datasetSlug: String!, $filename: String!) {{
            datasetLinkBySlug(workspaceSlug: $workspaceSlug, datasetSlug: $datasetSlug) {{
                dataset {{
                    {version_query} {{
                        fileByName(name: $filename) {{
                            downloadUrl(attachment: false)
                        }}
                    }}
                }}
            }}
        }}
        """.format(
            version_query="latestVersion" if version == "latest" else f'version(id: "{version}")'
        )

        try:
            transport = self._get_transport()
            client = Client(transport=transport, fetch_schema_from_transport=False)

            query = gql(query_string)
            result = client.execute(
                query,
                variable_values={
                    "workspaceSlug": workspace_slug,
                    "datasetSlug": dataset_slug,
                    "filename": filename,
                },
            )

            logger.info(f"Fetched download URL from API for {file_path}")

            version_key = "latestVersion" if version == "latest" else "version"
            path = f"datasetLinkBySlug.dataset.{version_key}.fileByName.downloadUrl"

            download_url = glom(result, path, default=None)

            # Store in global cache with current timestamp (simple LRU: remove oldest if cache is full)
            if len(_download_url_cache) >= _CACHE_SIZE:
                # Remove first (oldest) item
                oldest_key = next(iter(_download_url_cache))
                del _download_url_cache[oldest_key]
                logger.debug(f"Cache full, evicted {oldest_key}")

            _download_url_cache[file_path] = (download_url, time.time())
            logger.info(
                f"Fetched and cached download URL for {file_path} (cache size: {len(_download_url_cache)}, TTL: {_CACHE_TTL_SECONDS}s)"
            )

            return download_url

        except TransportQueryError as e:
            logger.error(
                f"GraphQL query failed for file path {file_path}: {e.errors if hasattr(e, 'errors') else e}"
            )
            return None
        except Exception as e:
            logger.error(f"Unexpected error querying file path {file_path}: {e}", exc_info=True)
            return None
