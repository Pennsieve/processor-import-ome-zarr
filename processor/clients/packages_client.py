import json
import logging

import requests

from .base_client import DEFAULT_TIMEOUT, BaseClient, SessionManager

log = logging.getLogger()


class PackagesClient(BaseClient):
    """Client for the Pennsieve packages-service viewer assets API."""

    def __init__(self, session_manager: SessionManager):
        super().__init__(session_manager)
        self.base_url = f"{session_manager.api_host2}/packages"

    @BaseClient.retry_with_refresh
    def create_viewer_asset(
        self,
        dataset_id: str,
        name: str,
        asset_type: str,
        package_ids: list[str],
        properties: dict | None = None,
    ) -> dict:
        """
        Create a viewer asset and obtain temporary STS credentials scoped to its S3 prefix.

        Returns the parsed createViewerAssetResponse: {"asset": {...}, "upload_credentials": {...}}.
        """
        url = f"{self.base_url}/assets?dataset_id={dataset_id}"
        body = {
            "name": name,
            "asset_type": asset_type,
            "package_ids": package_ids,
            "properties": properties or {},
        }

        try:
            response = requests.post(url, headers=self._get_headers(), json=body, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            log.error(f"Failed to create viewer asset: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode create viewer asset response: {e}")
            raise

    @BaseClient.retry_with_refresh
    def update_viewer_asset_status(self, asset_id: str, dataset_id: str, status: str) -> dict:
        """Set status on an existing viewer asset (enum: created | ready)."""
        url = f"{self.base_url}/assets/{asset_id}?dataset_id={dataset_id}"
        body = {"status": status}

        try:
            response = requests.patch(url, headers=self._get_headers(), json=body, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            log.error(f"Failed to update viewer asset {asset_id}: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode update viewer asset response: {e}")
            raise

    @BaseClient.retry_with_refresh
    def delete_viewer_asset(self, asset_id: str, dataset_id: str) -> None:
        """Delete a viewer asset and its S3 objects under the asset prefix."""
        url = f"{self.base_url}/assets/{asset_id}?dataset_id={dataset_id}"

        try:
            response = requests.delete(url, headers=self._get_headers(), timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
        except requests.HTTPError as e:
            log.error(f"Failed to delete viewer asset {asset_id}: {e}")
            raise
