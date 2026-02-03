import logging
import os

import backoff
import requests
from clients.base_client import BaseClient, SessionManager
from utils import get_file_extension

log = logging.getLogger()

# Maximum manifest size to avoid API Gateway limits
MAX_MANIFEST_FILES = 1000


class ImportClient(BaseClient):
    """Client for Pennsieve import service API."""

    def __init__(self, session_manager: SessionManager):
        """
        Initialize the import client.

        Args:
            session_manager: SessionManager instance for API access
        """
        super().__init__(session_manager)
        self.base_url = f"{session_manager.api_host2}/import"

    @BaseClient.retry_with_refresh
    def create_manifest(
        self,
        integration_id: str,
        files: list[dict],
        asset_type: str,
        asset_name: str,
        provenance_id: str,
    ) -> dict:
        """
        Create an import manifest for viewer assets.

        Uses the asset_name option to signal that all files should be grouped
        under a single viewer asset record pointing to the shared prefix.

        Args:
            integration_id: Workflow integration UUID
            files: List of file dicts with uploadId, s3Key, targetPath, targetName
            asset_type: Type of viewer asset (e.g., 'ome-zarr')
            asset_name: Name for the viewer asset (top-level prefix)
            provenance_id: Provenance UUID for the asset

        Returns:
            Manifest response dict containing manifest ID and file upload info
        """
        payload = {
            "integrationId": integration_id,
            "importType": "viewerassets",
            "files": files,
            "options": {
                "asset_type": asset_type,
                "asset_name": asset_name,
                "properties": {},
                "provenance_id": provenance_id,
            },
        }

        log.info(f"Creating import manifest with {len(files)} files, asset_name={asset_name}")

        response = requests.post(
            self.base_url,
            json=payload,
            headers=self._get_headers(),
        )
        response.raise_for_status()

        result = response.json()
        log.info(f"Created manifest: {result.get('id')}")
        return result

    @BaseClient.retry_with_refresh
    def append_files(self, manifest_id: str, files: list[dict]) -> dict:
        """
        Append additional files to an existing manifest.

        Args:
            manifest_id: ID of the existing manifest
            files: List of file dicts to append

        Returns:
            Updated manifest response
        """
        url = f"{self.base_url}/{manifest_id}/files"
        payload = {"files": files}

        log.info(f"Appending {len(files)} files to manifest {manifest_id}")

        response = requests.post(
            url,
            json=payload,
            headers=self._get_headers(),
        )
        response.raise_for_status()

        return response.json()

    @BaseClient.retry_with_refresh
    def get_presigned_url(self, manifest_id: str, upload_key: str) -> str:
        """
        Get a presigned S3 URL for uploading a file.

        Args:
            manifest_id: ID of the manifest
            upload_key: Upload key for the specific file

        Returns:
            Presigned S3 URL for PUT upload
        """
        url = f"{self.base_url}/{manifest_id}/upload/{upload_key}/presign"

        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()

        return response.json()["url"]

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def upload_file(self, presigned_url: str, file_path: str) -> None:
        """
        Upload a file to S3 using a presigned URL.

        Args:
            presigned_url: Presigned S3 URL for PUT
            file_path: Local path to the file to upload
        """
        with open(file_path, "rb") as f:
            response = requests.put(presigned_url, data=f)
            response.raise_for_status()

    def prepare_file_entries(self, files: list[tuple[str, str]], zarr_name: str) -> list[dict]:
        """
        Prepare file entries for the import manifest.

        Args:
            files: List of (absolute_path, relative_path) tuples
            zarr_name: Name of the OME-Zarr directory (used as prefix)

        Returns:
            List of file entry dicts for the manifest
        """
        entries = []
        for abs_path, rel_path in files:
            # Include zarr_name in the target path so files are grouped under it
            target_path = os.path.join(zarr_name, rel_path)
            filename = os.path.basename(rel_path)
            extension = get_file_extension(filename)

            entries.append(
                {
                    "targetPath": target_path,
                    "targetName": filename,
                    "fileExtension": extension,
                    # Store absolute path for later upload (not sent to API)
                    "_localPath": abs_path,
                }
            )

        return entries

    def create_manifest_batched(
        self,
        integration_id: str,
        files: list[tuple[str, str]],
        zarr_name: str,
        asset_type: str,
        provenance_id: str,
    ) -> tuple[str, list[dict]]:
        """
        Create an import manifest, batching if necessary to avoid API limits.

        Args:
            integration_id: Workflow integration UUID
            files: List of (absolute_path, relative_path) tuples
            zarr_name: Name of the OME-Zarr directory
            asset_type: Type of viewer asset (e.g., 'ome-zarr')
            provenance_id: Provenance UUID for the asset

        Returns:
            Tuple of (manifest_id, list of file entries with upload keys)
        """
        entries = self.prepare_file_entries(files, zarr_name)

        # Create initial manifest with first batch
        first_batch = entries[:MAX_MANIFEST_FILES]
        manifest_files = [{k: v for k, v in e.items() if not k.startswith("_")} for e in first_batch]

        result = self.create_manifest(
            integration_id=integration_id,
            files=manifest_files,
            asset_type=asset_type,
            asset_name=zarr_name,
            provenance_id=provenance_id,
        )

        manifest_id = result["id"]

        # Map upload keys back to entries
        file_results = result.get("files", [])
        for i, file_result in enumerate(file_results):
            entries[i]["uploadKey"] = file_result["uploadKey"]

        # Append remaining batches
        remaining = entries[MAX_MANIFEST_FILES:]
        batch_num = 2
        while remaining:
            batch = remaining[:MAX_MANIFEST_FILES]
            batch_files = [{k: v for k, v in e.items() if not k.startswith("_")} for e in batch]

            log.info(f"Appending batch {batch_num} with {len(batch)} files")
            append_result = self.append_files(manifest_id, batch_files)

            # Map upload keys
            append_files_result = append_result.get("files", [])
            start_idx = (batch_num - 1) * MAX_MANIFEST_FILES
            for i, file_result in enumerate(append_files_result):
                entries[start_idx + i]["uploadKey"] = file_result["uploadKey"]

            remaining = remaining[MAX_MANIFEST_FILES:]
            batch_num += 1

        return manifest_id, entries
