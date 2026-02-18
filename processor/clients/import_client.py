import json
import logging
import math
import posixpath
import uuid
from dataclasses import dataclass

import backoff
import requests

from .base_client import DEFAULT_TIMEOUT, BaseClient, SessionManager

log = logging.getLogger()

MAX_REQUEST_SIZE_BYTES = 1 * 1024 * 1024  # Stay under AWS API Gateway payload limit
DEFAULT_BATCH_SIZE = 1000


@dataclass(frozen=True, slots=True)
class ImportFile:
    """Represents a file to be imported with upload metadata."""

    upload_key: uuid.UUID
    file_path: str
    local_path: str


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
    def create(
        self, integration_id: str, dataset_id: str, package_id: str, import_files: list[ImportFile], options: dict
    ) -> str:
        """
        Create an import manifest for viewer assets.

        Args:
            integration_id: Workflow integration UUID
            dataset_id: Dataset ID
            package_id: Package ID to associate the import with
            import_files: List of ImportFile objects
            options: Options dict with asset_type, properties, provenance_id, and optionally asset_name

        Returns:
            Import manifest ID
        """
        url = f"{self.base_url}?dataset_id={dataset_id}"

        body = {
            "integration_id": integration_id,
            "package_id": package_id,
            "import_type": "viewerassets",
            "files": [{"upload_key": str(f.upload_key), "file_path": f.file_path} for f in import_files],
            "options": options,
        }

        try:
            response = requests.post(url, headers=self._get_headers(), json=body, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            return data["id"]
        except requests.HTTPError as e:
            log.error(f"Failed to create import: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode import response: {e}")
            raise

    @BaseClient.retry_with_refresh
    def append_files(self, import_id: str, dataset_id: str, import_files: list[ImportFile]) -> dict:
        """
        Append files to an existing import manifest.

        Args:
            import_id: The import manifest ID
            dataset_id: The dataset ID
            import_files: List of ImportFile objects to append

        Returns:
            Response from the API
        """
        url = f"{self.base_url}/{import_id}/files?dataset_id={dataset_id}"

        body = {"files": [{"upload_key": str(f.upload_key), "file_path": f.file_path} for f in import_files]}

        try:
            response = requests.post(url, headers=self._get_headers(), json=body, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            log.error(f"Failed to append files to import {import_id}: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode append files response: {e}")
            raise

    def create_batched(
        self, integration_id: str, dataset_id: str, package_id: str, import_files: list[ImportFile], options: dict
    ) -> str:
        """
        Create an import manifest with batched file additions to avoid API Gateway size limits.

        Args:
            integration_id: The workflow/integration ID
            dataset_id: The dataset ID
            package_id: The package ID to associate the import with
            import_files: List of all ImportFile objects
            options: Options dict for viewer assets

        Returns:
            The import ID
        """
        if not import_files:
            raise ValueError("No files provided for import")

        batch_size = calculate_batch_size(import_files)
        total_files = len(import_files)
        total_batches = math.ceil(total_files / batch_size)

        log.info(f"Creating import manifest with {total_files} files in {total_batches} batch(es)")

        first_batch = import_files[:batch_size]
        import_id = self.create(integration_id, dataset_id, package_id, first_batch, options)

        log.info(f"import_id={import_id} created manifest with initial batch of {len(first_batch)} files")

        for batch_num in range(1, total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_files)
            batch = import_files[start_idx:end_idx]

            self.append_files(import_id, dataset_id, batch)
            log.info(f"import_id={import_id} appended batch {batch_num + 1}/{total_batches} with {len(batch)} files")

        return import_id

    @BaseClient.retry_with_refresh
    def get_presign_url(self, import_id: str, dataset_id: str, upload_key) -> str:
        """
        Get a presigned S3 URL for uploading a file.

        Args:
            import_id: ID of the import manifest
            dataset_id: The dataset ID
            upload_key: Upload key for the specific file

        Returns:
            Presigned S3 URL for PUT upload
        """
        url = f"{self.base_url}/{import_id}/upload/{upload_key}/presign?dataset_id={dataset_id}"

        try:
            response = requests.get(url, headers=self._get_headers(), timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            return data["url"]
        except requests.HTTPError as e:
            log.error(f"Failed to get presign URL: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode presign URL response: {e}")
            raise

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def upload_file(self, presigned_url: str, file_path: str) -> None:
        """
        Upload a file to S3 using a presigned URL.

        Args:
            presigned_url: Presigned S3 URL for PUT
            file_path: Local path to the file to upload
        """
        # Use a longer timeout for uploads (60s read timeout for potentially large files)
        upload_timeout = (10, 60)
        with open(file_path, "rb") as f:
            response = requests.put(presigned_url, data=f, timeout=upload_timeout)
            response.raise_for_status()


def prepare_import_files(files: list[tuple[str, str]], zarr_name: str) -> list[ImportFile]:
    """
    Prepare ImportFile objects from file tuples.

    Args:
        files: List of (absolute_path, relative_path) tuples
        zarr_name: Name of the OME-Zarr directory (used as prefix in file_path)

    Returns:
        List of ImportFile objects with client-generated upload keys
    """
    import_files = []
    for abs_path, rel_path in files:
        # file_path includes the zarr_name prefix so files are grouped under it
        # Use posixpath.join to ensure forward slashes for API/S3 object keys
        # (rel_path may contain OS-specific separators on Windows)
        normalized_rel_path = rel_path.replace("\\", "/")
        file_path = posixpath.join(zarr_name, normalized_rel_path)
        import_file = ImportFile(
            upload_key=uuid.uuid4(),
            file_path=file_path,
            local_path=abs_path,
        )
        import_files.append(import_file)
    return import_files


def calculate_batch_size(sample_files: list[ImportFile], max_size_bytes: int = MAX_REQUEST_SIZE_BYTES) -> int:
    """
    Calculate the optimal batch size for manifest files based on actual payload size.

    Args:
        sample_files: List of ImportFile objects to estimate size from
        max_size_bytes: Maximum request size in bytes

    Returns:
        Number of files per batch
    """
    if not sample_files:
        return DEFAULT_BATCH_SIZE

    # Calculate actual size of a sample file entry using compact JSON encoding
    sample_size = 0
    sample_count = min(100, len(sample_files))
    for f in sample_files[:sample_count]:
        entry = {"upload_key": str(f.upload_key), "file_path": f.file_path}
        # Use compact separators to match typical wire format
        sample_size += len(json.dumps(entry, separators=(",", ":"))) + 1  # +1 for comma separator

    avg_bytes_per_file = sample_size / sample_count

    # Estimate fixed overhead for request body structure (integration_id, import_type, options, JSON punctuation)
    # Approximate: {"integration_id":"uuid","import_type":"viewerassets","files":[],"options":{...}}
    fixed_overhead_estimate = 500  # Conservative estimate for typical request

    # Calculate batch size with safety margin (80% of limit after fixed overhead)
    usable_size = (max_size_bytes - fixed_overhead_estimate) * 0.8
    batch_size = int(usable_size / avg_bytes_per_file)

    # Ensure at least 1 file per batch
    return max(1, batch_size)
