import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from clients import AuthenticationClient, ImportClient, SessionManager, WorkflowClient
from config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

# Number of parallel upload threads
UPLOAD_WORKERS = 10


class OmeZarrImporter:
    """Handles importing OME-Zarr files to Pennsieve."""

    def __init__(self, config: Config):
        """
        Initialize the importer.

        Args:
            config: Configuration instance
        """
        self.config = config
        self.session_manager = None
        self.import_client = None
        self.workflow_client = None

    def _initialize_clients(self) -> None:
        """Initialize API clients and authenticate."""
        self.session_manager = SessionManager(
            api_host=self.config.PENNSIEVE_API_HOST,
            api_host2=self.config.PENNSIEVE_API_HOST2,
            api_key=self.config.PENNSIEVE_API_KEY,
            api_secret=self.config.PENNSIEVE_API_SECRET,
        )

        # Authenticate
        auth_client = AuthenticationClient(self.session_manager)
        auth_client.authenticate()

        # Initialize other clients
        self.import_client = ImportClient(self.session_manager)
        self.workflow_client = WorkflowClient(self.session_manager)

    def _upload_file(self, manifest_id: str, entry: dict) -> str:
        """
        Upload a single file to S3.

        Args:
            manifest_id: Import manifest ID
            entry: File entry dict with uploadKey and _localPath

        Returns:
            Upload key of the uploaded file
        """
        upload_key = entry["uploadKey"]
        local_path = entry["_localPath"]

        presigned_url = self.import_client.get_presigned_url(manifest_id, upload_key)
        self.import_client.upload_file(presigned_url, local_path)

        return upload_key

    def _upload_files_parallel(self, manifest_id: str, entries: list[dict]) -> None:
        """
        Upload files in parallel using a thread pool.

        Args:
            manifest_id: Import manifest ID
            entries: List of file entries to upload
        """
        total = len(entries)
        completed = 0

        log.info(f"Starting parallel upload of {total} files")

        with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as executor:
            futures = {executor.submit(self._upload_file, manifest_id, entry): entry for entry in entries}

            for future in as_completed(futures):
                entry = futures[future]
                try:
                    future.result()  # Raises exception if upload failed
                    completed += 1
                    if completed % 100 == 0 or completed == total:
                        log.info(f"Upload progress: {completed}/{total} files")
                except Exception as e:
                    log.error(f"Failed to upload {entry['targetPath']}: {e}")
                    raise

        log.info(f"Completed upload of {total} files")

    def import_zarr(self, zarr_name: str, files: list[tuple[str, str]]) -> str:
        """
        Import an OME-Zarr directory to Pennsieve.

        Creates an import manifest with asset_name set to the zarr directory name,
        which signals that all files should be grouped under a single viewer asset
        record pointing to the shared prefix.

        Args:
            zarr_name: Name of the OME-Zarr directory (becomes the asset_name)
            files: List of (absolute_path, relative_path) tuples

        Returns:
            Import manifest ID
        """
        self._initialize_clients()

        # Get provenance ID from workflow integration
        integration_id = self.config.INTEGRATION_ID
        provenance_id = self.workflow_client.get_provenance_id(integration_id)

        # Create import manifest with asset_name to trigger single-record mode
        manifest_id, entries = self.import_client.create_manifest_batched(
            integration_id=integration_id,
            files=files,
            zarr_name=zarr_name,
            asset_type=self.config.ASSET_TYPE,
            provenance_id=provenance_id,
        )

        # Upload all files
        self._upload_files_parallel(manifest_id, entries)

        log.info(f"Import complete. Manifest ID: {manifest_id}")
        return manifest_id
