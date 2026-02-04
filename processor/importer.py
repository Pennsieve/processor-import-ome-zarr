import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Value

import backoff
import requests
from clients import AuthenticationClient, ImportClient, ImportFile, SessionManager, WorkflowClient, prepare_import_files
from config import Config

log = logging.getLogger(__name__)

# Number of parallel upload threads
UPLOAD_WORKERS = 4


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

        integration_id = self.config.INTEGRATION_ID

        # Get dataset_id from workflow instance
        workflow_instance = self.workflow_client.get_workflow_instance(integration_id)
        dataset_id = workflow_instance.dataset_id

        log.info(f"dataset_id={dataset_id} starting import of OME-Zarr files")

        # Prepare import files with client-generated upload keys
        import_files = prepare_import_files(files, zarr_name)

        # Options for viewer asset import
        options = {
            "asset_type": self.config.ASSET_TYPE,
            "asset_name": zarr_name,
            "properties": {},
        }

        # Create import manifest with batching
        import_id = self.import_client.create_batched(
            integration_id=integration_id,
            dataset_id=dataset_id,
            import_files=import_files,
            options=options,
        )

        log.info(f"import_id={import_id} initialized import with {len(import_files)} files for upload")

        # Upload all files
        self._upload_files(import_id, dataset_id, import_files)

        log.info(f"import_id={import_id} import complete")
        return import_id

    def _upload_files(self, import_id: str, dataset_id: str, import_files: list[ImportFile]) -> None:
        """
        Upload files to S3 using presigned URLs.

        Args:
            import_id: Import manifest ID
            dataset_id: Dataset ID
            import_files: List of ImportFile objects
        """
        total = len(import_files)
        upload_counter = Value("i", 0)
        upload_counter_lock = Lock()

        @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
        def upload_file(import_file):
            try:
                with upload_counter_lock:
                    upload_counter.value += 1
                    current = upload_counter.value
                    if current % 100 == 0 or current == total:
                        log.info(f"import_id={import_id} uploading {current}/{total}")

                upload_url = self.import_client.get_presign_url(import_id, dataset_id, import_file.upload_key)
                with open(import_file.local_path, "rb") as f:
                    response = requests.put(upload_url, data=f)
                    response.raise_for_status()
                return True
            except Exception as e:
                with upload_counter_lock:
                    upload_counter.value -= 1
                log.error(f"import_id={import_id} failed to upload {import_file.local_path}: {e}")
                raise

        log.info(f"import_id={import_id} starting upload of {total} files")

        with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as executor:
            results = list(executor.map(upload_file, import_files))

        assert sum(results) == total, "Failed to upload all files"
        log.info(f"import_id={import_id} uploaded {total} files")
