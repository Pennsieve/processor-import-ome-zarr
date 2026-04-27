import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import backoff
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from processor.clients import (
    AuthenticationClient,
    PackagesClient,
    SessionManager,
    WorkflowClient,
)
from processor.config import Config

log = logging.getLogger(__name__)


class OmeZarrImporter:
    """Handles importing OME-Zarr files to Pennsieve via the packages-service viewer assets API."""

    def __init__(self, config: Config):
        self.config = config
        self.session_manager = None
        self.packages_client = None
        self.workflow_client = None

    def _initialize_clients(self) -> None:
        """Initialize API clients and authenticate."""
        self.session_manager = SessionManager(
            api_host=self.config.PENNSIEVE_API_HOST,
            api_host2=self.config.PENNSIEVE_API_HOST2,
            api_key=self.config.PENNSIEVE_API_KEY,
            api_secret=self.config.PENNSIEVE_API_SECRET,
        )

        auth_client = AuthenticationClient(self.session_manager)
        auth_client.authenticate()

        self.packages_client = PackagesClient(self.session_manager)
        self.workflow_client = WorkflowClient(self.session_manager)

    def import_zarr(self, zarr_name: str, files: list[tuple[str, str]]) -> str:
        """
        Import an OME-Zarr directory to Pennsieve as a viewer asset.

        Calls packages-service POST /assets to create the asset row and obtain temporary
        STS credentials scoped to a single S3 prefix, uploads every file directly to S3,
        then PATCHes the asset to status="ready". On unrecoverable failure, DELETEs the
        partially populated asset.

        Args:
            zarr_name: Name of the OME-Zarr directory (used as the asset name)
            files: List of (absolute_path, relative_path) tuples

        Returns:
            The created viewer asset's UUID
        """
        self._initialize_clients()

        workflow_instance_id = self.config.WORKFLOW_INSTANCE_ID
        workflow_instance = self.workflow_client.get_workflow_instance(workflow_instance_id)
        dataset_id = workflow_instance.dataset_id
        package_id = workflow_instance.package_ids[0] if workflow_instance.package_ids else None

        if not package_id:
            raise ValueError("No package ID found in workflow instance")

        log.info(f"dataset_id={dataset_id} package_id={package_id} starting import of OME-Zarr files")

        response = self.packages_client.create_viewer_asset(
            dataset_id=dataset_id,
            name=zarr_name,
            asset_type=self.config.ASSET_TYPE,
            package_ids=[package_id],
            properties={},
        )
        asset = response["asset"]
        credentials = response["upload_credentials"]
        asset_id = asset["id"]

        log.info(
            f"asset_id={asset_id} created viewer asset; bucket={credentials['bucket']} "
            f"key_prefix={credentials['key_prefix']} creds_expire_at={credentials.get('expiration')}"
        )

        try:
            started = time.monotonic()
            self._upload_files(asset_id, files, credentials)
            elapsed = time.monotonic() - started
            log.info(f"asset_id={asset_id} uploaded {len(files)} files in {elapsed:.1f}s")

            self.packages_client.update_viewer_asset_status(asset_id, dataset_id, "ready")
            log.info(f"asset_id={asset_id} marked ready")
        except Exception:
            log.error(f"asset_id={asset_id} import failed; cleaning up asset", exc_info=True)
            try:
                self.packages_client.delete_viewer_asset(asset_id, dataset_id)
                log.info(f"asset_id={asset_id} deleted after failure")
            except Exception:
                log.exception(f"asset_id={asset_id} cleanup delete failed")
            raise

        return asset_id

    def _upload_files(
        self,
        asset_id: str,
        files: list[tuple[str, str]],
        credentials: dict,
    ) -> None:
        """
        Upload every zarr file to S3 under the asset's key prefix using the supplied
        STS credentials. Collects all per-file failures and raises RuntimeError if any
        upload fails.
        """
        bucket = credentials["bucket"]
        key_prefix = credentials["key_prefix"]
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=credentials["access_key_id"],
            aws_secret_access_key=credentials["secret_access_key"],
            aws_session_token=credentials["session_token"],
            region_name=credentials.get("region"),
        )

        total = len(files)
        upload_counter = 0
        counter_lock = threading.Lock()
        failed_uploads: list[tuple[str, Exception]] = []
        failed_lock = threading.Lock()

        @backoff.on_exception(backoff.expo, (BotoCoreError, ClientError), max_tries=5)
        def _put(local_path: str, key: str) -> None:
            s3_client.upload_file(local_path, bucket, key)

        def upload_one(local_path: str, rel_path: str) -> None:
            nonlocal upload_counter
            normalized_rel = rel_path.replace("\\", "/")
            key = key_prefix + normalized_rel
            try:
                _put(local_path, key)
                with counter_lock:
                    upload_counter += 1
                    current = upload_counter
                    if current % 100 == 0 or current == total:
                        log.info(f"asset_id={asset_id} uploaded {current}/{total}")
            except Exception as e:
                with failed_lock:
                    failed_uploads.append((local_path, e))
                log.error(f"asset_id={asset_id} failed to upload {local_path}: {e}", exc_info=True)
                raise

        log.info(f"asset_id={asset_id} starting upload of {total} files")

        with ThreadPoolExecutor(max_workers=self.config.UPLOAD_WORKERS) as executor:
            futures = {executor.submit(upload_one, abs_path, rel_path): abs_path for abs_path, rel_path in files}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass

        if failed_uploads:
            raise RuntimeError(f"Failed to upload {len(failed_uploads)} of {total} files")

        log.info(f"asset_id={asset_id} uploaded {total} files")
