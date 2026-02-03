from unittest.mock import MagicMock, Mock, patch

from importer import UPLOAD_WORKERS, OmeZarrImporter


class TestOmeZarrImporter:
    """Tests for OmeZarrImporter class."""

    def test_initialization(self, mock_config):
        """Should initialize with config."""
        importer = OmeZarrImporter(mock_config)

        assert importer.config == mock_config
        assert importer.session_manager is None
        assert importer.import_client is None
        assert importer.workflow_client is None

    @patch("importer.AuthenticationClient")
    @patch("importer.ImportClient")
    @patch("importer.WorkflowClient")
    @patch("importer.SessionManager")
    def test_initialize_clients(self, mock_sm_class, mock_wf_class, mock_import_class, mock_auth_class, mock_config):
        """Should initialize all clients and authenticate."""
        mock_session_manager = Mock()
        mock_sm_class.return_value = mock_session_manager

        importer = OmeZarrImporter(mock_config)
        importer._initialize_clients()

        mock_sm_class.assert_called_once_with(
            api_host=mock_config.PENNSIEVE_API_HOST,
            api_host2=mock_config.PENNSIEVE_API_HOST2,
            api_key=mock_config.PENNSIEVE_API_KEY,
            api_secret=mock_config.PENNSIEVE_API_SECRET,
        )
        mock_auth_class.assert_called_once_with(mock_session_manager)
        mock_auth_class.return_value.authenticate.assert_called_once()

    @patch("importer.AuthenticationClient")
    @patch("importer.ImportClient")
    @patch("importer.WorkflowClient")
    @patch("importer.SessionManager")
    def test_upload_file(self, mock_sm_class, mock_wf_class, mock_import_class, mock_auth_class, mock_config):
        """Should upload a single file using presigned URL."""
        mock_import_client = Mock()
        mock_import_class.return_value = mock_import_client
        mock_import_client.get_presigned_url.return_value = "https://s3.example.com/presigned"

        importer = OmeZarrImporter(mock_config)
        importer._initialize_clients()

        entry = {"uploadKey": "key-1", "_localPath": "/path/to/file"}
        result = importer._upload_file("manifest-123", entry)

        assert result == "key-1"
        mock_import_client.get_presigned_url.assert_called_once_with("manifest-123", "key-1")
        mock_import_client.upload_file.assert_called_once_with("https://s3.example.com/presigned", "/path/to/file")

    @patch("importer.AuthenticationClient")
    @patch("importer.ImportClient")
    @patch("importer.WorkflowClient")
    @patch("importer.SessionManager")
    @patch("importer.ThreadPoolExecutor")
    def test_upload_files_parallel(
        self, mock_executor_class, mock_sm_class, mock_wf_class, mock_import_class, mock_auth_class, mock_config
    ):
        """Should upload files in parallel using thread pool."""
        mock_executor = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Create mock futures
        mock_future1 = Mock()
        mock_future1.result.return_value = "key-1"
        mock_future2 = Mock()
        mock_future2.result.return_value = "key-2"

        entries = [
            {"uploadKey": "key-1", "_localPath": "/path/file1", "targetPath": "sample.zarr/file1"},
            {"uploadKey": "key-2", "_localPath": "/path/file2", "targetPath": "sample.zarr/file2"},
        ]

        mock_executor.submit.side_effect = [mock_future1, mock_future2]

        importer = OmeZarrImporter(mock_config)
        importer._initialize_clients()

        # Mock as_completed to return futures in order
        with patch("importer.as_completed", return_value=[mock_future1, mock_future2]):
            importer._upload_files_parallel("manifest-123", entries)

        assert mock_executor.submit.call_count == 2
        mock_executor_class.assert_called_once_with(max_workers=UPLOAD_WORKERS)

    @patch("importer.AuthenticationClient")
    @patch("importer.ImportClient")
    @patch("importer.WorkflowClient")
    @patch("importer.SessionManager")
    def test_import_zarr(self, mock_sm_class, mock_wf_class, mock_import_class, mock_auth_class, mock_config):
        """Should orchestrate full import workflow."""
        mock_workflow_client = Mock()
        mock_workflow_client.get_provenance_id.return_value = "provenance-123"
        mock_wf_class.return_value = mock_workflow_client

        mock_import_client = Mock()
        mock_import_client.create_manifest_batched.return_value = (
            "manifest-123",
            [{"uploadKey": "key-1", "_localPath": "/path/file1", "targetPath": "sample.zarr/file1"}],
        )
        mock_import_client.get_presigned_url.return_value = "https://s3.example.com/presigned"
        mock_import_class.return_value = mock_import_client

        importer = OmeZarrImporter(mock_config)

        files = [("/path/file1", "file1")]
        result = importer.import_zarr("sample.zarr", files)

        assert result == "manifest-123"
        mock_workflow_client.get_provenance_id.assert_called_once_with(mock_config.INTEGRATION_ID)
        mock_import_client.create_manifest_batched.assert_called_once_with(
            integration_id=mock_config.INTEGRATION_ID,
            files=files,
            zarr_name="sample.zarr",
            asset_type=mock_config.ASSET_TYPE,
            provenance_id="provenance-123",
        )
