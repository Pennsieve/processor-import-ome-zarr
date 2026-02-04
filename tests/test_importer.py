from unittest.mock import Mock, patch

from processor.importer import OmeZarrImporter


class TestOmeZarrImporter:
    """Tests for OmeZarrImporter class."""

    def test_initialization(self, mock_config):
        """Should initialize with config."""
        importer = OmeZarrImporter(mock_config)

        assert importer.config == mock_config
        assert importer.session_manager is None
        assert importer.import_client is None
        assert importer.workflow_client is None

    @patch("processor.importer.AuthenticationClient")
    @patch("processor.importer.ImportClient")
    @patch("processor.importer.WorkflowClient")
    @patch("processor.importer.SessionManager")
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
        mock_import_class.assert_called_once_with(mock_session_manager)
        mock_wf_class.assert_called_once_with(mock_session_manager)

    @patch("processor.importer.AuthenticationClient")
    @patch("processor.importer.ImportClient")
    @patch("processor.importer.WorkflowClient")
    @patch("processor.importer.SessionManager")
    @patch("processor.importer.prepare_import_files")
    def test_import_zarr(
        self,
        mock_prepare,
        mock_sm_class,
        mock_wf_class,
        mock_import_class,
        mock_auth_class,
        mock_config,
    ):
        """Should orchestrate full import workflow."""
        # Setup workflow client mock
        mock_workflow_instance = Mock()
        mock_workflow_instance.dataset_id = "dataset-123"
        mock_workflow_client = Mock()
        mock_workflow_client.get_workflow_instance.return_value = mock_workflow_instance
        mock_wf_class.return_value = mock_workflow_client

        # Setup import client mock
        mock_import_client = Mock()
        mock_import_client.create_batched.return_value = "import-123"
        mock_import_client.get_presign_url.return_value = "https://s3.example.com/presigned"
        mock_import_client.upload_file.return_value = None
        mock_import_class.return_value = mock_import_client

        # Setup prepare_import_files mock
        mock_import_file = Mock()
        mock_import_file.upload_key = "upload-key-1"
        mock_import_file.local_path = "/path/file1"
        mock_prepare.return_value = [mock_import_file]

        importer = OmeZarrImporter(mock_config)

        files = [("/path/file1", "file1")]
        result = importer.import_zarr("sample.zarr", files)

        assert result == "import-123"

        # Verify workflow instance was fetched
        mock_workflow_client.get_workflow_instance.assert_called_once_with(mock_config.WORKFLOW_INSTANCE_ID)

        # Verify prepare_import_files was called
        mock_prepare.assert_called_once_with(files, "sample.zarr")

        # Verify create_batched was called with correct options
        mock_import_client.create_batched.assert_called_once()
        call_kwargs = mock_import_client.create_batched.call_args[1]
        assert call_kwargs["integration_id"] == mock_config.WORKFLOW_INSTANCE_ID
        assert call_kwargs["dataset_id"] == "dataset-123"
        assert call_kwargs["options"]["asset_type"] == mock_config.ASSET_TYPE
        assert call_kwargs["options"]["asset_name"] == "sample.zarr"

        # Verify file upload flow: presign URL was fetched and upload_file was called
        mock_import_client.get_presign_url.assert_called_once_with("import-123", "dataset-123", "upload-key-1")
        mock_import_client.upload_file.assert_called_once_with("https://s3.example.com/presigned", "/path/file1")
