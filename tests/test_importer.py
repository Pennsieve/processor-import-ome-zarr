from unittest.mock import Mock, patch

import pytest

from processor.importer import OmeZarrImporter


def _make_create_response(asset_id="asset-uuid-1"):
    return {
        "asset": {"id": asset_id, "name": "sample.zarr", "asset_type": "ome-zarr", "status": "created"},
        "upload_credentials": {
            "access_key_id": "AKIA-MOCK",
            "secret_access_key": "secret",
            "session_token": "token",
            "expiration": "2030-01-01T00:00:00Z",
            "bucket": "pennsieve-storage",
            "region": "us-east-1",
            "key_prefix": "viewer-assets/O19/D2049/asset-uuid-1/",
        },
    }


class TestOmeZarrImporter:
    def test_initialization(self, mock_config):
        importer = OmeZarrImporter(mock_config)

        assert importer.config == mock_config
        assert importer.session_manager is None
        assert importer.packages_client is None
        assert importer.workflow_client is None

    @patch("processor.importer.AuthenticationClient")
    @patch("processor.importer.PackagesClient")
    @patch("processor.importer.WorkflowClient")
    @patch("processor.importer.SessionManager")
    def test_initialize_clients(self, mock_sm_class, mock_wf_class, mock_pkg_class, mock_auth_class, mock_config):
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
        mock_pkg_class.assert_called_once_with(mock_session_manager)
        mock_wf_class.assert_called_once_with(mock_session_manager)

    @patch("processor.importer.boto3")
    @patch("processor.importer.AuthenticationClient")
    @patch("processor.importer.PackagesClient")
    @patch("processor.importer.WorkflowClient")
    @patch("processor.importer.SessionManager")
    def test_import_zarr_success(
        self,
        mock_sm_class,
        mock_wf_class,
        mock_pkg_class,
        mock_auth_class,
        mock_boto3,
        mock_config,
    ):
        mock_workflow_instance = Mock()
        mock_workflow_instance.dataset_id = "dataset-123"
        mock_workflow_instance.package_ids = ["N:package:pkg-123"]
        mock_workflow_client = Mock()
        mock_workflow_client.get_workflow_instance.return_value = mock_workflow_instance
        mock_wf_class.return_value = mock_workflow_client

        mock_packages_client = Mock()
        mock_packages_client.create_viewer_asset.return_value = _make_create_response("asset-uuid-1")
        mock_pkg_class.return_value = mock_packages_client

        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3

        importer = OmeZarrImporter(mock_config)
        files = [
            ("/abs/sample.zarr/.zattrs", ".zattrs"),
            ("/abs/sample.zarr/0/0/0", "0/0/0"),
        ]
        result = importer.import_zarr("sample.zarr", files)

        assert result == "asset-uuid-1"

        mock_workflow_client.get_workflow_instance.assert_called_once_with(mock_config.WORKFLOW_INSTANCE_ID)

        mock_packages_client.create_viewer_asset.assert_called_once_with(
            dataset_id="dataset-123",
            name="sample.zarr",
            asset_type=mock_config.ASSET_TYPE,
            package_ids=["N:package:pkg-123"],
            properties={},
        )

        mock_boto3.client.assert_called_once_with(
            "s3",
            aws_access_key_id="AKIA-MOCK",
            aws_secret_access_key="secret",
            aws_session_token="token",
            region_name="us-east-1",
        )

        # One upload_file call per input file with the right Bucket / Key
        assert mock_s3.upload_file.call_count == 2
        called_keys = sorted(call.args[2] for call in mock_s3.upload_file.call_args_list)
        assert called_keys == [
            "viewer-assets/O19/D2049/asset-uuid-1/sample.zarr/.zattrs",
            "viewer-assets/O19/D2049/asset-uuid-1/sample.zarr/0/0/0",
        ]
        for call in mock_s3.upload_file.call_args_list:
            assert call.args[1] == "pennsieve-storage"

        mock_packages_client.update_viewer_asset_status.assert_called_once_with("asset-uuid-1", "dataset-123", "ready")
        mock_packages_client.delete_viewer_asset.assert_not_called()

    @patch("processor.importer.boto3")
    @patch("processor.importer.AuthenticationClient")
    @patch("processor.importer.PackagesClient")
    @patch("processor.importer.WorkflowClient")
    @patch("processor.importer.SessionManager")
    def test_import_zarr_normalizes_windows_separators(
        self,
        mock_sm_class,
        mock_wf_class,
        mock_pkg_class,
        mock_auth_class,
        mock_boto3,
        mock_config,
    ):
        mock_workflow_instance = Mock()
        mock_workflow_instance.dataset_id = "dataset-123"
        mock_workflow_instance.package_ids = ["N:package:pkg-123"]
        mock_wf_class.return_value.get_workflow_instance.return_value = mock_workflow_instance

        mock_packages_client = Mock()
        mock_packages_client.create_viewer_asset.return_value = _make_create_response()
        mock_pkg_class.return_value = mock_packages_client

        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3

        importer = OmeZarrImporter(mock_config)
        files = [("C:\\abs\\sample.zarr\\0\\0\\0", "0\\0\\0")]
        importer.import_zarr("sample.zarr", files)

        called_keys = [call.args[2] for call in mock_s3.upload_file.call_args_list]
        assert called_keys == ["viewer-assets/O19/D2049/asset-uuid-1/sample.zarr/0/0/0"]

    @patch("processor.importer.boto3")
    @patch("processor.importer.AuthenticationClient")
    @patch("processor.importer.PackagesClient")
    @patch("processor.importer.WorkflowClient")
    @patch("processor.importer.SessionManager")
    def test_import_zarr_failure_deletes_asset(
        self,
        mock_sm_class,
        mock_wf_class,
        mock_pkg_class,
        mock_auth_class,
        mock_boto3,
        mock_config,
    ):
        mock_workflow_instance = Mock()
        mock_workflow_instance.dataset_id = "dataset-123"
        mock_workflow_instance.package_ids = ["N:package:pkg-123"]
        mock_wf_class.return_value.get_workflow_instance.return_value = mock_workflow_instance

        mock_packages_client = Mock()
        mock_packages_client.create_viewer_asset.return_value = _make_create_response("asset-uuid-1")
        mock_pkg_class.return_value = mock_packages_client

        mock_s3 = Mock()
        mock_s3.upload_file.side_effect = RuntimeError("network exploded")
        mock_boto3.client.return_value = mock_s3

        importer = OmeZarrImporter(mock_config)
        files = [("/abs/sample.zarr/.zattrs", ".zattrs")]

        with pytest.raises(RuntimeError):
            importer.import_zarr("sample.zarr", files)

        mock_packages_client.delete_viewer_asset.assert_called_once_with("asset-uuid-1", "dataset-123")
        mock_packages_client.update_viewer_asset_status.assert_not_called()

    @patch("processor.importer.boto3")
    @patch("processor.importer.AuthenticationClient")
    @patch("processor.importer.PackagesClient")
    @patch("processor.importer.WorkflowClient")
    @patch("processor.importer.SessionManager")
    def test_import_zarr_raises_when_no_package(
        self,
        mock_sm_class,
        mock_wf_class,
        mock_pkg_class,
        mock_auth_class,
        mock_boto3,
        mock_config,
    ):
        mock_workflow_instance = Mock()
        mock_workflow_instance.dataset_id = "dataset-123"
        mock_workflow_instance.package_ids = []
        mock_wf_class.return_value.get_workflow_instance.return_value = mock_workflow_instance

        importer = OmeZarrImporter(mock_config)

        with pytest.raises(ValueError, match="No package ID"):
            importer.import_zarr("sample.zarr", [("/p", "p")])

        mock_pkg_class.return_value.create_viewer_asset.assert_not_called()
