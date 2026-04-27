import json

import responses

from processor.clients.packages_client import PackagesClient


class TestPackagesClient:
    def test_initialization(self, mock_session_manager):
        client = PackagesClient(mock_session_manager)
        assert client.base_url == "https://api2.pennsieve.net/packages"

    @responses.activate
    def test_create_viewer_asset(self, mock_session_manager):
        expected_response = {
            "asset": {
                "id": "asset-uuid-1",
                "name": "sample.zarr",
                "asset_type": "ome-zarr",
                "status": "created",
            },
            "upload_credentials": {
                "access_key_id": "AKIA",
                "secret_access_key": "secret",
                "session_token": "token",
                "expiration": "2030-01-01T00:00:00Z",
                "bucket": "pennsieve-storage",
                "region": "us-east-1",
                "key_prefix": "viewer-assets/O19/D2049/asset-uuid-1/",
            },
        }
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/packages/assets?dataset_id=dataset-123",
            json=expected_response,
            status=201,
        )

        client = PackagesClient(mock_session_manager)
        result = client.create_viewer_asset(
            dataset_id="dataset-123",
            name="sample.zarr",
            asset_type="ome-zarr",
            package_ids=["N:package:pkg-1"],
            properties={"foo": "bar"},
        )

        assert result == expected_response

        request = responses.calls[0].request
        body = json.loads(request.body)
        assert body == {
            "name": "sample.zarr",
            "asset_type": "ome-zarr",
            "package_ids": ["N:package:pkg-1"],
            "properties": {"foo": "bar"},
        }
        assert request.headers["Authorization"] == "Bearer mock-token-12345"
        assert request.headers["Content-Type"] == "application/json"

    @responses.activate
    def test_create_viewer_asset_defaults_properties_to_empty(self, mock_session_manager):
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/packages/assets?dataset_id=dataset-123",
            json={"asset": {"id": "x"}, "upload_credentials": {}},
            status=201,
        )

        client = PackagesClient(mock_session_manager)
        client.create_viewer_asset(
            dataset_id="dataset-123",
            name="n",
            asset_type="ome-zarr",
            package_ids=["p"],
        )

        body = json.loads(responses.calls[0].request.body)
        assert body["properties"] == {}

    @responses.activate
    def test_update_viewer_asset_status(self, mock_session_manager):
        responses.add(
            responses.PATCH,
            "https://api2.pennsieve.net/packages/assets/asset-uuid-1?dataset_id=dataset-123",
            json={"id": "asset-uuid-1", "status": "ready"},
            status=200,
        )

        client = PackagesClient(mock_session_manager)
        result = client.update_viewer_asset_status("asset-uuid-1", "dataset-123", "ready")

        assert result == {"id": "asset-uuid-1", "status": "ready"}
        body = json.loads(responses.calls[0].request.body)
        assert body == {"status": "ready"}

    @responses.activate
    def test_delete_viewer_asset(self, mock_session_manager):
        responses.add(
            responses.DELETE,
            "https://api2.pennsieve.net/packages/assets/asset-uuid-1?dataset_id=dataset-123",
            status=204,
        )

        client = PackagesClient(mock_session_manager)
        result = client.delete_viewer_asset("asset-uuid-1", "dataset-123")

        assert result is None
        assert len(responses.calls) == 1

    @responses.activate
    def test_create_viewer_asset_refreshes_session_on_401(self, mock_session_manager):
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/packages/assets?dataset_id=dataset-123",
            json={"message": "unauthorized"},
            status=401,
        )
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/packages/assets?dataset_id=dataset-123",
            json={"asset": {"id": "asset-uuid-1"}, "upload_credentials": {}},
            status=201,
        )

        client = PackagesClient(mock_session_manager)
        result = client.create_viewer_asset(
            dataset_id="dataset-123",
            name="sample.zarr",
            asset_type="ome-zarr",
            package_ids=["N:package:pkg-1"],
        )

        assert result["asset"]["id"] == "asset-uuid-1"
        mock_session_manager.refresh_session.assert_called_once()
        assert len(responses.calls) == 2
