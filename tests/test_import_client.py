import responses
from clients.import_client import MAX_MANIFEST_FILES, ImportClient


class TestImportClient:
    """Tests for ImportClient class."""

    def test_initialization(self, mock_session_manager):
        """Should initialize with correct base URL."""
        client = ImportClient(mock_session_manager)
        assert client.base_url == "https://api2.pennsieve.net/import"

    @responses.activate
    def test_create_manifest(self, mock_session_manager):
        """Should create import manifest with asset_name option."""
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import",
            json={
                "id": "manifest-123",
                "files": [{"uploadKey": "key-1"}, {"uploadKey": "key-2"}],
            },
            status=201,
        )

        client = ImportClient(mock_session_manager)
        result = client.create_manifest(
            integration_id="integration-123",
            files=[
                {"targetPath": "sample.zarr/.zattrs", "targetName": ".zattrs"},
                {"targetPath": "sample.zarr/.zgroup", "targetName": ".zgroup"},
            ],
            asset_type="ome-zarr",
            asset_name="sample.zarr",
            provenance_id="provenance-123",
        )

        assert result["id"] == "manifest-123"
        assert len(result["files"]) == 2

        # Verify request body
        request = responses.calls[0].request
        import json

        body = json.loads(request.body)
        assert body["integrationId"] == "integration-123"
        assert body["importType"] == "viewerassets"
        assert body["options"]["asset_type"] == "ome-zarr"
        assert body["options"]["asset_name"] == "sample.zarr"
        assert body["options"]["provenance_id"] == "provenance-123"

    @responses.activate
    def test_append_files(self, mock_session_manager):
        """Should append files to existing manifest."""
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import/manifest-123/files",
            json={"files": [{"uploadKey": "key-3"}]},
            status=200,
        )

        client = ImportClient(mock_session_manager)
        result = client.append_files(
            manifest_id="manifest-123",
            files=[{"targetPath": "sample.zarr/0/.zarray", "targetName": ".zarray"}],
        )

        assert len(result["files"]) == 1

    @responses.activate
    def test_get_presigned_url(self, mock_session_manager):
        """Should get presigned URL for file upload."""
        responses.add(
            responses.GET,
            "https://api2.pennsieve.net/import/manifest-123/upload/key-1/presign",
            json={"url": "https://s3.amazonaws.com/bucket/key?signature=xxx"},
            status=200,
        )

        client = ImportClient(mock_session_manager)
        result = client.get_presigned_url("manifest-123", "key-1")

        assert result == "https://s3.amazonaws.com/bucket/key?signature=xxx"

    @responses.activate
    def test_upload_file(self, mock_session_manager, tmp_path):
        """Should upload file to presigned URL."""
        responses.add(
            responses.PUT,
            "https://s3.amazonaws.com/bucket/key",
            status=200,
        )

        # Create test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        client = ImportClient(mock_session_manager)
        client.upload_file("https://s3.amazonaws.com/bucket/key", str(test_file))

        assert len(responses.calls) == 1
        assert responses.calls[0].request.body == b"test content"

    def test_prepare_file_entries(self, mock_session_manager, sample_zarr_files):
        """Should prepare file entries with correct paths."""
        client = ImportClient(mock_session_manager)
        entries = client.prepare_file_entries(sample_zarr_files, "sample.zarr")

        assert len(entries) == 5

        # Check first entry
        assert entries[0]["targetPath"] == "sample.zarr/.zattrs"
        assert entries[0]["targetName"] == ".zattrs"
        assert entries[0]["fileExtension"] == ""  # Hidden files have no extension
        assert entries[0]["_localPath"] == sample_zarr_files[0][0]

    @responses.activate
    def test_create_manifest_batched_single_batch(self, mock_session_manager):
        """Should create manifest without batching for small file lists."""
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import",
            json={
                "id": "manifest-123",
                "files": [
                    {"uploadKey": "key-0"},
                    {"uploadKey": "key-1"},
                ],
            },
            status=201,
        )

        client = ImportClient(mock_session_manager)
        files = [
            ("/path/to/.zattrs", ".zattrs"),
            ("/path/to/.zgroup", ".zgroup"),
        ]

        manifest_id, entries = client.create_manifest_batched(
            integration_id="integration-123",
            files=files,
            zarr_name="sample.zarr",
            asset_type="ome-zarr",
            provenance_id="provenance-123",
        )

        assert manifest_id == "manifest-123"
        assert len(entries) == 2
        assert entries[0]["uploadKey"] == "key-0"
        assert entries[1]["uploadKey"] == "key-1"

    @responses.activate
    def test_create_manifest_batched_multiple_batches(self, mock_session_manager):
        """Should batch manifest creation for large file lists."""
        # Create more files than MAX_MANIFEST_FILES
        num_files = MAX_MANIFEST_FILES + 5
        files = [(f"/path/to/file{i}", f"file{i}") for i in range(num_files)]

        # Response for initial create
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import",
            json={
                "id": "manifest-123",
                "files": [{"uploadKey": f"key-{i}"} for i in range(MAX_MANIFEST_FILES)],
            },
            status=201,
        )

        # Response for append
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import/manifest-123/files",
            json={
                "files": [{"uploadKey": f"key-{MAX_MANIFEST_FILES + i}"} for i in range(5)],
            },
            status=200,
        )

        client = ImportClient(mock_session_manager)
        manifest_id, entries = client.create_manifest_batched(
            integration_id="integration-123",
            files=files,
            zarr_name="sample.zarr",
            asset_type="ome-zarr",
            provenance_id="provenance-123",
        )

        assert manifest_id == "manifest-123"
        assert len(entries) == num_files
        assert len(responses.calls) == 2  # create + append
