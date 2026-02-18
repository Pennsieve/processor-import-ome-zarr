import uuid

import responses

from processor.clients.import_client import (
    DEFAULT_BATCH_SIZE,
    ImportClient,
    ImportFile,
    calculate_batch_size,
    prepare_import_files,
)


class TestImportFile:
    """Tests for ImportFile class."""

    def test_initialization(self):
        """Should store provided values."""
        upload_key = uuid.uuid4()
        import_file = ImportFile(
            upload_key=upload_key,
            file_path="sample.zarr/.zattrs",
            local_path="/data/sample.zarr/.zattrs",
        )

        assert import_file.upload_key == upload_key
        assert import_file.file_path == "sample.zarr/.zattrs"
        assert import_file.local_path == "/data/sample.zarr/.zattrs"

    def test_repr(self):
        """Should have useful repr."""
        upload_key = uuid.uuid4()
        import_file = ImportFile(upload_key, "file.txt", "/path/file.txt")
        repr_str = repr(import_file)

        assert "ImportFile" in repr_str
        assert "file.txt" in repr_str


class TestPrepareImportFiles:
    """Tests for prepare_import_files function."""

    def test_creates_import_files_with_zarr_prefix(self):
        """Should create ImportFile objects with zarr_name prefix in file_path."""
        files = [
            ("/data/sample.zarr/.zattrs", ".zattrs"),
            ("/data/sample.zarr/.zgroup", ".zgroup"),
            ("/data/sample.zarr/0/0/0", "0/0/0"),
        ]

        import_files = prepare_import_files(files, "sample.zarr")

        assert len(import_files) == 3
        assert import_files[0].file_path == "sample.zarr/.zattrs"
        assert import_files[0].local_path == "/data/sample.zarr/.zattrs"
        assert import_files[1].file_path == "sample.zarr/.zgroup"
        assert import_files[2].file_path == "sample.zarr/0/0/0"

        # Each should have a unique upload_key
        upload_keys = [f.upload_key for f in import_files]
        assert len(set(upload_keys)) == 3

    def test_generates_unique_upload_keys(self):
        """Should generate unique UUID upload keys for each file."""
        files = [("/path/a", "a"), ("/path/b", "b")]

        import_files = prepare_import_files(files, "test.zarr")

        assert import_files[0].upload_key != import_files[1].upload_key
        assert isinstance(import_files[0].upload_key, uuid.UUID)


class TestCalculateBatchSize:
    """Tests for calculate_batch_size function."""

    def test_returns_default_for_empty_list(self):
        """Should return default batch size for empty list."""
        assert calculate_batch_size([]) == DEFAULT_BATCH_SIZE

    def test_calculates_based_on_file_size(self):
        """Should calculate batch size based on payload size."""
        import_files = [ImportFile(uuid.uuid4(), f"sample.zarr/file{i}.txt", f"/path/file{i}.txt") for i in range(100)]

        batch_size = calculate_batch_size(import_files)

        # Should be a reasonable number based on 1MB limit
        assert batch_size > 0
        assert batch_size < 50000  # Sanity check


class TestImportClient:
    """Tests for ImportClient class."""

    def test_initialization(self, mock_session_manager):
        """Should initialize with correct base URL."""
        client = ImportClient(mock_session_manager)
        assert client.base_url == "https://api2.pennsieve.net/import"

    @responses.activate
    def test_create(self, mock_session_manager):
        """Should create import manifest with correct payload."""
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import?dataset_id=dataset-123",
            json={"id": "import-123"},
            status=201,
        )

        client = ImportClient(mock_session_manager)
        import_files = [
            ImportFile(uuid.UUID("11111111-1111-1111-1111-111111111111"), "sample.zarr/.zattrs", "/path/.zattrs"),
        ]
        options = {
            "asset_type": "ome-zarr",
            "asset_name": "sample.zarr",
            "properties": {},
            "provenance_id": "integration-123",
        }

        result = client.create("integration-123", "dataset-123", "N:package:pkg-123", import_files, options)

        assert result == "import-123"

        # Verify request body
        import json

        request = responses.calls[0].request
        body = json.loads(request.body)
        assert body["integration_id"] == "integration-123"
        assert body["package_id"] == "N:package:pkg-123"
        assert body["import_type"] == "viewerassets"
        assert body["files"][0]["upload_key"] == "11111111-1111-1111-1111-111111111111"
        assert body["files"][0]["file_path"] == "sample.zarr/.zattrs"
        assert body["options"]["asset_name"] == "sample.zarr"

    @responses.activate
    def test_append_files(self, mock_session_manager):
        """Should append files to existing manifest."""
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import/import-123/files?dataset_id=dataset-123",
            json={},
            status=200,
        )

        client = ImportClient(mock_session_manager)
        import_files = [
            ImportFile(uuid.UUID("22222222-2222-2222-2222-222222222222"), "sample.zarr/0/0", "/path/0/0"),
        ]

        client.append_files("import-123", "dataset-123", import_files)

        assert len(responses.calls) == 1

    @responses.activate
    def test_get_presign_url(self, mock_session_manager):
        """Should get presigned URL for file upload."""
        responses.add(
            responses.GET,
            "https://api2.pennsieve.net/import/import-123/upload/upload-key-1/presign?dataset_id=dataset-123",
            json={"url": "https://s3.amazonaws.com/bucket/key?signature=xxx"},
            status=200,
        )

        client = ImportClient(mock_session_manager)
        result = client.get_presign_url("import-123", "dataset-123", "upload-key-1")

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

    @responses.activate
    def test_create_batched_single_batch(self, mock_session_manager):
        """Should create manifest without batching for small file lists."""
        responses.add(
            responses.POST,
            "https://api2.pennsieve.net/import?dataset_id=dataset-123",
            json={"id": "import-123"},
            status=201,
        )

        client = ImportClient(mock_session_manager)
        import_files = [
            ImportFile(uuid.uuid4(), "sample.zarr/.zattrs", "/path/.zattrs"),
            ImportFile(uuid.uuid4(), "sample.zarr/.zgroup", "/path/.zgroup"),
        ]
        options = {"asset_type": "ome-zarr", "properties": {}, "provenance_id": "integration-123"}

        import_id = client.create_batched("integration-123", "dataset-123", "N:package:pkg-123", import_files, options)

        assert import_id == "import-123"
        assert len(responses.calls) == 1  # Only one create call, no appends
