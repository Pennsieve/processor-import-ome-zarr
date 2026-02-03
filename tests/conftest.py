import os
import sys
from unittest.mock import Mock

import pytest

# Add processor to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processor"))


@pytest.fixture
def mock_session_manager():
    """Create a mock session manager."""
    manager = Mock()
    manager.session_token = "mock-token-12345"
    manager.api_host = "https://api.pennsieve.net"
    manager.api_host2 = "https://api2.pennsieve.net"
    manager.api_key = "mock-api-key"
    manager.api_secret = "mock-api-secret"
    manager.refresh_session = Mock()
    return manager


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    config = Mock()
    config.ENVIRONMENT = "local"
    config.INPUT_DIR = "/data/input"
    config.OUTPUT_DIR = "/data/output"
    config.PENNSIEVE_API_HOST = "https://api.pennsieve.net"
    config.PENNSIEVE_API_HOST2 = "https://api2.pennsieve.net"
    config.PENNSIEVE_API_KEY = "mock-api-key"
    config.PENNSIEVE_API_SECRET = "mock-api-secret"
    config.INTEGRATION_ID = "mock-integration-id"
    config.IMPORTER_ENABLED = True
    config.ASSET_TYPE = "ome-zarr"
    return config


@pytest.fixture
def sample_zarr_files():
    """Create sample OME-Zarr file list."""
    return [
        ("/data/output/extracted/sample.zarr/.zattrs", ".zattrs"),
        ("/data/output/extracted/sample.zarr/.zgroup", ".zgroup"),
        ("/data/output/extracted/sample.zarr/0/.zarray", "0/.zarray"),
        ("/data/output/extracted/sample.zarr/0/0/0/0", "0/0/0/0"),
        ("/data/output/extracted/sample.zarr/0/0/0/1", "0/0/0/1"),
    ]


@pytest.fixture
def temp_zarr_directory(tmp_path):
    """Create a temporary OME-Zarr directory structure."""
    zarr_dir = tmp_path / "sample.zarr"
    zarr_dir.mkdir()

    # Create zarr metadata files
    (zarr_dir / ".zattrs").write_text('{"multiscales": []}')
    (zarr_dir / ".zgroup").write_text('{"zarr_format": 2}')

    # Create a resolution level
    level_dir = zarr_dir / "0"
    level_dir.mkdir()
    (level_dir / ".zarray").write_text('{"chunks": [1, 1, 256, 256]}')

    # Create some chunk files
    chunk_dir = level_dir / "0" / "0"
    chunk_dir.mkdir(parents=True)
    (chunk_dir / "0").write_bytes(b"\x00" * 100)
    (chunk_dir / "1").write_bytes(b"\x00" * 100)

    return zarr_dir
