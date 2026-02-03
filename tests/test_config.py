import os
from unittest.mock import patch

from config import Config, getboolenv


class TestGetBoolEnv:
    """Tests for getboolenv function."""

    def test_returns_default_when_not_set(self):
        """Should return default when environment variable is not set."""
        with patch.dict(os.environ, {}, clear=True):
            assert getboolenv("NONEXISTENT_VAR", False) is False
            assert getboolenv("NONEXISTENT_VAR", True) is True

    def test_true_values(self):
        """Should return True for true-like string values."""
        for value in ["true", "True", "TRUE", "1", "yes", "Yes", "YES"]:
            with patch.dict(os.environ, {"TEST_VAR": value}):
                assert getboolenv("TEST_VAR") is True

    def test_false_values(self):
        """Should return False for non-true string values."""
        for value in ["false", "False", "0", "no", "anything"]:
            with patch.dict(os.environ, {"TEST_VAR": value}):
                assert getboolenv("TEST_VAR") is False


class TestConfig:
    """Tests for Config class."""

    def test_default_values(self):
        """Should use default values when environment variables not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()
            assert config.ENVIRONMENT == "local"
            assert config.PENNSIEVE_API_HOST == "https://api.pennsieve.net"
            assert config.PENNSIEVE_API_HOST2 == "https://api2.pennsieve.net"
            assert config.ASSET_TYPE == "ome-zarr"

    def test_reads_environment_variables(self):
        """Should read values from environment variables."""
        env = {
            "ENVIRONMENT": "production",
            "INPUT_DIR": "/custom/input",
            "OUTPUT_DIR": "/custom/output",
            "PENNSIEVE_API_KEY": "test-key",
            "PENNSIEVE_API_SECRET": "test-secret",
            "INTEGRATION_ID": "test-integration",
            "ASSET_TYPE": "custom-type",
        }
        with patch.dict(os.environ, env, clear=True):
            config = Config()
            assert config.ENVIRONMENT == "production"
            assert config.INPUT_DIR == "/custom/input"
            assert config.OUTPUT_DIR == "/custom/output"
            assert config.PENNSIEVE_API_KEY == "test-key"
            assert config.PENNSIEVE_API_SECRET == "test-secret"
            assert config.INTEGRATION_ID == "test-integration"
            assert config.ASSET_TYPE == "custom-type"

    def test_importer_enabled_default_local(self):
        """Should disable importer by default in local environment."""
        with patch.dict(os.environ, {"ENVIRONMENT": "local"}, clear=True):
            config = Config()
            assert config.IMPORTER_ENABLED is False

    def test_importer_enabled_default_production(self):
        """Should enable importer by default in non-local environment."""
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=True):
            config = Config()
            assert config.IMPORTER_ENABLED is True

    def test_importer_enabled_override(self):
        """Should allow explicit override of importer enabled setting."""
        with patch.dict(os.environ, {"ENVIRONMENT": "local", "IMPORTER_ENABLED": "true"}, clear=True):
            config = Config()
            assert config.IMPORTER_ENABLED is True
