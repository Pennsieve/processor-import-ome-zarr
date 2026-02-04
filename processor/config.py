import os


def getboolenv(key: str, default: bool = False) -> bool:
    """Get a boolean value from an environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes")


class Config:
    """Configuration management for OME-Zarr import processor."""

    def __init__(self):
        self.ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
        self.INPUT_DIR = os.getenv("INPUT_DIR")
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR")

        # Pennsieve API configuration
        self.PENNSIEVE_API_HOST = os.getenv("PENNSIEVE_API_HOST", "https://api.pennsieve.net")
        self.PENNSIEVE_API_HOST2 = os.getenv("PENNSIEVE_API_HOST2", "https://api2.pennsieve.net")
        self.PENNSIEVE_API_KEY = os.getenv("PENNSIEVE_API_KEY")
        self.PENNSIEVE_API_SECRET = os.getenv("PENNSIEVE_API_SECRET")

        # Workflow instance identifier (passed to processor to identify the workflow run)
        # Note: Environment variable is INTEGRATION_ID for backwards compatibility
        self.WORKFLOW_INSTANCE_ID = os.getenv("INTEGRATION_ID")

        # Import settings
        self.IMPORTER_ENABLED = getboolenv("IMPORTER_ENABLED", self.ENVIRONMENT != "local")

        # Viewer asset configuration
        self.ASSET_TYPE = os.getenv("ASSET_TYPE", "ome-zarr")
