import logging
import sys

from processor.config import Config
from processor.extractor import OmeZarrExtractor
from processor.importer import OmeZarrImporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def main():
    """Main entry point for the OME-Zarr import processor."""
    log.info("Starting OME-Zarr import processor")

    # Load configuration
    config = Config()

    # Validate required configuration
    if not config.INPUT_DIR:
        raise ValueError("INPUT_DIR environment variable is required")
    if not config.OUTPUT_DIR:
        raise ValueError("OUTPUT_DIR environment variable is required")

    # Extract and process the OME-Zarr archive
    extractor = OmeZarrExtractor(config.INPUT_DIR, config.OUTPUT_DIR)
    _, zarr_name, files = extractor.process()

    log.info(f"Processed OME-Zarr: {zarr_name} with {len(files)} files")

    # Import to Pennsieve if enabled
    if config.IMPORTER_ENABLED:
        if not config.WORKFLOW_INSTANCE_ID:
            raise ValueError("WORKFLOW_INSTANCE_ID is required when importer is enabled")
        has_token_authentication = bool(config.SESSION_TOKEN)
        has_key_authentication = bool(config.PENNSIEVE_API_KEY and config.PENNSIEVE_API_SECRET)
        if not (has_token_authentication or has_key_authentication):
            raise ValueError(
                "authentication is required when importer is enabled: "
                "set SESSION_TOKEN (with optional REFRESH_TOKEN) or PENNSIEVE_API_KEY/PENNSIEVE_API_SECRET"
            )

        importer = OmeZarrImporter(config)
        try:
            asset_id = importer.import_zarr(zarr_name, files)
            log.info(f"Successfully imported OME-Zarr. Asset: {asset_id}")
        except Exception as e:
            log.error(f"Import failed: {e}")
            sys.exit(1)
    else:
        log.info("Importer disabled, skipping Pennsieve upload")

    log.info("OME-Zarr import processor completed successfully")


if __name__ == "__main__":
    main()
