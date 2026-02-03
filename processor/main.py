import logging
import sys

from config import Config
from extractor import OmeZarrExtractor
from importer import OmeZarrImporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()


def main():
    """Main entry point for the OME-Zarr import processor."""
    log.info("Starting OME-Zarr import processor")

    # Load configuration
    config = Config()

    # Validate required configuration
    assert config.INPUT_DIR, "INPUT_DIR environment variable is required"
    assert config.OUTPUT_DIR, "OUTPUT_DIR environment variable is required"

    # Extract and process the OME-Zarr archive
    extractor = OmeZarrExtractor(config.INPUT_DIR, config.OUTPUT_DIR)
    zarr_root, zarr_name, files = extractor.process()

    log.info(f"Processed OME-Zarr: {zarr_name} with {len(files)} files")

    # Import to Pennsieve if enabled
    if config.IMPORTER_ENABLED:
        assert config.INTEGRATION_ID, "INTEGRATION_ID is required when importer is enabled"
        assert config.PENNSIEVE_API_KEY, "PENNSIEVE_API_KEY is required when importer is enabled"
        assert config.PENNSIEVE_API_SECRET, "PENNSIEVE_API_SECRET is required when importer is enabled"

        importer = OmeZarrImporter(config)
        try:
            manifest_id = importer.import_zarr(zarr_name, files)
            log.info(f"Successfully imported OME-Zarr. Manifest: {manifest_id}")
        except Exception as e:
            log.error(f"Import failed: {e}")
            # Mark workflow as failed if we have a workflow client
            if importer.workflow_client:
                try:
                    importer.workflow_client.fail_integration(config.INTEGRATION_ID, str(e))
                except Exception as fail_error:
                    log.error(f"Failed to mark integration as failed: {fail_error}")
            sys.exit(1)
    else:
        log.info("Importer disabled, skipping Pennsieve upload")

    log.info("OME-Zarr import processor completed successfully")


if __name__ == "__main__":
    main()
