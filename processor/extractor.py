import logging
import os

from utils import collect_files, extract_zip, find_zarr_root

log = logging.getLogger(__name__)


class OmeZarrExtractor:
    """Extracts and processes zipped OME-Zarr archives."""

    def __init__(self, input_dir: str, output_dir: str):
        """
        Initialize the extractor.

        Args:
            input_dir: Directory containing input ZIP files
            output_dir: Directory for extracted output
        """
        self.input_dir = input_dir
        self.output_dir = output_dir

    def find_input_file(self) -> str:
        """
        Find the input ZIP file in the input directory.

        Returns:
            Path to the ZIP file

        Raises:
            AssertionError: If not exactly one ZIP file is found
        """
        zip_files = [f for f in os.listdir(self.input_dir) if f.endswith(".zip")]
        assert len(zip_files) == 1, f"Expected exactly one ZIP file, found {len(zip_files)}"
        return os.path.join(self.input_dir, zip_files[0])

    def extract(self, zip_path: str) -> str:
        """
        Extract a ZIP file and locate the OME-Zarr root.

        Args:
            zip_path: Path to the ZIP file

        Returns:
            Path to the OME-Zarr root directory

        Raises:
            AssertionError: If no valid OME-Zarr directory is found
        """
        log.info(f"Extracting ZIP file: {zip_path}")

        # Extract to output directory
        extraction_dir = os.path.join(self.output_dir, "extracted")
        os.makedirs(extraction_dir, exist_ok=True)
        extract_zip(zip_path, extraction_dir)

        # Find the OME-Zarr root
        zarr_root = find_zarr_root(extraction_dir)
        assert zarr_root is not None, "No valid OME-Zarr directory found in archive"

        log.info(f"Found OME-Zarr root: {zarr_root}")
        return zarr_root

    def get_zarr_name(self, zarr_root: str) -> str:
        """
        Get the name of the OME-Zarr directory.

        Args:
            zarr_root: Path to the OME-Zarr root directory

        Returns:
            Name of the OME-Zarr directory (e.g., 'sample.zarr')
        """
        return os.path.basename(zarr_root)

    def collect_zarr_files(self, zarr_root: str) -> list[tuple[str, str]]:
        """
        Collect all files within the OME-Zarr directory.

        Args:
            zarr_root: Path to the OME-Zarr root directory

        Returns:
            List of tuples (absolute_path, relative_path within zarr)
        """
        files = collect_files(zarr_root)
        log.info(f"Collected {len(files)} files from OME-Zarr directory")
        return files

    def process(self) -> tuple[str, str, list[tuple[str, str]]]:
        """
        Main processing method: find input, extract, and collect files.

        Returns:
            Tuple of (zarr_root_path, zarr_name, list of (abs_path, rel_path) tuples)
        """
        zip_path = self.find_input_file()
        zarr_root = self.extract(zip_path)
        zarr_name = self.get_zarr_name(zarr_root)
        files = self.collect_zarr_files(zarr_root)

        return zarr_root, zarr_name, files
