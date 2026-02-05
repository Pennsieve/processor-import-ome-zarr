import logging
import os

from processor.utils import (
    collect_files,
    extract_tar,
    extract_zip,
    find_zarr_root,
    get_archive_type,
    strip_archive_extension,
)

log = logging.getLogger(__name__)


class OmeZarrExtractor:
    """Extracts and processes OME-Zarr archives."""

    def __init__(self, input_dir: str, output_dir: str):
        """
        Initialize the extractor.

        Args:
            input_dir: Directory containing input archive files
            output_dir: Directory for extracted output
        """
        self.input_dir = input_dir
        self.output_dir = output_dir

    def find_input_file(self) -> str:
        """
        Find the input archive file in the input directory.

        Supports: .zip, .tar, .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.xz, .txz

        Returns:
            Path to the archive file

        Raises:
            FileNotFoundError: If no archive file is found
            ValueError: If multiple archive files are found
        """
        archive_files = [f for f in os.listdir(self.input_dir) if get_archive_type(f) is not None]
        if len(archive_files) == 0:
            raise FileNotFoundError("Expected exactly one archive file, found 0")
        if len(archive_files) > 1:
            raise ValueError(f"Expected exactly one archive file, found {len(archive_files)}")
        return os.path.join(self.input_dir, archive_files[0])

    def extract(self, archive_path: str) -> tuple[str, str]:
        """
        Extract an archive file and locate the OME-Zarr root.

        Extracts to a directory named after the archive file (minus extension),
        so the directory name becomes the zarr/asset name.

        Args:
            archive_path: Path to the archive file

        Returns:
            Tuple of (zarr_root_path, zarr_name)

        Raises:
            ValueError: If no valid OME-Zarr directory is found or unsupported format
        """
        log.info(f"Extracting archive: {archive_path}")

        # Derive zarr name from archive filename (strip archive extension)
        archive_basename = os.path.basename(archive_path)
        zarr_name = strip_archive_extension(archive_basename)

        # Extract to a directory named after the zarr
        extraction_dir = os.path.join(self.output_dir, zarr_name)
        os.makedirs(extraction_dir, exist_ok=True)

        # Choose extraction function based on archive type
        archive_type = get_archive_type(archive_basename)
        if archive_type == ".zip":
            extract_zip(archive_path, extraction_dir)
        elif archive_type in (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz"):
            extract_tar(archive_path, extraction_dir)
        else:
            raise ValueError(f"Unsupported archive format: {archive_basename}")

        # Find the OME-Zarr root
        zarr_root = find_zarr_root(extraction_dir)
        if zarr_root is None:
            raise ValueError("No valid OME-Zarr directory found in archive")

        log.info(f"Found OME-Zarr root: {zarr_root}")

        # If the archive contained a nested folder, use that folder's name instead
        if zarr_root != extraction_dir:
            zarr_name = os.path.basename(zarr_root)

        return zarr_root, zarr_name

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
        archive_path = self.find_input_file()
        zarr_root, zarr_name = self.extract(archive_path)
        files = self.collect_zarr_files(zarr_root)

        return zarr_root, zarr_name, files
