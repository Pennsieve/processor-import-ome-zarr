import os
import tarfile
import zipfile

# Supported archive extensions (order matters - longer extensions first)
SUPPORTED_EXTENSIONS = [
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".tar",
    ".zip",
]


def get_archive_type(filename: str) -> str | None:
    """
    Detect archive type from filename extension.

    Args:
        filename: Name of the file to check

    Returns:
        The matching extension (e.g., '.tar.gz', '.zip') or None if unsupported
    """
    lower = filename.lower()
    for ext in SUPPORTED_EXTENSIONS:
        if lower.endswith(ext):
            return ext
    return None


def strip_archive_extension(filename: str) -> str:
    """
    Strip archive extension(s) to get base name.

    Args:
        filename: Name of the archive file

    Returns:
        Filename without the archive extension

    Examples:
        sample.zarr.tar.gz -> sample.zarr
        data.zarr.zip -> data.zarr
    """
    lower = filename.lower()
    for ext in SUPPORTED_EXTENSIONS:
        if lower.endswith(ext):
            return filename[: -len(ext)]
    return filename


def find_zarr_root(extracted_dir: str) -> str | None:
    """
    Find the root OME-Zarr directory within an extracted archive.

    OME-Zarr directories are identified by the presence of .zattrs or .zgroup
    at the root level.

    Args:
        extracted_dir: Path to the directory containing extracted files

    Returns:
        Path to the OME-Zarr root directory, or None if not found
    """
    # Check if extracted_dir itself is a zarr root (has .zgroup or .zattrs)
    if _is_zarr_root(extracted_dir):
        return extracted_dir

    # Check immediate children for zarr roots
    for entry in os.listdir(extracted_dir):
        entry_path = os.path.join(extracted_dir, entry)
        if os.path.isdir(entry_path) and _is_zarr_root(entry_path):
            return entry_path

    return None


def _is_zarr_root(path: str) -> bool:
    """
    Check if a directory is a zarr root (group with .zgroup or .zattrs).

    This is more specific than is_zarr_directory - it requires .zgroup or .zattrs,
    not just .zarray, to avoid matching resolution level sub-arrays.

    Args:
        path: Path to check

    Returns:
        True if the path is a zarr root directory
    """
    if not os.path.isdir(path):
        return False

    zattrs_path = os.path.join(path, ".zattrs")
    zgroup_path = os.path.join(path, ".zgroup")

    return os.path.exists(zattrs_path) or os.path.exists(zgroup_path)


def is_zarr_directory(path: str) -> bool:
    """
    Check if a directory is a valid Zarr directory.

    A Zarr directory must contain .zattrs, .zgroup, or .zarray file at its root.
    - .zgroup: indicates a zarr group (hierarchical container)
    - .zarray: indicates a zarr array (data container)
    - .zattrs: indicates zarr attributes (often present with OME metadata)

    Args:
        path: Path to check

    Returns:
        True if the path is a valid Zarr directory
    """
    if not os.path.isdir(path):
        return False

    zattrs_path = os.path.join(path, ".zattrs")
    zgroup_path = os.path.join(path, ".zgroup")
    zarray_path = os.path.join(path, ".zarray")

    return os.path.exists(zattrs_path) or os.path.exists(zgroup_path) or os.path.exists(zarray_path)


def extract_zip(zip_path: str, output_dir: str) -> str:
    """
    Extract a ZIP file to the specified directory.

    Args:
        zip_path: Path to the ZIP file
        output_dir: Directory to extract files to

    Returns:
        Path to the extraction directory
    """
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)

    return output_dir


def extract_tar(tar_path: str, output_dir: str) -> str:
    """
    Extract a tar archive to the specified directory.

    Handles .tar, .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.xz, .txz formats
    automatically via tarfile's 'r:*' mode.

    Args:
        tar_path: Path to the tar archive
        output_dir: Directory to extract files to

    Returns:
        Path to the extraction directory
    """
    with tarfile.open(tar_path, "r:*") as tar:
        tar.extractall(output_dir)

    return output_dir


def collect_files(directory: str) -> list[tuple[str, str]]:
    """
    Recursively collect all files in a directory.

    Args:
        directory: Root directory to collect files from

    Returns:
        List of tuples (absolute_path, relative_path)
    """
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            abs_path = os.path.join(root, filename)
            rel_path = os.path.relpath(abs_path, directory)
            files.append((abs_path, rel_path))

    return files
