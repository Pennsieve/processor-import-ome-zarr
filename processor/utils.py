import os
import zipfile


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
