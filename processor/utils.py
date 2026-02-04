import os
import zipfile


def find_zarr_root(extracted_dir: str) -> str | None:
    """
    Find the root OME-Zarr directory within an extracted archive.

    OME-Zarr directories are identified by the presence of a .zattrs file
    at the root level containing OME metadata.

    Args:
        extracted_dir: Path to the directory containing extracted files

    Returns:
        Path to the OME-Zarr root directory, or None if not found
    """
    # Check if extracted_dir itself is a zarr directory
    if is_zarr_directory(extracted_dir):
        return extracted_dir

    # Check immediate children
    for entry in os.listdir(extracted_dir):
        entry_path = os.path.join(extracted_dir, entry)
        if os.path.isdir(entry_path) and is_zarr_directory(entry_path):
            return entry_path

    return None


def is_zarr_directory(path: str) -> bool:
    """
    Check if a directory is a valid Zarr directory.

    A Zarr directory must contain either .zarray or .zgroup file at its root.

    Args:
        path: Path to check

    Returns:
        True if the path is a valid Zarr directory
    """
    if not os.path.isdir(path):
        return False

    zattrs_path = os.path.join(path, ".zattrs")
    zgroup_path = os.path.join(path, ".zgroup")

    return os.path.exists(zattrs_path) or os.path.exists(zgroup_path)


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
