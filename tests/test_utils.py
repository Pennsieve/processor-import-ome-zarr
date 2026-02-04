import os
import zipfile

from processor.utils import collect_files, extract_zip, find_zarr_root, is_zarr_directory


class TestIsZarrDirectory:
    """Tests for is_zarr_directory function."""

    def test_returns_false_for_non_directory(self, tmp_path):
        """Should return False for a file path."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("content")
        assert is_zarr_directory(str(file_path)) is False

    def test_returns_false_for_empty_directory(self, tmp_path):
        """Should return False for directory without zarr files."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        assert is_zarr_directory(str(empty_dir)) is False

    def test_returns_true_for_directory_with_zattrs(self, tmp_path):
        """Should return True for directory with .zattrs file."""
        zarr_dir = tmp_path / "data.zarr"
        zarr_dir.mkdir()
        (zarr_dir / ".zattrs").write_text("{}")
        assert is_zarr_directory(str(zarr_dir)) is True

    def test_returns_true_for_directory_with_zgroup(self, tmp_path):
        """Should return True for directory with .zgroup file."""
        zarr_dir = tmp_path / "data.zarr"
        zarr_dir.mkdir()
        (zarr_dir / ".zgroup").write_text("{}")
        assert is_zarr_directory(str(zarr_dir)) is True


class TestFindZarrRoot:
    """Tests for find_zarr_root function."""

    def test_returns_none_when_no_zarr_found(self, tmp_path):
        """Should return None when no zarr directory exists."""
        (tmp_path / "random_file.txt").write_text("content")
        assert find_zarr_root(str(tmp_path)) is None

    def test_finds_zarr_in_extracted_dir(self, temp_zarr_directory):
        """Should find zarr directory at extracted root."""
        parent = temp_zarr_directory.parent
        result = find_zarr_root(str(parent))
        assert result == str(temp_zarr_directory)

    def test_returns_directory_if_itself_is_zarr(self, temp_zarr_directory):
        """Should return the directory itself if it is a zarr directory."""
        result = find_zarr_root(str(temp_zarr_directory))
        assert result == str(temp_zarr_directory)


class TestExtractZip:
    """Tests for extract_zip function."""

    def test_extracts_zip_contents(self, tmp_path):
        """Should extract all files from zip archive."""
        # Create a zip file
        zip_path = tmp_path / "test.zip"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file1.txt", "content1")
            zf.writestr("subdir/file2.txt", "content2")

        # Extract
        result = extract_zip(str(zip_path), str(output_dir))

        assert result == str(output_dir)
        assert (output_dir / "file1.txt").exists()
        assert (output_dir / "subdir" / "file2.txt").exists()
        assert (output_dir / "file1.txt").read_text() == "content1"


class TestCollectFiles:
    """Tests for collect_files function."""

    def test_collects_all_files_recursively(self, temp_zarr_directory):
        """Should collect all files with correct relative paths."""
        files = collect_files(str(temp_zarr_directory))

        # Convert to dict for easier checking
        files_dict = {rel: abs for abs, rel in files}

        assert ".zattrs" in files_dict
        assert ".zgroup" in files_dict
        assert os.path.join("0", ".zarray") in files_dict
        assert os.path.join("0", "0", "0", "0") in files_dict
        assert os.path.join("0", "0", "0", "1") in files_dict

    def test_returns_empty_for_empty_directory(self, tmp_path):
        """Should return empty list for empty directory."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        assert collect_files(str(empty_dir)) == []
