import os
import tarfile
import zipfile

import pytest

from processor.utils import (
    _is_zarr_root,
    collect_files,
    extract_archive,
    extract_tar,
    extract_zip,
    find_zarr_root,
    get_archive_type,
    is_zarr_directory,
    strip_archive_extension,
)


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

    def test_returns_true_for_directory_with_zarray(self, tmp_path):
        """Should return True for directory with .zarray file."""
        zarr_dir = tmp_path / "data.zarr"
        zarr_dir.mkdir()
        (zarr_dir / ".zarray").write_text("{}")
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

    def test_prefers_zarr_root_over_subarray(self, tmp_path):
        """Should find zarr root (.zgroup) over sub-array (.zarray only)."""
        # Create a structure like extracted zarr: root has .zgroup, subdirs have .zarray
        (tmp_path / ".zgroup").write_text("{}")
        (tmp_path / ".zattrs").write_text("{}")

        subdir = tmp_path / "0"
        subdir.mkdir()
        (subdir / ".zarray").write_text("{}")

        result = find_zarr_root(str(tmp_path))
        assert result == str(tmp_path)


class TestIsZarrRoot:
    """Tests for _is_zarr_root function (stricter than is_zarr_directory)."""

    def test_returns_true_for_zgroup(self, tmp_path):
        """Should return True for directory with .zgroup."""
        (tmp_path / ".zgroup").write_text("{}")
        assert _is_zarr_root(str(tmp_path)) is True

    def test_returns_true_for_zattrs(self, tmp_path):
        """Should return True for directory with .zattrs."""
        (tmp_path / ".zattrs").write_text("{}")
        assert _is_zarr_root(str(tmp_path)) is True

    def test_returns_false_for_zarray_only(self, tmp_path):
        """Should return False for directory with only .zarray (sub-array, not root)."""
        (tmp_path / ".zarray").write_text("{}")
        assert _is_zarr_root(str(tmp_path)) is False


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


class TestGetArchiveType:
    """Tests for get_archive_type function."""

    def test_returns_zip_for_zip_file(self):
        """Should return .zip for ZIP files."""
        assert get_archive_type("data.zip") == ".zip"
        assert get_archive_type("data.zarr.zip") == ".zip"

    def test_returns_tar_gz_for_gzipped_tarball(self):
        """Should return .tar.gz for gzipped tarballs."""
        assert get_archive_type("data.tar.gz") == ".tar.gz"
        assert get_archive_type("data.zarr.tar.gz") == ".tar.gz"

    def test_returns_tgz_for_tgz_extension(self):
        """Should return .tgz for .tgz files."""
        assert get_archive_type("data.tgz") == ".tgz"

    def test_returns_tar_bz2_for_bzip2_tarball(self):
        """Should return .tar.bz2 for bzip2 tarballs."""
        assert get_archive_type("data.tar.bz2") == ".tar.bz2"
        assert get_archive_type("data.tbz2") == ".tbz2"

    def test_returns_tar_xz_for_xz_tarball(self):
        """Should return .tar.xz for xz tarballs."""
        assert get_archive_type("data.tar.xz") == ".tar.xz"
        assert get_archive_type("data.txz") == ".txz"

    def test_returns_tar_for_plain_tarball(self):
        """Should return .tar for uncompressed tarballs."""
        assert get_archive_type("data.tar") == ".tar"

    def test_returns_none_for_unsupported(self):
        """Should return None for unsupported file types."""
        assert get_archive_type("data.txt") is None
        assert get_archive_type("data.rar") is None
        assert get_archive_type("data.7z") is None

    def test_case_insensitive(self):
        """Should match extensions case-insensitively."""
        assert get_archive_type("data.ZIP") == ".zip"
        assert get_archive_type("data.TAR.GZ") == ".tar.gz"
        assert get_archive_type("data.TGZ") == ".tgz"


class TestStripArchiveExtension:
    """Tests for strip_archive_extension function."""

    def test_strips_zip_extension(self):
        """Should strip .zip extension."""
        assert strip_archive_extension("data.zarr.zip") == "data.zarr"
        assert strip_archive_extension("sample.zip") == "sample"

    def test_strips_tar_gz_extension(self):
        """Should strip .tar.gz extension."""
        assert strip_archive_extension("data.zarr.tar.gz") == "data.zarr"

    def test_strips_tgz_extension(self):
        """Should strip .tgz extension."""
        assert strip_archive_extension("data.zarr.tgz") == "data.zarr"

    def test_strips_tar_bz2_extension(self):
        """Should strip .tar.bz2 extension."""
        assert strip_archive_extension("data.zarr.tar.bz2") == "data.zarr"

    def test_strips_tbz2_extension(self):
        """Should strip .tbz2 extension."""
        assert strip_archive_extension("data.zarr.tbz2") == "data.zarr"

    def test_strips_tar_xz_extension(self):
        """Should strip .tar.xz extension."""
        assert strip_archive_extension("data.zarr.tar.xz") == "data.zarr"

    def test_strips_txz_extension(self):
        """Should strip .txz extension."""
        assert strip_archive_extension("data.zarr.txz") == "data.zarr"

    def test_strips_plain_tar_extension(self):
        """Should strip .tar extension."""
        assert strip_archive_extension("data.zarr.tar") == "data.zarr"

    def test_returns_filename_if_no_extension(self):
        """Should return filename unchanged if no archive extension."""
        assert strip_archive_extension("data.zarr") == "data.zarr"
        assert strip_archive_extension("data.txt") == "data.txt"

    def test_case_insensitive(self):
        """Should match extensions case-insensitively but preserve base name case."""
        assert strip_archive_extension("Data.Zarr.ZIP") == "Data.Zarr"
        assert strip_archive_extension("Data.Zarr.TAR.GZ") == "Data.Zarr"


class TestExtractTar:
    """Tests for extract_tar function."""

    def test_extracts_tar_gz_contents(self, tmp_path):
        """Should extract all files from a gzipped tar archive."""
        tar_path = tmp_path / "test.tar.gz"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create a tar.gz file
        with tarfile.open(tar_path, "w:gz") as tf:
            # Add a file
            file1 = tmp_path / "file1.txt"
            file1.write_text("content1")
            tf.add(file1, arcname="file1.txt")

            # Add a file in a subdirectory
            subdir = tmp_path / "subdir"
            subdir.mkdir()
            file2 = subdir / "file2.txt"
            file2.write_text("content2")
            tf.add(file2, arcname="subdir/file2.txt")

        # Extract
        result = extract_tar(str(tar_path), str(output_dir))

        assert result == str(output_dir)
        assert (output_dir / "file1.txt").exists()
        assert (output_dir / "subdir" / "file2.txt").exists()
        assert (output_dir / "file1.txt").read_text() == "content1"
        assert (output_dir / "subdir" / "file2.txt").read_text() == "content2"

    def test_extracts_tar_bz2_contents(self, tmp_path):
        """Should extract all files from a bzip2 tar archive."""
        tar_path = tmp_path / "test.tar.bz2"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create a tar.bz2 file
        with tarfile.open(tar_path, "w:bz2") as tf:
            file1 = tmp_path / "file1.txt"
            file1.write_text("content1")
            tf.add(file1, arcname="file1.txt")

        # Extract
        result = extract_tar(str(tar_path), str(output_dir))

        assert result == str(output_dir)
        assert (output_dir / "file1.txt").exists()
        assert (output_dir / "file1.txt").read_text() == "content1"

    def test_extracts_plain_tar_contents(self, tmp_path):
        """Should extract all files from an uncompressed tar archive."""
        tar_path = tmp_path / "test.tar"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create a plain tar file
        with tarfile.open(tar_path, "w") as tf:
            file1 = tmp_path / "file1.txt"
            file1.write_text("content1")
            tf.add(file1, arcname="file1.txt")

        # Extract
        result = extract_tar(str(tar_path), str(output_dir))

        assert result == str(output_dir)
        assert (output_dir / "file1.txt").exists()
        assert (output_dir / "file1.txt").read_text() == "content1"


class TestExtractArchive:
    """Tests for extract_archive function."""

    def test_extracts_zip_file(self, tmp_path):
        """Should extract ZIP files."""
        zip_path = tmp_path / "test.zip"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file1.txt", "content1")

        result = extract_archive(str(zip_path), str(output_dir))

        assert result == str(output_dir)
        assert (output_dir / "file1.txt").read_text() == "content1"

    def test_extracts_tar_gz_file(self, tmp_path):
        """Should extract .tar.gz files."""
        tar_path = tmp_path / "test.tar.gz"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        file1 = tmp_path / "file1.txt"
        file1.write_text("content1")
        with tarfile.open(tar_path, "w:gz") as tf:
            tf.add(file1, arcname="file1.txt")

        result = extract_archive(str(tar_path), str(output_dir))

        assert result == str(output_dir)
        assert (output_dir / "file1.txt").read_text() == "content1"

    def test_raises_for_unsupported_format(self, tmp_path):
        """Should raise ValueError for unsupported formats."""
        unsupported_path = tmp_path / "test.rar"
        unsupported_path.write_bytes(b"fake")
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with pytest.raises(ValueError, match="Unsupported archive format"):
            extract_archive(str(unsupported_path), str(output_dir))
