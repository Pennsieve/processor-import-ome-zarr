import os
import tarfile
import zipfile

import pytest

from processor.extractor import OmeZarrExtractor


class TestOmeZarrExtractor:
    """Tests for OmeZarrExtractor class."""

    def test_find_input_file_single_zip(self, tmp_path):
        """Should find the single ZIP file in input directory."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data.zip").write_bytes(b"fake zip")

        extractor = OmeZarrExtractor(str(input_dir), str(tmp_path / "output"))
        result = extractor.find_input_file()

        assert result == str(input_dir / "data.zip")

    def test_find_input_file_no_archive(self, tmp_path):
        """Should raise FileNotFoundError when no archive file exists."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data.txt").write_text("not an archive")

        extractor = OmeZarrExtractor(str(input_dir), str(tmp_path / "output"))

        with pytest.raises(FileNotFoundError, match="Expected exactly one archive file"):
            extractor.find_input_file()

    def test_find_input_file_multiple_archives(self, tmp_path):
        """Should raise ValueError when multiple archive files exist."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data1.zip").write_bytes(b"fake zip")
        (input_dir / "data2.zip").write_bytes(b"fake zip")

        extractor = OmeZarrExtractor(str(input_dir), str(tmp_path / "output"))

        with pytest.raises(ValueError, match="Expected exactly one archive file"):
            extractor.find_input_file()

    def test_find_input_file_tar_gz(self, tmp_path):
        """Should find .tar.gz files."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data.tar.gz").write_bytes(b"fake tar")

        extractor = OmeZarrExtractor(str(input_dir), str(tmp_path / "output"))
        result = extractor.find_input_file()

        assert result == str(input_dir / "data.tar.gz")

    def test_find_input_file_tgz(self, tmp_path):
        """Should find .tgz files."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data.tgz").write_bytes(b"fake tar")

        extractor = OmeZarrExtractor(str(input_dir), str(tmp_path / "output"))
        result = extractor.find_input_file()

        assert result == str(input_dir / "data.tgz")

    def test_extract_valid_zarr_with_nested_folder(self, tmp_path):
        """Should extract ZIP with nested folder and use that folder's name."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create a ZIP with OME-Zarr structure inside a nested folder
        zip_path = input_dir / "data.zarr.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("sample.zarr/.zattrs", '{"multiscales": []}')
            zf.writestr("sample.zarr/.zgroup", '{"zarr_format": 2}')
            zf.writestr("sample.zarr/0/.zarray", "{}")

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))
        zarr_root, zarr_name = extractor.extract(str(zip_path))

        # Nested folder name takes precedence
        assert zarr_root.endswith("sample.zarr")
        assert zarr_name == "sample.zarr"
        assert os.path.exists(zarr_root)
        assert os.path.exists(os.path.join(zarr_root, ".zattrs"))

    def test_extract_valid_zarr_direct(self, tmp_path):
        """Should extract ZIP without container folder and use zip filename as zarr name."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create a ZIP with OME-Zarr files directly (no container folder)
        zip_path = input_dir / "my-data.zarr.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr(".zattrs", '{"multiscales": []}')
            zf.writestr(".zgroup", '{"zarr_format": 2}')
            zf.writestr("0/.zarray", "{}")

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))
        zarr_root, zarr_name = extractor.extract(str(zip_path))

        # Extracts to folder named after zip, that folder is the zarr root
        assert zarr_root.endswith("my-data.zarr")
        assert zarr_name == "my-data.zarr"
        assert os.path.exists(os.path.join(zarr_root, ".zattrs"))

    def test_extract_no_zarr_found(self, tmp_path):
        """Should raise ValueError when ZIP contains no zarr directory."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create a ZIP without zarr structure
        zip_path = input_dir / "data.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("random_file.txt", "content")

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))

        with pytest.raises(ValueError, match="No valid OME-Zarr directory"):
            extractor.extract(str(zip_path))

    def test_collect_zarr_files(self, temp_zarr_directory):
        """Should collect all files from zarr directory."""
        extractor = OmeZarrExtractor("/input", "/output")
        files = extractor.collect_zarr_files(str(temp_zarr_directory))

        assert len(files) == 5  # .zattrs, .zgroup, 0/.zarray, 0/0/0/0, 0/0/0/1

        # Check file paths are correct
        rel_paths = [rel for _, rel in files]
        assert ".zattrs" in rel_paths
        assert ".zgroup" in rel_paths

    def test_process_full_workflow(self, tmp_path):
        """Should process a complete OME-Zarr import workflow."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create a ZIP with OME-Zarr structure
        zip_path = input_dir / "sample.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("sample.zarr/.zattrs", '{"multiscales": []}')
            zf.writestr("sample.zarr/.zgroup", '{"zarr_format": 2}')
            zf.writestr("sample.zarr/0/.zarray", "{}")
            zf.writestr("sample.zarr/0/0/0", b"\x00" * 10)

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))
        zarr_root, zarr_name, files = extractor.process()

        assert zarr_name == "sample.zarr"
        assert zarr_root.endswith("sample.zarr")
        assert len(files) == 4  # .zattrs, .zgroup, 0/.zarray, 0/0/0

    def test_extract_tar_gz_with_nested_folder(self, tmp_path):
        """Should extract .tar.gz with nested folder and use that folder's name."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create temp directory structure for tarball
        tar_content_dir = tmp_path / "tar_content"
        zarr_dir = tar_content_dir / "sample.zarr"
        zarr_dir.mkdir(parents=True)
        (zarr_dir / ".zattrs").write_text('{"multiscales": []}')
        (zarr_dir / ".zgroup").write_text('{"zarr_format": 2}')
        (zarr_dir / "0").mkdir()
        (zarr_dir / "0" / ".zarray").write_text("{}")

        # Create a .tar.gz with the OME-Zarr structure
        tar_path = input_dir / "data.zarr.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            tf.add(zarr_dir, arcname="sample.zarr")

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))
        zarr_root, zarr_name = extractor.extract(str(tar_path))

        # Nested folder name takes precedence
        assert zarr_root.endswith("sample.zarr")
        assert zarr_name == "sample.zarr"
        assert os.path.exists(zarr_root)
        assert os.path.exists(os.path.join(zarr_root, ".zattrs"))

    def test_extract_tar_gz_direct(self, tmp_path):
        """Should extract .tar.gz without container folder and use archive filename as zarr name."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create temp directory structure for tarball (zarr files directly)
        tar_content_dir = tmp_path / "tar_content"
        tar_content_dir.mkdir()
        (tar_content_dir / ".zattrs").write_text('{"multiscales": []}')
        (tar_content_dir / ".zgroup").write_text('{"zarr_format": 2}')
        (tar_content_dir / "0").mkdir()
        (tar_content_dir / "0" / ".zarray").write_text("{}")

        # Create a .tar.gz with zarr files directly at root
        tar_path = input_dir / "my-data.zarr.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            for item in tar_content_dir.iterdir():
                tf.add(item, arcname=item.name)

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))
        zarr_root, zarr_name = extractor.extract(str(tar_path))

        # Extracts to folder named after archive, that folder is the zarr root
        assert zarr_root.endswith("my-data.zarr")
        assert zarr_name == "my-data.zarr"
        assert os.path.exists(os.path.join(zarr_root, ".zattrs"))

    def test_process_full_workflow_tar_gz(self, tmp_path):
        """Should process a complete OME-Zarr import workflow with .tar.gz."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create temp directory structure for tarball
        tar_content_dir = tmp_path / "tar_content"
        zarr_dir = tar_content_dir / "sample.zarr"
        zarr_dir.mkdir(parents=True)
        (zarr_dir / ".zattrs").write_text('{"multiscales": []}')
        (zarr_dir / ".zgroup").write_text('{"zarr_format": 2}')
        (zarr_dir / "0").mkdir()
        (zarr_dir / "0" / ".zarray").write_text("{}")
        (zarr_dir / "0" / "0").mkdir()
        (zarr_dir / "0" / "0" / "0").write_bytes(b"\x00" * 10)

        # Create a .tar.gz with OME-Zarr structure
        tar_path = input_dir / "sample.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            tf.add(zarr_dir, arcname="sample.zarr")

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))
        zarr_root, zarr_name, files = extractor.process()

        assert zarr_name == "sample.zarr"
        assert zarr_root.endswith("sample.zarr")
        assert len(files) == 4  # .zattrs, .zgroup, 0/.zarray, 0/0/0
