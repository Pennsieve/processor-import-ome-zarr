import os
import zipfile

import pytest
from extractor import OmeZarrExtractor


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

    def test_find_input_file_no_zip(self, tmp_path):
        """Should raise assertion error when no ZIP file exists."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data.txt").write_text("not a zip")

        extractor = OmeZarrExtractor(str(input_dir), str(tmp_path / "output"))

        with pytest.raises(AssertionError, match="Expected exactly one ZIP file"):
            extractor.find_input_file()

    def test_find_input_file_multiple_zips(self, tmp_path):
        """Should raise assertion error when multiple ZIP files exist."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data1.zip").write_bytes(b"fake zip")
        (input_dir / "data2.zip").write_bytes(b"fake zip")

        extractor = OmeZarrExtractor(str(input_dir), str(tmp_path / "output"))

        with pytest.raises(AssertionError, match="Expected exactly one ZIP file"):
            extractor.find_input_file()

    def test_get_zarr_name(self, tmp_path):
        """Should return the basename of the zarr directory."""
        extractor = OmeZarrExtractor(str(tmp_path), str(tmp_path))

        assert extractor.get_zarr_name("/path/to/sample.zarr") == "sample.zarr"
        assert extractor.get_zarr_name("/data/output/my-data.ome.zarr") == "my-data.ome.zarr"

    def test_extract_valid_zarr(self, tmp_path):
        """Should extract ZIP and find OME-Zarr root."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create a ZIP with OME-Zarr structure
        zip_path = input_dir / "data.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("sample.zarr/.zattrs", '{"multiscales": []}')
            zf.writestr("sample.zarr/.zgroup", '{"zarr_format": 2}')
            zf.writestr("sample.zarr/0/.zarray", "{}")

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))
        result = extractor.extract(str(zip_path))

        assert result.endswith("sample.zarr")
        assert os.path.exists(result)
        assert os.path.exists(os.path.join(result, ".zattrs"))

    def test_extract_no_zarr_found(self, tmp_path):
        """Should raise assertion error when ZIP contains no zarr directory."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create a ZIP without zarr structure
        zip_path = input_dir / "data.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("random_file.txt", "content")

        extractor = OmeZarrExtractor(str(input_dir), str(output_dir))

        with pytest.raises(AssertionError, match="No valid OME-Zarr directory"):
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
