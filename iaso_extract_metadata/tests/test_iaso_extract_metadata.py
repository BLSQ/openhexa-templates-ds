import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import config
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import (
    DatasetVersion,
    clean_string,
    export_to_file,
    format_form_metadata,
    in_dataset_version,
)


def test_clean_string():
    """Test the clean_string function.

    We test:
    (1) Unicode normalization
    (2) Everything that is not a letter, a digit, a whitespace, or a hyphen is removed
    (3) Trailing and leading whitespaces are removed
    (4) Spaces are replaced by _
    (5) All characters are converted to lowercase
    """
    for input_str, expected_output in config.strings_clean_string.items():
        assert clean_string(input_str) == expected_output


def test_format_form_metadata():
    """Test the format_form_metadata function.

    We test:
    (1) When there are 'valid_versions' in the form, we get the latest version
    (and not the last one)
    (2) When there are no 'valid_versions', we get the last version
    (3) We format the questions correctly
    (4) We format the choices correctly
    """
    questions_out_1, choices_out_1 = format_form_metadata(config.metadata_in_valid_versions)
    assert questions_out_1.equals(config.questions_out)
    assert choices_out_1.equals(config.choices_out)
    questions_out_2, choices_out_2 = format_form_metadata(config.metadata_in_no_valid_versions)
    assert questions_out_2.equals(config.questions_out)
    assert choices_out_2.equals(config.choices_out)


def test_export_to_file(tmp_path: Path):
    """Test the export_to_file function.

    We test:
    (1) The file is created
    (2) The content of the file is correct

    For the formats .csv, .parquet, and .xlsx.
    """
    output_format = ".csv"
    output_path = tmp_path / f"out{output_format}"
    output_file_path = export_to_file(
        config.questions_out, config.choices_out, output_format, output_path
    )
    assert output_file_path.exists()
    read_csv = pl.read_csv(output_file_path)
    assert read_csv.equals(config.questions_choices)

    output_format = ".parquet"
    output_path = tmp_path / f"out{output_format}"
    output_file_path = export_to_file(
        config.questions_out, config.choices_out, output_format, output_path
    )
    assert output_file_path.exists()
    read_parquet = pl.read_parquet(output_file_path)
    assert read_parquet.equals(config.questions_choices)

    output_format = ".xlsx"
    output_path = tmp_path / f"out{output_format}"
    output_file_path = export_to_file(
        config.questions_out, config.choices_out, output_format, output_path
    )
    assert output_file_path.exists()
    read_choices_xlsx = pl.read_excel(output_file_path, sheet_name="Choices")
    read_questions_xlsx = pl.read_excel(output_file_path, sheet_name="Questions")
    assert read_questions_xlsx.equals(config.questions_read)
    assert read_choices_xlsx.sort(["name", "choice_value"]).equals(
        config.choices_read.sort(["name", "choice_value"])
    )


def test_in_dataset_version_file_exists() -> None:
    """Test that a file is found in the dataset version when hashes match."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_file = Path(tmp_dir) / "test_file.txt"
        test_content = b"Test content for dataset version"
        test_file.write_bytes(test_content)

        # Mock DatasetVersion and file
        mock_file = MagicMock()
        mock_file.read.return_value = test_content

        mock_dataset_version = MagicMock(spec=DatasetVersion)
        mock_dataset_version.files = [mock_file]

        result = in_dataset_version(test_file, mock_dataset_version)

        assert result is True
        mock_file.read.assert_called_once()


def test_in_dataset_version_file_not_exists() -> None:
    """Test that a file is not found when hashes don't match."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_file = Path(tmp_dir) / "test_file.txt"
        test_content = b"Test content"
        test_file.write_bytes(test_content)

        # Mock DatasetVersion with different file content
        mock_file = MagicMock()
        mock_file.read.return_value = b"Different content"

        mock_dataset_version = MagicMock(spec=DatasetVersion)
        mock_dataset_version.files = [mock_file]

        result = in_dataset_version(test_file, mock_dataset_version)

        assert result is False


def test_in_dataset_version_empty_dataset() -> None:
    """Test with an empty dataset version (no files)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_file = Path(tmp_dir) / "test_file.txt"
        test_file.write_bytes(b"Test content")

        mock_dataset_version = MagicMock(spec=DatasetVersion)
        mock_dataset_version.files = []

        result = in_dataset_version(test_file, mock_dataset_version)

        assert result is False


def test_in_dataset_version_multiple_files() -> None:
    """Test with multiple files in dataset version, matching the last one."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_file = Path(tmp_dir) / "test_file.txt"
        test_content = b"Matching content"
        test_file.write_bytes(test_content)

        # Mock multiple files, with the last one matching
        mock_file1 = MagicMock()
        mock_file1.read.return_value = b"First file content"

        mock_file2 = MagicMock()
        mock_file2.read.return_value = b"Second file content"

        mock_file3 = MagicMock()
        mock_file3.read.return_value = test_content

        mock_dataset_version = MagicMock(spec=DatasetVersion)
        mock_dataset_version.files = [mock_file1, mock_file2, mock_file3]

        result = in_dataset_version(test_file, mock_dataset_version)

        assert result is True
        # Verify that we checked files until we found a match
        mock_file1.read.assert_called_once()
        mock_file2.read.assert_called_once()
        mock_file3.read.assert_called_once()
