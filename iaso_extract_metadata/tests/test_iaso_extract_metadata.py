import sys
from pathlib import Path

import config
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import clean_string, export_to_file, format_form_metadata


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
