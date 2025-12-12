from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from dhis2_metadata_extract.pipeline import (
    format_category_option_combos,
    format_data_element_groups,
    format_data_elements,
    format_datasets,
    format_organisation_units,
    format_organisation_units_groups,
    save_file,
)
from dhis2_metadata_extract.tests.testing_dataframes import (
    get_sample_category_option_combos_df,
    get_sample_data_element_groups_df,
    get_sample_data_elements_df,
    get_sample_datasets_df,
    get_sample_org_units_df,
    get_sample_org_units_groups_df,
)
from dhis2_metadata_extract.validate import validate_data
from dhis2_metadata_extract.validation_config import (
    org_unit_groups_expected_columns,
    org_units_expected_columns,
    retrieved_categorty_options_expected_columns,
    retrieved_data_element_groups_expected_columns,
    retrieved_data_elements_expected_columns,
    retrieved_datasets_expected_columns,
)

"""
The ideal tests would use the dataframe API extracting data from a DHIS2, this way we could
ensure end to end that the data extracted has the expected format.
For simplicity, these tests use hardcoded dataframe samples defined in the testing_dataframes.py.
"""


def test_format_organisation_units():  # noqa: D103
    org_units_df = get_sample_org_units_df()
    org_units_formatted = format_organisation_units(org_units_df, 4)
    assert org_units_df.shape == (3, 14), "Expected 3 rows and 14 columns"
    assert org_units_formatted.shape == (1, 8), "Expected 1 row and 8 columns after formatting"
    validate_data(org_units_formatted, org_units_expected_columns[0:8], data_name="org_units")


def test_format_organisation_units_groups():  # noqa: D103
    org_units_groups_df = get_sample_org_units_groups_df()
    org_units_groups_df_formatted = format_organisation_units_groups(org_units_groups_df)
    assert org_units_groups_df.shape == (5, 3), "Expected 5 rows and 3 columns"
    assert org_units_groups_df_formatted.shape == (5, 3), (
        "Expected 5 rows and 3 columns after formatting"
    )
    validate_data(
        org_units_groups_df_formatted, org_unit_groups_expected_columns, data_name="org_unit_groups"
    )


def test_format_datasets():  # noqa: D103
    datasets = get_sample_datasets_df()
    datasets_formatted = format_datasets(datasets)
    assert datasets.shape == (3, 6), "Expected 3 rows and 6 columns"
    assert datasets_formatted.shape == (3, 6), "Expected 3 rows and 6 columns after formatting"
    validate_data(datasets_formatted, retrieved_datasets_expected_columns, data_name="datasets")


def test_format_data_elements():  # noqa: D103
    data_elements = get_sample_data_elements_df()
    data_elements_formatted = format_data_elements(data_elements)
    assert data_elements.shape == (5, 3), "Expected 5 rows and 3 columns"
    assert data_elements_formatted.shape == (5, 3), "Expected 5 rows and 3 columns after formatting"
    validate_data(
        data_elements_formatted, retrieved_data_elements_expected_columns, data_name="data_elements"
    )


def test_format_data_element_groups():  # noqa: D103
    data_elements_groups = get_sample_data_element_groups_df()
    data_elements_groups_formatted = format_data_element_groups(data_elements_groups)
    assert data_elements_groups.shape == (4, 3), "Expected 4 rows and 3 columns"
    assert data_elements_groups_formatted.shape == (4, 3), (
        "Expected 4 rows and 3 columns after formatting"
    )
    validate_data(
        data_elements_groups_formatted,
        retrieved_data_element_groups_expected_columns,
        data_name="data_element_groups",
    )


def test_format_category_option_combos():  # noqa: D103
    category_option_combos = get_sample_category_option_combos_df()
    category_option_combos_formatted = format_category_option_combos(category_option_combos)
    assert category_option_combos.shape == (5, 2), "Expected 5 rows and 2 columns"
    assert category_option_combos_formatted.shape == (5, 2), (
        "Expected 5 rows and 2 columns after formatting"
    )
    validate_data(
        category_option_combos_formatted,
        retrieved_categorty_options_expected_columns,
        data_name="category_option_combos",
    )


def test_save_file(tmp_path: Path):  # noqa: D103
    df = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})

    # Act — call your function
    filename = "test.csv"
    save_file(df, tmp_path, filename)

    # Assert — check that file exists and contents match
    saved_file = tmp_path / filename
    assert saved_file.exists(), "File was not created."

    # Read back and validate contents
    loaded = pl.read_csv(saved_file)
    assert loaded.shape == (2, 2)
    assert loaded["id"].to_list() == ["a", "b"]
    assert loaded["value"].to_list() == [1, 2]


def test_save_file_directory_error(tmp_path: Path):  # noqa: D103
    df = pl.DataFrame({"a": [1]})
    bad_path = tmp_path / "bad"

    with (
        patch("pathlib.Path.mkdir", side_effect=Exception("cannot create")),
        pytest.raises(Exception, match="Error creating output directory"),
    ):
        save_file(df, bad_path, "x.csv")
