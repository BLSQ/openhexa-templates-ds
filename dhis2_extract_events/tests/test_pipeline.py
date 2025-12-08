import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import config
import polars as pl
import pytest
from openhexa.toolbox.dhis2 import DHIS2

sys.path.insert(0, str(Path(__file__).parent.parent))

from toolbox import extract_events, get_program_stages, get_programs, join_object_names


def mock_programs(
    dhis2: DHIS2,
    fields: str = "id,name,programType",
    page: int | None = None,
    page_size: int | None = None,
    filters: list[str] | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Mock the programs() function to return predefined data for testing.

    Returns
    -------
    list[dict[str, Any]]
        A list of program dictionaries with id, name, and programType.
    """
    if fields == "id,name,programType":
        return config.programs

    if fields == "id,name,programStages[id,name]":
        return config.programs_program_stages

    return []


class FakeDHIS2(DHIS2):
    """Fake DHIS2 for testing get_programs."""

    def __init__(self) -> None:
        self.api = MagicMock()


@patch("toolbox.programs")
def test_get_programs(mock_programs_fn: Mock) -> None:
    """Test the get_programs() function using the mock_programs function.

    We test:
    (1) That the returned value is a Polars DataFrame.
    (2) That the columns are [id, name, program_type].
    (3) The values of the columns
    (4) The shape of the DataFrame.
    """
    mock_programs_fn.side_effect = mock_programs

    dhis2_instance = FakeDHIS2()
    program_metadata = get_programs(dhis2_instance)

    assert isinstance(program_metadata, pl.DataFrame)

    expected_columns = ["id", "name", "program_type"]
    expected_ids = ["id_A", "id_B", "id_C"]
    expected_names = ["Name A", "Name B", "Name C"]
    expected_types = ["WITH_REGISTRATION", "WITHOUT_REGISTRATION", "WITH_REGISTRATION"]

    assert program_metadata.columns == expected_columns
    assert program_metadata["id"].to_list() == expected_ids
    assert program_metadata["name"].to_list() == expected_names
    assert program_metadata["program_type"].to_list() == expected_types
    assert program_metadata.shape == (3, 3)


@patch("toolbox.programs")
def test_get_program_stages(mock_programs_fn: Mock) -> None:
    """Test the get_program_stages() function using the mock_programs function.

    We test:
    (1) That the returned value is a Polars DataFrame.
    (2) That the columns are [program_stage_id, program_stage_name, program_id, program_name].
    (3) The values of the columns
    (4) The shape of the DataFrame.
    """
    mock_programs_fn.side_effect = mock_programs

    dhis2_instance = FakeDHIS2()
    program_metadata = get_program_stages(dhis2_instance)

    assert isinstance(program_metadata, pl.DataFrame)

    expected_columns = [
        "program_stage_id",
        "program_stage_name",
        "program_id",
        "program_name",
    ]
    expected_ps_ids = ["id_alpha", "id_beta", "id_gamma", "id_delta", "id_beta"]
    expected_ps_names = ["Name Alpha", "Name Beta", "Name Gamma", "Name Delta", "Name Beta"]
    expected_p_ids = ["id_A", "id_A", "id_B", "id_B", "id_B"]
    expected_p_names = ["Name A", "Name A", "Name B", "Name B", "Name B"]

    assert program_metadata.columns == expected_columns
    assert program_metadata["program_stage_id"].to_list() == expected_ps_ids
    assert program_metadata["program_stage_name"].to_list() == expected_ps_names
    assert program_metadata["program_id"].to_list() == expected_p_ids
    assert program_metadata["program_name"].to_list() == expected_p_names
    assert program_metadata.shape == (5, 4)


def test_extract_events() -> None:
    """Test the extract_events() function.

    We test:
    (1) That the returned value is a Polars DataFrame.
    (2) That the DataFrame contains expected columns.
    (3) That the values are what we expect. -- with this I test the explode already,
        so no need to do anything extra.
    """
    dhis2 = FakeDHIS2()
    dhis2.api.get_paged.return_value = config.event_pages_reduced

    events = extract_events(
        dhis2,
        program_id="not_used",
        org_units=["not_used"],
        occurred_after="not_used",
        occurred_before="not_used",
        include_children=True,  # but also not used
    )

    assert isinstance(events, pl.DataFrame)

    expected_columns = [
        "event_id",
        "status",
        "program_id",
        "program_stage_id",
        "enrollment_id",
        "tracked_entity_id",
        "organisation_unit_id",
        "occurred_at",
        "deleted",
        "attribute_option_combo_id",
        "data_element_id",
        "value",
    ]

    assert events.columns == expected_columns
    assert events.shape == (21, 12)

    events_sorted = events.sort(
        ["event_id", "data_element_id"]
    )  # I need to do this to be able to test the values

    assert events_sorted["event_id"].to_list() == config.event_id_values
    assert events_sorted["status"].to_list() == config.status_values
    assert events_sorted["program_id"].to_list() == config.program_id_values
    assert events_sorted["program_stage_id"].to_list() == config.program_stage_id_values
    assert events_sorted["enrollment_id"].to_list() == config.enrollment_id_values
    assert events_sorted["tracked_entity_id"].to_list() == config.tracked_entity_id_values
    assert events_sorted["organisation_unit_id"].to_list() == config.org_unit_values
    assert events_sorted["occurred_at"].to_list() == config.occurred_at_values
    assert events_sorted["deleted"].to_list() == config.deleted_values
    assert events_sorted["attribute_option_combo_id"].to_list() == config.att_option_combo_id_values
    assert events_sorted["data_element_id"].to_list() == config.data_element_id_values
    assert events_sorted["value"].to_list() == config.value_values


def test_join_object_names_no_metadata():
    """Test that join_object_names raises ValueError when no metadata is provided."""
    with pytest.raises(ValueError, match="No metadata to be joined provided"):
        join_object_names(config.df)


def test_join_object_names_data_elements():
    """Test join_object_names with only data elements metadata."""
    out = join_object_names(config.df, data_elements=config.data_elements_metadata)

    output_cols = [
        "data_element_id",
        "data_element_name",
        "program_id",
        "program_stage_id",
        "organisation_unit_id",
        "value",
        "extra_col",
    ]

    assert out.columns == output_cols
    assert out["data_element_name"].to_list() == [
        "Name 1",
        "Name 2",
        "Name 3",
        "Name 1",
        "Name 1",
    ]


def test_join_object_names_ous():
    """Test join_object_names with only organisation units."""
    out = join_object_names(config.df, organisation_units=config.ous_metadata)

    output_cols = [
        "data_element_id",
        "program_id",
        "program_stage_id",
        "organisation_unit_id",
        "value",
        "level_1_id",
        "level_2_id",
        "level_3_id",
        "level_1_name",
        "level_2_name",
        "level_3_name",
        "extra_col",
    ]
    assert out.columns == output_cols
    assert out["level_1_id"].to_list() == [
        "id_alpha",
        "id_alpha",
        "id_alpha",
        None,
        "id_alpha",
    ]
    assert out["level_1_name"].to_list() == [
        "Name Alpha",
        "Name Alpha",
        "Name Alpha",
        None,
        "Name Alpha",
    ]
    assert out["level_2_id"].to_list() == [
        "id_beta",
        "id_gamma",
        "id_beta",
        None,
        "id_beta",
    ]
    assert out["level_2_name"].to_list() == [
        "Name Beta",
        "Name Gamma",
        "Name Beta",
        None,
        "Name Beta",
    ]
    assert out["level_3_id"].to_list() == [
        "id_delta",
        "id_epsilon",
        "id_delta",
        None,
        "id_zeta",
    ]
    assert out["level_3_name"].to_list() == [
        "Name Delta",
        "Name Epsilon",
        "Name Delta",
        None,
        "Name Zeta",
    ]


def test_join_object_names_program_stages():
    """Test join_object_names with only program stages metadata."""
    out = join_object_names(config.df, program_stages=config.program_stages_metadata)

    output_cols = [
        "data_element_id",
        "program_id",
        "program_stage_id",
        "program_stage_name",
        "organisation_unit_id",
        "value",
        "extra_col",
    ]
    assert out.columns == output_cols
    assert out["program_stage_id"].to_list() == [
        "id_a",
        "id_b",
        "id_a",
        "missing",
        "id_b",
    ]
    assert out["program_stage_name"].to_list() == [
        "Name A",
        "Name B",
        "Name A",
        None,
        "Name B",
    ]


def test_join_object_names_programs():
    """Test join_object_names with only program stages metadata."""
    out = join_object_names(config.df, programs=config.programs_metadata)

    output_cols = [
        "data_element_id",
        "program_id",
        "program_name",
        "program_stage_id",
        "organisation_unit_id",
        "value",
        "extra_col",
    ]
    assert out.columns == output_cols
    assert out["program_id"].to_list() == ["id_one", "missing", "id_one", "id_three", "id_two"]
    assert out["program_name"].to_list() == [
        "Name One",
        None,
        "Name One",
        "Name Three",
        "Name Two",
    ]


def test_join_object_names_all_metadata():
    """Join all metadata types."""
    out = join_object_names(
        config.df,
        data_elements=config.data_elements_metadata,
        organisation_units=config.ous_metadata,
        program_stages=config.program_stages_metadata,
        programs=config.programs_metadata,
    )

    output_cols = [
        "data_element_id",
        "data_element_name",
        "program_id",
        "program_name",
        "program_stage_id",
        "program_stage_name",
        "organisation_unit_id",
        "value",
        "level_1_id",
        "level_2_id",
        "level_3_id",
        "level_1_name",
        "level_2_name",
        "level_3_name",
        "extra_col",
    ]
    assert out.columns == output_cols
    assert out["data_element_name"].to_list() == [
        "Name 1",
        "Name 2",
        "Name 3",
        "Name 1",
        "Name 1",
    ]
    assert out["program_id"].to_list() == ["id_one", "missing", "id_one", "id_three", "id_two"]
    assert out["program_name"].to_list() == [
        "Name One",
        None,
        "Name One",
        "Name Three",
        "Name Two",
    ]
    assert out["program_stage_id"].to_list() == [
        "id_a",
        "id_b",
        "id_a",
        "missing",
        "id_b",
    ]
    assert out["program_stage_name"].to_list() == [
        "Name A",
        "Name B",
        "Name A",
        None,
        "Name B",
    ]
    assert out["level_1_id"].to_list() == [
        "id_alpha",
        "id_alpha",
        "id_alpha",
        None,
        "id_alpha",
    ]
    assert out["level_1_name"].to_list() == [
        "Name Alpha",
        "Name Alpha",
        "Name Alpha",
        None,
        "Name Alpha",
    ]
    assert out["level_2_id"].to_list() == [
        "id_beta",
        "id_gamma",
        "id_beta",
        None,
        "id_beta",
    ]
    assert out["level_2_name"].to_list() == [
        "Name Beta",
        "Name Gamma",
        "Name Beta",
        None,
        "Name Beta",
    ]
    assert out["level_3_id"].to_list() == [
        "id_delta",
        "id_epsilon",
        "id_delta",
        None,
        "id_zeta",
    ]
    assert out["level_3_name"].to_list() == [
        "Name Delta",
        "Name Epsilon",
        "Name Delta",
        None,
        "Name Zeta",
    ]
    assert out["extra_col"].to_list() == [
        "extra_first",
        "extra_second",
        "extra_third",
        "extra_fourth",
        "extra_fifth",
    ]
