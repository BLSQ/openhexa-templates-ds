import json
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import (
    RequestParams,
    extract_params_from_config,
    raise_if_parameters_set,
    read_json,
    validate,
    validate_parameters,
)
from validate import DataValidationError


def test_validate_parameters() -> None:
    """Ensure we are raising errors when invalid parameters are provided."""
    valid_params = RequestParams(
        data_elements=["de1", "de2"],
        data_element_groups=None,
        organisation_units=["ou1"],
        organisation_unit_groups=None,
        include_children=False,
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
    validate_parameters(valid_params).run()

    # missing start_date
    with pytest.raises(ValueError, match="Start date is required"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=None,
                organisation_units=["ou1"],
                organisation_unit_groups=None,
                include_children=False,
                start_date=None,
                end_date=None,
            )
        ).run()

    # wrong format for start_date
    with pytest.raises(ValueError, match="not in ISO format"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=None,
                organisation_units=["ou1"],
                organisation_unit_groups=None,
                include_children=False,
                start_date="01-01-2024",
                end_date=None,
            )
        ).run()

    # end_date before start_date
    with pytest.raises(ValueError, match="after end date"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=None,
                organisation_units=["ou1"],
                organisation_unit_groups=None,
                include_children=False,
                start_date="2024-12-31",
                end_date="2024-01-01",
            )
        ).run()

    # no org unit provided
    with pytest.raises(ValueError, match="No organisation units"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=None,
                organisation_units=None,
                organisation_unit_groups=None,
                include_children=False,
                start_date="2024-01-01",
                end_date=None,
            )
        ).run()

    # no data elements provided
    with pytest.raises(ValueError, match="not both"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=["deg1"],
                organisation_units=["ou1"],
                organisation_unit_groups=None,
                include_children=False,
                start_date="2024-01-01",
                end_date=None,
            )
        ).run()


def test_extract_params_from_config() -> None:
    """Ensure config dictionaries are correctly mapped to RequestParams."""
    config = {
        "data_elements": ["de1", "de2"],
        "organisation_units": ["ou1"],
        "include_children": True,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
    }
    params = extract_params_from_config(config=config)
    assert params.data_elements == ["de1", "de2"]
    assert params.data_element_groups is None
    assert params.organisation_units == ["ou1"]
    assert params.organisation_unit_groups is None
    assert params.include_children is True
    assert params.start_date == "2024-01-01"
    assert params.end_date == "2024-12-31"

    # missing keys fall back to their defaults
    params = extract_params_from_config(config={"data_elements": ["de1"]})
    assert params.data_element_groups is None
    assert params.organisation_units is None
    assert params.organisation_unit_groups is None
    assert params.include_children is False
    assert params.start_date is None

    # missing end_date defaults to today
    assert params.end_date == datetime.now().strftime("%Y-%m-%d")


def test_raise_if_parameters_set() -> None:
    """Ensure conflicting parameters are rejected when a config file is used."""
    # no extraction parameters set: should not raise
    raise_if_parameters_set(None, None, None, None, None, None)

    # include_children is not part of the check, so True alone is allowed
    # (its value comes from the config file when one is provided)

    # each extraction parameter individually triggers the error
    conflicting = [
        {"data_elements": ["de1"]},
        {"data_element_groups": ["deg1"]},
        {"organisation_units": ["ou1"]},
        {"organisation_unit_groups": ["oug1"]},
        {"start_date": "2024-01-01"},
        {"end_date": "2024-12-31"},
    ]
    for kwargs in conflicting:
        with pytest.raises(ValueError, match="no other parameters may be set"):
            raise_if_parameters_set(
                data_elements=kwargs.get("data_elements"),
                data_element_groups=kwargs.get("data_element_groups"),
                organisation_units=kwargs.get("organisation_units"),
                organisation_unit_groups=kwargs.get("organisation_unit_groups"),
                start_date=kwargs.get("start_date"),
                end_date=kwargs.get("end_date"),
            )


def test_read_json(tmp_path: Path) -> None:
    """Ensure JSON config files are read and errors are handled."""
    # valid JSON file
    config = {"data_elements": ["de1"], "start_date": "2024-01-01"}
    valid_file = tmp_path / "config.json"
    valid_file.write_text(json.dumps(config), encoding="utf-8")
    assert read_json(path=valid_file) == config

    # missing file
    with pytest.raises(FileNotFoundError, match="was not found"):
        read_json(path=tmp_path / "does_not_exist.json")

    # invalid JSON
    invalid_file = tmp_path / "invalid.json"
    invalid_file.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ValueError, match="not valid JSON"):
        read_json(path=invalid_file)


def test_validate_data() -> None:
    """Test data validation against expected schema."""
    sample_file = Path(__file__).parent / "data" / "sample_output.parquet"
    df = pl.read_parquet(sample_file)

    # should not fail
    validate(df).run()

    # empty dataframe
    with pytest.raises(DataValidationError):
        validate(df.head(0)).run()

    # unexpected column in dataframe
    df_extra = df.with_columns(pl.lit("x").alias("unexpected_col"))
    with pytest.raises(DataValidationError):
        validate(df_extra).run()
