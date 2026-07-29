import json
import re
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import (
    RequestParams,
    check_dates,
    extract_params_from_config,
    get_dates,
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
    config_1 = {
        "data_elements": ["de1", "de2"],
        "organisation_units": ["ou1"],
        "include_children": True,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
    }
    config_2 = {"data_elements": ["de1"]}
    config_3 = {"data_elements": ["de1"], "period": 3}

    params, start_date, end_date, period = extract_params_from_config(config=config_1)
    assert params.data_elements == ["de1", "de2"]
    assert params.data_element_groups is None
    assert params.organisation_units == ["ou1"]
    assert params.organisation_unit_groups is None
    assert params.include_children is True
    assert params.start_date is None
    assert params.end_date is None
    assert start_date == "2024-01-01"
    assert end_date == "2024-12-31"
    assert period is None

    params, start_date, end_date, period = extract_params_from_config(config=config_2)
    assert params.data_element_groups is None
    assert params.organisation_units is None
    assert params.organisation_unit_groups is None
    assert params.include_children is False
    assert start_date is None
    assert end_date is None
    assert period is None

    _, _, _, period = extract_params_from_config(config=config_3)
    assert period == 3


@pytest.mark.parametrize(
    "dates",
    [
        {"start_date": "2024-01-01", "end_date": "2024-12-31"},
        {"start_date": "2024-01-01"},
        {"period": 3},
        {"end_date": "2024-12-31", "period": 3},
        {"start_date": "2024-01-01", "end_date": "2024-06-01", "period": 3},
    ],
)
def test_config_file_and_parameters_are_equivalent(
    dates: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure both ways of providing the parameters resolve to the same request.

    A configuration file and the equivalent pipeline parameters must produce
    identical RequestParams, including the resolved start and end dates.
    """
    monkeypatch.setattr("pipeline.run", MagicMock())

    # via a configuration file
    config = {"data_elements": ["de1"], "organisation_units": ["ou1"], **dates}
    from_config, start_date, end_date, period = extract_params_from_config(config=config)
    check_dates(start_date, end_date, period)
    start_date, end_date = get_dates(start_date, end_date, period)
    from_config = replace(from_config, start_date=start_date, end_date=end_date)

    # via the pipeline parameters
    from_params = RequestParams(
        data_elements=["de1"],
        data_element_groups=None,
        organisation_units=["ou1"],
        organisation_unit_groups=None,
        include_children=False,
    )
    start_date, end_date, period = (
        dates.get("start_date"),
        dates.get("end_date"),
        dates.get("period"),
    )
    check_dates(start_date, end_date, period)
    start_date, end_date = get_dates(start_date, end_date, period)
    from_params = replace(from_params, start_date=start_date, end_date=end_date)

    assert from_config == from_params


def test_raise_if_parameters_set() -> None:
    """Ensure conflicting parameters are rejected when a config file is used."""
    # no extraction parameters set: should not raise
    raise_if_parameters_set(None, None, None, None, None, None, None)

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
        {"period": 3},
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
                period=kwargs.get("period"),
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


def test_check_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test check_dates function.

    We test:
    (1) Raises ValueError when neither start nor period is provided.
    (2) Raises ValueError when period is not greater than 0.
    (3) Raises ValueError when period is not a whole number.
    (4) Raises ValueError when start is after end.
    (5) Valid combinations do not raise.
    """
    mock_run = MagicMock()
    monkeypatch.setattr("pipeline.run", mock_run)

    with pytest.raises(
        ValueError, match=re.escape("Either start date or period must be provided.")
    ):
        check_dates(None, None, None)

    with pytest.raises(
        ValueError, match=re.escape("Either start date or period must be provided.")
    ):
        check_dates(None, "2025-06-01", None)

    # a period coming from a configuration file is not type checked by openhexa
    with pytest.raises(ValueError, match=re.escape("must be a whole number of months")):
        check_dates(None, "2025-06-01", "3")

    with pytest.raises(ValueError, match=re.escape("must be a whole number of months")):
        check_dates(None, "2025-06-01", 3.5)

    with pytest.raises(ValueError, match=re.escape("Period must be greater than 0.")):
        check_dates(None, "2025-06-01", 0)

    with pytest.raises(ValueError, match=re.escape("Period must be greater than 0.")):
        check_dates(None, "2025-06-01", -1)

    with pytest.raises(
        ValueError,
        match=re.escape("Start date 2025-06-01 must not be after end date 2025-01-01."),
    ):
        check_dates("2025-06-01", "2025-01-01", None)

    # Valid: start provided without period
    check_dates("2025-01-01", None, None)

    # Valid: period provided without start
    check_dates(None, "2025-06-01", 3)

    # Valid: both start and period provided, start <= end
    check_dates("2025-01-01", "2025-06-01", 3)


def test_get_dates(monkeypatch: pytest.MonkeyPatch):
    """Test get_dates function.

    We test:
    (1) When end is None, today's date is used.
    (2) When start is None, it is calculated as end - period months.
    (3) When both are provided, they are returned as-is.
    """
    mock_run = MagicMock()
    monkeypatch.setattr("pipeline.run", mock_run)

    mock_datetime = MagicMock()
    mock_datetime.now.return_value.strftime.return_value = "2025-06-01"
    mock_datetime.strptime = datetime.strptime
    monkeypatch.setattr("pipeline.datetime", mock_datetime)

    # When end is None, today's date is used
    start, end = get_dates("2025-01-01", None, None)
    assert end == "2025-06-01"
    assert start == "2025-01-01"

    # When start is None, it is calculated as end - period months
    start, end = get_dates(None, "2025-06-01", 3)
    assert start == "2025-03-01"
    assert end == "2025-06-01"

    # When only period is provided, end defaults to today and start is calculated
    start, end = get_dates(None, None, 3)
    assert end == "2025-06-01"
    assert start == "2025-03-01"

    # When both are provided, they are returned as-is
    start, end = get_dates("2025-01-01", "2025-06-01", None)
    assert start == "2025-01-01"
    assert end == "2025-06-01"
