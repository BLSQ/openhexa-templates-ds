import re
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import config
import polars as pl
import pytest
from openhexa.toolbox.dhis2.periods import Period

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import (
    add_ds_information,
    get_dataelements_with_no_data,
    get_descendants,
    get_periods_with_no_data,
    isodate_to_period_type,
    valid_date,
    validate_ous_parameters,
)


def test_valid_date():
    """Test valid_date function.

    We test:
    (1) Valid date strings in ('YYYY-MM-DD', YYYYMMDD, YYYYWWDD, YYYYWW) format.
        (With this we test the function is_iso_date called inside the valid_date function.)
    (2) Invalid date strings.
        (With this we test the function is_iso_date called inside the valid_date function.)
    (3) Return current day if input is None.
    (4) Return dates in string
    """
    for valid_date_str in config.valid_dates:
        return_date = valid_date(valid_date_str)
        assert return_date == valid_date_str
        assert isinstance(return_date, str)

    for invalid_date_str in config.invalid_dates:
        with pytest.raises(
            ValueError,
            match=re.escape(
                f"Invalid date format: {invalid_date_str}. Expected ISO format (yyyy-mm-dd)."
            ),
        ):
            valid_date(invalid_date_str)

    return_date = valid_date(None)
    assert return_date == datetime.now().strftime("%Y-%m-%d")
    assert isinstance(return_date, str)


def test_validate_ous_parameters():
    """Test validate_ous_parameters function.

    We test:
    (1) Providing both orgUnits and orgUnitGroups parameters.
    (2) Providing none of orgUnits and orgUnitGroups parameters.
    (3) Providing only orgUnits parameter.
    (4) Providing only orgUnitGroups parameter.
    """
    with pytest.raises(
        ValueError,
        match="Please, choose only one option among",
    ):
        validate_ous_parameters(config.valid_ous, config.valid_ou_groups)
    with pytest.raises(
        ValueError,
        match=re.escape("Please provide either (1) Orgunits or (2) Group(s) of orgunits"),
    ):
        validate_ous_parameters(config.empty_ous, config.empty_ou_groups)

    validate_ous_parameters(config.valid_ous, config.empty_ou_groups)
    validate_ous_parameters(config.empty_ous, config.valid_ou_groups)


def test_add_ds_information():
    """Test add_ds_information function.

    We test:
    (1) Function returns a polars DataFrame.
    (2) The relevant columns are added correctly.
    (3) Empty DataFrame is returned unchanged.
    """
    result = add_ds_information(config.before_add_cols, config.df_ds_one)

    assert isinstance(result, pl.DataFrame)
    assert result.equals(config.after_add_cols)

    empty_df = pl.DataFrame({"period": [], "value": []})
    empty_result = add_ds_information(empty_df, config.df_ds_one)

    assert isinstance(empty_result, pl.DataFrame)
    assert empty_result.height == 0
    assert empty_result.columns == empty_df.columns


def test_get_periods_with_no_data(monkeypatch: pytest.MonkeyPatch):
    """Test get_periods_with_no_data function.

    We test:
    (1) Missing periods are logged correctly.
    (2) Extra periods are logged correctly.
    (3) Data with all periods does not log anything.
    """
    mock_run = MagicMock()
    monkeypatch.setattr("pipeline.run", mock_run)

    get_periods_with_no_data(
        config.data_with_periods_weird, config.start, config.end, config.df_ds_one
    )
    calls = [call[0][0] for call in mock_run.log_warning.call_args_list]

    assert len(calls) == 2
    assert (
        f"Following periods have no data: {config.missing_periods} for dataset Test Dataset"
        in calls
    )
    assert (
        f"Following periods not expected, but found: {config.extra_periods} "
        "for dataset Test Dataset" in calls
    )

    mock_run.log_warning.reset_mock()
    get_periods_with_no_data(
        config.data_with_periods_okey, config.start, config.end, config.df_ds_one
    )
    mock_run.log_warning.assert_not_called()


def test_get_dataelements_with_no_data(monkeypatch: pytest.MonkeyPatch):
    """Test get_dataelements_with_no_data function.

    We test:
    (1) Missing dataElements are logged correctly.
    (2) Extra dataElements are logged correctly.
    (3) Data with all dataElements does not log anything.
    """
    mock_run = MagicMock()
    monkeypatch.setattr("pipeline.run", mock_run)
    get_dataelements_with_no_data(config.data_with_periods_weird, config.df_ds_one)
    calls = [call[0][0] for call in mock_run.log_warning.call_args_list]

    assert len(calls) == 2
    assert (
        f"Following dataElements have no data: {config.missing_des} for dataset Test Dataset"
        in calls
    )
    assert (
        f"Following dataElements not expected, but found: {config.extra_des} "
        "for dataset Test Dataset" in calls
    )
    mock_run.log_warning.reset_mock()
    get_dataelements_with_no_data(config.data_with_periods_okey, config.df_ds_one)
    mock_run.log_warning.assert_not_called()


def test_get_descendants():
    """Test get_descendants function.

    We test:
    (1) When include_children=False, only parent OUs are returned.
    (2) When include_children=True, child OUs are included.
    """
    parent_ous = ["ou1", "ou3"]
    result = get_descendants(parent_ous, include_children=False, pyramid=config.pyramid)
    assert result == parent_ous
    result = get_descendants(["ou4", "ou10"], include_children=True, pyramid=config.pyramid)
    expected = ["ou4", "ou7", "ou8", "ou11", "ou12", "ou13", "ou10", "ou15", "ou16"]
    result = sorted(result)
    expected = sorted(expected)
    assert result == expected


def test_isodate_to_period_type():
    """Test isodate_to_period_type function.

    We test:
    (1) Daily, weekly, monthly, bi-monthly, quarterly, six-monthly, six-monthly April, yearly.
    (2) Financial year periods: April, July, October.
    (3) Weekly anchors (Monday-Sunday).
    (4) Unsupported period type raises ValueError.
    """
    for period_type, expected_str in config.expected_periods.items():
        period_obj = isodate_to_period_type(config.date_str, period_type)
        assert isinstance(period_obj, Period)
        assert str(period_obj) == expected_str, f"Failed for {period_type}"

    with pytest.raises(ValueError, match="Unsupported DHIS2 period type: UnsupportedType"):
        isodate_to_period_type(config.date_str, "UnsupportedType")
