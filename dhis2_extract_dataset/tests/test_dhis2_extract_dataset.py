import re
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import config
import polars as pl
import pytest
from openhexa.toolbox.dhis2.periods import Period

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import (
    add_ds_information,
    check_dates,
    drop_null_values_with_comment,
    get_dataelements_with_no_data,
    get_dates,
    get_descendants,
    get_periods_with_no_data,
    isodate_to_period_type,
    set_date_range_delta,
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


def test_check_dates(monkeypatch: pytest.MonkeyPatch):
    """Test check_dates function.

    We test:
    (1) Raises ValueError when neither start nor period is provided.
    (2) Raises ValueError when period is not greater than 0.
    (3) Raises ValueError when start is after end.
    (4) Valid combinations do not raise.
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
        check_dates(None, "2025-01-31", None)

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

    fixed_today = date(2025, 6, 1)
    mock_date = MagicMock()
    mock_date.today.return_value = fixed_today
    monkeypatch.setattr("pipeline.date", mock_date)

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
        match=re.escape(
            "Please provide either (1) Orgunits or (2) Group(s) of orgunits"
        ),
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
    result = get_descendants(
        ["ou4", "ou10"], include_children=True, pyramid=config.pyramid
    )
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

    with pytest.raises(
        ValueError, match="Unsupported DHIS2 period type: UnsupportedType"
    ):
        isodate_to_period_type(config.date_str, "UnsupportedType")


def test_drop_null_values_with_comment(monkeypatch: pytest.MonkeyPatch):
    """Test drop_null_values_with_comment function.

    We test:
    (1) Rows with null value and a comment are dropped.
    (2) Rows with null value and no comment are kept.
    (3) Rows with a non-null value are kept regardless of comment.
    (4) The drop count is logged when rows are removed.
    """
    mock_run = MagicMock()
    monkeypatch.setattr("pipeline.run", mock_run)

    result = drop_null_values_with_comment(config.df_with_nulls)

    assert isinstance(result, pl.DataFrame)
    assert result.equals(config.df_after_drop_nulls_with_comment)
    mock_run.log_info.assert_called_once_with(
        "Dropped 1 rows with null value and a comment"
    )


def test_set_date_range_delta():
    """Test set_date_range_delta function.

    We test:
    (1) Daily -> 1 month. Daily uses datetime.timedelta internally (not relativedelta),
        and must be capped to 1 month.
    (2) Weekly variants (Monday-Sunday) -> 1 month. These use relativedelta(weeks=1),
        which is shorter than a month and must be capped.
    (3) Monthly -> 1 month. Exactly at the threshold, no change.
    (4) Longer period types (BiMonthly, Quarterly, SixMonthly, Yearly, Financial variants)
        -> DATE_RANGE_DELTA matches the period's natural duration.
    """
    for period, expected_delta in config.period_delta_cases:
        mock_dhis = MagicMock()
        set_date_range_delta(mock_dhis, period)
        assert expected_delta == mock_dhis.data_value_sets.DATE_RANGE_DELTA, (
            f"Failed for {type(period).__name__}: expected {expected_delta}, "
            f"got {mock_dhis.data_value_sets.DATE_RANGE_DELTA}"
        )
