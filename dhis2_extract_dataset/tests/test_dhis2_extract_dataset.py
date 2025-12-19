import re
import sys
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import config
import polars as pl
import pytest
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import Period

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import (
    add_ds_information,
    get_dataelements_with_no_data,
    get_dataset_org_units,
    get_datasets_as_dict,
    get_descendants,
    get_periods_with_no_data,
    isodate_to_period_type,
    valid_date,
    validate_ous_parameters,
)


def mock_get_paged(
    endpoint: str,
    params: dict | None = None,
) -> Iterable:
    """Mock dhis.api.get_paged for datasets.

    Returns
    -------
    dict
        The paged response from the API.
    """
    if endpoint == "dataSets":
        return config.datasets_pages_in
    return []


def mock_get(
    endpoint: str,
    params: dict | None = None,
) -> Iterable:
    """Mock dhis.api.get for datasets.

    Returns
    -------
    dict
        The paged response from the API.
    """
    return config.dataset_ous_in


class FakeDHIS2(DHIS2):
    """Fake DHIS2 for testing get_datasets_as_dict."""

    def __init__(self) -> None:
        self.api = MagicMock()
        self.api.get_paged.side_effect = mock_get_paged
        self.api.get.side_effect = mock_get


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


def test_get_datasets_as_dict():
    """Test get_datasets_as_dict function.

    We test:
    (1) The function returns a dictionary.
    (2) The values of the dictionary are the expected ones.
    """
    dhis = FakeDHIS2()
    result = get_datasets_as_dict(dhis)

    assert isinstance(result, dict)
    assert result == config.datasets_pages_out


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
    result = add_ds_information(config.before_add_cols, config.dictionary_ds, "ds1")

    assert isinstance(result, pl.DataFrame)
    assert result.equals(config.after_add_cols)

    empty_df = pl.DataFrame({"period": [], "value": []})
    empty_result = add_ds_information(empty_df, config.dictionary_ds, "ds1")

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
        config.data_with_periods_weird, config.start, config.end, config.dictionary_ds_one
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
        config.data_with_periods_okey, config.start, config.end, config.dictionary_ds_one
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
    get_dataelements_with_no_data(config.data_with_periods_weird, config.dictionary_ds_one)
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
    get_dataelements_with_no_data(config.data_with_periods_okey, config.dictionary_ds_one)
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


def test_get_dataset_org_units():
    """Test get_dataset_org_units function.

    We test:
    (1) That the function returns the correct org units for a dataset.
    """
    dhis = FakeDHIS2()
    result = get_dataset_org_units(dhis, "irrelevant")

    assert isinstance(result, list)
    assert result == config.dataset_ous_out


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
