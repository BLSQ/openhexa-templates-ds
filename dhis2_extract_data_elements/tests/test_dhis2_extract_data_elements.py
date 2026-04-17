import re
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import RequestParams, check_dates, get_dates, validate, validate_parameters
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
        check_dates(None, "2025-06-01", None)

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
