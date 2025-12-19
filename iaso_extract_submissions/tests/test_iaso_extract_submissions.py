import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from pipeline import (
    authenticate_iaso,
    clean_string,
    deduplicate_columns,
    fetch_submissions,
    get_form_name,
    parse_cutoff_date,
    process_choices,
)


class FakeIASOConnection:  # noqa: B903
    """Simple fake IASOConnection for testing."""

    def __init__(
            self, 
            url: str = "https://iaso.test", 
            username: str = "user", 
            password: str = "pass"):
        self.url = url
        self.username = username
        self.password = password    


def test_successful_iaso_authentication():
    """Should return IASO object and log success when authentication succeeds."""
    conn = FakeIASOConnection()

    with patch("pipeline.IASO") as mock_iaso, patch("pipeline.current_run") as mock_current_run:

        mock_iaso_instance = MagicMock()
        mock_iaso.return_value = mock_iaso_instance

        result = authenticate_iaso(conn)

        # checking that IASO is instantiated correctly
        mock_iaso.assert_called_once_with(conn.url, conn.username, conn.password)

        # checking that success log is written
        mock_current_run.log_info.assert_called_once_with("IASO authentication successful")

        # checking the returned object is IASO instance
        assert result == mock_iaso_instance


def test_authenticate_iaso_failure():
    """Should log error and raise RuntimeError when authentication fails."""
    conn = FakeIASOConnection()

    with patch("pipeline.IASO") as mock_iaso, patch("pipeline.current_run") as mock_current_run:
        mock_iaso.side_effect = Exception("Invalid credentials")

        with pytest.raises(RuntimeError) as excinfo:
            authenticate_iaso(conn)
        
        # Check that the error log is written
        mock_current_run.log_error.assert_called_once()
        assert "IASO authentication failed" in str(excinfo.value)

        # ensuring that the original exception message is preserved
        assert "Invalid credentials" in str(excinfo.value)


# -------------------------------------------------------------------
# clean_string tests
# -------------------------------------------------------------------


@pytest.mark.parametrize(
        ("input_str", "expected"),
    [
        ("Household Survey", "household_survey"),
        ("   Survey Name   ", "survey_name"),
        ("Form@Name#2024!", "formname2024"),
        ("École Santé", "ecole_sante"),
        ("Curaçao", "curacao"),
        ("Post-Test Form", "post-test_form"),
        ("Form   Name", "form___name"),
        ("Form_2024 V2", "form_2024_v2"),
        ("", ""),
        ("@#$%^&*", ""),
    ],
)
def test_clean_string(input_str: str, expected: str):
    """clean_string should normalize, sanitize, and format strings consistently."""
    assert clean_string(input_str) == expected


# -------------------------------------------------------------------
# get_form_name tests
# -------------------------------------------------------------------

class FakeResponse:
    """Minimal fake response object with json() method."""

    def __init__(self, payload: json):
        self._payload = payload

    def json(self):  # noqa: ANN201, D102
        return self._payload


def test_get_form_name_success():
    """Should fetch form name and return cleaned value."""
    iaso = MagicMock()
    iaso.api_client.get.return_value = FakeResponse(
        {"name": "  École Santé Form  "}
    )

    with patch("pipeline.current_run") as mock_current_run:
        result = get_form_name(iaso, form_id=123)

        iaso.api_client.get.assert_called_once_with(
            "/api/forms/123",
            params={"fields": {"name"}},
        )
        mock_current_run.log_error.assert_not_called()
        assert result == "ecole_sante_form"


def test_get_form_name_missing_name_field():
    """Checking missing form name.

    If name is missing, clean_string(None) will raise,
    which should be caught and re-raised as ValueError.
    """
    iaso = MagicMock()
    iaso.api_client.get.return_value = FakeResponse({})

    with patch("pipeline.current_run") as mock_current_run:
        with pytest.raises(ValueError):  # noqa: PT011
            get_form_name(iaso, form_id=456)

        mock_current_run.log_error.assert_called_once()


def test_get_form_name_api_failure():
    """Should log error and raise ValueError when API call fails."""
    iaso = MagicMock()
    iaso.api_client.get.side_effect = Exception("404 Not Found")

    with patch("pipeline.current_run") as mock_current_run:
        with pytest.raises(ValueError) as excinfo:  # noqa: PT011
            get_form_name(iaso, form_id=999)

        mock_current_run.log_error.assert_called_once()
        assert "Invalid form ID" in str(excinfo.value)


# -------------------------------------------------------------------
# parse_cutoff_date tests
# -------------------------------------------------------------------

@pytest.mark.parametrize(
    ("input_date", "expected"),
    [
        ("2024-01-01", "2024-01-01"),
        ("1999-12-31", "1999-12-31"),
    ],
)
def test_parse_cutoff_date_valid(input_date: str, expected: str):
    """Should return normalized ISO date for valid inputs."""
    with patch("pipeline.current_run") as mock_current_run:
        result = parse_cutoff_date(input_date)

        assert result == expected
        mock_current_run.log_error.assert_not_called()


@pytest.mark.parametrize("input_date", [None, ""])
def test_parse_cutoff_date_none_or_empty(input_date: str):
    """Should return None for None or empty string inputs."""
    with patch("pipeline.current_run") as mock_current_run:
        result = parse_cutoff_date(input_date)

        assert result is None
        mock_current_run.log_error.assert_not_called()


@pytest.mark.parametrize(
    "input_date",
    [
        "01-01-2024",   # wrong format
        "2024/01/01",   # wrong separator
        "2024-13-01",   # invalid month
        "2024-00-10",   # invalid month
        "2024-02-30",   # invalid day
        "abcd-ef-gh",   # not a date
    ],
)
def test_parse_cutoff_date_invalid_format(input_date: str):
    """Should log error and raise ValueError for invalid date strings."""
    with patch("pipeline.current_run") as mock_current_run:
        with pytest.raises(ValueError) as excinfo:  # noqa: PT011
            parse_cutoff_date(input_date)

        mock_current_run.log_error.assert_called_once_with(
            "Invalid date format - must be YYYY-MM-DD"
        )
        assert "Invalid date format" in str(excinfo.value)


# -------------------------------------------------------------------
# fetch_submissions tests
# -------------------------------------------------------------------

def test_fetch_submissions_success():
    """Should log info and return DataFrame when extraction succeeds."""
    iaso = MagicMock()
    form_id = 123
    cutoff_date = "2024-01-01"

    expected_df = pl.DataFrame(
        {
            "id": [1, 2],
            "value": ["a", "b"],
        }
    )

    with patch("pipeline.dataframe.extract_submissions") as mock_extract, \
         patch("pipeline.current_run") as mock_current_run:

        mock_extract.return_value = expected_df

        result = fetch_submissions(
            iaso=iaso,
            form_id=form_id,
            cutoff_date=cutoff_date,
        )

        mock_current_run.log_info.assert_called_once_with(
            f"Fetching submissions for form ID {form_id}"
        )
        mock_current_run.log_error.assert_not_called()

        mock_extract.assert_called_once_with(
            iaso, form_id, cutoff_date
        )

        assert isinstance(result, pl.DataFrame)
        assert result.to_dicts() == expected_df.to_dicts()


def test_fetch_submissions_success_without_cutoff_date():
    """Should pass None cutoff_date through to extract_submissions."""
    iaso = MagicMock()
    form_id = 456

    expected_df = pl.DataFrame({"id": []})

    with patch("pipeline.dataframe.extract_submissions") as mock_extract, \
         patch("pipeline.current_run") as mock_current_run:

        mock_extract.return_value = expected_df

        result = fetch_submissions(
            iaso=iaso,
            form_id=form_id,
            cutoff_date=None,
        )

        mock_extract.assert_called_once_with(
            iaso, form_id, None
        )
        mock_current_run.log_error.assert_not_called()

        assert result.to_dicts() == expected_df.to_dicts()


def test_fetch_submissions_failure():
    """Should log error and re-raise exception when extraction fails."""
    iaso = MagicMock()
    form_id = 999
    cutoff_date = "2024-01-01"

    with patch("pipeline.dataframe.extract_submissions") as mock_extract, \
         patch("pipeline.current_run") as mock_current_run:

        mock_extract.side_effect = RuntimeError("API timeout")

        with pytest.raises(RuntimeError):
            fetch_submissions(
                iaso=iaso,
                form_id=form_id,
                cutoff_date=cutoff_date,
            )

        mock_current_run.log_info.assert_called_once_with(
            f"Fetching submissions for form ID {form_id}"
        )
        mock_current_run.log_error.assert_called_once()

# -------------------------------------------------------------------
# process_choices tests
# -------------------------------------------------------------------


@pytest.fixture
def submissions_df() -> pl.DataFrame:
    """Sample submissions DataFrame."""  # noqa: DOC201
    return pl.DataFrame(
        {
            "choice": ["A", "B"],
            "value": [1, 2],
        }
    )


@pytest.fixture
def mock_iaso_client() -> MagicMock:
    """Mock IASO client."""  # noqa: DOC201
    return MagicMock()


def test_process_choices_no_conversion(
    submissions_df: pl.DataFrame, mock_iaso_client: MagicMock
):
    """When convert=False, the function should return the input DataFrame unchanged."""
    result = process_choices(
        submissions=submissions_df,
        convert=False,
        iaso_client=mock_iaso_client,
        form_id=123,
    )

    assert result is submissions_df


@patch("pipeline.dataframe.replace_labels")
@patch("pipeline.dataframe.get_form_metadata")
def test_process_choices_with_conversion_success(
    mock_get_form_metadata: MagicMock,
    mock_replace_labels: MagicMock,
    submissions_df: pl.DataFrame,
    mock_iaso_client: MagicMock,
):
    """When convert=True and no error occurs, labels should be replaced using form metadata."""
    form_metadata = {"choices": {"A": "Alpha", "B": "Beta"}}
    converted_df = submissions_df.with_columns(
        pl.Series("choice", ["Alpha", "Beta"])
    )

    mock_get_form_metadata.return_value = form_metadata
    mock_replace_labels.return_value = converted_df

    result = process_choices(
        submissions=submissions_df,
        convert=True,
        iaso_client=mock_iaso_client,
        form_id=456,
    )

    mock_get_form_metadata.assert_called_once_with(mock_iaso_client, 456)
    mock_replace_labels.assert_called_once_with(
        submissions=submissions_df,
        form_metadata=form_metadata,
        language="French",
    )

    assert result.to_dicts() == converted_df.to_dicts()


@patch("pipeline.current_run")
@patch("pipeline.dataframe.get_form_metadata")
def test_process_choices_logs_and_raises_on_error(
    mock_get_form_metadata: MagicMock,
    mock_current_run: MagicMock,
    submissions_df: pl.DataFrame,
    mock_iaso_client: MagicMock,
):
    """If an exception occurs during conversion, the error should be logged and exception raised."""
    mock_get_form_metadata.side_effect = RuntimeError("IASO unavailable")

    with pytest.raises(RuntimeError, match="IASO unavailable"):
        process_choices(
            submissions=submissions_df,
            convert=True,
            iaso_client=mock_iaso_client,
            form_id=789,
        )

    mock_current_run.log_error.assert_called_once()
    assert "Choice conversion failed" in mock_current_run.log_error.call_args[0][0]


@patch("pipeline._process_submissions")
@patch("pipeline.clean_string")
def test_deduplicate_columns_renames_duplicates(
    mock_clean_string: MagicMock,
    mock_process: MagicMock,
):
    """Renames duplicate columns by appending incremental suffixes.

    Args:
        mock_clean_string: Mocked clean_string function.
        mock_process: Mocked _process_submissions function.
    """
    df = pl.DataFrame(
        {
            "Age": [10, 20],
            "age": [30, 40],
            "AGE": [50, 60],
        }
    )

    mock_clean_string.side_effect = lambda x: "age"
    mock_process.side_effect = lambda df: df

    result = deduplicate_columns(df)

    assert result.columns == ["age_1", "age_2", "age_3"]
    mock_process.assert_called_once()


@patch("pipeline._process_submissions")
@patch("pipeline.clean_string")
def test_deduplicate_columns_keeps_unique_columns(
    mock_clean_string: MagicMock,
    mock_process: MagicMock,
):
    """Leaves columns unchanged when no duplicates exist after cleaning.

    Args:
        mock_clean_string: Mocked clean_string function.
        mock_process: Mocked _process_submissions function.
    """
    df = pl.DataFrame(
        {
            "Height": [170, 180],
            "Weight": [70, 80],
        }
    )

    mock_clean_string.side_effect = lambda x: x.lower()
    mock_process.side_effect = lambda df: df

    result = deduplicate_columns(df)

    assert result.columns == ["height", "weight"]
    mock_process.assert_called_once()


@patch("pipeline._process_submissions")
@patch("pipeline.clean_string")
def test_deduplicate_columns_mixed_duplicates_and_unique(
    mock_clean_string: MagicMock,
    mock_process: MagicMock,
):
    """Handles mixed duplicate and unique columns correctly.

    Args:
        mock_clean_string: Mocked clean_string function.
        mock_process: Mocked _process_submissions function.

    """
    df = pl.DataFrame(
        {
            "City": ["A", "B"],
            "city": ["C", "D"],
            "Country": ["X", "Y"],
        }
    )

    mock_clean_string.side_effect = lambda x: x.lower()
    mock_process.side_effect = lambda df: df

    result = deduplicate_columns(df)

    assert result.columns == ["city_1", "city_2", "country"]
    mock_process.assert_called_once()


@patch("pipeline._process_submissions")
@patch("pipeline.clean_string")
def test_deduplicate_columns_suffix_order_is_stable(
    mock_clean_string: MagicMock,
    mock_process: MagicMock,
):
    """Applies suffixes in the original column order.

    Args:
        mock_clean_string: Mocked clean_string function.
        mock_process: Mocked _process_submissions function.
    """
    df = pl.DataFrame(
        {
            "B": [1],
            "b": [2],
            "B ": [3],
        }
    )

    mock_clean_string.side_effect = lambda x: "b"
    mock_process.side_effect = lambda df: df

    result = deduplicate_columns(df)

    assert result.columns == ["b_1", "b_2", "b_3"]


@patch("pipeline._process_submissions")
@patch("pipeline.clean_string")
def test_deduplicate_columns_calls_process_submissions(
    mock_clean_string: MagicMock,
    mock_process: MagicMock,
):
    """Calls _process_submissions after renaming columns.

    Args:
        mock_clean_string: Mocked clean_string function.
        mock_process: Mocked _process_submissions function.
    """
    df = pl.DataFrame({"A": [1], "a": [2]})

    mock_clean_string.side_effect = lambda x: x.lower()
    mock_process.return_value = "processed"

    result = deduplicate_columns(df)

    mock_process.assert_called_once()
    assert result == "processed"
