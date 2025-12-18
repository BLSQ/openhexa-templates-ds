import json
from unittest.mock import MagicMock, patch

import pytest
from pipeline import authenticate_iaso, clean_string, get_form_name


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