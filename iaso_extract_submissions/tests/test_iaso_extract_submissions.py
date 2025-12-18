from unittest.mock import MagicMock, patch

import pytest
from pipeline import authenticate_iaso


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