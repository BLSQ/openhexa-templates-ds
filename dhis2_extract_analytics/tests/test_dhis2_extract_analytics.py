import re
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import check_periods, default_output_path


@patch("pipeline.workspace")
@patch("pipeline.datetime")
def test_default_output_path_creates_expected_directory_and_returns_correct_path(
    mock_datetime: MagicMock,
    mock_workspace: MagicMock,
    tmp_path: Path,
) -> None:
    """Test that `default_output_path` builds and returns the correct parquet file path.

    This test ensures that:
        - The returned path includes the workspace base path and expected folder hierarchy.
        - The timestamp in the path matches the mocked datetime.
        - The parent directory is automatically created.
        - The returned path ends with 'data_values.parquet'.

    Args:
        mock_datetime (MagicMock): Mock for datetime to make output predictable.
        mock_workspace (MagicMock): Mock for the global workspace path.
        tmp_path (pathlib.Path): Temporary workspace directory provided by pytest.
    """
    # Arrange
    fixed_datetime = datetime(2025, 10, 22, 12, 0, 0)
    mock_datetime.now.return_value = fixed_datetime
    mock_datetime.strftime = datetime.strftime
    mock_workspace.files_path = str(tmp_path)

    expected_dir = tmp_path / "pipelines" / "dhis2_extract_analytics" / "2025-10-22_12-00-00"
    expected_file = expected_dir / "data_values.parquet"

    # Act
    result = default_output_path()

    # Assert
    assert result == expected_file
    assert result.name == "data_values.parquet"
    assert expected_dir.exists(), "Parent directory should be created automatically"
    assert result.as_posix().startswith(str(tmp_path))
    assert "pipelines/dhis2_extract_analytics" in str(result)


@patch("pipeline.workspace")
@patch("pipeline.datetime")
def test_default_output_path_does_not_fail_if_directory_exists(
    mock_datetime: MagicMock,
    mock_workspace: MagicMock,
    tmp_path: Path,
) -> None:
    """Test that `default_output_path` succeeds when directory already exists.

    This verifies that `mkdir(parents=True, exist_ok=True)` prevents any error from being raised
    if the same directory is present from a previous run.
    """
    # Arrange
    fixed_datetime = datetime(2025, 10, 22, 12, 0, 0)
    mock_datetime.now.return_value = fixed_datetime
    mock_datetime.strftime = datetime.strftime
    mock_workspace.files_path = str(tmp_path)

    existing_dir = tmp_path / "pipelines" / "dhis2_extract_analytics" / "2025-10-22_12-00-00"
    existing_dir.mkdir(parents=True, exist_ok=True)

    # Act
    result = default_output_path()

    # Assert
    assert result.exists() is False  # File not created, only directory
    assert existing_dir.exists(), "Existing directory should remain intact"
    assert result == existing_dir / "data_values.parquet"


@patch("pipeline.current_run")
def test_check_periods(mock_run: MagicMock) -> None:
    """Test check_periods function.

    We test:
    (1) Raises ValueError when neither start_period nor period is provided.
    (2) Raises ValueError when period is not greater than 0.
    (3) Raises ValueError when start_period is None and end_period is also None
        (period type cannot be inferred for relative computation).
    (4) Raises ValueError when start_period is after end_period.
    (5) Valid combinations do not raise.
    """
    with pytest.raises(
        ValueError, match=re.escape("Either start_period or period must be provided.")
    ):
        check_periods(None, None, None)

    with pytest.raises(ValueError, match=re.escape("Period must be greater than 0.")):
        check_periods(None, "202306", 0)

    with pytest.raises(ValueError, match=re.escape("Period must be greater than 0.")):
        check_periods(None, "202306", -1)

    with pytest.raises(
        ValueError,
        match=re.escape("end_period must be provided when start_period is not provided."),
    ):
        check_periods(None, None, 3)

    with pytest.raises(
        ValueError,
        match=re.escape("Start period '202306' must not be after end period '202301'."),
    ):
        check_periods("202306", "202301", None)

    # Valid: start provided without period
    check_periods("202301", None, None)

    # Valid: period and end provided (start will be computed from end)
    check_periods(None, "202306", 3)

    # Valid: both start and end provided, start <= end
    check_periods("202301", "202306", None)


@patch("pipeline.current_run")
def test_check_periods_invalid_period_strings(mock_run: MagicMock) -> None:
    """Test that invalid DHIS2 period strings raise ValueError with a clear message."""
    with pytest.raises(ValueError, match="not a valid DHIS2 period string"):
        check_periods("not-a-period", None, None)

    with pytest.raises(ValueError, match="not a valid DHIS2 period string"):
        check_periods("202301", "not-a-period", None)


@patch("pipeline.current_run")
def test_check_periods_weekly_ordering(mock_run: MagicMock) -> None:
    """Test that weekly period ordering uses chronological comparison, not lexicographic.

    Lexicographically '2023W2' > '2023W10' (because '2' > '1'), but week 2 is before week 10.
    The old string comparison would incorrectly reject this valid range.
    """
    # Valid: week 2 before week 10 — would fail with lexicographic comparison
    check_periods("2023W2", "2023W10", None)

    # Invalid: week 10 before week 2
    with pytest.raises(
        ValueError,
        match=re.escape("Start period '2023W10' must not be after end period '2023W2'."),
    ):
        check_periods("2023W10", "2023W2", None)


@patch("pipeline.current_run")
def test_check_periods_type_mismatch(mock_run: MagicMock) -> None:
    """Test that mixing period types (e.g. monthly and weekly) raises ValueError."""
    with pytest.raises(ValueError, match="must be the same period type"):
        check_periods("202301", "2023W10", None)
