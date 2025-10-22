from datetime import datetime
from unittest.mock import patch

# Import from your actual pipeline module
from dhis2_extract_analytics.pipeline import default_output_path


@patch("dhis2_extract_analytics.pipeline.workspace")
@patch("dhis2_extract_analytics.pipeline.datetime")
def test_default_output_path_creates_expected_directory_and_returns_correct_path(
    mock_datetime, mock_workspace, tmp_path  # noqa: ANN001
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

    expected_dir = (
        tmp_path / "pipelines" / "dhis2_extract_analytics" / "2025-10-22_12-00-00"
    )
    expected_file = expected_dir / "data_values.parquet"

    # Act
    result = default_output_path()

    # Assert
    assert result == expected_file
    assert result.name == "data_values.parquet"
    assert expected_dir.exists(), "Parent directory should be created automatically"
    assert result.as_posix().startswith(str(tmp_path))
    assert "pipelines/dhis2_extract_analytics" in str(result)


@patch("dhis2_extract_analytics.pipeline.workspace")
@patch("dhis2_extract_analytics.pipeline.datetime")
def test_default_output_path_does_not_fail_if_directory_exists(
    mock_datetime, mock_workspace, tmp_path  # noqa: ANN001
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

    existing_dir = (
        tmp_path / "pipelines" / "dhis2_extract_analytics" / "2025-10-22_12-00-00"
    )
    existing_dir.mkdir(parents=True, exist_ok=True)

    # Act
    result = default_output_path()

    # Assert
    assert result.exists() is False  # File not created, only directory
    assert existing_dir.exists(), "Existing directory should remain intact"
    assert result == existing_dir / "data_values.parquet"
