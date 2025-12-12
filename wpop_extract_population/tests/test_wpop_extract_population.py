
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from pipeline import load_boundaries, retrieve_population_data
from worlpopclient import WorldPopClient

# ---------------------------------------------------------

# TESTS FOR retrieve_population_data

# ---------------------------------------------------------


@pytest.fixture
def mock_current_run():
    """Fixture that mocks the pipeline.current_run object for isolated testing.

    This fixture replaces the real `pipeline.current_run` with a mock object
    whose logging methods (`log_info`, `log_debug`, `log_warning`) are replaced
    with MagicMock instances. This prevents actual logging side effects during
    tests and allows assertions on logging behavior if needed.

    Yields
    ------
    MagicMock
        A mocked version of `pipeline.current_run` with logging methods patched.
    """
    with patch("pipeline.current_run") as m:
        m.log_info = MagicMock()
        m.log_debug = MagicMock()
        m.log_warning = MagicMock()
        yield m


def test_retrieve_population_data_skips_when_exists(tmp_path: Path, mock_current_run: MagicMock):
    """Test that retrieve_population_data returns the existing file and skips downloading.

    This test creates a mock .tif file in a temporary directory and verifies that
    when overwrite=False, the function detects the existing file and does not call
    the download method on WorldPopClient.

    Parameters
    ----------
    tmp_path : Path
        Pytest-provided temporary directory used to simulate the output path.
    mock_current_run : MagicMock
        Mocked current_run object with logging methods replaced for isolation.
    """
    # Setup
    existing_file = tmp_path / "cod_ppp_2020.tif"
    existing_file.touch()

    with (
        patch.object(WorldPopClient, "target_tif_filename", return_value="cod_ppp_2020.tif"),
        patch.object(WorldPopClient, "download_data_for_country") as mock_dl):
        out = retrieve_population_data(
                country_code="COD",
                output_path=tmp_path,
                overwrite=False,
            )

    assert out == existing_file
    mock_dl.assert_not_called()


def test_retrieve_population_data_downloads_when_missing(
        tmp_path: Path, mock_current_run: MagicMock):
    """Test that retrieve_population_data downloads the file when it does not exist.

    The test ensures that when no .tif file is present in the output directory,
    the function invokes the WorldPopClient download method and returns the path
    to the newly downloaded file.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory used as the output path.
    mock_current_run : MagicMock
        Mocked current_run object used to avoid real logging calls.
    """
    with (
        patch.object(WorldPopClient, "target_tif_filename", return_value="cod_ppp_2020.tif"),
        patch.object(WorldPopClient, "download_data_for_country") as mock_dl):
        out = retrieve_population_data(
                country_code="COD",
                output_path=tmp_path,
                overwrite=False,
            )

    assert out == tmp_path / "cod_ppp_2020.tif"
    mock_dl.assert_called_once()


# ---------------------------------------------------------

# TESTS FOR load_boundaries

# ---------------------------------------------------------


def test_load_boundaries_success(tmp_path: Path):
    """Test that load_boundaries successfully loads a valid GeoJSON file.

    This test writes a simple GeoDataFrame to disk as a GeoJSON file,
    then asserts that load_boundaries returns a non-empty GeoDataFrame.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory used for creating the test GeoJSON file.
    """
    geojson = tmp_path / "b.geojson"
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=gpd.points_from_xy([0], [0]))
    gdf.to_file(geojson, driver="GeoJSON")

    out = load_boundaries(geojson)
    assert isinstance(out, gpd.GeoDataFrame)
    assert not out.empty


def test_load_boundaries_empty_file(tmp_path: Path):
    """Test that load_boundaries raises a ValueError when the GeoJSON file is empty.

    The test writes an empty GeoDataFrame to a GeoJSON file and verifies that
    load_boundaries detects the empty content and raises a ValueError.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory used to create an empty GeoJSON file.
    """
    geojson = tmp_path / "empty.geojson"
    gpd.GeoDataFrame(geometry=[]).to_file(geojson, driver="GeoJSON")

    with pytest.raises(ValueError, match="The boundaries file is empty"):
        load_boundaries(geojson)


def test_load_boundaries_raises_on_error():
    """Test that load_boundaries raises an exception for invalid file paths.

    The test calls load_boundaries with a non-existent file and asserts that
    an exception is raised due to inability to read the file.
    """
    with pytest.raises(Exception, match="missing"):
        load_boundaries(Path("missing.geojson"))
