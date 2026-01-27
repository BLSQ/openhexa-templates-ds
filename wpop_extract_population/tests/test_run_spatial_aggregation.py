import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from pipeline import run_spatial_aggregation
from shapely.geometry import Polygon


class MockRaster:
    """Mock object for `rasterio.open` context manager.

    Attributes
    ----------
    crs : str
        The coordinate reference system of the raster.
    nodata : str
        The nodata value of the raster.
    """

    def __init__(self, crs: str, nodata: str):
        """Init method.

        Parameters
        ----------
        crs : str
            Coordinate reference system to mock.
        nodata : str
            Nodata value to mock.
        """
        self.crs = crs
        self.nodata = nodata

    def __enter__(self) -> "MockRaster":
        """Enter the context manager.

        Returns
        -------
        MockRaster
            The mock raster object itself.
        """
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None,
                 exc_tb: types.TracebackType | None) -> None:
        """Exit the context manager.

        Parameters
        ----------
        exc_type : type[BaseException] | None
            The exception type, if any.
        exc_val : BaseException | None
            The exception value, if any.
        exc_tb : types.TracebackType | None
            The traceback, if any.
        """
        pass


@pytest.fixture
def mock_current_run():  # noqa: D103
    with patch("pipeline.current_run") as m:
        m.log_info = MagicMock()
        m.log_debug = MagicMock()
        m.log_warning = MagicMock()
        m.add_file_output = MagicMock()
        yield m


@pytest.fixture
def fake_boundaries(tmp_path: Path) -> Path:
    """Fixture to mock the `current_run` object used in pipelines.

    Returns
    -------
    MagicMock
        A mock of the current_run object with log and file output methods.
    """
    boundaries = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    path = tmp_path / "boundaries.geojson"
    boundaries.to_file(path, driver="GeoJSON")
    return path


def test_run_spatial_aggregation_missing_tif(
        fake_boundaries: Path, tmp_path: Path, mock_current_run: MagicMock):
    """Fixture to create a fake GeoJSON file for boundaries.

    Parameters
    ----------
    fake_boundaries : Path
        Path to fake boundaries GeoJSON.
    tmp_path : Path
        Temporary directory provided by pytest.
    mock_current_run : MagicMock
        Mocked pipeline current_run object.
    Path
        Path to the saved fake boundaries GeoJSON file.
    """
    missing_tif = tmp_path / "missing.tif"
    with pytest.raises(FileNotFoundError, match="WorldPop file not found"):
        run_spatial_aggregation(missing_tif, fake_boundaries, tmp_path)


def test_run_spatial_aggregation_missing_boundaries(tmp_path: Path, mock_current_run: MagicMock):
    """Test that the aggregation function raises FileNotFoundError for missing boundaries file.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for the test.
    mock_current_run : MagicMock
        Mocked pipeline current_run object.
    """
    tif = tmp_path / "file.tif"
    tif.touch()
    with pytest.raises(FileNotFoundError, match="Boundaries file not found"):
        run_spatial_aggregation(tif, Path("missing.geojson"), tmp_path)


def test_run_spatial_aggregation_success(
        tmp_path: Path, fake_boundaries: Path, mock_current_run: MagicMock):
    """Test successful spatial aggregation execution.

    Checks that output file is created and logging is performed.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for the test.
    fake_boundaries : Path
        Path to fake boundaries GeoJSON.
    mock_current_run : MagicMock
        Mocked pipeline current_run object.
    """
    tif = tmp_path / "pop.tif"
    tif.touch()

    # Create fake zonal_stats output with POPULATION
    fake_stats = [{"properties": {"id": 1, "population": 100, "pixel_count": 10}, "geometry": None}]

    with patch("rasterio.open", return_value=MockRaster(crs="EPSG:4326", nodata=-9999)), \
         patch("pipeline.zonal_stats", return_value=fake_stats):
        out = run_spatial_aggregation(tif, fake_boundaries, tmp_path)

    assert out.exists()
    assert out.suffix == ".parquet"
    mock_current_run.log_info.assert_any_call(
        f"Running spatial aggregation with WorldPop data {tif}")


def test_run_spatial_aggregation_reprojects(tmp_path: Path, mock_current_run: MagicMock):
    """Test that spatial aggregation reprojects boundaries when CRS differs from raster.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for the test.
    mock_current_run : MagicMock
        Mocked pipeline current_run object.
    """
    tif = tmp_path / "pop.tif"
    tif.touch()

    boundaries = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    boundaries_path = tmp_path / "b.geojson"
    boundaries.to_file(boundaries_path, driver="GeoJSON")

    fake_stats = [{"properties": {"id": 1, "population": 100, "pixel_count": 10}, "geometry": None}]

    with patch("rasterio.open", return_value=MockRaster(crs="EPSG:3857", nodata=-9999)), \
         patch("pipeline.zonal_stats", return_value=fake_stats):
        run_spatial_aggregation(tif, boundaries_path, tmp_path)

    mock_current_run.log_info.assert_any_call(
        "Boundaries CRS EPSG:4326 differs from raster CRS EPSG:3857. "
        "Reprojecting boundaries to EPSG:3857"
    )
