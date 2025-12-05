
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
from pipeline import (
    load_boundaries,
    retrieve_population_data,
    write_to_db,
)
from worlpopclient import WorldPopClient

# ---------------------------------------------------------

# TESTS FOR retrieve_population_data

# ---------------------------------------------------------


@pytest.fixture
def mock_current_run():  # noqa: D103
    with patch("pipeline.current_run") as m:
        m.log_info = MagicMock()
        m.log_debug = MagicMock()
        m.log_warning = MagicMock()
        yield m


def test_retrieve_population_data_skips_when_exists(tmp_path, mock_current_run):  # noqa: ANN001, D103
    # Setup
    existing_file = tmp_path / "cod_ppp_2020.tif"
    existing_file.touch()

    with patch.object(WorldPopClient, "target_tif_filename", return_value="cod_ppp_2020.tif"):  # noqa: SIM117
        with patch.object(WorldPopClient, "download_data_for_country") as mock_dl:
            out = retrieve_population_data(
                country_code="COD",
                output_path=tmp_path,
                overwrite=False,
            )

    assert out == existing_file
    mock_dl.assert_not_called()


def test_retrieve_population_data_downloads_when_missing(tmp_path, mock_current_run):  # noqa: ANN001, D103
    with patch.object(WorldPopClient, "target_tif_filename", return_value="cod_ppp_2020.tif"):  # noqa: SIM117
        with patch.object(WorldPopClient, "download_data_for_country") as mock_dl:
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


def test_load_boundaries_success(tmp_path):  # noqa: ANN001, D103
    geojson = tmp_path / "b.geojson"
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=gpd.points_from_xy([0], [0]))
    gdf.to_file(geojson, driver="GeoJSON")

    out = load_boundaries(geojson)
    assert isinstance(out, gpd.GeoDataFrame)
    assert not out.empty


def test_load_boundaries_empty_file(tmp_path):  # noqa: ANN001, D103
    geojson = tmp_path / "empty.geojson"
    gpd.GeoDataFrame(geometry=[]).to_file(geojson, driver="GeoJSON")

    with pytest.raises(ValueError):  # noqa: PT011
        load_boundaries(geojson)


def test_load_boundaries_raises_on_error():  # noqa: D103
    with pytest.raises(Exception):  # noqa: B017, PT011
        load_boundaries(Path("missing.geojson"))


# ---------------------------------------------------------

# TESTS FOR write_to_db

# ---------------------------------------------------------

def test_write_to_db_parquet_error():  # noqa: D103
    with patch("pandas.read_parquet", side_effect=Exception("badfile")):  # noqa: SIM117
        with pytest.raises(Exception):  # noqa: B017, PT011
            write_to_db(Path("bad.parquet"), "tbl")


@patch("pipeline.create_engine")
def test_write_to_db_sql_error(mock_engine):  # noqa: ANN001, D103
    df = pd.DataFrame({"a": [1]})
    with patch("pandas.read_parquet", return_value=df):  # noqa: SIM117
        with patch.object(df, "to_sql", side_effect=Exception("db error")):
            with pytest.raises(Exception):  # noqa: B017, PT011
                write_to_db(Path("ok.parquet"), "tbl")
