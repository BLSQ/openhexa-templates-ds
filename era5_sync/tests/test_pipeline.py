import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import polars as pl
import xarray as xr
from openhexa.toolbox.era5.extract import Cache, Client, Request
from openhexa.toolbox.era5.transform import Period

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import process_variables, sync_variables


def mock_retrieve_requests(
    client: Client, dataset_id: str, requests: list[Request], dst_dir: Path, cache: Cache
) -> None:
    """Copy test GRIB files into a temporary repository instead of making API calls."""
    print(f"Retrieving {len(requests)} requests into {dst_dir}")
    variable = requests[0]["variable"][0]
    test_data_dir = Path(__file__).parent / "data" / variable
    for grib_file in test_data_dir.glob("*.grib"):
        shutil.copy(grib_file, dst_dir / grib_file.name)
        print(f"Copied {grib_file} to {dst_dir / grib_file.name}.")


@patch("era5_sync.pipeline.retrieve_requests")
def test_sync_variables(mock_retrieve: Mock) -> None:
    """Test sync_variables task.

    NB: The test mocks the retrieve_requests function to avoid making API calls
    to the climate data store. We use test GRIB files stored in the tests/data
    directory instead.

    We also mock the Client because prepare_requests() make an API call to get
    collection start and end dates.
    """
    mock_retrieve.side_effect = mock_retrieve_requests
    mock_client = MagicMock()
    mock_client.get_collection.return_value = Mock(
        begin_datetime=datetime(2020, 1, 1),
        end_datetime=datetime(2025, 12, 31),
    )
    boundaries_file = Path(__file__).parent / "data" / "test_boundaries.geojson"
    start_date = "2024-12-01"
    end_date = "2025-02-03"
    variables = ["2m_temperature", "total_precipitation", "2m_dewpoint_temperature"]
    with tempfile.TemporaryDirectory() as tmp_dir:
        output_dir = Path(tmp_dir)
        output_dir = Path("data/tests_output")
        task = sync_variables(
            client=mock_client,
            start_date=start_date,
            boundaries_file=boundaries_file,
            variables=variables,
            output_dir=output_dir,
            cache=None,
            end_date=end_date,
        )
        task.run()
        # 3 zarr stores created - one per variable
        assert len(list(output_dir.glob("*.zarr"))) == 3
        ds = xr.open_dataset(
            output_dir / "2m_dewpoint_temperature.zarr", engine="zarr", decode_timedelta=False
        )
        assert len(ds.time) == 260  # n_steps (4) * n_days (65)
        assert len(ds.latitude) == 21
        assert len(ds.longitude) == 21
        ds = xr.open_dataset(
            output_dir / "2m_temperature.zarr", engine="zarr", decode_timedelta=False
        )
        assert len(ds.time) == 260  # n_steps (4) * n_days (65)
        assert len(ds.latitude) == 21
        assert len(ds.longitude) == 21
        ds = xr.open_dataset(
            output_dir / "total_precipitation.zarr", engine="zarr", decode_timedelta=False
        )
        assert len(ds.time) == 65  # n_steps (1) * n_days (65)
        assert len(ds.latitude) == 21
        assert len(ds.longitude) == 21


def test_process_variables() -> None:
    """Test process_variables task."""
    src_dir = Path(__file__).parent / "data"
    boundaries_file = Path(__file__).parent / "data" / "test_boundaries.geojson"
    boundaries_id_col = "boundary_id"
    periods = [Period.DAY, Period.WEEK, Period.MONTH]
    with tempfile.TemporaryDirectory() as tmp_dir:
        output_dir = Path(tmp_dir)
        task = process_variables(
            src_dir=src_dir,
            boundaries_file=boundaries_file,
            boundaries_id_col=boundaries_id_col,
            periods=periods,
            output_dir=output_dir,
        )
        task.run()

        # For instantaneous variables, we expect min, max and mean aggregations for all
        # period types
        for var in ("t2m", "d2m", "rh"):
            for period in ("day", "week", "month"):
                for agg in ("min", "max", "mean"):
                    fp = output_dir / f"{var}_{agg}_{period}.parquet"
                    assert fp.exists(), f"Expected file {fp} does not exist."
                    df = pl.read_parquet(fp)
                    assert df.schema == pl.Schema(
                        {
                            "boundary": pl.String,
                            "period": pl.String,
                            "value": pl.Float64,
                        }
                    )

        # For accumulation variables, we expect only sum aggregation for all period
        # types
        var = "tp"
        for period in ("day", "week", "month"):
            fp = output_dir / f"{var}_{period}.parquet"
            assert fp.exists(), f"Expected file {fp} does not exist."
            df = pl.read_parquet(fp)
            assert df.schema == pl.Schema(
                {
                    "boundary": pl.String,
                    "period": pl.String,
                    "value": pl.Float64,
                }
            )
