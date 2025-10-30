from __future__ import annotations

import logging
import tempfile
from collections.abc import Sequence
from datetime import date, datetime
from math import ceil, floor
from pathlib import Path

import geopandas as gpd
import xarray as xr
from openhexa.sdk import CustomConnection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.era5.cache import Cache
from openhexa.toolbox.era5.extract import (
    Client,
    grib_to_zarr,
    prepare_requests,
    retrieve_requests,
)
from openhexa.toolbox.era5.transform import (
    Period,
    aggregate_in_space,
    aggregate_in_time,
    calculate_relative_humidity,
    calculate_wind_speed,
    create_masks,
)
from openhexa.toolbox.era5.utils import get_variables

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)
logging.getLogger("openhexa.toolbox.era5").setLevel(logging.INFO)


@pipeline("era5_sync")
@parameter(
    code="start_date",
    type=str,
    name="Start date",
    help="Start date of extraction period",
    default="2020-01-01",
)
@parameter(
    code="end_date",
    type=str,
    name="End date",
    help="End date of extraction period (latest available by default)",
    required=False,
)
@parameter(
    code="cds_connection",
    name="Climate data store",
    type=CustomConnection,
    help="Credentials for connection to the Copernicus Climate Data Store",
    required=True,
)
@parameter(
    code="variables",
    name="Variables",
    type=str,
    multiple=True,
    help="ERA5-Land variables to sync",
    required=True,
    default=[
        "2m_dewpoint_temperature",
        "2m_temperature",
        "total_precipitation",
    ],
)
@parameter(
    code="boundaries_file",
    name="Path to boundaries file",
    type=str,
    help="Path to the boundaries file to use to determine the area to extract",
    required=True,
)
@parameter(
    code="boundaries_id_col",
    name="Boundaries ID column",
    type=str,
    help="Column in the boundaries file to use as identifier",
    required=True,
)
@parameter(
    code="output_dir",
    name="Output directory",
    type=str,
    help="Output directory for the downloaded data",
    required=True,
    default="pipelines/era5_sync/data",
)
def era5_sync(
    start_date: str,
    cds_connection: CustomConnection,
    boundaries_file: str,
    boundaries_id_col: str,
    output_dir: str,
    variables: list[str],
    end_date: str | None = None,
) -> None:
    """Synchronize ERA5-Land data from the Copernicus Climate Data Store (CDS).

    Checks for missing dates in the local dataset and downloads only the missing data.
    The data is downloaded in GRIB format and converted to Zarr format for easier
    processing.

    NB: Variables are synced individually in a sequential manner to avoid concurrent
    writes to the zarr store and overloading the CDS API with too many data requests.

    Args:
        start_date: Start date of the extraction period (YYYY-MM-DD).
        end_date: End date of the extraction period (YYYY-MM-DD, today by default).
        cds_connection: Credentials for connection to the CDS API.
        boundaries_file: Path to the boundaries file to use to determine the area to extract.
        boundaries_id_col: Column in the boundaries file to use as identifier.
        output_dir: Output directory for the downloaded data.
        variables: List of variables to sync.

    """
    output_path = Path(workspace.files_path, output_dir)
    raw_dir = output_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    cds_api_url: str = cds_connection.url  # type: ignore
    cds_api_key: str = cds_connection.api_key  # type: ignore

    client = Client(url=cds_api_url, key=cds_api_key, retry_after=30)
    cache = Cache(
        database_uri=workspace.database_url, cache_dir=Path(workspace.files_path) / "cache"
    )

    if not variables:
        raise ValueError("At least one variable must be selected for extraction.")

    task1 = sync_variables(
        client=client,
        cache=cache,
        start_date=start_date,
        end_date=end_date,
        boundaries_file=Path(workspace.files_path, boundaries_file),
        variables=variables,
        output_dir=output_path,
    )

    periods = [Period.DAY, Period.WEEK, Period.MONTH]

    process_variables(
        src_dir=output_path,
        boundaries_file=Path(workspace.files_path, boundaries_file),
        boundaries_id_col=boundaries_id_col,
        periods=periods,
        output_dir=output_path,
        wait_for=task1,
    )


@era5_sync.task
def sync_variables(
    client: Client,
    start_date: str,
    boundaries_file: Path,
    variables: Sequence[str],
    output_dir: Path,
    cache: Cache | None = None,
    end_date: str | None = None,
) -> bool:
    """Synchronize data for ERA5-Land variables.

    Args:
        client: CDS API client.
        start_date: Start date of the extraction period (YYYY-MM-DD).
        boundaries_file: Path to the boundaries file to use to determine the area to extract.
        variables: List of variables to sync.
        output_dir: Output directory for the downloaded data.
        cache: Cache for ERA5 toolbox.
        end_date: End date of the extraction period (YYYY-MM-DD, today by default).

    Returns:
        True when task is complete.

    """
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    if end_date:
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_dt = datetime.now().date()

    boundaries = _read_boundaries(boundaries_file)
    area = _get_area_from_boundaries(boundaries)

    metadata = get_variables()

    for variable in variables:
        if current_run:
            current_run.log_info(f"Syncing variable '{variable}'")

        zarr_store = output_dir / f"{variable}.zarr"
        zarr_store.parent.mkdir(parents=True, exist_ok=True)

        variable_short_name = metadata[variable]["short_name"]

        _sync_variable(
            client=client,
            variable=variable,
            data_var=variable_short_name,
            start_date_dt=start_date_dt,
            end_date_dt=end_date_dt,
            area=area,
            zarr_store=zarr_store,
            cache=cache,
        )

        if current_run:
            current_run.log_info(f"Variable '{variable}' synced successfully.")

    return True


def _sync_variable(
    client: Client,
    variable: str,
    data_var: str,
    start_date_dt: date,
    end_date_dt: date,
    area: tuple[int, int, int, int],
    zarr_store: Path,
    cache: Cache | None = None,
) -> None:
    """Synchronize a single variable for the specified date range and area.

    Args:
        client: CDS API client.
        variable: Name of the variable to sync (e.g. '2m_temperature').
        data_var: Short name of the variable in the dataset (e.g. 't2m' for 2m_temperature).
        start_date_dt: Start date of the extraction period.
        end_date_dt: End date of the extraction period.
        area: Area to extract (ymax, xmin, ymin, xmax).
        zarr_store: Path to the Zarr store for the variable.
        cache: Cache for ERA5 toolbox.
    """
    with tempfile.TemporaryDirectory(delete=False) as tmp_dir:
        raw_dir = Path(tmp_dir)
        requests = prepare_requests(
            client=client,
            dataset_id="reanalysis-era5-land",
            start_date=start_date_dt,
            end_date=end_date_dt,
            variable=variable,
            area=list(area),
            zarr_store=zarr_store,
        )
        if current_run:
            current_run.log_info(
                f"Prepared {len(requests)} data requests for variable '{variable}'"
            )
        retrieve_requests(
            client=client,
            dataset_id="reanalysis-era5-land",
            requests=requests,
            dst_dir=raw_dir,
            cache=cache,
        )
        if current_run:
            current_run.log_info(f"Retrieved data for variable '{variable}'")
        grib_to_zarr(src_dir=raw_dir, zarr_store=zarr_store, data_var=data_var)


def _read_boundaries(boundaries_file_fp: Path) -> gpd.GeoDataFrame:
    if boundaries_file_fp.suffix == ".parquet":
        boundaries = gpd.read_parquet(boundaries_file_fp)
    elif boundaries_file_fp.suffix.lower() in (".geojson", ".gpkg"):
        boundaries = gpd.read_file(boundaries_file_fp)
    else:
        msg = (
            f"Boundaries file '{boundaries_file_fp.name}' not supported. "
            "Supported file formats are: GeoJSON, Geopackage, and Parquet."
        )
        raise ValueError(msg)
    if boundaries.crs:
        if boundaries.crs != "EPSG:4326":
            msg = "Boundaries CRS must be EPSG:4326 (WGS84)."
            raise ValueError(msg)
    return boundaries


def _get_area_from_boundaries(boundaries: gpd.GeoDataFrame) -> tuple[int, int, int, int]:
    xmin, ymin, xmax, ymax = boundaries.total_bounds
    xmin = floor(xmin - 0.1)
    ymin = floor(ymin - 0.1)
    xmax = ceil(xmax + 0.1)
    ymax = ceil(ymax + 0.1)
    return (ymax, xmin, ymin, xmax)


@era5_sync.task
def process_variables(
    src_dir: Path,
    boundaries_file: Path,
    boundaries_id_col: str,
    periods: Sequence[Period],
    output_dir: Path,
    wait_for: bool | None = None,
) -> bool:
    """Aggregate ERA5-Land data in space and time.

    Args:
        src_dir: Directory containing the Zarr stores for the variables to process.
        boundaries_file: Path to the boundaries file to use for spatial aggregation.
        boundaries_id_col: Column in the boundaries GeoDataFrame to use as identifier.
        periods: List of periods to aggregate over (e.g. Period.DAY, Period.WEEK...).
        output_dir: Output directory for the aggregated data.
        wait_for: Wait for the completion of the specified task before starting.

    Returns:
        True when task is complete.

    """
    zarr_stores = list(src_dir.glob("*.zarr"))
    if not zarr_stores:
        raise ValueError(f"No Zarr stores found in directory '{src_dir}'")

    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = get_variables()

    # load 1st zarr store available to create masks from boundaries
    ds = xr.open_zarr(zarr_stores[0], consolidated=True, decode_timedelta=False)
    boundaries = _read_boundaries(boundaries_file)
    masks = create_masks(gdf=boundaries, id_column=boundaries_id_col, ds=ds)

    for zarr_store in zarr_stores:
        var_name = zarr_store.stem
        if var_name not in metadata:
            raise ValueError(f"Unsupported variable for zarr store '{zarr_store.name}'")

        ds = xr.open_zarr(zarr_store, consolidated=True, decode_timedelta=False)
        var_meta = metadata[var_name]
        if current_run:
            current_run.log_info(f"Processing variable '{var_name}'")

        # Process accumulated variables (total precipitation, runoff...)
        if var_meta["accumulated"]:
            _process_accumulated_variable(
                dataset=ds, masks=masks, periods=periods, output_dir=output_dir
            )
        # Process sampled variables (2m temperature, soil moisture...)
        else:
            _process_sampled_variable(
                dataset=ds, masks=masks, periods=periods, output_dir=output_dir
            )

        if current_run:
            current_run.log_info(f"Variable '{var_name}' processed successfully")

    # Calculate relative humidity if both t2m and d2m are available
    available_vars = [zarr_store.stem for zarr_store in zarr_stores]
    if "2m_temperature" in available_vars and "2m_dewpoint_temperature" in available_vars:
        if current_run:
            current_run.log_info("Calculating relative humidity")
        zarr_store_t2m = src_dir / "2m_temperature.zarr"
        zarr_store_d2m = src_dir / "2m_dewpoint_temperature.zarr"
        _process_relative_humidity(
            zarr_store_t2m=zarr_store_t2m,
            zarr_store_d2m=zarr_store_d2m,
            masks=masks,
            periods=periods,
            output_dir=output_dir,
        )
        current_run.log_info("Relative humidity calculated successfully")

    # Calculate wind speed if both u10 and v10 are available
    if "10m_u_component_of_wind" in available_vars and "10m_v_component_of_wind" in available_vars:
        if current_run:
            current_run.log_info("Calculating wind speed")
        zarr_store_u10 = src_dir / "10m_u_component_of_wind.zarr"
        zarr_store_v10 = src_dir / "10m_v_component_of_wind.zarr"
        _process_wind_speed(
            zarr_store_u10=zarr_store_u10,
            zarr_store_v10=zarr_store_v10,
            masks=masks,
            periods=periods,
            output_dir=output_dir,
        )
        if current_run:
            current_run.log_info("Wind speed calculated successfully")

    return True


def _process_sampled_variable(
    dataset: xr.Dataset,
    masks: xr.DataArray,
    periods: Sequence[Period],
    output_dir: Path,
) -> None:
    """Aggregate a sampled variable over a specified period.

    For sampled variables, daily min, max and mean are 1st computed. Then, spatial
    aggregation (mean) is performed using the provided masks, followed by temporal
    aggregation (mean) over the specified period.

    Args:
        dataset: The input xarray dataset.
        masks: The masks to apply for spatial aggregation.
        periods: Time periods to aggregate over.
        output_dir: Output directory for the aggregated data.
    """
    data_var = str(next(iter(dataset.data_vars)))
    daily_aggregation_method = {
        "mean": lambda ds: ds.resample(time="1D").mean(),
        "min": lambda ds: ds.resample(time="1D").min(),
        "max": lambda ds: ds.resample(time="1D").max(),
    }
    for agg_name, agg_func in daily_aggregation_method.items():
        daily_agg = agg_func(dataset)
        logger.debug(f"Computing daily {agg_name} for variable '{data_var}'")
        spatial_agg = aggregate_in_space(
            ds=daily_agg,
            masks=masks,
            data_var=data_var,
            agg="mean",
        )
        for period in periods:
            time_agg = aggregate_in_time(
                dataframe=spatial_agg,
                period=period,
                agg="mean",
            )
            fp = output_dir / f"{data_var}_{agg_name}_{period.value.lower()}.parquet"
            time_agg.write_parquet(fp)


def _process_accumulated_variable(
    dataset: xr.Dataset,
    masks: xr.DataArray,
    periods: Sequence[Period],
    output_dir: Path,
) -> None:
    """Aggregate an accumulated variable over a specified period.

    For accumulated variables, spatial aggregation (mean) is performed using the
    provided masks, followed by temporal aggregation (sum) over the specified period.

    Args:
        dataset: The input xarray dataset.
        masks: The masks to apply for spatial aggregation.
        periods: Time periods to aggregate over.
        output_dir: Output directory for the aggregated data.
    """
    data_var = str(next(iter(dataset.data_vars)))
    spatial_agg = aggregate_in_space(
        ds=dataset,
        masks=masks,
        data_var=data_var,
        agg="mean",
    )
    for period in periods:
        time_agg = aggregate_in_time(
            dataframe=spatial_agg,
            period=period,
            agg="sum",
        )
        fp = output_dir / f"{data_var}_{period.value.lower()}.parquet"
        time_agg.write_parquet(fp)


def _process_relative_humidity(
    zarr_store_t2m: Path,
    zarr_store_d2m: Path,
    masks: xr.DataArray,
    periods: Sequence[Period],
    output_dir: Path,
) -> None:
    """Calculate and aggregate relative humidity from temperature and dew point.

    Args:
        zarr_store_t2m: Path to the Zarr store containing the 2m temperature data.
        zarr_store_d2m: Path to the Zarr store containing the 2m dew point temperature data.
        masks: DataArray containing the masks to use for spatial aggregation.
        periods: List of periods to aggregate over (e.g. Period.DAY, Period.WEEK...).
        output_dir: Output directory for the aggregated data.
    """
    ds_t2m = xr.open_zarr(zarr_store_t2m)
    ds_d2m = xr.open_zarr(zarr_store_d2m)
    ds_rh = calculate_relative_humidity(ds_t2m.t2m, ds_d2m.d2m)
    _process_sampled_variable(dataset=ds_rh, masks=masks, periods=periods, output_dir=output_dir)


def _process_wind_speed(
    zarr_store_u10: Path,
    zarr_store_v10: Path,
    masks: xr.DataArray,
    periods: Sequence[Period],
    output_dir: Path,
) -> None:
    """Calculate and aggregate wind speed from u10 and v10 components.

    Args:
        zarr_store_u10: Path to the Zarr store containing the 10m u-component data.
        zarr_store_v10: Path to the Zarr store containing the 10m v-component data.
        masks: DataArray containing the masks to use for spatial aggregation.
        periods: List of periods to aggregate over (e.g. Period.DAY, Period.WEEK...).
        output_dir: Output directory for the aggregated data.
    """
    ds_u10 = xr.open_zarr(zarr_store_u10)
    ds_v10 = xr.open_zarr(zarr_store_v10)
    ds_wind_speed = calculate_wind_speed(ds_u10.u10, ds_v10.v10)
    _process_sampled_variable(
        dataset=ds_wind_speed, masks=masks, periods=periods, output_dir=output_dir
    )
