import base64
import hashlib
import re
from datetime import datetime
from pathlib import Path

import polars as pl
import requests
from openhexa.sdk.datasets.dataset import Dataset, DatasetVersion
from openhexa.sdk.pipelines.parameter import DHIS2Widget, parameter
from openhexa.sdk.pipelines.pipeline import pipeline
from openhexa.sdk.pipelines.run import current_run
from openhexa.sdk.workspaces import workspace
from openhexa.sdk.workspaces.connection import DHIS2Connection
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.api import DHIS2ApiError
from openhexa.toolbox.dhis2.dataframe import (
    extract_analytics,
    get_category_option_combos,
    get_data_elements,
    get_indicators,
    get_organisation_units,
    join_object_names,
)
from openhexa.toolbox.dhis2.periods import period_from_string
from validate import validate_data


@pipeline("dhis2-extract-analytics")
@parameter(
    code="src_dhis2",
    type=DHIS2Connection,
    name="Source DHIS2 instance",
    help="The DHIS2 instance to extract data elements from",
)
@parameter(
    code="data_elements",
    type=str,
    multiple=True,
    name="Data elements",
    help="Data elements to extract",
    required=False,
    widget=DHIS2Widget.DATA_ELEMENTS,
    connection="src_dhis2",
)
@parameter(
    code="data_element_groups",
    type=str,
    multiple=True,
    name="Data element groups",
    help="Data element groups to extract",
    required=False,
    widget=DHIS2Widget.DATA_ELEMENT_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="indicators",
    type=str,
    multiple=True,
    name="Indicators",
    help="Indicators to extract",
    required=False,
    widget=DHIS2Widget.INDICATORS,
    connection="src_dhis2",
)
@parameter(
    code="indicator_groups",
    type=str,
    multiple=True,
    name="Indicator groups",
    help="Indicator groups to extract",
    required=False,
    widget=DHIS2Widget.INDICATOR_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="org_units",
    type=str,
    multiple=True,
    name="Organisation units",
    help="IDs of organisation units to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNITS,
    connection="src_dhis2",
)
@parameter(
    code="org_unit_groups",
    type=str,
    multiple=True,
    name="Organisation unit groups",
    help="IDs of organisation unit groups to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNIT_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="org_unit_levels",
    type=str,
    multiple=True,
    name="Organisation unit levels",
    help="Organisation unit levels to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNIT_LEVELS,
    connection="src_dhis2",
)
@parameter(
    code="start_period",
    type=str,
    name="Start period",
    help="Start period for the extraction (DHIS2 format)",
    required=True,
)
@parameter(
    code="end_period",
    type=str,
    name="End period",
    help="End period for the extraction (DHIS2 format)",
    required=False,
)
@parameter(
    code="dst_file",
    type=str,
    name="Output file",
    help="Output file path in the workspace. Parent directory will automatically be created.",
    required=False,
)
@parameter(
    code="dst_dataset",
    type=Dataset,
    name="Output dataset",
    help="Output OpenHEXA dataset. A new version will be created if new content is detected.",
    required=False,
)
@parameter(
    code="dst_table",
    type=str,
    name="Output DB table",
    help="Output DB table name. If not provided, output will not be saved to a DB table.",
    required=False,
)
def dhis2_extract_data_elements(
    src_dhis2: DHIS2Connection,
    start_period: str,
    data_elements: list[str] | None = None,
    data_element_groups: list[str] | None = None,
    indicators: list[str] | None = None,
    indicator_groups: list[str] | None = None,
    org_units: list[str] | None = None,
    org_unit_groups: list[str] | None = None,
    org_unit_levels: list[str] | None = None,
    end_period: str | None = None,
    dst_file: str | None = None,
    dst_dataset: Dataset | None = None,
    dst_table: str | None = None,
):
    """Extract data elements from a DHIS2 instance and save them to a parquet file."""
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=src_dhis2, cache_dir=cache_dir)

    # raise an error if the instance is not reachable
    check_server_health(dhis2)

    # log last update of analytics tables
    last_update = last_analytics_update(dhis2)
    if last_update:
        current_run.log_info(
            f"Last update of analytics tables: {last_update.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    current_run.log_info("Reading metadata from source DHIS2 instance")
    src_data_elements = get_data_elements(dhis2)
    src_indicators = get_indicators(dhis2)
    src_organisation_units = get_organisation_units(dhis2)
    src_category_option_combos = get_category_option_combos(dhis2)
    current_run.log_info("Sucessfully read metadata from source DHIS2 instance")

    current_run.log_info("Checking data request")

    # convert start and end periods to Period objects
    # if it is not provided, the end Period is set using the same type of the start Period
    # and initialized with the current date
    start = period_from_string(start_period)
    if not end_period:
        end = type(start)(datetime.now())
        current_run.log_info(f"End period not provided, using latest period: {end}")
    else:
        end = period_from_string(end_period)

    # build the list of periods between start and end periods
    periods = [str(p) for p in start.get_range(end)]

    current_run.log_info("Starting data extraction")

    try:
        data_values = extract_analytics(
            dhis2=dhis2,
            periods=periods,
            data_elements=data_elements if data_elements else None,
            data_element_groups=data_element_groups if data_element_groups else None,
            indicators=indicators if indicators else None,
            indicator_groups=indicator_groups if indicator_groups else None,
            org_units=org_units if org_units else None,
            org_unit_groups=org_unit_groups if org_unit_groups else None,
            org_unit_levels=org_unit_levels if org_unit_levels else None,
        )
    except (ValueError, DHIS2ApiError) as e:
        current_run.log_error(str(e))
        raise

    current_run.log_info(f"Extracted {len(data_values)} data values")

    current_run.log_info("Joining object names to output data")
    data_values = join_object_names(
        df=data_values,
        data_elements=(
            src_data_elements if "data_element_id" in data_values.columns else None
        ),
        indicators=src_indicators if "indicator_id" in data_values.columns else None,
        organisation_units=src_organisation_units,
        category_option_combos=(
            src_category_option_combos
            if "category_option_combo_id" in data_values.columns
            else None
        ),
    )
    current_run.log_info("Sucessfully joined object names to output data")

    if dst_file:
        dst_file = Path(workspace.files_path) / dst_file
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_file = default_output_path()

    validate_data(data_values)
    current_run.log_info(f"Writing data to {dst_file}")
    data_values.write_parquet(dst_file)
    current_run.add_file_output(dst_file.as_posix())
    current_run.log_info(f"Data written to {dst_file}")

    if dst_dataset:
        write_to_dataset(fp=dst_file, dataset=dst_dataset)

    if dst_table:
        write_to_db(df=data_values, table_name=dst_table)


def default_output_path() -> Path:
    """Get default output path for pipeline outputs.

    Default output path is:
    <workspace>/pipelines/dhis2_extract_analytics/<date>/data_values.parquet

    Returns
    -------
    Path
        The default output path for the pipeline.
    """
    dst_dir = Path(workspace.files_path) / "pipelines" / "dhis2_extract_analytics"
    dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dst_dir.mkdir(parents=True, exist_ok=True)
    return dst_dir / "data_values.parquet"


def is_iso_date(date_string: str) -> bool:
    """Check if a string is in ISO date format (YYYY-MM-DD).

    Parameters
    ----------
    date_string : str
        The string to check.

    Returns
    -------
    bool
        True if the string is in ISO date format, False otherwise.
    """
    pattern = r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])$"
    return bool(re.match(pattern, date_string))


def check_server_health(dhis2: DHIS2):
    """Check if the DHIS2 server is responding."""
    try:
        dhis2.ping()
    except ConnectionError:
        current_run.log_error(f"Unable to reach DHIS2 instance at url {dhis2.api.url}")
        raise


def last_analytics_update(dhis2: DHIS2) -> datetime | None:
    """Get the last update date of the analytics tables.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to check.

    Returns
    -------
    datetime | None
        The last update date of the analytics tables. Returns None if the analytics tables have
        never been updated.
    """
    dtime_str = dhis2.meta.system_info().get("lastAnalyticsTableSuccess")
    return datetime.fromisoformat(dtime_str) if dtime_str else None


def write_to_dataset(fp: Path, dataset: Dataset):
    """Add file to an OpenHEXA dataset.

    Parameters
    ----------
    fp : Path
        The path to the file to write.
    dataset : Dataset
        The dataset to write to.
    """
    if dataset.latest_version is not None:
        if in_dataset_version(file=fp, dataset_version=dataset.latest_version):
            current_run.log_info(
                "File is already in the dataset and no changes have been detected"
            )
            return

    # increment dataset version name and create the new dataset version
    if dataset.latest_version is not None:
        version_number = int(dataset.latest_version.name.split("v")[-1])
        version_number += 1
    else:
        version_number = 1
    dataset_version = dataset.create_version(name=f"v{version_number}")

    dataset_version.add_file(fp, "data_values.parquet")
    current_run.log_info(
        f"File {fp.name} added to dataset {dataset.name} {dataset_version.name}"
    )


def md5_from_url(url: str) -> str:
    """Get the MD5 hash of a file from a URL.

    Assumes the file is hosted on Google Cloud Storage and uses the x-goog-hash header.

    Parameters
    ----------
    url : str
        The URL of the file.

    Returns
    -------
    str
        The MD5 hash of the file, base64 encoded.

    Raises
    ------
    ValueError
        If the x-goog-hash header is not found in the response.
    """
    r = requests.head(url)
    r.raise_for_status()
    x_goog_hash = r.headers.get("x-goog-hash")
    if x_goog_hash is None:
        raise ValueError("x-goog-hash header not found in response")
    return x_goog_hash.split("md5=")[-1]


def md5_from_file(fp: Path) -> str:
    """Get the MD5 hash of a file.

    Parameters
    ----------
    fp : Path
        The path to the file.

    Returns
    -------
    str
        The MD5 hash of the file, base64 encoded.
    """
    with fp.open("rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)
    return base64.b64encode(file_hash.digest()).decode("utf-8")


def in_dataset_version(file: Path, dataset_version: DatasetVersion) -> bool:
    """Check if a file is in the dataset version.

    Parameters
    ----------
    file : Path
        The path to the file.
    dataset_version : DatasetVersion
        The dataset version to check against.

    Returns
    -------
    bool
        True if the file is in the dataset version, False otherwise.
    """
    md5_file = md5_from_file(file)
    for f in dataset_version.files:
        md5_url = md5_from_url(f.download_url)
        if md5_file == md5_url:
            return True
    return False


def write_to_db(df: pl.DataFrame, table_name: str) -> None:
    """Write the dataframe to a DB table.

    Parameters
    ----------
    df : pl.DataFrame
        The dataframe to write.
    table_name : str
        The name of the table to write to.
    """
    df.write_database(
        table_name=table_name,
        connection=workspace.database_url,
        if_table_exists="replace",
    )
    current_run.log_info(f"Data written to DB table {table_name}")
