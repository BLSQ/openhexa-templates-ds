import base64
import hashlib
import logging
import re
from datetime import datetime
from pathlib import Path

import polars as pl
import requests
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.sdk.datasets.dataset import Dataset, DatasetVersion
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    extract_data_element_groups,
    extract_data_elements,
    get_category_option_combos,
    get_data_elements,
    get_organisation_unit_groups,
    get_organisation_units,
    join_object_names,
)
from validate import validate_data

logger = logging.getLogger(__name__)


class LocalRun:
    """Mock current_run for local executions."""

    def log_info(self, msg: str) -> None:
        """Mock current_run.log_info()."""
        logger.info(msg)

    def log_warning(self, msg: str) -> None:
        """Mock current_run.log_warning()."""
        logger.warning(msg)

    def log_error(self, msg: str) -> None:
        """Mock current_run.log_error()."""
        logger.error(msg)

    def add_file_output(self, fp: str) -> None:
        """Mock current_run.add_file_output()."""
        logger.info(f"File output added: {fp}")


run = current_run or LocalRun()


@pipeline("dhis2-extract-data-elements")
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
    required=True,
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
    code="organisation_units",
    type=str,
    multiple=True,
    name="Organisation units",
    help="IDs of organisation units to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNITS,
    connection="src_dhis2",
)
@parameter(
    code="organisation_unit_groups",
    type=str,
    multiple=True,
    name="Organisation unit groups",
    help="ID of organisation unit groups to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNIT_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="include_children",
    type=bool,
    name="Include children",
    help="Include children organisation units",
    default=False,
)
@parameter(
    code="start_date",
    type=str,
    name="Start date (YYYY-MM-DD)",
    help="Start date for the extraction",
    default="2020-01-01",
)
@parameter(
    code="end_date",
    type=str,
    name="End date (YYYY-MM-DD)",
    help="End date for the extraction (today by default)",
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
    start_date: str,
    data_elements: list[str] | None = None,
    data_element_groups: list[str] | None = None,
    organisation_units: list[str] | None = None,
    organisation_unit_groups: list[str] | None = None,
    include_children: bool = False,
    end_date: str | None = None,
    dst_file: str | None = None,
    dst_dataset: Dataset | None = None,
    dst_table: str | None = None,
):
    """Extract data elements from a DHIS2 instance and save them to a parquet file."""
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=src_dhis2, cache_dir=cache_dir)

    check_server_health(dhis2)

    run.log_info("Reading metadata from source DHIS2 instance")
    src_data_elements = get_data_elements(dhis2)
    src_organisation_units = get_organisation_units(dhis2)
    src_organisation_unit_groups = get_organisation_unit_groups(dhis2)
    src_category_option_combos = get_category_option_combos(dhis2)
    run.log_info("Sucessfully read metadata from source DHIS2 instance")

    run.log_info("Checking data request")

    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
        run.log_info(f"End date not provided, using {end_date}")

    where = organisation_units or organisation_unit_groups
    if not where:
        msg = "No organisation units or organisation unit groups provided"
        run.log_error(msg)
        raise ValueError(msg)

    if data_elements:
        data_elements = filter_objects(
            objects_in_request=data_elements,
            objects_in_dhis2=src_data_elements["id"].to_list(),
            object_type="Data element",
        )

    if organisation_units:
        organisation_units = filter_objects(
            objects_in_request=organisation_units,
            objects_in_dhis2=src_organisation_units["id"].to_list(),
            object_type="Organisation unit",
        )

    if organisation_unit_groups:
        organisation_unit_groups = filter_objects(
            objects_in_request=organisation_unit_groups,
            objects_in_dhis2=src_organisation_unit_groups["id"].to_list(),
            object_type="Organisation unit group",
        )

    run.log_info("Starting data extraction")

    if data_elements:
        data_values = extract_data_elements(
            dhis2=dhis2,
            data_elements=data_elements,
            org_units=organisation_units if organisation_units else None,
            org_unit_groups=(organisation_unit_groups if organisation_unit_groups else None),
            include_children=include_children,
            start_date=datetime.fromisoformat(start_date),
            end_date=datetime.fromisoformat(end_date),
        )

    elif data_element_groups:
        data_values = extract_data_element_groups(
            dhis2=dhis2,
            data_element_groups=data_element_groups,
            org_units=organisation_units if organisation_units else None,
            org_unit_groups=(organisation_unit_groups if organisation_unit_groups else None),
            include_children=include_children,
            start_date=datetime.fromisoformat(start_date),
            end_date=datetime.fromisoformat(end_date),
        )

    else:
        msg = "No data elements or data element groups provided"
        run.log_error(msg)
        raise ValueError(msg)

    run.log_info(f"Extracted {len(data_values)} data values")

    run.log_info("Joining object names to output data")
    data_values = join_object_names(
        df=data_values,
        data_elements=src_data_elements,
        organisation_units=src_organisation_units,
        category_option_combos=src_category_option_combos,
    )
    run.log_info("Sucessfully joined object names to output data")

    if dst_file:
        dst_file = Path(workspace.files_path) / dst_file
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_file = default_output_path()

    validate_data(data_values)

    run.log_info(f"Writing data to {dst_file}")
    data_values.write_parquet(dst_file)
    run.add_file_output(dst_file.as_posix())
    run.log_info(f"Data written to {dst_file}")

    if dst_dataset:
        write_to_dataset(fp=dst_file, dataset=dst_dataset)

    if dst_table:
        write_to_db(df=data_values, table_name=dst_table)


def default_output_path() -> Path:
    """Get default output path for pipeline outputs.

    Default output path is:
    <workspace>/pipelines/dhis2_extract_data_elements/<date>/data_values.parquet

    Returns
    -------
    Path
        The default output path for the pipeline.
    """
    dst_dir = Path(workspace.files_path) / "pipelines" / "dhis2_extract_data_elements"
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
        run.log_error(f"Unable to reach DHIS2 instance at url {dhis2.api.url}")
        raise


def filter_objects(
    objects_in_request: list[str], objects_in_dhis2: list[str], object_type: str
) -> list[str]:
    """Filter objects to only include those that are available in the DHIS2 instance.

    Parameters
    ----------
    objects_in_request : list[str]
        Objects in the data request, as a list of IDs.
    objects_in_dhis2 : list[str]
        Objects in the source DHIS2 instance, as a list of IDs.
    object_type : str
        The type of object being filtered (e.g., "data elements", "organisation units", etc.).
        Only used for logging purposes.

    Returns
    -------
    list[str]
        A list of objects that are available in the DHIS2 instance.
    """
    filtered_objects = []
    for obj in objects_in_request:
        if obj not in objects_in_dhis2:
            msg = f"{object_type} '{obj}' not found in source DHIS2 instance"
            run.log_warning(msg)
        else:
            filtered_objects.append(obj)

    return filtered_objects


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
            run.log_info("File is already in the dataset and no changes have been detected")
            return

    # increment dataset version name and create the new dataset version
    if dataset.latest_version is not None:
        version_number = int(dataset.latest_version.name.split("v")[-1])
        version_number += 1
    else:
        version_number = 1
    dataset_version = dataset.create_version(name=f"v{version_number}")

    dataset_version.add_file(fp, "data_values.parquet")
    run.log_info(f"File {fp.name} added to dataset {dataset.name} {dataset_version.name}")


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
    run.log_info(f"Data written to DB table {table_name}")
