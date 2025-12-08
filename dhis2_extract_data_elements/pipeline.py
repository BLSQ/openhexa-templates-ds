import base64
import hashlib
import logging
import re
from dataclasses import dataclass
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
) -> None:
    """Extract data elements from a DHIS2 instance and save them to a parquet file."""
    params = RequestParams(
        data_elements=data_elements,
        data_element_groups=data_element_groups,
        organisation_units=organisation_units,
        organisation_unit_groups=organisation_unit_groups,
        include_children=include_children,
        start_date=start_date,
        end_date=end_date,
    )

    validate_parameters(params=params)
    meta = extract_metadata(dhis2_connection=src_dhis2)
    data = extract_data(dhis2_connection=src_dhis2, params=params)
    data = add_names(data_values=data, metadata=meta)
    validate_dataframe(data_values=data)

    if dst_file:
        write_to_file(data_values=data, dst_file=dst_file)

    if dst_dataset:
        write_to_dataset(df=data, ds=dst_dataset)

    if dst_table:
        write_to_database(df=data, table_name=dst_table)


@dataclass
class Metadata:
    """Metadata extracted from DHIS2 instance."""

    data_elements: pl.DataFrame
    organisation_units: pl.DataFrame
    category_option_combos: pl.DataFrame


@dataclass
class RequestParams:
    """Parameters for data extraction request.

    Combined into a single dataclass because they are easier to pass around
    and are pickleable for task inputs.
    """

    data_elements: list[str] | None
    data_element_groups: list[str] | None
    organisation_units: list[str] | None
    organisation_unit_groups: list[str] | None
    include_children: bool
    start_date: str
    end_date: str | None


@dhis2_extract_data_elements.task
def extract_metadata(dhis2_connection: DHIS2Connection) -> Metadata:
    """Extract required metadata from DHIS2 instance.

    Args:
        dhis2_connection: Connection to the DHIS2 instance.

    Returns:
        Extracted metadata including data elements, organisation units, and category option combos.

    """
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=dhis2_connection, cache_dir=cache_dir)

    run.log_info("Reading metadata from source DHIS2 instance")
    data_elements = get_data_elements(dhis2)
    organisation_units = get_organisation_units(dhis2)
    category_option_combos = get_category_option_combos(dhis2)
    run.log_info("Sucessfully read metadata from source DHIS2 instance")

    return Metadata(
        data_elements=data_elements,
        organisation_units=organisation_units,
        category_option_combos=category_option_combos,
    )


@dhis2_extract_data_elements.task
def validate_parameters(
    params: RequestParams,
) -> bool:
    """Validate pipeline parameters.

    Returns:
        True if parameters are valid.

    Raises:
        ValueError: If any parameter is invalid.

    """
    if not is_iso_date(params.start_date):
        msg = f"Start date '{params.start_date}' is not in ISO format (YYYY-MM-DD)"
        run.log_error(msg)
        raise ValueError(msg)

    if params.end_date and not is_iso_date(params.end_date):
        msg = f"End date '{params.end_date}' is not in ISO format (YYYY-MM-DD)"
        run.log_error(msg)
        raise ValueError(msg)

    if params.end_date and params.start_date > params.end_date:
        msg = f"Start date '{params.start_date}' is after end date '{params.end_date}'"
        run.log_error(msg)
        raise ValueError(msg)

    where = params.organisation_units or params.organisation_unit_groups
    if not where:
        msg = "No organisation units or organisation unit groups provided"
        run.log_error(msg)
        raise ValueError(msg)

    if params.organisation_units and params.organisation_unit_groups:
        msg = "Provide either organisation units or organisation unit groups, not both"
        run.log_error(msg)
        raise ValueError(msg)

    if params.organisation_unit_groups and params.include_children:
        msg = "Include children option cannot be used with organisation unit groups"
        run.log_error(msg)
        raise ValueError(msg)

    what = params.data_elements or params.data_element_groups
    if not what:
        msg = "No data elements or data element groups provided"
        run.log_error(msg)
        raise ValueError(msg)

    if params.data_elements and params.data_element_groups:
        msg = "Provide either data elements or data element groups, not both"
        run.log_error(msg)
        raise ValueError(msg)

    return True


@dhis2_extract_data_elements.task
def extract_data(
    dhis2_connection: DHIS2Connection,
    params: RequestParams,
) -> pl.DataFrame:
    """Extract data values from DHIS2 instance.

    Args:
        dhis2_connection: Connection to the DHIS2 instance.
        params: Parameters for data extraction.

    Returns:
        Extracted data values as a Polars DataFrame.

    """
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=dhis2_connection, cache_dir=cache_dir)

    run.log_info("Starting data extraction")

    if params.data_elements:
        data_values = extract_data_elements(
            dhis2=dhis2,
            data_elements=params.data_elements,
            org_units=params.organisation_units,
            org_unit_groups=params.organisation_unit_groups,
            include_children=params.include_children,
            start_date=datetime.fromisoformat(params.start_date),
            end_date=datetime.fromisoformat(params.end_date) if params.end_date else None,
        )

    elif params.data_element_groups:
        data_values = extract_data_element_groups(
            dhis2=dhis2,
            data_element_groups=params.data_element_groups,
            org_units=params.organisation_units,
            org_unit_groups=params.organisation_unit_groups,
            include_children=params.include_children,
            start_date=datetime.fromisoformat(params.start_date),
            end_date=datetime.fromisoformat(params.end_date) if params.end_date else None,
        )

    else:
        msg = "No data elements or data element groups provided"
        run.log_error(msg)
        raise ValueError(msg)

    run.log_info(f"Extracted {len(data_values)} data values")

    return data_values


@dhis2_extract_data_elements.task
def add_names(
    data_values: pl.DataFrame,
    metadata: Metadata,
) -> pl.DataFrame:
    """Join object names to the extracted data values.

    Args:
        data_values: Extracted data values as a Polars DataFrame.
        metadata: Extracted metadata including data elements, organisation units,
            and category option combos.

    Returns:
        Data values with joined object names as a Polars DataFrame.

    """
    run.log_info("Joining object names to output data")
    data_values = join_object_names(
        df=data_values,
        data_elements=metadata.data_elements,
        organisation_units=metadata.organisation_units,
        category_option_combos=metadata.category_option_combos,
    )
    run.log_info("Sucessfully joined object names to output data")

    return data_values


@dhis2_extract_data_elements.task
def validate_dataframe(data_values: pl.DataFrame) -> pl.DataFrame:
    """Validate the extracted data values dataframe.

    Args:
        data_values: Extracted data values as a Polars DataFrame.

    Returns:
        Validated data values as a Polars DataFrame.

    Raises:
        ValueError: If validation fails.

    """
    validate_data(data_values)
    return data_values


@dhis2_extract_data_elements.task
def write_to_file(data_values: pl.DataFrame, dst_file: Path) -> Path:
    """Write the data values dataframe to a parquet file.

    Args:
        data_values: Extracted data values as a Polars DataFrame.
        dst_file: Destination file path.

    Returns:
        Path to the written parquet file.

    """
    run.log_info(f"Writing data to {dst_file}")
    data_values.write_parquet(dst_file)
    run.add_file_output(dst_file.as_posix())
    run.log_info(f"Data written to {dst_file}")
    return dst_file


@dhis2_extract_data_elements.task
def write_to_dataset(df: pl.DataFrame, ds: Dataset) -> None:
    """Add file to an OpenHEXA dataset.

    Args:
        df: DataFrame to write.
        ds: Dataset to write to.

    """
    dst_file = default_output_path()
    df.write_parquet(dst_file)

    if ds.latest_version is not None:
        if in_dataset_version(file=dst_file, dataset_version=ds.latest_version):
            run.log_info("File is already in the dataset and no changes have been detected")
            return

    # increment dataset version name and create the new dataset version
    if ds.latest_version is not None:
        version_number = int(ds.latest_version.name.split("v")[-1])
        version_number += 1
    else:
        version_number = 1
    dataset_version = ds.create_version(name=f"v{version_number}")

    dataset_version.add_file(dst_file, "data_values.parquet")
    run.log_info(f"File {dst_file.name} added to dataset {ds.name} {dataset_version.name}")


@dhis2_extract_data_elements.task
def write_to_database(df: pl.DataFrame, table_name: str) -> None:
    """Write the dataframe to a DB table.

    Args:
        df: DataFrame to write.
        table_name: Name of the table to write to.
    """
    df.write_database(
        table_name=table_name,
        connection=workspace.database_url,
        if_table_exists="replace",
    )
    run.log_info(f"Data written to DB table {table_name}")


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
