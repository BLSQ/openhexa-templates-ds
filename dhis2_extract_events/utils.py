import base64
import hashlib
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import polars as pl
import requests
from openhexa.sdk import current_run, workspace
from openhexa.sdk.datasets.dataset import Dataset, DatasetVersion


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
            current_run.log_warning(msg)
        else:
            filtered_objects.append(obj)

    return filtered_objects


def default_output_path() -> Path:
    """Get default output path for pipeline outputs.

    Default output path is:
    <workspace>/pipelines/dhis2_tracker_extract/<date>/events.parquet

    Returns
    -------
    Path
        The default output path for the pipeline.
    """
    dst_dir = Path(workspace.files_path) / "pipelines" / "dhis2_tracker_extract"
    dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dst_dir.mkdir(parents=True, exist_ok=True)
    return dst_dir / "events.parquet"


def validate_yyyymmdd(yyyymmdd_str: str) -> None:
    """Validate that the input is an integer in yyyymmdd format."""
    if len(yyyymmdd_str) != 8:
        current_run.log_error("Input must be an 8-digit string in yyyymmdd format.")
        raise ValueError("Input must be an 8-digit string in yyyymmdd format.")

    year = int(yyyymmdd_str[:4])
    month = int(yyyymmdd_str[4:6])
    day = int(yyyymmdd_str[6:])
    _, max_day = monthrange(year, month)

    if month < 1 or month > 12:
        current_run.log_error(f"Month must be between 01 and 12 in {yyyymmdd_str}, got {month}.")
        raise ValueError(f"Month must be between 01 and 12 in {yyyymmdd_str}, got {month}.")

    if day < 1 or day > max_day:
        current_run.log_error(f"Day must be between 01 and {max_day} in {yyyymmdd_str}, got {day}.")
        raise ValueError(f"Day must be between 01 and {max_day} in {yyyymmdd_str}, got {day}.")


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
            current_run.log_info("File is already in the dataset and no changes have been detected")
            return

    # increment dataset version name and create the new dataset version
    if dataset.latest_version is not None:
        version_number = int(dataset.latest_version.name.split("v")[-1])
        version_number += 1
    else:
        version_number = 1
    dataset_version = dataset.create_version(name=f"v{version_number}")

    dataset_version.add_file(fp, "data_values.parquet")
    current_run.log_info(f"File {fp.name} added to dataset {dataset.name} {dataset_version.name}")


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
