import logging
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
from dateutil import relativedelta
from openhexa.sdk import Dataset, workspace
from openhexa.sdk.pipelines import current_run, parameter, pipeline
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.sdk.workspaces.connection import DHIS2Connection
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    extract_dataset,
    get_category_option_combos,
    get_data_elements,
    get_datasets,
    get_organisation_units,
    join_object_names,
)
from openhexa.toolbox.dhis2.periods import Period, period_from_string
from sqlalchemy import create_engine
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


@pipeline("dhis2_extract_dataset")
@parameter(
    "dhis_con",
    name="DHIS2 Connection",
    type=DHIS2Connection,
    # default="dhis2-snis",
    required=True,
)
@parameter(
    "dataset_id",
    name="INPUT: DHIS2 dataset",
    type=str,
    widget=DHIS2Widget.DATASETS,
    connection="dhis_con",
    required=True,
    # default="Qa7YLSayXse",
)
@parameter(
    "start",
    name="Start Date (ISO format)",
    help="ISO format: yyyy-mm-dd",
    type=str,
    required=True,
    # default="2025-05-17",
)
@parameter(
    "end",
    name="End Date (ISO format)",
    help="ISO format: yyyy-mm-dd. Today by default",
    type=str,
    required=False,
    # default="2025-05-18",
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
@parameter("extract_name", name="Name your extraction", type=str, required=False)
@parameter(
    "ou_ids",
    name="Orgunits",
    widget=DHIS2Widget.ORG_UNITS,
    connection="dhis_con",
    type=str,
    multiple=True,
    required=False,
    # default=["h0s5E6VKCB2"],
)
@parameter(
    "include_children",
    name="Include children (of orgunits)",
    type=bool,
    help="Only works if Orgunits are selected.",
    required=False,
    default=True,
)
@parameter(
    "ou_group_ids",
    name="Group(s) of orgunits",
    widget=DHIS2Widget.ORG_UNIT_GROUPS,
    connection="dhis_con",
    type=str,
    multiple=True,
    required=False,
    # default=["pspCxQqWRGZ"],
)
@parameter(
    "max_nb_ou_extracted",
    name="Optional: Maximum number of orgunits per request",
    type=int,
    required=False,
    # default=50,
    help="This parameter is used to limit the number of orgunits per request.",
)
def dhis2_extract_dataset(
    dhis_con: DHIS2Connection,
    dst_dataset: Dataset,
    dst_table: str,
    extract_name: str,
    dataset_id: str,
    ou_group_ids: list[str],
    ou_ids: list[str],
    include_children: bool,
    start: str,
    end: str | None,
    max_nb_ou_extracted: int = 5,
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive
    computations.
    """
    dhis = get_dhis(dhis_con, max_nb_ou_extracted)
    start = valid_date(start)
    end = valid_date(end)
    dhis2_name = get_dhis2_name_domain(dhis_con)
    all_ds = get_datasets(dhis)
    ds = all_ds.filter(pl.col("id") == dataset_id)
    validate_ous_parameters(ou_ids, ou_group_ids)
    start_api = isodate_to_period_type(start, ds["period_type"].item())
    end_api = isodate_to_period_type(end, ds["period_type"].item())
    pyramid = get_organisation_units(dhis)
    des = get_data_elements(dhis)
    cocs = get_category_option_combos(dhis)
    data_values = extract_raw_data(
        dhis,
        pyramid,
        dataset_id,
        ds,
        start_api,
        end_api,
        ou_ids,
        ou_group_ids,
        include_children,
    )
    data_values = join_object_names(
        df=data_values,
        data_elements=des,
        category_option_combos=cocs,
        organisation_units=pyramid,
    )
    table = add_ds_information(
        data_values,
        ds,
    )
    warning_post_extraction(table, ds, dataset_id, start_api, end_api)
    validate_data(table)
    version_name = write_file(table, dhis2_name, extract_name)
    if dst_dataset:
        write_to_dataset(table, dst_dataset, version_name)

    if dst_table:
        write_to_db(data_values, dst_table)


def add_ds_information(
    data_values: pl.DataFrame,
    ds: pl.DataFrame,
) -> pl.DataFrame:
    """Adds dataset information to the extracted data values.

    Args:
        data_values (pl.DataFrame): The extracted data values.
        ds (pl.Dataframe): Dataframe containing the dataset information.


    Returns:
        pl.DataFrame: The data values with added dataset information.
    """
    dataset_name = ds["name"].item()
    dataset_period_type = ds["period_type"].item()

    if data_values.height > 0:
        data_values = data_values.with_columns(
            pl.lit(dataset_name).alias("dataset"),
            pl.lit(dataset_period_type).alias("period_type_configured_dataset"),
            pl.Series(
                "period_type_extracted",
                data_values["period"].map_elements(
                    lambda x: str(type(period_from_string(x)).__name__), return_dtype=pl.Utf8
                ),
            ),
        )

    return data_values


def validate_ous_parameters(ous: list[str], groups: list[str]):
    """Validates the parameters for organizational unit selection.

    Args:
        ous (list[str]): List of organization unit IDs or None.
        groups (list[str]): List of organization unit group IDs or None.
    """
    has_ous = isinstance(ous, list) and len(ous) > 0
    has_groups = isinstance(groups, list) and len(groups) > 0

    if has_ous and has_groups:
        msg = "Please, choose only one option among (1) Orgunits, (2) Group(s) of orgunits"
        run.log_error(msg)
        raise ValueError(msg)

    if not has_ous and not has_groups:
        msg = "Please provide either (1) Orgunits or (2) Group(s) of orgunits"
        run.log_error(msg)
        raise ValueError(msg)


# @dhis2_extract_dataset.task
def get_dhis2_name_domain(dhis_con: DHIS2Connection) -> str:
    """Extracts and formats the subdomain from a DHIS2 connection URL.

    This function takes a DHIS2Connection object, parses its URL to extract the subdomain,
    and replaces any hyphens ('-') in the subdomain with underscores ('_').

    Args:
        dhis_con (DHIS2Connection): An object containing the DHIS2 connection details,
            including the URL.

    Returns:
        str: The formatted subdomain extracted from the DHIS2 connection URL.
    """
    subdomain = "_".join(urlparse(dhis_con.url).netloc.split("."))
    return f"{subdomain.replace('-', '_')}"


# @dhis2_extract_dataset.task
def get_dhis(dhis_con: DHIS2Connection, max_nb_ou_extracted: int) -> DHIS2:  # noqa: D417
    """Creates and returns a DHIS2 object using the provided DHIS2Connection.

    Parameters
    ----------
        dhis_con (DHIS2Connection): The DHIS2Connection object to use for creating the DHIS2 object.

    Returns
    -------
        DHIS2: The created DHIS2 object.

    """
    dhis = DHIS2(dhis_con, cache_dir=Path(workspace.files_path) / ".cache")
    dhis.data_value_sets.MAX_ORG_UNITS = max_nb_ou_extracted
    dhis.data_value_sets.DATE_RANGE_DELTA = relativedelta.relativedelta(months=1)
    return dhis


def write_file(table: pl.DataFrame, dhis2_name: str, extract_name: str | None) -> str:
    """Write the file as a csv and a parquet file.

    Args:
        table (pl.DataFrame): The table to be saved.
        dhis2_name (str): The name of the DHIS2 instance.
        extract_name (str | None): The name of the extraction. If None, the current date and time
        will be used as the name.

    Returns:
        str: The version name used for the extraction.
    """
    dataset_name = table.select("dataset").unique().item()

    # Format timestamp
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y_%H:%M:%S")
    if extract_name is None:
        version_name = date_time
    else:
        version_name = date_time + "-" + extract_name
    version_name = version_name.replace("/", "-")
    output_path = (
        f"{workspace.files_path}/pipelines/dhis2_extract_dataset/{dhis2_name}/{dataset_name}"
    )
    Path.mkdir(Path(output_path), parents=True, exist_ok=True)

    table.write_csv(f"{output_path}/{version_name}.csv")
    table.write_parquet(f"{output_path}/{version_name}.parquet")

    run.add_file_output(f"{output_path}/{version_name}.csv")
    run.add_file_output(f"{output_path}/{version_name}.parquet")

    return version_name


def write_to_dataset(table: pl.DataFrame, dataset: Dataset, version_name: str):
    """Write the extracted data to an OpenHEXA dataset.

    Args:
        table (pl.DataFrame): The extracted data as a Polars DataFrame.
        dataset (Dataset): The OpenHEXA dataset to write the data to.
        version_name (str): The name of the dataset version.
    """
    # Dataset versioning
    version = dataset.create_version(name=version_name)

    # Write split files per dx_name
    for dx_name in table.select("data_element_name").unique().to_series():
        temp_df = table.filter(pl.col("data_element_name") == dx_name)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            temp_path = tmp_file.name
            temp_df.write_parquet(temp_path)
            version.add_file(source=temp_path, filename=f"{dx_name}.parquet")

    run.log_info(f"Data saved in the OpenHEXA dataset: {dataset.name}")


def write_to_db(table: pl.DataFrame, table_name: str):
    """Write the extracted data to the OpenHEXA workspace database.

    Args:
        table (pl.DataFrame): The extracted data.
        table_name (str): The name of the table to write the data to.
    """
    engine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    table.to_pandas().to_sql(table_name, con=engine, if_exists="replace", index=False)
    run.log_info(f"Table '{table_name}' saved in the workspace database")


# @dhis2_extract_dataset.task
def warning_post_extraction(
    table: pl.DataFrame, dataset: pl.DataFrame, dataset_id: str, start: Period, end: Period
):
    """Check for warnings in the extracted data.

    Args:
        table (pl.DataFrame): The extracted data.
        dataset (pl.DataFrame): DataFrame containing the dataset information.
        dataset_id (str): List of dataset IDs.
        start (str): Start date of the extraction.
        end (str): End date of the extraction.
    """
    if len(table) > 0:
        get_periods_with_no_data(table, start, end, dataset)
        get_dataelements_with_no_data(table, dataset)


# @dhis2_extract_dataset.task
def check_parameters_validation(ou_ids: list[str], ou_group_ids: list[str]):
    """Validates the parameters for organizational unit selection.

    Args:
        ou_ids (list[str] | None): List of organization unit IDs or None.
        ou_group_ids (list[str] | None): List of organization unit group IDs or None.

    """
    conditions = {
        "ou_ids + include_children": isinstance(ou_ids, list)
        and len(ou_ids) > 0
        and len(ou_group_ids) == 0,
        "ou_group_ids only": (isinstance(ou_group_ids, list))
        and (len(ou_group_ids) > 0)
        and (ou_ids is None or len(ou_ids) == 0),
    }
    if sum([1 for condition in conditions.values() if condition]) > 1:
        run.log_error("Please, choose only one option among (1) Orgunits, (2) Group(s) oforgunits")
        raise ValueError(
            "Please, choose only one option among (1) Orgunits, (2) Group(s) of orgunits"
        )


def warning_request(dataset_id: str, datasets: dict, selected_ou_ids: set) -> set | None:
    """Check for warnings in the datasets.

    Args:
        dataset_id (str): dataset ID.
        datasets (dict): Dictionary containing dataset information.
        selected_ou_ids (set): set of orgunit ids selected by the parameters.

    Returns:
        set or None: If `data_element_ids` is a non-empty list, returns a set of all data elements
        associated with the datasets that are not in `data_element_ids`. Otherwise, returns None.
    """
    if dataset_id not in datasets:
        run.log_error(f"Dataset id: {dataset_id} not found in this DHIS2 instance.")
        raise ValueError(f"Dataset id: {dataset_id} not found in this DHIS2 instance.")
    datasets_ous = {ou for ou in datasets[dataset_id]["organisation_units"]}
    dataset_ous_intersection = selected_ou_ids.intersection(datasets_ous)
    if len(dataset_ous_intersection) != len(selected_ou_ids):
        run.log_warning(
            f"Only {len(dataset_ous_intersection)} orgunits out of {len(selected_ou_ids)} \
            selected are associated to the datasets. If this is unexpected, verify the orgunits\
            associated to the dataset {datasets[dataset_id]['name']} in your DHIS2 instance."
        )
        if len(dataset_ous_intersection) == 0:
            run.log_error(f"No orgunits associated to the datasets {datasets[dataset_id]['name']}.")
    return dataset_ous_intersection


def valid_date(date_str: str | None) -> str:
    """Validates a date string and returns it if valid, otherwise logs an error.

    Args:
        date_str (str): The date string to validate.

    Returns:
        str: The validated date string.

    """
    if date_str is None:
        return date.today().isoformat()
    if is_iso_date(date_str):
        return date_str
    run.log_error(f"Invalid date format: {date_str}. Expected ISO format (yyyy-mm-dd).")
    raise ValueError(f"Invalid date format: {date_str}. Expected ISO format (yyyy-mm-dd).")


def fetch_dataset_data_for_valid_descendants(
    dhis: DHIS2,
    pyramid: pl.DataFrame,
    dataset_id: str,
    all_ous: list[str],
    include_children: bool,
    start_date: datetime,
    end_date: datetime,
    ds: pl.DataFrame,
) -> pl.DataFrame:
    """Fetch dataset data for valid descendant organization units.

    This function retrieves all descendant organization units for the given parent organization unit
    IDs, filters them to include only those linked to the specified dataset, and then extracts the
    dataset data for the valid organization units within the specified date range.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        pyramid (pl.DataFrame): A Polars DataFrame representing the organisational unit hierarchy.
        dataset_id (str): The ID of the dataset.
        all_ous (list[str]): List of all of the organizational unit IDs to consider.
        include_children (bool): Whether to include child organizational units.
        start_date (str): The start date for data extraction.
        end_date (str): The end date for data extraction.
        ds (pl.DataFrame): Dataframe containing the dataset metadata,

    Returns:
        pl.DataFrame: The extracted dataset data for the valid descendant organization units.
    """
    all_descendants = get_descendants(all_ous, include_children, pyramid)
    dataset_org_units = ds["organisation_units"].item()
    valid_org_units = [ou for ou in all_descendants if ou in dataset_org_units]

    try:
        data_values = extract_dataset(
            dhis2=dhis,
            dataset=dataset_id,
            start_date=start_date,
            end_date=end_date,
            org_units=valid_org_units,
        )
    except Exception as e:
        run.log_error(f"Failed to extract dataset: {e}")
        raise

    return data_values


def get_descendants(
    parent_ous: list[str],
    include_children: bool,
    pyramid: pl.DataFrame,
) -> list[str]:
    """Get all descendant organisation units for the given parent organisation units.

    Args:
        parent_ous (list[str]): List of parent organisation unit IDs.
        include_children (bool): Whether to include child organisational units.
        pyramid: A Polars DataFrame representing the organisational unit hierarchy.

    Returns:
        list[str]: A list of organisation unit IDs including descendants if specified.
    """
    if not include_children:
        return parent_ous

    all_descendants = set()
    for org_unit_id in parent_ous:
        level = pyramid.filter(pl.col("id") == org_unit_id)["level"].item()
        col_level = f"level_{level}_id"
        descendants = pyramid.filter(pl.col(col_level) == org_unit_id)["id"].to_list()
        all_descendants.update(descendants)

    return list(all_descendants)


def fetch_dataset_data_for_valid_group_orgunits(
    dhis: DHIS2,
    dataset_id: str,
    org_unit_group_ids: list[str],
    start_date: datetime,
    end_date: datetime,
    ds: pl.DataFrame,
) -> pl.DataFrame:
    """Fetch dataset data for valid organization units that belong to specified org unit groups.

    This function retrieves organization units from the specified groups, filters them to include
    only those linked to the given dataset, and then extracts the dataset data for these valid
    organization units within the specified date range.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        dataset_id (str): The ID of the dataset.
        org_unit_group_ids (list[str]): List of organization unit group IDs.
        start_date (datetime): The start date for data extraction.
        end_date (datetime): The end date for data extraction.
        ds (pl.DataFrame): Dataframe containing the dataset metadata,

    Returns:
        pl.DataFrame: The extracted dataset data for the valid organization units in the specified
        groups.

    Raises:
        Exception: If fetching dataset org units or extracting the dataset fails.
    """
    dataset_org_units = ds["organisation_units"].item()
    ous = get_ous_from_groups(dhis, org_unit_group_ids)
    valid_ous = [ou for ou in ous if ou in dataset_org_units]

    try:
        data_values = extract_dataset(
            dhis2=dhis,
            dataset=dataset_id,
            start_date=start_date,
            end_date=end_date,
            org_units=valid_ous,
        )
    except Exception as e:
        run.log_error(f"Failed to extract dataset from org unit groups: {e}")
        raise

    return data_values


def get_ous_from_groups(dhis: DHIS2, all_ou_groups: list[str]) -> list[str]:
    """Retrieve organisation unit IDs from the specified organisation unit groups.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        all_ou_groups (list[str]): List of organisation unit group IDs.

    Returns:
        list[str]: A list of organisation unit IDs from the specified groups.
    """
    ous = []

    for group_id in all_ou_groups:
        params = {"fields": "organisationUnits[id]"}
        response = dhis.api.get(f"organisationUnitGroups/{group_id}.json", params=params)
        group_ous = response.get("organisationUnits", [])
        group_ou_ids = [ou["id"] for ou in group_ous]
        ous.extend(group_ou_ids)

    return list(set(ous))


def extract_raw_data(
    dhis: DHIS2,
    pyramid: pl.DataFrame,
    dataset_id: str,
    datasets: pl.DataFrame,
    start: Period,
    end: Period,
    ou_ids: list[str],
    ou_group_ids: list[str],
    include_children: bool,
) -> pl.DataFrame:
    """Extracts raw data from DHIS2 for the given datasets and time range.

    Args:
        dhis (DHIS2): DHIS2 client object used to interact with the DHIS2 API.
        pyramid (pl.DataFrame): A Polars DataFrame representing the organisational unit hierarchy.
        dataset_id (str): dataset ID to extract data from.
        datasets (pl.Dataframe): Dataframe containing the dataset metadata,
        including data elements and organisation units.
        start (str): Start date of the time range to extract data from.
        end (str): End date of the time range to extract data from.
        ou_ids (list[str] | None): List of organization unit IDs or None.
        ou_group_ids (list[str] | None): List of organization unit group IDs or None.
        include_children (bool): Whether to include child organizational units.

    Returns:
        pl.DataFrame: Pandas DataFrame containing the extracted raw data, with additional columns
        for dataset name and period type.
    """
    dataset_name = datasets["name"].item()
    dataset_period_type = datasets["period_type"].item()
    run.log_info(f"Extracting data for dataset {dataset_name}")
    run.log_info(f"Period type: {dataset_period_type}")

    if len(ou_ids) == 0:
        ou_ids = None
        include_children = False
    if len(ou_group_ids) == 0:
        ou_group_ids = None

    try:
        data_values = extract_dataset(
            dhis2=dhis,
            dataset=dataset_id,
            start_date=start.datetime,
            end_date=end.datetime,
            org_units=ou_ids,
            org_unit_groups=ou_group_ids,
            include_children=include_children,
        )
    except Exception as e:
        if isinstance(ou_ids, list) and len(ou_ids) > 0:
            run.log_info(f"Fetching data cutting request for all descendants of orgunits: {ou_ids}")
            data_values = fetch_dataset_data_for_valid_descendants(
                dhis=dhis,
                pyramid=pyramid,
                dataset_id=dataset_id,
                all_ous=ou_ids,
                include_children=include_children,
                start_date=start.datetime,
                end_date=end.datetime,
                ds=datasets,
            )
        elif isinstance(ou_group_ids, list) and len(ou_group_ids) > 0:
            run.log_info(
                f"Fetching data cutting request for all orgunits in groups: {ou_group_ids}"
            )
            data_values = fetch_dataset_data_for_valid_group_orgunits(
                dhis=dhis,
                dataset_id=dataset_id,
                org_unit_group_ids=ou_group_ids,
                start_date=start.datetime,
                end_date=end.datetime,
                ds=datasets,
            )
        else:
            run.log_error(
                f"Failed to extract dataset {dataset_id} for period {start!s} to {end!s}."
            )
            raise e

    length_table = data_values.height
    run.log_info("Number of rows extracted (total) : " + str(length_table))

    return data_values


def is_iso_date(date_str: str) -> bool:
    """Check if a given string is a valid ISO 8601 date.

    Args:
    ----
    date_str (str): A string representing the date to be checked in ISO 8601 format.

    Returns:
    -------
    bool: True if the string is a valid ISO 8601 date, False otherwise.
    """
    try:
        # Try to parse the date string in ISO 8601 format
        datetime.fromisoformat(date_str)
        return True
    except ValueError:
        # If parsing fails, it is not a valid ISO 8601 date
        return False


def align_to_week_start(date: datetime, anchor_day: int) -> datetime:
    """Aligns a date to the start of the week based on DHIS2 anchor day.

    Returns:
        datetime: The aligned date corresponding to the start of the week.
    """
    weekday = date.weekday()
    days_offset = (weekday - anchor_day) % 7
    return date - timedelta(days=days_offset)


def isodate_to_period_type(date: str, period_type: str) -> Period:
    """Converts ISO date string to DHIS2-compatible period string based on a specified period type.

    Args:
        date (str): The ISO date string in the format "YYYY-MM-DD".
        period_type (str): The DHIS2 period type. Supported values include:
            - "Daily": Converts to a daily period (e.g., "20230101").
            - "Weekly": Converts to a weekly period starting on Monday (e.g., "2023W1").
            - "WeeklyWednesday", "WeeklyThursday", "WeekSlyaturday", "WeeklySunday":
               Converts to a weekly period aligned to the specified weekday.
            - "Monthly": Converts to a monthly period (e.g., "202301").
            - "BiMonthly": Converts to a bi-monthly period (e.g., "202301" for Jan-Feb).
            - "Quarterly": Converts to a quarterly period (e.g., "2023Q1").
            - "SixMonthly": Converts to a six-monthly period (e.g., "2023S1" for Jan-Jun).
            - "Yearly": Converts to a yearly period (e.g., "2023").
            - "FinancialApril": Converts to a financial year starting in April (e.g., "2023April").
            - "FinancialJuly": Converts to a financial year starting in July (e.g., "2023July").
            - "FinancialOct": Converts to a financial year starting in October (e.g., "2023Oct").
            - "FinancialNov": Converts to a financial year starting in November (e.g., "2023Nov").

    Returns:
        Period: A DHIS2-compatible period object created from the generated period string.

    Raises:
        ValueError: If the provided period type is unsupported.
    """
    weekly_anchors = {
        "Weekly": 0,
        "WeeklyWednesday": 2,
        "WeeklyThursday": 3,
        "WeeklySaturday": 5,
        "WeeklySunday": 6,
    }

    dt = datetime.strptime(date, "%Y-%m-%d")

    if period_type == "Daily":
        period_str = dt.strftime("%Y%m%d")

    elif period_type.startswith("Weekly"):
        anchor_day = weekly_anchors.get(period_type, 0)  # Default to Monday
        aligned_date = align_to_week_start(dt, anchor_day)
        iso_year, iso_week, _ = aligned_date.isocalendar()
        if period_type == "Weekly":
            period_str = f"{iso_year}W{iso_week}"  # No leading zero
        elif period_type in ["WeeklyWednesday", "WeeklyThursday", "WeeklySaturday", "WeeklySunday"]:
            period_str = f"{iso_year}{period_type.replace('Weekly', '')[:3]}W{iso_week}"

    elif period_type == "Monthly":
        period_str = dt.strftime("%Y%m")

    elif period_type == "BiMonthly":
        period_str = f"{dt.year}0{(dt.month - 1) // 2 + 1}"

    elif period_type == "Quarterly":
        period_str = f"{dt.year}Q{(dt.month - 1) // 3 + 1}"

    elif period_type == "SixMonthly":
        period_str = f"{dt.year}S{1 if dt.month <= 6 else 2}"

    elif period_type == "Yearly":
        period_str = f"{dt.year}"

    elif period_type == "FinancialApril":
        fy = dt.year if dt.month >= 4 else dt.year - 1
        period_str = f"{fy}April"

    elif period_type == "FinancialJuly":
        fy = dt.year if dt.month >= 7 else dt.year - 1
        period_str = f"{fy}July"

    elif period_type == "FinancialOct":
        fy = dt.year if dt.month >= 10 else dt.year - 1
        period_str = f"{fy}Oct"

    elif period_type == "FinancialNov":
        fy = dt.year if dt.month >= 11 else dt.year - 1
        period_str = f"{fy}Nov"

    else:
        raise ValueError(f"Unsupported DHIS2 period type: {period_type}")

    return period_from_string(period_str)


def get_periods_with_no_data(data: pl.DataFrame, start: Period, end: Period, dataset: pl.DataFrame):
    """Checks if there are differences between the expected periods and the extracted periods.

    Args:
        data (pl.DataFrame): The extracted data.9
        start (str): Start date in ISO format.
        end (str): End date in ISO format.
        dataset (pl.DataFrame): The dataset metadata.
    """
    if start != end:
        expected_periods = [str(p) for p in start.range(end)]
    else:
        expected_periods = [str(start)]

    extracted_periods = [str(p) for p in data["period"].unique()]

    missing_periods = [p for p in expected_periods if p not in extracted_periods]
    unexpected_periods = [p for p in extracted_periods if p not in expected_periods]

    dataset_name = dataset["name"].item()
    if len(missing_periods) > 0:
        run.log_warning(
            f"Following periods have no data: {sorted(missing_periods)} for dataset {dataset_name}"
        )

    if len(unexpected_periods) > 0:
        run.log_warning(
            "Following periods not expected, but found: "
            f"{sorted(unexpected_periods)} for dataset {dataset_name}"
        )


def get_dataelements_with_no_data(data: pl.DataFrame, dataset: pl.DataFrame):
    """Checks if there are differences between the expected and the extracted dataElements.

    Args:
        data (pl.DataFrame): The extracted data.
        dataset (pl.DataFrame): The dataset metadata.

    """
    expected_des = dataset["data_elements"].item()
    extracted_des = data["data_element_id"].unique()

    missing_des = [p for p in expected_des if p not in extracted_des]
    unexpected_des = [p for p in extracted_des if p not in expected_des]

    dataset_name = dataset["name"].item()
    if len(missing_des) > 0:
        run.log_warning(
            f"Following dataElements have no data: {sorted(missing_des)} for dataset {dataset_name}"
        )

    if len(unexpected_des) > 0:
        run.log_warning(
            "Following dataElements not expected, but found: "
            f"{sorted(unexpected_des)} for dataset {dataset_name}"
        )


if __name__ == "__main__":
    dhis2_extract_dataset()
