import os
import re
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
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
    get_organisation_units,
    join_object_names,
)
from openhexa.toolbox.dhis2.periods import Period, period_from_string
from sqlalchemy import create_engine


@pipeline("dhis2_extract_dataset")
@parameter(
    "dhis_con",
    name="DHIS2 Connection",
    type=DHIS2Connection,
    default="dhis2-demo-2-39",
    required=True,
)
@parameter(
    "dataset_id",
    name="INPUT: DHIS2 dataset",
    type=str,
    widget=DHIS2Widget.DATASETS,
    connection="dhis_con",
    required=True,
    default=None,
    # default="j38YW1Am7he",
)
@parameter(
    "start",
    name="Start Date (ISO format)",
    help="ISO format: yyyy-mm-dd",
    type=str,
    required=True,
    default="2024-01-01",
)
@parameter(
    "end",
    name="End Date (ISO format)",
    help="ISO format: yyyy-mm-dd",
    type=str,
    required=False,
    default=None,
)
@parameter("dataset", name="OUTPUT: Openhexa dataset", type=Dataset, required=True, default=None)
@parameter("extract_name", name="Name your extraction", type=str, required=False)
@parameter(
    "ou_ids",
    name="Orgunits",
    widget=DHIS2Widget.ORG_UNITS,
    connection="dhis_con",
    type=str,
    multiple=True,
    required=False,
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
    # default=["RXL3lPSK8oG"],
)
@parameter(
    "max_nb_ou_extracted",
    name="Optional: Maximum number of orgunits per request",
    type=int,
    required=False,
    default=5,
    help="This parameter is used to limit the number of orgunits per request.",
)
def dhis2_extract_dataset(
    dhis_con: DHIS2Connection,
    dataset: Dataset,
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
    ds = get_datasets_as_dict(dhis)
    check_parameters_validation(ou_ids, ou_group_ids)
    dhis2_name = create_extraction_folder(dhis2_name, ds, dataset_id)
    table = extract_raw_data(
        dhis,
        dataset_id,
        ds,
        start,
        end,
        ou_ids,
        ou_group_ids,
        include_children,
    )
    warning_post_extraction(table, ds, dataset_id, start, end)
    save_table(table, dhis2_name, dataset, extract_name)


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
def create_extraction_folder(dhis2_name: str, datasets: dict, dataset_id: str) -> str:
    """Creates a folder structure for data extraction.

    Args:
        dhis2_name (str): The name of the DHIS2 instance.
        datasets (dict): A dictionary containing dataset information.
        dataset_id (str): A dataset ID.

    Returns:
        str: The name of the DHIS2 instance.
    """
    name = datasets[dataset_id]["name"].replace("/", "-").replace("\\", "-")
    Path(f"{workspace.files_path}/pipelines/dhis2_extract_dataset/{dhis2_name}/{name}").mkdir(
        parents=True, exist_ok=True
    )
    return dhis2_name


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


# @dhis2_extract_dataset.task
def save_table(table: pd.DataFrame, dhis2_name: str, dataset: Dataset, extract_name: str | None):
    """Saves the given table to DHIS2 and optionally to the OH database.

    Args:
        table (pd.DataFrame): The table to be saved.
        dhis2_name (str): The name of the DHIS2 instance.
        dataset (Dataset): The OpenHexa dataset where the table will be saved.
        openhexa_dataset (Dataset | None): The OpenHexa dataset to save the table to. If None,
        the table will not be saved to the OpenHexa database.
        extract_name (str | None): The name of the extraction. If None, the current date and time
        will be used as the name.
    """
    dataset_name = table.select("dataset").unique().item()

    # Format timestamp
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    if extract_name is None:
        version_name = date_time
    else:
        version_name = date_time + "-" + extract_name
    version_name = version_name.replace("/", "-")
    # Output path
    output_path = (
        f"{workspace.files_path}/pipelines/dhis2_extract_dataset/"
        f"{dhis2_name}/{dataset_name}/{version_name}"
    )

    # Write to CSV and Parquet
    table.write_csv(f"{output_path}.csv")
    table.write_parquet(f"{output_path}.parquet")

    # Register outputs
    current_run.add_file_output(f"{output_path}.csv")
    current_run.add_file_output(f"{output_path}.parquet")

    # Dataset versioning
    version = dataset.create_version(name=version_name)

    # Write split files per dx_name
    for dx_name in table.select("data_element_name").unique().to_series():
        temp_df = table.filter(pl.col("data_element_name") == dx_name)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            temp_path = tmp_file.name
            temp_df.write_parquet(temp_path)
            version.add_file(source=temp_path, filename=f"{dx_name}.parquet")

    # Save to SQL (via pandas fallback)
    engine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    current_run.log_info(f"Table '{dataset_name}' saved in the workspace database")

    # ⚠️ Convert to pandas to use `.to_sql()` from Polars
    table.to_pandas().to_sql(dataset_name, con=engine, if_exists="replace", index=False)


# @dhis2_extract_dataset.task
def warning_post_extraction(
    table: pd.DataFrame, datasets: dict, dataset_id: str, start: str, end: str
):
    """Check for warnings in the extracted data.

    Args:
        table (pd.DataFrame): The extracted data.
        datasets (dict): Dictionary containing dataset information.
        dataset_id (str): List of dataset IDs.
        start (str): Start date of the extraction.
        end (str): End date of the extraction.

    """
    if len(table) > 0:
        periods = [str(p) for p in table["period"].unique()]
        get_periods_with_no_data(periods, start, end, datasets[dataset_id])
        get_dataelements_with_no_data(table["data_element_id"].unique(), datasets[dataset_id])


# @dhis2_extract_dataset.task
def check_parameters_validation(ou_ids: list[str], ou_group_ids: list[str]):
    """Validates the parameters for organizational unit selection.

    Args:
        ou_ids (list[str] | None): List of organization unit IDs or None.
        ou_group_ids (list[str] | None): List of organization unit group IDs or None.
        ou_level (int | None): Level of organization units or None.

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
        current_run.log_error(
            "Please, choose only one option among (1) Orgunits, (2) Group(s) oforgunits"
        )
        raise ValueError(
            "Please, choose only one option among (1) Orgunits, (2) Group(s) of orgunits"
        )


def warning_request(dataset_id: str, datasets: dict, selected_ou_ids: set) -> set | None:
    """Check for warnings in the datasets.

    Args:
        dataset_id (str): dataset ID.
        datasets (dict): Dictionary containing dataset information.
        ous (list): List of organisation unit IDs.
        selected_ou_ids (set): set of orgunit ids selected by the parameters.

    Returns:
        set or None: If `data_element_ids` is a non-empty list, returns a set of all data elements
        associated with the datasets that are not in `data_element_ids`. Otherwise, returns None.
    """
    if dataset_id not in datasets:
        current_run.log_error(f"Dataset id: {dataset_id} not found in this DHIS2 instance.")
        raise ValueError(f"Dataset id: {dataset_id} not found in this DHIS2 instance.")
    datasets_ous = {ou for ou in datasets[dataset_id]["organisation_units"]}
    dataset_ous_intersection = selected_ou_ids.intersection(datasets_ous)
    if len(dataset_ous_intersection) != len(selected_ou_ids):
        current_run.log_warning(
            f"Only {len(dataset_ous_intersection)} orgunits out of {len(selected_ou_ids)} \
            selected are associated to the datasets. If this is unexpected, verify the orgunits\
            associated to the dataset {datasets[dataset_id]['name']} in your DHIS2 instance."
        )
        if len(dataset_ous_intersection) == 0:
            current_run.log_error(
                f"No orgunits associated to the datasets {datasets[dataset_id]['name']}."
            )
    return dataset_ous_intersection


def get_ous(dhis: DHIS2) -> list[dict]:
    """Retrieves the organisation units from the DHIS instance.

    Args:
        dhis: The DHIS instance.

    Returns:
        A list of organisation units.
    """
    return dhis.meta.organisation_units()


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
    current_run.log_error(f"Invalid date format: {date_str}. Expected ISO format (yyyy-mm-dd).")
    raise ValueError(f"Invalid date format: {date_str}. Expected ISO format (yyyy-mm-dd).")


def get_all_descendant_org_units(dhis: DHIS2, org_unit_id: str) -> list[str]:
    """Recursively retrieves all descendant organization unit IDs for a given organization unit.

    Args:
        dhis: The DHIS2 client object used to interact with the DHIS2 API.
        org_unit_id: The ID of the parent organization unit.

    Returns:
        list[str]: A list of descendant organization unit IDs.
    """
    descendants = []

    def recurse(parent_id: str):
        params = {"fields": "children[id]"}
        response = dhis.api.get(f"organisationUnits/{parent_id}.json", params=params)
        children = response.get("children", [])

        for child in children:
            child_id = child["id"]
            descendants.append(child_id)
            recurse(child_id)

    recurse(org_unit_id)
    return descendants


def get_dataset_org_units(dhis: DHIS2, dataset_id: str) -> list[str]:
    """Retrieve the list of organization unit IDs associated with a given dataset.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        dataset_id (str): The ID of the dataset.

    Returns:
        list[str]: A list of organization unit IDs linked to the specified dataset.
    """
    params = {"fields": "organisationUnits[id]"}
    response = dhis.api.get(f"dataSets/{dataset_id}.json", params=params)
    return [ou["id"] for ou in response.get("organisationUnits", [])]


def fetch_dataset_data_for_valid_descendants(
    dhis: DHIS2, dataset_id: str, parent_org_unit_ids: list[str], start_date: str, end_date: str
) -> pl.DataFrame:
    """Fetch dataset data for valid descendant organization units.

    This function retrieves all descendant organization units for the given parent organization unit
    IDs, filters them to include only those linked to the specified dataset, and then extracts the
    dataset data for the valid organization units within the specified date range.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        dataset_id (str): The ID of the dataset.
        parent_org_unit_ids (list[str]): List of parent organization unit IDs.
        start_date (str): The start date for data extraction.
        end_date (str): The end date for data extraction.

    Returns:
        pl.DataFrame: The extracted dataset data for the valid descendant organization units.
    """
    # Step 1: Get all descendants
    all_descendants = set()
    for org_unit_id in parent_org_unit_ids:
        try:
            descendants = get_all_descendant_org_units(dhis, org_unit_id)
            all_descendants.update(descendants)
        except Exception as e:
            current_run.log_error(f"Failed to get descendants for {org_unit_id}: {e}")
            continue

    # Step 2: Get dataset-linked org units
    dataset_org_units = get_dataset_org_units(dhis, dataset_id)

    # Step 3: Filter intersection
    valid_org_units = [ou for ou in all_descendants if ou in dataset_org_units]

    # Step 4: Fetch data for valid org units
    try:
        data_values = extract_dataset(
            dhis2=dhis,
            dataset=dataset_id,
            start_date=start_date,
            end_date=end_date,
            org_units=valid_org_units,
            org_unit_groups=None,
        )
    except Exception as e:
        current_run.log_error(f"Failed to extract dataset: {e}")
        raise

    return data_values


def fetch_dataset_data_for_valid_group_orgunits(
    dhis: DHIS2,
    dataset_id: str,
    org_unit_group_ids: list[str],
    start_date: datetime,
    end_date: datetime,
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

    Returns:
        pl.DataFrame: The extracted dataset data for the valid organization units in the specified
        groups.

    Raises:
        Exception: If fetching dataset org units or extracting the dataset fails.
    """
    valid_org_units = []

    # Step 1: Get dataset-linked org units
    try:
        dataset_org_units = get_dataset_org_units(dhis, dataset_id)
    except Exception as e:
        current_run.log_error(f"Failed to fetch dataset org units: {e}")
        raise

    # Step 2: For each group, collect matching org units
    for group_id in org_unit_group_ids:
        try:
            params = {"fields": "organisationUnits[id]"}
            response = dhis.api.get(f"organisationUnitGroups/{group_id}.json", params=params)
            group_ous = response.get("organisationUnits", [])
            group_ou_ids = [ou["id"] for ou in group_ous]
            matching_ou_ids = [ou_id for ou_id in group_ou_ids if ou_id in dataset_org_units]
            valid_org_units.extend(matching_ou_ids)
        except Exception as e:
            current_run.log_error(f"Failed to process org unit group {group_id}: {e}")
            continue

    # Step 3: Remove duplicates
    valid_org_units = list(set(valid_org_units))

    # Step 4: Call extract_dataset on the final filtered org units
    try:
        data_values = extract_dataset(
            dhis2=dhis,
            dataset=dataset_id,
            start_date=start_date,
            end_date=end_date,
            org_units=valid_org_units,
            org_unit_groups=None,
        )
    except Exception as e:
        current_run.log_error(f"Failed to extract dataset from org unit groups: {e}")
        raise

    return data_values


def extract_raw_data(
    dhis: DHIS2,
    # selected_ou_ids: set,
    dataset_id: str,
    datasets: dict,
    start: str,
    end: str,
    ou_ids: list[str] | None,
    ou_group_ids: list[str] | None,
    include_children: bool,
) -> pd.DataFrame:
    """Extracts raw data from DHIS2 for the given datasets and time range.

    Args:
        dhis (DHIS2): DHIS2 client object used to interact with the DHIS2 API.
        selected_ou_ids (set): Set of selected organization unit IDs to extract data for.
        dataset_id (str): dataset ID to extract data from.
        datasets (dict): Dictionary containing dataset information, including data elements and
        organisation units.
        start (str): Start date of the time range to extract data from.
        end (str): End date of the time range to extract data from.
        ou_ids (list[str] | None): List of organization unit IDs or None.
        ou_group_ids (list[str] | None): List of organization unit group IDs or None.
        include_children (bool): Whether to include child organizational units.

    Returns:
        pd.DataFrame: Pandas DataFrame containing the extracted raw data, with additional columns
        for dataset name and period type.
    """
    start_init = start
    end_init = end
    period_type = datasets[dataset_id]["periodType"]
    start = isodate_to_period_type(start_init, period_type)
    end = isodate_to_period_type(end_init, period_type)
    dataset_name = datasets[dataset_id]["name"]
    dataset_period_type = datasets[dataset_id]["periodType"]
    current_run.log_info(f"Extracting data for dataset {dataset_name}")
    current_run.log_info(f"Period type: {dataset_period_type}")
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
        if isinstance(ou_ids, list) and len(ou_ids) > 0 and include_children:
            current_run.log_info(
                f"Fetching data cutting request for all descendants of orgunits: {ou_ids}"
            )
            data_values = fetch_dataset_data_for_valid_descendants(
                dhis=dhis,
                dataset_id=dataset_id,
                parent_org_unit_ids=ou_ids,
                start_date=start.datetime,
                end_date=end.datetime,
            )
        elif isinstance(ou_group_ids, list) and len(ou_group_ids) > 0:
            current_run.log_info(
                f"Fetching data cutting request for all orgunits in groups: {ou_group_ids}"
            )
            data_values = fetch_dataset_data_for_valid_group_orgunits(
                dhis=dhis,
                dataset_id=dataset_id,
                org_unit_group_ids=ou_group_ids,
                start_date=start.datetime,
                end_date=end.datetime,
            )
        else:
            current_run.log_error(
                f"Failed to extract dataset {dataset_id} for period {start_init} to {end_init}."
            )
            raise e

    length_table = data_values.height
    current_run.log_info("Number of rows extracted (total) : " + str(length_table))
    if length_table > 0:
        ous = get_organisation_units(dhis)
        data_elements = get_data_elements(dhis)
        category_option_combos = get_category_option_combos(dhis)

        data_values = join_object_names(
            df=data_values,
            data_elements=data_elements,
            category_option_combos=category_option_combos,
            organisation_units=ous,
        )

    data_values = data_values.with_columns(pl.lit(dataset_name).alias("dataset"))
    data_values = data_values.with_columns(
        [
            pl.Series(
                "period_type_extracted",
                data_values["period"].map_elements(
                    lambda x: str(type(period_from_string(x)).__name__).replace("Week", "Weekly")
                ),
            )
        ]
    )

    return data_values.with_columns(
        pl.lit(dataset_period_type).alias("period_type_configured_dataset")
    )


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
    """Converts an ISO date string to a DHIS2-compatible period string based on the specified period type.

    Args:
        date (str): The ISO date string in the format "YYYY-MM-DD".
        period_type (str): The DHIS2 period type. Supported values include:
            - "Daily": Converts to a daily period (e.g., "20230101").
            - "Weekly": Converts to a weekly period starting on Monday (e.g., "2023W1").
            - "WeeklyMonday", "WeeklyTuesday", ..., "WeeklySunday": Converts to a weekly period
              aligned to the specified weekday.
            - "Monthly": Converts to a monthly period (e.g., "202301").
            - "BiMonthly": Converts to a bi-monthly period (e.g., "202301" for Jan-Feb).
            - "Quarterly": Converts to a quarterly period (e.g., "2023Q1").
            - "SixMonthly": Converts to a six-monthly period (e.g., "2023S1" for Jan-Jun).
            - "SixMonthlyApril": Converts to a six-monthly period starting in April (e.g., "2023AprilS1").
            - "Yearly": Converts to a yearly period (e.g., "2023").
            - "FinancialApril": Converts to a financial year starting in April (e.g., "2023April").
            - "FinancialJuly": Converts to a financial year starting in July (e.g., "2023July").
            - "FinancialOct": Converts to a financial year starting in October (e.g., "2023Oct").

    Returns:
        Period: A DHIS2-compatible period object created from the generated period string.

    Raises:
        ValueError: If the provided period type is unsupported.
    """  # noqa: E501
    """Converts an ISO date to a DHIS2-compatible period string with support for weekly anchors."""
    # Maps DHIS2 weekly period type to its anchor weekday (0 = Monday, 6 = Sunday)
    weekly_anchors = {
        "Weekly": 0,
        "WeeklyMonday": 0,
        "WeeklyTuesday": 1,
        "WeeklyWednesday": 2,
        "WeeklyThursday": 3,
        "WeeklyFriday": 4,
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
        if period_type != "Weekly" and period_type != "WeeklyMonday":
            # If the period type specifies a weekday, append it
            period_str = f"{iso_year}{period_type.replace('Weekly', '')[:3]}W{iso_week}"
        else:
            period_str = f"{iso_year}W{iso_week}"  # No leading zero

    elif period_type == "Monthly":
        period_str = dt.strftime("%Y%m")

    elif period_type == "BiMonthly":
        period_str = f"{dt.year}0{(dt.month - 1) // 2 + 1}"

    elif period_type == "Quarterly":
        period_str = f"{dt.year}Q{(dt.month - 1) // 3 + 1}"

    elif period_type == "SixMonthly":
        period_str = f"{dt.year}S{1 if dt.month <= 6 else 2}"

    elif period_type == "SixMonthlyApril":
        if 4 <= dt.month <= 9:
            period_str = f"{dt.year}AprilS1"
        else:
            ref_year = dt.year if dt.month >= 4 else dt.year - 1
            period_str = f"{ref_year}AprilS2"

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

    else:
        raise ValueError(f"Unsupported DHIS2 period type: {period_type}")

    return period_from_string(period_str)


def get_periods_with_no_data(
    retrieve_periods: list[str], start: str, end: str, dataset: dict
) -> list[str]:
    """Get the periods with no data associated.

    Args:
        retrieve_periods (List[str]): List of periods with data.
        start (str): Start date in ISO format.
        end (str): End date in ISO format.
        dataset (dict): The dataset metadata.

    Returns:
        List[str]: List of periods with no data associated.
    """
    period_type = dataset["periodType"]
    dataset_name = dataset["name"]
    start = isodate_to_period_type(start, period_type)
    end = isodate_to_period_type(end, period_type)
    if start != end:
        expected_periods = start.range(end)
    else:
        expected_periods = [start]
    expected_periods = [str(p) for p in expected_periods]
    retrieve_periods = [str(p) for p in retrieve_periods]
    missing_periods = [p for p in expected_periods if p not in retrieve_periods]
    unexpected_periods = [p for p in retrieve_periods if p not in expected_periods]
    if len(missing_periods) > 0:
        current_run.log_warning(
            f"Following periods have no data: {sorted(missing_periods)} for dataset {dataset_name}"
        )
    if len(unexpected_periods) > 0:
        current_run.log_warning(
            f"Following periods not expected: \
                {sorted(unexpected_periods)} for dataset {dataset_name}"
        )

    return missing_periods


def get_dataelements_with_no_data(retrieve_dataelements: list[str], dataset: dict) -> list[str]:
    """Returns a list of data elements that are expected but not found in the retrieved data.

    Args:
        retrieve_dataelements (List[str]): A list of data elements retrieved from a source.
        dataset (dict): The dataset metadata.

    Returns:
        List[str]: A list of data elements that are expected but not found in the retrieved data.
    """
    expected_data_elements = dataset["data_elements"]
    dataset_name = dataset["name"]
    missing_dataelements = [dx for dx in expected_data_elements if dx not in retrieve_dataelements]
    if len(missing_dataelements) > 0:
        current_run.log_warning(
            f"The following data elements have no data: {missing_dataelements} for {dataset_name}"
        )
    return missing_dataelements


def get_datasets_as_dict(dhis: DHIS2) -> dict[dict]:
    """Get datasets metadata.

    Args:
    ----
    dhis (DHIS2): The DHIS2 connection object.
    dhis2_name (str): The name of the DHIS2 connection.

    Returns:
    -------
    dict[dict] : dictionnary of dict Id, name, data elements, indicators and org units of all
    datasets.
    """
    datasets = {}
    for page in dhis.api.get_paged(
        "dataSets",
        params={
            "fields": "id,name,dataSetElements,indicators,organisationUnits,periodType",
            "pageSize": 10,
        },
    ):
        for ds in page["dataSets"]:
            ds_id = ds.get("id")
            row = datasets.setdefault(ds_id, {})
            row["name"] = ds.get("name")
            row["data_elements"] = [dx["dataElement"]["id"] for dx in ds["dataSetElements"]]
            row["indicators"] = [indicator["id"] for indicator in ds["indicators"]]
            row["organisation_units"] = [ou["id"] for ou in ds["organisationUnits"]]
            row["periodType"] = ds["periodType"]
    return datasets


# --------------------------------------------------------------------------------------------
#  ----------------------------FUNCTIONS NOT USED ANYMORE -----------------------------------
# --------------------------------------------------------------------------------------------
def select_ous(
    dhis: DHIS2,
    all_ous: list[dict],
    ou_ids: list[str] | None,
    ou_group_ids: list[str] | None,
    include_children: bool,
    conditions: list[bool],
) -> list[dict]:
    """Select organizational units based on the provided filters.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        all_ous (list[dict]): A list of all organizational units.
        ou_ids (list[str] | None): List of organization unit IDs or None.
        ou_group_ids (list[str] | None): List of organization unit group IDs or None.
        include_children (bool): Whether to include child organizational units.
        conditions (list[bool]): Validation conditions for the parameters.

    Returns:
        list[dict]: A list of selected organizational units.
    """
    selected_ou_ids = set()
    if conditions["ou_ids + include_children"]:
        all_ous_dict = {ou["id"]: ou for ou in all_ous}
        for root_ou in ou_ids:
            selected_ou_ids.add(root_ou)
            if include_children:
                root_path = all_ous_dict[root_ou]["path"]
                for ou in all_ous:
                    if ou["path"].startswith(root_path + "/"):
                        selected_ou_ids.add(ou["id"])

    elif conditions["ou_group_ids only"]:
        dhis2_ou_groups = dhis.meta.organisation_unit_groups()
        ous_in_group_ids = [
            ou
            for group in dhis2_ou_groups
            for ou in group["organisationUnits"]
            if group["id"] in ou_group_ids
        ]
        print(ous_in_group_ids)
        for ou in ous_in_group_ids:
            selected_ou_ids.add(ou)
    else:
        selected_ou_ids = {ou["id"] for ou in all_ous}
    return selected_ou_ids


def parse_period_column(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and standardize the 'period' column in a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame containing a 'period' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'period' column parsed into a standardized
        datetime format.

    Raises:
        ValueError: If the 'period' column contains an unrecognized format.
    """
    if "pe" not in df.columns:
        return df
    df["pe"] = df["pe"].map(lambda x: period_from_string(x).datetime)
    return df


def period_to_period_type(period: str) -> str:
    """Detect the DHIS2 periodType from a DHIS2 period string,optionally specifying a weekly anchor.

    Args:
        period (str): A DHIS2 period string (e.g., '202401', '2023Q3', '2023W14', etc.)
        anchor_day (str): Week start day for weekly periods. E.g., 'Wednesday', 'Sunday'

    Returns:
        str: The detected DHIS2 period type (e.g., 'WeeklyWednesday')

    Raises:
        ValueError: If the format is unrecognized.
    """
    if re.fullmatch(r"\d{8}", period):
        return "Daily"

    if re.fullmatch(r"\d{6}", period):
        return "Monthly"

    if re.fullmatch(r"\d{5}", period):  # e.g., '20240' for BiMonthly
        return "BiMonthly"

    if re.fullmatch(r"\d{4}Q[1-4]", period):
        return "Quarterly"

    if re.fullmatch(r"\d{4}S[1-2]", period):
        return "SixMonthly"

    if re.fullmatch(r"\d{4}AprilS[1-2]", period):
        return "SixMonthlyApril"

    if re.fullmatch(r"\d{4}", period):
        return "Yearly"

    if re.fullmatch(r"\d{4}April", period):
        return "FinancialApril"

    if re.fullmatch(r"\d{4}July", period):
        return "FinancialJuly"

    if re.fullmatch(r"\d{4}Oct", period):
        return "FinancialOct"
    match = re.fullmatch(r"(\d{4})([A-Za-z]{3})?W(\d{1,2})", period)
    if match:
        _, day, _ = match.groups()
        anchor = day if day else ""
        return "Weekly" + anchor

    raise ValueError(f"Unrecognized DHIS2 period format: {period}")


if __name__ == "__main__":
    dhis2_extract_dataset()
