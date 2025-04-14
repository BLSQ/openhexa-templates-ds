"""Template for newly generated pipelines."""

import json
import tempfile
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import geopandas as gpd
import pandas as pd
import polars as pl
from openhexa.sdk import Dataset, workspace
from openhexa.sdk.pipelines import current_run, parameter, pipeline
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.sdk.workspaces.connection import DHIS2Connection
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import Period, period_from_string


@pipeline("dhis2_extract_dataset")
@parameter(
    "dhis_con",
    name="DHIS2 Connection",
    type=DHIS2Connection,
    default="dhis2-demo-2-41",
    required=True,
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
@parameter(
    "datasets_ids",
    type=str,
    widget=DHIS2Widget.DATASETS,
    connection="dhis_con",
    multiple=True,
    default=["TuL8IOPzpHh"],
    required=True,
)
@parameter(
    "data_element_ids",
    type=str,
    widget=DHIS2Widget.DATA_ELEMENTS,
    connection="dhis_con",
    multiple=True,
    required=False,
    default=["FvKdfA2SuWI", "p1MDHOT6ENy"],
)
@parameter(
    "ou_level",
    name="Level of orgunits",
    widget=DHIS2Widget.ORG_UNIT_LEVELS,
    connection="dhis_con",
    type=int,
    required=False,
    default=None,
)
@parameter(
    "ou_ids",
    name="Orgunits",
    widget=DHIS2Widget.ORG_UNITS,
    connection="dhis_con",
    type=str,
    multiple=True,
    required=False,
    default=None,
)
@parameter(
    "ou_group_ids",
    name="Group(s) of orgunits",
    widget=DHIS2Widget.ORG_UNIT_GROUPS,
    connection="dhis_con",
    type=str,
    multiple=True,
    required=False,
    default=None,
)
@parameter("include_children", type=bool, required=False, default=False)
@parameter(
    "use_cache",
    name="Use already extracted data if available",
    help="If true, the pipeline will use already extracted data if available.",
    type=bool,
    required=True,
    default=True,
)
@parameter("add_dx_name", type=bool, required=False, default=True)
@parameter("add_coc_name", type=bool, required=False, default=True)
@parameter("add_org_unit_parent", type=bool, required=False, default=True)
def dhis2_extract_dataset(
    dhis_con: DHIS2Connection,
    datasets_ids: list[str],
    data_element_ids: list[str] | None,
    ou_group_ids: list[str] | None,
    ou_ids: list[str] | None,
    ou_level: int | None,
    include_children: bool,
    start: str,
    end: str | None,
    use_cache: bool,
    add_dx_name: bool,
    add_org_unit_parent: bool,
    add_coc_name: bool,
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive
    computations.
    """
    dhis = get_dhis(dhis_con)
    start = valid_date(start)
    end = valid_date(end)
    dhis2_name = get_dhis2_name_domain(dhis_con)
    ds = get_datasets(dhis, dhis2_name)
    ous = get_ous(dhis)
    conditions = parameters_validation(ou_ids, ou_group_ids, ou_level)
    selected_ous = select_ous(
        dhis, ous, ou_ids, ou_group_ids, ou_level, include_children, conditions
    )
    data_element_ids = warning_request(datasets_ids, ds, data_element_ids, ous)
    dhis2_name = create_extraction_folder(dhis2_name, ds, datasets_ids)
    table = extract_raw_data(
        dhis, dhis2_name, use_cache, selected_ous, datasets_ids, ds, start, end, data_element_ids
    )
    table = enrich_data(
        dhis, dhis2_name, ous, table, add_dx_name, add_org_unit_parent, add_coc_name
    )
    warning_post_extraction(table, ds, datasets_ids, start, end)
    save_table(table, dhis2_name)


@dhis2_extract_dataset.task
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


@dhis2_extract_dataset.task
def create_extraction_folder(dhis2_name: str, ds: dict, ids: list[str]) -> str:
    """Creates a folder structure for data extraction.

    Args:
        dhis2_name (str): The name of the DHIS2 instance.
        ds (dict): A dictionary containing dataset information.
        ids (list[str]): A list of dataset IDs.

    Returns:
        str: The name of the DHIS2 instance.
    """
    Path(f"{workspace.files_path}/{dhis2_name}").mkdir(parents=True, exist_ok=True)
    for i in ids:
        name = ds[i]["name"].replace("/", "-").replace("\\", "-")
        Path(f"{workspace.files_path}/{dhis2_name}/{name}").mkdir(parents=True, exist_ok=True)
    return dhis2_name


@dhis2_extract_dataset.task
def get_dhis(dhis_con: DHIS2Connection) -> DHIS2:  # noqa: D417
    """Creates and returns a DHIS2 object using the provided DHIS2Connection.

    Parameters
    ----------
        dhis_con (DHIS2Connection): The DHIS2Connection object to use for creating the DHIS2 object.

    Returns
    -------
        DHIS2: The created DHIS2 object.

    """
    return DHIS2(dhis_con)


@dhis2_extract_dataset.task
def save_table(table: pd.DataFrame, dhis2_name: str):
    """Saves the given table to DHIS2 and optionally to the OH database.

    Args:
        table (pd.DataFrame): The table to be saved.
        dhis2_name (str): The name of the DHIS2 instance.
        openhexa_dataset (Dataset | None): The OpenHexa dataset to save the table to. If None,
        the table will not be saved to the OpenHexa database.
    """
    table.to_csv(f"{workspace.files_path}/{dhis2_name}/dataset_extraction.csv", index=False)
    current_run.add_file_output(f"{workspace.files_path}/{dhis2_name}/dataset_extraction.csv")
    current_run.log_info(f"Output: {workspace.files_path}/{dhis2_name}/dataset_extraction.csv")


@dhis2_extract_dataset.task
def warning_post_extraction(
    table: pd.DataFrame, datasets: dict, ids: list[str], start: str, end: str
):
    """Check for warnings in the extracted data.

    Args:
        table (pd.DataFrame): The extracted data.
        datasets (dict): Dictionary containing dataset information.
        ids (list[str]): List of dataset IDs.
        start (str): Start date of the extraction.
        end (str): End date of the extraction.

    """
    if len(table) > 0:
        periods = [str(p) for p in table["pe"].unique()]
        for i in ids:
            get_periods_with_no_data(periods, start, end, datasets[i])
            get_dataelements_with_no_data(table["dx"].unique(), datasets[i])


@dhis2_extract_dataset.task
def parameters_validation(
    ou_ids: list[str] | None, ou_group_ids: list[str] | None, ou_level: int | None
) -> dict[str, bool]:
    """Validates the parameters for organizational unit selection.

    Args:
        ou_ids (list[str] | None): List of organization unit IDs or None.
        ou_group_ids (list[str] | None): List of organization unit group IDs or None.
        ou_level (int | None): Level of organization units or None.

    Returns:
        dict[str, bool]: A dictionary indicating the validation conditions for the parameters.
    """
    conditions = {
        "ou_ids + include_children": isinstance(ou_ids, list)
        and len(ou_ids) > 0
        and len(ou_group_ids) == 0
        and not isinstance(ou_level, int),
        "ou_group_ids only": isinstance(ou_group_ids, list)
        and len(ou_group_ids) > 0
        and len(ou_ids) == 0
        and not isinstance(ou_level, int),
        "ou_level only": isinstance(ou_level, int) and len(ou_ids) == 0 and len(ou_group_ids) == 0,
    }
    if sum([1 for condition in conditions.values() if condition]) != 1:
        current_run.log_error(
            "Invalid orgunit filter: choose only one option among "
            "(1) ou_ids, (2) ou_group_ids, (3) ou_level"
        )
        raise ValueError(
            "Invalid orgunit filter: choose only one option among "
            "(1) ou_ids, (2) ou_group_ids, (3) ou_level"
        )
    return conditions


@dhis2_extract_dataset.task
def select_ous(
    dhis: DHIS2,
    all_ous: list[dict],
    ou_ids: list[str] | None,
    ou_group_ids: list[str] | None,
    ou_level: int | None,
    include_children: bool,
    conditions: list[bool],
) -> list[dict]:
    """Select organizational units based on the provided filters.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        all_ous (list[dict]): A list of all organizational units.
        ou_ids (list[str] | None): List of organization unit IDs or None.
        ou_group_ids (list[str] | None): List of organization unit group IDs or None.
        ou_level (int | None): Level of organization units or None.
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

    elif conditions["ou_level only"]:
        for ou in all_ous:
            if ou["level"] == ou_level:
                selected_ou_ids.add(ou["id"])
    return selected_ou_ids


@dhis2_extract_dataset.task
def warning_request(
    ids: list[str], datasets: dict, data_element_ids: list[str], ous: list[str]
) -> set | None:
    """Check for warnings in the datasets.

    Args:
        ids (list): List of dataset IDs.
        datasets (dict): Dictionary containing dataset information.
        data_element_ids (list): List of data element IDs.
        ous (list): List of organisation unit IDs.

    Returns:
        set or None: If `data_element_ids` is a non-empty list, returns a set of all data elements
        associated with the datasets that are not in `data_element_ids`. Otherwise, returns None.
    """
    levels = {level for i in ids for level in get_levels(ous, datasets[i]["organisation_units"])}
    frequencies = {datasets[i]["periodType"] for i in ids}
    if len(levels) > 1:
        current_run.log_warning(
            f"The orgunits associated to your datasets have mixed levels : {levels}"
        )
    if len(frequencies) > 1:
        current_run.log_warning(
            f"The frequency associated to your datasets are mixed : {frequencies}"
        )
    print(data_element_ids, type(data_element_ids))
    if isinstance(data_element_ids, list) and len(data_element_ids) > 0:
        all_data_elements = {dx for i in ids for dx in datasets[i]["data_elements"]}
        unmatched_data_elements = set(data_element_ids) - all_data_elements
        if len(unmatched_data_elements) > 0:
            current_run.log_error(
                f"Data elements {unmatched_data_elements} are not associated to any dataset"
            )
            if len(unmatched_data_elements) == len(data_element_ids):
                current_run.log_error(
                    f"All data elements {data_element_ids} are not associated to any dataset"
                )
                raise ValueError(
                    f"None of the data elements {data_element_ids} are associated any dataset"
                )
        return all_data_elements.intersection(set(data_element_ids))
    return None


@dhis2_extract_dataset.task
def get_datasets(dhis: DHIS2, dhis2_name: str) -> dict:
    """Retrieves datasets metadata from the DHIS2 instance.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        dhis2_name (str): The name of the DHIS2 instance.

    Returns:
        dict: A dictionary containing dataset metadata.
    """
    try:
        ds = dhis.meta.datasets()
        assert isinstance(ds, dict)
    except Exception:
        ds = datasets_temp(dhis, dhis2_name)
    return ds


@dhis2_extract_dataset.task
def get_ous(dhis: DHIS2) -> list[dict]:
    """Retrieves the organisation units from the DHIS instance.

    Args:
        dhis: The DHIS instance.

    Returns:
        A list of organisation units.
    """
    return dhis.meta.organisation_units()


@dhis2_extract_dataset.task
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


@dhis2_extract_dataset.task
def extract_raw_data(
    dhis: DHIS2,
    dhis2_name: str,
    use_cache: bool,
    selected_ou_ids: set,
    datasets_ids: list[str],
    datasets: dict,
    start: str,
    end: str,
    data_element_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Extracts raw data from DHIS2 for the given datasets and time range.

    Args:
        dhis (DHIS2): DHIS2 client object used to interact with the DHIS2 API.
        dhis2_name (str): The name of the DHIS2 instance.
        use_cache (bool): Use already extracted data.
        selected_ou_ids (set): Set of selected organization unit IDs to extract data for.
        datasets_ids (list[str]): List of dataset IDs to extract data from.
        datasets (dict): Dictionary containing dataset information, including data elements and
        organisation units.
        start (str): Start date of the time range to extract data from.
        end (str): End date of the time range to extract data from.
        data_element_ids (list[str] | None): Optional list of data element IDs to filter the
        extracted data. If None, all data elements will be included.

    Returns:
        pd.DataFrame: Pandas DataFrame containing the extracted raw data, with additional columns
        for dataset name and period type.
    """
    start_init = start
    end_init = end
    res = pd.DataFrame()
    for ds_id in datasets_ids:
        selected_data_elements = select_data_elements(
            data_element_ids, datasets[ds_id]["data_elements"]
        )
        datasets_ous = {ou for ou in datasets[ds_id]["organisation_units"]}
        dataset_ous_intersection = selected_ou_ids.intersection(datasets_ous)
        if len(dataset_ous_intersection) != len(selected_ou_ids):
            current_run.log_warning(
                f"Only {len(dataset_ous_intersection)} orgunits out of {len(selected_ou_ids)} \
                selected are associated to the datasets."
            )
            if len(dataset_ous_intersection) == 0:
                current_run.log_error(
                    f"No orgunits associated to the datasets {datasets[ds_id]['name']}."
                )
                continue
        period_type = datasets[ds_id]["periodType"]
        start = isodate_to_period_type(start_init, period_type)
        end = isodate_to_period_type(end_init, period_type)
        current_run.log_info(f"Extracting data for dataset {datasets[ds_id]['name']}")
        if (
            dhis.version >= "2.39"
            and selected_data_elements is not None
            and len(selected_data_elements) > 0
        ):
            for dx in selected_data_elements:
                dx_name = (
                    dhis.meta.data_elements(fields="id,name", filters={f"id:eq:{dx}"})[0]["name"]
                    .replace("/", "-")
                    .replace("\\", "-")
                )
                Path(
                    f"{workspace.files_path}/{dhis2_name}/{datasets[ds_id]['name']}/{dx_name}"
                ).mkdir(parents=True, exist_ok=True)
                for pe in start.get_range(end):
                    if (
                        Path(
                            f"{workspace.files_path}/{dhis2_name}/{datasets[ds_id]['name']}/{dx_name}/{pe}.csv"
                        ).exists()
                        and use_cache
                    ):
                        df = pd.read_csv(
                            f"{workspace.files_path}/{dhis2_name}/{datasets[ds_id]['name']}/{dx_name}/{pe}.csv"
                        )
                    else:
                        data_values = dhis.data_value_sets.get(
                            datasets=[ds_id],
                            data_elements=[dx],
                            org_units=list(dataset_ous_intersection),
                            periods=[pe],
                        )
                        df = pd.DataFrame(data_values)
                        if df.empty:
                            current_run.log_warning(f"No data for {dx_name} for period {pe}")
                            continue
                        df["dataset"] = datasets[ds_id]["name"]
                        df["periodType"] = datasets[ds_id]["periodType"]
                        df.to_csv(
                            f"{workspace.files_path}/{dhis2_name}/{datasets[ds_id]['name']}/{dx_name}/{pe}.csv"
                        )
                        current_run.log_info(f"Data for period {pe} saved: {df.shape[0]} rows")
                    res = pd.concat([res, df], ignore_index=True)
        else:
            for pe in start.get_range(end):
                data_values = dhis.data_value_sets.get(
                    datasets=[ds_id], org_units=list(dataset_ous_intersection), periods=[pe]
                )
                if len(data_values) == 0:
                    current_run.log_warning(
                        f"No data for {datasets[ds_id]['name']} for period {pe}"
                    )
                    continue
                df = pd.DataFrame(data_values)
                df["dataset"] = datasets[ds_id]["name"]
                df["periodType"] = datasets[ds_id]["periodType"]
                for dx in datasets[ds_id]["data_elements"]:
                    df_dx = df[df["dataElement"] == dx]
                    if df_dx.empty:
                        continue
                    dx_name = (
                        dhis.meta.data_elements(fields="id,name", filters={f"id:eq:{dx}"})[0][
                            "name"
                        ]
                        .replace("/", "-")
                        .replace("\\", "-")
                    )
                    Path(
                        f"{workspace.files_path}/{dhis2_name}/{datasets[ds_id]['name']}/{dx_name}"
                    ).mkdir(parents=True, exist_ok=True)
                    df_dx.to_csv(
                        f"{workspace.files_path}/{dhis2_name}/{datasets[ds_id]['name']}/{dx_name}/{pe}.csv"
                    )
                    current_run.log_info(f"Data for period {pe} saved: {df.shape[0]} rows")
                res = pd.concat([res, df], ignore_index=True)
    return res


@dhis2_extract_dataset.task
def enrich_data(
    dhis: DHIS2,
    dhis2_name: str,
    ous: list[dict],
    table: pd.DataFrame,
    add_dx_name: bool,
    add_org_unit_parent: bool,
    add_coc_name: bool,
) -> pd.DataFrame:
    """Enriches the given table with additional columns based on the specified parameters.

    Args:
        dhis (DHIS2): The DHIS2 object used for metadata retrieval.
        dhis2_name (str): The name of the DHIS2 instance.
        ous (list[dict]): A list of organisation units.
        table (pd.DataFrame): The table to be enriched.
        add_dx_name (bool): Whether to add the dx_name column.
        add_org_unit_parent (bool): Whether to add the org_unit_parent column.
        add_coc_name (bool): Whether to add the coc_name column.

    Returns:
        pd.DataFrame: The enriched table.
    """
    length_table = len(table)
    current_run.log_info("Number of rows extracted (total) : " + str(length_table))
    if length_table > 0:
        table = table.rename(columns={"dataElement": "dx", "orgUnit": "ou", "period": "pe"})
        table["pe"] = table["pe"].astype(str)
        print(table.sample(1))
        print(table.columns)
        if add_dx_name:
            table = dhis.meta.add_dx_name_column(table)
        if add_coc_name:
            table = dhis.meta.add_coc_name_column(table, "categoryOptionCombo")
        if add_org_unit_parent:
            # table = dhis.meta.add_org_unit_parent_columns(table)
            ous = get_orgunits_with_parents(ous, dhis2_name)
            table = add_parents(table, ous)
            table = table.drop(columns=["level", "geometry", "path"])

    return table


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


def get_week_as_dhis2(date: str) -> str:
    """Converts a given date to the DHIS2 week format.

    Args:
        date (str): The date in the format 'YYYY-MM-DD'.

    Returns:
        str: The date in the DHIS2 week format, e.g., 'YYYYWww'.
    """
    date_y = datetime.strptime(date, "%Y-%m-%d").strftime("%Y")
    week_number = datetime.strptime(date, "%Y-%m-%d").isocalendar().week
    return f"{date_y}W{week_number}"


def isodate_to_period_type(date: str, period_type: str) -> Period:
    """Converts a given date to the specified period type.

    Args:
        date (str): The input date in ISO format (YYYY-MM-DD).
        period_type (str): The desired period type. Valid options are "Monthly", "Yearly",
        "Quarterly", "Weekly", and "Daily".

    Returns:
        str: The converted date in the specified period type format.

    Raises:
        ValueError: If an invalid period type is provided.

    """
    if period_type == "Monthly":
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m")
    elif period_type == "Yearly":
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y")
    elif period_type == "Quarterly":
        date = (
            datetime.strptime(date, "%Y-%m-%d").strftime("%Y")
            + "Q"
            + str((datetime.strptime(date, "%Y-%m-%d").month - 1) // 3 + 1)
        )
    elif period_type == "Weekly":
        date = get_week_as_dhis2(date)
    elif period_type == "Daily":
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")
    else:
        raise ValueError("Invalid period type provided.")

    return period_from_string(date)


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
        excepted_periods = start.get_range(end)
    else:
        excepted_periods = [start]
    retrieve_periods = [period_from_string(p) for p in retrieve_periods]
    missing_periods = [p for p in excepted_periods if p not in retrieve_periods]
    if len(missing_periods) > 0:
        current_run.log_warning(
            f"The following periods have no data: {missing_periods} for dataset {dataset_name}"
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


# add the data to the dataset
def add_to_dataset(table: pd.DataFrame, dhis2_connection: DHIS2, dataset: Dataset):
    """Adds the given table data to a dataset in DHIS2.

    Args:
        table (pd.DataFrame): The table data to be added to the dataset.
        dhis2_connection (DHIS2): The DHIS2 connection object.
        dataset (Dataset): The dataset object to which the data will be added.

    """
    # we do not have access to the connection slug, so we use the url sub-domain instead..
    # for the moment
    subdomain = urlparse(dhis2_connection.url).netloc.split(".")[0]
    dataset_name = f"{subdomain.replace('-', '_')}_dataset_extraction"
    if dataset is None:
        dataset = search_dataset(dataset_name)
        if dataset is None:
            dataset = workspace.create_dataset(dataset_name, "dataset extraction")  # Create dataset
    add_data_to_dataset(data=table, dataset=dataset, fname=dataset_name, extension="csv")


def search_dataset(dataset_name: str) -> Dataset | None:
    """Searches for a dataset with the given name in the workspace.

    Args:
        dataset_name (str): The name of the dataset to search for.

    Returns:
        dataset: The dataset object if found, None otherwise.
    """
    try:
        dataset = workspace.get_dataset(dataset_name)
    except Exception:
        current_run.log_error(f"Dataset {dataset_name} not found")
        return None
    return dataset


def add_data_to_dataset(
    data: pd.DataFrame | gpd.GeoDataFrame | pl.DataFrame,
    dataset: Dataset,
    fname: str,
    extension: str = "csv",
):
    """Add files to a dataset by creating a new version."""
    try:
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, gpd.GeoDataFrame):
            df = data
        elif isinstance(data, pl.DataFrame):
            df = data.to_pandas()  # Convert polars dataFrame to pandas
        else:
            raise ValueError("Input data must be a DataFrame, GeoDataFrame or Polars DataFrame.")

        # Add file to dataset version
        with tempfile.NamedTemporaryFile(suffix=f".{extension}") as tmp:
            if extension == "parquet":
                df.to_parquet(tmp.name)
            elif extension == "csv":
                df.to_csv(tmp.name, index=False)
            elif extension == "gpkg":
                if not isinstance(df, gpd.GeoDataFrame):
                    raise ValueError("GeoDataFrame required for .gpkg format.")
                df.to_file(tmp.name, driver="GPKG")
            else:
                raise ValueError(f"Unsupported file extension: {extension}")

            # create new version (let's just keep the date as version name?)
            dataset_version_name = datetime.now().strftime("%Y-%m-%d_%H:%M")
            new_version = dataset.create_version(dataset_version_name)
            new_version.add_file(tmp.name, filename=f"{fname}.{extension}")

        current_run.log_info(
            f"File {fname}.{extension} saved in new dataset version: {dataset_version_name}"
        )
    except Exception as e:
        raise Exception(f"Error while saving the dataset version: {e}") from e


def select_data_elements(data_element_ids: list[str], data_elements: list[str]) -> list[str] | None:
    """Returns the data elements from data_element_ids that are present in data_elements.

    Args:
    ----
    data_element_ids (list[str]): A list of data element IDs.
    data_elements (list[str]): Another list of data elements IDs.

    Returns:
    -------
    list[str] | None: A list of selected data elements IDs if data_element_ids is not empty
    and contains valid data element IDs, None otherwise.
    """
    if data_element_ids:
        return [dx for dx in data_element_ids if dx in data_elements]
    return None


def get_levels(ous: list[dict], orgunit_ids: dict[str, str]) -> dict[int, str]:
    """Returns a dictionary mapping the levels of the given orgunits to their IDs.

    Args:
    ----
    ous (list[dict]): A list of organizational units, where each unit is represented as
    a dictionary.
    orgunit_ids (dict[str, str]): A dictionary of organizational unit IDs.

    Returns:
    -------
    dict[int, str]: A dictionary mapping the levels of the orgunits to their IDs.
    """
    return {ou["level"]: ou["id"] for ou in ous if ou["id"] in orgunit_ids}


def datasets_temp(dhis: DHIS2, dhis2_name: str) -> list[dict]:
    """Get datasets metadata.

    Args:
    ----
    dhis (DHIS2): The DHIS2 connection object.
    dhis2_name (str): The name of the DHIS2 connection.

    Returns:
    -------
    list[dict] : list of dict Id, name, data elements, indicators and org units of all datasets.
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
    save_metadata("datasets", dhis2_name, datasets)
    return datasets


def path_to_parents_ids(ou: dict) -> dict:
    """Adds parent IDs and names to an organizational unit (OU) dictionary.

    Args:
    ----
    ou (dict): A dictionary representing an organizational unit, containing at least "id", "name",
    "level", and "path".

    Returns:
    -------
    dict: The updated organizational unit dictionary with parent IDs and names added for each level.
    """
    level = ou.get("level")
    ou[f"level_{level}_id"] = ou.get("id")
    ou[f"level_{level}_name"] = ou.get("name")
    if level > 1:
        for parent_level in range(1, int(level)):
            ou[f"level_{parent_level}_id"] = ou.get("path").split("/")[parent_level]
    return ou


def ous_to_dict(ous: list) -> dict:
    """Convert a list of orgunits to a dictionary.

    Returns:
        dict: A dictionary where keys are orgunit IDs and values are the corresponding orgunit
        dictionaries.
    """
    return {ou["id"]: ou for ou in ous}


def find_name(ous: dict, ou_id: str) -> str:
    """Find the name of an orgunit by its ID.

    Returns:
        str: The name of the organizational unit if found, otherwise "unknown".
    """
    try:
        return ous.get(ou_id)["name"]
    except KeyError:
        current_run.log_warning(f"Not Found: {ou_id}")
        return "unknown"


def save_metadata(filename: str, dhis2_name: str, data: dict):
    """Save metadata to a JSON file.

    Args:
        filename (str): The name of the file to save the metadata.
        dhis2_name (str): The name of the DHIS2 connection.
        data (dict): The metadata to be saved.
    """
    if not Path(f"{workspace.files_path}/{dhis2_name}/metadata").exists():
        Path(f"{workspace.files_path}/{dhis2_name}/metadata").mkdir(parents=True, exist_ok=True)
    file_path = Path(f"{workspace.files_path}/{dhis2_name}/metadata/{filename}.json")
    with file_path.open("w", encoding="utf-8") as f:
        f.write(json.dumps(data))


def get_orgunits_with_parents(ous: list, dhis2_name: str) -> dict:
    """Processes a list of organizational units (OUs) and adds parent information to each OU.

    Args:
        ous (list or dict): A list or dictionary of organizational units. Each OU should contain at
        least an "id" and "level".
        dhis2_name (str): The name of the DHIS2 connection.

    Returns:
        dict: A dictionary of organizational units with added parent information. Each OU will have
        additional keys for parent names at each level.
    The function performs the following steps:
    1. Converts the input list of OUs to a dictionary if necessary.
    2. Iterates through each OU and adds parent information based on the OU's level.
    3. For each level above 1, it adds the parent name corresponding to that level.
    4. Saves the updated metadata.
    5. Returns the updated dictionary of OUs.
    """
    ous_dict = ous_to_dict(ous)
    for ou in ous_dict.values():
        ou_path = path_to_parents_ids(ou)
        lvl = ou_path.get("level")
        if lvl > 1:
            for upper_lvl in range(1, lvl):
                parent_id = ou_path.get(f"level_{upper_lvl}_id")
                ou_path[f"level_{upper_lvl}_name"] = find_name(ous_dict, parent_id)
    save_metadata("orgunits", dhis2_name, ous_dict)
    return ous_dict


def add_parents(df: pd.DataFrame, parents: dict) -> pd.DataFrame:
    """Add parent information to the DataFrame.

    This function merges parent information from a dictionary into a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with added parent information.
    """
    filtered_parents = {key: parents[key] for key in df["ou"] if key in parents}
    # Transform the `parents` dictionary into a DataFrame
    parents_df = pd.DataFrame.from_dict(filtered_parents, orient="index").reset_index()

    # Rename the index column to match the "ou" column
    parents_df = parents_df.rename(
        columns={"index": "ou"},
    )

    # Join the DataFrame with the parents DataFrame on the "ou" column
    return df.merge(parents_df, on="ou", how="left")


if __name__ == "__main__":
    dhis2_extract_dataset()
