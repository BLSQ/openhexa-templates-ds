"""Template for newly generated pipelines."""

import os
from datetime import date
from typing import List
from urllib.parse import urlparse

import pandas as pd
from helper import (
    # add_parents,
    # add_to_dataset,
    datasets_temp,
    get_dataelements_with_no_data,
    get_levels,
    # get_orgunits_with_parents,
    get_periods_with_no_data,
    is_iso_date,
    isodate_to_period_type,
    select_data_elements,
)
from openhexa.sdk import (
    Dataset,
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("dhis2_extract_dataset")
@parameter(
    "dhis_con",
    name="DHIS2 Connection",
    type=DHIS2Connection,
    default="dhis2-demo-2-41",
    required=True,
)
@parameter(
    "data_element_ids",
    type=str,
    multiple=True,
    required=False,
    default=["FvKdfA2SuWI", "p1MDHOT6ENy"],
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
@parameter("openhexa_dataset", name="Openhexa Dataset", type=Dataset, required=False, default=None)
@parameter(
    "save_by_month",
    name="Store datasets values by period in the folder spaces",
    type=bool,
    required=True,
    default=True,
)
@parameter("datasets_ids", type=str, multiple=True, default=["TuL8IOPzpHh"], required=True)
@parameter("add_dx_name", type=bool, required=False, default=False)
@parameter("add_coc_name", type=bool, required=False, default=False)
@parameter("add_org_unit_parent", type=bool, required=False, default=False)
def dhis2_extract_dataset(
    dhis_con,
    datasets_ids,
    data_element_ids,
    start,
    end,
    save_by_month,
    openhexa_dataset,
    add_dx_name,
    add_org_unit_parent,
    add_coc_name,
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    dhis = get_dhis(dhis_con)
    start = valid_date(start)
    end = valid_date(end)
    dhis2_name = get_dhis2_name_domain(dhis_con)
    ds = get_datasets(dhis, dhis2_name)
    ous = get_ous(dhis)
    data_element_ids = warning_request(datasets_ids, ds, data_element_ids, ous)
    dhis2_name = create_extraction_folder(dhis2_name, save_by_month, ds, datasets_ids)
    table = extract_raw_data(
        dhis, dhis2_name, save_by_month, datasets_ids, ds, start, end, data_element_ids
    )
    table = enrich_data(
        dhis, dhis2_name, ous, table, add_dx_name, add_org_unit_parent, add_coc_name
    )
    warning_post_extraction(table, ds, datasets_ids, start, end)
    save_table(table, dhis2_name)


@dhis2_extract_dataset.task
def get_dhis2_name_domain(dhis_con) -> str:
    subdomain = urlparse(dhis_con.url).netloc.split(".")[0]
    return f"{subdomain.replace('-', '_')}"


@dhis2_extract_dataset.task
def create_extraction_folder(dhis2_name, save_by_month, ds, ids):
    os.makedirs(f"{workspace.files_path}/{dhis2_name}", exist_ok=True)
    if save_by_month:
        for id in ids:
            name = ds[id]["name"]
            os.makedirs(f"{workspace.files_path}/{dhis2_name}/{name}", exist_ok=True)
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
def save_table(
    table: pd.DataFrame,
    dhis2_name: str,
):
    """Saves the given table to DHIS2 and optionally to the OH database.

    Args:
        table (pd.DataFrame): The table to be saved.
        dhis_con (DHIS2Connection): The DHIS2 connection object.
        openhexa_dataset (Dataset): The OpenHexa dataset object.
        save_in_db (bool): Whether to save the table in the OH database.
    """
    table.to_csv(f"{workspace.files_path}/{dhis2_name}/dataset_extraction.csv", index=False)


@dhis2_extract_dataset.task
def warning_post_extraction(table, datasets, ids, start, end):
    """Check for warnings in the extracted data.

    Args:
        table (pd.DataFrame): The extracted data.
        datasets (dict): Dictionary containing dataset information.
        start (str): Start date of the extraction.
        end (str): End date of the extraction.

    Returns:
        None
    """
    if len(table) == 0:
        current_run.log_warning("No data extracted")
    else:
        periods = [str(p) for p in table["pe"].unique()]
        for id in ids:
            get_periods_with_no_data(periods, start, end, datasets[id])
            get_dataelements_with_no_data(table["dx"].unique(), datasets[id])


@dhis2_extract_dataset.task
def warning_request(ids, datasets, data_element_ids, ous):
    """
    Check for warnings in the datasets.

    Args:
        ids (list): List of dataset IDs.
        datasets (dict): Dictionary containing dataset information.
        data_element_ids (list): List of data element IDs.
        ous (list): List of organisation unit IDs.

    Returns:
        set or None: If `data_element_ids` is a non-empty list, returns a set of all data elements
        associated with the datasets that are not in `data_element_ids`. Otherwise, returns None.
    """
    levels = {level for id in ids for level in get_levels(ous, datasets[id]["organisation_units"])}
    frequencies = {datasets[id]["periodType"] for id in ids}
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
        all_data_elements = {dx for id in ids for dx in datasets[id]["data_elements"]}
        unmatched_data_elements = set(data_element_ids) - all_data_elements
        if len(unmatched_data_elements) > 0:
            current_run.log_error(
                f"Data elements {unmatched_data_elements} are not associated to any dataset"
            )
        return all_data_elements - set(data_element_ids)
    else:
        return None


@dhis2_extract_dataset.task
def get_datasets(dhis: DHIS2, dhis2_name: str) -> dict:
    try:
        ds = dhis.meta.datasets()
        assert isinstance(ds, dict)
    except Exception:
        current_run.log_warning(
            "dataset function in toolbox is still returning a list and not a dict!"
        )
        ds = datasets_temp(dhis, dhis2_name)
    return ds


@dhis2_extract_dataset.task
def get_ous(dhis: DHIS2) -> List[dict]:
    """
    Retrieves the organisation units from the DHIS instance.

    Args:
        dhis: The DHIS instance.

    Returns:
        A list of organisation units.
    """
    return dhis.meta.organisation_units()


@dhis2_extract_dataset.task
def valid_date(date_str: str) -> str:
    """
    Validates a date string and returns it if valid, otherwise logs an error.

    Args:
        date_str (str): The date string to validate.

    Returns:
        str: The validated date string.

    """
    if date_str is None:
        return date.today().isoformat()
    elif is_iso_date(date_str):
        return date_str
    else:
        current_run.log_error(f"Invalid date format: {date_str}")


@dhis2_extract_dataset.task
def extract_raw_data(
    dhis, dhis2_name, save_by_month, datasets_ids, datasets, start, end, data_element_ids=None
):
    """
    Extracts raw data from DHIS2 for the given datasets and time range.

    Parameters:
    - dhis: DHIS2 client object used to interact with the DHIS2 API.
    - datasets_ids: List of dataset IDs to extract data from.
    - datasets: Dictionary containing dataset information, including data elements and organisation units.
    - start: Start date of the time range to extract data from.
    - end: End date of the time range to extract data from.
    - data_element_ids: Optional list of data element IDs to filter the extracted data. If None, all data elements will be included.

    Returns:
    - res: Pandas DataFrame containing the extracted raw data, with additional columns for dataset name and period type.
    """
    res = pd.DataFrame()
    for id in datasets_ids:
        selected_data_elements = select_data_elements(
            data_element_ids, datasets[id]["data_elements"]
        )
        periodType = datasets[id]["periodType"]
        current_run.log_info(f"Extracting data for dataset {datasets[id]['name']}")
        if save_by_month:
            start = isodate_to_period_type(start, periodType)
            end = isodate_to_period_type(end, periodType)
            for pe in start.get_range(end):
                current_run.log_info(f"Extracting data for period {pe}")
                # pe = str(pe)
                if os.path.exists(
                    f"{workspace.files_path}/{dhis2_name}/{datasets[id]['name']}/{pe}.csv"
                ):
                    df = pd.read_csv(
                        f"{workspace.files_path}/{dhis2_name}/{datasets[id]['name']}/{pe}.csv"
                    )
                else:
                    data_values = dhis.data_value_sets.get(
                        datasets=[id],
                        data_elements=selected_data_elements,
                        org_units=datasets[id]["organisation_units"],
                        periods=[pe],
                    )
                    df = pd.DataFrame(data_values)
                    df["dataset"] = datasets[id]["name"]
                    df["periodType"] = datasets[id]["periodType"]
                    df.to_csv(
                        f"{workspace.files_path}/{dhis2_name}/{datasets[id]['name']}/{pe}.csv",
                        index=False,
                    )
                    current_run.log_info(f"Data for period {pe} saved: {df.shape[0]} rows")
                res = pd.concat([res, df])
        else:
            data_values = dhis.data_value_sets.get(
                datasets=[id],
                data_elements=selected_data_elements,
                org_units=datasets[id]["organisation_units"],
                start_date=start,
                end_date=end,
            )
            df = pd.DataFrame(data_values)
            df["dataset"] = datasets[id]["name"]
            df["periodType"] = datasets[id]["periodType"]
            res = pd.concat([res, df])
    return res


@dhis2_extract_dataset.task
def enrich_data(
    dhis: DHIS2,
    dhis2_name: str,
    ous: List[dict],
    table: pd.DataFrame,
    add_dx_name: bool,
    add_org_unit_parent: bool,
    add_coc_name: bool,
) -> pd.DataFrame:
    """
    Enriches the given table with additional columns based on the specified parameters.

    Args:
        dhis (DHIS2): The DHIS2 object used for metadata retrieval.
        table (pd.DataFrame): The table to be enriched.
        add_dx_name (bool): Whether to add the dx_name column.
        add_org_unit_parent (bool): Whether to add the org_unit_parent column.
        add_coc_name (bool): Whether to add the coc_name column.

    Returns:
        pd.DataFrame: The enriched table.
    """
    table.rename(columns={"dataElement": "dx", "orgUnit": "ou", "period": "pe"}, inplace=True)
    print(table.columns)
    length_table = len(table)
    current_run.log_info("Length of the table is : " + str(length_table))
    if length_table > 0:
        if add_dx_name:
            table = dhis.meta.add_dx_name_column(table)
        if add_coc_name:
            table = dhis.meta.add_coc_name_column(table, "categoryOptionCombo")
        if add_org_unit_parent:
            # table = dhis.add_org_unit_parent_columns(table)
            print(table.sample(2))
            table = dhis.meta.add_org_unit_parent_columns(table)
            # table = add_parents(table, ous)

    return table


if __name__ == "__main__":
    dhis2_extract_dataset()
