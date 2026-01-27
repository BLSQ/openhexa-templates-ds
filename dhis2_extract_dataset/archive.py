import re
from pathlib import Path

import pandas as pd
from openhexa.sdk import workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string

# --------------------------------------------------------------------------------------------
#  ----------------------------FUNCTIONS NOT USED ANYMORE -----------------------------------
# --------------------------------------------------------------------------------------------


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


def get_ous(dhis: DHIS2) -> list[dict]:
    """Retrieves the organisation units from the DHIS instance.

    Args:
        dhis: The DHIS instance.

    Returns:
        A list of organisation units.
    """
    return dhis.meta.organisation_units()


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


def get_datasets_as_dict(dhis: DHIS2) -> dict[str, dict]:
    """Get datasets metadata.

    Args:
    ----
    dhis (DHIS2): The DHIS2 connection object.

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
