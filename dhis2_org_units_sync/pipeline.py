# import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from openhexa.sdk import (
    DHIS2Connection,
    DHIS2Widget,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.client import openhexa
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    get_organisation_unit_levels,
)
from utils import (
    OrgUnitObj,
    build_id_indexes,
    connect_to_dhis2,
    read_parquet_as_polars,
    retrieve_pyramid_to_level,
)

# from shapely.geometry import shape
# from shapely.geometry.base import BaseGeometry


@pipeline("dhis2_shapes_extract")
@parameter(
    "dhis2_source",
    name="DHIS2 source",
    type=DHIS2Connection,
    help="Credentials for the source DHIS2",
    required=False, # True
)
@parameter(
    "dhis2_target",
    name="DHIS2 target",
    type=DHIS2Connection,
    help="Credentials for the target DHIS2",
    required=False, # True
)
@parameter(
    "org_unit_level",
    type=str,
    multiple=False,
    name="Organisation unit level",
    help="Organisation unit level to sync the pyramid",
    required=False,
    default=None,
    widget=DHIS2Widget.ORG_UNIT_LEVELS,
    connection="dhis2_connection",
)
@parameter(
    "dry_run",
    name="Dry run import",
    type=bool,
    help="Simulates the process without modifying the target DHIS2",
    default=False,
    required=True,
)
def dhis2_org_units_sync(
    dhis2_source: DHIS2Connection,
    dhis2_target: DHIS2Connection,
    org_unit_level: list[str] | None,
    dry_run: bool,
):
    """Write your pipeline orchestration here."""
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_org_units_sync"
 
    try:
        configure_login(logs_path=pipeline_path / "logs", task_name="organisation_units_sync")

        sync_organisation_units(
            pipeline_path=pipeline_path,
            source_connection=workspace.get_connection("dhis2-demo-2-39"),#dhis2_source,
            target_connection=workspace.get_connection("dhis2-demo-2-41"),#dhis2_target,
            org_unit_level=org_unit_level,
            dry_run=dry_run,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


# @dhis2_org_units_sync.task
def sync_organisation_units(
    pipeline_path: Path,
    source_connection: DHIS2Connection,
    target_connection: DHIS2Connection,
    org_unit_level: str,
    dry_run: bool,
):
    """Put some data processing code here."""

    dhis2_client_source = connect_to_dhis2(
        connection=source_connection, cache_dir=pipeline_path / "data" / "cache"
    )
    dhis2_client_target = connect_to_dhis2(
        connection=target_connection, cache_dir=pipeline_path / "data" / "cache"
    )

    if org_unit_level is None:
        current_run.log_warning("Organisation unit level not specified, using base level 2")
        org_level = 2
    else:
        org_levels = get_organisation_unit_levels(dhis2_client_source)
        org_level = org_levels.filter(pl.col("id") == org_unit_level)["level"][0]

    extract_pyramid(
        dhis2_client=dhis2_client_source,
        org_level=org_level,
        output_dir=pipeline_path / "data" / "pyramid",
    )

    sync_pyramid_with(
        dhis2_client=dhis2_client_target,
        pipeline_path=pipeline_path,
        org_level=org_level,
        dry_run=dry_run,
    )


def extract_pyramid(dhis2_client: DHIS2, org_level: int, output_dir: Path) -> None:
    """Extracts all DHIS2 Org units up to the org_level and saves it as a Parquet file."""
    current_run.log_info("Retrieving SNIS DHIS2 pyramid data")

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        org_units = retrieve_pyramid_to_level(dhis2_client, org_level)
        pyramid_fname = output_dir / "pyramid_data.parquet"  # to be replaced

        # Save as Parquet
        org_units.write_parquet(pyramid_fname, use_pyarrow=True)
        current_run.log_info(f"Source DHIS2 pyramid saved: {pyramid_fname}")

    except Exception as e:
        raise Exception(f"Error while extracting source DHIS2 Pyramid: {e}") from e


def sync_pyramid_with(
    dhis2_client: DHIS2, pipeline_path: Path, org_level: int, dry_run: bool
) -> None:
    """Synchronize the organisation unit pyramid between the source and target DHIS2 instances.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client for the target instance.
    pipeline_path : Path
        The path to the pipeline's working directory.
    org_level : int
        The organisation unit level to synchronize.
    dry_run : bool
        If True, simulates the process without modifying the target DHIS2.
    """
    current_run.log_info("Starting organisation units sync.")

    # Load pyramid extract
    org_units_source = read_parquet_as_polars(
        pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"
    )

    if org_units_source.shape[0] > 0:
        # Retrieve the target org units to compare
        current_run.log_info(
            f"Retrieving organisation units from target DHIS2 instance {dhis2_client.api.url}"
        )
        current_run.log_info(f"Run org units sync with dry run: {dry_run}")
        org_units_target = retrieve_pyramid_to_level(dhis2_client, org_level)

        # Get list of ids for creation and update
        ou_new = list(set(org_units_source.id) - set(org_units_target.id))
        ou_matching = list(set(org_units_source.id).intersection(set(org_units_target.id)))
        target_dhis2_version = dhis2_client.meta.system_info().get("version")

        # Create orgUnits
        try:
            if len(ou_new) > 0:
                current_run.log_info(f"Creating {len(ou_new)} organisation units.")
                ou_to_create = org_units_source[org_units_source.id.isin(ou_new)]
                remove_geometry_if_needed([ou_to_create], target_dhis2_version)
                push_orgunits_create(
                    ou_df=ou_to_create,
                    dhis2_client_target=dhis2_client,
                    dry_run=dry_run,
                    report_path=pipeline_path / "logs",
                )
        except Exception as e:
            raise Exception(
                f"Unexpected error occurred while creating organisation units. Error: {e}"
            ) from e

        # Update orgUnits
        try:
            if len(ou_matching) > 0:
                current_run.log_info(
                    f"Checking for updates in {len(ou_matching)} organisation units"
                )
                remove_geometry_if_needed(
                    [org_units_source, org_units_target], target_dhis2_version
                )
                push_orgunits_update(
                    orgUnit_source=org_units_source,
                    orgUnit_target=org_units_target,
                    matching_ou_ids=ou_matching,
                    dhis2_client_target=dhis2_client,
                    dry_run=dry_run,
                    report_path=pipeline_path / "logs",
                )
                current_run.log_info("Organisation units push finished.")
        except Exception as e:
            raise Exception(
                f"Unexpected error occurred while updating organisation units. Error: {e}"
            ) from e

    else:
        current_run.log_warning(
            "No data found in the pyramid file. Organisation units task skipped."
        )


def push_orgunits_create(
    ou_df: pd.DataFrame, dhis2_client_target: DHIS2, dry_run: bool, report_path: str
):
    """Create organisation units in the target DHIS2 instance from the provided DataFrame.

    Parameters
    ----------
    ou_df : pd.DataFrame
        DataFrame containing organisation units to create.
    dhis2_client_target : DHIS2
        DHIS2 client for the target instance.
    dry_run : bool
        If True, simulates the process without modifying the target DHIS2.
    report_path : str
        Path to the directory where reports/logs are stored.
    """
    errors_count = 0
    for _, row in ou_df.iterrows():
        ou = OrgUnitObj(row)
        if ou.is_valid():
            response = push_orgunit(
                dhis2_client=dhis2_client_target,
                orgunit=ou,
                strategy="CREATE",
                dry_run=dry_run,  # dry_run=False -> Apply changes in the DHIS2
            )
            if response["status"] == "ERROR":
                errors_count = errors_count + 1
                logging.info(str(response))
            else:
                current_run.log_info(f"New organisation unit created: {ou}")
        else:
            logging.info(
                str(
                    {
                        "action": "CREATE",
                        "statusCode": None,
                        "status": "NOTVALID",
                        "response": None,
                        "ou_id": row.get("id"),
                    }
                )
            )

    if errors_count > 0:
        current_run.log_info(
            f"{errors_count} errors occurred during creation. Please check the latest execution report under {report_path}."
        )


def push_orgunits_update(
    orgUnit_source: pd.DataFrame,
    orgUnit_target: pd.DataFrame,
    matching_ou_ids: list,
    dhis2_client_target: DHIS2,
    dry_run: bool,
    report_path: str,
):
    """Update org units based matching id list"""
    # Use these columns to compare (check for Updates)
    comparison_cols = [
        "name",
        "shortName",
        "openingDate",
        "closedDate",
        "parent",
        "geometry",
    ]

    # build id dictionary (faster) and compare on selected columns
    index_dictionary = build_id_indexes(orgUnit_source, orgUnit_target, matching_ou_ids)
    orgUnit_source_f = orgUnit_source[comparison_cols]
    orgUnit_target_f = orgUnit_target[comparison_cols]

    errors_count = 0
    updates_count = 0
    progress_count = 0
    for id, indices in index_dictionary.items():
        progress_count = progress_count + 1
        source = orgUnit_source_f.iloc[indices["source"]]
        target = orgUnit_target_f.iloc[indices["target"]]
        # get cols with differences
        diff_fields = source[~((source == target) | (source.isna() & target.isna()))]

        # If there are differences update!
        if not diff_fields.empty:
            # add the ID for update
            source["id"] = id
            ou_update = OrgUnitObj(source)
            response = push_orgunit(
                dhis2_client=dhis2_client_target,
                orgunit=ou_update,
                strategy="UPDATE",
                dry_run=dry_run,  # dry_run=False -> Apply changes in the DHIS2
            )
            if response["status"] == "ERROR":
                errors_count = errors_count + 1
            else:
                updates_count = updates_count + 1
            logging.info(str(response))

        if progress_count % 5000 == 0:
            current_run.log_info(
                f"Organisation units checked: {progress_count}/{len(matching_ou_ids)}"
            )

    current_run.log_info(f"Organisation units updated: {updates_count}")
    if errors_count > 0:
        current_run.log_info(
            f"{errors_count} errors occurred during OU update. Please check the latest execution report under {report_path}."
        )


def push_orgunit(
    dhis2_client: DHIS2, orgunit: OrgUnitObj, strategy: str = "CREATE", dry_run: bool = True
):
    if strategy == "CREATE":
        endpoint = "organisationUnits"
        payload = orgunit.to_json()

    if strategy == "UPDATE":
        endpoint = "metadata"
        payload = {"organisationUnits": [orgunit.to_json()]}

    r = dhis2_client.api.session.post(
        f"{dhis2_client.api.url}/{endpoint}",
        json=payload,
        params={"dryRun": dry_run, "importStrategy": f"{strategy}"},
    )

    return build_formatted_response(response=r, strategy=strategy, ou_id=orgunit.id)


def build_formatted_response(response: requests.Response, strategy: str, ou_id: str) -> dict:
    """Build a formatted response dictionary from a DHIS2 API response.

    Parameters
    ----------
    response : requests.Response
        The HTTP response object returned by the DHIS2 API.
    strategy : str
        The import strategy used ("CREATE" or "UPDATE").
    ou_id : str
        The organisation unit ID associated with the response.

    Returns
    -------
    dict
        A dictionary containing the action, status code, status, response, and organisation unit ID.
    """
    return {
        "action": strategy,
        "statusCode": response.status_code,
        "status": response.json().get("status"),
        "response": response.json().get("response"),
        "ou_id": ou_id,
    }


def remove_geometry_if_needed(df_list: list[pl.DataFrame], version: str):
    """Remove geometry information from dataframes if DHIS2 version is not compatible.

    Parameters
    ----------
    df_list : list[pl.DataFrame]
        List of polars DataFrames to process.
    version : str
        DHIS2 version string; geometry is only kept for versions > 2.32.
    """
    # NOTE: Geometry is valid for versions > 2.32
    if version <= "2.32":
        for df in df_list:
            df["geometry"] = None
        current_run.log_warning(
            "DHIS2 version not compatible with geometry.Geometry will be ignored."
        )


def configure_login(logs_path: Path, task_name: str):
    """Configure logging for the pipeline task.

    Parameters
    ----------
    logs_path : Path
        The directory where log files will be stored.
    task_name : str
        The name of the task, used in the log filename.
    """
    # Configure logging
    logs_path.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d-%H_%M")
    logging.basicConfig(
        filename=logs_path / f"{task_name}_{now}.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )


def get_woskpace_connection_name(connection_slug: str) -> str:
    """Get the connection name from the workspace."""
    connection_query = """
        query getConnection($workspaceSlug:String!, $connectionSlug: String!) {
            connectionBySlug(workspaceSlug:$workspaceSlug, connectionSlug: $connectionSlug) {
                type
                name
                fields {
                    code
                    value
                }
            }
        }
        """
    result = openhexa.execute(
        query=connection_query,
        variables={"workspaceSlug": workspace.slug, "connectionSlug": connection_slug}
    )

    if result is None:
        raise ValueError(f"No response received for connection slug: '{connection_slug}'")    
    if result.status_code != 200:        
        raise Exception(f"Failed to retrieve data for connection: {result.json()}")
    found_slug  = result.json().get("data", {}).get("connectionBySlug")
    if found_slug is None:
        raise ValueError(f"Connection with slug '{connection_slug}' not found in workspace '{workspace.slug}'")
            
    return found_slug.get("name")


if __name__ == "__main__":
    dhis2_org_units_sync()
