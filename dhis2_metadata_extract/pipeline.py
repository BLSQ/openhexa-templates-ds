from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid1

import polars as pl
import urllib3
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox import lineage
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    get_category_option_combos,
    get_data_element_groups,
    get_data_elements,
    get_datasets,
    get_organisation_unit_groups,
    get_organisation_units,
)
from openhexa.toolbox.lineage import EventType
from openlineage.client.event_v2 import Dataset

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@pipeline("dhis2_metadata_extract")
@parameter(
    "dhis2_connection",
    name="DHIS2 instance",
    type=DHIS2Connection,
    help="Credentials for the DHIS2 instance connection",
    required=True,
)
@parameter(
    "get_org_units",
    name="Organisation units",
    type=bool,
    help="Retrieve organisation units data",
    required=False,
    default=False,
)
@parameter(
    "get_org_unit_groups",
    name="Organisation unit groups",
    type=bool,
    help="Retrieve organisation unit groups data",
    required=False,
    default=False,
)
@parameter(
    "get_datasets",
    name="Datasets",
    type=bool,
    help="Retrieve available datasets",
    required=False,
    default=False,
)
@parameter(
    "get_data_elements",
    name="Data elements",
    type=bool,
    help="Retrieve data elements data",
    required=False,
    default=False,
)
@parameter(
    "get_data_element_groups",
    name="Data elements groups",
    type=bool,
    help="Retrieve data element groups data",
    required=False,
    default=False,
)
@parameter(
    "get_category_options",
    name="Category option combos",
    type=bool,
    help="Retrieve category option combos data",
    required=False,
    default=False,
)
@parameter(
    "output_path",
    name="Output path",
    type=str,
    help="Output path for the shapes",
    required=False,
    default=None,
)
def dhis2_metadata_extract(
    dhis2_connection: DHIS2,
    get_org_units: bool,
    get_org_unit_groups: bool,
    get_datasets: bool,
    get_data_elements: bool,
    get_data_element_groups: bool,
    get_category_options: bool,
    output_path: str,
):
    """DHIS2 Metadata Extract Pipeline.

    This pipeline retrieves metadata from a DHIS2 instance and saves it to CSV files.

    Parameters
    ----------
    dhis2_connection : DHIS2Connection
        The connection to the DHIS2 instance.
    get_org_units : bool
        Whether to retrieve organisation units data.
    get_org_unit_groups : bool
        Whether to retrieve organisation unit groups data.
    get_datasets : bool
        Whether to retrieve available datasets.
    get_data_elements : bool
        Whether to retrieve data elements data.
    get_data_element_groups : bool
        Whether to retrieve data element groups data.
    get_category_options : bool
        Whether to retrieve category option combos data.
    output_path : str
        The output path for the shapes.
    """
    local = True

    initialize_lineage(local)

    if output_path is None:
        output_path = (
            Path(workspace.files_path)
            / "pipelines"
            / "dhis2_metadata_extract"
            / f"data_{datetime.now().strftime('%Y_%m_%d_%H%M')}"
        )
        current_run.log_info(f"Output path not specified, using default {output_path}")
    else:
        output_path = Path(output_path)

    try:
        dhis2_client = get_dhis2_client(dhis2_connection)
        dhis2_reference = initialize_dhis2_reference("dhis2_connection", "DHIS2Connection")
        retrieve_org_units(dhis2_client, output_path, dhis2_reference, get_org_units)
        retrieve_org_unit_groups(dhis2_client, output_path, dhis2_reference, get_org_unit_groups)
        retrieve_datasets(dhis2_client, output_path, dhis2_reference, get_datasets)
        retrieve_data_elements(dhis2_client, output_path, dhis2_reference, get_data_elements)
        retrieve_data_element_groups(
            dhis2_client, output_path, dhis2_reference, get_data_element_groups
        )
        retrieve_category_option_combos(
            dhis2_client, output_path, dhis2_reference, get_category_options
        )
        current_run.log_info("Pipeline finished.")
    except Exception as e:
        current_run.log_error(f"An error ocurred during the execution: {e}")
        raise


def initialize_dhis2_reference(connection_name: str, connection_type: str) -> Dataset:
    """Initialize a reference from a name and a type.

    Parameters
    ----------
    connection_name : str
        The name of the connection.
    connection_type : str
        The type of the connection, e.g., "DHIS2Connection".

    Returns
    -------
    Dataset:
        A dataset reference initialized from the connection name and type.
    """
    return lineage._client.dataset_from_connection(
        connection_name=connection_name, connection_type=connection_type
    )


def initialize_lineage(local: bool) -> None:
    """Initialize the lineage tracking for the pipeline."""
    lineage_id = uuid1()
    # This is not a very good way to do it -- I should probably be able to easily find the run
    # by this identifier.
    # The uuid1 incorporates the time, so we could use it to find the run.
    # Neverhteless, this is still not very good -- one would need to go and search for the run
    # at that time and then match it to Marquez.

    if local:
        workspace_slug = "leyreplayground"
    else:
        workspace_slug = workspace.slug

    lineage.init_client(
        url="https://lineage.demo.openhexa.org",
        # This is the demo. It should be changed to the real one.
        # I am not sure that the real one is available yet.
        workspace_slug=workspace_slug,
        pipeline_slug="dhis2_metadata_extract",
        pipeline_run_id=f"{lineage_id}",
    )

    if lineage.is_initialized():
        current_run.log_info(f"OpenLineage client initialized successfully, run id: {lineage_id} .")
    else:
        current_run.log_warning("OpenLineage client initialization failed.")


def get_dhis2_client(dhis2_connection: DHIS2Connection) -> DHIS2:
    """Get the DHIS2 connection.

    Parameters
    ----------
    dhis2_connection : DHIS2Connection
        The connection to the DHIS2 instance.

    Returns
    -------
    DHIS2
        An instance of the DHIS2 client connected to the specified DHIS2 instance.
    """
    try:
        dhis2_client = DHIS2(dhis2_connection, cache_dir=Path(workspace.files_path) / ".cache")
        current_run.log_info(f"Successfully connected to DHIS2 instance {dhis2_connection.url}")

        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to {dhis2_connection.url} error: {e}") from e


# @dhis2_metadata_extract.task
def retrieve_org_units(
    dhis2_client: DHIS2,
    output_path: Path,
    dhis2_reference: Dataset,
    run: bool = True,
) -> None:
    """Retrieve and save organisation units at the maximum level from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    dhis2_reference : Dataset
        The dataset reference for the DHIS2 connection.
    run : bool, optional
        Whether to execute the retrieval (default is True).
    """
    if run:
        current_run.log_info("Retrieving organisation units")
        file_reference = lineage._client.dataset_from_file("org_units.csv")
        # I am using only "org_units.csv" and not
        # "org_units_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv" for the file reference
        # I am not completly sure this is the way to go.
        # When we see the lineage, we will see the mapping to each of the files.
        # So, after each run, we will have a new mapping (since the naming is new),
        # instead of updating the already existing mapping.
        # On the other hand, we are not using the full name for the file reference,
        # which might end up being confusing.
        lineage.event(
            event_type=EventType.START,
            task_name="get_org_units",
            inputs=[dhis2_reference],
            outputs=[file_reference],
            start_time=datetime.now(timezone.utc),
        )
        try:
            org_units = get_organisation_units(dhis2_client)
            max_level = org_units.select(pl.col("level").max()).item()
            org_units = org_units.filter(pl.col("level") == max_level).drop(
                ["id", "name", "level", "opening_date", "closed_date", "geometry"]
            )
            filename = f"org_units_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=org_units, output_path=output_path, filename=filename)
            lineage.event(
                event_type=EventType.COMPLETE,
                task_name="get_org_units",
                # We could get the name of the function we are runnning,
                # but again I think this is overcomplicating things
                # and we can just hard-code it.
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
        except Exception as e:
            lineage.event(
                event_type=EventType.FAIL,
                task_name="get_org_units",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
            # We could try and put these lineage events in a wrapping.
            # I am not sure that it would not make things more confusing.
            raise Exception(f"Error while retrieving organisation units: {e}") from e
        current_run.log_info(f"Organisation units saved under: {output_path / filename}")


def retrieve_org_unit_groups(
    dhis2_client: DHIS2,
    output_path: Path,
    dhis2_reference: Dataset,
    run: bool = True,
) -> None:
    """Retrieve and save organisation unit groups from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    dhis2_reference : Dataset
        The dataset reference for the DHIS2 connection.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving organisation unit groups.
    """
    if run:
        current_run.log_info("Retrieving organisation unit groups")
        file_reference = lineage._client.dataset_from_file("org_units_groups.csv")
        try:
            lineage.event(
                event_type=EventType.START,
                task_name="get_organisation_unit_groups",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
            org_unit_groups = get_organisation_unit_groups(dhis2_client)
            org_unit_groups = org_unit_groups.with_columns(
                [pl.col("organisation_units").list.join(",").alias("organisation_units")]
            )
            filename = f"org_units_groups_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=org_unit_groups, output_path=output_path, filename=filename)
            lineage.event(
                event_type=EventType.COMPLETE,
                task_name="get_organisation_unit_groups",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
        except Exception as e:
            lineage.event(
                event_type=EventType.FAIL,
                task_name="get_organisation_unit_groups",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
            raise Exception(f"Error while retrieving organisation unit groups: {e}") from e
        current_run.log_info(f"Organisation units saved under: {output_path / filename}")


# The lineage is not working properly.
# When adding a new job, all of the previous datasets get matched to it.
# I am not sure why this is happening, I dont know if it is me or a bug in the library.
# Also, the run history is not saved -- or at least I cannot see the latests run in the Marquez UI.


def retrieve_datasets(
    dhis2_client: DHIS2,
    output_path: Path,
    dhis2_reference: Dataset,
    run: bool = True,
) -> None:
    """Retrieve and save datasets from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    dhis2_reference : Dataset
        The dataset reference for the DHIS2 connection.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving datasets.
    """
    if run:
        current_run.log_info("Retrieving datasets")
        file_reference = lineage._client.dataset_from_file("datasets.csv")
        lineage.event(
            event_type=EventType.START,
            task_name="retrieve_datasets",
            inputs=[dhis2_reference],
            outputs=[file_reference],
            start_time=datetime.now(timezone.utc),
        )
        try:
            datasets = get_datasets(dhis2_client)
            datasets = datasets.with_columns(
                [
                    pl.col("organisation_units").list.join(",").alias("organisation_units"),
                    pl.col("data_elements").list.join(",").alias("data_elements"),
                    pl.col("indicators").list.join(",").alias("indicators"),
                ]
            )
            filename = f"datasets_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=datasets, output_path=output_path, filename=filename)
            lineage.event(
                event_type=EventType.COMPLETE,
                task_name="retrieve_datasets",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
        except Exception as e:
            lineage.event(
                event_type=EventType.FAIL,
                task_name="retrieve_datasets",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
            raise Exception(f"Error while retrieving datasets: {e}") from e
        current_run.log_info(f"Datasets saved under: {output_path / filename}")


def retrieve_data_elements(
    dhis2_client: DHIS2, output_path: Path, dhis2_reference: Dataset, run: bool = True
) -> None:
    """Retrieve and save data elements from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    dhis2_reference : Dataset
        The dataset reference for the DHIS2 connection.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving data elements.
    """
    if run:
        current_run.log_info("Retrieving data elements")
        file_reference = lineage._client.dataset_from_file("data_elements.csv")
        lineage.event(
            event_type=EventType.START,
            task_name="retrieve_data_elements",
            inputs=[dhis2_reference],
            outputs=[file_reference],
            start_time=datetime.now(timezone.utc),
        )
        try:
            data_elements = get_data_elements(dhis2_client)
            filename = f"data_elements_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=data_elements, output_path=output_path, filename=filename)
            lineage.event(
                event_type=EventType.COMPLETE,
                task_name="retrieve_data_elements",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
        except Exception as e:
            lineage.event(
                event_type=EventType.FAIL,
                task_name="retrieve_data_elements",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
            raise Exception(f"Error while retrieving data elements: {e}") from e
        current_run.log_info(f"Data elements saved under: {output_path / filename}")


def retrieve_data_element_groups(
    dhis2_client: DHIS2, output_path: Path, dhis2_reference: Dataset, run: bool = True
) -> None:
    """Retrieve and save data element groups from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    dhis2_reference : Dataset
        The dataset reference for the DHIS2 connection.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving data elements.
    """
    if run:
        current_run.log_info("Retrieving data element groups")
        file_reference = lineage._client.dataset_from_file("data_element_groups.csv")
        lineage.event(
            event_type=EventType.START,
            task_name="retrieve_data_element_groups",
            inputs=[dhis2_reference],
            outputs=[file_reference],
            start_time=datetime.now(timezone.utc),
        )
        try:
            data_element_groups = get_data_element_groups(dhis2_client)
            data_element_groups = data_element_groups.with_columns(
                [pl.col("data_elements").list.join(",").alias("data_elements")]
            )
            filename = f"data_element_groups_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=data_element_groups, output_path=output_path, filename=filename)
            lineage.event(
                event_type=EventType.COMPLETE,
                task_name="retrieve_data_element_groups",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
        except Exception as e:
            lineage.event(
                event_type=EventType.FAIL,
                task_name="retrieve_data_element_groups",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
            raise Exception(f"Error while retrieving data element groups: {e}") from e
        current_run.log_info(f"Data element groups saved under: {output_path / filename}")


def retrieve_category_option_combos(
    dhis2_client: DHIS2, output_path: Path, dhis2_reference: Dataset, run: bool = True
) -> None:
    """Retrieve and save category option combos from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    dhis2_reference : Dataset
        The dataset reference for the DHIS2 connection.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving category option combos.
    """
    if run:
        current_run.log_info("Retrieving category option combos")
        file_reference = lineage._client.dataset_from_file("category_option_combos.csv")
        lineage.event(
            event_type=EventType.START,
            task_name="retrieve_category_option_combos",
            inputs=[dhis2_reference],
            outputs=[file_reference],
            start_time=datetime.now(timezone.utc),
        )
        try:
            categorty_options = get_category_option_combos(dhis2_client)
            filename = f"category_option_combos_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=categorty_options, output_path=output_path, filename=filename)
            lineage.event(
                event_type=EventType.COMPLETE,
                task_name="retrieve_category_option_combos",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
        except Exception as e:
            lineage.event(
                event_type=EventType.FAIL,
                task_name="retrieve_category_option_combos",
                inputs=[dhis2_reference],
                outputs=[file_reference],
                start_time=datetime.now(timezone.utc),
            )
            raise Exception(f"Error while retrieving categorty option combos: {e}") from e
        current_run.log_info(f"Category option combos saved under: {output_path / filename}")


def save_file(df: pl.DataFrame, output_path: Path, filename: str) -> None:
    """Save a GeoDataFrame to a file in the specified output path.

    Parameters
    ----------
    df : pl.DataFrame
        The pl.DataFrame containing the data to save.
    output_path : Path
        The directory where the file will be saved.
    filename : str
        The name of the file to save the shapes in.

    Raises
    ------
    PermissionError
        If there is no permission to access the file.
    OSError
        If an I/O error occurs during saving.
    Exception
        For any other unexpected errors.
    """
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(f"Error creating output directory {output_path}: {e}") from e

    try:
        output_fname = output_path / filename
        df.write_csv(output_fname)
    except PermissionError as e:
        raise PermissionError("Error: You don't have permission to access this file.") from e
    except OSError as e:
        raise OSError(f"An I/O error occurred: {e}") from e
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}") from e


if __name__ == "__main__":
    dhis2_metadata_extract()
