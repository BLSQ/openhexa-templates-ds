"""Template for newly generated pipelines."""

from datetime import datetime
from pathlib import Path

import config
import polars as pl
import urllib3
from dateutil.relativedelta import relativedelta
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.sdk.datasets.dataset import Dataset
from openhexa.toolbox import lineage
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_data_elements, get_organisation_units
from openhexa.toolbox.lineage import EventType
from toolbox import extract_events, get_program_stages, get_programs, join_object_names
from utils import (
    default_output_path,
    filter_objects,
    validate_yyyymmdd,
    write_to_dataset,
    write_to_db,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@pipeline("DHIS event extract")
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to DHIS2",
    required=True,
    default="dhis2-cousp",
)
@parameter(
    "program_id",
    type=str,
    help="ID of the DHIS2 event program",
    required=True,
    default="Fw4tCvSayjE",
)
@parameter(
    code="input_ous",
    type=str,
    multiple=True,
    name="Organisation units",
    help="IDs of organisation units to extract events from",
    required=True,
    default=["rdX5nU5lrcx"],
)
@parameter(
    code="include_children",
    type=bool,
    name="Include children",
    help="Include children organisation units",
    default=True,
)
@parameter(
    code="start_date",
    type=str,
    name="Start date (YYYYMMDD)",
    help="Start date for the extraction",
    required=False,
)
@parameter(
    code="end_date",
    type=str,
    name="End date (YYYYMMDD)",
    help="End date for the extraction (today by default)",
    required=False,
)
@parameter(
    code="period",
    type=int,
    name="Number of months to extract.",
    help="If no start_date is provided, it will be calculated as end_date - period.",
    required=False,
    default=2,
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
def dhis_event_extract(
    dhis_con: DHIS2Connection,
    program_id: str,
    input_ous: list[str],
    start_date: str | None = None,
    period: int | None = None,
    end_date: str | None = None,
    dst_file: str | None = None,
    dst_dataset: Dataset | None = None,
    dst_table: str | None = None,
    include_children: bool = False,
):
    """Pipeline to extract events from DHIS2."""
    initialize_lineage(True)
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=dhis_con, cache_dir=cache_dir)
    check_server_health(dhis2)
    current_run.log_info(f"DHIS2 instance {dhis_con.url} connected.")

    check_dates(start_date, end_date, period)

    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
        current_run.log_info(f"End date not provided, using {end_date}")

    if start_date is None:
        start_date = (
            datetime.strptime(end_date, "%Y%m%d") - relativedelta(months=period)
        ).strftime("%Y%m%d")
        current_run.log_info(f"Start date not provided, using {start_date}")

    current_run.log_info("Reading metadata from source DHIS2 instance")
    dhis2_dataset = lineage._client.dataset_from_connection(
        connection_name="dhis2_connection", connection_type="DHIS2Connection"
    )
    lineage.event(
        EventType.START,
        task_name="get_organisation_units",
        inputs=[dhis2_dataset],
        outputs=["all_ous"],
    )
    try:
        all_ous = get_organisation_units(dhis2)
        lineage.event(
            EventType.COMPLETE,
            task_name="get_organisation_units",
            inputs=[dhis2_dataset],
            outputs=["all_ous"],
        )
    except Exception:
        lineage.event(
            EventType.FAIL,
            task_name="get_organisation_units",
            inputs=[dhis2_dataset],
            outputs=["all_ous"],
        )
        raise
    lineage.event(
        EventType.START,
        task_name="get_data_elements",
        inputs=[dhis2_dataset],
        outputs=["de_metadata"],
    )
    try:
        de_metadata = get_data_elements(dhis2)
        lineage.event(
            EventType.COMPLETE,
            task_name="get_data_elements",
            inputs=[dhis2_dataset],
            outputs=["de_metadata"],
        )
    except Exception:
        lineage.event(
            EventType.FAIL,
            task_name="get_data_elements",
            inputs=[dhis2_dataset],
            outputs=["de_metadata"],
        )
        raise

    program_stages = get_program_stages(dhis2)
    program_metadata = get_programs(dhis2)
    current_run.log_info("Metadata from source DHIS2 instance read.")

    check_program(program_id, program_metadata)

    if input_ous:
        present_input_ous = filter_objects(
            objects_in_request=input_ous,
            objects_in_dhis2=all_ous["id"].to_list(),
            object_type="Organisation unit",
        )

    events = extract_events(
        dhis2=dhis2,
        program_id=program_id,
        occurred_after=start_date,
        occurred_before=end_date,
        org_units=present_input_ous,
        include_children=include_children,
    )
    current_run.log_info(f"Extracted {len(events)} events")

    events_complete = join_object_names(
        df=events,
        data_elements=de_metadata,
        organisation_units=all_ous,
        program_stages=program_stages,
        programs=program_metadata,
    )

    output_cols = [col for col in config.output_cols if col in events_complete.columns]
    events_complete = events_complete.select(output_cols)

    current_run.log_info("Sucessfully joined object names to output data")

    if dst_file:
        dst_file = Path(workspace.files_path) / dst_file
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_file = default_output_path()

    events_complete.write_parquet(dst_file)
    current_run.add_file_output(dst_file.as_posix())
    current_run.log_info(f"Data written to {dst_file}")

    if dst_dataset:
        write_to_dataset(fp=dst_file, dataset=dst_dataset)

    if dst_table:
        write_to_db(df=events_complete, table_name=dst_table)

    lineage.pipeline_complete()


def initialize_lineage(local: bool):
    """Initialize lineage tracking for the pipeline."""
    if local:
        workspace_slug = "local"
    else:
        workspace_slug = workspace.slug
    pipeline_slug = "dhis2_event_extract"

    lineage.init_client(
        url="https://lineage.demo.openhexa.org/",
        workspace_slug=workspace_slug,
        pipeline_slug=pipeline_slug,
        enable_pipeline_jobs="true",
    )

    if lineage.is_initialized():
        current_run.log_info(
            f"OpenLineage client initialized successfully, run id: {lineage._client.run_id} ."
        )
    else:
        current_run.log_warning("OpenLineage client initialization failed.")

    lineage.pipeline_start()


def check_program(program_id: str, program_metadata: pl.DataFrame):
    """Check if the program exists in the metadata."""
    if program_id not in program_metadata["id"]:
        current_run.log_error(f"Program with ID {program_id} is not present in the DHIS2 instance.")
        raise ValueError(f"Program with ID {program_id} is not present in the DHIS2 instance.")


def check_dates(start: str | None, end: str | None, period: int | None):
    """Check and format start and end dates."""
    if start is not None:
        validate_yyyymmdd(start)

    if end is not None:
        validate_yyyymmdd(end)

    if start is not None and end is not None:
        if start > end:
            current_run.log_error(f"Start date {start} must not be after end date {end}.")
            raise ValueError(f"Start date {start} must not be after end date {end}.")

    if start is None and period is None:
        current_run.log_error("Either start date or period must be provided.")
        raise ValueError("Either start date or period must be provided.")


def check_server_health(dhis2: DHIS2):
    """Check if the DHIS2 server is responding."""
    try:
        dhis2.ping()
    except ConnectionError:
        current_run.log_error(f"Unable to reach DHIS2 instance at url {dhis2.api.url}")
        raise


if __name__ == "__main__":
    dhis_event_extract()
