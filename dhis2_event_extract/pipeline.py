from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("dhis2-event-extract")
@parameter(
    code="source_connection",
    type=DHIS2Connection,
    name="Source DHIS2 instance",
    help="Source DHIS2 instance to extract events from",
    required=True,
)
@parameter(
    code="program",
    type=str,
    name="Program",
    help="Program UID from which to extract events",
    required=True,
)
@parameter(
    code="program_stage",
    type=str,
    name="Program Stage",
    help="Optional Program Stage UID (restrict to one stage)",
    required=False,
)
@parameter(
    code="org_units",
    type=str,
    multiple=True,
    name="Organisation Units",
    help="List of orgUnit UIDs to filter on",
    required=False,
)
@parameter(
    code="status",
    type=str,
    name="Event Status",
    help="Event status filter: ACTIVE, COMPLETED, ALL",
    default="COMPLETED",
)
@parameter(
    code="since_date",
    type=str,
    name="Since Date",
    help="Only extract events updated since this date (YYYY-MM-DD)",
    required=False,
)
@parameter(
    code="output_format",
    type=str,
    name="Output Format",
    help="Output format: csv, jsonl, parquet",
    default="parquet",
)
@parameter(
    code="output_path",
    type=str,
    name="Output Path",
    help="Output file path in workspace (auto-generated if not provided)",
    required=False,
)
def dhis2_event_extract(
    source_connection: DHIS2Connection,
    program: str,
    program_stage: str | None = None,
    org_units: list[str] | None = None,
    status: str = "COMPLETED",
    since_date: str | None = None,
    output_format: str = "parquet",
    output_path: str | None = None,
) -> dict[str, Any]:
    """Extract DHIS2 events and save them to a file."""
    current_run.log_info("Starting DHIS2 event extraction pipeline")

    # Validate connection and program
    client = validate_connection(source_connection, program)

    # Build query parameters
    query_params = build_query_params(program, program_stage, org_units, status, since_date)

    # Fetch events from DHIS2
    events = fetch_events(client, query_params)

    # Transform events to tabular format
    transformed_data = transform_events(events)

    # Write output to workspace
    output_file_path = write_output(transformed_data, program, output_format, output_path)

    # Generate summary
    summary = generate_summary(len(events), query_params, output_file_path, program, output_format)

    current_run.log_info(f"Pipeline complete. Extracted {len(events)} events to {output_file_path}")
    return summary


def validate_connection(source_connection: DHIS2Connection, program: str) -> DHIS2:
    """Validate DHIS2 connection and program existence."""
    current_run.log_info("Validating DHIS2 connection")

    # Initialize DHIS2 client
    client = DHIS2(source_connection)

    # Test connection by getting system info (validates credentials)
    try:
        info = client.api.get("system/info")
        current_run.log_info(f"Connected to DHIS2 version: {info.get('version', 'unknown')}")
    except Exception as e:
        raise ValueError(f"Cannot connect to DHIS2: {e}") from e

    # Validate program exists
    try:
        program_info = client.api.get(f"programs/{program}", params={"fields": "id,name"})
        current_run.log_info(f"Found program: {program_info.get('name', program)}")
    except Exception as e:
        raise ValueError(f"Program '{program}' not found in DHIS2: {e}") from e

    return client


def build_query_params(
    program: str,
    program_stage: str | None,
    org_units: list[str] | None,
    status: str,
    since_date: str | None,
) -> dict[str, Any]:
    """Build query parameters for DHIS2 events API."""
    current_run.log_info("Building query parameters")

    params: dict[str, Any] = {
        "program": program,
        "paging": "true",
        "pageSize": "250",
        "fields": (
            "event,program,programStage,orgUnit,eventDate,completedDate,status,"
            "dataValues[dataElement,value]"
        ),
    }

    # Add optional filters
    if program_stage:
        params["programStage"] = program_stage
        current_run.log_info(f"Filtering to program stage: {program_stage}")

    if org_units:
        # DHIS2 toolbox handles list of org units correctly
        params["orgUnit"] = org_units
        current_run.log_info(f"Filtering to {len(org_units)} organization units")

    if status != "ALL":
        params["status"] = status
        current_run.log_info(f"Filtering to status: {status}")

    if since_date:
        # Validate date format
        try:
            datetime.strptime(since_date, "%Y-%m-%d")
            params["lastUpdated"] = since_date
            current_run.log_info(f"Filtering events updated since: {since_date}")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {since_date}") from e

    return params


def fetch_events(client: DHIS2, query_params: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch events from DHIS2 with pagination."""
    current_run.log_info("Fetching events from DHIS2")

    all_events = []
    page = 1

    while True:
        current_params = query_params.copy()
        current_params["page"] = page

        try:
            response = client.api.get("events", params=current_params)

            events = response.get("events", [])
            if not events:
                break

            all_events.extend(events)
            current_run.log_info(
                f"Fetched page {page}: {len(events)} events (total: {len(all_events)})"
            )

            # Check if we have more pages
            pager = response.get("pager", {})
            if page >= pager.get("pageCount", 1):
                break

            page += 1

        except Exception as e:
            current_run.log_error(f"Error fetching events page {page}: {e}")
            raise ValueError(f"Failed to fetch events from DHIS2: {e}") from e

    current_run.log_info(f"Total events fetched: {len(all_events)}")
    return all_events


def transform_events(events: list[dict[str, Any]]) -> pl.DataFrame:
    """Transform DHIS2 events into tabular format using Polars."""
    current_run.log_info(f"Transforming {len(events)} events to tabular format")

    if not events:
        # Return empty DataFrame with expected schema
        return pl.DataFrame(
            schema={
                "event": pl.String,
                "program": pl.String,
                "programStage": pl.String,
                "orgUnit": pl.String,
                "eventDate": pl.String,
                "completedDate": pl.String,
                "status": pl.String,
            }
        )

    rows = []
    all_data_elements = set()

    # First pass: collect all data elements to create consistent schema
    for event in events:
        data_values = event.get("dataValues", [])
        for dv in data_values:
            de_uid = dv.get("dataElement")
            if de_uid:
                all_data_elements.add(f"{de_uid}_value")

    current_run.log_info(f"Found {len(all_data_elements)} unique data elements")

    # Second pass: create rows
    for event in events:
        row = {
            "event": event.get("event"),
            "program": event.get("program"),
            "programStage": event.get("programStage"),
            "orgUnit": event.get("orgUnit"),
            "eventDate": event.get("eventDate"),
            "completedDate": event.get("completedDate"),
            "status": event.get("status"),
        }

        # Add data element values
        data_values = event.get("dataValues", [])
        for dv in data_values:
            de_uid = dv.get("dataElement")
            if de_uid:
                row[f"{de_uid}_value"] = dv.get("value")

        # Fill missing data elements with None
        for de_col in all_data_elements:
            if de_col not in row:
                row[de_col] = None

        rows.append(row)

    df = pl.DataFrame(rows)
    current_run.log_info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df


def write_output(
    df: pl.DataFrame, program: str, output_format: str, output_path: str | None
) -> str:
    """Write transformed data to workspace."""
    current_run.log_info("Writing output data")

    output_format = output_format.lower()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Generate filename
    filename = f"dhis2_events_{program}_{timestamp}.{output_format}"

    # Use provided output_path or generate one
    if output_path:
        file_path = Path(workspace.files_path) / output_path
    else:
        file_path = Path(workspace.files_path) / filename

    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if output_format == "csv":
            df.write_csv(file_path)
        elif output_format == "jsonl":
            df.write_ndjson(file_path)
        elif output_format == "parquet":
            df.write_parquet(file_path)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        current_run.log_info(f"Data written to: {file_path}")
        current_run.add_file_output(file_path.as_posix())
        return str(file_path)

    except Exception as e:
        current_run.log_error(f"Error writing output: {e}")
        raise ValueError(f"Failed to write output data: {e}") from e


def generate_summary(
    total_events: int,
    query_params: dict[str, Any],
    output_path: str,
    program: str,
    output_format: str,
) -> dict[str, Any]:
    """Generate pipeline execution summary."""
    current_run.log_info("Generating execution summary")

    summary = {
        "pipeline": "dhis2-event-extract",
        "timestamp": datetime.now().isoformat(),
        "program": program,
        "program_stage": query_params.get("programStage"),
        "org_units": query_params.get("orgUnit"),
        "status_filter": query_params.get("status", "ALL"),
        "since_date": query_params.get("lastUpdated"),
        "output_format": output_format,
        "total_events_extracted": total_events,
        "output_file": output_path,
        "query_parameters": query_params,
    }

    current_run.log_info(f"Summary: {total_events} events extracted to {output_path}")
    return summary


if __name__ == "__main__":
    dhis2_event_extract()
