import re
from datetime import datetime
from pathlib import Path

from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    extract_data_elements,
    get_category_option_combos,
    get_data_elements,
    get_organisation_unit_groups,
    get_organisation_units,
    join_object_names,
)


@pipeline("dhis2-extract-data-elements")
@parameter(
    code="src_dhis2",
    type=DHIS2Connection,
    name="Source DHIS2 instance",
    help="The DHIS2 instance to extract data elements from",
)
@parameter(
    code="data_elements",
    type=str,
    multiple=True,
    name="Data elements",
    help="Data elements to extract",
    required=True,
    widget=DHIS2Widget.DATA_ELEMENTS,
    connection="src_dhis2",
)
@parameter(
    code="organisation_units",
    type=str,
    multiple=True,
    name="Organisation units",
    help="IDs of organisation units to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNITS,
    connection="src_dhis2",
)
@parameter(
    code="organisation_unit_groups",
    type=str,
    multiple=True,
    name="Organisation unit groups",
    help="IDs of organisation unit groups to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNIT_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="include_children",
    type=bool,
    name="Include children",
    help="Include children organisation units",
    default=False,
)
@parameter(
    code="start_date",
    type=str,
    name="Start date (YYYY-MM-DD)",
    help="Start date for the extraction",
    default="2020-01-01",
)
@parameter(
    code="end_date",
    type=str,
    name="End date (YYYY-MM-DD)",
    help="End date for the extraction (today by default)",
    required=False,
)
@parameter(
    code="dst_file",
    type=str,
    name="Output file",
    help="Output file path in the workspace. Parent directory will automatically be created.",
    required=False,
)
def dhis2_extract_data_elements(
    src_dhis2: DHIS2Connection,
    data_elements: list[str],
    start_date: str,
    organisation_units: list[str] | None = None,
    organisation_unit_groups: list[str] | None = None,
    include_children: bool = False,
    end_date: str | None = None,
    dst_file: str | None = None,
):
    """Extract data elements from a DHIS2 instance and save them to a parquet file."""
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=src_dhis2, cache_dir=cache_dir)

    check_server_health(dhis2)
    check_authentication(dhis2)

    current_run.log_info("Reading metadata from source DHIS2 instance")
    src_data_elements = get_data_elements(dhis2)
    src_organisation_units = get_organisation_units(dhis2)
    src_organisation_unit_groups = get_organisation_unit_groups(dhis2)
    src_category_option_combos = get_category_option_combos(dhis2)
    current_run.log_info("Sucessfully read metadata from source DHIS2 instance")

    current_run.log_info("Checking data request")

    where = organisation_units or organisation_unit_groups
    if not where:
        msg = "No organisation units or organisation unit groups provided"
        current_run.log_error(msg)
        raise ValueError(msg)

    if data_elements is not None:
        data_elements = filter_objects(
            objects_in_request=data_elements,
            objects_in_dhis2=src_data_elements["id"].to_list(),
            object_type="Data element",
        )

    if organisation_units is not None:
        organisation_units = filter_objects(
            objects_in_request=organisation_units,
            objects_in_dhis2=src_organisation_units["id"].to_list(),
            object_type="Organisation unit",
        )

    if organisation_unit_groups is not None:
        organisation_unit_groups = filter_objects(
            objects_in_request=organisation_unit_groups,
            objects_in_dhis2=src_organisation_unit_groups["id"].to_list(),
            object_type="Organisation unit group",
        )

    current_run.log_info("Starting data extraction")
    data_values = extract_data_elements(
        dhis2=dhis2,
        data_elements=data_elements,
        organisation_units=organisation_units,
        organisation_unit_groups=organisation_unit_groups,
        include_children=include_children,
        start_date=start_date,
        end_date=end_date,
    )
    current_run.log_info(f"Extracted {len(data_values)} data values")

    current_run.log_info("Joining object names to output data")
    data_values = join_object_names(
        df=data_values,
        data_elements=src_data_elements,
        organisation_units=src_organisation_units,
        organisation_unit_groups=src_organisation_unit_groups,
        category_option_combos=src_category_option_combos,
    )
    current_run.log_info("Sucessfully joined object names to output data")

    if dst_file:
        dst_file = Path(workspace.files_path) / dst_file
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_file = default_output_path()

    current_run.log_info(f"Writing data to {dst_file}")
    data_values.write_parquet(dst_file)
    current_run.add_file_output(dst_file.as_posix())
    current_run.log_info(f"Data written to {dst_file}")


def default_output_path() -> Path:
    """Get default output path for pipeline outputs.

    Default output path is:
    <workspace>/pipelines/dhis2_extract_data_elements/<date>/data_values.parquet

    Returns
    -------
    Path
        The default output path for the pipeline.
    """
    dst_dir = Path(workspace.files_path) / "pipelines" / "dhis2_extract_data_elements"
    dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dst_dir.mkdir(parents=True, exist_ok=True)
    return dst_dir / "data_values.parquet"


def is_iso_date(date_string: str) -> bool:
    """Check if a string is in ISO date format (YYYY-MM-DD).

    Parameters
    ----------
    date_string : str
        The string to check.

    Returns
    -------
    bool
        True if the string is in ISO date format, False otherwise.
    """
    pattern = r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])$"
    return bool(re.match(pattern, date_string))


def check_server_health(dhis2: DHIS2):
    """Check if the DHIS2 server is responding."""
    try:
        dhis2.ping()
    except ConnectionError:
        current_run.log_error(f"Unable to reach DHIS2 instance at url {dhis2.api.url}")
        raise


def check_authentication(dhis2: DHIS2):
    """Check if authentication was successful."""
    r = dhis2.me()
    if r.status_code != 200:
        msg = f"Unable to authenticate to DHIS2 instance at url: {dhis2.api.url}"
        current_run.log_error(msg)
        raise ConnectionError(msg)

    msg = f"Connected to DHIS2 instance at url {dhis2.api.url} with username '{r['username']}'"
    current_run.log_info(msg)


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
