import re
from datetime import datetime
from pathlib import Path

from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.api import DHIS2ApiError
from openhexa.toolbox.dhis2.dataframe import (
    extract_analytics,
    get_category_option_combos,
    get_data_elements,
    get_indicators,
    get_organisation_units,
    join_object_names,
)
from openhexa.toolbox.dhis2.periods import period_from_string


@pipeline("dhis2-extract-analytics")
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
    required=False,
    widget=DHIS2Widget.DATA_ELEMENTS,
    connection="src_dhis2",
)
@parameter(
    code="data_element_groups",
    type=str,
    multiple=True,
    name="Data element groups",
    help="Data element groups to extract",
    required=False,
    widget=DHIS2Widget.DATA_ELEMENT_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="indicators",
    type=str,
    multiple=True,
    name="Indicators",
    help="Indicators to extract",
    required=False,
    widget=DHIS2Widget.INDICATORS,
    connection="src_dhis2",
)
@parameter(
    code="indicator_groups",
    type=str,
    multiple=True,
    name="Indicator groups",
    help="Indicator groups to extract",
    required=False,
    widget=DHIS2Widget.INDICATOR_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="org_units",
    type=str,
    multiple=True,
    name="Organisation units",
    help="IDs of organisation units to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNITS,
    connection="src_dhis2",
)
@parameter(
    code="org_unit_groups",
    type=str,
    multiple=True,
    name="Organisation unit groups",
    help="IDs of organisation unit groups to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNIT_GROUPS,
    connection="src_dhis2",
)
@parameter(
    code="org_unit_levels",
    type=str,
    multiple=True,
    name="Organisation unit levels",
    help="Organisation unit levels to extract data elements from",
    required=False,
    widget=DHIS2Widget.ORG_UNIT_LEVELS,
    connection="src_dhis2",
)
@parameter(
    code="start_period",
    type=str,
    name="Start period",
    help="Start period for the extraction (DHIS2 format)",
    required=True,
)
@parameter(
    code="end_period",
    type=str,
    name="End period",
    help="End period for the extraction (DHIS2 format)",
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
    start_period: str,
    data_elements: list[str] | None = None,
    data_element_groups: list[str] | None = None,
    indicators: list[str] | None = None,
    indicator_groups: list[str] | None = None,
    org_units: list[str] | None = None,
    org_unit_groups: list[str] | None = None,
    org_unit_levels: list[str] | None = None,
    end_period: str | None = None,
    dst_file: str | None = None,
):
    """Extract data elements from a DHIS2 instance and save them to a parquet file."""
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=src_dhis2, cache_dir=cache_dir)

    check_server_health(dhis2)

    current_run.log_info("Reading metadata from source DHIS2 instance")
    src_data_elements = get_data_elements(dhis2)
    src_indicators = get_indicators(dhis2)
    src_organisation_units = get_organisation_units(dhis2)
    src_category_option_combos = get_category_option_combos(dhis2)
    current_run.log_info("Sucessfully read metadata from source DHIS2 instance")

    current_run.log_info("Checking data request")

    # convert start and end periods to Period objects
    # if it is not provided, the end Period is set using the same type of the start Period
    # and initialized with the current date
    start = period_from_string(start_period)
    if not end_period:
        end = type(start)(datetime.now())
        current_run.log_info(f"End period not provided, using latest period: {end}")
    else:
        end = period_from_string(end_period)

    # build the list of periods between start and end periods
    periods = [str(p) for p in start.get_range(end)]

    current_run.log_info("Starting data extraction")

    try:
        data_values = extract_analytics(
            dhis2=dhis2,
            periods=periods,
            data_elements=data_elements if data_elements else None,
            data_element_groups=data_element_groups if data_element_groups else None,
            indicators=indicators if indicators else None,
            indicator_groups=indicator_groups if indicator_groups else None,
            org_units=org_units if org_units else None,
            org_unit_groups=org_unit_groups if org_unit_groups else None,
            org_unit_levels=org_unit_levels if org_unit_levels else None,
        )
    except (ValueError, DHIS2ApiError) as e:
        current_run.log_error(str(e))
        raise

    current_run.log_info(f"Extracted {len(data_values)} data values")

    current_run.log_info("Joining object names to output data")
    data_values = join_object_names(
        df=data_values,
        data_elements=src_data_elements if "data_element_id" in data_values.columns else None,
        indicators=src_indicators if "indicator_id" in data_values.columns else None,
        organisation_units=src_organisation_units,
        category_option_combos=src_category_option_combos
        if "category_option_combo_id" in data_values.columns
        else None,
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
    <workspace>/pipelines/dhis2_extract_analytics/<date>/data_values.parquet

    Returns
    -------
    Path
        The default output path for the pipeline.
    """
    dst_dir = Path(workspace.files_path) / "pipelines" / "dhis2_extract_analytics"
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
