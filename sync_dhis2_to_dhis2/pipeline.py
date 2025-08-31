"""DHIS2 to DHIS2 Sync Check Pipeline.

Verifies if mapped Data Elements in a source DHIS2 instance have been updated
since a given date. Produces a decision report on whether synchronization is needed.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("sync_dhis2_to_dhis2")
@parameter(
    code="source_connection",
    type=DHIS2Connection,
    name="Source DHIS2 instance",
    help="Source DHIS2 instance to check for updates",
    required=True,
)
@parameter(
    code="target_connection",
    type=DHIS2Connection,
    name="Target DHIS2 instance",
    help="Target DHIS2 instance (for validation, not modified)",
    required=True,
)
@parameter(
    code="mapping_file",
    type=str,
    name="Mapping File",
    help="Path to JSON with dataElement, categoryOptionCombo, and orgUnit mappings",
    required=True,
)
@parameter(
    code="since_date",
    type=str,
    name="Since Date",
    help="Date in YYYY-MM-DD format. Check if source DE values updated since this date",
    required=True,
)
@parameter(
    code="dry_run",
    type=bool,
    name="Dry Run",
    help="Always true; no writing occurs, pipeline only checks for updates",
    default=True,
)
def sync_dhis2_to_dhis2(
    source_connection: DHIS2Connection,
    target_connection: DHIS2Connection,
    mapping_file: str,
    since_date: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Check if DHIS2 source data has been updated since given date."""
    current_run.log_info("Starting DHIS2 sync check pipeline")

    # Validate connections
    clients = validate_connections(source_connection, target_connection)

    # Load and validate mappings
    mappings = load_and_validate_mappings(mapping_file)

    # Fetch updates from source
    updates = fetch_updates_since_date(clients["source_client"], mappings, since_date)

    # Generate summary report
    summary = generate_summary(mappings, updates, since_date, dry_run)

    current_run.log_info(f"Pipeline complete. Sync needed: {summary['sync_needed']}")
    return summary


def validate_connections(source_connection: DHIS2Connection, target_connection: DHIS2Connection) -> dict[str, Any]:
    """Validate both DHIS2 connections and return clients."""
    current_run.log_info("Validating DHIS2 connections")

    # Initialize source client
    source_client = DHIS2(source_connection)

    # Initialize target client  
    target_client = DHIS2(target_connection)

    # Test source connection
    try:
        source_info = source_client.api.get("system/info")
        current_run.log_info(f"Source DHIS2 version: {source_info.get('version', 'unknown')}")
    except Exception as e:
        raise ValueError(f"Cannot connect to source DHIS2: {e}") from e

    # Test target connection
    try:
        target_info = target_client.api.get("system/info")
        current_run.log_info(f"Target DHIS2 version: {target_info.get('version', 'unknown')}")
    except Exception as e:
        raise ValueError(f"Cannot connect to target DHIS2: {e}") from e

    return {"source_client": source_client, "target_client": target_client}


def load_and_validate_mappings(mapping_file: str) -> dict[str, Any]:
    """Load and validate mapping JSON file."""
    current_run.log_info("Loading and validating mappings")

    # Resolve path relative to workspace if not absolute
    if not Path(mapping_file).is_absolute():
        mapping_path = Path(workspace.files_path) / mapping_file
    else:
        mapping_path = Path(mapping_file)

    try:
        with open(mapping_path, encoding="utf-8") as f:
            mappings = json.load(f)
    except FileNotFoundError as e:
        raise ValueError(f"Mapping file not found: {mapping_path}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in mapping file: {e}") from e

    # Validate structure
    if not isinstance(mappings, dict):
        raise ValueError("Mapping file must contain a JSON object")

    required_sections = ["dataElements", "categoryOptionCombos", "orgUnits"]
    for section in required_sections:
        if section not in mappings:
            raise ValueError(f"Mapping file must contain '{section}' section")
        if not isinstance(mappings[section], dict):
            raise ValueError(f"'{section}' must be a dictionary")

    de_count = len(mappings["dataElements"])
    coc_count = len(mappings["categoryOptionCombos"])
    ou_count = len(mappings["orgUnits"])
    current_run.log_info(
        f"Loaded {de_count} data element, {coc_count} category option combo, and {ou_count} org unit mappings"
    )

    return mappings


def fetch_updates_since_date(
    source_client: DHIS2, mappings: dict[str, Any], since_date: str
) -> list[dict[str, Any]]:
    """Query source DHIS2 for data values updated since the given date."""
    current_run.log_info("Fetching updates from source DHIS2 since date")

    # Validate date format
    try:
        datetime.strptime(since_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {since_date}") from e

    source_des = list(mappings["dataElements"].keys())
    source_cocs = list(mappings["categoryOptionCombos"].keys())
    source_orgunits = list(mappings["orgUnits"].keys())

    if not source_des:
        current_run.log_info("No data elements to check")
        return []

    if not source_orgunits:
        current_run.log_info("No organization units specified in mapping")
        return []

    current_run.log_info(f"Checking {len(source_des)} data elements for updates since {since_date}")

    params = {
        "dataElement": source_des,
        "categoryOptionCombo": source_cocs,
        "orgUnit": source_orgunits,
        "lastUpdated": since_date,
        "paging": "false",
    }

    try:
        response = source_client.api.get("dataValueSets", params=params)
        data_values = response.get("dataValues", [])

        current_run.log_info(f"Found {len(data_values)} updated data values since {since_date}")
        return data_values

    except Exception as e:
        current_run.log_error(f"Error fetching data values: {e}")
        raise ValueError(f"Failed to fetch updates from source DHIS2: {e}") from e


def generate_summary(
    mappings: dict[str, Any], updates: list[dict[str, Any]], since_date: str, dry_run: bool
) -> dict[str, Any]:
    """Generate summary report with sync recommendation."""
    current_run.log_info("Generating summary report")

    total_des = len(mappings["dataElements"])
    total_cocs = len(mappings["categoryOptionCombos"])
    total_orgunits = len(mappings["orgUnits"])

    updated_des = set()
    updated_cocs = set()
    updated_orgunits = set()
    update_timestamps = []

    for update in updates:
        if "dataElement" in update:
            updated_des.add(update["dataElement"])
        if "categoryOptionCombo" in update:
            updated_cocs.add(update["categoryOptionCombo"])
        if "orgUnit" in update:
            updated_orgunits.add(update["orgUnit"])
        if "lastUpdated" in update:
            update_timestamps.append(update["lastUpdated"])

    sync_needed = len(updates) > 0

    summary = {
        "pipeline": "sync_dhis2_to_dhis2",
        "timestamp": datetime.now().isoformat(),
        "since_date": since_date,
        "total_data_elements_checked": total_des,
        "total_category_option_combos_checked": total_cocs,
        "total_org_units_checked": total_orgunits,
        "updates_found": len(updates),
        "unique_data_elements_updated": len(updated_des),
        "unique_category_option_combos_updated": len(updated_cocs),
        "unique_org_units_updated": len(updated_orgunits),
        "sync_needed": sync_needed,
        "dry_run": dry_run,
        "updated_data_elements": list(updated_des),
        "updated_category_option_combos": list(updated_cocs),
        "updated_org_units": list(updated_orgunits),
        "latest_update_timestamps": sorted(update_timestamps)[-10:] if update_timestamps else [],
    }

    current_run.log_info(f"Summary: {len(updates)} updates found, sync_needed: {sync_needed}")

    if sync_needed:
        current_run.log_info(f"Data elements with updates: {list(updated_des)}")
        current_run.log_info(f"Category option combos with updates: {list(updated_cocs)}")
        current_run.log_info(f"Org units with updates: {list(updated_orgunits)}")
    else:
        current_run.log_info("No updates found since the specified date")

    return summary


def apply_mappings(data_value: dict[str, Any], mappings: dict[str, Any]) -> dict[str, Any]:
    """Apply DE/COC/orgUnit mappings to a data value."""
    result = data_value.copy()

    de_mappings = mappings.get("dataElements", {})
    coc_mappings = mappings.get("categoryOptionCombos", {})
    ou_mappings = mappings.get("orgUnits", {})

    # Apply mappings if they exist
    if "dataElement" in result and result["dataElement"] in de_mappings:
        result["dataElement"] = de_mappings[result["dataElement"]]

    if "categoryOptionCombo" in result and result["categoryOptionCombo"] in coc_mappings:
        result["categoryOptionCombo"] = coc_mappings[result["categoryOptionCombo"]]

    if "orgUnit" in result and result["orgUnit"] in ou_mappings:
        result["orgUnit"] = ou_mappings[result["orgUnit"]]

    return result


if __name__ == "__main__":
    sync_dhis2_to_dhis2()
