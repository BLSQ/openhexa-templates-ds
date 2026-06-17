# DHIS2 Event Extract Pipeline

Extract Events (program stage instances) from a DHIS2 instance for analytics and reporting.

## Overview

This pipeline extracts event-level data from DHIS2 programs and outputs structured datasets in multiple formats. It's designed for monitoring & evaluation teams and data engineers who need clean event extracts for reporting, dashboards, or data warehouses.

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `source_connection` | DHIS2Connection | Yes | - | Source DHIS2 instance to extract events from |
| `program` | str | Yes | - | Program UID from which to extract events |
| `program_stage` | str | No | - | Optional Program Stage UID (restrict to one stage) |
| `org_units` | list[str] | No | - | List of orgUnit UIDs to filter on |
| `status` | str | No | COMPLETED | Event status filter: ACTIVE, COMPLETED, ALL |
| `since_date` | str | No | - | Only extract events updated since this date (YYYY-MM-DD) |
| `output_format` | str | No | parquet | Output format: csv, jsonl, parquet |
| `output_path` | str | No | - | Output file path in workspace (auto-generated if not provided) |

## Output Schema

The pipeline transforms DHIS2 events into a tabular format with:

- `event` - Event UID
- `program` - Program UID  
- `programStage` - Program Stage UID
- `orgUnit` - Organization Unit UID
- `eventDate` - Event date
- `completedDate` - Completion date
- `status` - Event status (ACTIVE, COMPLETED, etc.)
- `{dataElementUID}_value` - One column per data element with its value

## Usage Example

```python
from openhexa.sdk import workspace

# Configure parameters
params = {
    "source_connection": workspace.dhis2_connections["my_dhis2"],
    "program": "IpHINAT79UW",  # Child Programme
    "program_stage": "A03MvHHogjR",  # Birth stage (optional)
    "org_units": ["DiszpKrYNg8", "jUb8gELQApl"],  # Specific org units
    "status": "COMPLETED",
    "since_date": "2024-01-01",
    "output_format": "parquet"
}

# Run pipeline
result = dhis2_event_extract(**params)
print(f"Extracted {result['total_events_extracted']} events")
```

## Local Development

### Quick Start

```bash
# 1. Install dependencies
pip install -e ".[dev]"

# 2. Run unit tests (no DHIS2 required)
pytest tests/ -k "not integration" -v

# 3. Run integration tests (requires DHIS2)
export DHIS2_URL=https://play.im.dhis2.org/stable-2-39-10-1
export DHIS2_USER=admin
export DHIS2_PASS=district
pytest tests/ -v

# 4. Run with Docker
make test
```

### Manual Setup

```bash
# Start local DHIS2 (optional)
make up

# Build test environment
make build-tests

# Run all tests
make test

# Clean up
make down
```

## Architecture

The pipeline follows OpenHEXA best practices with:

1. **Dual Function Architecture**: Each task has both an internal pure function (`_task_name`) and an OpenHEXA wrapper (`@pipeline.task`)
2. **Pagination Handling**: Streams large event datasets to avoid memory issues
3. **Dynamic Schema**: Adapts to different program structures and data elements
4. **Multiple Output Formats**: CSV, JSONL, and Parquet support using Polars
5. **Comprehensive Logging**: Task-level logging with progress tracking
6. **Graceful Error Handling**: Descriptive errors for common failure scenarios

## Testing

- **Unit Tests**: Parameter validation, data transformation logic (19+ tests)
- **Integration Tests**: End-to-end extraction with real DHIS2 servers (15+ tests)
- **Dynamic Testing**: Automatically discovers available programs and stages
- **Fallback Servers**: Tests against multiple DHIS2 demo servers for reliability

## Error Handling

- Invalid program UIDs raise descriptive errors
- Network timeouts include retry logic via DHIS2 pagination
- Malformed dates validated before API calls
- Missing data elements handled gracefully with null values
- Graceful degradation when DHIS2 servers unavailable