# DHIS2 to DHIS2 Sync Check Pipeline

This pipeline verifies if mapped Data Elements in a source DHIS2 instance have been updated since a given date. It produces a decision report indicating whether synchronization is needed, but does not perform the actual synchronization.

## Purpose

Data managers often need to avoid redundant synchronizations across DHIS2 instances. This pipeline helps by checking if source data elements have been updated since a reference date, allowing teams to run expensive sync jobs only when necessary.

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_connection` | DHIS2Connection | Yes | Source DHIS2 instance to check for updates |
| `target_connection` | DHIS2Connection | Yes | Target DHIS2 instance (for comparison, not modified) |
| `mapping_file` | str | Yes | Path to JSON file with dataElement and categoryOptionCombo mappings |
| `since_date` | str | Yes | Date in YYYY-MM-DD format. Check if source DE values updated since this date |
| `dry_run` | bool | No (default: True) | Always true; no writing occurs, required for standardization |

## Mapping File Schema

The mapping file must be a JSON file with the following structure:

```json
{
  "dataElements": {
    "SOURCE_DE_UID": "TARGET_DE_UID",
    "SOURCE_DE_UID_2": "TARGET_DE_UID_2"
  },
  "categoryOptionCombos": {
    "SOURCE_COC_UID": "TARGET_COC_UID",
    "SOURCE_COC_UID_2": "TARGET_COC_UID_2"
  }
}
```

## Output

The pipeline returns a summary report containing:

- `sync_needed`: Boolean indicating if synchronization is recommended
- `updates_found`: Number of data values updated since the specified date
- `unique_data_elements_updated`: Number of unique data elements with updates
- `unique_category_option_combos_updated`: Number of unique COCs with updates  
- `updated_data_elements`: List of data element UIDs that have updates
- `updated_category_option_combos`: List of COC UIDs that have updates
- `latest_update_timestamps`: Up to 10 most recent update timestamps

## Example Usage

```python
# Example mapping file: mapping.json
{
  "dataElements": {
    "fbfJHSPpUQD": "Uvn6LCg7dVU",
    "cYeuwXTCPkU": "sB79w2hiLp8"
  },
  "categoryOptionCombos": {
    "HllvX50cXC0": "rQLFnNXXIL0",
    "xYerKDKCefk": "Gmbgme7z9BF"
  }
}
```

## Testing

### Quick Testing (Unit Tests)
```bash
# Run unit tests (fast, no DHIS2 required)
pytest tests/ -k "not integration" -v
# Result: 19 passed ✅
```

### Full Integration Testing (with DHIS2 Demo Server)
```bash
# Use DHIS2 demo server (recommended)
export DHIS2_URL=https://play.im.dhis2.org/stable-2-39-10-1
export DHIS2_USER=admin
export DHIS2_PASS=district

# Run integration tests with dynamic data discovery
pytest tests/ -k "integration" -v
# Result: Tests discover real DHIS2 data or skip gracefully
```

### Complete Test Suite
```bash
# Run all tests in containerized environment
make test
# Result: 19 passed, 15 skipped (if DHIS2 unavailable) ✅
```

### Development Setup
```bash
# 1. Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# 2. Install in development mode  
pip install -e ".[dev]"

# 3. VSCode will auto-discover tests in Testing panel
# 4. Run tests: Ctrl/Cmd + Shift + P → "Test: Run All Tests"
```

## Pipeline Execution

Run the pipeline to check for updates since January 1, 2024:

```bash
# The pipeline will check if any mapped data elements 
# have been updated since 2024-01-01
```

## Local Development and Testing

### Prerequisites

- Docker and docker-compose
- Python 3.11+
- OpenHEXA SDK and toolbox

### Quick Start

1. **Start local DHIS2 environment:**
   ```bash
   make up
   ```
   This starts DHIS2 Core with Sierra Leone demo database on http://localhost:8080

2. **Run tests:**
   ```bash
   make test
   ```
   This builds the test container and runs the full test suite

3. **Stop environment:**
   ```bash
   make down
   ```

### Manual Commands

```bash
# Start DHIS2 manually
docker compose -f docker-compose.dhis2.yml up -d

# Build test image
docker build -t ohx-tests -f docker/Dockerfile.tests .

# Run tests in container
docker run --rm --network host --env-file .env.example -v "$PWD":/workspace ohx-tests

# Run specific test
docker run --rm --network host --env-file .env.example -v "$PWD":/workspace ohx-tests \
  pytest tests/test_integration_dry_run.py -v
```

### Environment Variables

Copy `.env.example` to `.env` and adjust as needed:

```env
DHIS2_URL=http://localhost:8080
DHIS2_USER=admin
DHIS2_PASS=district
DHIS2_IMAGE=dhis2/core:40.8.0
DHIS2_DB_DUMP_URL=https://databases.dhis2.org/sierra-leone/2.40/dhis2-db-sierra-leone.sql.gz
```

## Pipeline Tasks

1. **validate_connections**: Ensures both source and target DHIS2 instances are accessible
2. **load_and_validate_mappings**: Parses and validates the mapping JSON file
3. **fetch_updates_since_date**: Queries source DHIS2 for data values updated since the specified date
4. **generate_summary**: Produces the final decision report with sync recommendation

## Error Handling

The pipeline handles common error scenarios:

- Invalid date formats (expects YYYY-MM-DD)
- Missing or malformed mapping files
- DHIS2 connection failures
- Missing data elements or category option combos in source
- Empty update sets (returns `sync_needed: false`)

## Limitations

- This is a read-only pipeline - it does not perform actual synchronization
- Only checks for updates in data values, not metadata changes
- Requires valid mapping files with existing UIDs in both systems
- Date filtering relies on DHIS2's `lastUpdated` field accuracy