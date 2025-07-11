# DHIS2 to DHIS2 Data Elements Pipeline

This OpenHEXA pipeline extracts data values from a source DHIS2 instance for a given dataset and writes the values to a target DHIS2 instance, using mappings for `dataElement` and `categoryOptionCombo` IDs.

## Features

- ✅ Extract data values from source DHIS2 instance by dataset
- ✅ Apply data element and category option combo mappings
- ✅ Validate organization units exist in both systems
- ✅ Write mapped data values to target DHIS2 instance
- ✅ Handle unmapped data elements gracefully (log warnings)
- ✅ Prevent duplicate data value creation
- ✅ Dry run mode for testing
- ✅ Comprehensive logging and error handling
- ✅ Pipeline execution summary export

## Requirements

- OpenHEXA workspace with DHIS2 connections configured
- Source and target DHIS2 instances
- Mapping file (JSON format) for data elements and category option combos
- Organization units with identical UIDs in both DHIS2 instances

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_connection` | DHIS2Connection | Yes | Source DHIS2 instance connection |
| `target_connection` | DHIS2Connection | Yes | Target DHIS2 instance connection |
| `dataset_id` | String | Yes | Dataset ID to extract from source |
| `mapping_file` | File | Yes | JSON file containing data element and COC mappings |
| `start_date` | String | Yes | Start date (YYYY-MM-DD format) |
| `end_date` | String | Yes | End date (YYYY-MM-DD format) |
| `dry_run` | Boolean | No | Dry run mode (default: false) |

## Mapping File Format

The mapping file must be a JSON file with the following structure:

```json
{
  "dataElements": {
    "source_data_element_id_1": "target_data_element_id_1",
    "source_data_element_id_2": "target_data_element_id_2"
  },
  "categoryOptionCombos": {
    "source_coc_id_1": "target_coc_id_1",
    "source_coc_id_2": "target_coc_id_2"
  }
}
```

### Example Mapping File

```json
{
  "dataElements": {
    "FTRrcoaog83": "Uvn6LCg7dVU",
    "eY5ehpbEsB7": "sWoqcoByYmD",
    "Ix2HsbDMLea": "FTRrcoaog83"
  },
  "categoryOptionCombos": {
    "HllvX50cXC0": "rQLFnNXXIL0",
    "RkbOhHwiOgW": "HllvX50cXC0",
    "J40PpdN4Wkk": "RkbOhHwiOgW"
  }
}
```

## Workspace Configuration

Your `workspace.yaml` must include connections to both source and target DHIS2 instances:

```yaml
connections:
  dhis2-source:
    type: dhis2
    url: https://source.dhis2.org
    username: admin
    password: district
  dhis2-target:
    type: dhis2
    url: https://target.dhis2.org
    username: admin
    password: district
```

## Usage Examples

### 1. Basic Usage

```bash
openhexa pipeline run dhis2_to_dhis2_data_elements \\
  --source_connection=dhis2-source \\
  --target_connection=dhis2-target \\
  --dataset_id=BfMAe6Itzgt \\
  --mapping_file=mapping.json \\
  --start_date=2024-01-01 \\
  --end_date=2024-01-31
```

### 2. Dry Run Mode

```bash
openhexa pipeline run dhis2_to_dhis2_data_elements \\
  --source_connection=dhis2-source \\
  --target_connection=dhis2-target \\
  --dataset_id=BfMAe6Itzgt \\
  --mapping_file=mapping.json \\
  --start_date=2024-01-01 \\
  --end_date=2024-01-31 \\
  --dry_run=true
```

### 3. Python API Usage

```python
from openhexa.sdk import workspace

# Get connections
source_conn = workspace.dhis2_connections.get("dhis2-source")
target_conn = workspace.dhis2_connections.get("dhis2-target")

# Run pipeline
dhis2_to_dhis2_data_elements(
    source_connection=source_conn,
    target_connection=target_conn,
    dataset_id="BfMAe6Itzgt",
    mapping_file="mapping.json",
    start_date="2024-01-01",
    end_date="2024-01-31",
    dry_run=False
)
```

## Pipeline Tasks

The pipeline executes the following tasks in sequence:

1. **`validate_connections`** - Verify source and target DHIS2 connections
2. **`load_and_validate_mappings`** - Load and validate mapping file structure
3. **`extract_source_data`** - Extract data values from source DHIS2 instance
4. **`validate_org_units`** - Ensure org units exist in both systems
5. **`transform_data_values`** - Apply data element and COC mappings
6. **`post_to_target`** - Post transformed data to target DHIS2 instance
7. **`generate_summary`** - Create pipeline execution summary

## Data Flow

```
Source DHIS2 → Extract Dataset → Validate OrgUnits → Apply Mappings → Target DHIS2
     ↓              ↓                ↓                ↓               ↓
 Dataset ID    Data Values    Filtered Data    Mapped Data    Import Summary
```

## Error Handling

The pipeline includes comprehensive error handling:

- **Connection Errors**: Validates both DHIS2 connections before proceeding
- **Mapping Validation**: Checks that source and target IDs exist in respective systems
- **Organization Unit Validation**: Filters data for org units that exist in both systems
- **API Errors**: Handles DHIS2 API errors with detailed logging
- **Data Validation**: Ensures required fields are present before posting

## Logging

The pipeline provides detailed logging at each step:

- Connection validation results
- Mapping validation statistics
- Data extraction metrics
- Organization unit validation results
- Transformation statistics
- Import results and conflicts
- Final execution summary

## Output Files

The pipeline generates:

- **Pipeline Summary** (`pipeline_summary.json`): Comprehensive execution statistics
- **Log Output**: Detailed execution logs in OpenHEXA interface

### Pipeline Summary Structure

```json
{
  "pipeline": "dhis2_to_dhis2_data_elements",
  "execution_time": "2024-01-15T14:30:00",
  "dry_run": false,
  "extraction": {
    "original_records": 1500,
    "final_records": 1200
  },
  "transformation": {
    "mapped_data_elements": 1200,
    "unmapped_data_elements": 300,
    "mapped_category_option_combos": 1200
  },
  "import": {
    "status": "SUCCESS",
    "imported": 800,
    "updated": 400,
    "ignored": 0,
    "conflicts": 0
  }
}
```

## Troubleshooting

### Common Issues

**1. Connection Errors**
```
Error: ✗ Source DHIS2 connection failed: Connection timeout
```
- Check network connectivity to DHIS2 instance
- Verify credentials in workspace.yaml
- Ensure DHIS2 instance is accessible

**2. Missing Data Elements**
```
Warning: Missing source data elements: ['ABC123', 'DEF456']
```
- Verify data element IDs exist in source DHIS2
- Update mapping file with correct IDs
- Check data element access permissions

**3. Organization Unit Validation**
```
Warning: Missing org units in target: 45
```
- Ensure organization units exist in both systems
- Check organization unit hierarchy
- Verify organization unit access permissions

**4. Import Conflicts**
```
Warning: Conflicts encountered: 5
```
- Check data validation rules in target DHIS2
- Verify period and organization unit combinations
- Review category option combo mappings

### Best Practices

1. **Test with Dry Run**: Always test with `dry_run=true` first
2. **Validate Mappings**: Ensure all mappings are correct before running
3. **Check Permissions**: Verify user has necessary permissions in both systems
4. **Monitor Logs**: Review execution logs for warnings and errors
5. **Backup Data**: Consider backing up target data before large imports

## Development

### Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run linting
ruff check pipeline.py --fix
mypy pipeline.py

# Run tests (if available)
pytest tests/ -v
```

### Contributing

1. Follow existing code patterns and conventions
2. Add comprehensive logging for new features
3. Include error handling for all API calls
4. Update documentation for any parameter changes
5. Test with both dry run and live modes

## License

This pipeline is part of the OpenHEXA templates collection.