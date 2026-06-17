# DHIS2 to DHIS2 Data Elements Pipeline - PRP

## 🎯 Goal

Build an OpenHEXA pipeline that **extracts data values from a source DHIS2 instance** for a given dataset and **writes the values to a target DHIS2 instance**, using mappings for `dataElement` and `categoryOptionCombo` IDs.

The pipeline must:
- Only consider `orgUnits` with identical UIDs in both instances
- Be parameterized with a `dataset_id` and `mapping file`
- Use OpenHEXA `@pipeline`, `@task`, and `@parameter` decorators
- Rely on the `openhexa-toolbox.dhis2` library

---

## 🧭 Why This Pipeline

- **Data Migration**: Transfer data between DHIS2 instances during system migrations
- **Data Synchronization**: Keep multiple DHIS2 instances in sync with transformed data
- **Cross-Instance Reporting**: Aggregate data from multiple sources into a central reporting instance
- **Data Element Harmonization**: Map data elements with different IDs but same meaning across systems

---

## 📋 Feature Requirements

### Core Functionality
- [x] Extract data values from source DHIS2 instance by dataset
- [x] Apply data element and category option combo mappings
- [x] Validate organization units exist in both systems
- [x] Write mapped data values to target DHIS2 instance
- [x] Handle unmapped data elements gracefully (log warnings)
- [x] Prevent duplicate data value creation

### Parameters
- [x] Source DHIS2 connection
- [x] Target DHIS2 connection  
- [x] Dataset ID to extract
- [x] JSON mapping file for data elements and category option combos
- [x] Period range (start/end dates)
- [x] Dry run mode for testing

---

## 📚 Critical Context & Documentation

### Key Documentation URLs
```yaml
- url: https://docs.dhis2.org/fr/develop/using-the-api/dhis-core-version-242/introduction.html
  why: DHIS2 API reference for dataValueSets, dataElements, categoryOptionCombos endpoints

- url: https://github.com/BLSQ/openhexa-toolbox/tree/main/openhexa/toolbox/dhis2
  why: Functions like extract_dataset, import_data_values, get_data_elements_map, DHIS2 client

- url: https://github.com/BLSQ/openhexa/wiki/Writing-OpenHEXA-pipelines
  why: OpenHEXA @pipeline, @task, @parameter decorator patterns and conventions
```

### Critical DHIS2 API Endpoints
- `/dataValueSets` - For extracting and posting data values
- `/dataElements` - For validating data element mappings
- `/categoryOptionCombos` - For validating COC mappings
- `/organisationUnits` - For validating orgUnit existence

### OpenHEXA Toolbox Key Functions
```python
# Data extraction
from openhexa.toolbox.dhis2.dataframe import extract_dataset
from openhexa.toolbox.dhis2 import DHIS2

# Data transformation
from openhexa.toolbox.dhis2.dataframe import join_object_names, import_data_values
```

---

## 🏗️ Implementation Blueprint

### Task Structure
```yaml
Task 1: validate_connections
  PURPOSE: Verify source and target DHIS2 connections are working
  ACTIONS:
    - Initialize DHIS2 clients for both connections
    - Ping both servers to verify connectivity
    - Log connection status

Task 2: load_and_validate_mappings
  PURPOSE: Load and validate the mapping JSON file
  ACTIONS:
    - Read JSON mapping file from parameters
    - Validate structure: {dataElements: {}, categoryOptionCombos: {}}
    - Check that source IDs exist in source DHIS2
    - Check that target IDs exist in target DHIS2
    - Log mapping validation results

Task 3: extract_source_data
  PURPOSE: Extract data values from source DHIS2 instance
  ACTIONS:
    - Use extract_dataset() to get data values for dataset_id
    - Filter by period range if specified
    - Log extraction statistics (records, org units, data elements)

Task 4: validate_org_units
  PURPOSE: Ensure org units exist in both source and target systems
  ACTIONS:
    - Get unique org units from extracted data
    - Check existence in target DHIS2 instance
    - Filter data to only include matching org units
    - Log org unit validation results

Task 5: transform_data_values
  PURPOSE: Apply data element and COC mappings to extracted data
  ACTIONS:
    - Apply dataElement ID mappings
    - Apply categoryOptionCombo ID mappings
    - Filter out unmapped data elements/COCs
    - Validate transformed data structure
    - Log transformation statistics

Task 6: post_to_target
  PURPOSE: Send transformed data values to target DHIS2 instance
  ACTIONS:
    - Format data for DHIS2 dataValueSets API
    - Use import_data_values() or equivalent to post data
    - Handle API response and errors
    - Log import results (imported, updated, ignored, deleted)

Task 7: generate_summary
  PURPOSE: Create comprehensive pipeline execution summary
  ACTIONS:
    - Generate summary of extraction, transformation, and import
    - Log final statistics and any warnings
    - Export summary to output file
```

---

## 🔧 Critical Implementation Details

### Parameter Definitions
```python
@parameter("source_connection", type=DHIS2Connection, name="Source DHIS2 Connection", required=True)
@parameter("target_connection", type=DHIS2Connection, name="Target DHIS2 Connection", required=True)
@parameter("dataset_id", type=str, widget=DHIS2Widget.DATASETS, connection="source_connection", required=True)
@parameter("mapping_file", type=File, name="Mapping JSON File", required=True)
@parameter("start_date", type=str, name="Start Date (YYYY-MM-DD)", required=True)
@parameter("end_date", type=str, name="End Date (YYYY-MM-DD)", required=True)
@parameter("dry_run", type=bool, name="Dry Run Mode", default=False, required=False)
```

### Mapping File Structure
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

### Data Value Payload Structure
```python
# Required fields for DHIS2 dataValueSets API
{
  "dataElement": "mapped_data_element_id",
  "orgUnit": "org_unit_uid",  # Must exist in both systems
  "period": "period_string",
  "categoryOptionCombo": "mapped_coc_id", 
  "value": "data_value"
}
```

---

## 🔍 Code Examples from Existing Pipelines

### DHIS2 Client Initialization Pattern
```python
def get_dhis2_client(connection: DHIS2Connection) -> DHIS2:
    cache_dir = Path(workspace.files_path) / ".cache"
    dhis2 = DHIS2(connection=connection, cache_dir=cache_dir)
    return dhis2
```

### Data Extraction Pattern
```python
# From dhis2_extract_dataset/pipeline.py
data_values = extract_dataset(
    dhis2=dhis2,
    dataset=dataset_id,
    start_date=start_date,
    end_date=end_date,
    org_units=None,  # Will get all org units
    org_unit_groups=None,
    include_children=False,
)
```

### Data Posting Pattern
```python
# From era5_import_dhis2/pipeline.py
summary = dhis2.data_value_sets.post(
    data_values=payload,
    import_strategy="CREATE_AND_UPDATE",
    dry_run=dry_run,
    skip_validation=False,
)
```

### Error Handling Pattern
```python
def check_server_health(dhis2: DHIS2):
    try:
        dhis2.ping()
        current_run.log_info(f"Successfully connected to DHIS2 at {dhis2.api.url}")
    except ConnectionError:
        current_run.log_error(f"Unable to reach DHIS2 instance at {dhis2.api.url}")
        raise
```

---

## ⚠️ Critical Gotchas & Best Practices

### DHIS2 API Gotchas
```python
# ❌ Common mistakes to avoid:
# 1. Missing required fields in data value payload
# 2. Using non-existent org unit UIDs
# 3. Incorrect period format
# 4. Unmapped data elements causing API errors
# 5. Duplicate data values in single payload

# ✅ Best practices:
# 1. Always validate org units exist in both systems
# 2. Use CREATE_AND_UPDATE import strategy
# 3. Implement dry run mode for testing
# 4. Log detailed statistics at each step
# 5. Handle unmapped elements gracefully
```

### Data Validation Requirements
```python
# Validate payload structure before posting
required_fields = ["dataElement", "orgUnit", "period", "categoryOptionCombo", "value"]
for data_value in payload:
    missing_fields = [field for field in required_fields if field not in data_value]
    if missing_fields:
        current_run.log_error(f"Missing required fields: {missing_fields}")
        raise ValueError(f"Invalid data value structure")
```

### Workspace Configuration
```yaml
# workspace.yaml must include both connections
connections:
  dhis2-source:
    type: dhis2
    url: https://source.dhis2.org
    username: admin
    password: password
  dhis2-target:
    type: dhis2
    url: https://target.dhis2.org
    username: admin
    password: password
```

---

## 🧪 Validation Gates

### Syntax & Style Validation
```bash
# Must pass before deployment
ruff check dhis2_to_dhis2_data_elements/pipeline.py --fix
mypy dhis2_to_dhis2_data_elements/pipeline.py
```

### Unit Tests
```python
# Test mapping validation
def test_mapping_file_structure():
    mapping = {"dataElements": {"src1": "tgt1"}, "categoryOptionCombos": {"src2": "tgt2"}}
    assert validate_mapping_structure(mapping) == True

# Test data transformation
def test_data_value_transformation():
    source_data = {"dataElement": "src1", "categoryOptionCombo": "src2", "value": "10"}
    mapping = {"dataElements": {"src1": "tgt1"}, "categoryOptionCombos": {"src2": "tgt2"}}
    result = transform_data_value(source_data, mapping)
    assert result["dataElement"] == "tgt1"
    assert result["categoryOptionCombo"] == "tgt2"
```

### Integration Tests
```bash
# Test with real DHIS2 connections (dry run)
openhexa pipeline run dhis2_to_dhis2_data_elements \
  --source_connection=dhis2-demo-2-41 \
  --target_connection=dhis2-demo-2-39 \
  --dataset_id=BfMAe6Itzgt \
  --mapping_file=test_mapping.json \
  --start_date=2024-01-01 \
  --end_date=2024-01-31 \
  --dry_run=true
```

---

## 📁 Directory Structure

```
dhis2_to_dhis2_data_elements/
├── pipeline.py              # Main pipeline implementation
├── README.md               # Documentation and usage examples
├── requirements.txt        # Dependencies
├── tests/
│   ├── test_pipeline.py    # Unit tests
│   └── test_mapping.json   # Test mapping file
└── workspace.yaml          # Local workspace configuration
```

---

## 🎯 Success Criteria Checklist

- [ ] Pipeline uses only `@pipeline`, `@task`, and `@parameter` decorators
- [ ] Supports both source and target DHIS2 connections
- [ ] Validates mapping file structure and content
- [ ] Handles org unit validation across both systems
- [ ] Applies data element and COC mappings correctly
- [ ] Implements dry run mode for testing
- [ ] Provides detailed logging at each step
- [ ] Handles errors gracefully with meaningful messages
- [ ] Exports execution summary to output file
- [ ] Passes all linting and type checking
- [ ] Includes comprehensive README with examples

---

## 🎯 Confidence Score

**Score: 9/10**

**Reasoning:**
- All required DHIS2 toolbox functions are well-documented and tested
- Existing pipeline patterns provide clear implementation guidance
- Data extraction, transformation, and posting patterns are established
- Error handling and validation patterns are proven
- Only minor risk is around edge cases in mapping validation and org unit filtering

**Potential Issues:**
- Complex categoryOptionCombo mappings might need additional validation
- Large datasets might require batch processing optimization
- Period format validation may need enhancement for different DHIS2 versions

This PRP provides comprehensive context for one-pass implementation of a robust DHIS2 to DHIS2 data transfer pipeline.