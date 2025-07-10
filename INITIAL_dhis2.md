## FEATURE:

Build an OpenHEXA pipeline that **extracts data values from a source DHIS2 instance** for a given dataset and **writes the values to a target DHIS2 instance**, using mappings for `dataElement` and `categoryOptionCombo` IDs.  

- Only `orgUnits` with identical UIDs in both instances should be considered.
- The pipeline must be parameterized with:
  - A `dataset_id` to extract from
  - A `json mapping file` containing:
    - `{source_data_element_id: target_data_element_id}`
    - `{source_category_option_combo_id: target_category_option_combo_id}`
- The pipeline must use the OpenHEXA `@pipeline`, `@task`, and `@parameter` decorators and rely on the `openhexa-toolbox.dhis2` library.

## EXAMPLES:

Located in the current folder (openhexa-templates-ds) folder:

```
**/
   ├── pipeline.py
   ├── requirements.txt
   └── README.md
```

These examples showcase:
- How to structure a DHIS2 pipeline using `@pipeline`, `@task`, `@parameter`
- Connection usage via `workspace.yaml` with DHIS2 connector
- Reading from one DHIS2 source and optionally pushing data to a second system

Use these patterns and naming conventions as a base for the new pipeline.

## DOCUMENTATION:

```yaml
- url: https://docs.dhis2.org/fr/develop/using-the-api/dhis-core-version-242/introduction.html
  why: DHIS2 API reference for querying dataValues, orgUnits, datasets, categoryOptionCombos, etc.

- url: https://github.com/BLSQ/openhexa-toolbox/tree/main/openhexa/toolbox/dhis2
  why: Functions like `get_dataset_values`, `post_data_values`, `get_data_elements_map`

- url: https://github.com/BLSQ/openhexa/wiki/Writing-OpenHEXA-pipelines
  why: Explains usage of `@pipeline`, `@task`, and `@parameter` decorators and the pipeline structure

- docfile: pipelines/sample_pipeline/pipeline.py
  why: Reference pattern for building a working OpenHEXA DHIS2 pipeline
```

## OTHER CONSIDERATIONS:

```python
# DHIS2 POST payload must include: dataElement, orgUnit, period, categoryOptionCombo, value
# All orgUnits are assumed to exist in both systems (same UID)
# Only mapped dataElements and COCs should be sent
# Use @pipeline, @task, and @parameter decorators (no other custom decorators)
# Use the dhis2 connections defined in workspace.yaml:
#   - dhis2-demo-2-41 (source)
#   - dhis2-demo-2-39 (target)
# Use the openhexa-toolbox.dhis2 for all DHIS2 interactions (avoid custom requests unless needed)
# Mapping file must be validated before transformation (e.g. warn or fail on unmapped DEs/COCs)
# Avoid duplicate values and respect period/orgUnit constraints when posting data
# Use clear logging for extraction, transformation, and push steps
# 
```