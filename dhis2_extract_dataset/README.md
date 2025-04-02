
# DHIS2 Dataset Extraction Pipeline

This repository contains an OpenHexa ETL pipeline designed to extract datasets from a DHIS2 instance. The pipeline allows for filtering, enriching, and storing extracted data based on user-defined parameters.

## Pipeline Name

`dhis2_extract_dataset`

## Description

This pipeline extracts data from specified datasets in a DHIS2 instance. The user can define the desired datasets, time range, and optional filtering by data element IDs. Additionally, the pipeline supports enrichment of the extracted data by adding names for data elements, category option combos, and parent organisation units.

## Parameters

- **DHIS2 Connection (`dhis_con`)**: Required. The DHIS2 connection to use for data extraction. Defaults to `dhis2-demo-2-41`.
- **Data Element IDs (`data_element_ids`)**: Optional. A list of specific data element IDs to filter the extraction. If not specified, all elements in the datasets are extracted. Defaults to `["FvKdfA2SuWI", "p1MDHOT6ENy"]`.
- **Start Date (`start`)**: Required. The start date of the extraction range in ISO format (yyyy-mm-dd). Defaults to `2024-01-01`.
- **End Date (`end`)**: Optional. The end date of the extraction range in ISO format (yyyy-mm-dd). Defaults to `None` (extracts up to the latest available date).
- **OpenHexa Dataset (`openhexa_dataset`)**: Optional. If specified, saves the extracted table to an OpenHexa dataset.
- **Save by Month (`save_by_month`)**: Required. Whether to store datasets by period in the folder spaces. Defaults to `True`.
- **Dataset IDs (`datasets_ids`)**: Required. A list of dataset IDs to extract. Defaults to `["TuL8IOPzpHh"]`.
- **Add Data Element Names (`add_dx_name`)**: Optional. Whether to enrich the dataset with data element names. Defaults to `False`.
- **Add Category Option Combo Names (`add_coc_name`)**: Optional. Whether to enrich the dataset with category option combo names. Defaults to `False`.
- **Add Organisation Unit Parent Names (`add_org_unit_parent`)**: Optional. Whether to enrich the dataset with parent organisation unit names. Defaults to `False`.

## Output Files

The pipeline generates CSV files containing the extracted data. The files are saved to the workspace path under the folder corresponding to the `dhis2_name`.

Example output structure:
```
workspace/
└── dhis2_demo_2_41/
    ├── dataset_extraction.csv
    ├── Dataset1/
    │   ├── 202401.csv
    │   └── 202402.csv
    └── Dataset2/
        ├── 202401.csv
        └── 202402.csv
```

## Data Enrichment

The pipeline supports optional enrichment of extracted data by adding names of data elements, category option combos, and parent organisation units.

## Error Handling

Warnings and errors are logged using `current_run.log_warning` and `current_run.log_error`. This includes:
- Mixed levels of organisation units.
- Mixed frequencies of datasets.
- Missing data elements.
- Empty datasets.

## Dependencies

- `openhexa`
- `pandas`
- `os`

## License

This pipeline is licensed under the MIT License.
