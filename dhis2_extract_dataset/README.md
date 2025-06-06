# DHIS2 Dataset Extraction Pipeline

This pipeline extracts datasets from DHIS2 instances via the OpenHexa platform, converting them into structured, validated, and versioned outputs for further analysis or integration into OpenHexa datasets.

---

## 🚀 Overview

This pipeline automates the extraction of DHIS2 datasets by:
- Connecting to a DHIS2 instance via `DHIS2Connection`
- Pulling datasets based on user-specified criteria (dataset ID, orgunits, periods)
- Validating inputs and data completeness
- Formatting and enriching the data with object metadata
- Writing outputs in CSV, Parquet, and SQL formats
- Registering them in an OpenHexa dataset with versioning

---

## 🛠️ Parameters

| Parameter            | Type            | Description |
|----------------------|------------------|-------------|
| `dhis_con`           | DHIS2Connection | Connection to DHIS2 instance |
| `dataset_id`         | str             | DHIS2 dataset ID to extract |
| `start`, `end`       | str (ISO date)  | Date range for extraction |
| `ou_ids`             | list[str]       | Specific OrgUnit IDs |
| `ou_group_ids`       | list[str]       | OrgUnit Group IDs |
| `include_children`   | bool            | Include children of selected OrgUnits |
| `dataset`            | Dataset         | Optional OpenHexa dataset to store the result |
| `extract_name`       | str             | Optional label for dataset version |

---

## 📦 Features

- **Multi-level OrgUnit Selection**: Supports orgunits and orgunit groups with validation and warning logging.
- **Flexible Time Ranges**: Accepts various DHIS2 period types (weekly, monthly, yearly, etc.).
- **Comprehensive Metadata**: Enriches data with element, category, and orgunit names.
- **Validation Checks**:
  - Periods with no data
  - Missing data elements
  - Mismatched orgunits
- **Output Options**:
  - CSV and Parquet export
  - Versioned OpenHexa dataset
  - Direct SQL injection into workspace DB
  - Per-data-element Parquet export

---

## 📂 Output Structure

```
workspace/
└── files/
    └── pipelines/
        └── dhis2_extract_dataset/
            └── {instance_subdomain}/
                └── {dataset_name}/
                    └── {timestamp_or_extract_name}.parquet
                    └── {timestamp_or_extract_name}.csv
                    └── {data_element_name}.parquet
```

---

## ⚙️ Internal Workflow

1. **Connection & Metadata Fetch**: Fetch datasets, data elements, orgunits, categories.
2. **Input Validation**: Check orgunit inputs for ambiguity/conflict.
3. **Data Extraction**: Extract raw values via `extract_dataset()` and enrich with metadata.
4. **Validation**: Identify missing periods/data elements.
5. **Persistence**: Save to disk, register in OpenHexa, and write to DB.

---

## 😓 Logic Highlights

- **Period Conversion**: `isodate_to_period_type()` aligns ISO dates with DHIS2-specific period formats, including weekly anchors.
- **Validation Layer**:
  - `check_parameters_validation()` ensures only one orgunit selector is used.
  - `warning_post_extraction()` logs missing periods and elements.
- **Naming Convention**: Output folders and filenames are auto-generated using connection domain and dataset metadata.

---

## 🔐 Environment Variables

| Variable | Description |
|----------|-------------|
| `WORKSPACE_DATABASE_URL` | SQLAlchemy-compatible DB URL used for direct data injection |

---

## 📑 Example Use

```python
run = dhis2_extract_dataset(
    dhis_con="dhis2-demo-2-41",
    dataset_id="Nyh6laLdBEJ",
    start="2024-01-01",
    end="2024-03-31",
    ou_group_ids=["GGghZsfu7qV"],
    dataset=my_dataset,
    extract_name="Q1-Extraction"
)
```

## 📝 Notes

- Weekly period detection aligns to the DHIS2 anchor day (e.g., Monday).
- The pipeline is robust to missing data and supports multiple levels of period types.
- Designed to handle both small and large dataset extractions.

## Dependencies

- `openhexa`
- `pandas`
- `os`

## License

This pipeline is licensed under the MIT License.
