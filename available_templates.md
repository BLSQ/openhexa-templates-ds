# OpenHEXA Pipeline Templates Overview

This repository contains reusable pipeline templates for common data workflows. Use the tables below to identify which template best fits your use case. Each table is grouped by data source or domain (DHIS2, ERA5, IASO, Other).

---

## DHIS2 Pipelines

| **Template Name**                | **Description**                                                                                   | **Comments / Particularities**                                                                             |
|:----------------------------------|:-------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| **dhis2_extract_analytics**       | Extracts analytics data (for data elements, data element groups, indicators, indicator groups), enriching them with metadata. |                                                                                                            |
| **dhis2_extract_dataset**         | Extracts data from a DHIS2 dataset, enriches with metadata, outputs Parquet/CSV/DB/Dataset.      |                                                                                                            |
| **dhis2_extract_data_elements**   | Extracts data values for specified data elements or data element groups.                          |                                                                                                            |
| **dhis2_extract_events**          | Extracts event values for a DHIS2 program.                                                       |                                                                                                            |
| **dhis2_metadata_extract**        | Extracts information about DHIS2 metadata. It can extract data for: organisation units, organisation unit groups, datasets, data elements, data element groups, and category option combos. |                                                                                                            |
| **dhis2_shapes_extract**          | Extracts org unit geometries from DHIS2.                                                         | The organisation unit level to extract geometries from can be specified.                                   |
                          
---

## ERA5 Pipelines

| **Template Name**         | **Description**                                                                                   | **Comments / Particularities**                                                                             |
|:--------------------------|:-------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| **era5_extract**          | Downloads ERA5 hourly data from the CDS (Copernicus Climate Data Store) into the OpenHexa workspace. It can download 2m_temperature, total_precipitation, and volumetric_soil_water_layer_1 data.         | Uses a boundaries file from the OpenHexa workspace. |
| **era5_aggregate**        | Aggregates raw ERA5 data spatially (using a boundaries file) and temporally (producing daily, weekly, monthly, and epi-weekly outputs). | Requires both a boundaries file and organized input ERA5 data files. |
| **era5_import_dhis2**     | Imports ERA5 aggregated climate statistics into a DHIS2 instance.                          | Should be used after era5_extract and era5_aggregate pipelines, which generate its aggregated climate statistics. It can either overwrite or append the data already present in DHIS2. The dataset, data elements, and CoC from DHIS2 must already exist and be provided in the pipeline parameters. |

| **Template Name**         | **Description**                                                                                   | **Comments / Particularities**                                                                             |
|:--------------------------|:-------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| **era5_sync**             | Extracts ERA5-Land climate data from the Copernicus CDS; aggregates the data spatially (using a boundaries file) and temporally (producing daily, weekly, monthly, and epi-weekly outputs); and calculates derived variables (relative humidity, wind speed) when possible. | The pipeline is designed to run incrementally, downloading data only for missing dates with each run. It uses a boundaries file from the OpenHexa workspace.|
| **era5_load_dhis2**       | Imports ERA5 aggregated climate statistics into DHIS2 data elements.                          | Should be used after the era5_sync pipeline. Requires a JSON file containing the mappings between the ERA5 variables and the DHIS2 data elements. |


## IASO Pipelines

| **Template Name**                | **Description**                                                                                   | **Comments / Particularities**                                                                             |
|:----------------------------------|:-------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| **iaso_extract_metadata**         | Extracts detailed metadata about IASO forms, including questions and choices, and exports to multiple formats. |                                                                                                            |
| **iaso_extract_orgunits**         | Extracts organizational units from IASO, processes geometry, and exports to various formats.      |                                                                                                            |
| **iaso_extract_submissions**      | Extracts and processes form submissions from IASO, with options for incremental extraction and label conversion. | Converts choice codes to labels; deduplicates columns.|
| **iaso_import_submissions**       | Imports, updates, and deletes IASO form submissions from tabular files into IASO, supporting multiple strategies. | Supports CREATE, UPDATE, CREATE_AND_UPDATE, DELETE; strict validation option; outputs summary and logs. |

---

## Other Pipelines

| **Template Name**           | **Description**                                                                                   | **Comments / Particularities**                                                                             |
|:---------------------------|:--------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| **wpop_extract_population** | Extracts and aggregates WorldPop raster population data by spatial boundaries.                   | Uses a boundaries file from the OpenHexa workspace.                                                        |

---


## Further Documentation

You can find all of the pipelines here: https://github.com/BLSQ/openhexa-templates-ds. Each of them has a README file with detailed documentation about its parameters, expected inputs and outputs, and example runs.

---
