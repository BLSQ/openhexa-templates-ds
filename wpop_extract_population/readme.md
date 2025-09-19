# WorldPop Extract Population Pipeline

This pipeline (`wpop_extract_population`) extracts and aggregates population data from **WorldPop** raster files (.tif) into spatial units defined by a given shapes file.  
It is intended to support population analysis for health and development projects.

---

## Parameters

| Parameter      | Type  | Required | Description |
|----------------|-------|----------|-------------|
| `country_iso3` | str   | ✅       | 3-letter ISO code of the country (e.g., `COD`, `BFA`). Determines which WorldPop raster to download. |
| `un_adj`       | bool  | ✅       | If `true`, downloads **UN-adjusted** WorldPop grid data. If `false`, uses the original WorldPop data. |
| `shapes_path`  | File  | ✅       | Path to the shape file that defines the spatial aggregation units. Supported formats: `.geojson`, `.shp`, `.gpkg` (see [geopandas.read_file](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_file.html)). |
| `dst_dir`      | str   | ✅       | Output directory inside the OH workspace. Parent directory will be created if missing. |
| `dst_table`    | str   | ✅       | Name of the output table in the database. If provided, the aggregated results will be stored in the database. |

---

## Outputs

1. **Raw Data**
   - WorldPop `.tif` raster file is automatically downloaded to:
     ```
     wpop_extract_population/data/raw/
     ```
   - File name depends on the country ISO code and year.

2. **Aggregated Data**
   - A CSV/Parquet file saved to the specified `dst_dir`.
   - If `dst_dir` is **not provided**, the files are saved by default under:
     ```
     pipelines/wpop_extract_population/data/aggregated/
     ```
   - If `dst_table` is provided, results are also written to a database table.

---

## Flow Diagram

```mermaid
flowchart TD
    A[Start Pipeline] --> B[Download WorldPop raster .tif]
    B --> C{un_adj ?}
    C -- Yes --> B1[Download UN-adjusted raster]
    C -- No --> B2[Download non-adjusted raster]
    B1 --> D[Load shapes file]
    B2 --> D[Load shapes file]
    D --> E[Aggregate population by shapes]
    E --> F[Save results to dst_dir]
    F --> G{dst_table provided?}
    G -- Yes --> H[Write results to DB table]
    G -- No --> I[Skip DB storage]
    H --> J[End]
    I --> J[End]
