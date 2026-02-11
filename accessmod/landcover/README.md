# Lancover pipeline


This pipeline retrieves ESA WorldCover landcover data and produces a cropped raster based on a user-defined geometry of interest.<br>
It is part of the data extraction stage of a larger accessibility pipeline (for now called accessmod).


## Data source

- [Official](https://esa-worldcover.org/en/data-access) product page
- Product [User Manual](https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/docs/WorldCover_PUM_V2.0.pdf) (v2.0)
- [Public S3 bucket](https://registry.opendata.aws/esa-worldcover-vito/) description 



### Dataset characteristics

- **Version**: WorldCover v200 (2021)
- **Spatial resolution**: 10 m × 10 m
- **Spatial** tiling: global tiles of 3° × 3°
- **Coordinate reference system**: EPSG:4326
- **Data access**: public S3 bucket (no authentication required)

## Input parameters

### Geometry of interest 

*Boundaries input file path*: (mandatory) path to a geospatial file stored in OpenHexa. Supported formats: .gpkg, .parquet, .geojson or .shp. (requires associated .shx, .dbf, .cpg, .dbf, .prj files). The geometry is automatically reprojected to EPSG:4326 if needed.

🚧 Note: the input format may evolve in future versions of the pipeline.

### Output directory

*Output directory path*: (optional) path of the ouput directory. If not defined, the output files will be stored in a folder named based on the date of execution. 

```
└── landcover/
    └── YYYY-MM-DD_HH-MM-SS/
        └── landcover.tif
        └── buffered_geom.tif
```


## Output file

The output file is a single-band GeoTIFF raster, with the same spec as ESA data (resolution: 10m and CRS: EPSG4326), where pixel values correspond to one of the 11 classes:

| Value | Class description |
|------:|-------------------|
| 10 | Tree cover |
| 20 | Shrubland |
| 30 | Grassland |
| 40 | Cropland |
| 50 | Built-up |
| 60 | Bare / sparse vegetation |
| 70 | Snow and ice |
| 80 | Permanent water bodies |
| 90 | Herbaceous wetland |
| 95 | Mangroves |
| 100 | Moss and lichen |


## Requirements and dependencies 

The pipeline relies on the following tools and libraries:

- Python packages:
  - `geopandas`
  - `boto3`
  - `shapely`
- System dependency:
  - **GDAL** (must provide `gdalwarp`)

The pipeline assumes GDAL is available in the execution environment.