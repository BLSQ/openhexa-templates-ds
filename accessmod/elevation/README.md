# Elevation pipeline


This pipeline retrieves elevation data from the **Copernicus Digital Elevation Model (DEM)** and produces a **cropped elevation-derived raster** based on a user-defined geometry of interest. In addition to extracting elevation data, the pipeline **computes slope** and outputs a single raster file.<br>
It is part of the data extraction stage of a larger accessibility pipeline (for now called accessmod).


## Data source


- DEM API documentation:  
  https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Data/DEM.html
- DEM processing examples:  
  https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Process/Examples/DEM.html
- Public S3 bucket registry:  
  https://registry.opendata.aws/copernicus-dem/
- Dataset structure and processing details:  
  https://copernicus-dem-30m.s3.amazonaws.com/readme.html


### Dataset characteristics

- **Dataset**: Copernicus DEM (GLO-30)
- **Spatial resolution**: ~30 m
- **Spatial tiling**: global tiles of 1° × 1°
- **Coordinate reference system**: EPSG:4326
- **Data access**: public S3 bucket (no authentication required)


## Input parameters

### Geometry of interest 

*Boundaries input file path*: (mandatory) path to a geospatial file stored in OpenHexa. Supported formats: .gpkg, .parquet, .geojson or .shp. (requires associated .shx, .dbf, .cpg, .dbf, .prj files). The geometry is automatically reprojected to EPSG:4326 if needed.

🚧 Note: the input format may evolve in future versions of the pipeline.

### Output file name 

*Output file path*: (optional) path of the ouput raster file. If not defined, the ouput file will be stored in a folder named based on the date of execution, and will be named landcover.tif. 

```
pipelines/
└── accessmod/
    └── landcover/
        └── YYYY-MM-DD_HH-MM-SS/
            └── landcover.tif
```


## Output file

The output file is a single-band GeoTIFF raster, in EPSG:4326, where pixel correspond to a slope value (calculated using gdal.DEMProcessing).


## Requirements and dependencies 

The pipeline relies on the following tools and libraries:

- Python packages:
  - `geopandas`
  - `boto3`
  - `shapely`
- System dependency:
  - **GDAL** (must provide `gdalwarp` and 'gdal.DEMProcessing')

The pipeline assumes GDAL is available in the execution environment.