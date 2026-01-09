# Raster alignment and cropping pipeline


This pipeline aligns, reprojects, and crops multiple raster files `.tif` to a common spatial reference.
All rasters are:
 - Reprojected to a target coordinate reference system (CRS)
 - Resampled to match the lowest-resolution raster
 - Spatially aligned (same extent, resolution, and grid)
 - Cropped to a user-defined area of interest

Resampling methods are automatically selected based on raster type (e.g. land cover, elevation).


## Input parameters

### Area of interest 

*Boundaries input file path*: (mandatory) path to a geospatial file stored in OpenHexa. It wil be automatically reprojected to the output CRS before cropping.  
🚧 Note: the input format may evolve in future versions of the pipeline.

### Input rasters directory

*Input rasters directory*: (mandatory) path to the directory containing the raster files to be processed. All .tif files found in this directory will be included in the pipeline.  

### Target projection

*Target projection*: (optional) target CRS for output rasters (e.g., "EPSG:32734"). The default value is EPSG:4326.


## Output files

Aligned rasters are written to a timestamped output directory. Each output file keeps its original name with the suffix _aligned. 

```
pipelines/
└── accessmod/
    └── align_data/
        └── YYYY-MM-DD_HH-MM-SS/
            └── xxxx.tif
```


## Requirements and dependencies 

The pipeline relies on the following tools and libraries:

- Python packages:
  - `geopandas`
- System dependency:
  - **GDAL** (must provide `gdalwarp`)

The pipeline assumes GDAL is available in the execution environment.