import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

import boto3
import geopandas as gpd
import numpy as np
import rasterio
import rasterio.merge
from botocore import UNSIGNED
from botocore.config import Config
from openhexa.sdk import current_run, parameter, pipeline, workspace
from osgeo import gdal
from rasterio.features import geometry_mask
from rasterio.windows import Window, subdivide
from shapely.geometry import box

RASTERIO_DEFAULT_PROFILE = {
    "driver": "GTiff",
    "tiled": True,
    "blockxsize": 256,
    "blockysize": 256,
    "compress": "zstd",
    "predictor": 2,
    "num_threads": "all_cpus",
}

GDAL_CREATION_OPTIONS = [
    "TILED=TRUE",
    "BLOCKXSIZE=256",
    "BLOCKYSIZE=256",
    "COMPRESS=ZSTD",
    "PREDICTOR=2",
    "NUM_THREADS=ALL_CPUS",
]


@pipeline("elevation")
@parameter(
    "boundaries_file",
    name="Boundaries input file path",   # To update later
    help="Input fileof geometry of interest (should be located in Files).",
    type=str,
    required=True,
    multiple=False
)
@parameter(
    "output_path",
    name="Output file path",
    help="Output raster path in the workspace (parent directory will automatically be created).",
    type=str,
    required=False,
    multiple=False
)
def generate_elevation_raster(boundaries_file: str, output_path: str):

    """
    Extract, merge and crop elevation data from Copernicus, according the area of interest.
    In addition, the slope is computed, and the final raster is saved a .tif file.
    """

    # Load boundary
    boundaries = read_boundaries(boundaries_file)

    # Retrieves tiles
    tiles_name = retrieve_tiles(target_geom=boundaries)

    if not tiles_name:
        raise RuntimeError("💥 No Copernicus tile intersects the input geometry.")

    # Prepare output fie path
    if output_path:
        dst_file = Path(workspace.files_path) / output_path
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_dir = Path(workspace.files_path) / "pipelines" / "accessmod"
        dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst_file = dst_dir / "elevation.tif"

    current_run.log_info(f"Output raster path defined: {dst_file}")

    with tempfile.TemporaryDirectory(prefix="accessmod_") as tmpdirname:

        tmpdir = Path(tmpdirname)
        current_run.log_info(f"Temporary directory created at: {tmpdirname}")

        # Download data 
        current_run.log_info(f"Starting download of {len(tiles_name)} tiles to temporary folder {tmpdir}...")
        tiles = download_tiles(name_list=tiles_name, 
                               output_path=tmpdir)

        if not tiles:
            raise FileNotFoundError(f"💥 No tile found at {tmpdir}")

        # Merge tiles 
        current_run.log_info("Merging tiles into mosaic...")
        mosaic, mosaic_meta = merge_tiles(tiles=tiles, 
                                          output_file=tmpdirname + "elevation_tiles_merged.tif")

        if not Path(mosaic).exists():
            raise RuntimeError("💥 Mosaic generation failed")

        # Compute slope
        current_run.log_info("Calculating slope...")
        slope = compute_slope(input_file=mosaic, 
                              output_file=tmpdirname + "elevation_slope.tif")
        
        if not Path(slope).exists():
            raise RuntimeError("💥 Slope calculations failed")
        
        # Crop with buffer
        current_run.log_info("Cropping raster to boundaries with buffer...")
        cropped_raster = crop_with_buffer(geom=boundaries, 
                                          input_raster=slope, 
                                          input_meta=mosaic_meta, 
                                          output_raster=str(dst_file))
        
        if not dst_file.exists():
            raise RuntimeError("💥 Final raster was not created.")
        current_run.log_info(f"Final raster saved at: {dst_file} ({dst_file.stat().st_size / 1e6:.2f} MB)")

        current_run.log_info("🎉 Extraction of elevation data finished successfully!")
        current_run.add_file_output(cropped_raster)


#########################
def read_boundaries(file_path: str) -> gpd.GeoDataFrame:

    """Load boundary from a supported file format."""
    
    path = Path(file_path)
    if not path.exists():
        msg = f"File {file_path} not found in Files"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    suffixes = (".gpkg", ".parquet", ".geojson", ".shp")
    if not str(file_path).endswith(suffixes):
        raise ValueError("💥 File not in a correct format. Import it as .gpkg, .parquet, .geojson or .shp.")

    if str(file_path).endswith(".parquet"):
        return gpd.read_parquet(file_path)

    return gpd.read_file(file_path)


#########################
def create_global_grid_name(latitude_spacing: int, longitude_spacing: int, pattern: str) -> gpd.GeoDataFrame:

    """Create a global grid with tiles named according to a pattern."""
    
    tiles = []
    for lat in range(-90, 90, latitude_spacing):
        for lon in range(-180, 180, longitude_spacing):
            geom = box(lon, lat, lon + longitude_spacing, lat + latitude_spacing)
    
            NS = "N" if lat >= 0 else "S"
            EW = "E" if lon >= 0 else "W"
    
            # Tile name 
            name = pattern.format(meridional=NS, lat=abs(lat), zonal=EW, long=abs(lon))
            tiles.append({"geometry": geom, "name": name})

    return gpd.GeoDataFrame(tiles, crs="EPSG:4326")


#########################
def retrieve_tiles(target_geom: gpd.GeoDataFrame) -> List[str]:

    """Return a list of tile names that intersect the target geometry."""
    
    if target_geom.crs != "EPSG:4326":
        target_geom = target_geom.to_crs("EPSG:4326")

    # Create a global grid    
    pattern_name = "Copernicus_DSM_COG_10_{meridional}{lat:02d}_00_{zonal}{long:03d}_00_DEM"
    grid = create_global_grid_name(1, 1, pattern_name)

    # Select tiles intersecting the polygon
    selected_tiles = grid[grid.intersects(target_geom.union_all())] 

    return selected_tiles["name"].tolist()


#########################
def download_tiles(name_list: List[str], output_path: str):

    """Download tiles from ESA S3 bucket into the specified directory."""

    s3 = boto3.client("s3", region_name="eu-central-1", config=Config(signature_version=UNSIGNED))

    downloaded_files = []

    for name in name_list:
        output_file = Path(output_path) / f"{name}.tif"

        if output_file.exists():
            current_run.log_info(f"Tile already exists: {output_file}")
            downloaded_files.append(str(output_file))
            continue

        try:
            s3.download_file(
                "copernicus-dem-30m",
                f"{name}/{name}.tif",
                str(output_file)
            )
            downloaded_files.append(str(output_file))
        except Exception as e:
            current_run.log_error(f"💥 Failed to download {name}: {e}")
            raise

    return downloaded_files


#########################
def merge_tiles(tiles: List[str], output_file: str) -> (str, str):

    """Merge single-band raster tiles into a single mosaic raster."""

    with rasterio.open(tiles[0]) as src:
        meta = src.meta.copy()
        nodata = src.nodata

    mosaic, dst_transform = rasterio.merge.merge(tiles, nodata=nodata)
    meta.update({**RASTERIO_DEFAULT_PROFILE,
                 "transform": dst_transform,
                 "height": mosaic.shape[1],
                 "width": mosaic.shape[2],
                 "count": 1,               # single band
                 "nodata": nodata,
                 }) 

    with rasterio.open(output_file, "w", **meta) as dst:
        dst.write(mosaic[0], 1)

    current_run.log_info(
        f"Merged {len(tiles)} tiles into mosaic {output_file} "
        f"({meta['width']} x {meta['height']})"
    )

    return output_file, meta


#########################
def compute_slope(input_file: str, output_file: str) -> str:

    """Compute slope with GDAL"""

    src_ds = gdal.Open(input_file)
    if src_ds is None:
        raise RuntimeError(f"💥 Unable to open {input_file}")

    scale = None
    if not src_ds.GetSpatialRef().IsProjected():
        # because source ref system is EPSG:4326 + slope computed in meters 
        scale = 111120   
        # If it was in feet: scale = 370400

    options = gdal.DEMProcessingOptions(
        format="GTiff",
        scale=scale,    # ratio of vertical units to horizontal
        slopeFormat="degree",
        creationOptions=GDAL_CREATION_OPTIONS,
    )
    gdal.DEMProcessing(output_file, input_file, "slope", options=options)

    if not Path(output_file).exists():
        raise RuntimeError("💥 Slope computation failed.")

    src_ds = None  # Close dataset
    
    return output_file


def crop_with_buffer(geom: gpd.GeoDataFrame, input_raster: str, input_meta: str, output_raster: str) -> str:

    """Crop a raster with a buffer around the geometry and save to output path."""

    # Add a buffer to the geometry of interest (in degrees)
    geom_buffured = geom.to_crs("EPSG:4326").geometry.buffer(0.2)

    # Windows generation
    input_window = Window(0, 0, input_meta["width"], input_meta["height"])
    sub_windows = subdivide(input_window, input_meta["width"] / 8, input_meta["width"] / 8)

    # Crop the input raster
    with rasterio.open(input_raster) as src:

        nodata = src.nodata

        with rasterio.open(output_raster, "w", **input_meta) as dst:

            for window in sub_windows:

                data = src.read(1, window=window)
                window_transform = src.window_transform(window)
                mask = geometry_mask(geometries=geom_buffured.geometry, 
                                     out_shape=data.shape, 
                                     transform=window_transform, 
                                     invert=True)  # inside geom
                out_image = np.where(mask, data, nodata)

                dst.write(out_image, 1, window=window)

    return output_raster


#########################
if __name__ == "__main__":
    generate_elevation_raster()
