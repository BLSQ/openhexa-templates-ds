import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

import boto3
import geopandas as gpd
import rasterio
import rasterio.merge

from botocore import UNSIGNED
from botocore.config import Config
from openhexa.sdk import current_run, parameter, pipeline, workspace
from osgeo import gdal
from shapely.geometry import box
import rasterio.mask

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
def generate_elevation_raster(
    boundaries_file: str, 
    output_path: str):
    """Extract, merge and crop elevation data from Copernicus, according the area of interest.
    In addition, the slope is computed, and the final raster is saved a .tif file.
    """
    # Get boundaries
    boundaries = read_boundaries(boundaries_file)

    # Retrieves tiles
    tiles_name = retrieve_tiles(target_geom=boundaries)

    if not tiles_name:
        raise RuntimeError("💥 No Copernicus tile intersects the input geometry.")

    if output_path:
        dst_file = Path(workspace.files_path) / output_path
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_dir = Path(workspace.files_path) / "pipelines" / "accessmod"
        dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst_file = dst_dir / "elevation.tif"

    with tempfile.TemporaryDirectory(prefix="accessmod_") as tmpdirname:

        tmpdirname = tmpdirname + "/"

        current_run.log_info(f"Temporary directory created at: {tmpdirname}")

        # Download data 
        tiles = download_tiles(name_list=tiles_name, output_path=tmpdirname)

        # Check presence of files in tempory folder
        if not os.listdir(tmpdirname):
            raise FileNotFoundError(f"💥 No elevation tile found at {tmpdirname}")

        # Merge tiles 
        current_run.log_info("⚙️ Tiles merging in progress.")
        mosaic = merge_tiles(tiles=tiles, output_file=tmpdirname + "elevation_tiles_merged.tif")

        if not Path(mosaic).exists():
            raise RuntimeError("💥 Mosaic generation failed")
        current_run.log_info("✅ Tiles merged successfully.")

        # Compute slope
        current_run.log_info("⚙️ Slope computation in progress")
        slope = compute_slope(input_file=mosaic, output_file=tmpdirname + "elevation_slope.tif")
        current_run.log_info("✅ Slope computed successfully.")

        # Crop with buffer
        current_run.log_info("⚙️ Crop slope raster with buffer in progress")
        cropped_raster = crop_with_buffer(boundaries, input_raster=slope, output_raster=str(dst_file))
        if not dst_file.exists():
            raise RuntimeError("💥 Final raster was not created.")
        current_run.log_info(f"✅ Crop done successfully and final raster saved at {cropped_raster} !")
        current_run.log_info(f"Final raster size: {dst_file.stat().st_size / 1e6:.2f} MB")
        
        current_run.add_file_output(cropped_raster)


#########################
def read_boundaries(file_path: str) -> gpd.GeoDataFrame:
    
    if not os.path.exists(file_path):
        msg = f"💥 File {file_path} not found in Files"
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
def merge_tiles(tiles: List[str], output_file: str) -> str:

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

    return output_file


#########################
def compute_slope(input_file: str, output_file: str) -> str:

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


def crop_with_buffer(geom: gpd.GeoDataFrame, input_raster: str, output_raster: str) -> str:

    # Add a buffer to the geometry of interest (in degrees)
    # 🚧 To avoid too large buffers in longitude at equator: 
    # maybe project into 3857, compute the buffer in meters, and reproject back into 4326 
    geom_buffured = geom.to_crs("EPSG:4326").geometry.buffer(0.2)

    # Crop the input raster
    with rasterio.open(input_raster) as src:
        out_image, out_transform = rasterio.mask.mask(src, geom_buffured.geometry, crop=True)
        profile = src.profile.copy()

    profile.update({
        "height": out_image.shape[1],
        "width": out_image.shape[2],
        "transform": out_transform,
        "count": 1
    })
    
    with rasterio.open(output_raster, "w", **profile) as dst:
        dst.write(out_image[0], 1)

    return output_raster


if __name__ == "__main__":
    generate_elevation_raster()
