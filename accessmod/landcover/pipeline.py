import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

import boto3
import geopandas as gpd
import rasterio
import rasterio.mask
import rasterio.merge
from botocore import UNSIGNED
from botocore.config import Config
from openhexa.sdk import current_run, parameter, pipeline, workspace
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


@pipeline("landcover")
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
def generate_landcover_raster(
    boundaries_file: str, 
    output_path: str):
    """Extract, merge and crop landcover data from ESA, according the area of interest, and the final cropped raster is saved as a .tif file."""
    # Get boundaries
    boundaries = read_boundaries(boundaries_file)

    # Retrieves tiles
    tiles_name = retrieve_tiles(target_geom=boundaries)

    # 
    if output_path:
        dst_file = Path(workspace.files_path) / output_path
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_dir = Path(workspace.files_path) / "pipelines" / "accessmod" / "landcover"
        dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dst_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="accessmod_") as tmpdirname:

        tmpdirname = tmpdirname + "/"

        current_run.log_info(f"Temporary directory created at: {tmpdirname}")

        # Download data 
        tiles = download_tiles(name_list=tiles_name, 
                               output_path=tmpdirname)

        # Check presence of files in tempory folder
        if len(os.listdir(tmpdirname)) == 0:
            return FileNotFoundError(f"No landcover tile found at {tmpdirname}")

        # Merge tiles 
        mosaic = merge_tiles(tiles=tiles, 
                             output_file=tmpdirname + "landcover_tiles_merged.tif")

        # Crop with buffer 
        cropped_raster = crop_with_buffer(geom=boundaries, 
                                          input_raster=mosaic, 
                                          output_raster=output_path)

    return


#########################
def read_boundaries(file_path: str) -> gpd.GeoDataFrame:
    
    if not os.path.exists(file_path):
        msg = f"File {file_path} not found in Files"
        # current_run.log_error(msg)
        raise FileNotFoundError(msg)

    suffixes = (".gpkg", ".parquet", ".geojson", ".shp")
    if not file_path.endswith(suffixes):
        raise NameError("File not in a correct format. Import it as .gpkg, .parquet, .geojson or .shp.")

    if file_path.endswith(".parquet"):
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
        target_geom.to_crs("EPSG:4326")

    # Create a global grid (2021 data)
    pattern_name = "ESA_WorldCover_10m_2021_v200_{meridional}{lat:02d}{zonal}{long:03d}_Map.tif"
    grid = create_global_grid_name(3, 3, pattern_name)

    # Select tiles intersecting the polygon
    selected_tiles = grid[grid.intersects(target_geom.union_all())] 

    return selected_tiles["name"].tolist()


#########################
def download_tiles(name_list: List[str], output_path: str):

    s3 = boto3.client("s3", region_name="eu-central-1", config=Config(signature_version=UNSIGNED))

    for name in name_list:
        path = f"v200/2021/map/{name}"
        s3.download_file("esa-worldcover", path, output_path + name)

    return [output_path + file_name for file_name in name_list]


#########################
def merge_tiles(tiles: List[str], output_file: str) -> str:

    with rasterio.open(tiles[0]) as src:
        meta = src.meta.copy()

    mosaic, dst_transform = rasterio.merge.merge(tiles)
    meta.update(RASTERIO_DEFAULT_PROFILE) 
    meta.update(transform=dst_transform, height=mosaic.shape[1], width=mosaic.shape[2])

    with rasterio.open(output_file, "w", **meta) as dst:
        dst.write(mosaic)

    current_run.log_info(f"Merged {len(tiles)} tiles into mosaic {output_file}.")

    return output_file


def crop_with_buffer(geom: gpd.GeoDataFrame, input_raster: str, output_raster: str) -> str:

    # Add a buffer to the geometry of interest
    geom_buffured = geom.to_crs("EPSG:4326").geometry.buffer(0.2)

    # Crop the input raster
    with rasterio.open(input_raster) as src:
    
        profile = src.profile
        out_image, _out_transform = rasterio.mask.mask(src, geom_buffured.geometry, crop=False)
    
    # Save
    with rasterio.open(output_raster, "w", **profile) as dst:
        dst.write(out_image[0], 1)

    return output_raster


if __name__ == "__main__":
    generate_landcover_raster()