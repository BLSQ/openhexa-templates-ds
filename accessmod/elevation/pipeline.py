# from pathlib import Path
from openhexa.sdk import pipeline, current_run # workspace, parameter
import os 
from appdirs import user_cache_dir
import geopandas as gpd
from shapely.geometry import box
from typing import List
from shapely.geometry.base import BaseGeometry
import boto3
from botocore import UNSIGNED
from botocore.config import Config
# import processing
# import rasterio
# from rasterio.crs import CRS



WORK_DIR = os.path.join(user_cache_dir("accessmod"), "elevation")
# DATA_PATH = os.path.join(workspace.files_path, "pipelines/accessmod/elevation/data")
DATA_PATH = "tmp/accessmod/elevation/data"

print('string')

@pipeline("landcover")
def generate_land_cover(target_geometry):

    os.makedirs(WORK_DIR, exist_ok=True)

    # Get tiles
    tiles_name = retrieve_tiles(target_geom=target_geometry)

    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

    # Download data 
    tiles = download_tiles(name_list=tiles_name, output_path=DATA_PATH)

    # Merge tiles and crop result using the target geometry
    landcover = merge_tiles(tiles=tiles, target_geom=target_geometry)


    return None



def create_global_grid_name(latitude_spacing: int, longitude_spacing: int, pattern: str) -> gpd.GeoDataFrame:
    
    tiles = []
    for lat in range(-90, 90, latitude_spacing):
        for lon in range(-180, 180, longitude_spacing):
            geom = box(lon, lat, lon+longitude_spacing, lat+latitude_spacing)
    
            NS = "N" if lat >= 0 else "S"
            EW = "E" if lon >= 0 else "W"
    
            # Tile name 
            name = pattern.format(meridional=NS, lat=abs(lat), zonal=EW, long=abs(lon))
            tiles.append({"geometry": geom, "name": name})

    return gpd.GeoDataFrame(tiles, crs="EPSG:4326")


def retrieve_tiles(target_geom: BaseGeometry) -> List[str]:

    """
    Return a list of paths to files of interest 
    """
    
    if target_geom.crs != 'EPSG:4326':
        target_geom.to_crs(4326)

    # Create a global grid    
    pattern_name = "Copernicus_DSM_COG_10_{meridional}{lat:02d}_00_{zonal}{long:03d}_00_DEM"
    grid = create_global_grid_name(1, 1, pattern_name)

    # Select tiles intersecting the polygon
    selected_tiles = grid[grid.intersects(target_geom.union_all())] 

    return selected_tiles["name"].tolist()


def download_tiles(name_list: List[str], output_path: str):

    s3 = boto3.client('s3', region_name="eu-central-1", config=Config(signature_version=UNSIGNED))

    for name in name_list:
        output_file = name + '.tif'
        s3.download_file('copernicus-dem-30m', f"{name}/{name}.tif", output_path + output_file)

    return name_list


def merge_tiles(tiles: List[str], output_file: str, overwrite: bool = False):

    # fs = utils.filesystem(output_file)
    # if fs.isfile(dst_file) and not overwrite:
    #     current_run.log_info(f"File {output_file} already exists.")
    #     return output_file

    with rasterio.open(tiles[0]) as src:
        meta = src.meta.copy()

    mosaic, dst_transform = rasterio.merge.merge(tiles)
    # meta.update(RASTERIO_DEFAULT_PROFILE) 
    # RASTERIO_DEFAULT_PROFILE = {
                                # "driver": "GTiff",
                                # "tiled": True,
                                # "blockxsize": 256,
                                # "blockysize": 256,
                                # "compress": "zstd",
                                # "predictor": 2,
                                # "num_threads": "all_cpus",
                                # }
    meta.update(transform=dst_transform, height=mosaic.shape[1], width=mosaic.shape[2])

    with rasterio.open(output_file, "w", **meta) as dst:
        dst.write(mosaic)
    current_run.log_info(f"Merged {len(tiles)} tiles into mosaic {output_file}.")

    return output_file