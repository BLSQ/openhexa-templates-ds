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
    help="Path to save the final raster (.tif) in the workspace (parent directory will automatically be created).",
    type=str,
    required=False,
    multiple=False
)
def generate_landcover_raster(boundaries_file: str, output_path: str):

    """Extract, merge and crop landcover data from ESA, according the area of interest. The final raster is saved as a single-band .tif file."""

    # Load boundaries
    boundaries = read_boundaries(boundaries_file)

    # Determine which tiles intersect the boundaries
    tiles_name = retrieve_tiles(target_geom=boundaries)
    if not tiles_name:
        raise RuntimeError("💥 No ESA WorldCover tile intersects the input geometry.")

    # Prepare output file path
    if output_path:
        dst_file = Path(workspace.files_path) / output_path
        dst_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        dst_dir = Path(workspace.files_path) / "pipelines" / "accessmod" / "landcover"
        dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst_file = dst_dir / "landcover.tif"

    with tempfile.TemporaryDirectory(prefix="accessmod_landcover_") as tmpdirname:

        tmpdir = Path(tmpdirname)
        current_run.log_info(f"Temporary directory created at: {tmpdir}")

        # Download data 
        current_run.log_info(f"Starting download of {len(tiles_name)} tiles to temporary folder {tmpdir}...")
        tiles = download_tiles(name_list=tiles_name, 
                               output_path=tmpdir)
        if not tiles:
            raise FileNotFoundError(f"💥 No tiles found at {tmpdir}")

        # Merge tiles 
        current_run.log_info("Merging tiles into mosaic...")
        mosaic = merge_tiles(tiles=tiles, 
                             output_file=tmpdir / "landcover_tiles_merged.tif")
        if not Path(mosaic).exists():
            raise RuntimeError("💥 Mosaic generation failed")

        # Crop with buffer 
        current_run.log_info("Cropping raster to boundaries with buffer...")
        cropped_raster = crop_with_buffer(geom=boundaries, 
                                          input_raster=mosaic, 
                                          output_raster=dst_file)
        if not dst_file.exists():
            raise RuntimeError("💥 Final raster was not created.")
        current_run.log_info("Raster successfully cropped!")
        current_run.log_info(f"Final raster saved at: {dst_file} ({dst_file.stat().st_size / 1e6:.2f} MB)")
        current_run.log_info("🎉 Extraction of landcover data finished successfully!")
        
        current_run.add_file_output(cropped_raster)


#########################
def read_boundaries(file_path: str) -> gpd.GeoDataFrame:

    """Load boundaries from a supported file format."""

    path = Path(file_path)
    if not path.exists():
        msg = f"File {file_path} not found in Files"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    if path.suffix not in (".gpkg", ".parquet", ".geojson", ".shp"):
        raise ValueError("💥 File format not supported. Use .gpkg, .parquet, .geojson or .shp.")

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

    # Take into account buffer for later (cropping) to retrieve enough tiles
    target_geom = target_geom.geometry.buffer(0.2)

    # Create a global grid (2021 data)
    pattern_name = "ESA_WorldCover_10m_2021_v200_{meridional}{lat:02d}{zonal}{long:03d}_Map.tif"
    grid = create_global_grid_name(3, 3, pattern_name)

    # Select tiles intersecting the polygon
    selected_tiles = grid[grid.intersects(target_geom.union_all())] 

    return selected_tiles["name"].tolist()


#########################
def download_tiles(name_list: List[str], output_path: Path) -> List[str]:

    """Download tiles from ESA S3 bucket into the specified directory."""

    s3 = boto3.client("s3", region_name="eu-central-1", config=Config(signature_version=UNSIGNED))

    downloaded_files = []
    for name in name_list:
        output_file = output_path / name

        if output_file.exists():
            current_run.log_info(f"Tile already exists: {output_file}")
            downloaded_files.append(str(output_file))
            continue

        path = f"v200/2021/map/{name}"

        try:
            s3.download_file("esa-worldcover", path, str(output_file))
            downloaded_files.append(str(output_file))

        except Exception as e:
            s3.download_file("esa-worldcover", path, output_path + name)
            current_run.log_error(f"💥 Failed to download {name}: {e}")
            raise
    
    return downloaded_files


#########################
def merge_tiles(tiles: List[str], output_file: Path) -> str:

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

    return str(output_file)


#########################
def crop_with_buffer(geom: gpd.GeoDataFrame, input_raster: str, output_raster: Path) -> str:

    """Crop a raster with an optional buffer around the geometry and save to output path."""

    # Add a buffer to the geometry of interest
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
    
    # Save
    with rasterio.open(output_raster, "w", **profile) as dst:
        dst.write(out_image[0], 1)

    return str(output_raster)

#########################
if __name__ == "__main__":
    generate_landcover_raster()