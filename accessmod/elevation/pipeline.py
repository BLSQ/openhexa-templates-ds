import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

import boto3
import geopandas as gpd
from botocore import UNSIGNED
from botocore.config import Config
from openhexa.sdk import current_run, parameter, pipeline, workspace
from osgeo import gdal
from shapely.geometry import box

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
        dst_dir = Path(workspace.files_path) / "pipelines" / "accessmod" / "elevation"
        dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst_file = dst_dir / "elevation.tif"

    current_run.log_info(f"Output raster path defined: {dst_file}")

    with tempfile.TemporaryDirectory(prefix="accessmod_elevation_") as tmpdirname:

        tmpdir = Path(tmpdirname)
        current_run.log_info(f"Temporary directory created at: {tmpdirname}")

        # Download data 
        current_run.log_info(f"Starting download of {len(tiles_name)} tiles to temporary folder {tmpdir}...")
        tiles = download_tiles(name_list=tiles_name, 
                               output_path=tmpdir)

        if not tiles:
            raise FileNotFoundError(f"💥 No tile found at {tmpdir}")

        # Merge tiles and crop with raster
        current_run.log_info("Merging tiles into mosaic and cropping with buffered geometry...")
        mosaic = merge_crop_tiles(tiles=tiles,
                                  geom=boundaries,
                                  output_dir=tmpdir)

        if not Path(mosaic).exists():
            raise RuntimeError("💥 Mosaic generation failed")

        # Compute slope
        current_run.log_info("Calculating slope...")
        slope = compute_slope(input_file=mosaic, 
                              output_file=dst_file)
        
        current_run.log_info(f"Final raster saved at: {dst_file} ({dst_file.stat().st_size / 1e6:.2f} MB)")

        current_run.log_info("🎉 Extraction of elevation data finished successfully!")
        current_run.add_file_output(slope)


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

    # Take into account buffer for later (cropping) to retrieve enough tiles
    target_geom = target_geom.geometry.buffer(0.2)

    # Create a global grid    
    pattern_name = "Copernicus_DSM_COG_10_{meridional}{lat:02d}_00_{zonal}{long:03d}_00_DEM"
    grid = create_global_grid_name(1, 1, pattern_name)

    # Select tiles intersecting the polygon
    selected_tiles = grid[grid.intersects(target_geom.union_all())] 

    return selected_tiles["name"].tolist()


#########################
def download_tiles(name_list: List[str], output_path: Path) -> List[str]:

    """Download tiles from ESA S3 bucket into the specified directory."""

    s3 = boto3.client("s3", region_name="eu-central-1", config=Config(signature_version=UNSIGNED))

    downloaded_files = []

    for name in name_list:
        output_file = output_path / f"{name}.tif"

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
def merge_crop_tiles(tiles: List[str], geom: gpd.GeoDataFrame, output_dir: Path) -> str:

    """Merge single-band raster tiles into a single mosaic raster and crop it with the buffured geometry of interest."""

    output_geom_buffured = output_dir / "buffered_geom.gpkg"
    output_file = str(output_dir / "mosaic.tif")

    # Add a buffer to the geometry of interest (in degrees)
    geom_buffured = geom.to_crs("EPSG:4326").geometry.buffer(0.2)
    geom_buffured.to_file(output_geom_buffured)

    cmd = [
        "gdalwarp",
        "-cutline", str(output_geom_buffured),
        "-crop_to_cutline",                   # crop the raster with geometry of interest define line before
        "-multi",                             # multithreaded warping implementation 
        "-wm", "8192",                        # RAM usage 
        "-wo", "NUM_THREADS=ALL_CPUS",
        "-co", "COMPRESS=DEFLATE",            # compress 
        "-co", "TILED=YES",
        "-overwrite",
    ]

    cmd.extend(tiles)
    cmd.append(output_file)

    subprocess.run(cmd)

    return output_file


#########################
def compute_slope(input_file: str, output_file: Path) -> str:

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

    if not output_file.exists():
        raise RuntimeError("💥 Slope computation failed.")

    src_ds = None  # Close dataset
    
    return str(output_file)


#########################
if __name__ == "__main__":
    generate_elevation_raster()
