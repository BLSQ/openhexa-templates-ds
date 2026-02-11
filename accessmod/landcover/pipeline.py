import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
import geopandas as gpd
from botocore import UNSIGNED
from botocore.config import Config
from openhexa.sdk import current_run, parameter, pipeline, workspace
from shapely.geometry import Polygon


@pipeline("landcover")
@parameter(
    "boundaries_file",
    name="Boundaries input file path",
    help="Input fileof geometry of interest (should be located in Files).",
    type=str,
    default="workspace/Sao_Tome.gpkg",
    required=True,
    multiple=False
)
@parameter(
    "output_dir",
    name="Output directory path",
    help="Output directory path in the workspace (where output files will be stored)",
    type=str,
    required=False,
    default="landcover",
    multiple=False
)
def generate_landcover_raster(boundaries_file: str, output_dir: str):
    """Generate a landcover raster from ESA.

    This function extracts ESA tiles intersecting the input boundary,
    merges them into a single mosaic, crops it according to the buffered geometry. 
    The resulting landcover is saved as .tif file in the specified output directory. 

    Parameters
    ----------
    boundaries_file : str
        Path to the file containing the boundary geometry of the area of interest.
    output_dir : str
        Directory where the final elevation and slope raster files will be saved.
        If it does not exist, it will be created.
    """
    # Prepare output directory path
    output_dir = Path(output_dir)
    if not output_dir.exists():
        output_dir = Path(workspace.files_path) / output_dir
        output_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir.mkdir(parents=True, exist_ok=True)
    
    current_run.log_info(f"Output directory path defined: {output_dir}")
    
    # Load boundaries
    boundaries = read_boundaries(Path(boundaries_file))

    # Add buffer and save for later 
    target_geom = get_buffered_geom(boundaries=boundaries, buffer=0.2, output_dir=output_dir)

    # Determine which tiles intersect the boundaries
    tiles_name = find_intersecting_tiles(target_geom=target_geom)

    if not tiles_name:
        raise RuntimeError("💥 No ESA WorldCover tile intersects the input geometry.")

    with tempfile.TemporaryDirectory(prefix="accessmod_landcover_") as tmpdirname:

        tmpdir = Path(tmpdirname)
        current_run.log_info(f"Temporary directory created at: {tmpdir}")

        # Download data 
        current_run.log_info(f"Downloading of {len(tiles_name)} tiles to temporary folder")
        tiles = download_tiles(name_list=tiles_name, 
                               output_path=tmpdir)
        
        if not tiles:
            raise FileNotFoundError(f"💥 No tiles found at {tmpdir}")

        # Merge tiles and crop
        current_run.log_info("Merging tiles into mosaic and cropping with buffered geometry...")
        mosaic = merge_crop_tiles(tiles=tiles,
                                  boundaries_path=output_dir / "buffered_geom.gpkg",
                                  output_dir=output_dir)
        
        if not Path(mosaic).exists():
            raise RuntimeError("💥 Mosaic generation failed")
        
        current_run.log_info(f"Final landcover raster saved at: {mosaic}")

        current_run.log_info("🎉 Extraction of landcover data finished successfully!")
        current_run.add_file_output(mosaic.as_posix())


def read_boundaries(file_path: Path) -> gpd.GeoDataFrame:
    """Loads a boundary geometry from a supported vector file format.
    
    Parameters
    ----------
    file_path : Path
        Path to the vector file containing boundary geometries.
    
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing the loaded boundary geometries.
    """
    if not file_path.is_file():
        msg = f"File {file_path} not found in Files"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    suffixes = (".gpkg", ".parquet", ".geojson", ".shp")
    if not str(file_path).endswith(suffixes):
        raise ValueError("💥 File format not supported. Use .gpkg, .parquet, .geojson or .shp.")

    if str(file_path).endswith(".parquet"):
        return gpd.read_parquet(file_path)

    return gpd.read_file(file_path)


def get_buffered_geom(boundaries: gpd.GeoDataFrame, 
                      buffer: float, 
                      output_dir: Path) -> tuple[float, float, float, float]:
    """Create a buffered geometry from an area of interest and save it.

    Parameters
    ----------
    boundaries : geopandas.GeoDataFrame
        GeoDataFrame containing the geometry of the area of interest.
    buffer : float
        Buffer distance in degrees (EPSG:4326).
    output_dir : pathlib.Path
        Directory where the buffered geometry file will be written.

    Returns
    -------
    tuple of float
        Bounding box of the buffered geometry: (minx, miny, maxx, maxy).
    """
    geom = boundaries.to_crs("EPSG:4326").union_all()
    buffered_geom = geom.buffer(buffer)

    gpd.GeoDataFrame(geometry=[buffered_geom], 
                     crs="EPSG:4326").to_file(output_dir / "buffered_geom.gpkg")

    return buffered_geom


def find_intersecting_tiles(target_geom: Polygon) -> list[str]:
    """Return Copernicus DEM tile names intersecting a target geometry.

    Parameters
    ----------
    target_geom : geopandas.GeoDataFrame
        Geometry of interest.

    Returns
    -------
    list[str]
        List of intersecting tile names.
    """
    tiles = []
    minx, miny, maxx, maxy = target_geom.bounds

    for lat in range(int(miny // 3 * 3), int(maxy // 3 * 3) + 3, 3):
        for lon in range(int(minx // 3 * 3), int(maxx // 3 * 3) + 3, 3):
            ns = "N" if lat >= 0 else "S"
            ew = "E" if lon >= 0 else "W"
            name = f"ESA_WorldCover_10m_2021_v200_{ns}{abs(lat):02d}{ew}{abs(lon):03d}_Map.tif"
            tiles.append(name)

    return tiles


def download_tiles(name_list: list[str], 
                   output_path: Path) -> list[str]:
    """Download raster tiles from ESA S3 bucket into the specified directory.
    
    Parameters
    ----------
    name_list : list[str]
        List of tile names to download. Each name should correspond to the
        folder structure and file naming in the Copernicus DEM S3 bucket.
    output_path : Path
        Directory where the downloaded tiles will be saved. Must exist or be writable.

    Returns
    -------
    list[str]
        List of file paths to the successfully downloaded or already existing tiles.
    """
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
            s3.head_object(Bucket="esa-worldcover", Key=path)
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                current_run.log_info(f"Tile does not exist in S3, skipping: {name}")
                continue
            raise

        s3.download_file("esa-worldcover", path, str(output_file))
        downloaded_files.append(str(output_file))
    
    return downloaded_files


def merge_crop_tiles(tiles: list[str], 
                     boundaries_path: Path, 
                     output_dir: Path) -> Path:
    """Merges single-band raster tiles into a single mosaic raster.
     
    The mosaic is croped using a buffered geometry of interest.

    Parameters
    ----------
    tiles : list[str]
        List of file paths to single-band raster tiles to be merged.
    boundaries_path : Path
        Path to the boundaries file whose geometry has been buffered.
        The geometry will be reprojected to EPSG:4326 if necessary.
    output_dir : Path
        Directory where the buffered geometry and mosaic raster will be saved.

    Returns
    -------
    Path
        Path to the resulting cropped mosaic raster.
    """
    output_file = output_dir / "landcover.tif"
    cmd = [
        "gdalwarp",
        "-cutline", str(boundaries_path),
        "-crop_to_cutline",               # crop the raster with geometry of interest
        "-multi",                         # multithreaded warping implementation 
        "-wm", "8192",                    # RAM usage 
        "-wo", "NUM_THREADS=ALL_CPUS",
        "-co", "COMPRESS=DEFLATE",        # compress 
        "-of", "COG",
        "-overwrite",
    ]

    cmd.extend(tiles)
    cmd.append(output_file)

    subprocess.run(cmd, check=True)

    return output_file


if __name__ == "__main__":
    generate_landcover_raster()