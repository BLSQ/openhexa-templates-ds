import math
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
import geopandas as gpd
import numpy as np
from botocore import UNSIGNED
from botocore.config import Config
from openhexa.sdk import current_run, parameter, pipeline, workspace
from osgeo import gdal
from shapely.geometry import Polygon, box

gdal.UseExceptions()


@pipeline("elevation")
@parameter(
    "boundaries_file",
    name="Boundaries input file path",
    help="Input file of geometry of interest (should be located in Files).",
    type=str,
    required=True,
    default="DRC.gpkg",
    multiple=False,
)
@parameter(
    "output_dir",
    name="Output directory path",
    help="Output directory path in the workspace (where output files will be stored)",
    type=str,
    required=False,
    default="elevation",
    multiple=False,
)
def generate_elevation_raster(boundaries_file: str, output_dir: str):
    """Generate an elevation raster and slope from Copernicus DEM data.

    This function extracts Copernicus DEM tiles intersecting the input boundary,
    merges them into a single mosaic, crops it according to the buffered geometry,
    and computes the slope. The resulting elevation and slope rasters (with buffer!) are saved
    as .tif files in the specified output directory.

    Parameters
    ----------
    boundaries_file : str
        Path to the file containing the boundary geometry of the area of interest.
    output_dir : str
        Directory where the final elevation and slope raster files will be saved.
        If it does not exist, it will be created.
    """
    output_dir = Path(output_dir)
    if not output_dir.exists():
        output_dir = Path(workspace.files_path) / output_dir
        output_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir.mkdir(parents=True, exist_ok=True)

    current_run.log_info(f"Output directory path defined: {output_dir}")

    boundaries = read_boundaries(file_path=Path(boundaries_file))

    target_geom = get_buffered_geom(boundaries=boundaries, buffer=0.2, output_dir=output_dir)

    tiles_name = find_intersecting_tiles(target_geom)

    with tempfile.TemporaryDirectory(prefix="accessmod_elevation_") as tmpdirname:
        tmpdir = Path(tmpdirname)

        current_run.log_info(f"Downloading of {len(tiles_name)} tiles to temporary folder")
        tiles = download_tiles(name_list=tiles_name, output_path=tmpdir)

        current_run.log_info("Merging tiles into mosaic and cropping with buffered geometry...")
        mosaic_cog = merge_crop_tiles(
            tiles=tiles,
            boundaries_path=output_dir / "buffered_geom.gpkg",
            tmp_dir=tmpdir,
            output_dir=output_dir,
        )

        current_run.log_info("Calculating slope...")
        slope_cog = compute_slope(input_file=mosaic_cog, output_dir=output_dir)

        current_run.log_info(f"Elevation raster saved at: {mosaic_cog}")
        current_run.log_info(f"Slope raster saved at: {slope_cog}")
        current_run.add_file_output(mosaic_cog.as_posix())
        current_run.add_file_output(slope_cog.as_posix())


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
    file_path = workspace.files_path / file_path
    if not file_path.is_file():
        msg = f"File {file_path} not found in Files"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    suffixes = (".gpkg", ".parquet", ".geojson", ".shp")
    if not str(file_path).endswith(suffixes):
        raise ValueError(
            "💥 File not in a correct format. Import it as .gpkg, .parquet, .geojson or .shp."
        )

    if str(file_path).endswith(".parquet"):
        return gpd.read_parquet(file_path)

    return gpd.read_file(file_path)


def get_buffered_geom(
    boundaries: gpd.GeoDataFrame, buffer: float, output_dir: Path
) -> tuple[float, float, float, float]:
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

    gpd.GeoDataFrame(geometry=[buffered_geom], crs="EPSG:4326").to_file(
        output_dir / "buffered_geom.gpkg"
    )

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
    minx, miny, maxx, maxy = target_geom.bounds

    grid_polygons = []
    grid_names = []

    for lat in np.arange(math.floor(miny) - 1, math.ceil(maxy) + 1):
        for lon in np.arange(math.floor(minx) - 1, math.ceil(maxx) + 1):
            grid_polygons.append(box(lon, lat, lon + 1, lat + 1))

            ns = "N" if lat >= 0 else "S"
            ew = "E" if lon >= 0 else "W"
            name = f"Copernicus_DSM_COG_10_{ns}{abs(lat):02d}_00_{ew}{abs(lon):03d}_00_DEM"
            grid_names.append(name)

    grid = gpd.GeoDataFrame({"name": grid_names, "geometry": grid_polygons}, crs="EPSG:4326")
    tiles = grid[grid.intersects(target_geom)]["name"].tolist()

    if not tiles:
        raise RuntimeError("💥 No Copernicus tile intersects the input geometry.")

    return tiles


def download_tiles(name_list: list[str], output_path: Path) -> list[str]:
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
        output_file = output_path / f"{name}.tif"

        try:
            s3.download_file("copernicus-dem-30m", f"{name}/{name}.tif", str(output_file))
            downloaded_files.append(str(output_file))
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                current_run.log_info(f"Tile does not exist in S3, skipping: {name}")
                continue
            raise

    if not downloaded_files:
        raise FileNotFoundError(f"💥 No tile found at {output_path}")

    return downloaded_files


def save_raster_as_cog(input_raster_path: Path, output_raster_path: Path) -> Path:
    """Save raster in Cloud Optimized Geotiff format.

    Parameters
    ----------
    input_raster_path: Path
        Path to the input raster.
    output_raster_path: Path
        Path to the output raster.
    """
    options = gdal.TranslateOptions(
        format="COG",
        creationOptions=[
            "COMPRESS=ZSTD",
            "BLOCKSIZE=512",
            "PREDICTOR=YES",
            "BIGTIFF=IF_SAFER",
            "NUM_THREADS=ALL_CPUS",
        ],
    )

    gdal.Translate(str(output_raster_path), str(input_raster_path), options=options)


def merge_crop_tiles(
    tiles: list[str], boundaries_path: Path, tmp_dir: Path, output_dir: Path
) -> Path:
    """Merges single-band raster tiles into a single mosaic raster.

    The mosaic is croped using a buffered geometry of interest.

    Parameters
    ----------
    tiles : list[str]
        List of file paths to single-band raster tiles to be merged.
    boundaries_path : Path
        Path to the boundaries file whose geometry has been buffered.
    tmp_dir: Path
        Path to the temporary directory.
    output_dir : Path
        Directory where the buffered geometry and mosaic raster will be saved.

    Returns
    -------
    Path
        Path to the resulting cropped mosaic rasters (geotiff and COG formats).
    """
    tmp_file = tmp_dir / "mosaic.tif"
    output_file_cog = output_dir / "mosaic.tif"
    cmd = [
        "gdalwarp",
        "-cutline",
        str(boundaries_path),
        "-crop_to_cutline",
        "-multi",
        "-wm",
        "8192",
        "-wo",
        "NUM_THREADS=ALL_CPUS",
        "-co",
        "COMPRESS=ZSTD",
        "-co",
        "ZSTD_LEVEL=1",
        "-co",
        "TILED=YES",
        "-co",
        "BIGTIFF=YES",
        "-overwrite",
    ]
    cmd.extend(tiles)
    cmd.append(str(tmp_file))
    subprocess.run(cmd, check=True)

    save_raster_as_cog(input_raster_path=tmp_file, output_raster_path=output_file_cog)

    return output_file_cog


def compute_slope(input_file: Path, output_dir: Path) -> Path:
    """Compute slope raster from an elevation raster with GDAL.

    Parameters
    ----------
    input_file : Path
        Path to the input elevation raster (DEM).
    output_dir : Path
        Path where the output slope raster will be written.

    Returns
    -------
    Path
        Path to the generated slope rasters (geotiff and COG formats).
    """
    output_file_cog = output_dir / "slope.tif"

    src_ds = gdal.Open(str(input_file))
    if src_ds is None:
        raise RuntimeError(f"Unable to open {input_file}")

    scale = None
    if not src_ds.GetSpatialRef().IsProjected():
        # because source ref system is EPSG:4326 + slope computed in meters
        scale = 111120  # If it was in feet: scale = 370400
    src_ds = None

    options = gdal.DEMProcessingOptions(
        format="COG",
        scale=scale,  # ratio of vertical units to horizontal
        slopeFormat="degree",
        creationOptions=[
            "BLOCKSIZE=1024",
            "COMPRESS=ZSTD",
            "LEVEL=3",
            "PREDICTOR=2",
            "NUM_THREADS=ALL_CPUS",
            "BIGTIFF=YES",
        ],
    )

    gdal.DEMProcessing(str(output_file_cog), str(input_file), "slope", options=options)

    if not output_file_cog.exists():
        raise RuntimeError("💥 Slope computation failed.")

    return output_file_cog


if __name__ == "__main__":
    generate_elevation_raster()
