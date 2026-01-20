from datetime import datetime
from pathlib import Path

import geopandas as gpd
from openhexa.sdk import current_run, parameter, pipeline, workspace
from osgeo import gdal

RESAMPLING_RULES = {
    "landcover": "near",
    "elevation": "bilinear",
    "transport": "near",
    "water": "near",
}


@pipeline("align_rasters")
@parameter(
    "output_proj",
    name="Target projection",
    help="EPSG code or WKT",
    type=str,
    required=False,
    multiple=False,
    default="EPSG:4326"
)
@parameter(
    "input_dir",
    name="Input rasters directory",
    help="Path to the directory where input rasters are stored",
    type=str,
    required=False,
    multiple=False
)
@parameter(
    "boundaries_file",
    name="Boundaries input file path",
    help="Input file of geometry of interest",
    type=str,
    required=True,
    multiple=False
)
def align_rasters(input_dir: str, boundaries_file: str, output_proj: str):
    """Align, reproject, and crop all raster files in a directory using a reference raster (lowest resolution) and a geometry of interest.

    Parameters
    ----------
    input_dir : str 
        Path to the directory containing input raster files.
    boundaries_file : str
        Path to the geometry file used to clip rasters.
        NB: that could be the only .gpkg (.parquet, etc.) in the folder input_dir
    output_proj : str 
        Target projection for output rasters in EPSG format (e.g., "EPSG:4326").
    """
    # Create output directory
    dst_dir = Path(workspace.files_path) / "pipelines" / "accessmod" / "aligned_data"
    dst_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dst_dir.mkdir(parents=True, exist_ok=True)

    # Check if geom file exists
    geom_path = Path(boundaries_file)
    if not geom_path.exists():
        raise FileNotFoundError(f"Boundaries file does not exist: {geom_path}")

    # Align rasters
    aligned_files = align_crop_rasters(input_rasters_dir=Path(input_dir),
                                           geom_path=boundaries_file,
                                           output_projection=output_proj, 
                                           output_dir=dst_dir)
    
    current_run.log_info(f"Alignment completed. {len(aligned_files)} rasters saved to {dst_dir}")


def retrieve_file_paths(folder: Path, ext: str) -> list[Path]:
    """Get list of paths in a given folder with the specific extension.
    
    Parameters
    ----------
    folder : Path
        Path to the folder of interest
    ext : str
        Extension of the files of interest

    Returns
    -------
     list[str]
        List of file paths in the given folder with the specified extension
    """
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder}")
    return list(folder.glob(f"*.{ext}"))


def get_lowest_resolution_raster(rasters: list[str]) -> Path:
    """Return path to raster with highest pixel area.
    
    Parameters
    ----------
    rasters : list[str]
        List of file paths to raster files.

    Returns
    -------
    Path
        Path to the raster with the largest pixel area.
    """
    worst = None
    worst_res = 0

    for path in rasters:
        try:

            ds = gdal.Open(path)

            if ds is None:
                current_run.log_error(f"Could not open raster: {path}")
                continue

            gt = ds.GetGeoTransform()
            pixel_area = abs(gt[1] * gt[5])

            if pixel_area > worst_res:
                worst_res = pixel_area
                worst = path
        
        except Exception as e:
            current_run.log_error(f"Error reading raster {path}: {e}")

        finally:
            ds = None

    if worst is None:
        raise RuntimeError("No valid raster found to use as reference.")
    
    return worst


def resampling_from_filename(filename: Path, rules: dict, default: str = "near") -> str:
    """Determine the resampling algorithm based on raster filename and predefined rules."""
    name = filename.name.lower()
    for key, algo in rules.items():
        if key in name:
            return algo
    return default


def align_crop_rasters(input_rasters_dir: Path, 
                       geom_path: str, 
                       output_projection: str, 
                       output_dir: Path) -> list[str]:
    """Aligns, reprojects, resamples, and crops all `.tif` rasters in a directory to a common grid.

    Parameters
    ----------
    input_rasters_dir : Path
        Path to the directory containing `.tif` raster files.
    geom_path : str
        Path to the vector file (e.g., shapefile or GeoPackage) defining the area of interest.
    output_projection : str
        EPSG code or projection string for the output rasters.
    output_dir : Path
        Directory where processed rasters and reprojected geometry will be saved.

    Returns
    -------
    list[str]
        List of file paths to the aligned and cropped rasters.
    """
    rasters = retrieve_file_paths(input_rasters_dir, "tif")
    current_run.log_info(f"Found {len(rasters)} rasters to process")

    # Reproject boundaries_file to output_projection
    try:
        gdf = gpd.read_file(geom_path)
        gdf = gdf.to_crs(output_projection)
        reprojected_geom_path = output_dir / f"{Path(geom_path).stem}_reprojected.gpkg"
        gdf.to_file(reprojected_geom_path)
        current_run.log_info(
            f"Boundaries reprojected to {output_projection} -> {reprojected_geom_path}"
            )
    except Exception as e:
        current_run.log_error(f"Failed to reproject area of interest {geom_path}: {e}")
        raise RuntimeError(f"Reprojection failed: {e}") from e

    # Reference raster (lowest resolution)
    ref_raster = get_lowest_resolution_raster(rasters)
    current_run.log_info(f"Reference raster (lowest resolution): {ref_raster}")

    ref_ds = gdal.Open(ref_raster)

    # User defined ouput projection
    ref_proj_ds = gdal.Warp(
        "",  # empty string = in-memory
        ref_ds,
        format="MEM",
        dstSRS=output_projection,
        resampleAlg="nearest"
    )
    ref_gt = ref_proj_ds.GetGeoTransform()
    ref_xres = abs(ref_gt[1])
    ref_yres = abs(ref_gt[5])
    ref_bounds = gdal.Info(ref_proj_ds, format="json")["cornerCoordinates"]

    xmin = ref_bounds["lowerLeft"][0]
    ymin = ref_bounds["lowerLeft"][1]
    xmax = ref_bounds["upperRight"][0]
    ymax = ref_bounds["upperRight"][1]

    # Clean
    ref_ds = None
    ref_proj_ds = None

    outputs = []  # List[Path]

    for src_path in rasters:

        resampling_algo = resampling_from_filename(filename=src_path, 
                                                   rules=RESAMPLING_RULES, 
                                                   default="near")
        current_run.log_info(f"Processing raster: {src_path} | Resampling: {resampling_algo}")

        out_path = output_dir / (Path(src_path).stem + "_aligned.tif")

        current_run.log_info(f"Processing raster: {src_path.name} | Resampling: {resampling_algo}")

        warp_options = gdal.WarpOptions(
            format="GTiff",
            dstSRS=output_projection,
            xRes=ref_xres,
            yRes=ref_yres,
            targetAlignedPixels=True,
            outputBounds=(xmin, ymin, xmax, ymax),
            cutlineDSName=geom_path,
            cropToCutline=True,
            resampleAlg=resampling_algo,
            creationOptions=["TILED=YES", "COMPRESS=DEFLATE"],
            multithread=True
        )

        try:
            gdal.Warp(
                destNameOrDestDS=out_path,
                srcDSOrSrcDSTab=src_path,
                options=warp_options
            )
        except Exception as e:
            current_run.log_error(f"Failed to warp raster {src_path.name}: {e}")
            continue

        outputs.append(out_path)
        current_run.log_info(f"Saved aligned raster to {out_path}")

    return outputs


if __name__ == "__main__":
    align_rasters()