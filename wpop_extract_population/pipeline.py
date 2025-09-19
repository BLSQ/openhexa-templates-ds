from pathlib import Path

import geopandas as gpd
import pandas as pd
import rasterio
from openhexa.sdk import File, current_run, parameter, pipeline, workspace
from rasterstats import zonal_stats
from sqlalchemy import create_engine
from worlpopclient import WorldPopClient


@pipeline("wpop_extract_population")
@parameter(
    code="country_iso3",
    name="Country ISO3 code",
    help="Country ISO3 code to download population data for.",
    type=str,
    multiple=False,
    # List of valid ISO3 codes from WorldPop (as of 2025-09)
    choices=[
        "ABW",
        "AFG",
        "AGO",
        "AIA",
        "ALA",
        "ALB",
        "AND",
        "ARE",
        "ARG",
        "ARM",
        "ASM",
        "ATA",
        "ATF",
        "ATG",
        "AUS",
        "AUT",
        "AZE",
        "BDI",
        "BEL",
        "BEN",
        "BES",
        "BFA",
        "BGD",
        "BGR",
        "BHR",
        "BHS",
        "BIH",
        "BLM",
        "BLR",
        "BLZ",
        "BMU",
        "BOL",
        "BRA",
        "BRB",
        "BRN",
        "BTN",
        "BVT",
        "BWA",
        "CAF",
        "CAN",
        "CHE",
        "CHL",
        "CHN",
        "CIV",
        "CMR",
        "COD",
        "COG",
        "COK",
        "COL",
        "COM",
        "CPV",
        "CRI",
        "CUB",
        "CUW",
        "CYM",
        "CYP",
        "CZE",
        "DEU",
        "DJI",
        "DMA",
        "DNK",
        "DOM",
        "DZA",
        "ECU",
        "EGY",
        "ERI",
        "ESH",
        "ESP",
        "EST",
        "ETH",
        "FIN",
        "FJI",
        "FLK",
        "FRA",
        "FRO",
        "FSM",
        "GAB",
        "GBR",
        "GEO",
        "GGY",
        "GHA",
        "GIB",
        "GIN",
        "GLP",
        "GMB",
        "GNB",
        "GNQ",
        "GRC",
        "GRD",
        "GRL",
        "GTM",
        "GUF",
        "GUM",
        "GUY",
        "HKG",
        "HMD",
        "HND",
        "HRV",
        "HTI",
        "HUN",
        "IDN",
        "IMN",
        "IND",
        "IOT",
        "IRL",
        "IRN",
        "IRQ",
        "ISL",
        "ISR",
        "ITA",
        "JAM",
        "JEY",
        "JOR",
        "JPN",
        "KAZ",
        "KEN",
        "KGZ",
        "KHM",
        "KIR",
        "KNA",
        "KOR",
        "KOS",
        "KWT",
        "LAO",
        "LBN",
        "LBR",
        "LBY",
        "LCA",
        "LIE",
        "LKA",
        "LSO",
        "LTU",
        "LUX",
        "LVA",
        "MAC",
        "MAF",
        "MAR",
        "MCO",
        "MDA",
        "MDG",
        "MDV",
        "MEX",
        "MHL",
        "MKD",
        "MLI",
        "MLT",
        "MMR",
        "MNE",
        "MNG",
        "MNP",
        "MOZ",
        "MRT",
        "MSR",
        "MTQ",
        "MUS",
        "MWI",
        "MYS",
        "MYT",
        "NAM",
        "NCL",
        "NER",
        "NFK",
        "NGA",
        "NIC",
        "NIU",
        "NLD",
        "NOR",
        "NPL",
        "NRU",
        "NZL",
        "OMN",
        "PAK",
        "PAN",
        "PCN",
        "PER",
        "PHL",
        "PLW",
        "PNG",
        "POL",
        "PRI",
        "PRK",
        "PRT",
        "PRY",
        "PSE",
        "PYF",
        "QAT",
        "REU",
        "ROU",
        "RUS",
        "RWA",
        "SAU",
        "SDN",
        "SEN",
        "SGP",
        "SGS",
        "SHN",
        "SJM",
        "SLB",
        "SLE",
        "SLV",
        "SMR",
        "SOM",
        "SPM",
        "SPR",
        "SRB",
        "SSD",
        "STP",
        "SUR",
        "SVK",
        "SVN",
        "SWE",
        "SWZ",
        "SXM",
        "SYC",
        "SYR",
        "TCA",
        "TCD",
        "TGO",
        "THA",
        "TJK",
        "TKL",
        "TKM",
        "TLS",
        "TON",
        "TTO",
        "TUN",
        "TUR",
        "TUV",
        "TWN",
        "TZA",
        "UGA",
        "UKR",
        "UMI",
        "URY",
        "USA",
        "UZB",
        "VAT",
        "VCT",
        "VEN",
        "VGB",
        "VIR",
        "VNM",
        "VUT",
        "WLF",
        "WSM",
        "YEM",
        "ZAF",
        "ZMB",
        "ZWE",
    ],
    default=None,
    required=True,
)
@parameter(
    code="un_adj",
    name="UN adjusted population",
    type=bool,
    help="Download UN adjusted grid data.",
    default=False,
    required=False,
)
@parameter(
    code="shapes_path",
    name="Shape file",
    type=File,
    help="Shape file to use for the spatial aggregation. "
    "Supported extensions: .geoJSON, .shp, .gpkg. (See geopandas.read_file()).",
    required=True,
)
@parameter(
    code="dst_dir",
    type=str,
    name="Output directory",
    help="Output directory in the workspace. Parent directory will automatically be created.",
    default=None,
    required=False,
)
@parameter(
    code="dst_table",
    type=str,
    name="Output DB table",
    help="Output DB table name. If provided, output will be saved to a DB table.",
    default=None,
    required=False,
)
def wpop_extract_population(
    country_iso3: str,
    un_adj: bool,
    shapes_path: File,
    dst_dir: str,
    dst_table: str,
):
    """Pipeline to extract and aggregate population data from WorldPop.

    Parameters
    ----------
    country_iso3 : str
        The 3-letter ISO code of the country (e.g., "COD", "BFA").
    un_adj : bool
        Whether to download the UN adjusted grid data.
    shapes_path : File
        Shape file to use for the spatial aggregation. Supported extensions: .geoJSON, .shp, .gpkg.
        (See geopandas.read_file() for more details).
    dst_dir : str
        Output directory in the workspace. Parent directory will automatically be created.
    dst_table : str
        Output DB table name. If provided, output will be saved to a DB table.
    """
    # set paths
    root_path = Path(workspace.files_path)
    pipeline_path = root_path / "pipelines" / "wpop_extract_population"
    year = "2020"  # Latest available data in WorldPop
    current_run.log_debug(f"Shapes file path: {shapes_path.path}")

    if dst_dir is None:
        output_path = pipeline_path / "data" / "aggregated"
    else:
        output_path = root_path / dst_dir

    try:
        pop_file_path = retrieve_population_data(
            country_code=country_iso3,
            year=year,
            un_adj=un_adj,
            output_path=pipeline_path / "data" / "raw",
            overwrite=False,
        )

        pop_agg_file_path = run_spatial_aggregation(
            tif_file_path=pop_file_path,
            shapes_path=Path(shapes_path.path),
            output_dir=output_path,
        )

        if dst_table:
            write_to_db(pop_agg_file_path, dst_table)

    except Exception as e:
        current_run.log_error(f"Error : {e}")
        raise


def retrieve_population_data(
    country_code: str,
    output_path: Path,
    year: str = "2020",
    un_adj: bool = False,
    overwrite: bool = False,
) -> Path:
    """Retrieve raster population data from worldpop.

    Parameters
    ----------
    country_code : str
        The 3-letter ISO code of the country (e.g., "COD", "BFA").
    output_path : Path
        The directory where the population data will be saved.
    year : str, optional
        The year for which to retrieve the population data. Defaults to "2020".
    un_adj : bool, optional
        Whether to retrieve the UN adjusted population data. Defaults to False.
    overwrite : bool, optional (default=False)
        Whether to overwrite existing files. Defaults to False.

    Returns
    -------
    Path
        The path to the downloaded population data file.

    """
    current_run.log_info("Retrieving population data grid from Worldpop.")
    wpop_client = WorldPopClient()
    current_run.log_info(f"Downloading data from : {wpop_client.base_url}")

    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    pop_filename = wpop_client.target_tif_filename(country_code, year, un_adj)  # cleaner solution
    pop_file_path = output_path / pop_filename
    current_run.log_debug(f"target population filename: {pop_filename}")
    current_run.log_info(
        f"Retrieving data for country: {country_code} - year: {year} - UN adjusted: {un_adj}"
    )

    try:
        if not overwrite and pop_file_path.exists():
            current_run.log_info(f"File {pop_file_path} already exists. Skipping download")
            return pop_file_path

        _ = wpop_client.download_data_for_country(
            country_iso3=country_code,
            year=year,
            un_adj=un_adj,
            output_dir=output_path,
        )
        current_run.log_info(f"Population data successfully downloaded under : {pop_file_path}")
        return pop_file_path

    except Exception as e:
        raise Exception(
            f"Error retrieving WorldPop population data for {country_code} {year}: {e}"
        ) from e


def run_spatial_aggregation(tif_file_path: Path, shapes_path: Path, output_dir: Path) -> Path:
    """Run spatial aggregation on the worldpop population data (tif file).

    Parameters
    ----------
    tif_file_path : Path
        Path to the WorldPop population raster file (GeoTIFF).
    shapes_path : Path
        Path to the shape file (GeoJSON, Shapefile, etc.) for aggregation.
    output_dir : Path
        Directory where the aggregated output files will be saved.

    Returns
    -------
    Path
        Path to the output aggregated data file (Parquet format).
    """
    current_run.log_info(f"Running spatial aggregation with WorldPop data {tif_file_path}")

    if not tif_file_path.exists():
        raise FileNotFoundError(f"WorldPop file not found: {tif_file_path}")

    if not shapes_path.exists():
        raise FileNotFoundError(f"Shapes file not found: {shapes_path}")

    shapes = load_shapes(shapes_path)

    # Rename it to "geometry" if it's not already
    if shapes.geometry.name.lower() != "geometry":
        shapes = shapes.rename_geometry("geometry")

    if shapes.crs is None:
        raise ValueError("Shapes GeoDataFrame must have a defined CRS.")

    # Ensure CRS matches the raster & reproject if necessary
    with rasterio.open(tif_file_path) as src:
        # Reproject shapes if CRS is different
        if shapes.crs != src.crs:
            current_run.log_info(
                "The CRS data differs from the provided shapes file. "
                f"Reprojecting shapes with {src.crs}"
            )
            shapes = shapes.to_crs(src.crs)

        nodata = src.nodata  # No data value

    # get statistics
    current_run.log_info(f"Computing spacial aggregation for {len(shapes)} shapes")
    pop_stats = zonal_stats(
        shapes,
        tif_file_path,
        stats=["sum", "count"],
        nodata=nodata,  # -99999.0
        geojson_out=True,
    )

    # Formats
    result_gdf = gpd.GeoDataFrame.from_features(pop_stats)
    result_gdf = result_gdf.drop(columns=["geometry"])
    result_pd = pd.DataFrame(result_gdf)
    result_pd = result_pd.rename(columns={"sum": "population", "count": "pixel_count"})
    result_pd["population"] = result_pd["population"].round(0).astype(int)
    col_selection = [c for c in shapes.columns if c != "geometry"]
    result_pd = result_pd[col_selection + ["population", "pixel_count"]]  # Filter columns
    result_pd.columns = result_pd.columns.str.upper()

    # Log any administrative levels with no population data
    no_data = result_pd[result_pd["POPULATION"] == 0]
    if not no_data.empty:
        for _, row in no_data.iterrows():
            row_str = ", ".join(f"{col}={val}" for col, val in row.items())
            current_run.log_warning(f"Row with no population data: {row_str}")

    output_dir.mkdir(parents=True, exist_ok=True)
    result_pd.to_csv(output_dir / f"{tif_file_path.stem}.csv", index=False)
    result_pd.to_parquet(output_dir / f"{tif_file_path.stem}.parquet", index=False)
    current_run.log_info(
        f"Aggregated population data saved under: {output_dir / f'{tif_file_path.stem}.csv'}"
    )
    return output_dir / f"{tif_file_path.stem}.parquet"


def load_shapes(shapes_path: Path) -> gpd.GeoDataFrame:
    """Load shapes from a file into a GeoDataFrame.

    Parameters
    ----------
    shapes_path : Path
        Path to the shape file (GeoJSON, Shapefile, etc.).

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the shapes.
    """
    try:
        shapes = gpd.read_file(shapes_path)
        if shapes.empty:
            raise ValueError("The shapes file is empty")
        return shapes
    except Exception as e:
        raise ValueError(f"Error loading shapes from {shapes_path}: {e}") from e


def write_to_db(file_path: Path, table_name: str) -> None:
    """Write the dataframe to a DB table.

    Parameters
    ----------
    file_path : Path
        The path to the parquet file to read data from.
    table_name : str
        The name of the table to write to.
    """
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        raise ValueError(f"Error loading data from {file_path}: {e}") from e

    try:
        df.to_sql(
            table_name,
            con=create_engine(workspace.database_url),
            if_exists="replace",
            index=False,
            chunksize=1000,
        )
        current_run.log_info(f"Data written to DB table {table_name}")
    except Exception as e:
        raise ValueError(f"Error writing data to DB table {table_name}: {e}") from e


if __name__ == "__main__":
    wpop_extract_population()
