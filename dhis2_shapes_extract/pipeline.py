import json
from pathlib import Path

import geopandas as gpd
import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_units
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry


@pipeline(name="dhis2 shapes extract")
@parameter(
    "dhis2_connection",
    name="DHIS2 instance",
    type=DHIS2Connection,
    help="Credentials for the DHIS2 instance connection",
    required=True,
)
@parameter(
    "org_level",
    name="Organisational unit level",
    type=int,
    help="If not specified base level is retrieved",
    required=False,
    default=None,
)
@parameter(
    "output_path",
    name="Output path",
    type=str,
    help="Output path for the shapes",
    required=False,
    default=None,
)
def dhis2_shapes_extract(
    dhis2_connection: DHIS2Connection, org_level: int, output_path: str
) -> None:
    """Main pipeline function to extract shapes from a DHIS2 instance."""
    current_run.log_info("Shapes pipeline started")

    if output_path is None:
        current_run.log_info("Output path not specified, using default")
        output_path = Path(workspace.files_path).joinpath("dhis2_shapes_extract")
    else:
        output_path = Path(output_path)

    if not output_path.exists():
        current_run.log_info(f"Output path {output_path} does not exist, creating it")
        Path.mkdir(output_path, exist_ok=True)

    try:
        # connect to DHIS2
        dhis2 = get_dhis2_client(dhis2_connection)

        # Get shapes table
        shapes = extract_shapes(dhis2, org_level)

        # Save shapes to output path
        save_shapes(shapes, output_path, filename="shapes.gpkg")

    except Exception as e:
        current_run.log_error(f"Pipeline stopped: {e}")


# task 1
def get_dhis2_client(dhis2_connection: DHIS2Connection) -> DHIS2:
    """Get the DHIS2 connection.

    Returns:
        DHIS2: An instance of the DHIS2 client connected to the specified DHIS2 instance.
    """
    try:
        # Initialize DHIS2 connection
        dhis2_client = DHIS2(dhis2_connection, cache_dir=Path(workspace.files_path) / ".cache")
        current_run.log_info(f"Successfully connected to DHIS2 instance {dhis2_connection.url}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to {dhis2_connection.url} error: {e}") from e


# task 2
@dhis2_shapes_extract.task
def extract_shapes(dhis2_client: DHIS2, org_level: int) -> gpd.GeoDataFrame:
    """Retrieves organizational unit shapes from a DHIS2 instance.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing organizational unit shapes.
    """
    if org_level is None:
        current_run.log_info("No org level specified, using base level 1")
        org_level = 1

    try:
        df_polars = get_organisation_units(dhis2_client, max_level=org_level)

        # lets remove the empty names at this level
        df_polars_filtered = df_polars.filter(pl.col(f"level_{org_level}_id").is_not_null())

        # Convert GeoJSON strings to Shapely geometries
        def geojson_to_shapely(geojson_str: any) -> BaseGeometry:
            geojson = json.loads(geojson_str)
            return shape(geojson)

        # Apply the conversion to the geometry column
        shapely_geoms = (
            df_polars_filtered["geometry"]
            .map_elements(geojson_to_shapely, return_dtype=pl.Object)
            .to_list()
        )

        # Convert to GeoPandas DataFrame
        # NOTE: The geometry column is expected to be named 'geometry' in the GeoDataFrame
        return gpd.GeoDataFrame(
            df_polars_filtered.drop("geometry").to_pandas(),
            geometry=shapely_geoms,
            crs="EPSG:4326",  # Assuming WGS84 coordinate system
        )

    except Exception as e:
        current_run.log_error(f"Error while extracting shapes: {e}")
        return None


# task 3
@dhis2_shapes_extract.task
def save_shapes(shapes: gpd.GeoDataFrame, output_path: Path, filename: str) -> None:
    """Saves the shapes to the specified output path."""
    try:
        shapes.to_file(Path(output_path).joinpath(filename), driver="GPKG")
        current_run.log_info(f"GeoDataFrame successfully saved to {output_path}")
    except PermissionError:
        current_run.log_error("Error: You don't have permission to access this file.")
    except OSError as e:
        current_run.log_error(f"An I/O error occurred: {e}")
    except Exception as e:
        current_run.log_error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    dhis2_shapes_extract()
