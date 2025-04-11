import json
from datetime import datetime
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


@pipeline("dhis2_shapes_extract")
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
        output_path = Path(workspace.files_path).joinpath("pipelines", "dhis2_shapes_extract")
        current_run.log_info(f"Output path not specified, using default {output_path}")
    else:
        output_path = Path(output_path)

    if org_level is None:
        current_run.log_info("No org level specified, using base level 1")
        org_level = 1

    try:
        dhis2_client = get_dhis2_client(dhis2_connection)

        shapes = extract_shapes(dhis2_client, org_level)

        save_shapes(
            shapes=shapes,
            output_path=output_path,
            filename=f"shapes_level{org_level}_{datetime.now().strftime('%Y_%m_%d_%H%M')}.gpkg",
        )

    except Exception as e:
        current_run.log_error(f"Pipeline stopped: {e}")


@dhis2_shapes_extract.task
def get_dhis2_client(dhis2_connection: DHIS2Connection) -> DHIS2:
    """Get the DHIS2 connection.

    Returns:
        DHIS2: An instance of the DHIS2 client connected to the specified DHIS2 instance.
    """
    try:
        dhis2_client = DHIS2(dhis2_connection, cache_dir=Path(workspace.files_path) / ".cache")
        current_run.log_info(f"Successfully connected to DHIS2 instance {dhis2_connection.url}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to {dhis2_connection.url} error: {e}") from e


@dhis2_shapes_extract.task
def extract_shapes(
    dhis2_client: DHIS2, org_level: int, geometry_column: str = "geometry"
) -> gpd.GeoDataFrame:
    """Retrieves organizational unit shapes from a DHIS2 instance.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing organizational unit shapes.
    """
    try:
        df_pyramid = get_organisation_units(dhis2_client, max_level=org_level)
        df_pyramid = df_pyramid.filter(pl.col("level") == org_level).drop(
            ["id", "name", "level", "opening_date", "closed_date"]
        )
        current_run.log_info(f"{df_pyramid.shape[0]} Shapes extracted for org level {org_level}")

        if len(df_pyramid) == 0:
            raise ValueError("No shapes found for the specified org level")

        # Convert GeoJSON strings to Shapely geometries
        def geojson_to_shapely(geojson_str: any) -> BaseGeometry:
            geojson = json.loads(geojson_str)
            return shape(geojson)

        # Apply the conversion to the geometry column
        shapely_geoms = (
            df_pyramid[geometry_column]
            .map_elements(geojson_to_shapely, return_dtype=pl.Object)
            .to_list()
        )

        # Convert to GeoPandas DataFrame
        return gpd.GeoDataFrame(
            df_pyramid.drop(geometry_column).to_pandas(),
            geometry=shapely_geoms,
            crs="EPSG:4326",  # NOTE: Assuming WGS84 coordinate system
        )

    except Exception as e:
        raise Exception(f"Error while extracting shapes: {e}") from e


@dhis2_shapes_extract.task
def save_shapes(shapes: gpd.GeoDataFrame, output_path: Path, filename: str) -> None:
    """Saves the shapes to the specified output path."""
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(f"Error creating output directory {output_path}: {e}") from e

    try:
        output_fname = Path(output_path).joinpath(filename)
        shapes.to_file(output_fname, driver="GPKG")
        current_run.log_info(f"GeoDataFrame successfully saved to {output_fname}")
    except PermissionError as e:
        raise PermissionError("Error: You don't have permission to access this file.") from e
    except OSError as e:
        raise OSError(f"An I/O error occurred: {e}") from e
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}") from e


if __name__ == "__main__":
    dhis2_shapes_extract()
