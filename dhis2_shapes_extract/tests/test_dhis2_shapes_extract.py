import json

import geopandas as gpd
import polars as pl
from shapely.geometry import shape

from dhis2_shapes_extract.pipeline import transform_shapes

"""
We only test the core geometry transformations functionality.
"""


def test_geometries_polygons_transform():  # noqa: D103
    # GeoJSON strings
    polygon1 = json.dumps(
        {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}
    )
    polygon2 = json.dumps(
        {"type": "Polygon", "coordinates": [[[2, 0], [3, 0], [3, 1], [2, 1], [2, 0]]]}
    )
    multipolygon1 = json.dumps(
        {
            "type": "MultiPolygon",
            "coordinates": [
                [[[4, 0], [5, 0], [5, 1], [4, 1], [4, 0]]],
                [[[4.5, 0.5], [4.7, 0.5], [4.7, 0.7], [4.5, 0.7], [4.5, 0.5]]],
            ],
        }
    )
    multipolygon2 = json.dumps(
        {"type": "MultiPolygon", "coordinates": [[[[6, 0], [7, 0], [7, 1], [6, 1], [6, 0]]]]}
    )

    # Create dummy pyramid result from DHIS2 get_organisation_unit()
    pyramid = pl.DataFrame(
        {
            "level_1_id": ["L1A", "L1B", "L1C", "L1D"],
            "level_1_name": ["Region A", "Region B", "Region C", "Region D"],
            "level_2_id": ["L2A", "L2B", "L2C", "L2D"],
            "level_2_name": ["District A", "District B", "District C", "District D"],
            "geometry": [polygon1, polygon2, multipolygon1, multipolygon2],
        }
    )
    shapes = transform_shapes(pyramid)
    assert shapes.shape == (4, 5), "Expected 4 rows and 5 columns after transformation"
    assert isinstance(shapes, gpd.GeoDataFrame), "shapes should be a GeoDataFrame"
    assert all(shapes.columns == pyramid.columns), "Expected columns to match input columns"
    assert isinstance(shapes["geometry"].dtype, gpd.array.GeometryDtype), (
        "geometry column has wrong dtype"
    )
    assert shapes.geometry.notnull().all(), "All rows should have valid geometry"


def test_geometries_polygons_and_nulls_transform():
    """Test transformation of polygons and multipolygons with some null geometries."""
    # GeoJSON strings
    polygon1 = None
    polygon2 = json.dumps(
        {"type": "Polygon", "coordinates": [[[2, 0], [3, 0], [3, 1], [2, 1], [2, 0]]]}
    )
    multipolygon1 = None
    multipolygon2 = json.dumps(
        {"type": "MultiPolygon", "coordinates": [[[[6, 0], [7, 0], [7, 1], [6, 1], [6, 0]]]]}
    )

    # Create dummy pyramid result from DHIS2 get_organisation_unit()
    pyramid = pl.DataFrame(
        {
            "level_1_id": ["L1A", "L1B", "L1C", "L1D"],
            "level_1_name": ["Region A", "Region B", "Region C", "Region D"],
            "level_2_id": ["L2A", "L2B", "L2C", "L2D"],
            "level_2_name": ["District A", "District B", "District C", "District D"],
            "geometry": [polygon1, polygon2, multipolygon1, multipolygon2],
        }
    )
    shapes = transform_shapes(pyramid)
    assert shapes.shape == (4, 5), "Expected 4 rows and 5 columns after transformation"
    assert isinstance(shapes, gpd.GeoDataFrame), "shapes should be a GeoDataFrame"
    assert all(shapes.columns == pyramid.columns), "Expected columns to match input columns"
    assert isinstance(shapes["geometry"].dtype, gpd.array.GeometryDtype), (
        "geometry column has wrong dtype"
    )
    assert shapes.geometry.isnull().sum() == 2, "there should be 2 null geometries"


def test_null_geometries_transform():
    """Test transformation of all null geometries."""
    # Create dummy pyramid result from DHIS2 get_organisation_unit()
    pyramid = pl.DataFrame(
        {
            "level_1_id": ["L1A", "L1B", "L1C"],
            "level_1_name": ["Region A", "Region B", "Region C"],
            "level_2_id": ["L2A", "L2B", "L2C"],
            "level_2_name": ["District A", "District B", "District C"],
            "geometry": [None, None, None],
        }
    )
    shapes = transform_shapes(pyramid)
    assert shapes.shape == (3, 5), "Expected 3 rows and 5 columns after transformation"
    assert isinstance(shapes, gpd.GeoDataFrame), "shapes should be a GeoDataFrame"
    assert all(shapes.columns == pyramid.columns), "Expected columns to match input columns"
    assert isinstance(shapes["geometry"].dtype, gpd.array.GeometryDtype), (
        "geometry column has wrong dtype"
    )
    assert shapes.geometry.isnull().all(), "All rows should have null geometry"


def test_invalid_geometries_transform():
    """Test transformation of invalid geometries."""
    valid_polygon = json.dumps(
        {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}
    )
    # Self-intersecting polygon (invalid)
    invalid_polygon = json.dumps(
        {"type": "Polygon", "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]]}
    )
    point = json.dumps({"type": "Point", "coordinates": [0.5, 0.5]})

    # Create Polars DataFrame
    pyramid = pl.DataFrame(
        {
            "level_1_id": ["L1A", "L1B", "L1C"],
            "level_1_name": ["Region A", "Region B", "Region C"],
            "level_2_id": ["L2A", "L2B", "L2C"],
            "level_2_name": ["District A", "District B", "District C"],
            "geometry": [valid_polygon, invalid_polygon, point],
        }
    )
    geom = shape(json.loads(invalid_polygon))
    assert not geom.is_valid, "The invalid polygon should be detected as invalid"
    shapes = transform_shapes(pyramid)
    invalid_geoms = [g for g in shapes.geometry if not g.is_valid]
    assert len(invalid_geoms) == 1, "There should be exactly 1 invalid geometry"
