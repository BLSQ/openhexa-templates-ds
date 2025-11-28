import polars as pl
from pathlib import Path
from dhis2_shapes_extract.pipeline import transform_shapes

# import os
# os.chdir("dhis2_shapes_extract")
# os.getcwd()


# we are going to test only core functionalities here such as geometry manipulations.
def test_geometries_lvl_2():  # noqa: D103
    pyramid = pl.read_parquet(
        Path(r"dhis2_shapes_extract/tests/test_data/test_pyramid_lvl_2.parquet")
    )
    shapes = transform_shapes(pyramid)
    assert shapes.shape == (5, 5), "Expected 5 rows and 5 columns after transformation"
