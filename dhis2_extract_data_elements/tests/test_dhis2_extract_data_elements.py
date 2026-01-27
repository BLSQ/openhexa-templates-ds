import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from ..pipeline import RequestParams, validate, validate_parameters
from ..validate import DataValidationError


def test_validate_parameters() -> None:
    """Ensure we are raising errors when invalid parameters are provided."""
    valid_params = RequestParams(
        data_elements=["de1", "de2"],
        data_element_groups=None,
        organisation_units=["ou1"],
        organisation_unit_groups=None,
        include_children=False,
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
    validate_parameters(valid_params).run()

    # wrong format for start_date
    with pytest.raises(ValueError, match="not in ISO format"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=None,
                organisation_units=["ou1"],
                organisation_unit_groups=None,
                include_children=False,
                start_date="01-01-2024",
                end_date=None,
            )
        ).run()

    # end_date before start_date
    with pytest.raises(ValueError, match="after end date"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=None,
                organisation_units=["ou1"],
                organisation_unit_groups=None,
                include_children=False,
                start_date="2024-12-31",
                end_date="2024-01-01",
            )
        ).run()

    # no org unit provided
    with pytest.raises(ValueError, match="No organisation units"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=None,
                organisation_units=None,
                organisation_unit_groups=None,
                include_children=False,
                start_date="2024-01-01",
                end_date=None,
            )
        ).run()

    # no data elements provided
    with pytest.raises(ValueError, match="not both"):
        validate_parameters(
            RequestParams(
                data_elements=["de1"],
                data_element_groups=["deg1"],
                organisation_units=["ou1"],
                organisation_unit_groups=None,
                include_children=False,
                start_date="2024-01-01",
                end_date=None,
            )
        ).run()


def test_validate_data() -> None:
    """Test data validation against expected schema."""
    sample_file = Path(__file__).parent / "data" / "sample_output.parquet"
    df = pl.read_parquet(sample_file)

    # should not fail
    validate(df).run()

    # empty dataframe
    with pytest.raises(DataValidationError):
        validate(df.head(0)).run()

    # unexpected column in dataframe
    df_extra = df.with_columns(pl.lit("x").alias("unexpected_col"))
    with pytest.raises(DataValidationError):
        validate(df_extra).run()
