from datetime import UTC, datetime

import polars as pl
import pytest

from dhis2_extract_data_elements.validate import validate_data
from dhis2_extract_data_elements.validation_config import expected_columns


def test_validate_data():
    """Some random test."""
    # Check that the function raises error when it
    # detects an empty dataframe

    schema = {
        col["name"]: (
            pl.Datetime("ms", "UTC")
            if col["type"].startswith("Datetime")
            else getattr(pl, col["type"])()
        )
        for col in expected_columns
    }

    df = pl.DataFrame(schema=schema)
    with pytest.raises(RuntimeError) as execution_info:
        validate_data(df)
    assert execution_info.type is RuntimeError

    # check raise of error when datatype is not correct
    df2 = pl.DataFrame(
        {
            "data_element_id": [1, 2],  # test case
            "data_element_name": ["Malaria Cases", "HIV Tests"],
            "organisation_unit_id": ["OU_001", "OU_002"],
            "category_option_combo_id": ["CAT_01", "CAT_02"],
            "attribute_option_combo_id": ["ATT_01", "ATT_02"],
            "category_option_combo_name": ["Male", "Female"],
            "period": ["202501", "202502"],
            "value": ["12", "34"],
            "level_1_id": ["L1_001", "L1_001"],
            "level_2_id": ["L2_010", "L2_011"],
            "level_3_id": ["L3_100", "L3_101"],
            "level_4_id": ["L4_200", "L4_201"],
            "level_1_name": ["Country A", "Country A"],
            "level_2_name": ["Region X", "Region Y"],
            "level_3_name": ["District P", "District Q"],
            "level_4_name": ["Facility 1", "Facility 2"],
            "created": [
                datetime(2025, 1, 1, 10, 0, tzinfo=UTC),
                datetime(2025, 2, 1, 10, 0, tzinfo=UTC),
            ],
            "last_updated": [
                datetime(2025, 3, 1, 10, 0, tzinfo=UTC),
                datetime(2025, 4, 1, 10, 0, tzinfo=UTC),
            ],
        }
    )
    df2 = df2.with_columns(
        [
            pl.col("created").cast(pl.Datetime("ms", "UTC")),
            pl.col("last_updated").cast(pl.Datetime("ms", "UTC")),
        ]
    )
    with pytest.raises(RuntimeError) as execution_info:
        validate_data(df2)

    # check raise of error when number of characters is not correct
    df2 = pl.DataFrame(
        {
            "data_element_id": ["DE001", "DE002"],
            "data_element_name": ["Malaria Cases", "HIV Tests"],
            "organisation_unit_id": ["OU_001", "OU_002"],
            "category_option_combo_id": ["CAT_01", "CAT_02"],
            "attribute_option_combo_id": ["ATT_01", "ATT_02"],
            "category_option_combo_name": ["Male", "Female"],
            "period": ["2025013", "202502"],  # test case
            "value": ["12", "334"],  # test case
            "level_1_id": ["L1_001", "L1_001"],
            "level_2_id": ["L2_010", "L2_011"],
            "level_3_id": ["L3_100", "L3_101"],
            "level_4_id": ["L4_200", "L4_201"],
            "level_1_name": ["Country A", "Country A"],
            "level_2_name": ["Region X", "Region Y"],
            "level_3_name": ["District P", "District Q"],
            "level_4_name": ["Facility 1", "Facility 2"],
            "created": [
                datetime(2025, 1, 1, 10, 0, tzinfo=UTC),
                datetime(2025, 2, 1, 10, 0, tzinfo=UTC),
            ],
            "last_updated": [
                datetime(2025, 3, 1, 10, 0, tzinfo=UTC),
                datetime(2025, 4, 1, 10, 0, tzinfo=UTC),
            ],
        }
    )
    df2 = df2.with_columns(
        [
            pl.col("created").cast(pl.Datetime("ms", "UTC")),
            pl.col("last_updated").cast(pl.Datetime("ms", "UTC")),
        ]
    )
    with pytest.raises(RuntimeError) as execution_info:
        validate_data(df2)

    # check raise of error when values can not be converted to integers
    df2 = pl.DataFrame(
        {
            "data_element_id": ["DE001", "DE002"],
            "data_element_name": ["Malaria Cases", "HIV Tests"],
            "organisation_unit_id": ["OU_001", "OU_002"],
            "category_option_combo_id": ["CAT_01", "CAT_02"],
            "attribute_option_combo_id": ["ATT_01", "ATT_02"],
            "category_option_combo_name": ["Male", "Female"],
            "period": ["202503m", "202502"],  # test case
            "value": ["12", "34x"],  # test case
            "level_1_id": ["L1_001", "L1_001"],
            "level_2_id": ["L2_010", "L2_011"],
            "level_3_id": ["L3_100", "L3_101"],
            "level_4_id": ["L4_200", "L4_201"],
            "level_1_name": ["Country A", "Country A"],
            "level_2_name": ["Region X", "Region Y"],
            "level_3_name": ["District P", "District Q"],
            "level_4_name": ["Facility 1", "Facility 2"],
            "created": [
                datetime(2025, 1, 1, 10, 0, tzinfo=UTC),
                datetime(2025, 2, 1, 10, 0, tzinfo=UTC),
            ],
            "last_updated": [
                datetime(2025, 3, 1, 10, 0, tzinfo=UTC),
                datetime(2025, 4, 1, 10, 0, tzinfo=UTC),
            ],
        }
    )
    df2 = df2.with_columns(
        [
            pl.col("created").cast(pl.Datetime("ms", "UTC")),
            pl.col("last_updated").cast(pl.Datetime("ms", "UTC")),
        ]
    )
    with pytest.raises(RuntimeError) as execution_info:
        validate_data(df2)
