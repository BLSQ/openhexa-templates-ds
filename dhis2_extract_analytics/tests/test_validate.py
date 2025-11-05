import polars as pl
import pytest

from dhis2_extract_analytics.validate import validate_data
from dhis2_extract_analytics.validation_config import expected_columns


def test_validate_data():
    """Test suite for the `validate_data` function.

    This test verifies the following scenarios:

    1. **Empty DataFrame Validation**
       - Ensures that `validate_data` raises a `RuntimeError` when an empty
         DataFrame is passed.
       - Confirms that the raised error message includes the phrase
         `"data_values is empty"`.

    2. **Valid DataFrame Validation**
       - Checks that `validate_data` executes successfully without raising any
         errors when provided with a correctly structured and populated
         DataFrame containing all expected columns and valid data types.

    3. **Invalid Data Type Validation**
       - Verifies that a `RuntimeError` is raised when a column (e.g.,
         `indicator_id`) has a data type that does not match the expected
         schema definition (e.g., integers instead of strings).
       - Confirms that the error message includes the phrase
         `"does not match expected type: String"`.

    The test ensures that the validation function behaves predictably for both
    valid and invalid datasets, improving reliability and data integrity
    in the data ingestion pipeline.
    """
    # check that the function raises error when
    # empty dataframe is passed
    df = pl.DataFrame([], schema=[col["name"] for col in expected_columns])
    with pytest.raises(
        RuntimeError, match=r"data_values\s+is\s+empty"
    ) as execution_info:
        validate_data(df)
    assert execution_info.type is RuntimeError

    # check that the function does not raise error
    # when the dataframe is not empty
    df2 = df = pl.DataFrame({
    "indicator_id": ["IND001", "IND002"],
    "indicator_name": ["Malaria Cases", "HIV Tests"],
    "organisation_unit_id": ["OU_001", "OU_002"],
    "period": ["202501", "202502"],
    "value": ["123", "456"],
    "level_1_id": ["L1_001", "L1_001"],
    "level_2_id": ["L2_010", "L2_011"],
    "level_3_id": ["L3_100", "L3_101"],
    "level_4_id": ["L4_200", "L4_201"],
    "level_1_name": ["Country A", "Country A"],
    "level_2_name": ["Region X", "Region Y"],
    "level_3_name": ["District P", "District Q"],
    "level_4_name": ["Facility 1", "Facility 2"],
    })
    validate_data(df2)

    # check that the function raises error 
    # when type of data is not correct
    df3 = df = pl.DataFrame({
    "indicator_id": [1, 2],  # test case
    "indicator_name": ["Malaria Cases", "HIV Tests"],
    "organisation_unit_id": ["OU_001", "OU_002"],
    "period": ["202501", "202502"],
    "value": ["123", "456"],
    "level_1_id": ["L1_001", "L1_001"],
    "level_2_id": ["L2_010", "L2_011"],
    "level_3_id": ["L3_100", "L3_101"],
    "level_4_id": ["L4_200", "L4_201"],
    "level_1_name": ["Country A", "Country A"],
    "level_2_name": ["Region X", "Region Y"],
    "level_3_name": ["District P", "District Q"],
    "level_4_name": ["Facility 1", "Facility 2"],
    })
    with pytest.raises(
        RuntimeError, match=r"does\s+not\s+match\s+expected\s+type:\s+String"
    ) as execution_info:
        validate_data(df3)
    assert execution_info.type is RuntimeError