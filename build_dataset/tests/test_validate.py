import pandas as pd
import pytest

from build_dataset.validate import validate_data


def test_validate_data():
    """Test the `validate_data` function to ensure it correctly validates input DataFrames.

    This test performs two main checks:
    1. Verifies that a `RuntimeError` is raised when an empty DataFrame is passed to `validate_data`
    2. Confirms that no error is raised when a non-empty DataFrame is provided.

    The test ensures the validation logic correctly distinguishes between valid and invalid
    (empty) datasets, which is important for maintaining data integrity in the dataset build process
    """
    # check that the function raises error when 
    # empty dataframe is passed
    df = pd.DataFrame([])
    with pytest.raises(RuntimeError) as execution_info:
        validate_data(df)
    assert execution_info.type is RuntimeError

    # check that the function does not raise error
    # when the dataframe is not empty
    df2 = pd.DataFrame([
        {"col1": "value00", "col2": "value01"},
        {"col1": "value10", "col2": "value11"}
    ])
    validate_data(df2)
