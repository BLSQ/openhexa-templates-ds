import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from build_dataset.pipeline import parse_period_column


class TestBuildDataset:
    """Test class containing all unit tests for the `parse_period_column` function.

    These tests ensure that the function correctly parses and standardizes the 'period'
    column in various formats, while raising appropriate exceptions for invalid inputs.
    """

    def test_not_dataframe(self):
        """Ensure that passing a non-DataFrame input raises a RuntimeError."""
        with pytest.raises(RuntimeError, match="df is not of type pd.DataFrame"):  # noqa: RUF043
            parse_period_column("not_a_df")

    def test_no_period_column(self):
        """Verify that DataFrames without a 'period' column are returned unchanged."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = parse_period_column(df.copy())
        assert_frame_equal(result, df)

    def test_parse_date_format(self):
        """Check parsing when 'period' contains YYYY-MM-DD formatted strings."""
        df = pd.DataFrame({"period": ["2024-08-15", "2024-08-16"]})
        result = parse_period_column(df.copy())
        assert pd.api.types.is_datetime64_any_dtype(result["period"])
        assert result["period"].iloc[0] == pd.Timestamp("2024-08-15")

    def test_parse_year_month_format(self):
        """Check parsing when 'period' contains YYYYMM formatted strings."""
        df = pd.DataFrame({"period": ["202408", "202409"]})
        result = parse_period_column(df.copy())
        assert pd.api.types.is_datetime64_any_dtype(result["period"])
        assert result["period"].iloc[0] == pd.Timestamp("2024-08-01")

    def test_parse_year_format(self):
        """Check parsing when 'period' contains YYYY formatted strings."""
        df = pd.DataFrame({"period": ["2024", "2025"]})
        result = parse_period_column(df.copy())
        assert pd.api.types.is_datetime64_any_dtype(result["period"])
        assert result["period"].iloc[0] == pd.Timestamp("2024-01-01")

    def test_parse_quarter_format(self):
        """Check parsing when 'period' contains custom quarter format (e.g., '2024Q2')."""
        df = pd.DataFrame({"period": ["2024Q2", "2025Q4"]})
        result = parse_period_column(df.copy())
        assert pd.api.types.is_datetime64_any_dtype(result["period"])
        assert result["period"].iloc[0] == pd.Timestamp("2024-04-01")
        assert result["period"].iloc[1] == pd.Timestamp("2025-10-01")
