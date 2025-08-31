from pathlib import Path
from unittest.mock import patch

import pipeline as pipeline_module
import polars as pl
import pytest


def test_write_output_csv_format(tmp_path) -> None:
    """Test CSV output format."""
    df = pl.DataFrame({
        "event": ["E1", "E2"],
        "program": ["P1", "P1"],
        "orgUnit": ["O1", "O2"],
        "status": ["COMPLETED", "ACTIVE"],
        "DE1_value": ["Value1", "Value2"]
    })
    
    with patch("pipeline.workspace") as mock_workspace:
        mock_workspace.files_path = tmp_path
        
        output_path = pipeline_module.write_output(
            df, "TEST_PROGRAM", "csv", str(tmp_path / "test_output.csv")
        )
        
        assert output_path.endswith(".csv")
        assert Path(output_path).exists()
        
        # Verify CSV content
        written_df = pl.read_csv(output_path)
        assert len(written_df) == 2
        assert "event" in written_df.columns
        assert written_df["event"][0] == "E1"


def test_write_output_parquet_format(tmp_path) -> None:
    """Test Parquet output format."""
    df = pl.DataFrame({
        "event": ["E1", "E2"],
        "program": ["P1", "P1"],
        "DE1_value": ["Value1", "Value2"]
    })
    
    with patch("pipeline.workspace") as mock_workspace:
        mock_workspace.files_path = tmp_path
        
        output_path = pipeline_module.write_output(
            df, "TEST_PROGRAM", "parquet", str(tmp_path / "test_output.parquet")
        )
        
        assert output_path.endswith(".parquet")
        assert Path(output_path).exists()
        
        # Verify Parquet content
        written_df = pl.read_parquet(output_path)
        assert len(written_df) == 2
        assert written_df["event"][0] == "E1"


def test_write_output_jsonl_format(tmp_path) -> None:
    """Test JSONL output format."""
    df = pl.DataFrame({
        "event": ["E1", "E2"],
        "program": ["P1", "P1"],
        "DE1_value": ["Value1", "Value2"]
    })
    
    with patch("pipeline.workspace") as mock_workspace:
        mock_workspace.files_path = tmp_path
        
        output_path = pipeline_module.write_output(
            df, "TEST_PROGRAM", "jsonl", str(tmp_path / "test_output.jsonl")
        )
        
        assert output_path.endswith(".jsonl")
        assert Path(output_path).exists()
        
        # Verify JSONL content
        written_df = pl.read_ndjson(output_path)
        assert len(written_df) == 2
        assert written_df["event"][0] == "E1"


def test_write_output_auto_generated_path() -> None:
    """Test auto-generated output path when none provided."""
    df = pl.DataFrame({
        "event": ["E1"],
        "program": ["P1"]
    })
    
    with patch("pipeline.workspace") as mock_workspace, \
         patch("pathlib.Path.mkdir"), \
         patch("polars.DataFrame.write_csv"):
        
        mock_workspace.files_path = Path("/tmp/workspace")
        
        output_path = pipeline_module.write_output(
            df, "TEST_PROGRAM", "csv", None
        )
        
        assert "dhis2_events_TEST_PROGRAM_" in output_path
        assert output_path.endswith(".csv")


def test_write_output_empty_dataframe(tmp_path) -> None:
    """Test writing empty DataFrame."""
    df = pl.DataFrame(schema={
        "event": pl.String,
        "program": pl.String,
        "status": pl.String
    })
    
    with patch("pipeline.workspace") as mock_workspace:
        mock_workspace.files_path = tmp_path
        
        output_path = pipeline_module.write_output(
            df, "TEST_PROGRAM", "csv", str(tmp_path / "empty_output.csv")
        )
        
        assert Path(output_path).exists()
        
        # Should still write headers for CSV
        written_df = pl.read_csv(output_path)
        assert len(written_df) == 0
        assert "event" in written_df.columns


def test_write_output_unsupported_format_raises_error() -> None:
    """Test that unsupported output format raises error."""
    df = pl.DataFrame({"event": ["E1"], "program": ["P1"]})
    
    with patch("pipeline.workspace") as mock_workspace, \
         patch("pathlib.Path.mkdir"):
        
        mock_workspace.files_path = Path("/tmp/workspace")
        
        with pytest.raises(ValueError, match="Unsupported output format"):
            pipeline_module.write_output(
                df, "TEST_PROGRAM", "unsupported_format", None
            )