import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import pipeline as pipeline_module


def test_load_and_validate_mappings_with_valid_file(sample_mapping) -> None:
    """Test load_and_validate_mappings helper function with valid file."""
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    # Mock workspace
    pipeline_module.workspace = MagicMock()
    pipeline_module.workspace.files_path = str(sample_mapping.parent)
    
    # Call helper function directly
    result = pipeline_module.load_and_validate_mappings(str(sample_mapping))
    
    # Verify result structure
    assert result is not None
    assert isinstance(result, dict)
    assert "dataElements" in result
    assert "categoryOptionCombos" in result
    assert "orgUnits" in result
    assert isinstance(result["dataElements"], dict)
    assert isinstance(result["categoryOptionCombos"], dict)
    assert isinstance(result["orgUnits"], dict)
    
    # Verify content matches sample mapping
    assert result["dataElements"]["SRC_DE"] == "TGT_DE"
    assert result["categoryOptionCombos"]["SRC_COC"] == "TGT_COC"


def test_load_and_validate_mappings_with_missing_file() -> None:
    """Test load_and_validate_mappings helper function with missing file."""
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    # Mock workspace
    pipeline_module.workspace = MagicMock()
    pipeline_module.workspace.files_path = "/tmp"
    
    # This should raise a ValueError due to missing file
    with pytest.raises(ValueError, match="Mapping file not found"):
        pipeline_module.load_and_validate_mappings("/nonexistent/mapping.json")


def test_load_and_validate_mappings_with_invalid_json(tmp_path) -> None:
    """Test load_and_validate_mappings helper function with invalid JSON."""
    
    # Create invalid JSON file
    invalid_file = tmp_path / "invalid.json"
    invalid_file.write_text("{invalid json content", encoding="utf-8")
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    # Mock workspace
    pipeline_module.workspace = MagicMock()
    pipeline_module.workspace.files_path = str(tmp_path)
    
    # This should raise a ValueError due to invalid JSON
    with pytest.raises(ValueError, match="Invalid JSON in mapping file"):
        pipeline_module.load_and_validate_mappings(str(invalid_file))


def test_load_and_validate_mappings_missing_sections(tmp_path) -> None:
    """Test load_and_validate_mappings helper function with missing sections."""
    
    # Create mapping file missing dataElements section
    invalid_mapping = {"categoryOptionCombos": {"COC1": "COC2"}}
    invalid_file = tmp_path / "missing_sections.json"
    invalid_file.write_text(json.dumps(invalid_mapping), encoding="utf-8")
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    # Mock workspace
    pipeline_module.workspace = MagicMock()
    pipeline_module.workspace.files_path = str(tmp_path)
    
    # This should raise a ValueError due to missing dataElements section
    with pytest.raises(ValueError, match="must contain 'dataElements'"):
        pipeline_module.load_and_validate_mappings(str(invalid_file))


def test_load_and_validate_mappings_with_complex_mapping(tmp_path) -> None:
    """Test load_and_validate_mappings helper function with complex mapping."""
    
    # Create complex mapping file similar to real-world usage
    complex_mapping = {
        "dataElements": {
            "fbfJHSPpUQD": "Uvn6LCg7dVU",  # ANC 1st visit
            "cYeuwXTCPkU": "sB79w2hiLp8",  # ANC 4th visit
            "dwEq7wi6nXV": "F3ogKBuviRA",  # Delivery by skilled attendant
        },
        "categoryOptionCombos": {
            "HllvX50cXC0": "rQLFnNXXIL0",  # Default
            "xYerKDKCefk": "Gmbgme7z9BF",  # <15 years
            "WLG0EVE0mkV": "pq2XI5kz2BY",  # 15-19 years
        },
        "orgUnits": {
            "O6uvpzGd5pu": "O6uvpzGd5pu",  # Bo
            "fdc6uOvgoji": "fdc6uOvgoji",  # Bombali
            "lc3eMKXaEfw": "lc3eMKXaEfw",  # Bonthe
        }
    }
    
    complex_file = tmp_path / "complex_mapping.json"
    complex_file.write_text(json.dumps(complex_mapping), encoding="utf-8")
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    # Mock workspace
    pipeline_module.workspace = MagicMock()
    pipeline_module.workspace.files_path = str(tmp_path)
    
    # Call helper function directly
    result = pipeline_module.load_and_validate_mappings(str(complex_file))
    
    # Verify result structure and content
    assert result is not None
    assert isinstance(result, dict)
    assert len(result["dataElements"]) == 3
    assert len(result["categoryOptionCombos"]) == 3
    assert len(result["orgUnits"]) == 3
    
    # Verify specific mappings
    assert result["dataElements"]["fbfJHSPpUQD"] == "Uvn6LCg7dVU"
    assert result["categoryOptionCombos"]["HllvX50cXC0"] == "rQLFnNXXIL0"
    assert result["orgUnits"]["O6uvpzGd5pu"] == "O6uvpzGd5pu"