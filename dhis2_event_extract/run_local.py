#!/usr/bin/env python3
"""Local test runner for the DHIS2 event extract pipeline."""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

import pipeline as pipeline_module

def create_mock_connection(connection_name: str) -> MagicMock:
    """Create a mock DHIS2Connection from workspace.yaml."""
    import yaml
    
    # Load workspace.yaml
    workspace_path = Path(__file__).parent / "workspace.yaml"
    with open(workspace_path, 'r') as f:
        workspace_config = yaml.safe_load(f)
    
    # Get connection config
    conn_config = workspace_config["connections"][connection_name]
    
    # Create mock connection
    mock_conn = MagicMock()
    mock_conn.url = conn_config["url"]
    mock_conn.username = conn_config["username"]
    mock_conn.password = conn_config["password"]
    
    return mock_conn

def create_mock_workspace():
    """Create a mock workspace."""
    mock_workspace = MagicMock()
    mock_workspace.files_path = str(Path(__file__).parent / "workspace")
    return mock_workspace

def create_mock_current_run():
    """Create a mock current_run."""
    mock_run = MagicMock()
    mock_run.log_info = lambda msg: print(f"INFO: {msg}")
    mock_run.log_error = lambda msg: print(f"ERROR: {msg}")
    mock_run.add_file_output = lambda path: print(f"OUTPUT: {path}")
    return mock_run

def main():
    """Run the pipeline with parameters from parameters.json."""
    # Load parameters
    params_path = Path(__file__).parent / "parameters.json"
    with open(params_path, 'r') as f:
        params = json.load(f)
    
    print(f"Loading parameters from {params_path}")
    print(f"Parameters: {json.dumps(params, indent=2)}")
    
    # Create mocks
    source_connection = create_mock_connection(params["source_connection"])
    
    # Patch the global objects
    pipeline_module.workspace = create_mock_workspace()
    pipeline_module.current_run = create_mock_current_run()
    
    # Ensure workspace directory exists
    workspace_dir = Path(__file__).parent / "workspace"
    workspace_dir.mkdir(exist_ok=True)
    
    try:
        # Run the pipeline
        result = pipeline_module.dhis2_event_extract(
            source_connection=source_connection,
            program=params["program"],
            program_stage=params.get("program_stage"),
            org_units=params.get("org_units"),
            status=params.get("status", "COMPLETED"),
            since_date=params.get("since_date"),
            output_format=params.get("output_format", "parquet"),
            output_path=params.get("output_path")
        )
        
        print("\n" + "="*50)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*50)
        print(f"Result: {json.dumps(result, indent=2)}")
        
    except Exception as e:
        print(f"\nERROR: Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()