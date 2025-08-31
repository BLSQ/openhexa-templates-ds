#!/usr/bin/env python3
"""Test individual pipeline functions."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent))

import pipeline as pipeline_module
from openhexa.toolbox.dhis2 import DHIS2

def test_connection():
    """Test the validate_connection function directly."""
    # Create a real DHIS2 connection
    mock_conn = MagicMock()
    mock_conn.url = "https://play.im.dhis2.org/stable-2-39-10-1"
    mock_conn.username = "admin"
    mock_conn.password = "district"
    
    # Mock current_run
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    try:
        client = pipeline_module.validate_connection(mock_conn, "IpHINAT79UW")
        print(f"✅ Connection successful: {type(client)}")
        return client
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def test_query_params():
    """Test query parameter building."""
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    
    params = pipeline_module.build_query_params(
        program="IpHINAT79UW",
        program_stage=None,
        org_units=["DiszpKrYNg8"],
        status="COMPLETED", 
        since_date="2020-01-01"
    )
    print(f"✅ Query params: {json.dumps(params, indent=2)}")
    return params

if __name__ == "__main__":
    print("Testing individual functions...\n")
    
    print("1. Testing connection validation:")
    client = test_connection()
    
    print("\n2. Testing query parameter building:")
    params = test_query_params()
    
    if client and params:
        print("\n3. Testing event fetching (first page only):")
        try:
            # Limit to 5 events for testing
            params["pageSize"] = "5"
            events = pipeline_module.fetch_events(client, params)
            print(f"✅ Fetched {len(events)} events")
            
            if events:
                print("\n4. Testing event transformation:")
                df = pipeline_module.transform_events(events)
                print(f"✅ Transformed to DataFrame: {len(df)} rows, {len(df.columns)} columns")
                print(f"Columns: {list(df.columns)}")
        except Exception as e:
            print(f"❌ Event fetching failed: {e}")