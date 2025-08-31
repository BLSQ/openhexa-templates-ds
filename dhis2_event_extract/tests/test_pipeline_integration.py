from unittest.mock import MagicMock, patch

import pipeline as pipeline_module
import pytest
from openhexa.sdk import DHIS2Connection


@pytest.mark.integration
def test_full_pipeline_workflow_with_real_dhis2(dhis2_client, dhis2_env) -> None:
    """Test full pipeline workflow by calling helper functions in sequence."""
    
    try:
        # First check if DHIS2 is accessible
        dhis2_client.api.get("system/info")
    except Exception as e:
        pytest.skip(f"DHIS2 server not accessible: {e}")
    
    # Create mock connection
    mock_connection = MagicMock(spec=DHIS2Connection)
    mock_connection.url = dhis2_env["url"]
    mock_connection.username = dhis2_env["user"]
    mock_connection.password = dhis2_env["password"]
    
    try:
        # Step 1: Validate connection
        client = pipeline_module.validate_connection(mock_connection, "IpHINAT79UW")
        assert client is not None
        
        # Step 2: Build query parameters
        query_params = pipeline_module.build_query_params(
            program="IpHINAT79UW",
            program_stage=None,
            org_units=None,
            status="ALL",
            since_date=None,
        )
        assert query_params["program"] == "IpHINAT79UW"
        
        # Step 3: Fetch events (limit to small page for testing)
        query_params["pageSize"] = "5"  # Small page size for testing
        events = pipeline_module.fetch_events(client, query_params)
        assert isinstance(events, list)
        
        # Step 4: Transform events
        df = pipeline_module.transform_events(events)
        assert df is not None
        
        # Step 5: Mock workspace and test write_output
        with patch("pipeline.workspace") as mock_workspace, \
             patch("pathlib.Path.mkdir"), \
             patch("polars.DataFrame.write_parquet"):
            
            mock_workspace.files_path = "/tmp/workspace"
            output_path = pipeline_module.write_output(
                df, "IpHINAT79UW", "parquet", None
            )
            assert "dhis2_events_IpHINAT79UW_" in output_path
        
        # Step 6: Generate summary
        summary = pipeline_module.generate_summary(
            len(events), query_params, output_path, "IpHINAT79UW", "parquet"
        )
        assert summary["pipeline"] == "dhis2-event-extract"
        assert summary["total_events_extracted"] == len(events)
        
    except ValueError as e:
        if "not found" in str(e) or "does not exist" in str(e):
            pytest.skip(f"Test program/data not available on this server: {e}")
        else:
            raise


@pytest.mark.integration
def test_validate_connection_with_invalid_program(dhis2_client, dhis2_env) -> None:
    """Test connection validation with invalid program."""
    
    try:
        dhis2_client.api.get("system/info")
    except Exception as e:
        pytest.skip(f"DHIS2 server not accessible: {e}")
    
    mock_connection = MagicMock(spec=DHIS2Connection)
    mock_connection.url = dhis2_env["url"]
    mock_connection.username = dhis2_env["user"]
    mock_connection.password = dhis2_env["password"]
    
    with pytest.raises(ValueError, match="Program 'INVALID_PROGRAM' not found"):
        pipeline_module.validate_connection(mock_connection, "INVALID_PROGRAM")


def test_connection_error_handling() -> None:
    """Test connection error handling with invalid URL."""
    
    mock_connection = MagicMock(spec=DHIS2Connection)
    mock_connection.url = "https://invalid-dhis2-url.example.com"
    mock_connection.username = "invalid"
    mock_connection.password = "invalid"
    
    with pytest.raises(ValueError, match="Cannot connect to DHIS2"):
        pipeline_module.validate_connection(mock_connection, "INVALID_PROGRAM")


@pytest.mark.integration
def test_fetch_events_with_filters(dhis2_client, dhis2_env) -> None:
    """Test fetching events with various filters."""
    
    try:
        dhis2_client.api.get("system/info")
    except Exception as e:
        pytest.skip(f"DHIS2 server not accessible: {e}")
    
    # Test with filters that might return results
    query_params = {
        "program": "IpHINAT79UW",
        # No status filter means all statuses
        "paging": "true",
        "pageSize": "5",  # Small page for testing
        "fields": "event,program,programStage,orgUnit,eventDate,status,dataValues[dataElement,value]"
    }
    
    try:
        events = pipeline_module.fetch_events(dhis2_client, query_params)
        assert isinstance(events, list)
        
        # If we got events, verify structure
        for event in events[:2]:  # Check first couple events
            assert isinstance(event, dict)
            assert "event" in event
            assert "program" in event
            
    except ValueError as e:
        if "not found" in str(e):
            pytest.skip(f"Test program not available: {e}")
        else:
            raise


@pytest.mark.integration  
def test_integration_with_date_filter(dhis2_client, dhis2_env) -> None:
    """Test pipeline components with date filtering."""
    
    try:
        dhis2_client.api.get("system/info")
    except Exception as e:
        pytest.skip(f"DHIS2 server not accessible: {e}")
    
    mock_connection = MagicMock(spec=DHIS2Connection)
    mock_connection.url = dhis2_env["url"]
    mock_connection.username = dhis2_env["user"]
    mock_connection.password = dhis2_env["password"]
    
    try:
        # Validate connection with valid program
        client = pipeline_module.validate_connection(mock_connection, "IpHINAT79UW")
        
        # Build query with date filter (future date to likely get 0 results)
        query_params = pipeline_module.build_query_params(
            program="IpHINAT79UW",
            program_stage=None,
            org_units=None,
            status="COMPLETED",
            since_date="2099-12-31",  # Future date
        )
        
        # Should have date filter applied
        assert query_params["lastUpdated"] == "2099-12-31"
        
        # Fetch events (may be empty due to future date)
        events = pipeline_module.fetch_events(client, query_params)
        assert isinstance(events, list)
        
        # Transform should handle empty results
        df = pipeline_module.transform_events(events)
        assert df is not None
        assert len(df) >= 0  # Could be 0 or more
        
    except ValueError as e:
        if "not found" in str(e):
            pytest.skip(f"Test program not available: {e}")
        else:
            raise