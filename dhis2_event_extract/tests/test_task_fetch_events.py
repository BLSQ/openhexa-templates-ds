from unittest.mock import MagicMock

import pipeline as pipeline_module
import pytest


@pytest.mark.integration
def test_fetch_events_with_real_dhis2(dhis2_client, dhis2_env) -> None:
    """Test fetch_events function with real DHIS2 connection."""
    # Test query parameters
    query_params = {
        "program": "IpHINAT79UW",  # Child Programme
        "paging": "true",
        "pageSize": "10",  # Small page size for testing
        "fields": "event,program,programStage,orgUnit,eventDate,status,dataValues[dataElement,value]"
    }
    
    try:
        # First check if DHIS2 is accessible
        dhis2_client.api.get("system/info")
    except Exception as e:
        pytest.skip(f"DHIS2 server not accessible: {e}")
    
    # Call the actual fetch_events function
    result = pipeline_module.fetch_events(dhis2_client, query_params)
    
    # Verify result structure
    assert isinstance(result, list)
    
    # Each item should be a valid event structure (if any returned)
    for event in result:
        assert isinstance(event, dict)
        assert "event" in event
        assert "program" in event
        assert "orgUnit" in event


def test_fetch_events_empty_response() -> None:
    """Test fetch_events with empty response."""
    # Mock DHIS2 client
    mock_client = MagicMock()
    mock_client.api.get.return_value = {
        "events": [],
        "pager": {"page": 1, "pageCount": 1}
    }
    
    query_params = {
        "program": "TEST_PROGRAM",
        "paging": "true",
        "pageSize": "250"
    }
    
    result = pipeline_module.fetch_events(mock_client, query_params)
    assert result == []


def test_fetch_events_pagination() -> None:
    """Test fetch_events handles pagination correctly."""
    # Mock DHIS2 client with multiple pages
    mock_client = MagicMock()
    mock_client.api.get.side_effect = [
        {
            "events": [{"event": "E1"}, {"event": "E2"}],
            "pager": {"page": 1, "pageCount": 2}
        },
        {
            "events": [{"event": "E3"}],
            "pager": {"page": 2, "pageCount": 2}
        }
    ]
    
    query_params = {
        "program": "TEST_PROGRAM",
        "paging": "true",
        "pageSize": "2"
    }
    
    result = pipeline_module.fetch_events(mock_client, query_params)
    
    assert len(result) == 3
    assert result[0]["event"] == "E1"
    assert result[1]["event"] == "E2"
    assert result[2]["event"] == "E3"
    
    # Verify pagination calls
    assert mock_client.api.get.call_count == 2


def test_fetch_events_api_error() -> None:
    """Test fetch_events handles API errors gracefully."""
    # Mock DHIS2 client that raises error
    mock_client = MagicMock()
    mock_client.api.get.side_effect = Exception("API Error")
    
    query_params = {
        "program": "TEST_PROGRAM",
        "paging": "true",
        "pageSize": "250"
    }
    
    with pytest.raises(ValueError, match="Failed to fetch events from DHIS2"):
        pipeline_module.fetch_events(mock_client, query_params)