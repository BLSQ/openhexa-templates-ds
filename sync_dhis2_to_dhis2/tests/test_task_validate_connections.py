import pytest
from unittest.mock import MagicMock
import pipeline as pipeline_module
from openhexa.sdk import DHIS2Connection


@pytest.mark.integration
def test_validate_connections_with_real_dhis2(dhis2_client, dhis2_env) -> None:
    """Test validate_connections helper function with real DHIS2."""
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    try:
        # First check if DHIS2 is accessible
        dhis2_client.api.get("system/info")
    except Exception as e:
        pytest.skip(f"DHIS2 server not accessible: {e}")
    
    # Create mock connections
    source_connection = MagicMock(spec=DHIS2Connection)
    source_connection.url = dhis2_env["url"]
    source_connection.username = dhis2_env["user"]
    source_connection.password = dhis2_env["password"]
    
    target_connection = MagicMock(spec=DHIS2Connection)
    target_connection.url = dhis2_env["url"] 
    target_connection.username = dhis2_env["user"]
    target_connection.password = dhis2_env["password"]
    
    # Test the helper function directly
    result = pipeline_module.validate_connections(source_connection, target_connection)
    
    # Verify result structure
    assert result is not None
    assert isinstance(result, dict)
    assert "source_client" in result
    assert "target_client" in result
    
    # Verify clients can connect
    source_client = result["source_client"]
    target_client = result["target_client"]
    
    source_info = source_client.api.get("system/info")
    assert isinstance(source_info, dict)
    assert "version" in source_info
    
    target_info = target_client.api.get("system/info")
    assert isinstance(target_info, dict)
    assert "version" in target_info


def test_validate_connections_with_invalid_credentials() -> None:
    """Test validate_connections helper function with invalid credentials."""
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    # Create mock connections with invalid credentials
    source_connection = MagicMock(spec=DHIS2Connection)
    source_connection.url = "https://play.im.dhis2.org/stable-2-39-10-1"
    source_connection.username = "invalid_user"
    source_connection.password = "invalid_pass"
    
    target_connection = MagicMock(spec=DHIS2Connection)
    target_connection.url = "https://play.im.dhis2.org/stable-2-39-10-1"
    target_connection.username = "invalid_user"
    target_connection.password = "invalid_pass"
    
    # This should raise a ValueError due to invalid credentials
    with pytest.raises(ValueError, match="Cannot connect to source DHIS2"):
        pipeline_module.validate_connections(source_connection, target_connection)


def test_validate_connections_with_invalid_url() -> None:
    """Test validate_connections helper function with invalid DHIS2 URL."""
    
    # Mock current_run for logging
    pipeline_module.current_run = MagicMock()
    pipeline_module.current_run.log_info = lambda msg: print(f"INFO: {msg}")
    pipeline_module.current_run.log_error = lambda msg: print(f"ERROR: {msg}")
    
    # Create mock connections with invalid URL
    source_connection = MagicMock(spec=DHIS2Connection)
    source_connection.url = "http://nonexistent-server:8080"
    source_connection.username = "admin"
    source_connection.password = "district"
    
    target_connection = MagicMock(spec=DHIS2Connection)
    target_connection.url = "http://nonexistent-server:8080"
    target_connection.username = "admin"
    target_connection.password = "district"
    
    # This should raise a ValueError due to connection failure
    with pytest.raises(ValueError, match="Cannot connect to source DHIS2"):
        pipeline_module.validate_connections(source_connection, target_connection)