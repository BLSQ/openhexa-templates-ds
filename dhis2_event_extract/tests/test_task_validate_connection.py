from unittest.mock import MagicMock

import pipeline as pipeline_module
import pytest
from openhexa.sdk import DHIS2Connection


@pytest.mark.integration
def test_validate_connection_with_real_dhis2(dhis2_client, dhis2_env) -> None:
    """Test validate_connection function with real DHIS2 connection."""
    
    # Create a mock DHIS2Connection object
    mock_connection = MagicMock(spec=DHIS2Connection)
    mock_connection.url = dhis2_env["url"]
    mock_connection.username = dhis2_env["user"]
    mock_connection.password = dhis2_env["password"]
    
    # First check if DHIS2 is accessible
    try:
        dhis2_client.api.get("system/info")
    except Exception as e:
        pytest.skip(f"DHIS2 server not accessible for connection testing: {e}")
    
    # Call the actual validate_connection function
    result = pipeline_module.validate_connection(mock_connection, "IpHINAT79UW")
    
    # Verify result is DHIS2 client
    assert result is not None
    from openhexa.toolbox.dhis2 import DHIS2
    assert isinstance(result, DHIS2)
    
    # Verify client can actually connect to DHIS2
    info = result.api.get("system/info")
    assert isinstance(info, dict)
    assert "version" in info


@pytest.mark.integration
def test_validate_connection_with_invalid_credentials(dhis2_env) -> None:
    """Test validate_connection function with invalid credentials."""
    
    # Create a mock DHIS2Connection object with invalid credentials
    mock_connection = MagicMock(spec=DHIS2Connection)
    mock_connection.url = dhis2_env["url"]
    mock_connection.username = "invalid_user"
    mock_connection.password = "invalid_pass"
    
    # This should raise a ValueError due to invalid credentials
    with pytest.raises(ValueError, match="Cannot connect to DHIS2"):
        pipeline_module.validate_connection(mock_connection, "IpHINAT79UW")


def test_validate_connection_invalid_program_uid() -> None:
    """Test validate_connection with invalid program UID."""
    
    # Create a mock DHIS2Connection object
    mock_connection = MagicMock(spec=DHIS2Connection)
    mock_connection.url = "https://play.im.dhis2.org/stable-2-39-10-1"
    mock_connection.username = "admin"
    mock_connection.password = "district"
    
    with pytest.raises(ValueError, match="Program 'INVALID_PROGRAM_UID' not found"):
        pipeline_module.validate_connection(mock_connection, "INVALID_PROGRAM_UID")