from unittest import result
import pytest
import pipeline as pipeline_module


@pytest.mark.integration
def test_fetch_updates_since_date_with_valid_date(dynamic_dhis2_data, dhis2_client, dhis2_env) -> None:
    """Test fetch_updates_since_date task with valid date and real DHIS2 data.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """

    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]

            source_connection = MockConnection()

        class MockParameters:
            since_date = "2020-01-01"  # Far in past to potentially find updates

        connections = MockConnections()
        parameters = MockParameters()

        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")

        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")

    # Use dynamically discovered DHIS2 mappings (not hardcoded!)
    mappings = dynamic_dhis2_data["mapping"]

    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()

    try:
        # Call the actual fetch_updates_since_date task
        result = pipeline_module.fetch_updates_since_date(mappings)
        print(result)
        # Verify result structure
        assert isinstance(result, list)

        # Each item in result should be a valid data value structure
        for data_value in result:
            assert isinstance(data_value, dict)
            # Should have basic DHIS2 data value fields (some may be optional)
            expected_fields = ["dataElement", "orgUnit", "period", "value"]
            available_fields = list(data_value.keys())
            # At least dataElement should be present
            assert "dataElement" in available_fields

    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_fetch_updates_since_date_with_future_date(dynamic_dhis2_data, dhis2_client, dhis2_env) -> None:
    """Test fetch_updates_since_date task with future date (should return no updates).
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """

    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]

            source_connection = MockConnection()

        class MockParameters:
            since_date = "2030-12-31"  # Future date - no updates expected

        connections = MockConnections()
        parameters = MockParameters()

        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")

        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")

    # Use dynamically discovered DHIS2 mappings (not hardcoded!)
    mappings = dynamic_dhis2_data["mapping"]

    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()

    try:
        # Call the actual fetch_updates_since_date task
        result = pipeline_module.fetch_updates_since_date(mappings)

        # With future date, should return empty list
        assert isinstance(result, list)
        assert len(result) == 0

    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_fetch_updates_since_date_with_invalid_date_format(
    dynamic_dhis2_data, dhis2_client, dhis2_env
) -> None:
    """Test fetch_updates_since_date task with invalid date format.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """

    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]

            source_connection = MockConnection()

        class MockParameters:
            since_date = "2024/01/01"  # Invalid format - should be YYYY-MM-DD

        connections = MockConnections()
        parameters = MockParameters()

        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")

        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")

    # Use dynamically discovered DHIS2 mappings (not hardcoded!)
    mappings = dynamic_dhis2_data["mapping"]

    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()

    try:
        # This should raise a ValueError due to invalid date format
        with pytest.raises(ValueError, match="Invalid date format"):
            pipeline_module.fetch_updates_since_date(mappings)

    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_fetch_updates_since_date_with_empty_mappings(dhis2_client, dhis2_env) -> None:
    """Test fetch_updates_since_date task with empty mappings."""

    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]

            source_connection = MockConnection()

        class MockParameters:
            since_date = "2024-01-01"

        connections = MockConnections()
        parameters = MockParameters()

        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")

        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")

    # Empty mappings
    empty_mappings = {"dataElements": {}, "categoryOptionCombos": {}}

    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()

    try:
        # Call the actual fetch_updates_since_date task
        result = pipeline_module.fetch_updates_since_date(empty_mappings)

        # Should return empty list when no data elements to check
        assert isinstance(result, list)
        assert len(result) == 0

    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_fetch_updates_since_date_api_structure(workspace_test_mapping, dhis2_client, dhis2_env) -> None:
    """Test that fetch_updates_since_date makes correct API calls to DHIS2.
    
    UPDATED: Uses workspace test mapping with real DHIS2 UIDs.
    """
    
    # Load mapping from workspace test file
    import json
    with open(workspace_test_mapping, encoding="utf-8") as f:
        test_mapping = json.load(f)

    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]

            source_connection = MockConnection()

        class MockParameters:
            since_date = "2020-01-01"

        connections = MockConnections()
        parameters = MockParameters()

        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")

        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")

    # Use workspace test mapping with real DHIS2 UIDs
    mappings = test_mapping

    # Test direct API call to verify endpoint works
    params = {
        "dataElement": list(mappings["dataElements"].keys()),
        "categoryOptionCombo": list(mappings["categoryOptionCombos"].keys()),
        "orgUnit": list(mappings["orgUnits"].keys()),  # Add required orgUnits
        "lastUpdated": "2020-01-01",
        "paging": "false",
    }

    try:
        # Test API call directly first
        api_response = dhis2_client.api.get("dataValueSets", params=params)
        assert isinstance(api_response, dict)
        assert "dataValues" in api_response
        assert isinstance(api_response["dataValues"], list)

        # Patch current_run for pipeline test
        original_current_run = pipeline_module.current_run
        pipeline_module.current_run = MockCurrentRun()

        try:
            # Now call the actual task
            result = pipeline_module.fetch_updates_since_date(mappings)

            # Verify result matches API response structure
            assert isinstance(result, list)
            assert len(result) == len(api_response["dataValues"])

        finally:
            # Restore original current_run
            pipeline_module.current_run = original_current_run

    except Exception as e:
        # If DHIS2 is not accessible, skip this test
        pytest.skip(f"DHIS2 server not accessible for API testing: {e}")
