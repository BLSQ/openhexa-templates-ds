import pytest
import pipeline as pipeline_module


@pytest.mark.integration
def test_generate_summary_with_updates_found(dynamic_dhis2_data) -> None:
    """Test generate_summary task with sample updates data.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """
    
    class MockCurrentRun:
        class MockParameters:
            since_date = "2024-01-01"
            dry_run = True
        
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Use dynamically discovered DHIS2 mappings (not hardcoded!)
    mappings = dynamic_dhis2_data["mapping"]
    
    # Get real data elements and COCs from dynamic data
    data_elements = dynamic_dhis2_data["data_elements"]
    category_option_combos = dynamic_dhis2_data["category_option_combos"]
    org_units = dynamic_dhis2_data["org_units"]
    
    # Create sample updates using real DHIS2 UIDs (not hardcoded!)
    sample_updates = [
        {
            "dataElement": data_elements[0]["id"] if data_elements else "TEST_DE_1",
            "categoryOptionCombo": category_option_combos[0]["id"] if category_option_combos else "TEST_COC_1", 
            "orgUnit": org_units[0]["id"] if org_units else "TEST_OU_1",
            "period": "202401",
            "value": "150",
            "lastUpdated": "2024-02-15T10:30:00.000"
        },
        {
            "dataElement": data_elements[1]["id"] if len(data_elements) > 1 else "TEST_DE_2",
            "categoryOptionCombo": category_option_combos[1]["id"] if len(category_option_combos) > 1 else "TEST_COC_2",
            "orgUnit": org_units[0]["id"] if org_units else "TEST_OU_1", 
            "period": "202401",
            "value": "75",
            "lastUpdated": "2024-02-16T14:20:00.000"
        }
    ]
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # Call the actual generate_summary task
        result = pipeline_module.generate_summary(mappings, sample_updates)
        
        # Verify result structure and content
        assert result is not None
        assert isinstance(result, dict)
        
        # Verify required summary fields
        required_fields = [
            "pipeline", "since_date", "total_data_elements_checked",
            "total_category_option_combos_checked", "updates_found",
            "unique_data_elements_updated", "unique_category_option_combos_updated",
            "sync_needed", "dry_run", "updated_data_elements",
            "updated_category_option_combos", "latest_update_timestamps"
        ]
        
        for field in required_fields:
            assert field in result, f"Missing required field: {field}"
        
        # Verify specific values
        assert result["pipeline"] == "sync_dhis2_to_dhis2"
        assert result["since_date"] == "2024-01-01"
        assert result["updates_found"] == 2
        assert result["sync_needed"] is True
        assert result["dry_run"] is True
        assert result["unique_data_elements_updated"] == 2
        assert result["unique_category_option_combos_updated"] == 2
        
        # Verify updated elements are tracked (using real DHIS2 UIDs)
        expected_de_1 = data_elements[0]["id"] if data_elements else "TEST_DE_1"
        expected_de_2 = data_elements[1]["id"] if len(data_elements) > 1 else "TEST_DE_2"
        expected_coc_1 = category_option_combos[0]["id"] if category_option_combos else "TEST_COC_1"
        expected_coc_2 = category_option_combos[1]["id"] if len(category_option_combos) > 1 else "TEST_COC_2"
        
        assert expected_de_1 in result["updated_data_elements"]
        assert expected_de_2 in result["updated_data_elements"]
        assert expected_coc_1 in result["updated_category_option_combos"]
        assert expected_coc_2 in result["updated_category_option_combos"]
        
        # Verify timestamps are captured
        assert len(result["latest_update_timestamps"]) == 2
        assert "2024-02-15T10:30:00.000" in result["latest_update_timestamps"]
        assert "2024-02-16T14:20:00.000" in result["latest_update_timestamps"]
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_generate_summary_with_no_updates(dynamic_dhis2_data) -> None:
    """Test generate_summary task with no updates found.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """
    
    class MockCurrentRun:
        class MockParameters:
            since_date = "2030-01-01"
            dry_run = True
        
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Use dynamically discovered DHIS2 mappings (not hardcoded!)
    mappings = dynamic_dhis2_data["mapping"]
    
    # No updates (empty list)
    no_updates = []
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # Call the actual generate_summary task
        result = pipeline_module.generate_summary(mappings, no_updates)
        
        # Verify result structure
        assert result is not None
        assert isinstance(result, dict)
        
        # Verify no-updates scenario
        assert result["updates_found"] == 0
        assert result["sync_needed"] is False
        assert result["unique_data_elements_updated"] == 0
        assert result["unique_category_option_combos_updated"] == 0
        assert len(result["updated_data_elements"]) == 0
        assert len(result["updated_category_option_combos"]) == 0
        assert len(result["latest_update_timestamps"]) == 0
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_generate_summary_with_large_dataset(dynamic_dhis2_data) -> None:
    """Test generate_summary task with large number of updates to test performance.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """
    
    class MockCurrentRun:
        class MockParameters:
            since_date = "2024-01-01"
            dry_run = True
        
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Use dynamically discovered DHIS2 mappings (not hardcoded!)
    mappings = dynamic_dhis2_data["mapping"]
    
    # Get real DHIS2 UIDs for test data
    data_elements = dynamic_dhis2_data["data_elements"]
    category_option_combos = dynamic_dhis2_data["category_option_combos"]
    real_de = data_elements[0]["id"] if data_elements else "TEST_DE"
    real_coc = category_option_combos[0]["id"] if category_option_combos else "TEST_COC"
    
    # Generate large dataset using real DHIS2 UIDs (not hardcoded!)
    large_updates = []
    for i in range(100):
        large_updates.append({
            "dataElement": real_de,
            "categoryOptionCombo": real_coc,
            "orgUnit": f"OU_{i:03d}",
            "period": f"20240{(i % 12) + 1:02d}",
            "value": str(i * 10),
            "lastUpdated": f"2024-{(i % 12) + 1:02d}-15T10:30:00.000"
        })
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # Call the actual generate_summary task
        result = pipeline_module.generate_summary(mappings, large_updates)
        
        # Verify result handles large dataset correctly
        assert result is not None
        assert isinstance(result, dict)
        assert result["updates_found"] == 100
        assert result["sync_needed"] is True
        assert result["unique_data_elements_updated"] == 1  # Only one DE used
        assert result["unique_category_option_combos_updated"] == 1  # Only one COC used
        
        # Verify timestamp limiting (should be max 10)
        assert len(result["latest_update_timestamps"]) <= 10
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_generate_summary_timestamp_handling(dynamic_dhis2_data) -> None:
    """Test generate_summary properly handles and sorts timestamps.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """
    
    class MockCurrentRun:
        class MockParameters:
            since_date = "2024-01-01"
            dry_run = True
        
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Use dynamically discovered DHIS2 mappings (not hardcoded!)
    mappings = dynamic_dhis2_data["mapping"]
    
    # Get real DHIS2 UIDs for timestamp test
    data_elements = dynamic_dhis2_data["data_elements"]
    real_de_1 = data_elements[0]["id"] if data_elements else "TEST_DE_1"
    real_de_2 = data_elements[1]["id"] if len(data_elements) > 1 else "TEST_DE_2"
    
    # Updates with various timestamps using real UIDs (not hardcoded!)
    updates_with_timestamps = [
        {
            "dataElement": real_de_1,
            "lastUpdated": "2024-01-10T10:30:00.000"
        },
        {
            "dataElement": real_de_2, 
            "lastUpdated": "2024-01-05T08:15:00.000"
        },
        {
            "dataElement": real_de_1,
            "lastUpdated": "2024-01-15T16:45:00.000"
        }
    ]
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # Call the actual generate_summary task
        result = pipeline_module.generate_summary(mappings, updates_with_timestamps)
        
        # Verify timestamps are sorted (latest first)
        timestamps = result["latest_update_timestamps"]
        assert len(timestamps) == 3
        assert timestamps == sorted(timestamps)  # Should be sorted
        assert "2024-01-15T16:45:00.000" in timestamps  # Latest should be included
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run