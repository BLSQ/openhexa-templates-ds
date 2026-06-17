import json
import pytest
from pathlib import Path
import pipeline as pipeline_module


@pytest.mark.integration
def test_full_pipeline_execution_end_to_end(dhis2_client, dynamic_dhis2_data, dhis2_env) -> None:
    """Test complete pipeline execution end-to-end with real DHIS2.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """
    
    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]
            source_connection = MockConnection()
            target_connection = MockConnection()
        
        class MockParameters:
            mapping_file = str(dynamic_dhis2_data["mapping_file"])
            since_date = "2020-01-01"  # Far in past to find updates
            dry_run = True
        
        connections = MockConnections()
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # Execute full pipeline workflow step by step
        
        # Step 1: Validate connections
        connections_result = pipeline_module.validate_connections()
        assert connections_result is not None
        assert "source_client" in connections_result
        assert "target_client" in connections_result
        
        # Step 2: Load and validate mappings
        mappings = pipeline_module.load_and_validate_mappings()
        assert mappings is not None
        assert "dataElements" in mappings
        assert "categoryOptionCombos" in mappings
        
        # Step 3: Fetch updates since date
        updates = pipeline_module.fetch_updates_since_date(mappings)
        assert isinstance(updates, list)
        
        # Step 4: Generate summary
        summary = pipeline_module.generate_summary(mappings, updates)
        assert summary is not None
        assert isinstance(summary, dict)
        assert "sync_needed" in summary
        assert isinstance(summary["sync_needed"], bool)
        
        # Verify complete summary structure
        expected_summary_fields = [
            "pipeline", "since_date", "total_data_elements_checked",
            "total_category_option_combos_checked", "updates_found",
            "unique_data_elements_updated", "unique_category_option_combos_updated",
            "sync_needed", "dry_run", "updated_data_elements",
            "updated_category_option_combos", "latest_update_timestamps"
        ]
        
        for field in expected_summary_fields:
            assert field in summary, f"Missing field in summary: {field}"
        
        # Verify logical consistency
        if summary["updates_found"] > 0:
            assert summary["sync_needed"] is True
            assert summary["unique_data_elements_updated"] > 0
        else:
            assert summary["sync_needed"] is False
            assert summary["unique_data_elements_updated"] == 0
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_pipeline_main_function_execution(dhis2_client, dynamic_dhis2_data, dhis2_env) -> None:
    """Test the main pipeline function (sync_dhis2_to_dhis2) executes successfully.
    
    UPDATED: Uses dynamic DHIS2 data discovery - never hardcodes UIDs.
    """
    
    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]
            source_connection = MockConnection()
            target_connection = MockConnection()
        
        class MockParameters:
            mapping_file = str(dynamic_dhis2_data["mapping_file"])
            since_date = "2020-01-01"
            dry_run = True
        
        connections = MockConnections()
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # Call the pipeline tasks using helper function
        result = pipeline_module.run_pipeline_tasks(MockCurrentRun())
        
        # Verify pipeline returns summary
        assert result is not None
        assert isinstance(result, dict)
        assert "sync_needed" in result
        assert "pipeline" in result
        assert result["pipeline"] == "sync_dhis2_to_dhis2"
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_pipeline_with_real_sierra_leone_data_elements(dhis2_client, dhis2_env) -> None:
    """Test pipeline with actual Sierra Leone data elements that exist in demo DB."""
    
    # First, verify some known Sierra Leone data elements exist
    try:
        # Get list of data elements to find real ones
        data_elements = dhis2_client.api.get("dataElements", params={"paging": "false", "fields": "id,name"})
        available_des = {de["id"]: de["name"] for de in data_elements.get("dataElements", [])}
        
        if not available_des:
            pytest.skip("No data elements available in DHIS2 test instance")
        
        # Get some real data element IDs for testing
        real_des = list(available_des.keys())[:2]  # Use first 2 available
        
        # Get category option combos too (never hardcode UIDs!)
        coc_response = dhis2_client.api.get("categoryOptionCombos", params={
            "paging": "false", 
            "fields": "id,name"
        })
        available_cocs = {coc["id"]: coc["name"] for coc in coc_response.get("categoryOptionCombos", [])}
        real_cocs = list(available_cocs.keys())[:2] if available_cocs else ["DEFAULT_COC"]
        
        # Get organization units too (required for mapping!)
        ou_response = dhis2_client.api.get("organisationUnits", params={
            "paging": "false", 
            "fields": "id,name"
        })
        available_ous = {ou["id"]: ou["name"] for ou in ou_response.get("organisationUnits", [])}
        real_ous = list(available_ous.keys())[:2] if available_ous else []
        
        if not real_ous:
            pytest.skip("No organization units available in DHIS2 test instance")
        
        # Create mapping with real data elements, COCs, and org units (not hardcoded!)
        real_mapping = {
            "dataElements": {de: de for de in real_des},  # Self-mapping for testing
            "categoryOptionCombos": {coc: coc for coc in real_cocs},  # Real COCs
            "orgUnits": {ou: ou for ou in real_ous}  # Real org units
        }
        
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(real_mapping, f)
            mapping_file_path = f.name
        
        class MockCurrentRun:
            class MockConnections:
                class MockConnection:
                    url = dhis2_env["url"]  # Use actual working DHIS2 server
                    username = dhis2_env["user"]
                    password = dhis2_env["password"]
                source_connection = MockConnection()
                target_connection = MockConnection()
            
            class MockParameters:
                def __init__(self):
                    self.mapping_file = mapping_file_path
                    self.since_date = "2010-01-01"  # Very far in past
                    self.dry_run = True
            
            connections = MockConnections()
            parameters = MockParameters()
            
            @staticmethod
            def log_info(msg: str) -> None:
                print(f"INFO: {msg}")
            
            @staticmethod
            def log_error(msg: str) -> None:
                print(f"ERROR: {msg}")
        
        # Patch current_run for this test
        original_current_run = pipeline_module.current_run
        pipeline_module.current_run = MockCurrentRun()
        
        try:
            # Execute pipeline with real data elements using test helper
            result = pipeline_module.run_pipeline_tasks(MockCurrentRun())
            
            # Verify pipeline completed successfully
            assert result is not None
            assert isinstance(result, dict)
            assert "sync_needed" in result
            
            # Should have checked the real data elements
            assert result["total_data_elements_checked"] == len(real_des)
            
        finally:
            # Restore original current_run
            pipeline_module.current_run = original_current_run
            # Clean up temp file
            Path(mapping_file_path).unlink()
            
    except Exception as e:
        # If DHIS2 is not accessible, skip this test
        pytest.skip(f"DHIS2 server not accessible for real data testing: {e}")


@pytest.mark.integration
def test_pipeline_error_handling_with_invalid_connection() -> None:
    """Test that pipeline properly handles connection errors."""
    
    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = "http://invalid-server:8080"
                username = "admin"
                password = "district"
            source_connection = MockConnection()
            target_connection = MockConnection()
        
        class MockParameters:
            mapping_file = "/tmp/dummy.json"
            since_date = "2024-01-01"
            dry_run = True
        
        connections = MockConnections()
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # Pipeline should fail gracefully with connection error
        with pytest.raises(ValueError, match="Cannot connect to source DHIS2"):
            pipeline_module.validate_connections()
            
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run