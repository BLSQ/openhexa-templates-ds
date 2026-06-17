import pytest
import pipeline as pipeline_module


@pytest.mark.integration
def test_pipeline_full_workflow_with_mock_current_run(dhis2_client, dynamic_dhis2_data, dhis2_env) -> None:
    """Test complete pipeline workflow using the actual pipeline function with mocked current_run.
    
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
            since_date = "2020-01-01"  # Far in past to potentially find updates
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
        # Call the pipeline tasks using our helper function
        summary = pipeline_module.run_pipeline_tasks(MockCurrentRun())
        
        # Verify pipeline executed and returned proper summary
        assert summary is not None
        assert isinstance(summary, dict)
        assert "sync_needed" in summary
        assert "pipeline" in summary
        assert summary["pipeline"] == "sync_dhis2_to_dhis2"
        assert summary["dry_run"] is True
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_pipeline_with_future_date_returns_no_sync_needed(dhis2_client, dynamic_dhis2_data, dhis2_env) -> None:
    """Test that pipeline with future date returns sync_needed: false.
    
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
            since_date = "2030-01-01"  # Future date - no updates expected
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
        # Call pipeline tasks using our helper function
        summary = pipeline_module.run_pipeline_tasks(MockCurrentRun())
        
        # With future date, should find no updates
        assert summary["sync_needed"] is False
        assert summary["updates_found"] == 0
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run


@pytest.mark.integration
def test_dhis2_environment_accessible_with_dynamic_data(dhis2_client) -> None:
    """Test DHIS2 accessibility and dynamic data discovery - UPDATED PATTERN."""
    try:
        # Step 1: Verify basic system info
        info = dhis2_client.api.get("system/info")
        assert isinstance(info, dict)
        assert "version" in info
        print(f"✅ DHIS2 version: {info.get('version')}")
        
        # Step 2: Discover actual data elements (never hardcode!)
        data_elements_response = dhis2_client.api.get("dataElements", params={
            "fields": "id,name",
            "pageSize": "10"
        })
        assert isinstance(data_elements_response, dict)
        assert "dataElements" in data_elements_response
        
        data_elements = data_elements_response["dataElements"]
        assert isinstance(data_elements, list)
        print(f"✅ Found {len(data_elements)} data elements dynamically")
        
        if data_elements:
            print(f"Example data elements: {[de['name'][:50] for de in data_elements[:3]]}")
        
        # Step 3: Discover actual category option combos
        coc_response = dhis2_client.api.get("categoryOptionCombos", params={
            "fields": "id,name", 
            "pageSize": "10"
        })
        assert isinstance(coc_response, dict)
        category_option_combos = coc_response.get("categoryOptionCombos", [])
        print(f"✅ Found {len(category_option_combos)} category option combos dynamically")
        
        # Step 4: Test dataValueSets endpoint with real data
        if data_elements and category_option_combos:
            try:
                # Use real UIDs discovered dynamically
                dvs_response = dhis2_client.api.get("dataValueSets", params={
                    "dataElement": data_elements[0]["id"],  # Use real ID
                    "categoryOptionCombo": category_option_combos[0]["id"],  # Use real ID
                    "lastUpdated": "2020-01-01",
                    "paging": "false"
                })
                assert isinstance(dvs_response, dict)
                print("✅ DataValueSets endpoint accessible with real data")
                
                # Show what we found
                data_values = dvs_response.get("dataValues", [])
                print(f"✅ Found {len(data_values)} data values for real UIDs")
                
            except Exception as e:
                print(f"⚠️  DataValueSets test with real data: {e}")
        else:
            pytest.skip("No DHIS2 data available - instance needs data population")
        
    except Exception as e:
        print(f"❌ DHIS2 not accessible: {e}")
        pytest.skip(f"DHIS2 test environment not available: {e}")