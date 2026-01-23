import pipeline as pipeline_module
import polars as pl


def test_transform_empty_events() -> None:
    """Test transformation of empty event list."""
    
    class MockCurrentRun:
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
    
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        result = pipeline_module.transform_events([])
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
        expected_cols = ["event", "program", "programStage", "orgUnit", "eventDate", "completedDate", "status"]
        assert all(col in result.columns for col in expected_cols)
        
    finally:
        pipeline_module.current_run = original_current_run


def test_transform_single_event() -> None:
    """Test transformation of single event."""
    
    class MockCurrentRun:
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
    
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        events = [{
            "event": "EVENT_1",
            "program": "PROGRAM_1", 
            "programStage": "STAGE_1",
            "orgUnit": "OU_1",
            "eventDate": "2024-01-15",
            "completedDate": "2024-01-15",
            "status": "COMPLETED",
            "dataValues": [
                {"dataElement": "DE_1", "value": "Value1"},
                {"dataElement": "DE_2", "value": "Value2"}
            ]
        }]
        
        result = pipeline_module.transform_events(events)
        
        assert len(result) == 1
        assert result["event"][0] == "EVENT_1"
        assert result["program"][0] == "PROGRAM_1"
        assert result["programStage"][0] == "STAGE_1"
        assert result["orgUnit"][0] == "OU_1"
        assert result["eventDate"][0] == "2024-01-15"
        assert result["status"][0] == "COMPLETED"
        assert result["DE_1_value"][0] == "Value1"
        assert result["DE_2_value"][0] == "Value2"
        
    finally:
        pipeline_module.current_run = original_current_run


def test_transform_multiple_events_consistent_schema() -> None:
    """Test transformation ensures consistent schema across events."""
    
    class MockCurrentRun:
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
    
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        events = [
            {
                "event": "EVENT_1",
                "program": "PROGRAM_1",
                "programStage": "STAGE_1", 
                "orgUnit": "OU_1",
                "eventDate": "2024-01-15",
                "status": "COMPLETED",
                "dataValues": [
                    {"dataElement": "DE_1", "value": "Value1"},
                    {"dataElement": "DE_2", "value": "Value2"}
                ]
            },
            {
                "event": "EVENT_2",
                "program": "PROGRAM_1",
                "programStage": "STAGE_1",
                "orgUnit": "OU_2", 
                "eventDate": "2024-01-16",
                "status": "ACTIVE",
                "dataValues": [
                    {"dataElement": "DE_1", "value": "Value3"},
                    {"dataElement": "DE_3", "value": "Value4"}  # Different DE
                ]
            }
        ]
        
        result = pipeline_module.transform_events(events)
        
        assert len(result) == 2
        
        # Check that all data element columns exist
        assert "DE_1_value" in result.columns
        assert "DE_2_value" in result.columns  
        assert "DE_3_value" in result.columns
        
        # Check first event
        assert result["DE_1_value"][0] == "Value1"
        assert result["DE_2_value"][0] == "Value2"
        assert result["DE_3_value"][0] is None
        
        # Check second event
        assert result["DE_1_value"][1] == "Value3"
        assert result["DE_2_value"][1] is None
        assert result["DE_3_value"][1] == "Value4"
        
    finally:
        pipeline_module.current_run = original_current_run


def test_transform_events_missing_data_values() -> None:
    """Test transformation when event has no dataValues."""
    
    class MockCurrentRun:
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
    
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        events = [{
            "event": "EVENT_1",
            "program": "PROGRAM_1",
            "programStage": "STAGE_1",
            "orgUnit": "OU_1",
            "eventDate": "2024-01-15",
            "status": "COMPLETED"
            # No dataValues field
        }]
        
        result = pipeline_module.transform_events(events)
        
        assert len(result) == 1
        assert result["event"][0] == "EVENT_1"
        # Should only have base columns, no data element columns
        base_cols = ["event", "program", "programStage", "orgUnit", "eventDate", "completedDate", "status"]
        assert all(col in result.columns for col in base_cols)
        
    finally:
        pipeline_module.current_run = original_current_run