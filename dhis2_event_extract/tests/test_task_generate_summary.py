import pipeline as pipeline_module


def test_generate_summary_basic() -> None:
    """Test basic summary generation."""
    query_params = {
        "program": "TEST_PROGRAM",
        "status": "COMPLETED",
        "paging": "true",
        "pageSize": "250"
    }
    
    summary = pipeline_module.generate_summary(
        total_events=10,
        query_params=query_params,
        output_path="/workspace/output.parquet",
        program="TEST_PROGRAM",
        output_format="parquet"
    )
    
    assert summary["pipeline"] == "dhis2-event-extract"
    assert summary["program"] == "TEST_PROGRAM"
    assert summary["total_events_extracted"] == 10
    assert summary["output_file"] == "/workspace/output.parquet"
    assert summary["output_format"] == "parquet"
    assert summary["status_filter"] == "COMPLETED"
    assert "timestamp" in summary


def test_generate_summary_with_filters() -> None:
    """Test summary generation with all filters."""
    query_params = {
        "program": "TEST_PROGRAM",
        "programStage": "TEST_STAGE",
        "orgUnit": ["OU1", "OU2"],
        "status": "ACTIVE",
        "lastUpdated": "2024-01-01",
        "paging": "true",
        "pageSize": "250"
    }
    
    summary = pipeline_module.generate_summary(
        total_events=25,
        query_params=query_params,
        output_path="/workspace/events.csv",
        program="TEST_PROGRAM",
        output_format="csv"
    )
    
    assert summary["program_stage"] == "TEST_STAGE"
    assert summary["org_units"] == ["OU1", "OU2"]
    assert summary["status_filter"] == "ACTIVE"
    assert summary["since_date"] == "2024-01-01"
    assert summary["output_format"] == "csv"
    assert summary["total_events_extracted"] == 25


def test_generate_summary_minimal_params() -> None:
    """Test summary generation with minimal parameters."""
    query_params = {
        "program": "TEST_PROGRAM",
        "paging": "true",
        "pageSize": "250"
    }
    
    summary = pipeline_module.generate_summary(
        total_events=0,
        query_params=query_params,
        output_path="/workspace/empty.jsonl",
        program="TEST_PROGRAM",
        output_format="jsonl"
    )
    
    assert summary["program_stage"] is None
    assert summary["org_units"] is None
    assert summary["status_filter"] == "ALL"
    assert summary["since_date"] is None
    assert summary["total_events_extracted"] == 0