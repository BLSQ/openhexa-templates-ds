import pipeline as pipeline_module


def test_build_query_basic_parameters() -> None:
    """Test query building with basic parameters."""
    query = pipeline_module.build_query_params(
        program="TEST_PROGRAM",
        program_stage=None,
        org_units=None,
        status="COMPLETED",
        since_date=None,
    )
    
    assert query["program"] == "TEST_PROGRAM"
    assert query["status"] == "COMPLETED"
    assert query["paging"] == "true"
    assert query["pageSize"] == "250"
    assert "programStage" not in query
    assert "orgUnit" not in query
    assert "lastUpdated" not in query


def test_build_query_with_program_stage() -> None:
    """Test query building with program stage filter."""
    query = pipeline_module.build_query_params(
        program="TEST_PROGRAM",
        program_stage="TEST_STAGE",
        org_units=None,
        status="COMPLETED",
        since_date=None,
    )
    assert query["programStage"] == "TEST_STAGE"


def test_build_query_with_org_units_list() -> None:
    """Test query building with org units filter as list."""
    query = pipeline_module.build_query_params(
        program="TEST_PROGRAM",
        program_stage=None,
        org_units=["OU1", "OU2", "OU3"],
        status="COMPLETED",
        since_date=None,
    )
    assert query["orgUnit"] == ["OU1", "OU2", "OU3"]


def test_build_query_with_since_date_valid() -> None:
    """Test query building with valid since_date."""
    query = pipeline_module.build_query_params(
        program="TEST_PROGRAM",
        program_stage=None,
        org_units=None,
        status="COMPLETED",
        since_date="2024-01-15",
    )
    assert query["lastUpdated"] == "2024-01-15"


def test_build_query_with_since_date_invalid() -> None:
    """Test query building with invalid since_date raises error."""
    import pytest
    
    with pytest.raises(ValueError, match="Invalid date format"):
        pipeline_module.build_query_params(
            program="TEST_PROGRAM",
            program_stage=None,
            org_units=None,
            status="COMPLETED",
            since_date="invalid-date",
        )


def test_build_query_status_all_excludes_filter() -> None:
    """Test that status=ALL excludes status filter from query."""
    query = pipeline_module.build_query_params(
        program="TEST_PROGRAM",
        program_stage=None,
        org_units=None,
        status="ALL",
        since_date=None,
    )
    assert "status" not in query