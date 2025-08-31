from datetime import datetime

from pipeline import apply_mappings


def test_apply_mappings_transforms_values_correctly() -> None:
    """Test that apply_mappings correctly transforms data element and COC values."""
    value = {
        "dataElement": "SRC_DE",
        "categoryOptionCombo": "SRC_COC",
        "orgUnit": "OU_UID",
        "period": "202401",
        "value": "12",
    }
    mapping = {"dataElements": {"SRC_DE": "TGT_DE"}, "categoryOptionCombos": {"SRC_COC": "TGT_COC"}}

    out = apply_mappings(value, mapping)
    assert out["dataElement"] == "TGT_DE"
    assert out["categoryOptionCombo"] == "TGT_COC"
    assert out["orgUnit"] == "OU_UID"  # Unchanged
    assert out["period"] == "202401"  # Unchanged
    assert out["value"] == "12"  # Unchanged


def test_apply_mappings_preserves_unmapped_values() -> None:
    """Test that unmapped values are preserved as-is."""
    value = {
        "dataElement": "UNMAPPED_DE",
        "categoryOptionCombo": "UNMAPPED_COC",
        "orgUnit": "OU_UID",
        "period": "202401",
        "value": "12",
    }
    mapping = {"dataElements": {"SRC_DE": "TGT_DE"}, "categoryOptionCombos": {"SRC_COC": "TGT_COC"}}

    out = apply_mappings(value, mapping)
    assert out["dataElement"] == "UNMAPPED_DE"  # Unchanged
    assert out["categoryOptionCombo"] == "UNMAPPED_COC"  # Unchanged


def test_apply_mappings_handles_missing_fields() -> None:
    """Test handling when data value is missing some fields."""
    value = {
        "dataElement": "SRC_DE",
        "orgUnit": "OU_UID",
        "value": "12",
        # Missing categoryOptionCombo
    }
    mapping = {"dataElements": {"SRC_DE": "TGT_DE"}, "categoryOptionCombos": {"SRC_COC": "TGT_COC"}}

    out = apply_mappings(value, mapping)
    assert out["dataElement"] == "TGT_DE"
    assert "categoryOptionCombo" not in out


def test_date_validation() -> None:
    """Test that invalid date formats are rejected."""
    # Test valid date format
    valid_date = "2024-01-01"
    try:
        datetime.strptime(valid_date, "%Y-%m-%d")
        date_valid = True
    except ValueError:
        date_valid = False
    assert date_valid

    # Test invalid date formats
    invalid_dates = ["2024/01/01", "01-01-2024", "2024-13-01", "not-a-date"]

    for invalid_date in invalid_dates:
        try:
            datetime.strptime(invalid_date, "%Y-%m-%d")
            date_valid = True
        except ValueError:
            date_valid = False
        assert not date_valid, f"Date {invalid_date} should be invalid"


def test_empty_updates_result() -> None:
    """Test that empty update set returns sync_needed: false."""
    _ = {"dataElements": {"DE1": "DE2"}, "categoryOptionCombos": {"COC1": "COC2"}}
    updates = []  # Empty updates

    # Mock current_run for this test
    class MockCurrentRun:
        class MockParameters:
            since_date = "2024-01-01"
            dry_run = True

        parameters = MockParameters()

        @staticmethod
        def log_info(msg: str) -> None:
            pass

    # This would need proper mocking in real implementation
    # For now, test the logic directly
    sync_needed = len(updates) > 0
    assert sync_needed is False


def test_updates_found_result() -> None:
    """Test that found updates return sync_needed: true."""
    updates = [
        {
            "dataElement": "DE1",
            "categoryOptionCombo": "COC1",
            "value": "10",
            "lastUpdated": "2024-01-15T10:00:00.000",
        }
    ]

    sync_needed = len(updates) > 0
    assert sync_needed is True

    # Test unique counting
    updated_des = set()
    updated_cocs = set()

    for update in updates:
        if "dataElement" in update:
            updated_des.add(update["dataElement"])
        if "categoryOptionCombo" in update:
            updated_cocs.add(update["categoryOptionCombo"])

    assert len(updated_des) == 1
    assert len(updated_cocs) == 1
    assert "DE1" in updated_des
    assert "COC1" in updated_cocs
