"""Tests for submission validation, including the multilingual-choices regression."""

import polars as pl
import pytest
from validation import validate_data_structure, validate_field_constraints


def _multilingual_choices() -> pl.DataFrame:
    # Choices sheet of a multilingual form (no bare `label` column).
    return pl.DataFrame(
        {
            "list_name": ["yes_no", "yes_no", "districts", "districts"],
            "name": ["yes", "no", "d1", "d2"],
            "label::French (fr)": ["Oui", "Non", "District 1", "District 2"],
            "label::English (en)": ["Yes", "No", "District 1", "District 2"],
        }
    )


def _questions() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "type": ["select_one yes_no", "select_multiple districts", "text"],
            "name": ["consent", "zones", "comment"],
            "constraint": [None, None, None],
            "required": ["yes", "no", "no"],
        }
    )


def _constraint_questions(constraint: str) -> pl.DataFrame:
    # One-field survey whose single question carries the given constraint.
    return pl.DataFrame(
        {
            "type": ["integer"],
            "name": ["val"],
            "constraint": [constraint],
            "required": ["no"],
        }
    )


def test_validate_field_constraints_multilingual_does_not_raise():
    r"""Regression: multilingual choices must not raise `"label" not found`."""
    record = {"consent": "yes", "zones": "d1 d2", "comment": "free text", "org_unit_id": 1}

    # Previously this raised polars ColumnNotFoundError: "label".
    assert validate_field_constraints(record, _questions(), _multilingual_choices()) is True


def test_validate_field_constraints_rejects_unknown_choice_name():
    """A value that is not an allowed choice name is invalid."""
    record = {"consent": "maybe", "zones": "d1", "comment": "x", "org_unit_id": 1}
    assert validate_field_constraints(record, _questions(), _multilingual_choices()) is False


def test_validate_field_constraints_rejects_bad_multiselect_token():
    """Every token of a multi-select must be an allowed name."""
    record = {"consent": "yes", "zones": "d1 dX", "comment": "x", "org_unit_id": 1}
    assert validate_field_constraints(record, _questions(), _multilingual_choices()) is False


def test_validate_field_constraints_empty_choice_is_allowed():
    """An empty select answer is not treated as an invalid choice."""
    record = {"consent": "", "zones": None, "comment": "x", "org_unit_id": 1}
    assert validate_field_constraints(record, _questions(), _multilingual_choices()) is True


@pytest.mark.parametrize(
    ("value", "constraint", "expected"),
    [
        ("50", ". <= 100", True),
        ("150", ". <= 100", False),
        ("5", ".>=0", True),
        ("-1", ". >= 0", False),
        ("10", ". < 10", False),
        ("9", ". < 10", True),
        (None, ". <= 100", True),
        ("anything", ". >= 0 and . <= 100", True),  # compound -> not blocking
    ],
)
def test_constraints_with_whitespace(value: object, constraint: str, expected: bool):
    """Constraints with spaces around operators are now honoured end-to-end."""
    empty_choices = pl.DataFrame()
    result = validate_field_constraints(
        {"val": value}, _constraint_questions(constraint), empty_choices
    )
    assert result is expected


def test_regex_constraint():
    """Regex constraints validate the submitted value pattern."""
    questions = pl.DataFrame(
        {
            "type": ["text"],
            "name": ["code"],
            "constraint": ["regex(., '^[a-z]+$')"],
            "required": ["no"],
        }
    )
    empty_choices = pl.DataFrame()
    assert validate_field_constraints({"code": "abc"}, questions, empty_choices) is True
    assert validate_field_constraints({"code": "ABC"}, questions, empty_choices) is False


def test_validate_data_structure_missing_required_column():
    """CREATE requires org_unit_id; its absence is reported."""
    df = pl.DataFrame({"comment": ["a"]})
    result = validate_data_structure(df, _questions(), "CREATE")
    assert result["is_valid"] is False
    assert "org_unit_id" in result["missing_columns"]


def test_validate_data_structure_valid_create():
    """A minimal valid CREATE file (with required questions) passes validation."""
    df = pl.DataFrame(
        {
            "org_unit_id": pl.Series([1], dtype=pl.Int64),
            "consent": ["yes"],  # required question in the form
            "comment": ["a"],
        }
    )
    result = validate_data_structure(df, _questions(), "CREATE")
    assert result["is_valid"] is True


def test_validate_data_structure_empty_questions_metadata():
    """Absent survey metadata must not crash structure validation."""
    df = pl.DataFrame({"org_unit_id": pl.Series([1], dtype=pl.Int64)})
    result = validate_data_structure(df, pl.DataFrame(), "CREATE")
    assert result["is_valid"] is True
