"""Tests for the XLSForm helper utilities."""

from datetime import date, datetime

import polars as pl
import pytest
from utils import (
    clean_string,
    extract_list_name,
    get_choices_list_column,
    get_choices_name_column,
    get_label_columns,
    is_select_multiple,
    normalize_xml_value,
    optional_float,
    to_epoch,
)


@pytest.mark.parametrize(
    ("question_type", "expected"),
    [
        ("select_one districts", "districts"),
        ("select_multiple symptoms", "symptoms"),
        ("select one districts", "districts"),
        ("select_one_from_file facilities.csv", "facilities.csv"),
        ("text", None),
        ("integer", None),
        ("", None),
        (None, None),
    ],
)
def test_extract_list_name(question_type: str | None, expected: str | None):
    """The choice list name is resolved from the select question type."""
    assert extract_list_name(question_type) == expected


@pytest.mark.parametrize(
    ("question_type", "expected"),
    [
        ("select_multiple symptoms", True),
        ("select multiple symptoms", True),
        ("select_one districts", False),
        ("text", False),
        (None, False),
    ],
)
def test_is_select_multiple(question_type: str | None, expected: bool):
    """Multi-select detection covers both underscore and space syntaxes."""
    assert is_select_multiple(question_type) is expected


def test_get_choices_columns_single_language():
    """Single-language choices expose bare `list_name`, `name` and `label`."""
    choices = pl.DataFrame({"list_name": ["l"], "name": ["a"], "label": ["A"]})
    assert get_choices_list_column(choices) == "list_name"
    assert get_choices_name_column(choices) == "name"
    assert get_label_columns(choices) == ["label"]


def test_get_label_columns_multilingual():
    """Multilingual forms expose one label column per language."""
    choices = pl.DataFrame(
        {
            "list_name": ["l"],
            "name": ["a"],
            "label::French (fr)": ["A"],
            "label::English (en)": ["A"],
        }
    )
    assert get_label_columns(choices) == ["label::French (fr)", "label::English (en)"]
    # No bare `label` column exists: this is exactly what broke the naive code.
    assert "label" not in choices.columns


def test_get_choices_list_column_space_variant():
    """The `list name` (with a space) variant is also recognised."""
    choices = pl.DataFrame({"list name": ["l"], "name": ["a"]})
    assert get_choices_list_column(choices) == "list name"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        (True, "1"),
        (False, "0"),
        ("hello", "hello"),
        (42, "42"),
        (date(2024, 1, 2), "2024-01-02"),
        (datetime(2024, 1, 2, 3, 4, 5), "2024-01-02T03:04:05"),
    ],
)
def test_normalize_xml_value(value: object, expected: str):
    """Values are rendered in their OpenRosa XML string form."""
    assert normalize_xml_value(value) == expected


def test_optional_float_preserves_missing_as_none():
    """Missing or empty coordinates are None, never coerced to 0.0."""
    assert optional_float({}, "latitude") is None
    assert optional_float({"latitude": None}, "latitude") is None
    assert optional_float({"latitude": ""}, "latitude") is None
    assert optional_float({"latitude": "  "}, "latitude") is None
    assert optional_float({"latitude": "abc"}, "latitude") is None
    assert optional_float({"latitude": "4.5"}, "latitude") == 4.5
    assert optional_float({"latitude": 0}, "latitude") == 0.0


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        (1700000000, 1700000000),
        (date(2021, 11, 15), int(datetime(2021, 11, 15).timestamp())),
        ("not-a-date", None),
        (True, None),
    ],
)
def test_to_epoch(value: object, expected: int | None):
    """Dates/epochs convert to Unix timestamps; junk yields None."""
    assert to_epoch(value) == expected


def test_clean_string_normalizes_accents_and_spaces():
    """Accents are stripped and spaces become underscores (lowercased)."""
    assert clean_string("Centre de Santé") == "centre_de_sante"
