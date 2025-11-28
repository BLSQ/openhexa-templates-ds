import pytest
from matcher.matcher import FuzzyMatcher


@pytest.fixture
def fuzzy_matcher() -> FuzzyMatcher:
    """Fixture that returns an instance of FuzzyMatcher.

    Returns
    -------
    FuzzyMatcher
        An instance of the FuzzyMatcher class.
    """
    return FuzzyMatcher()


def test_fuzzy_matcher_str(fuzzy_matcher: FuzzyMatcher):
    """Test that the string representation of FuzzyMatcher contains expected substrings."""
    text = str(fuzzy_matcher)
    assert "FuzzyMatcher" in text
    assert "WRatio" in text


def test_fuzzy_matcher_ratio_change(fuzzy_matcher: FuzzyMatcher):
    """Test string representation of FuzzyMatcher contains 'ratio' when scorer==fuzz.ratio."""
    fuzzy_matcher.set_scorer("ratio")
    text = str(fuzzy_matcher)
    assert "FuzzyMatcher" in text
    assert "ratio" in text


def test_fuzzy_get_similarity_match(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns the correct match and score for a valid query."""
    candidates = {
        "TSHUAPA": ["ym2K6YcSNl9"],
        "HAUT LOMAMI": ["fEKDiQIuqeE"],
        "KWILU": ["BmKjwqc6BEw"],
        "HAUT KATANGA": ["F9w3VW1cQmb"],
        "EQUATEUR": ["XjeRGfqHMrl"],
        "MANIEMA": ["uyuwe6bqphf"],
    }
    result = fuzzy_matcher.get_similarity("TSHUAPA", candidates, threshold=80)
    assert result[0] == "TSHUAPA", "Expected query to be 'TSHUAPA'"
    assert result[1] == "TSHUAPA", "Expected query to be 'TSHUAPA'"
    assert result[2] == "ym2K6YcSNl9", "Expected  list of attributes, result[2] == 'ym2K6YcSNl9'"
    assert result[3] == 100, "Expected score to be 100"


def test_fuzzy_get_similarity_no_match(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns None when there is no match above the threshold."""
    candidates = {
        "TSHUAPA": ["ym2K6YcSNl9"],
        "HAUT LOMAMI": ["fEKDiQIuqeE"],
        "KWILU": ["BmKjwqc6BEw"],
        "HAUT KATANGA": ["F9w3VW1cQmb"],
        "EQUATEUR": ["XjeRGfqHMrl"],
        "MANIEMA": ["uyuwe6bqphf"],
    }
    result = fuzzy_matcher.get_similarity("NOWEHERE", candidates, threshold=80)
    assert result is None, "Expected query to be None"
