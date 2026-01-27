import pytest
from matcher.matcher import Matcher


@pytest.fixture
def matcher() -> Matcher:
    """Fixture that returns a Matcher instance for testing.

    Returns
    -------
    Matcher
        An instance of the Matcher class.
    """
    return Matcher()


def test_matcher_str_fuzzy(matcher: Matcher):
    """Test the string representation of the matcher."""
    text = str(matcher)
    assert "FuzzyMatcher" in text
    assert "WRatio" in text


def test_matcher_str_matcher_names(matcher: Matcher):
    """Test the string representation of the matcher."""
    text = str(matcher.matcher_names())
    assert "fuzzy" in text
    assert "transformer" in text
    assert "geometry" in text


def test_matcher_match_with_fuzzy(matcher: Matcher):
    """Test fuzzy matcher similarity function with a set of candidates."""
    candidates = {
        "TSHUAPA": ["ym2K6YcSNl9"],
        "HAUT LOMAMI": ["fEKDiQIuqeE"],
        "KWILU": ["BmKjwqc6BEw"],
        "HAUT KATANGA": ["F9w3VW1cQmb"],
        "EQUATEUR": ["XjeRGfqHMrl"],
        "MANIEMA": ["uyuwe6bqphf"],
    }
    result = matcher.match("TSHUAPA", candidates, threshold=80)
    assert result[0] == "TSHUAPA"
    assert result[1] == "TSHUAPA"
    assert result[2] == "ym2K6YcSNl9", "Expected  list of attributes, result[3] == 'ym2K6YcSNl9'"
    assert result[3] == 100, "Expected score = 100"


def test_matcher_match_with_geometry(matcher: Matcher):
    """Test the geometry matcher functionality (to be implemented)."""
    pytest.fail("TODO: implement geometry matcher test")
