"""Module with the functions to do the matching with."""

from rapidfuzz import fuzz, process


def fuzz_score(name: str, pyramid: dict, threshold: int) -> list | None:
    """Match a name against a pyramid using fuzzy matching.

    Parameters
    ----------
    name : str
        The name to match.
    pyramid : dict
        The pyramid to match against. It is a dictionary where the keys are the names
        in the pyramid and the values are lists with the attributes of the pyramid.
    threshold : int
        The threshold for the matching score.

    Returns
    -------
    list or None
        If a match is found, returns a list with the name to match, the matched pyramid
        name, the attributes of the matched pyramid, and the matching score.
        If no match is found or the match has a too small ratio, we return None
    """
    pyramid_names = list(pyramid.keys())
    best_match = process.extractOne(name, pyramid_names, scorer=fuzz.ratio)

    if best_match is None:
        return None

    if best_match[1] >= threshold:
        pyramid_name_match = best_match[0]
        attributes_pyramid = pyramid[pyramid_name_match]
        score = best_match[1]
        return [name, pyramid_name_match] + attributes_pyramid + [score]

    return None
