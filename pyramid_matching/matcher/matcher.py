from abc import ABC, abstractmethod
from typing import Any

from rapidfuzz import fuzz, process
from shapely.geometry.base import BaseGeometry


class BaseMatcher(ABC):
    """Abstract base class for matchers that compute similarity scores."""

    @abstractmethod
    def get_similarity(
        self, query: str | BaseGeometry, candidates: list[str | BaseGeometry], threshold: float
    ) -> dict:
        """Return similarity scores for the candidates."""
        pass


class FuzzyMatcher(BaseMatcher):
    """Matcher that uses fuzzy string matching to compute similarity scores."""

    def __init__(self, threshold: float, scorer_name: str):
        self.fuzz = fuzz
        self.process = process
        self.set_scorer(scorer_name)
        self.threshold = threshold

    def set_scorer(self, scorer_name: str):
        """Set the scorer function based on the provided name."""
        scorer_name = scorer_name.lower()
        if scorer_name == "ratio":
            self.scorer = self.fuzz.ratio
        elif scorer_name == "partial_ratio":
            self.scorer = self.fuzz.partial_ratio
        elif scorer_name == "token_sort_ratio":
            self.scorer = self.fuzz.token_sort_ratio
        elif scorer_name == "token_set_ratio":
            self.scorer = self.fuzz.token_set_ratio
        elif scorer_name == "wratio":
            self.scorer = self.fuzz.WRatio
        else:
            raise ValueError(f"Unknown scorer: {scorer_name}")

    def get_similarity(self, query: str | BaseGeometry, candidates: dict) -> list | None:
        """Return the best fuzzy match among candidates if above threshold.

        Returns:
            dict: A dictionary with the best match and its score, or None
            if no match meets the threshold.
        """
        candidate_strings = list(candidates.keys())
        best_match = self.process.extractOne(query, candidate_strings, scorer=self.scorer)

        if best_match is None:
            return None

        if best_match[1] >= self.threshold:
            match_str = best_match[0]
            attributes_pyramid = candidates[match_str]
            score = best_match[1]
            return [query, match_str] + attributes_pyramid + [score]

        return None

    def __str__(self) -> str:
        return f"FuzzyMatcher(scorer: {self.scorer.__name__})"


class SentenceTransformerMatcher(BaseMatcher):
    """Matcher that uses sentence transformers to compute similarity scores.

    NOTE: Not yet implemented.
    """

    def __init__(self, model_name: str | None = "sentence-transformers/all-MiniLM-L6-v2"):
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415

        model_name = "sentence-transformers/all-MiniLM-L6-v2"
        self.model = SentenceTransformer(model_name)

    def get_similarity(
        self, query: str | BaseGeometry, candidates: list[str | BaseGeometry], threshold: float
    ) -> dict:
        # from sentence_transformers.util import cos_sim
        # query_emb = self.model.encode(query, convert_to_tensor=True)
        # cand_embs = self.model.encode(candidates, convert_to_tensor=True)
        # scores = cos_sim(query_emb, cand_embs)[0].cpu().numpy()
        """Return similarity scores for the candidates using sentence transformers."""
        raise NotImplementedError("SentenceTransformerMatcher.get_similarity is not implemented.")

    def __str__(self) -> str:
        return f"TransformerMatcher(scorer: {self.model.__name__})"


class GeometryMatcher(BaseMatcher):
    """Matcher that uses geometric similarity to compute similarity scores.

    NOTE: Not yet implemented.
    """

    def __init__(self):
        import shapely.geometry  # noqa: PLC0415

        self.shapely = shapely.geometry
        self.distance = self.shapely.distance  # Distance for geometric similarity (closest point?)

    def get_similarity(
        self, query: str | BaseGeometry, candidates: list[str | BaseGeometry], threshold: float
    ) -> dict:
        """Return geometric similarity scores for the candidates."""
        raise NotImplementedError("GeometryMatcher.get_similarity is not implemented.")

    def __str__(self) -> str:
        return f"GeometryMatcher(distance: {self.scorer.__name__})"


class Matcher:
    """Class to select and use different matching strategies (fuzzy).

    Attributes
    ----------
    matcher : BaseMatcher
        The selected matcher instance.
    threshold : float
        The threshold for considering a match.
        This might be only used by some matchers.

    Methods
    -------
    match(query, candidates, threshold)
        Returns similarity scores using the selected matcher.
    matcher_type()
        Returns a list of available matcher names.
    """

    def __init__(
        self,
        matcher_type: str,
        threshold: float = 80,
        scorer_fuzzy: str = "wratio",
    ):
        matcher_name = matcher_type.lower()

        if matcher_type == "fuzzy":
            self.matcher = FuzzyMatcher(threshold=threshold, scorer_name=scorer_fuzzy)
        # elif matcher_type == "transformer":
        #    self.matcher = SentenceTransformerMatcher()
        # elif matcher_type == "geometry":
        #    self.matcher = GeometryMatcher()
        else:
            raise ValueError(f"Unknown matcher: {matcher_name}")

    def match(self, query: str | BaseGeometry, candidates: dict[Any, list[Any]]) -> list | None:
        """Returns similarity scores using the selected matcher.

        Parameters
        ----------
        query : str or BaseGeometry
            The query item to match against candidates.
        candidates : list of str or BaseGeometry
            The list of candidate items to compare.

        Returns
        -------
        dict
            Similarity scores or match results from the selected matcher.
        """
        return self.matcher.get_similarity(query, candidates)

    def matcher_types(self) -> list[str]:
        """Return a list of available matcher names.

        Returns
        -------
        list of str
            The names of available matcher strategies.
        """
        return ["fuzzy", "transformer", "geometry"]

    def set_scorer_fuzzy(self, scorer_name: str):
        """Set the scorer function for the fuzzy matcher."""
        if isinstance(self.matcher, FuzzyMatcher):
            self.matcher.set_scorer(scorer_name)
        else:
            raise ValueError("Current matcher is not a FuzzyMatcher, cannot set scorer.")

    def __str__(self) -> str:
        return str(self.matcher)
