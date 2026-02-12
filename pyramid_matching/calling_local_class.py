import os
from pathlib import Path

import polars as pl
from matcher.matchers import FuzzyMatcher
from matcher.pyramid_matcher import PyramidMatcher

os.chdir(Path(__file__).parent)


def main():
    """Example of calling the match_pyramids function."""
    dhis2_pyramid = pl.read_csv("data/dhis2_pyramid.csv")
    data = pl.read_csv("data/data_to_match_2_error.csv")

    matcher = FuzzyMatcher(threshold=80)
    pyramid_matcher = PyramidMatcher(matcher=matcher)

    matched_data, matched_data_simplified, reference_not_matcher, candidate_not_matched = (
        pyramid_matcher.run_matching(
            reference_pyramid=dhis2_pyramid,
            candidate_pyramid=data,
            # levels_to_match=["level_1", "level_2", "level_3", "level_4", "level_5"] # auto
        )
    )

    print(matched_data.head())
    print(matched_data_simplified.head())
    print(reference_not_matcher.head())
    print(candidate_not_matched.head())


if __name__ == "__main__":
    main()
