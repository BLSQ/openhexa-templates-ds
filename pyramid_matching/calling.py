import polars as pl
from matching_pyramids import match_pyramids


def main():
    """Example of calling the match_pyramids function."""
    pyramid = pl.read_csv(
        "/home/leyregarrido/01_github_repos/openhexa-templates-ds/pyramid_matching/dhis2_pyramid.csv"
    )
    data = pl.read_csv(
        "/home/leyregarrido/01_github_repos/openhexa-templates-ds/pyramid_matching/data_to_match_2.csv"
    )
    levels_to_match = ["level_2", "level_3", "level_4", "level_5"]
    threshold = 80
    method = "fuzz_ratio"
    matched_data, data_not_matched, pyramid_not_matched = match_pyramids(
        data, pyramid, threshold=threshold, method=method
    )
    print(matched_data.head())
    print(data_not_matched.head())
    print(pyramid_not_matched.head())
    print("yo")


if __name__ == "__main__":
    main()
