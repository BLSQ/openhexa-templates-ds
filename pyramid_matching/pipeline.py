import polars as pl
from matching_pyramids import match_pyramids
from openhexa.sdk.pipelines import current_run, pipeline


@pipeline("matching_pyramids")
def matching_pyramids():
    """Extract dataset from DHIS2."""
    current_run.log_info("Running the pipeline")
    pyramid = pl.read_csv(
        "/home/leyregarrido/01_github_repos/openhexa-templates-ds/pyramid_matching/data/dhis2_pyramid.csv"
    )
    data = pl.read_csv(
        "/home/leyregarrido/01_github_repos/openhexa-templates-ds/pyramid_matching/data/data_to_match_2.csv"
    )
    # levels_to_match = ["level_2", "level_3", "level_4", "level_5"]
    # threshold = 80
    # method = "fuzz_ratio"
    matched_data, matched_data_simplified, data_not_matched, pyramid_not_matched = match_pyramids(
        data, pyramid, current_run
    )
    print(matched_data.head())
    print(data_not_matched.head())
    print(pyramid_not_matched.head())
    print(matched_data_simplified.head())


if __name__ == "__main__":
    matching_pyramids()
