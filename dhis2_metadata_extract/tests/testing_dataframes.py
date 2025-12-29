from datetime import datetime

import polars as pl

"""This module provides functions to generate sample Polars DataFrames for testing purposes.
These DataFrames simulate typical data structures retrieved using the Dataframe API.
In the ideal world, we would retrieve data directly from a DHIS2 instance for end-to-end testing."""


def get_sample_org_units_df() -> pl.DataFrame:
    """Returns a sample Polars DataFrame representing organizational units with various attributes.

    Returns:
    pl.DataFrame
        A DataFrame containing sample organizational unit data.
    """
    data = {
        "id": ["bL4ooGhyHRQ", "RzKeCma9qb1", "GjJjES51GvK"],
        "name": ["Pujehun", "Barri", "Vaama MCHP"],
        "level": [2, 3, 4],
        "opening_date": [datetime(1970, 1, 1), datetime(1970, 1, 1), datetime(1970, 1, 1)],
        "closed_date": [None, None, None],
        "level_1_id": ["ImspTQPwCqd"] * 3,
        "level_1_name": ["Sierra Leone"] * 3,
        "level_2_id": ["bL4ooGhyHRQ"] * 3,
        "level_2_name": ["Pujehun"] * 3,
        "level_3_id": [None, "RzKeCma9qb1", "RzKeCma9qb1"],
        "level_3_name": [None, "Barri", "Barri"],
        "level_4_id": [None, None, "GjJjES51GvK"],
        "level_4_name": [None, None, "Vaama MCHP"],
        "geometry": [
            '{"type": "MultiPolygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]]}',
            '{"type": "Polygon", "coordinates": [[[2,2],[3,2],[3,3],[2,3],[2,2]]]}',
            '{"type": "Point", "coordinates": [4,4]}',
        ],
    }

    return pl.DataFrame(data)


def get_sample_org_units_groups_df() -> pl.DataFrame:
    """Returns a sample Polars DataFrame representing organizational unit groups.

    Returns:
    pl.DataFrame
        A DataFrame containing sample organizational unit group data.
    """
    data = {
        "id": ["oRVt7g429ZO", "GGghZsfu7qV", "jqBqIXoXpfy", "f25dqv3Y7Z0", "b0EsAxm8Nge"],
        "name": ["Public facilities", "Rural", "Southern Area", "Urban", "Western Area"],
        "organisation_units": [
            ["y77LiPqLMoq", "rwfuVQHnZJ5", "VdXuxcNkiad"],
            ["EQnfnY03sRp", "r06ohri9wA9", "GE25DpSrqpB"],
            ["O6uvpzGd5pu", "jmIPBj66vD6", "lc3eMKXaEfw"],
            ["y77LiPqLMoq", "Z9QaI6sxTwW", "VdXuxcNkiad"],
            ["at6UHUQatSo", "TEQlaapDQoK", "PMa2VCrupOd"],
        ],
    }

    # Create Polars DataFrame
    return pl.DataFrame(data)


def get_sample_datasets_df() -> pl.DataFrame:
    """Returns a sample Polars DataFrame representing datasets.

    Returns:
    pl.DataFrame
        A DataFrame containing sample dataset data.
    """
    data = {
        "id": ["Y8gAn9DfAGU", "QX4ZTUbOt3a", "N4fIX1HL3TQ"],
        "name": ["Project Management", "Reproductive Health", "Staffing"],
        "organisation_units": [
            ["y77LiPqLMoq", "rwfuVQHnZJ5", "VdXuxcNkiad"],
            ["y77LiPqLMoq", "rwfuVQHnZJ5", "VdXuxcNkiad"],
            ["y77LiPqLMoq", "rwfuVQHnZJ5", "VdXuxcNkiad"],
        ],
        "data_elements": [
            ["xPTAT98T2Jd", "JXwI2RLVRwa", "xk0krAO2KfJ"],
            ["rbkr8PL0rwM", "btSSE4w61kd", "hCVSHjcml9g"],
            ["kFmyXB7IYrK", "vBu1MTGwcZh", "Nz5YtOpDyuV"],
        ],
        "indicators": [
            [],  # empty list
            ["gNAXtpqAqW2", "n3fzCxYk3k3", "aEtcFtcJjtZ"],
            ["gNAXtpqAqW2", "n3fzCxYk3k3", "aEtcFtcJjtZ"],
        ],
        "period_type": ["Quarterly", "Monthly", "SixMonthly"],
    }

    # Create Polars DataFrame
    return pl.DataFrame(data)


def get_sample_data_elements_df() -> pl.DataFrame:
    """Returns a sample Polars DataFrame representing data elements.

    Returns:
    pl.DataFrame
        A DataFrame containing sample data element data.
    """
    data = {
        "id": ["l6byfWFUGaP", "hvdCBRWUk80", "XWU1Huh0Luy", "zSJF2b48kOg", "QN8WyI8KgpU"],
        "name": [
            "Yellow Fever doses given",
            "Yellow fever follow-up",
            "Yellow fever new",
            "Yellow fever referrals",
            "YesOnly",
        ],
        "value_type": ["NUMBER", "NUMBER", "NUMBER", "NUMBER", "TRUE_ONLY"],
    }

    # Create Polars DataFrame
    return pl.DataFrame(data)


def get_sample_data_element_groups_df() -> pl.DataFrame:
    """Returns a sample Polars DataFrame representing data element groups.

    Returns:
    pl.DataFrame
        A DataFrame containing sample data element group data.
    """
    data = {
        "id": ["qfxEYY9xAl6", "yhg8oYU9ekY", "M2cth8EmrlT", "k1M0nuodfhN"],
        "name": ["ANC", "ARI Treated Without Antibiotics", "ARI treated with antibiotics", "ART"],
        "data_elements": [
            ["V37YqbqpEhV", "hfdmMSPBgLG", "SA7WeFZnUci"],
            ["Cm4XUw6VAxv", "oLfWYAJhZb2", "RF4VFVGdFRO"],
            ["XTqOHygxDj5", "iKGjnOOaPlE", "FHD3wiSM7Sn"],
            ["wfKKFhBn0Q0", "ZgIaamZjBjz", "MeAvt39JtqN"],
        ],
    }

    # Create Polars DataFrame
    return pl.DataFrame(data)


def get_sample_category_option_combos_df() -> pl.DataFrame:
    """Returns a sample Polars DataFrame representing category option combos.

    Returns:
    pl.DataFrame
        A DataFrame containing sample category option combo data.
    """
    data = {
        "id": [
            "S34ULMcHMca",
            "sqGRzCziswD",
            "o2gxEt6Ek2C",
            "LEDQQXEpWUl",
            "wHBMVthqIX4",
        ],
        "name": [
            "0-11m",
            "0-11m",
            "0-4y",
            "12-59m",
            "12-59m",
        ],
    }
    # Create Polars DataFrame
    return pl.DataFrame(data)
