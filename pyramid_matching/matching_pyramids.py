import config
import polars as pl
from matching_names import fuzz_score


def match_pyramids(
    data: pl.DataFrame,
    pyramid: pl.DataFrame,
    levels_to_match: list | None = None,
    threshold: int = 80,
    method: str = "fuzz_ratio",
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Match the locations in data with the locations in pyramid.

    It returns a dataframe with the matched locations. It will include the name columns,
    other attribute columns from the data or pyramid dataframe (p.e., location IDs),
    the score of the matching and whether the same pyramid location has been matched
    to two different data locations.

    Depeding on the method specified, the names will be matched differently.

    Note that the data and pyramid dataframes should be cleaned.
    (a) The column names should be (for the level i) level_i_name, level_i_id,
    level_i_attribute1, ...
    (b) If there is any pre-cleaning to do (lowercase, no accents, no special characters, etc.),
    it should be done before calling this function.

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
        The column names should be (for the level _alpha_) level_alpha_name, level_alpha_id,
        level_alpha_attribute1, ...
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
        The column names should be (for the level _alpha_) level_alpha_name, level_alpha_id,
        level_alpha_attribute1, ...
    levels_to_match : list
        The list of levels to match. The levels should be in the order from higher to lower
        in the pyramid (e.g., ["level_2", "level_3"]).
    threshold : int, optional
        The threshold for the matching score. Default is 80.
    method : str, optional
        The method to use for the matching. Default is "fuzz_ratio".

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
        The dataframes with the matched data, the data not matched, and the pyramid not matched.
    """
    if not isinstance(data, pl.DataFrame):
        data = pl.DataFrame(data)
    if not isinstance(pyramid, pl.DataFrame):
        pyramid = pl.DataFrame(pyramid)

    data = data.unique()
    pyramid = pyramid.unique()

    if levels_to_match is None:
        levels_to_match = _get_levels_to_match(data, pyramid)
    else:
        _check_levels(data, pyramid, levels_to_match)
        levels_to_match.sort()

    attributes = _get_attributes(data, pyramid, levels_to_match)

    levels_already_matched = []
    list_data_not_matched = []
    list_pyramid_not_matched = []
    data_matched = pl.DataFrame()

    for level in levels_to_match:
        print(f"Matching level {level}...")
        # I am not sure what to do, if to remove this log or to use a current_run one.
        data_matched, data_no_match_level, pyramid_no_match_level = _match_level(
            data_matched,
            data,
            pyramid,
            level,
            levels_already_matched,
            threshold,
            method,
            attributes[level],
        )
        levels_already_matched.append(level)
        if not data_no_match_level.is_empty():
            data_no_match_level = data_no_match_level.with_columns(
                pl.lit(level).alias("unmatched_level")
            )
            list_data_not_matched.append(data_no_match_level)

        if not pyramid_no_match_level.is_empty():
            pyramid_no_match_level = pyramid_no_match_level.with_columns(
                pl.lit(level).alias("unmatched_level")
            )
            list_pyramid_not_matched.append(pyramid_no_match_level)

    repeated_levels_check = []
    for level in levels_to_match:
        data_matched = _add_repeated_matches(data_matched, level, repeated_levels_check)
        repeated_levels_check.append(level)

    data_not_matched = pl.concat(list_data_not_matched) if list_data_not_matched else pl.DataFrame()
    pyramid_not_matched = (
        pl.concat(list_pyramid_not_matched) if list_pyramid_not_matched else pl.DataFrame()
    )

    data_matched = _reorder_match_columns(data_matched, levels_to_match, attributes)

    return data_matched, data_not_matched, pyramid_not_matched


def _reorder_match_columns(
    data: pl.DataFrame, levels_to_match: list, attributes: dict
) -> pl.DataFrame:
    """Re order the columns in the matched dataframe.

    We want the columns to be ordered as:
    - For each level to match:
        - input name
        - input attributes
        - target name
        - target attributes
        - score
        - repeated matches indicator

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the matched data.
    levels_to_match : list
        The list of levels to match.
    attributes : dict
        The attributes in each level, both for data and pyramid.

    Returns
    -------
    pl.DataFrame
        The dataframe with the matched data, with the columns reordered.
    """
    cols_order = []
    for level in levels_to_match:
        cols_order.append(config.preffix_input_data + level + "_name")
        for attr in attributes[level]["data"]:
            cols_order.append(config.preffix_input_data + attr)
        cols_order.append(config.preffix_target_data + level + "_name")
        for attr in attributes[level]["pyramid"]:
            cols_order.append(config.preffix_target_data + attr)
        cols_order.append("score_" + level)
        cols_order.append("repeated_matches_" + level)
    other_cols = [col for col in data.columns if col not in cols_order]
    return data.select(other_cols + cols_order)


def _add_repeated_matches(data: pl.DataFrame, level: str, upper_levels: list) -> pl.DataFrame:
    """If we have matched the same target twice to different inputs, mark them.

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the matched data.
    level : str
        The level to check for repeated matches.
    upper_levels : list
        The list of levels above the current level.
        We also need them to match.

    Returns
    -------
    pl.DataFrame
        The dataframe with the matched data, with an additional column
        indicating if there were repeated matches for the target.
    """
    col_pyramid_name = config.preffix_target_data + level + "_name"
    col_data_name = config.preffix_input_data + level + "_name"
    col_repeated_matches = "repeated_matches_" + level
    list_group_by = [config.preffix_target_data + level + "_name" for level in upper_levels] + [
        col_pyramid_name
    ]

    counts = (
        data.group_by(list_group_by)
        .agg(pl.col(col_data_name).n_unique().alias("count"))
        .filter(pl.col("count") > 1)
    )

    data = data.join(counts, on=list_group_by, how="left")

    return data.with_columns(
        pl.when(pl.col("count").is_null()).then(False).otherwise(True).alias(col_repeated_matches)
    ).drop("count")


def _get_attributes(data: pl.DataFrame, pyramid: pl.DataFrame, levels_to_match: list) -> dict:
    """Get the attributes in each of the levels to match, for both the pyramid and the data.

    We will then add them in the output matched dataframe.

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
    levels_to_match : list
        The list of levels to match.

    Returns
    -------
    dict
        Per each of the levels to match, a list with the attribute columns to include.
    """
    attributes = {}

    for level in levels_to_match:
        attributes_data = []
        for col in data.columns:
            if col.startswith(level + "_") and col not in [
                level + "_name",
            ]:
                attributes_data.append(col)

        attributes_pyramid = []
        for col in pyramid.columns:
            if col.startswith(level + "_") and col not in [
                level + "_name",
            ]:
                attributes_pyramid.append(col)

        attributes[level] = {"data": attributes_data, "pyramid": attributes_pyramid}

    return attributes


def _check_levels(data: pl.DataFrame, pyramid: pl.DataFrame, levels_to_match: list) -> None:
    """Check if the levels to match are present in both data and pyramid dataframes.

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
    levels_to_match : list
        The list of levels to match.

    Raises
    ------
    ValueError
        If any of the levels to match is not present in both data and pyramid dataframes.
    """
    for level in levels_to_match:
        col_name = level + "_name"
        if col_name not in data.columns:
            raise ValueError(f"Level {level} not present in data dataframe.")
        if col_name not in pyramid.columns:
            raise ValueError(f"Level {level} not present in pyramid dataframe.")


def _get_levels_to_match(data: pl.DataFrame, pyramid: pl.DataFrame) -> list:
    """If no levels_to_match are provided, detect the levels to match dynamically.

    We find the levels that are present in both data and pyramid dataframes
    by looking for columns that end with "_name".

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.

    Returns
    -------
    list
        The list of levels to match.
    """
    levels_in_data = set()
    levels_in_pyramid = set()

    for col in data.columns:
        if col.endswith("_name"):
            level = col[:-5]
            levels_in_data.add(level)

    for col in pyramid.columns:
        if col.endswith("_name"):
            level = col[:-5]
            levels_in_pyramid.add(level)

    actual_levels = list(levels_in_data.intersection(levels_in_pyramid))
    actual_levels.sort()

    return actual_levels


def _match_level(
    already_matched: pl.DataFrame,
    data: pl.DataFrame,
    pyramid: pl.DataFrame,
    target_level: str,
    levels_already_matched: list,
    threshold: int,
    method: str,
    attributes_level: dict,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Do the matching for a particular level, taking into account previously matched levels.

    It matches a level of the data with the corresponding level of the pyramid.
    If there are already matched levels, it takes them into account:
    in order for the level_i to match, the level_i-1 should have been matched already.

    Parameters
    ----------
    already_matched : pl.DataFrame
        The dataframe with the already matched data.
    data : pl.DataFrame
        The dataframe with the data to be matched.
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
    target_level : str
        The level to match.
    levels_already_matched : list
        The list of levels already matched.
    threshold : int
        The threshold for the matching score.
    method : str
        The method to use for the matching.
    attributes_level : dict
        The attributes to include for this level, both for data and pyramid.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
        The dataframe with the matched data for this level.
    """
    schema_match = (
        [config.preffix_input_data + target_level + "_name"]
        + [config.preffix_target_data + target_level + "_name"]
        + [f"{config.preffix_target_data}{attr}" for attr in attributes_level["pyramid"]]
        + ["score_" + target_level]
        + [f"{config.preffix_input_data}{attr}" for attr in attributes_level["data"]]
    )
    if already_matched.is_empty():
        return _match_level_group(
            pyramid,
            data,
            target_level,
            threshold,
            method,
            attributes_level,
            schema_match,
        )

    list_match = []
    list_no_match_data = []
    list_no_match_pyramid = []
    for row in already_matched.iter_rows(named=True):
        pyramid_group, data_group = _select_group(data, pyramid, levels_already_matched, row)
        df_match_level_group, unmatched_data_group, unmatched_pyramid_group = _match_level_group(
            pyramid_group,
            data_group,
            target_level,
            threshold,
            method,
            attributes_level,
            schema_match,
        )
        if len(df_match_level_group) > 0:
            df_match_level_group = _add_already_matched_levels(row, df_match_level_group)
            list_match.append(df_match_level_group)

        if len(unmatched_data_group) > 0:
            list_no_match_data.append(unmatched_data_group)

        if len(unmatched_pyramid_group) > 0:
            list_no_match_pyramid.append(unmatched_pyramid_group)

    df_match = pl.concat(list_match) if len(list_match) > 0 else pl.DataFrame()
    df_no_match_data = (
        pl.concat(list_no_match_data) if len(list_no_match_data) > 0 else pl.DataFrame()
    )
    df_no_match_pyramid = (
        pl.concat(list_no_match_pyramid) if len(list_no_match_pyramid) > 0 else pl.DataFrame()
    )

    return df_match, df_no_match_data, df_no_match_pyramid


def _add_already_matched_levels(row: dict, df_match_row: pl.DataFrame) -> pl.DataFrame:
    """Add the information from the levels that were already matched to the new matched row.

    Add the information about the results of the matching of the level_i
    to the results of the matching of level_i+1.

    Parameters
    ----------
    row : dict
        The row with the already matched data. It is a dictionary with the column names as keys.
    df_match_row : pl.DataFrame
        The dataframe with the newly matched data for the current level.

    Returns
    -------
    pl.DataFrame
        The dataframe with the newly matched data for the current level.
    """
    for col, val in row.items():
        df_match_row = df_match_row.with_columns(pl.lit(val).alias(col))
    return df_match_row


def _match_level_group(
    pyramid: pl.DataFrame,
    data: pl.DataFrame,
    level: str,
    threshold: int,
    method: str,
    attributes_level: dict,
    schema_match: list,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Match a level of the data with the corresponding level of the pyramid.

    We assume that all of the data in data can be matched against the pyramid.

    Parameters
    ----------
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
    data : pl.DataFrame
        The dataframe with the data to be matched.
    level : str
        The level to match.
    threshold : int
        The threshold for the matching score.
    method : str
        The method to use for the matching.
    attributes_level : dict
        The attributes to include for this level, both for data and pyramid.
    schema_match: list
        The schema for the matched dataframe.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
        A tuple containing the matched data, unmatched data, and unmatched pyramid data.
    """
    col_name = level + "_name"

    data_to_match = {
        row[0]: list(row[1:])
        for row in data.select([col_name] + attributes_level["data"]).unique().rows()
    }
    pyramid_to_match = {
        row[0]: list(row[1:])
        for row in pyramid.select([col_name] + attributes_level["pyramid"]).unique().rows()
    }

    list_matches = []
    # The list will contain some lists with the matched names, attributes, and scores.

    if method == "fuzz_ratio":
        for name_to_match, attributes_data in data_to_match.items():
            matches = fuzz_score(name_to_match, pyramid_to_match, threshold)
            if matches:
                list_matches.append(matches + attributes_data)

        if len(list_matches) > 0:
            df_matches = pl.DataFrame(
                list_matches,
                schema=schema_match,
                orient="row",
            )
        else:
            df_matches = pl.DataFrame(schema=schema_match)
    else:
        raise ValueError(f"Method {method} not recognized.")

    matched_data_names = df_matches[config.preffix_input_data + level + "_name"].unique()
    df_unmatched_data = data.filter(~pl.col(col_name).is_in(matched_data_names))

    matched_pyramid_names = df_matches[config.preffix_target_data + level + "_name"].unique()
    df_unmatched_pyramid = pyramid.filter(~pl.col(col_name).is_in(matched_pyramid_names))

    return df_matches, df_unmatched_data, df_unmatched_pyramid


def _select_group(
    data: pl.DataFrame,
    pyramid: pl.DataFrame,
    levels_already_matched: list,
    row: dict,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Select the group of data and pyramid that match the already matched levels.

    We will use this function to make sure that when matching level_i,
    we only consider the data and pyramid
    that match the already matched levels (level_1, ..., level_i-1).

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
    levels_already_matched : list
        The list of levels already matched.
    row : dict
        The row with the already matched data. It is a dictionary with the column names as keys.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame]
        A tuple containing the relevant data and relevant pyramid dataframes.
    """
    relevant_data = data.clone()
    relevant_pyramid = pyramid.clone()
    for level in levels_already_matched:
        relevant_data = relevant_data.filter(
            pl.col(f"{level}_name") == row[f"{config.preffix_input_data}{level}_name"]
        )
        relevant_pyramid = relevant_pyramid.filter(
            pl.col(f"{level}_name") == row[f"{config.preffix_target_data}{level}_name"]
        )

    return relevant_pyramid, relevant_data
