import config
import polars as pl
from matcher.matcher import Matcher
from openhexa.sdk.pipelines.run import CurrentRun


def match_pyramids(
    data: pl.DataFrame,
    pyramid: pl.DataFrame,
    logger: CurrentRun | None = None,
    levels_to_match: list | None = None,
    matching_col_suffix: str = "_name",
    method: str = "fuzzy",
    threshold: int = 80,
    scorer_method_fuzzy: str = "WRatio",
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Match the locations in data with the locations in pyramid.

    It returns a dataframe with the matched locations. It will include the matching columns,
    other attribute columns from the data or pyramid dataframe (p.e., location IDs),
    the score of the matching and whether the same pyramid location has been matched
    to two different data locations.

    Depeding on the method specified, the matching columns will be matched differently.

    Note that the data and pyramid dataframes should be cleaned.
    (a) The column names should be (for the level i) level_i_matching, level_i_id,
    level_i_attribute1, ...
    (b) If there is any pre-cleaning to do (lowercase, no accents, no special characters, etc.),
    it should be done before calling this function.
    (c) We will do the matching in the columns that have the suffix specified in
    matching_col_suffix.

    It returns 4 dataframes:
    - The matched data, with all the information.
    - The simplified matched data, with only the matching columns and no extra information.
    - The data that could not be matched.
    - The pyramid locations that were not matched.

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
        The column matching should be (for the level _alpha_) level_alpha_matching, level_alpha_id,
        level_alpha_attribute1, ...
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
        The column matching should be (for the level _alpha_) level_alpha_matching, level_alpha_id,
        level_alpha_attribute1, ...
    logger : CurrentRun, optional
        The logger to use for logging information during the matching process.
        If None, no logging will be done.
    levels_to_match : list, optional
        The list of levels to match. The levels should be in the order from higher to lower
        in the pyramid (e.g., ["level_2", "level_3"]).
        If None, the levels will be detected automatically by looking for columns in the
        data and pyramid dataframes that end with the matching_col_suffix.
    matching_col_suffix: str, optional
        The suffix of the column that we will do the matching on.
        Default is "_name".
    method : str, optional
        The method to use for the matching. For now, the only available matcher is "fuzzy".
        (but we will add more in the future).
        Default is "fuzzy".
    threshold : int, optional
        The threshold for the matching score. Default is 80.
    scorer_method_fuzzy : str, optional
        The scorer method to use for the fuzzy matching. Only will be used if method=="fuzzy".
        Default is "WRatio".


    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]
        The dataframes with the matched data, the simplified matched data,
        the data not matched, and the pyramid not matched.
    """
    if not isinstance(data, pl.DataFrame):
        data = pl.DataFrame(data)
    if not isinstance(pyramid, pl.DataFrame):
        pyramid = pl.DataFrame(pyramid)

    data = data.unique()
    pyramid = pyramid.unique()

    if levels_to_match is None:
        levels_to_match = _get_levels_to_match(data, pyramid, matching_col_suffix)
        if logger:
            logger.log_info(f"Detected levels to match: {levels_to_match}")
    else:
        _check_levels(data, pyramid, levels_to_match, matching_col_suffix)
        levels_to_match.sort()

    attributes = _get_attributes(data, pyramid, levels_to_match, matching_col_suffix)

    levels_already_matched = []
    list_data_not_matched = []
    list_pyramid_not_matched = []
    data_matched = pl.DataFrame()

    # Initialize matcher
    try:
        matcher = Matcher(
            matcher_type=method, threshold=threshold, scorer_fuzzy=scorer_method_fuzzy
        )
        if logger:
            logger.log_info(f"Using matcher: {matcher!s}")
    except ValueError as e:
        raise ValueError(f"Unknown matching method: {method}") from e

    for level in levels_to_match:
        if logger:
            logger.log_info(f"Matching level {level}...")
        # I am not sure what to do, if to remove this log or to use a current_run one.
        data_matched, data_no_match_level, pyramid_no_match_level = _match_level(
            data_matched,
            data,
            pyramid,
            level,
            levels_already_matched,
            matcher,
            attributes[level],
            matching_col_suffix,
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
        data_matched = _add_repeated_matches(
            data_matched, level, repeated_levels_check, matching_col_suffix
        )
        repeated_levels_check.append(level)

    data_not_matched = pl.concat(list_data_not_matched) if list_data_not_matched else pl.DataFrame()
    pyramid_not_matched = (
        pl.concat(list_pyramid_not_matched) if list_pyramid_not_matched else pl.DataFrame()
    )

    data_matched, data_matched_simplified = _reorder_match_columns(
        data_matched, levels_to_match, attributes, matching_col_suffix
    )

    return data_matched, data_matched_simplified, data_not_matched, pyramid_not_matched


def _reorder_match_columns(
    data: pl.DataFrame, levels_to_match: list, attributes: dict, matching_col_suffix: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Re order the columns in the matched dataframe.

    We want the columns to be ordered as:
    - For each level to match:
        - input matching column
        - input attributes
        - target matching column
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
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

    Returns
    -------
    pl.DataFrame
        The dataframe with the matched data, with the columns reordered.
    pl.DataFrame
        The dataframe with the matched data, without any extra columns
    """
    cols_order_full = []
    cols_order_simple = []
    for level in levels_to_match:
        cols_order_full.append(config.preffix_input_data + level + matching_col_suffix)
        cols_order_simple.append(config.preffix_input_data + level + matching_col_suffix)
        for attr in attributes[level]["data"]:
            cols_order_full.append(config.preffix_input_data + attr)
            cols_order_simple.append(config.preffix_input_data + attr)
        cols_order_full.append(config.preffix_target_data + level + matching_col_suffix)
        cols_order_simple.append(config.preffix_target_data + level + matching_col_suffix)
        for attr in attributes[level]["pyramid"]:
            cols_order_full.append(config.preffix_target_data + attr)
            cols_order_simple.append(config.preffix_target_data + attr)
        cols_order_full.append("score_" + level)
        cols_order_full.append("repeated_matches_" + level)
    other_cols = [col for col in data.columns if col not in cols_order_full]
    return data.select(other_cols + cols_order_full), data.select(cols_order_simple)


def _add_repeated_matches(
    data: pl.DataFrame, level: str, upper_levels: list, matching_col_suffix: str
) -> pl.DataFrame:
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
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

    Returns
    -------
    pl.DataFrame
        The dataframe with the matched data, with an additional column
        indicating if there were repeated matches for the target.
    """
    col_pyramid_name = config.preffix_target_data + level + matching_col_suffix
    col_data_name = config.preffix_input_data + level + matching_col_suffix
    col_repeated_matches = "repeated_matches_" + level
    list_group_by = [
        config.preffix_target_data + level + matching_col_suffix for level in upper_levels
    ] + [col_pyramid_name]

    counts = (
        data.group_by(list_group_by)
        .agg(pl.col(col_data_name).n_unique().alias("count"))
        .filter(pl.col("count") > 1)
    )

    data = data.join(counts, on=list_group_by, how="left")

    return data.with_columns(
        pl.when(pl.col("count").is_null()).then(False).otherwise(True).alias(col_repeated_matches)
    ).drop("count")


def _get_attributes(
    data: pl.DataFrame, pyramid: pl.DataFrame, levels_to_match: list, matching_col_suffix: str
) -> dict:
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
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

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
                level + matching_col_suffix,
            ]:
                attributes_data.append(col)

        attributes_pyramid = []
        for col in pyramid.columns:
            if col.startswith(level + "_") and col not in [
                level + matching_col_suffix,
            ]:
                attributes_pyramid.append(col)

        attributes[level] = {"data": attributes_data, "pyramid": attributes_pyramid}

    return attributes


def _check_levels(
    data: pl.DataFrame, pyramid: pl.DataFrame, levels_to_match: list, matching_col_suffix: str
) -> None:
    """Check if the levels to match are present in both data and pyramid dataframes.

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
    levels_to_match : list
        The list of levels to match.
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

    Raises
    ------
    ValueError
        If any of the levels to match is not present in both data and pyramid dataframes.
    """
    for level in levels_to_match:
        col_name = level + matching_col_suffix
        if col_name not in data.columns:
            raise ValueError(f"Level {level} not present in data dataframe.")
        if col_name not in pyramid.columns:
            raise ValueError(f"Level {level} not present in pyramid dataframe.")


def _get_levels_to_match(
    data: pl.DataFrame, pyramid: pl.DataFrame, matching_col_suffix: str
) -> list:
    """If no levels_to_match are provided, detect the levels to match dynamically.

    We find the levels that are present in both data and pyramid dataframes
    by looking for columns that end with the matching_col_suffix.

    Parameters
    ----------
    data : pl.DataFrame
        The dataframe with the data to be matched.
    pyramid : pl.DataFrame
        The dataframe with the pyramid to match against.
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

    Returns
    -------
    list
        The list of levels to match.
    """
    levels_in_data = set()
    levels_in_pyramid = set()

    for col in data.columns:
        if col.endswith(matching_col_suffix):
            level = col[: -len(matching_col_suffix)]
            levels_in_data.add(level)

    for col in pyramid.columns:
        if col.endswith(matching_col_suffix):
            level = col[: -len(matching_col_suffix)]
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
    matcher: Matcher,
    attributes_level: dict,
    matching_col_suffix: str,
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
    matcher : Matcher
        The matcher to use for the matching.
    attributes_level : dict
        The attributes to include for this level, both for data and pyramid.
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
        The dataframe with the matched data for this level.
    """
    schema_match = (
        [config.preffix_input_data + target_level + matching_col_suffix]
        + [config.preffix_target_data + target_level + matching_col_suffix]
        + [f"{config.preffix_target_data}{attr}" for attr in attributes_level["pyramid"]]
        + ["score_" + target_level]
        + [f"{config.preffix_input_data}{attr}" for attr in attributes_level["data"]]
    )
    if already_matched.is_empty():
        return _match_level_group(
            pyramid,
            data,
            target_level,
            matcher,
            attributes_level,
            schema_match,
            matching_col_suffix,
        )

    list_match = []
    list_no_match_data = []
    list_no_match_pyramid = []
    for row in already_matched.iter_rows(named=True):
        pyramid_group, data_group = _select_group(
            data, pyramid, levels_already_matched, row, matching_col_suffix
        )
        df_match_level_group, unmatched_data_group, unmatched_pyramid_group = _match_level_group(
            pyramid_group,
            data_group,
            target_level,
            matcher,
            attributes_level,
            schema_match,
            matching_col_suffix,
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
        The row with the already matched data. It is a dictionary with the matching column as keys.
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
    matcher: Matcher,
    attributes_level: dict,
    schema_match: list,
    matching_col_suffix: str,
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
    matcher : Matcher
        The matcher to use for the matching.
    attributes_level : dict
        The attributes to include for this level, both for data and pyramid.
    schema_match: list
        The schema for the matched dataframe.
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
        A tuple containing the matched data, unmatched data, and unmatched pyramid data.
    """
    col_name = level + matching_col_suffix

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

    for name_to_match, attributes_data in data_to_match.items():
        matches = matcher.match(name_to_match, pyramid_to_match)
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

    matched_data_names = df_matches[
        config.preffix_input_data + level + matching_col_suffix
    ].unique()
    df_unmatched_data = data.filter(~pl.col(col_name).is_in(matched_data_names))

    matched_pyramid_names = df_matches[
        config.preffix_target_data + level + matching_col_suffix
    ].unique()
    df_unmatched_pyramid = pyramid.filter(~pl.col(col_name).is_in(matched_pyramid_names))

    return df_matches, df_unmatched_data, df_unmatched_pyramid


def _select_group(
    data: pl.DataFrame,
    pyramid: pl.DataFrame,
    levels_already_matched: list,
    row: dict,
    matching_col_suffix: str,
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
        The row with the already matched data. It is a dictionary with the matching column as keys.
    matching_col_suffix : str
        The suffix of the column that we will do the matching on.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame]
        A tuple containing the relevant data and relevant pyramid dataframes.
    """
    relevant_data = data.clone()
    relevant_pyramid = pyramid.clone()
    for level in levels_already_matched:
        relevant_data = relevant_data.filter(
            pl.col(f"{level}{matching_col_suffix}")
            == row[f"{config.preffix_input_data}{level}{matching_col_suffix}"]
        )
        relevant_pyramid = relevant_pyramid.filter(
            pl.col(f"{level}{matching_col_suffix}")
            == row[f"{config.preffix_target_data}{level}{matching_col_suffix}"]
        )

    return relevant_pyramid, relevant_data
