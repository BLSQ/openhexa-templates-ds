import polars as pl
from openhexa.sdk.pipelines.run import CurrentRun

from matcher import Matcher


class PyramidMatcher:
    """A class to perform hierarchical matching between a reference pyramid and a candidate pyramid.

    This class allows using customizable matching logic.

    Attributes
    ----------
    reference_pyramid : pl.DataFrame | None
        The reference pyramid DataFrame.
    candidate_pyramid : pl.DataFrame | None
        The candidate pyramid DataFrame.
    matcher : Matcher | None
        The matcher instance used for matching.
    required_columns : set
        The set of required columns for the pyramid DataFrames.
    preffix_input_data : str
        Prefix for input data columns.
    preffix_target_data : str
        Prefix for target data columns.
    logger : object
        Logger for logging messages.

    Methods
    -------
    run_matching(
        reference_pyramid,
        candidate_pyramid,
        levels_to_match=None,
        matching_col_suffix="_name")
    Perform hierarchical matching between the reference and candidate pyramids.
    """

    def __init__(self):
        self.reference_pyramid: pl.DataFrame | None = None
        self.candidate_pyramid: pl.DataFrame | None = None
        self.matcher: Matcher | None = None
        self.required_columns = {
            "level_1_name",
            "level_2_name",
            "level_3_name",
            "level_4_name",
            "level_5_name",
        }
        self.preffix_input_data: str = "input_"
        self.preffix_target_data: str = "target_"
        self.logger = CurrentRun  # add a default logger that just prints to console

    def _set_reference_pyramid(self, reference_pyramid: pl.DataFrame):
        """Load the reference pyramid."""
        if not isinstance(reference_pyramid, pl.DataFrame):
            reference_pyramid = pl.DataFrame(reference_pyramid)

        if self._is_valid(reference_pyramid):
            self._set_reference_pyramid(reference_pyramid.unique())
            # Log some details about the pyramid, like number of rows, columns, levels detected, etc.
        else:
            raise ValueError(
                "Invalid reference pyramid format. "
                "Please provide a DataFrame with the required columns."
            )

    def _set_candidate_pyramid(self, candidate_pyramid: pl.DataFrame):
        """Load the candidate pyramid."""
        if not isinstance(candidate_pyramid, pl.DataFrame):
            candidate_pyramid = pl.DataFrame(candidate_pyramid)

        if self._is_valid(candidate_pyramid):
            self.candidate_pyramid = candidate_pyramid.unique()
            # Log some details about the pyramid, like number of rows, columns, levels detected, etc.
        else:
            raise ValueError(
                "Invalid candidate pyramid format. "
                "Please provide a DataFrame with the required columns."
            )

    def _is_valid(self, pyramid: pl.DataFrame) -> bool:
        """Check if the pyramid has the required columns.

        Returns:
            bool: True if the pyramid has all required columns, False otherwise.
        """
        return self.required_columns.issubset(set(pyramid.columns))

    def run_matching(
        self,
        reference_pyramid: pl.DataFrame,
        candidate_pyramid: pl.DataFrame,
        levels_to_match: list | None = None,
        matching_col_suffix: str = "_name",
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Match data to a pyramid using the specified method and threshold.

        Args:
            reference_pyramid (pl.DataFrame):
                The input data frame that will work as reference.
            candidate_pyramid (pl.DataFrame):
                The pyramid to match against.
            levels_to_match (list, optional):
                List of levels to match; if None, levels are detected automatically.
            matching_col_suffix (str, optional):
                The suffix of the column in the pyramid to match against. Defaults to "_name".

        Returns:
            tuple: A tuple containing the matched data, simplified matched data, unmatched data,
            and unmatched pyramid entries.
        """
        if reference_pyramid is None or candidate_pyramid is None:
            raise ValueError("Both reference_pyramid and candidate_pyramid must be provided.")
        self._set_reference_pyramid(reference_pyramid)
        self._set_candidate_pyramid(candidate_pyramid)

        if levels_to_match is None:
            levels_to_match = self._get_levels_to_match(matching_col_suffix)
            self._log(f"Detected levels to match: {levels_to_match}")
        else:
            self._check_levels(levels_to_match, matching_col_suffix)
            levels_to_match.sort()

        attributes = self._get_attributes(levels_to_match, matching_col_suffix)

        levels_already_matched = []
        list_data_not_matched = []
        list_pyramid_not_matched = []
        data_matched = pl.DataFrame()

        if self.matcher is None:
            self.matcher = Matcher(matcher_type="fuzzy", threshold=80, scorer_fuzzy="wratio")
            self._log(f"Using default matcher: {self.matcher}")

        for level in levels_to_match:
            self._log(f"Matching level {level}...")
            # I am not sure what to do, if to remove this log or to use a current_run one.
            data_matched, data_no_match_level, pyramid_no_match_level = self._match_level(
                already_matched=data_matched,
                target_level=level,
                levels_already_matched=levels_already_matched,
                attributes_level=attributes[level],
                matching_col_suffix=matching_col_suffix,
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
            data_matched = self._add_repeated_matches(
                data_matched, level, repeated_levels_check, matching_col_suffix
            )
            repeated_levels_check.append(level)

        data_not_matched = (
            pl.concat(list_data_not_matched) if list_data_not_matched else pl.DataFrame()
        )
        pyramid_not_matched = (
            pl.concat(list_pyramid_not_matched) if list_pyramid_not_matched else pl.DataFrame()
        )

        data_matched, data_matched_simplified = self._reorder_match_columns(
            data_matched, levels_to_match, attributes, matching_col_suffix
        )

        return data_matched, data_matched_simplified, data_not_matched, pyramid_not_matched

    def _log(self, message: str, level: str = "info") -> None:
        """Log a message with the specified level."""
        if self.logger:
            if level == "info":
                self.logger.log_info(message)
            elif level == "warning":
                self.logger.log_warning(message)
            elif level == "error":
                self.logger.log_error(message)
            else:
                raise ValueError(f"Unknown log level: {level}")

    def _get_levels_to_match(self, matching_col_suffix: str) -> list:
        """If no levels_to_match are provided, detect the levels to match dynamically.

        We find the levels that are present in both data and pyramid dataframes
        by looking for columns that end with the matching_col_suffix.

        Parameters
        ----------
        matching_col_suffix : str
            The suffix of the column that we will do the matching on.

        Returns
        -------
        list
            The list of levels to match.
        """
        levels_in_data = set()
        levels_in_pyramid = set()

        for col in self.reference_pyramid.columns:
            if col.endswith(matching_col_suffix):
                level = col[: -len(matching_col_suffix)]
                levels_in_data.add(level)

        for col in self.candidate_pyramid.columns:
            if col.endswith(matching_col_suffix):
                level = col[: -len(matching_col_suffix)]
                levels_in_pyramid.add(level)

        actual_levels = list(levels_in_data.intersection(levels_in_pyramid))
        actual_levels.sort()

        return actual_levels

    def _match_level(
        self,
        already_matched: pl.DataFrame,
        target_level: str,
        levels_already_matched: list,
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
        target_level : str
            The level to match.
        levels_already_matched : list
            The list of levels already matched.
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
            [self.preffix_input_data + target_level + matching_col_suffix]
            + [self.preffix_target_data + target_level + matching_col_suffix]
            + [f"{self.preffix_target_data}{attr}" for attr in attributes_level["pyramid"]]
            + ["score_" + target_level]
            + [f"{self.preffix_input_data}{attr}" for attr in attributes_level["data"]]
        )
        if already_matched.is_empty():
            return self._match_level_group(
                self.reference_pyramid,
                self.candidate_pyramid,
                level=target_level,
                attributes_level=attributes_level,
                schema_match=schema_match,
                matching_col_suffix=matching_col_suffix,
            )

        list_match = []
        list_no_match_data = []
        list_no_match_pyramid = []
        for row in already_matched.iter_rows(named=True):
            data_group, pyramid_group = self._select_group(
                levels_already_matched, row, matching_col_suffix
            )
            df_match_level_group, unmatched_data_group, unmatched_pyramid_group = (
                self._match_level_group(
                    data_group,
                    pyramid_group,
                    target_level,
                    attributes_level,
                    schema_match,
                    matching_col_suffix,
                )
            )
            if len(df_match_level_group) > 0:
                df_match_level_group = self._add_already_matched_levels(row, df_match_level_group)
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

    def _check_levels(
        self,
        levels_to_match: list,
        matching_col_suffix: str,
    ) -> None:
        """Check if the levels to match are present in both data and pyramid dataframes.

        Parameters
        ----------
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
            if col_name not in self.reference_pyramid.columns:
                raise ValueError(f"Level {level} not present in data dataframe.")
            if col_name not in self.candidate_pyramid.columns:
                raise ValueError(f"Level {level} not present in pyramid dataframe.")

    def _match_level_group(
        self,
        reference_group: pl.DataFrame,
        candidate_group: pl.DataFrame,
        level: str,
        attributes_level: dict,
        schema_match: list,
        matching_col_suffix: str,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Match a level of the data with the corresponding level of the pyramid.

        We assume that all of the data in data can be matched against the pyramid.

        Parameters
        ----------
        reference_group : pl.DataFrame
            The group of pyramid data to match against.
        candidate_group : pl.DataFrame
            The group of input data to be matched.
        level : str
            The level to match.
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
            for row in reference_group.select([col_name] + attributes_level["data"]).unique().rows()
        }
        pyramid_to_match = {
            row[0]: list(row[1:])
            for row in candidate_group.select([col_name] + attributes_level["pyramid"])
            .unique()
            .rows()
        }

        list_matches = []
        # The list will contain some lists with the matched names, attributes, and scores.

        for name_to_match, attributes_data in data_to_match.items():
            matches = self.matcher.match(name_to_match, pyramid_to_match)
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
            self.preffix_input_data + level + matching_col_suffix
        ].unique()
        df_unmatched_data = self.reference_pyramid.filter(
            ~pl.col(col_name).is_in(matched_data_names)
        )

        matched_pyramid_names = df_matches[
            self.preffix_target_data + level + matching_col_suffix
        ].unique()
        df_unmatched_pyramid = self.candidate_pyramid.filter(
            ~pl.col(col_name).is_in(matched_pyramid_names)
        )

        return df_matches, df_unmatched_data, df_unmatched_pyramid

    def _add_already_matched_levels(self, row: dict, df_match_row: pl.DataFrame) -> pl.DataFrame:
        """Add the information from the levels that were already matched to the new matched row.

        Add the information about the results of the matching of the level_i
        to the results of the matching of level_i+1.

        Parameters
        ----------
        row : dict
            The row with the matched data. It is a dictionary with the matching column as keys.
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

    def _select_group(
        self,
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
        levels_already_matched : list
            The list of levels already matched.
        row : dict
            The row with the matched data. It is a dictionary with the matching column as keys.
        matching_col_suffix : str
            The suffix of the column that we will do the matching on.

        Returns
        -------
        tuple[pl.DataFrame, pl.DataFrame]
            A tuple containing the relevant data and relevant pyramid dataframes.
        """
        relevant_data = self.reference_pyramid.clone()
        relevant_pyramid = self.candidate_pyramid.clone()
        for level in levels_already_matched:
            relevant_data = relevant_data.filter(
                pl.col(f"{level}{matching_col_suffix}")
                == row[f"{self.preffix_input_data}{level}{matching_col_suffix}"]
            )
            relevant_pyramid = relevant_pyramid.filter(
                pl.col(f"{level}{matching_col_suffix}")
                == row[f"{self.preffix_target_data}{level}{matching_col_suffix}"]
            )

        return relevant_data, relevant_pyramid

    def _get_attributes(self, levels_to_match: list, matching_col_suffix: str) -> dict:
        """Get the attributes in each of the levels to match, for both the pyramid and the data.

        We will then add them in the output matched dataframe.

        Parameters
        ----------
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
            for col in self.reference_pyramid.columns:
                if col.startswith(level + "_") and col not in [
                    level + matching_col_suffix,
                ]:
                    attributes_data.append(col)

            attributes_pyramid = []
            for col in self.candidate_pyramid.columns:
                if col.startswith(level + "_") and col not in [
                    level + matching_col_suffix,
                ]:
                    attributes_pyramid.append(col)

            attributes[level] = {"data": attributes_data, "pyramid": attributes_pyramid}

        return attributes

    def _add_repeated_matches(
        self, data: pl.DataFrame, level: str, upper_levels: list, matching_col_suffix: str
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
        col_pyramid_name = self.preffix_target_data + level + matching_col_suffix
        col_data_name = self.preffix_input_data + level + matching_col_suffix
        col_repeated_matches = "repeated_matches_" + level
        list_group_by = [
            self.preffix_target_data + level + matching_col_suffix for level in upper_levels
        ] + [col_pyramid_name]

        counts = (
            data.group_by(list_group_by)
            .agg(pl.col(col_data_name).n_unique().alias("count"))
            .filter(pl.col("count") > 1)
        )

        data = data.join(counts, on=list_group_by, how="left")

        return data.with_columns(
            pl.when(pl.col("count").is_null())
            .then(False)
            .otherwise(True)
            .alias(col_repeated_matches)
        ).drop("count")

    def _reorder_match_columns(
        self, data: pl.DataFrame, levels_to_match: list, attributes: dict, matching_col_suffix: str
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
            cols_order_full.append(self.preffix_input_data + level + matching_col_suffix)
            cols_order_simple.append(self.preffix_input_data + level + matching_col_suffix)
            for attr in attributes[level]["data"]:
                cols_order_full.append(self.preffix_input_data + attr)
                cols_order_simple.append(self.preffix_input_data + attr)
            cols_order_full.append(self.preffix_target_data + level + matching_col_suffix)
            cols_order_simple.append(self.preffix_target_data + level + matching_col_suffix)
            for attr in attributes[level]["pyramid"]:
                cols_order_full.append(self.preffix_target_data + attr)
                cols_order_simple.append(self.preffix_target_data + attr)
            cols_order_full.append("score_" + level)
            cols_order_full.append("repeated_matches_" + level)
        other_cols = [col for col in data.columns if col not in cols_order_full]
        return data.select(other_cols + cols_order_full), data.select(cols_order_simple)
