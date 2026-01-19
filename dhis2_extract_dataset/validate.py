from typing import NotRequired, TypedDict

import polars as pl
from openhexa.sdk import current_run
from polars._typing import PolarsDataType


class ExpectedColumn(TypedDict):
    """Specification for a single expected DataFrame column.

    Defines the schema requirements for a column in a Polars DataFrame.
    Used by `validate_data` to enforce column presence, data type, nullability,
    string length, and integer convertibility.

    Attributes:
        name (str):
            The expected column name.
        type (PolarsDataType):
            The expected Polars data type of the column.
        not_null (bool):
            Whether the column must not contain null or empty-string values.
        number_of_characters (int, optional):
            Maximum allowed number of characters for string values. If specified,
            all values must match this exact character length.
        can_be_converted_to_integer (bool, optional):
            Whether all values in the column must be safely convertible to integers.
    """

    name: str
    type: PolarsDataType
    not_null: bool
    number_of_characters: NotRequired[int]
    can_be_converted_to_integer: NotRequired[bool]


expected_columns_full: list[ExpectedColumn] = [
    {"name": "data_element_id", "type": pl.String, "not_null": True},
    {"name": "data_element_name", "type": pl.String, "not_null": True},
    {"name": "organisation_unit_id", "type": pl.String, "not_null": True},
    {"name": "category_option_combo_id", "type": pl.String, "not_null": True},
    {"name": "attribute_option_combo_id", "type": pl.String, "not_null": True},
    {"name": "category_option_combo_name", "type": pl.String, "not_null": True},
    {"name": "period", "type": pl.String, "not_null": True},
    {"name": "value", "type": pl.String, "not_null": True},
    {"name": "level_1_id", "type": pl.String, "not_null": True},
    {"name": "level_2_id", "type": pl.String, "not_null": False},
    {"name": "level_3_id", "type": pl.String, "not_null": False},
    {"name": "level_4_id", "type": pl.String, "not_null": False},
    {"name": "level_5_id", "type": pl.String, "not_null": False},
    {"name": "level_6_id", "type": pl.String, "not_null": False},
    {"name": "level_7_id", "type": pl.String, "not_null": False},
    {"name": "level_1_name", "type": pl.String, "not_null": True},
    {"name": "level_2_name", "type": pl.String, "not_null": False},
    {"name": "level_3_name", "type": pl.String, "not_null": False},
    {"name": "level_4_name", "type": pl.String, "not_null": False},
    {"name": "level_5_name", "type": pl.String, "not_null": False},
    {"name": "level_6_name", "type": pl.String, "not_null": False},
    {"name": "level_7_name", "type": pl.String, "not_null": False},
    {"name": "created", "type": pl.Datetime, "not_null": True},
    {"name": "last_updated", "type": pl.Datetime, "not_null": True},
    {"name": "dataset", "type": pl.String, "not_null": True},
    {"name": "period_type_configured_dataset", "type": pl.String, "not_null": True},
    {"name": "period_type_extracted", "type": pl.String, "not_null": True},
]


def validate_data(df: pl.DataFrame) -> None:
    """Validate a Polars DataFrame against a predefined schema.

    This function performs a comprehensive validation of the DataFrame against
    the `expected_columns` schema. It checks for structural and data-quality issues,
    and raises a single `RuntimeError` summarizing all violations.

    Validation rules include:
        * DataFrame must not be empty.
        * Only expected columns are allowed; unexpected columns are flagged.
        * Each column must match the expected Polars data type.
        * Columns marked `not_null` cannot contain null or empty-string values.
        * Columns with a `number_of_characters` constraint must have exactly
          the specified number of characters.
        * Columns marked `can_be_converted_to_integer` must be safely castable
          to integers.

    Args:
        df (pl.DataFrame): The Polars DataFrame to validate.

    Raises:
        RuntimeError: If any of the validation rules are violated. Potential
        issues include:
            - Empty DataFrame
            - Unexpected or extra columns
            - Column data type mismatches
            - Null or empty values in non-nullable columns
            - String values exceeding or not matching the required character length
            - Values that cannot be converted to integers when expected
    """
    error_messages = ["\n"]
    if df.height == 0:
        error_messages.append("data_values is empty")

    max_org_unit_level = get_max_org_unit_level(df)
    expected_columns = adapt_cols_to_max_level(expected_columns_full, max_org_unit_level)

    # checking for unvalidated columns
    expected_column_names = [col["name"] for col in expected_columns]
    unvalidated_columns = [col for col in df.columns if col not in expected_column_names]
    if len(unvalidated_columns) > 0:
        error_messages.append(f"Data in column(s) {unvalidated_columns} is(are) not validated")
    # Stop early if names mismatch — prevents key errors
    if len(error_messages) > 1:
        raise RuntimeError("\n".join(error_messages))

    for col in expected_columns:
        col_name = col["name"]
        col_type = col["type"]
        # validating data types
        if df.schema[col_name] != col_type:
            error_messages.append(
                f"Type of column {col_name} is {df.schema[col_name]} and "
                f"does not match expected type: {col_type}"
            )
        # validating emptiness of a column
        if col["not_null"]:
            if col_type == pl.String:
                df_empty_or_null_column = df.filter(
                    pl.col(col_name).is_null() | (pl.col(col_name) == "")  # noqa: PLC1901
                )
            else:
                df_empty_or_null_column = df.filter(pl.col(col_name).is_null())
            if df_empty_or_null_column.height > 0:
                error_messages.append(
                    f"Column {col_name} has missing values. "
                    "It is not expected have any value missing"
                )

        # validating number_of_characters
        char_num = col.get("number_of_characters")
        if char_num:
            df_with_char_count = df.filter(
                pl.col(col_name).str.len_chars().alias("char_count") != char_num
            )

            if df_with_char_count.height > 0:
                raise RuntimeError(f"Found values exceeding {char_num} characters:\n{col_name}")

        # validating column values to be
        # able to converted to integers
        int_conversion = col.get("can_be_converted_to_integer")
        if int_conversion:
            try:
                df.with_columns(pl.col(col_name).cast(pl.Int64))
            except Exception as e:
                raise RuntimeError(  # noqa: B904
                    f"Column {col_name} cannot be converted to integer: {e}"
                )

    if len(error_messages) > 1:
        message = "\n".join(error_messages)
        current_run.log_error(message)
        raise RuntimeError(message)


def get_max_org_unit_level(df: pl.DataFrame) -> int:
    """Get the maximum organisation unit level present in the DataFrame.

    Args:
        df (pl.DataFrame): The Polars DataFrame containing organisation unit level columns.

    Returns:
        int: The highest organisation unit level found in the DataFrame.
    """
    level_cols = [col for col in df.columns if col.startswith("level_") and col.endswith("_id")]
    levels = [int(col.split("_")[1]) for col in level_cols]
    return max(levels)


def adapt_cols_to_max_level(
    expected_columns: list[ExpectedColumn], max_level: int
) -> list[ExpectedColumn]:
    """Adapt expected columns based on the maximum organisation unit level.

    This function filters the expected columns to include only those relevant
    up to the specified maximum organisation unit level.

    Args:
        expected_columns (list[ExpectedColumn]): The full list of expected columns.
        max_level (int): The maximum organisation unit level to consider.

    Returns:
        list[ExpectedColumn]: The filtered list of expected columns.
    """
    adapted_columns = []
    for col in expected_columns:
        if "level_" in col["name"]:
            level_num = int(col["name"].split("_")[1])
            if level_num > max_level:
                continue
        adapted_columns.append(col)
    return adapted_columns
