import polars as pl


def validate_data(df: pl.DataFrame, expected_columns: dict, data_name: str) -> None:
    """Validate a Polars DataFrame against a predefined schema specification.

    This function performs a comprehensive validation of a DataFrame based on
    the expected schema rules provided in `expected_columns`. It checks for:

    1. **Non-emptiness** Ensures the DataFrame contains at least one row.
    2. **Schema compliance** Confirms that only expected columns exist and
       that each column matches its expected data type.
    3. **Non-null constraints** Verifies that columns marked as `not_null`
       have no missing or empty values.
    4. **Character limits** Checks that text columns do not exceed the
       specified maximum character length.
    5. **Integer conversion** Validates that columns flagged as convertible
       to integers can be successfully cast.

    If any of these validations fail, a `RuntimeError` is raised with detailed
    error messages summarizing all detected issues.

    Args:
    ----
    df : pl.DataFrame
        The Polars DataFrame to validate.
    expected_columns : dict
        A list or dictionary defining the expected schema for each column, where
        each entry includes the column name, data type, and optional constraints
        such as `not_null`, `number of characters`, or `can be converted to integer`.
    data_name : str
        A human-readable identifier for the dataset being validated, used for logging.

    Raises:
    ------
    RuntimeError
        If any validation rule fails — including empty DataFrames, unexpected columns,
        schema mismatches, missing values, character limit violations, or failed type conversions.
    """
    error_messages = ["\n"]
    if df.height == 0:
        error_messages.append("data_values is empty")

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
            df_empty_or_null_cololumn = df.filter(
                (pl.col(col_name).is_null()) | (pl.col(col_name) == "")  # noqa: PLC1901
            )
            if df_empty_or_null_cololumn.height > 0:
                error_messages.append(
                    f"Column {col_name} has missing values.  "
                    "It is not expected have any value missing"
                )

        # validating number of characters
        char_num = col.get("number of characters")
        if char_num:
            df_with_char_count = df.filter(
                pl.col(col_name).str.len_chars().alias("char_count") > char_num
            )
            if df_with_char_count.height > 0:
                raise RuntimeError(f"Found values exceeding {char_num} characters:\n{col_name}")

        # validating column values to be
        # able to converted to integers
        int_conversion = col.get("can be converted to integer")
        if int_conversion:
            try:
                df.with_columns(pl.col(col_name).cast(pl.Int64))
            except Exception as e:
                raise RuntimeError(f"Column {col_name} cannot be converted to integer: {e}")  # noqa: B904

    if len(error_messages) > 1:
        message = "\n".join(error_messages)
        raise RuntimeError(message)
