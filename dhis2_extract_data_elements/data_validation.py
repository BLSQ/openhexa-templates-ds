import polars as pl

from dhis2_extract_data_elements.validation_config import expected_columns


def validate_data(df: pl.DataFrame) -> None:
    """Validate a Polars DataFrame against a predefined schema specification.

    This function enforces a series of validation checks to ensure the input 
    DataFrame adheres to expected data quality standards. Specifically, it:

    1. Ensures the DataFrame is not empty.
    2. Validates that only the expected columns are present.
    3. Confirms each column matches the expected data type.
    4. Checks that columns defined as `not null` do not contain null or empty values.
    5. Ensures string columns do not exceed the specified maximum character length.
    6. Verifies that columns marked as convertible to integers can be safely cast to integers.

    Any violations are collected and reported together in a raised `RuntimeError`
    with detailed messages describing each issue.

    Parameters
    ----------
    df : pl.DataFrame
        The Polars DataFrame to validate.

    Raises
    ------
    RuntimeError
        If any of the following conditions are met:
          - The DataFrame is empty.
          - Unexpected columns are found.
          - Column data types differ from expectations.
          - Required (non-null) columns contain missing or empty values.
          - Column values exceed defined character limits.
          - Columns expected to be integer-convertible cannot be cast.
    """
    error_messages = ["\n"]
    if df.height == 0:
        error_messages.append("data_values is empty")

    # checking for unvalidated columns
    expected_column_names = [col["name"] for col in expected_columns]
    unvalidated_columns = [
        col for col in df.columns if col not in expected_column_names
    ]
    if len(unvalidated_columns) > 0:
        error_messages.append(
            f"Data in column(s) {unvalidated_columns} is(are) not validated"
        )
    for col in expected_columns:
        col_name = col["name"]
        col_type = col["type"]
        # validating data types
        if str(df.schema[col_name]) != col_type:
            error_messages.append(
                f"Type of column {col_name} is {df.schema[col_name]} and"
                f"does not match expected type: {col_type}"
            )
        # validating emptiness of a column
        if col["not null"]:
            df_empty_or_null_cololumn = df.filter(
                (pl.col(col_name).is_null()) | (pl.col(col_name) == "")  # noqa: PLC1901
            )
            if df_empty_or_null_cololumn.height > 0:
                error_messages.append(f"Column {col_name} has missing values."
                                      "It is not expected have any value missing")

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
        raise RuntimeError("\n".join(error_messages))
