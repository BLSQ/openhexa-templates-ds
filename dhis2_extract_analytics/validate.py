import polars as pl

from dhis2_extract_analytics.validation_config import expected_columns


def validate_data(df: pl.DataFrame) -> None:
    """Validates the structure and content of a Polars DataFrame against expected schema rules.

    This function performs a series of validation checks on the provided DataFrame to ensure:
      1. The DataFrame is not empty.
      2. All expected columns are present and validated.
      3. Each column has the correct data type as defined in `expected_columns`.
      4. Columns marked as `not null` do not contain any null or empty string values.

    If any validation check fails, a `RuntimeError` is raised summarizing all detected issues.

    Args:
        df (pl.DataFrame): The Polars DataFrame to validate.

    Raises:
        RuntimeError: If one or more validation rules are violated. The error message includes
            all detected issues such as:
              - Empty DataFrame
              - Unexpected or unvalidated columns
              - Data type mismatches
              - Missing or null values in non-nullable columns

    Notes:
        - The function relies on a global variable `expected_columns`, which should be a list of
          dictionaries. Each dictionary must define:
              {
                  "name": <column_name: str>,
                  "type": <expected_polars_dtype: str>,
                  "not null": <bool>
              }

        - Example structure of `expected_columns`:
              expected_columns = [
                  {"name": "id", "type": "Int64", "not null": True},
                  {"name": "name", "type": "Utf8", "not null": True},
                  {"name": "age", "type": "Int64", "not null": False},
              ]
    """
    # validating none emptiness
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
                f"Type of column {col_name} is {df.schema[col_name]} and "
                f"does not match expected type: {col_type}"
            )
        # validating emptiness of a column
        if col["not null"]:
            df_empty_or_null_cololumn = df.filter(
                (pl.col(col_name).is_null()) | (pl.col(col_name) == "")  # noqa: PLC1901
            )
            if df_empty_or_null_cololumn.height > 0:
                error_messages.append(
                    f"Column {col_name} has missing values."
                    "It is not expected have any value missing"
                )

    if len(error_messages) > 1:
        raise RuntimeError("\n".join(error_messages))
