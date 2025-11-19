from typing import TypedDict

import polars as pl
from polars._typing import PolarsDataType


class ExpectedColumn(TypedDict):
    """Schema specification for a single expected DataFrame column.

    Attributes:
        name (str): The expected name of the column.
        type (pl.datatypes.DataType): The expected Polars data type for the column.
        not_null (bool): Whether the column must not contain null values.
    """

    name: str
    type: PolarsDataType
    not_null: bool


expected_columns: list[ExpectedColumn] = [
    {
        "name": "indicator_id",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "indicator_name",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "organisation_unit_id",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "period",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "value",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_1_id",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_2_id",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_3_id",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_4_id",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_1_name",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_2_name",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_3_name",
        "type": pl.String,
        "not_null": False,
    },
    {
        "name": "level_4_name",
        "type": pl.String,
        "not_null": False,
    },
]


def validate_data(df: pl.DataFrame) -> None:
    """Validate a Polars DataFrame against a predefined schema.

    This function checks the input DataFrame against the global
    `expected_columns` schema definition. It validates structural and
    content-level requirements to ensure data integrity before downstream
    processing.

    The validation performs the following checks:

    1. **Non-empty DataFrame** - Ensures the DataFrame contains at least one row.
    2. **Presence of expected columns** - All columns defined in
       `expected_columns` must exist in the DataFrame.
    3. **Correct data types** - Each column must match its expected Polars
       dtype.
    4. **Non-null constraints** - Columns marked as `not_null=True` must not
       contain null values or empty strings.

    If any validation rule fails, all detected issues are aggregated and
    raised as a `RuntimeError`.

    Args:
        df (pl.DataFrame): The Polars DataFrame to validate.

    Raises:
        RuntimeError: Raised when one or more validation checks fail. The
            error message includes details about:
                * empty DataFrame errors
                * missing or unexpected columns
                * incorrect data types
                * null or empty string values in non-nullable columns

    Notes:
        The function relies on the global `expected_columns` variable, which is a
        list of dictionaries defining the schema for validation. Each schema entry
        must follow the structure:

            {
                "name": str,                  # column name
                "type": PolarsDataType,       # expected Polars dtype
                "not_null": bool              # null constraint
            }

    Example:
        expected_columns = [
            {"name": "id", "type": pl.Int64, "not_null": True},
            {"name": "name", "type": pl.String, "not_null": True},
            {"name": "age", "type": pl.Int64, "not_null": False},
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
            df_empty_or_null_cololumn = df.filter(
                (pl.col(col_name).is_null()) | (pl.col(col_name) == "")  # noqa: PLC1901
            )
            if df_empty_or_null_cololumn.height > 0:
                error_messages.append(
                    f"Column {col_name} has missing values. "
                    "It is not expected have any value missing"
                )

    if len(error_messages) > 1:
        raise RuntimeError("\n".join(error_messages))
