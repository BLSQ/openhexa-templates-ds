from dataclasses import dataclass

import polars as pl
from polars._typing import PolarsDataType


@dataclass
class ExpectedColumn:
    """Specification for a single expected DataFrame column.

    Attributes:
        name: Column name in the dataframe.
        required: Whether the column is required to be present in the dataframe.
        type: Expected Polars data type of the column.
        n_chars: Expected number of characters for string columns.
        not_null: Whether the column must not contain null values.
    """

    name: str
    type: PolarsDataType
    n_chars: int | None = None
    required: bool = True
    not_null: bool = False


@dataclass
class ErrorMessage:
    """Error message from dataframe validation."""

    column_name: str
    message: str


class DataValidationError(Exception):
    """Exception raised for errors in the DataFrame validation.

    Attributes:
        errors (list[ErrorMessage]):
            List of error messages detailing the validation issues.
    """

    def __init__(self, errors: list[ErrorMessage]) -> None:
        self.errors = errors
        super().__init__(self._format_errors())

    def _format_errors(self) -> str:
        return "\n".join(f"{error.column_name}: {error.message}" for error in self.errors)


def validate_data_type(serie: pl.Series, expected_type: PolarsDataType) -> ErrorMessage | None:
    """Validate the data type of a Polars Series.

    Args:
        serie: The Polars Series to validate.
        expected_type: The expected Polars data type.

    Returns:
        An ErrorMessage if the data type does not match, otherwise None.
    """
    if serie.dtype != expected_type:
        message = (
            f"Type of column '{serie.name}' is '{serie.dtype}' "
            f"and does not match expected type ({expected_type})."
        )
        return ErrorMessage(column_name=serie.name, message=message)
    return None


def validate_not_null(serie: pl.Series) -> ErrorMessage | None:
    """Validate that a Polars Series does not contain null values.

    Args:
        serie: The Polars Series to validate.

    Returns:
        An ErrorMessage if null values are found, otherwise None.
    """
    null_count = serie.null_count()
    if null_count > 0:
        message = (
            f"Column '{serie.name}' has {null_count} null values. It is not expected to have any."
        )
        return ErrorMessage(column_name=serie.name, message=message)
    return None


def validate_nchars(serie: pl.Series, expected_nchars: int) -> ErrorMessage | None:
    """Validate the number of characters in a Polars Series.

    Args:
        serie: The Polars Series to validate.
        expected_nchars: The expected number of characters.

    Returns:
        An ErrorMessage if any value does not match the expected number of characters,
        other wise None.
    """
    if (serie.str.len_chars() != expected_nchars).any():
        message = (
            f"Column '{serie.name}' has values not matching "
            f"the expected number of characters ({expected_nchars})."
        )
        return ErrorMessage(column_name=serie.name, message=message)
    return None


def validate_dataframe(df: pl.DataFrame, expected_columns: list[ExpectedColumn]) -> None:
    """Validate a Polars DataFrame against expected columns.

    Args:
        df: The Polars DataFrame to validate.
        expected_columns: List of ExpectedColumn defining the expected schema.

    Raises:
        DataValidationError: If any validation errors are found.
    """
    errors: list[ErrorMessage] = []

    for column in expected_columns:
        if column.name not in df.columns:
            if column.required:
                message = f"Expected column '{column.name}' is missing."
                errors.append(ErrorMessage(column_name=column.name, message=message))
            continue

        serie = df[column.name]

        type_error = validate_data_type(serie, column.type)
        if type_error:
            errors.append(type_error)

        if column.not_null:
            not_null_error = validate_data_type(serie, column.type)
            if not_null_error:
                errors.append(not_null_error)

        if column.n_chars is not None:
            nchars_error = validate_nchars(serie, column.n_chars)
            if nchars_error:
                errors.append(nchars_error)

    if errors is not None:
        raise DataValidationError(errors)
