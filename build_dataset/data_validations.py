import pandas as pd


def validate_data(df: pd.DataFrame) -> None:
    """Validate that the provided DataFrame and its data.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to validate.

    Raises
    ------
    RuntimeError
        If the DataFrame has zero rows.
    """
    # validate for none emptiness
    if df.shape[0] == 0:
        raise RuntimeError("The output dataset is empty")