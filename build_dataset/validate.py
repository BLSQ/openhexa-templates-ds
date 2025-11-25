import pandas as pd
from openhexa.sdk import current_run


def validate_data(df: pd.DataFrame) -> None:
    """Validate that the provided DataFrame has data.

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
        current_run.log_error("The output dataset is empty")
        raise RuntimeError("The output dataset is empty")