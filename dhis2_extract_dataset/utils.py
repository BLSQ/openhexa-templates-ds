from datetime import datetime

import polars as pl
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import InvalidParameterError, MissingParameterError

DHIS2_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%3f%z"


def _data_values_to_dataframe(values: list[dict]) -> pl.DataFrame:
    schema = {
        "dataElement": str,
        "period": str,
        "orgUnit": str,
        "categoryOptionCombo": str,
        "attributeOptionCombo": str,
        "value": str,
        "comment": str,
        "created": str,
        "lastUpdated": str,
    }

    df = pl.DataFrame(data=values, schema=schema)
    return df.select(
        pl.col("dataElement").alias("data_element_id"),
        pl.col("period"),
        pl.col("orgUnit").alias("organisation_unit_id"),
        pl.col("categoryOptionCombo").alias("category_option_combo_id"),
        pl.col("attributeOptionCombo").alias("attribute_option_combo_id"),
        pl.col("value"),
        pl.col("comment"),
        pl.col("created").str.to_datetime(DHIS2_DATE_FORMAT).alias("created"),
        pl.col("lastUpdated").str.to_datetime(DHIS2_DATE_FORMAT).alias("last_updated"),
    )


def extract_dataset(
    dhis2: DHIS2,
    dataset: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    periods: list[str] | None = None,
    org_units: list[str] | None = None,
    org_unit_groups: list[str] | None = None,
    include_children: bool = False,
    last_updated: datetime | None = None,
) -> pl.DataFrame:
    """Extract dataset data values, including the comment field.

    Same as openhexa.toolbox.dhis2.dataframe.extract_dataset but also returns the
    comment column (null when no comment is set on a data value).

    Returns
    -------
    pl.DataFrame
        A dataframe with the following columns:
        - data_element_id: str
        - period: str
        - organisation_unit_id: str
        - category_option_combo_id: str
        - attribute_option_combo_id: str
        - value: str
        - comment: str (null when no comment is set on a data value)
        - created: datetime
        - last_updated: datetime
    """
    if org_units is None and org_unit_groups is None:
        msg = "org_units or org_unit_groups must be provided"
        raise MissingParameterError(msg)

    if org_units is not None and org_unit_groups is not None:
        msg = "org_units and org_unit_groups cannot be provided at the same time"
        raise InvalidParameterError(msg)

    if not (start_date and end_date) and not periods:
        msg = "Either start_date and end_date or periods must be provided"
        raise MissingParameterError(msg)

    if (start_date or end_date) and periods:
        msg = "Either start_date and end_date or periods must be provided, not both"
        raise InvalidParameterError(msg)

    if start_date:
        start_date = start_date.strftime("%Y-%m-%d")
    if end_date:
        end_date = end_date.strftime("%Y-%m-%d")

    values = dhis2.data_value_sets.get(
        datasets=[dataset],
        periods=periods if periods else None,
        start_date=start_date if start_date else None,
        end_date=end_date if end_date else None,
        org_units=org_units if org_units else None,
        org_unit_groups=org_unit_groups if org_unit_groups else None,
        children=include_children,
        last_updated=last_updated.isoformat() if last_updated else None,
    )

    return _data_values_to_dataframe(values)
