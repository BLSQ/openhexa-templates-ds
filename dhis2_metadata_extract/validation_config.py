from typing import TypedDict

import polars as pl
from polars._typing import PolarsDataType


class OrgUnitsExpectedColumns(TypedDict):
    """Schema specification for expected Organisation Unit columns.

    This typed dictionary defines the required schema for organisation unit
    hierarchy data. It is used in validation routines to ensure that each
    column exists, has the correct data type, and meets nullability constraints.

    Attributes:
        name (str):  
            The expected column name.

        type (PolarsDataType):  
            The required Polars data type for the column.

        not_null (bool):  
            Whether the column is required to contain no null or empty values.
    """

    name: str
    type: PolarsDataType
    not_null: bool


org_units_expected_columns: list[OrgUnitsExpectedColumns] = [
        {
            "name": "level_1_id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_1_name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_2_id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_2_name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_3_id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_3_name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_4_id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_4_name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_5_id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_5_name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_6_id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_6_name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_7_id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "level_7_name",
            "type": pl.String,
            "not_null": False,
        }
    ]


class OrgUnitsGroupsExpectedColumns(TypedDict):
    """Schema specification for expected Organisation Unit Group columns.

    This typed dictionary represents the schema definition for organisation
    unit group metadata. It ensures that all required columns exist and have
    valid types and nullability rules.

    Attributes:
        name (str):  
            The expected name of the column.

        type (PolarsDataType):  
            The expected Polars data type for the column.

        not_null (bool):  
            Whether the column must not contain null or empty-string values.
    """

    name: str
    type: PolarsDataType
    not_null: bool


org_unit_groups_expected_columns: list[OrgUnitsGroupsExpectedColumns] = [
        {
            "name": "id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "organisation_units",
            "type": pl.String,
            "not_null": False,
        }
]


class RetrievedDatasetsExpectedColumns(TypedDict):
    """Schema specification for expected Dataset metadata columns.

    This typed dictionary defines the schema for datasets retrieved from an
    external system. It ensures that essential dataset attributes—such as IDs,
    names, and metadata—meet type and nullability requirements.

    Attributes:
        name (str):  
            The expected column name.

        type (PolarsDataType):  
            The required Polars data type for the column.

        not_null (bool):  
            Whether the column is required to contain no null or empty values.
    """

    name: str
    type: PolarsDataType
    not_null: bool


retrieved_datasets_expected_columns: list[RetrievedDatasetsExpectedColumns] = [
        {
            "name": "id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "organisation_units",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "data_elements",
            "type": pl.String,
            "not_null": True,
        },
        {
            "name": "indicators",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "period_type",
            "type": pl.String,
            "not_null": False,
        }  
]


class RetrievedDataElementsExpectedColumns(TypedDict):
    """Schema specification for expected Data Element columns.

    This schema is used to validate retrieved data elements, ensuring that
    the identifier, name, and value type fields conform to expected types and
    nullability constraints.

    Attributes:
        name (str):  
            The expected column name.

        type (PolarsDataType):  
            The required Polars data type for the column.

        not_null (bool):  
            Whether the column must not contain null or empty values.
    """

    name: str
    type: PolarsDataType
    not_null: bool


retrieved_data_elements_expected_columns: list[RetrievedDataElementsExpectedColumns] = [
        {
            "name": "id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "value_type",
            "type": pl.String,
            "not_null": False,
        }
]


class RetrievedDataElementsGroupsExpectedColumns(TypedDict):
    """Schema specification for expected Data Element Group columns.

    This schema defines the expected structure for data element groups,
    including group identifiers, names, and the list of assigned data elements.

    Attributes:
        name (str):  
            The expected column name.

        type (PolarsDataType):  
            The expected Polars data type for the column.

        not_null (bool):  
            Whether the column must not contain null or empty-string values.
    """

    name: str
    type: PolarsDataType
    not_null: bool


retrieved_data_element_groups_expected_columns: list[RetrievedDataElementsGroupsExpectedColumns] = [
        {
            "name": "id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "name",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "data_elements",
            "type": pl.String,
            "not_null": False,
        }
]


class RetrievedCategoryOptionsExpectedColumns(TypedDict):
    """Schema specification for expected Category Option columns.

    This schema defines the required columns for category options retrieved
    from an external source. It validates the presence and type of each field.

    Attributes:
        name (str):  
            The expected column name.

        type (PolarsDataType):  
            The required Polars data type for the column.

        not_null (bool):  
            Whether the column must not contain null or empty values.
    """

    name: str
    type: PolarsDataType
    not_null: bool


retrieved_categorty_options_expected_columns: list[RetrievedCategoryOptionsExpectedColumns] = [
        {
            "name": "id",
            "type": pl.String,
            "not_null": False,
        },
        {
            "name": "name",
            "type": pl.String,
            "not_null": False,
        }
]