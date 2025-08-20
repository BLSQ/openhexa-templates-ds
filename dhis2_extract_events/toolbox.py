"""Functions that should probably be part of the OpenHexa toolbox."""

import logging
import re
from typing import Any, Optional, Union

import polars as pl
from openhexa.toolbox.dhis2 import DHIS2

logger = logging.getLogger(__name__)


# Modifying this function from the OH toolbox.
def join_object_names(
    df: pl.DataFrame,
    data_elements: pl.DataFrame | None = None,
    indicators: pl.DataFrame | None = None,
    organisation_units: pl.DataFrame | None = None,
    category_option_combos: pl.DataFrame | None = None,
    program_stages: pl.DataFrame | None = None,
    programs: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Add object names to the dataframe.

    Returns
    -------
    pl.DataFrame
        All of the data with the relevant names added.
    """
    if (
        (data_elements is None)
        and (organisation_units is None)
        and (category_option_combos is None)
        and (indicators is None)
        and (program_stages is None)
        and (programs is None)
    ):
        msg = "No metadata to be joined provided"
        logger.error(msg)
        raise ValueError(msg)

    if data_elements is not None and "data_element_name" not in df.columns:
        df = df.join(
            other=data_elements.select("id", pl.col("name").alias("data_element_name")),
            left_on="data_element_id",
            right_on="id",
            how="left",
        )

    if indicators is not None and "indicator_name" not in df.columns:
        df = df.join(
            other=indicators.select("id", pl.col("name").alias("indicator_name")),
            left_on="indicator_id",
            right_on="id",
            how="left",
        )

    if organisation_units is not None and "organisation_unit_name" not in df.columns:
        ou_ids = [
            col
            for col in organisation_units.columns
            if col.startswith("level_") and col.endswith("_id")
        ]
        ou_names = [
            col
            for col in organisation_units.columns
            if col.startswith("level_") and col.endswith("_name")
        ]
        df = df.join(
            other=organisation_units.select("id", *ou_ids, *ou_names),
            left_on="organisation_unit_id",
            right_on="id",
            how="left",
        )

    if category_option_combos is not None and "category_option_combo_name" not in df.columns:
        df = df.join(
            other=category_option_combos.select(
                "id", pl.col("name").alias("category_option_combo_name")
            ),
            left_on="category_option_combo_id",
            right_on="id",
            how="left",
        )

    if program_stages is not None and "program_stage_name" not in df.columns:
        df = df.join(
            other=program_stages.select(["program_stage_id", "program_stage_name"]),
            on="program_stage_id",
            how="left",
        )

    if programs is not None and "program_name" not in df.columns:
        df = df.join(
            other=programs.select("id", pl.col("name").alias("program_name")),
            left_on="program_id",
            right_on="id",
            how="left",
        )

    columns = [
        "dataset_name",
        "dataset_id",
        "dataset_period_type",
        "data_element_id",
        "data_element_name",
        "indicator_id",
        "indicator_name",
        "program_id",
        "program_name",
        "program_stage_id",
        "program_stage_name",
        "organisation_unit_id",
        "organisation_unit_name",
        "category_option_combo_id",
        "category_option_combo_name",
        "attribute_option_combo_id",
        "period",
        "value",
        *[col for col in df.columns if col.startswith("level_")],
        "created",
        "last_updated",
    ]

    # if additional columns were present in the original dataframe, keep them at the end
    columns += [col for col in df.columns if col not in columns]

    return df.select([col for col in columns if col in df.columns])


def programs(
    dhis2: DHIS2,
    fields: str = "id,name,programType",
    page: Optional[int] = None,
    pageSize: Optional[int] = None,
    filters: Optional[list[str]] = None,
) -> Union[list[dict[str, Any]], dict[str, Any]]:
    """Get program metadata from DHIS2.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    fields: str, optional
        Comma-separated DHIS2 fields to include in the response.
    page: int, optional
        Page number for paginated requests.
    pageSize: int, optional
        Number of results per page.
    filters: list of str, optional
        DHIS2 query filters.

    Returns
    -------
    Union[list[dict[str, Any]], dict[str, Any]]
        - If `page` and `pageSize` are **not** provided: Returns a **list** of programs.
        - If `page` and `pageSize` **are** provided: Returns a **dict** with `programs` and `pager`
        for pagination.
    """

    def format_program(program: dict[str, Any], fields: str) -> dict[str, Any]:
        splitted_fields = [f.split("[")[0] for f in re.split(r",(?![^\[]*\])", fields)]
        return {key: program.get(key) for key in splitted_fields}

    params = {"fields": fields}

    if filters:
        params["filter"] = filters

    if page and pageSize:
        params["page"] = page
        params["pageSize"] = pageSize
        response = dhis2.api.get("programs", params=params)

        program_stages = [format_program(ou, fields) for ou in response.get("programs", [])]

        return {"items": program_stages, "pager": response.get("pager", {})}

    return [
        format_program(program, fields)
        for page in dhis2.api.get_paged("programs", params=params)
        for program in page.get("programs", [])
    ]


def get_programs(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract programs metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing program stages metadata with the following columns: id, name, program_type.
    """
    meta = programs(dhis2, fields="id,name,programType", filters=filters)
    schema = {"id": str, "name": str, "programType": str}
    df = pl.DataFrame(meta, schema=schema)
    return df.select("id", "name", pl.col("programType").alias("program_type"))


def get_program_stages(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract programStages metadata.
    We extract all of the programStages, including the ones that are not accessible from the programStages endpoint.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing program stages metadata with the following columns: program_stage_id, program_stage_name, program_id, program_name.
    """
    meta = programs(dhis2, fields="id,name,programStages[id,name]", filters=filters)
    schema = {"id": str, "name": str, "programStages": list}
    df = pl.DataFrame(meta, schema=schema)
    df_flat = (
        df.explode("programStages")
        .with_columns(
            [
                pl.col("programStages").struct.field("name").alias("program_stage_name"),
                pl.col("programStages").struct.field("id").alias("program_stage_id"),
            ]
        )
        .select(
            [
                pl.col("program_stage_id"),
                pl.col("program_stage_name"),
                pl.col("id").alias("program_id"),
                pl.col("name").alias("program_name"),
            ]
        )
    )
    return df_flat


# Modifying this function from the OH toolbox.
def extract_events(
    dhis2: DHIS2,
    program_id: str,
    org_units: list[str],
    occurred_after: str | None = None,
    occurred_before: str | None = None,
    include_children: bool = True,
) -> pl.DataFrame:
    """Extract events data.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    program_id : str
        Program UID.
    org_units : list[str]
        Organisation units UIDs.
    occurred_after : str, optional
        Start date in the format "YYYYMMDD".
    occurred_before : str, optional
        End date in the format "YYYYMMDD".
    include_children: bool, default=True
        Whether to include child organisation units.

    Returns
    -------
    pl.DataFrame
        Dataframe containing events data with the following columns:
        event_id, status, program_id, program_stage_id, enrollment_id, tracked_entity_id,
        organisation_unit_id, occurred_at, deleted,
        attribute_option_combo_id, data_element_id, value.
    """
    data = []
    for org_unit in org_units:
        params = {
            "orgUnit": org_unit,
            "program": program_id,
            "fields": "event,status,program,programStage,trackedEntity,enrollment,orgUnit,occurredAt,deleted,attributeOptionCombo,dataValues",
            "totalPages": True,
        }
        if include_children:
            params["ouMode"] = "DESCENDANTS"
        else:
            params["ouMode"] = "SELECTED"
        if occurred_after:
            params["occurredAfter"] = occurred_after
        if occurred_before:
            params["occurredBefore"] = occurred_before
        for page in dhis2.api.get_paged("tracker/events", params=params):
            data.extend(page["instances"])

    schema = {
        "event": str,
        "status": str,
        "program": str,
        "programStage": str,
        "enrollment": str,
        "trackedEntity": str,
        "orgUnit": str,
        "occurredAt": str,
        "deleted": bool,
        "attributeOptionCombo": str,
        "dataValues": pl.List(pl.Struct({"dataElement": str, "value": str})),
    }

    df = pl.DataFrame(data, schema=schema)

    df = (
        df.explode("dataValues")
        .with_columns(
            [
                pl.col("dataValues").struct.field("dataElement").alias("data_element_id"),
                pl.col("dataValues").struct.field("value"),
            ]
        )
        .select(
            [
                pl.col("event").alias("event_id"),
                pl.col("status"),
                pl.col("program").alias("program_id"),
                pl.col("programStage").alias("program_stage_id"),
                pl.col("enrollment").alias("enrollment_id"),
                pl.col("trackedEntity").alias("tracked_entity_id"),
                pl.col("orgUnit").alias("organisation_unit_id"),
                pl.col("occurredAt").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3f").alias("occurred_at"),
                pl.col("deleted"),
                pl.col("attributeOptionCombo").alias("attribute_option_combo_id"),
                pl.col("data_element_id"),
                pl.col("value"),
            ]
        )
    ).unique()

    return df
