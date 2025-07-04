from pathlib import Path
import json
import pandas as pd
import polars as pl

# from datetime import datetime
# from dateutil.relativedelta import relativedelta
# from typing import List
from openhexa.sdk import DHIS2Connection, current_run
from openhexa.toolbox.dhis2 import DHIS2


def connect_to_dhis2(connection: DHIS2Connection, cache_dir: str) -> DHIS2:
    """Establish a connection to DHIS2 using the provided connection and cache directory.

    Parameters
    ----------
    connection : DHIS2Connection
        The DHIS2 connection object containing connection details.
    cache_dir : str
        The directory path to use for caching.

    Returns
    -------
    DHIS2
        An instance of the DHIS2 client.

    Raises
    ------
    Exception
        If there is an error while connecting to DHIS2.
    """
    try:
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        current_run.log_info(f"Connected to DHIS2 : {connection.url}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2 {connection.url}: {e}") from e


def retrieve_pyramid_to_level(
    dhis2_client: DHIS2Connection,
    org_level: int,
    fields: str = "id,name,shortName,openingDate,closedDate,parent,level,path,geometry",
) -> pl.DataFrame:
    """Retrieve all DHIS2 organisation units up to the specified organisation unit level.

    Parameters
    ----------
    dhis2_client : DHIS2Connection
        The DHIS2 connection object to retrieve organisation units from.
    org_level : int
        The maximum organisation unit level to include.
    fields : str
        The fields to be retrieved from org units

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the organisation units up to the specified level.
    """
    org_units = dhis2_client.meta.organisation_units(fields=fields)
    org_units = pl.DataFrame(org_units)
    org_units = org_units.filter(pl.col("level") <= org_level)

    level_5_units = org_units.filter(pl.col("level") == org_level).select("id").unique()
    current_run.log_info(
        f"Extracted {level_5_units.height} units at organisation unit level {org_level}"
    )
    return org_units


def read_parquet_as_polars(parquet_file: Path) -> pl.DataFrame:
    """Read a Parquet file and return its contents as a Polars DataFrame.

    Parameters
    ----------
    parquet_file : Path
        The path to the Parquet file to be read.

    Returns
    -------
    pl.DataFrame
        The contents of the Parquet file as a Polars DataFrame.

    Raises
    ------
    FileNotFoundError
        If the specified Parquet file does not exist.
    Exception
        If there is an error while loading the Parquet file.
    """
    try:
        parquet_df = pl.read_parquet(parquet_file)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Error while loading the extract: File was not found {parquet_file}."
        ) from e
    except Exception as e:
        raise Exception(f"Error while loading the extract: {parquet_file}. Error: {e}") from e

    return parquet_df


def split_list(src_list: list, length: int):
    """Split list into chunks."""
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]


def build_id_indexes(ou_source, ou_target, ou_matching_ids):
    # Set "id" as the index for faster lookup
    df1_lookup = {val: idx for idx, val in enumerate(ou_source["id"])}
    df2_lookup = {val: idx for idx, val in enumerate(ou_target["id"])}

    # Build the dictionary using prebuilt lookups
    index_dict = {
        match_id: {"source": df1_lookup[match_id], "target": df2_lookup[match_id]}
        for match_id in ou_matching_ids
        if match_id in df1_lookup and match_id in df2_lookup
    }
    return index_dict


class OrgUnitObj:
    """Helper class definition to store/create the correct OrgUnit JSON format"""

    def __init__(self, org_unit_row: pd.Series):
        """Create a new org unit instance.

        Parameters
        ----------
        orgUnit_row : pandas series
            Expects columns with names :
                ['id', 'name', 'shortName', 'openingDate', 'closedDate', 'parent','level', 'path', 'geometry']
        """
        self.initialize_from(org_unit_row.squeeze(axis=0))

    def initialize_from(self, row: pd.Series):
        # let's keep names consistent
        self.id = row.get("id")
        self.name = row.get("name")
        self.shortName = row.get("shortName")
        self.openingDate = row.get("openingDate")
        self.closedDate = row.get("closedDate")
        self.parent = row.get("parent")
        geometry = row.get("geometry")
        self.geometry = json.loads(geometry) if isinstance(geometry, str) else geometry

    def to_json(self) -> dict:
        json_dict = {
            "id": self.id,
            "name": self.name,
            "shortName": self.shortName,
            "openingDate": self.openingDate,
            "closedDate": self.closedDate,
            "parent": {"id": self.parent.get("id")} if self.parent else None,
        }
        if self.geometry:
            geometry = (
                json.loads(self.geometry) if isinstance(self.geometry, str) else self.geometry
            )
            json_dict["geometry"] = {
                "type": geometry["type"],
                "coordinates": geometry["coordinates"],
            }
        return {k: v for k, v in json_dict.items() if v is not None}

    def is_valid(self):
        if self.id is None:
            return False
        if self.name is None:
            return False
        if self.shortName is None:
            return False
        if self.openingDate is None:
            return False
        if self.parent is None:
            return False

        return True

    def __str__(self):
        return f"OrgUnitObj({self.id}, {self.name})"

    def copy(self):
        attributes = self.to_json()
        new_instance = OrgUnitObj(pd.Series(attributes))
        return new_instance
