"""Unit tests for IASO extract organizational units pipeline."""

import json
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import (
    authenticate_iaso,
    clean_string,
    convert_to_geometry,
    export_to_database,
    export_to_dataset,
    export_to_file,
    fetch_org_units,
    get_organisation_units,
)
from shapely.geometry import Point


@patch("pipeline.IASO")
@patch("pipeline.current_run")
def test_authenticate_iaso_success(
    mock_run: MagicMock,
    mock_iaso: MagicMock,
) -> None:
    """Test successful authentication to IASO API."""
    connection: MagicMock = MagicMock()
    connection.url = "url"
    connection.username = "user"
    connection.password = "pass"

    mock_iaso.return_value = "client"

    result = authenticate_iaso(connection)

    assert result == "client"
    mock_run.log_info.assert_called_once()


@patch("pipeline.IASO", side_effect=Exception("fail"))
@patch("pipeline.current_run")
def test_authenticate_iaso_failure(
    mock_run: MagicMock,
    mock_iaso: MagicMock,
) -> None:
    """Test authentication failure raises RuntimeError."""
    connection: MagicMock = MagicMock()

    with pytest.raises(RuntimeError):
        authenticate_iaso(connection)

    mock_run.log_error.assert_called_once()


@patch("pipeline.get_organisation_units")
def test_fetch_org_units_with_type_id(mock_get: MagicMock) -> None:
    """Test fetching org units when type ID is provided."""
    mock_client: MagicMock = MagicMock()
    mock_response: MagicMock = MagicMock()
    mock_response.json.return_value = {"orgUnitTypes": [{"id": 1}]}
    mock_client.api_client.get.return_value = mock_response

    mock_get.return_value = pl.DataFrame({"id": [1]})

    result = fetch_org_units(mock_client, 1)

    assert isinstance(result, pl.DataFrame)
    mock_get.assert_called_once()


@patch("pipeline.get_organisation_units")
def test_fetch_org_units_without_type_id(mock_get: MagicMock) -> None:
    """Test fetching org units when no type ID is provided."""
    mock_client: MagicMock = MagicMock()
    mock_get.return_value = pl.DataFrame({"id": [1]})

    result = fetch_org_units(mock_client, None)

    assert isinstance(result, pl.DataFrame)
    mock_get.assert_called_once()


@patch("pipeline.dataframe._get_org_units_geometries")
def test_get_organisation_units_with_type_id(
    mock_geoms: MagicMock,
) -> None:
    """Test retrieval of organization units with type ID."""
    csv_data: str = """ID,Nom,Type,Latitude,Longitude,Date d'ouverture,Date de fermeture,Date de création,Date de modification,Source,Validé,Référence externe
1,Test,TypeA,0,0,2020-01-01,2021-01-01,2020-01-01 10:00,2020-01-02 10:00,SRC,Yes,Ref1
"""  # noqa: E501
    mock_response: MagicMock = MagicMock()
    mock_response.content = csv_data.encode()
    mock_response.raise_for_status.return_value = None

    mock_client: MagicMock = MagicMock()
    mock_client.api_client.get.return_value = mock_response

    mock_geoms.return_value = {1: json.dumps({"type": "Point", "coordinates": [0, 0]})}

    df: pl.DataFrame = get_organisation_units(mock_client, 1)
    mock_client.api_client.get.assert_called_once_with(
        url="api/orgunits",
        params={"csv": True, "orgUnitTypeId": 1},
        stream=True,
    )

    assert isinstance(df, pl.DataFrame)
    assert "geometry" in df.columns
    assert df.columns == [
        "id",
        "name",
        "org_unit_type",
        "latitude",
        "longitude",
        "opening_date",
        "closing_date",
        "created_at",
        "updated_at",
        "source",
        "validation_status",
        "source_ref",
        "geometry",
    ]
    assert df["id"].to_list() == [1]
    assert df["created_at"].to_list()[0] == datetime(2020, 1, 1, 10, 0)
    assert df["geometry"].to_list() == ['{"type": "Point", "coordinates": [0, 0]}']


@patch("pipeline.dataframe._get_org_units_geometries")
def test_get_organisation_units_without_type_id(
    mock_geoms: MagicMock,
) -> None:
    """Test retrieval of organization units without type ID."""
    csv_data: str = """ID,Nom,Type,Latitude,Longitude,Date d'ouverture,Date de fermeture,Date de création,Date de modification,Source,Validé,Référence externe
1,Test,TypeA,0,0,2020-01-01,2021-01-01,2020-01-01 10:00,2020-01-02 10:00,SRC,Yes,Ref1
"""  # noqa: E501
    mock_response: MagicMock = MagicMock()
    mock_response.content = csv_data.encode()
    mock_response.raise_for_status.return_value = None

    mock_client: MagicMock = MagicMock()
    mock_client.api_client.get.return_value = mock_response

    mock_geoms.return_value = {}

    df: pl.DataFrame = get_organisation_units(mock_client)
    mock_client.api_client.get.assert_called_once_with(
        "/api/orgunits",
        params={"csv": True},
        stream=True,
    )

    assert isinstance(df, pl.DataFrame)


@patch("pipeline.current_run")
@patch("pipeline._generate_output_file_path")
def test_export_to_file(
    mock_path: MagicMock,
    mock_run: MagicMock,
    tmp_path: Path,
) -> None:
    """Test exporting org units to file."""
    df: pl.DataFrame = pl.DataFrame(
        {
            "id": [1],
            "geometry": [json.dumps({"type": "Point", "coordinates": [0, 0]})],
        }
    )

    output: Path = tmp_path / "file.csv"
    mock_path.return_value = output

    result: Path = export_to_file(
        output_format=".csv",
        org_units_df=df,
        ou_type_id=None,
        output_file_name=None,
    )

    assert result == output
    mock_run.add_file_output.assert_called()


@patch("pipeline._prepare_geodataframe")
@patch("pipeline.create_engine")
@patch("pipeline.workspace")
@patch("pipeline.current_run")
def test_export_to_database(
    mock_run: MagicMock,
    mock_workspace: MagicMock,
    mock_engine: MagicMock,
    mock_prepare: MagicMock,
) -> None:
    """Test exporting org units to spatial database."""
    mock_workspace.database_url = "postgresql://"

    geo_df: MagicMock = MagicMock()
    mock_prepare.return_value = geo_df

    export_to_database(pl.DataFrame(), "table", "replace")

    geo_df.to_postgis.assert_called_once()
    geo_df.to_postgis.assert_called_with(
        "table",
        mock_engine.return_value,
        if_exists="replace",
        index=False,
    )
    mock_run.add_database_output.assert_called_once()
    mock_run.add_database_output.assert_called_with("table")


@patch("pipeline.in_dataset_version", return_value=False)
@patch("pipeline.current_run")
def test_export_to_dataset(
    mock_run: MagicMock,
    mock_in_dataset: MagicMock,
    tmp_path: Path,
) -> None:
    """Test exporting file to dataset version."""
    file_path: Path = tmp_path / "file.csv"
    file_path.write_text("data", encoding="utf-8")

    mock_version: MagicMock = MagicMock()
    mock_version.name = "v1"

    mock_dataset: MagicMock = MagicMock()
    mock_dataset.latest_version = None
    mock_dataset.create_version.return_value = mock_version

    export_to_dataset(file_path, mock_dataset)

    mock_dataset.create_version.assert_called_once()
    mock_version.add_file.assert_called_once()


def test_convert_to_geometry() -> None:
    """Test conversion of GeoJSON string to Shapely geometry."""
    geojson: str = json.dumps({"type": "Point", "coordinates": [1, 2]})

    geom = convert_to_geometry(geojson)

    assert isinstance(geom, Point)
    assert geom.x == 1
    assert geom.y == 2


def test_clean_string() -> None:
    """Test normalization and sanitization of input string."""
    result: str = clean_string("Hôpital Général #1")

    assert result == "hopital_general_1"
