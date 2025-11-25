from __future__ import annotations

import json
import logging
from enum import StrEnum
from pathlib import Path
from typing import TypedDict

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    get_category_option_combos,
    import_data_values,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)
logging.getLogger("openhexa.toolbox.era5").setLevel(logging.INFO)


class LocalRun:
    """A local run that supports logging."""

    def log_info(self, msg: str) -> None:
        """Log info message."""
        logger.info(msg)

    def log_error(self, msg: str) -> None:
        """Log error message."""
        logger.error(msg)

    def log_debug(self, msg: str) -> None:
        """Log debug message."""
        logger.debug(msg)


run = current_run or LocalRun()


class ImportStrategy(StrEnum):
    """Import strategy for DHIS2 data values."""

    CREATE_AND_UPDATE = "CREATE_AND_UPDATE"
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class Period(StrEnum):
    """Aggregation period for ERA5 data."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@pipeline("era5_load_dhis2")
@parameter(
    code="dst_dhis2",
    type=DHIS2Connection,
    name="Target DHIS2",
    help="DHIS2 instance where data values will be imported",
)
@parameter(
    code="era5_dir",
    type=str,
    name="ERA5 data directory",
    help="Data directory with ERA5 aggregated data in Parquet format",
)
@parameter(
    code="variables",
    type=str,
    multiple=True,
    name="ERA5 variables",
    help="ERA5 variables to import (see docs for supported variables)",
)
@parameter(
    code="period",
    type=str,
    choices=["day", "week", "month"],
    name="Aggregation period",
    help="Aggregation period of the ERA5 data to import",
    default="month",
)
@parameter(
    code="import_strategy",
    type=str,
    name="Import strategy",
    help="Import strategy to use (CREATE_AND_UPDATE, CREATE, UPDATE, DELETE)",
    default="CREATE",
    choices=["CREATE_AND_UPDATE", "CREATE", "UPDATE", "DELETE"],
)
@parameter(
    code="data_elements_mapping",
    type=str,
    name="Data elements mapping",
    help="Data elements mapping in JSON (see docs for format)",
    required=True,
)
@parameter(
    code="org_units_mapping",
    type=str,
    name="Organisation units mapping",
    help="Organisation units mapping in JSON (see docs for format)",
    required=False,
)
@parameter(
    code="dry_run",
    type=bool,
    name="Dry run",
    help="If true, do not actually import data values, just simulate",
    default=True,
)
def era5_load_dhis2(
    dst_dhis2: DHIS2Connection,
    era5_dir: str,
    variables: list[str],
    data_elements_mapping: str,
    period: Period = Period.MONTH,
    org_units_mapping: str | None = None,
    import_strategy: ImportStrategy = ImportStrategy.CREATE,
    dry_run: bool = True,
) -> None:
    """Import data values into a DHIS2 instance from a CSV or Parquet file."""
    src_dir = Path(workspace.files_path, era5_dir)
    if not src_dir.exists():
        msg = f"ERA5 data directory not found: {src_dir}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    dhis2 = DHIS2(dst_dhis2, cache_dir=Path(workspace.files_path, ".cache"))
    files = index_data_dir(src_dir)
    de_mapping = read_mapping(data_elements_mapping)
    ou_mapping = read_mapping(org_units_mapping) if org_units_mapping else None

    default_coc = get_default_coc(dhis2)
    run.log_info(f"Using default category option combo: {default_coc}")
    coc_mapping = {"default": default_coc}
    aoc_mapping = {"default": default_coc}

    for variable in variables:
        data_values = as_data_values(files=files[variable], variable=variable, period=period)
        msg = f"Read {len(data_values)} data values for variable '{variable}' and period '{period}'"
        run.log_info(msg)
        report = load(
            dst_dhis2=dhis2,
            data_values=data_values,
            data_elements_mapping=de_mapping,
            org_units_mapping=ou_mapping,
            category_option_combos_mapping=coc_mapping,
            attribute_option_combos_mapping=aoc_mapping,
            import_strategy=import_strategy,
            dry_run=dry_run,
        )
        msg = f"Finished data import for variable '{variable}' and period '{period}'"
        run.log_info(msg)
        run.log_info(f"Import report: {report}")


class ERA5File(TypedDict):
    """An indexed ERA5 data file."""

    variable: str
    period: str
    fpath: Path


def index_data_dir(data_dir: Path) -> dict[str, list[ERA5File]]:
    """Index ERA5 data files in source directory.

    Args:
        data_dir: Path to the ERA5 data directory.

    Returns:
        Dictionary mapping variable names to ERA5File objects.

    """
    index: dict[str, list[ERA5File]] = {}
    for fp in data_dir.glob("*.parquet"):
        items = fp.stem.split("_")
        if len(items) == 2:
            var, period = items
        elif len(items) == 3:
            var = "_".join(items[:-1])
            period = items[2]
        else:
            msg = f"Invalid ERA5 data file name: {fp.name}"
            logger.error(msg)
            raise ValueError(msg)
        if var not in index:
            index[var] = []
        index[var].append(ERA5File(variable=var, period=period, fpath=fp))
    return index


def as_data_values(files: list[ERA5File], variable: str, period: Period) -> pl.DataFrame:
    """Convert ERA5 data files to DHIS2 data values dataframe for import.

    Args:
        files: List of indexed ERA5 data files.
        variable: Variable to include.
        period: Aggregation period.

    Returns:
        Data values as a Polars DataFrame.
    """
    schema = {
        "data_element_id": pl.String,
        "period": pl.String,
        "organisation_unit_id": pl.String,
        "category_option_combo_id": pl.String,
        "attribute_option_combo_id": pl.String,
        "value": pl.String,
    }

    df = pl.DataFrame(schema=schema)

    for f in files:
        if f["variable"] != variable or f["period"] != period:
            continue
        logger.info(f"Processing ERA5 file: {f['fpath'].name}")
        df_var = pl.read_parquet(f["fpath"])
        df_var = df_var.select(
            pl.lit(f["variable"]).alias("data_element_id"),
            pl.col("period"),
            pl.col("boundary").alias("organisation_unit_id"),
            pl.lit("default").alias("category_option_combo_id"),
            pl.lit("default").alias("attribute_option_combo_id"),
            pl.col("value").round(5).cast(pl.String),
        )
        df = df.vstack(df_var)

    return df


def read_mapping(mapping_path: str) -> dict[str, str]:
    """Read mapping from a JSON file.

    Args:
        mapping_path: Path to the mapping JSON file.

    Returns:
        Mapping as a dictionary.

    Raises:
        FileNotFoundError: If the mapping file does not exist.
        ValueError: If the mapping file is not JSON.
    """
    fpath = Path(workspace.files_path, mapping_path)
    if not fpath.exists():
        msg = f"Mapping file not found: {fpath}"
        logger.error(msg)
        raise FileNotFoundError(msg)
    if fpath.suffix.lower() != ".json":
        msg = "Mapping file must be JSON"
        logger.error(msg)
        raise ValueError(msg)
    with fpath.open(encoding="utf-8") as f:
        return json.load(f)


def get_default_coc(dhis2: DHIS2) -> str:
    """Get UID of the default category option combo.

    Args:
        dhis2: DHIS2 instance.

    Returns:
        UID of the default category option combo.
    """
    cocs = get_category_option_combos(dhis2)
    default_coc = cocs.filter(pl.col("name") == "default")
    if len(default_coc) == 0:
        msg = "Default category option combo not found in DHIS2"
        run.log_error(msg)
        raise ValueError(msg)
    return str(default_coc[0, "id"])


def load(
    dst_dhis2: DHIS2,
    data_values: pl.DataFrame,
    data_elements_mapping: dict[str, str] | None = None,
    org_units_mapping: dict[str, str] | None = None,
    category_option_combos_mapping: dict[str, str] | None = None,
    attribute_option_combos_mapping: dict[str, str] | None = None,
    import_strategy: ImportStrategy = ImportStrategy.CREATE,
    dry_run: bool = False,
) -> dict[str, int]:
    """Load data values into DHIS2.

    Returns:
        Import report as a dictionary.

    """
    msg = f"Starting import of {len(data_values)} data values into DHIS2"
    run.log_info(msg)
    return import_data_values(
        dhis2=dst_dhis2,
        data=data_values,
        data_elements_mapping=data_elements_mapping,
        org_units_mapping=org_units_mapping,
        category_option_combos_mapping=category_option_combos_mapping,
        attribute_option_combos_mapping=attribute_option_combos_mapping,
        import_strategy=str(import_strategy),  # type: ignore[arg-type]
        dry_run=dry_run,
    )
