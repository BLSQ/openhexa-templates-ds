import time
from datetime import datetime
from pathlib import Path

import polars as pl
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    get_category_option_combos,
    get_data_element_groups,
    get_data_elements,
    get_datasets,
    get_organisation_unit_groups,
    get_organisation_units,
)
from validation_config import (
    org_unit_groups_expected_columns,
    org_units_expected_columns,
    retrieved_categorty_options_expected_columns,
    retrieved_data_element_groups_expected_columns,
    retrieved_data_elements_expected_columns,
    retrieved_datasets_expected_columns,
)


@pipeline("dhis2_metadata_extract")
@parameter(
    "dhis2_connection",
    name="DHIS2 instance",
    type=DHIS2Connection,
    help="Credentials for the DHIS2 instance connection",
    required=True,
)
@parameter(
    "get_org_units",
    name="Organisation units",
    type=bool,
    help="Retrieve organisation units data",
    required=False,
    default=False,
)
@parameter(
    "get_org_unit_groups",
    name="Organisation unit groups",
    type=bool,
    help="Retrieve organisation unit groups data",
    required=False,
    default=False,
)
@parameter(
    "get_datasets",
    name="Datasets",
    type=bool,
    help="Retrieve available datasets",
    required=False,
    default=False,
)
@parameter(
    "get_data_elements",
    name="Data elements",
    type=bool,
    help="Retrieve data elements data",
    required=False,
    default=False,
)
@parameter(
    "get_data_element_groups",
    name="Data elements groups",
    type=bool,
    help="Retrieve data element groups data",
    required=False,
    default=False,
)
@parameter(
    "get_category_options",
    name="Category option combos",
    type=bool,
    help="Retrieve category option combos data",
    required=False,
    default=False,
)
@parameter(
    "output_path",
    name="Output path",
    type=str,
    help="Output path for the shapes",
    required=False,
    default=None,
)
def dhis2_metadata_extract(
    dhis2_connection: DHIS2,
    get_org_units: bool,
    get_org_unit_groups: bool,
    get_datasets: bool,
    get_data_elements: bool,
    get_data_element_groups: bool,
    get_category_options: bool,
    output_path: str,
):
    """DHIS2 Metadata Extract Pipeline.

    This pipeline retrieves metadata from a DHIS2 instance and saves it to CSV files.

    Parameters
    ----------
    dhis2_connection : DHIS2Connection
        The connection to the DHIS2 instance.
    get_org_units : bool
        Whether to retrieve organisation units data.
    get_org_unit_groups : bool
        Whether to retrieve organisation unit groups data.
    get_datasets : bool
        Whether to retrieve available datasets.
    get_data_elements : bool
        Whether to retrieve data elements data.
    get_data_element_groups : bool
        Whether to retrieve data element groups data.
    get_category_options : bool
        Whether to retrieve category option combos data.
    output_path : str
        The output path for the shapes.
    """
    if output_path is None:
        output_path = (
            Path(workspace.files_path)
            / "pipelines"
            / "dhis2_metadata_extract"
            / f"data_{datetime.now().strftime('%Y_%m_%d_%H%M')}"
        )
        current_run.log_info(f"Output path not specified, using default {output_path}")
    else:
        output_path = Path(output_path)

    try:
        dhis2_client = get_dhis2_client(dhis2_connection)
        retrieve_org_units(dhis2_client, output_path, get_org_units)
        retrieve_org_unit_groups(dhis2_client, output_path, get_org_unit_groups)
        retrieve_datasets(dhis2_client, output_path, get_datasets)
        retrieve_data_elements(dhis2_client, output_path, get_data_elements)
        retrieve_data_element_groups(dhis2_client, output_path, get_data_element_groups)
        retrieve_category_option_combos(dhis2_client, output_path, get_category_options)
        current_run.log_info("Pipeline finished.")
    except Exception as e:
        current_run.log_error(f"An error ocurred during the execution: {e}")
        raise


def validate_data(df: pl.DataFrame, expected_columns: dict, data_name: str) -> None:
    """Validate a Polars DataFrame against a predefined schema specification.

    This function performs a comprehensive validation of a DataFrame based on 
    the expected schema rules provided in `expected_columns`. It checks for:
    
    1. **Non-emptiness** Ensures the DataFrame contains at least one row.
    2. **Schema compliance** Confirms that only expected columns exist and 
       that each column matches its expected data type.
    3. **Non-null constraints** Verifies that columns marked as `not null` 
       have no missing or empty values.
    4. **Character limits** Checks that text columns do not exceed the 
       specified maximum character length.
    5. **Integer conversion** Validates that columns flagged as convertible 
       to integers can be successfully cast.

    If any of these validations fail, a `RuntimeError` is raised with detailed 
    error messages summarizing all detected issues.

    Parameters
    ----------
    df : pl.DataFrame
        The Polars DataFrame to validate.
    expected_columns : dict
        A list or dictionary defining the expected schema for each column, where 
        each entry includes the column name, data type, and optional constraints 
        such as `not null`, `number of characters`, or `can be converted to integer`.
    data_name : str
        A human-readable identifier for the dataset being validated, used for logging.

    Raises
    ------
    RuntimeError
        If any validation rule fails — including empty DataFrames, unexpected columns, 
        schema mismatches, missing values, character limit violations, or failed type conversions.
    """
    current_run.log_info(f"Validating data in {data_name}")
    validation_start_time = time.time()

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
    for col in expected_columns:
        col_name = col["name"]
        col_type = col["type"]
        # validating data types
        if str(df.schema[col_name]) != col_type:
            error_messages.append(
                f"Type of column {col_name} is {df.schema[col_name]} and"
                f"does not match expected type: {col_type}"
            )
        # validating emptiness of a column
        if col["not null"]:
            df_empty_or_null_cololumn = df.filter(
                (pl.col(col_name).is_null()) | (pl.col(col_name) == "")  # noqa: PLC1901
            )
            if df_empty_or_null_cololumn.height > 0:
                error_messages.append(f"Column {col_name} has missing values."
                                      "It is not expected have any value missing")

        # validating number of characters
        char_num = col.get("number of characters")
        if char_num:
            df_with_char_count = df.filter(
                pl.col(col_name).str.len_chars().alias("char_count") > char_num
                )
            if df_with_char_count.height > 0:
                raise RuntimeError(f"Found values exceeding {char_num} characters:\n{col_name}")

        # validating column values to be
        # able to converted to integers
        int_conversion = col.get("can be converted to integer")
        if int_conversion:
            try:
                df.with_columns(pl.col(col_name).cast(pl.Int64))
            except Exception as e:
                raise RuntimeError(f"Column {col_name} cannot be converted to integer: {e}")  # noqa: B904

    if len(error_messages) > 1:
        raise RuntimeError("\n".join(error_messages))
    
    validation_end_time = time.time()
    duration = validation_end_time - validation_start_time
    current_run.log_info(f"Validation of {data_name} took {duration:.2f} seconds.")


def get_dhis2_client(dhis2_connection: DHIS2Connection) -> DHIS2:
    """Get the DHIS2 connection.

    Parameters
    ----------
    dhis2_connection : DHIS2Connection
        The connection to the DHIS2 instance.

    Returns
    -------
    DHIS2
        An instance of the DHIS2 client connected to the specified DHIS2 instance.
    """
    try:
        dhis2_client = DHIS2(dhis2_connection, cache_dir=Path(workspace.files_path) / ".cache")
        current_run.log_info(f"Successfully connected to DHIS2 instance {dhis2_connection.url}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to {dhis2_connection.url} error: {e}") from e


# @dhis2_metadata_extract.task
def retrieve_org_units(
    dhis2_client: DHIS2,
    output_path: Path,
    run: bool = True,
) -> None:
    """Retrieve and save organisation units at the maximum level from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    run : bool, optional
        Whether to execute the retrieval (default is True).
    """
    if run:
        current_run.log_info("Retrieving organisation units")
        try:
            org_units = get_organisation_units(dhis2_client)
            max_level = org_units.select(pl.col("level").max()).item()
            org_units = org_units.filter(pl.col("level") == max_level).drop(
                ["id", "name", "level", "opening_date", "closed_date", "geometry"]
            )
            validate_data(org_units, org_units_expected_columns, data_name="org_units")
            filename = f"org_units_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=org_units, output_path=output_path, filename=filename)
        except Exception as e:
            raise Exception(f"Error while retrieving organisation units: {e}") from e
        current_run.log_info(f"Organisation units saved under: {output_path / filename}")


def retrieve_org_unit_groups(
    dhis2_client: DHIS2,
    output_path: Path,
    run: bool = True,
) -> None:
    """Retrieve and save organisation unit groups from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving organisation unit groups.
    """
    if run:
        current_run.log_info("Retrieving organisation unit groups")
        try:
            org_unit_groups = get_organisation_unit_groups(dhis2_client)
            org_unit_groups = org_unit_groups.with_columns(
                [pl.col("organisation_units").list.join(",").alias("organisation_units")]
            )

            validate_data(
                org_unit_groups,
                org_unit_groups_expected_columns,
                data_name="org_unit_groups"
                )
            filename = f"org_units_groups_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=org_unit_groups, output_path=output_path, filename=filename)
        except Exception as e:
            raise Exception(f"Error while retrieving organisation unit groups: {e}") from e
        current_run.log_info(f"Organisation units saved under: {output_path / filename}")


def retrieve_datasets(
    dhis2_client: DHIS2,
    output_path: Path,
    run: bool = True,
) -> None:
    """Retrieve and save datasets from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving datasets.
    """
    if run:
        current_run.log_info("Retrieving datasets")
        try:
            datasets = get_datasets(dhis2_client)
            datasets = datasets.with_columns(
                [
                    pl.col("organisation_units").list.join(",").alias("organisation_units"),
                    pl.col("data_elements").list.join(",").alias("data_elements"),
                    pl.col("indicators").list.join(",").alias("indicators"),
                ]
            )

            validate_data(
                datasets,
                retrieved_datasets_expected_columns,
                data_name="datasets"
                )
            filename = f"datasets_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=datasets, output_path=output_path, filename=filename)
        except Exception as e:
            raise Exception(f"Error while retrieving datasets: {e}") from e
        current_run.log_info(f"Datasets saved under: {output_path / filename}")


def retrieve_data_elements(dhis2_client: DHIS2, output_path: Path, run: bool = True) -> None:
    """Retrieve and save data elements from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving data elements.
    """
    if run:
        current_run.log_info("Retrieving data elements")
        try:
            data_elements = get_data_elements(dhis2_client)
            filename = f"data_elements_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            print(data_elements)
            validate_data(
                data_elements,
                retrieved_data_elements_expected_columns,
                data_name="data_elements"
                )
            save_file(df=data_elements, output_path=output_path, filename=filename)
        except Exception as e:
            raise Exception(f"Error while retrieving data elements: {e}") from e
        current_run.log_info(f"Data elements saved under: {output_path / filename}")


def retrieve_data_element_groups(dhis2_client: DHIS2, output_path: Path, run: bool = True) -> None:
    """Retrieve and save data element groups from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving data elements.
    """
    if run:
        current_run.log_info("Retrieving data element groups")
        try:
            data_element_groups = get_data_element_groups(dhis2_client)
            data_element_groups = data_element_groups.with_columns(
                [pl.col("data_elements").list.join(",").alias("data_elements")]
            )

            validate_data(
                data_element_groups,
                retrieved_data_element_groups_expected_columns,
                data_name="data_element_groups"
                )
            filename = f"data_element_groups_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=data_element_groups, output_path=output_path, filename=filename)
        except Exception as e:
            raise Exception(f"Error while retrieving data element groups: {e}") from e
        current_run.log_info(f"Data element groups saved under: {output_path / filename}")


def retrieve_category_option_combos(
    dhis2_client: DHIS2, output_path: Path, run: bool = True
) -> None:
    """Retrieve and save category option combos from the DHIS2 instance.

    Parameters
    ----------
    dhis2_client : DHIS2
        The DHIS2 client instance.
    output_path : Path
        The directory where the output CSV file will be saved.
    run : bool, optional
        Whether to execute the retrieval (default is True).

    Raises
    ------
    Exception
        If an error occurs while retrieving category option combos.
    """
    if run:
        current_run.log_info("Retrieving category option combos")
        try:
            categorty_options = get_category_option_combos(dhis2_client)

            validate_data(
                categorty_options,
                retrieved_categorty_options_expected_columns,
                data_name="categorty_options"
                )
            filename = f"category_option_combos_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"
            save_file(df=categorty_options, output_path=output_path, filename=filename)
        except Exception as e:
            raise Exception(f"Error while retrieving categorty option combos: {e}") from e
        current_run.log_info(f"Category option combos saved under: {output_path / filename}")


def save_file(df: pl.DataFrame, output_path: Path, filename: str) -> None:
    """Save a GeoDataFrame to a file in the specified output path.

    Parameters
    ----------
    df : pl.DataFrame
        The pl.DataFrame containing the data to save.
    output_path : Path
        The directory where the file will be saved.
    filename : str
        The name of the file to save the shapes in.

    Raises
    ------
    PermissionError
        If there is no permission to access the file.
    OSError
        If an I/O error occurs during saving.
    Exception
        For any other unexpected errors.
    """
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(f"Error creating output directory {output_path}: {e}") from e

    try:
        output_fname = output_path / filename
        df.write_csv(output_fname)
    except PermissionError as e:
        raise PermissionError("Error: You don't have permission to access this file.") from e
    except OSError as e:
        raise OSError(f"An I/O error occurred: {e}") from e
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}") from e


if __name__ == "__main__":
    dhis2_metadata_extract()
