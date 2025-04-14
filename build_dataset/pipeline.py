"""Template for newly generated pipelines."""

import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace


@pipeline("build_dataset")
@parameter("dataset", name="Select existing dataset", type=Dataset)
@parameter("dataset_paths", name="Select existing dataset (str path)", type=str, multiple=True)
def build_dataset(dataset: Dataset, dataset_paths: list[str]):
    """Pipeline to build a dataset from multiple folders of CSV files.

    This pipeline will create a new dataset version for each folder and upload the CSV files to the
    corresponding dataset version.
    """
    load_and_save(dataset_paths, dataset)


@build_dataset.task
def load_and_save(dataset_paths: list[str], dataset: Dataset):
    """Loads data from multiple dataset folders, processes the data, and saves it to a new version.

    of the given dataset.

    Args:
        dataset_paths (list[str]): A list of folder paths, each representing one dataset.
            Each folder is expected to contain subdirectories with CSV files.
            is expected to contain subdirectories with CSV files.
        dataset (Dataset): The dataset object where the processed data will be saved.
    Workflow:
        1. Iterates through the provided dataset paths.
        2. For each dataset path:
            - Logs the processing of the dataset folder.
            - Creates a new version of the dataset with a timestamp as the version name.
                - Reads all CSV files in the subdirectory and concatenates them
                  into a single DataFrame.
                - Reads all CSV files in the subdirectory and concatenates them into a single
                DataFrame.
                - Logs warnings for non-directory entries or failed CSV reads.
                - Logs an error if no valid CSV files are found in the subdirectory.
            - Saves the concatenated DataFrame to a CSV file in the dataset path.
            - Uploads the saved CSV file to the newly created dataset version.

    Notes:
        - The function uses `DatasetVersion.add_file` to upload the processed files
          to the dataset version.
          grouping of data that should be concatenated into a single CSV file.
        - The function uses `DatasetVersion.add_file` to upload the processed files to the dataset
        version.

    Raises:
        Exception: If any unexpected error occurs during file reading or processing.
    Logs:
        - Logs information about dataset processing and version creation.
        - Logs warnings for non-directory entries or failed CSV reads.
        - Logs errors if no valid CSV files are found in a subdirectory.
    """
    dataset_name = dataset.name
    for dataset_path_str in dataset_paths:
        dataset_path = Path(f"{workspace.files_path}/{dataset_path_str}")
        if not dataset_path.exists():
            current_run.log_error(f"Dataset path does not exist: {dataset_path}")
            continue

        current_run.log_info(f"Processing dataset folder: {dataset_name}")

        # Create a new version
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        version = dataset.create_version(name=date_time)
        current_run.log_info(f"Created version: {version.name} for dataset {dataset.name}")
        skip_columns = "Unnamed:"
        for data_element_folder in dataset_path.iterdir():
            if not data_element_folder.is_dir():
                current_run.log_warning(f"Skipping non-directory: {data_element_folder}")
                continue

            all_dfs = []
            for csv_file in sorted(data_element_folder.glob("*.csv")):
                try:
                    df = pd.read_csv(csv_file)
                    if "period" in df.columns:
                        df = parse_period_column(df)
                    else:
                        current_run.log_warning(
                            f"{csv_file} does not contain 'period' column standard name for date\
                                column."
                        )

                    keep = [col for col in df.columns if skip_columns not in col]
                    all_dfs.append(df[keep])
                    if len(all_dfs) == 1:
                        first_csv_file = csv_file
                    elif len(all_dfs) > 1:
                        inconsistent_columns = set(all_dfs[0].columns) - set(
                            all_dfs[-1].columns
                        ) | set(all_dfs[-1].columns) - set(all_dfs[0].columns)
                        if inconsistent_columns:
                            current_run.log_warning(
                                f"Inconsistent columns between {csv_file} and {first_csv_file}:\
                                      {inconsistent_columns}"
                            )

                except Exception as e:
                    current_run.log_warning(f"Failed to read {csv_file}: {e}")

            if not all_dfs:
                current_run.log_error(f"No valid CSV files found in {data_element_folder}")
                continue

        concatenated_df = pd.concat(all_dfs, ignore_index=True)
    if len(dataset_paths) > 1:
        output_filename = f"{dataset_path_str}.csv"
        output_path = f"{workspace.files_path}/{dataset_path_str}/{output_filename}"
    else:
        output_filename = f"{dataset_name}.csv"
        output_path = f"{workspace.files_path}/{output_filename}"
    concatenated_df.to_csv(output_path, index=False)

    # Upload to dataset version using DatasetVersion.add_file
    version.add_file(source=output_path, filename=output_filename)
    current_run.log_info(f"Added file: {output_filename} to dataset version")


def parse_period_column(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and standardize the 'period' column in a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame containing a 'period' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'period' column parsed into a standardized datetime
        format.

    Raises:
        ValueError: If the 'period' column contains an unrecognized format.
    """
    # Only process if "period" exists
    if "period" not in df.columns:
        return df

    sample_value = str(df["period"].dropna().iloc[0])

    # Define format checkers
    known_formats = [
        ("%Y-%m-%d", lambda v: re.fullmatch(r"\d{4}-\d{2}-\d{2}", v)),
        ("%Y%m", lambda v: re.fullmatch(r"\d{6}", v)),
        ("%Y", lambda v: re.fullmatch(r"\d{4}", v)),
        ("%YQ%q", lambda v: re.fullmatch(r"\d{4}Q[1-4]", v)),  # custom handled
    ]

    for fmt, checker in known_formats:
        if checker(sample_value):
            if fmt == "%YQ%q":
                # Custom parse YYYYQq (e.g., "2024Q2")
                df["period"] = df["period"].apply(_parse_quarter)
            else:
                df["period"] = pd.to_datetime(df["period"], format=fmt)
            return df

    # Fallback to automatic parsing
    try:
        df["period"] = pd.to_datetime(df["period"])
    except Exception:
        raise ValueError(f"Unrecognized period format: {sample_value}") from None

    return df


def _parse_quarter(qstr: str) -> pd.Timestamp:
    match = re.match(r"(\d{4})Q([1-4])", qstr)
    if not match:
        raise ValueError(f"Invalid quarter format: {qstr}")
    year, quarter = int(match.group(1)), int(match.group(2))
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


if __name__ == "__main__":
    build_dataset()
