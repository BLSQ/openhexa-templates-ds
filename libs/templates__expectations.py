import inspect
import pathlib

import great_expectations as gx
import pandas as pd
import yaml


class TemplatesExpectations:
    """Module to maintain data quality.
    
    Module that takes definition of expected output of a pipeline to run
    check on the real pipeline output to ensure maintenance of expected data quality.
    """

    def __init__(self, dataset: pd.DataFrame, expectations_yml_file: str | None = None):
        """Onset of definitions."""
        if not isinstance(expectations_yml_file, str) and expectations_yml_file is not None:
            raise ValueError("expectations_yml_file should be a string")
            
        if not isinstance(dataset, pd.DataFrame):
            raise ValueError("dataset should be a pandas dataframe")
        
        if expectations_yml_file is None:
            caller_file = inspect.stack()[1].filename
            caller_dir = pathlib.Path(pathlib.Path(caller_file).resolve()).parent
            self.expectations_yml_file = f"{caller_dir}/expectations.yml"
        else:
            self.expectations_yml_file = expectations_yml_file
        
        self.dataset = dataset

    def _read_definitions(self):  # noqa: ANN202
        """Read the yml file contaning definitions."""  # noqa: DOC201
        try:
            with open(self.expectations_yml_file) as file:  # noqa: PLW1514, PTH123
                expectations = yaml.safe_load(file)
        except FileNotFoundError:
            print("Error: 'expectations.yaml' not found.")
            raise 
        except yaml.YAMLError as e:
            print(f"Error parsing 'expectations.yaml' file: {e}")
            raise
        return expectations
    
    def validate_expectations(self):
        """Use the list of expectations provided to run validations."""
        context = gx.get_context()
        data_source = context.data_sources.add_pandas(name="pandas")
        data_asset = data_source.add_dataframe_asset(name="pd_dataframe_asset")
    
        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch-def")

        batch_parameters = {"dataframe": self.dataset}
        batch = batch_definition.get_batch(batch_parameters=batch_parameters)  # noqa: F841

        expectations = self._read_definitions()
        # validate dataframe length
        if self.dataset.empty and expectations["dataframe"]["size"] == "not empty":
            raise Exception("DataFrame is empty")
        
        # validate number of columns
        if expectations["dataframe"]["no_columns"]:
            expected_no_columns = expectations["dataframe"]["no_columns"]
            real_no_columns = self.dataset.shape[1]
            if real_no_columns != int(expected_no_columns):
                raise Exception(
                    f"""Expected number of columns is {expected_no_columns}
                    real number of colums is {real_no_columns}""")
        
        # validate expected number of rows
        if expectations["dataframe"]["no_rows"]:
            expected_no_rows = expectations["dataframe"]["no_rows"]
            real_no_rows = self.dataset.shape[0]
            if real_no_rows != int(expected_no_rows):
                raise Exception(
                    f"""Expected number of rows is {expected_no_rows}
                    real number of rows is {real_no_rows}""")
        # validate datatypes
        print(self.dataset.dtypes)
        # creating expectation suite
        suite = context.suites.add(
            gx.core.expectation_suite.ExpectationSuite(name="expectations_suite")
        )
        # validate expected column schema
        for column in expectations["columns"]:
            # validate datatype

            # validate min and max
            column_expectation = expectations["columns"][column]
            if column_expectation["type"].lower() in ["float", "integer"]:
                if column_expectation["maximum"] and column_expectation["minimum"]:
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeBetween(
                        column=column, min_value=column_expectation["minimum"],
                        max_value=column_expectation["maximum"]
                    ))
                elif column_expectation["maximum"] and not column_expectation["minimum"]:
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeBetween(
                        column=column, max_value=column_expectation["maximum"]
                    ))
                elif column_expectation["minimum"] and not column_expectation["maximum"]:
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeBetween(
                        column=column, max_value=column_expectation["minimum"]
                    ))
    
        # validation definition
        validation_definition = context.validation_definitions.add(
            gx.core.validation_definition.ValidationDefinition(
                name="validation definition",
                data=batch_definition,
                suite=suite
            )
        )

        checkpoint = context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name="context", validation_definitions=[validation_definition]
            )
        )
        batch_parameters = {"dataframe": self.dataset}
        checkpoint_result = checkpoint.run(batch_parameters=batch_parameters)
        print(checkpoint_result.describe())


if __name__ == "__main__":
    df = pd.DataFrame(
        {"age": [19, 20, 30], "height": [7, 5, 6], "gender": ["male", "female", "other"]}
        )
    validator = TemplatesExpectations(df)
    validator.validate_expectations()