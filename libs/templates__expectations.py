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
        self.numeric_types = ["float64", "int64"]

    def _read_definitions(self) -> dict:
        """Read the yml file contaning definitions.
        
        Returns:
            dictionary containing validation values read from expectations file
        """ 
        try:
            with pathlib.Path.open(self.expectations_yml_file, encoding="utf-8") as file:
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
                err = f"""
                    Columns missmatch:
                    Expected number of columns is {expected_no_columns}
                    real number of colums is {real_no_columns}"""
                raise Exception(err)
        
        # validate expected number of rows
        if expectations["dataframe"]["no_rows"]:
            expected_no_rows = expectations["dataframe"]["no_rows"]
            real_no_rows = self.dataset.shape[0]
            if real_no_rows != int(expected_no_rows):
                raise Exception(
                    f"""
                    Rows missmatch:
                    Expected number of rows is {expected_no_rows}
                    real number of rows is {real_no_rows}""")
        # validate datatypes
        print(self.dataset.dtypes)
        # creating expectation suite
        suite = context.suites.add(
            gx.core.expectation_suite.ExpectationSuite(name="expectations_suite")
        )
        # validate expected column schema
        for column in expectations["columns"]:
            column_expectation = expectations["columns"][column]
            # validate datatype
            if column_expectation["type"]:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=column,
                        type_=column_expectation["type"]
                    )
                )
            # validate min and max
            print(column_expectation)
            if column_expectation["type"] in self.numeric_types:
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
            # validating missing values
            if column_expectation["not-null"]:
                suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

            if column_expectation["type"] in ["object"]:
                # validate classes
                if column_expectation["classes"]:
                    suite.add_expectation(
                        gx.expectations.ExpectColumnDistinctValuesToBeInSet(
                            column=column,
                            value_set=column_expectation["classes"]
                        )
                    )
                # validate value length
                if column_expectation["length-between"]:
                    if len(column_expectation["length-between"]) == 1:
                        suite.add_expectation(
                            gx.expectations.ExpectColumnValueLengthsToEqual(
                                column=column,
                                value=column_expectation["length-between"][0]
                            )
                        )
                    elif len(column_expectation["length-between"]) == 2:
                        suite.add_expectation(
                            gx.expectations.ExpectColumnValueLengthsToBeBetween(
                                column=column,
                                max_value=max(column_expectation["length-between"]),
                                min_value=min(column_expectation["length-between"])
                            )
                        )
                    else:
                        raise Exception("length-between should have either 1 or 2 entries.")

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
        {
            "age": [19, 20, 30],
            "height": [7, 5, 6],
            "gender": ["male", "female", None],
            "phone": ["0711222333", "0722111333", "+256744123432"],
            "shirt_size": ["s", "m", "l"],
            }
        )
    validator = TemplatesExpectations(df)
    validator.validate_expectations()
