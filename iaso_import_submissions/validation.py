import re

import polars as pl
from openhexa.sdk import current_run
from pydantic import BaseModel, Field
from utils import (
    calculate_to_polars_expr,
    extract_list_name,
    get_choices_list_column,
    get_choices_name_column,
    is_select_multiple,
)


def _build_choice_map(
    questions: pl.DataFrame, choices: pl.DataFrame
) -> dict[str, tuple[set[str], bool]]:
    """Map each select question name to its allowed choice names and multi flag.

    The mapping resolves two XLSForm subtleties that the naive implementation
    missed:

    - the choice list name comes from the question ``type`` (``select_one <list>``),
      not from the question ``name``;
    - the value stored in a submission is the choice ``name`` (code), never the
      ``label`` (which may also be multilingual, e.g. ``label::French (fr)``).

    Args:
        questions (pl.DataFrame): Survey-sheet metadata.
        choices (pl.DataFrame): Choices-sheet metadata.

    Returns:
        dict[str, tuple[set[str], bool]]: question name -> (allowed names,
        is_select_multiple).
    """
    if questions.is_empty() or "type" not in questions.columns or "name" not in questions.columns:
        return {}

    list_col = get_choices_list_column(choices)
    name_col = get_choices_name_column(choices)
    if list_col is None or name_col is None:
        return {}

    mapping: dict[str, tuple[set[str], bool]] = {}
    for row in questions.select(["type", "name"]).iter_rows(named=True):
        list_name = extract_list_name(row["type"])
        if list_name is None or not row["name"]:
            continue
        allowed = set(
            choices.filter(pl.col(list_col) == list_name)[name_col].drop_nulls().to_list()
        )
        mapping[row["name"]] = (allowed, is_select_multiple(row["type"]))
    return mapping


def _value_in_choices(value: object, allowed: set[str], multiple: bool) -> bool:
    """Check that a submitted select value belongs to the allowed choice names.

    Args:
        value (object): The submitted value (a single name, or space-separated
            names for a multi-select).
        allowed (set[str]): Allowed choice names for the question.
        multiple (bool): Whether the question is a multi-select.

    Returns:
        bool: True if the value (or every token of a multi-select) is allowed.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        # An empty answer is not an invalid choice; requiredness is handled elsewhere.
        return True
    if multiple:
        tokens = [t for t in str(value).split() if t]
        return all(token in allowed for token in tokens)
    return str(value) in allowed


class ValidationResult(BaseModel):
    """Represents the result of validating submission data.

    Attributes:
        is_valid (bool): Indicates if the data is valid.
        errors (list[str]): List of error messages.
        warnings (list[str]): List of warning messages.
        missing_columns (list[str]): List of missing columns.
        invalid_types (dict[str, tuple[str, str]]): Mapping of columns to expected and actual types.
        required_columns_present (set[str]): Set of required columns that are present.
    """

    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    missing_columns: list[str] = Field(default_factory=list)
    invalid_types: dict[str, tuple[str, str]] = Field(default_factory=dict)
    required_columns_present: set[str] = Field(default_factory=set)


def validate_data_structure(
    df: pl.DataFrame,
    questions: pl.DataFrame,
    import_strategy: str,
) -> dict:
    """Validate the structure of the submissions data.

    Args:
        df (pl.DataFrame): DataFrame containing the submissions data.
        questions (pl.DataFrame): DataFrame containing the form questions metadata.
        import_strategy (str): The import strategy being used.

    Returns:
        dict: A dictionary representation of the validation result with the following keys:
            - is_valid (bool): Whether the data structure is considered valid.
            - errors (list[str]): List of error messages.
            - warnings (list[str]): List of warning messages.
            - missing_columns (list[str]): List of missing columns in the data.
            - invalid_types (dict[str, tuple[str, str]]): Mapping of columns to a tuple of
              (expected_type, actual_type).
            - required_columns_present (set[str]): Set of required columns that are present.
    """
    result = ValidationResult(is_valid=True)

    # Définition des exigences par stratégie
    strategy_requirements = {
        "CREATE": {
            "required": {"org_unit_id"},
            "optional": {"created_at", "form_version", "latitude", "longitude"},
            "types": {
                "org_unit_id": pl.Int64,
                "created_at": (pl.Date, pl.Datetime, pl.Utf8),
                "form_version": pl.Utf8,
            },
        },
        "UPDATE": {
            "required": {"id", "instanceID"},
            "optional": {"org_unit_id", "created_at", "form_version"},
            "types": {
                "id": pl.Utf8,
                "org_unit_id": pl.Int64,
                "instanceID": pl.Utf8,
                "created_at": (pl.Date, pl.Datetime, pl.Utf8),
            },
        },
        "CREATE_AND_UPDATE": {
            "required": {"org_unit_id"},
            "optional": {"id", "form_version", "instanceID"},
            "types": {
                "org_unit_id": pl.Int64,
                "instanceID": pl.Utf8,
                "id": pl.Utf8,
            },
        },
        "DELETE": {
            "required": {"id"},
            "optional": set(),
            "types": {"id": pl.Utf8},
        },
    }
    # We start by the latest form metadata version
    requirements = strategy_requirements[import_strategy]
    current_columns = set(df.columns)

    # 1. Check for required columns
    form_required: set[str] = set()
    if "required" in questions.columns and "name" in questions.columns:
        form_required = set(
            questions.filter(pl.col("required").cast(pl.Utf8).str.to_lowercase() == "yes")[
                "name"
            ].unique()
        )
    required_columns = requirements["required"] | form_required
    missing_required = required_columns - current_columns

    if missing_required:
        # Généralement ce sont les informations sur les hint qui ne figure pas dans le fichier d'importation  # noqa: E501
        result.is_valid = False
        result.missing_columns.extend(missing_required)
        result.errors.append(
            f"Strategy {import_strategy}: required columns missing: {', '.join(missing_required)}"
        )

    # 2. Check for column types
    has_type = "type" in questions.columns and "name" in questions.columns

    def _names_of_type(type_value: str) -> list[str]:
        if not has_type:
            return []
        return questions.filter(pl.col("type") == type_value)["name"].unique().to_list()

    dico_dtypes = {
        **{name: pl.String for name in _names_of_type("text")},
        **{name: pl.Int64 for name in _names_of_type("integer")},
        **{name: pl.String for name in _names_of_type("calculate")},
    }
    type_validation_result = _validate_column_types(
        df, {**requirements["types"], **dico_dtypes}, import_strategy
    )
    result.invalid_types.update(type_validation_result)
    if type_validation_result:
        result.is_valid = False
        for col, (expected, actual) in type_validation_result.items():
            result.errors.append(
                f"Invalid type for column '{col}': expected {expected}, got {actual}"
            )

    # 3. Validate column presence
    result.required_columns_present = requirements["required"] & current_columns

    # 4. Detect unexpected columns
    expected_columns = requirements["required"] | requirements["optional"]
    unexpected_columns = current_columns - expected_columns

    if unexpected_columns:
        known_question_names = (
            set(questions["name"].unique()) if "name" in questions.columns else set()
        )
        truly_unexpected = unexpected_columns - known_question_names
        for col in truly_unexpected:
            result.warnings.append(f"Unexpected column found: '{col}'")

    return dict(result.__dict__)


def validate_global_data(
    df: pl.DataFrame, questions: pl.DataFrame, choices: pl.DataFrame
) -> pl.DataFrame:
    """Validate global data constraints and choices for form submissions.

    Args:
        df: DataFrame containing the submissions data.
        questions: DataFrame containing the form questions metadata.
        choices: DataFrame containing the form choices metadata.

    Returns:
        DataFrame with added validation columns for constraints and choices.
    """
    if questions.is_empty() or "name" not in questions.columns:
        return df

    # 1) Computed fields: add missing calculate columns
    computed_fields = (
        questions.filter(pl.col("type") == "calculate").select(["name", "calculation"]).to_dicts()
        if {"type", "calculation"} <= set(questions.columns)
        else []
    )
    for rule in computed_fields:
        col_name = rule["name"]
        calculation = rule.get("calculation")
        if not calculation or col_name in df.columns:
            continue

        expr_str = calculate_to_polars_expr(calculation)
        try:
            # expr_str is expected to be a Polars expression string using `pl` namespace
            expr = eval(expr_str, {"pl": pl})
            df = df.with_columns(expr.alias(col_name))
            # Only round/int-cast genuinely numeric results; string calculates
            # (e.g. concat(), if(...,'a','b')) must be kept as-is.
            if df.schema[col_name].is_numeric():
                df = df.with_columns(pl.col(col_name).round().cast(pl.Int64).alias(col_name))
            current_run.log_info(
                f"Added calculated column '{col_name}' to submissions after computation."
            )
        except Exception as exc:  # keep narrow enough but robust
            current_run.log_warning(
                f"Could not compute calculated column '{col_name}': {exc}; "
                "leaving it empty for the submission"
            )
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col_name))

    # 2) Constraints validation: add <col>_valid boolean columns
    constraints_list = (
        questions.filter(pl.col("constraint").is_not_null())
        .select(["name", "constraint"])
        .to_dicts()
        if "constraint" in questions.columns
        else []
    )
    constraint_cols = []
    for rule in constraints_list:
        col_name = rule["name"]
        constraint = rule["constraint"]
        if col_name not in df.columns:
            current_run.log_warning(f"Constraint for missing column '{col_name}' skipped")
            continue

        # Use a Python validator per element because constraints may be arbitrary;
        # bind the constraint string into the function to avoid late-binding issues.
        def _validate_elem(v: object, c: str = constraint) -> bool:
            try:
                return bool(_validate_value(str(v), c))
            except Exception:
                return False

        df = df.with_columns(
            pl.col(col_name)
            .map_elements(_validate_elem)
            .cast(pl.Boolean)
            .fill_null(False)
            .alias(f"{col_name}_valid")
        )
        constraint_cols.append(f"{col_name}_valid")

    # combine constraints into a per-row summary
    if constraint_cols:
        df = df.with_columns(
            pl.fold(
                acc=pl.lit(True),
                function=lambda acc, x: acc & x,
                exprs=[pl.col(c) for c in constraint_cols],
            ).alias("constraints_validation_summary")
        )
        # drop individual constraint columns to keep output tidy
        df = df.drop(constraint_cols)

    # 3) Choices validation: compare each submitted value against the allowed
    # choice *names* of the question's list (resolved from its `type`).
    choice_map = _build_choice_map(questions, choices)
    if not choice_map:
        current_run.log_warning(
            "Choices metadata could not be resolved (missing 'list_name'/'name' columns "
            "or no select questions); skipping choices validation"
        )
        return df

    choices_cols = []
    for col_name, (allowed, multiple) in choice_map.items():
        if col_name not in df.columns:
            # nothing to validate for absent column
            continue

        df = df.with_columns(
            pl.col(col_name)
            .map_elements(
                lambda v, allowed=allowed, multiple=multiple: _value_in_choices(
                    v, allowed, multiple
                ),
                return_dtype=pl.Boolean,
            )
            .fill_null(True)
            .alias(f"{col_name}_choices_valid")
        )
        choices_cols.append(f"{col_name}_choices_valid")

    if choices_cols:
        df = df.with_columns(
            pl.fold(
                acc=pl.lit(True),
                function=lambda acc, x: acc & x,
                exprs=[pl.col(c) for c in choices_cols],
            ).alias("choices_validation_summary")
        )

    return df


def validate_field_constraints(
    record: dict, questions: pl.DataFrame, choices: pl.DataFrame
) -> bool:
    """Validate field constraints and choices for a single record.

    Args:
        record: Dictionary containing field values to validate.
        questions: DataFrame containing form questions metadata.
        choices: DataFrame containing form choices metadata.

    Returns:
        bool: True if all field values satisfy their constraints, False otherwise.
    """
    has_constraint = "constraint" in questions.columns
    constraints_fields = (
        questions.filter(pl.col("constraint").is_not_null())["name"].to_list()
        if has_constraint
        else []
    )
    choice_map = _build_choice_map(questions, choices)

    is_valid = True
    for col, value in record.items():
        if col in constraints_fields:
            constraints = questions.filter(pl.col("name") == col)["constraint"][0]
            if constraints is not None:
                is_valid = is_valid and _validate_value(value, constraints)

        if col in choice_map:
            allowed, multiple = choice_map[col]
            is_valid = is_valid and _value_in_choices(value, allowed, multiple)
    return is_valid


# A single XLSForm comparison constraint, e.g. ". <= 100", ".>=0", ". = 'yes'".
# Operators may be surrounded by any amount of whitespace, which the previous
# implementation (matching ".<=" with no spaces) silently ignored.
_COMPARISON_RE = re.compile(r"^\.\s*(<=|>=|!=|==|<|>|=)\s*(.+)$")
_REGEX_RE = re.compile(r"regex\(\s*\.\s*,\s*['\"](.+)['\"]\s*\)")


def _validate_value(value: object, constraints: str) -> bool:
    """Validate a single value against one XLSForm constraint expression.

    Supports ``regex(., 'pattern')`` and single comparison constraints with the
    current-value dot reference (``.``), tolerant of surrounding whitespace.
    Unsupported/compound expressions are treated as passing rather than blocking.

    Args:
        value (object): The submitted value.
        constraints (str): The XLSForm constraint expression.

    Returns:
        bool: Whether the value satisfies the constraint.
    """
    if value is None or constraints is None:
        return True
    constraint = str(constraints).strip()

    regex_match = _REGEX_RE.search(constraint)
    if regex_match:
        try:
            return bool(re.search(regex_match.group(1), str(value)))
        except (re.error, ValueError, TypeError):
            return False

    comparison = _COMPARISON_RE.match(constraint)
    if comparison:
        op = comparison.group(1)
        threshold = comparison.group(2).strip().strip("'\"")
        try:
            left, right = float(value), float(threshold)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            if op in ("=", "=="):
                return str(value) == threshold
            if op == "!=":
                return str(value) != threshold
            return True  # non-numeric value with an ordering operator: don't block
        comparisons = {
            "<=": left <= right,
            ">=": left >= right,
            "<": left < right,
            ">": left > right,
            "=": left == right,
            "==": left == right,
            "!=": left != right,
        }
        return comparisons[op]

    # Compound or unsupported expressions are not evaluated; do not block the row.
    return True


def _validate_column_types(
    df: pl.DataFrame, type_requirements: dict, import_strategy: str
) -> dict[str, tuple[str, str]]:
    """Validates column types with flexibility."""  # noqa: DOC201
    invalid_types = {}
    schema = df.schema

    if import_strategy == "DELETE":
        type_requirements = {"id": pl.Utf8}

    for column, expected_type in type_requirements.items():
        if column not in schema:
            continue

        actual_type = schema[column]

        # Handle cases where expected_type is a tuple of types
        if isinstance(expected_type, tuple):
            if actual_type not in expected_type:
                expected_str = " or ".join(str(t) for t in expected_type)
                invalid_types[column] = (expected_str, str(actual_type))
        else:
            if actual_type != expected_type:
                invalid_types[column] = (str(expected_type), str(actual_type))

    return invalid_types
