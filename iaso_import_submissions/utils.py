import re
import unicodedata
from datetime import UTC, date, datetime

import polars as pl

# Precompile regex pattern for string cleaning
CLEAN_PATTERN = re.compile(r"[^\w\s-]")

# XLSForm structural markers (spaces or underscores are both accepted by pyxform).
GROUP_START_TYPES = {"begin group", "begin_group"}
REPEAT_START_TYPES = {"begin repeat", "begin_repeat"}
GROUP_END_TYPES = {"end group", "end_group"}
REPEAT_END_TYPES = {"end repeat", "end_repeat"}
STRUCTURAL_TYPES = GROUP_START_TYPES | REPEAT_START_TYPES | GROUP_END_TYPES | REPEAT_END_TYPES

# XLSForm question types that never carry a submitted value in the instance XML.
NON_DATA_TYPES = STRUCTURAL_TYPES | {"note"}

# Select question type prefixes (the choice list name is the token that follows).
# Longer, more specific prefixes must come first so that e.g.
# "select_one_from_file" is not shadowed by "select_one".
SELECT_PREFIXES = (
    "select_one_from_file",
    "select_multiple_from_file",
    "select_one",
    "select_multiple",
    "select one",
    "select multiple",
)


def clean_string(input_str: str) -> str:
    """Normalize and sanitize string for safe file/table names.

    Args:
        input_str: Original input string

    Returns:
        Normalized string with special characters removed
    """
    normalized = unicodedata.normalize("NFD", input_str)
    cleaned = "".join(c for c in normalized if not unicodedata.combining(c))
    sanitized = CLEAN_PATTERN.sub("", cleaned)
    return sanitized.strip().replace(" ", "_").lower()


def calculate_to_polars_expr(calc_str: str) -> str:
    """Convert ODK calculate expressions to Polars expressions.

    Args:
        calc_str: The ODK calculate expression string to convert.

    Returns:
        A string containing the equivalent Polars expression.
    """
    expr = calc_str.strip()

    if expr in ("0", "0.0"):
        return "pl.lit(0)"

    expr = re.sub(r"\$\{([^}]+)\}", r'pl.col("\1")', expr)

    expr = expr.replace(" div ", " / ")

    expr = re.sub(r"round\((.+?),\s*0\)", r"(\1).round()", expr)

    expr = re.sub(r"round\((.+?)\)", r"(\1).round()", expr)

    expr = re.sub(r"abs\((.+?)\)", r"(\1).abs()", expr)

    def repl_coalesce(match: re.Match) -> str:
        args = match.group(1)
        return f"pl.coalesce([{args}])"

    return re.sub(r"coalesce\((.+?)\)", repl_coalesce, expr)


def local_name_xml_tag(tag: str) -> str:
    """Extract local name from a potentially namespaced XML tag.

    Args:
        tag (str): The XML tag name, possibly including namespace.

    Returns:
        str: The local name without namespace.
    """
    return tag.split("}", 1)[-1] if "}" in tag else tag


def extract_list_name(question_type: str | None) -> str | None:
    """Extract the choice list name from an XLSForm select question type.

    XLSForm select questions are typed as ``select_one <list_name>`` or
    ``select_multiple <list_name>``. The list name is what links a question to
    the rows of the ``choices`` sheet, and it is generally different from the
    question ``name``.

    Args:
        question_type (str | None): The raw ``type`` value from the survey sheet.

    Returns:
        str | None: The referenced choice list name, or None if the type is not
        a select question or carries no list name.
    """
    if not question_type:
        return None
    stripped = question_type.strip()
    for prefix in SELECT_PREFIXES:
        if stripped.startswith(prefix):
            remainder = stripped[len(prefix) :].strip()
            # `select_one_from_file file.csv` -> keep the reference token only.
            return remainder.split()[0] if remainder else None
    return None


def is_select_multiple(question_type: str | None) -> bool:
    """Return whether an XLSForm question type is a multi-select.

    Args:
        question_type (str | None): The raw ``type`` value from the survey sheet.

    Returns:
        bool: True when the question stores several space-separated choice names.
    """
    if not question_type:
        return False
    stripped = question_type.strip()
    return stripped.startswith(("select_multiple", "select multiple"))


def get_choices_list_column(choices: pl.DataFrame) -> str | None:
    """Return the name of the list-name column in a choices DataFrame.

    Args:
        choices (pl.DataFrame): DataFrame parsed from the XLSForm ``choices`` sheet.

    Returns:
        str | None: ``"list_name"`` or ``"list name"`` if present, else None.
    """
    return next((c for c in ("list_name", "list name") if c in choices.columns), None)


def get_choices_name_column(choices: pl.DataFrame) -> str | None:
    """Return the name of the choice-value column in a choices DataFrame.

    The submitted value of a select question is the choice ``name`` (the stored
    code), never the human-readable ``label``. This helper locates that column.

    Args:
        choices (pl.DataFrame): DataFrame parsed from the XLSForm ``choices`` sheet.

    Returns:
        str | None: The name of the value column, or None if absent.
    """
    return next((c for c in ("name", "value") if c in choices.columns), None)


def get_label_columns(df: pl.DataFrame) -> list[str]:
    """Return the label columns of an XLSForm sheet, including multilingual ones.

    Single-language forms expose a bare ``label`` column, while multilingual
    forms expose one column per language such as ``label::English (en)``.

    Args:
        df (pl.DataFrame): DataFrame parsed from an XLSForm sheet.

    Returns:
        list[str]: All columns whose name is ``label`` or starts with ``label::``.
    """
    return [c for c in df.columns if c == "label" or c.startswith("label::")]


def normalize_xml_value(value: object) -> str:
    """Render a submission value as the string expected in OpenRosa XML.

    Booleans map to ``"1"``/``"0"``, dates/datetimes are ISO-formatted and
    ``None`` becomes an empty string. Everything else is stringified as-is.

    Args:
        value (object): The raw value coming from the submissions DataFrame.

    Returns:
        str: The XML text representation of the value.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def optional_float(record: dict, key: str) -> float | None:
    """Extract an optional float value from a record without coercing null to 0.

    Args:
        record (dict): The submission record.
        key (str): The column to read.

    Returns:
        float | None: The parsed float, or None when the column is absent, null
        or not parseable (so missing coordinates are never sent as ``0.0``).
    """
    if key not in record:
        return None
    value = record.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def to_epoch(value: object) -> int | None:
    """Convert a date/datetime/epoch-like value to a Unix timestamp (seconds).

    Args:
        value (object): A datetime, date, numeric epoch, or ISO date string.

    Returns:
        int | None: The corresponding Unix timestamp, or None if not parseable.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, date):
        return int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp())
    try:
        return int(datetime.fromisoformat(str(value)).timestamp())
    except (ValueError, TypeError):
        return None
