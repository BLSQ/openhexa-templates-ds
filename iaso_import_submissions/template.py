import xml.etree.ElementTree as ET

import polars as pl
from jinja2 import Environment
from utils import (
    GROUP_END_TYPES,
    GROUP_START_TYPES,
    NON_DATA_TYPES,
    REPEAT_END_TYPES,
    REPEAT_START_TYPES,
    local_name_xml_tag,
    normalize_xml_value,
)

# Columns that are submission metadata handled outside of the form body.
RESERVED_COLUMNS = {
    "org_unit_id",
    "id",
    "instanceID",
    "form_version",
    "created_at",
    "latitude",
    "longitude",
    "altitude",
    "accuracy",
    "uuid",
}

# Autoescaping environment so that submitted values containing `&`, `<`, `>`, `"`
# or `'` produce well-formed XML instead of breaking the document.
_JINJA_ENV = Environment(autoescape=True)


def _build_body_lines(df: pl.DataFrame, questions: pl.DataFrame, indent: str) -> list[str]:
    """Build the indented body of the instance XML, mirroring the form tree.

    The survey sheet is walked in order while a stack tracks the currently open
    groups/repeats, so nested groups and repeat sections are reflected in the
    generated XML instead of being flattened into a single group.

    Args:
        df (pl.DataFrame): Submissions data (only its columns are used here).
        questions (pl.DataFrame): Survey-sheet metadata (ordered).
        indent (str): One indentation unit.

    Returns:
        list[str]: The body lines (without the root element or the meta block).
    """
    included = set(df.columns)

    # Fallback: no usable survey metadata -> flat body of non-reserved columns.
    if questions.is_empty() or "type" not in questions.columns or "name" not in questions.columns:
        return [
            f"{indent}<{col}>{{{{ {col} }}}}</{col}>"
            for col in df.columns
            if col not in RESERVED_COLUMNS
        ]

    lines: list[str] = []
    stack: list[str] = []  # names of currently open groups/repeats

    for row in questions.select(["type", "name"]).iter_rows(named=True):
        qtype = (row["type"] or "").strip()
        name = row["name"]

        if qtype in GROUP_START_TYPES or qtype in REPEAT_START_TYPES:
            if not name:
                continue
            lines.append(f"{indent * (len(stack) + 1)}<{name}>")
            stack.append(name)
        elif qtype in GROUP_END_TYPES or qtype in REPEAT_END_TYPES:
            if stack:
                name = stack.pop()
                lines.append(f"{indent * (len(stack) + 1)}</{name}>")
        elif name and name in included and qtype not in NON_DATA_TYPES:
            depth = len(stack) + 1
            lines.append(f"{indent * depth}<{name}>{{{{ {name} }}}}</{name}>")

    # Close any groups left open by a malformed survey sheet.
    while stack:
        name = stack.pop()
        lines.append(f"{indent * (len(stack) + 1)}</{name}>")

    return lines


def generate_xml_template(
    df: pl.DataFrame,
    questions: pl.DataFrame,
    id_form: str,
    form_version: str,
    root_tag: str = "data",
) -> str:
    """Generate an OpenRosa XML template mirroring the form's structure.

    Unlike a flat, single-group template, this reconstructs nested groups and
    repeat sections from the survey sheet, only emitting fields that are present
    both in the submissions file and in the form definition.

    Args:
        df (pl.DataFrame): DataFrame containing the submissions data.
        questions (pl.DataFrame): DataFrame containing the form questions metadata.
        id_form (str): The ID form on IASO.
        form_version (str): The form version.
        root_tag (str): The instance root element name (default ``data``).

    Returns:
        str: The generated XML template as a Jinja string.
    """
    indent = "    "
    body_lines = _build_body_lines(df, questions, indent)

    template_parts = [
        f'<{root_tag} xmlns:jr="http://openrosa.org/javarosa" '
        'xmlns:orx="http://openrosa.org/xforms"',
        f'      id="{id_form}" version="{form_version}">',
        *body_lines,
        f"{indent}<meta>",
        f"{indent * 2}<instanceID>uuid:{{{{ uuid }}}}</instanceID>",
        f"{indent}</meta>",
        f"</{root_tag}>",
    ]

    return "\n".join(template_parts)


def render_submission_xml(xml_template: str, data: dict) -> str:
    """Render a submission XML template with XML-safe value substitution.

    Values are normalized (booleans, dates, None) and auto-escaped so free-text
    answers cannot produce malformed XML.

    Args:
        xml_template (str): The Jinja XML template produced by
            :func:`generate_xml_template`.
        data (dict): The record values plus the generated ``uuid``.

    Returns:
        str: The rendered, well-formed XML document body.
    """
    template = _JINJA_ENV.from_string(xml_template)
    return template.render(**{k: normalize_xml_value(v) for k, v in data.items()})


def enrich_submission_xml(
    xml_str: str,
    iaso_instance: int | None = None,
    edit_user_id: int | None = None,
) -> bytes:
    """Enrich an IASO submission XML string with instance/user metadata.

    Args:
    xml_str (str): Original XML template content.
    iaso_instance (int | None): IASO instance numeric ID to embed (added as attribute).
    edit_user_id (int | None): User ID performing edit; added/updated under <meta><editUserID>.

    Returns:
    bytes: Updated XML including namespace safeguards and optional metadata elements.
    """
    root = ET.fromstring(bytes(xml_str, encoding="utf-8"))

    if iaso_instance is not None:
        root.set("iasoInstance", str(iaso_instance))

    meta = None
    for elem in root.iter():
        if local_name_xml_tag(elem.tag).lower() == "meta":
            meta = elem
            break

    if meta is None and edit_user_id is not None:
        meta = ET.SubElement(root, "meta")

    if edit_user_id is not None and meta is not None:
        found = None
        for ch in list(meta):
            if local_name_xml_tag(ch.tag).lower() == "edituserid":
                found = ch
                break
        if found is None:
            found = ET.SubElement(meta, "editUserID")
        found.text = str(edit_user_id)

    out = ET.tostring(root, encoding="utf-8")

    # ElementTree may drop unused namespace declarations on serialization.
    # Ensure the standard OpenRosa namespaces are present on the <data> root.
    missing = []
    if b"xmlns:jr=" not in out:
        missing.append(b'xmlns:jr="http://openrosa.org/javarosa"')
    if b"xmlns:orx=" not in out:
        missing.append(b'xmlns:orx="http://openrosa.org/xforms"')

    if missing:
        insertion = b" " + b" ".join(missing)
        out = out.replace(b"<data", b"<data" + insertion, 1)

    # Prepend XML declaration if it's missing
    if not out.lstrip().startswith(b"<?xml"):
        out = b'<?xml version="1.0" encoding="utf-8"?>\n' + out

    return out
