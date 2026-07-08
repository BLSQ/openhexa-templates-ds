"""Tests for XML template generation and rendering."""

import xml.etree.ElementTree as ET

import polars as pl
from template import enrich_submission_xml, generate_xml_template, render_submission_xml


def _questions(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={"type": pl.Utf8, "name": pl.Utf8})


def test_generate_template_nested_groups():
    """Nested groups/repeats are reflected in the XML tree, not flattened."""
    questions = _questions(
        [
            {"type": "begin group", "name": "grp"},
            {"type": "text", "name": "q1"},
            {"type": "begin repeat", "name": "rep"},
            {"type": "integer", "name": "q2"},
            {"type": "end repeat", "name": None},
            {"type": "end group", "name": None},
            {"type": "text", "name": "q3"},
        ]
    )
    df = pl.DataFrame({"q1": ["a"], "q2": [1], "q3": ["c"], "org_unit_id": [10]})

    template = generate_xml_template(df, questions, id_form="42", form_version="v1")

    # It must be valid XML once rendered.
    rendered = render_submission_xml(template, {"q1": "a", "q2": 1, "q3": "c", "uuid": "u"})
    root = ET.fromstring(rendered)

    assert root.tag == "data"
    assert root.attrib["id"] == "42"
    grp = root.find("grp")
    assert grp is not None
    assert grp.find("q1") is not None
    rep = grp.find("rep")
    assert rep is not None
    assert rep.find("q2") is not None
    # q3 lives outside the group, directly under the root.
    assert root.find("q3") is not None
    assert root.find("grp/q3") is None


def test_generate_template_excludes_non_form_and_note_fields():
    """Only questions present in the file and carrying data are emitted."""
    questions = _questions(
        [
            {"type": "note", "name": "note1"},
            {"type": "text", "name": "q1"},
        ]
    )
    df = pl.DataFrame({"q1": ["a"], "note1": ["ignored"], "org_unit_id": [1]})

    template = generate_xml_template(df, questions, id_form="1", form_version="v1")

    assert "<q1>" in template
    assert "note1" not in template
    assert "org_unit_id" not in template


def test_generate_template_fallback_without_metadata():
    """With no survey metadata, non-reserved columns are emitted flat."""
    questions = pl.DataFrame()
    df = pl.DataFrame({"q1": ["a"], "org_unit_id": [1], "latitude": [1.0]})

    template = generate_xml_template(df, questions, id_form="1", form_version="v1")

    assert "<q1>" in template
    assert "org_unit_id" not in template
    assert "latitude" not in template


def test_render_escapes_special_characters():
    """Free-text values with XML metacharacters produce well-formed XML."""
    questions = _questions([{"type": "text", "name": "comment"}])
    df = pl.DataFrame({"comment": ["x"], "org_unit_id": [1]})
    template = generate_xml_template(df, questions, id_form="1", form_version="v1")

    raw_value = 'Tom & Jerry <3> "quote"'
    rendered = render_submission_xml(template, {"comment": raw_value, "uuid": "abc"})

    # Must parse without error and preserve the literal text.
    root = ET.fromstring(rendered)
    comment = root.find("comment")
    assert comment is not None
    assert comment.text == raw_value


def test_render_none_becomes_empty():
    """None values render as empty elements."""
    questions = _questions([{"type": "text", "name": "q1"}])
    df = pl.DataFrame({"q1": ["a"], "org_unit_id": [1]})
    template = generate_xml_template(df, questions, id_form="1", form_version="v1")

    rendered = render_submission_xml(template, {"q1": None, "uuid": "abc"})
    root = ET.fromstring(rendered)
    q1 = root.find("q1")
    assert q1 is not None
    assert not q1.text


def test_enrich_submission_xml_adds_metadata_and_namespaces():
    """Enrichment injects instance id/edit user and keeps OpenRosa namespaces."""
    xml_str = (
        '<data xmlns:jr="http://openrosa.org/javarosa" '
        'xmlns:orx="http://openrosa.org/xforms" id="1" version="v1">'
        "<meta><instanceID>uuid:abc</instanceID></meta></data>"
    )
    out = enrich_submission_xml(xml_str, iaso_instance=99, edit_user_id=7)

    assert out.lstrip().startswith(b"<?xml")
    assert b'xmlns:jr="http://openrosa.org/javarosa"' in out
    assert b'xmlns:orx="http://openrosa.org/xforms"' in out

    root = ET.fromstring(out)
    assert root.attrib["iasoInstance"] == "99"
    edit_user = root.find("meta/editUserID")
    assert edit_user is not None
    assert edit_user.text == "7"
