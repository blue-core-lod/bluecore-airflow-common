import marc_bibframe
import pymarc
import pytest
from rdflib import Literal

from bluecore_airflow_common.integration import convert
from bluecore_airflow_common.integration.errors import InputTooLarge, InvalidInput

TITLE = "A test title"


def marc_record() -> bytes:
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="001", data="rec1"))
    record.add_field(
        pymarc.Field(
            tag="245",
            indicators=pymarc.Indicators("0", "0"),
            subfields=[pymarc.Subfield("a", TITLE)],
        )
    )
    return record.as_marc()


MARC = marc_record()
MARCXML = marc_bibframe.marc_to_marcxml(MARC)


def has_title(graph) -> bool:
    return any(
        str(value) == TITLE for value in graph.objects() if isinstance(value, Literal)
    )


def test_marc2xml_converts_binary_marc():
    """Binary MARC comes back as a MARCXML collection holding the record"""
    marcxml = convert.marc2xml(MARC)

    assert marcxml.lstrip().startswith(b"<?xml")
    assert TITLE.encode() in marcxml


@pytest.mark.parametrize(
    "marc",
    [MARC, MARCXML, b"\xef\xbb\xbf" + MARCXML],
    ids=["binary", "marcxml", "marcxml-with-bom"],
)
def test_marc2bibframe_accepts_marc_or_marcxml(marc):
    """
    marc2bibframe takes binary MARC or MARCXML, including MARCXML saved with a
    UTF-8 byte order mark, and returns a BIBFRAME graph with the record's title.
    """
    assert has_title(convert.marc2bibframe(marc))


def test_marc2bibframe_passes_options_to_marc_bibframe():
    """Options such as baseuri reach marc-bibframe, so minted URIs use it"""
    graph = convert.marc2bibframe(MARC, baseuri="https://bcld.info/")

    assert any(
        str(subject).startswith("https://bcld.info/rec1")
        for subject in graph.subjects()
    )


def test_bibframe2marc_converts_bibframe():
    """BIBFRAME RDF/XML comes back as MARCXML with the title"""
    rdf = marc_bibframe.marcxml_to_rdfxml(MARCXML)

    marcxml = convert.bibframe2marc(rdf, authority_lookup=False)

    assert TITLE.encode() in marcxml


@pytest.mark.parametrize(
    "function,name",
    [
        (convert.marc2xml, "marc_to_marcxml"),
        (convert.marc2bibframe, "marcxml_to_graph"),
        (convert.bibframe2marc, "bibframe_to_marcxml"),
    ],
)
def test_oversized_input_is_refused_before_converting(monkeypatch, function, name):
    """
    Input over max_bytes raises InputTooLarge, and marc-bibframe is never
    called with it.
    """

    def never_called(*args, **kwargs):
        raise AssertionError("marc-bibframe was called")

    monkeypatch.setattr(marc_bibframe, name, never_called)
    monkeypatch.setattr(marc_bibframe, "marc_to_marcxml", never_called)

    with pytest.raises(InputTooLarge):
        function(b"x" * 11, max_bytes=10)


def test_input_at_the_limit_is_converted():
    """Input exactly max_bytes long is allowed through"""
    assert TITLE.encode() in convert.marc2xml(MARC, max_bytes=len(MARC))


def test_default_limit_is_two_gibibytes():
    """With no max_bytes, the limit is 2 GiB"""
    assert convert.MAX_BYTES == 2 * 1024 * 1024 * 1024


@pytest.mark.parametrize(
    "function,data",
    [
        (convert.marc2xml, b"not marc"),
        (convert.marc2bibframe, b"not marc"),
        (convert.marc2bibframe, b"<record"),
        (convert.bibframe2marc, b"not rdf"),
    ],
    ids=["marc2xml", "marc2bibframe-binary", "marc2bibframe-xml", "bibframe2marc"],
)
def test_unconvertible_input_is_invalid_input(function, data):
    """
    Input marc-bibframe can't read raises InvalidInput (422), with the
    original exception kept as its cause for the logs.
    """
    with pytest.raises(InvalidInput) as raised:
        function(data)

    assert raised.value.__cause__ is not None
