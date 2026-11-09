"""MARC and BIBFRAME conversion for plugins, with marc-bibframe

Plugins convert in their own process with the library the Blue Core API uses
rather than through the API's conversion routes. Conversion is CPU-bound, so
call these from a plain ``def`` route, which FastAPI runs in its threadpool,
never from an ``async def`` route::

    @app.post("/convert/marc2bibframe")
    def marc2bibframe(body: Annotated[bytes, Body()]) -> Response:
        graph = convert.marc2bibframe(
            body, max_bytes=1000 * 1024 * 1024, baseuri="https://bcld.info/"
        )
        return Response(
            graph.serialize(format="json-ld"), media_type="application/ld+json"
        )

Input over ``max_bytes`` raises ``InputTooLarge`` (413) before it is parsed,
and input that can't be converted raises ``InvalidInput`` (422).
"""

from typing import Any

import marc_bibframe
from rdflib import Graph

from bluecore_airflow_common.integration.errors import InputTooLarge, InvalidInput

MAX_BYTES = 2 * 1024**3  # 2 GB Max size

_UTF8_BOM = b"\xef\xbb\xbf"


def _check_size(data: bytes, max_bytes: int) -> None:
    if len(data) > max_bytes:
        raise InputTooLarge(f"input is {len(data)} bytes, the limit is {max_bytes}")


def marc2xml(marc: bytes, *, max_bytes: int = MAX_BYTES) -> bytes:
    """Binary MARC21 as a MARCXML ``<collection>``"""
    _check_size(marc, max_bytes)
    try:
        return marc_bibframe.marc_to_marcxml(marc)
    except Exception as exc:
        raise InvalidInput(f"could not read MARC: {exc}") from exc


def marc2bibframe(marc: bytes, *, max_bytes: int = MAX_BYTES, **options: Any) -> Graph:
    """MARCXML or binary MARC21 as a BIBFRAME graph

    ``options`` are passed to ``marc_bibframe.marcxml_to_graph``, such as
    ``baseuri``.
    """
    _check_size(marc, max_bytes)
    marc = marc.removeprefix(_UTF8_BOM)
    try:
        if not marc.lstrip().startswith(b"<"):
            marc = marc_bibframe.marc_to_marcxml(marc)
        return marc_bibframe.marcxml_to_graph(marc, **options)
    except Exception as exc:
        raise InvalidInput(f"could not convert MARC: {exc}") from exc


def bibframe2marc(rdf: bytes, *, max_bytes: int = MAX_BYTES, **options: Any) -> bytes:
    """BIBFRAME RDF as a MARCXML ``<collection>``, one record per ``bf:Instance``

    ``options`` are passed to ``marc_bibframe.bibframe_to_marcxml``. It looks up
    authorities on id.loc.gov by default, waiting up to 30 seconds for each;
    pass ``request_timeout=settings.request_timeout``, or
    ``authority_lookup=False`` to convert without the network.
    """
    _check_size(rdf, max_bytes)
    try:
        return marc_bibframe.bibframe_to_marcxml(rdf, **options)
    except Exception as exc:
        raise InvalidInput(f"could not convert BIBFRAME: {exc}") from exc
