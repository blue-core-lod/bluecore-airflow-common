import base64
import hashlib
import importlib.util
import pathlib
import shutil
from html.parser import HTMLParser

import httpx
import pytest
import respx
from airflow.configuration import conf

import bluecore_airflow_common as common

PROJECT_DIR = pathlib.Path(__file__).parents[1]
INCLUDE = "bluecore/_pyscript.html"


def _load_sri_script():
    spec = importlib.util.spec_from_file_location(
        "pyscript_sri", PROJECT_DIR / "scripts" / "pyscript_sri.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pyscript_sri = _load_sri_script()


class _PyScriptTags(HTMLParser):
    """Collects every <script> and <link> that loads from pyscript.net"""

    def __init__(self) -> None:
        super().__init__()
        self.tags: list[dict[str, str | None]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        url = attributes.get("src") or attributes.get("href") or ""
        if tag in ("script", "link") and "pyscript.net" in url:
            self.tags.append({"tag": tag, **attributes})

    handle_startendtag = handle_starttag


def pyscript_tags(html: str) -> list[dict[str, str | None]]:
    parser = _PyScriptTags()
    parser.feed(html)
    return parser.tags


def unprotected(html: str) -> list[dict[str, str | None]]:
    """The pyscript.net tags without a sha384 hash or crossorigin="anonymous" """
    return [
        tag
        for tag in pyscript_tags(html)
        if not (tag.get("integrity") or "").startswith("sha384-")
        or tag.get("crossorigin") != "anonymous"
    ]


def sha384(content: bytes) -> str:
    return "sha384-" + base64.b64encode(hashlib.sha384(content).digest()).decode()


@pytest.fixture(autouse=True)
def airflow_at_root(monkeypatch):
    monkeypatch.setattr(conf, "get", lambda *args, **kwargs: "")


@pytest.fixture
def rendered() -> str:
    return common.templates().get_template(INCLUDE).render()


@pytest.fixture
def template_copy(tmp_path, monkeypatch) -> pathlib.Path:
    """A copy of the include for the SRI script to rewrite"""
    copy = tmp_path / "_pyscript.html"
    shutil.copy(pyscript_sri.TEMPLATE, copy)
    monkeypatch.setattr(pyscript_sri, "TEMPLATE", copy)
    return copy


def test_include_loads_core_css_and_core_js(rendered):
    """The include renders a stylesheet link for core.css and a module script
    for core.js, from the same pinned pyscript.net release."""
    tags = {tag["tag"]: tag for tag in pyscript_tags(rendered)}

    assert set(tags) == {"link", "script"}
    css, js = tags["link"]["href"], tags["script"]["src"]
    assert css is not None and css.endswith("/core.css")
    assert js is not None and js.endswith("/core.js")
    assert css.removesuffix("core.css") == js.removesuffix("core.js")
    assert tags["link"]["rel"] == "stylesheet"
    assert tags["script"]["type"] == "module"


def test_every_pyscript_tag_has_sri_and_crossorigin(rendered):
    """Every pyscript.net tag carries a sha384 integrity hash and
    crossorigin="anonymous", without which the browser skips the SRI check."""
    assert pyscript_tags(rendered)
    assert unprotected(rendered) == []


def test_check_catches_a_tag_without_sri():
    """The check this module relies on reports a pyscript.net tag that is
    missing its hash or crossorigin, so the test above can actually fail."""
    html = (
        '<script type="module" src="https://pyscript.net/releases/1/core.js"></script>'
        '<link rel="stylesheet" href="https://pyscript.net/releases/1/core.css"'
        ' integrity="sha384-abc" />'
        '<script src="/static/bluecore/bluecore.js"></script>'
    )

    assert [tag["tag"] for tag in unprotected(html)] == ["script", "link"]


def test_plugin_page_includes_pyscript_in_its_head(tmp_path):
    """A plugin page that includes the partial in the base template's head
    block gets the pinned tags inside <head>, with no extra context."""
    (tmp_path / "page.html").write_text(
        '{% extends "bluecore/base.html" %}'
        '{% block head %}{% include "bluecore/_pyscript.html" %}{% endblock %}'
        "{% block content %}<h1>Graph Toolbox</h1>{% endblock %}"
    )

    html = (
        common.templates(tmp_path)
        .get_template("page.html")
        .render(common.context("/graph-toolbox", title="Graph Toolbox"))
    )

    head = html.split("</head>")[0]
    assert len(pyscript_tags(head)) == 2
    assert unprotected(html) == []


def test_sri_script_rewrites_only_the_hashes(template_copy):
    """Re-pinning the current release with the files pyscript.net serves for
    it leaves the template unchanged, apart from its hashes."""
    original = template_copy.read_text()
    version = original.split('pyscript_version = "')[1].split('"')[0]

    with respx.mock(assert_all_called=True) as mock:
        mock.get(f"{pyscript_sri.RELEASES}/{version}/core.css").respond(content=b"css")
        mock.get(f"{pyscript_sri.RELEASES}/{version}/core.js").respond(content=b"js")
        pyscript_sri.main(version)

    updated = template_copy.read_text()
    assert f'pyscript_core_css_sha384 = "{sha384(b"css")}"' in updated
    assert f'pyscript_core_js_sha384 = "{sha384(b"js")}"' in updated
    assert updated.count("\n") == original.count("\n")


def test_sri_script_pins_a_new_release(template_copy):
    """Pinning another release rewrites the version and both hashes, and the
    rendered include then loads that release with those hashes."""
    with respx.mock(assert_all_called=True) as mock:
        mock.get(f"{pyscript_sri.RELEASES}/2099.1.1/core.css").respond(content=b"a")
        mock.get(f"{pyscript_sri.RELEASES}/2099.1.1/core.js").respond(content=b"b")
        pyscript_sri.main("2099.1.1")

    html = (
        common.templates(template_copy.parent).get_template("_pyscript.html").render()
    )
    tags = {tag["tag"]: tag for tag in pyscript_tags(html)}
    assert tags["link"]["href"] == "https://pyscript.net/releases/2099.1.1/core.css"
    assert tags["link"]["integrity"] == sha384(b"a")
    assert tags["script"]["src"] == "https://pyscript.net/releases/2099.1.1/core.js"
    assert tags["script"]["integrity"] == sha384(b"b")


@pytest.mark.parametrize("version", ["../etc", "latest", "2026.7", ""])
def test_sri_script_rejects_a_version_that_is_not_a_release(template_copy, version):
    """A version that isn't YYYY.M.D is refused before anything is fetched or
    written, so it can't build an arbitrary pyscript.net URL."""
    original = template_copy.read_text()

    with respx.mock(assert_all_mocked=True), pytest.raises(SystemExit):
        pyscript_sri.main(version)

    assert template_copy.read_text() == original


@pytest.mark.parametrize("status", [301, 404])
def test_sri_script_refuses_to_hash_a_redirect_or_error(template_copy, status):
    """A redirect or missing release stops the script instead of pinning the
    hash of an error page, and the template is left as it was."""
    original = template_copy.read_text()

    with respx.mock() as mock:
        mock.get(url__startswith=pyscript_sri.RELEASES).respond(
            status, headers={"Location": "https://example.test/"}
        )
        with pytest.raises(httpx.HTTPStatusError):
            pyscript_sri.main("2099.1.1")

    assert template_copy.read_text() == original


def test_pin_requires_exactly_one_setting():
    """pin() fails loudly when the {% set %} line it rewrites is missing or
    duplicated, rather than leaving an old hash in place."""
    line = '{% set pyscript_version = "1" %}\n'

    assert pyscript_sri.pin(line, "pyscript_version", "2") == line.replace("1", "2")
    with pytest.raises(SystemExit):
        pyscript_sri.pin("", "pyscript_version", "2")
    with pytest.raises(SystemExit):
        pyscript_sri.pin(line * 2, "pyscript_version", "2")
