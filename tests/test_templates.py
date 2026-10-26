import pytest
from airflow.configuration import conf

import bluecore_airflow_common as common


@pytest.fixture(autouse=True)
def airflow_at_root(monkeypatch):
    monkeypatch.setattr(conf, "get", lambda *args, **kwargs: "")


def test_common_base_is_found_without_a_plugin_directory():
    template = common.templates().get_template("bluecore/base.html")

    assert template.filename.endswith("templates/bluecore/base.html")


def test_plugin_templates_are_searched_first(tmp_path):
    (tmp_path / "bluecore").mkdir()
    (tmp_path / "bluecore" / "base.html").write_text("plugin copy")

    template = common.templates(tmp_path).get_template("bluecore/base.html")

    assert template.render() == "plugin copy"


def test_plugin_page_extends_the_common_base(tmp_path):
    (tmp_path / "page.html").write_text(
        '{% extends "bluecore/base.html" %}'
        "{% block body_class %}centered{% endblock %}"
        "{% block content %}<h1>Bulk Operations</h1>{% endblock %}"
    )

    html = (
        common.templates(tmp_path)
        .get_template("page.html")
        .render(common.context("/bulk-operations", title="Some Title"))
    )

    assert "<title>Some Title</title>" in html
    assert 'href="/bulk-operations/static/bluecore/bluecore.css"' in html
    assert 'src="/bulk-operations/static/bluecore/bluecore.js"' in html
    assert '<body class="centered">' in html
    assert "<h1>Bulk Operations</h1>" in html


def test_templates_autoescape_context_values():
    html = (
        common.templates()
        .get_template("bluecore/base.html")
        .render(common.context("/bulk-operations", title="<script>alert(1)</script>"))
    )

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
