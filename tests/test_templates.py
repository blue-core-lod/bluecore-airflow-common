import re

import pytest
from airflow.configuration import conf

import bluecore_airflow_common as common


@pytest.fixture(autouse=True)
def airflow_at_root(monkeypatch):
    monkeypatch.setattr(conf, "get", lambda *args, **kwargs: "")


def test_common_base_is_found_without_a_plugin_directory():
    template = common.templates().get_template("bluecore/base.html")

    assert template.filename is not None
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


def _render(**extra):
    return (
        common.templates()
        .get_template("bluecore/base.html")
        .render(common.context("/graph-toolbox", title="Toolbox", **extra))
    )


def test_base_loads_the_stacks_header_footer_script():
    html = _render()

    assert '<script src="/assets/bluecore/bluecore-header-footer.js"></script>' in html
    # in <head>, so the elements render as the page is parsed
    assert html.index("bluecore-header-footer.js") < html.index("</head>")


def test_base_puts_the_content_between_the_header_and_footer(tmp_path):
    (tmp_path / "page.html").write_text(
        '{% extends "bluecore/base.html" %}{% block content %}<h1>Dash</h1>{% endblock %}'
    )

    html = (
        common.templates(tmp_path)
        .get_template("page.html")
        .render(common.context("/graph-toolbox", title="Toolbox"))
    )

    header = html.index("<bluecore-header")
    main = html.index('<main class="bluecore-main"><h1>Dash</h1></main>')
    footer = html.index("<bluecore-footer></bluecore-footer>")
    assert header < main < footer


def test_header_gets_the_app_name_logo_and_current_app():
    html = _render(
        app_name="TOOLBOX",
        app_logo="/graph-toolbox/static/toolbox.svg",
        current_app="toolbox",
    )

    assert 'app-name="TOOLBOX"' in html
    assert 'app-logo="/graph-toolbox/static/toolbox.svg"' in html
    assert 'current-app="toolbox"' in html


def test_header_does_not_show_who_is_signed_in():
    # the design has no "Signed in as": a username from a plugin is left out
    html = _render(username="cataloger1")

    assert "username=" not in html
    assert "cataloger1" not in html


def test_header_leaves_out_unset_options():
    html = _render()

    assert 'app-name=""' in html
    assert "app-logo=" not in html
    assert "current-app=" not in html
    assert "username=" not in html


def test_header_logs_out_through_airflow():
    html = _render()

    assert re.search(r"<bluecore-header[^>]*\slogout\s", html)
    assert 'data-logout-url="/api/v2/auth/logout"' in html


def test_logout_url_follows_the_airflow_base_path(monkeypatch):
    monkeypatch.setattr(
        conf, "get", lambda *args, **kwargs: "https://bcld.info/workflows/"
    )

    assert 'data-logout-url="/workflows/api/v2/auth/logout"' in _render()


def test_header_values_are_escaped():
    html = _render(app_name='"><script>alert(1)</script>')

    assert "<script>alert(1)</script>" not in html
    assert 'app-name="&#34;&gt;&lt;script&gt;' in html


def test_a_page_can_replace_the_header_and_footer(tmp_path):
    (tmp_path / "page.html").write_text(
        '{% extends "bluecore/base.html" %}'
        "{% block header %}{% endblock %}{% block footer %}{% endblock %}"
    )

    html = (
        common.templates(tmp_path)
        .get_template("page.html")
        .render(common.context("/graph-toolbox", title="Toolbox"))
    )

    assert "<bluecore-header" not in html
    assert "<bluecore-footer" not in html
