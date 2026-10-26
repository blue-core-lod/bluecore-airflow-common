import pytest
from airflow.configuration import conf

import bluecore_airflow_common as common


@pytest.mark.parametrize(
    "base_url,expected",
    [
        ("http://localhost:8080/", ""),
        # the bluecore-stack proxies Airflow under /workflows
        ("http://localhost/workflows", "/workflows"),
        ("http://localhost/workflows/", "/workflows"),
        ("https://dev.bcld.info/workflows/", "/workflows"),
        ("/", ""),
        ("", ""),
    ],
)
def test_airflow_root_path(monkeypatch, base_url, expected):
    monkeypatch.setattr(conf, "get", lambda *args, **kwargs: base_url)

    assert common.airflow_root_path() == expected


def test_static_url_includes_the_plugin_prefix(monkeypatch):
    monkeypatch.setattr(
        conf, "get", lambda *args, **kwargs: "http://localhost/workflows"
    )

    assert (
        common.static_url("/bulk-operations", "logo.svg")
        == "/workflows/bulk-operations/static/logo.svg"
    )


def test_static_url_addresses_common_files_under_bluecore(monkeypatch):
    monkeypatch.setattr(conf, "get", lambda *args, **kwargs: "")

    assert (
        common.static_url("/bulk-operations", "bluecore/bluecore.css")
        == "/bulk-operations/static/bluecore/bluecore.css"
    )


def test_context_gives_templates_a_static_url_helper():
    context = common.context("/bulk-operations", title="Some Title")

    assert context["title"] == "Some Title"
    assert context["static_url"]("bluecore/bluecore.css").endswith(
        "/bulk-operations/static/bluecore/bluecore.css"
    )
