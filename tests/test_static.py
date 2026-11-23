import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import bluecore_airflow_common as common


@pytest.mark.parametrize("filename", ["bluecore.css", "bluecore.js"])
def test_common_static_files_are_served(filename):
    app = FastAPI()
    common.mount_static(app)

    # No auth override: these are decorative assets served unauthenticated.
    response = TestClient(app).get(f"/static/bluecore/{filename}")

    assert response.status_code == 200
    assert response.content == (common.STATIC_DIR / filename).read_bytes()


def test_plugin_static_files_are_served(tmp_path):
    (tmp_path / "layers.svg").write_text("<svg/>")
    app = FastAPI()
    common.mount_static(app, tmp_path)

    response = TestClient(app).get("/static/layers.svg")

    assert response.status_code == 200
    assert response.text == "<svg/>"


def test_plugin_static_does_not_shadow_common_files(tmp_path):
    (tmp_path / "bluecore").mkdir()
    (tmp_path / "bluecore" / "bluecore.css").write_text("plugin copy")
    app = FastAPI()
    common.mount_static(app, tmp_path)

    response = TestClient(app).get("/static/bluecore/bluecore.css")

    assert response.status_code == 200
    assert response.text != "plugin copy"


def test_plugin_static_is_not_mounted_without_a_directory():
    app = FastAPI()
    common.mount_static(app)

    response = TestClient(app).get("/static/layers.svg")

    assert response.status_code == 404


def test_bluecore_js_sends_the_whole_window_to_logout():
    script = (common.STATIC_DIR / "bluecore.js").read_text()

    assert '"bluecore-logout"' in script
    assert 'getAttribute("data-logout-url")' in script
    # Airflow's iframe can't change the window, so it clicks a link on Airflow's page
    assert "window.top.location.assign(url)" not in script
    assert "window.top.document" in script
    assert 'a[target="_top"]' in script
