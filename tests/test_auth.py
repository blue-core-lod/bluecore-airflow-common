from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

import bluecore_airflow_common as common


def plugin_app():
    app = FastAPI()

    @app.get("/", dependencies=[Depends(common.requires_login)])
    def index():
        return {"ok": True}

    return app


def test_pages_require_login():
    response = TestClient(plugin_app()).get("/")

    assert response.status_code == 401


def test_login_can_be_overridden_in_tests():
    app = plugin_app()
    app.dependency_overrides[common.requires_login] = lambda: None

    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert response.json() == {"ok": True}
