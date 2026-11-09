import os
import subprocess
import sys
from typing import Annotated, Any

import pydantic
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from bluecore_airflow_common.integration import settings

REQUIRED = {
    "BLUECORE_API_INTERNAL_URL": "http://bc_api:8100/",
    "BLUECORE_URL": "http://localhost/",
    "AIRFLOW_API_INTERNAL_URL": "http://airflow-apiserver:8080/workflows",
}


def run_python(code: str) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if k not in settings.VARIABLES}
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture(autouse=True)
def fresh_settings(monkeypatch):
    for name in settings.VARIABLES:
        monkeypatch.delenv(name, raising=False)
    settings.get_settings.cache_clear()
    yield
    settings.get_settings.cache_clear()


@pytest.fixture
def configured(monkeypatch):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)


def test_top_level_package_does_not_import_integration():
    result = run_python(
        "import sys, bluecore_airflow_common; "
        "assert 'bluecore_airflow_common.integration' not in sys.modules"
    )

    assert result.returncode == 0, result.stderr


def test_import_logs_missing_config_without_raising():
    result = run_python("import bluecore_airflow_common.integration.settings")

    assert result.returncode == 0, result.stderr
    output = result.stdout + result.stderr
    assert "Blue Core integration calls will fail" in output
    assert "BLUECORE_API_INTERNAL_URL" in output
    assert "BLUECORE_URL" in output


def test_every_field_is_read_from_a_variable():
    aliases = [
        field.validation_alias
        for field in settings.BlueCoreSettings.model_fields.values()
    ]

    assert None not in aliases
    assert settings.VARIABLES == [
        "BLUECORE_API_INTERNAL_URL",
        "BLUECORE_URL",
        "BLUECORE_REQUEST_TIMEOUT",
        "BLUECORE_MAX_RETRIES",
        "AIRFLOW_API_INTERNAL_URL",
    ]


def test_missing_config_raises_on_first_use():
    with pytest.raises(settings.SettingsError, match="BLUECORE_API_INTERNAL_URL"):
        settings.get_settings()


@pytest.mark.usefixtures("configured")
def test_loads_valid_config(monkeypatch):
    monkeypatch.setenv("BLUECORE_REQUEST_TIMEOUT", "2.5")
    monkeypatch.setenv("BLUECORE_MAX_RETRIES", "3")

    loaded = settings.get_settings()

    assert loaded.internal_url == "http://bc_api:8100"
    assert loaded.public_url == "http://localhost"
    assert loaded.request_timeout == 2.5
    assert loaded.max_retries == 3
    assert loaded.airflow_internal_url == "http://airflow-apiserver:8080/workflows"


@pytest.mark.usefixtures("configured")
@pytest.mark.parametrize("value", [None, ""])
def test_unset_or_empty_tunables_use_defaults(monkeypatch, value):
    if value is not None:
        monkeypatch.setenv("BLUECORE_REQUEST_TIMEOUT", value)
        monkeypatch.setenv("BLUECORE_MAX_RETRIES", value)

    loaded = settings.get_settings()

    assert loaded.request_timeout == 10.0
    assert loaded.max_retries == 2


@pytest.mark.usefixtures("configured")
@pytest.mark.parametrize(
    "name,value",
    [
        ("BLUECORE_API_INTERNAL_URL", "bc_api:8100"),
        ("BLUECORE_URL", "ftp://localhost"),
        ("BLUECORE_REQUEST_TIMEOUT", "0"),
        ("BLUECORE_REQUEST_TIMEOUT", "soon"),
        ("BLUECORE_MAX_RETRIES", "6"),
        ("BLUECORE_MAX_RETRIES", "-1"),
        ("AIRFLOW_API_INTERNAL_URL", "airflow-apiserver:8080"),
    ],
)
def test_rejects_invalid_config(monkeypatch, name, value):
    monkeypatch.setenv(name, value)

    with pytest.raises(settings.SettingsError, match=name):
        settings.get_settings()


@pytest.mark.parametrize("value", [None, ""])
def test_requires_airflow_url(monkeypatch, value):
    """
    AIRFLOW_API_INTERNAL_URL is required: unset or empty, loading settings
    raises SettingsError naming it.
    """
    for name, required in REQUIRED.items():
        monkeypatch.setenv(name, required)
    if value is None:
        monkeypatch.delenv("AIRFLOW_API_INTERNAL_URL")
    else:
        monkeypatch.setenv("AIRFLOW_API_INTERNAL_URL", value)

    with pytest.raises(settings.SettingsError, match="AIRFLOW_API_INTERNAL_URL"):
        settings.get_settings()


def test_failed_load_is_retried(monkeypatch):
    with pytest.raises(settings.SettingsError):
        settings.get_settings()

    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)

    assert settings.get_settings().internal_url == "http://bc_api:8100"


@pytest.mark.usefixtures("configured")
def test_settings_are_frozen():
    loaded = settings.get_settings()

    with pytest.raises(pydantic.ValidationError):
        loaded.internal_url = "http://elsewhere"  # ty: ignore[invalid-assignment]


def test_errors_never_echo_other_variables(monkeypatch):
    monkeypatch.setenv("KEYCLOAK_ADMIN_PASSWORD", "not-for-logs")

    with pytest.raises(settings.SettingsError) as error:
        settings.get_settings()

    assert "not-for-logs" not in str(error.value)


@pytest.mark.usefixtures("configured")
def test_public_config_is_an_allowlist():
    public = settings.public_config(settings.get_settings())

    assert public == {"public_url": "http://localhost"}


def test_public_config_as_a_dependency():
    app = FastAPI()

    @app.get("/config")
    def config(
        public: Annotated[dict[str, Any], Depends(settings.public_config)],
    ) -> dict[str, Any]:
        return public

    app.dependency_overrides[settings.get_settings] = lambda: settings.BlueCoreSettings(
        internal_url="http://bc_api:8100",
        public_url="https://bcld.info",
        airflow_internal_url="http://airflow-apiserver:8080/workflows",
    )

    body = TestClient(app).get("/config").json()

    assert body == {"public_url": "https://bcld.info"}
    assert "bc_api" not in str(body)
