import logging
from types import SimpleNamespace

import pytest
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.core_api.security import get_user
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bluecore_airflow_common.integration import auth
from bluecore_airflow_common.integration.errors import (
    ReauthRequired,
    install_error_handler,
)

# Shaped like a JWT (eyJ plus three dot-separated parts) but not a real one
FAKE_JWT = "eyJhbGciOi.eyJzdWIiOiJ4In0.c2ln"


class FakeUser(BaseUser):
    """An Airflow user with whatever attributes a test gives it"""

    def __init__(self, **attributes):
        self.__dict__.update(attributes)

    def get_id(self) -> str:
        return "user-1"

    def get_name(self) -> str:
        return "Test User"


def test_keycloak_token_is_the_users_access_token():
    """A Keycloak user's access token is returned as it is"""
    assert auth.keycloak_token(FakeUser(access_token="abc")) == "abc"


@pytest.mark.parametrize(
    "user",
    [None, FakeUser(), FakeUser(access_token=""), FakeUser(access_token=None)],
    ids=["no-user", "missing", "empty", "none"],
)
def test_keycloak_token_without_a_token_requires_reauth(user):
    """A user with no usable token raises ReauthRequired

    "no-user" is what get_user passes on Airflow 3.3 when the browser JWT
    arrives without the Keycloak cookies: the provider's get_user_from_token
    returns None. "missing" is a user from another auth manager, which has no
    access_token at all. "empty" and "none" guard against a provider that
    returns the user with its token cleared.
    """
    with pytest.raises(ReauthRequired):
        auth.keycloak_token(user)


@pytest.mark.parametrize(
    "user",
    [None, FakeUser(access_token=""), FakeUser(access_token=None)],
    ids=["no-user", "empty", "none"],
)
def test_airflow_token_without_a_session_requires_reauth(monkeypatch, user):
    """No user, or a Keycloak user with no token, raises before minting a JWT"""

    def generate(user, *, expiration_time_in_seconds):
        pytest.fail("a JWT was minted for a user without a session")

    manager = SimpleNamespace(generate_api_jwt=generate, generate_jwt=generate)
    monkeypatch.setattr(auth, "get_auth_manager", lambda: manager)

    with pytest.raises(ReauthRequired):
        auth.airflow_token(user)


def test_a_missing_user_gets_401_from_airflow_token():
    """Through a plugin route, a None user is a 401, not a 500"""
    app = FastAPI()
    install_error_handler(app)

    @app.get("/")
    def index(token: auth.AirflowToken):
        return {"token": token}

    app.dependency_overrides[get_user] = lambda: None

    response = TestClient(app).get("/")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "reauth_required"


def test_a_user_without_a_token_gets_401_reauth_required():
    """Through a plugin route, a missing token is a 401 with the shared body

    get_user is overridden rather than keycloak_token, so the real
    KeycloakToken dependency and the shared error handler both run.
    """
    app = FastAPI()
    install_error_handler(app)

    @app.get("/")
    def index(token: auth.KeycloakToken):
        return {"token": token}

    app.dependency_overrides[get_user] = lambda: FakeUser(access_token="")

    response = TestClient(app).get("/")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "reauth_required"


def test_airflow_token_prefers_generate_api_jwt(monkeypatch):
    """generate_api_jwt is used when the auth manager has it

    On Airflow 3.3 only that token carries the Keycloak tokens /api/v2 needs,
    so generate_jwt fails the test if it is called.
    """
    calls = []

    def generate_api_jwt(user, *, expiration_time_in_seconds):
        calls.append((user, expiration_time_in_seconds))
        return "api-jwt"

    def generate_jwt(user, *, expiration_time_in_seconds):
        pytest.fail("generate_jwt was used although generate_api_jwt exists")

    manager = SimpleNamespace(
        generate_api_jwt=generate_api_jwt, generate_jwt=generate_jwt
    )
    monkeypatch.setattr(auth, "get_auth_manager", lambda: manager)
    user = FakeUser()

    assert auth.airflow_token(user) == "api-jwt"
    assert calls == [(user, auth.AIRFLOW_TOKEN_SECONDS)]


def test_airflow_token_falls_back_to_generate_jwt(monkeypatch):
    """generate_jwt is used when the auth manager has no generate_api_jwt

    That's the simple auth manager, an older Keycloak provider, or Airflow
    3.2, where generate_jwt still includes the Keycloak tokens.
    """
    calls = []

    def generate_jwt(user, *, expiration_time_in_seconds):
        calls.append((user, expiration_time_in_seconds))
        return "jwt"

    manager = SimpleNamespace(generate_jwt=generate_jwt)
    monkeypatch.setattr(auth, "get_auth_manager", lambda: manager)
    user = FakeUser()

    assert auth.airflow_token(user) == "jwt"
    assert calls == [(user, auth.AIRFLOW_TOKEN_SECONDS)]


def test_airflow_tokens_are_short_lived():
    """Minted Airflow tokens last no longer than a Keycloak access token

    The realm's access tokens last 300 seconds. Raising the limit past that
    should be a deliberate change to this test.
    """
    assert auth.AIRFLOW_TOKEN_SECONDS <= 300


def make_record(msg, args=()):
    return logging.LogRecord("test", logging.INFO, "x.py", 1, msg, args, None)


@pytest.mark.parametrize(
    ("msg", "args", "expected"),
    [
        ("Authorization: Bearer abc.def-123", (), "Authorization: [REDACTED]"),
        ("Authorization: bearer abc", (), "Authorization: [REDACTED]"),
        (f"refresh={FAKE_JWT} done", (), "refresh=[REDACTED] done"),
        ("token %s", (FAKE_JWT,), "token [REDACTED]"),
        ("ran %d tasks", (3,), "ran 3 tasks"),
    ],
    ids=["bearer", "lowercase-bearer", "jwt-in-message", "jwt-in-args", "clean"],
)
def test_redact_tokens(msg, args, expected):
    """Bearer values and JWTs are redacted; the record is always kept

    "jwt-in-args" is the case that needs record.args cleared: without that,
    formatting the message again would put the token back.
    """
    record = make_record(msg, args)

    assert auth.RedactTokens().filter(record) is True
    assert record.getMessage() == expected


def test_redact_tokens_in_a_structlog_event(monkeypatch):
    """A structlog record is redacted and still formatted

    Airflow 3.3's root handler uses structlog's ProcessorFormatter, which
    needs record.msg to stay the event dict. Turning it into a string drops
    the line with a logging error.
    """
    import io

    import structlog

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer()
        )
    )
    handler.addFilter(auth.RedactTokens())
    logger = logging.getLogger("test_structlog_redaction")
    monkeypatch.setattr(logger, "handlers", [handler])
    monkeypatch.setattr(logger, "propagate", False)
    logger.setLevel(logging.INFO)
    log = structlog.wrap_logger(
        logger,
        processors=[structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    errors = []
    monkeypatch.setattr(handler, "handleError", errors.append)

    log.info("calling", headers={"Authorization": f"Bearer {FAKE_JWT}"}, n=[FAKE_JWT])

    assert errors == []
    output = stream.getvalue()
    assert FAKE_JWT not in output
    assert '"Authorization": "[REDACTED]"' in output
    assert '"event": "calling"' in output


def test_install_token_redaction_adds_one_filter_per_handler(monkeypatch):
    """Every root handler gets exactly one RedactTokens, however many calls

    Several plugins may each call it when they load, so a second call must not
    add a second filter.
    """
    handlers = [logging.StreamHandler(), logging.StreamHandler()]
    monkeypatch.setattr(logging.getLogger(), "handlers", handlers)

    auth.install_token_redaction()
    auth.install_token_redaction()

    for handler in handlers:
        assert [type(f) for f in handler.filters] == [auth.RedactTokens]
