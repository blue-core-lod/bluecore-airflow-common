import bluecore_client
import httpx
import pytest
import respx

from bluecore_airflow_common.integration import blue_core, retry
from bluecore_airflow_common.integration.settings import (
    BlueCoreSettings,
    SettingsError,
)

API = "http://bc_api:8100"
WORK = f"{API}/works/w1"
USER_AGENT = "graph-toolbox/0.4.0"

# Everything bluecore-client would otherwise read from the environment, set to
# values that must never be used.
ENVIRONMENT = {
    "API_URL": "http://wrong:9999",
    "BLUECORE_URL": "http://wrong:9999",
    "BLUECORE_TOKEN": "environment-token",
    "API_KEYCLOAK_USER": "service-account",
    "API_KEYCLOAK_PASSWORD": "service-password",
}


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """Start every test with none of bluecore-client's variables set"""
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def hostile_environment(monkeypatch):
    """Set every variable bluecore-client falls back to, with wrong values"""
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def settings() -> BlueCoreSettings:
    return BlueCoreSettings(
        internal_url=API,
        public_url="https://bcld.info",
        max_retries=1,
        airflow_internal_url="http://airflow-apiserver:8080/workflows",
    )


@pytest.fixture
def api():
    """Mock the network; any request without a route, such as Keycloak, fails"""
    with respx.mock(assert_all_called=False) as mock:
        yield mock


@pytest.fixture
def no_backoff(monkeypatch):
    """Make every retry wait zero seconds, so retry tests don't sleep"""
    monkeypatch.setattr(retry.random, "uniform", lambda low, high: 0.0)


def test_read_is_anonymous(settings, api, hostile_environment):
    """
    Without a token, a read sends no Authorization header and makes no
    Keycloak request, even with a service account and BLUECORE_TOKEN set.
    """
    route = api.get(WORK).respond(200, json={"@id": "w1"})

    with blue_core.make_client(settings, user_agent=USER_AGENT) as client:
        work = client.works.get("w1")

    assert work == {"@id": "w1"}
    assert "Authorization" not in route.calls.last.request.headers
    assert api.calls.call_count == 1


def test_empty_token_is_anonymous(settings, api, hostile_environment):
    """
    An empty token builds an anonymous client, rather than letting
    bluecore-client replace it with BLUECORE_TOKEN from the environment.
    """
    route = api.get(WORK).respond(200, json={})

    with blue_core.make_client(settings, user_agent=USER_AGENT, token="") as client:
        client.works.get("w1")

    assert "Authorization" not in route.calls.last.request.headers


def test_token_is_sent_as_bearer(settings, api):
    """The signed-in user's token is sent as the Bearer token"""
    route = api.get(WORK).respond(200, json={})

    with blue_core.make_client(
        settings, user_agent=USER_AGENT, token="user-token"
    ) as client:
        client.works.get("w1")

    assert route.calls.last.request.headers["Authorization"] == "Bearer user-token"


def test_token_refused_with_service_account_in_environment(
    settings, api, hostile_environment
):
    """
    With API_KEYCLOAK_USER and API_KEYCLOAK_PASSWORD set, a client for the
    user's token is refused before any request, because bluecore-client would
    answer a 401 by logging in as that account.
    """
    with pytest.raises(SettingsError):
        blue_core.make_client(settings, user_agent=USER_AGENT, token="user-token")

    assert api.calls.call_count == 0


def test_rejected_token_is_not_replaced(settings, api):
    """
    A 401 on the user's token raises AuthError after one request, with no
    Keycloak login and no second try.
    """
    route = api.get(WORK).respond(401, json={"detail": "expired"})

    with (
        blue_core.make_client(
            settings, user_agent=USER_AGENT, token="user-token"
        ) as client,
        pytest.raises(bluecore_client.AuthError),
    ):
        client.works.get("w1")

    assert route.call_count == 1
    assert api.calls.call_count == 1


def test_requests_go_to_internal_url(settings, api, hostile_environment):
    """
    The API URL comes from settings, not from API_URL or BLUECORE_URL. Only
    the internal URL is mocked, so a request anywhere else would fail.
    """
    route = api.get(WORK).respond(200, json={})

    with blue_core.make_client(settings, user_agent=USER_AGENT) as client:
        client.works.get("w1")

    assert route.called


def test_user_agent_names_the_plugin(settings, api):
    """Every request carries the calling plugin's User-Agent"""
    route = api.get(WORK).respond(200, json={})

    with blue_core.make_client(settings, user_agent=USER_AGENT) as client:
        client.works.get("w1")

    assert route.calls.last.request.headers["User-Agent"] == USER_AGENT


def test_get_timeout_is_retried(settings, api, no_backoff):
    """
    A GET that times out is retried by RetryTransport, and the retry's
    response is returned.
    """
    route = api.get(WORK).mock(
        side_effect=[httpx.ReadTimeout("slow"), httpx.Response(200, json={})]
    )

    with blue_core.make_client(settings, user_agent=USER_AGENT) as client:
        client.works.get("w1")

    assert route.call_count == 2


def test_post_timeout_is_not_retried(settings, api, no_backoff):
    """
    A POST that times out is sent exactly once, and surfaces as
    ConnectionFailed with the timeout as its cause.
    """
    route = api.post(f"{API}/works/").mock(side_effect=httpx.ReadTimeout("slow"))

    with (
        blue_core.make_client(
            settings, user_agent=USER_AGENT, token="user-token"
        ) as client,
        pytest.raises(bluecore_client.ConnectionFailed) as raised,
    ):
        client.works.create({"@id": "w1"})

    assert route.call_count == 1
    assert isinstance(raised.value.__cause__, httpx.ReadTimeout)
