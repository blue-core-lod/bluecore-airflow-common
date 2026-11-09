import urllib3
from airflow_client.client import ApiClient

from bluecore_airflow_common.integration import airflow
from bluecore_airflow_common.integration.settings import BlueCoreSettings

ROOT = "http://airflow-apiserver:8080/workflows"
USER_AGENT = "graph-toolbox/0.4.0"


def make_client() -> ApiClient:
    settings = BlueCoreSettings(
        internal_url="http://bc_api:8100",
        public_url="https://bcld.info",
        airflow_internal_url=ROOT,
        max_retries=3,
    )
    return airflow.make_client(settings, token="user-token", user_agent=USER_AGENT)


def test_client_is_configured_from_settings():
    """
    The factory returns an ApiClient for the Airflow root in settings, with
    the user's token and the plugin's User-Agent.
    """
    client = make_client()

    assert client.configuration.host == ROOT
    assert client.configuration.access_token == "user-token"
    assert client.default_headers["User-Agent"] == USER_AGENT


def test_retries_match_retry_transport():
    """
    Retries use max_retries from settings, only for GET and HEAD on 502, 503
    and 504, never follow a redirect, and raise the last status rather than
    urllib3's MaxRetryError.
    """
    retries = make_client().configuration.retries

    assert isinstance(retries, urllib3.Retry)
    assert retries.total == 3
    assert retries.allowed_methods == {"GET", "HEAD"}
    assert retries.status_forcelist == {502, 503, 504}
    assert retries.redirect == 0
    assert retries.raise_on_status is False
