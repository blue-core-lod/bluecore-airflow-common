"""Blue Core API clients built from the deployment's settings

``make_client()`` returns a ``bluecore_client.BluecoreClient``, and plugins use
bluecore-client's own API on it::

    with make_client(settings, user_agent="graph-toolbox/0.4.0") as client:
        work = client.works.get(uuid)

Without a token the client is anonymous, which is all resource reads need. For
a write, pass the signed-in user's token from ``auth.KeycloakToken``. Make a
new client for each request, since each request carries a different token.
"""

import httpx
from bluecore_client import BluecoreClient

from bluecore_airflow_common.integration.retry import RetryTransport
from bluecore_airflow_common.integration.settings import (
    BlueCoreSettings,
    SettingsError,
)


def make_client(
    settings: BlueCoreSettings, *, user_agent: str, token: str | None = None
) -> BluecoreClient:
    """A Blue Core client for one request, configured from ``settings``"""

    def set_user_agent(request: httpx.Request) -> None:
        request.headers["User-Agent"] = user_agent

    client = BluecoreClient(
        api_url=settings.internal_url,
        token=token,
        anonymous=not token,
        timeout=settings.request_timeout,
        transport=RetryTransport(
            httpx.HTTPTransport(), max_retries=settings.max_retries
        ),
        load_dotenv=False,
        event_hooks={"request": [set_user_agent]},
    )
    # With credentials, bluecore-client answers a 401 on the user's token by
    # logging in as that account. Remove once bluecore-client#5 is fixed.
    if token and client.config.has_credentials:
        raise SettingsError("API_KEYCLOAK_USER and API_KEYCLOAK_PASSWORD are set")
    return client
