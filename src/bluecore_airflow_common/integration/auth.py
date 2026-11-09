"""Forward the signed-in user's identity to the Blue Core API and Airflow

Plugins act as the user who is signed in to Airflow, never as a shared
service account. Both dependencies read that user from Airflow's
``GetUserDep``:

``keycloak_token``
    The user's Keycloak access token, for Blue Core API writes. With the
    Keycloak auth manager, ``KeycloakJWTMiddleware`` has already refreshed it
    if it expired. Blue Core reads don't need it: the API serves resource
    GETs without Keycloak.
``airflow_token``
    A short-lived Airflow JWT for ``/api/v2``. From Airflow 3.3 the browser's
    session JWT leaves out the Keycloak tokens, so ``/api/v2`` would reject
    it; a token minted with ``generate_api_jwt`` carries them.

``install_token_redaction()`` keeps bearer tokens and JWTs out of the logs.
"""

import logging
import re
from typing import Annotated, Any

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.core_api.security import GetUserDep
from fastapi import Depends

from bluecore_airflow_common.integration.errors import ReauthRequired

# Long enough for a single plugin request to Airflow, short enough that a
# token leaked from a log is useless.
AIRFLOW_TOKEN_SECONDS = 60

_SECRETS = re.compile(
    r"(?i:bearer)\s+[A-Za-z0-9._~+/=-]+"  # an Authorization header value
    r"|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*"  # any JWT
)
REDACTED = "[REDACTED]"


def keycloak_token(user: GetUserDep) -> str:
    """The signed-in user's Keycloak access token

    Raises:
        ReauthRequired: if the user has none, for example because Airflow
            isn't using the Keycloak auth manager or the session has ended.
    """
    token = getattr(user, "access_token", None)
    if not token:
        raise ReauthRequired("the signed-in user has no Keycloak access token")
    return token


def airflow_token(user: GetUserDep) -> str:
    """A short-lived Airflow JWT for the signed-in user, for ``/api/v2``

    Raises:
        ReauthRequired: if there is no user, or a Keycloak user has lost their
            access token. On Airflow 3.3 the Keycloak provider's
            ``get_user_from_token`` returns ``None`` when the browser JWT
            arrives without the Keycloak cookies, and ``get_user`` passes that
            on. Users from auth managers without ``access_token`` (the simple
            auth manager) are let through.
    """
    if user is None or (hasattr(user, "access_token") and not user.access_token):
        raise ReauthRequired("the signed-in user's Keycloak session has ended")
    manager = get_auth_manager()
    # The Keycloak provider's generate_api_jwt puts the Keycloak tokens in the
    # claims. Auth managers without it (and Airflow 3.2, where generate_jwt
    # still includes them) use generate_jwt.
    generate = getattr(manager, "generate_api_jwt", None) or manager.generate_jwt
    return generate(user, expiration_time_in_seconds=AIRFLOW_TOKEN_SECONDS)


KeycloakToken = Annotated[str, Depends(keycloak_token)]
AirflowToken = Annotated[str, Depends(airflow_token)]


def _redact(value: Any) -> Any:
    """``value`` with secrets replaced in every string inside it"""
    if isinstance(value, str):
        return _SECRETS.sub(REDACTED, value)
    if isinstance(value, dict):
        return {key: _redact(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_redact(item) for item in value)
    return value


class RedactTokens(logging.Filter):
    """Replace bearer tokens and JWTs in a log message with ``[REDACTED]``"""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, dict):
            # A structlog event dict: ProcessorFormatter needs a dict here, so
            # redact its values in place of turning it into a string.
            record.msg = _redact(record.msg)
            return True
        message = record.getMessage()
        redacted = _SECRETS.sub(REDACTED, message)
        if redacted != message:
            record.msg = redacted
            record.args = None
        return True


def install_token_redaction() -> None:
    """Add ``RedactTokens`` to every handler on the root logger

    A filter on a logger only sees records logged to that exact logger, so it
    goes on the handlers, which see every record that reaches them. Call it
    once when the plugin loads; calling it again adds nothing.
    """
    for handler in logging.getLogger().handlers:
        if not any(isinstance(f, RedactTokens) for f in handler.filters):
            handler.addFilter(RedactTokens())
