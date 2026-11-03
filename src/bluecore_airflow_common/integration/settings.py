"""Deployment-selected configuration for the Blue Core integration clients.

Settings come from the same environment variables as the rest of the bluecore-stack
(see its ``docs/configuration.md``), which compose passes to every Airflow
container from ``.env``:

``BLUECORE_API_INTERNAL_URL`` (required)
    Blue Core API root as the Airflow containers reach it, e.g.
    ``http://bc_api:8100``. Not sent to browsers.
``BLUECORE_URL`` (required)
    Blue Core site root as the browser reaches it, e.g. ``https://bcld.info``.
    The browser reaches the API under ``/api``.
``BLUECORE_REQUEST_TIMEOUT``
    Seconds allowed for each upstream request. Default 10.
``BLUECORE_MAX_RETRIES``
    Retries for idempotent (GET and HEAD) requests. Default 2, at most 5.

Importing this module never raises and never touches the network: missing or
invalid configuration is logged, and only raises ``SettingsError`` when the
settings are first used.
"""

from __future__ import annotations

import functools
import logging
import os
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import Depends
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
)

logger = logging.getLogger(__name__)


class SettingsError(RuntimeError):
    """The Blue Core configuration is missing or invalid"""


def _base_url(value: str) -> str:
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError("must be an absolute http(s) URL")
    return value.rstrip("/")


BaseUrl = Annotated[str, AfterValidator(_base_url)]


class BlueCoreSettings(BaseModel):
    # Fields are read from the environment variables named in validation_alias.
    # populate_by_name lets tests and plugins build one by field name instead.
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    internal_url: BaseUrl = Field(validation_alias="BLUECORE_API_INTERNAL_URL")
    public_url: BaseUrl = Field(validation_alias="BLUECORE_URL")
    request_timeout: float = Field(
        default=10.0, gt=0, validation_alias="BLUECORE_REQUEST_TIMEOUT"
    )
    max_retries: int = Field(
        default=2, ge=0, le=5, validation_alias="BLUECORE_MAX_RETRIES"
    )


# Only these variables are passed to validation, so an error message can never
# echo the rest of the environment, which holds secrets.
VARIABLES = [
    str(field.validation_alias) for field in BlueCoreSettings.model_fields.values()
]


def _read_environment() -> dict[str, str]:
    # Compose sets an unset ${VAR} to "", so an empty value counts as missing.
    return {name: value for name in VARIABLES if (value := os.environ.get(name))}


def _load() -> BlueCoreSettings:
    try:
        return BlueCoreSettings.model_validate(_read_environment())
    except ValidationError as exc:
        raise SettingsError(f"invalid Blue Core configuration: {exc}") from None


@functools.cache
def get_settings() -> BlueCoreSettings:
    """Return the settings, loading and validating them on first use.

    Use it as a FastAPI dependency, ``Depends(get_settings)``, and replace it in
    tests with ``app.dependency_overrides[get_settings]``. It takes no
    arguments on purpose: the environment is chosen by the deployment, never by
    a request. A failed load isn't cached, so it is retried on the next call.

    Raises:
        SettingsError: if the configuration is missing or invalid.
    """
    return _load()


def public_config(
    settings: Annotated[BlueCoreSettings, Depends(get_settings)],
) -> dict[str, Any]:
    """Return the settings that are safe to send to a browser.

    This is an allowlist, so a new setting stays server-side until it is added
    here. ``internal_url`` is left out because it can name an internal host.
    """
    return {
        "public_url": settings.public_url,
    }


def _check_at_import() -> None:
    try:
        _load()
    except SettingsError as exc:
        logger.error(
            "Blue Core integration calls will fail until this is fixed: %s", exc
        )
    except Exception:
        logger.exception("Could not read the Blue Core configuration")


_check_at_import()
