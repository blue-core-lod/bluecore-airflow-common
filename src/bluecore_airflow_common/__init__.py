"""Shared support for Blue Core Airflow plugins.

Airflow gives each plugin its own FastAPI app mounted at its own ``url_prefix``,
so there is no single origin the plugins can hang assets off. Every plugin
therefore serves this package's ``static`` directory under its own prefix, at
``{url_prefix}/static/bluecore/``, next to its own ``{url_prefix}/static/``.

Templates are shared through the Jinja search path: a plugin's own templates
are searched first, then this package's. The common ones live under
``bluecore/`` so a plugin's own ``base.html`` can't shadow them by accident::

    {% extends "bluecore/base.html" %}
    {% block content %}...{% endblock %}
"""

from __future__ import annotations

import functools
import os
import pathlib
from typing import Any
from urllib.parse import urlsplit

from airflow.api_fastapi.core_api.security import requires_authenticated
from airflow.configuration import conf
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

__all__ = [
    "airflow_root_path",
    "context",
    "mount_static",
    "requires_login",
    "static_url",
    "templates",
]

PACKAGE_DIR = pathlib.Path(__file__).parent
STATIC_DIR = PACKAGE_DIR / "static"
TEMPLATES_DIR = PACKAGE_DIR / "templates"

STATIC_ROUTE = "/static"
COMMON_STATIC_ROUTE = f"{STATIC_ROUTE}/bluecore"

# Authentication is delegated to Airflow: the iframe request carries Airflow's
# JWT cookie, which this resolves through the configured auth manager (Keycloak
# in a deployed Blue Core stack). It has to stay one module-level object, since
# tests switch it off with app.dependency_overrides[requires_login].
requires_login = requires_authenticated()


def airflow_root_path() -> str:
    """Return the path Airflow is served under, without a trailing slash.

    Airflow is at the root in local development, but the bluecore-stack proxies
    it under ``/workflows``, so links into a plugin have to be prefixed with
    whatever ``[api] base_url`` says.
    """
    return urlsplit(conf.get("api", "base_url", fallback="") or "/").path.rstrip("/")


def templates(
    plugin_templates_dir: str | os.PathLike[str] | None = None,
) -> Jinja2Templates:
    """Return a template renderer that searches the plugin's templates first.

    Falls back to this package's templates, so a plugin page can extend
    ``bluecore/base.html`` without carrying its own copy.
    """
    directories: list[pathlib.Path] = [TEMPLATES_DIR]
    if plugin_templates_dir is not None:
        directories.insert(0, pathlib.Path(plugin_templates_dir))
    return Jinja2Templates(directory=directories)


def mount_static(
    app: FastAPI, plugin_static_dir: str | os.PathLike[str] | None = None
) -> None:
    """Serve the common static files, and optionally the plugin's own.

    Left unauthenticated: these are decorative assets that the browser requests
    as plain images and stylesheets.
    """
    # Starlette tries mounts in order and a "/static" mount would also claim
    # "/static/bluecore/...", so the common directory has to be mounted first.
    app.mount(
        COMMON_STATIC_ROUTE, StaticFiles(directory=STATIC_DIR), name="bluecore_static"
    )
    if plugin_static_dir is not None:
        app.mount(STATIC_ROUTE, StaticFiles(directory=plugin_static_dir), name="static")


def static_url(url_prefix: str, filename: str) -> str:
    """Return the URL a browser should use to fetch a static file.

    Common files are addressed as ``bluecore/<name>``; anything else comes from
    the plugin's own static directory.
    """
    return f"{airflow_root_path()}{url_prefix}{STATIC_ROUTE}/{filename}"


def context(url_prefix: str, **extra: Any) -> dict[str, Any]:
    """Build a template context, adding what every plugin page needs.

    Templates get a ``static_url`` function rather than a fixed path, so they
    keep working for pages nested under a plugin's prefix, where a relative URL
    would resolve against the wrong directory.
    """
    return {"static_url": functools.partial(static_url, url_prefix), **extra}
