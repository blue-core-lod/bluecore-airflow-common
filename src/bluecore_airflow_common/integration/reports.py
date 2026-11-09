"""Reports that DAG runs write to the reports volume

bluecore-workflows writes each run's report to
``$BLUECORE_REPORTS_DIR/<dag_id>/<run_id>/`` (see its
``ils_middleware/tasks/report.py``), with each id made safe by the same
``slug()`` as here. A plugin finds a run's directory with ``run_dir()`` and
reads fixed names in it, such as ``report.json``, never a name from the request::

    directory = reports.run_dir(dag_id, run_id)
    if not reports.owner_matches(dag_run.conf, user.get_id(), roles=roles,
                                 admin_roles={"bluecore_admin"}):
        raise Forbidden()
    return FileResponse(directory / "index.html")
"""

import os
import pathlib
import re
from collections.abc import Iterable, Mapping
from typing import Any

from bluecore_airflow_common.integration.errors import NotFound

DEFAULT_REPORTS_DIR = "/opt/airflow/reports"

# The same pattern as bluecore-workflows, so both resolve the same directory.
UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def reports_dir() -> pathlib.Path:
    """The reports volume, ``$BLUECORE_REPORTS_DIR`` or ``/opt/airflow/reports``"""
    return pathlib.Path(os.environ.get("BLUECORE_REPORTS_DIR", DEFAULT_REPORTS_DIR))


def slug(value: str) -> str:
    """Make a string safe to use as a single path component"""
    return UNSAFE.sub("_", value).strip("_")


def run_dir(
    dag_id: str, run_id: str, base_dir: pathlib.Path | None = None
) -> pathlib.Path:
    """The directory for a run's report, which may not exist yet

    Raises:
        NotFound: if either id can't name a directory inside the reports volume.
    """
    parts = [slug(dag_id), slug(run_id)]
    if any(part in ("", ".", "..") for part in parts):
        raise NotFound(f"no report directory for {dag_id!r} {run_id!r}")
    return (base_dir or reports_dir()).joinpath(*parts)


def owner_matches(
    conf: Mapping[str, Any] | None,
    user_uid: str,
    *,
    roles: Iterable[str] = (),
    admin_roles: Iterable[str] = (),
) -> bool:
    """Whether the caller may see a run: they started it, or have an admin role

    ``conf`` is the run's ``dag_run.conf``, ``user_uid`` the caller's Keycloak
    subject. A run with no ``user_uid`` belongs to no one, so only admins match.
    """
    if not set(roles).isdisjoint(admin_roles):
        return True
    return bool(user_uid) and (conf or {}).get("user_uid") == user_uid
