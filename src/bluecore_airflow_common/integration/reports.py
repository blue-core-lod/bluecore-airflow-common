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
    files = reports.report_files(directory)   # {"html": Path, ...}, only those present
    counts = reports.read_counts(directory)   # RunCounts(entities=12, ...) or None

DAG tasks write the reports volume, so its files are untrusted input. The
readers here open only the fixed names in ``REPORT_FILES`` and ``COUNTS_FILE``,
follow a symlink only if it stays inside the run's own directory, and cap how
much they read.

``counts.json`` is a JSON object of non-negative integers, described by
``RunCounts``: ``{"marc_records": int, "entities": int}``. Each key is optional:
a missing key means the count is unknown, and ``0`` means zero. A plugin that
reads more counts subclasses ``RunCounts`` and calls ``read()`` on its subclass.
"""

import json
import os
import pathlib
import re
import stat
from collections.abc import Iterable, Mapping
from typing import Annotated, Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    Strict,
    ValidationError,
    ValidatorFunctionWrapHandler,
    WrapValidator,
)

from bluecore_airflow_common.integration.errors import NotFound

DEFAULT_REPORTS_DIR = "/opt/airflow/reports"

# The same pattern as bluecore-workflows, so both resolve the same directory.
UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")

# The report files a run may have, by format. Only these names are ever opened.
REPORT_FILES = {"html": "index.html", "json": "report.json", "csv": "report.csv"}

# The run's counts, read by read_counts().
COUNTS_FILE = "counts.json"
MAX_COUNTS_BYTES = 64 * 1024


def _none_if_invalid(value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
    """A count that isn't usable reads as unknown, not as a bad file"""
    try:
        return handler(value)
    except ValidationError:
        return None


# A non-negative integer, or None when missing or unusable. Strict, so true,
# false, "3" and 2.0 are not counts.
Count = Annotated[
    Annotated[int, Strict(), Field(ge=0)] | None, WrapValidator(_none_if_invalid)
]


def reports_dir() -> pathlib.Path:
    """The reports volume, ``$BLUECORE_REPORTS_DIR`` or ``/opt/airflow/reports``"""
    return pathlib.Path(os.environ.get("BLUECORE_REPORTS_DIR", DEFAULT_REPORTS_DIR))


def slug(value: str) -> str:
    """Make a string safe to use as a single path component"""
    return UNSAFE.sub("_", value).strip("_")


def safe_parts(*values: str) -> list[str]:
    """Each value slugged, for joining under a base directory

    ``slug()`` removes every separator, so the only way out of the base
    directory is a part that slugs to ``..``, ``.`` or nothing.

    Raises:
        ValueError: if any value slugs to ``..``, ``.`` or nothing.
    """
    parts = [slug(value) for value in values]
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"cannot make path parts from {values!r}")
    return parts


def run_dir(
    dag_id: str, run_id: str, base_dir: pathlib.Path | None = None
) -> pathlib.Path:
    """The directory for a run's report, which may not exist yet

    Raises:
        NotFound: if either id can't name a directory inside the reports volume.
    """
    try:
        parts = safe_parts(dag_id, run_id)
    except ValueError:
        raise NotFound(f"no report directory for {dag_id!r} {run_id!r}") from None
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


def _bare_name(name: str) -> str:
    """``name`` if it is a single file name, otherwise ValueError"""
    if name in ("", ".", "..") or "/" in name or "\\" in name or "\0" in name:
        raise ValueError(f"not a bare file name: {name!r}")
    return name


def _contained(
    directory: pathlib.Path, name: str, base_dir: pathlib.Path | None
) -> pathlib.Path | None:
    """The resolved path of ``directory / name``, or None if it isn't a regular
    file inside the run's own directory

    The run's directory must itself resolve to somewhere below the reports
    volume (``base_dir``, by default ``reports_dir()``). A symlink is followed
    only if its target stays in the run's directory, so one pointing out of the
    volume or into another run's directory is ignored.
    """
    try:
        real_base = (base_dir or reports_dir()).resolve(strict=True)
        real_dir = directory.resolve(strict=True)
        if real_dir == real_base or not real_dir.is_relative_to(real_base):
            return None
        path = (directory / name).resolve(strict=True)
        if path.parent != real_dir or not path.is_file():
            return None
    except (OSError, RuntimeError, ValueError):
        return None
    return path


def report_files(
    directory: pathlib.Path, base_dir: pathlib.Path | None = None
) -> dict[str, pathlib.Path]:
    """The run's report files that exist, by format (see ``REPORT_FILES``)

    Each path is resolved. A missing file, a directory with a report's name and
    a symlink that leaves the run's directory are all left out.
    """
    files = {}
    for fmt, name in REPORT_FILES.items():
        path = _contained(directory, name, base_dir)
        if path is not None:
            files[fmt] = path
    return files


def read_json(
    directory: pathlib.Path,
    name: str,
    *,
    max_bytes: int,
    base_dir: pathlib.Path | None = None,
) -> dict[str, Any] | None:
    """The JSON object in the run's file ``name``, or None if it can't be used

    None when the file is missing, outside the run's directory, larger than
    ``max_bytes``, not UTF-8, not JSON, or not a JSON object. A bad file never
    raises.

    Raises:
        ValueError: if ``name`` is not a bare file name.
    """
    path = _contained(directory, _bare_name(name), base_dir)
    if path is None:
        return None
    try:
        # O_NONBLOCK so a file swapped for a FIFO since the check can't hang us.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    try:
        with os.fdopen(fd, "rb") as file:
            info = os.fstat(file.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > max_bytes:
                return None
            data = file.read(max_bytes + 1)
    except OSError:
        return None
    if len(data) > max_bytes:
        return None
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


class RunCounts(BaseModel):
    """The counts in a run's ``counts.json``

    Subclass it to read counts that only one DAG writes::

        class BulkUpdateCounts(reports.RunCounts):
            works: reports.Count = None

    Unknown keys are ignored, so a DAG can add a count before its readers know it.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    marc_records: Count = None
    entities: Count = None

    @classmethod
    def read(
        cls, directory: pathlib.Path, base_dir: pathlib.Path | None = None
    ) -> Self | None:
        """The run's counts, or None if ``counts.json`` has none that are usable"""
        data = read_json(
            directory, COUNTS_FILE, max_bytes=MAX_COUNTS_BYTES, base_dir=base_dir
        )
        if data is None:
            return None
        try:
            counts = cls.model_validate(data)
        except ValidationError:
            return None
        return counts if counts.model_dump(exclude_none=True) else None


def read_counts(
    directory: pathlib.Path, base_dir: pathlib.Path | None = None
) -> RunCounts | None:
    """The run's ``RunCounts``, or None if there are none

    The run's RunCounts, or None if there are none. Subclass RunCounts and
    call read() to read more counts.
    """
    return RunCounts.read(directory, base_dir)
