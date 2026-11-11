"""Stage uploaded files on the shared uploads volume, for DAG runs to read

Every Airflow container mounts the same uploads volume, so a plugin saves an
upload there and passes its path to a DAG run in ``conf``::

    @app.post("/jobs")
    def submit(file: UploadFile, user: GetUserDep) -> dict[str, str]:
        path, sha256 = uploads.stage_upload(
            file.file,
            file.filename or "",
            plugin="graph-toolbox",
            user=user.get_id(),
            suffixes={".mrc", ".xml"},
        )
        ...

The file is saved under ``<plugin>/<user>/`` with a new random name. Only the
suffix of the uploaded name is kept.
"""

import hashlib
import os
import pathlib
import uuid
from collections.abc import Iterable
from typing import BinaryIO

from bluecore_airflow_common.integration.errors import InputTooLarge, InvalidInput
from bluecore_airflow_common.integration.reports import safe_parts

DEFAULT_UPLOADS_DIR = "/opt/airflow/uploads"
MAX_BYTES = 2 * 1024**3  # 2 GiB
_CHUNK = 1024 * 1024


def uploads_dir() -> pathlib.Path:
    """The uploads volume, ``$BLUECORE_UPLOADS_DIR`` or ``/opt/airflow/uploads``"""
    return pathlib.Path(os.environ.get("BLUECORE_UPLOADS_DIR", DEFAULT_UPLOADS_DIR))


def stage_upload(
    source: BinaryIO,
    filename: str,
    *,
    plugin: str,
    user: str,
    suffixes: Iterable[str],
    max_bytes: int = MAX_BYTES,
    base_dir: pathlib.Path | None = None,
) -> tuple[pathlib.Path, str]:
    """Copy ``source`` to the uploads volume, and return its path and SHA-256

    Raises:
        InvalidInput: if ``filename`` doesn't end in one of ``suffixes``.
        InputTooLarge: if the file is over ``max_bytes``. Nothing is left behind.
    """
    allowed = {suffix.lower() for suffix in suffixes}
    suffix = pathlib.PurePath(filename).suffix.lower()
    if suffix not in allowed:
        raise InvalidInput(f"{filename!r} does not end in one of {sorted(allowed)}")
    directory = (base_dir or uploads_dir()).joinpath(*safe_parts(plugin, user))
    directory.mkdir(parents=True, exist_ok=True)

    name = uuid.uuid4().hex
    partial = directory / f"{name}.part"
    digest = hashlib.sha256()
    size = 0
    try:
        with partial.open("xb") as target:
            while chunk := source.read(_CHUNK):
                size += len(chunk)
                if size > max_bytes:
                    raise InputTooLarge(f"upload is over {max_bytes} bytes")
                digest.update(chunk)
                target.write(chunk)
        path = partial.rename(directory / f"{name}{suffix}")
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return path, digest.hexdigest()
