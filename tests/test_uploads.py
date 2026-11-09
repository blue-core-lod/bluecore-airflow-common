import hashlib
import io
import pathlib

import pytest

from bluecore_airflow_common.integration import uploads
from bluecore_airflow_common.integration.errors import InputTooLarge, InvalidInput

CONTENT = b"MARC records " * 100


def stage(
    base_dir: pathlib.Path,
    content: bytes = CONTENT,
    filename: str = "records.mrc",
    *,
    plugin: str = "graph-toolbox",
    user: str = "u-1",
    max_bytes: int = uploads.MAX_BYTES,
) -> tuple[pathlib.Path, str]:
    return uploads.stage_upload(
        io.BytesIO(content),
        filename,
        plugin=plugin,
        user=user,
        suffixes={".mrc"},
        max_bytes=max_bytes,
        base_dir=base_dir,
    )


def files_in(directory: pathlib.Path) -> list[str]:
    return sorted(path.name for path in directory.iterdir())


def test_stages_file_with_its_sha256(tmp_path):
    """The upload is copied under <plugin>/<user>/ and its SHA-256 returned"""
    path, sha256 = stage(tmp_path)

    assert path.parent == tmp_path / "graph-toolbox" / "u-1"
    assert path.read_bytes() == CONTENT
    assert sha256 == hashlib.sha256(CONTENT).hexdigest()


def test_uploaded_name_is_never_used_as_a_path(tmp_path):
    """
    Only the suffix of the uploaded name is kept, lowercased, so a name with
    a path in it can't place the file anywhere else.
    """
    path, _ = stage(tmp_path, filename="../../etc/RECORDS.MRC")

    assert path.parent == tmp_path / "graph-toolbox" / "u-1"
    assert path.suffix == ".mrc"
    assert "RECORDS" not in path.name


def test_same_name_twice_keeps_both(tmp_path):
    """Two uploads with the same name get different paths"""
    first, _ = stage(tmp_path, content=b"one")
    second, _ = stage(tmp_path, content=b"two")

    assert first != second
    assert first.read_bytes() == b"one"
    assert second.read_bytes() == b"two"


@pytest.mark.parametrize("filename", ["records.txt", "records", "records.mrc.exe", ""])
def test_other_suffixes_are_refused_before_writing(tmp_path, filename):
    """
    A name without an allowed suffix raises InvalidInput, and nothing is
    written, not even the plugin's directory.
    """
    with pytest.raises(InvalidInput):
        stage(tmp_path, filename=filename)

    assert files_in(tmp_path) == []


def test_file_over_the_cap_leaves_nothing_behind(tmp_path):
    """
    A file over max_bytes raises InputTooLarge, and the partial file is
    deleted, even though copying stopped part way through a chunk.
    """
    with pytest.raises(InputTooLarge):
        stage(tmp_path, max_bytes=len(CONTENT) - 1)

    assert files_in(tmp_path / "graph-toolbox" / "u-1") == []


def test_file_at_the_cap_is_staged(tmp_path):
    """A file exactly max_bytes long is allowed"""
    path, _ = stage(tmp_path, max_bytes=len(CONTENT))

    assert path.read_bytes() == CONTENT


def test_read_failure_leaves_nothing_behind(tmp_path):
    """
    If reading the upload fails part way, such as a client disconnecting, the
    error is raised and the partial file is deleted.
    """

    class Disconnects(io.BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise OSError("client disconnected")
            return super().read(size)

    source = Disconnects(b"x" * (3 * 1024 * 1024))

    with pytest.raises(OSError, match="disconnected"):
        uploads.stage_upload(
            source,
            "records.mrc",
            plugin="graph-toolbox",
            user="u-1",
            suffixes={".mrc"},
            base_dir=tmp_path,
        )

    assert files_in(tmp_path / "graph-toolbox" / "u-1") == []


@pytest.mark.parametrize(
    "plugin,user", [("..", "u-1"), ("graph-toolbox", ".."), ("", "u-1")]
)
def test_unsafe_plugin_or_user_is_a_server_error(tmp_path, plugin, user):
    """
    A plugin or user that can't name a directory is a programming error, so
    it raises ValueError (a 500) rather than an IntegrationError for the user.
    """
    with pytest.raises(ValueError):
        stage(tmp_path, plugin=plugin, user=user)


def test_uploads_dir_comes_from_environment(monkeypatch):
    """BLUECORE_UPLOADS_DIR sets the uploads volume"""
    monkeypatch.setenv("BLUECORE_UPLOADS_DIR", "/srv/uploads")

    assert uploads.uploads_dir() == pathlib.Path("/srv/uploads")


def test_uploads_dir_defaults_to_airflow_volume(monkeypatch):
    """Without BLUECORE_UPLOADS_DIR, uploads go to /opt/airflow/uploads"""
    monkeypatch.delenv("BLUECORE_UPLOADS_DIR", raising=False)

    assert uploads.uploads_dir() == pathlib.Path("/opt/airflow/uploads")
