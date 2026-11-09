import pathlib

import pytest

from bluecore_airflow_common.integration import reports
from bluecore_airflow_common.integration.errors import NotFound


def test_slug_matches_bluecore_workflows():
    """
    An Airflow run id slugs the way bluecore-workflows' report.py slugs it, so
    the plugin looks in the directory the DAG wrote to.
    """
    assert (
        reports.slug("manual__2026-09-08T12:00:00+00:00")
        == "manual__2026-09-08T12_00_00_00_00"
    )


def test_run_dir_is_dag_then_run(tmp_path):
    """A run's directory is <reports>/<dag_id>/<run_id>, with both slugged"""
    directory = reports.run_dir("marc_to_bibframe", "manual__1+00:00", tmp_path)

    assert directory == tmp_path / "marc_to_bibframe" / "manual__1_00_00"


def test_run_dir_does_not_create_the_directory(tmp_path):
    """run_dir only names the directory; a plugin reading reports creates nothing"""
    directory = reports.run_dir("dag", "run", tmp_path)

    assert not directory.exists()


def test_reports_dir_comes_from_environment(monkeypatch):
    """BLUECORE_REPORTS_DIR sets the reports volume"""
    monkeypatch.setenv("BLUECORE_REPORTS_DIR", "/srv/reports")

    assert reports.run_dir("dag", "run") == pathlib.Path("/srv/reports/dag/run")


def test_reports_dir_defaults_to_airflow_volume(monkeypatch):
    """Without BLUECORE_REPORTS_DIR, reports are under /opt/airflow/reports"""
    monkeypatch.delenv("BLUECORE_REPORTS_DIR", raising=False)

    assert reports.reports_dir() == pathlib.Path("/opt/airflow/reports")


@pytest.mark.parametrize("unsafe", ["..", ".", "", "///", "_"])
@pytest.mark.parametrize("position", ["dag_id", "run_id"])
def test_run_dir_rejects_ids_that_leave_their_directory(tmp_path, unsafe, position):
    """
    An id that slugs to "..", "." or nothing raises NotFound, whether it is
    the DAG id or the run id, so no report outside a run's own directory can
    be named.
    """
    ids = {"dag_id": "dag", "run_id": "run", position: unsafe}

    with pytest.raises(NotFound):
        reports.run_dir(ids["dag_id"], ids["run_id"], tmp_path)


@pytest.mark.parametrize("traversal", ["../etc", "../../etc/passwd", "a/../../b"])
def test_run_dir_keeps_separators_out(tmp_path, traversal):
    """
    An id with path separators slugs to a single component, so the directory
    is always exactly two levels below the reports volume.
    """
    directory = reports.run_dir("dag", traversal, tmp_path)

    assert directory.parent == tmp_path / "dag"
    assert "/" not in directory.name


@pytest.mark.parametrize(
    "conf,user_uid,roles,expected",
    [
        ({"user_uid": "u-1"}, "u-1", (), True),
        ({"user_uid": "u-1"}, "u-2", (), False),
        ({"user_uid": "u-1"}, "u-2", ("bluecore_admin",), True),
        ({"user_uid": "u-1"}, "u-2", ("editor",), False),
        ({}, "", (), False),
        ({"user_uid": None}, "", (), False),
        ({"user_uid": ""}, "", (), False),
        (None, "u-1", (), False),
        (None, "u-1", ("bluecore_admin",), True),
    ],
    ids=[
        "owner",
        "someone-else",
        "admin",
        "other-role",
        "no-owner-empty-caller",
        "none-owner-empty-caller",
        "empty-owner-empty-caller",
        "no-conf",
        "no-conf-admin",
    ],
)
def test_owner_matches(conf, user_uid, roles, expected):
    """
    The run's starter or an admin may see it. A run with no user_uid belongs
    to no one, so an empty caller uid never matches it.
    """
    assert (
        reports.owner_matches(
            conf, user_uid, roles=roles, admin_roles={"bluecore_admin"}
        )
        is expected
    )
