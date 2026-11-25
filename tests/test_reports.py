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


def test_safe_parts_slugs_each_value():
    """Each value comes back slugged, in order, as one path component"""
    assert reports.safe_parts("graph-toolbox", "a/../b") == ["graph-toolbox", "a_.._b"]


@pytest.mark.parametrize("unsafe", ["..", ".", "", "///", "_"])
def test_safe_parts_rejects_parts_that_leave_the_base(unsafe):
    """A value that slugs to "..", "." or nothing raises ValueError"""
    with pytest.raises(ValueError):
        reports.safe_parts("ok", unsafe)


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


@pytest.fixture
def run(tmp_path) -> pathlib.Path:
    """A run's directory in a reports volume at tmp_path / "reports\""""
    directory = reports.run_dir("dag", "run", tmp_path / "reports")
    directory.mkdir(parents=True)
    return directory


def base(run: pathlib.Path) -> pathlib.Path:
    return run.parent.parent


def test_report_files_keeps_regular_files(run):
    """Each fixed report file that exists comes back resolved, by format"""
    (run / "index.html").write_text("<html></html>")
    (run / "report.csv").write_text("a,b\n")

    assert reports.report_files(run, base(run)) == {
        "html": (run / "index.html").resolve(),
        "csv": (run / "report.csv").resolve(),
    }


def test_report_files_skips_missing_files_and_directories(run):
    """A missing file is left out, and so is a directory with a report's name"""
    (run / "report.json").mkdir()

    assert reports.report_files(run, base(run)) == {}


def test_report_files_ignores_other_names(run):
    """Only the fixed names in REPORT_FILES are looked at"""
    (run / "report.xml").write_text("<x/>")

    assert reports.report_files(run, base(run)) == {}


def test_report_files_keeps_symlinks_within_the_run(run):
    """A symlink to another file in the same run directory is followed"""
    (run / "full.html").write_text("<html></html>")
    (run / "index.html").symlink_to(run / "full.html")

    assert reports.report_files(run, base(run)) == {
        "html": (run / "full.html").resolve()
    }


def test_report_files_skips_symlinks_out_of_the_volume(run, tmp_path):
    """A symlink to a file outside the reports volume is left out"""
    secret = tmp_path / "secret.html"
    secret.write_text("secret")
    (run / "index.html").symlink_to(secret)

    assert reports.report_files(run, base(run)) == {}


def test_report_files_skips_symlinks_to_another_run(run):
    """A symlink into another run's directory is left out"""
    other = reports.run_dir("dag", "other", base(run))
    other.mkdir()
    (other / "index.html").write_text("someone else's")
    (run / "index.html").symlink_to(other / "index.html")

    assert reports.report_files(run, base(run)) == {}


def test_report_files_skips_a_run_directory_outside_the_volume(tmp_path):
    """A run directory that isn't under the reports volume has no reports"""
    volume = tmp_path / "reports"
    volume.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "index.html").write_text("x")

    assert reports.report_files(outside, volume) == {}


def test_report_files_missing_run_directory(run):
    """A run that never wrote a report has no files"""
    assert reports.report_files(run / "nope", base(run)) == {}


def test_report_files_uses_reports_dir_by_default(run, monkeypatch):
    """Without base_dir, the run must be under BLUECORE_REPORTS_DIR"""
    (run / "index.html").write_text("x")
    monkeypatch.setenv("BLUECORE_REPORTS_DIR", str(base(run)))

    assert set(reports.report_files(run)) == {"html"}


def test_read_json_reads_an_object(run):
    (run / "report.json").write_text('{"processed": 3}')

    assert reports.read_json(run, "report.json", max_bytes=100, base_dir=base(run)) == {
        "processed": 3
    }


@pytest.mark.parametrize(
    "content",
    [b"{not json", b'{"a": "\xff"}', b"[1, 2]", b'"text"', b"null", b""],
    ids=["malformed", "not-utf8", "array", "string", "null", "empty"],
)
def test_read_json_returns_none_for_bad_files(run, content):
    """Malformed JSON, non-UTF-8 bytes and anything but an object are None"""
    (run / "report.json").write_bytes(content)

    assert (
        reports.read_json(run, "report.json", max_bytes=100, base_dir=base(run)) is None
    )


def test_read_json_missing_file(run):
    assert (
        reports.read_json(run, "report.json", max_bytes=100, base_dir=base(run)) is None
    )


def test_read_json_size_limit(run):
    """A file of exactly max_bytes is read; one byte more is None"""
    body = '{"a": "' + "x" * 50 + '"}'
    (run / "exact.json").write_text(body)
    (run / "over.json").write_text(body + " ")

    assert reports.read_json(run, "exact.json", max_bytes=len(body), base_dir=base(run))
    assert (
        reports.read_json(run, "over.json", max_bytes=len(body), base_dir=base(run))
        is None
    )


def test_read_json_escaping_symlink(run, tmp_path):
    """A symlink out of the run's directory is None"""
    secret = tmp_path / "secret.json"
    secret.write_text('{"token": "abc"}')
    (run / "report.json").symlink_to(secret)

    assert (
        reports.read_json(run, "report.json", max_bytes=100, base_dir=base(run)) is None
    )


@pytest.mark.parametrize(
    "name", ["", ".", "..", "../report.json", "a/b.json", "/etc/passwd", "a\\b", "a\0b"]
)
def test_read_json_rejects_names_that_are_not_bare(run, name):
    with pytest.raises(ValueError):
        reports.read_json(run, name, max_bytes=100, base_dir=base(run))


def test_read_counts_keeps_known_non_negative_ints(run):
    (run / "counts.json").write_text('{"marc_records": 0, "entities": 12}')

    assert reports.read_counts(run, base(run)) == reports.RunCounts(
        marc_records=0, entities=12
    )


def test_read_counts_drops_unusable_values(run):
    """Booleans, negatives, strings, floats and unknown keys are dropped"""
    (run / "counts.json").write_text(
        '{"marc_records": true, "entities": 4, "works": 9, "x": -1}'
    )

    assert reports.read_counts(run, base(run)) == reports.RunCounts(entities=4)


@pytest.mark.parametrize(
    "content",
    [
        '{"marc_records": false, "entities": -1}',
        '{"marc_records": "3", "entities": 2.0}',
        '{"marc_records": null, "entities": null}',
        '{"works": 1}',
        "{}",
        "[]",
    ],
)
def test_read_counts_none_when_nothing_usable(run, content):
    (run / "counts.json").write_text(content)

    assert reports.read_counts(run, base(run)) is None


def test_read_counts_missing_file(run):
    assert reports.read_counts(run, base(run)) is None


def test_read_counts_too_large(run):
    padding = " " * reports.MAX_COUNTS_BYTES
    (run / "counts.json").write_text('{"entities": 1}' + padding)

    assert reports.read_counts(run, base(run)) is None


class BulkUpdateCounts(reports.RunCounts):
    works: reports.Count = None


def test_subclass_reads_its_own_counts(run):
    """
    A plugin's subclass reads counts that RunCounts ignores
    """
    (run / "counts.json").write_text('{"entities": 4, "works": 9}')

    counts = BulkUpdateCounts.read(run, base(run))

    assert isinstance(counts, BulkUpdateCounts)
    assert counts == BulkUpdateCounts(entities=4, works=9)
    assert reports.read_counts(run, base(run)) == reports.RunCounts(entities=4)


def test_subclass_counts_only_its_own_fields_as_usable(run):
    """A count only the subclass knows is enough for it, and nothing for RunCounts"""
    (run / "counts.json").write_text('{"works": 7}')

    assert BulkUpdateCounts.read(run, base(run)) == BulkUpdateCounts(works=7)
    assert reports.read_counts(run, base(run)) is None


def test_subclass_validation_eeror_is_none(run):
    """A subclass field that isn't a lenient Count still never raises for a bad file"""
    (run / "counts.json").write_text('{"entitites": 4}')

    class LabelledCounts(reports.RunCounts):
        label: str

    assert LabelledCounts.read(run, base(run)) is None


def test_count_schema_is_a_non_negative_integer_or_null():
    schema = reports.RunCounts.model_json_schema()["properties"]["entities"]

    assert schema["anyOf"] == [{"minimum": 0, "type": "integer"}, {"type": "null"}]
