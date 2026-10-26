import pathlib
import shutil
import subprocess
import zipfile

import pytest

PROJECT_DIR = pathlib.Path(__file__).parents[1]

PACKAGE_DATA = [
    "bluecore_airflow_common/py.typed",
    "bluecore_airflow_common/static/bluecore.css",
    "bluecore_airflow_common/static/bluecore.js",
    "bluecore_airflow_common/templates/bluecore/base.html",
]


@pytest.fixture(scope="module")
def wheel_contents(tmp_path_factory):
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is needed to build the wheel")
    out_dir = tmp_path_factory.mktemp("dist")
    subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(out_dir), str(PROJECT_DIR)],
        check=True,
        capture_output=True,
    )
    (wheel,) = out_dir.glob("*.whl")
    with zipfile.ZipFile(wheel) as archive:
        return set(archive.namelist())


@pytest.mark.parametrize("path", PACKAGE_DATA)
def test_wheel_includes_package_data(wheel_contents, path):
    assert path in wheel_contents


def test_wheel_includes_the_license(wheel_contents):
    assert any(name.endswith(".dist-info/licenses/LICENSE") for name in wheel_contents)
