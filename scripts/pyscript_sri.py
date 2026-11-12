"""Pin the shared PyScript include to a release, with its SRI hashes

    uv run python scripts/pyscript_sri.py 2026.7.3

Downloads core.js and core.css for that release from pyscript.net, and rewrites
the version and sha384 hashes in
src/bluecore_airflow_common/templates/bluecore/_pyscript.html. Read the release
notes before bumping: every plugin that includes the template moves with it,
and the Content-Security-Policy in docs/integration-contracts.md names the
release directory too.
"""

import base64
import hashlib
import pathlib
import re
import sys

import httpx

TEMPLATE = (
    pathlib.Path(__file__).parents[1]
    / "src/bluecore_airflow_common/templates/bluecore/_pyscript.html"
)
RELEASES = "https://pyscript.net/releases"
RELEASE = re.compile(r"\d{4}\.\d{1,2}\.\d{1,2}")


def sri(url: str) -> str:
    """The ``sha384-…`` integrity value for the file at ``url``"""
    response = httpx.get(url, follow_redirects=False, timeout=30)
    response.raise_for_status()
    digest = hashlib.sha384(response.content).digest()
    return "sha384-" + base64.b64encode(digest).decode()


def pin(template: str, name: str, value: str) -> str:
    """``template`` with ``{% set name = "…" %}`` set to ``value``"""
    pattern = re.compile(rf'(\{{% set {name} = ")[^"]*(" %\}})')
    updated, count = pattern.subn(lambda m: f"{m[1]}{value}{m[2]}", template)
    if count != 1:
        raise SystemExit(f"expected one {name} in {TEMPLATE}, found {count}")
    return updated


def main(version: str) -> None:
    if not RELEASE.fullmatch(version):
        raise SystemExit(f"not a PyScript release: {version!r}")
    template = pin(TEMPLATE.read_text(), "pyscript_version", version)
    for kind in ("css", "js"):
        url = f"{RELEASES}/{version}/core.{kind}"
        template = pin(template, f"pyscript_core_{kind}_sha384", sri(url))
    TEMPLATE.write_text(template)
    print(f"Pinned PyScript {version} in {TEMPLATE}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    main(sys.argv[1])
