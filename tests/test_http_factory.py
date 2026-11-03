from collections.abc import AsyncIterator
from importlib import metadata

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bluecore_airflow_common.integration import errors, http_factory, retry
from bluecore_airflow_common.integration.settings import BlueCoreSettings

PLUGIN = "bluecore-airflow-common"
VERSION = metadata.version(PLUGIN)
SETTINGS = BlueCoreSettings(
    internal_url="http://bc_api:8100",
    public_url="https://bcld.info",
    request_timeout=2.5,
    max_retries=2,
)


class Upstream:
    """A scripted upstream that records each request it receives"""

    def __init__(self, response: httpx.Response | None = None) -> None:
        self.response = response or httpx.Response(200, json={"ok": True})
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.response


class ClosingTransport(httpx.MockTransport):
    """A mock transport that remembers whether it was closed"""

    closed = False

    async def aclose(self) -> None:
        self.closed = True


def make_client(upstream: Upstream, **options) -> httpx.AsyncClient:
    options.setdefault("plugin", PLUGIN)
    return http_factory.create_client(
        SETTINGS, transport=httpx.MockTransport(upstream), **options
    )


async def chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


@pytest.fixture
def no_backoff(monkeypatch):
    monkeypatch.setattr(retry.random, "uniform", lambda low, high: 0.0)


def test_user_agent_names_plugin_and_this_package():
    """The User-Agent carries the calling plugin's name and installed version,
    followed by this package's, so upstream logs show which plugin called."""
    assert http_factory.user_agent(PLUGIN) == f"{PLUGIN}/{VERSION} {PLUGIN}/{VERSION}"


def test_user_agent_tolerates_uninstalled_plugin():
    """A plugin running from a source checkout has no installed metadata; its
    version is reported as "unknown" instead of failing the request."""
    agent = http_factory.user_agent("not-an-installed-plugin")

    assert agent.startswith("not-an-installed-plugin/unknown ")


async def test_client_sends_user_agent_and_identity_encoding():
    """Every request carries the plugin User-Agent and asks for an uncompressed
    body, so the byte cap counts the bytes the caller actually reads."""
    upstream = Upstream()

    async with make_client(upstream) as client:
        await client.get("/resources")

    (request,) = upstream.requests
    assert request.headers["User-Agent"] == http_factory.user_agent(PLUGIN)
    assert request.headers["Accept-Encoding"] == "identity"


async def test_client_uses_explicit_timeout_from_settings():
    """The connect, read, write and pool timeouts all come from
    request_timeout rather than httpx's hidden 5 second default."""
    async with make_client(Upstream()) as client:
        assert client.timeout == httpx.Timeout(2.5)


async def test_client_defaults_to_internal_url():
    """Relative paths resolve against the Blue Core API's internal URL from the
    deployment settings."""
    upstream = Upstream()

    async with make_client(upstream) as client:
        await client.get("/resources/1")

    assert str(upstream.requests[0].url) == "http://bc_api:8100/resources/1"


async def test_client_accepts_base_url_from_code():
    """Code can point the client at another upstream, such as Airflow, in
    place of the Blue Core API."""
    upstream = Upstream()

    async with make_client(upstream, base_url="http://airflow:8080") as client:
        await client.get("/api/v2/dags")

    assert str(upstream.requests[0].url) == "http://airflow:8080/api/v2/dags"


async def test_client_does_not_follow_redirects():
    """A redirect is returned to the caller instead of being followed, so a
    forwarded token is never sent to the host named in Location."""
    upstream = Upstream(httpx.Response(302, headers={"Location": "http://evil/"}))

    async with make_client(upstream) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 302
    assert len(upstream.requests) == 1


async def test_body_at_the_limit_is_accepted():
    """A body exactly max_response_bytes long is read in full."""
    upstream = Upstream(httpx.Response(200, content=b"x" * 10))

    async with make_client(upstream, max_response_bytes=10) as client:
        response = await client.get("/resources/1")

    assert response.content == b"x" * 10


async def test_declared_length_over_the_limit_is_rejected():
    """A Content-Length over the cap fails as soon as the headers arrive,
    before any of the body is read."""
    upstream = Upstream(httpx.Response(200, content=b"x" * 11))

    async with make_client(upstream, max_response_bytes=10) as client:
        with pytest.raises(errors.BadUpstreamResponse) as caught:
            await client.get("/resources/1")

    assert caught.value.upstream_status == 200
    assert "declared 11 bytes" in str(caught.value)


async def test_streamed_body_over_the_limit_is_rejected():
    """A chunked body with no Content-Length fails once the bytes received
    pass the cap."""
    upstream = Upstream(httpx.Response(200, content=chunks(b"xxxx", b"xxxx", b"xxxx")))

    async with make_client(upstream, max_response_bytes=10) as client:
        with pytest.raises(errors.BadUpstreamResponse, match="over 10 bytes"):
            await client.get("/resources/1")


async def test_streamed_body_under_the_limit_is_accepted():
    """A chunked body that stays under the cap is read in full."""
    upstream = Upstream(httpx.Response(200, content=chunks(b"xxxx", b"xxxx")))

    async with make_client(upstream, max_response_bytes=10) as client:
        response = await client.get("/resources/1")

    assert response.content == b"x" * 8


async def test_invalid_content_length_falls_back_to_counting():
    """An unparseable Content-Length is ignored, and the streamed byte count
    still enforces the cap."""
    response = httpx.Response(
        200, headers={"Content-Length": "lots"}, content=chunks(b"x" * 11)
    )

    async with make_client(Upstream(response), max_response_bytes=10) as client:
        with pytest.raises(errors.BadUpstreamResponse, match="over 10 bytes"):
            await client.get("/resources/1")


async def test_oversized_response_is_not_retried():
    """A size error isn't a transient network failure, so even a GET is sent
    only once."""
    upstream = Upstream(httpx.Response(200, content=b"x" * 11))

    async with make_client(upstream, max_response_bytes=10) as client:
        with pytest.raises(errors.BadUpstreamResponse):
            await client.get("/resources/1")

    assert len(upstream.requests) == 1


@pytest.mark.usefixtures("no_backoff")
async def test_get_is_retried_up_to_max_retries():
    """The factory wires in RetryTransport with settings.max_retries, so a GET
    that keeps getting a 503 is sent once plus max_retries more times."""
    upstream = Upstream(httpx.Response(503))

    async with make_client(upstream) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 503
    assert len(upstream.requests) == 1 + SETTINGS.max_retries


async def test_post_is_never_retried():
    """A POST that gets a 503 is sent exactly once, so a conversion or DAG
    trigger is never submitted twice."""
    upstream = Upstream(httpx.Response(503))

    async with make_client(upstream) as client:
        response = await client.post("/marc2bibframe", content=b"record")

    assert response.status_code == 503
    assert len(upstream.requests) == 1


def test_handler_returns_size_error_without_url():
    """The shared error handler turns an oversized upstream body into a 502
    bad_upstream_response, and the internal URL stays in the server log."""
    app = FastAPI()
    errors.install_error_handler(app)
    upstream = Upstream(httpx.Response(200, content=b"x" * 11))

    @app.get("/proxy")
    async def proxy() -> dict[str, str]:
        async with make_client(upstream, max_response_bytes=10) as client:
            await client.get("/resources/1")
        return {}

    response = TestClient(app).get("/proxy")

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "bad_upstream_response"
    assert "bc_api" not in response.text


async def test_closing_client_closes_inner_transport():
    """Closing the client passes through the cap and retry layers to the
    connection pool, so connections aren't leaked."""
    transport = ClosingTransport(Upstream())
    client = http_factory.create_client(SETTINGS, plugin=PLUGIN, transport=transport)

    await client.aclose()

    assert transport.closed
