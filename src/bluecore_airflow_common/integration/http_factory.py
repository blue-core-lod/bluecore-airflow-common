"""The httpx client factory shared by the Blue Core integration clients

``create_client`` builds an ``httpx.AsyncClient`` with the same behavior for
every plugin:

- an explicit ``httpx.Timeout`` from ``request_timeout``
- redirects are not followed, so a request can't be sent somewhere else
- bounded connection pool limits
- bounded retries for GET and HEAD (see ``retry.py``)
- a cap on response body bytes
- a User-Agent naming the calling plugin and its version

The body cap counts the bytes as they arrive. The client asks for
uncompressed responses, so those are the bytes the caller reads.
"""

from collections.abc import AsyncIterator
from importlib import metadata

import httpx

from bluecore_airflow_common.integration.errors import BadUpstreamResponse
from bluecore_airflow_common.integration.retry import RetryTransport
from bluecore_airflow_common.integration.settings import BlueCoreSettings

DISTRIBUTION = "bluecore-airflow-common"
MAX_RESPONSE_BYTES = 10 * 1024 * 1024
LIMITS = httpx.Limits(
    max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0
)


def _version(distribution: str) -> str:
    try:
        return metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return "unknown"


def user_agent(plugin: str) -> str:
    """User-Agent naming the plugin's distribution and this package, with versions"""
    return f"{plugin}/{_version(plugin)} {DISTRIBUTION}/{_version(DISTRIBUTION)}"


class _CappedStream(httpx.AsyncByteStream):
    def __init__(self, stream: httpx.AsyncByteStream, limit: int, url: str) -> None:
        self._stream = stream
        self._limit = limit
        self._url = url

    async def __aiter__(self) -> AsyncIterator[bytes]:
        received = 0
        async for chunk in self._stream:
            received += len(chunk)
            if received > self._limit:
                raise BadUpstreamResponse(
                    f"{self._url} response is over {self._limit} bytes"
                )
            yield chunk

    async def aclose(self) -> None:
        await self._stream.aclose()


class ByteCapTransport(httpx.AsyncBaseTransport):
    """An httpx transport that fails a response whose body is over ``limit`` bytes"""

    def __init__(self, transport: httpx.AsyncBaseTransport, *, limit: int) -> None:
        self._transport = transport
        self._limit = limit

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._transport.handle_async_request(request)
        url = str(request.url)
        try:
            declared = int(response.headers.get("Content-Length", "0"))
        except ValueError:
            declared = 0
        if declared > self._limit:
            await response.aclose()
            raise BadUpstreamResponse(
                f"{url} declared {declared} bytes, over {self._limit}",
                upstream_status=response.status_code,
            )
        assert isinstance(response.stream, httpx.AsyncByteStream)
        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            stream=_CappedStream(response.stream, self._limit, url),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self._transport.aclose()


def create_client(
    settings: BlueCoreSettings,
    *,
    plugin: str,
    base_url: str | None = None,
    max_response_bytes: int = MAX_RESPONSE_BYTES,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Return an ``httpx.AsyncClient`` for calling an upstream service.

    Args:
        settings: the deployment's settings, usually from ``get_settings()``.
        plugin: the calling plugin's distribution name, e.g.
            ``"bluecore-graph-toolbox"``. Its installed version is looked up.
        base_url: the upstream root. Defaults to ``settings.internal_url``.
            Code chooses it, never a request.
        max_response_bytes: the largest response body accepted.
        transport: the innermost transport. Tests pass an
            ``httpx.MockTransport``; the default is a real connection pool.
    """
    inner = transport or httpx.AsyncHTTPTransport(limits=LIMITS)
    return httpx.AsyncClient(
        base_url=base_url or settings.internal_url,
        timeout=httpx.Timeout(settings.request_timeout),
        follow_redirects=False,
        headers={"User-Agent": user_agent(plugin), "Accept-Encoding": "identity"},
        transport=ByteCapTransport(
            RetryTransport(inner, max_retries=settings.max_retries),
            limit=max_response_bytes,
        ),
    )
