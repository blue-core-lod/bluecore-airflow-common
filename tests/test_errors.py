import logging

import bluecore_client
import httpx
import pytest
import urllib3
import urllib3.connection
from airflow_client.client.exceptions import (
    ApiException,
    ForbiddenException,
    NotFoundException,
    ServiceException,
    UnauthorizedException,
    UnprocessableEntityException,
)
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bluecore_airflow_common.integration import errors

BF2MARC_ISSUE = "https://github.com/blue-core-lod/bluecore_api/issues/13"
SECRET_BODY = '{"detail": "token abc123 rejected for http://bc_api:8100"}'


def upstream_response(status: int, text: str = SECRET_BODY) -> httpx.Response:
    request = httpx.Request("GET", "http://bc_api:8100/resources/1")
    return httpx.Response(status, text=text, request=request)


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    errors.install_error_handler(app)

    @app.get("/upstream/{status}")
    def upstream(status: int):
        errors.raise_for_status(upstream_response(status))

    @app.get("/timeout")
    def timeout():
        raise httpx.ReadTimeout("slow")

    @app.get("/bf2marc")
    def bf2marc():
        raise errors.NotImplementedUpstream("BIBFRAME to MARC", BF2MARC_ISSUE)

    @app.get("/input/too-large")
    def too_large():
        raise errors.InputTooLarge("input is 3000000000 bytes, secret-detail")

    @app.get("/input/invalid")
    def invalid():
        raise errors.InvalidInput("could not read MARC: secret-detail")

    return TestClient(app)


@pytest.mark.parametrize(
    "status,error_class,code,http_status",
    [
        (401, errors.ReauthRequired, "reauth_required", 401),
        (403, errors.Forbidden, "forbidden", 403),
        (404, errors.NotFound, "not_found", 404),
        (413, errors.InvalidRequest, "invalid_request", 422),
        (422, errors.InvalidRequest, "invalid_request", 422),
        (500, errors.UpstreamUnavailable, "upstream_unavailable", 502),
        (503, errors.UpstreamUnavailable, "upstream_unavailable", 502),
        (302, errors.BadUpstreamResponse, "bad_upstream_response", 502),
        (400, errors.BadUpstreamResponse, "bad_upstream_response", 502),
        (429, errors.BadUpstreamResponse, "bad_upstream_response", 502),
    ],
)
def test_from_response_maps_status(status, error_class, code, http_status):
    error = errors.from_response(upstream_response(status))

    assert type(error) is error_class
    assert error.code == code
    assert error.status_code == http_status
    assert error.upstream_status == status


def test_from_response_keeps_upstream_body_for_logging():
    error = errors.from_response(upstream_response(500, "x" * 5000))

    assert error.upstream_body == "x" * 2000
    assert "GET http://bc_api:8100/resources/1 returned 500" in str(error)


def test_from_response_tolerates_unread_body():
    request = httpx.Request("GET", "http://bc_api:8100/")
    response = httpx.Response(
        502, stream=httpx.ByteStream(b"never read"), request=request
    )

    assert errors.from_response(response).upstream_body == ""


@pytest.mark.parametrize(
    "exc,error_class",
    [
        (httpx.ConnectError("refused"), errors.UpstreamUnavailable),
        (httpx.RemoteProtocolError("closed"), errors.UpstreamUnavailable),
        (httpx.ReadTimeout("slow"), errors.UpstreamTimeout),
        (httpx.ConnectTimeout("slow"), errors.UpstreamTimeout),
        (httpx.DecodingError("bad gzip"), errors.BadUpstreamResponse),
    ],
)
def test_from_exception_maps_httpx_errors(exc, error_class):
    assert type(errors.from_exception(exc)) is error_class


def test_from_exception_maps_status_errors_by_response():
    response = upstream_response(403)
    exc = httpx.HTTPStatusError("denied", request=response.request, response=response)

    assert type(errors.from_exception(exc)) is errors.Forbidden


def test_from_exception_returns_integration_errors_unchanged():
    error = errors.NotFound()

    assert errors.from_exception(error) is error


def test_raise_for_status_passes_success():
    errors.raise_for_status(upstream_response(200, "{}"))


def test_raise_for_status_raises_mapped_error():
    with pytest.raises(errors.ReauthRequired):
        errors.raise_for_status(upstream_response(401))


def test_every_error_is_an_integration_error():
    subclasses = errors.IntegrationError.__subclasses__()

    assert {cls.__name__ for cls in subclasses} == {
        "ReauthRequired",
        "Forbidden",
        "NotFound",
        "InvalidRequest",
        "UpstreamTimeout",
        "UpstreamUnavailable",
        "BadUpstreamResponse",
        "InputTooLarge",
        "InvalidInput",
        "NotImplementedUpstream",
    }


def test_handler_returns_error_body(client):
    response = client.get("/upstream/401", headers={"X-Request-ID": "req-1"})

    assert response.status_code == 401
    assert response.headers["X-Request-ID"] == "req-1"
    assert response.json() == {
        "error": {
            "code": "reauth_required",
            "message": errors.ReauthRequired.message,
            "upstream_status": 401,
            "request_id": "req-1",
        }
    }


def test_handler_never_returns_upstream_detail(client):
    response = client.get("/upstream/422")

    assert response.status_code == 422
    for leaked in ("abc123", "bc_api", "Traceback"):
        assert leaked not in response.text


def test_handler_logs_upstream_body_and_traceback(client, caplog):
    with caplog.at_level(logging.ERROR, logger=errors.__name__):
        response = client.get("/upstream/500", headers={"X-Request-ID": "req-2"})

    (record,) = caplog.records
    assert "request_id=req-2" in record.getMessage()
    assert "abc123" in record.getMessage()
    assert record.exc_info is not None
    assert response.json()["error"]["request_id"] == "req-2"


def test_handler_catches_raw_httpx_errors(client):
    response = client.get("/timeout")

    assert response.status_code == 504
    assert response.json()["error"]["code"] == "upstream_timeout"
    assert response.json()["error"]["upstream_status"] is None


def test_handler_links_missing_capability(client):
    response = client.get("/bf2marc")

    assert response.status_code == 501
    assert response.json()["error"]["code"] == "not_available"
    assert BF2MARC_ISSUE in response.json()["error"]["message"]


@pytest.mark.parametrize(
    "path,http_status,code",
    [
        ("/input/too-large", 413, "input_too_large"),
        ("/input/invalid", 422, "invalid_input"),
    ],
)
def test_handler_returns_refused_input(client, path, http_status, code):
    """
    Input the integration refuses comes back in the shared error body with no
    upstream status, and the detail (sizes, parser messages) stays in the logs.
    """
    response = client.get(path)

    assert response.status_code == http_status
    assert response.json()["error"]["code"] == code
    assert response.json()["error"]["upstream_status"] is None
    assert "secret-detail" not in response.text


@pytest.mark.parametrize("supplied", ["", "abc\nevil", "x" * 65, "<script>"])
def test_handler_replaces_unsafe_request_ids(client, supplied):
    response = client.get("/upstream/404", headers={"X-Request-ID": supplied})

    request_id = response.json()["error"]["request_id"]
    assert request_id != supplied
    assert len(request_id) == 32
    assert response.headers["X-Request-ID"] == request_id


def bluecore_get(upstream) -> httpx.Response:
    """GET a work through bluecore-client in a route with the error handler

    ``upstream`` answers bluecore-client's request, as an httpx.MockTransport
    handler.
    """
    app = FastAPI()
    errors.install_error_handler(app)

    @app.get("/work")
    def work():
        client = bluecore_client.BluecoreClient(
            api_url="http://bc_api:8100",
            anonymous=True,
            load_dotenv=False,
            transport=httpx.MockTransport(upstream),
        )
        return client.works.get("w1")

    return TestClient(app).get("/work")


def answer(outcome: httpx.Response | Exception):
    """An upstream handler that returns or raises ``outcome``"""

    def upstream(request: httpx.Request) -> httpx.Response:
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return upstream


@pytest.mark.parametrize(
    "outcome,http_status,code",
    [
        (httpx.Response(401), 401, "reauth_required"),
        (httpx.Response(403), 403, "forbidden"),
        (httpx.Response(404), 404, "not_found"),
        (httpx.Response(413), 422, "invalid_request"),
        (httpx.Response(422, json={"detail": []}), 422, "invalid_request"),
        (httpx.Response(503), 502, "upstream_unavailable"),
        (httpx.ReadTimeout("slow"), 504, "upstream_timeout"),
        (httpx.ConnectError("refused"), 502, "upstream_unavailable"),
        (
            httpx.Response(301, headers={"Location": "https://elsewhere/"}),
            502,
            "bad_upstream_response",
        ),
    ],
)
def test_handler_maps_bluecore_client_errors(outcome, http_status, code):
    """
    Each upstream status or failure, raised by bluecore-client as its own
    exception, comes back as the shared error body with the mapped status.
    """
    response = bluecore_get(answer(outcome))

    assert response.status_code == http_status
    assert response.json()["error"]["code"] == code


def test_bluecore_redirect_is_not_followed():
    """
    A redirect from Blue Core becomes a 502 with upstream_status 301, and its
    Location is never requested.
    """
    hosts = []

    def upstream(request: httpx.Request) -> httpx.Response:
        hosts.append(request.url.host)
        return httpx.Response(301, headers={"Location": "https://elsewhere/"})

    response = bluecore_get(upstream)

    assert response.status_code == 502
    assert response.json()["error"]["upstream_status"] == 301
    assert hosts == ["bc_api"]


def test_from_exception_maps_auth_error_without_response():
    """
    An AuthError with no response, raised when a token has expired and there
    are no credentials to renew it, means the user has to sign in again.
    """
    exc = bluecore_client.AuthError("Access token is expired")

    assert type(errors.from_exception(exc)) is errors.ReauthRequired


def test_from_exception_maps_connection_failed_by_cause():
    """
    ConnectionFailed is mapped by the httpx error it wraps, so a timeout is a
    504 and a refused connection is a 502.
    """
    timeout = bluecore_client.ConnectionFailed("could not reach")
    timeout.__cause__ = httpx.ReadTimeout("slow")
    refused = bluecore_client.ConnectionFailed("could not reach")
    refused.__cause__ = httpx.ConnectError("refused")

    assert type(errors.from_exception(timeout)) is errors.UpstreamTimeout
    assert type(errors.from_exception(refused)) is errors.UpstreamUnavailable


@pytest.mark.parametrize(
    "exc,error_class,http_status",
    [
        (UnauthorizedException(status=401), errors.ReauthRequired, 401),
        (ForbiddenException(status=403), errors.Forbidden, 403),
        (NotFoundException(status=404), errors.NotFound, 404),
        (UnprocessableEntityException(status=422), errors.InvalidRequest, 422),
        (ServiceException(status=503), errors.UpstreamUnavailable, 502),
        (ApiException(status=301), errors.BadUpstreamResponse, 502),
    ],
)
def test_from_exception_maps_airflow_status(exc, error_class, http_status):
    """
    An Airflow client ApiException maps by its status with the same rules as
    a Blue Core response, and keeps that status as upstream_status.
    """
    error = errors.from_exception(exc)

    assert type(error) is error_class
    assert error.status_code == http_status
    assert error.upstream_status == exc.status


def test_from_exception_keeps_airflow_body_for_logging():
    """
    The Airflow body is kept for the log, cut to 2,000 characters, and left
    out of the error's message.
    """
    error = errors.from_exception(ServiceException(status=500, body="x" * 5000))

    assert error.upstream_body == "x" * 2000
    assert str(error) == "Airflow returned 500"


def test_from_exception_maps_airflow_ssl_failure():
    """
    The Airflow client reports an SSL failure as ApiException with status 0,
    which is an unavailable upstream, not an upstream status.
    """
    error = errors.from_exception(ApiException(status=0, reason="SSL failed"))

    assert type(error) is errors.UpstreamUnavailable
    assert error.upstream_status is None


AIRFLOW_POOL = urllib3.HTTPConnectionPool("airflow-apiserver", 8080)


def refused() -> urllib3.exceptions.NewConnectionError:
    connection = urllib3.connection.HTTPConnection("airflow-apiserver", 8080)
    return urllib3.exceptions.NewConnectionError(connection, "connection refused")


def read_timeout() -> urllib3.exceptions.ReadTimeoutError:
    return urllib3.exceptions.ReadTimeoutError(AIRFLOW_POOL, "/api/v2", "timed out")


def out_of_retries(reason: Exception) -> urllib3.exceptions.MaxRetryError:
    return urllib3.exceptions.MaxRetryError(AIRFLOW_POOL, "/api/v2", reason)


@pytest.mark.parametrize(
    "exc,error_class",
    [
        (read_timeout(), errors.UpstreamTimeout),
        (urllib3.exceptions.ConnectTimeoutError("slow"), errors.UpstreamTimeout),
        (refused(), errors.UpstreamUnavailable),
        (urllib3.exceptions.ProtocolError("reset"), errors.UpstreamUnavailable),
        (
            out_of_retries(read_timeout()),
            errors.UpstreamTimeout,
        ),
        (
            out_of_retries(refused()),
            errors.UpstreamUnavailable,
        ),
    ],
)
def test_from_exception_maps_urllib3_errors(exc, error_class):
    """
    urllib3 errors from the Airflow client map like httpx ones: a timeout is
    504 and anything else 502. MaxRetryError maps by the error it wraps, and a
    refused connection is 502 even though urllib3 makes NewConnectionError a
    ConnectTimeoutError.
    """
    assert type(errors.from_exception(exc)) is error_class


@pytest.mark.parametrize(
    "exc,http_status,code",
    [
        (NotFoundException(status=404), 404, "not_found"),
        (ServiceException(status=503), 502, "upstream_unavailable"),
        (
            out_of_retries(read_timeout()),
            504,
            "upstream_timeout",
        ),
    ],
)
def test_handler_maps_airflow_client_errors(exc, http_status, code):
    """
    An Airflow client or urllib3 error that a route lets propagate comes back
    as the shared error body, without the Airflow body.
    """
    app = FastAPI()
    errors.install_error_handler(app)

    @app.get("/run")
    def run():
        exc.body = SECRET_BODY
        raise exc

    response = TestClient(app).get("/run")

    assert response.status_code == http_status
    assert response.json()["error"]["code"] == code
    assert "abc123" not in response.text
