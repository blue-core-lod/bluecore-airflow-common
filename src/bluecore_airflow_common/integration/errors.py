"""Errors raised by the Blue Core integration, and the handler that returns them

Every failure talking to an upstream service (the Blue Core API or Airflow) is
raised as an ``IntegrationError`` subclass. ``install_error_handler(app)``
turns each one into a response with this body::

    {"error": {"code": ..., "message": ..., "upstream_status": ..., "request_id": ...}}

The message is fixed by the error class. Upstream bodies and stack traces are
logged under the ``request_id`` and are never returned.
"""

import logging
import re
import uuid
from typing import Any, ClassVar

import bluecore_client
import httpx
import urllib3
from airflow_client.client.exceptions import ApiException
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
_SAFE_REQUEST_ID = re.compile(r"[A-Za-z0-9._-]{1,64}")
_BODY_LOG_LIMIT = 2000


class IntegrationError(Exception):
    """Base class for every error the integration raises"""

    status_code: ClassVar[int] = 502
    code: ClassVar[str] = "upstream_unavailable"
    message: ClassVar[str] = "The upstream service is unavailable"

    def __init__(
        self,
        detail: str = "",
        *,
        upstream_status: int | None = None,
        upstream_body: str | None = None,
    ) -> None:
        super().__init__(detail or self.message)
        self.upstream_status = upstream_status
        self.upstream_body = upstream_body

    def public_message(self) -> str:
        return self.message

    def body(self, request_id: str) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.public_message(),
                "upstream_status": self.upstream_status,
                "request_id": request_id,
            }
        }


class ReauthRequired(IntegrationError):
    status_code = 401
    code = "reauth_required"
    message = "Your session has expired. Sign in again."


class Forbidden(IntegrationError):
    status_code = 403
    code = "forbidden"
    message = "You do not have permission to do this"


class NotFound(IntegrationError):
    status_code = 404
    code = "not_found"
    message = "Not found"


class InvalidRequest(IntegrationError):
    status_code = 422
    code = "invalid_request"
    message = "The upstream service rejected the request"


class UpstreamTimeout(IntegrationError):
    status_code = 504
    code = "upstream_timeout"
    message = "The upstream service did not respond in time"


class UpstreamUnavailable(IntegrationError):
    pass


class BadUpstreamResponse(IntegrationError):
    code = "bad_upstream_response"
    message = "The upstream service sent a response that could not be used"


class NotImplementedUpstream(IntegrationError):
    """A capability the upstream service doesn't provide yet"""

    status_code = 501
    code = "not_available"
    message = "This is not available yet"

    def __init__(self, capability: str, issue_url: str) -> None:
        super().__init__(f"{capability} is not available upstream, see {issue_url}")
        self.issue_url = issue_url

    def public_message(self) -> str:
        return f"{self.message}, see {self.issue_url}"


_BY_STATUS: dict[int, type[IntegrationError]] = {
    401: ReauthRequired,
    403: Forbidden,
    404: NotFound,
    413: InvalidRequest,
    422: InvalidRequest,
}


def _body_excerpt(response: httpx.Response) -> str:
    try:
        return response.text[:_BODY_LOG_LIMIT]
    except httpx.ResponseNotRead:
        return ""


def _error_class(status: int) -> type[IntegrationError]:
    default = UpstreamUnavailable if status >= 500 else BadUpstreamResponse
    return _BY_STATUS.get(status, default)


def from_response(response: httpx.Response) -> IntegrationError:
    """The error for an unsuccessful upstream response"""
    status = response.status_code
    error_class = _error_class(status)
    return error_class(
        f"{response.request.method} {response.request.url} returned {status}",
        upstream_status=status,
        upstream_body=_body_excerpt(response),
    )


def from_exception(exc: Exception) -> IntegrationError:
    """The error for an exception raised by httpx or bluecore client or the
    Airflow"""
    if isinstance(exc, IntegrationError):
        return exc
    if isinstance(exc, bluecore_client.APIError) and exc.response is not None:
        return from_response(exc.response)
    if isinstance(exc, bluecore_client.AuthError):
        return ReauthRequired(str(exc))
    if isinstance(exc, ApiException) and exc.status:
        return _error_class(exc.status)(
            f"Airflow returned {exc.status}",
            upstream_status=exc.status,
            upstream_body=(exc.body or "")[:_BODY_LOG_LIMIT],
        )
    if isinstance(exc, urllib3.exceptions.MaxRetryError) and exc.reason:
        return from_exception(exc.reason)
    if isinstance(exc, urllib3.exceptions.NewConnectionError):
        return UpstreamUnavailable(repr(exc))
    if isinstance(exc, urllib3.exceptions.TimeoutError):
        return UpstreamTimeout(repr(exc))
    if isinstance(exc.__cause__, httpx.HTTPError):
        return from_exception(exc.__cause__)
    if isinstance(exc, httpx.HTTPStatusError):
        return from_response(exc.response)
    if isinstance(exc, httpx.TimeoutException):
        return UpstreamTimeout(repr(exc))
    if isinstance(exc, httpx.DecodingError):
        return BadUpstreamResponse(repr(exc))
    return UpstreamUnavailable(repr(exc))


def raise_for_status(response: httpx.Response) -> None:
    """Raise the matching IntegrationError unless the response is a 2xx"""
    if not response.is_success:
        raise from_response(response)


def request_id(request: Request) -> str:
    """The caller's X-Request-ID if it is safe to log, otherwise a new one"""
    supplied = request.headers.get(REQUEST_ID_HEADER, "")
    if _SAFE_REQUEST_ID.fullmatch(supplied):
        return supplied
    return uuid.uuid4().hex


async def _handle(request: Request, exc: Exception) -> JSONResponse:
    error = from_exception(exc)
    rid = request_id(request)
    logger.error(
        "%s %s failed [request_id=%s]: %s; upstream body: %r",
        request.method,
        request.url.path,
        rid,
        error,
        error.upstream_body,
        exc_info=exc,
    )
    return JSONResponse(
        error.body(rid),
        status_code=error.status_code,
        headers={REQUEST_ID_HEADER: rid},
    )


def install_error_handler(app: FastAPI) -> None:
    """Return IntegrationError and httpx bluecore-client and Airflow client errors
    as the shared error body"""
    app.add_exception_handler(IntegrationError, _handle)
    app.add_exception_handler(httpx.HTTPError, _handle)
    app.add_exception_handler(bluecore_client.BluecoreError, _handle)
    app.add_exception_handler(ApiException, _handle)
    app.add_exception_handler(urllib3.exceptions.HTTPError, _handle)
