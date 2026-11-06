"""Bounded retry for idempotent requests

``RetryTransport`` wraps another httpx transport. It retries GET and HEAD
requests only, and only after a connect error, a connect or read timeout, or
a 502, 503 or 504 response. Every other method, POST and PATCH included, is
sent exactly once.

Waits use exponential backoff with full jitter, or the server's
``Retry-After`` when it sends one. No retry starts if it would end after the
overall deadline; the last error or response is returned instead.

Only failures before the response headers arrive are retried here. A
timeout while the client reads the body is not.
"""

import email.utils
import logging
import math
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime

import httpx

logger = logging.getLogger(__name__)

RETRY_METHODS = frozenset({"GET", "HEAD"})
RETRY_STATUSES = frozenset({502, 503, 504})
RETRY_EXCEPTIONS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout)


def retry_after(response: httpx.Response) -> float | None:
    """Seconds to wait from a Retry-After header, or None if absent or invalid"""
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - datetime.now(UTC)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


class RetryTransport(httpx.BaseTransport):
    """An httpx transport that retries idempotent requests a bounded number of times"""

    def __init__(
        self,
        transport: httpx.BaseTransport,
        *,
        max_retries: int,
        backoff: float = 0.5,
        max_backoff: float = 8.0,
        deadline: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._transport = transport
        self._max_retries = max_retries
        self._backoff = backoff
        self._max_backoff = max_backoff
        self._deadline = deadline
        self._sleep = sleep
        self._clock = clock

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if request.method not in RETRY_METHODS:
            return self._transport.handle_request(request)

        give_up_at = self._clock() + self._deadline
        attempt = 0
        while True:
            try:
                response = self._transport.handle_request(request)
            except RETRY_EXCEPTIONS as exc:
                delay = self._delay(attempt, None, give_up_at)
                if delay is None:
                    raise
                reason = type(exc).__name__
            else:
                if response.status_code not in RETRY_STATUSES:
                    return response
                delay = self._delay(attempt, retry_after(response), give_up_at)
                if delay is None:
                    return response
                response.close()
                reason = str(response.status_code)

            attempt += 1
            logger.warning(
                "Retrying %s %s after %s in %.2fs (retry %d of %d)",
                request.method,
                request.url,
                reason,
                delay,
                attempt,
                self._max_retries,
            )
            self._sleep(delay)

    def _delay(
        self, attempt: int, server_delay: float | None, give_up_at: float
    ) -> float | None:
        """Seconds to wait before the next try, or None to stop retrying"""
        if attempt >= self._max_retries:
            return None
        if server_delay is None:
            ceiling = min(self._max_backoff, self._backoff * 2**attempt)
            delay = random.uniform(0, ceiling)
        else:
            delay = server_delay
        if self._clock() + delay > give_up_at:
            return None
        return delay

    def close(self) -> None:
        self._transport.close()
