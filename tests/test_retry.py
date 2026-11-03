from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from bluecore_airflow_common.integration import retry


class FakeTime:
    """A clock that only moves when the transport sleeps"""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class Upstream:
    """A scripted upstream; the last outcome repeats once the others are used"""

    def __init__(self, *outcomes: int | type[Exception] | httpx.Response) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request.method)
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, httpx.Response):
            return outcome
        if isinstance(outcome, int):
            return httpx.Response(outcome)
        raise outcome("scripted failure")


@pytest.fixture
def fake_time() -> FakeTime:
    return FakeTime()


def make_client(
    upstream: Upstream, fake_time: FakeTime, **options
) -> httpx.AsyncClient:
    options.setdefault("max_retries", 2)
    transport = retry.RetryTransport(
        httpx.MockTransport(upstream),
        sleep=fake_time.sleep,
        clock=fake_time.clock,
        **options,
    )
    return httpx.AsyncClient(transport=transport, base_url="http://bc_api:8100")


def with_retry_after(value: str) -> httpx.Response:
    return httpx.Response(503, headers={"Retry-After": value})


@pytest.mark.parametrize(
    "value,expected",
    [
        ("3", 3.0),
        ("1.5", 1.5),
        ("0", 0.0),
        ("-4", 0.0),
        ("soon", None),
        ("nan", None),
        ("inf", None),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0),
    ],
)
def test_retry_after_parses_header(value, expected):
    assert retry.retry_after(with_retry_after(value)) == expected


def test_retry_after_parses_future_http_date():
    when = datetime.now(UTC) + timedelta(seconds=30)

    seconds = retry.retry_after(with_retry_after(format_datetime(when, usegmt=True)))

    assert seconds is not None
    assert 28 <= seconds <= 30


def test_retry_after_missing_header():
    assert retry.retry_after(httpx.Response(503)) is None


@pytest.mark.parametrize("failure", [502, 503, 504])
async def test_get_retries_retryable_status(fake_time, failure):
    """
    Every status (502, 503, 504) gets exactly one retry, then succeeds.
    """
    upstream = Upstream(failure, 200)

    async with make_client(upstream, fake_time) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 200
    assert upstream.calls == ["GET", "GET"]


@pytest.mark.parametrize(
    "failure", [httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout]
)
async def test_get_retries_retryable_exception(fake_time, failure):
    """
    Every exception in RETRY_EXCEPTIONS gets exactly one retry, then succeeds
    """
    upstream = Upstream(failure, 200)

    async with make_client(upstream, fake_time) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 200
    assert upstream.calls == ["GET", "GET"]


async def test_head_is_retried(fake_time):
    upstream = Upstream(503, 200)

    async with make_client(upstream, fake_time) as client:
        response = await client.head("/resources/1")

    assert response.status_code == 200
    assert upstream.calls == ["HEAD", "HEAD"]


async def test_gives_up_with_last_response(fake_time):
    """
    With max_retries=2 there are 3 calls in total: one attempt plus two retries.
    Then the last response is returned.
    """
    upstream = Upstream(503)

    async with make_client(upstream, fake_time, max_retries=2) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 503
    assert len(upstream.calls) == 3


async def test_gives_up_by_raising_last_exception(fake_time):
    """
    With max_retries=2 there are 3 calls in total: one attempt plus two retries.
    Then the last exception is re-raised unchanged
    """
    upstream = Upstream(httpx.ReadTimeout)

    async with make_client(upstream, fake_time, max_retries=2) as client:
        with pytest.raises(httpx.ReadTimeout):
            await client.get("/resources/1")

    assert len(upstream.calls) == 3


async def test_zero_retries_sends_once(fake_time):
    upstream = Upstream(503)

    async with make_client(upstream, fake_time, max_retries=0) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 503
    assert upstream.calls == ["GET"]
    assert fake_time.sleeps == []


@pytest.mark.parametrize("status", [200, 404, 429, 500, 501])
async def test_other_statuses_are_not_retried(fake_time, status):
    upstream = Upstream(status)

    async with make_client(upstream, fake_time) as client:
        response = await client.get("/resources/1")

    assert response.status_code == status
    assert upstream.calls == ["GET"]


@pytest.mark.parametrize(
    "failure", [httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError]
)
async def test_other_exceptions_are_not_retried(fake_time, failure):
    upstream = Upstream(failure)

    async with make_client(upstream, fake_time) as client:
        with pytest.raises(failure):
            await client.get("/resources/1")

    assert upstream.calls == ["GET"]


@pytest.mark.parametrize("method", ["POST", "PATCH", "PUT", "DELETE"])
@pytest.mark.parametrize("failure", [503, httpx.ConnectError, httpx.ReadTimeout])
async def test_unsafe_methods_are_never_retried(fake_time, method, failure):
    """
    A 4 x 3 grid of 12 cases: each unsafe method, against each kind of retryable failure, with max_retries=5
    so that only the method check can stop a retry. The script is Upstream(failure, 200), so a wrongly
    retried request would succeed. The test therefore can't pass by accident because a retry also failed.
    """
    upstream = Upstream(failure, 200)

    async with make_client(upstream, fake_time, max_retries=5) as client:
        try:
            await client.request(method, "/resources/1")
        except httpx.TransportError:
            pass

    assert upstream.calls == [method]
    assert fake_time.sleeps == []


async def test_backoff_doubles_up_to_the_cap(fake_time, monkeypatch):
    """
    Replaces random.uniform with a version that always returns the upper limit. That
    turns the jittered wait into its ceiling, so the exact schedule 0.5, 1, 2, 4, 8 can be asserted.
    This checks both the doubling and the 8-second cap: with max_retries=5 the fifth wait would be
    8 even without a cap, so the cap is what keeps later waits from growing.
    """
    monkeypatch.setattr(retry.random, "uniform", lambda low, high: high)
    upstream = Upstream(503)

    async with make_client(upstream, fake_time, max_retries=5, deadline=60) as client:
        await client.get("/resources/1")

    assert fake_time.sleeps == [0.5, 1.0, 2.0, 4.0, 8.0]


async def test_backoff_is_jittered(fake_time):
    """
    Uses the real random and checks only that each wait falls between 0 and its ceiling.
    zip(..., strict=True) also fails if the number of waits is wrong.
    """
    upstream = Upstream(503)

    async with make_client(upstream, fake_time, max_retries=5, deadline=60) as client:
        await client.get("/resources/1")

    ceilings = [0.5, 1.0, 2.0, 4.0, 8.0]
    assert all(0 <= s <= c for s, c in zip(fake_time.sleeps, ceilings, strict=True))


async def test_retry_after_replaces_backoff(fake_time):
    """
    The wait is exactly the server's Retry-After of 3 seconds, not a jittered value.
    """
    upstream = Upstream(with_retry_after("3"), 200)

    async with make_client(upstream, fake_time) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 200
    assert fake_time.sleeps == [3.0]


async def test_retry_after_past_deadline_stops(fake_time):
    """
    A 120-second Retry-After with a 30-second deadline means no wait and no second call.
    The 503 comes back right away, which errors.py turns into 502. The script ends in a
    200, so a wrongly retried request would succeed and the test would fail.
    """
    upstream = Upstream(with_retry_after("120"), 200)

    async with make_client(upstream, fake_time, deadline=30) as client:
        response = await client.get("/resources/1")

    assert response.status_code == 503
    assert upstream.calls == ["GET"]
    assert fake_time.sleeps == []


async def test_deadline_bounds_total_wait(fake_time):
    """
    Shows the deadline, not the retry count of 4, is what stopped it.
    """
    upstream = Upstream(with_retry_after("4"))

    async with make_client(upstream, fake_time, max_retries=5, deadline=10) as client:
        await client.get("/resources/1")

    assert fake_time.sleeps == [4.0, 4.0]
    assert len(upstream.calls) == 3


async def test_discarded_responses_are_closed(fake_time):
    first = httpx.Response(503)
    upstream = Upstream(first, 200)

    async with make_client(upstream, fake_time) as client:
        await client.get("/resources/1")

    assert first.is_closed


async def test_retries_are_logged(fake_time, caplog):
    upstream = Upstream(503, 200)

    async with make_client(upstream, fake_time) as client:
        await client.get("/resources/1")

    (record,) = caplog.records
    assert "GET http://bc_api:8100/resources/1 after 503" in record.getMessage()
    assert "retry 1 of 2" in record.getMessage()


async def test_aclose_closes_wrapped_transport():
    class Wrapped(httpx.AsyncBaseTransport):
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    wrapped = Wrapped()

    await retry.RetryTransport(wrapped, max_retries=2).aclose()

    assert wrapped.closed
