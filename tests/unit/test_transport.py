import time
from email.utils import formatdate
from typing import List, Optional

from alitycs.transport import HttpTransport, SendFailed, SendRejected, SendSuccess, parse_retry_after
from alitycs.types import AnalyticsEvent, BatchPayload, EventContext, EventType
from tests.conftest import CaptureServer


def make_payload(event_name="e1") -> BatchPayload:
    event = AnalyticsEvent(
        event_id=f"evt_{event_name}",
        event=event_name,
        event_type=EventType.TRACK,
        anonymous_id="anon_1",
        session_id="sess_1",
        timestamp=1_700_000_000_000,
        properties={},
        context=EventContext(sdk_version="1.0.0", sdk_language="python"),
    )
    return BatchPayload(batch_id="batch_test", sent_at=1, events=[event])


def make_transport(server: CaptureServer, sleeps: Optional[List[float]] = None, **overrides) -> HttpTransport:
    def _sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    params = dict(max_retries=2, retry_backoff_base=0.0, sleep=_sleep)
    params.update(overrides)
    return HttpTransport(server.url, "pk_test", **params)


def test_successful_post_sends_headers_and_body(capture_server):
    transport = make_transport(capture_server)
    transport.send(make_payload())

    assert len(capture_server.requests) == 1
    request = capture_server.requests[0]
    assert request["headers"]["authorization"] == "Bearer pk_test"
    assert request["headers"]["content-type"] == "application/json"
    body = request["payload"]
    assert body["batchId"] == "batch_test"
    assert body["sentAt"] == 1
    assert body["events"][0]["eventType"] == "track"


def test_server_error_is_retried_until_success(capture_factory):
    server = capture_factory(fail_on=(1, 2))
    sleeps: List[float] = []
    transport = make_transport(server, sleeps=sleeps, max_retries=3, retry_backoff_base=1.0)
    transport.send(make_payload())

    assert [request["status"] for request in server.requests] == [500, 500, 202]
    # Backoff doubles from the base.
    assert sleeps == [1.0, 2.0]


def test_backoff_is_capped_at_ten_seconds(capture_factory):
    # Three failures force three retries: 8.0 doubles to 16→capped 10, then 32→capped 10.
    server = capture_factory(fail_on=(1, 2, 3))
    sleeps: List[float] = []
    transport = make_transport(server, sleeps=sleeps, max_retries=4, retry_backoff_base=8.0)
    transport.send(make_payload())
    assert [request["status"] for request in server.requests] == [500, 500, 500, 202]
    assert sleeps == [8.0, 10.0, 10.0]


def test_client_errors_are_not_retried_and_reported_as_rejections(capture_factory):
    server = capture_factory(responder=lambda request: 422)
    transport = make_transport(server, max_retries=5)
    outcome = transport.send(make_payload())
    assert len(server.requests) == 1
    assert isinstance(outcome, SendRejected)
    assert outcome.status == 422
    assert outcome.is_batch_reject is False


def test_400_is_reported_as_a_whole_batch_rejection(capture_factory):
    server = capture_factory(responder=lambda request: 400)
    transport = make_transport(server, max_retries=5)
    outcome = transport.send(make_payload())
    assert isinstance(outcome, SendRejected)
    assert outcome.is_batch_reject is True


def test_429_is_retried(capture_factory):
    statuses = iter([429, 202])
    server = capture_factory(responder=lambda request: next(statuses))
    transport = make_transport(server, max_retries=2)
    outcome = transport.send(make_payload())
    assert [request["status"] for request in server.requests] == [429, 202]
    assert isinstance(outcome, SendSuccess)


def test_429_retry_after_seconds_is_honoured(capture_factory):
    """A structured Retry-After replaces the default backoff for the next attempt."""
    responses = iter([(429, {"Retry-After": "2"}), 202])
    server = capture_factory(responder=lambda request: next(responses))
    sleeps: List[float] = []
    transport = make_transport(server, sleeps=sleeps, max_retries=1)
    transport.send(make_payload())

    assert [request["status"] for request in server.requests] == [429, 202]
    # Even with retry_backoff_base=0 (the make_transport default) the wait is the
    # full server-suggested 2s.
    assert sleeps == [2.0]


def test_429_retry_after_http_date_is_honoured(capture_factory):
    responses = iter([(429, {"Retry-After": formatdate(time.time() + 3, usegmt=True)}), 202])
    server = capture_factory(responder=lambda request: next(responses))
    sleeps: List[float] = []
    transport = make_transport(server, sleeps=sleeps, max_retries=1)
    transport.send(make_payload())

    assert [request["status"] for request in server.requests] == [429, 202]
    # Wall time passes between the header being formatted and parsed; the default
    # backoff here is 0s, so anything well above that proves the date was honoured.
    assert 2.0 <= sleeps[0] <= 3.0


def test_429_retry_after_is_capped_at_ten_seconds(capture_factory):
    responses = iter([(429, {"Retry-After": "3600"}), 202])
    server = capture_factory(responder=lambda request: next(responses))
    sleeps: List[float] = []
    transport = make_transport(server, sleeps=sleeps, max_retries=1)
    transport.send(make_payload())

    assert sleeps == [10.0]


def test_parse_retry_after_variants():
    assert parse_retry_after({"Retry-After": "5"}) == 5.0
    assert parse_retry_after({"Retry-After": " 120 "}) == 120.0
    assert parse_retry_after({}) is None
    assert parse_retry_after(None) is None
    assert parse_retry_after({"Retry-After": "soon"}) is None
    # A date in the past clamps to zero instead of going negative.
    past = formatdate(time.time() - 60, usegmt=True)
    assert parse_retry_after({"Retry-After": past}) == 0.0
    future = formatdate(time.time() + 30, usegmt=True)
    assert 28.0 <= parse_retry_after({"Retry-After": future}) <= 30.0


def test_exhausted_retries_drop_the_batch_without_raising(capture_factory):
    server = capture_factory(responder=lambda request: 500)
    transport = make_transport(server)
    outcome = transport.send(make_payload())  # must not raise
    assert len(server.requests) == 3  # initial + two retries
    assert isinstance(outcome, SendFailed)


def test_network_failure_is_retried_then_dropped():
    # Nothing listens on port 1; every attempt raises inside urllib and is retried.
    transport = HttpTransport(
        "http://127.0.0.1:1/events",
        "pk_test",
        max_retries=2,
        retry_backoff_base=0.0,
        sleep=lambda seconds: None,
    )
    outcome = transport.send(make_payload())  # must not raise
    assert isinstance(outcome, SendFailed)


def test_debug_logging_on_exhaustion(capture_factory, capsys):
    server = capture_factory(responder=lambda request: 503)
    transport = make_transport(server, max_retries=1, debug=True)
    transport.send(make_payload())
    captured = capsys.readouterr()
    assert "all retries exhausted" in captured.err
    assert "[Alitycs]" in captured.err


def test_final_failures_are_warned_even_when_debug_is_disabled(capture_factory, capsys):
    """Delivery failures are never silent: dropped batches must be visible without
    opting into debug logging."""
    server = capture_factory(responder=lambda request: 400)
    transport = make_transport(server)
    transport.send(make_payload())
    captured = capsys.readouterr()
    assert "WARN" in captured.err
    assert "not retrying" in captured.err
