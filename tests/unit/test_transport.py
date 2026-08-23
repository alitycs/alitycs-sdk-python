from typing import List, Optional

from alitycs.transport import HttpTransport
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


def test_client_errors_are_not_retried(capture_factory):
    server = capture_factory(responder=lambda request: 422)
    transport = make_transport(server, max_retries=5)
    transport.send(make_payload())
    assert len(server.requests) == 1


def test_429_is_retried(capture_factory):
    statuses = iter([429, 202])
    server = capture_factory(responder=lambda request: next(statuses))
    transport = make_transport(server, max_retries=2)
    transport.send(make_payload())
    assert [request["status"] for request in server.requests] == [429, 202]


def test_exhausted_retries_drop_the_batch_without_raising(capture_factory):
    server = capture_factory(responder=lambda request: 500)
    transport = make_transport(server)
    transport.send(make_payload())  # must not raise
    assert len(server.requests) == 3  # initial + two retries


def test_network_failure_is_retried_then_dropped():
    # Nothing listens on port 1; every attempt raises inside urllib and is retried.
    transport = HttpTransport(
        "http://127.0.0.1:1/events",
        "pk_test",
        max_retries=2,
        retry_backoff_base=0.0,
        sleep=lambda seconds: None,
    )
    transport.send(make_payload())  # must not raise


def test_debug_logging_on_exhaustion(capture_factory, capsys):
    server = capture_factory(responder=lambda request: 503)
    transport = make_transport(server, max_retries=1, debug=True)
    transport.send(make_payload())
    captured = capsys.readouterr()
    assert "all retries exhausted" in captured.err
    assert "[Alitycs]" in captured.err


def test_no_logging_when_debug_disabled(capture_factory, capsys):
    server = capture_factory(responder=lambda request: 400)
    transport = make_transport(server)
    transport.send(make_payload())
    captured = capsys.readouterr()
    assert captured.err == ""
