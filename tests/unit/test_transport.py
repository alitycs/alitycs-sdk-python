import os
import subprocess
import sys
import time
from email.utils import formatdate
from typing import List, Optional

import pytest

from alitycs.persistence import FileBatchStore
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


def test_429_retry_after_is_not_shortened_to_client_backoff_cap(capture_factory):
    responses = iter([(429, {"Retry-After": "3600"}), 202])
    server = capture_factory(responder=lambda request: next(responses))
    sleeps: List[float] = []
    transport = make_transport(server, sleeps=sleeps, max_retries=1)
    transport.send(make_payload())

    assert sleeps == [3600.0]


def test_parse_retry_after_variants():
    assert parse_retry_after({"Retry-After": "5"}) == 5.0
    assert parse_retry_after({"Retry-After": " 120 "}) == 120.0
    assert parse_retry_after({}) is None
    assert parse_retry_after(None) is None
    assert parse_retry_after({"Retry-After": "soon"}) is None
    assert parse_retry_after({"Retry-After": "1.5"}) is None
    # A date in the past clamps to zero instead of going negative.
    past = formatdate(time.time() - 60, usegmt=True)
    assert parse_retry_after({"Retry-After": past}) == 0.0
    future = formatdate(time.time() + 30, usegmt=True)
    assert 28.0 <= parse_retry_after({"Retry-After": future}) <= 30.0
    assert parse_retry_after({"Retry-After": "100000000"}) == 3600.0
    far_future = formatdate(time.time() + 7200, usegmt=True)
    assert parse_retry_after({"Retry-After": far_future}) == 3600.0


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


def test_persisted_batch_is_replayed_byte_identically_after_restart(capture_factory, tmp_path):
    server = capture_factory(fail_on=(1,))
    state_file = tmp_path / "alitycs-wal.json"
    first = make_transport(server, max_retries=0, persistence_path=str(state_file))

    outcome = first.send(make_payload("restart"))
    assert isinstance(outcome, SendFailed)
    assert outcome.durable is True
    assert first.durable_pending_events == 1
    assert state_file.exists()
    first.close()

    restarted = make_transport(server, max_retries=0, persistence_path=str(state_file))
    assert restarted.recover() is True
    assert restarted.durable_pending_events == 0
    assert server.requests[0]["raw"] == server.requests[1]["raw"]
    assert not state_file.exists()


def test_persist_stores_without_network_then_recovery_delivers(capture_server, tmp_path):
    state_file = tmp_path / "alitycs-wal.json"
    transport = make_transport(capture_server, max_retries=0, persistence_path=str(state_file))
    payload = make_payload("shutdown")

    assert transport.persist(payload) is True
    assert transport.durable_pending_events == 1
    assert capture_server.requests == []

    assert transport.recover() is True
    assert transport.durable_pending_events == 0
    assert capture_server.requests[0]["payload"]["batchId"] == payload.batch_id


def test_durable_pending_snapshot_reports_active_overlap(capture_server, tmp_path):
    transport = make_transport(
        capture_server,
        max_retries=0,
        persistence_path=str(tmp_path / "pending-snapshot-wal.json"),
    )
    payload = make_payload("snapshot")

    assert transport.persist(payload) is True
    assert transport.durable_pending_snapshot([payload.batch_id]) == (1, 1)
    assert transport.durable_pending_snapshot(["batch_elsewhere"]) == (1, 0)
    transport.close()


@pytest.mark.skipif(os.name == "nt", reason="POSIX persistence parent validation")
def test_unavailable_persistence_parent_fails_initialization_without_network(
    capture_server, tmp_path
):
    parent_file = tmp_path / "parent-file"
    parent_file.write_text("not a directory", encoding="utf-8")
    with pytest.raises(ValueError, match="unavailable"):
        make_transport(
            capture_server,
            max_retries=0,
            persistence_path=str(parent_file / "wal.json"),
        )
    assert capture_server.requests == []


def test_restart_honours_persisted_retry_after_deadline(capture_factory, tmp_path):
    server = capture_factory(
        responder=lambda request: (429, {"Retry-After": "3"}) if request["sequence"] == 1 else 202
    )
    state_file = tmp_path / "alitycs-wal.json"
    first = make_transport(server, max_retries=0, persistence_path=str(state_file))
    assert isinstance(first.send(make_payload("paused")), SendFailed)
    first.close()

    sleeps: List[float] = []
    restarted = make_transport(
        server,
        sleeps=sleeps,
        max_retries=0,
        persistence_path=str(state_file),
    )
    assert restarted.recover() is True
    assert sleeps and sleeps[0] >= 2.5
    assert server.requests[0]["raw"] == server.requests[1]["raw"]


def test_corrupt_persistence_file_fails_initialization(tmp_path):
    state_file = tmp_path / "alitycs-wal.json"
    state_file.write_text("not-json", encoding="utf-8")
    try:
        HttpTransport("http://127.0.0.1:1/events", "pk", persistence_path=str(state_file))
    except ValueError as error:
        assert "Invalid Alitycs persistence file" in str(error)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("corrupt persistence state must fail initialization")


def test_terminal_recovery_acknowledges_and_continues(capture_factory, tmp_path):
    state_file = tmp_path / "alitycs-wal.json"
    store = FileBatchStore(str(state_file))
    store.put("batch_rejected", b'{"batchId":"batch_rejected"}', 1)
    store.put("batch_healthy", b'{"batchId":"batch_healthy"}', 1)
    store.close()
    responses = iter([400, 202])
    server = capture_factory(responder=lambda request: next(responses))
    transport = make_transport(server, max_retries=0, persistence_path=str(state_file))

    assert transport.recover() is True
    assert len(server.requests) == 2
    assert transport.durable_pending_events == 0


def test_retry_after_from_503_is_persisted_and_honoured_after_restart(
    capture_factory, tmp_path
):
    server = capture_factory(
        responder=lambda request: (503, {"Retry-After": "300"})
        if request["sequence"] == 1
        else 202
    )
    state_file = tmp_path / "alitycs-wal.json"
    first = make_transport(server, max_retries=0, persistence_path=str(state_file))
    outcome = first.send(make_payload("paused-503"))
    assert isinstance(outcome, SendFailed)
    assert outcome.retry_after_until is not None
    first.close()

    sleeps: List[float] = []
    restarted = make_transport(
        server,
        sleeps=sleeps,
        max_retries=0,
        persistence_path=str(state_file),
    )
    assert restarted.recover() is True
    assert sleeps and 299.0 <= sleeps[0] <= 300.0


def test_recover_returns_before_pause_that_outlives_deadline(capture_factory, tmp_path):
    server = capture_factory(responder=lambda request: (429, {"Retry-After": "300"}))
    state_file = tmp_path / "alitycs-wal.json"
    first = make_transport(server, max_retries=0, persistence_path=str(state_file))
    assert isinstance(first.send(make_payload("deadline")), SendFailed)
    first.close()

    sleeps: List[float] = []
    restarted = make_transport(
        server,
        sleeps=sleeps,
        max_retries=0,
        persistence_path=str(state_file),
    )
    started_at = time.monotonic()
    assert restarted.recover(time.monotonic() + 0.05) is False
    assert time.monotonic() - started_at < 0.5
    assert sleeps == []
    assert len(server.requests) == 1
    assert restarted.durable_pending_events == 1


def test_send_deadline_skips_retry_after_sleep_and_retains_batch(capture_factory, tmp_path):
    server = capture_factory(responder=lambda request: (429, {"Retry-After": "300"}))
    sleeps: List[float] = []
    transport = make_transport(
        server,
        sleeps=sleeps,
        max_retries=1,
        persistence_path=str(tmp_path / "alitycs-wal.json"),
    )

    outcome = transport.send(make_payload("bounded"), time.monotonic() + 0.05)

    assert isinstance(outcome, SendFailed)
    assert outcome.durable is True
    assert sleeps == []
    assert len(server.requests) == 1
    assert transport.durable_pending_events == 1


def test_fork_reset_detaches_child_without_removing_parent_wal(capture_factory, tmp_path):
    state_file = tmp_path / "alitycs-wal.json"
    server = capture_factory(fail_on=(1,))
    transport = make_transport(server, max_retries=0, persistence_path=str(state_file))
    assert isinstance(transport.send(make_payload("parent")), SendFailed)

    transport.reset_for_child()

    assert transport.durable_enabled is False
    assert transport.durable_pending_events == 0
    assert state_file.exists()


def test_store_rolls_back_memory_when_persist_fails(monkeypatch, tmp_path):
    store = FileBatchStore(str(tmp_path / "alitycs-wal.json"))

    def fail() -> None:
        raise OSError("disk full")

    monkeypatch.setattr(store, "_persist", fail)
    with pytest.raises(OSError, match="disk full"):
        store.put("batch_new", b"{}", 3)
    assert store.pending_events == 0


@pytest.mark.parametrize(
    "paused_until",
    [float("nan"), float("inf"), float("-inf"), 10**10000],
    ids=["nan", "positive-infinity", "negative-infinity", "oversized-integer"],
)
def test_store_rejects_non_finite_pause_without_mutating_wal(tmp_path, paused_until):
    state_file = tmp_path / "alitycs-wal.json"
    store = FileBatchStore(str(state_file))
    store.put("batch", b"{}", 1)
    original = state_file.read_bytes()

    with pytest.raises(ValueError, match="finite"):
        store.pause("batch", paused_until)

    assert store.snapshot()[0]["paused_until"] is None
    assert state_file.read_bytes() == original


def test_store_pending_event_limit_bounds_wal_growth(tmp_path):
    store = FileBatchStore(str(tmp_path / "alitycs-wal.json"), max_pending_events=2)
    store.put("batch_first", b"{}", 2)
    with pytest.raises(ValueError, match="event limit"):
        store.put("batch_overflow", b"{}", 1)
    assert store.pending_events == 2


def test_store_rejects_overlapping_live_owners_and_allows_reopen_after_close(tmp_path):
    state_file = tmp_path / "alitycs-wal.json"
    first = FileBatchStore(str(state_file))

    with pytest.raises(ValueError, match="already in use"):
        FileBatchStore(str(state_file))

    first.close()
    reopened = FileBatchStore(str(state_file))
    reopened.close()


@pytest.mark.skipif(os.name == "nt", reason="POSIX advisory lock")
def test_store_rejects_cross_process_owner(tmp_path):
    state_file = tmp_path / "alitycs-wal.json"
    first = FileBatchStore(str(state_file))
    script = """
import sys
from alitycs.persistence import FileBatchStore

try:
    store = FileBatchStore(sys.argv[1])
except ValueError:
    raise SystemExit(17)
store.close()
"""

    blocked = subprocess.run([sys.executable, "-c", script, str(state_file)], check=False)
    assert blocked.returncode == 17

    first.close()
    reopened = subprocess.run([sys.executable, "-c", script, str(state_file)], check=False)
    assert reopened.returncode == 0


def test_store_rejects_invalid_or_oversized_persistence_limit(tmp_path):
    with pytest.raises(ValueError, match="positive"):
        FileBatchStore(None, max_pending_events=0)

    state_file = tmp_path / "alitycs-wal.json"
    state_file.write_text(
        '{"version":1,"batches":[{"batch_id":"batch","body":"{}",'
        '"event_count":2,"paused_until":null}]}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Invalid Alitycs persistence file"):
        FileBatchStore(str(state_file), max_pending_events=1)


@pytest.mark.parametrize(
    "batches",
    [
        [{"batch_id": 42, "body": "{}", "event_count": 1, "paused_until": None}],
        [{"batch_id": "batch", "body": {}, "event_count": 1, "paused_until": None}],
        [{"batch_id": "batch", "body": "{}", "event_count": True, "paused_until": None}],
        [{"batch_id": "batch", "body": "{}", "event_count": 1, "paused_until": float("inf")}],
        [{"batch_id": "batch", "body": "{}", "event_count": 1, "paused_until": 10**4000}],
        [
            {"batch_id": "duplicate", "body": "{}", "event_count": 1, "paused_until": None},
            {"batch_id": "duplicate", "body": "{}", "event_count": 1, "paused_until": None},
        ],
    ],
)
def test_store_rejects_invalid_and_duplicate_records(tmp_path, batches):
    import json

    state_file = tmp_path / "alitycs-wal.json"
    state_file.write_text(json.dumps({"version": 1, "batches": batches}), encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid Alitycs persistence file"):
        FileBatchStore(str(state_file))
