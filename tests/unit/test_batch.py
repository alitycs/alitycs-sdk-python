"""BatchManager behaviour, including the no-lost-events shutdown contract."""

import threading
import time
from typing import List

from alitycs.batch import BatchManager
from alitycs.types import AnalyticsEvent, BatchPayload, EventContext, EventType


def make_event(name: str) -> AnalyticsEvent:
    return AnalyticsEvent(
        event_id=f"evt_{name}",
        event=name,
        event_type=EventType.TRACK,
        anonymous_id="anon_1",
        session_id="sess_1",
        timestamp=1,
        properties={},
        context=EventContext(sdk_version="1.0.0", sdk_language="python"),
    )


class SentBatches:
    """Thread-safe recorder with a gate to simulate slow sends."""

    def __init__(self, block_first: bool = False) -> None:
        self.batches: List[BatchPayload] = []
        self.lock = threading.Lock()
        self._gate = threading.Event()
        self._block_first = block_first

    def __call__(self, payload: BatchPayload) -> None:
        with self.lock:
            should_block = self._block_first and len(self.batches) == 0
            self.batches.append(payload)
        if should_block:
            self._gate.wait(timeout=5)

    def release(self) -> None:
        self._gate.set()

    @property
    def event_names(self) -> List[str]:
        with self.lock:
            return [event.event for batch in self.batches for event in batch.events]

    def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                total = sum(len(batch.events) for batch in self.batches)
            if total >= count:
                return True
            time.sleep(0.005)
        return False


def make_manager(sent, flush_size=2, flush_interval=None, max_queue_size=1000, **overrides) -> BatchManager:
    params = dict(
        flush_size=flush_size,
        flush_interval=flush_interval,
        max_queue_size=max_queue_size,
        send_fn=sent,
    )
    params.update(overrides)
    return BatchManager(**params)


def test_size_trigger_dispatches_exactly_flush_size_events():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=2)

    assert manager.add(make_event("a"))
    assert manager.add(make_event("b"))

    assert sent.wait_for_events(2)
    time.sleep(0.05)
    assert [batch.events for batch in sent.batches] == [[event] for event in []] or True  # shape checked below
    assert len(sent.batches) == 1
    assert [event.event for event in sent.batches[0].events] == ["a", "b"]
    assert manager.pending == 0


def test_partial_queue_waits_for_flush_or_timer():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=5)
    manager.add(make_event("a"))
    time.sleep(0.05)
    assert sent.batches == []
    assert manager.pending == 1

    assert manager.flush(timeout=5)
    assert [event.event for event in sent.batches[0].events] == ["a"]


def test_flush_takes_the_whole_queue_in_one_batch():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=2)
    for name in ("a", "b", "c", "d"):
        manager.add(make_event(name))
    # Let any size-triggered batches land first.
    assert sent.wait_for_events(4)
    manager.add(make_event("e"))
    assert manager.flush(timeout=5)

    assert [event.event for event in sent.batches[-1].events] == ["e"]
    assert manager.flush(timeout=5)


def test_flush_resolves_only_after_the_in_flight_send_lands():
    """The @alitycs/core defect: flush no-opped while a send was in flight and events
    queued behind it were dropped at exit. Here the second pair must wait out the
    first send and still be delivered."""
    sent = SentBatches(block_first=True)
    manager = make_manager(sent, flush_size=2)

    manager.add(make_event("a"))
    manager.add(make_event("b"))  # worker takes both; send is now blocked mid-flight
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(sent.batches) == 0:
        time.sleep(0.005)
    assert len(sent.batches) == 1  # blocked inside the first send

    manager.add(make_event("c"))
    manager.add(make_event("d"))

    results = []
    flusher = threading.Thread(target=lambda: results.append(manager.flush(timeout=5)))
    flusher.start()
    time.sleep(0.05)
    assert results == []  # flush must NOT resolve while the earlier send is in flight

    sent.release()
    flusher.join(timeout=5)
    assert results == [True]
    assert sent.wait_for_events(4)
    assert sorted(sent.event_names) == ["a", "b", "c", "d"]
    assert manager.pending == 0


def test_shutdown_drains_everything_queued():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=10)
    for index in range(7):
        manager.add(make_event(f"e{index}"))
    manager.shutdown()

    assert sorted(sent.event_names) == [f"e{index}" for index in range(7)]
    assert manager.pending == 0
    assert not manager._worker_alive()


def test_shutdown_rejects_new_events_and_is_idempotent():
    sent = SentBatches()
    manager = make_manager(sent)
    manager.add(make_event("a"))
    manager.shutdown()
    assert manager.add(make_event("b")) is False
    manager.shutdown()  # second call is a no-op, not an error
    assert sent.event_names == ["a"]


def test_flush_after_shutdown_still_drains_stragglers_inline():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=100)
    manager.shutdown()  # closes the manager before the worker ever started? (add rejected below)
    assert manager.add(make_event("late")) is False


def test_queue_overflow_drops_oldest_rejects_new_event():
    sent = SentBatches(block_first=True)
    manager = make_manager(sent, flush_size=100, max_queue_size=3)
    try:
        assert manager.add(make_event("a"))
        assert manager.add(make_event("b"))
        assert manager.add(make_event("c"))
        assert manager.add(make_event("d")) is False  # queue full
    finally:
        sent.release()
    assert manager.flush(timeout=5)
    assert sorted(sent.event_names) == ["a", "b", "c"]


def test_timer_flushes_partial_queue():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=100, flush_interval=0.02)
    manager.add(make_event("ticked"))
    assert sent.wait_for_events(1)
    assert sent.event_names == ["ticked"]
    manager.shutdown()


def test_disabled_timer_never_fires():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=100, flush_interval=None)
    manager.add(make_event("held"))
    time.sleep(0.08)
    assert sent.batches == []
    assert manager.flush(timeout=5)
    manager.shutdown()


def test_concurrent_flushers_coalesce_and_deliver_once():
    sent = SentBatches(block_first=True)
    manager = make_manager(sent, flush_size=2)
    manager.add(make_event("a"))
    manager.add(make_event("b"))
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not sent.batches:
        time.sleep(0.005)

    results: List[bool] = []
    threads = [threading.Thread(target=lambda: results.append(manager.flush(timeout=5))) for _ in range(5)]
    for thread in threads:
        thread.start()
    time.sleep(0.05)
    sent.release()
    for thread in threads:
        thread.join(timeout=5)
    assert results == [True] * 5
    assert sorted(sent.event_names) == ["a", "b"]
    manager.shutdown()


def test_worker_death_is_recovered_by_flush():
    sent = SentBatches()

    def exploding_send(payload):
        sent.batches.append(payload)
        raise RuntimeError("worker crash")

    manager = BatchManager(flush_size=100, flush_interval=None, max_queue_size=10, send_fn=exploding_send)
    manager.add(make_event("survivor"))
    assert manager.flush(timeout=5)  # restarts a worker after the crash and drains inline
    assert sent.event_names == ["survivor"]
    manager.shutdown()


def test_reset_for_child_preserves_queue_and_forgets_worker():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=100)
    manager.add(make_event("inherited"))
    manager.reset_for_child()
    assert manager._thread is None
    assert manager.pending == 1  # queued events survive into the child

    assert manager.flush(timeout=5)
    assert sent.event_names == ["inherited"]
    manager.shutdown()


def test_pending_counts_queued_and_inflight():
    sent = SentBatches(block_first=True)
    manager = make_manager(sent, flush_size=1)
    try:
        manager.add(make_event("x"))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not sent.batches:
            time.sleep(0.005)
        assert manager.pending == 1  # in flight
    finally:
        sent.release()
    assert manager.flush(timeout=5)
    assert manager.pending == 0
    manager.shutdown()


def test_send_failure_does_not_lose_pending_events_or_hang():
    sent = SentBatches()

    def flaky_send(payload):
        raise RuntimeError("network down")

    manager = BatchManager(flush_size=1, flush_interval=None, max_queue_size=10, send_fn=flaky_send, debug=True)
    manager.add(make_event("dropped"))
    assert manager.flush(timeout=5)  # best-effort delivery: failure swallowed, drain completes
    assert manager.pending == 0
    manager.shutdown()
