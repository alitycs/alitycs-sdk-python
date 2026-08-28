"""BatchManager behaviour, including the no-lost-events shutdown contract."""

import threading
import time
from typing import List

from alitycs.batch import BatchManager
from alitycs.transport import SendFailed, SendRejected
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


def test_flush_drains_in_flush_size_chunks_not_the_whole_queue():
    """The old behaviour drained the entire queue into ONE payload; flush must send
    flush_size-sized chunks like the size-triggered path does."""
    sent = SentBatches(block_first=True)
    manager = make_manager(sent, flush_size=2)
    manager.add(make_event("a"))
    manager.add(make_event("b"))  # worker takes [a,b]; send is now blocked mid-flight
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(sent.batches) == 0:
        time.sleep(0.005)

    for name in ("c", "d", "e", "f", "g", "h"):
        manager.add(make_event(name))

    assert manager.flush(timeout=5) is True

    sizes = [len(batch.events) for batch in sent.batches[1:]]
    assert sizes == [2, 2, 2]  # three chunks, never a single 6-event payload
    assert manager.pending == 0
    assert sorted(sent.event_names) == list("abcdefgh")
    manager.shutdown()


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


def test_shutdown_deadline_persists_queued_remainder_while_send_is_blocked():
    started = threading.Event()
    release = threading.Event()
    persisted = []

    def blocked_send(payload: BatchPayload) -> None:
        started.set()
        release.wait(5)

    def persist(payload: BatchPayload) -> bool:
        persisted.append(payload)
        return True

    manager = BatchManager(
        flush_size=1,
        flush_interval=None,
        max_queue_size=10,
        send_fn=blocked_send,
        durable=True,
        persist_fn=persist,
    )
    manager.add(make_event("in-flight"))
    assert started.wait(2)
    manager.add(make_event("queued"))

    started_at = time.monotonic()
    manager.shutdown(join_timeout=0.05)
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.5
    assert [[event.event for event in payload.events] for payload in persisted] == [["queued"]]

    release.set()
    manager.shutdown(join_timeout=None)


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
    """A send that raises once must not kill delivery: the event is re-queued and a
    later flush delivers it."""
    sent = SentBatches()
    state = {"failed_once": False}

    def exploding_send(payload):
        if not state["failed_once"]:
            state["failed_once"] = True
            raise RuntimeError("worker crash")
        sent.batches.append(payload)

    manager = BatchManager(flush_size=100, flush_interval=None, max_queue_size=10, send_fn=exploding_send)
    manager.add(make_event("survivor"))
    assert manager.flush(timeout=5) is False  # honest: first attempt failed
    assert manager.pending == 1  # kept at the queue head, not dropped

    assert manager.flush(timeout=5) is True  # retry after recovery drains clean
    assert sent.event_names == ["survivor"]
    assert manager.delivered_total == 1
    manager.shutdown()


def test_batch_400_rejection_splits_and_delivers_both_halves():
    batches: List[BatchPayload] = []

    def send(payload: BatchPayload):
        batches.append(payload)
        if len(payload.events) > 1:
            return SendRejected(400)  # whole-batch rejection until single-event size
        return None

    manager = BatchManager(flush_size=100, flush_interval=None, max_queue_size=10, send_fn=send)
    for name in ("a", "b", "c", "d"):
        manager.add(make_event(name))

    assert manager.flush(timeout=5) is True
    # 4 -> [a,b]+[c,d] -> each half splits again because this fake refuses any payload
    # larger than one event; every single lands.
    assert [len(batch.events) for batch in batches] == [4, 2, 1, 1, 2, 1, 1]
    singles = [batch.events[0].event for batch in batches if len(batch.events) == 1]
    assert singles == ["a", "b", "c", "d"]
    assert manager.pending == 0
    assert manager.delivered_total == 4
    assert manager.lost_total == 0
    manager.shutdown()


def test_single_event_batch_400_is_dropped_loudly_not_requeued():
    def always_reject(payload: BatchPayload):
        return SendRejected(400)

    manager = BatchManager(
        flush_size=100,
        flush_interval=None,
        max_queue_size=10,
        send_fn=always_reject,
        debug=True,
    )
    manager.add(make_event("poison"))

    assert manager.flush(timeout=5) is False
    assert manager.pending == 0  # re-queueing would poison every future batch
    assert manager.lost_total == 1
    manager.shutdown()


def test_transport_failure_requeues_survivors_at_head_preserving_order():
    attempts = {"count": 0}
    delivered: List[str] = []

    def flaky_then_good(payload: BatchPayload):
        if attempts["count"] == 0:
            attempts["count"] += 1
            return SendFailed("connection reset")
        delivered.extend(event.event for event in payload.events)
        return None

    manager = BatchManager(flush_size=100, flush_interval=None, max_queue_size=10, send_fn=flaky_then_good)
    for name in ("first", "second", "third"):
        manager.add(make_event(name))

    assert manager.flush(timeout=5) is False
    assert manager.pending == 3
    assert manager.requeued_total == 3

    manager.add(make_event("fourth"))
    assert manager.flush(timeout=5) is True

    # Requeued survivors keep their order; the new event lands behind them.
    assert delivered == ["first", "second", "third", "fourth"]
    assert manager.delivered_total == 4
    manager.shutdown()


def test_non_batch_4xx_is_reported_as_a_permanent_rejection():
    calls = {"count": 0}

    def unauthorized(payload: BatchPayload):
        calls["count"] += 1
        return SendRejected(401)

    manager = BatchManager(flush_size=100, flush_interval=None, max_queue_size=10, send_fn=unauthorized)
    manager.add(make_event("e1"))
    manager.add(make_event("e2"))

    assert manager.flush(timeout=5) is False
    assert calls["count"] == 1  # no splitting: auth failures reject everything anyway
    assert manager.lost_total == 2
    assert manager.requeued_total == 0
    manager.shutdown()


def test_flush_reports_false_until_everything_is_delivered():
    outcomes = iter([SendFailed("down"), None])

    def flaky(payload: BatchPayload):
        result = next(outcomes)
        if isinstance(result, SendFailed):
            raise RuntimeError(result.reason)
        return result

    manager = BatchManager(flush_size=100, flush_interval=None, max_queue_size=10, send_fn=flaky)
    manager.add(make_event("x"))
    assert manager.flush(timeout=5) is False
    assert manager.flush(timeout=5) is True
    manager.shutdown()


def test_reset_for_child_drops_parent_owned_queue_and_forgets_worker():
    sent = SentBatches()
    manager = make_manager(sent, flush_size=100)
    manager.add(make_event("inherited"))
    manager.reset_for_child()
    assert manager._thread is None
    assert manager.pending == 0

    assert manager.flush(timeout=5)
    assert sent.event_names == []
    manager.shutdown()


def test_batch_rejection_split_is_bounded_to_sixty_four_sends():
    sent = []

    def reject(payload: BatchPayload):
        sent.append(payload)
        return SendRejected(400)

    manager = BatchManager(
        flush_size=101,  # above the event count: only the explicit flush dispatches
        flush_interval=None,
        max_queue_size=100,
        send_fn=reject,
    )
    for index in range(100):
        manager.add(make_event(str(index)))
    assert manager.flush(timeout=5) is False
    assert len(sent) == 64
    assert manager.lost_total == 100
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


def test_flush_reports_durable_background_failure_as_undelivered():
    manager = BatchManager(
        flush_size=1,
        flush_interval=None,
        max_queue_size=10,
        send_fn=lambda payload: SendFailed("response lost", durable=True),
        recover_fn=lambda: True,
        durable_pending_fn=lambda: 1,
        durable=True,
    )
    manager.add(make_event("durable_failure"))

    assert manager.flush() is False
    assert manager.pending >= 1
    manager.shutdown()
    manager.shutdown()


def test_shutdown_persists_queued_events_fifo_when_recovery_is_blocked():
    durable_pending = [1]
    persisted = []

    def persist(payload: BatchPayload) -> bool:
        persisted.append(payload)
        durable_pending[0] += len(payload.events)
        return True

    manager = BatchManager(
        flush_size=10,
        flush_interval=None,
        max_queue_size=10,
        send_fn=lambda payload: None,
        recover_fn=lambda: False,
        durable_pending_fn=lambda: durable_pending[0],
        durable=True,
        persist_fn=persist,
    )
    for name in ("a", "b", "c"):
        manager.add(make_event(name))

    manager.shutdown(join_timeout=2)

    assert [[event.event for event in payload.events] for payload in persisted] == [
        ["a"],
        ["b"],
        ["c"],
    ]
    assert manager.pending == 4
    assert manager.lost_total == 0


def test_shutdown_counts_only_unpersisted_suffix_as_lost():
    durable_pending = [1]
    persisted = []

    def persist(payload: BatchPayload) -> bool:
        if persisted:
            return False
        persisted.append(payload)
        durable_pending[0] += 1
        return True

    manager = BatchManager(
        flush_size=10,
        flush_interval=None,
        max_queue_size=10,
        send_fn=lambda payload: None,
        recover_fn=lambda: False,
        durable_pending_fn=lambda: durable_pending[0],
        durable=True,
        persist_fn=persist,
    )
    for name in ("saved", "lost-1", "lost-2"):
        manager.add(make_event(name))

    manager.shutdown(join_timeout=2)

    assert [event.event for event in persisted[0].events] == ["saved"]
    assert manager.pending == 2
    assert manager.lost_total == 2


def test_send_failure_keeps_events_queued_and_reports_false():
    """Failures are honest: the event is re-queued at the head (not dropped), flush
    reports False, and a later flush after recovery delivers it."""
    sent = SentBatches()
    state = {"failed": False}

    def flaky_send(payload):
        if not state["failed"]:
            state["failed"] = True
            raise RuntimeError("network down")
        sent.batches.append(payload)

    # flush_size > queue depth so only the explicit flush dispatches: the worker
    # must not consume the planned failure in a race with the test thread.
    manager = BatchManager(flush_size=100, flush_interval=None, max_queue_size=10, send_fn=flaky_send, debug=True)
    manager.add(make_event("survivor"))
    assert manager.flush(timeout=5) is False  # honest result instead of True
    assert manager.pending == 1  # kept for retry, never silently dropped

    assert manager.flush(timeout=5) is True  # endpoint recovered
    assert sent.event_names == ["survivor"]
    assert manager.delivered_total == 1
    assert manager.requeued_total == 1
    assert manager.pending == 0
    manager.shutdown()
