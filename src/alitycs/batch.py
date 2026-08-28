"""Queueing and batch dispatch on a background flusher thread.

Design notes, in the shadow of the ``@alitycs/core`` flush-lock defect this SDK must
never repeat (see .agents/plans/phase-0-harness.md §1.1):

- A single daemon worker thread owns size-triggered and timer-triggered dispatch,
  taking exactly ``flush_size`` events per batch.
- :meth:`flush` does not signal the worker and hope — it drains the queue itself on
  the calling thread, ``flush_size`` events at a time, and resolves only once nothing
  is queued **and** no send is in flight. Waiting against an in-flight send is
  precisely the spot where ``core`` used to no-op and lose whatever was queued
  behind it.
- Delivery is honest: :meth:`flush` returns ``True`` only when everything drained was
  delivered. A whole-batch rejection (HTTP 400 — one invalid event poisons the entire
  batch) splits the payload in half and re-sends each half; a transient failure
  re-queues survivors at the head of the queue preserving order. Drained-but-
  undelivered events are never silently dropped.
- :meth:`shutdown` marks the manager draining and gives the worker a bounded window.
  When that deadline expires, queued work is persisted when durability is enabled;
  callers can pass ``join_timeout=None`` when they explicitly want an unbounded drain.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable, List, Optional, Union

from .transport import SendFailed, SendRejected, SendSuccess
from .types import AnalyticsEvent, BatchPayload
from .utils import debug_warn, generate_id, now_ms, warn

#: Legacy fakes may return ``None``; that is treated as success.
SendFn = Callable[[BatchPayload], Optional[Union[SendSuccess, SendRejected, SendFailed]]]
DeadlineSendFn = Callable[
    [BatchPayload, Optional[float]], Optional[Union[SendSuccess, SendRejected, SendFailed]]
]
Clock = Callable[[], float]
RecoverFn = Callable[[Optional[float]], bool]
DurablePendingFn = Callable[[], int]
PersistFn = Callable[[BatchPayload], bool]

# Outcome of one dispatched batch.
_OK = "ok"  # every event delivered
_REJECTED = "rejected"  # server permanently refused; events dropped loudly
_TRANSIENT = "transient"  # transport failure; events re-queued at the head

# Pause for the background worker after a transient failure so it does not
# hot-loop over re-queued events; explicit flushes retry immediately instead.
_WORKER_RETRY_BACKOFF_SECONDS = 0.5

# Whole-batch rejection isolation is useful but must not amplify one response into an
# unbounded request storm when a caller configures a very large flush size.
_MAX_SPLIT_SENDS = 64


class BatchManager:
    def __init__(
        self,
        flush_size: int,
        flush_interval: Optional[float],
        max_queue_size: int,
        send_fn: SendFn,
        debug: bool = False,
        clock: Optional[Clock] = None,
        recover_fn: Optional[RecoverFn] = None,
        durable_pending_fn: Optional[DurablePendingFn] = None,
        durable: bool = False,
        persist_fn: Optional[PersistFn] = None,
        send_with_deadline_fn: Optional[DeadlineSendFn] = None,
    ) -> None:
        self._flush_size = flush_size
        self._flush_interval = flush_interval
        self._max_queue_size = max_queue_size
        self._send_fn = send_fn
        self._debug = debug
        self._clock: Clock = clock if clock is not None else time.monotonic
        self._recover_fn = recover_fn
        self._durable_pending_fn = durable_pending_fn
        self._durable = durable
        self._persist_fn = persist_fn
        self._send_with_deadline_fn = send_with_deadline_fn

        self._cv = threading.Condition()
        self._queue: "deque[AnalyticsEvent]" = deque()
        self._inflight = 0
        self._stopping = False
        self._closed = False
        self._finite_shutdown_complete = False
        self._thread: Optional[threading.Thread] = None
        self._next_tick = self._clock() + flush_interval if flush_interval else None

        self._delivered_count = 0
        self._requeued_count = 0
        self._lost_count = 0

    @property
    def pending(self) -> int:
        """Events queued, in flight, or persisted for restart."""
        with self._cv:
            memory_pending = len(self._queue) + self._inflight
        durable_pending = 0 if self._durable_pending_fn is None else self._durable_pending_fn()
        return memory_pending + durable_pending

    @property
    def closed(self) -> bool:
        """True once :meth:`shutdown` stopped this manager from accepting events."""
        with self._cv:
            return self._closed

    @property
    def delivered_total(self) -> int:
        """Events confirmed delivered since this manager was created."""
        with self._cv:
            return self._delivered_count

    @property
    def requeued_total(self) -> int:
        """Events re-queued at the head after transient send failures."""
        with self._cv:
            return self._requeued_count

    @property
    def lost_total(self) -> int:
        """Events permanently lost: dropped by the server or overflowed from the queue."""
        with self._cv:
            return self._lost_count

    def add(self, event: AnalyticsEvent) -> bool:
        """Enqueue one event, dispatching when the queue reaches ``flush_size``.
        Returns ``False`` when the event was dropped (queue full or shut down)."""
        with self._cv:
            if self._closed:
                if self._debug:
                    debug_warn("Queue closed — dropping event")
                return False
            if len(self._queue) >= self._max_queue_size:
                self._lost_count += 1
                warn("Queue full — dropping event")
                return False
            self._queue.append(event)
            self._cv.notify_all()
        self._ensure_worker()
        return True

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Drain fully: send everything queued and wait for in-flight sends to land.
        Returns ``True`` only when the drain finished with every event delivered;
        ``False`` when a send failed (survivors stay queued), or ``timeout`` elapsed.
        Safe to call concurrently — callers share in-flight sends instead of
        duplicating them."""
        deadline = None if timeout is None else self._clock() + timeout
        with self._cv:
            # A caller has explicitly resumed draining after a previous finite shutdown.
            # Late failures must become pending work for this caller, not terminal loss.
            self._finite_shutdown_complete = False
        if not self._recover_durable(deadline):
            return False
        self._ensure_worker()
        all_delivered = True
        while True:
            batch: List[AnalyticsEvent] = []
            cv = self._cv  # bind: reset_for_child may swap the condition mid-flight
            with cv:
                while True:
                    now = self._clock()
                    if not self._queue and self._inflight == 0:
                        durable_pending = (
                            0 if self._durable_pending_fn is None else self._durable_pending_fn()
                        )
                        return all_delivered and durable_pending == 0
                    if deadline is not None and now >= deadline:
                        return False
                    if self._queue:
                        # Drain inline rather than depend on the worker waking up;
                        # this is the path shutdown relies on. Take flush_size-sized
                        # chunks so payloads match the size-triggered batches.
                        batch = self._pop(min(self._flush_size, len(self._queue)))
                        self._advance_tick(now)
                        break
                    # Nothing queued but a send is in flight: wait for its completion
                    # notification (or the worker picking up newly queued events).
                    cv.wait(None if deadline is None else max(0.0, deadline - now))
            if not batch:
                continue
            outcome = self._send_batch(batch, deadline)
            if outcome == _OK:
                continue
            all_delivered = False
            if outcome == _REJECTED:
                # Those events are gone (loudly); keep draining the rest of the queue.
                continue
            # Transient failure: survivors were re-queued at the head. Retrying inside
            # this loop would hot-spin against the same failure — wait out any other
            # in-flight sends and report honestly; a later flush retries.
            self._wait_for_inflight(deadline)
            return False

    def shutdown(self, join_timeout: Optional[float] = 30.0) -> None:
        """Stop accepting events and wait up to ``join_timeout`` for the worker.

        When the deadline expires, queued events are persisted when durability is
        enabled instead of extending process shutdown with unbounded network waits.
        Passing ``None`` preserves the fully blocking drain contract.
        """
        deadline = None if join_timeout is None else self._clock() + max(0.0, join_timeout)
        with self._cv:
            if deadline is None:
                self._finite_shutdown_complete = False
            self._stopping = True
            self._closed = True
            self._cv.notify_all()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            remaining = None if deadline is None else max(0.0, deadline - self._clock())
            thread.join(remaining)

        if deadline is not None:
            # The worker owns network delivery. A finite shutdown must never re-enter
            # that path inline because HTTP retries and Retry-After sleeps can outlive
            # the caller's deadline. Any queued remainder is safe on disk when durable;
            # an in-flight durable batch was persisted by the transport before sending.
            with self._cv:
                self._finite_shutdown_complete = True
            self._persist_queued_for_shutdown()
            return

        # An explicitly unbounded shutdown retains the full drain guarantee.
        self._wait_for_inflight(None)
        if not self.flush():
            self._persist_queued_for_shutdown()

    def reset_for_child(self) -> None:
        """Post-``os.fork`` repair, run in the child only.

        Locks are replaced because a parent thread may have held them at fork time. The
        inherited queue is discarded: the parent still owns those events, and sending
        the child's copy would double-count them.
        """
        self._cv = threading.Condition()
        with self._cv:
            self._thread = None
            self._inflight = 0  # the parent's in-flight send can never complete here
            self._queue.clear()
            self._next_tick = self._clock() + self._flush_interval if self._flush_interval else None

    # Internals ------------------------------------------------------------

    def _ensure_worker(self) -> None:
        with self._cv:
            if self._closed:
                return
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(target=self._run, name="alitycs-flusher", daemon=True)
            # Start under the lock so two racing callers cannot spawn duplicates.
            self._thread.start()

    def _worker_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        try:
            while True:
                cv = self._cv  # bind: reset_for_child may swap the condition mid-flight
                with cv:
                    batch = self._take_batch(cv)
                    if batch is None:
                        return
                outcome = self._send_batch(batch)
                if outcome == _TRANSIENT:
                    with self._cv:
                        if self._stopping:
                            return
                    # Survivors were re-queued at the head; back off before retrying so
                    # a downed endpoint does not turn into a hot loop.
                    time.sleep(_WORKER_RETRY_BACKOFF_SECONDS)
        finally:
            with self._cv:
                self._cv.notify_all()

    def _take_batch(self, cv: threading.Condition) -> Optional[List[AnalyticsEvent]]:
        """Pop the next batch under the lock, or ``None`` when fully drained and
        shutting down. Caller holds ``cv`` (bound by :meth:`_run`, not re-read from
        ``self._cv`` which ``reset_for_child`` may have replaced)."""
        while True:
            now = self._clock()
            if self._stopping and not self._queue:
                return None
            if self._stopping:
                return self._pop(min(self._flush_size, len(self._queue)))
            if len(self._queue) >= self._flush_size:
                return self._pop(self._flush_size)
            if self._timer_due(now):
                self._advance_tick(now)
                if self._queue:
                    return self._pop(min(self._flush_size, len(self._queue)))
                continue  # timer fired with an empty queue: skip the missed tick
            cv.wait(self._time_until_tick(now))

    def _pop(self, count: int) -> List[AnalyticsEvent]:
        batch: List[AnalyticsEvent] = []
        while self._queue and len(batch) < count:
            batch.append(self._queue.popleft())
        self._inflight += len(batch)
        return batch

    def _timer_due(self, now: float) -> bool:
        return self._next_tick is not None and now >= self._next_tick

    def _advance_tick(self, now: float) -> None:
        if self._flush_interval:
            self._next_tick = now + self._flush_interval

    def _time_until_tick(self, now: float) -> Optional[float]:
        if self._next_tick is None:
            return None
        return max(0.0, self._next_tick - now)

    def _wait_for_inflight(self, deadline: Optional[float]) -> bool:
        """Block until no send is in flight. Returns True when the queue and inflight
        are both empty, False otherwise (timeout or undelivered work remains)."""
        with self._cv:
            while True:
                now = self._clock()
                if self._inflight == 0:
                    return not self._queue
                if deadline is not None and now >= deadline:
                    return False
                self._cv.wait(None if deadline is None else max(0.0, deadline - now))

    def _send_batch(
        self, batch: List[AnalyticsEvent], deadline: Optional[float] = None
    ) -> str:
        """Dispatch one batch. Returns ``_OK``, ``_REJECTED``, or ``_TRANSIENT``
        (with survivors re-queued at the head). Never raises."""
        try:
            if not self._recover_durable(deadline):
                self._requeue_at_head(batch)
                result = _TRANSIENT
            else:
                result = self._deliver(list(batch), [_MAX_SPLIT_SENDS], deadline)
        except Exception as exc:  # noqa: BLE001 - delivery must never crash the host
            warn(f"Batch dispatch failed ({type(exc).__name__}: {exc})")
            self._requeue_at_head(batch)
            result = _TRANSIENT
        finally:
            with self._cv:
                self._inflight -= len(batch)
                self._cv.notify_all()
        return result

    def _recover_durable(self, deadline: Optional[float] = None) -> bool:
        if self._recover_fn is None:
            return True
        try:
            return self._recover_fn(deadline)
        except Exception as exc:  # noqa: BLE001 - persistence/network failures are reported
            warn(f"Durable batch recovery failed ({type(exc).__name__}: {exc})")
            return False

    def _persist_queued_for_shutdown(self) -> None:
        """Persist queued work FIFO when an older durable batch blocks recovery."""
        with self._cv:
            queued = list(self._queue)
            self._queue.clear()

        if not queued:
            return
        if not self._durable or self._persist_fn is None:
            with self._cv:
                self._lost_count += len(queued)
            warn(f"Shutdown could not deliver {len(queued)} queued event(s) — persistence unavailable")
            return

        for index, event in enumerate(queued):
            payload = BatchPayload(
                batch_id=f"batch_{generate_id()}",
                sent_at=now_ms(),
                events=[event],
            )
            try:
                persisted = self._persist_fn(payload)
            except Exception as exc:  # noqa: BLE001 - count the unresolved suffix honestly
                warn(f"Shutdown persistence failed ({type(exc).__name__}: {exc})")
                persisted = False
            if persisted:
                continue

            lost = len(queued) - index
            with self._cv:
                self._lost_count += lost
            warn(f"Shutdown persistence failed — {lost} queued event(s) lost")
            return

    def _deliver(
        self,
        events: List[AnalyticsEvent],
        remaining_sends: List[int],
        deadline: Optional[float] = None,
    ) -> str:
        if remaining_sends[0] <= 0:
            with self._cv:
                self._lost_count += len(events)
            warn(
                f"Batch rejection split limit reached — dropping {len(events)} unresolved event(s)"
            )
            return _REJECTED
        remaining_sends[0] -= 1
        payload = BatchPayload(
            batch_id=f"batch_{generate_id()}",
            sent_at=now_ms(),
            events=list(events),
        )
        try:
            outcome = (
                self._send_fn(payload)
                if self._send_with_deadline_fn is None
                else self._send_with_deadline_fn(payload, deadline)
            )
        except Exception as exc:  # noqa: BLE001 - legacy send fns raise instead of outcomes
            warn(f"Batch send failed ({type(exc).__name__}: {exc})")
            outcome = SendFailed(f"{type(exc).__name__}: {exc}")

        if isinstance(outcome, SendRejected):
            if outcome.is_batch_reject and len(events) > 1:
                # The whole batch bounced because one event violated a limit. Split in
                # half and retry each side so valid events still land; depth is bounded
                # by log2(len(events)).
                mid = len(events) // 2
                left = self._deliver(events[:mid], remaining_sends, deadline)
                right = self._deliver(events[mid:], remaining_sends, deadline)
                if _TRANSIENT in (left, right):
                    return _TRANSIENT
                return _OK if _REJECTED not in (left, right) else _REJECTED
            with self._cv:
                self._lost_count += len(events)
            warn(
                f"Server rejected {len(events)} event(s) with HTTP {outcome.status} — "
                "dropped, not retried"
            )
            return _REJECTED

        if isinstance(outcome, SendFailed):
            if self._durable and outcome.durable:
                warn(f"Transport failure ({outcome.reason}) — exact batch retained for restart")
            else:
                warn(f"Transport failure ({outcome.reason}) — re-queueing {len(events)} event(s)")
                self._requeue_at_head(events)
            return _TRANSIENT

        if self._debug and outcome is not None and not isinstance(outcome, SendSuccess):
            debug_warn(f"Unexpected send outcome {outcome!r} — treating as delivered")

        with self._cv:
            self._delivered_count += len(events)
        return _OK

    def _requeue_at_head(self, events: List[AnalyticsEvent]) -> None:
        abandoned = False
        with self._cv:
            if self._finite_shutdown_complete:
                self._lost_count += len(events)
                abandoned = True
            else:
                self._requeued_count += len(events)
                # extendleft reverses its argument, so pass the reversed list to keep the
                # original order at the head of the queue.
                self._queue.extendleft(reversed(events))
            self._cv.notify_all()
        if abandoned:
            warn(
                f"Transport failed after the shutdown deadline — {len(events)} "
                "in-flight event(s) lost"
            )
