"""Queueing and batch dispatch on a background flusher thread.

Design notes, in the shadow of the ``@alitycs/core`` flush-lock defect this SDK must
never repeat (see .agents/plans/phase-0-harness.md §1.1):

- A single daemon worker thread owns size-triggered and timer-triggered dispatch,
  taking exactly ``flush_size`` events per size-triggered batch.
- :meth:`flush` does not signal the worker and hope — it drains the queue itself on
  the calling thread and resolves only once nothing is queued **and** no send is in
  flight. Waiting against an in-flight send is precisely the spot where ``core``
  used to no-op and lose whatever was queued behind it.
- :meth:`shutdown` marks the manager draining, joins the worker, and then flushes
  again as a safety net. Whatever the app enqueued before ``shutdown()`` is delivered,
  or the call does not return normally.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable, List, Optional

from .types import AnalyticsEvent, BatchPayload
from .utils import debug_warn, generate_id, now_ms

SendFn = Callable[[BatchPayload], None]
Clock = Callable[[], float]


class BatchManager:
    def __init__(
        self,
        flush_size: int,
        flush_interval: Optional[float],
        max_queue_size: int,
        send_fn: SendFn,
        debug: bool = False,
        clock: Optional[Clock] = None,
    ) -> None:
        self._flush_size = flush_size
        self._flush_interval = flush_interval
        self._max_queue_size = max_queue_size
        self._send_fn = send_fn
        self._debug = debug
        self._clock: Clock = clock if clock is not None else time.monotonic

        self._cv = threading.Condition()
        self._queue: "deque[AnalyticsEvent]" = deque()
        self._inflight = 0
        self._stopping = False
        self._closed = False
        self._thread: Optional[threading.Thread] = None
        self._next_tick = self._clock() + flush_interval if flush_interval else None

    @property
    def pending(self) -> int:
        """Events queued plus events in an in-flight send."""
        with self._cv:
            return len(self._queue) + self._inflight

    def add(self, event: AnalyticsEvent) -> bool:
        """Enqueue one event, dispatching when the queue reaches ``flush_size``.
        Returns ``False`` when the event was dropped (queue full or shut down)."""
        with self._cv:
            if self._closed:
                if self._debug:
                    debug_warn("Queue closed — dropping event")
                return False
            if len(self._queue) >= self._max_queue_size:
                if self._debug:
                    debug_warn("Queue full — dropping event")
                return False
            self._queue.append(event)
            self._cv.notify_all()
        self._ensure_worker()
        return True

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Drain fully: send everything queued and wait for in-flight sends to land.
        Returns ``True`` once done, ``False`` if ``timeout`` elapsed first. Safe to
        call concurrently — callers share in-flight sends instead of duplicating them."""
        self._ensure_worker()
        deadline = None if timeout is None else self._clock() + timeout
        while True:
            batch: List[AnalyticsEvent] = []
            with self._cv:
                while True:
                    now = self._clock()
                    if not self._queue and self._inflight == 0:
                        return True
                    if deadline is not None and now >= deadline:
                        return False
                    if self._queue:
                        # Drain inline rather than depend on the worker waking up;
                        # this is the path shutdown relies on. An explicit drain takes
                        # the whole queue in one batch, like @alitycs/core and jvm do.
                        while self._queue:
                            batch.append(self._queue.popleft())
                        self._advance_tick(now)
                        self._inflight += 1
                        break
                    # Nothing queued but a send is in flight: wait for its completion
                    # notification (or the worker picking up newly queued events).
                    self._cv.wait(None if deadline is None else max(0.0, deadline - now))
            if batch:
                self._send_batch(batch)

    def shutdown(self, join_timeout: Optional[float] = 30.0) -> None:
        """Stop accepting events, drain everything, and join the worker. Returns only
        once nothing is queued or in flight — shutdown must not lose events."""
        with self._cv:
            self._stopping = True
            self._closed = True
            self._cv.notify_all()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(join_timeout)
        # Safety net: if the worker died or the join timed out, finish the drain inline.
        self.flush()

    def reset_for_child(self) -> None:
        """Post-``os.fork`` repair, run in the child only. Locks are replaced untouched
        (a parent thread may have held the inherited ones at fork time), the parent's
        worker is forgotten, and queued events are kept so the child can deliver them
        once a fresh worker starts lazily."""
        self._cv = threading.Condition()
        with self._cv:
            self._thread = None
            self._inflight = 0  # the parent's in-flight send can never complete here
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
                with self._cv:
                    batch = self._take_batch()
                    if batch is None:
                        return
                self._send_batch(batch)
        finally:
            with self._cv:
                self._cv.notify_all()

    def _take_batch(self) -> Optional[List[AnalyticsEvent]]:
        """Pop the next batch under the lock, or ``None`` when fully drained and
        shutting down. Caller holds ``self._cv``."""
        while True:
            now = self._clock()
            if self._stopping and not self._queue:
                return None
            if self._stopping:
                return self._pop(len(self._queue))
            if len(self._queue) >= self._flush_size:
                return self._pop(self._flush_size)
            if self._timer_due(now):
                self._advance_tick(now)
                if self._queue:
                    return self._pop(len(self._queue))
                continue  # timer fired with an empty queue: skip the missed tick
            self._cv.wait(self._time_until_tick(now))

    def _pop(self, count: int) -> List[AnalyticsEvent]:
        batch: List[AnalyticsEvent] = []
        while self._queue and len(batch) < count:
            batch.append(self._queue.popleft())
        self._inflight += 1
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

    def _send_batch(self, batch: List[AnalyticsEvent]) -> None:
        try:
            payload = BatchPayload(
                batch_id=f"batch_{generate_id()}",
                sent_at=now_ms(),
                events=list(batch),
            )
            self._send_fn(payload)
        except Exception:  # noqa: BLE001 - delivery is best-effort; retries happened upstream
            if self._debug:
                debug_warn("Batch send failed — events dropped")
        finally:
            with self._cv:
                self._inflight -= 1
                self._cv.notify_all()
