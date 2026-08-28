"""HTTP transport: batch POST with exponential-backoff retry, zero dependencies.

Mirrors ``HttpTransport.kt``: ``max_retries`` retries after the initial attempt,
backoff doubling from :attr:`retry_backoff_base` capped at ten seconds, 4xx responses
(except 429) are not retried. A retryable response carrying ``Retry-After``
(delta-seconds or an HTTP-date) is honoured up to a one-hour ceiling: the next attempt
uses it instead of the default backoff. :meth:`HttpTransport.send` never raises and never swallows
silently — it returns a :class:`SendOutcome` so callers can react honestly:
re-split a whole-batch rejection or re-queue survivors of a transient failure.
"""

from __future__ import annotations

import email.utils
import json
import threading
import time
from datetime import timezone
import urllib.error
import urllib.request
from typing import Callable, Iterable, Optional, Tuple, Union

from .persistence import FileBatchStore
from .types import BatchPayload
from .utils import debug_warn, warn

_MAX_BACKOFF_SECONDS = 10.0
_MAX_RETRY_AFTER_SECONDS = 3600.0
_HTTP_BATCH_REJECT_STATUS = 400

Sleep = Callable[[float], None]


class SendSuccess:
    """Batch accepted (2xx)."""

    def __repr__(self) -> str:
        return "SendSuccess()"


class SendRejected:
    """Server definitively refused the payload (non-retryable 4xx).

    ``is_batch_reject`` is true for HTTP 400: the server rejected the whole batch,
    which a single invalid event can cause.
    """

    def __init__(self, status: int) -> None:
        self.status = status
        self.is_batch_reject = status == _HTTP_BATCH_REJECT_STATUS

    def __repr__(self) -> str:
        return f"SendRejected(status={self.status}, is_batch_reject={self.is_batch_reject})"


class SendFailed:
    """Transient failure: network error, timeout, or 429/5xx after all retries."""

    def __init__(
        self,
        reason: str,
        retry_after_until: Optional[float] = None,
        durable: bool = False,
    ) -> None:
        self.reason = reason
        self.retry_after_until = retry_after_until
        self.durable = durable

    def __repr__(self) -> str:
        return (
            f"SendFailed(reason={self.reason!r}, retry_after_until={self.retry_after_until!r}, "
            f"durable={self.durable!r})"
        )


SendOutcome = Union[SendSuccess, SendRejected, SendFailed]

_SUCCESS = SendSuccess()


class HttpTransport:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        max_retries: int = 3,
        request_timeout: float = 10.0,
        retry_backoff_base: float = 1.0,
        debug: bool = False,
        sleep: Optional[Sleep] = None,
        persistence_path: Optional[str] = None,
        max_pending_events: int = 1000,
    ) -> None:
        self.endpoint = endpoint
        self.api_key = api_key
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.retry_backoff_base = retry_backoff_base
        self.debug = debug
        self._sleep: Sleep = sleep if sleep is not None else time.sleep
        self._store = FileBatchStore(persistence_path, max_pending_events)
        self._delivery_lock = threading.RLock()

    def send(self, payload: BatchPayload, deadline: Optional[float] = None) -> SendOutcome:
        """POST one batch until it succeeds, is rejected as non-retryable, or retries
        run out. ``deadline`` is an absolute monotonic deadline. Returns a
        :class:`SendOutcome`; never raises."""
        acquired = False
        try:
            body = json.dumps(payload.to_dict(), separators=(",", ":")).encode("utf-8")
            acquired = self._acquire_delivery_lock(deadline)
            if not acquired:
                return SendFailed("delivery deadline elapsed before send")
            self._store.put(payload.batch_id, body, len(payload.events))
            return self._send_record(payload.batch_id, body, deadline)
        except Exception as exc:  # noqa: BLE001 - persistence failures are delivery outcomes
            durable = self._store.enabled and self._store.contains(payload.batch_id)
            reason = f"{type(exc).__name__}: {exc}"
            warn(f"Transport persistence failed — delivery unresolved: {reason}")
            return SendFailed(reason, durable=durable)
        finally:
            if acquired:
                self._delivery_lock.release()

    def persist(self, payload: BatchPayload) -> bool:
        """Persist one exact batch without attempting the network."""
        try:
            body = json.dumps(payload.to_dict(), separators=(",", ":")).encode("utf-8")
            # FileBatchStore serializes its own mutations. Do not wait on the delivery
            # lock here: finite shutdown uses this path while a network send may still
            # own that lock beyond the caller's deadline.
            self._store.put(payload.batch_id, body, len(payload.events))
            return self._store.contains(payload.batch_id)
        except Exception as exc:  # noqa: BLE001 - shutdown must report storage failure
            warn(f"Shutdown persistence failed ({type(exc).__name__}: {exc})")
            return False

    def recover(self, deadline: Optional[float] = None) -> bool:
        """Replay persisted bodies exactly within an optional monotonic deadline."""
        if not self._acquire_delivery_lock(deadline):
            return False
        try:
            for record in self._store.snapshot():
                paused_until = record["paused_until"]
                if paused_until is not None:
                    remaining = max(0.0, paused_until - time.time())
                    if remaining > 0:
                        if self._delay_exceeds_deadline(remaining, deadline):
                            return False
                        self._sleep(remaining)
                outcome = self._send_record(
                    record["batch_id"], record["body"].encode("utf-8"), deadline
                )
                # Terminal responses have already been acknowledged and must not block
                # later durable batches. Only a transient failure stops ordered replay.
                if isinstance(outcome, SendFailed):
                    return False
            return True
        finally:
            self._delivery_lock.release()

    def close(self) -> None:
        """Release exclusive ownership of the persistence path."""
        with self._delivery_lock:
            self._store.close()

    def reset_for_child(self) -> None:
        """Replace inherited locks and detach the child from the parent's WAL."""
        self._delivery_lock = threading.RLock()
        if self._store.reset_for_child():
            warn(
                "Fork detected — inherited persistence disabled in child; create a new "
                "client with a child-specific persistence_path for durable child delivery"
            )

    @property
    def durable_pending_events(self) -> int:
        return self._store.pending_events

    def durable_pending_snapshot(self, active_batch_ids: Iterable[str]) -> Tuple[int, int]:
        """Return durable events and those duplicated by active caller counters."""
        return self._store.pending_snapshot(active_batch_ids)

    @property
    def durable_enabled(self) -> bool:
        return self._store.enabled

    def _send_record(
        self, batch_id: str, body: bytes, deadline: Optional[float] = None
    ) -> SendOutcome:
        last_error = "unknown error"
        retry_after: Optional[float] = None
        retry_after_until: Optional[float] = None

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                # Exponential backoff and server Retry-After use separate documented
                # ceilings; Retry-After is not shortened to the smaller client cap.
                exponent = min(attempt - 1, 63)
                delay = min(self.retry_backoff_base * (2**exponent), _MAX_BACKOFF_SECONDS)
                if retry_after is not None:
                    delay = max(retry_after, 0.0)
                    retry_after = None
                if self._delay_exceeds_deadline(delay, deadline):
                    last_error = f"delivery deadline elapsed after {last_error}"
                    break
                if self.debug:
                    debug_warn(f"Transport: attempt {attempt} failed ({last_error}), retrying in {delay:.1f}s")
                self._sleep(delay)

            request_timeout = self.request_timeout
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    last_error = "delivery deadline elapsed before request"
                    break
                request_timeout = min(request_timeout, remaining)
            try:
                status, retry_after = self._post(body, request_timeout)
            except Exception as exc:  # noqa: BLE001 - network errors are retried, then reported
                last_error = f"{type(exc).__name__}: {exc}"
                retry_after = None
                continue

            if 200 <= status < 300:
                self._store.acknowledge(batch_id)
                return _SUCCESS
            if 300 <= status < 400:
                warn(f"Transport: HTTP {status} redirect — not retrying")
                self._store.acknowledge(batch_id)
                return SendRejected(status)
            if 400 <= status < 500 and status != 429:
                warn(f"Transport: HTTP {status} — not retrying")
                self._store.acknowledge(batch_id)
                return SendRejected(status)
            if retry_after is not None:
                retry_after_until = time.time() + retry_after
            last_error = f"HTTP {status}"

        durable = self._store.enabled
        if durable:
            self._store.pause(batch_id, retry_after_until)
            warn(f"Transport: all retries exhausted — batch retained for restart: {last_error}")
        else:
            warn(f"Transport: all retries exhausted — batch not delivered: {last_error}")
        return SendFailed(last_error, retry_after_until, durable=durable)

    def _post(
        self, body: bytes, request_timeout: Optional[float] = None
    ) -> Tuple[int, Optional[float]]:
        """Perform one POST and return ``(status, retry_after_seconds)`` without raising
        for non-2xx statuses (urllib turns those into ``HTTPError``). The second element
        is the parsed ``Retry-After`` header when the server sent one, else ``None``."""
        request = urllib.request.Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
        )
        try:
            timeout = self.request_timeout if request_timeout is None else request_timeout
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return int(response.status), parse_retry_after(response.headers)
        except urllib.error.HTTPError as error:
            headers = error.headers
            code = int(error.code)
            error.close()
            return code, parse_retry_after(headers)

    def _acquire_delivery_lock(self, deadline: Optional[float]) -> bool:
        if deadline is None:
            self._delivery_lock.acquire()
            return True
        remaining = deadline - time.monotonic()
        return remaining > 0.0 and self._delivery_lock.acquire(timeout=remaining)

    @staticmethod
    def _delay_exceeds_deadline(delay: float, deadline: Optional[float]) -> bool:
        return deadline is not None and time.monotonic() + delay > deadline


def parse_retry_after(headers: Optional[object]) -> Optional[float]:
    """Parse a ``Retry-After`` header into seconds: a delta-seconds number or an
    HTTP-date, capped at one hour. Returns ``None`` when absent or unparseable; a
    past date yields ``0.0``."""
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        try:
            return min(float(int(value)), _MAX_RETRY_AFTER_SECONDS)
        except (OverflowError, ValueError):
            return None
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:  # pragma: no cover - parsedate_to_datetime raises instead on bad input
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return min(max(0.0, when.timestamp() - time.time()), _MAX_RETRY_AFTER_SECONDS)
