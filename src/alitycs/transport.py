"""HTTP transport: batch POST with exponential-backoff retry, zero dependencies.

Mirrors ``HttpTransport.kt``: ``max_retries`` retries after the initial attempt,
backoff doubling from :attr:`retry_backoff_base` capped at ten seconds, 4xx responses
(except 429) are not retried. A 429 carrying ``Retry-After`` (delta-seconds or an
HTTP-date) is honoured: the next attempt waits at least that long instead of the
default backoff. :meth:`HttpTransport.send` never raises and never swallows
silently — it returns a :class:`SendOutcome` so callers can react honestly:
re-split a whole-batch rejection or re-queue survivors of a transient failure.
"""

from __future__ import annotations

import email.utils
import json
import threading
import time
import urllib.error
import urllib.request
from typing import Callable, Optional, Tuple, Union

from .persistence import FileBatchStore
from .types import BatchPayload
from .utils import debug_warn, warn

_MAX_BACKOFF_SECONDS = 10.0
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
    ) -> None:
        self.endpoint = endpoint
        self.api_key = api_key
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.retry_backoff_base = retry_backoff_base
        self.debug = debug
        self._sleep: Sleep = sleep if sleep is not None else time.sleep
        self._store = FileBatchStore(persistence_path)
        self._delivery_lock = threading.RLock()

    def send(self, payload: BatchPayload) -> SendOutcome:
        """POST one batch until it succeeds, is rejected as non-retryable, or retries
        run out. Returns a :class:`SendOutcome`; never raises."""
        body = json.dumps(payload.to_dict(), separators=(",", ":")).encode("utf-8")
        with self._delivery_lock:
            self._store.put(payload.batch_id, body, len(payload.events))
            return self._send_record(payload.batch_id, body)

    def recover(self) -> bool:
        """Replay persisted bodies exactly, honoring any remaining Retry-After pause."""
        with self._delivery_lock:
            for record in self._store.snapshot():
                paused_until = record["paused_until"]
                if paused_until is not None:
                    remaining = max(0.0, paused_until - time.time())
                    if remaining > 0:
                        self._sleep(remaining)
                outcome = self._send_record(record["batch_id"], record["body"].encode("utf-8"))
                if not isinstance(outcome, SendSuccess):
                    return False
            return True

    @property
    def durable_pending_events(self) -> int:
        return self._store.pending_events

    @property
    def durable_enabled(self) -> bool:
        return self._store.enabled

    def _send_record(self, batch_id: str, body: bytes) -> SendOutcome:
        last_error = "unknown error"
        retry_after: Optional[float] = None
        retry_after_until: Optional[float] = None

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                # A 429's Retry-After (delta-seconds or HTTP-date) replaces the default
                # backoff for the attempt that follows it, still capped at ten seconds.
                delay = min(self.retry_backoff_base * (2 ** (attempt - 1)), _MAX_BACKOFF_SECONDS)
                if retry_after is not None:
                    delay = min(max(retry_after, 0.0), _MAX_BACKOFF_SECONDS)
                    retry_after = None
                if self.debug:
                    debug_warn(f"Transport: attempt {attempt} failed ({last_error}), retrying in {delay:.1f}s")
                self._sleep(delay)

            try:
                status, retry_after = self._post(body)
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
            if status == 429:
                retry_after_until = None if retry_after is None else time.time() + retry_after
            last_error = f"HTTP {status}"

        self._store.pause(batch_id, retry_after_until)
        warn(f"Transport: all retries exhausted — batch retained for restart: {last_error}")
        return SendFailed(last_error, retry_after_until, durable=self._store.enabled)

    def _post(self, body: bytes) -> Tuple[int, Optional[float]]:
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
            with urllib.request.urlopen(request, timeout=self.request_timeout) as response:
                return int(response.status), parse_retry_after(response.headers)
        except urllib.error.HTTPError as error:
            headers = error.headers
            code = int(error.code)
            error.close()
            return code, parse_retry_after(headers)


def parse_retry_after(headers: Optional[object]) -> Optional[float]:
    """Parse a ``Retry-After`` header into seconds: a delta-seconds number or an
    HTTP-date. Returns ``None`` when absent or unparseable; a past date yields ``0.0``."""
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:  # pragma: no cover - parsedate_to_datetime raises instead on bad input
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=time.timezone)
    return max(0.0, when.timestamp() - time.time())
