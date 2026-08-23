"""HTTP transport: batch POST with exponential-backoff retry, zero dependencies.

Mirrors ``HttpTransport.kt``: ``max_retries`` retries after the initial attempt,
backoff doubling from :attr:`retry_backoff_base` capped at ten seconds, 4xx responses
(except 429) are not retried, and a batch whose attempts are exhausted is dropped —
analytics delivery is best-effort and must never crash the host application.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Callable, Optional

from .types import BatchPayload
from .utils import debug_warn

_MAX_BACKOFF_SECONDS = 10.0

Sleep = Callable[[float], None]


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
    ) -> None:
        self.endpoint = endpoint
        self.api_key = api_key
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.retry_backoff_base = retry_backoff_base
        self.debug = debug
        self._sleep: Sleep = sleep if sleep is not None else time.sleep

    def send(self, payload: BatchPayload) -> None:
        """POST one batch until it succeeds, is rejected as non-retryable, or retries
        run out. Returns nothing; failures are swallowed after logging."""
        body = json.dumps(payload.to_dict(), separators=(",", ":")).encode("utf-8")
        last_error = "unknown error"

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                delay = min(self.retry_backoff_base * (2 ** (attempt - 1)), _MAX_BACKOFF_SECONDS)
                if self.debug:
                    debug_warn(f"Transport: attempt {attempt} failed ({last_error}), retrying in {delay:.1f}s")
                self._sleep(delay)

            try:
                status = self._post(body)
            except Exception as exc:  # noqa: BLE001 - network errors are retried, then dropped
                last_error = f"{type(exc).__name__}: {exc}"
                continue

            if 200 <= status < 300:
                return
            if 400 <= status < 500 and status != 429:
                if self.debug:
                    debug_warn(f"Transport: {status} — not retrying")
                return
            last_error = f"HTTP {status}"

        if self.debug:
            debug_warn(f"Transport: all retries exhausted — dropping batch: {last_error}")

    def _post(self, body: bytes) -> int:
        """Perform one POST and return the HTTP status code without raising for
        non-2xx statuses (urllib turns those into ``HTTPError``)."""
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
                return int(response.status)
        except urllib.error.HTTPError as error:
            error.close()
            return int(error.code)
