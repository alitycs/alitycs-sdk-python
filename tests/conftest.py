"""Shared test fixtures: a real local HTTP capture server (never a mocked urlopen)."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, List, Optional, Union

import pytest


class CapturedRequest(Dict[str, Any]):
    pass


class CaptureServer:
    """Records every POST as ``{sequence, headers, payload, raw, status}``.

    ``fail_on`` marks specific 1-based request sequences to answer 500 (once each),
    exercising the transport's retry path against real socket I/O. ``delay`` slows a
    matching response (seconds, or a callable receiving the request dict) so tests can
    observe a send genuinely in flight. A ``responder`` may return an int status or a
    ``(status, extra_headers)`` tuple (e.g. to send ``Retry-After`` with a 429).
    """

    def __init__(
        self,
        fail_on: tuple = (),
        responder: Optional[Callable[[Dict[str, Any]], int]] = None,
        delay: Union[float, Callable[[Dict[str, Any]], float], None] = None,
    ):
        self.requests: List[Dict[str, Any]] = []
        self._fail_on = set(fail_on)
        self._responder = responder
        self._delay = delay
        self._lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length)
                try:
                    payload = json.loads(raw)
                except Exception:
                    payload = None
                with outer._lock:
                    sequence = len(outer.requests) + 1
                    extra_headers: Dict[str, str] = {}
                    if responder is not None:
                        answered = responder({"sequence": sequence, "payload": payload})
                        if isinstance(answered, tuple):
                            status, extra_headers = answered
                        else:
                            status = answered
                    elif sequence in outer._fail_on:
                        status = 500
                    else:
                        status = 202
                    outer.requests.append(
                        {
                            "sequence": sequence,
                            "headers": {key.lower(): value for key, value in self.headers.items()},
                            "raw": raw,
                            "payload": payload,
                            "status": status,
                        }
                    )
                    seconds = 0.0
                    if callable(delay):
                        seconds = float(delay({"sequence": sequence, "payload": payload}))
                    elif isinstance(delay, (int, float)):
                        seconds = float(delay)
                    if seconds > 0:
                        time.sleep(seconds)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                for name, value in extra_headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(b'{"accepted":true}')

            def log_message(self, *args: Any) -> None:
                pass

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_port}/events"
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "CaptureServer":
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def payloads(self) -> List[Dict[str, Any]]:
        return [request["payload"] for request in self.requests]

    @property
    def events(self) -> List[Dict[str, Any]]:
        return [event for batch in self.payloads for event in (batch or {}).get("events", [])]

    @property
    def event_names(self) -> List[str]:
        return [event["event"] for event in self.events]

    def wait_for_event_count(self, count: int, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.events) >= count:
                return True
            time.sleep(0.01)
        return len(self.events) >= count


@pytest.fixture()
def capture_server() -> CaptureServer:
    server = CaptureServer().start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture()
def capture_factory():
    servers: List[CaptureServer] = []

    def make(*args: Any, **kwargs: Any) -> CaptureServer:
        server = CaptureServer(*args, **kwargs).start()
        servers.append(server)
        return server

    try:
        yield make
    finally:
        for server in servers:
            server.stop()
