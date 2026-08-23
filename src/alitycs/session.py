"""Session and anonymous identity management."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

from .utils import generate_id, now_ms


@dataclass(frozen=True)
class SessionData:
    id: str
    anonymous_id: str
    start_time_ms: int
    last_activity_ms: int
    user_id: Optional[str] = None


class SessionManager:
    """Tracks the current session, rotating it after :attr:`session_timeout` seconds of
    inactivity. Rotation keeps the anonymous id stable for the process lifetime;
    only an explicit :meth:`reset` mints a new one.
    """

    def __init__(self, session_timeout: float = 1800.0) -> None:
        self._session_timeout = session_timeout
        self._lock = threading.Lock()
        self._session = self._create_session()

    def get_session(self) -> SessionData:
        with self._lock:
            return self._session

    def touch(self) -> None:
        """Advance activity; rotates the session first when the timeout has elapsed."""
        with self._lock:
            if self._is_expired():
                self._session = self._create_session(anonymous_id=self._session.anonymous_id)
            else:
                self._session = SessionData(
                    id=self._session.id,
                    anonymous_id=self._session.anonymous_id,
                    start_time_ms=self._session.start_time_ms,
                    last_activity_ms=now_ms(),
                    user_id=self._session.user_id,
                )

    def set_user_id(self, user_id: str) -> None:
        with self._lock:
            self._session = SessionData(
                id=self._session.id,
                anonymous_id=self._session.anonymous_id,
                start_time_ms=self._session.start_time_ms,
                last_activity_ms=now_ms(),
                user_id=user_id,
            )

    def reset(self) -> SessionData:
        """Discard the session and anonymous identity entirely."""
        with self._lock:
            self._session = self._create_session()
            return self._session

    def reset_for_child(self) -> None:
        """Drop the lock inherited across ``os.fork``; the child keeps its data but must
        not share lock state with threads that exist only in the parent."""
        self._lock = threading.Lock()

    def _is_expired(self) -> bool:
        return now_ms() - self._session.last_activity_ms > self._session_timeout * 1000

    def _create_session(self, anonymous_id: Optional[str] = None) -> SessionData:
        timestamp = now_ms()
        return SessionData(
            id=f"sess_{generate_id()}",
            anonymous_id=anonymous_id or f"anon_{generate_id()}",
            start_time_ms=timestamp,
            last_activity_ms=timestamp,
        )
