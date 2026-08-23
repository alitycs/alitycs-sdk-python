"""The :class:`Alitycs` client plus module-level convenience functions."""

from __future__ import annotations

import atexit
import os
import threading
import weakref
from typing import Any, Dict, Mapping, Optional

from .batch import BatchManager
from .config import DEFAULT_ENDPOINT, AlitycsConfig
from .context import collect_context
from .session import SessionManager
from .transport import HttpTransport
from .types import AnalyticsEvent, BatchPayload, EventType, RevenuePayload
from .utils import debug_warn, generate_id, now_ms, serialize_properties

__version__ = "1.0.0"

_DEFAULT_SHUTDOWN_JOIN_TIMEOUT = 30.0


class Alitycs:
    """Alitycs analytics client for Python servers.

    Events are queued and dispatched in batches on a daemon flusher thread, so
    ``track`` never blocks on network I/O. Call :meth:`flush` to pin a delivery point,
    or :meth:`shutdown` before process exit — it drains fully and loses nothing.
    """

    def __init__(
        self,
        api_key: str,
        endpoint: str = DEFAULT_ENDPOINT,
        flush_size: int = 20,
        flush_interval: Optional[float] = 2.0,
        debug: bool = False,
        *,
        max_queue_size: int = 1000,
        max_retries: int = 3,
        session_timeout: float = 1800.0,
        batching: bool = True,
        request_timeout: float = 10.0,
        retry_backoff_base: float = 1.0,
    ) -> None:
        self._config = AlitycsConfig(
            api_key=api_key,
            endpoint=endpoint,
            flush_size=flush_size,
            flush_interval=flush_interval,
            max_queue_size=max_queue_size,
            max_retries=max_retries,
            debug=debug,
            session_timeout=session_timeout,
            batching=batching,
            request_timeout=request_timeout,
            retry_backoff_base=retry_backoff_base,
        )
        self._transport = HttpTransport(
            endpoint=self._config.endpoint,
            api_key=self._config.api_key,
            max_retries=self._config.max_retries,
            request_timeout=self._config.request_timeout,
            retry_backoff_base=self._config.retry_backoff_base,
            debug=self._config.debug,
        )
        self._session_manager = SessionManager(self._config.session_timeout)
        self._batch_manager: Optional[BatchManager] = (
            BatchManager(
                flush_size=self._config.flush_size,
                flush_interval=self._config.flush_interval,
                max_queue_size=self._config.max_queue_size,
                send_fn=self._transport.send,
                debug=self._config.debug,
            )
            if self._config.batching
            else None
        )
        # Guards identity and global property mutations across threads.
        self._identity_lock = threading.RLock()
        self._user_id: Optional[str] = None
        self._global_properties: Dict[str, Any] = {}
        _LIVE_INSTANCES.add(self)

    @property
    def config(self) -> AlitycsConfig:
        return self._config

    @property
    def pending(self) -> int:
        """Events not yet delivered: queued plus in an in-flight send."""
        if self._batch_manager is None:
            return 0
        return self._batch_manager.pending

    def track(
        self,
        event_name: str,
        properties: Optional[Mapping[str, Any]] = None,
        *,
        user_id: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        """Record a custom event. Blank names are ignored."""
        if not event_name or not event_name.strip():
            return
        self._enqueue(EventType.TRACK, event_name, properties, user_id=user_id, timestamp=timestamp)

    def track_revenue(
        self,
        payload: RevenuePayload,
        properties: Optional[Mapping[str, Any]] = None,
        *,
        user_id: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        """Ingest a trusted revenue fact. Requires a key with ``revenue:write``."""
        if not isinstance(payload, RevenuePayload):
            raise TypeError("track_revenue expects a RevenuePayload built from one of its constructors")
        self._enqueue(
            EventType.TRACK,
            f"revenue_{payload.kind}",
            properties,
            revenue=payload,
            user_id=user_id,
            timestamp=timestamp,
        )

    def capture_error(
        self,
        error_name: str,
        properties: Optional[Mapping[str, Any]] = None,
        *,
        user_id: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        """Record a handled error. Blank names are ignored."""
        if not error_name or not error_name.strip():
            return
        self._enqueue(EventType.ERROR, error_name, properties, user_id=user_id, timestamp=timestamp)

    def identify(
        self,
        user_id: str,
        traits: Optional[Mapping[str, Any]] = None,
        *,
        timestamp: Optional[int] = None,
    ) -> None:
        """Bind subsequent events to ``user_id``; blank ids are ignored."""
        if not user_id or not user_id.strip():
            return
        with self._identity_lock:
            self._user_id = user_id
        self._session_manager.set_user_id(user_id)
        merged: Dict[str, Any] = {"userId": user_id}
        if traits:
            merged.update(traits)
        self._enqueue(EventType.IDENTIFY, "identify", merged, timestamp=timestamp)

    def page(
        self,
        name: Optional[str] = None,
        properties: Optional[Mapping[str, Any]] = None,
        *,
        user_id: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        """Record a named screen or route view; servers have no page context beyond
        the name they pass in."""
        page_name = name if name and name.strip() else "page_view"
        self._enqueue(EventType.PAGE, page_name, properties, user_id=user_id, timestamp=timestamp)

    def reset(self) -> None:
        """Forget the identified user and rotate both session and anonymous ids."""
        with self._identity_lock:
            self._user_id = None
        self._session_manager.reset()

    def set_global_properties(self, properties: Mapping[str, Any]) -> None:
        """Attach ``properties`` to every event recorded from now on."""
        with self._identity_lock:
            self._global_properties.update(dict(properties))

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Send everything queued and wait for in-flight sends. Returns ``False`` only
        when a ``timeout`` was given and elapsed before the drain finished."""
        if self._batch_manager is None:
            return True
        return self._batch_manager.flush(timeout)

    def shutdown(self, join_timeout: Optional[float] = _DEFAULT_SHUTDOWN_JOIN_TIMEOUT) -> None:
        """Stop accepting events and drain fully. Safe to call from ``atexit``
        handlers and more than once."""
        if self._batch_manager is not None:
            self._batch_manager.shutdown(join_timeout)
        _LIVE_INSTANCES.discard(self)

    def _enqueue(
        self,
        event_type: EventType,
        name: str,
        properties: Optional[Mapping[str, Any]],
        revenue: Optional[RevenuePayload] = None,
        user_id: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        self._session_manager.touch()
        session = self._session_manager.get_session()
        with self._identity_lock:
            merged: Dict[str, Any] = dict(self._global_properties)
            effective_user = user_id or self._user_id
        if properties:
            merged.update(properties)

        event = AnalyticsEvent(
            event_id=f"evt_{generate_id()}",
            event=name,
            event_type=event_type,
            user_id=effective_user,
            anonymous_id=session.anonymous_id,
            session_id=session.id,
            timestamp=timestamp if timestamp is not None else now_ms(),
            properties=serialize_properties(merged),
            revenue=revenue,
            context=collect_context(__version__),
        )

        if self._batch_manager is not None:
            self._batch_manager.add(event)
        else:
            # Batching disabled: deliver inline, still best-effort.
            payload = BatchPayload(
                batch_id=f"batch_{generate_id()}",
                sent_at=now_ms(),
                events=[event],
            )
            try:
                self._transport.send(payload)
            except Exception as exc:  # noqa: BLE001 - analytics must never break the host
                if self._config.debug:
                    debug_warn(f"Batch send failed — events dropped ({exc})")

    def _reset_for_child(self) -> None:
        """Post-fork repair in the child process: drop inherited locks and forget the
        parent's flusher thread so a fresh one starts lazily."""
        self._batch_manager.reset_for_child()
        self._session_manager.reset_for_child()
        self._identity_lock = threading.RLock()


# Module-level convenience API over a default instance ----------------------------

_default_instance: Optional[Alitycs] = None
_LIVE_INSTANCES: "weakref.WeakSet[Alitycs]" = weakref.WeakSet()


def init(*args: Any, **kwargs: Any) -> Alitycs:
    """Create an :class:`Alitycs` instance and install it as the module default."""
    global _default_instance
    instance = Alitycs(*args, **kwargs)
    _default_instance = instance
    return instance


def get_default_instance() -> Optional[Alitycs]:
    return _default_instance


def track(event_name: str, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    if _default_instance is not None:
        _default_instance.track(event_name, properties, **kwargs)


def track_revenue(payload: RevenuePayload, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    if _default_instance is not None:
        _default_instance.track_revenue(payload, properties, **kwargs)


def capture_error(error_name: str, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    if _default_instance is not None:
        _default_instance.capture_error(error_name, properties, **kwargs)


def identify(user_id: str, traits: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    if _default_instance is not None:
        _default_instance.identify(user_id, traits, **kwargs)


def page(name: Optional[str] = None, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    if _default_instance is not None:
        _default_instance.page(name, properties, **kwargs)


def reset() -> None:
    if _default_instance is not None:
        _default_instance.reset()


def set_global_properties(properties: Mapping[str, Any]) -> None:
    if _default_instance is not None:
        _default_instance.set_global_properties(properties)


def flush(timeout: Optional[float] = None) -> bool:
    if _default_instance is not None:
        return _default_instance.flush(timeout)
    return True


def shutdown(**kwargs: Any) -> None:
    global _default_instance
    if _default_instance is not None:
        _default_instance.shutdown(**kwargs)
        _default_instance = None


def _shutdown_all_at_exit() -> None:
    """Interpreter-exit safety net for apps that never call ``shutdown()``."""
    for instance in list(_LIVE_INSTANCES):
        try:
            instance.shutdown()
        except Exception:  # noqa: BLE001 - exit handlers must not raise
            pass


atexit.register(_shutdown_all_at_exit)

if hasattr(os, "register_at_fork"):

    def _reset_children() -> None:
        for instance in list(_LIVE_INSTANCES):
            instance._reset_for_child()

    os.register_at_fork(after_in_child=_reset_children)
