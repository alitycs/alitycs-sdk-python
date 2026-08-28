"""The :class:`Alitycs` client plus module-level convenience functions."""

from __future__ import annotations

import atexit
import os
import signal
import threading
from typing import Any, Dict, Mapping, Optional

from .batch import BatchManager
from .config import DEFAULT_ENDPOINT, AlitycsConfig
from .context import collect_context
from .session import SessionManager
from .transport import HttpTransport, SendFailed, SendRejected
from .types import AnalyticsEvent, BatchPayload, EventType, RevenuePayload
from .utils import (
    EventRejectedError,
    generate_id,
    now_ms,
    serialize_properties,
    validate_event,
    warn,
)

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
        persistence_path: Optional[str] = None,
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
            persistence_path=persistence_path,
        )
        self._transport = HttpTransport(
            endpoint=self._config.endpoint,
            api_key=self._config.api_key,
            max_retries=self._config.max_retries,
            request_timeout=self._config.request_timeout,
            retry_backoff_base=self._config.retry_backoff_base,
            debug=self._config.debug,
            persistence_path=self._config.persistence_path,
        )
        self._session_manager = SessionManager(self._config.session_timeout)
        self._batch_manager: Optional[BatchManager] = (
            BatchManager(
                flush_size=self._config.flush_size,
                flush_interval=self._config.flush_interval,
                max_queue_size=self._config.max_queue_size,
                send_fn=self._transport.send,
                debug=self._config.debug,
                recover_fn=self._transport.recover,
                durable_pending_fn=lambda: self._transport.durable_pending_events,
                durable=self._transport.durable_enabled,
            )
            if self._config.batching
            else None
        )
        # Guards identity and global property mutations across threads.
        self._identity_lock = threading.RLock()
        self._user_id: Optional[str] = None
        self._global_properties: Dict[str, Any] = {}
        self._rejected_locally_count = 0
        _LIVE_INSTANCES.add(self)

    @property
    def config(self) -> AlitycsConfig:
        return self._config

    @property
    def is_shutdown(self) -> bool:
        """True once :meth:`shutdown` has run; a shut-down client accepts no events."""
        return self._batch_manager is not None and self._batch_manager.closed

    @property
    def pending(self) -> int:
        """Events not yet delivered: queued plus in an in-flight send."""
        if self._batch_manager is None:
            return self._transport.durable_pending_events
        return self._batch_manager.pending

    @property
    def rejected_locally(self) -> int:
        """Events rejected at build time for violating ingestion limits (also logged
        at warn level when they happen)."""
        with self._identity_lock:
            return self._rejected_locally_count

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
        """Send everything queued and wait for in-flight sends. Returns ``True`` only
        when every event was delivered; ``False`` when a send failed (survivors stay
        queued for a later flush) or a ``timeout`` was given and elapsed first."""
        if self._batch_manager is None:
            return self._transport.recover()
        return self._batch_manager.flush(timeout)

    def shutdown(self, join_timeout: Optional[float] = _DEFAULT_SHUTDOWN_JOIN_TIMEOUT) -> None:
        """Stop accepting events and drain fully. Safe to call from ``atexit``
        handlers and more than once."""
        if self._batch_manager is not None:
            self._batch_manager.shutdown(join_timeout)
        else:
            self._transport.recover()
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

        try:
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
            validate_event(event)
        except EventRejectedError as exc:
            # Rejected locally: never queued, never sent (the server would refuse the
            # whole batch). Surfaced at warn level and counted, never truncated.
            with self._identity_lock:
                self._rejected_locally_count += 1
            warn(str(exc))
            return

        if self._batch_manager is not None:
            self._batch_manager.add(event)
        else:
            # Batching disabled: deliver inline; failures are reported, not hidden.
            payload = BatchPayload(
                batch_id=f"batch_{generate_id()}",
                sent_at=now_ms(),
                events=[event],
            )
            outcome = self._transport.send(payload)
            if isinstance(outcome, SendRejected):
                warn(f"Server rejected event {event.event_id} with HTTP {outcome.status}")
            elif isinstance(outcome, SendFailed):
                warn(f"Transport failure ({outcome.reason}) — event {event.event_id} not delivered")

    def _reset_for_child(self) -> None:
        """Post-fork repair in the child process: drop inherited locks and forget the
        parent's flusher thread so a fresh one starts lazily."""
        self._batch_manager.reset_for_child()
        self._session_manager.reset_for_child()
        self._identity_lock = threading.RLock()


# Module-level convenience API over a default instance ----------------------------

_default_instance: Optional[Alitycs] = None
# Strong references on purpose: the daemon flusher thread keeps only the batch manager
# alive, so without this registry the garbage collector could collect an instance while
# its events are still in flight — escaping both shutdown() and the atexit safety net.
# Instances remove themselves in shutdown().
_LIVE_INSTANCES: "set[Alitycs]" = set()


def init(*args: Any, **kwargs: Any) -> Alitycs:
    """Create an :class:`Alitycs` instance and install it as the module default."""
    global _default_instance
    instance = Alitycs(*args, **kwargs)
    _default_instance = instance
    return instance


def get_default_instance() -> Optional[Alitycs]:
    """The current module default, if one has been initialized. Stays installed after
    ``shutdown()`` — check :attr:`Alitycs.is_shutdown` to see whether it is usable."""
    return _default_instance


def _default_for_write() -> Optional[Alitycs]:
    """The default instance to delegate to, or ``None`` for the pre-init no-op contract.

    Raises ``RuntimeError`` when the previous default has already been shut down:
    silently dropping events after shutdown hides data loss from callers.
    """
    instance = _default_instance
    if instance is not None and instance.is_shutdown:
        raise RuntimeError(
            "alitycs.<fn>() called after the default instance was shut down; "
            "create a new default with alitycs.init()"
        )
    return instance


def track(event_name: str, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    instance = _default_for_write()
    if instance is not None:
        instance.track(event_name, properties, **kwargs)


def track_revenue(payload: RevenuePayload, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    instance = _default_for_write()
    if instance is not None:
        instance.track_revenue(payload, properties, **kwargs)


def capture_error(error_name: str, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    instance = _default_for_write()
    if instance is not None:
        instance.capture_error(error_name, properties, **kwargs)


def identify(user_id: str, traits: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    instance = _default_for_write()
    if instance is not None:
        instance.identify(user_id, traits, **kwargs)


def page(name: Optional[str] = None, properties: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
    instance = _default_for_write()
    if instance is not None:
        instance.page(name, properties, **kwargs)


def reset() -> None:
    instance = _default_for_write()
    if instance is not None:
        instance.reset()


def set_global_properties(properties: Mapping[str, Any]) -> None:
    instance = _default_for_write()
    if instance is not None:
        instance.set_global_properties(properties)


def flush(timeout: Optional[float] = None) -> bool:
    instance = _default_for_write()
    if instance is None:
        return True
    return instance.flush(timeout)


def shutdown(**kwargs: Any) -> None:
    """Shut down the default instance (drains fully). Safe to call more than once and
    before :func:`init`; afterwards the module-level API raises until a new default
    is created with :func:`init`."""
    if _default_instance is not None:
        _default_instance.shutdown(**kwargs)


def _shutdown_all_at_exit() -> None:
    """Interpreter-exit safety net for apps that never call ``shutdown()``."""
    for instance in list(_LIVE_INSTANCES):
        try:
            instance.shutdown()
        except Exception:  # noqa: BLE001 - exit handlers must not raise
            pass


atexit.register(_shutdown_all_at_exit)


def _flush_all_live(timeout: float = 10.0) -> None:
    """Best-effort drain of every live instance; never raises."""
    for instance in list(_LIVE_INSTANCES):
        try:
            instance.flush(timeout)
        except Exception:  # noqa: BLE001 - signal handlers must not raise
            pass


def _make_termination_handler(signum: int):
    """Build the handler installed for SIGTERM/SIGINT: flush live instances, then
    restore the default disposition and re-raise the signal so process exit status
    still reflects the termination."""

    def _handle(signum_received: int, frame: Any) -> None:  # noqa: ARG001 - signal API
        _flush_all_live()
        try:
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
            return  # unreachable once the default handler terminates the process
        except Exception:  # noqa: BLE001 - last resort when re-raising is impossible
            raise SystemExit(128 + signum) from None

    return _handle


def _install_signal_handlers() -> None:
    """Register SIGTERM/SIGINT flush handlers from the main thread only. Non-main
    threads cannot install signal handlers (Python raises ``ValueError``); there the
    atexit safety net remains the fallback."""
    if threading.current_thread() is not threading.main_thread():
        return
    if os.name == "nt" and not hasattr(signal, "SIGTERM"):
        return
    for sig in (signal.SIGTERM, getattr(signal, "SIGINT", None)):
        if sig is None:
            continue
        try:
            signal.signal(sig, _make_termination_handler(sig))
        except (ValueError, OSError, RuntimeError):
            # Not permitted here (e.g. embedded interpreter or non-main thread race).
            pass


_install_signal_handlers()

if hasattr(os, "register_at_fork"):

    def _reset_children() -> None:
        for instance in list(_LIVE_INSTANCES):
            instance._reset_for_child()

    os.register_at_fork(after_in_child=_reset_children)
