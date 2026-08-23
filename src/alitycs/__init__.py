"""Official Alitycs analytics SDK for Python servers.

Quickstart::

    import alitycs

    alitycs.init("pk_...", flush_size=20, flush_interval=2.0)
    alitycs.identify("usr_123", {"plan": "pro"})
    alitycs.track("checkout_completed", {"total": "19.99"})
    alitycs.shutdown()  # drains fully; nothing queued is lost

Zero runtime dependencies — HTTP goes through ``urllib.request``.
"""

from .client import (
    Alitycs,
    capture_error,
    flush,
    get_default_instance,
    identify,
    init,
    page,
    reset,
    set_global_properties,
    shutdown,
    track,
    track_revenue,
)
from .config import DEFAULT_ENDPOINT, AlitycsConfig
from .types import (
    AnalyticsEvent,
    BatchPayload,
    EventContext,
    EventType,
    RevenueError,
    RevenuePayload,
)

__version__ = "1.0.0"

__all__ = [
    "Alitycs",
    "AlitycsConfig",
    "AnalyticsEvent",
    "BatchPayload",
    "DEFAULT_ENDPOINT",
    "EventContext",
    "EventType",
    "RevenueError",
    "RevenuePayload",
    "__version__",
    "capture_error",
    "flush",
    "get_default_instance",
    "identify",
    "init",
    "page",
    "reset",
    "set_global_properties",
    "shutdown",
    "track",
    "track_revenue",
]
