"""Process context collection for server-side events."""

from __future__ import annotations

import platform
from datetime import datetime, timezone
from typing import Callable, Optional

from .types import EventContext

SDK_LANGUAGE = "python"


def collect_context(sdk_version: str) -> EventContext:
    """Snapshot process metadata. Browser-only fields stay unset — a server has no page
    URL, referrer, or user agent to report, and none may be fabricated."""
    return EventContext(
        sdk_version=sdk_version,
        sdk_language=SDK_LANGUAGE,
        locale=_safe(_get_locale),
        timezone=_safe(_get_timezone),
        os_name=_safe(lambda: platform.system() or None),
        os_version=_safe(lambda: platform.release() or None),
        python_version=_safe(lambda: platform.python_version() or None),
    )


def _get_locale() -> Optional[str]:
    import locale

    value = locale.getlocale()
    if not value or not value[0]:
        return None
    # Python reports "en_US"; the contract expects BCP-47 ("en-US").
    return value[0].replace("_", "-") or None


def _get_timezone() -> Optional[str]:
    return datetime.now(timezone.utc).astimezone().tzname()


def _safe(getter: Callable[[], Optional[str]]) -> Optional[str]:
    try:
        return getter()
    except Exception:  # noqa: BLE001 - environment probing must never break tracking
        return None
