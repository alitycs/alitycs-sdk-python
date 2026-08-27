"""Process context collection for server-side events."""

from __future__ import annotations

import os
import platform
from datetime import datetime, timezone
from typing import Callable, List, Optional

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
    """Report the local timezone as an IANA identifier ("America/New_York").

    Abbreviations like "EST" are ambiguous (several zones share them) and carry no
    UTC offset, so an IANA key is resolved from ``TZ`` or the ``/etc/localtime``
    target first; the abbreviation stays as the fallback where neither resolves
    (notably Windows).
    """
    try:
        from zoneinfo import ZoneInfo

        key = _local_iana_key()
        if key:
            return ZoneInfo(key).key
    except Exception:  # noqa: BLE001 - environment probing must never break tracking
        pass
    return datetime.now(timezone.utc).astimezone().tzname()


def _local_iana_key() -> Optional[str]:
    """Best-effort IANA key of the system timezone, or ``None`` when unknowable."""
    candidates: List[str] = []
    env_tz = os.environ.get("TZ")
    if env_tz:
        candidates.append(env_tz.lstrip(":"))
    try:
        real = os.path.realpath("/etc/localtime")
        marker = "zoneinfo/"
        if marker in real:
            candidates.append(real.rsplit(marker, 1)[1])
    except OSError:
        pass
    for candidate in candidates:
        try:
            from zoneinfo import ZoneInfo

            return ZoneInfo(candidate).key
        except Exception:  # noqa: BLE001 - unresolvable candidates fall through
            continue
    return None


def _safe(getter: Callable[[], Optional[str]]) -> Optional[str]:
    try:
        return getter()
    except Exception:  # noqa: BLE001 - environment probing must never break tracking
        return None
