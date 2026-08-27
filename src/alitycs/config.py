"""SDK configuration."""

from __future__ import annotations

import math
from dataclasses import dataclass, fields

DEFAULT_ENDPOINT = "https://api.alitycs.com/events"


@dataclass(frozen=True)
class AlitycsConfig:
    """Validated SDK configuration.

    ``flush_interval=None`` disables the periodic flush timer entirely; batching is
    then driven by :attr:`flush_size` and explicit :meth:`Alitycs.flush` calls only.
    """

    api_key: str
    endpoint: str = DEFAULT_ENDPOINT
    flush_size: int = 20
    flush_interval: float = 2.0
    max_queue_size: int = 1000
    max_retries: int = 3
    debug: bool = False
    session_timeout: float = 1800.0
    batching: bool = True
    request_timeout: float = 10.0
    retry_backoff_base: float = 1.0

    def __post_init__(self) -> None:
        if not isinstance(self.api_key, str) or not self.api_key.strip():
            raise ValueError("api_key is required")
        if not self.endpoint:
            raise ValueError("endpoint is required")
        _require_positive_int(self, "flush_size")
        _require_positive_int(self, "max_queue_size")
        if not isinstance(self.max_retries, int) or isinstance(self.max_retries, bool) or self.max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")
        if self.flush_interval is not None and self.flush_interval <= 0:
            raise ValueError("flush_interval must be positive or None to disable the timer")
        _require_positive_number(self, "request_timeout")
        _require_positive_number(self, "retry_backoff_base")
        _require_positive_number(self, "session_timeout")

    def __repr__(self) -> str:
        # Mask the api_key so logging a config (or an exception carrying one) can never
        # leak credentials; only the last four characters stay identifiable.
        masked = f"…{self.api_key[-4:]}" if len(self.api_key) > 4 else "…"
        rendered = ", ".join(
            f"{field.name}={masked!r}" if field.name == "api_key" else f"{field.name}={getattr(self, field.name)!r}"
            for field in fields(self)
        )
        return f"{type(self).__name__}({rendered})"


def _require_positive_int(config: AlitycsConfig, field_name: str) -> None:
    value = getattr(config, field_name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_positive_number(config: AlitycsConfig, field_name: str) -> None:
    """Validate a non-bool, finite number > 0.

    ``retry_backoff_base`` especially must be caught here: a negative base reaches
    ``time.sleep()`` in the transport retry loop and raises there — outside the send's
    error handling, so every retried batch would be lost to the caller.
    """
    value = getattr(config, field_name)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field_name} must be a positive number")
