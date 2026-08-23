"""SDK configuration."""

from __future__ import annotations

from dataclasses import dataclass

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


def _require_positive_int(config: AlitycsConfig, field_name: str) -> None:
    value = getattr(config, field_name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
