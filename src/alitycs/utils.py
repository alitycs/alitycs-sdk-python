"""Shared helpers: id generation, property serialization, limits, debug logging."""

from __future__ import annotations

import json
import sys
import time
import uuid
from typing import Any, Dict, Mapping


class EventRejectedError(ValueError):
    """Raised when an event violates a canonical ingestion limit.

    The event is rejected locally: it is never queued and never sent. The server
    rejects an entire batch when a single event violates these limits, so sending
    would poison every other event in the batch.
    """


class Limits:
    """Canonical ingestion limits — encode the server's ``EventValidator`` exactly."""

    MAX_PROPERTIES_COUNT = 50
    MAX_PROPERTY_KEY_LENGTH = 100
    MAX_PROPERTY_VALUE_LENGTH = 1000
    MAX_EVENT_SIZE_BYTES = 64 * 1024

    # Constant overhead the server adds to every event when estimating its size.
    EVENT_SIZE_OVERHEAD = 200

    # Timestamps below this are seconds-scale, not epoch milliseconds.
    MIN_EPOCH_MILLIS = 1_000_000_000_000

    MAX_EVENT_AGE_MS = 7 * 24 * 60 * 60 * 1000


def warn(message: str) -> None:
    """Emit a warning on stderr. Warn-level diagnostics are never debug-gated:
    dropped or rejected events must be visible by default."""
    print(f"[Alitycs] WARN {message}", file=sys.stderr)


def debug_warn(message: str) -> None:
    """Emit a diagnostic line on stderr; called only when ``debug`` is enabled."""
    print(f"[Alitycs] {message}", file=sys.stderr)


def generate_id() -> str:
    """Return a random UUID string; call sites add their own ``evt_``/``sess_`` prefixes."""
    return str(uuid.uuid4())


def now_ms() -> int:
    """Current Unix timestamp in milliseconds."""
    return int(time.time() * 1000)


def serialize_properties(props: Mapping[str, Any]) -> Dict[str, str]:
    """Flatten event properties to the wire contract's all-strings shape.

    Mirrors @alitycs/core: ``None`` is dropped, scalars are stringified (booleans as
    JSON-style ``true``/``false``), and containers are serialized as JSON strings.

    Raises :class:`EventRejectedError` when the property set violates the canonical
    limits; user data is never truncated silently.
    """
    if len(props) > Limits.MAX_PROPERTIES_COUNT:
        raise EventRejectedError(
            f"Event rejected locally: {len(props)} properties exceeds the maximum of "
            f"{Limits.MAX_PROPERTIES_COUNT} per event"
        )
    result: Dict[str, str] = {}
    for key, value in props.items():
        if len(key) > Limits.MAX_PROPERTY_KEY_LENGTH:
            raise EventRejectedError(
                f"Event rejected locally: property key {key!r} exceeds the maximum of "
                f"{Limits.MAX_PROPERTY_KEY_LENGTH} characters"
            )
        if value is None:
            continue
        if isinstance(value, str):
            serialized = value
        elif isinstance(value, bool):
            serialized = "true" if value else "false"
        elif isinstance(value, (dict, list, tuple)):
            serialized = json.dumps(value)
        else:
            serialized = str(value)
        if len(serialized) > Limits.MAX_PROPERTY_VALUE_LENGTH:
            raise EventRejectedError(
                f"Event rejected locally: value for property key {key!r} exceeds the "
                f"maximum of {Limits.MAX_PROPERTY_VALUE_LENGTH} characters"
            )
        result[key] = serialized
    return result


def validate_event(event: Any) -> None:
    """Validate a fully built event against the canonical server limits.

    Raises :class:`EventRejectedError` listing every violation.
    """
    errors = []

    if not event.event.strip():
        errors.append("action is required and cannot be blank")
    anonymous_blank = not isinstance(event.anonymous_id, str) or not event.anonymous_id.strip()
    user_blank = event.user_id is None or not str(event.user_id).strip()
    if anonymous_blank and user_blank:
        errors.append("at least one of userId or anonymousId is required")

    timestamp = event.timestamp
    if not isinstance(timestamp, int) or isinstance(timestamp, bool):
        errors.append("timestamp must be epoch milliseconds as an integer")
    else:
        if timestamp < Limits.MIN_EPOCH_MILLIS:
            errors.append(
                f"timestamp must be epoch milliseconds (got {timestamp}, which looks "
                "like seconds-scale)"
            )
        else:
            now = now_ms()
            if timestamp > now:
                errors.append("timestamp cannot be in the future")
            elif timestamp < now - Limits.MAX_EVENT_AGE_MS:
                errors.append("timestamp is too old (older than 7 days)")

    properties = event.properties or {}
    if len(properties) > Limits.MAX_PROPERTIES_COUNT:
        errors.append(
            f"properties contains too many entries "
            f"(max {Limits.MAX_PROPERTIES_COUNT}, got {len(properties)})"
        )
    for key, value in properties.items():
        if len(key) > Limits.MAX_PROPERTY_KEY_LENGTH:
            errors.append(
                f"property key {key!r} exceeds the maximum of "
                f"{Limits.MAX_PROPERTY_KEY_LENGTH} characters"
            )
        if len(value) > Limits.MAX_PROPERTY_VALUE_LENGTH:
            errors.append(
                f"value for property key {key!r} exceeds the maximum of "
                f"{Limits.MAX_PROPERTY_VALUE_LENGTH} characters"
            )

    estimated_size = (
        len((event.user_id or "").encode("utf-8"))
        + len((event.anonymous_id or "").encode("utf-8"))
        + len(event.event.encode("utf-8"))
        + sum(len(key.encode("utf-8")) + len(value.encode("utf-8")) for key, value in properties.items())
        + Limits.EVENT_SIZE_OVERHEAD
    )
    if estimated_size > Limits.MAX_EVENT_SIZE_BYTES:
        errors.append(
            f"event size (~{estimated_size} bytes) exceeds the maximum allowed size "
            f"({Limits.MAX_EVENT_SIZE_BYTES} bytes)"
        )

    if errors:
        raise EventRejectedError("Event rejected locally: " + "; ".join(errors))
