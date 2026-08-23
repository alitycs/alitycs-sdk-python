"""Shared helpers: id generation, property serialization, debug logging."""

from __future__ import annotations

import json
import sys
import time
import uuid
from typing import Any, Dict, Mapping


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
    """
    result: Dict[str, str] = {}
    for key, value in props.items():
        if value is None:
            continue
        if isinstance(value, str):
            result[key] = value
        elif isinstance(value, bool):
            result[key] = "true" if value else "false"
        elif isinstance(value, (dict, list, tuple)):
            result[key] = json.dumps(value)
        else:
            result[key] = str(value)
    return result
