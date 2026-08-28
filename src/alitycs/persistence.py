"""Atomic file-backed storage for exact serialized in-flight batches."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Dict, List, Optional, TypedDict


class DurableBatchRecord(TypedDict):
    batch_id: str
    body: str
    event_count: int
    paused_until: Optional[float]


class FileBatchStore:
    """A single-process write-ahead log replaced atomically after every mutation."""

    def __init__(self, path: Optional[str]) -> None:
        self._path = Path(path) if path is not None else None
        self._lock = threading.RLock()
        self._records: Dict[str, DurableBatchRecord] = {}
        if self._path is not None and self._path.is_file():
            try:
                state = json.loads(self._path.read_text(encoding="utf-8"))
                if state.get("version") != 1 or not isinstance(state.get("batches"), list):
                    raise ValueError("unsupported persistence schema")
                for raw in state["batches"]:
                    record = DurableBatchRecord(
                        batch_id=str(raw["batch_id"]),
                        body=str(raw["body"]),
                        event_count=int(raw["event_count"]),
                        paused_until=(
                            None if raw.get("paused_until") is None else float(raw["paused_until"])
                        ),
                    )
                    self._records[record["batch_id"]] = record
            except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
                raise ValueError(f"Invalid Alitycs persistence file: {self._path}") from exc

    @property
    def enabled(self) -> bool:
        return self._path is not None

    def put(self, batch_id: str, body: bytes, event_count: int) -> None:
        if not self.enabled:
            return
        with self._lock:
            if batch_id in self._records:
                return
            self._records[batch_id] = DurableBatchRecord(
                batch_id=batch_id,
                body=body.decode("utf-8"),
                event_count=event_count,
                paused_until=None,
            )
            self._persist()

    def acknowledge(self, batch_id: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self._records.pop(batch_id, None) is not None:
                self._persist()

    def pause(self, batch_id: str, paused_until: Optional[float]) -> None:
        if not self.enabled:
            return
        with self._lock:
            record = self._records.get(batch_id)
            if record is not None:
                record["paused_until"] = paused_until
                self._persist()

    def snapshot(self) -> List[DurableBatchRecord]:
        with self._lock:
            return [record.copy() for record in self._records.values()]  # type: ignore[misc]

    @property
    def pending_events(self) -> int:
        with self._lock:
            return sum(record["event_count"] for record in self._records.values())

    def _persist(self) -> None:
        assert self._path is not None
        if not self._records:
            try:
                self._path.unlink()
            except FileNotFoundError:
                pass
            return

        self._path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=f"{self._path.name}.tmp.", dir=str(self._path.parent)
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(
                    {"version": 1, "batches": list(self._records.values())},
                    handle,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self._path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
