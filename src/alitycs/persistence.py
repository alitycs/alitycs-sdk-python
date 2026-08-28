"""Atomic file-backed storage for exact serialized in-flight batches."""

from __future__ import annotations

import json
import math
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

    def __init__(self, path: Optional[str], max_pending_events: int = 1000) -> None:
        if max_pending_events < 1:
            raise ValueError("Alitycs persistence max_pending_events must be positive")
        self._path = Path(path) if path is not None else None
        self._max_pending_events = max_pending_events
        self._lock = threading.RLock()
        self._records: Dict[str, DurableBatchRecord] = {}
        if self._path is not None and self._path.is_file():
            try:
                state = json.loads(self._path.read_text(encoding="utf-8"))
                if (
                    not isinstance(state, dict)
                    or state.get("version") != 1
                    or not isinstance(state.get("batches"), list)
                ):
                    raise ValueError("unsupported persistence schema")
                for raw in state["batches"]:
                    if not isinstance(raw, dict):
                        raise ValueError("invalid persistence record")
                    batch_id = raw.get("batch_id")
                    body = raw.get("body")
                    event_count = raw.get("event_count")
                    paused_until = raw.get("paused_until")
                    if (
                        not isinstance(batch_id, str)
                        or not isinstance(body, str)
                        or not isinstance(event_count, int)
                        or isinstance(event_count, bool)
                        or event_count < 1
                        or (
                            paused_until is not None
                            and (
                                not isinstance(paused_until, (int, float))
                                or isinstance(paused_until, bool)
                                or not math.isfinite(float(paused_until))
                            )
                        )
                    ):
                        raise ValueError("invalid persistence record")
                    if batch_id in self._records:
                        raise ValueError("duplicate persistence record")
                    record = DurableBatchRecord(
                        batch_id=batch_id,
                        body=body,
                        event_count=event_count,
                        paused_until=(
                            None if paused_until is None else float(paused_until)
                        ),
                    )
                    self._records[record["batch_id"]] = record
                if self.pending_events > self._max_pending_events:
                    raise ValueError("persistence event limit exceeded")
            except (OSError, TypeError, ValueError, OverflowError, KeyError, json.JSONDecodeError) as exc:
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
            if event_count < 1 or self.pending_events + event_count > self._max_pending_events:
                raise ValueError("Alitycs persistence event limit exceeded")
            previous = self._copy_records()
            self._records[batch_id] = DurableBatchRecord(
                batch_id=batch_id,
                body=body.decode("utf-8"),
                event_count=event_count,
                paused_until=None,
            )
            try:
                self._persist()
            except Exception:
                self._records = previous
                raise

    def acknowledge(self, batch_id: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            if batch_id not in self._records:
                return
            previous = self._copy_records()
            self._records.pop(batch_id)
            try:
                self._persist()
            except Exception:
                self._records = previous
                raise

    def pause(self, batch_id: str, paused_until: Optional[float]) -> None:
        if not self.enabled:
            return
        with self._lock:
            record = self._records.get(batch_id)
            if record is not None:
                previous = self._copy_records()
                record["paused_until"] = paused_until
                try:
                    self._persist()
                except Exception:
                    self._records = previous
                    raise

    def reset_for_child(self) -> bool:
        """Drop state and inherited locks after ``fork()``.

        A persistence path has a single-process owner. Letting both parent and child
        mutate the inherited snapshot can corrupt it or replay the same batches, so the
        child continues in non-durable mode. Applications that need child durability
        should create a fresh client with a child-specific path.

        Returns ``True`` when inherited durable state was disabled.
        """
        inherited = self._path is not None
        self._lock = threading.RLock()
        self._records = {}
        self._path = None
        return inherited

    def snapshot(self) -> List[DurableBatchRecord]:
        with self._lock:
            return [record.copy() for record in self._records.values()]  # type: ignore[misc]

    @property
    def pending_events(self) -> int:
        with self._lock:
            return sum(record["event_count"] for record in self._records.values())

    def contains(self, batch_id: str) -> bool:
        with self._lock:
            return batch_id in self._records

    def _persist(self) -> None:
        assert self._path is not None
        if not self._records:
            try:
                self._path.unlink()
            except FileNotFoundError:
                pass
            self._sync_parent_best_effort()
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
            self._sync_parent_best_effort()
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def _copy_records(self) -> Dict[str, DurableBatchRecord]:
        return {batch_id: record.copy() for batch_id, record in self._records.items()}  # type: ignore[misc]

    def _sync_parent_best_effort(self) -> None:
        """Persist directory metadata where the platform supports directory fsync."""
        assert self._path is not None
        try:
            descriptor = os.open(self._path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)
