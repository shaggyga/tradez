#!/usr/bin/env python3
"""Independent process heartbeat for long-running OANDA workers."""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except OSError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


class WorkerHeartbeat:
    """Write liveness independently from the worker's main processing loop."""

    def __init__(
        self,
        path: Path,
        *,
        worker: str,
        role: str = "",
        interval_sec: float = 5.0,
    ) -> None:
        self.path = Path(path)
        self.worker = str(worker)
        self.role = str(role)
        self.interval_sec = max(1.0, float(interval_sec))
        self.started_at = utc_now()
        self.started_monotonic = time.monotonic()
        self._lock = threading.Lock()
        self._details: dict[str, Any] = {}
        self._phase = "starting"
        self._phase_updated_at = self.started_at
        self._phase_updated_monotonic = self.started_monotonic
        self._progress_sequence = 0
        self._progress_updated_at = self.started_at
        self._progress_updated_monotonic = self.started_monotonic
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> "WorkerHeartbeat":
        if self._thread is not None:
            return self
        self._write("running")
        self._thread = threading.Thread(
            target=self._run,
            name=f"{self.worker}-heartbeat",
            daemon=True,
        )
        self._thread.start()
        return self

    def update(self, *, phase: str | None = None, **details: Any) -> None:
        with self._lock:
            if phase is not None and str(phase) != self._phase:
                self._phase = str(phase)
                self._phase_updated_at = utc_now()
                self._phase_updated_monotonic = time.monotonic()
            self._details.update(details)

    def mark_progress(self, *, phase: str | None = None, **details: Any) -> None:
        """Record durable main-loop progress separately from thread liveness."""
        now = utc_now()
        monotonic_now = time.monotonic()
        with self._lock:
            if phase is not None and str(phase) != self._phase:
                self._phase = str(phase)
                self._phase_updated_at = now
                self._phase_updated_monotonic = monotonic_now
            self._progress_sequence += 1
            self._progress_updated_at = now
            self._progress_updated_monotonic = monotonic_now
            self._details.update(details)

    @property
    def phase(self) -> str:
        """Return the current worker phase for precise error attribution."""
        with self._lock:
            return self._phase

    def _payload(self, status: str) -> dict[str, Any]:
        with self._lock:
            details = dict(self._details)
            phase = self._phase
            phase_updated_at = self._phase_updated_at
            phase_age_sec = max(
                0.0,
                time.monotonic() - self._phase_updated_monotonic,
            )
            progress_sequence = self._progress_sequence
            progress_updated_at = self._progress_updated_at
            progress_age_sec = max(
                0.0,
                time.monotonic() - self._progress_updated_monotonic,
            )
        return {
            "schema_version": 1,
            "worker": self.worker,
            "role": self.role,
            "status": status,
            "phase": phase,
            "phase_updated_at": phase_updated_at,
            "phase_age_sec": round(phase_age_sec, 3),
            "progress_sequence": progress_sequence,
            "progress_updated_at": progress_updated_at,
            "progress_age_sec": round(progress_age_sec, 3),
            "pid": os.getpid(),
            "started_at": self.started_at,
            "updated_at": utc_now(),
            "uptime_sec": round(max(0.0, time.monotonic() - self.started_monotonic), 3),
            "details": details,
        }

    def _write(self, status: str) -> None:
        try:
            write_json_atomic(self.path, self._payload(status))
        except OSError:
            return

    def _run(self) -> None:
        while not self._stop.wait(self.interval_sec):
            self._write("running")

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_sec + 1.0))
        self._write("stopped")

    def __enter__(self) -> "WorkerHeartbeat":
        return self.start()

    def __exit__(self, *_: Any) -> None:
        self.close()


__all__ = ["WorkerHeartbeat", "write_json_atomic"]
