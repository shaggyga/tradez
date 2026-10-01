#!/usr/bin/env python3
"""Non-blocking, durable transport for live quote snapshots.

The price stream must never wait for a dashboard JSON reader.  A coalescing
background writer therefore publishes the newest complete snapshot to a small
SQLite/WAL store and mirrors it to the legacy JSON path on a best-effort basis.
Readers prefer SQLite and fall back to JSON for backwards compatibility.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def quote_database_path(snapshot_path: Path) -> Path:
    path = Path(snapshot_path)
    return path.with_name(f"{path.name}.sqlite")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json_mirror(path: Path, payload_json: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    temporary.write_text(payload_json + "\n", encoding="utf-8")
    try:
        for attempt in range(5):
            try:
                temporary.replace(path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(min(0.08, 0.005 * (2**attempt)))
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


class QuoteSnapshotPublisher:
    """Coalesce snapshots and write them away from the market-data thread."""

    def __init__(self, snapshot_path: Path, *, retain: int = 4) -> None:
        self.snapshot_path = Path(snapshot_path)
        self.database_path = quote_database_path(self.snapshot_path)
        self.retain = max(2, int(retain))
        self._condition = threading.Condition()
        self._pending: tuple[int, dict[str, Any]] | None = None
        self._stop = False
        self._submitted_generation = 0
        self._written_generation = 0
        self._coalesced = 0
        self._writing = False
        self._last_write_ms: float | None = None
        self._last_success_epoch: float | None = None
        self._last_error = ""
        self._mirror_error = ""
        self._thread = threading.Thread(
            target=self._run,
            name="oanda-quote-snapshot-publisher",
            daemon=True,
        )
        self._thread.start()

    def submit(self, payload: dict[str, Any]) -> int:
        snapshot = dict(payload)
        with self._condition:
            self._submitted_generation += 1
            generation = self._submitted_generation
            if self._pending is not None:
                self._coalesced += 1
            self._pending = (generation, snapshot)
            self._condition.notify()
            return generation

    def close(self, timeout_sec: float = 5.0) -> None:
        with self._condition:
            self._stop = True
            self._condition.notify_all()
        self._thread.join(timeout=max(0.0, timeout_sec))

    def stats(self) -> dict[str, Any]:
        with self._condition:
            return {
                "enabled": True,
                "database": str(self.database_path),
                "mirror": str(self.snapshot_path),
                "submitted_generation": self._submitted_generation,
                "written_generation": self._written_generation,
                "coalesced_snapshots": self._coalesced,
                "pending": self._pending is not None,
                "writing": self._writing,
                "last_write_ms": self._last_write_ms,
                "last_success_age_sec": (
                    None
                    if self._last_success_epoch is None
                    else round(max(0.0, time.time() - self._last_success_epoch), 3)
                ),
                "last_error": self._last_error,
                "mirror_error": self._mirror_error,
                "thread_alive": self._thread.is_alive(),
            }

    def _connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA wal_autocheckpoint=100")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS quote_snapshots_v2 (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                producer_generation INTEGER NOT NULL,
                published_epoch REAL NOT NULL,
                generated_utc TEXT NOT NULL,
                producer TEXT NOT NULL,
                quote_count INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        connection.commit()
        return connection

    def _run(self) -> None:
        connection: sqlite3.Connection | None = None
        while True:
            with self._condition:
                while self._pending is None and not self._stop:
                    self._condition.wait(timeout=1.0)
                if self._pending is None and self._stop:
                    break
                generation, payload = self._pending
                self._pending = None
                self._writing = True
            started = time.perf_counter()
            try:
                if connection is None:
                    connection = self._connect()
                payload_json = json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
                connection.execute(
                    """
                    INSERT INTO quote_snapshots_v2
                        (producer_generation, published_epoch, generated_utc,
                         producer, quote_count, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        generation,
                        time.time(),
                        str(payload.get("generated_utc") or _utc_now()),
                        str(payload.get("producer") or "unknown"),
                        int(payload.get("quote_count") or len(payload.get("quotes") or {})),
                        payload_json,
                    ),
                )
                connection.execute(
                    """
                    DELETE FROM quote_snapshots_v2
                    WHERE sequence < (
                        SELECT COALESCE(MAX(sequence), 0) - ?
                        FROM quote_snapshots_v2
                    )
                    """,
                    (self.retain - 1,),
                )
                connection.commit()
                mirror_error = ""
                try:
                    _atomic_json_mirror(self.snapshot_path, payload_json)
                except OSError as exc:
                    mirror_error = f"{type(exc).__name__}: {exc}"
                with self._condition:
                    self._written_generation = generation
                    self._last_write_ms = round(
                        (time.perf_counter() - started) * 1000.0, 3
                    )
                    self._last_success_epoch = time.time()
                    self._last_error = ""
                    self._mirror_error = mirror_error
            except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
                if connection is not None:
                    try:
                        connection.close()
                    except sqlite3.Error:
                        pass
                    connection = None
                with self._condition:
                    self._last_error = f"{type(exc).__name__}: {exc}"
            finally:
                with self._condition:
                    self._writing = False
                    self._condition.notify_all()
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass


def load_quote_snapshot(snapshot_path: Path) -> dict[str, Any]:
    """Read the newest complete snapshot without depending on the JSON mirror."""

    path = Path(snapshot_path)
    database = quote_database_path(path)
    if database.exists():
        try:
            uri = f"file:{database.as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=1.0)
            try:
                connection.execute("PRAGMA busy_timeout=1000")
                row = None
                has_v2 = connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='quote_snapshots_v2'"
                ).fetchone()
                if has_v2:
                    row = connection.execute(
                        """
                        SELECT sequence, payload_json
                        FROM quote_snapshots_v2
                        ORDER BY sequence DESC
                        LIMIT 1
                        """
                    ).fetchone()
                if row is None:
                    has_v1 = connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type='table' AND name='quote_snapshots'"
                    ).fetchone()
                    if has_v1:
                        row = connection.execute(
                            """
                            SELECT generation, payload_json
                            FROM quote_snapshots
                            ORDER BY generation DESC
                            LIMIT 1
                            """
                        ).fetchone()
            finally:
                connection.close()
            if row:
                payload = json.loads(str(row[1]))
                if isinstance(payload, dict):
                    payload.setdefault("transport", {})
                    if isinstance(payload["transport"], dict):
                        payload["transport"].update(
                            {"source": "sqlite_wal", "sequence": int(row[0])}
                        )
                    return payload
        except (OSError, sqlite3.Error, json.JSONDecodeError, TypeError, ValueError):
            pass
    for attempt in range(3):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("transport", {})
                if isinstance(payload["transport"], dict):
                    payload["transport"].setdefault("source", "json_fallback")
                return payload
        except (OSError, json.JSONDecodeError):
            if attempt < 2:
                time.sleep(0.01 * (attempt + 1))
    return {}
