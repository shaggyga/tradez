#!/usr/bin/env python3
"""Bounded, research-only signed quote-event intensity ledger.

This records changes in executable OANDA quotes observed by the dedicated
practice price stream.  Quote changes are a microstructure proxy, not trade
signs, dealer inventory, or interdealer order flow.  The module has no broker
client and cannot place orders.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


CAN_PLACE_ORDERS = False
RESEARCH_ONLY = True
HALF_LIVES_SEC = (5, 30, 120)


def _epoch(value: str) -> float | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{threading.get_ident()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


class SignedQuoteIntensityTracker:
    """Accumulate causal quote-change intensity and persist one row/pair/minute."""

    def __init__(
        self,
        database: Path,
        state_path: Path,
        *,
        retention_days: int = 7,
        retention_clock: Callable[[], float] = time.time,
    ) -> None:
        self.database = Path(database)
        self.state_path = Path(state_path)
        self.retention_days = max(1, int(retention_days))
        self._retention_clock = retention_clock
        self._lock = threading.Lock()
        self._states: dict[str, dict[str, Any]] = {}
        self._pending: list[dict[str, Any]] = []
        self._last_flush_ms: float | None = None
        self._last_error = ""
        self._rows_written = 0

    @staticmethod
    def _decay(value: float, elapsed: float, half_life: int) -> float:
        return value * math.exp(-math.log(2.0) * max(0.0, elapsed) / half_life)

    @staticmethod
    def _row(instrument: str, state: dict[str, Any]) -> dict[str, Any]:
        row = {
            "minute_epoch": int(state["minute_epoch"]),
            "instrument": instrument,
            "last_broker_time": str(state.get("last_broker_time") or ""),
            "last_event_epoch": float(state.get("last_event_epoch") or 0.0),
            "updates": int(state.get("updates") or 0),
            "up_moves": int(state.get("up_moves") or 0),
            "down_moves": int(state.get("down_moves") or 0),
            "flat_updates": int(state.get("flat_updates") or 0),
            "last_mid": float(state.get("last_mid") or 0.0),
            "average_spread_pips": (
                float(state.get("spread_sum") or 0.0)
                / max(1, int(state.get("updates") or 0))
            ),
        }
        for half_life in HALF_LIVES_SEC:
            up = float(state.get(f"up_{half_life}") or 0.0)
            down = float(state.get(f"down_{half_life}") or 0.0)
            row[f"up_intensity_{half_life}s"] = up
            row[f"down_intensity_{half_life}s"] = down
            row[f"imbalance_{half_life}s"] = (up - down) / max(up + down, 1e-9)
        return row

    def observe(
        self,
        instrument: str,
        *,
        mid: float,
        spread_pips: float,
        broker_time: str,
        received_monotonic: float | None = None,
    ) -> None:
        event_epoch = _epoch(broker_time)
        if not instrument or event_epoch is None or not math.isfinite(mid) or mid <= 0.0:
            return
        now_mono = time.monotonic() if received_monotonic is None else float(received_monotonic)
        minute_epoch = int(event_epoch // 60) * 60
        with self._lock:
            state = self._states.get(instrument)
            if state is None:
                state = {
                    "minute_epoch": minute_epoch,
                    "last_mid": mid,
                    "last_monotonic": now_mono,
                    **{f"up_{half_life}": 0.0 for half_life in HALF_LIVES_SEC},
                    **{f"down_{half_life}": 0.0 for half_life in HALF_LIVES_SEC},
                }
                self._states[instrument] = state
            elif minute_epoch != int(state["minute_epoch"]):
                self._pending.append(self._row(instrument, state))
                state["minute_epoch"] = minute_epoch
                state["updates"] = 0
                state["up_moves"] = 0
                state["down_moves"] = 0
                state["flat_updates"] = 0
                state["spread_sum"] = 0.0

            elapsed = max(0.0, now_mono - float(state.get("last_monotonic") or now_mono))
            for half_life in HALF_LIVES_SEC:
                state[f"up_{half_life}"] = self._decay(
                    float(state.get(f"up_{half_life}") or 0.0), elapsed, half_life
                )
                state[f"down_{half_life}"] = self._decay(
                    float(state.get(f"down_{half_life}") or 0.0), elapsed, half_life
                )
            previous = float(state.get("last_mid") or mid)
            change_sign = int(mid > previous) - int(mid < previous)
            if change_sign > 0:
                state["up_moves"] = int(state.get("up_moves") or 0) + 1
                for half_life in HALF_LIVES_SEC:
                    state[f"up_{half_life}"] += 1.0
            elif change_sign < 0:
                state["down_moves"] = int(state.get("down_moves") or 0) + 1
                for half_life in HALF_LIVES_SEC:
                    state[f"down_{half_life}"] += 1.0
            else:
                state["flat_updates"] = int(state.get("flat_updates") or 0) + 1
            state["updates"] = int(state.get("updates") or 0) + 1
            state["spread_sum"] = float(state.get("spread_sum") or 0.0) + max(0.0, float(spread_pips))
            state["last_mid"] = mid
            state["last_monotonic"] = now_mono
            state["last_event_epoch"] = event_epoch
            state["last_broker_time"] = broker_time

    def _connect(self) -> sqlite3.Connection:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database, timeout=10.0)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=10000")
        columns = ",\n".join(
            f"up_intensity_{half_life}s REAL NOT NULL, down_intensity_{half_life}s REAL NOT NULL, imbalance_{half_life}s REAL NOT NULL"
            for half_life in HALF_LIVES_SEC
        )
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS quote_intensity_minutes_v1 (
                minute_epoch INTEGER NOT NULL,
                instrument TEXT NOT NULL,
                last_broker_time TEXT NOT NULL,
                last_event_epoch REAL NOT NULL,
                updates INTEGER NOT NULL,
                up_moves INTEGER NOT NULL,
                down_moves INTEGER NOT NULL,
                flat_updates INTEGER NOT NULL,
                last_mid REAL NOT NULL,
                average_spread_pips REAL NOT NULL,
                {columns},
                PRIMARY KEY(minute_epoch, instrument)
            )
            """
        )
        return connection

    def flush(self, *, include_current: bool = True) -> int:
        started = time.perf_counter()
        with self._lock:
            rows = list(self._pending)
            self._pending.clear()
            if include_current:
                rows.extend(self._row(name, state) for name, state in self._states.items())
        if not rows:
            return 0
        keys = list(rows[0])
        placeholders = ",".join("?" for _ in keys)
        updates = ",".join(f"{key}=excluded.{key}" for key in keys[2:])
        error = ""
        try:
            connection = self._connect()
            try:
                connection.executemany(
                    f"INSERT INTO quote_intensity_minutes_v1 ({','.join(keys)}) VALUES ({placeholders}) "
                    f"ON CONFLICT(minute_epoch,instrument) DO UPDATE SET {updates}",
                    [[row[key] for key in keys] for row in rows],
                )
                cutoff = int(self._retention_clock()) - self.retention_days * 86400
                connection.execute(
                    "DELETE FROM quote_intensity_minutes_v1 WHERE minute_epoch < ?", (cutoff,)
                )
                connection.commit()
            finally:
                connection.close()
            self._rows_written += len(rows)
        except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
            error = f"{type(exc).__name__}: {exc}"[:500]
        self._last_error = error
        self._last_flush_ms = round((time.perf_counter() - started) * 1000.0, 3)
        snapshot = self.snapshot()
        _atomic_json(self.state_path, snapshot)
        return 0 if error else len(rows)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            rows = [self._row(name, state) for name, state in self._states.items()]
            pending = len(self._pending)
        ranked = sorted(
            rows,
            key=lambda row: abs(float(row.get("imbalance_30s") or 0.0)),
            reverse=True,
        )
        return {
            "schema_version": 1,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "research_only": True,
            "can_place_orders": False,
            "measurement": "signed executable-quote changes; not trade/order flow",
            "database": str(self.database),
            "instrument_count": len(rows),
            "pending_completed_minutes": pending,
            "rows_written_total": self._rows_written,
            "last_flush_ms": self._last_flush_ms,
            "last_error": self._last_error,
            "top_absolute_30s_imbalances": ranked[:10],
        }

    def close(self) -> None:
        self.flush(include_current=True)
