#!/usr/bin/env python3
"""Mature canonical shadow forecasts from the fast executable quote snapshot.

This worker is deliberately observation-only.  It never imports broker order
code and cannot place, modify, or close a trade.  Its single job is to keep
forecast production latency from contaminating outcome timing.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:  # Support both ``python file.py`` and package-based unit tests.
    from oanda_shadow_outcome_store import ShadowOutcomeStore
except ModuleNotFoundError:  # pragma: no cover - exercised by package imports
    from .oanda_shadow_outcome_store import ShadowOutcomeStore


UTC = timezone.utc
PATH_LEVELS = (1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 20.0, 30.0, 50.0)


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_iso(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).isoformat()


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def theoretical_pips(
    direction: str,
    pip: float,
    entry_bid: float,
    entry_ask: float,
    exit_bid: float,
    exit_ask: float,
) -> float:
    if pip <= 0.0:
        return 0.0
    if direction == "buy":
        return (exit_bid - entry_ask) / pip
    return (entry_bid - exit_ask) / pip


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def load_path_cache(path: Path) -> dict[str, dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_path_cache(path: Path, cache: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=3) as handle:
        json.dump(cache, handle, separators=(",", ":"), sort_keys=True)
    os.replace(temporary, path)


def read_quote_snapshot(path: Path) -> tuple[dict[str, Any] | None, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, "quote_snapshot_missing"
    except (OSError, json.JSONDecodeError):
        return None, "quote_snapshot_unreadable"
    quotes = payload.get("quotes")
    if not isinstance(quotes, dict) or not quotes:
        return None, "quote_snapshot_empty"
    generated = parse_time(payload.get("generated_utc"))
    if generated is None:
        return None, "quote_snapshot_time_invalid"
    payload["_generated_datetime"] = generated
    return payload, ""


class MaturityQueue:
    """Small restart-safe mutable queue; canonical evidence stays immutable."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=5.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA wal_autocheckpoint=4096")
        self.connection.execute("PRAGMA journal_size_limit=67108864")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS worker_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS maturity_queue (
                event_id TEXT PRIMARY KEY,
                source_rowid INTEGER NOT NULL,
                recorded_utc TEXT NOT NULL,
                forecast_json TEXT NOT NULL,
                remaining_json TEXT NOT NULL,
                first_observed_utc TEXT NOT NULL,
                last_sample_utc TEXT NOT NULL,
                path_complete INTEGER NOT NULL,
                path_samples INTEGER NOT NULL,
                positive_path_samples INTEGER NOT NULL,
                max_favorable_pips REAL NOT NULL,
                max_adverse_pips REAL NOT NULL,
                first_positive_sec REAL,
                favorable_hits_json TEXT NOT NULL,
                adverse_hits_json TEXT NOT NULL,
                configured_stop_hit_sec REAL,
                configured_target_hit_sec REAL,
                next_due_epoch REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS maturity_queue_due
                ON maturity_queue(recorded_utc);
            """
        )
        columns = {
            str(row[1]) for row in self.connection.execute("PRAGMA table_info(maturity_queue)")
        }
        if "next_due_epoch" not in columns:
            self.connection.execute(
                "ALTER TABLE maturity_queue ADD COLUMN next_due_epoch REAL NOT NULL DEFAULT 0"
            )
        pending_backfill = self.connection.execute(
            "SELECT event_id,recorded_utc,remaining_json FROM maturity_queue WHERE next_due_epoch<=0"
        ).fetchall()
        backfill: list[tuple[float, str]] = []
        for event_id, recorded_text, remaining_json in pending_backfill:
            recorded = parse_time(recorded_text)
            try:
                remaining = [int(value) for value in json.loads(str(remaining_json))]
            except (TypeError, ValueError, json.JSONDecodeError):
                remaining = []
            next_due = (
                recorded.timestamp() + min(remaining)
                if recorded is not None and remaining
                else 0.0
            )
            backfill.append((float(next_due), str(event_id)))
        if backfill:
            self.connection.executemany(
                "UPDATE maturity_queue SET next_due_epoch=? WHERE event_id=?", backfill
            )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS maturity_queue_next_due ON maturity_queue(next_due_epoch,source_rowid)"
        )
        self.connection.commit()

    def cursor(self) -> int:
        row = self.connection.execute(
            "SELECT value FROM worker_meta WHERE key='forecast_rowid_cursor'"
        ).fetchone()
        return int(row[0]) if row else 0

    def set_cursor(self, value: int) -> None:
        self.connection.execute(
            """
            INSERT INTO worker_meta(key,value) VALUES('forecast_rowid_cursor',?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(int(value)),),
        )

    def insert(
        self,
        *,
        rowid: int,
        event_id: str,
        recorded_utc: str,
        forecast_json: str,
        remaining: list[int],
        path_complete: bool,
        observed_utc: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO maturity_queue (
                event_id,source_rowid,recorded_utc,forecast_json,remaining_json,
                first_observed_utc,last_sample_utc,path_complete,path_samples,
                positive_path_samples,max_favorable_pips,max_adverse_pips,
                first_positive_sec,favorable_hits_json,adverse_hits_json,
                configured_stop_hit_sec,configured_target_hit_sec,next_due_epoch
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event_id,
                int(rowid),
                recorded_utc,
                forecast_json,
                json.dumps(remaining, separators=(",", ":")),
                observed_utc,
                "",
                int(path_complete),
                0,
                0,
                0.0,
                0.0,
                None,
                "{}",
                "{}",
                None,
                None,
                parse_time(recorded_utc).timestamp() + min(remaining),
            ),
        )

    def rows(self) -> list[sqlite3.Row]:
        return list(self.connection.execute("SELECT * FROM maturity_queue"))

    def relevant_rows(
        self,
        now_epoch: float,
        *,
        due_limit: int = 20000,
        path_limit: int = 2048,
    ) -> list[sqlite3.Row]:
        """Return due rows first and a bounded round-robin path sample.

        Empty/future rows must not make exact endpoints wait behind the full
        active path inventory.  The mutable cursor affects sampling only; it
        has no bearing on immutable forecast or outcome evidence.
        """
        due = list(
            self.connection.execute(
                "SELECT * FROM maturity_queue WHERE next_due_epoch<=? "
                "ORDER BY next_due_epoch,source_rowid LIMIT ?",
                (float(now_epoch), max(1, int(due_limit))),
            )
        )
        if path_limit <= 0:
            return due
        cursor_row = self.connection.execute(
            "SELECT value FROM worker_meta WHERE key='path_sample_rowid_cursor'"
        ).fetchone()
        cursor = int(cursor_row[0]) if cursor_row else 0
        future = list(
            self.connection.execute(
                "SELECT * FROM maturity_queue WHERE next_due_epoch>? AND source_rowid>? "
                "ORDER BY source_rowid LIMIT ?",
                (float(now_epoch), cursor, int(path_limit)),
            )
        )
        if len(future) < path_limit:
            future.extend(
                self.connection.execute(
                    "SELECT * FROM maturity_queue WHERE next_due_epoch>? AND source_rowid<=? "
                    "ORDER BY source_rowid LIMIT ?",
                    (float(now_epoch), cursor, int(path_limit) - len(future)),
                )
            )
        if future:
            self.connection.execute(
                "INSERT INTO worker_meta(key,value) VALUES('path_sample_rowid_cursor',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(int(future[-1]["source_rowid"])),),
            )
        seen = {str(row["event_id"]) for row in due}
        return due + [row for row in future if str(row["event_id"]) not in seen]

    def update(
        self,
        event_id: str,
        *,
        remaining: list[int],
        sample_utc: str,
        path_samples: int,
        positive_path_samples: int,
        max_favorable_pips: float,
        max_adverse_pips: float,
        first_positive_sec: float | None,
        favorable_hits: dict[str, float],
        adverse_hits: dict[str, float],
        configured_stop_hit_sec: float | None,
        configured_target_hit_sec: float | None,
        next_due_epoch: float,
    ) -> None:
        if not remaining:
            self.connection.execute(
                "DELETE FROM maturity_queue WHERE event_id=?", (event_id,)
            )
            return
        self.connection.execute(
            """
            UPDATE maturity_queue SET
                remaining_json=?,last_sample_utc=?,path_samples=?,
                positive_path_samples=?,max_favorable_pips=?,max_adverse_pips=?,
                first_positive_sec=?,favorable_hits_json=?,adverse_hits_json=?,
                configured_stop_hit_sec=?,configured_target_hit_sec=?
                ,next_due_epoch=?
            WHERE event_id=?
            """,
            (
                json.dumps(remaining, separators=(",", ":")),
                sample_utc,
                int(path_samples),
                int(positive_path_samples),
                float(max_favorable_pips),
                float(max_adverse_pips),
                first_positive_sec,
                json.dumps(favorable_hits, separators=(",", ":"), sort_keys=True),
                json.dumps(adverse_hits, separators=(",", ":"), sort_keys=True),
                configured_stop_hit_sec,
                configured_target_hit_sec,
                float(next_due_epoch),
                event_id,
            ),
        )

    def commit(self) -> None:
        self.connection.commit()

    def count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM maturity_queue").fetchone()[0])

    def close(self) -> None:
        self.connection.close()


def existing_maturities(
    connection: sqlite3.Connection, event_ids: list[str]
) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    for start in range(0, len(event_ids), 400):
        chunk = event_ids[start : start + 400]
        placeholders = ",".join("?" for _ in chunk)
        for event_id, horizon in connection.execute(
            f"SELECT event_id,horizon_sec FROM canonical_outcomes WHERE event_id IN ({placeholders})",
            chunk,
        ):
            result.setdefault(str(event_id), set()).add(int(horizon))
    return result


def ingest_forecasts(
    source: sqlite3.Connection,
    queue: MaturityQueue,
    evidence: ShadowOutcomeStore,
    *,
    grace_sec: float,
    path_complete_sec: float,
    batch_size: int,
) -> tuple[int, int]:
    cursor = queue.cursor()
    rows = source.execute(
        """
        SELECT rowid,event_id,recorded_utc,horizons_json,forecast_json
        FROM canonical_forecasts
        WHERE rowid>? AND track_outcome=1
        ORDER BY rowid
        LIMIT ?
        """,
        (cursor, max(1, int(batch_size))),
    ).fetchall()
    if not rows:
        return 0, 0
    matured = existing_maturities(source, [str(row[1]) for row in rows])
    now = utc_now()
    inserted = 0
    missed = 0
    for rowid, event_id, recorded_text, horizons_json, forecast_json in rows:
        recorded = parse_time(recorded_text)
        if recorded is None:
            evidence.observe_integrity_event(
                event_id=str(event_id),
                horizon_sec=0,
                event_type="forecast_time_invalid",
                reason="recorded_utc_unparseable",
                details={"recorded_utc": str(recorded_text)},
            )
            queue.set_cursor(int(rowid))
            continue
        try:
            horizons = sorted({int(value) for value in json.loads(horizons_json)})
        except (TypeError, ValueError, json.JSONDecodeError):
            horizons = []
        remaining: list[int] = []
        for horizon in horizons:
            if horizon in matured.get(str(event_id), set()):
                continue
            delay = (now - (recorded + timedelta(seconds=horizon))).total_seconds()
            if delay > grace_sec:
                missed += 1
                evidence.observe_integrity_event(
                    event_id=str(event_id),
                    horizon_sec=horizon,
                    event_type="outcome_window_missed",
                    reason="maturity_worker_observed_after_grace",
                    details={
                        "recorded_utc": str(recorded_text),
                        "delay_sec": round(delay, 3),
                    },
                )
            else:
                remaining.append(horizon)
        if remaining:
            age = max(0.0, (now - recorded).total_seconds())
            queue.insert(
                rowid=int(rowid),
                event_id=str(event_id),
                recorded_utc=str(recorded_text),
                forecast_json=str(forecast_json),
                remaining=remaining,
                path_complete=age <= path_complete_sec,
                observed_utc=utc_iso(now),
            )
            inserted += 1
        queue.set_cursor(int(rowid))
    queue.commit()
    evidence.flush(force=True)
    return inserted, missed


def level_key(value: float) -> str:
    return f"{value:g}"


def process_snapshot(
    snapshot: dict[str, Any] | None,
    snapshot_error: str,
    queue: MaturityQueue,
    evidence: ShadowOutcomeStore,
    *,
    grace_sec: float,
    max_snapshot_age_sec: float,
    path_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, int]:
    cache = path_cache if path_cache is not None else {}
    now = utc_now()
    quotes = snapshot.get("quotes", {}) if snapshot else {}
    generated = snapshot.get("_generated_datetime") if snapshot else None
    snapshot_age = (
        max(0.0, (now - generated).total_seconds())
        if isinstance(generated, datetime)
        else math.inf
    )
    usable_snapshot = bool(snapshot) and snapshot_age <= max_snapshot_age_sec
    counts = {"matured": 0, "missed": 0, "sampled": 0}
    for row in queue.relevant_rows(now.timestamp()):
        event_id = str(row["event_id"])
        recorded = parse_time(row["recorded_utc"])
        if recorded is None:
            continue
        forecast = json.loads(str(row["forecast_json"]))
        cached = cache.get(event_id) or {}
        remaining = [
            int(value)
            for value in cached.get(
                "remaining", json.loads(str(row["remaining_json"]))
            )
        ]
        instrument = str(forecast.get("instrument") or "")
        quote = quotes.get(instrument) if usable_snapshot else None
        bid = finite(quote.get("bid")) if isinstance(quote, dict) else 0.0
        ask = finite(quote.get("ask")) if isinstance(quote, dict) else 0.0
        quote_usable = bid > 0.0 and ask >= bid
        quote_causal = bool(
            quote_usable
            and isinstance(generated, datetime)
            and generated >= recorded
        )
        age = max(0.0, (now - recorded).total_seconds())
        track_path = bool(
            quote_causal and str(forecast.get("kind") or "") == "signal"
        )
        any_due = any(
            now >= recorded + timedelta(seconds=horizon)
            for horizon in remaining
        )
        if not track_path and not any_due:
            continue
        pips = 0.0
        path_samples = int(cached.get("path_samples", row["path_samples"]))
        positive_samples = int(
            cached.get("positive_path_samples", row["positive_path_samples"])
        )
        mfe = finite(cached.get("max_favorable_pips", row["max_favorable_pips"]))
        mae = finite(cached.get("max_adverse_pips", row["max_adverse_pips"]))
        first_positive = cached.get("first_positive_sec", row["first_positive_sec"])
        favorable_hits = dict(
            cached.get("favorable_hits")
            or json.loads(str(row["favorable_hits_json"]))
        )
        adverse_hits = dict(
            cached.get("adverse_hits")
            or json.loads(str(row["adverse_hits_json"]))
        )
        stop_hit = cached.get(
            "configured_stop_hit_sec", row["configured_stop_hit_sec"]
        )
        target_hit = cached.get(
            "configured_target_hit_sec", row["configured_target_hit_sec"]
        )
        if quote_causal:
            pips = theoretical_pips(
                str(forecast.get("direction") or ""),
                finite(forecast.get("pip")),
                finite(forecast.get("entry_bid")),
                finite(forecast.get("entry_ask")),
                bid,
                ask,
            )
            if track_path:
                path_samples += 1
                counts["sampled"] += 1
                if pips > 0.0:
                    positive_samples += 1
                    if first_positive is None:
                        first_positive = round(age, 3)
                mfe = max(mfe, pips)
                mae = max(mae, -pips)
                for level in PATH_LEVELS:
                    key = level_key(level)
                    if pips >= level and key not in favorable_hits:
                        favorable_hits[key] = round(age, 3)
                    if pips <= -level and key not in adverse_hits:
                        adverse_hits[key] = round(age, 3)
                stop_pips = finite(forecast.get("stop_loss_pips"))
                target_pips = stop_pips * finite(forecast.get("take_profit_r"))
                if stop_pips > 0.0 and pips <= -stop_pips and stop_hit is None:
                    stop_hit = round(age, 3)
                if target_pips > 0.0 and pips >= target_pips and target_hit is None:
                    target_hit = round(age, 3)
        next_remaining: list[int] = []
        for horizon in remaining:
            due = recorded + timedelta(seconds=horizon)
            if now < due:
                next_remaining.append(horizon)
                continue
            wall_delay = max(0.0, (now - due).total_seconds())
            snapshot_delay = (
                (generated - due).total_seconds()
                if isinstance(generated, datetime)
                else math.inf
            )
            boundary_quote_usable = bool(quote_causal and snapshot_delay >= 0.0)
            if snapshot_delay > grace_sec or (
                not boundary_quote_usable and wall_delay > grace_sec
            ):
                counts["missed"] += 1
                evidence.observe_integrity_event(
                    event_id=event_id,
                    horizon_sec=horizon,
                    event_type="outcome_window_missed",
                    reason="maturity_delay_exceeded_grace",
                    details={
                        "instrument": instrument,
                        "wall_delay_sec": round(wall_delay, 3),
                        "snapshot_boundary_delay_sec": None
                        if math.isinf(snapshot_delay)
                        else round(snapshot_delay, 3),
                        "quote_available": bool(quote_usable),
                        "snapshot_age_sec": None
                        if math.isinf(snapshot_age)
                        else round(snapshot_age, 3),
                    },
                )
                continue
            if not boundary_quote_usable:
                next_remaining.append(horizon)
                continue
            delay = max(0.0, snapshot_delay)
            payload = dict(forecast)
            payload.update(
                {
                    "id": event_id,
                    "horizon_sec": horizon,
                    "theoretical_pips": round(pips, 4),
                    "outcome_delay_sec": round(delay, 3),
                    # The local snapshot timestamp is the time this quote was
                    # actually available to the system.  Provider timestamps
                    # are retained separately because broker and host clocks
                    # can differ materially.
                    "exit_time": str(snapshot.get("generated_utc") or utc_iso(now)),
                    "exit_bid": bid,
                    "exit_ask": ask,
                    "max_favorable_pips": round(mfe, 4),
                    "max_adverse_pips": round(mae, 4),
                    "first_positive_sec": first_positive,
                    "path_samples": path_samples,
                    "positive_path_samples": positive_samples,
                    "configured_stop_pips": finite(forecast.get("stop_loss_pips")),
                    "configured_target_r": finite(forecast.get("take_profit_r")),
                    "configured_stop_hit_sec": stop_hit,
                    "configured_target_hit_sec": target_hit,
                    "favorable_hits": favorable_hits,
                    "adverse_hits": adverse_hits,
                    "maturity_worker": "canonical_quote_snapshot_v1",
                    "forecast_recorded_utc": str(row["recorded_utc"]),
                    "quote_snapshot_generated_utc": snapshot.get("generated_utc"),
                    "provider_quote_time": quote.get("time"),
                    "quote_snapshot_age_sec": round(snapshot_age, 3),
                    "path_complete": bool(row["path_complete"]),
                    "first_worker_observed_utc": str(row["first_observed_utc"]),
                    "research_only": True,
                }
            )
            evidence.observe(payload)
            counts["matured"] += 1
        state = {
            "remaining": next_remaining,
            "last_sample_utc": utc_iso(now) if track_path else str(row["last_sample_utc"]),
            "path_samples": path_samples,
            "positive_path_samples": positive_samples,
            "max_favorable_pips": mfe,
            "max_adverse_pips": mae,
            "first_positive_sec": first_positive,
            "favorable_hits": favorable_hits,
            "adverse_hits": adverse_hits,
            "configured_stop_hit_sec": stop_hit,
            "configured_target_hit_sec": target_hit,
        }
        # Persist maturity transitions immediately. Between horizons, keep the
        # high-frequency path state in the compact process checkpoint rather
        # than rewriting thousands of SQLite rows every five seconds.
        if next_remaining != remaining:
            queue.update(
                event_id,
                remaining=next_remaining,
                sample_utc=state["last_sample_utc"],
                path_samples=path_samples,
                positive_path_samples=positive_samples,
                max_favorable_pips=mfe,
                max_adverse_pips=mae,
                first_positive_sec=first_positive,
                favorable_hits=favorable_hits,
                adverse_hits=adverse_hits,
                configured_stop_hit_sec=stop_hit,
                configured_target_hit_sec=target_hit,
                next_due_epoch=(
                    recorded.timestamp() + min(next_remaining)
                    if next_remaining
                    else 0.0
                ),
            )
        if track_path and next_remaining:
            cache[event_id] = state
        elif not next_remaining:
            cache.pop(event_id, None)
    queue.commit()
    evidence.flush(force=True)
    return counts


def build_parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parent
    state = root / "data" / "oanda_training_manager" / "state"
    parser = argparse.ArgumentParser()
    parser.add_argument("--forecast-database", type=Path, default=state / "strategy_shadow_outcomes_v1.sqlite")
    parser.add_argument("--queue-database", type=Path, default=state / "canonical_outcome_worker_v1.sqlite")
    parser.add_argument("--quotes", type=Path, default=state / "practice_007_market_quotes_v1.json")
    parser.add_argument("--heartbeat", type=Path, default=state / "canonical_outcome_worker_v1.json")
    parser.add_argument(
        "--path-checkpoint",
        type=Path,
        default=state / "canonical_outcome_path_checkpoint_v1.json.gz",
    )
    parser.add_argument("--poll-sec", type=float, default=0.5)
    parser.add_argument("--ingest-sec", type=float, default=1.0)
    parser.add_argument("--heartbeat-sec", type=float, default=5.0)
    parser.add_argument("--path-checkpoint-sec", type=float, default=60.0)
    parser.add_argument("--max-snapshot-age-sec", type=float, default=15.0)
    parser.add_argument("--maturity-grace-sec", type=float, default=15.0)
    parser.add_argument("--path-complete-sec", type=float, default=10.0)
    parser.add_argument("--ingest-batch-size", type=int, default=10000)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    source = sqlite3.connect(args.forecast_database, timeout=5.0)
    source.execute("PRAGMA query_only=ON")
    source.execute("PRAGMA busy_timeout=5000")
    queue = MaturityQueue(args.queue_database)
    evidence = ShadowOutcomeStore(args.forecast_database, batch_size=512, flush_sec=0.25)
    started = time.monotonic()
    next_ingest = 0.0
    next_heartbeat = 0.0
    last_snapshot_signature = ""
    path_cache = load_path_cache(args.path_checkpoint)
    next_path_checkpoint = time.monotonic() + max(5.0, args.path_checkpoint_sec)
    totals = {"ingested": 0, "matured": 0, "missed": 0, "sampled": 0}
    last_error = ""
    try:
        while time.monotonic() - started < args.duration_sec:
            now_mono = time.monotonic()
            try:
                if now_mono >= next_ingest:
                    ingested, missed = ingest_forecasts(
                        source,
                        queue,
                        evidence,
                        grace_sec=args.maturity_grace_sec,
                        path_complete_sec=args.path_complete_sec,
                        batch_size=args.ingest_batch_size,
                    )
                    totals["ingested"] += ingested
                    totals["missed"] += missed
                    next_ingest = now_mono + max(0.25, args.ingest_sec)
                snapshot, snapshot_error = read_quote_snapshot(args.quotes)
                signature = ""
                if snapshot:
                    signature = str(snapshot.get("generated_utc") or "")
                snapshot_is_stale = bool(
                    snapshot
                    and isinstance(snapshot.get("_generated_datetime"), datetime)
                    and (
                        utc_now() - snapshot["_generated_datetime"]
                    ).total_seconds() > args.max_snapshot_age_sec
                )
                if signature and (
                    signature != last_snapshot_signature or snapshot_is_stale
                ):
                    counts = process_snapshot(
                        snapshot,
                        snapshot_error,
                        queue,
                        evidence,
                        grace_sec=args.maturity_grace_sec,
                        max_snapshot_age_sec=args.max_snapshot_age_sec,
                        path_cache=path_cache,
                    )
                    for key, value in counts.items():
                        totals[key] += value
                    last_snapshot_signature = signature
                elif snapshot_error:
                    # Process the error only when horizons may actually be due.
                    counts = process_snapshot(
                        None,
                        snapshot_error,
                        queue,
                        evidence,
                        grace_sec=args.maturity_grace_sec,
                        max_snapshot_age_sec=args.max_snapshot_age_sec,
                        path_cache=path_cache,
                    )
                    totals["missed"] += counts["missed"]
                last_error = ""
            except (OSError, sqlite3.Error, ValueError, json.JSONDecodeError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if now_mono >= next_path_checkpoint:
                write_path_cache(args.path_checkpoint, path_cache)
                next_path_checkpoint = now_mono + max(5.0, args.path_checkpoint_sec)
            if now_mono >= next_heartbeat:
                atomic_write_json(
                    args.heartbeat,
                    {
                        "schema_version": 1,
                        "generated_utc": utc_iso(),
                        "phase": "collecting",
                        "observation_only": True,
                        "broker_access": False,
                        "forecast_database": str(args.forecast_database.resolve()),
                        "quote_snapshot": str(args.quotes.resolve()),
                        "queue_count": queue.count(),
                        "path_cache_count": len(path_cache),
                        "path_checkpoint": str(args.path_checkpoint.resolve()),
                        "forecast_rowid_cursor": queue.cursor(),
                        "last_quote_snapshot": last_snapshot_signature,
                        "totals": totals,
                        "last_error": last_error,
                    },
                )
                next_heartbeat = now_mono + max(1.0, args.heartbeat_sec)
            time.sleep(max(0.1, args.poll_sec))
    finally:
        write_path_cache(args.path_checkpoint, path_cache)
        evidence.close()
        queue.close()
        source.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
