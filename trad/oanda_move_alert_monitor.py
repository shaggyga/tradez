#!/usr/bin/env python3
"""Causal live move alerts and forecast-miss attribution for the FX project.

The monitor consumes the read-only practice quote and consolidated signal
snapshots. It has no broker client and no execution path. Large moves are
measured with executable bid/ask economics, attributed only to forecast
snapshots available before the move, and written to a compact durable error
surface for operators and future research chats.

Two lagged move-state forecasts can be published to the shared contribution
feed. They are hard-registered as research-only and account-ineligible.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = ROOT / "data" / "oanda_training_manager"
STATE_ROOT = TRAINING_ROOT / "state"
OUTPUT_ROOT = ROOT / "data" / "significant_moves" / "live_alerts"

DEFAULT_QUOTES = STATE_ROOT / "practice_007_market_quotes_v1.json"
DEFAULT_SIGNALS = STATE_ROOT / "practice_007_signal_snapshot_v1.json"
DEFAULT_SIGNAL_FEED = STATE_ROOT / "practice_007_signal_feed_v1.sqlite"
DEFAULT_DATABASE = OUTPUT_ROOT / "move_alerts_v1.sqlite"
DEFAULT_ALERT_JSON = OUTPUT_ROOT / "MOVE_ALERT_LATEST.json"
DEFAULT_ALERT_MD = OUTPUT_ROOT / "MOVE_ALERT_LATEST.md"
DEFAULT_MISSED_JSON = OUTPUT_ROOT / "MISSED_MOVES_LATEST.json"
DEFAULT_MISSED_MD = OUTPUT_ROOT / "MISSED_MOVES_LATEST.md"
DEFAULT_TERMS_JSON = OUTPUT_ROOT / "MOVE_STATE_TERMS_LATEST.json"
DEFAULT_HEARTBEAT = STATE_ROOT / "move_alert_monitor_v1.json"
DEFAULT_LOCK = STATE_ROOT / "move_alert_monitor_v1.lock"
DEFAULT_DEPTH_ROOT = TRAINING_ROOT / "prospective_depth_parquet"

SCHEMA_VERSION = 1
HORIZONS_MINUTES = (1, 5, 15, 30, 60)
HORIZON_PERCENT_FLOORS = {
    1: 0.00018,
    5: 0.00035,
    15: 0.00060,
    30: 0.00090,
    60: 0.00130,
}
TERM_MODEL_IDS = (
    "move_alert.causal_continuation_state",
    "move_alert.causal_reversal_hazard",
)


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def utc_iso(epoch: float | None = None) -> str:
    value = time.time() if epoch is None else float(epoch)
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def parse_epoch(value: Any) -> float:
    if isinstance(value, (int, float)):
        return finite(value)
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def json_compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def pip_size(instrument: str, raw: dict[str, Any] | None = None) -> float:
    supplied = finite((raw or {}).get("pip"))
    if supplied > 0.0:
        return supplied
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def pair_parts(instrument: str) -> tuple[str, str]:
    parts = str(instrument).upper().split("_", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("", "")


def robust_sigma(values: Iterable[float]) -> float:
    rows = [finite(value) for value in values if math.isfinite(finite(value))]
    if len(rows) < 5:
        return 0.0
    center = statistics.median(rows)
    mad = statistics.median(abs(value - center) for value in rows)
    return 1.4826 * mad


class ProcessLock:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.handle: Any = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        try:
            import msvcrt

            self.handle.seek(0)
            self.handle.write(b"1")
            self.handle.flush()
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
        except (ImportError, OSError):
            self.handle.close()
            self.handle = None
            return False
        return True

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            import msvcrt

            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        except (ImportError, OSError):
            pass
        self.handle.close()
        self.handle = None


def quote_rows(
    payload: dict[str, Any],
    *,
    now: float,
    max_quote_age_sec: float,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    generated = parse_epoch(payload.get("generated_utc")) or now
    for instrument, raw in (payload.get("quotes") or {}).items():
        if not isinstance(raw, dict):
            continue
        bid = finite(raw.get("bid"))
        ask = finite(raw.get("ask"))
        observed = parse_epoch(raw.get("time")) or generated
        if bid <= 0.0 or ask < bid:
            continue
        if now - observed > max(1.0, max_quote_age_sec) or observed > now + 30.0:
            continue
        output.append(
            {
                "instrument": str(instrument).upper(),
                "observed_epoch": observed,
                "bid": bid,
                "ask": ask,
                "pip": pip_size(str(instrument).upper(), raw),
            }
        )
    return output


def _forecast_direction(point: dict[str, Any]) -> str:
    signed = finite(point.get("raw_ensemble_signed_net_pips"))
    probability_up = finite(
        point.get("raw_probability_up"),
        finite(point.get("probability_up"), 0.5),
    )
    if abs(signed) >= 0.05:
        return "buy" if signed > 0.0 else "sell"
    if probability_up >= 0.505:
        return "buy"
    if probability_up <= 0.495:
        return "sell"
    return "neutral"


def summarize_signal_snapshot(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    snapshot_epoch = parse_epoch(snapshot.get("updated_at"))
    if snapshot_epoch <= 0.0:
        return []
    by_instrument: dict[str, dict[str, Any]] = {}
    for signal in snapshot.get("top_signals") or []:
        if not isinstance(signal, dict):
            continue
        instrument = str(signal.get("instrument") or "").upper()
        if not instrument:
            continue
        horizon_rows: dict[str, dict[str, Any]] = {}
        for point in signal.get("horizon_breakdown") or []:
            if not isinstance(point, dict):
                continue
            horizon = int(finite(point.get("horizon_sec")))
            if horizon <= 0 or horizon > 3600:
                continue
            probability_up = finite(
                point.get("raw_probability_up"),
                finite(point.get("probability_up"), 0.5),
            )
            raw_signed = finite(point.get("raw_ensemble_signed_net_pips"))
            direction = _forecast_direction(point)
            horizon_rows[str(horizon)] = {
                "horizon_sec": horizon,
                "raw_direction": direction,
                "raw_probability_up": probability_up,
                "raw_confidence": max(probability_up, 1.0 - probability_up),
                "raw_signed_net_pips": raw_signed,
                "gated_direction": (
                    str(point.get("direction") or "").lower()
                    if bool(point.get("signal_eligible"))
                    else "neutral"
                ),
                "signal_eligible": bool(point.get("signal_eligible")),
                "paper_consensus_eligible": bool(
                    point.get("paper_consensus_eligible")
                ),
                "component_count": int(finite(point.get("component_count"))),
                "eligible_component_count": int(
                    finite(point.get("eligible_component_count"))
                ),
                "blockers": sorted(
                    {str(value) for value in point.get("signal_blocked_by") or []}
                ),
                "best_model_id": str(point.get("best_model_id") or ""),
                "best_family": str(point.get("best_family") or ""),
                "best_input_timeframe": str(
                    point.get("best_input_timeframe") or ""
                ),
            }
        if not horizon_rows:
            continue
        candidate = {
            "instrument": instrument,
            "snapshot_epoch": snapshot_epoch,
            "snapshot_utc": utc_iso(snapshot_epoch),
            "source_updated_at": str(snapshot.get("updated_at") or ""),
            "consolidated_direction_state": str(
                signal.get("direction_state") or "neutral"
            ).lower(),
            "direction_conflict": bool(signal.get("direction_conflict")),
            "component_count": int(finite(signal.get("component_count"))),
            "component_models": [
                str(value) for value in signal.get("component_models") or []
            ],
            "component_families": [
                str(value) for value in signal.get("component_families") or []
            ],
            "blockers": sorted(
                {str(value) for value in signal.get("signal_blocked_by") or []}
            ),
            "horizons": horizon_rows,
        }
        existing = by_instrument.get(instrument)
        if existing is None or candidate["component_count"] > existing["component_count"]:
            by_instrument[instrument] = candidate
    return list(by_instrument.values())


def select_forecast_horizon(
    payload: dict[str, Any], horizon_minutes: int
) -> dict[str, Any] | None:
    points = payload.get("horizons") or {}
    if not isinstance(points, dict) or not points:
        return None
    target = horizon_minutes * 60
    valid: list[tuple[int, dict[str, Any]]] = []
    for raw_horizon, point in points.items():
        if not isinstance(point, dict):
            continue
        horizon = int(finite(raw_horizon, finite(point.get("horizon_sec"))))
        if horizon > 0:
            valid.append((horizon, point))
    if not valid:
        return None
    return min(valid, key=lambda row: (abs(row[0] - target), row[0]))[1]


def classify_forecast(
    payload: dict[str, Any] | None,
    *,
    move_direction: str,
    horizon_minutes: int,
    move_net_pips: float,
    threshold_pips: float,
    late_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if payload is None:
        result = {
            "classification": "unattributed",
            "error_code": "NO_ARCHIVED_PREMOVE_FORECAST",
            "missed": True,
            "caught": False,
            "detail": "No forecast snapshot existed at or before move start.",
        }
    else:
        point = select_forecast_horizon(payload, horizon_minutes)
        if point is None:
            result = {
                "classification": "coverage_miss",
                "error_code": "FORECAST_HORIZON_COVERAGE_MISS",
                "missed": True,
                "caught": False,
                "detail": "A pre-move snapshot existed but lacked an intrahour horizon.",
            }
        else:
            raw_direction = str(point.get("raw_direction") or "neutral")
            raw_confidence = finite(point.get("raw_confidence"), 0.5)
            raw_signed = finite(point.get("raw_signed_net_pips"))
            gated_direction = str(point.get("gated_direction") or "neutral")
            magnitude_credible = abs(raw_signed) >= max(
                0.25 * threshold_pips,
                0.10 * move_net_pips,
            )
            direction_aligned = raw_direction == move_direction
            strong = raw_confidence >= 0.52 or magnitude_credible
            gated_aligned = (
                gated_direction == move_direction
                and bool(point.get("signal_eligible"))
            )
            if direction_aligned and strong and gated_aligned:
                code = "CAUGHT_ELIGIBLE_PREMOVE"
                classification = "caught_eligible"
                missed = False
            elif direction_aligned and strong:
                code = "GATED_ALIGNED_PREMOVE_SIGNAL"
                classification = "gated_aligned"
                missed = True
            elif direction_aligned:
                code = "WEAK_ALIGNED_PREMOVE_SIGNAL"
                classification = "weak_aligned"
                missed = True
            elif raw_direction in {"buy", "sell"}:
                code = "WRONG_DIRECTION_PREMOVE_SIGNAL"
                classification = "wrong_direction"
                missed = True
            else:
                code = "NEUTRAL_PREMOVE_FORECAST"
                classification = "neutral"
                missed = True
            result = {
                "classification": classification,
                "error_code": code,
                "missed": missed,
                "caught": not missed,
                "detail": (
                    f"raw={raw_direction} confidence={raw_confidence:.4f} "
                    f"signed={raw_signed:.3f} gated={gated_direction}"
                ),
                "evaluated_horizon_sec": int(
                    finite(point.get("horizon_sec"))
                ),
                "raw_direction": raw_direction,
                "raw_confidence": raw_confidence,
                "raw_signed_net_pips": raw_signed,
                "gated_direction": gated_direction,
                "signal_eligible": bool(point.get("signal_eligible")),
                "blockers": list(point.get("blockers") or payload.get("blockers") or []),
                "best_model_id": str(point.get("best_model_id") or ""),
                "best_family": str(point.get("best_family") or ""),
            }
    if result["missed"] and late_payload is not None:
        late_point = select_forecast_horizon(late_payload, horizon_minutes)
        if (
            late_point is not None
            and str(late_point.get("raw_direction") or "") == move_direction
            and (
                finite(late_point.get("raw_confidence"), 0.5) >= 0.52
                or abs(finite(late_point.get("raw_signed_net_pips")))
                >= 0.10 * move_net_pips
            )
        ):
            result["classification"] = "late_aligned"
            result["error_code"] = "LATE_ALIGNED_SIGNAL"
            result["detail"] += " A matching forecast appeared only after move start."
    return result


class MoveAlertLedger:
    def __init__(self, path: Path, *, retention_days: int = 7) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.retention_days = max(1, int(retention_days))
        self.connection = sqlite3.connect(self.path, timeout=5.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA wal_autocheckpoint=1000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS quote_bars (
                instrument TEXT NOT NULL,
                minute_epoch INTEGER NOT NULL,
                first_epoch REAL NOT NULL,
                last_epoch REAL NOT NULL,
                open_bid REAL NOT NULL,
                open_ask REAL NOT NULL,
                close_bid REAL NOT NULL,
                close_ask REAL NOT NULL,
                high_mid REAL NOT NULL,
                low_mid REAL NOT NULL,
                pip REAL NOT NULL,
                PRIMARY KEY (instrument, minute_epoch)
            );
            CREATE INDEX IF NOT EXISTS idx_move_quote_time
                ON quote_bars(minute_epoch, instrument);

            CREATE TABLE IF NOT EXISTS forecast_snapshots (
                instrument TEXT NOT NULL,
                snapshot_epoch REAL NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (instrument, snapshot_epoch)
            );
            CREATE INDEX IF NOT EXISTS idx_move_forecast_lookup
                ON forecast_snapshots(instrument, snapshot_epoch);

            CREATE TABLE IF NOT EXISTS move_events (
                event_id TEXT PRIMARY KEY,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                start_epoch REAL NOT NULL,
                detected_epoch REAL NOT NULL,
                last_seen_epoch REAL NOT NULL,
                end_epoch REAL NOT NULL,
                horizon_minutes INTEGER NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                exit_bid REAL NOT NULL,
                exit_ask REAL NOT NULL,
                pip REAL NOT NULL,
                gross_mid_pips REAL NOT NULL,
                executable_net_pips REAL NOT NULL,
                threshold_pips REAL NOT NULL,
                percentage_move REAL NOT NULL,
                spread_cost_pips REAL NOT NULL,
                severity REAL NOT NULL,
                volatility_z REAL NOT NULL,
                status TEXT NOT NULL,
                classification TEXT NOT NULL,
                error_code TEXT NOT NULL,
                missed INTEGER NOT NULL,
                forecast_snapshot_epoch REAL,
                forecast_lag_sec REAL,
                forecast_json TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                cluster_currency TEXT NOT NULL,
                cluster_direction TEXT NOT NULL,
                causal_terms_json TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_move_event_time
                ON move_events(start_epoch, detected_epoch);
            CREATE INDEX IF NOT EXISTS idx_move_event_pair
                ON move_events(instrument, direction, status, last_seen_epoch);
            CREATE INDEX IF NOT EXISTS idx_move_event_error
                ON move_events(missed, error_code, detected_epoch);
            """
        )
        self.connection.commit()

    def ingest_quotes(self, rows: list[dict[str, Any]]) -> int:
        values = []
        for row in rows:
            observed = finite(row.get("observed_epoch"))
            bid = finite(row.get("bid"))
            ask = finite(row.get("ask"))
            pip = finite(row.get("pip"))
            if observed <= 0.0 or bid <= 0.0 or ask < bid or pip <= 0.0:
                continue
            minute = int(observed // 60) * 60
            mid = 0.5 * (bid + ask)
            values.append(
                (
                    str(row["instrument"]),
                    minute,
                    observed,
                    observed,
                    bid,
                    ask,
                    bid,
                    ask,
                    mid,
                    mid,
                    pip,
                )
            )
        if values:
            self.connection.executemany(
                """
                INSERT INTO quote_bars(
                    instrument, minute_epoch, first_epoch, last_epoch,
                    open_bid, open_ask, close_bid, close_ask, high_mid,
                    low_mid, pip
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument, minute_epoch) DO UPDATE SET
                    last_epoch=excluded.last_epoch,
                    close_bid=excluded.close_bid,
                    close_ask=excluded.close_ask,
                    high_mid=MAX(quote_bars.high_mid, excluded.high_mid),
                    low_mid=MIN(quote_bars.low_mid, excluded.low_mid),
                    pip=excluded.pip
                """,
                values,
            )
            self.connection.commit()
        return len(values)

    def quote_history_span_sec(self) -> float:
        row = self.connection.execute(
            "SELECT MIN(minute_epoch), MAX(minute_epoch) FROM quote_bars"
        ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            return 0.0
        return max(0.0, finite(row[1]) - finite(row[0]))

    def archive_forecasts(self, snapshot: dict[str, Any]) -> int:
        rows = summarize_signal_snapshot(snapshot)
        if not rows:
            return 0
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO forecast_snapshots(
                instrument, snapshot_epoch, payload_json
            ) VALUES (?, ?, ?)
            """,
            [
                (
                    row["instrument"],
                    row["snapshot_epoch"],
                    json_compact(row),
                )
                for row in rows
            ],
        )
        self.connection.commit()
        return len(rows)

    def _pair_bars(self, instrument: str, now: float) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT * FROM quote_bars
            WHERE instrument = ? AND minute_epoch >= ?
            ORDER BY minute_epoch
            """,
            (instrument, int(now - 6 * 3600)),
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _profile(
        instrument: str,
        bars: list[dict[str, Any]],
        horizon_minutes: int,
    ) -> dict[str, Any] | None:
        if len(bars) < 2:
            return None
        current = bars[-1]
        target = int(current["minute_epoch"]) - horizon_minutes * 60
        eligible = [
            row for row in bars if int(row["minute_epoch"]) <= target
        ]
        if not eligible:
            return None
        entry = eligible[-1]
        if target - int(entry["minute_epoch"]) > 120:
            return None
        pip = max(1e-12, finite(current.get("pip"), pip_size(instrument)))
        entry_mid = 0.5 * (
            finite(entry.get("open_bid")) + finite(entry.get("open_ask"))
        )
        exit_mid = 0.5 * (
            finite(current.get("close_bid")) + finite(current.get("close_ask"))
        )
        signed_mid = (exit_mid - entry_mid) / pip
        direction = "buy" if signed_mid >= 0.0 else "sell"
        executable = (
            (finite(current.get("close_bid")) - finite(entry.get("open_ask"))) / pip
            if direction == "buy"
            else (finite(entry.get("open_bid")) - finite(current.get("close_ask"))) / pip
        )
        entry_spread = (
            finite(entry.get("open_ask")) - finite(entry.get("open_bid"))
        ) / pip
        exit_spread = (
            finite(current.get("close_ask")) - finite(current.get("close_bid"))
        ) / pip
        spread_cost = 0.5 * (entry_spread + exit_spread)
        mids = [
            0.5 * (finite(row["close_bid"]) + finite(row["close_ask"]))
            for row in bars
        ]
        minute_changes = [
            (mids[index] - mids[index - 1]) / pip
            for index in range(1, len(mids))
            if int(bars[index]["minute_epoch"]) - int(bars[index - 1]["minute_epoch"])
            <= 120
        ]
        sigma_1m = robust_sigma(minute_changes[-240:])
        volatility_threshold = 3.0 * sigma_1m * math.sqrt(horizon_minutes)
        percentage_threshold = (
            entry_mid * HORIZON_PERCENT_FLOORS[horizon_minutes] / pip
        )
        threshold = max(
            1.0,
            3.0 * spread_cost,
            percentage_threshold,
            volatility_threshold,
        )
        volatility_z = (
            abs(signed_mid) / max(0.1, sigma_1m * math.sqrt(horizon_minutes))
            if sigma_1m > 0.0
            else 0.0
        )
        acceleration = 0.0
        halfway_target = int(current["minute_epoch"]) - (horizon_minutes // 2) * 60
        halfway_rows = (
            [
                row for row in bars if int(row["minute_epoch"]) <= halfway_target
            ]
            if horizon_minutes > 1
            else []
        )
        if halfway_rows:
            halfway = halfway_rows[-1]
            half_mid = 0.5 * (
                finite(halfway["close_bid"]) + finite(halfway["close_ask"])
            )
            first = (half_mid - entry_mid) / pip
            second = (exit_mid - half_mid) / pip
            sign = 1.0 if direction == "buy" else -1.0
            acceleration = (sign * second - sign * first) / max(
                1.0, abs(signed_mid)
            )
        return {
            "instrument": instrument,
            "direction": direction,
            "start_epoch": finite(entry["first_epoch"]),
            "end_epoch": finite(current["last_epoch"]),
            "horizon_minutes": horizon_minutes,
            "entry_bid": finite(entry["open_bid"]),
            "entry_ask": finite(entry["open_ask"]),
            "exit_bid": finite(current["close_bid"]),
            "exit_ask": finite(current["close_ask"]),
            "pip": pip,
            "gross_mid_pips": signed_mid,
            "executable_net_pips": executable,
            "threshold_pips": threshold,
            "percentage_move": (exit_mid / entry_mid - 1.0) * 100.0,
            "spread_cost_pips": spread_cost,
            "severity": executable / threshold,
            "volatility_z": volatility_z,
            "acceleration": acceleration,
            "velocity_pips_per_minute": executable / horizon_minutes,
        }

    def current_profiles(
        self, now: float, horizons: Iterable[int] = HORIZONS_MINUTES
    ) -> list[dict[str, Any]]:
        instruments = [
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT DISTINCT instrument FROM quote_bars
                WHERE minute_epoch >= ?
                """,
                (int(now - 180),),
            )
        ]
        output: list[dict[str, Any]] = []
        for instrument in instruments:
            bars = self._pair_bars(instrument, now)
            for horizon in horizons:
                profile = self._profile(instrument, bars, int(horizon))
                if profile is not None:
                    output.append(profile)
        return output

    def _forecast_at_or_before(
        self, instrument: str, epoch: float, *, max_age_sec: float = 900.0
    ) -> tuple[float | None, dict[str, Any] | None]:
        row = self.connection.execute(
            """
            SELECT snapshot_epoch, payload_json FROM forecast_snapshots
            WHERE instrument = ? AND snapshot_epoch <= ? AND snapshot_epoch >= ?
            ORDER BY snapshot_epoch DESC LIMIT 1
            """,
            (instrument, epoch, epoch - max_age_sec),
        ).fetchone()
        if row is None:
            return None, None
        return finite(row["snapshot_epoch"]), json.loads(row["payload_json"])

    def _forecast_between(
        self, instrument: str, start: float, end: float
    ) -> dict[str, Any] | None:
        rows = self.connection.execute(
            """
            SELECT payload_json FROM forecast_snapshots
            WHERE instrument = ? AND snapshot_epoch > ? AND snapshot_epoch <= ?
            ORDER BY snapshot_epoch
            """,
            (instrument, start, end),
        ).fetchall()
        for row in rows:
            payload = json.loads(row["payload_json"])
            if payload:
                return payload
        return None

    def record_moves(
        self, profiles: list[dict[str, Any]], now: float
    ) -> list[str]:
        strongest: dict[str, dict[str, Any]] = {}
        for profile in profiles:
            if finite(profile.get("executable_net_pips")) <= 0.0:
                continue
            if finite(profile.get("severity")) < 1.0:
                continue
            instrument = str(profile["instrument"])
            prior = strongest.get(instrument)
            if prior is None or finite(profile["severity"]) > finite(prior["severity"]):
                strongest[instrument] = profile
        touched: list[str] = []
        for profile in strongest.values():
            existing = self.connection.execute(
                """
                SELECT * FROM move_events
                WHERE instrument = ? AND direction = ? AND status = 'active'
                  AND last_seen_epoch >= ? AND end_epoch >= ?
                ORDER BY severity DESC LIMIT 1
                """,
                (
                    profile["instrument"],
                    profile["direction"],
                    now - 900.0,
                    profile["start_epoch"] - 300.0,
                ),
            ).fetchone()
            if existing is None:
                identity = (
                    f"{profile['instrument']}|{profile['direction']}|"
                    f"{int(profile['start_epoch'] // 60)}"
                )
                event_id = "move-" + hashlib.sha256(
                    identity.encode("utf-8")
                ).hexdigest()[:24]
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO move_events(
                        event_id, instrument, direction, start_epoch,
                        detected_epoch, last_seen_epoch, end_epoch,
                        horizon_minutes, entry_bid, entry_ask, exit_bid,
                        exit_ask, pip, gross_mid_pips, executable_net_pips,
                        threshold_pips, percentage_move, spread_cost_pips,
                        severity, volatility_z, status, classification,
                        error_code, missed, forecast_snapshot_epoch,
                        forecast_lag_sec, forecast_json, cluster_id,
                        cluster_currency, cluster_direction,
                        causal_terms_json, created_utc, updated_utc
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, 'active', 'pending', 'PENDING_ATTRIBUTION', 1,
                        NULL, NULL, '{}', '', '', '', ?, ?, ?
                    )
                    """,
                    (
                        event_id,
                        profile["instrument"],
                        profile["direction"],
                        profile["start_epoch"],
                        now,
                        now,
                        profile["end_epoch"],
                        profile["horizon_minutes"],
                        profile["entry_bid"],
                        profile["entry_ask"],
                        profile["exit_bid"],
                        profile["exit_ask"],
                        profile["pip"],
                        profile["gross_mid_pips"],
                        profile["executable_net_pips"],
                        profile["threshold_pips"],
                        profile["percentage_move"],
                        profile["spread_cost_pips"],
                        profile["severity"],
                        profile["volatility_z"],
                        json_compact(profile),
                        utc_iso(now),
                        utc_iso(now),
                    ),
                )
            else:
                event_id = str(existing["event_id"])
                if finite(profile["severity"]) >= finite(existing["severity"]):
                    self.connection.execute(
                        """
                        UPDATE move_events SET
                            start_epoch=MIN(start_epoch, ?),
                            last_seen_epoch=?, end_epoch=?, horizon_minutes=?,
                            entry_bid=?, entry_ask=?, exit_bid=?, exit_ask=?,
                            pip=?, gross_mid_pips=?, executable_net_pips=?,
                            threshold_pips=?, percentage_move=?,
                            spread_cost_pips=?, severity=?, volatility_z=?,
                            causal_terms_json=?, updated_utc=?
                        WHERE event_id=?
                        """,
                        (
                            profile["start_epoch"],
                            now,
                            profile["end_epoch"],
                            profile["horizon_minutes"],
                            profile["entry_bid"],
                            profile["entry_ask"],
                            profile["exit_bid"],
                            profile["exit_ask"],
                            profile["pip"],
                            profile["gross_mid_pips"],
                            profile["executable_net_pips"],
                            profile["threshold_pips"],
                            profile["percentage_move"],
                            profile["spread_cost_pips"],
                            profile["severity"],
                            profile["volatility_z"],
                            json_compact(profile),
                            utc_iso(now),
                            event_id,
                        ),
                    )
                else:
                    self.connection.execute(
                        """
                        UPDATE move_events
                        SET last_seen_epoch=?, end_epoch=MAX(end_epoch, ?),
                            updated_utc=?
                        WHERE event_id=?
                        """,
                        (now, profile["end_epoch"], utc_iso(now), event_id),
                    )
            touched.append(event_id)
        self.connection.execute(
            """
            UPDATE move_events SET status='resolved', updated_utc=?
            WHERE status='active' AND last_seen_epoch < ?
            """,
            (utc_iso(now), now - 900.0),
        )
        self.connection.commit()
        for event_id in touched:
            self.attribute_event(event_id)
        self.assign_clusters(now)
        return touched

    def attribute_event(self, event_id: str) -> None:
        event = self.connection.execute(
            "SELECT * FROM move_events WHERE event_id=?", (event_id,)
        ).fetchone()
        if event is None:
            return
        snapshot_epoch, payload = self._forecast_at_or_before(
            str(event["instrument"]), finite(event["start_epoch"])
        )
        late = self._forecast_between(
            str(event["instrument"]),
            finite(event["start_epoch"]),
            finite(event["detected_epoch"]),
        )
        result = classify_forecast(
            payload,
            move_direction=str(event["direction"]),
            horizon_minutes=int(event["horizon_minutes"]),
            move_net_pips=finite(event["executable_net_pips"]),
            threshold_pips=finite(event["threshold_pips"]),
            late_payload=late,
        )
        lag = (
            finite(event["start_epoch"]) - finite(snapshot_epoch)
            if snapshot_epoch is not None
            else None
        )
        result["premove_snapshot"] = payload
        result["attribution_is_causal"] = True
        result["forecast_must_precede_move_start"] = True
        self.connection.execute(
            """
            UPDATE move_events SET classification=?, error_code=?, missed=?,
                forecast_snapshot_epoch=?, forecast_lag_sec=?,
                forecast_json=?, updated_utc=?
            WHERE event_id=?
            """,
            (
                result["classification"],
                result["error_code"],
                int(bool(result["missed"])),
                snapshot_epoch,
                lag,
                json_compact(result),
                utc_iso(),
                event_id,
            ),
        )
        self.connection.commit()

    def assign_clusters(self, now: float) -> None:
        events = self.connection.execute(
            """
            SELECT event_id, instrument, direction, start_epoch, severity
            FROM move_events WHERE start_epoch >= ?
            """,
            (now - 24 * 3600,),
        ).fetchall()
        buckets: dict[int, list[sqlite3.Row]] = {}
        for event in events:
            bucket = int(finite(event["start_epoch"]) // 900) * 900
            buckets.setdefault(bucket, []).append(event)
        for bucket, rows in buckets.items():
            votes: dict[str, float] = {}
            counts: dict[str, int] = {}
            for row in rows:
                base, quote = pair_parts(str(row["instrument"]))
                sign = 1.0 if str(row["direction"]) == "buy" else -1.0
                weight = max(1.0, finite(row["severity"]))
                for currency, value in ((base, sign), (quote, -sign)):
                    if not currency:
                        continue
                    votes[currency] = votes.get(currency, 0.0) + value * weight
                    counts[currency] = counts.get(currency, 0) + 1
            eligible = [
                (currency, score)
                for currency, score in votes.items()
                if counts.get(currency, 0) >= 2
            ]
            if not eligible:
                continue
            currency, score = max(eligible, key=lambda item: abs(item[1]))
            cluster_direction = "strengthening" if score > 0.0 else "weakening"
            cluster_id = (
                "currency-shock-"
                + hashlib.sha256(
                    f"{currency}|{cluster_direction}|{bucket}".encode("utf-8")
                ).hexdigest()[:20]
            )
            for row in rows:
                if currency not in pair_parts(str(row["instrument"])):
                    continue
                self.connection.execute(
                    """
                    UPDATE move_events SET cluster_id=?, cluster_currency=?,
                        cluster_direction=?, updated_utc=?
                    WHERE event_id=?
                    """,
                    (
                        cluster_id,
                        currency,
                        cluster_direction,
                        utc_iso(),
                        row["event_id"],
                    ),
                )
        self.connection.commit()

    def prune(self, now: float) -> dict[str, int]:
        cutoff = now - self.retention_days * 86400
        quote_count = self.connection.execute(
            "DELETE FROM quote_bars WHERE minute_epoch < ?", (int(cutoff),)
        ).rowcount
        forecast_count = self.connection.execute(
            "DELETE FROM forecast_snapshots WHERE snapshot_epoch < ?", (cutoff,)
        ).rowcount
        self.connection.commit()
        return {"quote_bars": quote_count, "forecast_snapshots": forecast_count}

    @staticmethod
    def _event_dict(row: sqlite3.Row) -> dict[str, Any]:
        output = dict(row)
        for key in ("forecast_json", "causal_terms_json"):
            try:
                output[key.removesuffix("_json")] = json.loads(output.pop(key))
            except (json.JSONDecodeError, TypeError):
                output[key.removesuffix("_json")] = {}
        output["start_utc"] = utc_iso(finite(output["start_epoch"]))
        output["detected_utc"] = utc_iso(finite(output["detected_epoch"]))
        output["end_utc"] = utc_iso(finite(output["end_epoch"]))
        output["missed"] = bool(output["missed"])
        return output

    def recent_events(self, now: float, *, hours: int = 24) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT * FROM move_events WHERE detected_epoch >= ?
            ORDER BY severity DESC, detected_epoch DESC
            """,
            (now - hours * 3600,),
        ).fetchall()
        return [self._event_dict(row) for row in rows]

    def close(self) -> None:
        self.connection.close()


def bootstrap_depth_history(
    ledger: MoveAlertLedger,
    depth_root: Path,
    *,
    now: float,
    hours: float,
    max_files: int,
    time_budget_sec: float,
) -> dict[str, Any]:
    requested_hours = max(0.0, float(hours))
    if requested_hours <= 0.0:
        return {"status": "disabled", "rows": 0, "files": 0}
    required_span = min(requested_hours * 3600.0 * 0.80, 4 * 3600.0)
    existing_span = ledger.quote_history_span_sec()
    if existing_span >= required_span:
        return {
            "status": "existing_history_sufficient",
            "rows": 0,
            "files": 0,
            "existing_span_sec": existing_span,
        }
    try:
        import pyarrow.parquet as parquet
    except ImportError as exc:
        return {
            "status": "skipped_dependency",
            "reason": str(exc),
            "rows": 0,
            "files": 0,
        }
    cutoff = now - requested_hours * 3600.0
    partition_hours: set[tuple[str, str]] = set()
    cursor = int(cutoff // 3600) * 3600
    while cursor <= now:
        instant = datetime.fromtimestamp(cursor, timezone.utc)
        partition_hours.add(
            (instant.strftime("%Y%m%d"), instant.strftime("%H"))
        )
        cursor += 3600
    discovered_files: list[Path] = []
    for date_slug, hour_slug in sorted(partition_hours):
        directory = depth_root / f"date={date_slug}" / f"hour={hour_slug}"
        discovered_files.extend(directory.glob("*.parquet"))
    files = sorted(
        discovered_files,
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )[: max(1, int(max_files))]
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    attempted = 0
    bootstrap_started = time.monotonic()
    for path in files:
        if (
            attempted > 0
            and time.monotonic() - bootstrap_started
            >= max(1.0, float(time_budget_sec))
        ):
            break
        attempted += 1
        try:
            table = parquet.read_table(
                path,
                columns=["collected_utc", "instrument", "bid", "ask"],
            )
            for raw in table.to_pylist():
                observed = parse_epoch(raw.get("collected_utc"))
                instrument = str(raw.get("instrument") or "").upper()
                bid = finite(raw.get("bid"))
                ask = finite(raw.get("ask"))
                if (
                    observed < cutoff
                    or observed > now + 30.0
                    or not instrument
                    or bid <= 0.0
                    or ask < bid
                ):
                    continue
                rows.append(
                    {
                        "instrument": instrument,
                        "observed_epoch": observed,
                        "bid": bid,
                        "ask": ask,
                        "pip": pip_size(instrument),
                    }
                )
        except Exception as exc:
            errors.append(f"{path.name}:{type(exc).__name__}:{exc}")
    rows.sort(key=lambda row: (finite(row["observed_epoch"]), row["instrument"]))
    inserted = 0
    for offset in range(0, len(rows), 5000):
        inserted += ledger.ingest_quotes(rows[offset : offset + 5000])
    return {
        "status": "loaded" if rows else "no_rows",
        "files": attempted,
        "files_discovered": len(discovered_files),
        "file_budget": max(1, int(max_files)),
        "time_budget_sec": max(1.0, float(time_budget_sec)),
        "elapsed_sec": round(time.monotonic() - bootstrap_started, 3),
        "rows": len(rows),
        "ingested_rows": inserted,
        "errors": errors[:20],
        "requested_hours": requested_hours,
        "resulting_span_sec": ledger.quote_history_span_sec(),
    }


def build_causal_terms(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_instrument: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        instrument = str(profile["instrument"])
        prior = by_instrument.get(instrument)
        if prior is None or finite(profile["severity"]) > finite(prior["severity"]):
            by_instrument[instrument] = profile
    output: list[dict[str, Any]] = []
    for profile in by_instrument.values():
        ratio = finite(profile.get("severity"))
        if ratio < 0.50:
            continue
        acceleration = finite(profile.get("acceleration"))
        continuation = clamp(
            0.5 + 0.035 * ratio + 0.025 * max(0.0, acceleration),
            0.501,
            0.65,
        )
        reversal = clamp(
            0.5 + 0.025 * max(0.0, ratio - 0.75)
            + 0.025 * max(0.0, -acceleration),
            0.501,
            0.62,
        )
        output.append(
            {
                **profile,
                "continuation_confidence": continuation,
                "reversal_confidence": reversal,
                "active_alert": ratio >= 1.0,
                "causal": True,
                "research_only": True,
                "account_eligible": False,
            }
        )
    return output


def build_term_forecasts(
    terms: list[dict[str, Any]],
    quotes: dict[str, Any],
    now: float,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    coefficients = {300: 0.12, 900: 0.22, 1800: 0.32, 3600: 0.45}
    for term in terms:
        instrument = str(term["instrument"])
        quote = (quotes.get("quotes") or {}).get(instrument) or {}
        bid = finite(quote.get("bid"))
        ask = finite(quote.get("ask"))
        if bid <= 0.0 or ask < bid:
            continue
        base_sign = 1.0 if str(term["direction"]) == "buy" else -1.0
        observed_move = max(0.05, finite(term.get("executable_net_pips")))
        metadata = {
            "causal": True,
            "hindsight_labels_used": False,
            "validation_status": "prospective_shadow_pending",
            "account_eligible": False,
            "feature_terms": {
                "recent_move_direction": str(term["direction"]),
                "recent_move_net_pips": finite(term["executable_net_pips"]),
                "recent_move_threshold_ratio": finite(term["severity"]),
                "recent_move_volatility_z": finite(term["volatility_z"]),
                "recent_move_acceleration": finite(term["acceleration"]),
                "recent_move_velocity_pips_per_minute": finite(
                    term["velocity_pips_per_minute"]
                ),
                "source_horizon_minutes": int(term["horizon_minutes"]),
            },
        }
        for model_id, sign, confidence_key in (
            (TERM_MODEL_IDS[0], base_sign, "continuation_confidence"),
            (TERM_MODEL_IDS[1], -base_sign, "reversal_confidence"),
        ):
            confidence = finite(term[confidence_key], 0.501)
            direction = "buy" if sign > 0.0 else "sell"
            points = []
            for horizon, coefficient in coefficients.items():
                signed = sign * observed_move * coefficient
                probability_up = confidence if sign > 0.0 else 1.0 - confidence
                points.append(
                    {
                        "horizon_sec": horizon,
                        "direction": direction,
                        "probability_up": probability_up,
                        "signal_confidence": confidence,
                        "predicted_signed_pips": signed,
                        "predicted_magnitude_pips": abs(signed),
                        "account_eligible": False,
                    }
                )
            output.append(
                {
                    "model_id": model_id,
                    "family": "move_alert_causal_state",
                    "instrument": instrument,
                    "input_timeframe": "M1",
                    "generated_epoch": now,
                    "generated_utc": utc_iso(now),
                    "direction": direction,
                    "bid": bid,
                    "ask": ask,
                    "pip": pip_size(instrument, quote),
                    "forecast_curve": points,
                    "account_eligible": False,
                    "research_only": True,
                    "signal_role": "structural_shadow",
                    "producer_metadata": {
                        **metadata,
                        "term_kind": model_id.rsplit(".", 1)[-1],
                    },
                }
            )
    return output


def register_term_contributors(feed: Any) -> int:
    return feed.register_contributors(
        [
            {
                "contributor_id": model_id,
                "family": "move_alert_causal_state",
                "source_kind": "causal_move_state",
                "adapter_module": "oanda_move_alert_monitor",
                "runtime_policy": "prospective_shadow_pending",
                "expected": True,
                "account_eligible": False,
                "metadata": {
                    "causal": True,
                    "research_only": True,
                    "can_place_orders": False,
                    "validation_status": "prospective_shadow_pending",
                },
            }
            for model_id in TERM_MODEL_IDS
        ]
    )


def publish_terms(feed: Any, forecasts: list[dict[str, Any]]) -> dict[str, Any]:
    if not forecasts:
        return {"accepted": 0, "rejected": 0, "reason": "no_active_terms"}
    return feed.publish_forecasts(
        forecasts,
        source="move_alert_monitor",
        ttl_sec=180.0,
        max_age_sec=60.0,
    )


def event_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    missed = [row for row in events if row.get("missed")]
    active = [row for row in events if row.get("status") == "active"]
    caught = [row for row in events if row.get("caught") or not row.get("missed")]
    coverage_gap_codes = {
        "NO_ARCHIVED_PREMOVE_FORECAST",
        "FORECAST_HORIZON_COVERAGE_MISS",
    }
    covered = [
        row
        for row in events
        if str(row.get("error_code") or "") not in coverage_gap_codes
    ]
    gated_aligned = [
        row
        for row in events
        if str(row.get("error_code") or "")
        == "GATED_ALIGNED_PREMOVE_SIGNAL"
    ]
    weak_aligned = [
        row
        for row in events
        if str(row.get("error_code") or "")
        == "WEAK_ALIGNED_PREMOVE_SIGNAL"
    ]
    late_aligned = [
        row
        for row in events
        if str(row.get("error_code") or "") == "LATE_ALIGNED_SIGNAL"
    ]
    codes: dict[str, int] = {}
    for row in missed:
        code = str(row.get("error_code") or "UNKNOWN")
        codes[code] = codes.get(code, 0) + 1
    gated_blockers: dict[str, int] = {}
    gated_horizons: dict[str, int] = {}
    for row in gated_aligned:
        forecast = row.get("forecast") or {}
        if not isinstance(forecast, dict):
            forecast = {}
        for blocker in forecast.get("blockers") or []:
            name = str(blocker or "unknown")
            gated_blockers[name] = gated_blockers.get(name, 0) + 1
        horizon = int(
            finite(
                forecast.get("evaluated_horizon_sec"),
                finite(row.get("horizon_minutes")) * 60.0,
            )
        )
        label = str(horizon) if horizon > 0 else "unknown"
        gated_horizons[label] = gated_horizons.get(label, 0) + 1
    covered_capture_rate = (
        len(caught) / len(covered) if covered else None
    )
    strong_aligned_count = len(caught) + len(gated_aligned)
    return {
        "events_24h": len(events),
        "active_events": len(active),
        "missed_events": len(missed),
        "caught_events": len(caught),
        "miss_rate": (len(missed) / len(events)) if events else None,
        "forecast_coverage": {
            "covered_events": len(covered),
            "coverage_gap_events": len(events) - len(covered),
            "coverage_rate": len(covered) / len(events) if events else None,
        },
        "capture_rates": {
            "all_events": len(caught) / len(events) if events else None,
            "covered_events": covered_capture_rate,
        },
        "gate_what_if": {
            # This is a diagnostic upper bound, not permission to execute.
            # It counts strong, correctly directed pre-move forecasts that
            # would have been catches if only their current gates disappeared.
            "strong_aligned_caught_or_gated": strong_aligned_count,
            "strong_aligned_but_gated": len(gated_aligned),
            "weak_aligned": len(weak_aligned),
            "late_aligned": len(late_aligned),
            "covered_strong_alignment_rate": (
                strong_aligned_count / len(covered) if covered else None
            ),
            "gated_blocker_counts": dict(sorted(gated_blockers.items())),
            "gated_horizon_counts": dict(sorted(gated_horizons.items())),
            "execution_authority": False,
        },
        "error_code_counts": dict(sorted(codes.items())),
    }


def _event_table(events: list[dict[str, Any]], limit: int = 20) -> list[str]:
    if not events:
        return ["No qualifying events are available yet."]
    lines = [
        "| Start UTC | Pair | Side | Horizon | Net pips | Severity | Result | Cluster |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in events[:limit]:
        cluster = (
            f"{row.get('cluster_currency')} {row.get('cluster_direction')}"
            if row.get("cluster_currency")
            else ""
        )
        lines.append(
            f"| {str(row.get('start_utc'))[:19]} | {row.get('instrument')} | "
            f"{str(row.get('direction')).upper()} | {row.get('horizon_minutes')}m | "
            f"{finite(row.get('executable_net_pips')):.1f} | "
            f"{finite(row.get('severity')):.2f}x | {row.get('error_code')} | "
            f"{cluster} |"
        )
    return lines


def write_reports(
    *,
    ledger: MoveAlertLedger,
    terms: list[dict[str, Any]],
    now: float,
    alert_json_path: Path,
    alert_md_path: Path,
    missed_json_path: Path,
    missed_md_path: Path,
    terms_json_path: Path,
) -> dict[str, Any]:
    events = ledger.recent_events(now)
    missed = [row for row in events if row.get("missed")]
    active = [row for row in events if row.get("status") == "active"]
    summary = event_summary(events)
    common = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": utc_iso(now),
        "database": str(ledger.path.resolve()),
        "diagnostic_scope": "research_only",
        "execution_authority": False,
        "account_eligible": False,
        "attribution_contract": {
            "forecast_snapshot_must_be_at_or_before_move_start": True,
            "prices": "executable_bid_ask",
            "horizons_minutes": list(HORIZONS_MINUTES),
            "hindsight_move_labels_are_not_features": True,
        },
        "summary": summary,
    }
    alert_payload = {
        **common,
        "active_alerts": active,
        "recent_events": events[:100],
        "causal_term_count": len(terms),
        "missed_moves_path": str(missed_json_path.resolve()),
    }
    missed_payload = {
        **common,
        "error_surface": "forecast_move_coverage",
        "missed_moves": missed[:100],
        "read_first": True,
    }
    term_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": utc_iso(now),
        "research_only": True,
        "account_eligible": False,
        "validation_status": "prospective_shadow_pending",
        "terms": terms,
    }
    atomic_json(alert_json_path, alert_payload)
    atomic_json(missed_json_path, missed_payload)
    atomic_json(terms_json_path, term_payload)

    alert_lines = [
        "# Move Alert",
        "",
        "**Canonical live move/error surface. Research-only; no execution authority.**",
        "",
        f"- Generated UTC: `{utc_iso(now)}`",
        f"- Active alerts: `{summary['active_events']}`",
        f"- Events in 24h: `{summary['events_24h']}`",
        f"- Missed: `{summary['missed_events']}`",
        f"- Caught: `{summary['caught_events']}`",
        "- Forecast coverage: "
        f"`{summary['forecast_coverage']['covered_events']}/"
        f"{summary['events_24h']}`",
        "- Capture rate on covered events: "
        f"`{finite(summary['capture_rates']['covered_events']) * 100.0:.1f}%`",
        f"- Causal move-state rows: `{len(terms)}`",
        "- Economics: executable entry/exit bid and ask.",
        "- Attribution: only forecasts timestamped at or before move start.",
        "- New move-state contributors remain shadow-only until prospective validation.",
        "",
        "## Active",
        "",
        *_event_table(active),
        "",
        "## Largest Recent",
        "",
        *_event_table(events),
    ]
    atomic_text(alert_md_path, "\n".join(alert_lines) + "\n")

    miss_lines = [
        "# Missed Moves",
        "",
        "**Read this file as the current forecast-coverage error list.**",
        "",
        f"- Generated UTC: `{utc_iso(now)}`",
        f"- Missed events in 24h: `{summary['missed_events']}`",
        "- Forecast attribution coverage: "
        f"`{summary['forecast_coverage']['covered_events']}/"
        f"{summary['events_24h']}`",
        "- Strong aligned but gated: "
        f"`{summary['gate_what_if']['strong_aligned_but_gated']}`",
        "- Weak aligned: "
        f"`{summary['gate_what_if']['weak_aligned']}`",
        "- Late aligned: "
        f"`{summary['gate_what_if']['late_aligned']}`",
        f"- Error codes: `{json_compact(summary['error_code_counts'])}`",
        "- `NO_ARCHIVED_PREMOVE_FORECAST` is an attribution gap, not proof the model was neutral.",
        "- `GATED_ALIGNED_PREMOVE_SIGNAL` identifies raw directional support suppressed by gates.",
        "- `LATE_ALIGNED_SIGNAL` identifies a forecast that reacted after onset.",
        "",
        "## Errors",
        "",
        *_event_table(missed, limit=50),
    ]
    atomic_text(missed_md_path, "\n".join(miss_lines) + "\n")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    parser.add_argument("--signal-feed", type=Path, default=DEFAULT_SIGNAL_FEED)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--alert-json", type=Path, default=DEFAULT_ALERT_JSON)
    parser.add_argument("--alert-md", type=Path, default=DEFAULT_ALERT_MD)
    parser.add_argument("--missed-json", type=Path, default=DEFAULT_MISSED_JSON)
    parser.add_argument("--missed-md", type=Path, default=DEFAULT_MISSED_MD)
    parser.add_argument("--terms-json", type=Path, default=DEFAULT_TERMS_JSON)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--process-lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--depth-root", type=Path, default=DEFAULT_DEPTH_ROOT)
    parser.add_argument("--bootstrap-depth-hours", type=float, default=0.0)
    parser.add_argument("--bootstrap-max-files", type=int, default=24)
    parser.add_argument("--bootstrap-time-budget-sec", type=float, default=30.0)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=28800.0)
    parser.add_argument("--max-quote-age-sec", type=float, default=90.0)
    parser.add_argument("--retention-days", type=int, default=7)
    parser.add_argument("--publish-terms", action="store_true")
    parser.add_argument("--publish-interval-sec", type=float, default=60.0)
    parser.add_argument("--report-interval-sec", type=float, default=3600.0)
    parser.add_argument("--heartbeat-interval-sec", type=float, default=300.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    lock = ProcessLock(args.process_lock)
    if not lock.acquire():
        raise SystemExit("move alert monitor already running")
    ledger: MoveAlertLedger | None = None
    feed: Any = None
    last_signal_mtime = -1
    last_publish = 0.0
    last_feed_attempt = 0.0
    last_prune = 0.0
    last_report = 0.0
    last_heartbeat = 0.0
    last_event_fingerprint = ""
    summary: dict[str, Any] = {}
    totals = {
        "quote_rows": 0,
        "forecast_rows": 0,
        "events_touched": 0,
        "term_forecasts_accepted": 0,
        "term_forecasts_rejected": 0,
        "cycles": 0,
        "errors": 0,
    }
    started = time.time()
    bootstrap: dict[str, Any] = {
        "status": "deferred_no_fresh_quotes",
        "files": 0,
        "rows": 0,
    }
    feed_error = (
        "deferred_no_fresh_quotes"
        if args.publish_terms
        else "publication_disabled"
    )
    try:
        while True:
            cycle_started = time.time()
            now = cycle_started
            status = "ok"
            publish_result: dict[str, Any] = {}
            try:
                quotes_payload = read_json(args.quotes)
                rows = quote_rows(
                    quotes_payload,
                    now=now,
                    max_quote_age_sec=args.max_quote_age_sec,
                )
                if not rows:
                    publish_result = {
                        "accepted": 0,
                        "rejected": 0,
                        "reason": "no_fresh_quotes",
                    }
                else:
                    if ledger is None:
                        ledger = MoveAlertLedger(
                            args.database,
                            retention_days=args.retention_days,
                        )
                        bootstrap = bootstrap_depth_history(
                            ledger,
                            args.depth_root,
                            now=now,
                            hours=args.bootstrap_depth_hours,
                            max_files=args.bootstrap_max_files,
                            time_budget_sec=args.bootstrap_time_budget_sec,
                        )
                    if (
                        args.publish_terms
                        and feed is None
                        and now - last_feed_attempt >= 60.0
                    ):
                        last_feed_attempt = now
                        try:
                            from oanda_signal_contribution_feed import (
                                SignalContributionFeed,
                            )

                            feed = SignalContributionFeed(args.signal_feed)
                            register_term_contributors(feed)
                        except Exception as exc:
                            totals["errors"] += 1
                            feed = None
                            feed_error = f"{type(exc).__name__}: {exc}"
                        else:
                            feed_error = ""
                    totals["quote_rows"] += ledger.ingest_quotes(rows)
                    try:
                        signal_mtime = args.signals.stat().st_mtime_ns
                    except OSError:
                        signal_mtime = -1
                    if signal_mtime >= 0 and signal_mtime != last_signal_mtime:
                        totals["forecast_rows"] += ledger.archive_forecasts(
                            read_json(args.signals)
                        )
                        last_signal_mtime = signal_mtime
                    profiles = ledger.current_profiles(now)
                    touched = ledger.record_moves(profiles, now)
                    totals["events_touched"] += len(touched)
                    terms = build_causal_terms(profiles)
                    if (
                        feed is not None
                        and now - last_publish >= args.publish_interval_sec
                    ):
                        forecasts = build_term_forecasts(terms, quotes_payload, now)
                        publish_result = publish_terms(feed, forecasts)
                        totals["term_forecasts_accepted"] += int(
                            finite(publish_result.get("accepted"))
                        )
                        totals["term_forecasts_rejected"] += int(
                            finite(publish_result.get("rejected"))
                        )
                        last_publish = now
                    if now - last_prune >= 3600.0:
                        ledger.prune(now)
                        last_prune = now
                    current_events = ledger.recent_events(now)
                    summary = event_summary(current_events)
                    event_fingerprint = hashlib.sha256(
                        json_compact(
                            [
                                (
                                    row.get("event_id"),
                                    row.get("status"),
                                    row.get("error_code"),
                                    row.get("cluster_id"),
                                )
                                for row in current_events
                            ]
                        ).encode("utf-8")
                    ).hexdigest()
                    report_due = (
                        last_report <= 0.0
                        or (
                            event_fingerprint != last_event_fingerprint
                            and now - last_report >= 30.0
                        )
                        or now - last_report
                        >= max(60.0, args.report_interval_sec)
                    )
                    if report_due:
                        summary = write_reports(
                            ledger=ledger,
                            terms=terms,
                            now=now,
                            alert_json_path=args.alert_json,
                            alert_md_path=args.alert_md,
                            missed_json_path=args.missed_json,
                            missed_md_path=args.missed_md,
                            terms_json_path=args.terms_json,
                        )
                        last_report = now
                        last_event_fingerprint = event_fingerprint
            except Exception as exc:
                totals["errors"] += 1
                status = f"error:{type(exc).__name__}:{exc}"
            totals["cycles"] += 1
            heartbeat = {
                "schema_version": SCHEMA_VERSION,
                "generated_utc": utc_iso(),
                "status": status,
                "pid": os.getpid(),
                "started_utc": utc_iso(started),
                "uptime_sec": round(time.time() - started, 3),
                "move_alert_execution_enabled": False,
                "can_place_orders": False,
                "shared_account_execution_changed": False,
                "research_terms_account_eligible": False,
                "publication_requested": bool(args.publish_terms),
                "publication_error": feed_error,
                "depth_bootstrap": bootstrap,
                "last_publish": publish_result,
                "summary": summary,
                "totals": totals,
                "paths": {
                    "database": str(args.database.resolve()),
                    "move_alert_latest": str(args.alert_md.resolve()),
                    "missed_moves_latest": str(args.missed_md.resolve()),
                    "move_state_terms": str(args.terms_json.resolve()),
                },
            }
            heartbeat_due = (
                last_heartbeat <= 0.0
                or time.time() - last_heartbeat
                >= max(30.0, args.heartbeat_interval_sec)
                or status.startswith("error:")
                or args.once
            )
            if heartbeat_due:
                atomic_json(args.heartbeat, heartbeat)
                last_heartbeat = time.time()
            if args.once or time.time() - started >= args.duration_sec:
                print(json.dumps(heartbeat, indent=2, sort_keys=True))
                break
            elapsed = time.time() - cycle_started
            time.sleep(max(0.05, args.interval_sec - elapsed))
    finally:
        if feed is not None:
            feed.close()
        if ledger is not None:
            ledger.close()
        lock.release()
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
