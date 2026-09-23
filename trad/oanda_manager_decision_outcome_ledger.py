#!/usr/bin/env python3
"""Mature account-manager research forecasts at exact executable quote time.

This ledger is observation-only.  It cannot submit, amend, or close orders and
is deliberately separate from model proof cohorts and broker account P/L.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
SCHEMA_VERSION = 1
MAX_TARGET_QUOTE_DISTANCE_SEC = 60.0


def iso_utc(value: dt.datetime | None = None) -> str:
    return (value or dt.datetime.now(UTC)).astimezone(UTC).isoformat()


def parse_epoch(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_id(prefix: str, value: Any) -> str:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:32]}"


def pip_size(instrument: str, signal: dict[str, Any]) -> float:
    try:
        value = float(signal.get("pip"))
        if value > 0.0:
            return value
    except (TypeError, ValueError):
        pass
    return 0.01 if str(instrument).endswith("_JPY") else 0.0001


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS manager_forecasts (
            forecast_id TEXT PRIMARY KEY,
            observed_epoch REAL NOT NULL,
            observed_utc TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            horizon_sec REAL NOT NULL,
            target_epoch REAL NOT NULL,
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            pip REAL NOT NULL,
            source_kind TEXT,
            source_line_number INTEGER NOT NULL,
            imported_utc TEXT NOT NULL,
            status TEXT NOT NULL,
            forecast_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS manager_outcomes (
            forecast_id TEXT PRIMARY KEY,
            matured_utc TEXT NOT NULL,
            outcome_quote_epoch REAL NOT NULL,
            outcome_quote_at TEXT NOT NULL,
            target_quote_distance_sec REAL NOT NULL,
            exit_bid REAL NOT NULL,
            exit_ask REAL NOT NULL,
            gross_mid_pips REAL NOT NULL,
            executable_net_pips REAL NOT NULL,
            realized_cost_pips REAL NOT NULL,
            no_trade_net_pips REAL NOT NULL,
            delta_vs_no_trade_pips REAL NOT NULL,
            comparator_json TEXT NOT NULL,
            outcome_json TEXT NOT NULL,
            FOREIGN KEY(forecast_id) REFERENCES manager_forecasts(forecast_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS manager_integrity_events (
            event_id TEXT PRIMARY KEY,
            forecast_id TEXT NOT NULL,
            recorded_utc TEXT NOT NULL,
            reason TEXT NOT NULL,
            detail_json TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def normalize_forecast(row: dict[str, Any], line_number: int) -> dict[str, Any] | None:
    signal = row.get("signal")
    if not isinstance(signal, dict):
        return None
    instrument = str(signal.get("instrument") or "").upper()
    direction_raw = str(signal.get("direction") or "").lower()
    direction = {"long": "buy", "short": "sell"}.get(direction_raw, direction_raw)
    observed = parse_epoch(signal.get("broker_quote_time") or row.get("broker_quote_time"))
    clock_source = "broker_quote_time"
    if observed is None:
        observed = parse_epoch(row.get("observed_utc"))
        clock_source = "manager_observed_utc"
    try:
        horizon = float(signal.get("horizon_sec") or 60.0 * float(signal.get("horizon_min")))
        entry_bid = float(signal.get("entry_bid"))
        entry_ask = float(signal.get("entry_ask"))
    except (TypeError, ValueError):
        return None
    if (
        not instrument
        or direction not in {"buy", "sell"}
        or observed is None
        or horizon <= 0.0
        or entry_ask < entry_bid
    ):
        return None
    identity = signal.get("episode_id") or signal.get("signal_id")
    if not identity:
        identity = stable_id(
            "manager",
            {
                "observed": observed,
                "instrument": instrument,
                "direction": direction,
                "horizon": horizon,
                "entry_bid": entry_bid,
                "entry_ask": entry_ask,
            },
        )
    return {
        "forecast_id": str(identity),
        "observed_epoch": observed,
        "observed_utc": iso_utc(dt.datetime.fromtimestamp(observed, UTC)),
        "instrument": instrument,
        "direction": direction,
        "horizon_sec": horizon,
        "target_epoch": observed + horizon,
        "entry_bid": entry_bid,
        "entry_ask": entry_ask,
        "pip": pip_size(instrument, signal),
        "source_kind": str(row.get("kind") or "manager_forecast"),
        "source_line_number": line_number,
        "decision_clock_source": clock_source,
        "forecast": row,
    }


def import_forecasts(connection: sqlite3.Connection, decisions: Path) -> dict[str, int]:
    inserted = duplicates = ignored = invalid = 0
    try:
        lines = decisions.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {"inserted": 0, "duplicates": 0, "ignored": 0, "invalid": 0}
    for line_number, line in enumerate(lines, 1):
        try:
            source = json.loads(line)
        except json.JSONDecodeError:
            invalid += 1
            continue
        if not isinstance(source, dict):
            invalid += 1
            continue
        forecast = normalize_forecast(source, line_number)
        if forecast is None:
            ignored += 1
            continue
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO manager_forecasts VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                forecast["forecast_id"], forecast["observed_epoch"],
                forecast["observed_utc"], forecast["instrument"],
                forecast["direction"], forecast["horizon_sec"],
                forecast["target_epoch"], forecast["entry_bid"],
                forecast["entry_ask"], forecast["pip"],
                forecast["source_kind"], forecast["source_line_number"],
                iso_utc(), "pending", canonical_json(forecast),
            ),
        )
        if cursor.rowcount:
            inserted += 1
        else:
            duplicates += 1
    connection.commit()
    return {
        "inserted": inserted,
        "duplicates": duplicates,
        "ignored": ignored,
        "invalid": invalid,
    }


def quote_epoch(row: dict[str, Any]) -> float | None:
    return parse_epoch(row.get("time") or row.get("timestamp") or row.get("quote_epoch"))


def first_instrument_quote_at_or_after(
    quote_database: Path,
    instrument: str,
    target_epoch: float,
    *,
    search_window_sec: float = 180.0,
) -> tuple[float, dict[str, Any]] | None:
    """Select by embedded broker quote time, not the host publication clock."""

    connection = sqlite3.connect(
        f"file:{quote_database.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        rows = connection.execute(
            """
            SELECT payload_json FROM quote_snapshots_v2
            WHERE published_epoch BETWEEN ? AND ?
            ORDER BY published_epoch
            """,
            (target_epoch - search_window_sec, target_epoch + search_window_sec),
        ).fetchall()
    finally:
        connection.close()
    candidates: list[tuple[float, dict[str, Any]]] = []
    for (raw,) in rows:
        try:
            payload = json.loads(str(raw))
            quote = (payload.get("quotes") or {}).get(instrument)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(quote, dict):
            continue
        epoch = quote_epoch(quote)
        try:
            bid, ask = float(quote.get("bid")), float(quote.get("ask"))
        except (TypeError, ValueError):
            continue
        if epoch is None or epoch < target_epoch or ask < bid:
            continue
        candidates.append((epoch, {**quote, "bid": bid, "ask": ask}))
    return min(candidates, key=lambda item: item[0]) if candidates else None


def record_integrity(
    connection: sqlite3.Connection, forecast_id: str, reason: str, detail: dict[str, Any]
) -> None:
    event_id = stable_id("manager_integrity", {"forecast_id": forecast_id, "reason": reason})
    connection.execute(
        "INSERT OR IGNORE INTO manager_integrity_events VALUES (?,?,?,?,?)",
        (event_id, forecast_id, iso_utc(), reason, canonical_json(detail)),
    )
    connection.execute(
        "UPDATE manager_forecasts SET status='quarantined_maturity' WHERE forecast_id=?",
        (forecast_id,),
    )


def mature_forecasts(
    connection: sqlite3.Connection,
    quote_database: Path,
    *,
    now_epoch: float | None = None,
    maximum_distance_sec: float = MAX_TARGET_QUOTE_DISTANCE_SEC,
) -> dict[str, int]:
    current = float(now_epoch if now_epoch is not None else time.time())
    rows = connection.execute(
        """
        SELECT forecast_id,instrument,direction,target_epoch,entry_bid,entry_ask,pip,
               forecast_json FROM manager_forecasts
        WHERE status='pending' AND target_epoch <= ? ORDER BY target_epoch
        """,
        (current,),
    ).fetchall()
    matured = quarantined = waiting = 0
    for forecast_id, instrument, direction, target, entry_bid, entry_ask, pip, raw in rows:
        target = float(target)
        result = first_instrument_quote_at_or_after(quote_database, instrument, target)
        if result is None:
            if current <= target + maximum_distance_sec:
                waiting += 1
                continue
            record_integrity(
                connection, forecast_id, "missing_exact_target_quote",
                {"instrument": instrument, "target_epoch": target},
            )
            quarantined += 1
            continue
        outcome_epoch, quote = result
        distance = outcome_epoch - target
        if distance > maximum_distance_sec:
            record_integrity(
                connection, forecast_id, "target_quote_too_late",
                {
                    "instrument": instrument, "target_epoch": target,
                    "outcome_quote_epoch": outcome_epoch,
                    "target_quote_distance_sec": distance,
                },
            )
            quarantined += 1
            continue
        pip = float(pip)
        entry_mid = (float(entry_bid) + float(entry_ask)) / 2.0
        exit_mid = (quote["bid"] + quote["ask"]) / 2.0
        sign = 1.0 if direction == "buy" else -1.0
        gross = sign * (exit_mid - entry_mid) / pip
        executable = (
            (quote["bid"] - float(entry_ask)) / pip
            if direction == "buy"
            else (float(entry_bid) - quote["ask"]) / pip
        )
        cost = gross - executable
        comparator = {
            "no_trade": {"net_pips": 0.0, "available": True},
            "best_rejected_alternative": {
                "available": False,
                "reason": "complete_candidate_set_not_recorded_in_manager_log",
            },
            "hold_or_rotate": {
                "available": False,
                "reason": "position_and_alternative_set_not_recorded_in_manager_log",
            },
        }
        outcome = {
            "forecast": json.loads(str(raw)),
            "outcome_quote_epoch": outcome_epoch,
            "outcome_quote_at": iso_utc(dt.datetime.fromtimestamp(outcome_epoch, UTC)),
            "target_quote_distance_sec": distance,
            "exit_bid": quote["bid"], "exit_ask": quote["ask"],
            "gross_mid_pips": gross, "executable_net_pips": executable,
            "realized_cost_pips": cost, "comparators": comparator,
            "research_only": True, "execution_authorized": False,
        }
        connection.execute(
            """
            INSERT INTO manager_outcomes VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                forecast_id, iso_utc(), outcome_epoch, outcome["outcome_quote_at"],
                distance, quote["bid"], quote["ask"], gross, executable, cost,
                0.0, executable, canonical_json(comparator), canonical_json(outcome),
            ),
        )
        connection.execute(
            "UPDATE manager_forecasts SET status='matured' WHERE forecast_id=?",
            (forecast_id,),
        )
        matured += 1
    connection.commit()
    return {"matured": matured, "quarantined": quarantined, "waiting": waiting}


def state_payload(connection: sqlite3.Connection, database: Path) -> dict[str, Any]:
    status_counts = dict(
        connection.execute(
            "SELECT status,COUNT(*) FROM manager_forecasts GROUP BY status"
        ).fetchall()
    )
    summary = connection.execute(
        """
        SELECT COUNT(*),AVG(executable_net_pips),
               AVG(CASE WHEN executable_net_pips>0 THEN 1.0 ELSE 0.0 END)
        FROM manager_outcomes
        """
    ).fetchone()
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": iso_utc(),
        "status": "ok",
        "database": str(database.resolve()),
        "forecasts": {str(key): int(value) for key, value in status_counts.items()},
        "outcomes": {
            "matured": int(summary[0] or 0),
            "average_executable_net_pips": None if summary[1] is None else float(summary[1]),
            "after_cost_win_rate": None if summary[2] is None else float(summary[2]),
        },
        "integrity_events": int(
            connection.execute("SELECT COUNT(*) FROM manager_integrity_events").fetchone()[0]
        ),
        "measurement_contract": {
            "target_time": "declared_horizon_from_recorded_decision_clock",
            "outcome_time": "first_embedded_broker_quote_at_or_after_target",
            "maximum_target_quote_distance_sec": MAX_TARGET_QUOTE_DISTANCE_SEC,
            "account_pnl_mixed": False,
            "model_proof_cohorts_mixed": False,
        },
        "research_only": True,
        "can_place_orders": False,
        "real_money_routing": False,
    }


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    root = Path(__file__).resolve().parent
    state_root = root / "data" / "oanda_training_manager" / "state"
    report_root = root / "data" / "oanda_training_manager" / "reports"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--decisions", type=Path,
        default=report_root / "LIVE_ACCOUNT_MANAGER_FORECASTS_20260810.jsonl",
    )
    parser.add_argument(
        "--quotes-database", type=Path,
        default=state_root / "practice_007_market_quotes_v1.json.sqlite",
    )
    parser.add_argument(
        "--database", type=Path,
        default=state_root / "manager_decision_outcomes_v1.sqlite",
    )
    parser.add_argument(
        "--state", type=Path,
        default=state_root / "manager_decision_outcomes_v1.json",
    )
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    args = parser.parse_args()
    connection = connect(args.database)
    deadline = time.monotonic() + max(0.0, args.duration_sec)
    try:
        while time.monotonic() < deadline:
            imported = import_forecasts(connection, args.decisions)
            matured = mature_forecasts(connection, args.quotes_database)
            payload = state_payload(connection, args.database)
            payload["latest_cycle"] = {"import": imported, "maturity": matured}
            atomic_write_json(args.state, payload)
            time.sleep(max(0.25, args.interval_sec))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
