#!/usr/bin/env python3
"""Immutable prospective collector for high GDELT currency attention.

The frozen hypothesis predicts only whether a currency-hour with at least
three independent mapped GDELT stories will produce cost-relevant movement at
60 or 240 minutes.  It explicitly abstains on direction and has no broker,
authorization, promotion, or execution client.
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

from oanda_gdelt_attention_mapping_audit import build_currency_hours, load_mapped_stories
from oanda_local_news_sentiment import (
    COLLECTOR_COHORT_ID,
    COLLECTOR_CONTRACT_ID,
    OBSERVATION_TIME_CONTRACT_ID,
    normalized_observation_time,
    prospective_collector_provenance,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "gdelt_attention_magnitude_v1.json"
NEWS = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
LEDGER = STATE / "gdelt_attention_magnitude_prospective_v1.sqlite"
OUTPUT = STATE / "gdelt_attention_magnitude_prospective_v1.json"
REPORT = DATA / "reports" / "news_mapping" / "GDELT_ATTENTION_PROSPECTIVE_CURRENT.md"
MAPPING_HELPER = ROOT / "oanda_gdelt_attention_mapping_audit.py"


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def parse_utc(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    for attempt in range(8):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 7:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.4, 0.025 * (2**attempt)))


def contract(
    config: dict[str, Any], *,
    config_sha256: str | None = None,
    collector_sha256: str | None = None,
    observation_helper_sha256: str | None = None,
    mapping_helper_sha256: str | None = None,
    observation_time_contract_id: str = OBSERVATION_TIME_CONTRACT_ID,
) -> dict[str, Any]:
    config_sha = config_sha256 or digest(CONFIG)
    collector_sha = collector_sha256 or digest(Path(__file__))
    helper_sha = observation_helper_sha256 or digest(
        ROOT / "oanda_local_news_sentiment.py"
    )
    mapping_sha = mapping_helper_sha256 or digest(MAPPING_HELPER)
    definition_sha = hashlib.sha256(
        (
            f"{config_sha}|{collector_sha}|{helper_sha}|{mapping_sha}|"
            f"{observation_time_contract_id}"
        ).encode("utf-8")
    ).hexdigest()
    legacy_config_only_id = (
        f"gdelt_attention_magnitude_v1.discovery.20260808.{config_sha[:16]}"
    )
    return {
        "cohort_id": f"gdelt_attention_magnitude_v1.discovery.20260808.{definition_sha[:16]}",
        "supersedes_cohort_id": legacy_config_only_id,
        "cohort_definition_sha256": definition_sha,
        "config_sha256": config_sha,
        "collector_sha256": collector_sha,
        "observation_helper_sha256": helper_sha,
        "mapping_helper_sha256": mapping_sha,
        "observation_time_contract_id": observation_time_contract_id,
        "direction_policy": "abstain",
        "material_change_requires_new_cohort": True,
        "forecasts_written_before_outcomes": True,
        "late_backfill_refused": True,
        "research_only": True,
        "execution_eligible": False,
    }


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS forecasts(
          forecast_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,
          issued_at_utc TEXT NOT NULL,source_hour_start_utc TEXT NOT NULL,
          decision_utc TEXT NOT NULL,source_story_count INTEGER NOT NULL,
          source_count INTEGER NOT NULL,lineage_ids_json TEXT NOT NULL,
          source_payload_sha256 TEXT NOT NULL,currency TEXT NOT NULL,
          instrument TEXT NOT NULL,horizon_sec INTEGER NOT NULL,
          entry_quote_utc TEXT NOT NULL,entry_bid REAL NOT NULL,entry_ask REAL NOT NULL,
          entry_mid REAL NOT NULL,entry_spread_bps REAL NOT NULL,
          direction_policy TEXT NOT NULL CHECK(direction_policy='abstain'),
          config_sha256 TEXT NOT NULL,collector_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(cohort_id,decision_utc,currency,instrument,horizon_sec)
        );
        CREATE TABLE IF NOT EXISTS outcomes(
          forecast_id TEXT PRIMARY KEY REFERENCES forecasts(forecast_id),
          matured_at_utc TEXT NOT NULL,outcome_quote_utc TEXT NOT NULL,
          exit_bid REAL NOT NULL,exit_ask REAL NOT NULL,exit_mid REAL NOT NULL,
          signed_pair_move_bps REAL NOT NULL,absolute_move_bps REAL NOT NULL,
          strengthening_net_bps REAL NOT NULL,weakening_net_bps REAL NOT NULL,
          best_direction_net_bps REAL NOT NULL,movement_cleared_cost INTEGER NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
        );
        CREATE TABLE IF NOT EXISTS cycles(
          generated_utc TEXT PRIMARY KEY,status TEXT NOT NULL,payload_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS gdelt_forecasts_no_update BEFORE UPDATE ON forecasts
          BEGIN SELECT RAISE(ABORT,'immutable forecasts'); END;
        CREATE TRIGGER IF NOT EXISTS gdelt_forecasts_no_delete BEFORE DELETE ON forecasts
          BEGIN SELECT RAISE(ABORT,'immutable forecasts'); END;
        CREATE TRIGGER IF NOT EXISTS gdelt_outcomes_no_update BEFORE UPDATE ON outcomes
          BEGIN SELECT RAISE(ABORT,'immutable outcomes'); END;
        CREATE TRIGGER IF NOT EXISTS gdelt_outcomes_no_delete BEFORE DELETE ON outcomes
          BEGIN SELECT RAISE(ABORT,'immutable outcomes'); END;
        """
    )
    return db


def quote_rows(path: Path, pair_map: dict[str, str]) -> tuple[dict[str, dict[str, Any]], dt.datetime | None]:
    payload = read_json(path)
    output = {}
    latest = None
    for instrument in sorted(set(pair_map.values())):
        raw = (payload.get("quotes") or {}).get(instrument)
        if not isinstance(raw, dict):
            continue
        stamp = parse_utc(raw.get("time"))
        try:
            bid, ask = float(raw.get("bid")), float(raw.get("ask"))
        except (TypeError, ValueError):
            continue
        if stamp is None or bid <= 0 or ask < bid:
            continue
        output[instrument] = {"time": stamp, "bid": bid, "ask": ask, "mid": (bid + ask) / 2.0}
        latest = stamp if latest is None or stamp > latest else latest
    return output, latest


def completed_hour(latest_quote: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
    end = latest_quote.replace(minute=0, second=0, microsecond=0)
    return end - dt.timedelta(hours=1), end


def eligible_attention_hours(
    news_database: Path,
    hour_start: dt.datetime,
    decision: dt.datetime,
    minimum_stories: int,
) -> list[dict[str, Any]]:
    stories, _ = load_mapped_stories(news_database)
    stories = [row for row in stories if prospective_collector_provenance(row)]
    rows = build_currency_hours(stories)
    return [
        row
        for row in rows
        if row["hour_start_utc"] == iso(hour_start)
        and row["decision_utc"] == iso(decision)
        and int(row["story_count"]) >= minimum_stories
    ]


def issue(
    db: sqlite3.Connection,
    rows: list[dict[str, Any]],
    quotes: dict[str, dict[str, Any]],
    latest_quote: dt.datetime,
    config: dict[str, Any],
    cohort: dict[str, Any],
    generated: dt.datetime,
) -> tuple[int, list[str]]:
    inserted = 0
    skipped = []
    pair_map = {str(key): str(value) for key, value in (config.get("pair_map") or {}).items()}
    decision = latest_quote.replace(minute=0, second=0, microsecond=0)
    if (latest_quote - decision).total_seconds() > float(config["maximum_issue_lag_sec"]):
        return 0, ["completed_hour_late_backfill_refused"]
    for row in rows:
        currency = str(row["currency"])
        instrument = pair_map.get(currency)
        quote = quotes.get(str(instrument))
        if not instrument or not quote:
            skipped.append(f"{currency}_missing_quote")
            continue
        quote_time = quote["time"]
        if quote_time < decision or (latest_quote - quote_time).total_seconds() > float(config["maximum_quote_age_sec"]):
            skipped.append(f"{currency}_quote_not_causal_or_fresh")
            continue
        source_payload = {
            "currency": currency,
            "hour_start_utc": row["hour_start_utc"],
            "decision_utc": row["decision_utc"],
            "story_count": int(row["story_count"]),
            "source_count": int(row["source_count"]),
            "lineage_ids": row["lineage_ids"],
        }
        source_sha = hashlib.sha256(json.dumps(source_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        spread_bps = (quote["ask"] - quote["bid"]) / quote["mid"] * 10000.0
        for horizon in config.get("horizons_sec") or []:
            key = f"{cohort['cohort_id']}|{row['decision_utc']}|{currency}|{instrument}|{int(horizon)}"
            cursor = db.execute(
                "INSERT OR IGNORE INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    hashlib.sha256(key.encode()).hexdigest(), cohort["cohort_id"], generated.isoformat(),
                    row["hour_start_utc"], row["decision_utc"], int(row["story_count"]),
                    int(row["source_count"]), json.dumps(row["lineage_ids"], sort_keys=True), source_sha,
                    currency, instrument, int(horizon), iso(quote_time), quote["bid"], quote["ask"], quote["mid"],
                    spread_bps, "abstain", cohort["config_sha256"], cohort["collector_sha256"], 1, 0,
                ),
            )
            inserted += int(bool(cursor.rowcount))
    return inserted, skipped


def mature(
    db: sqlite3.Connection,
    quotes: dict[str, dict[str, Any]],
    generated: dt.datetime,
    maximum_lag_sec: float,
) -> int:
    rows = db.execute(
        """SELECT f.forecast_id,f.decision_utc,f.currency,f.instrument,f.horizon_sec,
                  f.entry_bid,f.entry_ask,f.entry_mid
           FROM forecasts f LEFT JOIN outcomes o ON o.forecast_id=f.forecast_id
           WHERE o.forecast_id IS NULL"""
    ).fetchall()
    inserted = 0
    for forecast_id, decision_text, currency, instrument, horizon, entry_bid, entry_ask, entry_mid in rows:
        target = parse_utc(decision_text)
        quote = quotes.get(str(instrument))
        if target is None or not quote:
            continue
        target += dt.timedelta(seconds=int(horizon))
        quote_time = quote["time"]
        lag = (quote_time - target).total_seconds()
        if lag < 0 or lag > maximum_lag_sec:
            continue
        pair_move = (quote["mid"] / float(entry_mid) - 1.0) * 10000.0
        base, quote_currency = str(instrument).split("_")
        orientation = 1 if base == currency else -1 if quote_currency == currency else 0
        strengthening_pair_side = orientation
        if strengthening_pair_side > 0:
            strengthening = (quote["bid"] - float(entry_ask)) / float(entry_mid) * 10000.0
            weakening = (float(entry_bid) - quote["ask"]) / float(entry_mid) * 10000.0
        else:
            strengthening = (float(entry_bid) - quote["ask"]) / float(entry_mid) * 10000.0
            weakening = (quote["bid"] - float(entry_ask)) / float(entry_mid) * 10000.0
        cursor = db.execute(
            "INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                forecast_id, generated.isoformat(), iso(quote_time), quote["bid"], quote["ask"], quote["mid"],
                pair_move, abs(pair_move), strengthening, weakening, max(strengthening, weakening),
                int(max(strengthening, weakening) > 0.0), 1, 0,
            ),
        )
        inserted += int(bool(cursor.rowcount))
    return inserted


def totals(
    db: sqlite3.Connection, cohort_id: str | None = None
) -> dict[str, Any]:
    where = " WHERE cohort_id=?" if cohort_id is not None else ""
    parameters = (cohort_id,) if cohort_id is not None else ()
    forecast = db.execute(
        "SELECT COUNT(*),COUNT(DISTINCT decision_utc),COUNT(DISTINCT currency) "
        f"FROM forecasts{where}",
        parameters,
    ).fetchone()
    outcome = db.execute(
        "SELECT COUNT(*),AVG(o.movement_cleared_cost),"
        "AVG(o.best_direction_net_bps),AVG(o.absolute_move_bps) "
        "FROM outcomes o JOIN forecasts f USING(forecast_id)"
        + (" WHERE f.cohort_id=?" if cohort_id is not None else ""),
        parameters,
    ).fetchone()
    return {
        "forecasts": int(forecast[0] or 0),
        "decision_hours": int(forecast[1] or 0),
        "currencies": int(forecast[2] or 0),
        "matured": int(outcome[0] or 0),
        "cost_clear_rate": outcome[1],
        "mean_best_direction_net_bps": outcome[2],
        "mean_absolute_move_bps": outcome[3],
    }


def run_once(
    *,
    news_database: Path = NEWS,
    quotes_path: Path = QUOTES,
    ledger_path: Path = LEDGER,
    output: Path = OUTPUT,
    report: Path = REPORT,
) -> dict[str, Any]:
    generated, observation_clock = normalized_observation_time(dt.datetime.now(dt.timezone.utc))
    config = read_json(CONFIG)
    cohort = contract(config)
    quotes, latest = quote_rows(quotes_path, config.get("pair_map") or {})
    status = "market_or_quote_stale"
    new_forecasts = new_outcomes = 0
    skipped = []
    source_hour = decision = None
    db = open_ledger(ledger_path)
    clock_trusted = bool(
        observation_clock.get("trusted_for_prospective_evidence") is True
        and observation_clock.get("contract_id") == OBSERVATION_TIME_CONTRACT_ID
    )
    quote_fresh = bool(
        latest is not None
        and -5.0 <= (generated - latest).total_seconds() <= float(config["maximum_quote_age_sec"])
    )
    current_before = totals(db, cohort["cohort_id"])
    matured_all_history = 0
    if quote_fresh and clock_trusted:
        status = "ok"
        matured_all_history = mature(
            db, quotes, generated, float(config["maximum_maturity_lag_sec"])
        )
        source_hour, decision = completed_hour(latest)
        rows = eligible_attention_hours(
            news_database, source_hour, decision,
            int(config["minimum_independent_stories_per_currency_hour"]),
        )
        new_forecasts, skipped = issue(db, rows, quotes, latest, config, cohort, generated)
    elif quote_fresh and not clock_trusted:
        status = "blocked_clock_integrity"
    current = totals(db, cohort["cohort_id"])
    new_outcomes = current["matured"] - current_before["matured"]
    all_history = totals(db)
    payload = {
        "schema_version": 1,
        "generated_utc": generated.isoformat(),
        "status": status,
        "cohort": cohort,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
        "direction_policy": "abstain",
        "observation_clock": observation_clock,
        "latest_quote_utc": iso(latest) if latest else None,
        "source_hour_start_utc": iso(source_hour) if source_hour else None,
        "decision_utc": iso(decision) if decision else None,
        "cycle": {
            "new_forecasts": new_forecasts,
            "new_outcomes": new_outcomes,
            "all_history_new_outcomes": matured_all_history,
            "skipped": skipped,
        },
        "totals": current,
        "all_history_diagnostic_totals": all_history,
    }
    db.execute("INSERT INTO cycles VALUES (?,?,?)", (generated.isoformat(), status, json.dumps(payload, sort_keys=True)))
    db.commit(); db.close()
    atomic(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# GDELT Attention Magnitude Prospective Cohort", "",
        f"Generated: `{payload['generated_utc']}`", "",
        f"- Status: **{status}**",
        f"- Cohort: `{cohort['cohort_id']}`",
        f"- Forecasts / maturities: **{current['forecasts']} / {current['matured']}**",
        f"- Current cycle: **+{new_forecasts} forecasts / +{new_outcomes} outcomes**",
        "- Direction: **abstain**",
        "- Execution: **research-only; no_trade**", "",
        "Stale markets and missed issuance windows create no forecast; late backfill is forbidden.", "",
    ]
    atomic(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec
    while True:
        run_once()
        if args.once or time.monotonic() >= stop:
            return 0
        time.sleep(min(args.interval_sec, max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["completed_hour", "contract", "mature", "open_ledger", "quote_rows", "run_once"]
