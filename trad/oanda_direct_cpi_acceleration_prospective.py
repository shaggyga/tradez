#!/usr/bin/env python3
"""Collect the frozen direct-official CPI-acceleration hypothesis in shadow.

The discovery result that motivated this cohort was current-view and therefore
cannot prove an edge.  This worker accepts only direct, verified, timely
official observations first known after the frozen cohort start.  It cannot
trade, authorize, promote, or change Practice-007 policy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_local_news_sentiment import normalized_observation_time
from oanda_rate_relative_strength_prospective import (
    arm_for,
    finite,
    iso,
    load_quotes,
    parse_utc,
    read_json,
    stable_hash,
    technical_context,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
MACRO_DB = STATE / "macro_surprise_v1.sqlite"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
OPPORTUNITY_DB = STATE / "executable_opportunity_prospective_v1.sqlite"
OPPORTUNITY_STATE = STATE / "executable_opportunity_prospective_v1.json"
CONFIG = ROOT / "config" / "direct_cpi_acceleration_prospective_v1.json"
DATABASE = STATE / "direct_cpi_acceleration_prospective_v1.sqlite"
OUTPUT = STATE / "direct_cpi_acceleration_prospective_v1.json"
REPORT = DATA / "reports" / "macro_relative_strength" / "DIRECT_CPI_ACCELERATION_PROSPECTIVE_CURRENT.md"
UTC = dt.timezone.utc


def atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _eligible_row(row: sqlite3.Row, config: Mapping[str, Any]) -> dict[str, Any] | None:
    known = parse_utc(row["causal_known_utc"])
    cohort_start = parse_utc(config.get("cohort_start_utc"))
    actual = finite(row["actual_value"])
    previous = finite(row["previous_value"])
    if known is None or cohort_start is None or known < cohort_start:
        return None
    if actual is None or previous is None or row["unit"] != config.get("eligible_unit"):
        return None
    if not bool(row["source_verified"]) or not bool(row["source_direct"]):
        return None
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except json.JSONDecodeError:
        return None
    if bool(payload.get("source_listing_bootstrap")) or not bool(payload.get("forward_signal_timely")):
        return None
    currencies = json.loads(row["currencies_json"] or "[]")
    if len(currencies) != 1:
        return None
    currency = str(currencies[0]).upper()
    allowed = config.get("eligible_series_by_currency") or {}
    if str(row["event_series_id"]) not in set(allowed.get(currency) or []):
        return None
    return {
        "currency": currency,
        "release_key": str(row["release_key"]),
        "source_event_id": str(row["source_event_id"]),
        "event_series_id": str(row["event_series_id"]),
        "reference_period": str(row["reference_period"]),
        "known_utc": iso(known),
        "actual_yoy": actual,
        "previous_yoy": previous,
        "acceleration_pct_points": actual - previous,
        "source_id": str(row["source_id"]),
        "source_url": str(row["source_url"]),
        "payload_sha256": str(row["payload_sha256"]),
    }


def load_prospective_cpi_events(
    path: Path,
    instruments: Sequence[str],
    config: Mapping[str, Any],
    observed: dt.datetime,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        """SELECT release_key,source_event_id,event_series_id,currencies_json,
                  reference_period,unit,actual_value,previous_value,causal_known_utc,
                  source_id,source_url,source_verified,source_direct,payload_sha256,payload_json
             FROM macro_release_latest
            WHERE actual_value IS NOT NULL AND previous_value IS NOT NULL
            ORDER BY causal_known_utc,release_key"""
    ).fetchall()
    db.close()
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = _eligible_row(row, config)
        if item is None:
            continue
        current = latest.get(item["currency"])
        if current is None or item["known_utc"] > current["known_utc"]:
            latest[item["currency"]] = item
    maximum_age = float(config.get("maximum_observation_age_sec") or 3888000)
    threshold = float(config.get("minimum_absolute_acceleration_differential_pct_points") or 0.25)
    events: list[dict[str, Any]] = []
    for pair in instruments:
        if "_" not in pair:
            continue
        base, quote = pair.split("_", 1)
        base_row, quote_row = latest.get(base), latest.get(quote)
        if base_row is None or quote_row is None:
            continue
        base_known, quote_known = parse_utc(base_row["known_utc"]), parse_utc(quote_row["known_utc"])
        if base_known is None or quote_known is None:
            continue
        signal_time = max(base_known, quote_known)
        if (observed - base_known).total_seconds() > maximum_age or (observed - quote_known).total_seconds() > maximum_age:
            continue
        differential = float(base_row["acceleration_pct_points"]) - float(quote_row["acceleration_pct_points"])
        if abs(differential) < threshold:
            continue
        driver = base_row if base_known >= quote_known else quote_row
        event_id = "direct_cpi_accel_event_" + stable_hash(
            (pair, base_row["release_key"], quote_row["release_key"])
        )[:32]
        events.append(
            {
                "event_id": event_id,
                "factor_episode_id": "direct_cpi_release_" + stable_hash(driver["source_event_id"])[:24],
                "pair": pair,
                "base_currency": base,
                "quote_currency": quote,
                "signal_utc": iso(signal_time),
                "driver_currency": driver["currency"],
                "base_observation": base_row,
                "quote_observation": quote_row,
                "base_acceleration": float(base_row["acceleration_pct_points"]),
                "quote_acceleration": float(quote_row["acceleration_pct_points"]),
                "acceleration_differential": differential,
                "predicted_side": "long" if differential > 0 else "short",
            }
        )
    return events


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
          forecast_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,event_id TEXT NOT NULL,
          factor_episode_id TEXT NOT NULL,pair TEXT NOT NULL,base_currency TEXT NOT NULL,
          quote_currency TEXT NOT NULL,signal_utc TEXT NOT NULL,issued_utc TEXT NOT NULL,
          entry_quote_time TEXT NOT NULL,entry_bid REAL NOT NULL,entry_ask REAL NOT NULL,
          entry_mid REAL NOT NULL,pip REAL NOT NULL,entry_spread_pips REAL NOT NULL,
          base_acceleration REAL NOT NULL,quote_acceleration REAL NOT NULL,
          acceleration_differential REAL NOT NULL,predicted_side TEXT NOT NULL,
          arm TEXT NOT NULL,primary_counterfactual INTEGER NOT NULL,
          technical_json TEXT NOT NULL,payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS path_samples (
          forecast_id TEXT NOT NULL,quote_time TEXT NOT NULL,directional_mid_pips REAL NOT NULL,
          executable_net_pips REAL NOT NULL,PRIMARY KEY(forecast_id,quote_time)
        );
        CREATE TABLE IF NOT EXISTS outcomes (
          outcome_id TEXT PRIMARY KEY,forecast_id TEXT NOT NULL UNIQUE,cohort_id TEXT NOT NULL,
          outcome_quote_time TEXT NOT NULL,horizon_sec INTEGER NOT NULL,elapsed_sec REAL NOT NULL,
          exact_horizon INTEGER NOT NULL,rule_after_cost_pips REAL NOT NULL,
          flipped_after_cost_pips REAL NOT NULL,max_favorable_pips REAL NOT NULL,
          max_adverse_pips REAL NOT NULL,path_samples INTEGER NOT NULL,payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_direct_cpi_factor ON forecasts(factor_episode_id,pair);
        CREATE TRIGGER IF NOT EXISTS direct_cpi_forecasts_no_update BEFORE UPDATE ON forecasts
          BEGIN SELECT RAISE(ABORT,'direct CPI forecasts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS direct_cpi_forecasts_no_delete BEFORE DELETE ON forecasts
          BEGIN SELECT RAISE(ABORT,'direct CPI forecasts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS direct_cpi_outcomes_no_update BEFORE UPDATE ON outcomes
          BEGIN SELECT RAISE(ABORT,'direct CPI outcomes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS direct_cpi_outcomes_no_delete BEFORE DELETE ON outcomes
          BEGIN SELECT RAISE(ABORT,'direct CPI outcomes are immutable'); END;
        """
    )
    return db


def issue_forecasts(
    db: sqlite3.Connection,
    events: Sequence[Mapping[str, Any]],
    quotes: Mapping[str, Mapping[str, Any]],
    observed: dt.datetime,
    config: Mapping[str, Any],
    opportunity_path: Path | None,
    opportunity_state_path: Path | None,
) -> int:
    maximum_spread = float(config.get("maximum_entry_spread_pips") or 3.0)
    technical_horizon = int(config.get("technical_context_horizon_sec") or 1800)
    technical_age = int(config.get("maximum_technical_age_sec") or 1800)
    cohort_id = str(config.get("contract_id"))
    eligible = []
    for event in events:
        quote = quotes.get(str(event["pair"]))
        signal = parse_utc(event["signal_utc"])
        quote_time = parse_utc(quote.get("time")) if quote else None
        if (
            quote is None or not quote.get("fresh") or signal is None or quote_time is None
            or quote_time < signal or float(quote["spread_pips"]) > maximum_spread
        ):
            continue
        eligible.append((event, quote))
    scores = {
        str(event["event_id"]): abs(float(event["acceleration_differential"]))
        / max(float(quote["spread_pips"]), 1e-9)
        for event, quote in eligible
    }
    best_by_factor: dict[str, str] = {}
    for event, _ in eligible:
        factor = str(event["factor_episode_id"])
        candidate = str(event["event_id"])
        if factor not in best_by_factor or scores[candidate] > scores[best_by_factor[factor]]:
            best_by_factor[factor] = candidate
    inserted = 0
    for event, quote in eligible:
        technical = technical_context(
            opportunity_path, opportunity_state_path, str(event["pair"]), observed,
            technical_horizon, technical_age,
        )
        arm = arm_for(str(event["predicted_side"]), technical)
        forecast_prefix = str(
            config.get("forecast_id_prefix") or "direct_cpi_accel_forecast"
        ).strip("_")
        forecast_id = forecast_prefix + "_" + stable_hash(
            (cohort_id, event["event_id"])
        )[:32]
        primary = int(best_by_factor[str(event["factor_episode_id"])] == str(event["event_id"]))
        payload = {
            **dict(event), "cohort_id": cohort_id, "issued_utc": iso(observed),
            "entry_quote": dict(quote), "arm": arm, "technical_context": technical,
            "primary_counterfactual": bool(primary), "research_only": True,
            "execution_eligible": False, "post_release_consensus_used": False,
        }
        db.execute(
            """INSERT OR IGNORE INTO forecasts VALUES
               (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                forecast_id,cohort_id,event["event_id"],event["factor_episode_id"],event["pair"],
                event["base_currency"],event["quote_currency"],event["signal_utc"],iso(observed),
                quote["time"],quote["bid"],quote["ask"],quote["mid"],quote["pip"],
                quote["spread_pips"],event["base_acceleration"],event["quote_acceleration"],
                event["acceleration_differential"],event["predicted_side"],arm,primary,
                json.dumps(technical,sort_keys=True),json.dumps(payload,sort_keys=True),
            ),
        )
        inserted += int(db.execute("SELECT changes()").fetchone()[0] > 0)
    db.commit()
    return inserted


def sample_and_mature(
    db: sqlite3.Connection,
    quotes: Mapping[str, Mapping[str, Any]],
    horizon_sec: int,
) -> tuple[int, int]:
    rows = db.execute(
        """SELECT f.forecast_id,f.cohort_id,f.pair,f.predicted_side,f.entry_quote_time,
                  f.entry_bid,f.entry_ask,f.entry_mid,f.pip
             FROM forecasts f LEFT JOIN outcomes o USING(forecast_id)
            WHERE o.forecast_id IS NULL ORDER BY f.issued_utc,f.forecast_id"""
    ).fetchall()
    sampled = matured = 0
    for row in rows:
        quote = quotes.get(str(row[2]))
        entry_time = parse_utc(row[4])
        if quote is None or not quote.get("fresh") or entry_time is None:
            continue
        quote_time = parse_utc(quote["time"])
        if quote_time is None:
            continue
        elapsed = (quote_time - entry_time).total_seconds()
        if elapsed < 0:
            continue
        pip = float(row[8])
        if row[3] == "long":
            directional_mid = (float(quote["mid"]) - float(row[7])) / pip
            net = (float(quote["bid"]) - float(row[6])) / pip
            flipped = (float(row[5]) - float(quote["ask"])) / pip
        else:
            directional_mid = (float(row[7]) - float(quote["mid"])) / pip
            net = (float(row[5]) - float(quote["ask"])) / pip
            flipped = (float(quote["bid"]) - float(row[6])) / pip
        db.execute("INSERT OR IGNORE INTO path_samples VALUES (?,?,?,?)", (row[0],quote["time"],directional_mid,net))
        sampled += int(db.execute("SELECT changes()").fetchone()[0] > 0)
        if elapsed < horizon_sec:
            continue
        values = [float(item[0]) for item in db.execute(
            "SELECT directional_mid_pips FROM path_samples WHERE forecast_id=?", (row[0],)
        )]
        exact = int(abs(elapsed - horizon_sec) <= 300)
        payload = {
            "forecast_id":row[0],"outcome_quote_time":quote["time"],"elapsed_sec":elapsed,
            "exact_horizon":bool(exact),"rule_after_cost_pips":net,
            "flipped_after_cost_pips":flipped,"max_favorable_pips":max(values),
            "max_adverse_pips":min(values),"research_only":True,"execution_eligible":False,
        }
        outcome_id = "direct_cpi_accel_outcome_" + stable_hash((row[0],quote["time"]))[:32]
        db.execute(
            "INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (outcome_id,row[0],row[1],quote["time"],horizon_sec,elapsed,exact,net,flipped,
             max(values),min(values),len(values),json.dumps(payload,sort_keys=True)),
        )
        matured += int(db.execute("SELECT changes()").fetchone()[0] > 0)
    db.commit()
    return sampled,matured


def summarize(db: sqlite3.Connection) -> dict[str, Any]:
    forecasts = int(db.execute("SELECT count(*) FROM forecasts").fetchone()[0])
    outcomes = int(db.execute("SELECT count(*) FROM outcomes").fetchone()[0])
    exact = int(db.execute("SELECT count(*) FROM outcomes WHERE exact_horizon=1").fetchone()[0])
    factors = int(db.execute("SELECT count(DISTINCT factor_episode_id) FROM forecasts").fetchone()[0])
    arms = []
    for row in db.execute(
        """SELECT f.arm,count(*),sum(o.outcome_id IS NOT NULL),
                  avg(CASE WHEN o.exact_horizon=1 THEN o.rule_after_cost_pips END),
                  avg(CASE WHEN o.exact_horizon=1 THEN o.rule_after_cost_pips>0 END)
             FROM forecasts f LEFT JOIN outcomes o USING(forecast_id)
            GROUP BY f.arm ORDER BY f.arm"""
    ):
        arms.append({"arm":row[0],"forecasts":int(row[1]),"matured":int(row[2] or 0),
                     "exact_mean_net_pips":row[3],"exact_win_rate":row[4],"proof_eligible":False})
    return {"forecasts":forecasts,"outcomes":outcomes,"exact_outcomes":exact,
            "independent_factor_episodes":factors,"arms":arms}


def run_once(
    macro_path: Path = MACRO_DB,
    quotes_path: Path = QUOTES,
    opportunity_path: Path | None = OPPORTUNITY_DB,
    opportunity_state_path: Path | None = OPPORTUNITY_STATE,
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
) -> dict[str, Any]:
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {"source":"provided_replay_time","trusted_for_prospective_evidence":True,"normalized":False}
    config = read_json(config_path)
    quotes = load_quotes(quotes_path, observed, float(config.get("maximum_quote_age_sec") or 180))
    events = load_prospective_cpi_events(macro_path, sorted(quotes), config, observed)
    db = open_database(database_path)
    trusted = bool(clock.get("trusted_for_prospective_evidence"))
    issued = issue_forecasts(db,events,quotes,observed,config,opportunity_path,opportunity_state_path) if trusted else 0
    sampled,matured = sample_and_mature(db,quotes,int(config.get("horizon_sec") or 604800)) if trusted else (0,0)
    summary = summarize(db)
    integrity = db.execute("PRAGMA quick_check").fetchone()[0]
    db.close()
    payload = {
        "schema_version":1,"generated_utc":iso(observed),
        "status":"ok" if trusted and integrity=="ok" else "blocked",
        "contract_id":config.get("contract_id"),"cohort_start_utc":config.get("cohort_start_utc"),
        "research_only":True,"execution_eligible":False,"can_place_orders":False,
        "can_authorize":False,"can_promote":False,"observation_clock":clock,
        "fresh_quote_count":sum(bool(row["fresh"]) for row in quotes.values()),
        "prospective_pair_events":len(events),"issued_this_cycle":issued,
        "sampled_this_cycle":sampled,"matured_this_cycle":matured,"summary":summary,
        "database_integrity":integrity,"supported_execution_decision":"no_trade",
        "policy":{"current_view_discovery_not_proof":True,"both_legs_post_cohort":True,
                  "direct_verified_timely_only":True,"post_release_consensus_used":False,
                  "technical_arms_separate":True},
    }
    atomic(output_path,json.dumps(payload,indent=2,sort_keys=True)+"\n")
    lines = [
        "# Prospective Direct-Official CPI Acceleration", "", f"Generated: `{payload['generated_utc']}`", "",
        "Research-only; no promotion, authorization, or execution path.", "",
        f"- Cohort start: **{payload['cohort_start_utc']}**",
        f"- Fresh priced pairs: **{payload['fresh_quote_count']}**",
        f"- Eligible prospective pair events: **{len(events)}**",
        f"- Forecasts / exact outcomes / factor episodes: **{summary['forecasts']} / {summary['exact_outcomes']} / {summary['independent_factor_episodes']}**",
        f"- New forecasts / maturities: **{issued} / {matured}**", "",
        "| Arm | Forecasts | Matured | Exact avg net | Exact win |", "|---|---:|---:|---:|---:|",
    ]
    for row in summary["arms"]:
        avg = "n/a" if row["exact_mean_net_pips"] is None else f"{row['exact_mean_net_pips']:.3f}"
        win = "n/a" if row["exact_win_rate"] is None else f"{row['exact_win_rate']:.1%}"
        lines.append(f"| {row['arm']} | {row['forecasts']} | {row['matured']} | {avg} | {win} |")
    lines += ["", "Only official observations first known after cohort start are eligible. A zero count is expected until both currency legs acquire qualifying releases.", ""]
    atomic(report_path,"\n".join(lines))
    return payload


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec",type=float,default=60)
    parser.add_argument("--duration-sec",type=float,default=604800)
    parser.add_argument("--once",action="store_true")
    args=parser.parse_args(); stop=time.monotonic()+args.duration_sec
    while True:
        run_once()
        if args.once or time.monotonic()>=stop:
            return 0
        time.sleep(min(args.interval_sec,max(0.0,stop-time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
