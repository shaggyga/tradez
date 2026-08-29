#!/usr/bin/env python3
"""Prospectively collect the frozen official-rate relative-strength hypothesis.

Only independently first-observed, prospective official two-year observations
on both currency legs can open a shadow forecast.  The macro-only, aligned,
conflicted, and technical-unavailable arms remain separate.  All related pairs
on one rate date share one factor episode.  This worker cannot trade, promote,
authorize, or alter Practice 007 policy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_local_news_sentiment import normalized_observation_time


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
RATE_DB = STATE / "official_daily_rate_context_v1.sqlite"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
OPPORTUNITY_DB = STATE / "executable_opportunity_prospective_v1.sqlite"
OPPORTUNITY_STATE = STATE / "executable_opportunity_prospective_v1.json"
CONFIG = ROOT / "config" / "rate_relative_strength_prospective_v1.json"
DATABASE = STATE / "rate_relative_strength_prospective_v1.sqlite"
OUTPUT = STATE / "rate_relative_strength_prospective_v1.json"
REPORT = DATA / "reports" / "rate_relative_strength" / "RATE_RELATIVE_STRENGTH_PROSPECTIVE_CURRENT.md"
UTC = dt.timezone.utc


def parse_utc(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def stable_hash(value: Any) -> str:
    material = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def load_quotes(path: Path, observed: dt.datetime, maximum_age: float) -> dict[str, dict[str, Any]]:
    output = {}
    for pair, raw in (read_json(path).get("quotes") or {}).items():
        if not isinstance(raw, Mapping):
            continue
        bid = finite(raw.get("bid"))
        ask = finite(raw.get("ask"))
        stamp = parse_utc(raw.get("time") or raw.get("quote_time_utc"))
        if bid is None or ask is None or bid <= 0 or ask <= bid or stamp is None:
            continue
        pip = finite(raw.get("pip")) or (0.01 if str(pair).endswith("_JPY") else 0.0001)
        age = (observed - stamp).total_seconds()
        output[str(pair)] = {
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "pip": pip,
            "spread_pips": (ask - bid) / pip,
            "time": iso(stamp),
            "fresh": -5 <= age <= maximum_age,
        }
    return output


def load_prospective_rate_events(
    path: Path,
    instruments: Sequence[str],
    comparison_group: str,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        """SELECT observation_id,cohort_id,currency,rate_date,rate_pct,
                  first_seen_utc,prospective_eligible,bootstrap_current_view,
                  source_id,provider,source_contract_json,version
             FROM daily_rate_observations
            WHERE json_extract(source_contract_json,'$.comparison_group')=?
            ORDER BY currency,rate_date,first_seen_utc,version""",
        (comparison_group,),
    ).fetchall()
    db.close()
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        rate = finite(row[4])
        first_seen = parse_utc(row[5])
        if rate is None or first_seen is None:
            continue
        key = (str(row[2]).upper(), str(row[3]))
        candidate = {
            "observation_id": str(row[0]),
            "cohort_id": str(row[1]),
            "currency": key[0],
            "rate_date": key[1],
            "rate_pct": rate,
            "first_seen_utc": iso(first_seen),
            "prospective_eligible": bool(row[6]),
            "bootstrap_current_view": bool(row[7]),
            "source_id": str(row[8]),
            "provider": str(row[9]),
        }
        current = latest.get(key)
        if current is None or candidate["first_seen_utc"] < current["first_seen_utc"]:
            latest[key] = candidate
    by_currency: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in latest.values():
        by_currency[row["currency"]].append(row)
    for currency, values in by_currency.items():
        values.sort(key=lambda row: row["rate_date"])
        previous = None
        for row in values:
            row["change_bps"] = (
                (float(row["rate_pct"]) - float(previous["rate_pct"])) * 100.0
                if previous is not None
                else None
            )
            previous = row
    by_key = {
        (currency, str(row["rate_date"])): row
        for currency, values in by_currency.items()
        for row in values
        if row.get("change_bps") is not None
        and row.get("prospective_eligible")
        and not row.get("bootstrap_current_view")
    }
    events = []
    for pair in instruments:
        base, quote = pair.split("_")
        dates = sorted(
            {date for currency, date in by_key if currency == base}
            & {date for currency, date in by_key if currency == quote}
        )
        for rate_date in dates:
            base_row = by_key[(base, rate_date)]
            quote_row = by_key[(quote, rate_date)]
            signal_time = max(
                parse_utc(base_row["first_seen_utc"]),
                parse_utc(quote_row["first_seen_utc"]),
            )
            differential = float(base_row["change_bps"]) - float(quote_row["change_bps"])
            if signal_time is None or not math.isfinite(differential):
                continue
            events.append(
                {
                    "event_id": "rate_relative_event_"
                    + stable_hash((pair, rate_date, base_row["observation_id"], quote_row["observation_id"]))[:32],
                    "factor_episode_id": "official_2y_rate_factor_" + rate_date,
                    "pair": pair,
                    "base_currency": base,
                    "quote_currency": quote,
                    "rate_date": rate_date,
                    "signal_utc": iso(signal_time),
                    "base_observation": base_row,
                    "quote_observation": quote_row,
                    "base_change_bps": float(base_row["change_bps"]),
                    "quote_change_bps": float(quote_row["change_bps"]),
                    "change_differential_bps": differential,
                    "predicted_side": "long" if differential > 0 else "short",
                }
            )
    return events


def technical_context(
    path: Path | None,
    state_path: Path | None,
    pair: str,
    decided: dt.datetime,
    horizon_sec: int,
    maximum_age_sec: int,
) -> dict[str, Any]:
    unavailable = {"state": "unavailable", "direction": 0}
    if path is None or state_path is None or not path.exists() or not state_path.exists():
        return unavailable
    state = read_json(state_path)
    cohort_id = str(state.get("cohort_id") or "")
    if not cohort_id:
        return unavailable
    try:
        db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
        row = db.execute(
            """SELECT forecast_id,cohort_id,issued_at_utc,predicted_direction,
                      predicted_clear_probability,predicted_magnitude_pips
                 FROM forecasts
                WHERE cohort_id=? AND instrument=? AND horizon_sec=?
                  AND issued_at_utc<=? AND issued_at_utc>=?
                ORDER BY issued_at_utc DESC,forecast_id DESC LIMIT 1""",
            (
                cohort_id,
                pair,
                horizon_sec,
                iso(decided),
                iso(decided - dt.timedelta(seconds=maximum_age_sec)),
            ),
        ).fetchone()
        db.close()
    except sqlite3.Error as exc:
        return {**unavailable, "state": "unavailable", "error": str(exc)}
    if row is None:
        return unavailable
    direction = int(math.copysign(1, float(row[3]))) if finite(row[3]) not in (None, 0) else 0
    return {
        "state": "available" if direction else "neutral",
        "forecast_id": str(row[0]),
        "cohort_id": str(row[1]),
        "issued_utc": str(row[2]),
        "direction": direction,
        "predicted_clear_probability": finite(row[4]),
        "predicted_magnitude_pips": finite(row[5]),
    }


def arm_for(macro_side: str, technical: Mapping[str, Any]) -> str:
    direction = int(technical.get("direction") or 0)
    if direction == 0:
        return "macro_only_technical_unavailable"
    macro_direction = 1 if macro_side == "long" else -1
    return (
        "macro_technical_aligned"
        if direction == macro_direction
        else "macro_technical_conflicted"
    )


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
          forecast_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,event_id TEXT NOT NULL,
          factor_episode_id TEXT NOT NULL,rate_date TEXT NOT NULL,pair TEXT NOT NULL,
          base_currency TEXT NOT NULL,quote_currency TEXT NOT NULL,signal_utc TEXT NOT NULL,
          issued_utc TEXT NOT NULL,entry_quote_time TEXT NOT NULL,entry_bid REAL NOT NULL,
          entry_ask REAL NOT NULL,entry_mid REAL NOT NULL,pip REAL NOT NULL,
          entry_spread_pips REAL NOT NULL,base_change_bps REAL NOT NULL,
          quote_change_bps REAL NOT NULL,change_differential_bps REAL NOT NULL,
          predicted_side TEXT NOT NULL,arm TEXT NOT NULL,primary_counterfactual INTEGER NOT NULL,
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
        CREATE INDEX IF NOT EXISTS ix_rate_relative_factor ON forecasts(factor_episode_id,pair);
        CREATE TRIGGER IF NOT EXISTS rate_relative_forecasts_no_update BEFORE UPDATE ON forecasts
          BEGIN SELECT RAISE(ABORT,'rate-relative forecasts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rate_relative_forecasts_no_delete BEFORE DELETE ON forecasts
          BEGIN SELECT RAISE(ABORT,'rate-relative forecasts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rate_relative_outcomes_no_update BEFORE UPDATE ON outcomes
          BEGIN SELECT RAISE(ABORT,'rate-relative outcomes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rate_relative_outcomes_no_delete BEFORE DELETE ON outcomes
          BEGIN SELECT RAISE(ABORT,'rate-relative outcomes are immutable'); END;
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
    threshold = float(config.get("minimum_absolute_change_differential_bps") or 5.0)
    maximum_age = float(config.get("maximum_signal_age_sec") or 86400)
    maximum_spread = float(config.get("maximum_entry_spread_pips") or 5.0)
    technical_horizon = int(config.get("technical_context_horizon_sec") or 1800)
    technical_age = int(config.get("maximum_technical_age_sec") or 1800)
    cohort_id = str(config.get("contract_id"))
    eligible = []
    for event in events:
        signal = parse_utc(event["signal_utc"])
        quote = quotes.get(str(event["pair"]))
        if signal is None or quote is None or not quote.get("fresh"):
            continue
        age = (observed - signal).total_seconds()
        quote_time = parse_utc(quote["time"])
        if (
            age < -5
            or age > maximum_age
            or quote_time is None
            or quote_time < signal
            or float(quote["spread_pips"]) > maximum_spread
            or abs(float(event["change_differential_bps"])) < threshold
        ):
            continue
        eligible.append((event, quote))
    scores = {
        str(event["event_id"]): abs(float(event["change_differential_bps"]))
        / max(float(quote["spread_pips"]), 1e-9)
        for event, quote in eligible
    }
    best_by_factor: dict[str, str] = {}
    for event, _ in eligible:
        factor = str(event["factor_episode_id"])
        current = best_by_factor.get(factor)
        if current is None or scores[str(event["event_id"])] > scores[current]:
            best_by_factor[factor] = str(event["event_id"])
    inserted = 0
    for event, quote in eligible:
        technical = technical_context(
            opportunity_path,
            opportunity_state_path,
            str(event["pair"]),
            observed,
            technical_horizon,
            technical_age,
        )
        arm = arm_for(str(event["predicted_side"]), technical)
        forecast_id = "rate_relative_forecast_" + stable_hash(
            (cohort_id, event["event_id"])
        )[:32]
        primary = int(best_by_factor[str(event["factor_episode_id"])] == str(event["event_id"]))
        payload = {
            **dict(event),
            "cohort_id": cohort_id,
            "issued_utc": iso(observed),
            "entry_quote": dict(quote),
            "arm": arm,
            "technical_context": technical,
            "primary_counterfactual": bool(primary),
            "research_only": True,
            "execution_eligible": False,
        }
        db.execute(
            """INSERT OR IGNORE INTO forecasts VALUES
               (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                forecast_id,cohort_id,event["event_id"],event["factor_episode_id"],
                event["rate_date"],event["pair"],event["base_currency"],
                event["quote_currency"],event["signal_utc"],iso(observed),quote["time"],
                quote["bid"],quote["ask"],quote["mid"],quote["pip"],
                quote["spread_pips"],event["base_change_bps"],event["quote_change_bps"],
                event["change_differential_bps"],event["predicted_side"],arm,primary,
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
        db.execute(
            "INSERT OR IGNORE INTO path_samples VALUES (?,?,?,?)",
            (row[0],quote["time"],directional_mid,net),
        )
        sampled += int(db.execute("SELECT changes()").fetchone()[0] > 0)
        if elapsed < horizon_sec:
            continue
        path = db.execute(
            "SELECT directional_mid_pips FROM path_samples WHERE forecast_id=?",
            (row[0],),
        ).fetchall()
        values = [float(item[0]) for item in path]
        exact = int(abs(elapsed - horizon_sec) <= 300)
        payload = {
            "forecast_id": row[0],
            "outcome_quote_time": quote["time"],
            "elapsed_sec": elapsed,
            "exact_horizon": bool(exact),
            "rule_after_cost_pips": net,
            "flipped_after_cost_pips": flipped,
            "max_favorable_pips": max(values),
            "max_adverse_pips": min(values),
            "research_only": True,
            "execution_eligible": False,
        }
        outcome_id = "rate_relative_outcome_" + stable_hash((row[0],quote["time"]))[:32]
        db.execute(
            "INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                outcome_id,row[0],row[1],quote["time"],horizon_sec,elapsed,exact,
                net,flipped,max(values),min(values),len(values),json.dumps(payload,sort_keys=True),
            ),
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
        arms.append(
            {"arm":row[0],"forecasts":int(row[1]),"matured":int(row[2] or 0),
             "exact_mean_net_pips":row[3],"exact_win_rate":row[4],"proof_eligible":False}
        )
    primary = db.execute(
        """SELECT count(*),sum(o.outcome_id IS NOT NULL),
                  avg(CASE WHEN o.exact_horizon=1 THEN o.rule_after_cost_pips END)
             FROM forecasts f LEFT JOIN outcomes o USING(forecast_id)
            WHERE f.primary_counterfactual=1"""
    ).fetchone()
    return {
        "forecasts":forecasts,"outcomes":outcomes,"exact_outcomes":exact,
        "independent_factor_episodes":factors,"arms":arms,
        "primary_counterfactual":{"forecasts":int(primary[0]),"matured":int(primary[1] or 0),
                                   "exact_mean_net_pips":primary[2],"proof_eligible":False},
    }


def run_once(
    rate_path: Path = RATE_DB,
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
        observed,clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed=observed.astimezone(UTC)
        clock={"source":"provided_replay_time","trusted_for_prospective_evidence":True,"normalized":False}
    config=read_json(config_path)
    quotes=load_quotes(quotes_path,observed,float(config.get("maximum_quote_age_sec") or 180))
    instruments=sorted(quotes)
    events=load_prospective_rate_events(
        rate_path,instruments,str(config.get("comparison_group") or "two_year_market_rate_context")
    )
    db=open_database(database_path)
    trusted=bool(clock.get("trusted_for_prospective_evidence"))
    issued=issue_forecasts(db,events,quotes,observed,config,opportunity_path,opportunity_state_path) if trusted else 0
    sampled,matured=sample_and_mature(db,quotes,int(config.get("horizon_sec") or 14400)) if trusted else (0,0)
    summary=summarize(db);integrity=db.execute("PRAGMA quick_check").fetchone()[0];db.close()
    payload={
        "schema_version":1,"generated_utc":iso(observed),"status":"ok" if trusted and integrity=="ok" else "blocked",
        "contract_id":config.get("contract_id"),"research_only":True,"execution_eligible":False,
        "can_place_orders":False,"can_authorize":False,"can_promote":False,
        "observation_clock":clock,"fresh_quote_count":sum(bool(row["fresh"]) for row in quotes.values()),
        "prospective_aligned_rate_events":len(events),"issued_this_cycle":issued,
        "sampled_this_cycle":sampled,"matured_this_cycle":matured,"summary":summary,
        "database_integrity":integrity,"supported_execution_decision":"no_trade",
        "policy":{"one_rate_date_factor_episode":True,"bootstrap_rows_ineligible":True,
                  "both_legs_must_be_prospective":True,"technical_arms_separate":True},
    }
    atomic(output_path,json.dumps(payload,indent=2,sort_keys=True)+"\n")
    lines=["# Prospective Official 2Y Relative Strength","",f"Generated: `{payload['generated_utc']}`","",
           "Research-only; no promotion, authorization, or execution path.","",
           f"- Fresh priced pairs: **{payload['fresh_quote_count']}**",
           f"- Prospective aligned rate events: **{len(events)}**",
           f"- Forecasts / exact outcomes / factor episodes: **{summary['forecasts']} / {summary['exact_outcomes']} / {summary['independent_factor_episodes']}**",
           f"- New forecasts / maturities: **{issued} / {matured}**","",
           "| Arm | Forecasts | Matured | Exact avg net | Exact win |","|---|---:|---:|---:|---:|"]
    for row in summary["arms"]:
        avg="n/a" if row["exact_mean_net_pips"] is None else f"{row['exact_mean_net_pips']:.3f}"
        win="n/a" if row["exact_win_rate"] is None else f"{row['exact_win_rate']:.1%}"
        lines.append(f"| {row['arm']} | {row['forecasts']} | {row['matured']} | {avg} | {win} |")
    lines += ["","All pairs sharing one official rate date remain one factor episode. The highest differential-to-spread pair is labeled as the primary counterfactual; it is not an order.",""]
    atomic(report_path,"\n".join(lines))
    return payload


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec",type=float,default=30)
    parser.add_argument("--duration-sec",type=float,default=604800)
    parser.add_argument("--once",action="store_true")
    args=parser.parse_args();stop=time.monotonic()+args.duration_sec
    while True:
        run_once()
        if args.once or time.monotonic()>=stop:return 0
        time.sleep(min(args.interval_sec,max(0.0,stop-time.monotonic())))


if __name__=="__main__":raise SystemExit(main())
