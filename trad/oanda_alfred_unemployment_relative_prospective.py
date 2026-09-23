#!/usr/bin/env python3
"""Collect a frozen ALFRED unemployment-change relative hypothesis in shadow.

The source is a point-in-time FRED/ALFRED publication adapter, not the
country's first-party release clock and not market consensus. Initial
current-view rows are excluded. A pair event is issued only after at least one
leg obtains a later prospective source observation in the exact frozen source
cohort. This worker cannot trade, authorize, promote, or alter Practice 007.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_direct_cpi_acceleration_prospective import (
    atomic,
    issue_forecasts,
    open_database,
    sample_and_mature,
    summarize,
)
from oanda_local_news_sentiment import normalized_observation_time
from oanda_rate_relative_strength_prospective import (
    iso,
    load_quotes,
    parse_utc,
    read_json,
    stable_hash,
)
from oanda_worker_heartbeat import WorkerHeartbeat


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
SOURCE_DB = STATE / "alfred_vintage_prospective_v1.sqlite"
SOURCE_STATE = STATE / "alfred_vintage_prospective_v1.json"
SOURCE_CONFIG = ROOT / "config" / "alfred_vintage_prospective_v1.json"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
OPPORTUNITY_DB = STATE / "executable_opportunity_prospective_v1.sqlite"
OPPORTUNITY_STATE = STATE / "executable_opportunity_prospective_v1.json"
CONFIG = ROOT / "config" / "alfred_unemployment_relative_prospective_v1.json"
DATABASE = STATE / "alfred_unemployment_relative_prospective_v1.sqlite"
OUTPUT = STATE / "alfred_unemployment_relative_prospective_v1.json"
HEARTBEAT = STATE / "alfred_unemployment_relative_prospective_heartbeat_v1.json"
REPORT = (
    DATA
    / "reports"
    / "macro_relative_strength"
    / "ALFRED_UNEMPLOYMENT_RELATIVE_PROSPECTIVE_CURRENT.md"
)
UTC = dt.timezone.utc


def series_contracts(path: Path) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("series_id")): dict(row)
        for row in read_json(path).get("series") or []
        if isinstance(row, Mapping) and row.get("series_id")
    }


def load_events(
    database_path: Path,
    state_path: Path,
    source_config_path: Path,
    instruments: Sequence[str],
    config: Mapping[str, Any],
    observed: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_state = read_json(state_path)
    observed_cohort = str((source_state.get("cohort") or {}).get("cohort_id") or "")
    required_cohort = str(config.get("source_cohort_id") or "")
    required_contract = str(config.get("source_contract_id") or "")
    observed_contract = str(
        (source_state.get("cohort") or {}).get("source_contract_id") or ""
    )
    source_status = {
        "required_source_cohort_id": required_cohort,
        "observed_source_cohort_id": observed_cohort,
        "required_source_contract_id": required_contract,
        "observed_source_contract_id": observed_contract,
        "source_cohort_match": bool(
            required_cohort
            and required_cohort == observed_cohort
            and required_contract == observed_contract
        ),
    }
    if not source_status["source_cohort_match"] or not database_path.exists():
        return [], source_status

    contracts = series_contracts(source_config_path)
    eligible_groups = set(config.get("eligible_frequency_groups") or [])
    eligible_series = {
        series_id: row
        for series_id, row in contracts.items()
        if row.get("cross_currency_frequency_group") in eligible_groups
    }
    db = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True, timeout=10
    )
    rows = db.execute(
        """SELECT series_id,currency,observation_date,value,first_seen_utc,
                  version,bootstrap_current_view,prospective_eligible
             FROM vintage_observations
            WHERE cohort_id=?
            ORDER BY series_id,observation_date,version,rowid""",
        (observed_cohort,),
    ).fetchall()
    db.close()
    latest: dict[tuple[str, str], tuple[Any, ...]] = {}
    for row in rows:
        if str(row[0]) in eligible_series:
            latest[(str(row[0]), str(row[2]))] = row

    panel: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in latest.values():
        series_id, currency = str(row[0]), str(row[1]).upper()
        group = str(eligible_series[series_id]["cross_currency_frequency_group"])
        known = parse_utc(row[4])
        try:
            value = float(row[3])
            reference_date = dt.date.fromisoformat(str(row[2]))
        except (TypeError, ValueError):
            continue
        if known is None:
            continue
        panel[currency][group].append(
            {
                "series_id": series_id,
                # Forecast envelopes are persisted as JSON by the shared
                # prospective worker.  Keep the point-in-time observation
                # date in its canonical wire form instead of leaking a
                # datetime.date object into that immutable payload.
                "reference_date": reference_date.isoformat(),
                "value": value,
                "first_seen_utc": iso(known),
                "bootstrap": bool(row[6]),
                "prospective": bool(row[7]),
            }
        )
    lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for currency, groups in panel.items():
        for group, values in groups.items():
            previous = None
            for row in sorted(values, key=lambda value: value["reference_date"]):
                row["change"] = (
                    row["value"] - previous["value"] if previous is not None else None
                )
                previous = row
                lookup[(currency, group, str(row["reference_date"]))] = row

    dates_by_group: dict[str, set[str]] = defaultdict(set)
    for _currency, group, date_text in lookup:
        dates_by_group[group].add(date_text)
    cohort_start = parse_utc(config.get("cohort_start_utc"))
    maximum_age = float(config.get("maximum_signal_age_sec") or 604800)
    threshold = float(
        config.get("minimum_absolute_change_differential_pct_points") or 0.0
    )
    events: list[dict[str, Any]] = []
    for pair in instruments:
        if "_" not in pair:
            continue
        base, quote = pair.split("_", 1)
        for group, dates in dates_by_group.items():
            for date_text in sorted(dates):
                base_row = lookup.get((base, group, date_text))
                quote_row = lookup.get((quote, group, date_text))
                if base_row is None or quote_row is None:
                    continue
                if base_row.get("change") is None or quote_row.get("change") is None:
                    continue
                if not (base_row["prospective"] or quote_row["prospective"]):
                    continue
                base_known = parse_utc(base_row["first_seen_utc"])
                quote_known = parse_utc(quote_row["first_seen_utc"])
                if base_known is None or quote_known is None:
                    continue
                signal_time = max(base_known, quote_known)
                if (
                    cohort_start is None
                    or signal_time < cohort_start
                    or (observed - signal_time).total_seconds() > maximum_age
                ):
                    continue
                if str(config.get("change_direction_policy") or "lower_is_stronger") == "higher_is_stronger":
                    differential = float(base_row["change"]) - float(quote_row["change"])
                else:
                    differential = float(quote_row["change"]) - float(base_row["change"])
                if abs(differential) < threshold or abs(differential) < 1e-12:
                    continue
                event_id = str(
                    config.get("event_id_prefix")
                    or "alfred_unemployment_change_event_"
                ) + stable_hash(
                    (pair, group, date_text, base_row["series_id"], quote_row["series_id"])
                )[:32]
                events.append(
                    {
                        "event_id": event_id,
                        "factor_episode_id": str(
                            config.get("factor_episode_prefix")
                            or "alfred_unemployment_poll_"
                        )
                        + stable_hash((group, iso(signal_time)))[:24],
                        "pair": pair,
                        "base_currency": base,
                        "quote_currency": quote,
                        "signal_utc": iso(signal_time),
                        "driver_currency": (
                            base if base_known >= quote_known else quote
                        ),
                        "base_observation": base_row,
                        "quote_observation": quote_row,
                        "base_acceleration": float(base_row["change"]),
                        "quote_acceleration": float(quote_row["change"]),
                        "acceleration_differential": differential,
                        "predicted_side": "long" if differential > 0 else "short",
                    }
                )
    source_status.update(
        {
            "panel_currency_count": len(panel),
            "eligible_series_count": len(eligible_series),
            "prospective_pair_events": len(events),
        }
    )
    return events, source_status


def run_once(
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
    source_database_path: Path = SOURCE_DB,
    source_state_path: Path = SOURCE_STATE,
    source_config_path: Path = SOURCE_CONFIG,
    quotes_path: Path = QUOTES,
) -> dict[str, Any]:
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {
            "source": "provided_replay_time",
            "trusted_for_prospective_evidence": True,
            "normalized": False,
        }
    config = read_json(config_path)
    quotes = load_quotes(
        quotes_path, observed, float(config.get("maximum_quote_age_sec") or 180)
    )
    events, source_status = load_events(
        source_database_path,
        source_state_path,
        source_config_path,
        sorted(quotes),
        config,
        observed,
    )
    db = open_database(database_path)
    trusted = bool(clock.get("trusted_for_prospective_evidence"))
    ready = bool(source_status.get("source_cohort_match"))
    issued = (
        issue_forecasts(
            db,
            events,
            quotes,
            observed,
            config,
            OPPORTUNITY_DB,
            OPPORTUNITY_STATE,
        )
        if trusted and ready
        else 0
    )
    sampled, matured = (
        sample_and_mature(db, quotes, int(config.get("horizon_sec") or 86400))
        if trusted and ready
        else (0, 0)
    )
    summary = summarize(db)
    integrity = db.execute("PRAGMA quick_check").fetchone()[0]
    db.close()
    payload = {
        "schema_version": 1,
        "generated_utc": iso(observed),
        "status": "ok" if trusted and ready and integrity == "ok" else "blocked",
        "contract_id": config.get("contract_id"),
        "cohort_start_utc": config.get("cohort_start_utc"),
        "source_state": source_status,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "observation_clock": clock,
        "fresh_quote_count": sum(bool(row["fresh"]) for row in quotes.values()),
        "prospective_pair_events": len(events),
        "issued_this_cycle": issued,
        "sampled_this_cycle": sampled,
        "matured_this_cycle": matured,
        "summary": summary,
        "database_integrity": integrity,
        "supported_execution_decision": "no_trade",
        "policy": {
            "initial_current_view_excluded": True,
            "at_least_one_leg_prospective": True,
            "market_consensus": False,
            "technical_arms_separate": True,
            "source_is_alfred_not_first_party_release_clock": True,
        },
    }
    atomic(output_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    lines = [
        str(
            config.get("report_title")
            or "# Prospective ALFRED Unemployment-Change Relative Strength"
        ),
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only; no promotion, authorization, or execution path.",
        "",
        f"- Source cohort match: **{source_status.get('source_cohort_match')}**",
        f"- Eligible source series / panel currencies: **{source_status.get('eligible_series_count', 0)} / {source_status.get('panel_currency_count', 0)}**",
        f"- Fresh priced pairs: **{payload['fresh_quote_count']}**",
        f"- Eligible prospective pair events: **{len(events)}**",
        f"- Forecasts / exact outcomes / factor episodes: **{summary['forecasts']} / {summary['exact_outcomes']} / {summary['independent_factor_episodes']}**",
        f"- New forecasts / maturities: **{issued} / {matured}**",
        "",
        "Initial current-view rows are excluded. Zero is expected until a later ALFRED observation appears; this is not a substitute for the faster first-party release feed or intraday rate repricing.",
        "",
    ]
    atomic(report_path, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec", type=float, default=60)
    parser.add_argument("--duration-sec", type=float, default=604800)
    parser.add_argument("--heartbeat-state", type=Path, default=HEARTBEAT)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec
    heartbeat = WorkerHeartbeat(
        args.heartbeat_state,
        worker="oanda_alfred_unemployment_relative_prospective",
        role="research_only_macro_relative_strength",
    ).start()
    cycle = 0
    try:
        while True:
            cycle += 1
            heartbeat.update(phase="running_cycle", cycle=cycle)
            payload = run_once()
            heartbeat.update(
                phase="observing",
                cycle=cycle,
                generated_utc=payload.get("generated_utc"),
                status=payload.get("status"),
                forecast_count=(payload.get("summary") or {}).get("forecasts", 0),
                matured_count=(payload.get("summary") or {}).get("exact_outcomes", 0),
            )
            if args.once or time.monotonic() >= stop:
                return 0
            time.sleep(min(args.interval_sec, max(0.0, stop - time.monotonic())))
    finally:
        heartbeat.close()


if __name__ == "__main__":
    raise SystemExit(main())
