#!/usr/bin/env python3
"""Test simple daily U.S. Treasury repricing rules against executable FX costs.

The Treasury XML feed is official but its historical rows are retrieved now,
not from an immutable vintage captured on each original date.  Accordingly,
this is a rejection/discovery harness only.  Each close observation becomes
usable at 00:00 UTC on the following calendar day, which is conservative for
ordinary same-day publication.  Any positive result must start a new forward
source cohort; this script cannot promote, authorize, or trade.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests

from oanda_cftc_positioning_historical_discovery import (
    DISCOVERY_END,
    HORIZONS,
    atomic_text,
    evaluate,
    fetch_daily_candles,
    iso,
    parse_utc,
    signed,
    summarise,
)
from oanda_news_feed_backtest import readonly_price_token


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
TREASURY_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
DEFAULT_ARCHIVE = DATA / "source_archives" / "us_treasury_yield_discovery_20260808"
DEFAULT_JSON = DATA / "reports" / "rates" / "US_TREASURY_YIELD_DISCOVERY_20260808.json"
DEFAULT_REPORT = DATA / "reports" / "rates" / "US_TREASURY_YIELD_DISCOVERY_20260808.md"
CREDS = ROOT / "creds"
PAIRS = ("EUR_USD", "GBP_USD", "AUD_USD", "NZD_USD", "USD_JPY", "USD_CHF", "USD_CAD")
RATE_HORIZONS = (1, 3)
ATOM = "http://www.w3.org/2005/Atom"
META = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
DATA_NS = "http://schemas.microsoft.com/ado/2007/08/dataservices"


def fetch_year(year: int) -> bytes:
    response = requests.get(
        TREASURY_URL,
        params={"data": "daily_treasury_yield_curve", "field_tdr_date_value": str(year)},
        headers={"Accept": "application/xml", "User-Agent": "forex-shadow-research/1.0"},
        timeout=45,
    )
    response.raise_for_status()
    return response.content


def parse_yield_xml(raw: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(raw)
    namespace = {"a": ATOM, "m": META, "d": DATA_NS}
    rows = []
    for entry in root.findall("a:entry", namespace):
        properties = entry.find("a:content/m:properties", namespace)
        if properties is None:
            continue
        values = {child.tag.split("}")[-1]: child.text for child in properties}
        try:
            date = parse_utc(values["NEW_DATE"])
            two_year = float(values["BC_2YEAR"])
            ten_year = float(values["BC_10YEAR"])
        except (KeyError, TypeError, ValueError):
            continue
        rows.append(
            {
                "yield_date": date.date().isoformat(),
                "feed_updated_utc": entry.findtext("a:updated", default="", namespaces=namespace),
                "two_year_pct": two_year,
                "ten_year_pct": ten_year,
                "curve_2s10s_bps": (ten_year - two_year) * 100.0,
            }
        )
    return sorted(rows, key=lambda row: row["yield_date"])


def yield_features(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    previous = None
    for row in rows:
        date = dt.date.fromisoformat(row["yield_date"])
        available = dt.datetime.combine(date + dt.timedelta(days=1), dt.time(), tzinfo=dt.timezone.utc)
        enriched = {
            **row,
            "currency": "USD",
            "report_date": row["yield_date"],
            "availability_utc": iso(available),
            "two_year_change_bps": None,
            "ten_year_change_bps": None,
            "front_end_relative_change_bps": None,
        }
        if previous is not None:
            two_change = (row["two_year_pct"] - previous["two_year_pct"]) * 100.0
            ten_change = (row["ten_year_pct"] - previous["ten_year_pct"]) * 100.0
            enriched["two_year_change_bps"] = two_change
            enriched["ten_year_change_bps"] = ten_change
            enriched["front_end_relative_change_bps"] = two_change - ten_change
        output.append(enriched)
        previous = row
    return output


def rate_direction(row: dict[str, Any], rule: str) -> int:
    if rule == "two_year_change_usd_trend":
        return signed(row.get("two_year_change_bps"), 2.0)
    if rule == "ten_year_change_usd_trend":
        return signed(row.get("ten_year_change_bps"), 3.0)
    if rule == "front_end_relative_usd_trend":
        return signed(row.get("front_end_relative_change_bps"), 2.0)
    raise KeyError(rule)


RULES = (
    "two_year_change_usd_trend",
    "ten_year_change_usd_trend",
    "front_end_relative_usd_trend",
)


def apply_cross_period_gate(summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    discovery = {
        (row["rule"], row["horizon_trading_days"]): row
        for row in summaries
        if row["split"] == "discovery"
    }
    for row in summaries:
        prior = discovery.get((row["rule"], row["horizon_trading_days"]))
        row["cross_period_stable"] = bool(
            row["split"] == "holdout"
            and prior
            and prior["mean_net_bps"] > 0.0
            and prior["mean_without_best_currency_bps"] is not None
            and prior["mean_without_best_currency_bps"] > 0.0
            and prior["mean_without_best_week_bps"] is not None
            and prior["mean_without_best_week_bps"] > 0.0
        )
        row["discovery_candidate"] = bool(
            row.get("split_local_candidate") and row["cross_period_stable"]
        )
    return summaries


def run(
    *,
    start_year: int = 2023,
    end_year: int = 2026,
    archive: Path = DEFAULT_ARCHIVE,
    output: Path = DEFAULT_JSON,
    report: Path = DEFAULT_REPORT,
    creds: Path = CREDS,
) -> dict[str, Any]:
    archive.mkdir(parents=True, exist_ok=True)
    raw_hashes = {}
    rate_rows = []
    errors = []
    for year in range(start_year, end_year + 1):
        try:
            raw = fetch_year(year)
            path = archive / f"treasury_daily_yield_curve_{year}.xml"
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            temporary.write_bytes(raw)
            os.replace(temporary, path)
            raw_hashes[str(year)] = hashlib.sha256(raw).hexdigest()
            rate_rows.extend(parse_yield_xml(raw))
        except Exception as exc:
            errors.append({"source": "US_TREASURY", "year": year, "error": f"{type(exc).__name__}: {exc}"})
    rate_rows = yield_features(sorted(rate_rows, key=lambda row: row["yield_date"]))
    token, base_url = readonly_price_token(creds)
    candles_by_pair = {}
    candle_hashes = {}
    start = dt.datetime(start_year, 1, 1, tzinfo=dt.timezone.utc)
    requested_end = dt.datetime(end_year, 12, 31, tzinfo=dt.timezone.utc)
    end = min(requested_end, dt.datetime.now(dt.timezone.utc))
    for instrument in PAIRS:
        try:
            candles = fetch_daily_candles(token, base_url, instrument, start, end)
            serial = [{**row, "time": iso(row["time"])} for row in candles]
            text = json.dumps(serial, indent=2, sort_keys=True)
            atomic_text(archive / f"oanda_{instrument}_D_{start_year}_{end_year}.json", text)
            candle_hashes[instrument] = hashlib.sha256(text.encode("utf-8")).hexdigest()
            candles_by_pair[instrument] = candles
        except Exception as exc:
            errors.append({"source": "OANDA", "instrument": instrument, "error": f"{type(exc).__name__}: {exc}"})
    outcomes = []
    for row in rate_rows:
        for rule in RULES:
            direction = rate_direction(row, rule)
            for instrument in PAIRS:
                candles = candles_by_pair.get(instrument) or []
                for horizon in RATE_HORIZONS:
                    outcome = evaluate(row, instrument, candles, rule, direction, horizon)
                    if outcome is not None:
                        outcome["concentration_key"] = instrument
                        outcomes.append(outcome)
    summaries = apply_cross_period_gate(
        summarise(outcomes, "discovery") + summarise(outcomes, "holdout")
    )
    candidates = [row for row in summaries if row["discovery_candidate"]]
    payload = {
        "schema_version": 1,
        "generated_utc": iso(dt.datetime.now(dt.timezone.utc)),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "proof_eligible": False,
        "evidence_class": "official_current_view_availability_counterfactual_discovery",
        "availability_contract": "next calendar day at 00:00 UTC",
        "limitations": [
            "historical Treasury XML was retrieved now and is not an immutable original-date vintage",
            "daily par yields are closing indications, not intraday OIS or policy-futures repricing",
            "all pairs share one USD factor per rate date; effective N is the date episode",
            "the 2026 split is retrospective discovery rather than prospective confirmation",
        ],
        "date_range": {"start_year": start_year, "end_year": end_year},
        "discovery_end": DISCOVERY_END.isoformat(),
        "source_hashes": raw_hashes,
        "candle_hashes": candle_hashes,
        "rate_rows": len(rate_rows),
        "outcome_rows": len(outcomes),
        "errors": errors,
        "summaries": summaries,
        "candidate_count": len(candidates),
        "supported_action": "freeze_new_prospective_shadow_cohort" if candidates else "reject_tested_rules",
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# U.S. Treasury Yield-to-FX Historical Discovery",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Official-source, availability-counterfactual research only. It cannot promote or execute.",
        "",
        f"- Daily yield rows: **{len(rate_rows)}**",
        f"- Executable bid/ask outcomes: **{len(outcomes)}**",
        f"- Fetch errors: **{len(errors)}**",
        f"- Frozen prospective candidates: **{len(candidates)}**",
        f"- Supported action: **{payload['supported_action']}**",
        "",
        "| Split | Rule | Hold | Raw N | Effective dates | Mean net bps | Win rate | p | BH q | Ex-best pair/currency bps | Ex-best date bps | Stable candidate |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in summaries:
        ex_currency = row["mean_without_best_currency_bps"]
        ex_week = row["mean_without_best_week_bps"]
        lines.append(
            f"| {row['split']} | {row['rule']} | {row['horizon_trading_days']}d | {row['raw_n']} | "
            f"{row['effective_week_episodes']} | {row['mean_net_bps']:.3f} | {row['after_cost_win_rate']:.1%} | "
            f"{row['sign_flip_p']:.4f} | {row['bh_q']:.4f} | "
            f"{'n/a' if ex_currency is None else f'{ex_currency:.3f}'} | "
            f"{'n/a' if ex_week is None else f'{ex_week:.3f}'} | "
            f"{'yes' if row['discovery_candidate'] else 'no'} |"
        )
    lines += [
        "",
        "This daily source does not satisfy the separate intraday rates/OIS contract. Any surviving rule still needs a new prospective source cohort.",
        "",
    ]
    atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=2023)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--output", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--creds", type=Path, default=CREDS)
    args = parser.parse_args()
    result = run(
        start_year=args.start_year,
        end_year=args.end_year,
        archive=args.archive,
        output=args.output,
        report=args.report,
        creds=args.creds,
    )
    print(json.dumps({
        "rate_rows": result["rate_rows"],
        "outcome_rows": result["outcome_rows"],
        "candidate_count": result["candidate_count"],
        "supported_action": result["supported_action"],
        "errors": result["errors"],
    }, sort_keys=True))
    return 0 if not result["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["RULES", "apply_cross_period_gate", "parse_yield_xml", "rate_direction", "yield_features"]
