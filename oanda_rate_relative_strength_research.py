#!/usr/bin/env python3
"""Discovery replay for relative official two-year rate changes across FX pairs.

Historical official rate curves in the local archive were initially collected
after their native dates, so this is an availability-counterfactual diagnostic,
not prospective evidence.  It applies a conservative configurable lag, uses
OANDA practice GET-only executable bid/ask candles, reports every predeclared
threshold/horizon, and retains the flipped direction as a negative control.
It cannot trade, promote, authorize, or change allocator policy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import statistics
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_news_feed_backtest import readonly_price_token


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
RATE_DB = STATE / "official_daily_rate_context_v1.sqlite"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
CREDS = ROOT / "creds"
CONFIG = ROOT / "config" / "rate_relative_strength_research_v1.json"
OUTPUT = DATA / "reports" / "rate_relative_strength" / "RATE_RELATIVE_STRENGTH_DISCOVERY_20260816.json"
REPORT = DATA / "reports" / "rate_relative_strength" / "RATE_RELATIVE_STRENGTH_DISCOVERY_20260816.md"
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


def instrument_universe(path: Path) -> list[str]:
    quotes = read_json(path).get("quotes") or {}
    return sorted(
        str(pair)
        for pair in quotes
        if isinstance(pair, str) and len(pair.split("_")) == 2
    )


def load_rate_history(
    path: Path,
    comparison_group: str,
    earliest: dt.date,
) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        return {}
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        """SELECT observation_id,currency,rate_date,rate_pct,first_seen_utc,
                  source_id,provider,source_contract_json,version
             FROM daily_rate_observations
            WHERE rate_date>=?
              AND json_extract(source_contract_json,'$.comparison_group')=?
            ORDER BY currency,rate_date,first_seen_utc,version""",
        (earliest.isoformat(), comparison_group),
    ).fetchall()
    db.close()
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        try:
            rate = float(row[3])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(rate):
            continue
        key = (str(row[1]).upper(), str(row[2]))
        latest[key] = {
            "observation_id": str(row[0]),
            "currency": key[0],
            "rate_date": key[1],
            "rate_pct": rate,
            "first_seen_utc": str(row[4]),
            "source_id": str(row[5]),
            "provider": str(row[6]),
        }
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in latest.values():
        result[row["currency"]].append(row)
    for currency, values in result.items():
        values.sort(key=lambda row: row["rate_date"])
        previous = None
        for row in values:
            row["change_bps"] = (
                (float(row["rate_pct"]) - float(previous["rate_pct"])) * 100.0
                if previous is not None
                else None
            )
            previous = row
    return dict(result)


def aligned_rate_events(
    histories: Mapping[str, Sequence[Mapping[str, Any]]],
    instruments: Sequence[str],
) -> list[dict[str, Any]]:
    by_currency_date = {
        (currency, str(row["rate_date"])): row
        for currency, rows in histories.items()
        for row in rows
        if row.get("change_bps") is not None
    }
    dates = sorted({key[1] for key in by_currency_date})
    events = []
    for pair in instruments:
        base, quote = pair.split("_")
        for date in dates:
            base_row = by_currency_date.get((base, date))
            quote_row = by_currency_date.get((quote, date))
            if base_row is None or quote_row is None:
                continue
            differential = float(base_row["change_bps"]) - float(quote_row["change_bps"])
            if not math.isfinite(differential) or abs(differential) < 1e-12:
                continue
            events.append(
                {
                    "pair": pair,
                    "base_currency": base,
                    "quote_currency": quote,
                    "rate_date": date,
                    "base_change_bps": float(base_row["change_bps"]),
                    "quote_change_bps": float(quote_row["change_bps"]),
                    "change_differential_bps": differential,
                    "predicted_side": "long" if differential > 0 else "short",
                    "base_source_id": base_row["source_id"],
                    "quote_source_id": quote_row["source_id"],
                }
            )
    return events


def fetch_candles(
    token: str,
    base_url: str,
    instrument: str,
    start: dt.datetime,
    end: dt.datetime,
) -> list[dict[str, Any]]:
    params = {
        "price": "BAM",
        "granularity": "H1",
        "from": iso(start).replace("+00:00", "Z"),
        "to": iso(end).replace("+00:00", "Z"),
        "smooth": "false",
    }
    request = urllib.request.Request(
        f"{base_url}/v3/instruments/{urllib.parse.quote(instrument)}/candles?{urllib.parse.urlencode(params)}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    rows = []
    for candle in payload.get("candles") or []:
        stamp = parse_utc(candle.get("time"))
        bid = candle.get("bid") or {}
        ask = candle.get("ask") or {}
        mid = candle.get("mid") or {}
        if stamp is None or not candle.get("complete", True):
            continue
        try:
            rows.append(
                {
                    "time": stamp,
                    "bid_o": float(bid["o"]),
                    "ask_o": float(ask["o"]),
                    "mid_o": float(mid["o"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return rows


def evaluate_event(
    event: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    lag_hours: int,
    horizons_hours: Sequence[int],
) -> list[dict[str, Any]]:
    date = dt.date.fromisoformat(str(event["rate_date"]))
    usable_after = dt.datetime.combine(date, dt.time(0), tzinfo=UTC) + dt.timedelta(
        hours=lag_hours
    )
    entry = next((row for row in candles if row["time"] >= usable_after), None)
    if entry is None or (entry["time"] - usable_after).total_seconds() > 72 * 3600:
        return []
    pair = str(event["pair"])
    pip = 0.01 if pair.endswith("_JPY") else 0.0001
    side = str(event["predicted_side"])
    entry_spread = (float(entry["ask_o"]) - float(entry["bid_o"])) / pip
    rows = []
    for horizon in horizons_hours:
        target_time = entry["time"] + dt.timedelta(hours=int(horizon))
        exit_row = next((row for row in candles if row["time"] >= target_time), None)
        if exit_row is None or (exit_row["time"] - target_time).total_seconds() > 3 * 3600:
            continue
        if side == "long":
            net = (float(exit_row["bid_o"]) - float(entry["ask_o"])) / pip
            flipped = (float(entry["bid_o"]) - float(exit_row["ask_o"])) / pip
        else:
            net = (float(entry["bid_o"]) - float(exit_row["ask_o"])) / pip
            flipped = (float(exit_row["bid_o"]) - float(entry["ask_o"])) / pip
        rows.append(
            {
                **dict(event),
                "horizon_hours": int(horizon),
                "usable_after_utc": iso(usable_after),
                "entry_time_utc": iso(entry["time"]),
                "exit_time_utc": iso(exit_row["time"]),
                "entry_spread_pips": entry_spread,
                "rule_after_cost_pips": net,
                "flipped_after_cost_pips": flipped,
                "direction_hit": net + entry_spread > 0,
                "research_only": True,
                "proof_eligible": False,
            }
        )
    return rows


def metrics(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    values = [float(row[field]) for row in rows]
    wins = [value > 0 for value in values]
    gross = sum(max(0.0, value) for value in values)
    loss = -sum(min(0.0, value) for value in values)
    pair_totals: dict[str, float] = defaultdict(float)
    date_totals: dict[str, float] = defaultdict(float)
    for row, value in zip(rows, values):
        pair_totals[str(row["pair"])] += value
        date_totals[str(row["rate_date"])] += value
    total = sum(values)
    return {
        "raw_n": len(values),
        "independent_rate_dates": len(date_totals),
        "pair_count": len(pair_totals),
        "win_rate": sum(wins) / len(wins) if wins else None,
        "average_net_pips": statistics.fmean(values) if values else None,
        "median_net_pips": statistics.median(values) if values else None,
        "total_net_pips": total,
        "profit_factor": gross / loss if loss > 0 else None,
        "minimum_net_pips": min(values) if values else None,
        "maximum_net_pips": max(values) if values else None,
        "best_pair_profit_share": (
            max(pair_totals.values()) / total if pair_totals and total > 0 else None
        ),
        "best_date_profit_share": (
            max(date_totals.values()) / total if date_totals and total > 0 else None
        ),
        "proof_eligible": False,
    }


def robustness(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Expose concentration, chronology, factor aggregation, and cost fragility."""

    rows = list(rows)
    if not rows:
        return {"state": "empty", "proof_eligible": False}
    pair_totals: dict[str, float] = defaultdict(float)
    date_totals: dict[str, float] = defaultdict(float)
    for row in rows:
        value = float(row["rule_after_cost_pips"])
        pair_totals[str(row["pair"])] += value
        date_totals[str(row["rate_date"])] += value
    best_pair = max(pair_totals, key=pair_totals.get)
    best_date = max(date_totals, key=date_totals.get)
    without_pair = [row for row in rows if row["pair"] != best_pair]
    without_date = [row for row in rows if row["rate_date"] != best_date]
    dates = sorted(date_totals)
    split_index = max(1, len(dates) // 2)
    early_dates = set(dates[:split_index])
    late_dates = set(dates[split_index:])

    def average(values: Sequence[float]) -> float | None:
        return statistics.fmean(values) if values else None

    factor_values = [
        statistics.fmean(
            float(row["rule_after_cost_pips"])
            for row in rows
            if row["rate_date"] == rate_date
        )
        for rate_date in dates
    ]
    stressed_half = [
        float(row["rule_after_cost_pips"]) - 0.5 * float(row["entry_spread_pips"])
        for row in rows
    ]
    stressed_full = [
        float(row["rule_after_cost_pips"]) - float(row["entry_spread_pips"])
        for row in rows
    ]
    currencies = sorted(
        {str(row["base_currency"]) for row in rows}
        | {str(row["quote_currency"]) for row in rows}
    )
    leave_currency_out = {}
    for currency in currencies:
        values = [
            float(row["rule_after_cost_pips"])
            for row in rows
            if row["base_currency"] != currency and row["quote_currency"] != currency
        ]
        leave_currency_out[currency] = average(values)
    return {
        "state": "available",
        "best_pair": best_pair,
        "best_pair_total_pips": pair_totals[best_pair],
        "mean_without_best_pair_pips": average(
            [float(row["rule_after_cost_pips"]) for row in without_pair]
        ),
        "best_date": best_date,
        "best_date_total_pips": date_totals[best_date],
        "mean_without_best_date_pips": average(
            [float(row["rule_after_cost_pips"]) for row in without_date]
        ),
        "early_date_count": len(early_dates),
        "late_date_count": len(late_dates),
        "early_mean_net_pips": average(
            [
                float(row["rule_after_cost_pips"])
                for row in rows
                if row["rate_date"] in early_dates
            ]
        ),
        "late_mean_net_pips": average(
            [
                float(row["rule_after_cost_pips"])
                for row in rows
                if row["rate_date"] in late_dates
            ]
        ),
        "date_factor_count": len(factor_values),
        "date_factor_mean_pips": average(factor_values),
        "date_factor_median_pips": statistics.median(factor_values),
        "date_factor_win_rate": sum(value > 0 for value in factor_values) / len(factor_values),
        "additional_half_spread_mean_pips": average(stressed_half),
        "additional_full_spread_mean_pips": average(stressed_full),
        "leave_one_currency_out_mean_pips": leave_currency_out,
        "worst_leave_one_currency_out_mean_pips": min(
            value for value in leave_currency_out.values() if value is not None
        ),
        "proof_eligible": False,
    }


def run(
    rate_path: Path = RATE_DB,
    quotes_path: Path = QUOTES,
    creds_path: Path = CREDS,
    config_path: Path = CONFIG,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    config = read_json(config_path)
    lookback = int(config.get("lookback_days") or 45)
    lag_hours = int(config.get("availability_lag_hours") or 36)
    horizons = [int(value) for value in config.get("horizons_hours") or [4, 24, 72]]
    thresholds = [float(value) for value in config.get("absolute_change_thresholds_bps") or [0, 2, 5]]
    histories = load_rate_history(
        rate_path,
        str(config.get("comparison_group") or "two_year_market_rate_context"),
        (dt.datetime.now(UTC) - dt.timedelta(days=lookback + 7)).date(),
    )
    instruments = instrument_universe(quotes_path)
    events = aligned_rate_events(histories, instruments)
    rows: list[dict[str, Any]] = []
    errors = []
    if events:
        token, base_url = readonly_price_token(creds_path)
        first_date = min(dt.date.fromisoformat(row["rate_date"]) for row in events)
        last_date = max(dt.date.fromisoformat(row["rate_date"]) for row in events)
        start = dt.datetime.combine(first_date, dt.time(0), tzinfo=UTC)
        end = min(
            dt.datetime.now(UTC),
            dt.datetime.combine(last_date, dt.time(0), tzinfo=UTC)
            + dt.timedelta(hours=lag_hours + max(horizons) + 96),
        )
        for pair in sorted({row["pair"] for row in events}):
            try:
                candles = fetch_candles(token, base_url, pair, start, end)
            except Exception as exc:
                errors.append({"pair": pair, "error": f"{type(exc).__name__}: {exc}"})
                continue
            for event in (row for row in events if row["pair"] == pair):
                rows.extend(evaluate_event(event, candles, lag_hours, horizons))
    summaries = []
    for threshold in thresholds:
        for horizon in horizons:
            selected = [
                row
                for row in rows
                if row["horizon_hours"] == horizon
                and abs(float(row["change_differential_bps"])) >= threshold
            ]
            summaries.append(
                {
                    "threshold_bps": threshold,
                    "horizon_hours": horizon,
                    "rule": metrics(selected, "rule_after_cost_pips"),
                    "flipped_negative_control": metrics(
                        selected, "flipped_after_cost_pips"
                    ),
                    "robustness": robustness(selected),
                }
            )
    payload = {
        "schema_version": 1,
        "generated_utc": iso(dt.datetime.now(UTC)),
        "contract_id": config.get("contract_id"),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "evidence_class": config.get("evidence_class"),
        "proof_eligible": False,
        "comparison_group": config.get("comparison_group"),
        "availability_lag_hours": lag_hours,
        "currencies": sorted(histories),
        "instrument_count": len(instruments),
        "aligned_rate_pair_dates": len(events),
        "pair_horizon_rows": len(rows),
        "errors": errors,
        "summaries": summaries,
        "details": rows,
        "limitations": [
            "official histories were bootstrap-collected after native dates",
            "exact historical publication availability is not proven",
            "multiple pairs on one date share currency factors",
            "thresholds are a declared discovery sweep and are not selection-adjusted proof",
            "no result may promote or execute from this replay",
        ],
        "supported_execution_decision": "no_trade",
    }
    atomic(output_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    lines = [
        "# Official 2Y Rate Relative-Strength Discovery",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Historical availability-counterfactual discovery only. No promotion or execution path.",
        "",
        f"- Comparable currencies: **{len(payload['currencies'])}** ({', '.join(payload['currencies'])})",
        f"- Aligned pair/date inputs: **{len(events)}**",
        f"- Pair/horizon outcomes: **{len(rows)}**",
        f"- GET-only candle errors: **{len(errors)}**",
        f"- Conservative availability lag: **{lag_hours} hours**",
        "",
        "| Threshold | Horizon | Raw N | Dates | Pairs | Win | Avg net | Median | PF | Flipped avg |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        rule = row["rule"]
        flip = row["flipped_negative_control"]
        if rule["raw_n"]:
            lines.append(
                f"| {row['threshold_bps']:.1f} bps | {row['horizon_hours']}h | {rule['raw_n']} | "
                f"{rule['independent_rate_dates']} | {rule['pair_count']} | {rule['win_rate']:.1%} | "
                f"{rule['average_net_pips']:.3f} | {rule['median_net_pips']:.3f} | "
                f"{rule['profit_factor']:.3f} | {flip['average_net_pips']:.3f} |"
            )
    positive = [
        row
        for row in summaries
        if row["rule"]["average_net_pips"] is not None
        and row["rule"]["average_net_pips"] > 0
    ]
    if positive:
        lines += ["", "## Positive point estimates: robustness", ""]
        for row in positive:
            check = row["robustness"]
            lines += [
                f"### {row['threshold_bps']:.1f} bps / {row['horizon_hours']}h",
                "",
                f"- Best pair: **{check['best_pair']}** ({check['best_pair_total_pips']:.1f} pips); mean after removing it: **{check['mean_without_best_pair_pips']:.3f}**.",
                f"- Best date: **{check['best_date']}** ({check['best_date_total_pips']:.1f} pips); mean after removing it: **{check['mean_without_best_date_pips']:.3f}**.",
                f"- Chronological early/late mean: **{check['early_mean_net_pips']:.3f} / {check['late_mean_net_pips']:.3f}**.",
                f"- One-date-factor mean/median/win rate: **{check['date_factor_mean_pips']:.3f} / {check['date_factor_median_pips']:.3f} / {check['date_factor_win_rate']:.1%}** over **{check['date_factor_count']}** dates.",
                f"- Extra 0.5x/1.0x entry-spread stress mean: **{check['additional_half_spread_mean_pips']:.3f} / {check['additional_full_spread_mean_pips']:.3f}**.",
                f"- Worst leave-one-currency-out mean: **{check['worst_leave_one_currency_out_mean_pips']:.3f}**.",
                "",
            ]
    lines += [
        "",
        "The economic hypothesis is fixed: the currency with the larger same-date two-year",
        "rate increase strengthens relative to the other leg. Correlated pair rows are not",
        "independent evidence; the `Dates` column is the more honest first census.",
        "",
    ]
    atomic(report_path, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=Path, default=RATE_DB)
    parser.add_argument("--quotes", type=Path, default=QUOTES)
    parser.add_argument("--creds", type=Path, default=CREDS)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.rates, args.quotes, args.creds, args.config, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
