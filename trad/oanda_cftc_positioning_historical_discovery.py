#!/usr/bin/env python3
"""Conservative historical discovery audit for CFTC currency positioning.

This script is deliberately not a proof or promotion adapter.  Current CFTC
history can contain later corrections and the exact historical publication
clock is not reconstructed here.  To avoid using Tuesday report data before
its ordinary Friday publication, every row becomes usable only at 00:00 UTC
seven calendar days after the report date.  That conservative lag makes the
audit useful for rejecting weak weekly ideas while keeping any positive result
ineligible until it is reproduced prospectively by the forward collector.

Market outcomes use OANDA practice GET-only daily bid/ask candles.  No broker
write endpoint is imported or called.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import random
import statistics
import urllib.parse
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

import requests

from oanda_cftc_positioning_shadow import API_URL, CONTRACTS, FIELDS, normalized_position
from oanda_news_feed_backtest import readonly_price_token


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_ARCHIVE = DATA / "source_archives" / "cftc_discovery_20260808"
DEFAULT_JSON = DATA / "reports" / "cftc_positioning" / "CFTC_POSITIONING_DISCOVERY_20260808.json"
DEFAULT_REPORT = DATA / "reports" / "cftc_positioning" / "CFTC_POSITIONING_DISCOVERY_20260808.md"
CREDS = ROOT / "creds"
PAIR_MAP = {
    "AUD": "AUD_USD",
    "CAD": "USD_CAD",
    "CHF": "USD_CHF",
    "EUR": "EUR_USD",
    "GBP": "GBP_USD",
    "JPY": "USD_JPY",
    "MXN": "USD_MXN",
    "NZD": "NZD_USD",
}
HORIZONS = (1, 3, 5)
DISCOVERY_END = dt.date(2025, 12, 31)
HOLDOUT_START = dt.date(2026, 1, 1)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def parse_utc(value: Any) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat()


def conservative_availability(report_date: dt.datetime) -> dt.datetime:
    """Return a deliberately lagged knowledge time for discovery replay.

    TFF positions are ordinarily as-of Tuesday and published Friday.  Tuesday
    00:00 UTC one week after the report date is later than the normal release
    and avoids pretending that we reconstructed every historical exception.
    """
    day = report_date.astimezone(dt.timezone.utc).date() + dt.timedelta(days=7)
    return dt.datetime.combine(day, dt.time(), tzinfo=dt.timezone.utc)


def fetch_json(url: str, params: dict[str, str], token: str | None = None) -> Any:
    headers = {"Accept": "application/json", "User-Agent": "forex-shadow-research/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = requests.get(url, params=params, headers=headers, timeout=45)
    response.raise_for_status()
    return response.json()


def fetch_cftc_history(
    currency: str,
    market_name: str,
    start: dt.date,
    end: dt.date,
) -> list[dict[str, Any]]:
    params = {
        "$select": ",".join(FIELDS),
        "$where": (
            f"market_and_exchange_names='{market_name}' AND "
            f"report_date_as_yyyy_mm_dd >= '{start.isoformat()}T00:00:00.000' AND "
            f"report_date_as_yyyy_mm_dd <= '{end.isoformat()}T23:59:59.999'"
        ),
        "$order": "report_date_as_yyyy_mm_dd asc",
        "$limit": "1000",
    }
    rows = fetch_json(API_URL, params)
    if not isinstance(rows, list):
        raise ValueError(f"unexpected CFTC payload for {currency}")
    return [row for row in rows if isinstance(row, dict)]


def fetch_daily_candles(
    token: str,
    base_url: str,
    instrument: str,
    start: dt.datetime,
    end: dt.datetime,
) -> list[dict[str, Any]]:
    url = f"{base_url}/v3/instruments/{urllib.parse.quote(instrument)}/candles"
    raw = fetch_json(
        url,
        {
            "price": "BAM",
            "granularity": "D",
            "from": iso(start).replace("+00:00", "Z"),
            "to": iso(end).replace("+00:00", "Z"),
            "smooth": "false",
            "dailyAlignment": "0",
            "alignmentTimezone": "UTC",
        },
        token=token,
    )
    output = []
    for candle in raw.get("candles") or []:
        if not candle.get("complete", True):
            continue
        bid, ask, mid = candle.get("bid") or {}, candle.get("ask") or {}, candle.get("mid") or {}
        try:
            output.append(
                {
                    "time": parse_utc(candle["time"]),
                    "bid_o": float(bid["o"]),
                    "bid_h": float(bid["h"]),
                    "bid_l": float(bid["l"]),
                    "ask_o": float(ask["o"]),
                    "ask_h": float(ask["h"]),
                    "ask_l": float(ask["l"]),
                    "mid_o": float(mid["o"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return output


def positioning_rows(currency: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    previous: dict[str, Any] | None = None
    for row in rows:
        report = parse_utc(row["report_date_as_yyyy_mm_dd"])
        lev = normalized_position(row, "lev_money")
        asset = normalized_position(row, "asset_mgr")
        dealer = normalized_position(row, "dealer")
        previous_lev = normalized_position(previous, "lev_money") if previous else math.nan
        previous_asset = normalized_position(previous, "asset_mgr") if previous else math.nan
        output.append(
            {
                "currency": currency,
                "report_date": report.date().isoformat(),
                "availability_utc": iso(conservative_availability(report)),
                "leveraged_net_pct_oi": lev,
                "leveraged_change": lev - previous_lev if math.isfinite(previous_lev) else None,
                "asset_net_pct_oi": asset,
                "asset_change": asset - previous_asset if math.isfinite(previous_asset) else None,
                "dealer_net_pct_oi": dealer,
            }
        )
        previous = row
    return output


def signed(value: Any, threshold: float = 0.0) -> int:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(number) or abs(number) < threshold:
        return 0
    return 1 if number > 0 else -1


Rule = Callable[[dict[str, Any]], int]
RULES: dict[str, Rule] = {
    "leveraged_change_trend": lambda row: signed(row.get("leveraged_change"), 0.005),
    "asset_change_trend": lambda row: signed(row.get("asset_change"), 0.005),
    "leveraged_level_trend": lambda row: signed(row.get("leveraged_net_pct_oi"), 0.05),
    "leveraged_extreme_contrarian": lambda row: -signed(row.get("leveraged_net_pct_oi"), 0.20),
}


def evaluate(
    position: dict[str, Any],
    instrument: str,
    candles: list[dict[str, Any]],
    rule_name: str,
    direction: int,
    horizon_days: int,
) -> dict[str, Any] | None:
    if direction not in {-1, 1}:
        return None
    available = parse_utc(position["availability_utc"])
    entry_index = next(
        (index for index, row in enumerate(candles) if row["time"] >= available), None
    )
    if entry_index is None or entry_index + horizon_days >= len(candles):
        return None
    entry, exit_row = candles[entry_index], candles[entry_index + horizon_days]
    base, quote = instrument.split("_")
    currency = str(position["currency"])
    orientation = 1 if base == currency else -1 if quote == currency else 0
    if orientation == 0:
        return None
    pair_direction = direction * orientation
    pip = 0.01 if quote == "JPY" else 0.0001
    if pair_direction > 0:
        gross = (exit_row["mid_o"] - entry["mid_o"]) / pip
        net = (exit_row["bid_o"] - entry["ask_o"]) / pip
        favorable = max(row["bid_h"] - entry["ask_o"] for row in candles[entry_index : entry_index + horizon_days + 1]) / pip
        adverse = min(row["bid_l"] - entry["ask_o"] for row in candles[entry_index : entry_index + horizon_days + 1]) / pip
    else:
        gross = (entry["mid_o"] - exit_row["mid_o"]) / pip
        net = (entry["bid_o"] - exit_row["ask_o"]) / pip
        favorable = max(entry["bid_o"] - row["ask_l"] for row in candles[entry_index : entry_index + horizon_days + 1]) / pip
        adverse = min(entry["bid_o"] - row["ask_h"] for row in candles[entry_index : entry_index + horizon_days + 1]) / pip
    gross_bps = gross * pip / entry["mid_o"] * 10000.0
    net_bps = net * pip / entry["mid_o"] * 10000.0
    return {
        **position,
        "instrument": instrument,
        "rule": rule_name,
        "currency_direction": direction,
        "pair_direction": pair_direction,
        "horizon_trading_days": horizon_days,
        "entry_utc": iso(entry["time"]),
        "exit_utc": iso(exit_row["time"]),
        "entry_spread_pips": (entry["ask_o"] - entry["bid_o"]) / pip,
        "gross_pips": gross,
        "net_pips": net,
        "gross_bps": gross_bps,
        "net_bps": net_bps,
        "win_after_cost": net > 0.0,
        "mfe_after_cost_pips": favorable,
        "mae_after_cost_pips": adverse,
    }


def sign_flip_pvalue(episode_values: list[float], *, seed: int, draws: int = 4000) -> float:
    if not episode_values:
        return 1.0
    observed = statistics.fmean(episode_values)
    rng = random.Random(seed)
    at_least = 0
    for _ in range(draws):
        value = statistics.fmean(item * (1 if rng.random() >= 0.5 else -1) for item in episode_values)
        at_least += int(value >= observed)
    return (at_least + 1) / (draws + 1)


def bh_qvalues(pvalues: list[float]) -> list[float]:
    if not pvalues:
        return []
    ordered = sorted(enumerate(pvalues), key=lambda item: item[1])
    output = [1.0] * len(pvalues)
    running = 1.0
    total = len(pvalues)
    for rank_from_end in range(total - 1, -1, -1):
        index, value = ordered[rank_from_end]
        rank = rank_from_end + 1
        running = min(running, value * total / rank)
        output[index] = min(1.0, running)
    return output


def summarise(rows: list[dict[str, Any]], split: str) -> list[dict[str, Any]]:
    selected = [
        row
        for row in rows
        if (row["report_date"] <= DISCOVERY_END.isoformat()) == (split == "discovery")
    ]
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in selected:
        groups[(row["rule"], int(row["horizon_trading_days"]))].append(row)
    summaries = []
    for (rule, horizon), group in sorted(groups.items()):
        episodes: dict[str, list[float]] = defaultdict(list)
        currencies: dict[str, list[float]] = defaultdict(list)
        for row in group:
            episodes[row["report_date"]].append(float(row["net_bps"]))
            concentration_key = str(row.get("concentration_key") or row["currency"])
            currencies[concentration_key].append(float(row["net_bps"]))
        episode_values = [statistics.fmean(values) for values in episodes.values()]
        currency_means = {key: statistics.fmean(value) for key, value in currencies.items()}
        best_currency = max(currency_means, key=currency_means.get)
        best_episode = max(episodes, key=lambda key: statistics.fmean(episodes[key]))
        without_best_currency = [
            row
            for row in group
            if str(row.get("concentration_key") or row["currency"]) != best_currency
        ]
        without_best_episode = [row for row in group if row["report_date"] != best_episode]
        summaries.append(
            {
                "split": split,
                "rule": rule,
                "horizon_trading_days": horizon,
                "raw_n": len(group),
                "effective_week_episodes": len(episodes),
                "currency_count": len(currencies),
                "mean_net_bps": statistics.fmean(row["net_bps"] for row in group),
                "median_net_bps": statistics.median(row["net_bps"] for row in group),
                "mean_pair_level_net_pips": statistics.fmean(row["net_pips"] for row in group),
                "after_cost_win_rate": statistics.fmean(row["win_after_cost"] for row in group),
                "mean_entry_spread_pips": statistics.fmean(row["entry_spread_pips"] for row in group),
                "episode_mean_net_bps": statistics.fmean(episode_values),
                "sign_flip_p": sign_flip_pvalue(
                    episode_values, seed=int(stable_hash((split, rule, horizon))[:8], 16)
                ),
                "best_currency": best_currency,
                "best_currency_mean_net_bps": currency_means[best_currency],
                "mean_without_best_currency_bps": (
                    statistics.fmean(row["net_bps"] for row in without_best_currency)
                    if without_best_currency
                    else None
                ),
                "best_week": best_episode,
                "mean_without_best_week_bps": (
                    statistics.fmean(row["net_bps"] for row in without_best_episode)
                    if without_best_episode
                    else None
                ),
            }
        )
    qvalues = bh_qvalues([row["sign_flip_p"] for row in summaries])
    for row, qvalue in zip(summaries, qvalues):
        row["bh_q"] = qvalue
        row["split_local_candidate"] = bool(
            split == "holdout"
            and row["effective_week_episodes"] >= 20
            and row["mean_net_bps"] > 0.0
            and qvalue <= 0.10
            and row["mean_without_best_currency_bps"] is not None
            and row["mean_without_best_currency_bps"] > 0.0
            and row["mean_without_best_week_bps"] is not None
            and row["mean_without_best_week_bps"] > 0.0
        )
    return summaries


def run(
    *,
    start: dt.date,
    end: dt.date,
    archive: Path = DEFAULT_ARCHIVE,
    output: Path = DEFAULT_JSON,
    report: Path = DEFAULT_REPORT,
    creds: Path = CREDS,
) -> dict[str, Any]:
    token, base_url = readonly_price_token(creds)
    archive.mkdir(parents=True, exist_ok=True)
    positions: list[dict[str, Any]] = []
    raw_hashes: dict[str, str] = {}
    candle_hashes: dict[str, str] = {}
    candles_by_pair: dict[str, list[dict[str, Any]]] = {}
    errors = []
    for currency, market in CONTRACTS.items():
        try:
            raw = fetch_cftc_history(currency, market, start, end)
            raw_text = json.dumps(raw, indent=2, sort_keys=True)
            atomic_text(archive / f"cftc_{currency}_{start}_{end}.json", raw_text)
            raw_hashes[currency] = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
            positions.extend(positioning_rows(currency, raw))
        except Exception as exc:  # keep per-source failure visible
            errors.append({"source": "CFTC", "currency": currency, "error": f"{type(exc).__name__}: {exc}"})
    price_start = dt.datetime.combine(start, dt.time(), tzinfo=dt.timezone.utc)
    requested_price_end = dt.datetime.combine(
        end + dt.timedelta(days=15), dt.time(), tzinfo=dt.timezone.utc
    )
    # OANDA rejects a candle range whose end lies in the future.
    price_end = min(requested_price_end, dt.datetime.now(dt.timezone.utc))
    for currency, instrument in PAIR_MAP.items():
        try:
            candles = fetch_daily_candles(token, base_url, instrument, price_start, price_end)
            serial = [
                {**row, "time": iso(row["time"])} for row in candles
            ]
            raw_text = json.dumps(serial, indent=2, sort_keys=True)
            atomic_text(archive / f"oanda_{instrument}_D_{start}_{end}.json", raw_text)
            candle_hashes[instrument] = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
            candles_by_pair[instrument] = candles
        except Exception as exc:
            errors.append({"source": "OANDA", "instrument": instrument, "error": f"{type(exc).__name__}: {exc}"})
    outcomes = []
    for position in positions:
        currency = position["currency"]
        instrument = PAIR_MAP[currency]
        candles = candles_by_pair.get(instrument) or []
        for rule_name, rule in RULES.items():
            direction = rule(position)
            for horizon in HORIZONS:
                row = evaluate(position, instrument, candles, rule_name, direction, horizon)
                if row is not None:
                    outcomes.append(row)
    summaries = summarise(outcomes, "discovery") + summarise(outcomes, "holdout")
    discovery_by_cell = {
        (row["rule"], row["horizon_trading_days"]): row
        for row in summaries
        if row["split"] == "discovery"
    }
    for row in summaries:
        prior = discovery_by_cell.get((row["rule"], row["horizon_trading_days"]))
        row["cross_period_stable"] = bool(
            row["split"] == "holdout"
            and prior is not None
            and prior["mean_net_bps"] > 0.0
            and prior["mean_without_best_currency_bps"] is not None
            and prior["mean_without_best_currency_bps"] > 0.0
            and prior["mean_without_best_week_bps"] is not None
            and prior["mean_without_best_week_bps"] > 0.0
        )
        row["discovery_candidate"] = bool(
            row["split_local_candidate"] and row["cross_period_stable"]
        )
    candidates = [row for row in summaries if row["discovery_candidate"]]
    payload = {
        "schema_version": 1,
        "generated_utc": iso(dt.datetime.now(dt.timezone.utc)),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "environment": "practice GET-only candles plus official CFTC public API",
        "proof_eligible": False,
        "evidence_class": "knowledge-time-lagged_historical_discovery",
        "availability_contract": "report date plus seven calendar days at 00:00 UTC",
        "limitations": [
            "historical CFTC records may include later corrections",
            "exact historical release exceptions are not reconstructed",
            "the seven-day availability lag is conservative but synthetic",
            "the 2026 split remains retrospective and is not prospective confirmation",
            "multiple currencies in one week share a market episode; effective N is weekly",
        ],
        "date_range": {"start": start.isoformat(), "end": end.isoformat()},
        "discovery_end": DISCOVERY_END.isoformat(),
        "holdout_start": HOLDOUT_START.isoformat(),
        "source_hashes": raw_hashes,
        "candle_hashes": candle_hashes,
        "position_rows": len(positions),
        "outcome_rows": len(outcomes),
        "errors": errors,
        "summaries": summaries,
        "candidate_count": len(candidates),
        "supported_action": "freeze_new_prospective_shadow_cohort" if candidates else "reject_tested_rules",
        "orders_placed": 0,
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# CFTC Positioning Historical Discovery",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Availability-counterfactual discovery only. It cannot promote or execute.",
        "",
        f"- CFTC position rows: **{len(positions)}**",
        f"- Executable bid/ask outcomes: **{len(outcomes)}**",
        f"- Fetch errors: **{len(errors)}**",
        f"- Frozen prospective candidates: **{len(candidates)}**",
        f"- Supported action: **{payload['supported_action']}**",
        "",
        "| Split | Rule | Hold | Raw N | Effective weeks | Mean net bps | Win rate | p | BH q | Ex-best currency bps | Ex-best week bps | Candidate |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in summaries:
        ex_currency = row["mean_without_best_currency_bps"]
        ex_week = row["mean_without_best_week_bps"]
        ex_currency_text = "n/a" if ex_currency is None else f"{ex_currency:.3f}"
        ex_week_text = "n/a" if ex_week is None else f"{ex_week:.3f}"
        lines.append(
            f"| {row['split']} | {row['rule']} | {row['horizon_trading_days']}d | {row['raw_n']} | "
            f"{row['effective_week_episodes']} | {row['mean_net_bps']:.3f} | "
            f"{row['after_cost_win_rate']:.1%} | {row['sign_flip_p']:.4f} | {row['bh_q']:.4f} | "
            f"{ex_currency_text} | {ex_week_text} | "
            f"{'yes' if row['discovery_candidate'] else 'no'} |"
        )
    lines += [
        "",
        "Any positive cell above is only a discovery result. A candidate must start a new immutable prospective cohort from the next unseen CFTC report.",
        "",
    ]
    atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2024, 1, 1))
    parser.add_argument("--end", type=dt.date.fromisoformat, default=dt.date(2026, 8, 4))
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--output", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--creds", type=Path, default=CREDS)
    args = parser.parse_args()
    result = run(
        start=args.start,
        end=args.end,
        archive=args.archive,
        output=args.output,
        report=args.report,
        creds=args.creds,
    )
    print(canonical_json({
        "position_rows": result["position_rows"],
        "outcome_rows": result["outcome_rows"],
        "candidate_count": result["candidate_count"],
        "errors": result["errors"],
        "supported_action": result["supported_action"],
    }))
    return 0 if not result["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "RULES",
    "bh_qvalues",
    "conservative_availability",
    "evaluate",
    "positioning_rows",
    "sign_flip_pvalue",
    "summarise",
]
