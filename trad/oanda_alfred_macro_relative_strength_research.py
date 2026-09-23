#!/usr/bin/env python3
"""Discovery replay for relative macro state across the priced FX universe.

The input is the current-view FRED/ALFRED bootstrap panel, not a historical
record of exact release-time vintages.  A conservative family-specific lag is
therefore imposed before an observation may affect a price outcome.  Results
are availability-counterfactual diagnostics only and can never promote,
authorize, or trade.
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
ALFRED_DB = STATE / "alfred_vintage_prospective_v1.sqlite"
ALFRED_STATE = STATE / "alfred_vintage_prospective_v1.json"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
CREDS = ROOT / "creds"
CONFIG = ROOT / "config" / "alfred_macro_relative_strength_research_v1.json"
OUTPUT = DATA / "reports" / "macro_relative_strength" / "ALFRED_MACRO_RELATIVE_STRENGTH_DISCOVERY_20260816.json"
REPORT = DATA / "reports" / "macro_relative_strength" / "ALFRED_MACRO_RELATIVE_STRENGTH_DISCOVERY_20260816.md"
UTC = dt.timezone.utc


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def parse_utc(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        result = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def instrument_universe(path: Path) -> list[str]:
    return sorted(
        pair
        for pair in (read_json(path).get("quotes") or {})
        if isinstance(pair, str) and len(pair.split("_")) == 2
    )


def series_contract(config_path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json(config_path)
    return {
        str(row.get("series_id")): dict(row)
        for row in payload.get("series") or []
        if row.get("series_id")
    }


def load_panel(
    database_path: Path,
    state_path: Path,
    alfred_config_path: Path,
) -> tuple[dict[str, dict[str, list[dict[str, Any]]]], dict[str, Any]]:
    """Load only the current immutable cohort and comparable frequency groups."""

    state = read_json(state_path)
    cohort = str((state.get("cohort") or {}).get("cohort_id") or "")
    contracts = series_contract(alfred_config_path)
    if not cohort or not database_path.exists():
        return {}, {"cohort_id": cohort, "reason": "missing_cohort_or_database"}
    db = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True, timeout=10
    )
    db.execute("PRAGMA query_only=ON")
    rows = db.execute(
        """SELECT series_id,currency,observation_date,value,bootstrap_current_view,
                  prospective_eligible,first_seen_utc
             FROM vintage_observations
            WHERE cohort_id=?
            ORDER BY series_id,observation_date,version,rowid""",
        (cohort,),
    ).fetchall()
    db.close()
    latest: dict[tuple[str, str], tuple[Any, ...]] = {}
    for row in rows:
        latest[(str(row[0]), str(row[2]))] = row
    raw: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    excluded_context = set()
    for row in latest.values():
        series_id, currency, date_text, value = str(row[0]), str(row[1]), str(row[2]), row[3]
        contract = contracts.get(series_id) or {}
        group = str(contract.get("cross_currency_frequency_group") or "")
        if contract.get("comparison_eligible") is False or group == "annual_context_only":
            excluded_context.add(currency)
            continue
        if group not in {
            "monthly_cpi_yoy",
            "quarterly_cpi_yoy",
            "monthly_unemployment_rate",
            "quarterly_unemployment_rate",
            "monthly_short_term_interest_rate",
        }:
            # CPIAUCSL is a monthly index. Convert it below to a causal-shape
            # annual rate so it can share the monthly comparison group.
            if currency == "USD" and series_id == "CPIAUCSL":
                group = "monthly_cpi_index_to_yoy"
            else:
                continue
        try:
            number = float(value)
            date = dt.date.fromisoformat(date_text)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(number):
            continue
        raw[currency][group].append(
            {
                "series_id": series_id,
                "reference_date": date,
                "source_value": number,
                "bootstrap": bool(row[4]),
                "prospective": bool(row[5]),
                "first_seen_utc": str(row[6]),
            }
        )
    # Convert the US index to a twelve-month rate. All other selected FRED
    # series already contain the provider's reported annual growth rate.
    converted = []
    us_rows = sorted(raw.get("USD", {}).pop("monthly_cpi_index_to_yoy", []), key=lambda x: x["reference_date"])
    by_date = {row["reference_date"]: row for row in us_rows}
    for row in us_rows:
        prior_date = dt.date(row["reference_date"].year - 1, row["reference_date"].month, 1)
        prior = by_date.get(prior_date)
        if prior is None or float(prior["source_value"]) == 0:
            continue
        converted.append(
            {
                **row,
                "source_value": 100.0 * (float(row["source_value"]) / float(prior["source_value"]) - 1.0),
                "derived_from_index": True,
            }
        )
    if converted:
        raw["USD"]["monthly_cpi_yoy"] = converted
    panel: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for currency, groups in raw.items():
        panel[currency] = {}
        for group, values in groups.items():
            ordered = sorted(values, key=lambda x: x["reference_date"])
            previous = None
            for value in ordered:
                value["macro_value"] = float(value["source_value"])
                value["macro_acceleration"] = (
                    float(value["macro_value"]) - float(previous["macro_value"])
                    if previous is not None
                    else None
                )
                previous = value
            panel[currency][group] = ordered
    return panel, {
        "cohort_id": cohort,
        "input_currency_count": len(panel),
        "excluded_annual_context_currencies": sorted(excluded_context),
        "all_rows_bootstrap": all(
            row["bootstrap"]
            for groups in panel.values()
            for values in groups.values()
            for row in values
        ),
    }


def aligned_events(
    panel: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    instruments: Sequence[str],
    lag_days: Mapping[str, Any],
    availability_policy: str = "reference_date_plus_lag",
) -> list[dict[str, Any]]:
    lookup = {
        (currency, group, str(row["reference_date"])): row
        for currency, groups in panel.items()
        for group, rows in groups.items()
        for row in rows
    }
    dates_by_group: dict[str, set[str]] = defaultdict(set)
    for _currency, group, date_text in lookup:
        dates_by_group[group].add(date_text)
    events = []
    for pair in instruments:
        base, quote = pair.split("_")
        for group, dates in dates_by_group.items():
            for date_text in sorted(dates):
                base_row = lookup.get((base, group, date_text))
                quote_row = lookup.get((quote, group, date_text))
                if base_row is None or quote_row is None:
                    continue
                reference_date = dt.date.fromisoformat(date_text)
                if availability_policy == "max_first_seen_utc":
                    base_available = parse_utc(base_row.get("first_seen_utc"))
                    quote_available = parse_utc(quote_row.get("first_seen_utc"))
                    if base_available is None or quote_available is None:
                        continue
                    # A relative-value signal cannot exist until both legs'
                    # initial releases were known.  The point-in-time snapshot
                    # uses a conservative next-provider-day timestamp because
                    # ALFRED's vintage field has civil-date, not intraday,
                    # precision.
                    available = max(base_available, quote_available)
                else:
                    available = dt.datetime.combine(
                        reference_date, dt.time(12), tzinfo=UTC
                    ) + dt.timedelta(days=int(lag_days.get(group) or 45))
                is_unemployment = "unemployment" in group
                is_short_rate = "short_term_interest_rate" in group or "short_rate" in group
                rule_fields = (
                    (
                        "unemployment_level_differential",
                        "macro_value",
                    ),
                    (
                        "unemployment_change_differential",
                        "macro_acceleration",
                    ),
                ) if is_unemployment else (
                    (
                        "short_rate_level_differential",
                        "macro_value",
                    ),
                    (
                        "short_rate_change_differential",
                        "macro_acceleration",
                    ),
                ) if is_short_rate else (
                    ("cpi_level_differential", "macro_value"),
                    ("cpi_acceleration_differential", "macro_acceleration"),
                )
                for rule, field in rule_fields:
                    if base_row.get(field) is None or quote_row.get(field) is None:
                        continue
                    # Higher CPI and higher/less-declining short rates are the
                    # tightening/relative-carry hypotheses used by discovery.
                    # For unemployment, lower is stronger, so only that family
                    # reverses the signed differential. Positive always means
                    # long base currency.
                    signal = (
                        float(quote_row[field]) - float(base_row[field])
                        if is_unemployment
                        else float(base_row[field]) - float(quote_row[field])
                    )
                    if not math.isfinite(signal) or abs(signal) < 1e-12:
                        continue
                    events.append(
                        {
                            "pair": pair,
                            "base_currency": base,
                            "quote_currency": quote,
                            "frequency_group": group,
                            "reference_date": date_text,
                            "available_utc": available,
                            "signal_rule": rule,
                            "signal_value": signal,
                            "predicted_side": "long" if signal > 0 else "short",
                            "factor_episode_id": (
                                f"{'unemployment' if is_unemployment else 'short_rate' if is_short_rate else 'cpi'}:"
                                f"{group}:{date_text}"
                            ),
                        }
                    )
    return events


def fetch_candle_chunk(
    token: str,
    base_url: str,
    pair: str,
    start: dt.datetime,
    end: dt.datetime,
    granularity: str,
) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode(
        {
            "price": "BAM",
            "granularity": granularity,
            "from": iso(start).replace("+00:00", "Z"),
            "to": iso(end).replace("+00:00", "Z"),
            "smooth": "false",
        }
    )
    request = urllib.request.Request(
        f"{base_url}/v3/instruments/{urllib.parse.quote(pair)}/candles?{query}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    output = []
    for row in payload.get("candles") or []:
        stamp = parse_utc(row.get("time"))
        try:
            output.append(
                {
                    "time": stamp,
                    "bid_o": float((row.get("bid") or {})["o"]),
                    "ask_o": float((row.get("ask") or {})["o"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return [row for row in output if row["time"] is not None and row.get("complete", True)]


def fetch_candles(
    token: str,
    base_url: str,
    pair: str,
    start: dt.datetime,
    end: dt.datetime,
    granularity: str,
) -> list[dict[str, Any]]:
    """Fetch bounded chunks so OANDA's maximum-candle limit cannot truncate a pair."""

    chunk_days = 600 if granularity.upper() == "H4" else 180
    cursor = start
    rows: dict[str, dict[str, Any]] = {}
    while cursor < end:
        boundary = min(end, cursor + dt.timedelta(days=chunk_days))
        for row in fetch_candle_chunk(
            token, base_url, pair, cursor, boundary, granularity
        ):
            rows[iso(row["time"])] = row
        cursor = boundary
    return [rows[key] for key in sorted(rows)]


def spread_bucket(spread_pips: float, buckets: Mapping[str, Any]) -> str:
    if spread_pips <= float(buckets.get("liquid") or 3.0):
        return "liquid"
    if spread_pips <= float(buckets.get("moderate") or 10.0):
        return "moderate"
    return "wide"


def evaluate(
    event: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    horizons: Sequence[int],
    spread_buckets: Mapping[str, Any] | None = None,
    technical_lookback_bars: int = 6,
) -> list[dict[str, Any]]:
    available = event["available_utc"]
    entry_index = next(
        (index for index, row in enumerate(candles) if row["time"] >= available),
        None,
    )
    entry = candles[entry_index] if entry_index is not None else None
    if entry is None or (entry["time"] - available).total_seconds() > 72 * 3600:
        return []
    pair = str(event["pair"])
    pip = 0.01 if pair.endswith("_JPY") else 0.0001
    spread = (float(entry["ask_o"]) - float(entry["bid_o"])) / pip
    technical_direction = 0
    technical_return_pips = None
    lookback = max(1, int(technical_lookback_bars))
    if entry_index is not None and entry_index >= lookback:
        prior = candles[entry_index - lookback]
        entry_mid = (float(entry["bid_o"]) + float(entry["ask_o"])) / 2.0
        prior_mid = (float(prior["bid_o"]) + float(prior["ask_o"])) / 2.0
        technical_return_pips = (entry_mid - prior_mid) / pip
        technical_direction = (technical_return_pips > 0) - (technical_return_pips < 0)
    macro_direction = 1 if event["predicted_side"] == "long" else -1
    technical_arm = (
        "macro_only_technical_unavailable"
        if technical_direction == 0
        else "macro_technical_aligned"
        if technical_direction == macro_direction
        else "macro_technical_conflicted"
    )
    output = []
    for horizon in horizons:
        target = entry["time"] + dt.timedelta(hours=int(horizon))
        exit_row = next((row for row in candles if row["time"] >= target), None)
        if exit_row is None or (exit_row["time"] - target).total_seconds() > 8 * 3600:
            continue
        if event["predicted_side"] == "long":
            net = (float(exit_row["bid_o"]) - float(entry["ask_o"])) / pip
            flipped = (float(entry["bid_o"]) - float(exit_row["ask_o"])) / pip
        else:
            net = (float(entry["bid_o"]) - float(exit_row["ask_o"])) / pip
            flipped = (float(exit_row["bid_o"]) - float(entry["ask_o"])) / pip
        output.append(
            {
                **{k: (iso(v) if isinstance(v, dt.datetime) else v) for k, v in event.items()},
                "horizon_hours": int(horizon),
                "entry_utc": iso(entry["time"]),
                "exit_utc": iso(exit_row["time"]),
                "entry_spread_pips": spread,
                "liquidity_bucket": spread_bucket(spread, spread_buckets or {}),
                "technical_lookback_bars": lookback,
                "technical_return_pips": technical_return_pips,
                "technical_direction": technical_direction,
                "technical_arm": technical_arm,
                "rule_after_cost_pips": net,
                "flipped_after_cost_pips": flipped,
                "proof_eligible": False,
            }
        )
    return output


def statistics_for(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    values = [float(row[field]) for row in rows]
    factors: dict[str, list[float]] = defaultdict(list)
    pairs: dict[str, list[float]] = defaultdict(list)
    for row, value in zip(rows, values):
        factors[str(row["factor_episode_id"])].append(value)
        pairs[str(row["pair"])].append(value)
    factor_means = [statistics.fmean(part) for part in factors.values()]
    factor_mean = statistics.fmean(factor_means) if factor_means else None
    factor_std = statistics.stdev(factor_means) if len(factor_means) >= 2 else None
    factor_se = (
        factor_std / math.sqrt(len(factor_means))
        if factor_std is not None
        else None
    )
    factor_z = (
        factor_mean / factor_se
        if factor_mean is not None and factor_se not in (None, 0.0)
        else None
    )
    gross = sum(max(0.0, value) for value in values)
    losses = -sum(min(0.0, value) for value in values)
    best_pair = max(pairs, key=lambda key: sum(pairs[key])) if pairs else None
    without_best = [
        float(row[field]) for row in rows if str(row["pair"]) != best_pair
    ]
    factor_keys = sorted(factors)
    best_factor = (
        max(factor_keys, key=lambda key: statistics.fmean(factors[key]))
        if factor_keys
        else None
    )
    factor_without_best = [
        statistics.fmean(factors[key]) for key in factor_keys if key != best_factor
    ]
    split = max(1, len(factor_keys) // 2)
    early_factor_values = [statistics.fmean(factors[key]) for key in factor_keys[:split]]
    late_factor_values = [statistics.fmean(factors[key]) for key in factor_keys[split:]]
    currencies = sorted(
        {str(row.get("base_currency") or "") for row in rows}
        | {str(row.get("quote_currency") or "") for row in rows}
    )
    leave_one_currency_out: dict[str, float | None] = {}
    for currency in currencies:
        remaining: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            if currency in {row.get("base_currency"), row.get("quote_currency")}:
                continue
            remaining[str(row["factor_episode_id"])].append(float(row[field]))
        remaining_means = [statistics.fmean(part) for part in remaining.values()]
        leave_one_currency_out[currency] = (
            statistics.fmean(remaining_means) if remaining_means else None
        )
    stressed = [
        float(row[field]) - float(row["entry_spread_pips"])
        for row in rows
    ]
    return {
        "raw_n": len(values),
        "factor_episode_n": len(factor_means),
        "pair_n": len(pairs),
        "win_rate": sum(value > 0 for value in values) / len(values) if values else None,
        "average_net_pips": statistics.fmean(values) if values else None,
        "median_net_pips": statistics.median(values) if values else None,
        "profit_factor": gross / losses if losses else None,
        "factor_mean_pips": factor_mean,
        "factor_win_rate": sum(value > 0 for value in factor_means) / len(factor_means) if factor_means else None,
        "factor_lcb_normal_95_pips": (
            factor_mean - 1.96 * factor_se
            if factor_mean is not None and factor_se is not None
            else None
        ),
        "one_sided_normal_p_value": (
            1.0 - statistics.NormalDist().cdf(factor_z)
            if factor_z is not None
            else None
        ),
        "best_pair": best_pair,
        "mean_without_best_pair_pips": statistics.fmean(without_best) if without_best else None,
        "best_factor_episode": best_factor,
        "factor_mean_without_best_episode_pips": (
            statistics.fmean(factor_without_best) if factor_without_best else None
        ),
        "early_factor_mean_pips": (
            statistics.fmean(early_factor_values) if early_factor_values else None
        ),
        "late_factor_mean_pips": (
            statistics.fmean(late_factor_values) if late_factor_values else None
        ),
        "leave_one_currency_out_factor_mean_pips": leave_one_currency_out,
        "worst_leave_one_currency_out_factor_mean_pips": (
            min(value for value in leave_one_currency_out.values() if value is not None)
            if any(value is not None for value in leave_one_currency_out.values())
            else None
        ),
        "additional_full_spread_mean_pips": statistics.fmean(stressed) if stressed else None,
        "proof_eligible": False,
    }


def apply_benjamini_hochberg(summaries: Sequence[dict[str, Any]]) -> None:
    """Attach discovery q-values while retaining every declared comparison."""

    indexed = [
        (index, float(row["rule"]["one_sided_normal_p_value"]))
        for index, row in enumerate(summaries)
        if row["rule"].get("one_sided_normal_p_value") is not None
    ]
    ordered = sorted(indexed, key=lambda item: item[1])
    adjusted: dict[int, float] = {}
    running = 1.0
    total = len(ordered)
    for rank, (index, p_value) in reversed(list(enumerate(ordered, start=1))):
        running = min(running, p_value * total / rank)
        adjusted[index] = min(1.0, running)
    for index, row in enumerate(summaries):
        row["rule"]["discovery_bh_q_value"] = adjusted.get(index)
        row["rule"]["multiplicity_family_size"] = total
        row["rule"]["survives_bh_5pct"] = bool(
            adjusted.get(index) is not None and adjusted[index] <= 0.05
        )


def run(
    database_path: Path = ALFRED_DB,
    state_path: Path = ALFRED_STATE,
    alfred_config_path: Path = ROOT / "config" / "alfred_vintage_prospective_v1.json",
    quote_path: Path = QUOTES,
    creds_path: Path = CREDS,
    config_path: Path = CONFIG,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    config = read_json(config_path)
    panel, input_state = load_panel(database_path, state_path, alfred_config_path)
    instruments = instrument_universe(quote_path)
    availability_policy = str(
        config.get("availability_policy") or "reference_date_plus_lag"
    )
    events = aligned_events(
        panel,
        instruments,
        config.get("availability_lag_days") or {},
        availability_policy,
    )
    minimum_start = parse_utc(config.get("minimum_price_start_utc")) or dt.datetime(2023, 1, 1, tzinfo=UTC)
    horizons = [int(value) for value in config.get("horizons_hours") or [24, 72, 168]]
    granularity = str(config.get("price_granularity") or "H4")
    rows: list[dict[str, Any]] = []
    errors = []
    if events:
        token, base_url = readonly_price_token(creds_path)
        for pair in sorted({str(row["pair"]) for row in events}):
            pair_events = [row for row in events if row["pair"] == pair and row["available_utc"] >= minimum_start]
            if not pair_events:
                continue
            start = min(row["available_utc"] for row in pair_events) - dt.timedelta(days=4)
            end = min(dt.datetime.now(UTC), max(row["available_utc"] for row in pair_events) + dt.timedelta(hours=max(horizons) + 96))
            try:
                candles = fetch_candles(token, base_url, pair, start, end, granularity)
            except Exception as exc:
                errors.append({"pair": pair, "error": f"{type(exc).__name__}: {exc}"})
                continue
            for event in pair_events:
                rows.extend(
                    evaluate(
                        event,
                        candles,
                        horizons,
                        config.get("spread_buckets_pips") or {},
                        int(config.get("technical_confirmation_lookback_bars") or 6),
                    )
                )
    summaries = []
    for rule in config.get("signal_rules") or []:
        for threshold in config.get("absolute_signal_thresholds") or []:
            for horizon in horizons:
                for bucket in ("all", "liquid", "moderate", "wide"):
                    selected = [
                        row for row in rows
                        if row["signal_rule"] == rule
                        and row["horizon_hours"] == horizon
                        and abs(float(row["signal_value"])) >= float(threshold)
                        and (bucket == "all" or row["liquidity_bucket"] == bucket)
                    ]
                    summaries.append(
                        {
                            "signal_rule": rule,
                            "threshold": float(threshold),
                            "horizon_hours": horizon,
                            "liquidity_bucket": bucket,
                            "rule": statistics_for(selected, "rule_after_cost_pips"),
                            "flipped_negative_control": statistics_for(selected, "flipped_after_cost_pips"),
                        }
                    )
    apply_benjamini_hochberg(summaries)
    # This candidate was selected after examining the discovery sweep.  Its
    # technical split is useful for defining later prospective arms, but it is
    # explicitly not an untouched confirmation or a route around multiplicity.
    configured_candidates = list(
        config.get("technical_confirmation_candidates") or []
    )
    if not configured_candidates and config.get("technical_confirmation_candidate"):
        configured_candidates = [config["technical_confirmation_candidate"]]
    technical_confirmations = []
    for configured_candidate in configured_candidates:
        technical_candidate = dict(configured_candidate)
        candidate_rows = [
            row for row in rows
            if row["signal_rule"] == technical_candidate.get("signal_rule")
            and row["horizon_hours"]
            == int(technical_candidate.get("horizon_hours") or 168)
            and abs(float(row["signal_value"]))
            >= float(
                technical_candidate.get("absolute_signal_threshold")
                if technical_candidate.get("absolute_signal_threshold") is not None
                else 0.25
            )
            and row["liquidity_bucket"]
            == str(technical_candidate.get("liquidity_bucket") or "liquid")
        ]
        technical_confirmations.append(
            {
                "candidate": technical_candidate,
                "selection_state": "post_discovery_diagnostic_not_confirmation",
                "lookback_rule": config.get("technical_confirmation_rule"),
                "arms": [
                    {
                        "arm": arm,
                        "rule": statistics_for(
                            [
                                row
                                for row in candidate_rows
                                if arm == "macro_all"
                                or row["technical_arm"] == arm
                            ],
                            "rule_after_cost_pips",
                        ),
                        "flipped_negative_control": statistics_for(
                            [
                                row
                                for row in candidate_rows
                                if arm == "macro_all"
                                or row["technical_arm"] == arm
                            ],
                            "flipped_after_cost_pips",
                        ),
                    }
                    for arm in (
                        "macro_all",
                        "macro_technical_aligned",
                        "macro_technical_conflicted",
                        "macro_only_technical_unavailable",
                    )
                ],
                "proof_eligible": False,
            }
        )
    technical_confirmation = technical_confirmations[0] if technical_confirmations else None
    default_limitations = [
        "current-view bootstrap values do not prove historical release-time availability",
        "conservative 45-day monthly and 75-day quarterly lags are counterfactual assumptions",
        "FRED international series are stale and are not current live predictors",
        "multiple pairs on one reference date share a macro factor episode",
        "all thresholds, rules, and horizons are a discovery sweep",
    ]
    point_in_time_limitations = [
        "ALFRED initial-release vintages are historical backfill, not prospective proof",
        "ALFRED vintage dates have civil-date rather than intraday precision; availability is delayed to the next provider-local day",
        "the fixed candidate was selected on a different current-view discovery replay",
        "multiple pairs on one reference date share a macro factor episode",
        "historical results cannot promote, authorize, or execute",
    ]
    payload = {
        "schema_version": 1,
        "generated_utc": iso(dt.datetime.now(UTC)),
        "contract_id": config.get("contract_id"),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "proof_eligible": False,
        "analysis_mode": config.get("analysis_mode") or "discovery_sweep",
        "availability_policy": availability_policy,
        "input_state": input_state,
        "panel_currencies": sorted(panel),
        "aligned_pair_events": len(events),
        "evaluated_rows": len(rows),
        "errors": errors,
        "summaries": summaries,
        "technical_confirmation_diagnostic": technical_confirmation,
        "technical_confirmation_diagnostics": technical_confirmations,
        "details": rows,
        "limitations": (
            point_in_time_limitations
            if availability_policy == "max_first_seen_utc"
            else default_limitations
        ),
        "supported_execution_decision": "no_trade",
    }
    atomic(output_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    lines = [
        f"# {config.get('report_title') or 'ALFRED Macro Relative-Strength Discovery'}",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        str(
            config.get("report_subtitle")
            or "Availability-counterfactual historical discovery only; no promotion or execution path."
        ),
        "",
        f"- Comparable panel currencies: **{len(panel)}** ({', '.join(sorted(panel))})",
        f"- Aligned pair events: **{len(events)}**",
        f"- Evaluated pair/horizon rows: **{len(rows)}**",
        f"- OANDA GET-only price errors: **{len(errors)}**",
        f"- Excluded annual context currencies: **{', '.join(input_state.get('excluded_annual_context_currencies') or []) or 'none'}**",
        "",
        "| Rule | Bucket | Threshold | Horizon | Raw N | Factors | Pairs | Win | Avg net | Factor avg | Factor LCB | BH q | Flipped avg |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        rule = row["rule"]
        flip = row["flipped_negative_control"]
        if not rule["raw_n"]:
            continue
        factor_lcb = (
            "n/a"
            if rule["factor_lcb_normal_95_pips"] is None
            else f"{rule['factor_lcb_normal_95_pips']:.3f}"
        )
        q_value = (
            "n/a"
            if rule["discovery_bh_q_value"] is None
            else f"{rule['discovery_bh_q_value']:.3f}"
        )
        lines.append(
            f"| {row['signal_rule']} | {row['liquidity_bucket']} | {row['threshold']:.2f} | {row['horizon_hours']}h | "
            f"{rule['raw_n']} | {rule['factor_episode_n']} | {rule['pair_n']} | "
            f"{rule['win_rate']:.1%} | {rule['average_net_pips']:.3f} | "
            f"{rule['factor_mean_pips']:.3f} | "
            f"{factor_lcb} | "
            f"{q_value} | "
            f"{flip['average_net_pips']:.3f} |"
        )
    positive_liquid = sorted(
        (
            row for row in summaries
            if row["liquidity_bucket"] == "liquid"
            and row["rule"]["average_net_pips"] is not None
            and row["rule"]["average_net_pips"] > 0
        ),
        key=lambda row: row["rule"]["average_net_pips"],
        reverse=True,
    )
    if positive_liquid:
        top = positive_liquid[0]
        check = top["rule"]
        lines += [
            "",
            "## Strongest liquid-bucket discovery stress",
            "",
            f"- Definition: **{top['signal_rule']} / {top['threshold']:.2f} / {top['horizon_hours']}h**.",
            f"- Raw/factor N: **{check['raw_n']} / {check['factor_episode_n']}**; average/factor mean: **{check['average_net_pips']:.3f} / {check['factor_mean_pips']:.3f} pips**.",
            f"- Normal 95% factor lower bound and BH q: **{check['factor_lcb_normal_95_pips']:.3f} / {check['discovery_bh_q_value']:.3f}**.",
            f"- Mean after best pair / best episode removal: **{check['mean_without_best_pair_pips']:.3f} / {check['factor_mean_without_best_episode_pips']:.3f} pips**.",
            f"- Early/late factor means: **{check['early_factor_mean_pips']:.3f} / {check['late_factor_mean_pips']:.3f} pips**.",
            f"- Extra full-spread stress and worst leave-one-currency-out factor mean: **{check['additional_full_spread_mean_pips']:.3f} / {check['worst_leave_one_currency_out_factor_mean_pips']:.3f} pips**.",
        ]
    lines += [
    ]
    for diagnostic in technical_confirmations:
        candidate = diagnostic["candidate"]
        lines += [
            "",
            "## Fixed candidate technical-confirmation diagnostic",
            "",
            f"Candidate: **{candidate.get('candidate_id') or candidate.get('signal_rule')}**.",
            "This split was selected after discovery and cannot confirm the candidate.",
            "",
            "| Arm | Raw N | Factors | Avg net | Factor avg | LCB | Flipped avg |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        for arm_row in diagnostic["arms"]:
            rule = arm_row["rule"]
            flip = arm_row["flipped_negative_control"]
            lines.append(
                f"| {arm_row['arm']} | {rule['raw_n']} | {rule['factor_episode_n']} | "
                f"{rule['average_net_pips'] if rule['average_net_pips'] is not None else float('nan'):.3f} | "
                f"{rule['factor_mean_pips'] if rule['factor_mean_pips'] is not None else float('nan'):.3f} | "
                f"{rule['factor_lcb_normal_95_pips'] if rule['factor_lcb_normal_95_pips'] is not None else float('nan'):.3f} | "
                f"{flip['average_net_pips'] if flip['average_net_pips'] is not None else float('nan'):.3f} |"
            )
    lines += [
        "",
        "A positive point estimate here is not edge. It must survive factor deduplication, concentration and cost stress, then be re-created as a frozen prospective cohort from later first-seen releases.",
        "",
    ]
    atomic(report_path, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ALFRED_DB)
    parser.add_argument("--state", type=Path, default=ALFRED_STATE)
    parser.add_argument("--alfred-config", type=Path, default=ROOT / "config" / "alfred_vintage_prospective_v1.json")
    parser.add_argument("--quotes", type=Path, default=QUOTES)
    parser.add_argument("--creds", type=Path, default=CREDS)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.database, args.state, args.alfred_config, args.quotes, args.creds, args.config, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
