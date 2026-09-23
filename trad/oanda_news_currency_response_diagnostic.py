#!/usr/bin/env python3
"""Source-first 21-currency/68-pair historical response diagnostic.

This is deliberately a *discovery* backtest.  It uses article first-seen clocks
and executable OANDA bid/ask paths, but many article directions were produced
by classifier versions improved after historical miss review.  The output may
reject a rule or seed a newly frozen prospective rule; it cannot prove alpha.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_news_historical_quality_audit import (
    CURRENCIES,
    DATA,
    DEFAULT_CANDLES,
    DEFAULT_NEWS,
    atomic_text,
    load_articles,
    parse_json,
    parse_time,
)


DEFAULT_OUTPUT = DATA / "reports" / "news_currency_response" / "NEWS_CURRENCY_RESPONSE_DIAGNOSTIC_V1.json"
DEFAULT_REPORT = DATA / "reports" / "news_currency_response" / "NEWS_CURRENCY_RESPONSE_DIAGNOSTIC_V1.md"
HORIZONS = (5, 15, 60)
BUCKET_MINUTES = 5
PRIOR_TECHNICAL_MINUTES = 15
MAX_CANDLE_DELAY_SECONDS = 90


def ceil_clock(value: dt.datetime, minutes: int = BUCKET_MINUTES) -> dt.datetime:
    value = value.astimezone(dt.timezone.utc)
    seconds = int(value.timestamp())
    width = minutes * 60
    return dt.datetime.fromtimestamp(((seconds + width - 1) // width) * width, dt.timezone.utc)


def story_id(row: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    explicit = payload.get("event_lineage_id") or payload.get("story_cluster_id")
    if explicit:
        return str(explicit)
    headline = " ".join(str(row.get("headline") or "").lower().split())
    return headline.rsplit(" - ", 1)[0] or str(row.get("event_id") or "")


def build_currency_clocks(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    raw_rows = 0
    rejected = Counter()
    groups: dict[tuple[str, dt.datetime], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        raw_rows += 1
        payload = parse_json(row.get("payload_json"), {})
        if not bool(row.get("relevant")):
            rejected["not_relevant"] += 1
            continue
        if payload.get("forward_signal_timely") is not True:
            rejected["not_forward_timely"] += 1
            continue
        if payload.get("reports_prior_market_move") is True:
            rejected["reports_prior_market_move"] += 1
            continue
        first_seen = parse_time(row.get("first_seen_utc"))
        published = parse_time(row.get("published_utc"))
        if first_seen is None:
            rejected["missing_first_seen"] += 1
            continue
        if published and published > first_seen + dt.timedelta(minutes=5):
            rejected["future_publication_clock"] += 1
            continue
        scores = parse_json(row.get("currency_scores_json"), {})
        scores = {
            str(currency).upper(): float(score)
            for currency, score in scores.items()
            if str(currency).upper() in CURRENCIES
            and isinstance(score, (int, float)) and not isinstance(score, bool)
            and math.isfinite(float(score)) and abs(float(score)) >= 0.05
        }
        if not scores:
            rejected["no_currency_direction"] += 1
            continue
        decision = ceil_clock(first_seen)
        lineage = story_id(row, payload)
        for currency, score in scores.items():
            key = (currency, decision)
            # One syndicated story contributes once per currency/decision.
            prior = groups[key].get(lineage)
            candidate = {
                "score": score,
                "story_id": lineage,
                "source_id": str(row.get("source_id") or "unknown"),
                "source_verified": bool(row.get("source_verified")),
                "source_direct": payload.get("source_direct") is True,
                "clock_trusted": payload.get("observation_clock_trusted") is True,
                "headline": str(row.get("headline") or ""),
            }
            if prior is None or abs(score) > abs(float(prior["score"])):
                groups[key][lineage] = candidate
    clocks: list[dict[str, Any]] = []
    for (currency, decision), stories in sorted(groups.items(), key=lambda item: item[0]):
        values = list(stories.values())
        score = statistics.fmean(float(row["score"]) for row in values)
        if abs(score) < 0.05:
            rejected["cross_story_cancellation"] += 1
            continue
        clocks.append({
            "clock_id": "currency_clock_" + hashlib.sha256(
                f"{currency}|{decision.isoformat()}|{'|'.join(sorted(stories))}".encode()
            ).hexdigest()[:24],
            "currency": currency,
            "decision_utc": decision,
            "score": score,
            "direction": 1 if score > 0 else -1,
            "story_count": len(values),
            "source_count": len({row["source_id"] for row in values}),
            "verified_story_count": sum(row["source_verified"] for row in values),
            "direct_story_count": sum(row["source_direct"] for row in values),
            "trusted_story_count": sum(row["clock_trusted"] for row in values),
            "story_ids": sorted(stories),
            "source_ids": sorted({row["source_id"] for row in values}),
            "headlines": [row["headline"] for row in values[:5]],
        })
    return clocks, {"raw_articles": raw_rows, "currency_clocks": len(clocks), **dict(rejected)}


def load_pair(path: Path) -> tuple[list[dt.datetime], list[tuple[float, float, float]]]:
    times: list[dt.datetime] = []
    prices: list[tuple[float, float, float]] = []
    with path.open("r", encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            stamp = parse_time(row.get("datetime") or row.get("time"))
            try:
                bid = float(row["bid_open"])
                ask = float(row["ask_open"])
                mid = float(row["open"])
            except (KeyError, TypeError, ValueError):
                continue
            if stamp is None or not (0 < bid <= ask and mid > 0):
                continue
            times.append(stamp)
            prices.append((bid, ask, mid))
    return times, prices


def near_index(times: list[dt.datetime], target: dt.datetime) -> int | None:
    index = bisect.bisect_left(times, target)
    if index >= len(times):
        return None
    if (times[index] - target).total_seconds() > MAX_CANDLE_DELAY_SECONDS:
        return None
    return index


def executable_net_bps(direction: int, entry: tuple[float, float, float], exit_price: tuple[float, float, float]) -> float:
    entry_bid, entry_ask, _ = entry
    exit_bid, exit_ask, _ = exit_price
    if direction > 0:
        return (exit_bid / entry_ask - 1.0) * 10000.0
    return (entry_bid / exit_ask - 1.0) * 10000.0


def price_candidates(
    clocks: list[dict[str, Any]], candle_path: Path,
    *, horizons: Iterable[int] = HORIZONS,
) -> dict[str, list[dict[str, Any]]]:
    requested_horizons = tuple(int(value) for value in horizons)
    by_currency: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for clock in clocks:
        by_currency[clock["currency"]].append(clock)
    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in sorted(candle_path.glob("*_M1.csv")):
        instrument = source.stem.removesuffix("_M1")
        parts = instrument.split("_")
        if len(parts) != 2:
            continue
        relevant = {
            clock["clock_id"]: clock
            for currency in parts for clock in by_currency.get(currency, [])
        }
        if not relevant:
            continue
        times, prices = load_pair(source)
        if not times:
            continue
        for clock in relevant.values():
            decision = clock["decision_utc"]
            entry_index = near_index(times, decision)
            prior_index = near_index(
                times, decision - dt.timedelta(minutes=PRIOR_TECHNICAL_MINUTES)
            )
            if entry_index is None or prior_index is None or prior_index >= entry_index:
                continue
            entry = prices[entry_index]
            spread_bps = (entry[1] / entry[0] - 1.0) * 10000.0
            base_multiplier = 1 if clock["currency"] == parts[0] else -1
            prior_pair_bps = (entry[2] / prices[prior_index][2] - 1.0) * 10000.0
            prior_currency_bps = prior_pair_bps * base_multiplier
            horizon_results: dict[int, dict[str | int, float]] = {}
            for horizon in requested_horizons:
                exit_index = near_index(times, decision + dt.timedelta(minutes=horizon))
                if exit_index is None:
                    continue
                horizon_results[horizon] = {
                    1: executable_net_bps(1, entry, prices[exit_index]),
                    -1: executable_net_bps(-1, entry, prices[exit_index]),
                    "pair_mid_bps": (
                        prices[exit_index][2] / entry[2] - 1.0
                    ) * 10000.0,
                }
            if not horizon_results:
                continue
            candidates[clock["clock_id"]].append({
                "instrument": instrument,
                "spread_bps": spread_bps,
                "entry_utc": times[entry_index].isoformat(),
                "prior_currency_bps": prior_currency_bps,
                "horizon_pair_net_bps": horizon_results,
                "base_multiplier": base_multiplier,
                "decision": decision,
            })
    return candidates


def placebo_directions(clocks: list[dict[str, Any]]) -> dict[str, int]:
    output: dict[str, int] = {}
    by_currency: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for clock in clocks:
        by_currency[clock["currency"]].append(clock)
    for currency, values in by_currency.items():
        values.sort(key=lambda row: row["decision_utc"])
        offset = max(1, len(values) // 2)
        shifted = [int(row["direction"]) for row in values[offset:] + values[:offset]]
        for row, direction in zip(values, shifted):
            output[row["clock_id"]] = direction
    return output


def evaluate(clocks: list[dict[str, Any]], candidates: Mapping[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    placebos = placebo_directions(clocks)
    outcomes: list[dict[str, Any]] = []
    for clock in clocks:
        choices = candidates.get(clock["clock_id"], [])
        if not choices:
            continue
        # Selecting the cheapest executable expression is a causal policy;
        # outcome magnitude never enters this choice.
        choice = min(choices, key=lambda row: (row["spread_bps"], row["instrument"]))
        technical_direction = (
            1 if choice["prior_currency_bps"] > 0
            else -1 if choice["prior_currency_bps"] < 0 else 0
        )
        base_multiplier = int(choice["base_multiplier"])
        decision = choice["decision"]
        for horizon, pair_results in choice["horizon_pair_net_bps"].items():
            news_pair_direction = int(clock["direction"]) * base_multiplier
            news_net = pair_results[news_pair_direction]
            result = {
                "clock_id": clock["clock_id"],
                "currency": clock["currency"],
                "decision_utc": decision.isoformat(),
                "instrument": choice["instrument"],
                "horizon_minutes": horizon,
                "spread_bps": choice["spread_bps"],
                "score": clock["score"],
                "story_count": clock["story_count"],
                "source_count": clock["source_count"],
                "verified_story_count": clock["verified_story_count"],
                "direct_story_count": clock["direct_story_count"],
                "trusted_story_count": clock["trusted_story_count"],
                "prior_currency_bps": choice["prior_currency_bps"],
                "realized_currency_mid_bps": (
                    float(pair_results["pair_mid_bps"]) * base_multiplier
                ),
                "magnitude_cleared_spread": abs(
                    float(pair_results["pair_mid_bps"])
                ) > choice["spread_bps"],
                "source_ids": clock["source_ids"],
                "arm_net_bps": {"news_only": news_net},
                "arm_directions": {"news_only": int(clock["direction"])},
            }
            if technical_direction:
                technical_pair_direction = technical_direction * base_multiplier
                technical_net = pair_results[technical_pair_direction]
                result["arm_net_bps"]["technical_only_same_clocks"] = technical_net
                result["arm_directions"]["technical_only_same_clocks"] = technical_direction
                if technical_direction == int(clock["direction"]):
                    result["arm_net_bps"]["news_technical_confirmed"] = news_net
                    result["arm_directions"]["news_technical_confirmed"] = int(clock["direction"])
                else:
                    result["arm_net_bps"]["news_technical_conflicted"] = news_net
                    result["arm_directions"]["news_technical_conflicted"] = int(clock["direction"])
            placebo_pair_direction = placebos[clock["clock_id"]] * base_multiplier
            result["arm_net_bps"]["direction_permuted_placebo"] = pair_results[
                placebo_pair_direction
            ]
            result["arm_directions"]["direction_permuted_placebo"] = placebos[
                clock["clock_id"]
            ]
            outcomes.append(result)
    return outcomes


def summarize(outcomes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int], list[tuple[Mapping[str, Any], float]]] = defaultdict(list)
    for row in outcomes:
        for arm, value in row["arm_net_bps"].items():
            groups[(str(arm), int(row["horizon_minutes"]))].append((row, float(value)))
    output = []
    for (arm, horizon), values in sorted(groups.items()):
        days = defaultdict(list)
        currencies = Counter()
        for row, value in values:
            days[str(row["decision_utc"])[:10]].append(value)
            currencies[str(row["currency"])] += 1
        best_day = max(days, key=lambda key: statistics.fmean(days[key]))
        ex_best = [value for row, value in values if str(row["decision_utc"])[:10] != best_day]
        output.append({
            "arm": arm,
            "horizon_minutes": horizon,
            "raw_n": len(values),
            "market_days": len(days),
            "currency_count": len(currencies),
            "mean_net_bps": statistics.fmean(value for _, value in values),
            "median_net_bps": statistics.median(value for _, value in values),
            "after_cost_win_rate": statistics.fmean(value > 0 for _, value in values),
            "direction_accuracy": statistics.fmean(
                int(row["arm_directions"][arm])
                * float(row["realized_currency_mid_bps"]) > 0
                for row, _ in values
            ),
            "mean_signed_gross_bps": statistics.fmean(
                int(row["arm_directions"][arm])
                * float(row["realized_currency_mid_bps"])
                for row, _ in values
            ),
            "mean_entry_spread_bps": statistics.fmean(
                float(row["spread_bps"]) for row, _ in values
            ),
            "magnitude_cost_clear_fraction": statistics.fmean(
                bool(row["magnitude_cleared_spread"]) for row, _ in values
            ),
            "mean_without_best_day_bps": statistics.fmean(ex_best) if ex_best else None,
            "best_currency_fraction": max(currencies.values()) / len(values),
            "evidence_class": "retrospective_classifier_adaptive_diagnostic",
            "confirmation_eligible": False,
        })
    return output


def render(payload: Mapping[str, Any]) -> str:
    lines = [
        "# 21-currency news response diagnostic",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Retrospective discovery only. Article clocks are source-first, but classifier rules were improved after historical miss review; nothing here can promote or execute.",
        "",
        f"- Input articles: **{payload['selection']['raw_articles']:,}**",
        f"- Deduplicated 5-minute currency clocks: **{payload['selection']['currency_clocks']:,}**",
        f"- Price-scored currency clocks: **{payload['scored_currency_clocks']:,}**",
        f"- Pair/horizon outcomes: **{payload['outcome_rows']:,}**",
        f"- Strict trusted/prospective proof rows: **0** (reported separately by the quality audit)",
        "",
        "At each source clock the diagnostic chooses the containing pair with the lowest observed entry spread. It never chooses using the later outcome.",
        "",
        "| Arm | Horizon | N | Days | Dir. hit | Cost-clear magnitude | Win | Gross signed bps | Mean net bps | Spread bps |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["summaries"]:
        ex_best = row["mean_without_best_day_bps"]
        ex_best_text = "n/a" if ex_best is None else f"{ex_best:.3f}"
        lines.append(
            f"| {row['arm']} | {row['horizon_minutes']}m | {row['raw_n']} | "
            f"{row['market_days']} | {row['direction_accuracy']:.1%} | "
            f"{row['magnitude_cost_clear_fraction']:.1%} | "
            f"{row['after_cost_win_rate']:.1%} | "
            f"{row['mean_signed_gross_bps']:.3f} | {row['mean_net_bps']:.3f} | "
            f"{row['mean_entry_spread_bps']:.3f} |"
        )
    lines += [
        "",
        "The direction-permuted arm keeps the same event clocks and costs while assigning directions from other events of the same currency. This tests whether semantic direction adds information beyond merely trading when news is busy.",
        "",
        "A positive retrospective row is only a candidate for a new immutable prospective cohort. A negative row may reject the simple rule immediately.",
        "",
    ]
    return "\n".join(lines)


def run(
    *, news: Path = DEFAULT_NEWS, candles: Path = DEFAULT_CANDLES,
    output: Path = DEFAULT_OUTPUT, report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    clocks, selection = build_currency_clocks(load_articles(news))
    candidates = price_candidates(clocks, candles)
    outcomes = evaluate(clocks, candidates)
    payload = {
        "schema_version": "news_currency_response_diagnostic_v1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "selection": selection,
        "scored_currency_clocks": len({row["clock_id"] for row in outcomes}),
        "outcome_rows": len(outcomes),
        "summaries": summarize(outcomes),
        "supported_action": "inspect_diagnostic_then_freeze_only_predeclared_candidate",
        "limitations": [
            "classifier rules are adaptive to reviewed historical misses",
            "source history spans weeks rather than independent macro regimes",
            "strict trusted-clock prospective population has zero price-matured rows",
            "currency clocks remain correlated within shared stories and market episodes",
        ],
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    atomic_text(report, render(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--candles", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    payload = run(**vars(args))
    print(json.dumps({
        "selection": payload["selection"],
        "scored_currency_clocks": payload["scored_currency_clocks"],
        "outcome_rows": payload["outcome_rows"],
        "output": str(args.output), "report": str(args.report),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
