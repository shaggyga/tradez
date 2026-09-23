#!/usr/bin/env python3
"""Causal GDELT attention/news-versus-technical FX mapping audit.

The retained GDELT window is short, so this tool can diagnose mapping quality
and reject weak rules but cannot confirm an edge.  It uses local first-seen
time, collapses source lineages, aggregates only information known by the end
of each UTC hour, and scores later OANDA practice GET-only M15 bid/ask paths.
No broker write endpoint exists in this module.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

from oanda_cftc_positioning_historical_discovery import (
    atomic_text,
    bh_qvalues,
    iso,
    parse_utc,
    sign_flip_pvalue,
)
from oanda_news_feed_backtest import readonly_price_token


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_NEWS = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
DEFAULT_ARCHIVE = DATA / "source_archives" / "gdelt_mapping_20260808"
DEFAULT_JSON = DATA / "reports" / "news_mapping" / "GDELT_ATTENTION_MAPPING_AUDIT_20260808.json"
DEFAULT_REPORT = DATA / "reports" / "news_mapping" / "GDELT_ATTENTION_MAPPING_AUDIT_20260808.md"
CREDS = ROOT / "creds"
SOURCES = ("gdelt_fx", "gdelt_fx_macro_discovery")
PAIR_MAP = {
    "AUD": "AUD_USD",
    "CAD": "USD_CAD",
    "CHF": "USD_CHF",
    "EUR": "EUR_USD",
    "GBP": "GBP_USD",
    "JPY": "USD_JPY",
    "NZD": "NZD_USD",
}
HORIZONS_MINUTES = (15, 60, 240)
TONE_THRESHOLD = 0.02
HIGH_ATTENTION_STORIES = 3
MIN_CONFIRMATION_MARKET_DAYS = 20


def floor_hour(value: dt.datetime) -> dt.datetime:
    return value.astimezone(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)


def parse_json(value: Any, default: Any) -> Any:
    try:
        return json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def load_mapped_stories(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in SOURCES)
    rows = connection.execute(
        f"""
        SELECT event_id,source_id,published_utc,first_seen_utc,headline,relevant,
               currencies_json,generic_sentiment_score,directional_confidence,
               duplicate_count,payload_json
        FROM articles WHERE source_id IN ({placeholders})
        ORDER BY first_seen_utc,event_id
        """,
        SOURCES,
    ).fetchall()
    connection.close()
    raw_count = len(rows)
    nonzero_tone = currency_mapped = relevant = timely = 0
    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        payload = parse_json(raw["payload_json"], {})
        currencies = [
            str(value).upper()
            for value in parse_json(raw["currencies_json"], [])
            if str(value).upper() in PAIR_MAP
        ]
        if currencies:
            currency_mapped += 1
        if abs(float(raw["generic_sentiment_score"] or 0.0)) > 1e-12:
            nonzero_tone += 1
        relevant += int(bool(raw["relevant"]))
        timely += int(bool(payload.get("forward_signal_timely")))
        first_seen = parse_utc(raw["first_seen_utc"])
        lineage = str(
            payload.get("event_lineage_id")
            or payload.get("story_cluster_id")
            or raw["event_id"]
        )
        for currency in sorted(set(currencies)):
            key = (lineage, currency)
            value = {
                "lineage_id": lineage,
                "currency": currency,
                "source_id": str(raw["source_id"]),
                "first_seen_utc": iso(first_seen),
                "published_utc": str(raw["published_utc"]),
                "generic_tone": float(raw["generic_sentiment_score"] or 0.0),
                "directional_confidence": float(raw["directional_confidence"] or 0.0),
                "relevant": bool(raw["relevant"]),
                "forward_signal_timely": bool(payload.get("forward_signal_timely")),
                "duplicate_observations": int(raw["duplicate_count"] or 0),
                "headline": str(raw["headline"]),
                "collector_contract_id": str(
                    payload.get("collector_contract_id") or ""
                ),
                "collector_cohort_id": str(
                    payload.get("collector_cohort_id") or ""
                ),
                "observation_time_contract_id": str(
                    payload.get("observation_time_contract_id") or ""
                ),
                "observation_clock_trusted": (
                    payload.get("observation_clock_trusted") is True
                ),
            }
            prior = dedup.get(key)
            if prior is None or value["first_seen_utc"] < prior["first_seen_utc"]:
                dedup[key] = value
    stories = sorted(dedup.values(), key=lambda row: (row["first_seen_utc"], row["currency"], row["lineage_id"]))
    stats = {
        "raw_articles": raw_count,
        "articles_with_supported_currency": currency_mapped,
        "articles_relevant": relevant,
        "articles_with_nonzero_tone": nonzero_tone,
        "articles_marked_forward_timely": timely,
        "independent_currency_story_rows": len(stories),
        "supported_currency_fraction": currency_mapped / raw_count if raw_count else 0.0,
        "relevant_fraction": relevant / raw_count if raw_count else 0.0,
        "nonzero_tone_fraction": nonzero_tone / raw_count if raw_count else 0.0,
    }
    return stories, stats


def build_currency_hours(stories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, dt.datetime], list[dict[str, Any]]] = defaultdict(list)
    for story in stories:
        stamp = parse_utc(story["first_seen_utc"])
        groups[(story["currency"], floor_hour(stamp))].append(story)
    output = []
    for (currency, hour), rows in sorted(groups.items(), key=lambda item: item[0]):
        decision = hour + dt.timedelta(hours=1)
        tones = [float(row["generic_tone"]) for row in rows]
        output.append(
            {
                "currency": currency,
                "hour_start_utc": iso(hour),
                "decision_utc": iso(decision),
                "story_count": len(rows),
                "source_count": len({row["source_id"] for row in rows}),
                "relevant_story_count": sum(row["relevant"] for row in rows),
                "timely_story_count": sum(row["forward_signal_timely"] for row in rows),
                "mean_tone": statistics.fmean(tones),
                "mean_absolute_tone": statistics.fmean(abs(value) for value in tones),
                "duplicate_observations": sum(row["duplicate_observations"] for row in rows),
                "lineage_ids": sorted(row["lineage_id"] for row in rows),
            }
        )
    return output


def fetch_m15(
    token: str,
    base_url: str,
    instrument: str,
    start: dt.datetime,
    end: dt.datetime,
) -> list[dict[str, Any]]:
    url = f"{base_url}/v3/instruments/{instrument}/candles"
    response = requests.get(
        url,
        params={
            "price": "BAM",
            "granularity": "M15",
            "from": iso(start).replace("+00:00", "Z"),
            "to": iso(end).replace("+00:00", "Z"),
            "smooth": "false",
        },
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=45,
    )
    response.raise_for_status()
    output = []
    for candle in response.json().get("candles") or []:
        if not candle.get("complete", True):
            continue
        bid, ask, mid = candle.get("bid") or {}, candle.get("ask") or {}, candle.get("mid") or {}
        try:
            output.append({
                "time": parse_utc(candle["time"]),
                "bid_o": float(bid["o"]), "bid_h": float(bid["h"]), "bid_l": float(bid["l"]),
                "ask_o": float(ask["o"]), "ask_h": float(ask["h"]), "ask_l": float(ask["l"]),
                "mid_o": float(mid["o"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return output


def first_index(candles: list[dict[str, Any]], timestamp: dt.datetime) -> int | None:
    return next((index for index, row in enumerate(candles) if row["time"] >= timestamp), None)


def currency_move_bps(
    currency: str,
    instrument: str,
    entry: dict[str, Any],
    exit_row: dict[str, Any],
) -> float:
    base, quote = instrument.split("_")
    orientation = 1 if base == currency else -1 if quote == currency else 0
    return orientation * (exit_row["mid_o"] / entry["mid_o"] - 1.0) * 10000.0


def executable_net_bps(
    currency: str,
    instrument: str,
    direction: int,
    entry: dict[str, Any],
    exit_row: dict[str, Any],
) -> float:
    base, quote = instrument.split("_")
    pair_direction = direction * (1 if base == currency else -1 if quote == currency else 0)
    if pair_direction > 0:
        return (exit_row["bid_o"] - entry["ask_o"]) / entry["mid_o"] * 10000.0
    return (entry["bid_o"] - exit_row["ask_o"]) / entry["mid_o"] * 10000.0


def evaluate_hours(
    hours: list[dict[str, Any]],
    candles_by_pair: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    outcomes = []
    for episode in hours:
        currency = episode["currency"]
        instrument = PAIR_MAP[currency]
        candles = candles_by_pair.get(instrument) or []
        decision = parse_utc(episode["decision_utc"])
        entry_index = first_index(candles, decision)
        prior_index = first_index(candles, decision - dt.timedelta(hours=1))
        if entry_index is None or prior_index is None or entry_index <= prior_index:
            continue
        if (candles[entry_index]["time"] - decision).total_seconds() > 900:
            continue
        prior_move = currency_move_bps(
            currency, instrument, candles[prior_index], candles[entry_index]
        )
        technical_direction = 1 if prior_move > 0 else -1 if prior_move < 0 else 0
        tone = float(episode["mean_tone"])
        news_direction = 1 if tone >= TONE_THRESHOLD else -1 if tone <= -TONE_THRESHOLD else 0
        arms = {
            "news_only": news_direction,
            "technical_only": technical_direction,
            "news_technical_confirmed": news_direction if news_direction and news_direction == technical_direction else 0,
            "news_technical_conflicted": news_direction if news_direction and technical_direction and news_direction != technical_direction else 0,
        }
        for horizon in HORIZONS_MINUTES:
            target = decision + dt.timedelta(minutes=horizon)
            exit_index = first_index(candles, target)
            if exit_index is None or (candles[exit_index]["time"] - target).total_seconds() > 900:
                continue
            entry, exit_row = candles[entry_index], candles[exit_index]
            gross_currency_bps = currency_move_bps(currency, instrument, entry, exit_row)
            spread_bps = (entry["ask_o"] - entry["bid_o"]) / entry["mid_o"] * 10000.0
            magnitude_cleared_cost = abs(gross_currency_bps) > spread_bps
            outcomes.append({
                **episode,
                "instrument": instrument,
                "horizon_minutes": horizon,
                "entry_utc": iso(entry["time"]),
                "exit_utc": iso(exit_row["time"]),
                "prior_currency_move_bps": prior_move,
                "gross_currency_move_bps": gross_currency_bps,
                "absolute_currency_move_bps": abs(gross_currency_bps),
                "entry_spread_bps": spread_bps,
                "magnitude_cleared_cost": magnitude_cleared_cost,
                "high_attention": episode["story_count"] >= HIGH_ATTENTION_STORIES,
                "arm_results": {
                    arm: {
                        "direction": direction,
                        "net_bps": executable_net_bps(currency, instrument, direction, entry, exit_row),
                    }
                    for arm, direction in arms.items()
                    if direction
                },
            })
    return outcomes


def summarize(outcomes: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    arm_groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    magnitude_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for outcome in outcomes:
        if outcome["high_attention"]:
            magnitude_groups[int(outcome["horizon_minutes"])].append(outcome)
        for arm, result in outcome["arm_results"].items():
            arm_groups[(arm, int(outcome["horizon_minutes"]))].append({**outcome, **result})
    arm_rows = []
    for (arm, horizon), group in sorted(arm_groups.items()):
        days: dict[str, list[float]] = defaultdict(list)
        for row in group:
            days[row["decision_utc"][:10]].append(float(row["net_bps"]))
        episode_values = [statistics.fmean(values) for values in days.values()]
        best_day = max(days, key=lambda key: statistics.fmean(days[key]))
        without_best = [row for row in group if row["decision_utc"][:10] != best_day]
        arm_rows.append({
            "arm": arm,
            "horizon_minutes": horizon,
            "raw_n": len(group),
            "independent_market_days": len(days),
            "mean_net_bps": statistics.fmean(row["net_bps"] for row in group),
            "median_net_bps": statistics.median(row["net_bps"] for row in group),
            "after_cost_win_rate": statistics.fmean(row["net_bps"] > 0 for row in group),
            "mean_without_best_day_bps": statistics.fmean(row["net_bps"] for row in without_best) if without_best else None,
            "sign_flip_p": sign_flip_pvalue(episode_values, seed=int(hashlib.sha256(f"{arm}|{horizon}".encode()).hexdigest()[:8], 16)),
        })
    qvalues = bh_qvalues([row["sign_flip_p"] for row in arm_rows])
    for row, qvalue in zip(arm_rows, qvalues):
        row["bh_q"] = qvalue
        row["confirmation_eligible"] = bool(
            row["independent_market_days"] >= MIN_CONFIRMATION_MARKET_DAYS
            and row["mean_net_bps"] > 0
            and row["mean_without_best_day_bps"] is not None
            and row["mean_without_best_day_bps"] > 0
            and qvalue <= 0.10
        )
    magnitude_rows = []
    for horizon, group in sorted(magnitude_groups.items()):
        days = {row["decision_utc"][:10] for row in group}
        magnitude_rows.append({
            "horizon_minutes": horizon,
            "raw_n": len(group),
            "independent_market_days": len(days),
            "mean_absolute_move_bps": statistics.fmean(row["absolute_currency_move_bps"] for row in group),
            "cost_clear_fraction": statistics.fmean(row["magnitude_cleared_cost"] for row in group),
            "mean_story_count": statistics.fmean(row["story_count"] for row in group),
            "confirmation_eligible": False,
        })
    return arm_rows, magnitude_rows


def run(
    *,
    news_database: Path = DEFAULT_NEWS,
    archive: Path = DEFAULT_ARCHIVE,
    output: Path = DEFAULT_JSON,
    report: Path = DEFAULT_REPORT,
    creds: Path = CREDS,
) -> dict[str, Any]:
    stories, mapping = load_mapped_stories(news_database)
    hours = build_currency_hours(stories)
    errors = []
    candles_by_pair = {}
    candle_hashes = {}
    archive.mkdir(parents=True, exist_ok=True)
    if hours:
        start = min(parse_utc(row["decision_utc"]) for row in hours) - dt.timedelta(hours=2)
        requested_end = max(parse_utc(row["decision_utc"]) for row in hours) + dt.timedelta(hours=5)
        end = min(requested_end, dt.datetime.now(dt.timezone.utc))
        token, base_url = readonly_price_token(creds)
        for instrument in sorted(set(PAIR_MAP.values())):
            try:
                candles = fetch_m15(token, base_url, instrument, start, end)
                serial = [{**row, "time": iso(row["time"])} for row in candles]
                text = json.dumps(serial, indent=2, sort_keys=True)
                atomic_text(archive / f"oanda_{instrument}_M15.json", text)
                candle_hashes[instrument] = hashlib.sha256(text.encode()).hexdigest()
                candles_by_pair[instrument] = candles
            except Exception as exc:
                errors.append({"instrument": instrument, "error": f"{type(exc).__name__}: {exc}"})
    outcomes = evaluate_hours(hours, candles_by_pair)
    arm_rows, magnitude_rows = summarize(outcomes)
    payload = {
        "schema_version": 1,
        "generated_utc": iso(dt.datetime.now(dt.timezone.utc)),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "proof_eligible": False,
        "source_ids": list(SOURCES),
        "mapping_quality": mapping,
        "currency_hour_episodes": len(hours),
        "market_days": len({row["decision_utc"][:10] for row in hours}),
        "scored_pair_horizon_outcomes": len(outcomes),
        "candle_hashes": candle_hashes,
        "errors": errors,
        "arm_summaries": arm_rows,
        "attention_magnitude_summaries": magnitude_rows,
        "supported_action": (
            "continue_integrity_collection"
            if not any(row["confirmation_eligible"] for row in arm_rows)
            else "open_new_untouched_prospective_candidate"
        ),
        "limitations": [
            "retained GDELT history covers too few independent market days for confirmation",
            "generic linguistic tone is tested only as a naive negative-control currency direction",
            "technical confirmation uses only the completed hour before the decision",
            "all source observations are deduplicated by source lineage and currency-hour",
        ],
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# GDELT Attention and News/Technical Mapping Audit",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only diagnostic; cannot promote, authorize, or execute.",
        "",
        f"- Raw GDELT articles: **{mapping['raw_articles']:,}**",
        f"- Supported-currency articles: **{mapping['articles_with_supported_currency']:,} ({mapping['supported_currency_fraction']:.1%})**",
        f"- Relevant articles: **{mapping['articles_relevant']:,} ({mapping['relevant_fraction']:.1%})**",
        f"- Nonzero-tone articles: **{mapping['articles_with_nonzero_tone']:,} ({mapping['nonzero_tone_fraction']:.1%})**",
        f"- Independent currency/story rows: **{mapping['independent_currency_story_rows']:,}**",
        f"- Currency-hour / market-day episodes: **{len(hours):,} / {payload['market_days']}**",
        f"- Scored pair/horizon outcomes: **{len(outcomes):,}**",
        f"- Fetch errors: **{len(errors)}**",
        f"- Supported action: **{payload['supported_action']}**",
        "",
        "| Arm | Horizon | Raw N | Market days | Mean net bps | Win rate | Ex-best day | BH q | Eligible |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in arm_rows:
        ex_best = row["mean_without_best_day_bps"]
        lines.append(
            f"| {row['arm']} | {row['horizon_minutes']}m | {row['raw_n']} | {row['independent_market_days']} | "
            f"{row['mean_net_bps']:.3f} | {row['after_cost_win_rate']:.1%} | "
            f"{'n/a' if ex_best is None else f'{ex_best:.3f}'} | {row['bh_q']:.4f} | "
            f"{'yes' if row['confirmation_eligible'] else 'no'} |"
        )
    lines += [
        "",
        "## High-attention magnitude diagnostic",
        "",
        "| Horizon | Raw N | Market days | Mean absolute bps | Cost-clear | Mean stories |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for row in magnitude_rows:
        lines.append(
            f"| {row['horizon_minutes']}m | {row['raw_n']} | {row['independent_market_days']} | "
            f"{row['mean_absolute_move_bps']:.3f} | {row['cost_clear_fraction']:.1%} | {row['mean_story_count']:.2f} |"
        )
    lines += [
        "",
        "A high article count is an attention feature, not independent evidence. No arm may graduate from this short window.",
        "",
    ]
    atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news-database", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--output", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--creds", type=Path, default=CREDS)
    args = parser.parse_args()
    result = run(
        news_database=args.news_database,
        archive=args.archive,
        output=args.output,
        report=args.report,
        creds=args.creds,
    )
    print(json.dumps({
        "currency_hour_episodes": result["currency_hour_episodes"],
        "market_days": result["market_days"],
        "scored_outcomes": result["scored_pair_horizon_outcomes"],
        "supported_action": result["supported_action"],
        "errors": result["errors"],
    }, sort_keys=True))
    return 0 if not result["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_currency_hours",
    "currency_move_bps",
    "executable_net_bps",
    "floor_hour",
    "summarize",
]
