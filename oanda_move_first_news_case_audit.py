#!/usr/bin/env python3
"""Audit every retained independent FX move from the news outward.

The move inventory is primary.  Forecast availability never determines whether
a move enters the audit.  Causally available news is assessed first; retained
model directions are attached only as secondary technical confirmation.

This is retrospective research.  It cannot promote, authorize, or place an
order.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import oanda_major_move_gap_census as census
import oanda_news_feed_backtest as news_backtest
from oanda_instrument_pips import fallback_pip_size


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "reports" / "move_first_news_case_audit"
)
MOVE_DETAIL = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "major_move_gap_census"
    / "MAJOR_MOVE_GAP_CENSUS_DETAIL_CURRENT.csv"
)
SOURCE_DB = STATE / "source_governance_v1.sqlite"
OUTPUT_JSON = REPORT_ROOT / "MOVE_FIRST_NEWS_CASE_AUDIT_CURRENT.json"
OUTPUT_MD = REPORT_ROOT / "MOVE_FIRST_NEWS_CASE_AUDIT_CURRENT.md"
OUTPUT_CSV = REPORT_ROOT / "MOVE_FIRST_NEWS_CASE_AUDIT_DETAIL_CURRENT.csv"
DEFAULT_CANDLE_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "candles"
)
DEFAULT_INSTRUMENT_METADATA = (
    DEFAULT_CANDLE_ROOT / "instrument_metadata_v1.json"
)

BIAS_VALUE = {
    "BULLISH": 1.0,
    "BEARISH": -1.0,
    "POSITIVE": 1.0,
    "NEGATIVE": -1.0,
    "HAWKISH": 1.0,
    "DOVISH": -1.0,
}
RETROSPECTIVE_TEMPORALITIES = {
    "retrospective_market_report",
    "historical_context",
    "post_event_recap",
}
WINDOWS_MINUTES = (5, 15, 30, 60, 120)


def parse_payload(event: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(str(event.get("payload_json") or "{}"))
    except json.JSONDecodeError:
        return {}
    raw = payload.get("raw_payload")
    return dict(raw) if isinstance(raw, Mapping) else payload


def normalized_headline(payload: Mapping[str, Any]) -> str:
    headline = str(
        payload.get("headline")
        or payload.get("event_name")
        or payload.get("summary")
        or ""
    ).strip()
    return re.sub(r"\W+", " ", headline.lower(), flags=re.UNICODE).strip()


def independent_story_key(event: Mapping[str, Any]) -> str:
    payload = parse_payload(event)
    headline = normalized_headline(payload)
    if headline:
        return f"headline:{headline}"
    return str(
        event.get("story_cluster_id")
        or payload.get("event_lineage_id")
        or payload.get("event_id")
        or event.get("source_event_id")
        or ""
    )


def pair_score(
    event: Mapping[str, Any], base_currency: str, quote_currency: str
) -> tuple[float | None, str]:
    """Return base-minus-quote directional score and evidence disposition."""

    payload = parse_payload(event)
    temporality = str(payload.get("event_temporality") or "").lower()
    if temporality in RETROSPECTIVE_TEMPORALITIES:
        return None, "retrospective"
    if bool(payload.get("context_only")):
        return None, "context_only"

    raw_scores = payload.get("currency_scores")
    scores = raw_scores if isinstance(raw_scores, Mapping) else {}

    def value(currency: str) -> float | None:
        raw = scores.get(currency)
        if raw is not None:
            try:
                return float(raw)
            except (TypeError, ValueError):
                pass
        biases = payload.get("directional_bias")
        if isinstance(biases, Mapping):
            label = str(biases.get(currency) or "").upper()
            return BIAS_VALUE.get(label)
        return None

    base = value(base_currency)
    quote = value(quote_currency)
    if base is None and quote is None:
        return None, "no_pair_direction"
    score = float(base or 0.0) - float(quote or 0.0)
    if score == 0.0:
        return None, "pair_neutral"
    return score, "directional"


def event_confidence(event: Mapping[str, Any]) -> float:
    payload = parse_payload(event)
    try:
        return max(0.05, min(1.0, float(payload.get("directional_confidence") or 0.25)))
    except (TypeError, ValueError):
        return 0.25


def event_descriptor(
    event: Mapping[str, Any], score: float, entry_epoch: int
) -> dict[str, Any]:
    payload = parse_payload(event)
    effective = int(event.get("effective_epoch") or 0)
    return {
        "effective_from_utc": str(event.get("effective_from_utc") or ""),
        "minutes_before_entry": round((entry_epoch - effective) / 60.0, 3),
        "source_id": str(event.get("source_id") or ""),
        "source_population": str(event.get("source_population") or ""),
        "event_type": str(event.get("event_type") or ""),
        "headline": str(
            payload.get("headline")
            or payload.get("event_name")
            or payload.get("summary")
            or ""
        )[:500],
        "pair_score": score,
        "side": "long" if score > 0 else "short",
        "confidence": event_confidence(event),
        "event_temporality": str(payload.get("event_temporality") or "unknown"),
        "forward_signal_timely": payload.get("forward_signal_timely"),
        "directional_publish_eligible": bool(
            payload.get("directional_publish_eligible")
        ),
        "official": str(event.get("source_population") or "").startswith("official"),
        "official_policy_release": bool(payload.get("official_policy_release")),
    }


def directional_vote(
    events: Sequence[Mapping[str, Any]],
    base_currency: str,
    quote_currency: str,
    entry_epoch: int,
    *,
    strict_forward: bool = False,
    official_only: bool = False,
    policy_only: bool = False,
    minimum_confidence: float = 0.0,
    age_decay_half_life_minutes: float | None = None,
) -> dict[str, Any]:
    seen: set[str] = set()
    descriptors: list[dict[str, Any]] = []
    exclusions: Counter[str] = Counter()
    weighted_score = 0.0

    # Newest retained version wins when duplicate source records exist.
    for event in reversed(events):
        key = independent_story_key(event)
        if not key or key in seen:
            exclusions["duplicate_story"] += 1
            continue
        seen.add(key)
        score, disposition = pair_score(event, base_currency, quote_currency)
        if score is None:
            exclusions[disposition] += 1
            continue
        descriptor = event_descriptor(event, score, entry_epoch)
        if official_only and not descriptor["official"]:
            exclusions["not_official"] += 1
            continue
        if policy_only and not descriptor["official_policy_release"]:
            exclusions["not_official_policy_release"] += 1
            continue
        if strict_forward and descriptor["forward_signal_timely"] is False:
            exclusions["not_forward_timely"] += 1
            continue
        if strict_forward and not descriptor["directional_publish_eligible"]:
            exclusions["not_directional_publish_eligible"] += 1
            continue
        if descriptor["confidence"] < minimum_confidence:
            exclusions["below_confidence"] += 1
            continue
        descriptors.append(descriptor)
        age_weight = 1.0
        if age_decay_half_life_minutes and age_decay_half_life_minutes > 0:
            age_weight = 0.5 ** (
                max(0.0, float(descriptor["minutes_before_entry"]))
                / age_decay_half_life_minutes
            )
        descriptor["age_weight"] = round(age_weight, 9)
        descriptor["vote_weight"] = round(
            float(descriptor["confidence"]) * age_weight,
            9,
        )
        weighted_score += (
            (1.0 if score > 0 else -1.0)
            * float(descriptor["confidence"])
            * age_weight
        )

    side = 1 if weighted_score > 0 else -1 if weighted_score < 0 else 0
    descriptors.sort(
        key=lambda row: (
            float(row["confidence"]),
            -float(row["minutes_before_entry"]),
        ),
        reverse=True,
    )
    return {
        "side": side,
        "weighted_score": weighted_score,
        "independent_directional_stories": len(descriptors),
        "stories": descriptors,
        "exclusions": dict(exclusions),
    }


def technical_directions(row: Mapping[str, str]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    raw_top = str(row.get("top_signal_json") or "").strip()
    if raw_top:
        try:
            top = json.loads(raw_top)
            side = census.normalize_side(top.get("direction"))
            if side:
                output.append(
                    {
                        "source": "top_signal",
                        "side": side,
                        "eligible": bool(top.get("signal_eligible")),
                        "confidence": top.get("confidence"),
                        "family": top.get("family"),
                    }
                )
        except json.JSONDecodeError:
            pass

    raw_prospective = str(row.get("prospective_forecast_json") or "").strip()
    if raw_prospective:
        try:
            forecast = json.loads(raw_prospective)
            side = census.normalize_side(forecast.get("direction"))
            if side:
                output.append(
                    {
                        "source": "prospective_ranker",
                        "side": side,
                        "eligible": bool(forecast.get("passed_frozen_gate")),
                        "confidence": forecast.get("predicted_direction_confidence"),
                        "family": "executable_opportunity_ranking",
                    }
                )
        except json.JSONDecodeError:
            pass

    h1_side = census.normalize_side(row.get("independent_h1_consensus_side"))
    if h1_side:
        output.append(
            {
                "source": "independent_h1_consensus",
                "side": h1_side,
                "eligible": False,
                "confidence": None,
                "family": "four_family_h1_consensus",
            }
        )
    return output


def movement_side(row: Mapping[str, str]) -> int:
    label = str(row.get("selected_side_label") or "").lower()
    return 1 if label == "long" else -1 if label == "short" else 0


def load_moves(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def source_index_bounds(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[int | None, int | None]:
    """Return the exact source-time span required by the retained moves.

    The widest pre-move rule is the 24-hour official-policy state. During-move
    attribution ends at the selected move end. Loading source events outside
    that interval cannot affect any vote and only makes the comprehensive
    governance ledger progressively more expensive to audit.
    """

    starts: list[float] = []
    ends: list[float] = []
    for row in rows:
        if not census.truthy(row.get("factor_representative")):
            continue
        if not movement_side(row):
            continue
        start = census.parse_epoch(row.get("start_utc"))
        end = census.parse_epoch(row.get("end_utc"))
        if start is None or end is None:
            continue
        starts.append(start)
        ends.append(end)
    if not starts or not ends:
        return None, None
    return int(min(starts) - 24 * 60 * 60), int(max(ends))


def initial_refresh_delay_sec(
    output_json: Path,
    upstream_move_detail: Path,
    interval_sec: float,
    *,
    now_epoch: float | None = None,
) -> float:
    """Delay a periodic worker when its existing report already covers input."""

    interval = max(0.0, float(interval_sec))
    if interval <= 0.0:
        return 0.0
    try:
        output_mtime = output_json.stat().st_mtime
        upstream_mtime = upstream_move_detail.stat().st_mtime
    except OSError:
        return 0.0
    current = time.time() if now_epoch is None else float(now_epoch)
    age = max(0.0, current - output_mtime)
    if output_mtime < upstream_mtime or age >= interval:
        return 0.0
    return interval - age


def first_source_epoch(path: Path) -> int:
    db = census.open_readonly(path)
    has_causal_view = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='view' AND name='source_events_causal_v1'"
    ).fetchone() is not None
    source_event_relation = (
        "source_events_causal_v1" if has_causal_view else "source_events"
    )
    value = db.execute(
        f"SELECT MIN(effective_from_utc) FROM {source_event_relation}"
    ).fetchone()[0]
    db.close()
    parsed = census.parse_epoch(value)
    return int(parsed or 0)


def case_class(primary: Mapping[str, Any], during: Mapping[str, Any], actual: int) -> str:
    pre_side = int(primary.get("side") or 0)
    during_side = int(during.get("side") or 0)
    if pre_side == actual:
        return "pre_move_news_correct"
    if pre_side == -actual:
        return "pre_move_news_wrong"
    if int(primary.get("independent_directional_stories") or 0):
        return "pre_move_news_conflicted"
    if during_side == actual:
        return "during_move_news_correct"
    if during_side == -actual:
        return "during_move_news_wrong"
    if int(during.get("independent_directional_stories") or 0):
        return "during_move_news_conflicted"
    return "no_directional_news_mapping"


def side_label(value: int) -> str:
    return "long" if value > 0 else "short" if value < 0 else "conflicted_or_none"


def pct(value: int, total: int) -> str:
    return f"{(100.0 * value / total):.1f}%" if total else "n/a"


def pct_fraction(value: float | None) -> str:
    return "n/a" if value is None else f"{100.0 * value:.1f}%"


def wilson_lower_bound(successes: int, total: int, z: float = 1.959963984540054) -> float | None:
    """Return a two-sided 95% Wilson lower bound for a hit proportion."""

    if total <= 0:
        return None
    p = max(0.0, min(1.0, float(successes) / float(total)))
    z2 = z * z
    denominator = 1.0 + z2 / total
    center = p + z2 / (2.0 * total)
    radius = z * math.sqrt((p * (1.0 - p) + z2 / (4.0 * total)) / total)
    return max(0.0, (center - radius) / denominator)


def prior_price_reaction(
    candles: Sequence[Mapping[str, Any]],
    *,
    event_time: dt.datetime,
    decision_time: dt.datetime,
    predicted_side: int,
    pip_size: float,
    modeled_cost_pips: float,
) -> dict[str, Any]:
    """Classify how much of a news-aligned move occurred before a decision.

    Only completed observations strictly before ``decision_time`` are used.
    The result separates fresh/muted reactions from continuation entries and
    from cases where price had already rejected the mapped direction.
    """

    if predicted_side not in {-1, 1}:
        return {"state": "no_directional_news"}
    if event_time >= decision_time or len(candles) < 2:
        return {"state": "insufficient_price_history"}
    times = [row["timestamp"] for row in candles]
    start_index = bisect.bisect_left(times, event_time)
    end_index = bisect.bisect_left(times, decision_time) - 1
    if start_index < 0 or end_index <= start_index or end_index >= len(candles):
        return {"state": "insufficient_price_history"}
    start = candles[start_index]
    end = candles[end_index]
    if start["timestamp"] >= decision_time or end["timestamp"] >= decision_time:
        return {"state": "timestamp_violation"}
    start_mid = 0.5 * (
        float(start.get("bid_open") or 0.0)
        + float(start.get("ask_open") or 0.0)
    )
    end_mid = 0.5 * (
        float(end.get("bid_open") or 0.0)
        + float(end.get("ask_open") or 0.0)
    )
    if pip_size <= 0 or start_mid <= 0 or end_mid <= 0:
        return {"state": "invalid_price"}
    market_pips = (end_mid - start_mid) / pip_size
    signed_pips = market_pips * predicted_side
    threshold = max(1.0, float(modeled_cost_pips))
    state = (
        "aligned_move_already_underway"
        if signed_pips > threshold
        else "mapped_direction_already_rejected"
        if signed_pips < -threshold
        else "fresh_or_muted_reaction"
    )
    return {
        "state": state,
        "signed_prior_reaction_pips": round(signed_pips, 6),
        "absolute_prior_reaction_pips": round(abs(market_pips), 6),
        "threshold_pips": round(threshold, 6),
        "event_utc": event_time.isoformat(),
        "last_completed_price_utc": end["timestamp"].isoformat(),
    }


def causal_m1_technical_state(
    candles: Sequence[Mapping[str, Any]],
    *,
    decision_time: dt.datetime,
    pip_size: float,
) -> dict[str, Any]:
    """Reconstruct a compact technical state from completed pre-decision M1 bars.

    OANDA M1 timestamps denote the bar open.  A bar is therefore admitted only
    when its full one-minute interval ended no later than ``decision_time``.
    This is a research diagnostic, not a replacement for a timestamped model
    forecast and never participates in eligibility or execution decisions.
    """

    if pip_size <= 0:
        return {"state": "invalid_pip_size", "research_only": True}
    completed = [
        row
        for row in candles
        if row.get("timestamp") is not None
        and row["timestamp"] + dt.timedelta(minutes=1) <= decision_time
    ]
    if len(completed) < 61:
        return {
            "state": "insufficient_completed_m1_history",
            "completed_bar_count": len(completed),
            "decision_utc": decision_time.isoformat(),
            "research_only": True,
        }
    rows = completed[-121:]
    mids = [0.5 * (float(row["bid_open"]) + float(row["ask_open"])) for row in rows]
    last = rows[-1]
    last_mid = mids[-1]

    def return_pips(minutes: int) -> float | None:
        if len(mids) <= minutes:
            return None
        return round((last_mid - mids[-1 - minutes]) / pip_size, 6)

    def sma(length: int) -> float:
        return sum(mids[-length:]) / float(length)

    def ema(length: int) -> float:
        alpha = 2.0 / (length + 1.0)
        value = mids[0]
        for price in mids[1:]:
            value = alpha * price + (1.0 - alpha) * value
        return value

    ranges: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    for row in rows[-60:]:
        high = 0.5 * (float(row["bid_high"]) + float(row["ask_high"]))
        low = 0.5 * (float(row["bid_low"]) + float(row["ask_low"]))
        highs.append(high)
        lows.append(low)
        ranges.append(max(0.0, high - low) / pip_size)
    prior_20_high = max(highs[-21:-1])
    prior_20_low = min(lows[-21:-1])
    range_low = min(lows)
    range_high = max(highs)
    range_position = (
        (last_mid - range_low) / (range_high - range_low)
        if range_high > range_low
        else 0.5
    )
    r5 = return_pips(5)
    r15 = return_pips(15)
    r60 = return_pips(60)
    breakout = (
        "up"
        if last_mid > prior_20_high
        else "down"
        if last_mid < prior_20_low
        else "inside"
    )
    exhaustion = (
        "upper_extreme"
        if range_position >= 0.9 and r15 is not None and r15 > 0
        else "lower_extreme"
        if range_position <= 0.1 and r15 is not None and r15 < 0
        else "none"
    )
    spread = (float(last["ask_open"]) - float(last["bid_open"])) / pip_size
    observed = last["timestamp"] + dt.timedelta(minutes=1)
    return {
        "state": "available",
        "source": "all68_completed_m1_archive",
        "decision_utc": decision_time.isoformat(),
        "last_bar_open_utc": last["timestamp"].isoformat(),
        "observed_utc": observed.isoformat(),
        "observation_age_seconds": round((decision_time - observed).total_seconds(), 6),
        "completed_bar_count": len(completed),
        "return_5m_pips": r5,
        "return_15m_pips": r15,
        "return_60m_pips": r60,
        "velocity_5m_pips_per_min": round(float(r5 or 0.0) / 5.0, 6),
        "range_position_60m": round(range_position, 6),
        "sma5_minus_sma20_pips": round((sma(5) - sma(20)) / pip_size, 6),
        "sma20_minus_sma60_pips": round((sma(20) - sma(60)) / pip_size, 6),
        "ema5_minus_ema20_pips": round((ema(5) - ema(20)) / pip_size, 6),
        "atr14_pips": round(sum(ranges[-14:]) / 14.0, 6),
        "breakout_20m": breakout,
        "exhaustion_60m": exhaustion,
        "spread_pips": round(spread, 6),
        "research_only": True,
        "execution_eligible": False,
        "is_model_forecast": False,
    }


def technical_incremental_table(
    cases: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str]] = Counter()
    for case in cases:
        news_state = (
            "no_directional_news"
            if str(case.get("pre30_side")) == "conflicted_or_none"
            else "news_correct"
            if bool(case.get("pre30_correct"))
            else "news_wrong"
        )
        technical_state = (
            "technical_unavailable"
            if not bool(case.get("technical_available"))
            else "technical_correct"
            if bool(case.get("technical_confirmed"))
            else "technical_wrong"
        )
        counts[(news_state, technical_state)] += 1
    return [
        {
            "news_state": news_state,
            "technical_state": technical_state,
            "count": counts[(news_state, technical_state)],
        }
        for news_state in (
            "news_correct",
            "news_wrong",
            "no_directional_news",
        )
        for technical_state in (
            "technical_correct",
            "technical_wrong",
            "technical_unavailable",
        )
    ]


def top_story_outcomes(
    cases: Sequence[Mapping[str, Any]],
    dimension: str,
) -> list[dict[str, Any]]:
    groups: dict[str, Counter[str]] = {}
    net_room: dict[str, list[float]] = {}
    for case in cases:
        stories = list(case.get("_top_pre") or [])
        if not stories:
            continue
        label = str(stories[0].get(dimension) or "unknown")
        bucket = groups.setdefault(label, Counter())
        bucket["moves"] += 1
        bucket["correct"] += int(bool(case.get("pre30_correct")))
        bucket["liquid_major"] += int(bool(case.get("liquid_major")))
        bucket["fresh_or_muted"] += int(
            str(case.get("prior_reaction_state")) == "fresh_or_muted_reaction"
        )
        net_room.setdefault(label, []).append(
            float(case.get("endpoint_after_cost_pips") or 0.0)
        )
    output = [
        {
            "label": label,
            **dict(counts),
            "average_selected_move_net_room_pips": round(
                sum(net_room[label]) / len(net_room[label]), 6
            ),
        }
        for label, counts in groups.items()
    ]
    output.sort(key=lambda row: (-int(row["moves"]), str(row["label"])))
    return output


def write_csv(path: Path, cases: Sequence[Mapping[str, Any]]) -> None:
    fields = [
        "move_id",
        "factor_episode_id",
        "inventory",
        "start_utc",
        "end_utc",
        "instrument",
        "horizon_min",
        "actual_side",
        "gross_magnitude_pips",
        "endpoint_after_cost_pips",
        "modeled_cost_pips",
        "liquid_major",
        "case_class",
        "pre30_side",
        "pre30_correct",
        "pre30_story_count",
        "during_side",
        "during_correct",
        "during_story_count",
        "strict_pre30_side",
        "strict_pre30_correct",
        "official120_side",
        "official120_correct",
        "official120_story_count",
        "official_policy24h_side",
        "official_policy24h_correct",
        "official_policy24h_story_count",
        "technical_available",
        "technical_confirmed",
        "existing_gap",
        "top_pre_news_json",
        "top_during_news_json",
        "technical_directions_json",
        "causal_m1_technical_state_json",
        "top_official_news_json",
        "top_official_policy_state_json",
        "prior_reaction_state",
        "signed_prior_reaction_pips",
        "prior_reaction_threshold_pips",
        "prior_reaction_event_utc",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for case in cases:
            writer.writerow({key: case.get(key, "") for key in fields})


def render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Move-First News Case Audit",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only. Moves enter independently of whether any model forecast them. News is assessed from knowledge-time records; technical forecasts are secondary confirmation only.",
        "",
        f"- Independent direction-scorable moves within retained source history: **{payload['case_count']:,}**",
        f"- Primary pre-move window: **{payload['primary_window_minutes']} minutes**",
        f"- Execution candidates created: **0**",
        "",
        "## Pre-move news rule comparison",
        "",
        "| Rule | Covered moves | Correct | Direction accuracy | 95% lower bound |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in payload["rule_results"]:
        lines.append(
            f"| {row['rule']} | {row['covered']:,} | {row['correct']:,} | "
            f"{pct(row['correct'], row['covered'])} | "
            f"{pct_fraction(row['wilson_lower_bound_95'])} |"
        )
    lines.extend(
        [
            "",
            "### Liquid major-currency subset",
            "",
            f"Independent moves: **{payload['liquid_major_case_count']:,}**",
            "",
            "| Rule | Covered moves | Correct | Direction accuracy | 95% lower bound |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in payload["liquid_major_rule_results"]:
        lines.append(
            f"| {row['rule']} | {row['covered']:,} | {row['correct']:,} | "
            f"{pct(row['correct'], row['covered'])} | "
            f"{pct_fraction(row['wilson_lower_bound_95'])} |"
        )
    lines.extend(
        [
            "",
            "These are movement-conditioned discovery statistics, not tradable win rates. Every audited row was selected because a large move later occurred.",
            "",
            "## Case disposition",
            "",
            "| Class | Independent moves |",
            "|---|---:|",
        ]
    )
    for row in payload["case_classes"]:
        lines.append(f"| {row['case_class']} | {row['count']:,} |")
    lines.extend(
        [
            "",
            "## Was the mapped information already in price?",
            "",
            "| Knowledge-time price state | Moves | News direction correct |",
            "|---|---:|---:|",
        ]
    )
    for row in payload["prior_reaction_results"]:
        lines.append(
            f"| {row['state']} | {row['count']:,} | "
            f"{pct(row['news_correct'], row['count'])} |"
        )
    lines.extend(
        [
            "",
            "The price state uses only M1 observations completed before the selected move start. `aligned_move_already_underway` is a continuation/late-entry condition, not fresh causal prediction.",
            "",
            "## Technical confirmation cross-check",
            "",
            "| News state | Technical state | Moves |",
            "|---|---|---:|",
        ]
    )
    for row in payload["technical_incremental_table"]:
        lines.append(
            f"| {row['news_state']} | {row['technical_state']} | {row['count']:,} |"
        )
    lines.extend(
        [
            "",
            "## Strongest pre-move story by source population",
            "",
            "| Population | Moves | Correct | Accuracy | Fresh/muted |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in payload["source_population_results"]:
        lines.append(
            f"| {row['label']} | {row['moves']:,} | {row['correct']:,} | "
            f"{pct(row['correct'], row['moves'])} | {row['fresh_or_muted']:,} |"
        )
    lines.extend(
        [
            "",
            "## Strongest pre-move story by event type",
            "",
            "| Event type | Moves | Correct | Accuracy | Fresh/muted |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in payload["event_type_results"]:
        lines.append(
            f"| {row['label']} | {row['moves']:,} | {row['correct']:,} | "
            f"{pct(row['correct'], row['moves'])} | {row['fresh_or_muted']:,} |"
        )
    lines.extend(
        [
            "",
            "## Largest cases",
            "",
            "| UTC | Pair | Horizon | Move | Net room | News disposition | Pre-news | Technical confirmation | Strongest retained pre-move headline |",
            "|---|---|---:|---|---:|---|---|---|---|",
        ]
    )
    for case in payload["largest_cases"]:
        headline = str(case.get("headline") or "").replace("|", "/")
        lines.append(
            f"| {case['start_utc']} | {case['instrument']} | {case['horizon_min']}m | "
            f"{case['actual_side']} | {case['endpoint_after_cost_pips']:.3f} | "
            f"{case['case_class']} | {case['pre30_side']} | "
            f"{case['technical_status']} | {headline} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation boundaries",
            "",
            "- Pre-move news means the source event was retained as known before the move's selected start. It does not prove the event caused the move.",
            "- Retrospective market recaps and context-only stories are excluded from directional voting.",
            "- Syndicated or repeated headlines receive one vote.",
            "- The complete case CSV retains every independent move in source-history scope, including moves with no model forecast.",
            "- Results cannot change lifecycle state, promotion, authorization, or execution.",
            "",
            f"Detail: `{payload['detail_csv']}`",
            "",
            "Execution decision remains `no_trade`.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    move_detail: Path = MOVE_DETAIL,
    source_database: Path = SOURCE_DB,
    output_json: Path = OUTPUT_JSON,
    output_md: Path = OUTPUT_MD,
    output_csv: Path = OUTPUT_CSV,
    candle_root: Path = DEFAULT_CANDLE_ROOT,
    instrument_metadata_path: Path = DEFAULT_INSTRUMENT_METADATA,
) -> dict[str, Any]:
    rows = load_moves(move_detail)
    minimum_source_epoch, maximum_source_epoch = source_index_bounds(rows)
    source_index, source_highwater = census.load_source_index(
        source_database,
        minimum_effective_epoch=minimum_source_epoch,
        maximum_effective_epoch=maximum_source_epoch,
    )
    source_start = first_source_epoch(source_database)
    pip_sizes = news_backtest.load_pip_sizes(instrument_metadata_path)
    candle_cache: dict[str, list[dict[str, Any]]] = {}
    cases: list[dict[str, Any]] = []
    rule_counts: dict[str, Counter[str]] = {
        f"all_directional_{window}m": Counter() for window in WINDOWS_MINUTES
    }
    rule_counts.update(
        {
            "strict_forward_30m": Counter(),
            "high_confidence_30m": Counter(),
            "official_only_120m": Counter(),
            "official_policy_state_24h": Counter(),
        }
    )
    liquid_rule_counts: dict[str, Counter[str]] = {
        key: Counter() for key in rule_counts
    }

    for row in rows:
        actual = movement_side(row)
        if not actual or not census.truthy(row.get("factor_representative")):
            continue
        entry = census.parse_epoch(row.get("start_utc"))
        end = census.parse_epoch(row.get("end_utc"))
        if entry is None or end is None or entry < source_start:
            continue
        base, quote = str(row["instrument"]).split("_", 1)
        liquid_major = (
            base in census.MAJOR_CURRENCIES
            and quote in census.MAJOR_CURRENCIES
            and float(row.get("modeled_cost_pips") or 0.0) <= 5.0
        )
        windows: dict[int, dict[str, Any]] = {}
        for window in WINDOWS_MINUTES:
            events = census.relevant_source_events(
                source_index, (base, quote), entry - window * 60, entry, True
            )
            vote = directional_vote(events, base, quote, entry)
            windows[window] = vote
            key = f"all_directional_{window}m"
            if int(vote["side"]):
                rule_counts[key]["covered"] += 1
                rule_counts[key]["correct"] += int(int(vote["side"]) == actual)
                if liquid_major:
                    liquid_rule_counts[key]["covered"] += 1
                    liquid_rule_counts[key]["correct"] += int(
                        int(vote["side"]) == actual
                    )

        primary_events = census.relevant_source_events(
            source_index, (base, quote), entry - 30 * 60, entry, True
        )
        strict = directional_vote(
            primary_events, base, quote, entry, strict_forward=True
        )
        high_confidence = directional_vote(
            primary_events, base, quote, entry, minimum_confidence=0.5
        )
        official_events = census.relevant_source_events(
            source_index, (base, quote), entry - 120 * 60, entry, True
        )
        official = directional_vote(
            official_events, base, quote, entry, official_only=True
        )
        official_policy_events = census.relevant_source_events(
            source_index, (base, quote), entry - 1440 * 60, entry, True
        )
        official_policy = directional_vote(
            official_policy_events,
            base,
            quote,
            entry,
            official_only=True,
            policy_only=True,
            age_decay_half_life_minutes=720.0,
        )
        for key, vote in (
            ("strict_forward_30m", strict),
            ("high_confidence_30m", high_confidence),
            ("official_only_120m", official),
            ("official_policy_state_24h", official_policy),
        ):
            if int(vote["side"]):
                rule_counts[key]["covered"] += 1
                rule_counts[key]["correct"] += int(int(vote["side"]) == actual)
                if liquid_major:
                    liquid_rule_counts[key]["covered"] += 1
                    liquid_rule_counts[key]["correct"] += int(
                        int(vote["side"]) == actual
                    )

        during_events = census.relevant_source_events(
            source_index, (base, quote), entry + 1, end, False
        )
        during = directional_vote(during_events, base, quote, entry)
        # The case label is intended to answer whether a usable pre-move news
        # signal existed.  The broad window remains in the aggregate
        # diagnostics, but it may contain retrospective price recaps,
        # uncorroborated research scores, or other deliberately non-publishable
        # context.  Using it for the case label made hindsight price reports
        # look like successful forecasts.  Bind the case-level diagnosis to
        # the actual causal/publishable vote instead.
        primary = strict
        technical = technical_directions(row)
        technical_confirmed = any(int(item["side"]) == actual for item in technical)
        instrument = str(row["instrument"])
        if instrument not in candle_cache:
            candle_path = candle_root / f"{instrument}_M1.csv"
            candle_cache[instrument] = (
                news_backtest.load_candles(
                    candle_path,
                    since=dt.datetime.fromtimestamp(
                        int(minimum_source_epoch or entry) + 21 * 60 * 60,
                        tz=dt.timezone.utc,
                    ),
                )
                if candle_path.exists()
                else []
            )
        causal_technical = causal_m1_technical_state(
            candle_cache[instrument],
            decision_time=dt.datetime.fromtimestamp(entry, tz=dt.timezone.utc),
            pip_size=float(
                pip_sizes.get(instrument)
                or fallback_pip_size(instrument)
            ),
        )
        classification = case_class(primary, during, actual)
        top_pre = list(primary["stories"][:5])
        top_during = list(during["stories"][:5])
        prior_reaction: dict[str, Any] = {"state": "no_directional_news"}
        if int(primary["side"]) and top_pre:
            event_epoch = census.parse_epoch(top_pre[0].get("effective_from_utc"))
            if event_epoch is not None:
                if str(row["instrument"]) not in candle_cache:
                    candle_path = candle_root / f"{row['instrument']}_M1.csv"
                    candle_cache[str(row["instrument"])] = (
                        news_backtest.load_candles(
                            candle_path,
                            since=dt.datetime.fromtimestamp(
                                source_start - 3600, tz=dt.timezone.utc
                            ),
                        )
                        if candle_path.exists()
                        else []
                    )
                prior_reaction = prior_price_reaction(
                    candle_cache[str(row["instrument"])],
                    event_time=dt.datetime.fromtimestamp(
                        event_epoch, tz=dt.timezone.utc
                    ),
                    decision_time=dt.datetime.fromtimestamp(
                        entry, tz=dt.timezone.utc
                    ),
                    predicted_side=int(primary["side"]),
                    pip_size=float(
                        pip_sizes.get(str(row["instrument"]))
                        or fallback_pip_size(str(row["instrument"]))
                    ),
                    modeled_cost_pips=float(row.get("modeled_cost_pips") or 0.0),
                )
        case = {
            "move_id": row["move_id"],
            "factor_episode_id": row["factor_episode_id"],
            "inventory": row["inventory"],
            "start_utc": row["start_utc"],
            "end_utc": row["end_utc"],
            "instrument": row["instrument"],
            "horizon_min": int(float(row["horizon_min"])),
            "actual_side": side_label(actual),
            "gross_magnitude_pips": float(row["gross_magnitude_pips"] or 0.0),
            "endpoint_after_cost_pips": float(
                row["endpoint_after_cost_pips"] or 0.0
            ),
            "modeled_cost_pips": float(row["modeled_cost_pips"] or 0.0),
            "liquid_major": liquid_major,
            "case_class": classification,
            "pre30_side": side_label(int(primary["side"])),
            "pre30_correct": int(primary["side"]) == actual,
            "pre30_story_count": int(primary["independent_directional_stories"]),
            "during_side": side_label(int(during["side"])),
            "during_correct": int(during["side"]) == actual,
            "during_story_count": int(during["independent_directional_stories"]),
            "strict_pre30_side": side_label(int(strict["side"])),
            "strict_pre30_correct": int(strict["side"]) == actual,
            "official120_side": side_label(int(official["side"])),
            "official120_correct": int(official["side"]) == actual,
            "official120_story_count": int(
                official["independent_directional_stories"]
            ),
            "official_policy24h_side": side_label(int(official_policy["side"])),
            "official_policy24h_correct": int(official_policy["side"]) == actual,
            "official_policy24h_story_count": int(
                official_policy["independent_directional_stories"]
            ),
            "technical_available": bool(technical),
            "technical_confirmed": technical_confirmed,
            "causal_m1_technical_state_json": census.canonical_json(causal_technical),
            "existing_gap": row["primary_gap"],
            "top_pre_news_json": census.canonical_json(top_pre),
            "top_during_news_json": census.canonical_json(top_during),
            "technical_directions_json": census.canonical_json(technical),
            "top_official_news_json": census.canonical_json(
                list(official["stories"][:5])
            ),
            "top_official_policy_state_json": census.canonical_json(
                list(official_policy["stories"][:5])
            ),
            "prior_reaction_state": prior_reaction.get("state"),
            "signed_prior_reaction_pips": prior_reaction.get(
                "signed_prior_reaction_pips"
            ),
            "prior_reaction_threshold_pips": prior_reaction.get(
                "threshold_pips"
            ),
            "prior_reaction_event_utc": prior_reaction.get("event_utc"),
            "_top_pre": top_pre,
        }
        cases.append(case)

    cases.sort(
        key=lambda row: (
            float(row["endpoint_after_cost_pips"]),
            float(row["gross_magnitude_pips"]),
        ),
        reverse=True,
    )
    write_csv(output_csv, cases)
    case_counts = Counter(str(case["case_class"]) for case in cases)
    rule_results = [
        {
            "rule": rule,
            "covered": counts["covered"],
            "correct": counts["correct"],
            "accuracy": (
                counts["correct"] / counts["covered"]
                if counts["covered"]
                else None
            ),
            "wilson_lower_bound_95": wilson_lower_bound(
                counts["correct"], counts["covered"]
            ),
        }
        for rule, counts in rule_counts.items()
    ]
    prior_reaction_counts: dict[str, Counter[str]] = {}
    for case in cases:
        state = str(case.get("prior_reaction_state") or "unknown")
        bucket = prior_reaction_counts.setdefault(state, Counter())
        bucket["count"] += 1
        bucket["news_correct"] += int(bool(case.get("pre30_correct")))
    largest_cases = []
    headline_cases = [
        case
        for case in cases
        if bool(case["liquid_major"]) and int(case["horizon_min"]) <= 60
    ]
    for case in headline_cases[:75]:
        top = list(case.get("_top_pre") or [])
        largest_cases.append(
            {
                "start_utc": case["start_utc"],
                "instrument": case["instrument"],
                "horizon_min": case["horizon_min"],
                "actual_side": case["actual_side"],
                "endpoint_after_cost_pips": case["endpoint_after_cost_pips"],
                "case_class": case["case_class"],
                "pre30_side": case["pre30_side"],
                "technical_status": (
                    "confirmed"
                    if case["technical_confirmed"]
                    else "contradicted_or_absent"
                    if case["technical_available"]
                    else "unavailable"
                ),
                "headline": top[0]["headline"] if top else "",
            }
        )
    payload = {
        "schema_version": 2,
        "research_id": "move_first_news_case_audit_v2",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
        "case_count": len(cases),
        "liquid_major_case_count": sum(bool(case["liquid_major"]) for case in cases),
        "primary_window_minutes": 30,
        "source_history_start_utc": census.iso_epoch(source_start),
        "source_history_highwater_utc": source_highwater,
        "case_classes": [
            {"case_class": name, "count": count}
            for name, count in case_counts.most_common()
        ],
        "rule_results": rule_results,
        "liquid_major_rule_results": [
            {
                "rule": rule,
                "covered": counts["covered"],
                "correct": counts["correct"],
                "accuracy": (
                    counts["correct"] / counts["covered"]
                    if counts["covered"]
                    else None
                ),
                "wilson_lower_bound_95": wilson_lower_bound(
                    counts["correct"], counts["covered"]
                ),
            }
            for rule, counts in liquid_rule_counts.items()
        ],
        "technical_coverage": sum(bool(case["technical_available"]) for case in cases),
        "technical_confirmed": sum(bool(case["technical_confirmed"]) for case in cases),
        "causal_m1_technical_coverage": sum(
            json.loads(str(case["causal_m1_technical_state_json"])).get("state")
            == "available"
            for case in cases
        ),
        "technical_incremental_table": technical_incremental_table(cases),
        "liquid_major_technical_incremental_table": technical_incremental_table(
            [case for case in cases if bool(case.get("liquid_major"))]
        ),
        "prior_reaction_results": [
            {
                "state": state,
                "count": counts["count"],
                "news_correct": counts["news_correct"],
            }
            for state, counts in sorted(prior_reaction_counts.items())
        ],
        "source_population_results": top_story_outcomes(
            cases, "source_population"
        ),
        "source_id_results": top_story_outcomes(cases, "source_id"),
        "event_type_results": top_story_outcomes(cases, "event_type"),
        "largest_cases": largest_cases,
        "detail_csv": str(output_csv),
        "limitations": [
            "Movement-conditioned discovery audit; not a backtest or tradable win rate.",
            "A retained pre-move story does not by itself establish that the story caused the move.",
            "Source history begins later than movement history, so older moves remain outside news-scoring scope.",
            "Technical directions are secondary annotations and never determine case inclusion.",
            "Reconstructed M1 technical state uses only bars completed before the move clock; it is diagnostic state, not a historical model forecast.",
        ],
    }
    census.atomic_text(output_json, json.dumps(payload, indent=2, sort_keys=True))
    census.atomic_text(output_md, render_markdown(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--move-detail", type=Path, default=MOVE_DETAIL)
    parser.add_argument("--source-database", type=Path, default=SOURCE_DB)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    parser.add_argument("--output-csv", type=Path, default=OUTPUT_CSV)
    parser.add_argument("--candle-root", type=Path, default=DEFAULT_CANDLE_ROOT)
    parser.add_argument(
        "--instrument-metadata",
        type=Path,
        default=DEFAULT_INSTRUMENT_METADATA,
    )
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    args = parser.parse_args()
    stop = time.monotonic() + max(0.0, float(args.duration_sec))
    initial_delay = initial_refresh_delay_sec(
        args.output_json,
        args.move_detail,
        args.interval_sec,
    )
    if initial_delay > 0.0:
        remaining = max(0.0, stop - time.monotonic())
        time.sleep(min(initial_delay, remaining))
        # A bounded one-shot monitor must not perform an expensive refresh
        # after its requested lifetime merely because the existing report was
        # already fresh when it started.
        if initial_delay >= remaining:
            return 0
    while True:
        payload = run(
            move_detail=args.move_detail,
            source_database=args.source_database,
            output_json=args.output_json,
            output_md=args.output_md,
            output_csv=args.output_csv,
            candle_root=args.candle_root,
            instrument_metadata_path=args.instrument_metadata,
        )
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "case_count": payload["case_count"],
                    "execution_decision": payload["execution_decision"],
                },
                indent=2,
            ),
            flush=True,
        )
        if float(args.interval_sec) <= 0.0 or time.monotonic() >= stop:
            return 0
        time.sleep(min(float(args.interval_sec), max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
