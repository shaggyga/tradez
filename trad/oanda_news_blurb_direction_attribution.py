#!/usr/bin/env python3
"""Evaluate blurb/source currency direction against executable FX movements.

The recovered blurb project selected movements first and researched their
information environments second.  This module preserves that boundary while
making the directional contract explicit:

    pair impulse = base-currency strength - quote-currency strength

Only source events already known at the episode entry are eligible for the
causal diagnostic arms.  In-window/exit blurbs are never read.  Results remain
movement-conditioned discovery evidence and have no execution, authorization,
or promotion surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "news_blurb_direction_attribution_v1.json"
MOVEMENT_DB = ROOT / "data" / "oanda_training_manager" / "state" / "movement_news_episode_research_v1.sqlite"
SOURCE_DB = ROOT / "data" / "oanda_training_manager" / "state" / "source_governance_v1.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "news_blurb_direction_attribution" / "NEWS_BLURB_DIRECTION_ATTRIBUTION_V1.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "news_blurb_direction_attribution" / "NEWS_BLURB_DIRECTION_ATTRIBUTION_V1.md"


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _parse_utc(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def extract_event_view(payload_json: Any, *, use_research_scores: bool = False) -> dict[str, Any]:
    """Return the normalized directional fields without guessing from prose."""

    outer = _json(payload_json)
    raw = _json(outer.get("raw_payload")) or outer
    published = _json(raw.get("currency_scores"))
    research = _json(raw.get("research_currency_scores"))
    selected = research if use_research_scores and research else published
    scores = {
        str(currency).upper(): max(-1.0, min(1.0, _finite(score)))
        for currency, score in selected.items()
        if str(currency).strip() and abs(_finite(score)) > 1e-12
    }
    return {
        "scores": scores,
        "published_scores_available": bool(published),
        "research_scores_used": bool(use_research_scores and research),
        "headline": str(raw.get("headline") or outer.get("headline") or ""),
        "category": str(raw.get("category") or outer.get("event_type") or "unclassified").lower(),
        "topic_action": str(raw.get("topic_action") or ""),
        "confidence": max(0.0, min(1.0, _finite(raw.get("directional_confidence"), 0.0))),
        "source_quality": max(0.0, min(1.0, _finite(raw.get("source_quality"), 0.5))),
        "source_grade": str(raw.get("directional_source_grade") or "unknown"),
        "source_verified": bool(raw.get("source_verified")),
        "source_direct": bool(raw.get("source_direct")),
        "official_policy_release": bool(raw.get("official_policy_release")),
        "directional_evidence": bool(raw.get("directional_evidence")),
        "directional_publish_eligible": bool(raw.get("directional_publish_eligible")),
        "semantic_claim_conflict": bool(raw.get("semantic_claim_conflict")),
        "reports_prior_market_move": bool(raw.get("reports_prior_market_move")),
        "context_only": bool(raw.get("context_only")),
        "reaction_horizon_min": max(1.0, _finite(raw.get("estimated_reaction_horizon_minutes"), 60.0)),
    }


def pair_score(scores: Mapping[str, Any], base: str, quote: str) -> float:
    """Positive means the base strengthens relative to the quote."""

    return _finite(scores.get(str(base).upper())) - _finite(scores.get(str(quote).upper()))


def event_weight(
    view: Mapping[str, Any],
    *,
    age_minutes: float,
    episode_horizon_min: int,
    config: Mapping[str, Any],
) -> float:
    grade_weights = config.get("source_grade_weights") or {}
    grade = str(view.get("source_grade") or "unknown")
    grade_weight = _finite(grade_weights.get(grade), _finite(grade_weights.get("unknown"), 0.25))
    quality = max(0.05, _finite(view.get("source_quality"), 0.5))
    confidence = max(0.05, _finite(view.get("confidence"), 0.0))
    expected = max(1.0, _finite(view.get("reaction_horizon_min"), 60.0))
    recency = math.exp(-math.log(2.0) * max(0.0, age_minutes) / expected)
    horizon_match = math.exp(-abs(math.log(max(1.0, episode_horizon_min) / expected)))
    return grade_weight * quality * confidence * recency * horizon_match


def executable_net_pips(episode: Mapping[str, Any], direction: int) -> float:
    entry_bid = _finite(episode.get("entry_bid"))
    entry_ask = _finite(episode.get("entry_ask"))
    exit_bid = _finite(episode.get("exit_bid"))
    exit_ask = _finite(episode.get("exit_ask"))
    spread_pips = _finite(episode.get("entry_spread_pips"))
    pip = abs(entry_ask - entry_bid) / spread_pips if spread_pips > 0 else 0.0
    if pip <= 0:
        pip = 0.01 if max(abs(entry_bid), abs(entry_ask)) >= 20 else 0.0001
    return (exit_bid - entry_ask) / pip if direction > 0 else (entry_bid - exit_ask) / pip


def normalize_source_row(
    row: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    use_research_scores: bool,
) -> dict[str, Any] | None:
    if str(row.get("relation") or "") != "pre_entry_causal" or int(row.get("causal_entry_eligible") or 0) != 1:
        return None
    effective = _parse_utc(row.get("effective_from_utc"))
    entry = _parse_utc(row.get("entry_utc"))
    if effective is None or entry is None or effective > entry:
        return None
    age_minutes = (entry - effective).total_seconds() / 60.0
    if age_minutes < 0 or age_minutes > _finite(config.get("maximum_event_age_minutes"), 360.0):
        return None
    view = extract_event_view(row.get("payload_json"), use_research_scores=use_research_scores)
    if view["semantic_claim_conflict"] or view["reports_prior_market_move"]:
        return None
    raw_pair_score = pair_score(view["scores"], str(row.get("base_currency")), str(row.get("quote_currency")))
    if abs(raw_pair_score) < _finite(config.get("minimum_absolute_pair_score"), 0.12):
        return None
    weight = event_weight(
        view,
        age_minutes=age_minutes,
        episode_horizon_min=int(row.get("horizon_min") or 0),
        config=config,
    )
    weighted_score = raw_pair_score * weight
    if abs(weighted_score) < _finite(config.get("minimum_weighted_pair_score"), 0.03):
        return None
    return {
        **dict(row),
        **view,
        "age_minutes": round(age_minutes, 6),
        "raw_pair_score": round(raw_pair_score, 9),
        "weight": round(weight, 9),
        "weighted_pair_score": round(weighted_score, 9),
        "story_key": str(row.get("story_cluster_id") or row.get("source_event_id") or ""),
    }


def deduplicate_stories(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """One story gets one vote; prefer the strongest direct/verified observation."""

    best: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        key = str(row.get("story_key") or row.get("source_event_id") or "")
        if not key:
            continue
        rank = (
            int(bool(row.get("source_verified"))),
            int(bool(row.get("source_direct"))),
            abs(_finite(row.get("weighted_pair_score"))),
            str(row.get("source_event_id") or ""),
        )
        current = best.get(key)
        if current is None:
            best[key] = row
            continue
        current_rank = (
            int(bool(current.get("source_verified"))),
            int(bool(current.get("source_direct"))),
            abs(_finite(current.get("weighted_pair_score"))),
            str(current.get("source_event_id") or ""),
        )
        if rank > current_rank:
            best[key] = row
    return [best[key] for key in sorted(best)]


def aggregate_episode(rows: Sequence[Mapping[str, Any]], episode: Mapping[str, Any]) -> dict[str, Any] | None:
    stories = deduplicate_stories(rows)
    if not stories:
        return None
    score = sum(_finite(row.get("weighted_pair_score")) for row in stories)
    if abs(score) < 1e-12:
        return None
    direction = 1 if score > 0 else -1
    realized = 1 if _finite(episode.get("signed_mid_move_pips")) > 0 else -1
    return {
        "episode_id": str(episode.get("episode_id")),
        "entry_utc": str(episode.get("entry_utc")),
        "instrument": str(episode.get("instrument")),
        "base_currency": str(episode.get("base_currency")),
        "quote_currency": str(episode.get("quote_currency")),
        "horizon_min": int(episode.get("horizon_min") or 0),
        "market_episode_id": str(episode.get("market_episode_id")),
        "signed_currency_factor": str(episode.get("signed_currency_factor")),
        "story_count": len(stories),
        "entry_spread_pips": _finite(episode.get("entry_spread_pips")),
        "source_event_ids": [str(row.get("source_event_id")) for row in stories],
        "event_types": sorted({str(row.get("category") or row.get("event_type") or "unclassified") for row in stories}),
        "headlines": [str(row.get("headline") or "") for row in stories[:3]],
        "score": round(score, 9),
        "predicted_direction": "LONG" if direction > 0 else "SHORT",
        "realized_direction": "LONG" if realized > 0 else "SHORT",
        "direction_hit": direction == realized,
        "predicted_after_cost_pips": round(executable_net_pips(episode, direction), 6),
        "flipped_after_cost_pips": round(executable_net_pips(episode, -direction), 6),
        "gross_magnitude_pips": _finite(episode.get("gross_magnitude_pips")),
    }


def independent_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Select one executable representative per factor/market episode."""

    grouped: dict[tuple[Any, Any], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row.get("market_episode_id"), row.get("signed_currency_factor"))].append(row)
    return [
        dict(min(group, key=lambda row: (_finite(row.get("entry_spread_pips"), 1e9), -abs(_finite(row.get("score"))), str(row.get("instrument")))))
        for _key, group in sorted(grouped.items(), key=lambda item: (str(item[0][0]), str(item[0][1])))
    ]


def metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    effective = independent_rows(rows)
    values = [_finite(row.get("predicted_after_cost_pips")) for row in effective]
    flipped = [_finite(row.get("flipped_after_cost_pips")) for row in effective]
    ordered = sorted(values)
    trim = int(len(ordered) * 0.10)
    trimmed = ordered[trim: len(ordered) - trim] if trim and len(ordered) > 2 * trim else ordered
    pair_totals: dict[str, float] = defaultdict(float)
    for row in effective:
        pair_totals[str(row.get("instrument"))] += _finite(row.get("predicted_after_cost_pips"))
    best_pair, best_pair_total = max(pair_totals.items(), key=lambda item: item[1], default=(None, 0.0))
    return {
        "raw_episode_rows": len(rows),
        "episodes": len(effective),
        "independent_market_factors": len(effective),
        "direction_accuracy": round(sum(bool(row.get("direction_hit")) for row in effective) / len(effective), 6) if effective else None,
        "after_cost_win_rate": round(sum(value > 0 for value in values) / len(values), 6) if values else None,
        "mean_after_cost_pips": round(sum(values) / len(values), 6) if values else None,
        "median_after_cost_pips": round(ordered[len(ordered) // 2], 6) if values else None,
        "trimmed_10pct_mean_after_cost_pips": round(sum(trimmed) / len(trimmed), 6) if trimmed else None,
        "minimum_after_cost_pips": round(min(values), 6) if values else None,
        "maximum_after_cost_pips": round(max(values), 6) if values else None,
        "mean_flipped_after_cost_pips": round(sum(flipped) / len(flipped), 6) if flipped else None,
        "best_pair": best_pair,
        "best_pair_total_pips": round(best_pair_total, 6) if best_pair is not None else None,
    }


def liquidity_bucket(spread_pips: float, config: Mapping[str, Any]) -> str:
    buckets = config.get("liquidity_buckets") or {}
    if spread_pips <= _finite(buckets.get("liquid"), 2.5):
        return "liquid"
    if spread_pips <= _finite(buckets.get("moderate"), 5.0):
        return "moderate"
    if spread_pips <= _finite(buckets.get("wide"), 20.0):
        return "wide"
    return "extreme"


def _event_action_cells(
    source_rows: Sequence[Mapping[str, Any]],
    episode_by_id: Mapping[str, Mapping[str, Any]],
    split_utc: str,
    config: Mapping[str, Any],
) -> tuple[dict[tuple[str, int], str], dict[str, dict[str, Any]]]:
    grouped_scores: dict[tuple[str, int], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    realized_by_factor: dict[str, int] = {}
    for row in source_rows:
        episode = episode_by_id.get(str(row.get("episode_id")))
        if episode is None or str(episode.get("entry_utc")) >= split_utc:
            continue
        category = str(row.get("category") or row.get("event_type") or "unclassified")
        horizon = int(episode.get("horizon_min") or 0)
        independent = f"{episode.get('market_episode_id')}|{episode.get('signed_currency_factor')}"
        value = _finite(row.get("weighted_pair_score"))
        grouped_scores[(category, horizon)][independent] += value
        grouped_scores[("__all__", horizon)][independent] += value
        realized_by_factor[independent] = 1 if _finite(episode.get("signed_mid_move_pips")) > 0 else -1
    minimum_n = int(config.get("minimum_train_episodes_per_response_cell") or 20)
    threshold = _finite(config.get("minimum_train_direction_accuracy"), 0.57)
    actions: dict[tuple[str, int], str] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    for cell, factor_scores in sorted(grouped_scores.items()):
        hits = [
            (1 if score > 0 else -1) == realized_by_factor[independent]
            for independent, score in factor_scores.items()
            if abs(score) > 1e-12
        ]
        accuracy = sum(hits) / len(hits) if hits else None
        if len(hits) < minimum_n:
            actions[cell] = "abstain"
        else:
            actions[cell] = "follow" if accuracy is not None and accuracy >= threshold else "fade" if accuracy is not None and (1.0 - accuracy) >= threshold else "abstain"
        diagnostics[f"{cell[0]}|H{cell[1]}"] = {
            "independent_train_factors": len(hits),
            "raw_direction_accuracy": None if accuracy is None else round(accuracy, 6),
            "action": actions[cell],
            "minimum_train_factors": minimum_n,
            "accuracy_threshold": threshold,
        }
    return actions, diagnostics


def response_calibrated_holdout(
    source_rows: Sequence[Mapping[str, Any]],
    episode_by_id: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    epochs = sorted({str(episode.get("entry_utc")) for episode in episode_by_id.values()})
    if len(epochs) < 2:
        return [], {"split_utc": None, "cells": {}}
    index = min(len(epochs) - 1, max(1, int(len(epochs) * _finite(config.get("chronological_train_fraction"), 0.7))))
    split_utc = epochs[index]
    actions, cell_diagnostics = _event_action_cells(source_rows, episode_by_id, split_utc, config)
    by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in source_rows:
        episode = episode_by_id.get(str(raw.get("episode_id")))
        if episode is None or str(episode.get("entry_utc")) < split_utc:
            continue
        row = dict(raw)
        cell = (str(row.get("category") or row.get("event_type") or "unclassified"), int(episode.get("horizon_min") or 0))
        action = actions.get(cell, "abstain")
        action_basis = "event_type_horizon"
        if action == "abstain":
            action = actions.get(("__all__", cell[1]), "abstain")
            action_basis = "horizon_backoff"
        if action == "abstain":
            continue
        if action == "fade":
            row["weighted_pair_score"] = -_finite(row.get("weighted_pair_score"))
        row["response_action"] = action
        row["response_action_basis"] = action_basis
        by_episode[str(episode.get("episode_id"))].append(row)
    results = [
        result
        for episode_id, rows in by_episode.items()
        if (result := aggregate_episode(rows, episode_by_id[episode_id])) is not None
    ]
    serialized = {f"{category}|H{horizon}": action for (category, horizon), action in sorted(actions.items())}
    return results, {"split_utc": split_utc, "cells": serialized, "cell_diagnostics": cell_diagnostics}


def _open_rows(movement_db: Path, source_db: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    connection = sqlite3.connect(f"file:{movement_db.resolve().as_posix()}?mode=ro", uri=True, timeout=60)
    connection.row_factory = sqlite3.Row
    source_uri = f"file:{source_db.resolve().as_posix()}?mode=ro&immutable=1"
    connection.execute("ATTACH DATABASE ? AS source", (source_uri,))
    episodes = [dict(row) for row in connection.execute("SELECT * FROM movement_episodes ORDER BY entry_utc,episode_id")]
    rows = [
        dict(row)
        for row in connection.execute(
            """SELECT l.episode_id,l.source_event_id,l.relation,l.effective_from_utc,
                      l.source_id,l.source_population,l.event_type,l.story_cluster_id,
                      l.causal_entry_eligible,e.entry_utc,e.base_currency,e.quote_currency,
                      e.horizon_min,s.payload_json
                 FROM episode_source_links l
                 JOIN movement_episodes e ON e.episode_id=l.episode_id
                 JOIN source.source_events s ON s.source_event_id=l.source_event_id
                WHERE l.relation='pre_entry_causal' AND l.causal_entry_eligible=1
                ORDER BY e.entry_utc,l.episode_id,l.source_event_id"""
        )
    ]
    connection.close()
    return episodes, rows


def build(
    episodes: Sequence[Mapping[str, Any]],
    raw_source_rows: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    episode_by_id = {str(row.get("episode_id")): dict(row) for row in episodes}
    normalized_published = [
        row for raw in raw_source_rows
        if (row := normalize_source_row(raw, config, use_research_scores=False)) is not None
    ]
    normalized_research = [
        row for raw in raw_source_rows
        if (row := normalize_source_row(raw, config, use_research_scores=True)) is not None
    ]

    def evaluate(source_rows: Sequence[Mapping[str, Any]], official_only: bool = False) -> list[dict[str, Any]]:
        grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in source_rows:
            if official_only and not (str(row.get("source_population") or "").startswith("official") or bool(row.get("official_policy_release"))):
                continue
            grouped[str(row.get("episode_id"))].append(row)
        return [
            result
            for episode_id, rows in grouped.items()
            if episode_id in episode_by_id and (result := aggregate_episode(rows, episode_by_id[episode_id])) is not None
        ]

    published = evaluate(normalized_published)
    research = evaluate(normalized_research)
    official = evaluate(normalized_research, official_only=True)
    calibrated, calibration = response_calibrated_holdout(normalized_research, episode_by_id, config)
    arms = {
        "published_directional": published,
        "research_extended": research,
        "official_only": official,
        "response_calibrated": calibrated,
    }
    limit = int(config.get("maximum_examples_per_arm") or 20)
    return {
        "schema_version": 1,
        "research_id": str(config.get("research_id")),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "evidence_class": "movement_conditioned_direction_attribution_discovery",
        "movement_episode_count": len(episodes),
        "pre_entry_link_count": len(raw_source_rows),
        "normalized_published_link_count": len(normalized_published),
        "normalized_research_link_count": len(normalized_research),
        "calibration": calibration,
        "arms": {
            name: {
                "metrics": metrics(rows),
                "by_horizon": {
                    str(horizon): metrics([row for row in rows if int(row.get("horizon_min") or 0) == horizon])
                    for horizon in sorted({int(row.get("horizon_min") or 0) for row in rows})
                },
                "by_liquidity": {
                    bucket: metrics([
                        row for row in rows
                        if liquidity_bucket(_finite(row.get("entry_spread_pips")), config) == bucket
                    ])
                    for bucket in ("liquid", "moderate", "wide", "extreme")
                },
                "examples": sorted(rows, key=lambda row: (-abs(_finite(row.get("predicted_after_cost_pips"))), str(row.get("episode_id"))))[:limit],
            }
            for name, rows in arms.items()
        },
        "limitations": [
            "movement-selected sample cannot estimate unconditional trade expectancy",
            "archive has been inspected and is discovery evidence only",
            "semantic scores are hypotheses until confirmed prospectively",
            "response-calibrated cells use a chronological diagnostic holdout, not an untouched confirmation cohort",
        ],
    }


def run(
    config_path: Path = CONFIG,
    movement_db: Path = MOVEMENT_DB,
    source_db: Path = SOURCE_DB,
    output: Path = OUTPUT,
    report: Path = REPORT,
) -> dict[str, Any]:
    config = _json(config_path.read_text(encoding="utf-8"))
    episodes, source_rows = _open_rows(movement_db, source_db)
    payload = build(episodes, source_rows, config)
    payload.update({
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "config_sha256": _digest(config_path),
        "source_code_sha256": _digest(Path(__file__)),
    })
    _atomic(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# News-blurb currency-direction attribution", "",
        "Movement-conditioned discovery only. Currency strength is scored first; pair direction is base minus quote. No execution or promotion path exists.", "",
        f"- Movement episodes: **{payload['movement_episode_count']:,}**",
        f"- Pre-entry source links: **{payload['pre_entry_link_count']:,}**",
        f"- Published directional links retained: **{payload['normalized_published_link_count']:,}**",
        f"- Research-extended links retained: **{payload['normalized_research_link_count']:,}**", "",
        "| Arm | Raw rows | Effective factors | Direction | After-cost wins | Mean net | Median | Trimmed mean | Flipped mean |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, arm in payload["arms"].items():
        row = arm["metrics"]
        direction = "n/a" if row["direction_accuracy"] is None else f"{row['direction_accuracy']:.1%}"
        wins = "n/a" if row["after_cost_win_rate"] is None else f"{row['after_cost_win_rate']:.1%}"
        mean = "n/a" if row["mean_after_cost_pips"] is None else f"{row['mean_after_cost_pips']:+.3f}"
        flipped = "n/a" if row["mean_flipped_after_cost_pips"] is None else f"{row['mean_flipped_after_cost_pips']:+.3f}"
        median = "n/a" if row["median_after_cost_pips"] is None else f"{row['median_after_cost_pips']:+.3f}"
        trimmed = "n/a" if row["trimmed_10pct_mean_after_cost_pips"] is None else f"{row['trimmed_10pct_mean_after_cost_pips']:+.3f}"
        lines.append(f"| {name} | {row['raw_episode_rows']:,} | {row['independent_market_factors']:,} | {direction} | {wins} | {mean} | {median} | {trimmed} | {flipped} |")
    lines += [
        "", "## Liquidity/cost split", "",
        "| Arm | Bucket | Effective factors | Direction | Mean net | Median | Trimmed mean |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for name, arm in payload["arms"].items():
        for bucket, row in arm["by_liquidity"].items():
            direction = "n/a" if row["direction_accuracy"] is None else f"{row['direction_accuracy']:.1%}"
            mean = "n/a" if row["mean_after_cost_pips"] is None else f"{row['mean_after_cost_pips']:+.3f}"
            median = "n/a" if row["median_after_cost_pips"] is None else f"{row['median_after_cost_pips']:+.3f}"
            trimmed = "n/a" if row["trimmed_10pct_mean_after_cost_pips"] is None else f"{row['trimmed_10pct_mean_after_cost_pips']:+.3f}"
            lines.append(f"| {name} | {bucket} | {row['independent_market_factors']:,} | {direction} | {mean} | {median} | {trimmed} |")
    lines += ["", "The response-calibrated arm may follow, fade, or abstain by event class and horizon using only the earlier chronological segment. It remains diagnostic and cannot authorize a trade.", ""]
    _atomic(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--movement-db", type=Path, default=MOVEMENT_DB)
    parser.add_argument("--source-db", type=Path, default=SOURCE_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.config, args.movement_db, args.source_db, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
