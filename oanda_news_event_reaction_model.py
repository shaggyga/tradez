#!/usr/bin/env python3
"""Causal, episode-weighted news reaction specialist (research only).

This model learns whether a semantic FX direction should be followed, faded,
or ignored at each direct horizon.  It consumes the retained news replay where
signals are timestamped by first-seen time, never by an older publication time.
Each news episode receives one vote regardless of pair fan-out.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


HORIZONS_MIN = (5, 15, 30, 60, 120, 240)
SURPRISE_FIELDS = {
    "actual": ("actual", "actual_value", "economic_actual", "release_actual"),
    "consensus": (
        "consensus",
        "consensus_value",
        "economic_consensus",
        "forecast_value",
    ),
    "previous": ("previous", "previous_value", "economic_previous"),
    "surprise": ("surprise", "surprise_value", "standardized_surprise"),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def epoch(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def load_calls(report_path: Path, version: str = "cleaned_current") -> list[dict[str, Any]]:
    payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
    policy = payload.get("policy") or {}
    if not bool(policy.get("chronological_first_seen_only")):
        raise ValueError("news replay is not chronological-first-seen-only")
    output: list[dict[str, Any]] = []
    for raw in payload.get("scored_call_details") or []:
        if str(raw.get("version") or "") != version:
            continue
        timestamp = epoch(raw.get("signal_utc"))
        if timestamp is None:
            continue
        follow = finite(raw.get("pnl_pips"))
        # If follow = signed_mid_move - round_trip_cost, reversing the side is
        # -signed_mid_move - round_trip_cost.
        fade = -follow - max(0.0, finite(raw.get("entry_spread_pips"))) - max(
            0.0, finite(raw.get("exit_spread_pips"))
        )
        output.append(
            {
                **raw,
                "signal_epoch": timestamp,
                "follow_net_pips": follow,
                "fade_net_pips": fade,
            }
        )
    return output


def build_episode_rows(
    calls: Iterable[Mapping[str, Any]],
    cluster_minutes: int = 30,
) -> list[dict[str, Any]]:
    """Collapse pair fan-out and near-duplicate topics to independent episodes."""

    grouped: dict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in calls:
        horizon = int(finite(row.get("horizon_minutes")))
        topic_id = str(row.get("topic_id") or "")
        if horizon > 0 and topic_id:
            grouped[(horizon, topic_id)].append(row)
    topics: list[dict[str, Any]] = []
    for (horizon, topic_id), rows in grouped.items():
        times = [finite(row.get("signal_epoch")) for row in rows]
        topics.append(
            {
                "horizon_minutes": horizon,
                "topic_id": topic_id,
                "signal_epoch": min(times),
                "category": str(rows[0].get("category") or "unclassified"),
                "headline": str(rows[0].get("headline") or ""),
                "pair_legs": len(rows),
                "follow_net_pips": sum(finite(row.get("follow_net_pips")) for row in rows)
                / len(rows),
                "fade_net_pips": sum(finite(row.get("fade_net_pips")) for row in rows)
                / len(rows),
                "mean_round_trip_spread_pips": sum(
                    max(0.0, finite(row.get("entry_spread_pips")))
                    + max(0.0, finite(row.get("exit_spread_pips")))
                    for row in rows
                )
                / len(rows),
                "mean_abs_pair_score": sum(abs(finite(row.get("pair_score"))) for row in rows)
                / len(rows),
                "distinct_source_count": max(
                    int(finite(row.get("distinct_source_count"))) for row in rows
                ),
                "topic_article_count": max(
                    int(finite(row.get("topic_article_count"))) for row in rows
                ),
                "verified_fraction": sum(bool(row.get("source_verified")) for row in rows)
                / len(rows),
                "direct_fraction": sum(bool(row.get("source_direct")) for row in rows)
                / len(rows),
            }
        )

    output: list[dict[str, Any]] = []
    for horizon in sorted({int(row["horizon_minutes"]) for row in topics}):
        horizon_topics = sorted(
            (row for row in topics if int(row["horizon_minutes"]) == horizon),
            key=lambda row: (finite(row.get("signal_epoch")), str(row.get("topic_id"))),
        )
        cluster_id = 0
        cluster_start = -math.inf
        for row in horizon_topics:
            timestamp = finite(row.get("signal_epoch"))
            if timestamp > cluster_start + cluster_minutes * 60:
                cluster_id += 1
                cluster_start = timestamp
                output.append(
                    {
                        **row,
                        "episode_id": f"H{horizon}_E{cluster_id:05d}",
                        "cluster_policy": "first_known_topic_only",
                    }
                )
    return output


def surprise_coverage(database_path: Path) -> dict[str, Any]:
    path = Path(database_path)
    if not path.is_file():
        return {"status": "database_missing", "topics": 0, "coverage": {}}
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    rows = connection.execute("SELECT payload_json FROM topic_events").fetchall()
    connection.close()
    counts = {label: 0 for label in SURPRISE_FIELDS}
    complete = 0
    for (text,) in rows:
        try:
            payload = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        present: dict[str, bool] = {}
        for label, names in SURPRISE_FIELDS.items():
            present[label] = any(payload.get(name) not in (None, "") for name in names)
            counts[label] += int(present[label])
        complete += int(present["actual"] and present["consensus"])
    total = len(rows)
    return {
        "status": "ready" if complete else "true_surprise_fields_unavailable",
        "topics": total,
        "actual_and_consensus_topics": complete,
        "actual_and_consensus_coverage": round(complete / total, 6) if total else 0.0,
        "coverage": {
            label: round(count / total, 6) if total else 0.0
            for label, count in counts.items()
        },
    }


def _mean_ci90(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = sum(values) / len(values)
    if len(values) < 2:
        return mean, mean
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return mean, mean - 1.645 * math.sqrt(variance / len(values))


def metrics(rows: Iterable[Mapping[str, Any]], action_by_category: Mapping[str, str]) -> dict[str, Any]:
    realised: list[float] = []
    actions: dict[str, int] = defaultdict(int)
    for row in rows:
        action = str(action_by_category.get(str(row.get("category")), "no_trade"))
        actions[action] += 1
        if action == "follow":
            realised.append(finite(row.get("follow_net_pips")))
        elif action == "fade":
            realised.append(finite(row.get("fade_net_pips")))
    mean, lower = _mean_ci90(realised)
    return {
        "episodes": len(realised),
        "average_net_pips": round(mean, 6),
        "ci90_lower_pips": round(lower, 6),
        "win_rate": round(sum(value > 0.0 for value in realised) / len(realised), 6)
        if realised
        else 0.0,
        "total_net_pips": round(sum(realised), 6),
        "actions": dict(sorted(actions.items())),
    }


def _learn_category_actions(
    rows: list[Mapping[str, Any]],
    minimum_category_episodes: int,
    threshold: float,
) -> dict[str, str]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("category") or "unclassified")].append(row)
    actions: dict[str, str] = {}
    for category, category_rows in grouped.items():
        if len(category_rows) < minimum_category_episodes:
            actions[category] = "no_trade"
            continue
        follow = sum(finite(row.get("follow_net_pips")) for row in category_rows) / len(category_rows)
        fade = sum(finite(row.get("fade_net_pips")) for row in category_rows) / len(category_rows)
        best = max(follow, fade)
        actions[category] = (
            "follow" if follow >= fade else "fade"
        ) if best >= threshold else "no_trade"
    return actions


def fit_horizon(
    rows: list[dict[str, Any]],
    minimum_episodes: int = 60,
    minimum_category_episodes: int = 8,
) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: finite(row.get("signal_epoch")))
    categories = {str(row.get("category")) for row in ordered}
    naive_follow = metrics(ordered, {category: "follow" for category in categories})
    naive_fade = metrics(ordered, {category: "fade" for category in categories})
    if len(ordered) < minimum_episodes:
        return {
            "status": "insufficient_independent_news_episodes",
            "episodes": len(ordered),
            "minimum_episodes": minimum_episodes,
            "naive_follow": naive_follow,
            "naive_fade": naive_fade,
            "shadow_decision": "no_trade_insufficient_news_history",
            "account_eligible": False,
        }
    train_end = int(len(ordered) * 0.60)
    selection_end = int(len(ordered) * 0.80)
    train = ordered[:train_end]
    selection = ordered[train_end:selection_end]
    holdout = ordered[selection_end:]
    candidates: list[dict[str, Any]] = []
    for threshold in (0.0, 0.25, 0.5, 1.0, 2.0, 4.0):
        actions = _learn_category_actions(
            train, minimum_category_episodes, threshold
        )
        result = metrics(selection, actions)
        candidates.append(
            {"threshold": threshold, "actions": actions, "selection": result}
        )
    viable = [
        row
        for row in candidates
        if int(row["selection"]["episodes"]) >= 10
        and finite(row["selection"]["average_net_pips"]) > 0.0
        and finite(row["selection"]["ci90_lower_pips"]) > 0.0
    ]
    if not viable:
        return {
            "status": "no_viable_purged_selection_policy",
            "episodes": len(ordered),
            "split": {"train": len(train), "selection": len(selection), "holdout": len(holdout)},
            "naive_follow": naive_follow,
            "naive_fade": naive_fade,
            "selection_candidates": candidates,
            "shadow_decision": "no_trade_failed_news_selection",
            "account_eligible": False,
        }
    selected = max(
        viable,
        key=lambda row: finite(row["selection"]["ci90_lower_pips"]),
    )
    holdout_result = metrics(holdout, selected["actions"])
    checks = {
        "minimum_10_holdout_episodes": int(holdout_result["episodes"]) >= 10,
        "positive_holdout_average": finite(holdout_result["average_net_pips"]) > 0.0,
        "positive_holdout_ci90_lower": finite(holdout_result["ci90_lower_pips"]) > 0.0,
    }
    return {
        "status": "historical_candidate" if all(checks.values()) else "holdout_failed",
        "episodes": len(ordered),
        "split": {"train": len(train), "selection": len(selection), "holdout": len(holdout)},
        "selected_policy": selected,
        "untouched_holdout": holdout_result,
        "promotion_checks": checks,
        "shadow_decision": "forward_observe_only" if all(checks.values()) else "no_trade_failed_news_holdout",
        "account_eligible": False,
        "naive_follow": naive_follow,
        "naive_fade": naive_fade,
    }


def run_audit(
    replay_path: Path,
    news_database: Path,
    output_path: Path,
    minimum_episodes: int = 60,
) -> dict[str, Any]:
    calls = load_calls(replay_path)
    episodes = build_episode_rows(calls)
    horizons: dict[str, Any] = {}
    for horizon in HORIZONS_MIN:
        horizon_rows = [row for row in episodes if int(row["horizon_minutes"]) == horizon]
        horizons[str(horizon)] = fit_horizon(
            horizon_rows, minimum_episodes=minimum_episodes
        )
    payload = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "model": "causal_episode_weighted_news_reaction_v1",
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "execution_adapter": False,
        "origin_time_policy": "first_seen_utc_only",
        "pair_fanout_policy": "equal_weight_pairs_then_one_vote_per_30m_episode",
        "replay": str(Path(replay_path).resolve()),
        "calls": len(calls),
        "episodes": len(episodes),
        "true_surprise_coverage": surprise_coverage(news_database),
        "horizons": horizons,
    }
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--news-database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-episodes", type=int, default=60)
    args = parser.parse_args()
    result = run_audit(
        args.replay,
        args.news_database,
        args.output,
        minimum_episodes=max(20, int(args.minimum_episodes)),
    )
    print(json.dumps({
        "output": str(args.output.resolve()),
        "episodes": result["episodes"],
        "surprise_status": result["true_surprise_coverage"]["status"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_episode_rows",
    "fit_horizon",
    "load_calls",
    "metrics",
    "run_audit",
    "surprise_coverage",
]
