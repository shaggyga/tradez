#!/usr/bin/env python3
"""Observe executor signal-source availability without affecting execution."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
SHADOW_TTL_SCENARIOS_SEC = (60.0, 90.0, 120.0, 180.0, 300.0)


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso_utc(value: dt.datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).isoformat()


def market_state(value: dt.datetime) -> str:
    """Classify the regular OANDA FX weekend closure in UTC (summer hours)."""
    value = value.astimezone(UTC)
    if value.weekday() == 5:
        return "weekend_closed"
    if value.weekday() == 4 and value.hour >= 21:
        return "weekend_closed"
    if value.weekday() == 6 and value.hour < 21:
        return "weekend_closed"
    return "open_or_transition"


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return default


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.4, 0.025 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def quote_points(payload: dict[str, Any]) -> dict[str, dict[str, float]]:
    rows = payload.get("quotes") if isinstance(payload, dict) else {}
    rows = rows if isinstance(rows, dict) else {}
    output: dict[str, dict[str, float]] = {}
    for instrument, row in rows.items():
        if not isinstance(row, dict):
            continue
        try:
            bid = float(row.get("bid"))
            ask = float(row.get("ask"))
            pip = float(row.get("pip"))
        except (TypeError, ValueError):
            continue
        if pip <= 0.0 or ask < bid:
            continue
        output[str(instrument)] = {
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "pip": pip,
            "spread_pips": (ask - bid) / pip,
        }
    return output


def gap_market_movement(
    baseline: dict[str, dict[str, float]],
    current: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    movements: list[dict[str, Any]] = []
    for instrument in sorted(set(baseline) & set(current)):
        start = baseline[instrument]
        end = current[instrument]
        pip = float(start.get("pip") or 0.0)
        if pip <= 0.0:
            continue
        move = (float(end["mid"]) - float(start["mid"])) / pip
        start_spread = max(0.0, float(start.get("spread_pips") or 0.0))
        movements.append(
            {
                "instrument": instrument,
                "signed_mid_move_pips": round(move, 3),
                "absolute_mid_move_pips": round(abs(move), 3),
                "start_spread_pips": round(start_spread, 3),
                "end_spread_pips": round(
                    max(0.0, float(end.get("spread_pips") or 0.0)), 3
                ),
                "absolute_move_to_start_spread": round(
                    abs(move) / max(0.1, start_spread), 3
                ),
            }
        )
    movements.sort(
        key=lambda row: (
            float(row["absolute_mid_move_pips"]), row["instrument"]
        ),
        reverse=True,
    )
    return movements


def candidate_gap_outcomes(
    candidates: list[dict[str, Any]],
    baseline: dict[str, dict[str, float]],
    current: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    """Mark candidate direction from gap-start entry through gap-end exit.

    Returns use executable quote sides, so ``net_move_pips`` already includes
    the entry and exit spreads.  This is diagnostic shadow evidence only.
    """

    outcomes: list[dict[str, Any]] = []
    for candidate in candidates:
        instrument = str(candidate.get("instrument") or "")
        direction = str(candidate.get("direction") or "").lower()
        if instrument not in baseline or instrument not in current:
            continue
        if direction not in {"buy", "sell"}:
            continue
        start = baseline[instrument]
        end = current[instrument]
        pip = float(start.get("pip") or 0.0)
        if pip <= 0.0:
            continue
        if direction == "buy":
            gross_move = (float(end["mid"]) - float(start["mid"])) / pip
            net_move = (float(end["bid"]) - float(start["ask"])) / pip
        else:
            gross_move = (float(start["mid"]) - float(end["mid"])) / pip
            net_move = (float(start["bid"]) - float(end["ask"])) / pip
        outcomes.append(
            {
                "candidate_id": str(candidate.get("id") or ""),
                "instrument": instrument,
                "direction": direction,
                "family": str(candidate.get("family") or ""),
                "horizon_sec": candidate.get(
                    "execution_horizon_sec",
                    candidate.get("preferred_horizon_sec"),
                ),
                "direction_conflict": bool(
                    candidate.get("direction_conflict")
                ),
                "execution_permitted_after_conflict_gate": bool(
                    candidate.get("execution_permitted_after_conflict_gate")
                ),
                "gross_move_pips": round(gross_move, 3),
                "net_move_pips": round(net_move, 3),
                "start_spread_pips": round(
                    max(0.0, float(start.get("spread_pips") or 0.0)), 3
                ),
                "end_spread_pips": round(
                    max(0.0, float(end.get("spread_pips") or 0.0)), 3
                ),
                "would_cover_round_trip_cost": net_move > 0.0,
            }
        )
    outcomes.sort(
        key=lambda row: (float(row["net_move_pips"]), row["candidate_id"]),
        reverse=True,
    )
    return outcomes


def source_observation(
    heartbeat: dict[str, Any],
    *,
    source: str,
    heartbeat_age_sec: float,
) -> dict[str, Any]:
    details = heartbeat.get("details") if isinstance(heartbeat, dict) else {}
    details = details if isinstance(details, dict) else {}
    cache = details.get("feed_cache")
    cache = cache if isinstance(cache, dict) else {}
    counts = cache.get("source_counts")
    counts = counts if isinstance(counts, dict) else {}
    ages = cache.get("source_newest_age_sec")
    ages = ages if isinstance(ages, dict) else {}
    try:
        ttl_sec = float(cache.get("ttl_sec", 90.0))
    except (TypeError, ValueError):
        ttl_sec = 90.0
    sources = [item.strip() for item in str(source).split("|") if item.strip()]
    source_count = 0
    source_ages: list[float] = []
    for item in sources:
        try:
            source_count += int(counts.get(item, 0))
        except (TypeError, ValueError):
            pass
        try:
            source_ages.append(float(ages.get(item)))
        except (TypeError, ValueError):
            pass
    source_age_sec = min(source_ages) if source_ages else None
    heartbeat_fresh = heartbeat_age_sec <= max(15.0, ttl_sec)
    cache_fresh = bool(cache.get("fresh")) and not bool(cache.get("fail_closed"))
    source_fresh = source_age_sec is not None and source_age_sec < ttl_sec
    available = heartbeat_fresh and cache_fresh and source_count > 0 and source_fresh
    reasons: list[str] = []
    if not heartbeat_fresh:
        reasons.append("executor_heartbeat_stale")
    if not cache_fresh:
        reasons.append("feed_cache_not_fresh")
    if source_count <= 0:
        reasons.append("source_absent")
    if source_age_sec is None:
        reasons.append("source_age_missing")
    elif source_age_sec >= ttl_sec:
        reasons.append("source_expired")
    observation = {
        "available": available,
        "reasons": reasons,
        "source": source,
        "sources": sources,
        "source_count": source_count,
        "source_newest_age_sec": source_age_sec,
        "ttl_sec": ttl_sec,
        "feed_cache_fail_closed": bool(cache.get("fail_closed")),
        "heartbeat_age_sec": round(heartbeat_age_sec, 3),
        "feed_generation": cache.get("generation"),
        "raw_candidate_count": cache.get("raw_candidate_count"),
        "fresh_candidate_count": cache.get("candidate_count"),
        "expired_candidate_count": cache.get("expired_candidate_count"),
        "prefiltered_candidates": details.get("prefiltered_candidates"),
        "qualified_candidates": details.get("qualified_candidates"),
        "nonconflicting_qualified_candidates": details.get(
            "nonconflicting_qualified_candidates"
        ),
        "qualified_candidate_preview": [
            dict(row)
            for row in details.get("qualified_candidate_preview") or []
            if isinstance(row, dict)
        ][:5],
        "final_selection_blocks": [
            dict(row)
            for row in details.get("final_selection_blocks") or []
            if isinstance(row, dict)
        ][:5],
        "last_execution_skip_reason": details.get(
            "last_execution_skip_reason"
        ),
        "executor_status": heartbeat.get("status"),
        "executor_phase": heartbeat.get("phase"),
        "executor_started_at": heartbeat.get("started_at"),
        "executor_uptime_sec": heartbeat.get("uptime_sec"),
    }
    observation["shadow_ttl_scenarios"] = shadow_ttl_scenarios(observation)
    return observation


def shadow_ttl_scenarios(
    observation: dict[str, Any],
    scenarios: tuple[float, ...] = SHADOW_TTL_SCENARIOS_SEC,
) -> list[dict[str, Any]]:
    """Report counterfactual source availability without changing the live TTL.

    The current executor/cache decision remains authoritative.  These rows
    answer only whether a source publication would have remained observable
    under a predeclared research TTL, while preserving heartbeat and
    fail-closed constraints.
    """

    try:
        source_count = int(observation.get("source_count") or 0)
    except (TypeError, ValueError):
        source_count = 0
    try:
        source_age = float(observation.get("source_newest_age_sec"))
    except (TypeError, ValueError):
        source_age = None
    try:
        heartbeat_age = float(observation.get("heartbeat_age_sec"))
    except (TypeError, ValueError):
        heartbeat_age = float("inf")
    fail_closed = bool(observation.get("feed_cache_fail_closed"))
    rows: list[dict[str, Any]] = []
    for ttl in sorted({float(value) for value in scenarios if float(value) > 0.0}):
        source_fresh = source_age is not None and source_age < ttl
        heartbeat_fresh = heartbeat_age <= max(15.0, ttl)
        available = (
            not fail_closed
            and source_count > 0
            and source_fresh
            and heartbeat_fresh
        )
        reasons: list[str] = []
        if fail_closed:
            reasons.append("feed_cache_fail_closed")
        if source_count <= 0:
            reasons.append("source_absent")
        if source_age is None:
            reasons.append("source_age_missing")
        elif not source_fresh:
            reasons.append("source_expired_at_scenario_ttl")
        if not heartbeat_fresh:
            reasons.append("executor_heartbeat_stale_at_scenario_ttl")
        rows.append(
            {
                "ttl_sec": ttl,
                "available": available,
                "reasons": reasons,
                "research_only": True,
                "execution_policy_changed": False,
            }
        )
    return rows


def gap_classification(observation: dict[str, Any]) -> str:
    """Distinguish producer cadence from executor deployment/health gaps."""

    if observation.get("market_state") == "weekend_closed":
        return "market_closed_expected"
    try:
        uptime_sec = float(observation.get("executor_uptime_sec"))
    except (TypeError, ValueError):
        uptime_sec = None
    if observation.get("executor_status") not in {None, "running"}:
        return "executor_unavailable"
    if observation.get("feed_generation") is None:
        return "executor_startup_or_restart"
    if uptime_sec is not None and uptime_sec < 30.0:
        return "executor_startup_or_restart"
    reasons = set(observation.get("reasons") or [])
    if "executor_heartbeat_stale" in reasons:
        return "executor_unavailable"
    if "source_absent" in reasons or "source_expired" in reasons:
        return "source_publication_expiry"
    if "feed_cache_not_fresh" in reasons:
        return "feed_cache_unavailable"
    return "other_unavailability"


def update_state(
    state: dict[str, Any],
    observation: dict[str, Any],
    now_epoch: float,
    quotes: dict[str, dict[str, float]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    state = dict(state or {})
    state.setdefault("schema_version", 1)
    state.setdefault("samples", 0)
    state.setdefault("available_samples", 0)
    state.setdefault("unavailable_samples", 0)
    state.setdefault("gap_count", 0)
    state.setdefault("completed_gap_total_sec", 0.0)
    state.setdefault("maximum_completed_gap_sec", 0.0)
    state.setdefault("started_utc", iso_utc())
    preview = [
        dict(row)
        for row in observation.get("qualified_candidate_preview") or []
        if isinstance(row, dict)
    ]
    if preview:
        state["last_qualified_candidate_observation"] = {
            "observed_epoch": now_epoch,
            "observed_utc": iso_utc(
                dt.datetime.fromtimestamp(now_epoch, UTC)
            ),
            "candidates": preview[:5],
            "nonconflicting_qualified_candidates": observation.get(
                "nonconflicting_qualified_candidates"
            ),
            "final_selection_blocks": observation.get(
                "final_selection_blocks"
            ) or [],
        }
    state["samples"] += 1
    available = bool(observation.get("available"))
    state["available_samples" if available else "unavailable_samples"] += 1
    transition = None
    gap_start_epoch = state.get("current_gap_start_epoch")
    if not available and gap_start_epoch is None:
        classification = gap_classification(observation)
        state["current_gap_start_epoch"] = now_epoch
        state["current_gap_start_utc"] = iso_utc(
            dt.datetime.fromtimestamp(now_epoch, UTC)
        )
        state["gap_count"] += 1
        state["current_gap_quote_points"] = dict(quotes or {})
        state["current_gap_classification"] = classification
        last_candidates = state.get("last_qualified_candidate_observation")
        last_candidates = (
            dict(last_candidates)
            if isinstance(last_candidates, dict)
            else {}
        )
        try:
            preceding_age_sec = max(
                0.0,
                now_epoch - float(last_candidates.get("observed_epoch")),
            )
        except (TypeError, ValueError):
            preceding_age_sec = float("inf")
        ttl_sec = float(observation.get("ttl_sec") or 90.0)
        if preceding_age_sec <= ttl_sec:
            state["current_gap_preceding_candidate_observation"] = {
                **last_candidates,
                "age_at_gap_start_sec": round(preceding_age_sec, 3),
            }
        else:
            state["current_gap_preceding_candidate_observation"] = {}
        transition = {
            "event": "availability_gap_started",
            "gap_classification": classification,
            **observation,
        }
    elif not available and gap_start_epoch is not None:
        current_classification = str(
            state.get("current_gap_classification") or "other_unavailability"
        )
        new_classification = gap_classification(observation)
        if (
            new_classification == "market_closed_expected"
            and current_classification != "market_closed_expected"
        ):
            state["current_gap_classification"] = "market_closed_expected"
            state["market_closed_reclassified_at_utc"] = iso_utc(
                dt.datetime.fromtimestamp(now_epoch, UTC)
            )
    elif available and gap_start_epoch is not None:
        gap_sec = max(0.0, now_epoch - float(gap_start_epoch))
        state["completed_gap_total_sec"] += gap_sec
        state["maximum_completed_gap_sec"] = max(
            float(state["maximum_completed_gap_sec"]), gap_sec
        )
        state["last_completed_gap_sec"] = round(gap_sec, 3)
        state["last_completed_gap_utc"] = iso_utc()
        classification = str(
            state.get("current_gap_classification")
            or "other_unavailability"
        )
        classification_counts = dict(
            state.get("completed_gap_classification_counts") or {}
        )
        classification_counts[classification] = int(
            classification_counts.get(classification) or 0
        ) + 1
        state["completed_gap_classification_counts"] = dict(
            sorted(classification_counts.items())
        )
        state["operational_source_gap_count"] = int(
            classification_counts.get("source_publication_expiry") or 0
        )
        state["last_completed_gap_classification"] = classification
        state["current_gap_start_epoch"] = None
        state["current_gap_start_utc"] = None
        movements = (
            [] if classification == "market_closed_expected" else
            gap_market_movement(
                state.get("current_gap_quote_points") or {}, quotes or {}
            )
        )
        state["last_completed_gap_market_movement"] = movements[:10]
        preceding = state.get("current_gap_preceding_candidate_observation")
        preceding = preceding if isinstance(preceding, dict) else {}
        candidate_outcomes = candidate_gap_outcomes(
            [
                dict(row)
                for row in preceding.get("candidates") or []
                if isinstance(row, dict)
            ],
            state.get("current_gap_quote_points") or {},
            quotes or {},
        )
        state["last_completed_gap_candidate_outcomes"] = candidate_outcomes
        if candidate_outcomes:
            state["completed_gaps_with_preceding_qualified"] = int(
                state.get("completed_gaps_with_preceding_qualified") or 0
            ) + 1
        if any(
            row.get("execution_permitted_after_conflict_gate")
            for row in candidate_outcomes
        ):
            state["completed_gaps_with_nonconflicting_preceding"] = int(
                state.get(
                    "completed_gaps_with_nonconflicting_preceding"
                ) or 0
            ) + 1
        state["current_gap_quote_points"] = {}
        state["current_gap_preceding_candidate_observation"] = {}
        state["current_gap_classification"] = None
        transition = {
            "event": "availability_gap_ended",
            "gap_duration_sec": round(gap_sec, 3),
            "gap_classification": classification,
            "market_movement": movements[:10],
            "preceding_candidate_outcomes": candidate_outcomes,
            **observation,
        }
    current_gap_sec = (
        max(0.0, now_epoch - float(state["current_gap_start_epoch"]))
        if state.get("current_gap_start_epoch") is not None
        else 0.0
    )
    state["current_gap_duration_sec"] = round(current_gap_sec, 3)
    if (
        not available
        and state.get("current_gap_quote_points")
        and state.get("current_gap_classification") != "market_closed_expected"
    ):
        state["current_gap_market_movement"] = gap_market_movement(
            state["current_gap_quote_points"], quotes or {}
        )[:10]
    else:
        state["current_gap_market_movement"] = []
    state["availability_sample_rate"] = round(
        state["available_samples"] / max(1, state["samples"]), 6
    )
    state["latest"] = observation
    state["updated_utc"] = iso_utc()
    state["execution_authorized"] = False
    return state, transition


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--heartbeat", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--quotes", type=Path)
    parser.add_argument("--source", default="strategy_lab")
    parser.add_argument("--comparison-source", default="")
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    args = parser.parse_args()
    state = load_json(args.state, {})
    deadline = time.monotonic() + max(0.0, args.duration_sec)
    while time.monotonic() < deadline:
        now_epoch = time.time()
        heartbeat = load_json(args.heartbeat, {})
        try:
            heartbeat_age_sec = max(
                0.0, now_epoch - args.heartbeat.stat().st_mtime
            )
        except OSError:
            heartbeat_age_sec = float("inf")
        observation = source_observation(
            heartbeat,
            source=args.source,
            heartbeat_age_sec=heartbeat_age_sec,
        )
        observation["market_state"] = market_state(
            dt.datetime.fromtimestamp(now_epoch, UTC)
        )
        quotes = quote_points(load_json(args.quotes, {})) if args.quotes else {}
        state, transition = update_state(
            state,
            observation,
            now_epoch,
            quotes,
        )
        comparison_transition = None
        if args.comparison_source:
            comparison_observation = source_observation(
                heartbeat,
                source=args.comparison_source,
                heartbeat_age_sec=heartbeat_age_sec,
            )
            comparison_observation["market_state"] = observation["market_state"]
            comparison_state, comparison_transition = update_state(
                state.get("comparison") or {},
                comparison_observation,
                now_epoch,
                quotes,
            )
            state["comparison"] = comparison_state
        atomic_write_json(args.state, state)
        if transition is not None:
            transition["time"] = iso_utc()
            append_jsonl(args.events, transition)
        if comparison_transition is not None:
            comparison_transition["time"] = iso_utc()
            comparison_transition["scope"] = "comparison"
            append_jsonl(args.events, comparison_transition)
        time.sleep(max(0.25, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
