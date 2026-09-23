#!/usr/bin/env python3
"""Reconstruct the retained WTD live-move universe with causal FX factors.

This is a read-only/on-demand research audit.  It does not rewrite the
append-only mover history, does not alter evidence or lifecycle state,
and has no authorization, broker, promotion, or execution surface.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import shutil
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import oanda_live_move_news_snapshot as live
import oanda_major_move_gap_census as census
import oanda_move_first_news_case_audit as news_audit


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORTS = DATA / "reports"
# The V1 file stopped receiving cases after the live capture contract advanced.
# Bind the current-week report to the append-only history actually written by
# the supervised V7r3 capture. Historical files remain immutable and can still
# be selected explicitly with ``--history``.
DEFAULT_HISTORY = STATE / "live_move_news_cases_v7r3.sqlite"
DEFAULT_SOURCES = STATE / "source_governance_v1.sqlite"
DEFAULT_CANDLES = DATA / "candles"
DEFAULT_OUTPUT_DIRECTORY = REPORTS / "wtd_live_move_reconstruction"
LEGACY_MANUAL_REPORT = REPORTS / "WEEK_TO_DATE_EVENT_MOVE_AUDIT_20260827.md"
LEGACY_BASELINE_NAME = "WEEK_TO_DATE_EVENT_MOVE_AUDIT_PRE_CAUSAL_FACTOR_V1_20260827.md"

CONTRACT_ID = "wtd_live_move_reconstruction_v4_current_history_all68_m1_le60_20260904"
FACTOR_CONTRACT_ID = "wtd_live_factor_episode_v4_current_history_all68_m1_le60_20260904"
EXPECTED_PAIRS = 68
MINIMUM_FRESH_PAIRS = 60
MAXIMUM_PAIR_AGE_SEC = 180
MAXIMUM_FACTOR_DURATION_MINUTES = 60.0
FACTOR_NEIGHBOR_SEC = 15 * 60
NEWS_LOOKBACK_MINUTES = 120
MATERIAL_THRESHOLDS_BPS = (5, 10, 15, 20, 25)
UTC = dt.timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
LEGACY_MANUAL_FREEZE_UTC = dt.datetime(2026, 8, 27, 4, 6, tzinfo=UTC)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True))


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def week_start(as_of: dt.datetime) -> dt.datetime:
    local = as_of.astimezone(NEW_YORK)
    monday = local - dt.timedelta(days=local.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def parse_case_payload(raw: str) -> dict[str, Any] | None:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None
    return dict(value) if isinstance(value, Mapping) else None


def move_side(value: Any) -> int:
    return live.move_side(value)


def logical_case_key(row: Mapping[str, Any]) -> tuple[str, str, int]:
    return (
        str(row.get("instrument") or ""),
        str(row.get("start_utc") or ""),
        move_side(row.get("move_direction")),
    )


def load_retained_cases(
    database: Path,
    *,
    start_epoch: int,
    as_of_epoch: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read and logically deduplicate append-only mover records.

    A contract transition can insert the same pair/start/direction under a new
    case ID.  The earliest retained observation is the causal case version;
    later physical rows remain untouched and are counted as preserved history
    versions rather than independent market moves.
    """

    connection = census.open_readonly(database)
    physical: list[dict[str, Any]] = []
    malformed = 0
    contracts: Counter[str] = Counter()
    try:
        query = """
            SELECT case_id,first_recorded_utc,instrument,start_utc,end_utc,case_json
            FROM mover_cases
            WHERE first_recorded_utc <= ?
            ORDER BY first_recorded_utc,case_id
        """
        for values in connection.execute(query, (census.iso_epoch(as_of_epoch),)):
            payload = parse_case_payload(str(values[5] or ""))
            if payload is None:
                malformed += 1
                continue
            start = census.parse_epoch(payload.get("start_utc") or values[3])
            end = census.parse_epoch(payload.get("end_utc") or values[4])
            side = move_side(payload.get("move_direction"))
            instrument = str(payload.get("instrument") or values[2])
            if (
                start is None
                or end is None
                or int(start) < int(start_epoch)
                or int(end) > int(as_of_epoch)
                or int(end) < int(start)
                or not side
                or "_" not in instrument
            ):
                continue
            payload.update(
                {
                    "instrument": instrument,
                    "start_utc": str(payload.get("start_utc") or values[3]),
                    "end_utc": str(payload.get("end_utc") or values[4]),
                    "start_epoch": int(start),
                    "end_epoch": int(end),
                    "actual_side": side,
                    "history_case_id": str(values[0]),
                    "first_recorded_utc": str(values[1]),
                    "history_contract_id": str(
                        payload.get("contract_id") or "pre_contract_field"
                    ),
                }
            )
            contracts[payload["history_contract_id"]] += 1
            physical.append(payload)
    finally:
        connection.close()

    versions: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in physical:
        versions[logical_case_key(row)].append(row)
    logical: list[dict[str, Any]] = []
    for key in sorted(versions):
        candidates = sorted(
            versions[key],
            key=lambda row: (
                str(row.get("first_recorded_utc") or ""),
                str(row.get("history_case_id") or ""),
            ),
        )
        selected = dict(candidates[0])
        selected["physical_version_count"] = len(candidates)
        selected["preserved_history_case_ids"] = [
            str(row.get("history_case_id") or "") for row in candidates
        ]
        logical.append(selected)
    logical.sort(
        key=lambda row: (
            int(row["start_epoch"]),
            str(row["instrument"]),
            str(row["history_case_id"]),
        )
    )
    return logical, {
        "physical_record_count": len(physical),
        "logical_raw_case_count": len(logical),
        "preserved_duplicate_contract_record_count": len(physical) - len(logical),
        "malformed_record_count": malformed,
        "history_contract_counts": dict(sorted(contracts.items())),
        "logical_dedup_key": ["instrument", "start_utc", "move_direction"],
        "logical_version_rule": "earliest_first_recorded_utc",
    }


def physical_record_count_at(database: Path, cutoff_epoch: int) -> int:
    connection = census.open_readonly(database)
    try:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM mover_cases WHERE first_recorded_utc <= ?",
                (census.iso_epoch(cutoff_epoch),),
            ).fetchone()[0]
        )
    finally:
        connection.close()


def load_m1_candles(path: Path) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    try:
        handle = path.open("r", encoding="utf-8-sig", newline="")
    except OSError:
        return output
    with handle:
        for source in csv.DictReader(handle):
            epoch = census.parse_epoch(source.get("time") or source.get("datetime"))
            values = {
                name: census.safe_float(source.get(name))
                for name in (
                    "bid_open",
                    "bid_high",
                    "bid_low",
                    "bid_close",
                    "ask_open",
                    "ask_high",
                    "ask_low",
                    "ask_close",
                )
            }
            if epoch is None or any(value is None for value in values.values()):
                continue
            output.append({"epoch": int(epoch), **values})
    output.sort(key=lambda row: int(row["epoch"]))
    return output


def causal_technical_at_start(
    candles: Sequence[Mapping[str, Any]],
    *,
    start_epoch: int,
    pip_size: float,
) -> dict[str, Any]:
    """Call the shared M1 diagnostic with completed bars only."""

    open_times = [int(row["epoch"]) for row in candles]
    completed_index = bisect.bisect_right(open_times, int(start_epoch) - 60) - 1
    if completed_index < 0:
        return {
            "state": "insufficient_completed_m1_history",
            "completed_bar_count": 0,
            "research_only": True,
        }
    selected = candles[max(0, completed_index - 120) : completed_index + 1]
    converted = [
        {
            "timestamp": dt.datetime.fromtimestamp(int(row["epoch"]), tz=UTC),
            **{key: row[key] for key in row if key != "epoch"},
        }
        for row in selected
    ]
    return news_audit.causal_m1_technical_state(
        converted,
        decision_time=dt.datetime.fromtimestamp(int(start_epoch), tz=UTC),
        pip_size=float(pip_size),
    )


def build_archive_evidence(
    rows: Sequence[dict[str, Any]],
    candle_root: Path,
) -> tuple[dict[tuple[int, int], dict[str, Any]], dict[str, Any]]:
    requests = {
        (
            int(row["end_epoch"]),
            live.closest_factor_strength_horizon(row.get("duration_minutes")),
        )
        for row in rows
    }
    observations: dict[tuple[int, int], dict[str, dict[str, Any]]] = {}
    by_instrument: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_instrument[str(row["instrument"])].append(row)
    paths = sorted(candle_root.glob("*_M1.csv"))
    expected_instruments = [path.stem.removesuffix("_M1") for path in paths]
    missing_case_files: Counter[str] = Counter()
    for path in paths:
        instrument = path.stem.removesuffix("_M1")
        candles = load_m1_candles(path)
        census.collect_causal_factor_observations(
            instrument,
            candles,
            requests,
            observations,
            maximum_pair_age_sec=MAXIMUM_PAIR_AGE_SEC,
        )
        for row in by_instrument.get(instrument, []):
            row["causal_m1_technical_state"] = causal_technical_at_start(
                candles,
                start_epoch=int(row["start_epoch"]),
                pip_size=census.pip_size(instrument),
            )
    available = set(expected_instruments)
    for instrument, instrument_rows in by_instrument.items():
        if instrument not in available:
            missing_case_files[instrument] += len(instrument_rows)
            for row in instrument_rows:
                row["causal_m1_technical_state"] = {
                    "state": "missing_candle_file",
                    "research_only": True,
                }
    surfaces, meta = census.solve_causal_factor_strength_surfaces(
        requests,
        observations,
        expected_instruments,
        minimum_observation_count=MINIMUM_FRESH_PAIRS,
        expected_observation_count=EXPECTED_PAIRS,
    )
    meta.update(
        {
            "candle_root": str(candle_root.resolve()),
            "candle_file_count": len(paths),
            "missing_case_candle_file_counts": dict(missing_case_files),
            "maximum_pair_age_sec": MAXIMUM_PAIR_AGE_SEC,
            "m1_close_knowledge_clock": "bar_open_epoch_plus_60_seconds",
        }
    )
    return surfaces, meta


def signed_score(token: str, strengths: Mapping[str, Any]) -> float | None:
    return census._signed_factor_score(token, strengths)


def assign_primary_factors(
    rows: Sequence[dict[str, Any]],
    surfaces: Mapping[tuple[int, int], Mapping[str, Any]],
) -> None:
    bucket_counts: dict[int, Counter[str]] = defaultdict(Counter)
    for row in rows:
        base, quote = str(row["instrument"]).split("_", 1)
        side = int(row["actual_side"])
        tokens = [
            f"{base}{'+' if side > 0 else '-'}",
            f"{quote}{'-' if side > 0 else '+'}",
        ]
        bucket = int((int(row["start_epoch"]) + FACTOR_NEIGHBOR_SEC // 2) // FACTOR_NEIGHBOR_SEC)
        row["factor_tokens"] = tokens
        row["factor_fallback_bucket"] = bucket
        bucket_counts[bucket].update(tokens)

    for row in rows:
        horizon = live.closest_factor_strength_horizon(row.get("duration_minutes"))
        surface = dict(surfaces.get((int(row["end_epoch"]), horizon)) or {})
        strengths = dict(surface.get("currency_strength_bps") or {})
        scores = {
            token: signed_score(token, strengths) for token in row["factor_tokens"]
        }
        causal = bool(surface.get("valid")) and all(
            value is not None for value in scores.values()
        )
        if causal:
            ordered = sorted(
                row["factor_tokens"], key=lambda token: (-float(scores[token]), token)
            )
            primary = ordered[0]
            margin = float(scores[ordered[0]]) - float(scores[ordered[1]])
            method = "causal_all68_currency_strength"
            ambiguous = margin <= live.FACTOR_STRENGTH_AMBIGUITY_BPS
        else:
            counts = bucket_counts[int(row["factor_fallback_bucket"])]
            ordered = sorted(
                row["factor_tokens"], key=lambda token: (-counts[token], token)
            )
            primary = ordered[0]
            margin = float(counts[ordered[0]] - counts[ordered[1]])
            method = "time_bucket_token_recurrence_fallback"
            ambiguous = margin <= 0.0
        row.update(
            {
                "factor_primary_token": primary,
                "factor_primary_method": method,
                "factor_primary_scores_bps": {
                    token: round(float(value), 6)
                    for token, value in scores.items()
                    if value is not None
                },
                "factor_primary_margin": round(margin, 6),
                "factor_primary_margin_unit": "bps" if causal else "token_count",
                "factor_primary_ambiguous": ambiguous,
                "factor_strength_horizon_minutes": horizon,
                "factor_strength_surface_status": str(
                    surface.get("status") or "unavailable"
                ),
                "factor_strength_as_of_utc": str(surface.get("as_of_utc") or ""),
                "factor_strength_as_of_age_sec": surface.get("as_of_age_sec"),
                "factor_strength_oldest_pair_age_sec": surface.get(
                    "oldest_pair_age_sec"
                ),
                "factor_strength_observation_count": int(
                    surface.get("observation_count") or 0
                ),
                "factor_strength_coverage_pct": surface.get("coverage_pct"),
                "factor_strength_missing_currencies": list(
                    surface.get("missing_currencies") or []
                ),
            }
        )


def snapshot_representative_count(rows: Sequence[Mapping[str, Any]]) -> int:
    groups: set[tuple[str, str, int]] = set()
    for row in rows:
        groups.add(
            (
                str(row.get("first_recorded_utc") or ""),
                str(row.get("factor_primary_token") or ""),
                int(row.get("factor_fallback_bucket") or 0),
            )
        )
    return len(groups)


def factor_analysis_universe(
    rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int | float]]:
    """Select the declared through-60-minute live-move factor universe.

    ``mover_cases`` also retains rolling continuous segments that can span
    many hours.  They belong in the raw append-only census but not in the
    manual audit's explicitly declared horizons-through-60-minutes universe.
    Keeping the two counts separate prevents a long rolling segment from
    appearing as a new intrahour factor episode.
    """

    eligible: list[dict[str, Any]] = []
    over_maximum = 0
    invalid = 0
    for row in rows:
        try:
            duration = float(row.get("duration_minutes") or 0.0)
        except (TypeError, ValueError):
            duration = 0.0
        if not math.isfinite(duration) or duration <= 0.0:
            invalid += 1
        elif duration <= MAXIMUM_FACTOR_DURATION_MINUTES:
            eligible.append(row)
        else:
            over_maximum += 1
    return eligible, {
        "factor_eligible_logical_case_count": len(eligible),
        "over_60m_logical_case_count_excluded": over_maximum,
        "invalid_duration_logical_case_count_excluded": invalid,
        "maximum_factor_duration_minutes": MAXIMUM_FACTOR_DURATION_MINUTES,
    }


def representative_rank(row: Mapping[str, Any]) -> tuple[float, float, float, str]:
    return (
        abs(float(row.get("move_bps") or 0.0)),
        abs(float(row.get("executable_net_pips") or 0.0)),
        float(row.get("duration_minutes") or 0.0),
        str(row.get("instrument") or ""),
    )


def snapshot_representatives(
    rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep one strongest expression of a factor per retained live snapshot.

    ``first_recorded_utc`` is the immutable discovery/snapshot clock retained
    by ``mover_cases``.  The rounded 15-minute start bucket is also retained,
    matching the live V5 factor contract: alternative pair expressions of the
    same signed factor and start bucket collapse, while distinct swing windows
    visible in one snapshot remain explicitly separate.
    """

    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[
            (
                str(row.get("first_recorded_utc") or ""),
                str(row.get("factor_primary_token") or ""),
                int(row.get("factor_fallback_bucket") or 0),
            )
        ].append(row)
    output: list[dict[str, Any]] = []
    for key in sorted(groups):
        members = groups[key]
        representative = dict(max(members, key=representative_rank))
        representative["snapshot_factor_member_count"] = len(members)
        representative["snapshot_factor_member_case_ids"] = sorted(
            str(row.get("history_case_id") or "") for row in members
        )
        representative["snapshot_factor_member_methods"] = sorted(
            {str(row.get("factor_primary_method") or "") for row in members}
        )
        representative["snapshot_factor_contains_ambiguous_row"] = any(
            bool(row.get("factor_primary_ambiguous")) for row in members
        )
        output.append(representative)
    return output


def build_factor_episodes(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse neighboring same-factor starts using a fixed episode anchor.

    Chaining every adjacent row can turn a dense rolling detector into one
    day-long pseudo-episode.  Each episode is therefore anchored at its first
    start; another row joins only when its start is within 15 minutes of that
    fixed anchor.  This makes the reduction deterministic and auditable.
    """

    by_factor: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_factor[str(row["factor_primary_token"])].append(row)
    episodes: list[dict[str, Any]] = []
    for token in sorted(by_factor):
        ordered = sorted(
            by_factor[token],
            key=lambda row: (
                int(row["start_epoch"]),
                str(row["instrument"]),
                str(row["history_case_id"]),
            ),
        )
        cluster: list[dict[str, Any]] = []
        anchor = 0

        def flush() -> None:
            if not cluster:
                return
            representative = max(cluster, key=representative_rank)
            methods: set[str] = set()
            for row in cluster:
                methods.add(str(row["factor_primary_method"]))
                methods.update(
                    str(value)
                    for value in row.get("snapshot_factor_member_methods") or []
                    if str(value)
                )
            ambiguous = any(
                bool(row["factor_primary_ambiguous"])
                or bool(row.get("snapshot_factor_contains_ambiguous_row"))
                for row in cluster
            )
            category = (
                "fallback"
                if methods != {"causal_all68_currency_strength"}
                else "ambiguous"
                if ambiguous
                else "exact"
            )
            first_start = min(int(row["start_epoch"]) for row in cluster)
            last_start = max(int(row["start_epoch"]) for row in cluster)
            episode_id = "wtd_factor_" + hashlib.sha256(
                f"{FACTOR_CONTRACT_ID}|{token}|{first_start}".encode("utf-8")
            ).hexdigest()[:24]
            output = dict(representative)
            output.update(
                {
                    "factor_episode_id": episode_id,
                    "factor_episode_category": category,
                    "factor_episode_member_count": len(cluster),
                    "factor_episode_first_start_utc": census.iso_epoch(first_start),
                    "factor_episode_last_start_utc": census.iso_epoch(last_start),
                    "factor_episode_last_end_utc": census.iso_epoch(
                        max(int(row["end_epoch"]) for row in cluster)
                    ),
                    "factor_episode_contains_ambiguous_row": ambiguous,
                    "factor_episode_member_methods": sorted(methods),
                }
            )
            episodes.append(output)

        for row in ordered:
            start = int(row["start_epoch"])
            if cluster and start - anchor > FACTOR_NEIGHBOR_SEC:
                flush()
                cluster = []
            if not cluster:
                anchor = start
            cluster.append(row)
        flush()
    episodes.sort(
        key=lambda row: (
            int(row["start_epoch"]),
            str(row["factor_primary_token"]),
            str(row["instrument"]),
        )
    )
    return episodes


def mapping_state(vote: Mapping[str, Any], actual: int, event_count: int) -> str:
    side = int(vote.get("side") or 0)
    if side == actual:
        return "aligned"
    if side == -actual:
        return "opposed"
    if event_count <= 0:
        return "no_relevant_source"
    return "neutral_or_conflicted"


def attach_news_mapping(
    episodes: Sequence[dict[str, Any]],
    source_database: Path,
) -> dict[str, Any]:
    if not episodes:
        return {"broad": {}, "strict": {}, "source_highwater_utc": None}
    minimum = min(int(row["start_epoch"]) for row in episodes) - NEWS_LOOKBACK_MINUTES * 60
    maximum = max(int(row["start_epoch"]) for row in episodes)
    source_index, highwater = census.load_source_index(
        source_database,
        minimum_effective_epoch=minimum,
        maximum_effective_epoch=maximum,
    )
    broad_counts: Counter[str] = Counter()
    strict_counts: Counter[str] = Counter()
    for row in episodes:
        base, quote = str(row["instrument"]).split("_", 1)
        entry = int(row["start_epoch"])
        events = census.relevant_source_events(
            source_index,
            (base, quote),
            entry - NEWS_LOOKBACK_MINUTES * 60,
            entry,
            True,
        )
        broad = news_audit.directional_vote(events, base, quote, entry)
        strict = news_audit.directional_vote(
            events, base, quote, entry, strict_forward=True
        )
        broad_state = mapping_state(broad, int(row["actual_side"]), len(events))
        strict_state = mapping_state(strict, int(row["actual_side"]), len(events))
        row.update(
            {
                "broad_news_state": broad_state,
                "broad_news_side": news_audit.side_label(int(broad.get("side") or 0)),
                "broad_news_story_count": int(
                    broad.get("independent_directional_stories") or 0
                ),
                "strict_news_state": strict_state,
                "strict_news_side": news_audit.side_label(int(strict.get("side") or 0)),
                "strict_news_story_count": int(
                    strict.get("independent_directional_stories") or 0
                ),
                "strict_news_exclusions": dict(strict.get("exclusions") or {}),
                "top_broad_story": (
                    dict((broad.get("stories") or [])[0])
                    if broad.get("stories")
                    else {}
                ),
                "top_strict_story": (
                    dict((strict.get("stories") or [])[0])
                    if strict.get("stories")
                    else {}
                ),
            }
        )
        broad_counts[broad_state] += 1
        strict_counts[strict_state] += 1
    return {
        "broad": dict(sorted(broad_counts.items())),
        "strict": dict(sorted(strict_counts.items())),
        "source_highwater_utc": highwater,
        "lookback_minutes": NEWS_LOOKBACK_MINUTES,
        "mapping_clock": "effective_from_at_or_before_move_start",
    }


def attach_technical_labels(episodes: Sequence[dict[str, Any]]) -> dict[str, Any]:
    trend_counts: Counter[str] = Counter()
    breakout_counts: Counter[str] = Counter()
    exhaustion_counts: Counter[str] = Counter()
    availability: Counter[str] = Counter()
    for row in episodes:
        state = dict(row.get("causal_m1_technical_state") or {})
        available = str(state.get("state") or "unavailable") == "available"
        availability["available" if available else str(state.get("state") or "unavailable")] += 1
        actual = int(row["actual_side"])
        if not available:
            trend = "unavailable"
            breakout = "unavailable"
            exhaustion = "unavailable"
        else:
            r15 = float(state.get("return_15m_pips") or 0.0)
            trend = (
                "aligned"
                if r15 * actual > 0.0
                else "opposed"
                if r15 * actual < 0.0
                else "neutral"
            )
            raw_breakout = str(state.get("breakout_20m") or "inside")
            breakout_side = 1 if raw_breakout == "up" else -1 if raw_breakout == "down" else 0
            breakout = (
                "aligned"
                if breakout_side == actual
                else "opposed"
                if breakout_side == -actual
                else "inside"
            )
            exhaustion = str(state.get("exhaustion_60m") or "none")
        row["technical_trend_15m_state"] = trend
        row["technical_breakout_20m_state"] = breakout
        row["technical_exhaustion_60m_state"] = exhaustion
        trend_counts[trend] += 1
        breakout_counts[breakout] += 1
        exhaustion_counts[exhaustion] += 1
    return {
        "availability": dict(sorted(availability.items())),
        "trend_15m_vs_move": dict(sorted(trend_counts.items())),
        "breakout_20m_vs_move": dict(sorted(breakout_counts.items())),
        "exhaustion_60m": dict(sorted(exhaustion_counts.items())),
        "role": "causal_completed_m1_diagnostic_not_historical_model_forecast",
        "selection_warning": (
            "The retained mover detector defines swing starts at hindsight local "
            "extrema after a reversal threshold is observed. Prior 15-minute trend "
            "is therefore mechanically biased toward opposition and is descriptive, "
            "not a forecast hit-rate estimate."
        ),
    }


def threshold_counts(episodes: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        f"gte_{threshold}_bps": sum(
            abs(float(row.get("move_bps") or 0.0)) >= threshold for row in episodes
        )
        for threshold in MATERIAL_THRESHOLDS_BPS
    }


CSV_FIELDS = (
    "factor_episode_id",
    "factor_episode_category",
    "factor_primary_token",
    "factor_primary_method",
    "factor_primary_ambiguous",
    "factor_primary_margin",
    "factor_primary_margin_unit",
    "factor_primary_scores_bps_json",
    "factor_strength_horizon_minutes",
    "factor_strength_surface_status",
    "factor_strength_observation_count",
    "factor_strength_coverage_pct",
    "factor_strength_as_of_utc",
    "factor_strength_as_of_age_sec",
    "factor_strength_oldest_pair_age_sec",
    "factor_strength_missing_currencies_json",
    "factor_episode_member_count",
    "factor_episode_first_start_utc",
    "factor_episode_last_start_utc",
    "factor_episode_last_end_utc",
    "instrument",
    "start_utc",
    "end_utc",
    "duration_minutes",
    "move_direction",
    "move_bps",
    "gross_pips",
    "executable_net_pips",
    "broad_news_state",
    "broad_news_side",
    "broad_news_story_count",
    "strict_news_state",
    "strict_news_side",
    "strict_news_story_count",
    "top_broad_source_id",
    "top_broad_effective_from_utc",
    "top_broad_headline",
    "technical_trend_15m_state",
    "technical_breakout_20m_state",
    "technical_exhaustion_60m_state",
    "causal_m1_technical_state_json",
    "history_case_id",
    "history_contract_id",
    "physical_version_count",
)


def csv_row(row: Mapping[str, Any]) -> dict[str, Any]:
    top = dict(row.get("top_broad_story") or {})
    output = {field: row.get(field, "") for field in CSV_FIELDS}
    output.update(
        {
            "factor_primary_scores_bps_json": json.dumps(
                row.get("factor_primary_scores_bps") or {}, sort_keys=True
            ),
            "factor_strength_missing_currencies_json": json.dumps(
                row.get("factor_strength_missing_currencies") or []
            ),
            "top_broad_source_id": top.get("source_id", ""),
            "top_broad_effective_from_utc": top.get("effective_from_utc", ""),
            "top_broad_headline": str(top.get("headline") or "").replace("\n", " "),
            "causal_m1_technical_state_json": json.dumps(
                row.get("causal_m1_technical_state") or {}, sort_keys=True
            ),
        }
    )
    return output


def write_csv(path: Path, episodes: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in episodes:
            writer.writerow(csv_row(row))
    temporary.replace(path)


def clean_cell(value: Any, limit: int = 100) -> str:
    return str(value or "").replace("|", "/").replace("\n", " ")[:limit]


def render_markdown(payload: Mapping[str, Any]) -> str:
    universe = payload["universe"]
    factor = payload["factor_assignment"]
    news = payload["news_mapping"]
    technical = payload["technical_state"]
    lines = [
        "# WTD live-move causal reconstruction",
        "",
        f"Generated: `{payload['generated_utc']}`  ",
        f"Evidence window: `{universe['week_start_utc']}` through `{universe['as_of_utc']}`  ",
        f"Contract: `{payload['contract_id']}`",
        "",
        "## Universe",
        "",
        f"- Physical append-only mover records: **{universe['physical_record_count']:,}**.",
        f"- Logical raw live-move cases after cross-contract version deduplication: **{universe['logical_raw_case_count']:,}**.",
        f"- Preserved duplicate cross-contract history rows excluded from independent counting: **{universe['preserved_duplicate_contract_record_count']:,}**.",
        f"- Logical cases eligible for the declared >0 to <=60-minute factor analysis: **{universe['factor_eligible_logical_case_count']:,}**.",
        f"- Longer continuous segments retained in raw history but excluded from factor episodes: **{universe['over_60m_logical_case_count_excluded']:,}**.",
        f"- Invalid/nonpositive-duration logical cases excluded: **{universe['invalid_duration_logical_case_count_excluded']:,}**.",
        f"- Per-snapshot causal factor representatives: **{factor['snapshot_factor_representative_count']:,}**.",
        f"- Global fixed-anchor 15-minute factor episodes: **{factor['factor_episode_count']:,}**.",
        "",
        "The raw source is the append-only `mover_cases` table populated from the top ten clear movers at each live snapshot. It is not the complete all-path major-move census and it is not 68 independent observations per clock. A clear mover had positive executable net room and gross movement at least 1.5 times its recorded cost. Logical deduplication selects the earliest retained observation for the same instrument/start/direction while preserving every physical cross-contract row. Factor assignment, news mapping, technical labels, thresholds, and the complete >=15-bps table use only positive-duration moves through 60 minutes, matching the frozen manual scope; longer rolling segments remain visible only in raw-universe counts.",
        "",
        "## Frozen-manual-cut reconciliation",
        "",
    ]
    comparison = payload.get("legacy_manual_freeze_reconstruction") or {}
    if comparison:
        old = comparison.get("reported_v1_like_counts") or {}
        corrected = comparison.get("corrected_counts") or {}
        lines.extend(
            [
                f"At the original `{comparison.get('freeze_utc')}` cutoff, the manual report recorded {old.get('physical_raw_cases', 0):,} raw rows, {old.get('per_snapshot_representatives', 0):,} per-snapshot representatives, and {old.get('global_factor_episodes', 0):,} V1-like globally reduced episodes.",
                f"The same cutoff under this causal contract contains **{corrected.get('physical_raw_cases', 0):,} physical rows**, **{corrected.get('logical_raw_cases', 0):,} logical cases**, **{corrected.get('factor_eligible_logical_cases', 0):,} >0 to <=60-minute factor-eligible cases**, **{corrected.get('over_60m_logical_cases_excluded', 0):,} longer retained segments excluded**, **{corrected.get('per_snapshot_representatives', 0):,} per-snapshot representatives**, and **{corrected.get('global_factor_episodes', 0):,} global factor episodes**.",
                f"Corrected magnitude thresholds: `{json.dumps(comparison.get('magnitude_thresholds') or {}, sort_keys=True)}`.",
                f"Corrected broad news mapping: `{json.dumps((comparison.get('news_mapping') or {}).get('broad') or {}, sort_keys=True)}`.",
                f"Corrected strict news mapping: `{json.dumps((comparison.get('news_mapping') or {}).get('strict') or {}, sort_keys=True)}`.",
                "The frozen 905 count was the sum of stored V1-like `factor_representative` flags among physical <=60-minute rows. The replacement re-solves each logical case using completed-M1 information available at its move end, then applies the declared live-style snapshot/factor/start-bucket reduction. The frozen 496 global count had no immutable machine-readable reconstruction contract in the preserved report; the replacement deliberately uses a fixed 15-minute anchor so dense rolling observations cannot chain indefinitely. The corrected counts are therefore a governed replacement, not a numerical restatement of 905/496.",
                "",
            ]
        )
    lines.extend(
        [
        "## Causal factor result",
        "",
        f"- Exact episodes: **{factor['episode_categories'].get('exact', 0):,}**.",
        f"- Ambiguous causal episodes: **{factor['episode_categories'].get('ambiguous', 0):,}**.",
        f"- Fallback episodes: **{factor['episode_categories'].get('fallback', 0):,}**.",
        f"- Strength surfaces require at least {MINIMUM_FRESH_PAIRS}/{EXPECTED_PAIRS} completed, fresh pair observations and both row currencies.",
        "- M1 close fields are knowledge-time bound to bar-open +60 seconds; later bars and same-bar future closes are excluded.",
        "",
        "### Magnitude thresholds",
        "",
        ]
    )
    for threshold in MATERIAL_THRESHOLDS_BPS:
        lines.append(
            f"- >= {threshold} bps: **{payload['magnitude_thresholds'][f'gte_{threshold}_bps']:,}** episodes."
        )
    lines.extend(
        [
            "",
            "## News mapping",
            "",
            f"- Broad research mapping: `{json.dumps(news['broad'], sort_keys=True)}`.",
            f"- Strict forward/publishable mapping: `{json.dumps(news['strict'], sort_keys=True)}`.",
            "- Broad context is diagnostic and may not be interpreted as a forecast. Strict direction requires forward-timely publish eligibility at the move start.",
            "",
            "## Causal M1 technical state",
            "",
            f"- 15-minute trend versus move: `{json.dumps(technical['trend_15m_vs_move'], sort_keys=True)}`.",
            f"- 20-minute breakout versus move: `{json.dumps(technical['breakout_20m_vs_move'], sort_keys=True)}`.",
            f"- 60-minute exhaustion: `{json.dumps(technical['exhaustion_60m'], sort_keys=True)}`.",
            "- This is a completed-M1 reconstruction, not proof that a historical model emitted that state.",
            f"- Selection warning: {technical.get('selection_warning', '')}",
            "",
            "## Complete >=15 bps episodes",
            "",
            "| # | Factor | Pair | Start to end UTC | Move | Net pips | Factor evidence | Broad / strict news | Pre-move technical | Closest broad source |",
            "|---:|---|---|---|---:|---:|---|---|---|---|",
        ]
    )
    material = sorted(
        (
            row
            for row in payload.get("episodes") or []
            if abs(float(row.get("move_bps") or 0.0)) >= 15.0
        ),
        key=lambda row: abs(float(row.get("move_bps") or 0.0)),
        reverse=True,
    )
    for index, row in enumerate(material, 1):
        top = dict(row.get("top_broad_story") or {})
        lines.append(
            f"| {index} | {row['factor_primary_token']} | {row['instrument']} | "
            f"{clean_cell(row['start_utc'], 19)} to {clean_cell(row['end_utc'], 19)} | "
            f"{float(row.get('move_bps') or 0.0):.3f} bps | "
            f"{float(row.get('executable_net_pips') or 0.0):.2f} | "
            f"{row['factor_episode_category']}; {row['factor_primary_method']}; "
            f"{row.get('factor_strength_observation_count', 0)}/{EXPECTED_PAIRS} | "
            f"{row.get('broad_news_state')} / {row.get('strict_news_state')} | "
            f"trend {row.get('technical_trend_15m_state')}; breakout {row.get('technical_breakout_20m_state')} | "
            f"{clean_cell(top.get('source_id'))}: {clean_cell(top.get('headline'), 120)} |"
        )
    if not material:
        lines.append("| - | - | - | - | - | - | - | - | - | No retained episode reached 15 bps. |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "Research-only, retrospective movement audit. It cannot place orders, promote a hypothesis, authorize Practice 007, or alter policy. Episode counts are exact under this declared reconstruction contract; they are not claims that the market shocks are economically independent.",
            "",
        ]
    )
    return "\n".join(lines)


def json_episode(row: Mapping[str, Any]) -> dict[str, Any]:
    excluded = {
        "recent_context_stories",
        "during_context_stories",
        "strict_forward_stories",
        "continuous_narrative_state",
        "preserved_history_case_ids",
    }
    return {key: value for key, value in row.items() if key not in excluded}


def preserve_manual_baseline(output_directory: Path, source: Path) -> Path | None:
    if not source.exists():
        return None
    target = output_directory / LEGACY_BASELINE_NAME
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return target


def run(
    *,
    history_database: Path = DEFAULT_HISTORY,
    source_database: Path = DEFAULT_SOURCES,
    candle_root: Path = DEFAULT_CANDLES,
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY,
    as_of: dt.datetime | None = None,
    legacy_manual_report: Path = LEGACY_MANUAL_REPORT,
) -> dict[str, Any]:
    generated = as_of.astimezone(UTC) if as_of is not None else utc_now()
    local_start = week_start(generated)
    start_utc = local_start.astimezone(UTC)
    as_of_epoch = int(generated.timestamp())
    start_epoch = int(start_utc.timestamp())
    rows, universe = load_retained_cases(
        history_database, start_epoch=start_epoch, as_of_epoch=as_of_epoch
    )
    factor_rows, factor_universe = factor_analysis_universe(rows)
    universe.update(factor_universe)
    surfaces, factor_meta = build_archive_evidence(factor_rows, candle_root)
    assign_primary_factors(factor_rows, surfaces)
    snapshot_rows = snapshot_representatives(factor_rows)
    snapshot_count = len(snapshot_rows)
    episodes = build_factor_episodes(snapshot_rows)
    news_mapping = attach_news_mapping(episodes, source_database)
    technical_state = attach_technical_labels(episodes)
    legacy_cut_epoch = int(LEGACY_MANUAL_FREEZE_UTC.timestamp())
    legacy_rows = [
        row
        for row in rows
        if int(census.parse_epoch(row.get("first_recorded_utc")) or 0)
        <= legacy_cut_epoch
    ]
    legacy_factor_rows, legacy_factor_universe = factor_analysis_universe(legacy_rows)
    legacy_snapshot_rows = snapshot_representatives(legacy_factor_rows)
    legacy_episodes = build_factor_episodes(legacy_snapshot_rows)
    legacy_news_mapping = attach_news_mapping(legacy_episodes, source_database)
    legacy_technical_state = attach_technical_labels(legacy_episodes)
    legacy_categories = Counter(
        str(row["factor_episode_category"]) for row in legacy_episodes
    )
    categories = Counter(str(row["factor_episode_category"]) for row in episodes)
    methods = Counter(str(row["factor_primary_method"]) for row in factor_rows)
    baseline = preserve_manual_baseline(output_directory, legacy_manual_report)
    stamp = generated.strftime("%Y%m%dT%H%M%SZ")
    output_json = output_directory / f"WTD_LIVE_MOVE_RECONSTRUCTION_{stamp}.json"
    output_md = output_directory / f"WTD_LIVE_MOVE_RECONSTRUCTION_{stamp}.md"
    output_csv = output_directory / f"WTD_LIVE_MOVE_RECONSTRUCTION_{stamp}.csv"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "factor_contract_id": FACTOR_CONTRACT_ID,
        "generated_utc": iso(generated),
        "universe": {
            **universe,
            "week_start_local": local_start.isoformat(),
            "week_start_utc": iso(start_utc),
            "as_of_utc": iso(generated),
            "history_database": str(history_database.resolve()),
            "source_table": "mover_cases",
            "live_selection_scope": "top_ten_clear_unique_instruments_per_snapshot",
            "clear_move_definition": "executable_net_positive_and_gross_at_least_1.5x_recorded_cost",
            "not_equivalent_to": [
                "major_move_gap_census",
                "complete_all_possible_intervals",
                "independent_market_episode_sample",
            ],
        },
        "factor_assignment": {
            **factor_meta,
            "contract_id": FACTOR_CONTRACT_ID,
            "primary_method_counts_factor_eligible_cases": dict(sorted(methods.items())),
            "method_count_scope": "factor_eligible_logical_cases_gt0_le60m",
            "snapshot_factor_representative_count": snapshot_count,
            "snapshot_factor_representative_rule": (
                "one_strongest_case_per_first_recorded_snapshot_primary_factor_"
                "and_rounded_15m_start_bucket"
            ),
            "factor_episode_count": len(episodes),
            "episode_categories": dict(sorted(categories.items())),
            "neighbor_window_sec": FACTOR_NEIGHBOR_SEC,
            "neighbor_rule": "same_primary_factor_and_start_within_15m_of_fixed_episode_anchor",
        },
        "magnitude_thresholds": threshold_counts(episodes),
        "news_mapping": news_mapping,
        "technical_state": technical_state,
        "legacy_manual_freeze_reconstruction": {
            "freeze_utc": iso(LEGACY_MANUAL_FREEZE_UTC),
            "reported_v1_like_counts": {
                "physical_raw_cases": 1702,
                "per_snapshot_representatives": 905,
                "global_factor_episodes": 496,
            },
            "reported_v1_like_905_definition_verified_from_retained_history": (
                "physical_rows_with_0_lt_duration_le_60m_and_stored_factor_representative_true"
            ),
            "reported_v1_like_496_reconstruction_lineage": (
                "preserved_markdown_only_no_immutable_machine_readable_contract"
            ),
            "corrected_counts": {
                "physical_raw_cases": physical_record_count_at(
                    history_database, legacy_cut_epoch
                ),
                "logical_raw_cases": len(legacy_rows),
                "factor_eligible_logical_cases": len(legacy_factor_rows),
                "over_60m_logical_cases_excluded": legacy_factor_universe[
                    "over_60m_logical_case_count_excluded"
                ],
                "invalid_duration_logical_cases_excluded": legacy_factor_universe[
                    "invalid_duration_logical_case_count_excluded"
                ],
                "per_snapshot_representatives": snapshot_representative_count(
                    legacy_factor_rows
                ),
                "global_factor_episodes": len(legacy_episodes),
                "episode_categories": dict(sorted(legacy_categories.items())),
            },
            "magnitude_thresholds": threshold_counts(legacy_episodes),
            "news_mapping": legacy_news_mapping,
            "technical_state": legacy_technical_state,
            "gte_15_bps_episodes": [
                json_episode(row)
                for row in legacy_episodes
                if abs(float(row.get("move_bps") or 0.0)) >= 15.0
            ],
        },
        "legacy_manual_baseline": str(baseline.resolve()) if baseline else None,
        "artifacts": {
            "json": str(output_json.resolve()),
            "markdown": str(output_md.resolve()),
            "csv": str(output_csv.resolve()),
        },
        "episodes": [json_episode(row) for row in episodes],
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "diagnostic_only",
    }
    atomic_json(output_json, payload)
    atomic_text(output_md, render_markdown(payload))
    write_csv(output_csv, episodes)
    return payload


def parse_datetime(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--candles", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--output-directory", type=Path, default=DEFAULT_OUTPUT_DIRECTORY)
    parser.add_argument("--as-of-utc", default="")
    parser.add_argument("--legacy-manual-report", type=Path, default=LEGACY_MANUAL_REPORT)
    args = parser.parse_args()
    payload = run(
        history_database=args.history,
        source_database=args.sources,
        candle_root=args.candles,
        output_directory=args.output_directory,
        as_of=parse_datetime(args.as_of_utc) if args.as_of_utc else None,
        legacy_manual_report=args.legacy_manual_report,
    )
    print(
        json.dumps(
            {
                "generated_utc": payload["generated_utc"],
                "raw_n": payload["universe"]["logical_raw_case_count"],
                "factor_episode_count": payload["factor_assignment"]["factor_episode_count"],
                "gte_15_bps": payload["magnitude_thresholds"]["gte_15_bps"],
                "execution_eligible": payload["execution_eligible"],
                "artifacts": payload["artifacts"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
