#!/usr/bin/env python3
"""Join the freshest independent FX moves to point-in-time news evidence.

This diagnostic is movement-first: a move is retained whether or not news or a
model explained it. It reads the current all-68 mover state and a narrow window
of the immutable source-governance ledger, then reports strict forward news,
broader research context, and the frozen continuous-narrative state separately.
It has no broker, authorization, promotion, or execution surface.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_major_move_gap_census as census
import oanda_market_sentiment_ticker as market_ticker
import oanda_move_first_news_case_audit as news_audit


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_MOVES = STATE / "practice_007_latest_moves_v1.json"
DEFAULT_SOURCES = STATE / "source_governance_v1.sqlite"
DEFAULT_NARRATIVE = STATE / "continuous_narrative_meter_v12.json"
DEFAULT_QUOTES = STATE / "practice_007_market_quotes_v1.json"
DEFAULT_FACTOR_HISTORY = DATA / "market_sentiment_ticker" / "quote_history.json"
DEFAULT_OUTPUT = STATE / "live_move_news_snapshot_v1.json"
DEFAULT_HISTORY = STATE / "live_move_news_cases_v1.sqlite"
DEFAULT_REPORT = (
    DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_CURRENT.md"
)
CONTRACT_ID = "live_move_news_snapshot_v5_causal_factor_strength_20260827"
FACTOR_STRENGTH_HORIZONS = tuple(int(value) for value in market_ticker.WINDOWS_MINUTES)
FACTOR_STRENGTH_EXPECTED_OBSERVATIONS = 68
FACTOR_STRENGTH_MIN_OBSERVATIONS = 60
FACTOR_STRENGTH_MAX_PAIR_AGE_SEC = 180
FACTOR_STRENGTH_AMBIGUITY_BPS = 0.5


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True), encoding="utf-8"
        )
        _replace_with_retry(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(value, encoding="utf-8")
        _replace_with_retry(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _replace_with_retry(temporary: Path, path: Path) -> None:
    for attempt in range(8):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 7:
                raise
            time.sleep(min(0.5, 0.01 * (2**attempt)))


def stable_case_id(row: Mapping[str, Any]) -> str:
    material = "|".join(
        (
            CONTRACT_ID,
            str(row.get("instrument") or ""),
            str(row.get("start_utc") or ""),
            str(row.get("move_direction") or ""),
        )
    )
    return "live_move_news_case_" + hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:32]


def connect_history(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS mover_cases (
            case_id TEXT PRIMARY KEY,
            first_recorded_utc TEXT NOT NULL,
            instrument TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            case_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_mover_cases_start
            ON mover_cases(start_utc,instrument,case_id);
        CREATE TRIGGER IF NOT EXISTS mover_cases_no_update
            BEFORE UPDATE ON mover_cases BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS mover_cases_no_delete
            BEFORE DELETE ON mover_cases BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def record_cases(
    path: Path, rows: Sequence[Mapping[str, Any]], *, recorded_utc: str
) -> dict[str, int]:
    connection = connect_history(path)
    inserted = 0
    try:
        for source in rows:
            row = dict(source)
            case_id = stable_case_id(row)
            row["case_id"] = case_id
            before = connection.total_changes
            connection.execute(
                "INSERT OR IGNORE INTO mover_cases VALUES (?,?,?,?,?,?)",
                (
                    case_id,
                    recorded_utc,
                    str(row.get("instrument") or ""),
                    str(row.get("start_utc") or ""),
                    str(row.get("end_utc") or ""),
                    json.dumps(row, sort_keys=True, separators=(",", ":")),
                ),
            )
            inserted += int(connection.total_changes > before)
        connection.commit()
        total = int(
            connection.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0]
        )
    finally:
        connection.close()
    return {"inserted": inserted, "total": total}


def move_side(direction: Any) -> int:
    label = str(direction or "").strip().lower()
    return 1 if label in {"increase", "up", "long"} else -1 if label in {
        "decrease",
        "down",
        "short",
    } else 0


def explanation_state(
    *,
    actual_side: int,
    strict_side: int,
    broad_side: int,
    narrative_side: int,
    relevant_event_count: int,
) -> str:
    if strict_side == actual_side:
        return "strict_forward_source_aligned"
    if strict_side == -actual_side:
        return "strict_forward_source_opposed"
    if broad_side == actual_side:
        return "research_context_aligned_not_publishable"
    if broad_side == -actual_side:
        return "research_context_opposed_not_publishable"
    if narrative_side == actual_side:
        return "continuous_narrative_aligned_unvalidated"
    if narrative_side == -actual_side:
        return "continuous_narrative_opposed_unvalidated"
    if relevant_event_count <= 0:
        return "no_relevant_source_event"
    return "context_only_conflicted_or_pair_neutral"


def closest_factor_strength_horizon(duration_minutes: Any) -> int:
    """Choose the declared strength horizon nearest to the completed move."""

    try:
        duration = max(1.0, float(duration_minutes))
    except (TypeError, ValueError):
        duration = 15.0
    return min(
        FACTOR_STRENGTH_HORIZONS,
        key=lambda horizon: (abs(float(horizon) - duration), horizon),
    )


def build_causal_factor_strength_surfaces(
    rows: Sequence[Mapping[str, Any]],
    history_rows: Sequence[Mapping[str, Any]],
    *,
    minimum_observation_count: int = FACTOR_STRENGTH_MIN_OBSERVATIONS,
    maximum_pair_age_sec: int = FACTOR_STRENGTH_MAX_PAIR_AGE_SEC,
) -> dict[tuple[int, int], dict[str, Any]]:
    """Build synchronized all-pair strength strictly at/before each move end.

    This is an after-the-fact clustering input, never a forecast input. The
    quote-history solver is evaluated with the move end as its watermark, so
    later bars cannot affect the factor label. Stale pairs are excluded and
    logged; a surface is invalid unless the configured fresh-coverage floor
    survives and both currencies needed by a row are present.
    """

    requested: set[tuple[int, int]] = set()
    for row in rows:
        end_epoch = census.parse_epoch(row.get("end_utc"))
        if end_epoch is None:
            continue
        requested.add(
            (
                int(end_epoch),
                closest_factor_strength_horizon(row.get("duration_minutes")),
            )
        )
    pair_cache: dict[int, dict[str, dict[str, Any]]] = {}
    output: dict[tuple[int, int], dict[str, Any]] = {}
    for end_epoch, horizon in sorted(requested):
        if end_epoch not in pair_cache:
            pair_cache[end_epoch] = market_ticker.pair_windows(
                history_rows, end_epoch
            )
        pair_moves = pair_cache[end_epoch]
        eligible_pairs = {
            instrument: pair
            for instrument, pair in pair_moves.items()
            if (pair.get("windows") or {}).get(str(horizon))
            and int(pair.get("latest_epoch") or 0) > 0
        }
        future_instruments = sorted(
            instrument
            for instrument, pair in eligible_pairs.items()
            if int(pair.get("latest_epoch") or 0) > end_epoch
        )
        stale_instruments = sorted(
            instrument
            for instrument, pair in eligible_pairs.items()
            if end_epoch - int(pair.get("latest_epoch") or 0)
            > int(maximum_pair_age_sec)
        )
        excluded = set(future_instruments) | set(stale_instruments)
        fresh_pair_moves = {
            instrument: pair
            for instrument, pair in eligible_pairs.items()
            if instrument not in excluded
        }
        eligible_pair_clocks = [
            int(pair.get("latest_epoch") or 0)
            for pair in fresh_pair_moves.values()
        ]
        future_clock_count = len(future_instruments)
        oldest_pair_age = (
            max(0, end_epoch - min(eligible_pair_clocks))
            if eligible_pair_clocks
            else None
        )
        newest_pair_clock = max(eligible_pair_clocks, default=0)
        newest_pair_age = (
            max(0, end_epoch - newest_pair_clock) if newest_pair_clock else None
        )
        solved = market_ticker.solve_currency_strength(fresh_pair_moves, horizon)
        observations = int(solved.get("observation_count") or 0)
        all_currencies = sorted(
            {
                currency
                for instrument in eligible_pairs
                for currency in instrument.split("_", 1)
            }
        )
        solved_currencies = set((solved.get("currency_strength_bps") or {}).keys())
        missing_currencies = sorted(set(all_currencies) - solved_currencies)
        valid = bool(
            future_clock_count == 0
            and observations >= int(minimum_observation_count)
            and oldest_pair_age is not None
            and oldest_pair_age <= int(maximum_pair_age_sec)
        )
        if future_clock_count:
            status = "future_clock_rejected"
        elif observations < int(minimum_observation_count):
            status = "insufficient_all_pair_coverage"
        elif oldest_pair_age is None or oldest_pair_age > int(maximum_pair_age_sec):
            status = "stale_pair_surface"
        elif stale_instruments or observations < FACTOR_STRENGTH_EXPECTED_OBSERVATIONS:
            status = "ready_with_logged_pair_exclusions"
        else:
            status = "ready"
        output[(end_epoch, horizon)] = {
            "valid": valid,
            "status": status,
            "horizon_minutes": horizon,
            "move_end_utc": census.iso_epoch(end_epoch),
            "as_of_utc": census.iso_epoch(newest_pair_clock) if newest_pair_clock else "",
            "as_of_age_sec": newest_pair_age,
            "oldest_pair_age_sec": oldest_pair_age,
            "future_clock_count": future_clock_count,
            "future_instruments": future_instruments,
            "stale_instruments": stale_instruments,
            "missing_currencies": missing_currencies,
            "expected_observation_count": FACTOR_STRENGTH_EXPECTED_OBSERVATIONS,
            "observation_count": observations,
            "coverage_pct": round(
                100.0
                * observations
                / max(1, FACTOR_STRENGTH_EXPECTED_OBSERVATIONS),
                3,
            ),
            "currency_count": int(solved.get("currency_count") or 0),
            "currency_strength_bps": (
                dict(solved.get("currency_strength_bps") or {}) if valid else {}
            ),
            "source_contract_id": market_ticker.SCHEMA_VERSION,
        }
    return output


def _signed_factor_score(token: str, strengths: Mapping[str, Any]) -> float | None:
    currency = str(token or "")[:-1]
    suffix = str(token or "")[-1:]
    if currency not in strengths or suffix not in {"+", "-"}:
        return None
    try:
        value = float(strengths[currency])
    except (TypeError, ValueError):
        return None
    return value if suffix == "+" else -value


def assign_factor_episodes(
    rows: Sequence[dict[str, Any]],
    *,
    strength_surfaces: Mapping[tuple[int, int], Mapping[str, Any]] | None = None,
    window_sec: int = 900,
) -> int:
    """Mark one representative per signed-currency shock and time bucket.

    Prefer the dominant signed currency contribution from a synchronized
    all-pair surface frozen at the move endpoint. If it is unavailable, use a
    time-bucket-local recurrence label; unrelated buckets cannot affect it.
    """

    surfaces = strength_surfaces or {}
    width = max(60, int(window_sec))
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        base, quote = str(row.get("instrument") or "_").split("_", 1)
        side = move_side(row.get("move_direction"))
        tokens = [
            f"{base}{'+' if side > 0 else '-'}",
            f"{quote}{'-' if side > 0 else '+'}",
        ]
        row["factor_tokens"] = tokens
        start = int(census.parse_epoch(row.get("start_utc")) or 0)
        bucket = int((start + width // 2) // width)
        row["_factor_bucket"] = bucket
        buckets.setdefault(bucket, []).append(row)
    bucket_counts = {
        bucket: Counter(
            token for row in bucket_rows for token in row["factor_tokens"]
        )
        for bucket, bucket_rows in buckets.items()
    }
    representatives: dict[str, dict[str, Any]] = {}
    for row in rows:
        bucket = int(row.pop("_factor_bucket"))
        end_epoch = int(census.parse_epoch(row.get("end_utc")) or 0)
        horizon = closest_factor_strength_horizon(row.get("duration_minutes"))
        surface = dict(surfaces.get((end_epoch, horizon)) or {})
        strengths = surface.get("currency_strength_bps") or {}
        scored = {
            token: _signed_factor_score(token, strengths)
            for token in row["factor_tokens"]
        }
        if bool(surface.get("valid")) and all(
            value is not None for value in scored.values()
        ):
            ordered = sorted(
                row["factor_tokens"],
                key=lambda token: (-float(scored[token]), token),
            )
            primary = ordered[0]
            margin = float(scored[ordered[0]]) - float(scored[ordered[1]])
            method = "causal_all68_currency_strength"
            fallback_counts: dict[str, int] = {}
        else:
            counts = bucket_counts[bucket]
            ordered = sorted(
                row["factor_tokens"], key=lambda token: (-counts[token], token)
            )
            primary = ordered[0]
            margin = float(counts[ordered[0]] - counts[ordered[1]])
            method = "time_bucket_token_recurrence_fallback"
            fallback_counts = {
                token: int(counts[token]) for token in row["factor_tokens"]
            }
        row["factor_primary_method"] = method
        row["factor_strength_surface_status"] = str(
            surface.get("status") or "unavailable"
        )
        row["factor_strength_horizon_minutes"] = horizon
        row["factor_strength_as_of_utc"] = str(surface.get("as_of_utc") or "")
        row["factor_strength_as_of_age_sec"] = surface.get("as_of_age_sec")
        row["factor_strength_oldest_pair_age_sec"] = surface.get(
            "oldest_pair_age_sec"
        )
        row["factor_strength_observation_count"] = int(
            surface.get("observation_count") or 0
        )
        row["factor_strength_expected_observation_count"] = int(
            surface.get("expected_observation_count")
            or FACTOR_STRENGTH_EXPECTED_OBSERVATIONS
        )
        row["factor_strength_coverage_pct"] = surface.get("coverage_pct")
        row["factor_strength_currency_count"] = int(
            surface.get("currency_count") or 0
        )
        row["factor_strength_stale_instruments"] = list(
            surface.get("stale_instruments") or []
        )
        row["factor_strength_future_instruments"] = list(
            surface.get("future_instruments") or []
        )
        row["factor_strength_missing_currencies"] = list(
            surface.get("missing_currencies") or []
        )
        row["factor_strength_source_contract_id"] = str(
            surface.get("source_contract_id") or ""
        )
        row["factor_primary_scores_bps"] = {
            token: round(float(value), 6)
            for token, value in scored.items()
            if value is not None
        }
        row["factor_primary_fallback_counts"] = fallback_counts
        row["factor_primary_margin"] = round(margin, 6)
        row["factor_primary_margin_unit"] = (
            "bps"
            if method == "causal_all68_currency_strength"
            else "token_count"
        )
        row["factor_primary_ambiguous"] = bool(
            margin <= FACTOR_STRENGTH_AMBIGUITY_BPS
            if method == "causal_all68_currency_strength"
            else margin <= 0.0
        )
        factor_id = "live_factor_" + hashlib.sha256(
            f"{CONTRACT_ID}|{width}|{bucket}|{primary}".encode("utf-8")
        ).hexdigest()[:24]
        row["factor_primary_token"] = primary
        row["factor_episode_id"] = factor_id
        candidate = (
            abs(float(row.get("move_bps") or 0.0)),
            abs(float(row.get("executable_net_pips") or 0.0)),
            str(row.get("instrument") or ""),
        )
        incumbent = representatives.get(factor_id)
        incumbent_rank = (
            (
                abs(float(incumbent.get("move_bps") or 0.0)),
                abs(float(incumbent.get("executable_net_pips") or 0.0)),
                str(incumbent.get("instrument") or ""),
            )
            if incumbent is not None
            else (-1.0, -1.0, "")
        )
        if incumbent is None or candidate > incumbent_rank:
            representatives[factor_id] = row
    for row in rows:
        row["factor_representative"] = (
            representatives[row["factor_episode_id"]] is row
        )
    return len(representatives)


def attach_entry_quote(
    row: dict[str, Any],
    quote: Mapping[str, Any],
    *,
    observation_epoch: int,
    maximum_age_sec: float = 30.0,
) -> None:
    quote_epoch = census.parse_epoch(quote.get("time"))
    try:
        bid = float(quote.get("bid"))
        ask = float(quote.get("ask"))
        pip = float(quote.get("pip"))
    except (TypeError, ValueError):
        bid = ask = pip = 0.0
    age = (
        max(0.0, float(observation_epoch - quote_epoch))
        if quote_epoch is not None
        else None
    )
    valid = bool(
        quote_epoch is not None
        and bid > 0.0
        and ask >= bid
        and pip > 0.0
        and age is not None
        and age <= float(maximum_age_sec)
    )
    row.update(
        {
            "entry_quote_time": str(quote.get("time") or ""),
            "entry_quote_age_sec": round(age, 3) if age is not None else None,
            "entry_bid": bid if valid else None,
            "entry_ask": ask if valid else None,
            "entry_pip": pip if valid else None,
            "entry_spread_pips": round((ask - bid) / pip, 6) if valid else None,
            "entry_quote_fresh": valid,
            "forward_shadow_eligible": valid,
        }
    )
    broad = move_side(row.get("broad_research_side"))
    narrative = move_side(
        (row.get("continuous_narrative_state") or {}).get("direction")
    )
    strict = move_side(row.get("strict_forward_side"))
    continuation = move_side(row.get("move_direction"))
    row["forward_shadow_arms"] = {
        "technical_continuation": continuation,
        "broad_context_direction": broad,
        "strict_forward_direction": strict,
        "continuous_narrative_direction": narrative,
        "broad_context_plus_continuation": (
            continuation if broad == continuation else 0
        ),
    }


def selected_movers(state: Mapping[str, Any], limit: int) -> list[dict[str, Any]]:
    rows = ((state.get("rankings") or {}).get("live_velocity") or [])
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        instrument = str(raw.get("instrument") or "")
        if instrument in seen or "_" not in instrument or not move_side(
            raw.get("direction")
        ):
            continue
        if not raw.get("clear_move"):
            continue
        seen.add(instrument)
        output.append(dict(raw))
        if len(output) >= max(1, int(limit)):
            break
    return output


def _context_story(event: Mapping[str, Any], base: str, quote: str) -> dict[str, Any]:
    payload = news_audit.parse_payload(event)
    score, disposition = news_audit.pair_score(event, base, quote)
    return {
        "source_event_id": str(event.get("source_event_id") or ""),
        "story_cluster_id": str(event.get("story_cluster_id") or ""),
        "published_at_utc": str(event.get("published_at_utc") or ""),
        "first_seen_at_utc": str(event.get("first_seen_at_utc") or ""),
        "retrieved_at_utc": str(event.get("retrieved_at_utc") or ""),
        "effective_from_utc": str(event.get("effective_from_utc") or ""),
        "revised_at_utc": str(event.get("revised_at_utc") or ""),
        "event_version": int(event.get("event_version") or 1),
        "supersedes_source_event_id": str(
            event.get("supersedes_source_event_id") or ""
        ),
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
        "direction_disposition": disposition,
        "forward_signal_timely": payload.get("forward_signal_timely"),
        "directional_publish_eligible": bool(
            payload.get("directional_publish_eligible")
        ),
    }


def _context_stories(
    events: Sequence[Mapping[str, Any]], base: str, quote: str, limit: int = 5
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in reversed(events):
        key = news_audit.independent_story_key(event)
        if not key or key in seen:
            continue
        seen.add(key)
        story = _context_story(event, base, quote)
        if not story["headline"]:
            continue
        output.append(story)
        if len(output) >= limit:
            break
    return output


def mover_case(
    row: Mapping[str, Any],
    source_index: Mapping[str, Any],
    narrative_pairs: Mapping[str, Any],
    *,
    generated_epoch: int,
    lookback_minutes: int,
) -> dict[str, Any]:
    instrument = str(row.get("instrument") or "")
    base, quote = instrument.split("_", 1)
    start = census.parse_epoch(row.get("start_utc"))
    end = census.parse_epoch(row.get("end_utc"))
    actual = move_side(row.get("direction"))
    if start is None or end is None or not actual:
        raise ValueError("invalid_mover_clock_or_direction")
    pre_events = census.relevant_source_events(
        source_index,
        (base, quote),
        int(start - lookback_minutes * 60),
        int(start),
        True,
    )
    during_events = census.relevant_source_events(
        source_index, (base, quote), int(start + 1), int(end), False
    )
    strict = news_audit.directional_vote(
        pre_events, base, quote, int(start), strict_forward=True
    )
    broad = news_audit.directional_vote(pre_events, base, quote, int(start))
    strict_side = int(strict.get("side") or 0)
    broad_side = int(broad.get("side") or 0)
    if strict_side == actual:
        alignment = "aligned"
    elif strict_side == -actual:
        alignment = "opposed"
    else:
        alignment = "no_strict_direction"
    populations = Counter(str(event.get("source_population") or "") for event in pre_events)
    event_types = Counter(str(event.get("event_type") or "") for event in pre_events)
    narrative_state = dict(narrative_pairs.get(instrument) or {})
    narrative_side = move_side(narrative_state.get("direction"))
    return {
        "instrument": instrument,
        "move_direction": "up" if actual > 0 else "down",
        "start_utc": str(row.get("start_utc") or ""),
        "end_utc": str(row.get("end_utc") or ""),
        "age_sec": round(max(0.0, generated_epoch - end), 3),
        "duration_minutes": row.get("duration_minutes"),
        "gross_pips": row.get("gross_pips"),
        "executable_net_pips": row.get("executable_net_pips"),
        "move_bps": row.get("move_bps"),
        "velocity_bps_per_hour": row.get("velocity_bps_per_hour"),
        "pre_move_event_count": len(pre_events),
        "during_move_event_count": len(during_events),
        "pre_move_source_populations": dict(populations),
        "pre_move_event_types": dict(event_types),
        "strict_forward_side": (
            "long" if strict_side > 0 else "short" if strict_side < 0 else "neutral"
        ),
        "strict_forward_alignment": alignment,
        "strict_forward_independent_story_count": int(
            strict.get("independent_directional_stories") or 0
        ),
        "strict_forward_exclusions": strict.get("exclusions") or {},
        "strict_forward_stories": strict.get("stories") or [],
        "broad_research_side": (
            "long"
            if broad_side > 0
            else "short"
            if broad_side < 0
            else "neutral"
        ),
        "explanation_state": explanation_state(
            actual_side=actual,
            strict_side=strict_side,
            broad_side=broad_side,
            narrative_side=narrative_side,
            relevant_event_count=len(pre_events),
        ),
        "recent_context_stories": _context_stories(pre_events, base, quote),
        "during_context_stories": _context_stories(during_events, base, quote),
        "continuous_narrative_state": narrative_state,
        "execution_eligible": False,
    }


def render_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Live move/news snapshot",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        "Research-only movement-first diagnostic; it cannot place orders.",
        "",
        "| Pair | Move | Net pips | Duration | Explanation | Raw events |",
        "|---|---:|---:|---:|---|---:|",
    ]
    for row in payload.get("movers") or []:
        lines.append(
            f"| {row['instrument']} | {row['move_direction']} | "
            f"{row.get('executable_net_pips')} | {row.get('duration_minutes')}m | "
            f"{row.get('explanation_state')} | "
            f"{row.get('pre_move_event_count')} |"
        )
    lines.extend(
        [
            "",
            "Raw event count is context volume, not independent directional evidence.",
            "Strict direction requires a forward-timely publish-eligible source record.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    moves_path: Path = DEFAULT_MOVES,
    source_database: Path = DEFAULT_SOURCES,
    narrative_path: Path = DEFAULT_NARRATIVE,
    quotes_path: Path = DEFAULT_QUOTES,
    factor_history_path: Path = DEFAULT_FACTOR_HISTORY,
    output_path: Path = DEFAULT_OUTPUT,
    history_path: Path = DEFAULT_HISTORY,
    report_path: Path = DEFAULT_REPORT,
    lookback_minutes: int = 120,
    mover_count: int = 10,
) -> dict[str, Any]:
    generated = utc_now()
    generated_epoch = int(census.parse_epoch(generated) or time.time())
    moves_state = read_json(moves_path)
    movers = selected_movers(moves_state, mover_count)
    narrative = read_json(narrative_path)
    narrative_pairs = narrative.get("pairs") or {}
    if not isinstance(narrative_pairs, Mapping):
        narrative_pairs = {}
    quote_state = read_json(quotes_path)
    quotes = quote_state.get("quotes") or {}
    if not isinstance(quotes, Mapping):
        quotes = {}
    starts = [
        census.parse_epoch(row.get("start_utc"))
        for row in movers
    ]
    ends = [census.parse_epoch(row.get("end_utc")) for row in movers]
    valid_starts = [value for value in starts if value is not None]
    valid_ends = [value for value in ends if value is not None]
    minimum = (
        int(min(valid_starts) - lookback_minutes * 60)
        if valid_starts
        else generated_epoch - lookback_minutes * 60
    )
    maximum = int(max(valid_ends)) if valid_ends else generated_epoch
    source_index, source_highwater = census.load_source_index(
        source_database,
        minimum_effective_epoch=minimum,
        maximum_effective_epoch=maximum,
    )
    cases = [
        mover_case(
            row,
            source_index,
            narrative_pairs,
            generated_epoch=generated_epoch,
            lookback_minutes=lookback_minutes,
        )
        for row in movers
    ]
    factor_history = market_ticker.load_history(factor_history_path)
    factor_surfaces = build_causal_factor_strength_surfaces(cases, factor_history)
    factor_episode_count = assign_factor_episodes(
        cases, strength_surfaces=factor_surfaces
    )
    for row in cases:
        row["observation_utc"] = generated
        attach_entry_quote(
            row,
            quotes.get(str(row.get("instrument") or "")) or {},
            observation_epoch=generated_epoch,
        )
    for row in cases:
        row["contract_id"] = CONTRACT_ID
        row["case_id"] = stable_case_id(row)
    history = record_cases(history_path, cases, recorded_utc=generated)
    payload = {
        "schema_version": 5,
        "contract_id": CONTRACT_ID,
        "generated_utc": generated,
        "moves_generated_utc": moves_state.get("generated_utc"),
        "quotes_generated_utc": quote_state.get("generated_utc"),
        "source_history_highwater_utc": source_highwater,
        "factor_strength_history": str(factor_history_path.resolve()),
        "factor_strength_contract": {
            "source_contract_id": market_ticker.SCHEMA_VERSION,
            "expected_pair_observations": FACTOR_STRENGTH_EXPECTED_OBSERVATIONS,
            "minimum_fresh_pair_observations": FACTOR_STRENGTH_MIN_OBSERVATIONS,
            "maximum_pair_age_sec": FACTOR_STRENGTH_MAX_PAIR_AGE_SEC,
            "declared_horizons_minutes": list(FACTOR_STRENGTH_HORIZONS),
            "watermark": "at_or_before_move_end",
            "role": "after_the_fact_factor_clustering_only",
        },
        "factor_strength_surface_count": len(factor_surfaces),
        "causal_factor_strength_mover_count": sum(
            row.get("factor_primary_method") == "causal_all68_currency_strength"
            for row in cases
        ),
        "fallback_factor_mover_count": sum(
            row.get("factor_primary_method")
            == "time_bucket_token_recurrence_fallback"
            for row in cases
        ),
        "lookback_minutes": int(lookback_minutes),
        "mover_count": len(cases),
        "factor_episode_count": factor_episode_count,
        "factor_representative_count": sum(
            bool(row.get("factor_representative")) for row in cases
        ),
        "fresh_entry_quote_count": sum(
            bool(row.get("entry_quote_fresh")) for row in cases
        ),
        "strict_directional_mover_count": sum(
            row["strict_forward_alignment"] != "no_strict_direction"
            for row in cases
        ),
        "strict_aligned_mover_count": sum(
            row["strict_forward_alignment"] == "aligned" for row in cases
        ),
        "history_database": str(history_path.resolve()),
        "inserted_case_count": history["inserted"],
        "retained_case_count": history["total"],
        "movers": cases,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "diagnostic_only",
    }
    atomic_json(output_path, payload)
    atomic_text(report_path, render_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--moves", type=Path, default=DEFAULT_MOVES)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--narrative", type=Path, default=DEFAULT_NARRATIVE)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument(
        "--factor-history", type=Path, default=DEFAULT_FACTOR_HISTORY
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--lookback-minutes", type=int, default=120)
    parser.add_argument("--mover-count", type=int, default=10)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        payload = run(
            moves_path=args.moves,
            source_database=args.sources,
            narrative_path=args.narrative,
            quotes_path=args.quotes,
            factor_history_path=args.factor_history,
            output_path=args.output,
            history_path=args.history,
            report_path=args.report,
            lookback_minutes=max(5, args.lookback_minutes),
            mover_count=max(1, args.mover_count),
        )
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "mover_count": payload["mover_count"],
                    "strict_directional_mover_count": payload[
                        "strict_directional_mover_count"
                    ],
                }
            ),
            flush=True,
        )
        if args.interval_sec <= 0:
            break
        if args.duration_sec > 0 and time.time() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
