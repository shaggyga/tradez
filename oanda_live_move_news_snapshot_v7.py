#!/usr/bin/env python3
"""Boundary-independent factor episodes for the causal move/news snapshot.

V6R2 remains frozen in its original code, state, report, and history files.
This prospective V7R2 research cohort reuses the V6R2 point-in-time V12
narrative join, but replaces centered time-bucket factor deduplication with
deterministic interval components. Moves with the same causally assigned
primary signed currency factor share an episode when their intervals overlap
or are adjacent within the declared gap. Primary identity is measured at a
fixed five-minute cutoff after move onset and then frozen on first prospective
observation. Existing episode roots are retained when a component is extended;
a bridge between two already-frozen roots fails closed. Rows without a valid
causal all-68 strength surface remain explicitly unresolved and are never
cross-pair deduplicated.

The module has no broker, authorization, lifecycle, promotion, or execution
surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_live_move_news_snapshot as v5
import oanda_live_move_news_snapshot_v6 as v6
import oanda_major_move_gap_census as census
import oanda_market_sentiment_ticker as market_ticker
from oanda_continuous_narrative_meter_v12 import (
    METER_ACTIVATED_UTC,
    METER_CONTRACT_ID,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"

DEFAULT_MOVES = v6.DEFAULT_MOVES
DEFAULT_SOURCES = v6.DEFAULT_SOURCES
DEFAULT_NARRATIVE_DATABASE = v6.DEFAULT_NARRATIVE_DATABASE
DEFAULT_QUOTES = v6.DEFAULT_QUOTES
DEFAULT_FACTOR_HISTORY = v6.DEFAULT_FACTOR_HISTORY
DEFAULT_OUTPUT = STATE / "live_move_news_snapshot_v7r2.json"
DEFAULT_HISTORY = STATE / "live_move_news_cases_v7r2.sqlite"
DEFAULT_REPORT = (
    DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_CURRENT_V7R2.md"
)

CONTRACT_ID = (
    "live_move_news_snapshot_v7r2_overlap_factor_episodes_"
    "fixed_onset_primary_20260827"
)
CONTRACT_ACTIVATED_UTC = dt.datetime(2026, 8, 27, 8, 42, tzinfo=dt.timezone.utc)
FACTOR_EPISODE_CONTRACT_ID = (
    "live_factor_episode_overlap_adjacency_v2_gap60_"
    "fixed_onset5m_frozen_primary_20260827"
)
PREVIOUS_CONTRACT_ID = v6.CONTRACT_ID
SCHEMA_VERSION = 7
MAXIMUM_EPISODE_GAP_SEC = 60
FACTOR_CLASSIFICATION_MINUTES = 5


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _precise_epoch(value: Any) -> float | None:
    return v6._precise_epoch(value)


def _episode_id(primary: str, anchor_epoch: float, maximum_gap_sec: int) -> str:
    anchor_microseconds = int(round(float(anchor_epoch) * 1_000_000.0))
    material = "|".join(
        (
            FACTOR_EPISODE_CONTRACT_ID,
            str(maximum_gap_sec),
            str(primary),
            str(anchor_microseconds),
        )
    )
    return "live_factor_v7_" + hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:24]


def _unresolved_primary(row: Mapping[str, Any]) -> str:
    return "UNRESOLVED:" + ":".join(
        (
            str(row.get("instrument") or "unknown"),
            str(row.get("move_direction") or "unknown"),
            str(row.get("start_utc") or "unknown"),
        )
    )


def normalize_primary_factor_assignments(rows: Sequence[dict[str, Any]]) -> None:
    """Fail closed when the V5 primary assignment lacked causal strength.

    V5 populates the all-68 strength diagnostics that V7 retains.  Its legacy
    recurrence fallback, however, depends on centered time buckets.  V7 never
    uses that fallback to merge pairs: each unresolved row gets a unique token.
    """

    for row in rows:
        if str(row.get("factor_primary_method") or "").startswith(
            "causal_all68_currency_strength"
        ):
            continue
        row["factor_primary_token"] = _unresolved_primary(row)
        row["factor_primary_method"] = "unresolved_without_causal_strength"
        row["factor_primary_ambiguous"] = True
        row["factor_primary_margin"] = 0.0
        row["factor_primary_margin_unit"] = "unresolved"


def assign_fixed_onset_primary_factors(
    rows: Sequence[dict[str, Any]],
    history_rows: Sequence[Mapping[str, Any]],
    *,
    classification_minutes: int = FACTOR_CLASSIFICATION_MINUTES,
) -> dict[tuple[int, int], dict[str, Any]]:
    """Assign primary factors from one fixed post-onset causal surface.

    Rolling mover endpoints can lengthen while a move remains on the live
    leaderboard. Using that endpoint for the primary factor can therefore
    change a case from (for example) USD+ to PLN-. V7R2 instead evaluates the
    synchronized all-68 surface exactly ``classification_minutes`` after the
    immutable move start. This is an after-the-fact dedup label only; it is not
    a forecasting feature and cannot authorize execution.
    """

    minutes = max(1, int(classification_minutes))
    classification_rows: list[dict[str, Any]] = []
    originals: list[dict[str, Any]] = []
    for row in rows:
        start = census.parse_epoch(row.get("start_utc"))
        if start is None:
            row["factor_classification_status"] = "invalid_move_start"
            continue
        cutoff = int(start) + minutes * 60
        clone = dict(row)
        clone["end_utc"] = census.iso_epoch(cutoff)
        clone["duration_minutes"] = minutes
        classification_rows.append(clone)
        originals.append(row)

    surfaces = v5.build_causal_factor_strength_surfaces(
        classification_rows, history_rows
    )
    if classification_rows:
        v5.assign_factor_episodes(
            classification_rows, strength_surfaces=surfaces
        )

    copied_fields = (
        "factor_tokens",
        "factor_primary_token",
        "factor_strength_surface_status",
        "factor_strength_horizon_minutes",
        "factor_strength_as_of_utc",
        "factor_strength_as_of_age_sec",
        "factor_strength_oldest_pair_age_sec",
        "factor_strength_observation_count",
        "factor_strength_expected_observation_count",
        "factor_strength_coverage_pct",
        "factor_strength_currency_count",
        "factor_strength_stale_instruments",
        "factor_strength_future_instruments",
        "factor_strength_missing_currencies",
        "factor_strength_source_contract_id",
        "factor_primary_scores_bps",
        "factor_primary_fallback_counts",
        "factor_primary_margin",
        "factor_primary_margin_unit",
        "factor_primary_ambiguous",
    )
    for row, classified in zip(originals, classification_rows):
        for field in copied_fields:
            row[field] = classified.get(field)
        start = int(census.parse_epoch(row.get("start_utc")) or 0)
        row["factor_classification_cutoff_utc"] = census.iso_epoch(
            start + minutes * 60
        )
        row["factor_classification_minutes"] = minutes
        row["factor_classification_watermark"] = (
            "synchronized_all68_at_fixed_move_start_plus_5m"
        )
        if classified.get("factor_primary_method") == (
            "causal_all68_currency_strength"
        ):
            row["factor_primary_method"] = (
                "causal_all68_currency_strength_fixed_onset_5m"
            )
            row["factor_classification_status"] = "ready"
        else:
            row["factor_primary_method"] = str(
                classified.get("factor_primary_method") or "unavailable"
            )
            row["factor_classification_status"] = "unresolved"

    normalize_primary_factor_assignments(rows)
    return surfaces


def load_frozen_memberships(path: Path) -> dict[str, dict[str, str]]:
    """Load first-observation factor identity without creating the database."""

    if not path.exists():
        return {}
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro", uri=True
        )
        try:
            rows = connection.execute(
                "SELECT case_id,factor_episode_id,factor_primary_token "
                "FROM factor_episode_membership"
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error:
        return {}
    return {
        str(case_id): {
            "factor_episode_id": str(episode_id),
            "factor_primary_token": str(primary),
        }
        for case_id, episode_id, primary in rows
    }


def freeze_observed_primary_factors(
    rows: Sequence[dict[str, Any]],
    memberships: Mapping[str, Mapping[str, str]],
) -> None:
    """Retain the primary token registered on a case's first observation."""

    for row in rows:
        persisted = memberships.get(str(row.get("case_id") or ""))
        if not persisted:
            row["factor_primary_frozen_from_history"] = False
            continue
        observed = str(row.get("factor_primary_token") or "")
        frozen = str(persisted.get("factor_primary_token") or "")
        row["factor_primary_observed_token"] = observed
        row["factor_primary_token"] = frozen
        row["factor_primary_frozen_from_history"] = True
        row["factor_primary_changed_after_freeze"] = bool(observed != frozen)


def assign_overlap_factor_episodes(
    rows: Sequence[dict[str, Any]],
    *,
    maximum_gap_sec: int = MAXIMUM_EPISODE_GAP_SEC,
    frozen_memberships: Mapping[str, Mapping[str, str]] | None = None,
) -> int:
    """Assign deterministic interval-connected episodes by primary factor.

    Components are computed separately for each primary signed-currency token.
    Two intervals are connected when they overlap or the later one begins no
    more than ``maximum_gap_sec`` after the component's current end.  Sorting
    makes the result independent of input order and of wall-clock bucket edges.
    """

    gap = max(0, int(maximum_gap_sec))
    persisted = frozen_memberships or {}
    by_primary: dict[str, list[tuple[float, float, dict[str, Any]]]] = {}
    invalid: list[dict[str, Any]] = []
    for row in rows:
        primary = str(row.get("factor_primary_token") or "")
        start = _precise_epoch(row.get("start_utc"))
        end = _precise_epoch(row.get("end_utc"))
        if not primary or start is None or end is None or end < start:
            invalid.append(row)
            continue
        by_primary.setdefault(primary, []).append((start, end, row))

    components: list[tuple[str, float, float, list[dict[str, Any]]]] = []
    for primary in sorted(by_primary):
        ordered = sorted(
            by_primary[primary],
            key=lambda item: (
                item[0],
                item[1],
                str(item[2].get("instrument") or ""),
            ),
        )
        component_start = ordered[0][0]
        component_end = ordered[0][1]
        members = [ordered[0][2]]
        for start, end, row in ordered[1:]:
            if start <= component_end + gap:
                component_end = max(component_end, end)
                members.append(row)
                continue
            components.append(
                (primary, component_start, component_end, list(members))
            )
            component_start = start
            component_end = end
            members = [row]
        components.append((primary, component_start, component_end, list(members)))

    for row in invalid:
        start = _precise_epoch(row.get("start_utc")) or 0.0
        primary = _unresolved_primary(row)
        row["factor_primary_token"] = primary
        row["factor_primary_method"] = "invalid_interval_unresolved"
        row["factor_primary_ambiguous"] = True
        components.append((primary, start, start, [row]))

    for primary, start, end, members in components:
        persisted_episode_ids = sorted(
            {
                str(persisted_entry.get("factor_episode_id") or "")
                for member in members
                if (
                    persisted_entry := persisted.get(
                        str(member.get("case_id") or "")
                    )
                )
                and str(persisted_entry.get("factor_episode_id") or "")
            }
        )
        merge_conflict = len(persisted_episode_ids) > 1
        episode_id = (
            persisted_episode_ids[0]
            if persisted_episode_ids
            else _episode_id(primary, start, gap)
        )
        representative = max(
            members,
            key=lambda row: (
                abs(float(row.get("move_bps") or 0.0)),
                abs(float(row.get("executable_net_pips") or 0.0)),
                str(row.get("instrument") or ""),
            ),
        )
        anchor_utc = dt.datetime.fromtimestamp(
            start, tz=dt.timezone.utc
        ).isoformat()
        end_utc = dt.datetime.fromtimestamp(end, tz=dt.timezone.utc).isoformat()
        for row in members:
            row["factor_episode_id"] = episode_id
            row["factor_representative"] = row is representative
            row["factor_episode_contract_id"] = FACTOR_EPISODE_CONTRACT_ID
            row["factor_episode_grouping_method"] = (
                "same_primary_interval_overlap_or_adjacency"
            )
            row["factor_episode_maximum_gap_sec"] = gap
            row["factor_episode_anchor_utc"] = anchor_utc
            row["factor_episode_interval_end_utc"] = end_utc
            row["factor_episode_member_count"] = len(members)
            row["factor_episode_frozen_root_reused"] = bool(
                persisted_episode_ids
            )
            row["factor_episode_merge_conflict"] = merge_conflict
            row["factor_episode_persisted_roots"] = persisted_episode_ids
    return len(components)


def stable_case_id(row: Mapping[str, Any]) -> str:
    material = "|".join(
        (
            CONTRACT_ID,
            str(row.get("instrument") or ""),
            str(row.get("start_utc") or ""),
            str(row.get("move_direction") or ""),
        )
    )
    return "live_move_news_case_v7_" + hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:32]


def connect_history(path: Path) -> sqlite3.Connection:
    connection = v6.connect_history(path)
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS factor_episode_contract_registry(
            factor_episode_contract_id TEXT PRIMARY KEY,
            snapshot_contract_id TEXT NOT NULL,
            previous_snapshot_contract_id TEXT NOT NULL,
            grouping_method TEXT NOT NULL,
            maximum_gap_sec INTEGER NOT NULL,
            unresolved_primary_policy TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            created_utc TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS factor_episode_contract_registry_no_update
            BEFORE UPDATE ON factor_episode_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS factor_episode_contract_registry_no_delete
            BEFORE DELETE ON factor_episode_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TABLE IF NOT EXISTS factor_episode_membership(
            case_id TEXT PRIMARY KEY,
            factor_episode_contract_id TEXT NOT NULL,
            factor_episode_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL,
            registered_utc TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS factor_episode_membership_no_update
            BEFORE UPDATE ON factor_episode_membership
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS factor_episode_membership_no_delete
            BEFORE DELETE ON factor_episode_membership
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TABLE IF NOT EXISTS factor_episode_membership_conflicts(
            conflict_id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            persisted_factor_episode_id TEXT NOT NULL,
            observed_factor_episode_id TEXT NOT NULL,
            detected_utc TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
        );
        CREATE TRIGGER IF NOT EXISTS factor_episode_membership_conflicts_no_update
            BEFORE UPDATE ON factor_episode_membership_conflicts
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS factor_episode_membership_conflicts_no_delete
            BEFORE DELETE ON factor_episode_membership_conflicts
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def record_cases(
    path: Path,
    rows: Sequence[dict[str, Any]],
    *,
    recorded_utc: str,
    meter_database: Path,
) -> dict[str, int]:
    connection = connect_history(path)
    inserted = 0
    new_conflicts = 0
    try:
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO mover_case_contract_registry VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    CONTRACT_ID,
                    CONTRACT_ACTIVATED_UTC.isoformat(),
                    METER_CONTRACT_ID,
                    str(meter_database.resolve()),
                    (
                        "latest complete V12 bucket with clock_utc and sealed_at_utc "
                        "both at_or_before mover start"
                    ),
                    1,
                    1,
                    0,
                    recorded_utc,
                ),
            )
            connection.execute(
                "INSERT OR IGNORE INTO factor_episode_contract_registry VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    FACTOR_EPISODE_CONTRACT_ID,
                    CONTRACT_ID,
                    PREVIOUS_CONTRACT_ID,
                    (
                        "fixed_onset_primary_then_same_primary_"
                        "interval_overlap_or_adjacency"
                    ),
                    MAXIMUM_EPISODE_GAP_SEC,
                    (
                        "fixed onset + first-observation freeze; unresolved "
                        "rows never cross-pair deduplicate"
                    ),
                    1,
                    0,
                    recorded_utc,
                ),
            )
            for row in rows:
                case_id = stable_case_id(row)
                row["case_id"] = case_id
                episode_id = str(row.get("factor_episode_id") or "")
                existing = connection.execute(
                    "SELECT factor_episode_id FROM factor_episode_membership "
                    "WHERE case_id=?",
                    (case_id,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO factor_episode_membership VALUES(?,?,?,?,?)",
                        (
                            case_id,
                            FACTOR_EPISODE_CONTRACT_ID,
                            episode_id,
                            str(row.get("factor_primary_token") or ""),
                            recorded_utc,
                        ),
                    )
                    row["factor_episode_membership_conflict"] = False
                elif str(existing[0]) != episode_id:
                    conflict_id = "factor_membership_conflict_" + hashlib.sha256(
                        "|".join((case_id, str(existing[0]), episode_id)).encode(
                            "utf-8"
                        )
                    ).hexdigest()[:24]
                    before = connection.total_changes
                    connection.execute(
                        "INSERT OR IGNORE INTO factor_episode_membership_conflicts "
                        "VALUES(?,?,?,?,?,?,?)",
                        (
                            conflict_id,
                            case_id,
                            str(existing[0]),
                            episode_id,
                            recorded_utc,
                            1,
                            0,
                        ),
                    )
                    new_conflicts += int(connection.total_changes > before)
                    row["factor_episode_membership_conflict"] = True
                    row["persisted_factor_episode_id"] = str(existing[0])
                else:
                    row["factor_episode_membership_conflict"] = False
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
        total = int(connection.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0])
        conflict_total = int(
            connection.execute(
                "SELECT COUNT(*) FROM factor_episode_membership_conflicts"
            ).fetchone()[0]
        )
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()
    if integrity != "ok":
        raise sqlite3.IntegrityError(f"mover history integrity:{integrity}")
    return {
        "inserted": inserted,
        "total": total,
        "new_membership_conflicts": new_conflicts,
        "membership_conflict_total": conflict_total,
    }


def render_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Live move/news snapshot V7R2",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        (
            "Research-only. V12 narrative clocks are sealed by move start and "
            "same-primary factor episodes use interval overlap/adjacency rather "
            "than centered time buckets. Primary identity is measured at the "
            "fixed five-minute onset cutoff and prospectively frozen. It cannot "
            "place orders."
        ),
        "",
        "| Pair | Move | Net pips | Start | Primary factor | Episode | Rep |",
        "|---|---:|---:|---|---|---|---:|",
    ]
    for row in payload.get("movers") or []:
        lines.append(
            f"| {row.get('instrument')} | {row.get('move_direction')} | "
            f"{row.get('executable_net_pips')} | {row.get('start_utc')} | "
            f"{row.get('factor_primary_token')} | "
            f"{row.get('factor_episode_id')} | "
            f"{int(bool(row.get('factor_representative')))} |"
        )
    lines.extend(
        [
            "",
            (
                "Unresolved primary factors never cross-pair deduplicate. "
                "Membership conflicts fail the cohort's integrity flag closed."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    moves_path: Path = DEFAULT_MOVES,
    source_database: Path = DEFAULT_SOURCES,
    narrative_database: Path = DEFAULT_NARRATIVE_DATABASE,
    quotes_path: Path = DEFAULT_QUOTES,
    factor_history_path: Path = DEFAULT_FACTOR_HISTORY,
    output_path: Path = DEFAULT_OUTPUT,
    history_path: Path = DEFAULT_HISTORY,
    report_path: Path = DEFAULT_REPORT,
    lookback_minutes: int = 120,
    mover_count: int = 10,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    observed = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    generated = observed.isoformat()
    generated_epoch = int(observed.timestamp())
    moves_state = v5.read_json(moves_path)
    movers = v5.selected_movers(moves_state, mover_count)
    quotes_state = v5.read_json(quotes_path)
    quotes = quotes_state.get("quotes") or {}
    if not isinstance(quotes, Mapping):
        quotes = {}
    starts = [census.parse_epoch(row.get("start_utc")) for row in movers]
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
    meter_status = v6.meter_database_status(narrative_database)
    meter_connection: sqlite3.Connection | None = None
    if meter_status.get("ok"):
        meter_connection = v6._read_only_database(narrative_database)
    cases: list[dict[str, Any]] = []
    try:
        for mover in movers:
            instrument = str(mover.get("instrument") or "")
            start_epoch = census.parse_epoch(mover.get("start_utc"))
            if start_epoch is None or meter_connection is None:
                pair_state, provenance = v6._unavailable_pair_state(
                    instrument,
                    "meter_database_unavailable_or_invalid"
                    if meter_connection is None
                    else "invalid_move_start",
                )
            else:
                pair_state, provenance = v6.causal_pair_state(
                    meter_connection,
                    instrument,
                    move_start_epoch=int(start_epoch),
                )
            case = v5.mover_case(
                mover,
                source_index,
                {instrument: pair_state},
                generated_epoch=generated_epoch,
                lookback_minutes=lookback_minutes,
            )
            case["continuous_narrative_provenance"] = provenance
            cases.append(case)
    finally:
        if meter_connection is not None:
            meter_connection.close()

    for row in cases:
        row["contract_id"] = CONTRACT_ID
        row["case_id"] = stable_case_id(row)

    factor_history = market_ticker.load_history(factor_history_path)
    factor_surfaces = assign_fixed_onset_primary_factors(cases, factor_history)
    frozen_memberships = load_frozen_memberships(history_path)
    freeze_observed_primary_factors(cases, frozen_memberships)
    factor_episode_count = assign_overlap_factor_episodes(
        cases, frozen_memberships=frozen_memberships
    )
    factor_episode_merge_conflict_count = sum(
        bool(row.get("factor_episode_merge_conflict")) for row in cases
    )
    for row in cases:
        row["observation_utc"] = generated
        v5.attach_entry_quote(
            row,
            quotes.get(str(row.get("instrument") or "")) or {},
            observation_epoch=generated_epoch,
        )

    if observed >= CONTRACT_ACTIVATED_UTC:
        history = record_cases(
            history_path,
            cases,
            recorded_utc=generated,
            meter_database=narrative_database,
        )
    else:
        history = {
            "inserted": 0,
            "total": 0,
            "new_membership_conflicts": 0,
            "membership_conflict_total": 0,
        }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "previous_contract_id": PREVIOUS_CONTRACT_ID,
        "contract_activated_utc": CONTRACT_ACTIVATED_UTC.isoformat(),
        "generated_utc": generated,
        "moves_generated_utc": moves_state.get("generated_utc"),
        "quotes_generated_utc": quotes_state.get("generated_utc"),
        "source_history_highwater_utc": source_highwater,
        "narrative_join_contract": {
            "meter_database": str(narrative_database.resolve()),
            "meter_contract_id": METER_CONTRACT_ID,
            "meter_activation_utc": METER_ACTIVATED_UTC.isoformat(),
            "availability_semantics": (
                "latest complete bucket with clock_utc<=move_start and "
                "sealed_at_utc<=move_start"
            ),
            "partial_live_excluded": True,
            "sealed_v12_database_only": True,
            "research_only": True,
            "execution_eligible": False,
        },
        "factor_episode_contract": {
            "contract_id": FACTOR_EPISODE_CONTRACT_ID,
            "previous_snapshot_contract_id": PREVIOUS_CONTRACT_ID,
            "grouping_method": "same_primary_interval_overlap_or_adjacency",
            "maximum_gap_sec": MAXIMUM_EPISODE_GAP_SEC,
            "centered_time_buckets_used": False,
            "input_order_invariant": True,
            "primary_factor_classification_minutes": (
                FACTOR_CLASSIFICATION_MINUTES
            ),
            "primary_factor_watermark": (
                "fixed_move_start_plus_5m_then_first_observation_frozen"
            ),
            "existing_episode_root_policy": (
                "reuse_frozen_root; multiple_roots_fail_closed"
            ),
            "unresolved_primary_policy": (
                "unresolved rows never cross-pair deduplicate"
            ),
            "role": "after_the_fact_factor_clustering_only",
        },
        "narrative_meter_database_status": meter_status,
        "narrative_causally_available_mover_count": sum(
            bool((row.get("continuous_narrative_state") or {}).get("state_available"))
            for row in cases
        ),
        "narrative_unavailable_mover_count": sum(
            not bool(
                (row.get("continuous_narrative_state") or {}).get("state_available")
            )
            for row in cases
        ),
        "factor_strength_history": str(factor_history_path.resolve()),
        "factor_strength_surface_count": len(factor_surfaces),
        "causal_factor_strength_mover_count": sum(
            str(row.get("factor_primary_method") or "").startswith(
                "causal_all68_currency_strength"
            )
            for row in cases
        ),
        "unresolved_factor_mover_count": sum(
            not str(row.get("factor_primary_method") or "").startswith(
                "causal_all68_currency_strength"
            )
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
        "factor_episode_membership_new_conflict_count": history[
            "new_membership_conflicts"
        ],
        "factor_episode_membership_conflict_total": history[
            "membership_conflict_total"
        ],
        "factor_episode_merge_conflict_count": (
            factor_episode_merge_conflict_count
        ),
        "factor_episode_integrity_ok": bool(
            history["membership_conflict_total"] == 0
            and factor_episode_merge_conflict_count == 0
        ),
        "movers": cases,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "broker_access": False,
        "supported_decision": "diagnostic_only",
    }
    v5.atomic_json(output_path, payload)
    v5.atomic_text(report_path, render_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--moves", type=Path, default=DEFAULT_MOVES)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument(
        "--narrative-database", type=Path, default=DEFAULT_NARRATIVE_DATABASE
    )
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--factor-history", type=Path, default=DEFAULT_FACTOR_HISTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--lookback-minutes", type=int, default=120)
    parser.add_argument("--mover-count", type=int, default=10)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run(
            moves_path=args.moves,
            source_database=args.sources,
            narrative_database=args.narrative_database,
            quotes_path=args.quotes,
            factor_history_path=args.factor_history,
            output_path=args.output,
            history_path=args.history,
            report_path=args.report,
            lookback_minutes=max(1, args.lookback_minutes),
            mover_count=max(1, args.mover_count),
        )
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "mover_count": payload["mover_count"],
                    "factor_episode_count": payload["factor_episode_count"],
                    "factor_episode_integrity_ok": payload[
                        "factor_episode_integrity_ok"
                    ],
                    "retained_case_count": payload["retained_case_count"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.interval_sec <= 0:
            return 0
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
