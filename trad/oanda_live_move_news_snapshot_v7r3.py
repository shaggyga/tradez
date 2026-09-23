#!/usr/bin/env python3
"""Transitive-root V7R3 mover/news snapshot cohort.

V6R2, the rejected V7 preflight, and the rejected V7R2 fixed-primary preflight
remain untouched. V7R3 retains V7R2's fixed five-minute onset classification,
but represents later top-N observation-order bridges as append-only root-union
aliases. A union can only reduce the number of independent factor episodes;
case memberships are never rewritten. Different-primary and disjoint intervals
never merge. The module is research-only and has no broker surface.
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

import oanda_live_move_news_snapshot_v7 as v7r2


v5 = v7r2.v5
v6 = v7r2.v6
census = v7r2.census
market_ticker = v7r2.market_ticker

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"

DEFAULT_MOVES = v7r2.DEFAULT_MOVES
DEFAULT_SOURCES = v7r2.DEFAULT_SOURCES
DEFAULT_NARRATIVE_DATABASE = v7r2.DEFAULT_NARRATIVE_DATABASE
DEFAULT_QUOTES = v7r2.DEFAULT_QUOTES
DEFAULT_FACTOR_HISTORY = v7r2.DEFAULT_FACTOR_HISTORY
DEFAULT_OUTPUT = STATE / "live_move_news_snapshot_v7r3.json"
DEFAULT_HISTORY = STATE / "live_move_news_cases_v7r3.sqlite"
DEFAULT_REPORT = (
    DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_CURRENT_V7R3.md"
)

CONTRACT_ID = (
    "live_move_news_snapshot_v7r3_transitive_factor_root_union_20260827"
)
CONTRACT_ACTIVATED_UTC = dt.datetime(2026, 8, 27, 8, 58, tzinfo=dt.timezone.utc)
FACTOR_EPISODE_CONTRACT_ID = (
    "live_factor_episode_overlap_adjacency_v3_gap60_fixed_onset5m_"
    "append_only_transitive_root_union_20260827"
)
PREVIOUS_CONTRACT_ID = v7r2.CONTRACT_ID
BASELINE_CONTRACT_ID = v6.CONTRACT_ID
SCHEMA_VERSION = 7
MAXIMUM_EPISODE_GAP_SEC = v7r2.MAXIMUM_EPISODE_GAP_SEC
FACTOR_CLASSIFICATION_MINUTES = v7r2.FACTOR_CLASSIFICATION_MINUTES


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def stable_case_id(row: Mapping[str, Any]) -> str:
    material = "|".join(
        (
            CONTRACT_ID,
            str(row.get("instrument") or ""),
            str(row.get("start_utc") or ""),
            str(row.get("move_direction") or ""),
        )
    )
    return "live_move_news_case_v7r3_" + hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:32]


def _new_root_id(primary: str, start_epoch: float) -> str:
    material = "|".join(
        (
            FACTOR_EPISODE_CONTRACT_ID,
            str(primary),
            str(int(round(start_epoch * 1_000_000.0))),
        )
    )
    return "live_factor_v7r3_" + hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:24]


def resolve_root(root: str, edges: Mapping[str, str]) -> tuple[str, bool]:
    """Resolve an append-only alias chain and report cycles fail-closed."""

    current = str(root or "")
    seen: set[str] = set()
    while current in edges:
        if current in seen:
            return current, True
        seen.add(current)
        current = str(edges[current])
    return current, False


def load_union_registry(path: Path) -> dict[str, Any]:
    empty = {
        "memberships": {},
        "edges": {},
        "root_registered_utc": {},
        "cycle": False,
    }
    if not path.exists():
        return empty
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro", uri=True
        )
        try:
            memberships = connection.execute(
                "SELECT case_id,factor_episode_id,factor_primary_token,registered_utc "
                "FROM factor_episode_membership"
            ).fetchall()
            merge_rows = connection.execute(
                "SELECT from_root_id,into_root_id FROM factor_episode_root_merges "
                "ORDER BY detected_utc,merge_id"
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error:
        return empty
    edges: dict[str, str] = {}
    cycle = False
    for source, target in merge_rows:
        source_text = str(source)
        target_text = str(target)
        existing = edges.get(source_text)
        if existing is not None and existing != target_text:
            cycle = True
        edges[source_text] = target_text
    for root in set(edges) | set(edges.values()):
        _, found_cycle = resolve_root(root, edges)
        cycle = cycle or found_cycle
    registered: dict[str, str] = {}
    output_memberships: dict[str, dict[str, str]] = {}
    for case_id, root, primary, observed in memberships:
        raw = str(root)
        canonical, found_cycle = resolve_root(raw, edges)
        cycle = cycle or found_cycle
        output_memberships[str(case_id)] = {
            "raw_root_id": raw,
            "canonical_root_id": canonical,
            "factor_primary_token": str(primary),
            "registered_utc": str(observed),
        }
        prior = registered.get(canonical)
        if prior is None or str(observed) < prior:
            registered[canonical] = str(observed)
    return {
        "memberships": output_memberships,
        "edges": edges,
        "root_registered_utc": registered,
        "cycle": cycle,
    }


def freeze_observed_primaries(
    rows: Sequence[dict[str, Any]], registry: Mapping[str, Any]
) -> None:
    memberships = registry.get("memberships") or {}
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


def _components(
    rows: Sequence[dict[str, Any]], maximum_gap_sec: int
) -> list[tuple[str, float, float, list[dict[str, Any]]]]:
    grouped: dict[str, list[tuple[float, float, dict[str, Any]]]] = {}
    for row in rows:
        primary = str(row.get("factor_primary_token") or "")
        start = v7r2._precise_epoch(row.get("start_utc"))
        end = v7r2._precise_epoch(row.get("end_utc"))
        if not primary or start is None or end is None or end < start:
            primary = v7r2._unresolved_primary(row)
            row["factor_primary_token"] = primary
            row["factor_primary_method"] = "invalid_interval_unresolved"
            start = start or 0.0
            end = start
        grouped.setdefault(primary, []).append((start, end, row))
    output: list[tuple[str, float, float, list[dict[str, Any]]]] = []
    gap = max(0, int(maximum_gap_sec))
    for primary in sorted(grouped):
        ordered = sorted(
            grouped[primary],
            key=lambda item: (
                item[0],
                item[1],
                str(item[2].get("instrument") or ""),
            ),
        )
        component_start, component_end, first = ordered[0]
        members = [first]
        for start, end, row in ordered[1:]:
            if start <= component_end + gap:
                component_end = max(component_end, end)
                members.append(row)
            else:
                output.append(
                    (primary, component_start, component_end, list(members))
                )
                component_start, component_end, members = start, end, [row]
        output.append((primary, component_start, component_end, list(members)))
    return output


def assign_transitive_factor_episodes(
    rows: Sequence[dict[str, Any]],
    registry: Mapping[str, Any],
    *,
    maximum_gap_sec: int = MAXIMUM_EPISODE_GAP_SEC,
    detected_utc: str,
) -> dict[str, Any]:
    """Assign current canonical roots and plan append-only reducing unions."""

    memberships = registry.get("memberships") or {}
    existing_edges = dict(registry.get("edges") or {})
    root_registered = dict(registry.get("root_registered_utc") or {})
    cycle = bool(registry.get("cycle"))
    planned_merges: list[dict[str, str]] = []
    component_count = 0
    for primary, start, end, members in _components(rows, maximum_gap_sec):
        component_count += 1
        roots: set[str] = set()
        for member in members:
            persisted = memberships.get(str(member.get("case_id") or ""))
            if persisted:
                root = str(persisted.get("raw_root_id") or "")
                canonical, found_cycle = resolve_root(root, existing_edges)
                cycle = cycle or found_cycle
                if canonical:
                    roots.add(canonical)
        if not roots:
            winner = _new_root_id(primary, start)
            root_registered.setdefault(winner, detected_utc)
        else:
            winner = min(
                roots,
                key=lambda root: (root_registered.get(root, detected_utc), root),
            )
            for loser in sorted(roots - {winner}):
                existing_edges[loser] = winner
                planned_merges.append(
                    {
                        "from_root_id": loser,
                        "into_root_id": winner,
                        "factor_primary_token": primary,
                        "bridge_case_ids": ",".join(
                            sorted(str(row.get("case_id") or "") for row in members)
                        ),
                    }
                )
        representative = max(
            members,
            key=lambda row: (
                abs(float(row.get("move_bps") or 0.0)),
                abs(float(row.get("executable_net_pips") or 0.0)),
                str(row.get("instrument") or ""),
            ),
        )
        anchor = dt.datetime.fromtimestamp(start, tz=dt.timezone.utc).isoformat()
        interval_end = dt.datetime.fromtimestamp(end, tz=dt.timezone.utc).isoformat()
        for row in members:
            row["factor_episode_id"] = winner
            row["factor_episode_canonical_root_id"] = winner
            row["factor_representative"] = row is representative
            row["factor_episode_contract_id"] = FACTOR_EPISODE_CONTRACT_ID
            row["factor_episode_grouping_method"] = (
                "same_primary_interval_overlap_or_adjacency_transitive_union"
            )
            row["factor_episode_maximum_gap_sec"] = int(maximum_gap_sec)
            row["factor_episode_anchor_utc"] = anchor
            row["factor_episode_interval_end_utc"] = interval_end
            row["factor_episode_member_count"] = len(members)
            row["factor_episode_union_planned"] = bool(len(roots) > 1)
    for root in set(existing_edges) | set(existing_edges.values()):
        _, found_cycle = resolve_root(root, existing_edges)
        cycle = cycle or found_cycle
    # A root selected for an earlier current component can itself become the
    # losing root of a later component in this same pass.  Resolve every row
    # only after all planned edges are known so the published
    # ``factor_episode_canonical_root_id`` is actually canonical at the
    # snapshot's knowledge time.  Raw append-only membership remains preserved
    # in the history ledger and is never rewritten.
    for row in rows:
        canonical, found_cycle = resolve_root(
            str(row.get("factor_episode_canonical_root_id") or ""),
            existing_edges,
        )
        cycle = cycle or found_cycle
        row["factor_episode_id"] = canonical
        row["factor_episode_canonical_root_id"] = canonical
    return {
        "component_count": component_count,
        "planned_merges": planned_merges,
        "planned_merge_count": len(planned_merges),
        "graph_cycle": cycle,
        "canonical_episode_count": len(
            {str(row.get("factor_episode_id") or "") for row in rows}
        ),
    }


def connect_history(path: Path) -> sqlite3.Connection:
    connection = v7r2.connect_history(path)
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS factor_episode_root_merges(
            merge_id TEXT PRIMARY KEY,
            factor_episode_contract_id TEXT NOT NULL,
            from_root_id TEXT NOT NULL,
            into_root_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL,
            bridge_case_ids TEXT NOT NULL,
            detected_utc TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
        );
        CREATE INDEX IF NOT EXISTS ix_factor_episode_root_merges_from
            ON factor_episode_root_merges(factor_episode_contract_id,from_root_id);
        CREATE TRIGGER IF NOT EXISTS factor_episode_root_merges_no_update
            BEFORE UPDATE ON factor_episode_root_merges
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS factor_episode_root_merges_no_delete
            BEFORE DELETE ON factor_episode_root_merges
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
    planned_merges: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    connection = connect_history(path)
    inserted = 0
    new_conflicts = 0
    inserted_merges = 0
    graph_cycle = False
    try:
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO mover_case_contract_registry "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    CONTRACT_ID,
                    CONTRACT_ACTIVATED_UTC.isoformat(),
                    v7r2.METER_CONTRACT_ID,
                    str(meter_database.resolve()),
                    (
                        "latest complete V12 bucket sealed at_or_before mover "
                        "start; fixed onset factor; transitive root union"
                    ),
                    1,
                    1,
                    0,
                    recorded_utc,
                ),
            )
            connection.execute(
                "INSERT OR IGNORE INTO factor_episode_contract_registry "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    FACTOR_EPISODE_CONTRACT_ID,
                    CONTRACT_ID,
                    PREVIOUS_CONTRACT_ID,
                    (
                        "fixed_onset_primary_same_primary_interval_"
                        "overlap_adjacency_transitive_root_union"
                    ),
                    MAXIMUM_EPISODE_GAP_SEC,
                    (
                        "unresolved never cross-pair deduplicate; root unions "
                        "are append-only and can only reduce effective N"
                    ),
                    1,
                    0,
                    recorded_utc,
                ),
            )
            for merge in planned_merges:
                source = str(merge.get("from_root_id") or "")
                target = str(merge.get("into_root_id") or "")
                merge_id = "factor_root_merge_" + hashlib.sha256(
                    "|".join((FACTOR_EPISODE_CONTRACT_ID, source, target)).encode(
                        "utf-8"
                    )
                ).hexdigest()[:24]
                before = connection.total_changes
                connection.execute(
                    "INSERT OR IGNORE INTO factor_episode_root_merges "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        merge_id,
                        FACTOR_EPISODE_CONTRACT_ID,
                        source,
                        target,
                        str(merge.get("factor_primary_token") or ""),
                        str(merge.get("bridge_case_ids") or ""),
                        recorded_utc,
                        1,
                        0,
                    ),
                )
                inserted_merges += int(connection.total_changes > before)

            edge_rows = connection.execute(
                "SELECT from_root_id,into_root_id FROM factor_episode_root_merges "
                "WHERE factor_episode_contract_id=? ORDER BY detected_utc,merge_id",
                (FACTOR_EPISODE_CONTRACT_ID,),
            ).fetchall()
            edges = {str(source): str(target) for source, target in edge_rows}
            for root in set(edges) | set(edges.values()):
                _, found_cycle = resolve_root(root, edges)
                graph_cycle = graph_cycle or found_cycle

            for row in rows:
                case_id = stable_case_id(row)
                row["case_id"] = case_id
                assigned = str(row.get("factor_episode_id") or "")
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
                            assigned,
                            str(row.get("factor_primary_token") or ""),
                            recorded_utc,
                        ),
                    )
                    row["factor_episode_membership_conflict"] = False
                else:
                    persisted_raw = str(existing[0])
                    persisted_canonical, found_cycle = resolve_root(
                        persisted_raw, edges
                    )
                    graph_cycle = graph_cycle or found_cycle
                    if persisted_canonical != assigned:
                        conflict_id = "factor_membership_conflict_" + hashlib.sha256(
                            "|".join(
                                (case_id, persisted_canonical, assigned)
                            ).encode("utf-8")
                        ).hexdigest()[:24]
                        before = connection.total_changes
                        connection.execute(
                            "INSERT OR IGNORE INTO factor_episode_membership_conflicts "
                            "VALUES(?,?,?,?,?,?,?)",
                            (
                                conflict_id,
                                case_id,
                                persisted_canonical,
                                assigned,
                                recorded_utc,
                                1,
                                0,
                            ),
                        )
                        new_conflicts += int(connection.total_changes > before)
                        row["factor_episode_membership_conflict"] = True
                        row["persisted_factor_episode_id"] = persisted_canonical
                    else:
                        row["factor_episode_membership_conflict"] = False
                before = connection.total_changes
                connection.execute(
                    "INSERT OR IGNORE INTO mover_cases VALUES(?,?,?,?,?,?)",
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
        merge_total = int(
            connection.execute(
                "SELECT COUNT(*) FROM factor_episode_root_merges "
                "WHERE factor_episode_contract_id=?",
                (FACTOR_EPISODE_CONTRACT_ID,),
            ).fetchone()[0]
        )
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        connection.close()
    if integrity != "ok":
        raise sqlite3.IntegrityError(f"mover history integrity:{integrity}")
    return {
        "inserted": inserted,
        "total": total,
        "inserted_root_merges": inserted_merges,
        "root_merge_total": merge_total,
        "new_membership_conflicts": new_conflicts,
        "membership_conflict_total": conflict_total,
        "graph_cycle": graph_cycle,
    }


def render_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Live move/news snapshot V7R3",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        (
            "Research-only. Fixed-onset same-primary interval episodes use "
            "append-only transitive root unions; unions can only reduce "
            "effective N. It cannot place orders."
        ),
        "",
        "| Pair | Move | Net pips | Start | Primary | Canonical root | Rep |",
        "|---|---:|---:|---|---|---|---:|",
    ]
    for row in payload.get("movers") or []:
        lines.append(
            f"| {row.get('instrument')} | {row.get('move_direction')} | "
            f"{row.get('executable_net_pips')} | {row.get('start_utc')} | "
            f"{row.get('factor_primary_token')} | {row.get('factor_episode_id')} | "
            f"{int(bool(row.get('factor_representative')))} |"
        )
    lines.extend(
        [
            "",
            f"Root merges this pass: `{payload.get('inserted_root_merge_count')}`; "
            f"retained: `{payload.get('root_merge_total')}`.",
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
    quote_state = v5.read_json(quotes_path)
    quotes = quote_state.get("quotes") or {}
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
            case["contract_id"] = CONTRACT_ID
            case["case_id"] = stable_case_id(case)
            cases.append(case)
    finally:
        if meter_connection is not None:
            meter_connection.close()

    factor_history = market_ticker.load_history(factor_history_path)
    factor_surfaces = v7r2.assign_fixed_onset_primary_factors(
        cases, factor_history
    )
    registry = load_union_registry(history_path)
    freeze_observed_primaries(cases, registry)
    assignment = assign_transitive_factor_episodes(
        cases,
        registry,
        maximum_gap_sec=MAXIMUM_EPISODE_GAP_SEC,
        detected_utc=generated,
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
            planned_merges=assignment["planned_merges"],
        )
    else:
        history = {
            "inserted": 0,
            "total": 0,
            "inserted_root_merges": 0,
            "root_merge_total": 0,
            "new_membership_conflicts": 0,
            "membership_conflict_total": 0,
            "graph_cycle": False,
        }
    integrity_ok = bool(
        not assignment["graph_cycle"]
        and not history["graph_cycle"]
        and history["membership_conflict_total"] == 0
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "previous_contract_id": PREVIOUS_CONTRACT_ID,
        "baseline_contract_id": BASELINE_CONTRACT_ID,
        "contract_activated_utc": CONTRACT_ACTIVATED_UTC.isoformat(),
        "generated_utc": generated,
        "moves_generated_utc": moves_state.get("generated_utc"),
        "quotes_generated_utc": quote_state.get("generated_utc"),
        "source_history_highwater_utc": source_highwater,
        "narrative_join_contract": {
            "meter_database": str(narrative_database.resolve()),
            "meter_contract_id": v7r2.METER_CONTRACT_ID,
            "meter_activation_utc": v7r2.METER_ACTIVATED_UTC.isoformat(),
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
            "grouping_method": (
                "fixed_onset_same_primary_interval_overlap_adjacency_"
                "append_only_transitive_root_union"
            ),
            "maximum_gap_sec": MAXIMUM_EPISODE_GAP_SEC,
            "primary_factor_classification_minutes": FACTOR_CLASSIFICATION_MINUTES,
            "centered_time_buckets_used": False,
            "root_union_can_increase_effective_n": False,
            "membership_rewrites_allowed": False,
            "role": "after_the_fact_factor_clustering_only",
        },
        "narrative_meter_database_status": meter_status,
        "narrative_causally_available_mover_count": sum(
            bool((row.get("continuous_narrative_state") or {}).get("state_available"))
            for row in cases
        ),
        "narrative_unavailable_mover_count": sum(
            not bool((row.get("continuous_narrative_state") or {}).get("state_available"))
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
        "factor_episode_count": int(assignment["canonical_episode_count"]),
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
        "planned_root_merge_count": int(assignment["planned_merge_count"]),
        "inserted_root_merge_count": history["inserted_root_merges"],
        "root_merge_total": history["root_merge_total"],
        "factor_episode_membership_new_conflict_count": history[
            "new_membership_conflicts"
        ],
        "factor_episode_membership_conflict_total": history[
            "membership_conflict_total"
        ],
        "factor_episode_graph_cycle": bool(
            assignment["graph_cycle"] or history["graph_cycle"]
        ),
        "factor_episode_integrity_ok": integrity_ok,
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
                    "inserted_root_merge_count": payload[
                        "inserted_root_merge_count"
                    ],
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
