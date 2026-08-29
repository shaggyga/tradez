#!/usr/bin/env python3
"""V7R3-root-union-bound declared-horizon context companion.

V3R2 remains the live frozen consumer and V4R2 remains preserved as the
rejected V7R2 preflight.  This separate V5R3 research cohort accepts only the
exact V7R3 snapshot and independently verifies its append-only transitive-root
history before enriching any mover.  It has no broker, authorization,
lifecycle, promotion, or execution surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping

import oanda_live_move_news_snapshot_v7r3 as live_v7r3
import oanda_live_move_persistent_news_context as base
import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORTS = DATA / "reports"

DEFAULT_SNAPSHOT = live_v7r3.DEFAULT_OUTPUT
DEFAULT_SNAPSHOT_HISTORY = live_v7r3.DEFAULT_HISTORY
DEFAULT_SOURCES = STATE / "source_governance_v1.sqlite"
DEFAULT_OUTPUT = STATE / "live_move_persistent_news_context_v5r3.json"
DEFAULT_HISTORY = STATE / "live_move_persistent_news_context_v5r3.sqlite"
DEFAULT_REPORT = (
    REPORTS / "live_move_news" / "LIVE_MOVE_PERSISTENT_CONTEXT_V5R3.md"
)

SCHEMA_VERSION = 5
CONTRACT_ID = (
    "live_move_persistent_news_context_v5r3_exact_v7r3_"
    "transitive_root_union_binding_20260827"
)
CONTRACT_ACTIVATED_UTC = dt.datetime(
    2026, 8, 27, 9, 6, tzinfo=dt.timezone.utc
)
EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID = live_v7r3.CONTRACT_ID
EXPECTED_FACTOR_EPISODE_CONTRACT_ID = live_v7r3.FACTOR_EPISODE_CONTRACT_ID
EXPECTED_NARRATIVE_METER_CONTRACT_ID = live_v7r3.v7r2.METER_CONTRACT_ID

REQUIRED_UPSTREAM_APPEND_ONLY_TRIGGERS = {
    "factor_episode_contract_registry_no_update",
    "factor_episode_contract_registry_no_delete",
    "factor_episode_membership_no_update",
    "factor_episode_membership_no_delete",
    "factor_episode_membership_conflicts_no_update",
    "factor_episode_membership_conflicts_no_delete",
    "factor_episode_root_merges_no_update",
    "factor_episode_root_merges_no_delete",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_input_snapshot(source: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []

    def require(condition: bool, reason: str) -> None:
        if not condition:
            reasons.append(reason)

    movers = source.get("movers")
    mover_rows = (
        [row for row in movers if isinstance(row, Mapping)]
        if isinstance(movers, list)
        else []
    )
    factor = source.get("factor_episode_contract")
    factor = factor if isinstance(factor, Mapping) else {}
    narrative = source.get("narrative_join_contract")
    narrative = narrative if isinstance(narrative, Mapping) else {}

    require(
        source.get("contract_id") == EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
        "snapshot_contract_mismatch",
    )
    require(source.get("schema_version") == live_v7r3.SCHEMA_VERSION, "schema_mismatch")
    require(
        source.get("previous_contract_id") == live_v7r3.PREVIOUS_CONTRACT_ID,
        "previous_snapshot_contract_mismatch",
    )
    require(
        source.get("baseline_contract_id") == live_v7r3.BASELINE_CONTRACT_ID,
        "baseline_snapshot_contract_mismatch",
    )
    require(bool(source.get("generated_utc")), "missing_snapshot_generated_utc")
    require(bool(source.get("history_database")), "missing_upstream_history_database")
    require(
        factor.get("contract_id") == EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
        "factor_episode_contract_mismatch",
    )
    require(
        factor.get("previous_snapshot_contract_id")
        == live_v7r3.PREVIOUS_CONTRACT_ID,
        "factor_previous_snapshot_contract_mismatch",
    )
    require(
        factor.get("grouping_method")
        == (
            "fixed_onset_same_primary_interval_overlap_adjacency_"
            "append_only_transitive_root_union"
        ),
        "factor_episode_grouping_mismatch",
    )
    require(
        factor.get("maximum_gap_sec") == live_v7r3.MAXIMUM_EPISODE_GAP_SEC,
        "factor_episode_gap_mismatch",
    )
    require(
        factor.get("primary_factor_classification_minutes")
        == live_v7r3.FACTOR_CLASSIFICATION_MINUTES,
        "factor_classification_cutoff_mismatch",
    )
    require(
        factor.get("centered_time_buckets_used") is False,
        "centered_bucket_contract_not_disabled",
    )
    require(
        factor.get("root_union_can_increase_effective_n") is False,
        "root_union_nonexpansion_contract_missing",
    )
    require(
        factor.get("membership_rewrites_allowed") is False,
        "membership_rewrite_contract_not_disabled",
    )
    require(
        source.get("factor_episode_integrity_ok") is True,
        "factor_episode_integrity_failed",
    )
    require(
        source.get("factor_episode_membership_conflict_total") == 0,
        "factor_episode_membership_conflict",
    )
    require(
        source.get("factor_episode_graph_cycle") is False,
        "factor_episode_graph_cycle",
    )
    for key in (
        "planned_root_merge_count",
        "inserted_root_merge_count",
        "root_merge_total",
    ):
        value = source.get(key)
        require(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0,
            f"invalid_{key}",
        )
    if all(
        isinstance(source.get(key), int)
        and not isinstance(source.get(key), bool)
        for key in (
            "planned_root_merge_count",
            "inserted_root_merge_count",
            "root_merge_total",
        )
    ):
        require(
            int(source["inserted_root_merge_count"])
            <= int(source["planned_root_merge_count"]),
            "inserted_root_merges_exceed_planned",
        )
        require(
            int(source["inserted_root_merge_count"])
            <= int(source["root_merge_total"]),
            "inserted_root_merges_exceed_total",
        )
    require(
        narrative.get("meter_contract_id")
        == EXPECTED_NARRATIVE_METER_CONTRACT_ID,
        "narrative_meter_contract_mismatch",
    )
    require(
        narrative.get("partial_live_excluded") is True,
        "partial_narrative_not_excluded",
    )
    require(
        narrative.get("sealed_v12_database_only") is True,
        "narrative_database_not_sealed_v12_only",
    )
    require(source.get("research_only") is True, "upstream_not_research_only")
    require(source.get("execution_eligible") is False, "upstream_execution_eligible")
    require(source.get("can_place_orders") is False, "upstream_can_place_orders")
    require(source.get("can_promote") is False, "upstream_can_promote")
    require(source.get("broker_access") is False, "upstream_broker_access")
    require(
        source.get("supported_decision") == "diagnostic_only",
        "upstream_decision_not_diagnostic_only",
    )
    require(isinstance(movers, list), "movers_not_list")
    require(
        not isinstance(movers, list) or len(movers) == len(mover_rows),
        "mover_rows_not_objects",
    )
    require(source.get("mover_count") == len(mover_rows), "mover_count_mismatch")

    canonical_roots: set[str] = set()
    representative_roots: list[str] = []
    for index, row in enumerate(mover_rows):
        prefix = f"mover_{index}"
        require(
            row.get("contract_id") == EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
            f"{prefix}_contract_mismatch",
        )
        require(bool(row.get("case_id")), f"{prefix}_missing_case_id")
        require(bool(row.get("factor_primary_token")), f"{prefix}_missing_primary_factor")
        require(
            row.get("factor_episode_contract_id")
            == EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
            f"{prefix}_factor_contract_mismatch",
        )
        require(bool(row.get("factor_episode_id")), f"{prefix}_missing_factor_episode_id")
        canonical = str(row.get("factor_episode_canonical_root_id") or "")
        require(bool(canonical), f"{prefix}_missing_canonical_root")
        if canonical:
            canonical_roots.add(canonical)
        if row.get("factor_representative") is True:
            representative_roots.append(canonical)
        require(
            row.get("factor_episode_membership_conflict") is False,
            f"{prefix}_membership_conflict",
        )

    require(
        source.get("factor_episode_count") == len(canonical_roots),
        "factor_episode_count_mismatch",
    )
    require(
        source.get("factor_representative_count") == len(representative_roots),
        "factor_representative_count_mismatch",
    )
    require(
        len(representative_roots) == len(canonical_roots),
        "factor_episode_representative_invariant_failed",
    )
    require(
        len(set(representative_roots)) == len(representative_roots),
        "duplicate_factor_episode_representative",
    )
    require(
        set(representative_roots) == canonical_roots,
        "missing_factor_episode_representative",
    )
    return sorted(set(reasons))


def validate_root_union_history(
    source: Mapping[str, Any], path: Path
) -> tuple[list[str], dict[str, Any]]:
    """Independently verify V7R3's append-only root-union ledger."""

    reasons: list[str] = []
    status: dict[str, Any] = {
        "path": str(path.resolve()),
        "quick_check": None,
        "contract_registry_ok": False,
        "append_only_triggers_ok": False,
        "membership_conflict_total": None,
        "root_merge_total": None,
        "graph_cycle": None,
        "current_mover_membership_ok": False,
        "root_union_nonexpansive": False,
        "ok": False,
    }
    if not path.is_file():
        reasons.append("upstream_factor_history_missing")
        return reasons, status
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro", uri=True
        )
        try:
            quick = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            status["quick_check"] = quick
            if quick != "ok":
                reasons.append("upstream_factor_history_quick_check_failed")
            registry = connection.execute(
                "SELECT snapshot_contract_id,previous_snapshot_contract_id,"
                "grouping_method,maximum_gap_sec,research_only,execution_eligible "
                "FROM factor_episode_contract_registry "
                "WHERE factor_episode_contract_id=?",
                (EXPECTED_FACTOR_EPISODE_CONTRACT_ID,),
            ).fetchone()
            expected_registry = (
                EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
                live_v7r3.PREVIOUS_CONTRACT_ID,
                (
                    "fixed_onset_primary_same_primary_interval_"
                    "overlap_adjacency_transitive_root_union"
                ),
                live_v7r3.MAXIMUM_EPISODE_GAP_SEC,
                1,
                0,
            )
            status["contract_registry_ok"] = registry == expected_registry
            if registry != expected_registry:
                reasons.append("upstream_factor_contract_registry_mismatch")
            triggers = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                )
            }
            missing_triggers = sorted(
                REQUIRED_UPSTREAM_APPEND_ONLY_TRIGGERS - triggers
            )
            status["missing_append_only_triggers"] = missing_triggers
            status["append_only_triggers_ok"] = not missing_triggers
            if missing_triggers:
                reasons.append("upstream_append_only_triggers_missing")
            conflict_rows = connection.execute(
                "SELECT conflict_id,case_id,persisted_factor_episode_id,"
                "observed_factor_episode_id,detected_utc "
                "FROM factor_episode_membership_conflicts ORDER BY conflict_id"
            ).fetchall()
            conflict_total = len(conflict_rows)
            merge_rows = connection.execute(
                "SELECT from_root_id,into_root_id,factor_primary_token "
                "FROM factor_episode_root_merges "
                "WHERE factor_episode_contract_id=? ORDER BY detected_utc,merge_id",
                (EXPECTED_FACTOR_EPISODE_CONTRACT_ID,),
            ).fetchall()
            memberships = connection.execute(
                "SELECT case_id,factor_episode_id,factor_primary_token "
                "FROM factor_episode_membership "
                "WHERE factor_episode_contract_id=? ORDER BY case_id",
                (EXPECTED_FACTOR_EPISODE_CONTRACT_ID,),
            ).fetchall()
            mover_case_count = int(
                connection.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0]
            )
        finally:
            connection.close()
    except sqlite3.Error as exc:
        reasons.append(f"upstream_factor_history_read_failed:{type(exc).__name__}")
        return sorted(set(reasons)), status

    status["membership_conflict_total"] = conflict_total
    status["root_merge_total"] = len(merge_rows)
    status["retained_case_count"] = mover_case_count
    semantic_payload = {
        "registry": list(registry) if registry is not None else None,
        "append_only_triggers": sorted(
            trigger
            for trigger in triggers
            if trigger in REQUIRED_UPSTREAM_APPEND_ONLY_TRIGGERS
        ),
        "conflicts": [list(row) for row in conflict_rows],
        "root_merges": [list(row) for row in merge_rows],
        "memberships": [list(row) for row in memberships],
        "mover_case_count": mover_case_count,
    }
    status["semantic_sha256"] = _sha256_text(
        json.dumps(semantic_payload, sort_keys=True, separators=(",", ":"))
    )
    if conflict_total:
        reasons.append("upstream_membership_conflicts_present")
    if source.get("factor_episode_membership_conflict_total") != conflict_total:
        reasons.append("upstream_membership_conflict_count_mismatch")
    if source.get("root_merge_total") != len(merge_rows):
        reasons.append("upstream_root_merge_count_mismatch")
    if source.get("retained_case_count") != mover_case_count:
        reasons.append("upstream_retained_case_count_mismatch")

    edges: dict[str, str] = {}
    graph_cycle = False
    root_primaries: dict[str, set[str]] = {}
    membership_map: dict[str, tuple[str, str]] = {}
    for case_id, raw_root, primary in memberships:
        raw_text = str(raw_root)
        primary_text = str(primary)
        membership_map[str(case_id)] = (raw_text, primary_text)
        root_primaries.setdefault(raw_text, set()).add(primary_text)
    for from_root, into_root, primary in merge_rows:
        source_root = str(from_root)
        target_root = str(into_root)
        primary_text = str(primary)
        if not source_root or not target_root or source_root == target_root:
            reasons.append("invalid_upstream_root_merge_edge")
        prior = edges.get(source_root)
        if prior is not None and prior != target_root:
            reasons.append("upstream_root_has_multiple_targets")
        edges[source_root] = target_root
        for root in (source_root, target_root):
            observed = root_primaries.get(root) or set()
            if observed and observed != {primary_text}:
                reasons.append("upstream_root_merge_primary_mismatch")
    for root in set(edges) | set(edges.values()):
        _, found_cycle = live_v7r3.resolve_root(root, edges)
        graph_cycle = graph_cycle or found_cycle
    status["graph_cycle"] = graph_cycle
    if graph_cycle:
        reasons.append("upstream_root_union_cycle")

    raw_roots = {raw for raw, _ in membership_map.values()}
    canonical_roots = {
        live_v7r3.resolve_root(raw, edges)[0] for raw in raw_roots
    }
    nonexpansive = len(canonical_roots) <= len(raw_roots)
    status["raw_root_count"] = len(raw_roots)
    status["canonical_root_count"] = len(canonical_roots)
    status["root_union_nonexpansive"] = nonexpansive
    if not nonexpansive:
        reasons.append("upstream_root_union_increased_effective_n")

    current_membership_ok = True
    for index, row in enumerate(source.get("movers") or []):
        case_id = str(row.get("case_id") or "")
        persisted = membership_map.get(case_id)
        if persisted is None:
            reasons.append(f"mover_{index}_missing_from_upstream_history")
            current_membership_ok = False
            continue
        raw_root, primary = persisted
        canonical, found_cycle = live_v7r3.resolve_root(raw_root, edges)
        expected_canonical = str(
            row.get("factor_episode_canonical_root_id") or ""
        )
        if found_cycle or canonical != expected_canonical:
            reasons.append(f"mover_{index}_canonical_root_history_mismatch")
            current_membership_ok = False
        if primary != str(row.get("factor_primary_token") or ""):
            reasons.append(f"mover_{index}_primary_history_mismatch")
            current_membership_ok = False
    status["current_mover_membership_ok"] = current_membership_ok
    status["ok"] = not reasons
    return sorted(set(reasons)), status


def connect_history(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS context_contract_registry(
            contract_id TEXT PRIMARY KEY,
            activated_utc TEXT NOT NULL,
            required_snapshot_contract_id TEXT NOT NULL,
            required_factor_episode_contract_id TEXT NOT NULL,
            required_narrative_meter_contract_id TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
            broker_access INTEGER NOT NULL CHECK(broker_access=0),
            created_utc TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS context_contract_registry_no_update
            BEFORE UPDATE ON context_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS context_contract_registry_no_delete
            BEFORE DELETE ON context_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TABLE IF NOT EXISTS context_observations(
            observation_id TEXT PRIMARY KEY,
            context_contract_id TEXT NOT NULL,
            generated_utc TEXT NOT NULL,
            input_snapshot_generated_utc TEXT,
            input_snapshot_sha256 TEXT NOT NULL,
            upstream_history_sha256 TEXT NOT NULL,
            input_integrity_ok INTEGER NOT NULL CHECK(input_integrity_ok IN (0,1)),
            rejection_reasons_json TEXT NOT NULL,
            mover_count INTEGER NOT NULL,
            source_history_highwater_utc TEXT,
            payload_sha256 TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
            broker_access INTEGER NOT NULL CHECK(broker_access=0),
            FOREIGN KEY(context_contract_id)
                REFERENCES context_contract_registry(contract_id)
        );
        CREATE TRIGGER IF NOT EXISTS context_observations_no_update
            BEFORE UPDATE ON context_observations
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS context_observations_no_delete
            BEFORE DELETE ON context_observations
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def observation_id(
    *, generated_utc: str, input_snapshot_sha256: str, upstream_history_sha256: str
) -> str:
    material = "|".join(
        (
            CONTRACT_ID,
            generated_utc,
            input_snapshot_sha256,
            upstream_history_sha256,
        )
    )
    return "persistent_context_v5_" + _sha256_text(material)[:24]


def record_observation(
    path: Path,
    payload: Mapping[str, Any],
    *,
    input_snapshot_sha256: str,
    upstream_history_sha256: str,
) -> dict[str, Any]:
    payload_json = _canonical_json(payload)
    payload_sha256 = _sha256_text(payload_json)
    identity = str(payload.get("history_observation_id") or "")
    connection = connect_history(path)
    inserted = 0
    try:
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO context_contract_registry VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    CONTRACT_ID,
                    CONTRACT_ACTIVATED_UTC.isoformat(),
                    EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
                    EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
                    EXPECTED_NARRATIVE_METER_CONTRACT_ID,
                    1,
                    0,
                    0,
                    0,
                    str(payload.get("generated_utc") or utc_now()),
                ),
            )
            registry = connection.execute(
                "SELECT required_snapshot_contract_id,"
                "required_factor_episode_contract_id,"
                "required_narrative_meter_contract_id,research_only,"
                "execution_eligible,can_place_orders,broker_access "
                "FROM context_contract_registry WHERE contract_id=?",
                (CONTRACT_ID,),
            ).fetchone()
            expected = (
                EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
                EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
                EXPECTED_NARRATIVE_METER_CONTRACT_ID,
                1,
                0,
                0,
                0,
            )
            if registry != expected:
                raise sqlite3.IntegrityError("context_contract_registry_mismatch")
            existing = connection.execute(
                "SELECT payload_sha256 FROM context_observations WHERE observation_id=?",
                (identity,),
            ).fetchone()
            if existing is not None and str(existing[0]) != payload_sha256:
                raise sqlite3.IntegrityError("observation_identity_payload_mismatch")
            before = connection.total_changes
            connection.execute(
                "INSERT OR IGNORE INTO context_observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    CONTRACT_ID,
                    str(payload.get("generated_utc") or ""),
                    str(payload.get("input_snapshot_generated_utc") or ""),
                    input_snapshot_sha256,
                    upstream_history_sha256,
                    int(bool(payload.get("input_integrity_ok"))),
                    json.dumps(
                        payload.get("input_rejection_reasons") or [],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    int(payload.get("mover_count") or 0),
                    str(payload.get("source_history_highwater_utc") or ""),
                    payload_sha256,
                    payload_json,
                    1,
                    0,
                    0,
                    0,
                ),
            )
            inserted = int(connection.total_changes > before)
        total = int(connection.execute("SELECT COUNT(*) FROM context_observations").fetchone()[0])
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()
    if integrity != "ok":
        raise sqlite3.IntegrityError(f"context history integrity:{integrity}")
    return {"inserted": inserted, "total": total, "integrity": integrity}


def render(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Live mover persistent-news context V5R3",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        (
            "Parallel research-only consumer bound exactly to the V7R3 "
            "append-only transitive-root cohort. It cannot place orders."
        ),
        "",
        f"Input integrity: `{payload.get('input_integrity_ok')}`",
    ]
    reasons = payload.get("input_rejection_reasons") or []
    if reasons:
        lines.extend(("", "Rejected input:", ""))
        lines.extend(f"- `{reason}`" for reason in reasons)
        return "\n".join(lines) + "\n"
    lines.extend(("", *base.render(payload).splitlines()[6:]))
    return "\n".join(lines) + "\n"


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        return _sha256_text("missing:" + str(path.resolve()))
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run(
    *,
    snapshot_path: Path = DEFAULT_SNAPSHOT,
    snapshot_history_path: Path = DEFAULT_SNAPSHOT_HISTORY,
    source_database: Path = DEFAULT_SOURCES,
    output_path: Path = DEFAULT_OUTPUT,
    history_path: Path = DEFAULT_HISTORY,
    report_path: Path = DEFAULT_REPORT,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    observed = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    generated = observed.isoformat()
    source = base.read_json(snapshot_path)
    source_sha256 = _sha256_text(_canonical_json(source))
    upstream_history_sha256 = _file_sha256(snapshot_history_path)
    reasons = validate_input_snapshot(source)
    snapshot_generated_epoch = census.parse_epoch(source.get("generated_utc"))
    if source.get("generated_utc") and snapshot_generated_epoch is None:
        reasons.append("invalid_snapshot_generated_utc")
    elif snapshot_generated_epoch is not None and snapshot_generated_epoch > int(observed.timestamp()) + 1:
        reasons.append("snapshot_generated_in_future")
    declared_history = str(source.get("history_database") or "")
    if declared_history:
        try:
            if Path(declared_history).resolve() != snapshot_history_path.resolve():
                reasons.append("upstream_history_path_mismatch")
        except OSError:
            reasons.append("invalid_upstream_history_path")

    upstream_history_status: dict[str, Any] = {
        "path": str(snapshot_history_path.resolve()),
        "ok": False,
        "not_checked_due_to_snapshot_rejection": bool(reasons),
    }
    if not reasons:
        history_reasons, upstream_history_status = validate_root_union_history(
            source, snapshot_history_path
        )
        reasons.extend(history_reasons)
        upstream_history_sha256 = str(
            upstream_history_status.get("semantic_sha256")
            or upstream_history_sha256
        )

    rows: list[dict[str, Any]] = []
    source_highwater: str | None = None
    if not reasons:
        movers = [row for row in source.get("movers") or [] if isinstance(row, Mapping)]
        starts = [census.parse_epoch(row.get("start_utc")) for row in movers]
        valid = [int(value) for value in starts if value is not None]
        lower = min(valid) - base.MAXIMUM_LOOKBACK_MINUTES * 60 if valid else int(observed.timestamp())
        upper = max(valid) if valid else int(observed.timestamp())
        try:
            source_index, source_highwater = census.load_source_index(
                source_database,
                minimum_effective_epoch=lower,
                maximum_effective_epoch=upper,
            )
            rows = [base.enrich_case(row, source_index) for row in movers]
        except (OSError, sqlite3.DatabaseError) as exc:
            reasons.append(f"source_database_load_failed:{type(exc).__name__}")
            rows = []
            source_highwater = None

    reasons = sorted(set(reasons))
    integrity_ok = not reasons
    identity = observation_id(
        generated_utc=generated,
        input_snapshot_sha256=source_sha256,
        upstream_history_sha256=upstream_history_sha256,
    )
    output: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "contract_activated_utc": CONTRACT_ACTIVATED_UTC.isoformat(),
        "generated_utc": generated,
        "input_snapshot_path": str(snapshot_path.resolve()),
        "input_snapshot_sha256": source_sha256,
        "input_snapshot_contract_id": source.get("contract_id"),
        "expected_input_snapshot_contract_id": EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
        "input_factor_episode_contract_id": (
            (source.get("factor_episode_contract") or {}).get("contract_id")
            if isinstance(source.get("factor_episode_contract"), Mapping)
            else None
        ),
        "expected_factor_episode_contract_id": EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
        "input_snapshot_generated_utc": source.get("generated_utc"),
        "input_narrative_join_contract": source.get("narrative_join_contract"),
        "upstream_factor_history_path": str(snapshot_history_path.resolve()),
        "upstream_factor_history_sha256": upstream_history_sha256,
        "upstream_factor_history_fingerprint_method": (
            "canonical_registry_merges_memberships_conflicts_v1"
            if upstream_history_status.get("semantic_sha256")
            else "physical_file_sha256_or_missing_sentinel"
        ),
        "upstream_factor_history_status": upstream_history_status,
        "input_integrity_ok": integrity_ok,
        "input_rejection_reasons": reasons,
        "source_history_highwater_utc": source_highwater,
        "maximum_lookback_minutes": base.MAXIMUM_LOOKBACK_MINUTES,
        "mover_count": len(rows),
        "movers": rows,
        "history_database": str(history_path.resolve()),
        "history_observation_id": identity,
        "history_append_only": True,
        "history_integrity_required": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "broker_access": False,
        "supported_decision": "diagnostic_only" if integrity_ok else "reject_input",
    }
    record_observation(
        history_path,
        output,
        input_snapshot_sha256=source_sha256,
        upstream_history_sha256=upstream_history_sha256,
    )
    live_v7r3.v5.atomic_json(output_path, output)
    live_v7r3.v5.atomic_text(report_path, render(output))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--snapshot-history", type=Path, default=DEFAULT_SNAPSHOT_HISTORY)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        result = run(
            snapshot_path=args.snapshot,
            snapshot_history_path=args.snapshot_history,
            source_database=args.sources,
            output_path=args.output,
            history_path=args.history,
            report_path=args.report,
        )
        print(
            json.dumps(
                {
                    "generated_utc": result["generated_utc"],
                    "input_integrity_ok": result["input_integrity_ok"],
                    "mover_count": result["mover_count"],
                    "supported_decision": result["supported_decision"],
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
