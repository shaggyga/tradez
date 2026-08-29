#!/usr/bin/env python3
"""V7R3-bound outcomes with canonical transitive-root deduplication.

This separate V4R3 prospective research cohort validates the exact V7R3 case
and factor-root contracts before appending executable-path outcomes. Summary
effective N resolves immutable raw memberships through append-only transitive
root aliases. V2R2/V6R2, rejected V3R2/V7R2, and all live bindings remain
untouched. The worker cannot route, authorize, promote, or place orders.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_live_move_news_outcomes as base
import oanda_live_move_news_snapshot_v7r3 as live_v7r3


DATA = Path(__file__).resolve().parent / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_DATABASE = STATE / "live_move_news_cases_v7r3.sqlite"
DEFAULT_CANDLES = DATA / "candles"
DEFAULT_OUTPUT = STATE / "live_move_news_outcomes_v4r3.json"
DEFAULT_REPORT = (
    DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_OUTCOMES_CURRENT_V4R3.md"
)
CONTRACT_ID = (
    "live_move_news_forward_outcomes_v4r3_v7r3_"
    "transitive_factor_root_union_20260827"
)
CASE_CONTRACT_ID = live_v7r3.CONTRACT_ID
FACTOR_EPISODE_CONTRACT_ID = live_v7r3.FACTOR_EPISODE_CONTRACT_ID


class UpstreamIntegrityError(RuntimeError):
    """The exact V7R3 case/root-union contract was absent or inconsistent."""


def _read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)


def validate_case_database(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": False,
        "database": str(path.resolve()),
        "case_contract_id": CASE_CONTRACT_ID,
        "factor_episode_contract_id": FACTOR_EPISODE_CONTRACT_ID,
        "reason": "database_missing",
    }
    if not path.exists():
        return result
    try:
        connection = _read_only(path)
        try:
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            required = {
                "mover_cases",
                "mover_case_contract_registry",
                "factor_episode_contract_registry",
                "factor_episode_membership",
                "factor_episode_membership_conflicts",
                "factor_episode_root_merges",
            }
            missing = sorted(required - tables)
            if missing:
                result.update(
                    quick_check=quick_check,
                    reason="required_tables_missing",
                    missing_tables=missing,
                )
                return result
            case_registry = int(
                connection.execute(
                    "SELECT COUNT(*) FROM mover_case_contract_registry "
                    "WHERE contract_id=? AND research_only=1 AND execution_eligible=0",
                    (CASE_CONTRACT_ID,),
                ).fetchone()[0]
            )
            factor_registry = int(
                connection.execute(
                    "SELECT COUNT(*) FROM factor_episode_contract_registry "
                    "WHERE factor_episode_contract_id=? AND snapshot_contract_id=? "
                    "AND research_only=1 AND execution_eligible=0",
                    (FACTOR_EPISODE_CONTRACT_ID, CASE_CONTRACT_ID),
                ).fetchone()[0]
            )
            case_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM mover_cases "
                    "WHERE json_extract(case_json,'$.contract_id')=?",
                    (CASE_CONTRACT_ID,),
                ).fetchone()[0]
            )
            invalid_json = int(
                connection.execute(
                    "SELECT COUNT(*) FROM mover_cases WHERE json_valid(case_json)=0"
                ).fetchone()[0]
            )
            conflicts = int(
                connection.execute(
                    "SELECT COUNT(*) FROM factor_episode_membership_conflicts"
                ).fetchone()[0]
            )
            membership_mismatches = int(
                connection.execute(
                    "SELECT COUNT(*) FROM mover_cases c "
                    "LEFT JOIN factor_episode_membership f ON f.case_id=c.case_id "
                    "WHERE json_extract(c.case_json,'$.contract_id')=? AND ("
                    "f.case_id IS NULL OR f.factor_episode_contract_id<>? OR "
                    "f.factor_episode_id<>json_extract(c.case_json,'$.factor_episode_id') OR "
                    "f.factor_primary_token<>json_extract(c.case_json,'$.factor_primary_token'))",
                    (CASE_CONTRACT_ID, FACTOR_EPISODE_CONTRACT_ID),
                ).fetchone()[0]
            )
            invalid_merges = int(
                connection.execute(
                    "SELECT COUNT(*) FROM factor_episode_root_merges WHERE "
                    "factor_episode_contract_id<>? OR from_root_id=into_root_id "
                    "OR factor_primary_token='' OR research_only<>1 "
                    "OR execution_eligible<>0",
                    (FACTOR_EPISODE_CONTRACT_ID,),
                ).fetchone()[0]
            )
            merge_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM factor_episode_root_merges WHERE "
                    "factor_episode_contract_id=?",
                    (FACTOR_EPISODE_CONTRACT_ID,),
                ).fetchone()[0]
            )
        finally:
            connection.close()
        registry = live_v7r3.load_union_registry(path)
        raw_roots = {
            str(row.get("raw_root_id") or "")
            for row in (registry.get("memberships") or {}).values()
        }
        canonical_roots = {
            str(row.get("canonical_root_id") or "")
            for row in (registry.get("memberships") or {}).values()
        }
    except (sqlite3.Error, TypeError, IndexError) as exc:
        result["reason"] = f"database_validation_error:{type(exc).__name__}"
        return result
    failures: list[str] = []
    if quick_check != "ok":
        failures.append("sqlite_quick_check")
    if case_registry != 1:
        failures.append("case_contract_registry")
    if factor_registry != 1:
        failures.append("factor_contract_registry")
    if conflicts:
        failures.append("membership_conflicts")
    if membership_mismatches:
        failures.append("membership_mismatch")
    if invalid_json:
        failures.append("invalid_case_json")
    if invalid_merges:
        failures.append("invalid_root_merges")
    if bool(registry.get("cycle")):
        failures.append("root_union_cycle")
    if len(canonical_roots) > len(raw_roots):
        failures.append("root_union_inflated_effective_n")
    result.update(
        quick_check=quick_check,
        case_registry_count=case_registry,
        factor_registry_count=factor_registry,
        case_count=case_count,
        membership_conflict_count=conflicts,
        membership_mismatch_count=membership_mismatches,
        invalid_json_count=invalid_json,
        invalid_root_merge_count=invalid_merges,
        root_merge_count=merge_count,
        raw_root_count=len(raw_roots),
        canonical_root_count=len(canonical_roots),
        root_union_cycle=bool(registry.get("cycle")),
        failures=failures,
        ok=not failures,
        reason="ready" if not failures else ";".join(failures),
    )
    return result


def _cost_bucket(spread: float | None) -> str:
    if spread is None or spread <= 0:
        return "unknown"
    if spread <= 2:
        return "liquid_le_2p"
    if spread <= 5:
        return "moderate_2_5p"
    if spread <= 15:
        return "wide_5_15p"
    return "very_wide_gt_15p"


def _average(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return round(float(statistics.fmean(values)), 6) if values else None


def summarize_canonical(
    connection: sqlite3.Connection,
    *,
    case_contract_id: str = CASE_CONTRACT_ID,
) -> list[dict[str, Any]]:
    registry = live_v7r3.load_union_registry(Path(connection.execute("PRAGMA database_list").fetchone()[2]))
    memberships = registry.get("memberships") or {}
    raw = connection.execute(
        "SELECT o.case_id,o.arm,o.horizon_min,o.after_cost_pips,"
        "c.first_recorded_utc,c.case_json "
        "FROM mover_case_outcomes o JOIN mover_cases c ON c.case_id=o.case_id "
        "WHERE o.case_contract_id=?",
        (case_contract_id,),
    ).fetchall()
    cells: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for case_id, arm, horizon, after_cost, first_seen, case_json in raw:
        case = json.loads(str(case_json))
        spread_value = case.get("entry_spread_pips")
        try:
            spread = float(spread_value)
        except (TypeError, ValueError):
            spread = None
        membership = memberships.get(str(case_id)) or {}
        canonical = str(membership.get("canonical_root_id") or case_id)
        row = {
            "case_id": str(case_id),
            "arm": str(arm),
            "horizon_min": int(horizon),
            "after_cost_pips": float(after_cost),
            "spread_multiple": (
                float(after_cost) / spread if spread is not None and spread > 0 else None
            ),
            "first_recorded_utc": str(first_seen),
            "canonical_root_id": canonical,
            "stored_representative": bool(case.get("factor_representative")),
            "absolute_move_bps": abs(float(case.get("move_bps") or 0.0)),
        }
        cells[(str(arm), int(horizon), _cost_bucket(spread))].append(row)
    output: list[dict[str, Any]] = []
    for (arm, horizon, bucket), rows in sorted(cells.items()):
        factor_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            factor_groups[str(row["canonical_root_id"])].append(row)
        selected = [
            min(
                group,
                key=lambda row: (
                    str(row["first_recorded_utc"]),
                    -int(bool(row["stored_representative"])),
                    -float(row["absolute_move_bps"]),
                    str(row["case_id"]),
                ),
            )
            for group in factor_groups.values()
        ]
        output.append(
            {
                "arm": arm,
                "horizon_min": horizon,
                "cost_bucket": bucket,
                "raw_n": len(rows),
                "win_rate": round(
                    sum(float(row["after_cost_pips"]) > 0 for row in rows)
                    / len(rows),
                    6,
                ),
                "average_after_cost_pips": _average(rows, "after_cost_pips"),
                "average_after_cost_spread_multiple": _average(
                    rows, "spread_multiple"
                ),
                "factor_representative_n": len(selected),
                "factor_representative_win_rate": (
                    round(
                        sum(
                            float(row["after_cost_pips"]) > 0
                            for row in selected
                        )
                        / len(selected),
                        6,
                    )
                    if selected
                    else None
                ),
                "factor_representative_average_after_cost_pips": _average(
                    selected, "after_cost_pips"
                ),
                "factor_representative_average_after_cost_spread_multiple": (
                    _average(selected, "spread_multiple")
                ),
            }
        )
    return output


def render(payload: Mapping[str, Any]) -> str:
    coverage = payload.get("candle_archive") or {}
    lines = [
        "# Live mover/news forward outcomes V4R3",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        (
            f"Cases: **{payload.get('case_count')}**; retained outcomes: "
            f"**{payload.get('retained_outcome_count')}**; inserted: "
            f"**{payload.get('inserted_outcome_count')}**."
        ),
        "",
        (
            "Executable M1 archive: "
            f"**{coverage.get('instrument_count')}** instruments; pending paths "
            f"**{coverage.get('pending_archive_arm_horizon_count')} / "
            f"{coverage.get('potential_arm_horizon_count')}**."
        ),
        "",
        "| Arm | Horizon | Cost bucket | Raw N | Raw win | Raw avg net | Raw net/spread | Canonical factor N | Factor win | Factor avg net | Factor net/spread |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for cell in payload.get("cells") or []:
        lines.append(
            f"| {cell['arm']} | {cell['horizon_min']}m | {cell['cost_bucket']} | "
            f"{cell['raw_n']} | {cell['win_rate']} | "
            f"{cell['average_after_cost_pips']} | "
            f"{cell['average_after_cost_spread_multiple']} | "
            f"{cell['factor_representative_n']} | "
            f"{cell['factor_representative_win_rate']} | "
            f"{cell['factor_representative_average_after_cost_pips']} | "
            f"{cell['factor_representative_average_after_cost_spread_multiple']} |"
        )
    lines.extend(
        [
            "",
            (
                "Research-only. Effective factor N resolves immutable raw "
                "memberships through append-only V7R3 root aliases; aliases "
                "can only reduce N."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    database: Path = DEFAULT_DATABASE,
    candle_root: Path = DEFAULT_CANDLES,
    output: Path = DEFAULT_OUTPUT,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    upstream = validate_case_database(database)
    if not upstream.get("ok"):
        raise UpstreamIntegrityError(
            f"V7R3 case database blocked: {upstream.get('reason')}"
        )
    payload = base.run(
        database=database,
        candle_root=candle_root,
        output=output,
        report=report,
        case_contract_id=CASE_CONTRACT_ID,
        outcome_contract_id=CONTRACT_ID,
    )
    connection = _read_only(database)
    try:
        cells = summarize_canonical(connection)
    finally:
        connection.close()
    payload.update(
        schema_version=4,
        cells=cells,
        upstream_integrity=upstream,
        factor_episode_contract_id=FACTOR_EPISODE_CONTRACT_ID,
        previous_outcome_contract_id=(
            "live_move_news_forward_outcomes_v3r2_v7r2_"
            "overlap_factor_episodes_20260827"
        ),
        factor_deduplication=(
            "earliest case per canonical transitive factor root, arm, horizon, "
            "and cost bucket; append-only aliases can only reduce effective N"
        ),
        execution_eligible=False,
        research_only=True,
        can_place_orders=False,
        can_promote=False,
    )
    base.atomic_json(output, payload)
    base.atomic_text(report, render(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--candles", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run(
            database=args.database,
            candle_root=args.candles,
            output=args.output,
            report=args.report,
        )
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "case_count": payload["case_count"],
                    "retained_outcome_count": payload["retained_outcome_count"],
                    "canonical_root_count": payload["upstream_integrity"][
                        "canonical_root_count"
                    ],
                    "upstream_integrity_ok": payload["upstream_integrity"]["ok"],
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
