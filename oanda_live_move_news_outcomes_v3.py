#!/usr/bin/env python3
"""V7R2-bound forward outcomes for boundary-independent mover episodes.

This is a separate prospective research cohort. It reads only the V7R2 case
database and writes only V3R2 outcome artifacts. V2R2/V6R2 evidence remains
untouched. The worker validates the exact snapshot and factor-episode contracts
before allowing the shared executable-path maturation logic to append outcomes.
It has no broker, authorization, promotion, or execution surface.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import oanda_live_move_news_outcomes as base
import oanda_live_move_news_snapshot_v7 as live_v7r2


DATA = Path(__file__).resolve().parent / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_DATABASE = STATE / "live_move_news_cases_v7r2.sqlite"
DEFAULT_CANDLES = DATA / "candles"
DEFAULT_OUTPUT = STATE / "live_move_news_outcomes_v3r2.json"
DEFAULT_REPORT = (
    DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_OUTCOMES_CURRENT_V3R2.md"
)
CONTRACT_ID = (
    "live_move_news_forward_outcomes_v3r2_v7r2_"
    "overlap_factor_episodes_20260827"
)
CASE_CONTRACT_ID = live_v7r2.CONTRACT_ID
FACTOR_EPISODE_CONTRACT_ID = live_v7r2.FACTOR_EPISODE_CONTRACT_ID


class UpstreamIntegrityError(RuntimeError):
    """The exact V7R2 case/factor contract was absent or inconsistent."""


def _read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)


def validate_case_database(path: Path) -> dict[str, Any]:
    """Validate exact V7R2 lineage and membership before outcome mutation."""

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
            quick_check = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
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
            }
            missing_tables = sorted(required - tables)
            if missing_tables:
                result.update(
                    quick_check=quick_check,
                    reason="required_tables_missing",
                    missing_tables=missing_tables,
                )
                return result
            case_registry = connection.execute(
                "SELECT COUNT(*) FROM mover_case_contract_registry "
                "WHERE contract_id=? AND research_only=1 "
                "AND execution_eligible=0",
                (CASE_CONTRACT_ID,),
            ).fetchone()[0]
            factor_registry = connection.execute(
                "SELECT COUNT(*) FROM factor_episode_contract_registry "
                "WHERE factor_episode_contract_id=? "
                "AND snapshot_contract_id=? AND research_only=1 "
                "AND execution_eligible=0",
                (FACTOR_EPISODE_CONTRACT_ID, CASE_CONTRACT_ID),
            ).fetchone()[0]
            conflict_count = connection.execute(
                "SELECT COUNT(*) FROM factor_episode_membership_conflicts"
            ).fetchone()[0]
            invalid_json_count = connection.execute(
                "SELECT COUNT(*) FROM mover_cases WHERE json_valid(case_json)=0"
            ).fetchone()[0]
            case_count = connection.execute(
                "SELECT COUNT(*) FROM mover_cases "
                "WHERE json_extract(case_json,'$.contract_id')=?",
                (CASE_CONTRACT_ID,),
            ).fetchone()[0]
            membership_mismatch_count = connection.execute(
                "SELECT COUNT(*) FROM mover_cases c "
                "LEFT JOIN factor_episode_membership f ON f.case_id=c.case_id "
                "WHERE json_extract(c.case_json,'$.contract_id')=? AND ("
                " f.case_id IS NULL OR f.factor_episode_contract_id<>? OR "
                " f.factor_episode_id<>json_extract(c.case_json,'$.factor_episode_id') OR "
                " f.factor_primary_token<>json_extract(c.case_json,'$.factor_primary_token'))",
                (CASE_CONTRACT_ID, FACTOR_EPISODE_CONTRACT_ID),
            ).fetchone()[0]
            row_conflict_count = connection.execute(
                "SELECT COUNT(*) FROM mover_cases WHERE "
                "json_extract(case_json,'$.contract_id')=? AND ("
                "COALESCE(json_extract(case_json,'$.factor_episode_merge_conflict'),0)=1 "
                "OR COALESCE(json_extract(case_json,'$.factor_episode_membership_conflict'),0)=1)",
                (CASE_CONTRACT_ID,),
            ).fetchone()[0]
        finally:
            connection.close()
    except (sqlite3.Error, TypeError, IndexError) as exc:
        result["reason"] = f"database_validation_error:{type(exc).__name__}"
        return result
    result.update(
        quick_check=quick_check,
        case_registry_count=int(case_registry),
        factor_registry_count=int(factor_registry),
        case_count=int(case_count),
        membership_conflict_count=int(conflict_count),
        membership_mismatch_count=int(membership_mismatch_count),
        row_conflict_count=int(row_conflict_count),
        invalid_json_count=int(invalid_json_count),
    )
    failures = []
    if quick_check != "ok":
        failures.append("sqlite_quick_check")
    if int(case_registry) != 1:
        failures.append("case_contract_registry")
    if int(factor_registry) != 1:
        failures.append("factor_contract_registry")
    if int(conflict_count):
        failures.append("membership_conflicts")
    if int(membership_mismatch_count):
        failures.append("membership_mismatch")
    if int(row_conflict_count):
        failures.append("row_conflicts")
    if int(invalid_json_count):
        failures.append("invalid_case_json")
    result["failures"] = failures
    result["ok"] = not failures
    result["reason"] = "ready" if not failures else ";".join(failures)
    return result


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
            f"V7R2 case database blocked: {upstream.get('reason')}"
        )
    payload = base.run(
        database=database,
        candle_root=candle_root,
        output=output,
        report=report,
        case_contract_id=CASE_CONTRACT_ID,
        outcome_contract_id=CONTRACT_ID,
    )
    payload["schema_version"] = 3
    payload["upstream_integrity"] = upstream
    payload["factor_episode_contract_id"] = FACTOR_EPISODE_CONTRACT_ID
    payload["previous_outcome_contract_id"] = (
        "live_move_news_forward_outcomes_v2r2_precise_start_clock_20260827"
    )
    payload["execution_eligible"] = False
    payload["research_only"] = True
    payload["can_place_orders"] = False
    payload["can_promote"] = False
    base.atomic_json(output, payload)
    with report.open("a", encoding="utf-8") as stream:
        stream.write(
            "\nV7R2 upstream integrity: **ready**; exact case and factor "
            "contracts verified before maturation.\n"
        )
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
                    "retained_outcome_count": payload[
                        "retained_outcome_count"
                    ],
                    "upstream_integrity_ok": payload[
                        "upstream_integrity"
                    ]["ok"],
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
