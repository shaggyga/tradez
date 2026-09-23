#!/usr/bin/env python3
"""Main fail-closed control plane for Forex system improvement branches.

This engine does not trade, modify models, promote hypotheses, or invent data.
It converts current system evidence into a deterministic branch portfolio:
one active internal build, zero or more frozen collection branches, and explicit
external blockers.  Safety failures always preempt research work.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REGISTRY = ROOT / "config" / "improvement_branch_registry_v1.json"
DEFAULT_STATE = STATE / "improvement_control_engine_v1.json"
DEFAULT_DB = STATE / "improvement_control_engine_v1.sqlite"
DEFAULT_REPORT = DATA / "reports" / "improvement_control" / "IMPROVEMENT_CONTROL_CURRENT.md"
MAJOR_MOVE_GAP_REPORT = (
    DATA / "reports" / "major_move_gap_census" / "MAJOR_MOVE_GAP_CENSUS_CURRENT.json"
)
EXTERNALLY_BLOCKED_INTEGRITY_FAILURES = {"clock_explicitly_classified"}
CONTAINED_HISTORICAL_INTEGRITY_FAILURES = {
    "shadow_archive_historical_detail_reconciled"
}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def system_facts(state_root: Path = STATE) -> dict[str, Any]:
    integrity = read_json(state_root / "project_integrity_audit_v1.json")
    macro = read_json(state_root / "macro_surprise_v1.json")
    rates = read_json(ROOT / "config" / "rates_policy_repricing_v1.json")
    watch = read_json(state_root / "news_technical_watchlist_v1.json")
    lifecycle = read_json(state_root / "evidence_lifecycle_v1.json")
    allocator = read_json(state_root / "allocator_proof_v1.json")
    cftc = read_json(state_root / "cftc_currency_positioning_shadow_v1.json")
    direct = read_json(state_root / "direct_source_response_v1.json")
    executable_opportunity = read_json(state_root / "executable_opportunity_prospective_v1.json")
    storage = read_json(state_root / "storage_headroom_v1.json")
    move_gaps = read_json(MAJOR_MOVE_GAP_REPORT)
    news_outcome_audit = read_json(
        state_root / "news_outcome_improvement_audit_v2.json"
    )
    news_record_generated = str(news_outcome_audit.get("generated_utc") or "")
    try:
        news_record_age_sec = max(
            0.0,
            (
                now_utc()
                - dt.datetime.fromisoformat(
                    news_record_generated.replace("Z", "+00:00")
                )
            ).total_seconds(),
        )
    except ValueError:
        news_record_age_sec = math.inf
    news_audit_summary = news_outcome_audit.get("summary") or {}
    move_gap_generated = str(move_gaps.get("generated_utc") or "")
    try:
        move_gap_age_sec = max(
            0.0,
            (now_utc() - dt.datetime.fromisoformat(move_gap_generated.replace("Z", "+00:00"))).total_seconds(),
        )
    except ValueError:
        move_gap_age_sec = math.inf
    primary_move_gaps = {
        str(row.get("primary_gap")): int(row.get("count") or 0)
        for row in (move_gaps.get("primary_gaps_effective") or [])
        if isinstance(row, Mapping)
    }
    states = (lifecycle.get("lifecycle") or {}).get("states") or {}
    integrity_failures = [str(value) for value in (integrity.get("failures") or [])]
    actionable_integrity_failures = [
        value for value in integrity_failures
        if value not in EXTERNALLY_BLOCKED_INTEGRITY_FAILURES
        and value not in CONTAINED_HISTORICAL_INTEGRITY_FAILURES
    ]
    return {
        "integrity_ok": integrity.get("status") == "ok",
        "integrity_failures": integrity_failures,
        "actionable_integrity_failures": actionable_integrity_failures,
        "externally_blocked_integrity_failures": [
            value for value in integrity_failures
            if value in EXTERNALLY_BLOCKED_INTEGRITY_FAILURES
        ],
        "contained_historical_integrity_failures": [
            value for value in integrity_failures
            if value in CONTAINED_HISTORICAL_INTEGRITY_FAILURES
        ],
        "storage_ok": storage.get("status") == "ok",
        "causal_consensus_count": int(macro.get("causal_consensus_observation_count") or 0),
        "standardized_surprise_count": int(macro.get("standardized_surprise_count") or 0),
        "rates_connected": rates.get("state") == "connected" and bool(rates.get("currencies")),
        "news_watchlist_running": watch.get("status") == "ok",
        "news_matured_entries": sum(
            int((row or {}).get("matured_n") or 0)
            for row in ((watch.get("evidence") or {}).get("arms") or {}).values()
        ),
        "news_outcome_record_valid": (
            news_outcome_audit.get("contract_id")
            == "news_outcome_improvement_audit_v2_20260826"
            and news_outcome_audit.get("status") == "ok"
            and news_outcome_audit.get("refresh_mode")
            == "on_demand_append_only"
            and news_outcome_audit.get("recurring_polling") is False
            and bool(news_outcome_audit.get("research_only"))
            and not bool(news_outcome_audit.get("execution_eligible"))
            and not bool(news_outcome_audit.get("can_place_orders"))
            and not bool(news_outcome_audit.get("can_promote"))
            and not bool(news_outcome_audit.get("can_modify_execution_policy"))
            and news_outcome_audit.get("sqlite_integrity") == "ok"
        ),
        "news_outcome_record_age_sec": news_record_age_sec,
        "news_diagnosed_effective_theses": int(
            news_audit_summary.get("current_effective_theses") or 0
        ),
        "news_diagnosed_misses_or_gaps": int(
            news_audit_summary.get("misses_or_gaps") or 0
        ),
        "news_improvement_queue_count": int(
            (news_outcome_audit.get("queue") or {}).get("queue_count") or 0
        ),
        "confirmed_candidates": int(states.get("confirmed_candidate") or 0),
        "collecting_hypotheses": int(states.get("continue_collecting") or 0),
        "futility_retired": int(states.get("futility_rejected") or 0),
        "allocator_frozen": bool(allocator.get("cohort")),
        "cftc_collecting": bool(cftc.get("currency_count") or cftc.get("currencies")),
        "direct_source_response_running": direct.get("status") == "ok",
        "direct_source_matured_targets": int(
            ((direct.get("summary") or {}).get("target_states") or {}).get("matured") or 0
        ),
        "executable_opportunity_collecting": (
            executable_opportunity.get("status") in {"ok", "market_or_source_stale"}
            and bool(executable_opportunity.get("research_only"))
            and not bool(executable_opportunity.get("execution_eligible"))
            and not (executable_opportunity.get("artifact_failures") or [])
        ),
        "executable_opportunity_forecasts": int(
            (executable_opportunity.get("totals") or {}).get("forecasts") or 0
        ),
        "executable_opportunity_maturities": int(
            (executable_opportunity.get("totals") or {}).get("matured") or 0
        ),
        "major_move_gap_census_ok": bool(move_gaps.get("research_only"))
        and not bool(move_gaps.get("execution_eligible"))
        and int(move_gaps.get("raw_move_rows") or 0) > 0
        and move_gap_age_sec <= 28_800,
        "major_move_gap_age_sec": move_gap_age_sec,
        "major_move_rows": int(move_gaps.get("raw_move_rows") or 0),
        "major_move_factor_episodes": int(move_gaps.get("factor_episode_count") or 0),
        "major_move_cross_selection_gaps": int(
            primary_move_gaps.get("cross_sectional_selection_gap") or 0
        ),
        "major_move_wrong_direction_gaps": int(
            primary_move_gaps.get("wrong_direction_gate_suppressed") or 0
        )
        + int(primary_move_gaps.get("selected_wrong_direction") or 0),
        "major_move_magnitude_gaps": int(
            primary_move_gaps.get("correct_direction_low_cost_clearance") or 0
        )
        + int(primary_move_gaps.get("correct_direction_underconfident") or 0),
    }


def classify_branch(branch: Mapping[str, Any], facts: Mapping[str, Any]) -> tuple[str, str, float]:
    branch_id = str(branch["branch_id"])
    if branch_id == "operational_integrity":
        actionable = list(facts.get("actionable_integrity_failures") or [])
        blocked = list(facts.get("externally_blocked_integrity_failures") or [])
        contained = list(facts.get("contained_historical_integrity_failures") or [])
        if not facts["storage_ok"] or actionable:
            reason = "storage unsafe" if not facts["storage_ok"] else "actionable integrity failure: " + ", ".join(actionable)
            return "active_build", reason, 1000.0
        if blocked:
            suffix = (
                "; contained historical incident: " + ", ".join(contained)
                if contained else ""
            )
            return (
                "blocked_external",
                "fail-closed external remediation required: "
                + ", ".join(blocked) + suffix,
                0.0,
            )
        if contained:
            return (
                "contained_degraded",
                "historical evidence incident localized and current write/delete path hardened: "
                + ", ".join(contained),
                0.0,
            )
        return "standby", "healthy", 0.0
    if branch_id == "causal_macro_rates":
        missing = []
        if facts["causal_consensus_count"] <= 0:
            missing.append("pre-release consensus")
        if not facts["rates_connected"]:
            missing.append("timestamp-safe rates/OIS repricing")
        if missing:
            return "blocked_external", "missing " + " and ".join(missing), 0.0
        return "collecting", "causal inputs connected; freeze prospective source cohort", 0.0
    if branch_id == "magnitude_cost_targets":
        if facts.get("direct_source_response_running") and facts.get("executable_opportunity_collecting"):
            return "collecting", (
                "direct-source and frozen executable-opportunity cohorts collecting; "
                f"{int(facts.get('direct_source_matured_targets') or 0)} direct targets and "
                f"{int(facts.get('executable_opportunity_maturities') or 0)} opportunity forecasts matured"
            ), 0.0
        if facts.get("direct_source_response_running"):
            return "candidate_build", "start the frozen executable-opportunity prospective collector", 0.0
        return "candidate_build", "best unblocked internal improvement", 0.0
    if branch_id == "positioning_crowding":
        return ("collecting" if facts["cftc_collecting"] else "candidate_build", "CFTC collecting; retail rights/source still pending" if facts["cftc_collecting"] else "start authoritative CFTC collection", 0.0)
    if branch_id == "event_response_archetypes":
        if not facts["news_watchlist_running"]:
            return "blocked_internal", "news/technical prospective ledger unavailable", 0.0
        if not facts.get("news_outcome_record_valid"):
            return "candidate_build", "canonical on-demand news outcome record absent or invalid", 0.0
        return (
            "collecting",
            "four immutable news/technical arms accumulating; canonical review record "
            f"tracks {int(facts.get('news_diagnosed_effective_theses') or 0)} "
            "effective theses, "
            f"{int(facts.get('news_diagnosed_misses_or_gaps') or 0)} misses/gaps, and "
            f"{int(facts.get('news_improvement_queue_count') or 0)} evidence-gated proposals",
            0.0,
        )
    if branch_id == "allocator_policy_proof":
        return ("collecting" if facts["allocator_frozen"] else "blocked_internal", "frozen allocator accumulating" if facts["allocator_frozen"] else "allocator cohort absent", 0.0)
    if branch_id == "major_move_gap_replay":
        if not facts.get("major_move_gap_census_ok"):
            return "candidate_build", "major-move census absent or older than eight hours", 0.0
        return (
            "collecting",
            f"{int(facts.get('major_move_rows') or 0)} raw moves / "
            f"{int(facts.get('major_move_factor_episodes') or 0)} factor episodes; "
            f"effective gaps: {int(facts.get('major_move_cross_selection_gaps') or 0)} selection, "
            f"{int(facts.get('major_move_wrong_direction_gaps') or 0)} direction, "
            f"{int(facts.get('major_move_magnitude_gaps') or 0)} magnitude-cost",
            0.0,
        )
    if branch_id == "practice_canary_execution":
        return ("ready_for_canary" if facts["confirmed_candidates"] > 0 else "blocked_governance", "confirmed candidate exists" if facts["confirmed_candidates"] > 0 else "zero confirmed candidates", 0.0)
    return "standby", "unclassified", 0.0


def branch_score(branch: Mapping[str, Any]) -> float:
    return round(
        float(branch.get("priority", 0))
        * (0.4 * float(branch.get("uniqueness", 0)) + 0.4 * float(branch.get("unblock_leverage", 0)) + 0.2 * (1.0 - float(branch.get("implementation_cost", 1)))),
        3,
    )


def evaluate(registry: Mapping[str, Any], facts: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for raw in registry.get("branches") or []:
        branch = dict(raw)
        state, reason, override = classify_branch(branch, facts)
        branch.update({"state": state, "state_reason": reason, "score": override or branch_score(branch)})
        rows.append(branch)
    active_safety = [row for row in rows if row["branch_id"] == "operational_integrity" and row["state"] == "active_build"]
    candidates = [row for row in rows if row["state"] == "candidate_build"]
    active = (active_safety or sorted(candidates, key=lambda row: (-row["score"], row["branch_id"])))[:1]
    active_id = active[0]["branch_id"] if active else None
    for row in rows:
        if row["branch_id"] == active_id:
            row["state"] = "active_build"
    blocked = [row["branch_id"] for row in rows if row["state"].startswith("blocked")]
    collecting = [row["branch_id"] for row in rows if row["state"] == "collecting"]
    return {
        "active_build_branch": active_id,
        "active_build_action": active[0]["next_action"] if active else "observe governed evidence",
        "collecting_branches": collecting,
        "blocked_branches": blocked,
        "branches": rows,
    }


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE IF NOT EXISTS decisions (generated_utc TEXT PRIMARY KEY, active_branch TEXT, payload_json TEXT NOT NULL)")
    return db


def run_once(registry_path: Path = REGISTRY, state_path: Path = DEFAULT_STATE, db_path: Path = DEFAULT_DB, report_path: Path = DEFAULT_REPORT) -> dict[str, Any]:
    registry = read_json(registry_path)
    facts = system_facts()
    portfolio = evaluate(registry, facts)
    generated = now_utc().isoformat()
    payload = {
        "schema_version": 1, "generated_utc": generated,
        "controller_id": registry.get("controller_id"), "status": "ok",
        "research_only": True, "execution_eligible": False,
        "can_place_orders": False, "can_promote": False,
        "supported_execution_decision": "no_trade" if facts["confirmed_candidates"] == 0 else "requires_exact_canary_authorization",
        "facts": facts, "portfolio": portfolio,
        "control_contract": {
            "one_active_build_branch": True,
            "frozen_collectors_are_not_retuned": True,
            "safety_preempts_research": True,
            "blocked_external_sources_are_not_fabricated": True,
            "research_cannot_promote_or_execute": True,
        },
    }
    db = open_db(db_path)
    prior = db.execute("SELECT active_branch FROM decisions ORDER BY generated_utc DESC LIMIT 1").fetchone()
    active = portfolio["active_build_branch"] or ""
    if prior is None or prior[0] != active:
        db.execute("INSERT INTO decisions VALUES (?,?,?)", (generated, active, json.dumps(payload, sort_keys=True)))
        db.commit()
    db.close()
    atomic_write(state_path, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# Forex Improvement Control", "", f"Generated: `{generated}`", "",
        f"- Active build branch: **{active or 'none'}**",
        f"- Next action: **{portfolio['active_build_action']}**",
        f"- Collecting unchanged: **{', '.join(portfolio['collecting_branches']) or 'none'}**",
        f"- Blocked: **{', '.join(portfolio['blocked_branches']) or 'none'}**",
        f"- Execution decision: **{payload['supported_execution_decision']}**", "",
        "| Engine stage | Branch | State | Score | Reason |", "|---|---|---|---:|---|",
    ]
    for row in portfolio["branches"]:
        lines.append(f"| {row['engine_stage']} | {row['branch_id']} | {row['state']} | {row['score']:.2f} | {row['state_reason']} |")
    lines += ["", "This controller selects research work only. It cannot trade, promote, retune frozen cohorts, or invent a missing source.", ""]
    atomic_write(report_path, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=60)
    parser.add_argument("--duration-sec", type=float, default=604800)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec
    while True:
        run_once(args.registry, args.state, args.database, args.report)
        if args.once or time.monotonic() >= stop:
            return 0
        time.sleep(min(args.interval_sec, max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
