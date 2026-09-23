#!/usr/bin/env python3
"""Executable model-space plan for the OANDA FX trainer.

This is the control layer above the research queue.  It checks what has already
been completed, what is still pending, and seeds the next under-covered phase
instead of letting the trainer drift through only random/generated specs.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from oanda_model_lifecycle import (
    ModelLifecycleRegistry,
    experiment_spec_hash,
    validation_spec_for_screen_pass,
)
from oanda_model_space_agenda import (
    EXTERNAL_BASELINE_LANES,
    PAIR_FAMILY_SUBSETS,
    PROMOTIONS_ROOT,
    REGISTRY_ROOT,
    SCOUT_PRIORITY_SUBSETS,
    TRAINING_ROOT,
    build_agenda,
    installed_model_types,
)


MODEL_SPACE_ROOT = TRAINING_ROOT / "model_space"
PLAN_LATEST_PATH = MODEL_SPACE_ROOT / "model_space_plan_latest.json"
PLAN_HISTORY_PATH = MODEL_SPACE_ROOT / "model_space_plan_history.jsonl"
RESEARCH_LEDGER_PATH = TRAINING_ROOT / "continuous_research" / "experiment_ledger.csv"
ROBUST_VALIDATION_FOLLOWUP_PROFILE = "robust_screen_winner_followup_v1"


PLAN_PHASES: List[Dict[str, Any]] = [
    {
        "phase_id": "P0a_live_account_split_specialists",
        "priority": 4,
        "target_pending": 70,
        "target_completed": 160,
        "description": "Current live split: strict tech-champion specialists plus broader primary-challenger scout variants. GPT production account is intentionally excluded.",
        "deployment_lanes": [
            "live_tech_champion_candidate",
            "live_primary_challenger_candidate",
        ],
    },
    {
        "phase_id": "P0_scout_lead_major_moves",
        "priority": 5,
        "target_pending": 90,
        "target_completed": 180,
        "description": "Lead-time major-move models for the live technical scout: predict volatile spikes before onset, then score current-entry path outcome.",
        "dataset_kinds": ["technical_spike"],
        "target_prefixes": ["major_event_lead_"],
        "research_roles": ["major_move_factor"],
        "instrument_subsets": SCOUT_PRIORITY_SUBSETS,
    },
    {
        "phase_id": "P0b_pair_family_return_curves",
        "priority": 8,
        "target_pending": 80,
        "target_completed": 180,
        "description": "Pair-family specialist return-curve/path-quality models for entry, exit, MFE, MAE, and giveback behavior.",
        "dataset_kinds": ["technical_spike"],
        "target_prefixes": ["long_curve_profit_", "short_curve_profit_", "continuation_curve_profit_", "reversal_curve_profit_"],
        "research_roles": ["return_curve_candidate"],
        "instrument_subsets": SCOUT_PRIORITY_SUBSETS + PAIR_FAMILY_SUBSETS,
    },
    {
        "phase_id": "P1_opportunity_gate",
        "priority": 10,
        "target_pending": 120,
        "target_completed": 250,
        "description": "Find profitable-move/opportunity gates, especially for volatile and non-USD volatile pairs.",
        "dataset_kinds": ["profitable_move_precursor"],
        "target_prefixes": ["profitable_any_move_"],
        "instrument_subsets": ["volatile", "non_usd_volatile", "volatile_exotic", "all", "majors"],
    },
    {
        "phase_id": "P2_directional_technical",
        "priority": 20,
        "target_pending": 100,
        "target_completed": 220,
        "description": "Find direction-capable technical models for long/short/continuation/reversal profit targets.",
        "dataset_kinds": ["technical_spike"],
        "target_prefixes": ["long_profit_", "short_profit_", "continuation_profit_", "reversal_profit_"],
        "instrument_subsets": ["volatile", "non_usd_volatile", "volatile_exotic", "exotic", "majors", "all"],
    },
    {
        "phase_id": "P2b_return_curve_technical",
        "priority": 18,
        "target_pending": 100,
        "target_completed": 220,
        "description": "Find technical models that predict tradable forward return curves, including early follow-through, MFE, MAE, giveback, and endpoint retention.",
        "dataset_kinds": ["technical_spike"],
        "target_prefixes": ["long_curve_profit_", "short_curve_profit_", "continuation_curve_profit_", "reversal_curve_profit_"],
        "research_roles": ["return_curve_candidate"],
        "instrument_subsets": ["volatile", "non_usd_volatile", "volatile_exotic", "exotic", "majors", "all"],
    },
    {
        "phase_id": "P3_major_event_factors",
        "priority": 30,
        "target_pending": 80,
        "target_completed": 160,
        "description": "Train major-spike factor models for the scout/technical accounts.",
        "dataset_kinds": ["technical_spike"],
        "target_prefixes": ["major_event_"],
        "research_roles": ["major_move_factor"],
        "instrument_subsets": ["volatile", "non_usd_volatile", "volatile_exotic", "all"],
    },
    {
        "phase_id": "P4_base_quality_sanity",
        "priority": 40,
        "target_pending": 20,
        "target_completed": 60,
        "description": "Keep legacy/base account-manager signal-quality baselines refreshed.",
        "dataset_kinds": ["base_trade_quality"],
        "target_prefixes": ["would_profit_30m"],
        "instrument_subsets": ["all"],
    },
    {
        "phase_id": "P5_external_baseline_lanes",
        "priority": 50,
        "target_pending": 0,
        "target_completed": 0,
        "description": "Non-tabular lanes are tracked separately; each needs its own evaluator and evidence report.",
        "external_only": True,
    },
]


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def append_jsonl(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(obj, sort_keys=True, default=str) + "\n")


def read_queue_rows() -> List[Dict[str, Any]]:
    path = REGISTRY_ROOT / "research_queue.jsonl"
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        try:
            item = json.loads(raw)
        except Exception:
            continue
        if isinstance(item, dict) and isinstance(item.get("spec"), dict):
            item["spec_hash"] = str(item.get("spec_hash") or experiment_spec_hash(item["spec"]))
            rows.append(item)
    return rows


def read_ledger_rows() -> List[Dict[str, Any]]:
    if not RESEARCH_LEDGER_PATH.exists():
        return []
    try:
        with RESEARCH_LEDGER_PATH.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def spec_matches_phase(spec: Dict[str, Any], phase: Dict[str, Any]) -> bool:
    if phase.get("external_only"):
        return False
    deployment_lanes = set(phase.get("deployment_lanes") or [])
    if deployment_lanes and str(spec.get("deployment_lane") or "") not in deployment_lanes:
        return False
    dataset_kinds = set(phase.get("dataset_kinds") or [])
    if dataset_kinds and str(spec.get("dataset_kind") or "") not in dataset_kinds:
        return False
    prefixes = list(phase.get("target_prefixes") or [])
    target = str(spec.get("target") or "")
    if prefixes and not any(target.startswith(prefix) for prefix in prefixes):
        return False
    roles = set(phase.get("research_roles") or [])
    if roles and str(spec.get("research_role") or "") not in roles:
        return False
    subsets = set(phase.get("instrument_subsets") or [])
    if subsets and str(spec.get("instrument_subset") or "") not in subsets:
        return False
    return True


def phase_for_spec(spec: Dict[str, Any]) -> str:
    for phase in PLAN_PHASES:
        if spec_matches_phase(spec, phase):
            return str(phase["phase_id"])
    return "unclassified"


def unique_pending_specs(queue_rows: Iterable[Dict[str, Any]], completed: set[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in queue_rows:
        spec = row.get("spec")
        if not isinstance(spec, dict):
            continue
        spec_hash = str(row.get("spec_hash") or experiment_spec_hash(spec))
        if spec_hash in completed:
            continue
        out.setdefault(spec_hash, spec)
    return out


def ledger_latest_by_hash(ledger_rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in ledger_rows:
        spec_hash = str(row.get("spec_hash") or "").strip()
        if not spec_hash:
            continue
        latest[spec_hash] = row
    return latest


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def load_experiment_spec(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    detail_path = Path(str(row.get("detail_path") or ""))
    if not detail_path.exists():
        return None
    try:
        payload = json.loads(detail_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    spec = payload.get("spec")
    return spec if isinstance(spec, dict) else None


def is_non_scout_validation_followup_candidate(spec: Dict[str, Any]) -> bool:
    target = str(spec.get("target") or "").lower()
    role = str(spec.get("research_role") or "").lower()
    subset = str(spec.get("instrument_subset") or "").lower()
    if target.startswith("major_event") or role == "major_move_factor":
        return False
    if (
        role == "return_curve_candidate"
        or "_curve_profit_" in target
        or role == "profitable_move_precursor"
    ):
        return subset in {
            "volatile",
            "non_usd_volatile",
            "volatile_exotic",
            "exotic",
            "exotic_high_spread",
            "volatile_non_usd",
            "jpy_risk",
            "commodity",
            "chf_safe_haven",
        }
    return False


def select_robust_validation_followups(
    *,
    ledger_rows: Sequence[Dict[str, Any]],
    seen_hashes: set[str],
    limit: int,
    deployment_lane: Optional[str] = None,
    account_focus: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Pick high-scoring screen winners for longer validation follow-up.

    This is intentionally not a broad reseed.  It promotes only already
    successful screen rows into stricter validation windows, prioritizing
    return-curve and pair-family candidates that are plausible for the
    non-GPT technical/primary lanes.
    """
    candidates: List[tuple[float, float, float, str, Dict[str, Any]]] = []
    for row in ledger_rows:
        if str(row.get("evaluation_stage") or "").lower() != "screen":
            continue
        if str(row.get("status") or "").lower() != "successful":
            continue
        if not truthy(row.get("gate_passed")):
            continue
        spec = load_experiment_spec(row)
        if not spec or not is_non_scout_validation_followup_candidate(spec):
            continue
        if not spec_matches_requested_scope(
            spec,
            deployment_lane=deployment_lane,
            account_focus=account_focus,
        ):
            continue
        validation_spec = validation_spec_for_screen_pass(spec)
        validation_spec["validation_followup_profile"] = ROBUST_VALIDATION_FOLLOWUP_PROFILE
        validation_spec["validation_followup_source_experiment"] = row.get(
            "experiment_id",
            "",
        )
        spec_hash = experiment_spec_hash(validation_spec)
        if spec_hash in seen_hashes:
            continue
        score = safe_float(row.get("score"), 0.0)
        mean_auc = safe_float(row.get("mean_auc"), 0.0)
        mean_net = safe_float(row.get("mean_net_pips"), 0.0)
        candidates.append((score, mean_auc, mean_net, spec_hash, validation_spec))
    candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    out: List[Dict[str, Any]] = []
    for _, _, _, spec_hash, spec in candidates:
        if spec_hash in seen_hashes:
            continue
        seen_hashes.add(spec_hash)
        out.append(spec)
        if len(out) >= limit:
            break
    return out


def phase_rows(
    *,
    agenda_specs: Sequence[Dict[str, Any]],
    pending_specs: Dict[str, Dict[str, Any]],
    latest_ledger: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for phase in PLAN_PHASES:
        if phase.get("external_only"):
            rows.append({
                "phase_id": phase["phase_id"],
                "priority": phase["priority"],
                "description": phase["description"],
                "external_only": True,
                "external_lanes": [lane["lane"] for lane in EXTERNAL_BASELINE_LANES],
                "total_enumerated": 0,
                "pending": 0,
                "completed": 0,
                "successful": 0,
                "unsuccessful": 0,
                "error": 0,
                "target_pending": phase.get("target_pending", 0),
                "target_completed": phase.get("target_completed", 0),
                "needs_seed": False,
            })
            continue
        total = 0
        pending = 0
        completed = 0
        successful = 0
        unsuccessful = 0
        error = 0
        for spec in agenda_specs:
            if not spec_matches_phase(spec, phase):
                continue
            total += 1
            spec_hash = experiment_spec_hash(spec)
            if spec_hash in pending_specs:
                pending += 1
            row = latest_ledger.get(spec_hash)
            if row:
                completed += 1
                status = str(row.get("status") or "").lower()
                if status == "successful":
                    successful += 1
                elif status == "error":
                    error += 1
                else:
                    unsuccessful += 1
        target_pending = int(phase.get("target_pending") or 0)
        target_completed = int(phase.get("target_completed") or 0)
        effective_target_pending = min(target_pending, total)
        remaining_unseen = max(0, total - completed - pending)
        rows.append({
            "phase_id": phase["phase_id"],
            "priority": phase["priority"],
            "description": phase["description"],
            "external_only": False,
            "total_enumerated": total,
            "pending": pending,
            "completed": completed,
            "successful": successful,
            "unsuccessful": unsuccessful,
            "error": error,
            "target_pending": target_pending,
            "effective_target_pending": effective_target_pending,
            "target_completed": target_completed,
            "remaining_unseen": remaining_unseen,
            "coverage_completed_pct": round((completed / total) * 100.0, 3) if total else 0.0,
            "success_rate_completed_pct": round((successful / completed) * 100.0, 3) if completed else 0.0,
            "needs_seed": (
                pending < effective_target_pending
                and completed < target_completed
                and remaining_unseen > 0
            ),
        })
    return rows


def select_specs_for_phase(
    *,
    phase: Dict[str, Any],
    agenda_specs: Sequence[Dict[str, Any]],
    seen_hashes: set[str],
    limit: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for spec in agenda_specs:
        if not spec_matches_phase(spec, phase):
            continue
        spec_hash = experiment_spec_hash(spec)
        if spec_hash in seen_hashes:
            continue
        seen_hashes.add(spec_hash)
        out.append(spec)
        if len(out) >= limit:
            break
    return out


def spec_matches_requested_scope(
    spec: Dict[str, Any],
    *,
    deployment_lane: Optional[str] = None,
    account_focus: Optional[str] = None,
) -> bool:
    """Restrict plan selection to one account/research lane when requested."""
    if deployment_lane is not None and str(spec.get("deployment_lane") or "") != deployment_lane:
        return False
    if account_focus is not None and str(spec.get("account_focus") or "") != account_focus:
        return False
    return True


def run_plan(
    *,
    apply: bool = False,
    max_seed: int = 80,
    include_new_estimators: bool = True,
    min_total_pending: int = 40,
    phase_id: Optional[str] = None,
    validation_followups: bool = False,
    validation_followups_only: bool = False,
    deployment_lane: Optional[str] = None,
    account_focus: Optional[str] = None,
) -> Dict[str, Any]:
    registry = ModelLifecycleRegistry(REGISTRY_ROOT, PROMOTIONS_ROOT)
    model_types = installed_model_types(include_new_estimators=include_new_estimators)
    agenda_specs_all = build_agenda(model_types)
    agenda_specs = [
        spec
        for spec in agenda_specs_all
        if spec_matches_requested_scope(
            spec,
            deployment_lane=deployment_lane,
            account_focus=account_focus,
        )
    ]
    queue_rows = read_queue_rows()
    ledger_rows = read_ledger_rows()
    completed_hashes = {str(row.get("spec_hash") or "").strip() for row in ledger_rows if str(row.get("spec_hash") or "").strip()}
    latest_ledger = ledger_latest_by_hash(ledger_rows)
    pending_specs = unique_pending_specs(queue_rows, completed_hashes)
    rows = phase_rows(
        agenda_specs=agenda_specs,
        pending_specs=pending_specs,
        latest_ledger=latest_ledger,
    )

    total_pending = len(pending_specs)
    selected_specs: List[Dict[str, Any]] = []
    seeded_by_phase: Dict[str, int] = {}
    seen_hashes = set(completed_hashes) | set(pending_specs.keys())
    validation_followup_count = 0
    if max_seed > 0 and validation_followups:
        chosen_followups = select_robust_validation_followups(
            ledger_rows=ledger_rows,
            seen_hashes=seen_hashes,
            limit=max_seed,
            deployment_lane=deployment_lane,
            account_focus=account_focus,
        )
        selected_specs.extend(chosen_followups)
        validation_followup_count = len(chosen_followups)
        if chosen_followups:
            seeded_by_phase["robust_validation_followups"] = len(chosen_followups)
    should_seed = (
        max_seed > 0
        and not validation_followups_only
        and (apply or total_pending < min_total_pending)
    )
    if should_seed:
        row_by_phase = {str(row["phase_id"]): row for row in rows}
        for phase in sorted(PLAN_PHASES, key=lambda item: int(item.get("priority") or 0)):
            current_phase_id = str(phase["phase_id"])
            if phase.get("external_only"):
                continue
            if phase_id and phase_id != current_phase_id:
                continue
            row = row_by_phase.get(current_phase_id, {})
            if not row.get("needs_seed") and not phase_id:
                continue
            deficit = max(0, int(row.get("target_pending") or 0) - int(row.get("pending") or 0))
            if phase_id:
                deficit = max(deficit, max_seed - len(selected_specs))
            room = max_seed - len(selected_specs)
            if room <= 0:
                break
            take = min(deficit, room)
            if take <= 0:
                continue
            chosen = select_specs_for_phase(
                phase=phase,
                agenda_specs=agenda_specs,
                seen_hashes=seen_hashes,
                limit=take,
            )
            selected_specs.extend(chosen)
            seeded_by_phase[current_phase_id] = seeded_by_phase.get(current_phase_id, 0) + len(chosen)
            if len(selected_specs) >= max_seed:
                break

    if apply:
        for spec in selected_specs:
            registry.enqueue(spec, reason=f"model-space plan {phase_for_spec(spec)}")

    # Re-read after apply for accurate visible state.
    if apply and selected_specs:
        queue_rows = read_queue_rows()
        ledger_rows = read_ledger_rows()
        completed_hashes = {str(row.get("spec_hash") or "").strip() for row in ledger_rows if str(row.get("spec_hash") or "").strip()}
        latest_ledger = ledger_latest_by_hash(ledger_rows)
        pending_specs = unique_pending_specs(queue_rows, completed_hashes)
        rows = phase_rows(
            agenda_specs=agenda_specs,
            pending_specs=pending_specs,
            latest_ledger=latest_ledger,
        )
        total_pending = len(pending_specs)

    active_phase = next(
        (
            row["phase_id"]
            for row in sorted(rows, key=lambda item: int(item.get("priority") or 0))
            if row.get("needs_seed")
        ),
        "queue_sufficient_or_external",
    )
    successful_rows = [
        row for row in ledger_rows
        if str(row.get("status") or "").lower() == "successful"
    ]
    latest_success = successful_rows[-1] if successful_rows else {}
    plan = {
        "updated_utc": utc_iso(),
        "applied": bool(apply),
        "model_types": model_types,
        "scope": {
            "deployment_lane": deployment_lane or "",
            "account_focus": account_focus or "",
            "agenda_specs_in_scope": len(agenda_specs),
            "agenda_specs_total": len(agenda_specs_all),
        },
        "max_seed": max_seed,
        "min_total_pending": min_total_pending,
        "total_pending_unique": total_pending,
        "total_queue_rows": len(queue_rows),
        "total_completed_hashes": len(completed_hashes),
        "seeded_this_run": len(selected_specs) if apply else 0,
        "selected_this_run": len(selected_specs),
        "seeded_by_phase": seeded_by_phase,
        "validation_followups_selected": validation_followup_count,
        "active_phase": active_phase,
        "phase_rows": rows,
        "latest_success": {
            "time_utc": latest_success.get("time_utc", ""),
            "experiment_id": latest_success.get("experiment_id", ""),
            "source": latest_success.get("source", ""),
            "dataset_kind": latest_success.get("dataset_kind", ""),
            "model_type": latest_success.get("model_type", ""),
            "target": latest_success.get("target", ""),
            "instrument_subset": latest_success.get("instrument_subset", ""),
            "mean_net_pips": latest_success.get("mean_net_pips", ""),
            "gate_passed": latest_success.get("gate_passed", ""),
        },
        "external_baseline_lanes": EXTERNAL_BASELINE_LANES,
        "next_actions": [
            "Keep trainer consuming queued walk-forward screen specs, with scout lead-time and return-curve phases first.",
            "Escalate high-scoring non-scout screen winners into longer robust validation before any promotion decision.",
            "Promote only candidates that pass validation/shadow/canary gates.",
            "Refresh ensemble shadow evidence from validated members; keep it non-executing until it beats active production under comparable validation.",
            "Build separate evaluators for external baseline lanes rather than forcing them into tabular trainer.",
        ],
    }
    atomic_write_json(PLAN_LATEST_PATH, plan)
    append_jsonl(PLAN_HISTORY_PATH, plan)
    return plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the executable model-space plan for the FX trainer")
    parser.add_argument("--apply", action="store_true", help="Append selected specs to the trainer queue")
    parser.add_argument("--max-seed", type=int, default=80, help="Maximum specs to seed this run")
    parser.add_argument("--min-total-pending", type=int, default=40, help="Seed when total unique pending specs are below this")
    parser.add_argument("--phase", default="", help="Force seeding/evaluation for one phase_id")
    parser.add_argument(
        "--deployment-lane",
        default="",
        help="Restrict agenda seeding/reporting to one deployment_lane value",
    )
    parser.add_argument(
        "--account-focus",
        default="",
        help="Restrict agenda seeding/reporting to one account_focus value",
    )
    parser.add_argument("--no-new-estimators", action="store_true", help="Exclude ExtraTrees/HistGradient from the plan")
    parser.add_argument(
        "--validation-followups",
        action="store_true",
        help="Select robust validation jobs from successful non-scout screen winners",
    )
    parser.add_argument(
        "--validation-followups-only",
        action="store_true",
        help="Only seed robust validation follow-ups, not broad agenda specs",
    )
    args = parser.parse_args()
    plan = run_plan(
        apply=bool(args.apply),
        max_seed=max(0, args.max_seed),
        include_new_estimators=not bool(args.no_new_estimators),
        min_total_pending=max(0, args.min_total_pending),
        phase_id=args.phase or None,
        validation_followups=bool(args.validation_followups or args.validation_followups_only),
        validation_followups_only=bool(args.validation_followups_only),
        deployment_lane=args.deployment_lane or None,
        account_focus=args.account_focus or None,
    )
    print(json.dumps({
        "updated_utc": plan["updated_utc"],
        "applied": plan["applied"],
        "scope": plan["scope"],
        "active_phase": plan["active_phase"],
        "seeded_this_run": plan["seeded_this_run"],
        "seeded_by_phase": plan["seeded_by_phase"],
        "validation_followups_selected": plan["validation_followups_selected"],
        "total_pending_unique": plan["total_pending_unique"],
        "total_queue_rows": plan["total_queue_rows"],
        "latest_success": plan["latest_success"],
        "plan_latest": str(PLAN_LATEST_PATH),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
