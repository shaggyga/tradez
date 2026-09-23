#!/usr/bin/env python3
"""Build the frozen seven-Wednesday, research-only all-68 policy expansion."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping

import oanda_sequential_all68_policy_challenger as hardened
from src.forex_system.research.sequential_all68_policy_challenger_v1 import (
    ARM_NAMES,
    SAFETY,
    apply_action,
    build_calibration,
    calibrate_candidates,
    candidate_roundtrip_observation,
    choose_cost_hurdle,
    choose_explicit_hold_vs_switch,
    choose_no_trade,
    eligible_2x,
    factor_consistent_candidates,
    flat_state,
    liquidation_equity,
    seal_row,
    stable_hash,
)
from src.forex_system.research.sequential_all68_policy_expansion_v1 import (
    ordered_session_keys,
    strictly_prior_session_map,
    validate_session_roles,
)


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_all68_policy_expansion_v1.json"
SUPPORT_RUNNER = ROOT / "oanda_sequential_all68_policy_challenger.py"
POLICY_CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_policy_challenger_v1.py"
EXPANSION_CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_policy_expansion_v1.py"
FOUNDATION_CORE = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
VERIFIER = ROOT / "oanda_sequential_all68_policy_expansion_verifier.py"
VERIFIER_FOUNDATION = ROOT / "oanda_sequential_all68_policy_challenger_verifier.py"
ALLOWED_LEDGER_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers"
ARTIFACT_TIME_CONTRACT = {
    "kind": "maximum_bound_source_feedback_epoch",
    "timezone": "UTC",
    "wall_clock_independent": True,
}


def ordered_sessions(manifest: Mapping[str, Any]) -> list[str]:
    return ordered_session_keys(manifest)


def training_session_map(manifest: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, list[str]]:
    """Map each application session to every strictly earlier completed session."""
    return strictly_prior_session_map(manifest)


def validate_config(config: Mapping[str, Any], manifest: Mapping[str, Any] | None = None) -> None:
    hardened.validate_config(config)
    roles = config["session_roles"]
    if roles.get("all_sessions_are_historical_training") is not True:
        raise ValueError("expansion sessions must remain historical training")
    if manifest is not None:
        validate_session_roles(manifest, roles)


def render_report(state: Mapping[str, Any]) -> str:
    lines = [
        "# Sequential All-68 Policy Expansion V1", "",
        "Status: seven-session historical training comparison; research-only, nonexecuting, proof-ineligible", "",
        f"- Cohort: `{state['cohort_id']}`",
        f"- Bound replay: `{state['source_binding']['cohort_id']}`",
        f"- Bound source pack: `{state['source_binding']['source_pack_id']}`",
        f"- Sessions / global market repetitions: **{state['session_count']} / {state['market_repetition_count']}**",
        f"- Arm decisions: **{state['arm_decision_count']}** (zero extra repetitions)",
        f"- Prior discovery sessions: `{', '.join(state['session_roles']['prior_discovery_session_keys'])}`", "",
        "## Strictly prior-session training map", "",
    ]
    for application, prior in state["application_training_sessions"].items():
        lines.append(f"- `{application}` <- `{', '.join(prior) if prior else 'none; abstain'}`")
    lines += ["", "## Matched arm results", "", "| Arm | Actions | Legs | Result (pips) | Versus V1 | Terminal |", "|---|---:|---:|---:|---:|---|"]
    baseline = float(state["arm_summaries"]["v1_baseline_reference"]["realized_pips"])
    for arm in state["arms"]:
        summary = state["arm_summaries"][arm]
        lines.append(
            f"| {arm} | {summary['non_wait_action_count']} | {summary['execution_leg_count']} | "
            f"{summary['realized_pips']:+.4f} | {summary['realized_pips'] - baseline:+.4f} | "
            f"{'flat' if summary['terminal_flat'] else 'censored'} |"
        )
    lines += [
        "", "## Session concentration check", "",
        "| Arm | Positive sessions | Best session | Best session pips | Result without best |", "|---|---:|---|---:|---:|",
    ]
    for arm in state["arms"]:
        summary = state["arm_summaries"][arm]
        lines.append(
            f"| {arm} | {summary['positive_session_count']} / {state['session_count']} | "
            f"{summary['best_session_key'] or 'none'} | {summary['best_session_pips']:+.4f} | "
            f"{summary['result_excluding_best_session_pips']:+.4f} |"
        )
    lines += [
        "", "All seven Wednesdays are historical training. The August 26 session remains prior discovery, not confirmation.",
        "A positive pooled training result is not stable edge when removing its best Wednesday makes it negative.",
        "The OOF arm trains only on completed earlier listed Wednesdays; its first application session must abstain.",
        "The 2x cost hurdle is an abstention/turnover hypothesis and does not imply positive conditional expectancy.",
        "One scheduled global clock is one repetition. Arms, candidates, pairs, and reruns add zero repetitions.",
        "No arm can prove, promote, authorize, publish a signal, access an account, or place an order. Supported decision: no_trade.",
    ]
    return "\n".join(lines) + "\n"


def run(config_path: Path = CONFIG, output_dir: Path | None = None) -> tuple[dict[str, Any], Path, Path]:
    if hardened.is_link_or_reparse(config_path):
        raise ValueError("linked config rejected")
    hardened.reject_link_chain(config_path)
    config_path = config_path.resolve()
    config = hardened.read_json(config_path)
    validate_config(config)
    source_root, source_state, manifest, source_binding, source_data = hardened.source_contract(config)
    validate_config(config, manifest)
    session_training = training_session_map(manifest, config)
    sessions = ordered_sessions(manifest)

    material = {
        "schema_version": 1,
        "config_sha256": hardened.file_sha(config_path),
        "runner_sha256": hardened.file_sha(Path(__file__).resolve()),
        "hardened_support_runner_sha256": hardened.file_sha(SUPPORT_RUNNER),
        "policy_core_sha256": hardened.file_sha(POLICY_CORE),
        "expansion_core_sha256": hardened.file_sha(EXPANSION_CORE),
        "foundation_core_sha256": hardened.file_sha(FOUNDATION_CORE),
        "verifier_sha256": hardened.file_sha(VERIFIER),
        "independent_verifier_foundation_sha256": hardened.file_sha(VERIFIER_FOUNDATION),
        "source_binding": source_binding,
        "arms": list(ARM_NAMES),
        "costs": config["costs"],
        "policy": config["policy"],
        "oof_calibration": config["oof_calibration"],
        "session_roles": config["session_roles"],
        "application_training_sessions": session_training,
        "dataset_limits": config["dataset_limits"],
        "independence": config["independence"],
        "artifact_time_contract": ARTIFACT_TIME_CONTRACT,
        "safety": SAFETY,
    }
    material_sha = stable_hash(material)
    cohort_id = config["experiment_key"] + "." + material_sha[:20]
    if output_dir is not None:
        hardened.reject_link_chain(output_dir)
    artifact_root = output_dir.resolve() if output_dir else hardened.safe_relative(ROOT, config["storage"]["artifact_relative_root"])
    if output_dir is None and ALLOWED_LEDGER_ROOT.resolve() not in artifact_root.parents:
        raise ValueError("artifact root outside research ledgers")
    cohort_root = hardened.safe_relative(artifact_root, str(Path("cohorts") / cohort_id))
    cohort_root.mkdir(parents=True, exist_ok=True)
    for name in ("state.json", "verifier_receipt.json", "source_pack_manifest.json", "source_pack_clean_verifier_receipt.json"):
        source_path = hardened.safe_relative(source_root, name)
        hardened.require_regular_file(source_path, maximum_bytes=16 * 1024 * 1024)
        hardened.immutable_bytes(hardened.safe_relative(cohort_root, f"bound_{name}"), source_path.read_bytes())

    source_clocks = source_data["clocks"]
    source_decisions = {row["clock_id"]: row for row in source_data["decisions"]}
    source_feedback = {row["clock_id"]: row for row in source_data["feedback"]}
    expected_clock_ids = {row["clock_id"] for row in source_clocks}
    if len(source_clocks) != 336 or set(source_decisions) != expected_clock_ids or set(source_feedback) != expected_clock_ids:
        raise ValueError("bound replay clock completeness failure")
    if int(source_state["global_clock_count"]) != len(source_clocks) or int(source_state["session_count"]) != len(sessions):
        raise ValueError("bound replay declared counts changed")
    markets = hardened.load_markets(manifest, config)

    prior_discovery = set(config["session_roles"]["prior_discovery_session_keys"])
    clock_rows: list[dict[str, Any]] = []
    for source_clock in source_clocks:
        session = str(source_clock["session_key"])
        clock_rows.append(seal_row({
            "clock_id": "policyexpclock_" + stable_hash(cohort_id, source_clock["clock_id"])[:28],
            "cohort_id": cohort_id,
            "source_clock_id": source_clock["clock_id"],
            "source_clock_row_sha256": source_clock["row_sha256"],
            "sequence_no": source_clock["sequence_no"],
            "session_clock_ordinal": source_clock.get("session_clock_ordinal"),
            "session_key": session,
            "decision_epoch": source_clock["decision_epoch"],
            "execution_epoch": source_clock["execution_epoch"],
            "feedback_epoch": source_clock["feedback_epoch"],
            "session_end_epoch": source_clock["session_end_epoch"],
            "terminal_clock": source_clock["terminal_clock"],
            "historical_training": True,
            "prior_discovery": session in prior_discovery,
            "source_candidate_count": len(source_decisions[source_clock["clock_id"]]["candidate_rank"]),
            "source_causal_candidate_count": source_decisions[source_clock["clock_id"]]["causal_candidate_count"],
            "source_feedback_status": source_feedback[source_clock["clock_id"]]["status"],
            "counts_as_market_repetition": 1,
            "counts_as_regime_repetition": 0,
            "proof_eligible": False,
            "execution_eligible": False,
        }))

    training_rows: list[dict[str, Any]] = []
    raw_training: dict[str, list[dict[str, Any]]] = {}
    slippage = float(config["costs"]["slippage_per_execution_leg_pips"])
    for clock in source_clocks:
        source_decision = source_decisions[clock["clock_id"]]
        session = str(clock["session_key"])
        for candidate in source_decision["candidate_rank"]:
            observation = candidate_roundtrip_observation(
                candidate,
                markets[session],
                execution_epoch=int(clock["execution_epoch"]),
                feedback_epoch=int(clock["feedback_epoch"]),
                slippage=slippage,
            )
            raw_training.setdefault(session, []).append(observation)
            training_rows.append(seal_row({
                "training_observation_id": "policyexptrain_" + stable_hash(cohort_id, clock["clock_id"], candidate["snapshot_id"])[:28],
                "cohort_id": cohort_id,
                "source_clock_id": clock["clock_id"],
                "session_key": session,
                "historical_training": True,
                "prior_discovery": session in prior_discovery,
                **observation,
                "counts_as_market_repetition": 0,
                "counts_as_regime_repetition": 0,
                "proof_eligible": False,
            }))

    calibrations: dict[str, dict[str, Any]] = {}
    calibration_rows: list[dict[str, Any]] = []
    for session in sessions:
        training_sessions = session_training[session]
        observations = [row for prior in training_sessions for row in raw_training.get(prior, [])]
        cells = build_calibration(observations, config["oof_calibration"])
        calibrations[session] = cells
        for bucket in sorted(set(cells) | {"liquid", "normal", "elevated"}):
            cell = cells.get(bucket, {
                "observation_count": 0,
                "mean_after_cost_pips": None,
                "sample_standard_deviation_pips": None,
                "lower_bound_after_cost_pips": None,
                "sufficient_observations": False,
                "eligible": False,
            })
            calibration_rows.append(seal_row({
                "calibration_id": "policyexpcal_" + stable_hash(cohort_id, session, bucket, training_sessions, cell)[:28],
                "cohort_id": cohort_id,
                "application_session_key": session,
                "training_session_keys": training_sessions,
                "liquidity_bucket": bucket,
                **cell,
                "same_session_training": session in training_sessions,
                "future_session_training": any(sessions.index(item) >= sessions.index(session) for item in training_sessions),
                "historical_training": True,
                "prior_discovery_application": session in prior_discovery,
                "counts_as_market_repetition": 0,
                "counts_as_regime_repetition": 0,
                "proof_eligible": False,
            }))

    arm_rows: list[dict[str, Any]] = []
    terminal_rows: list[dict[str, Any]] = []
    arm_summaries: dict[str, dict[str, Any]] = {}
    source_terminals = {row["session_key"]: row for row in source_data["terminals"]}
    for arm in ARM_NAMES:
        state = flat_state()
        actions: dict[str, int] = {}
        failures: dict[str, int] = {}
        session_realized: dict[str, float] = {}
        prior_terminal_realized = 0.0
        legs = 0
        for clock, source_clock in zip(clock_rows, source_clocks):
            session = str(clock["session_key"])
            source_decision = source_decisions[source_clock["clock_id"]]
            source_fb = source_feedback[source_clock["clock_id"]]
            pre_state = json.loads(json.dumps(state))
            candidates = source_decision["candidate_rank"]
            policy_diagnostics: dict[str, Any]
            if arm == "v1_baseline_reference":
                decision_payload = source_decision["decision"]
                execution = source_decision["execution"]
                state = source_decision["state_after"]
                feedback = {"status": source_fb["status"], "liquidation_equity_pips": source_fb["liquidation_equity_pips"]}
                policy_diagnostics = {
                    "reference_only": True,
                    "source_decision_id": source_decision["decision_id"],
                    "source_decision_row_sha256": source_decision["row_sha256"],
                    "source_feedback_row_sha256": source_fb["row_sha256"],
                }
            else:
                if arm == "no_trade":
                    decision_payload, policy_diagnostics = choose_no_trade(state, terminal_clock=bool(clock["terminal_clock"]))
                elif arm == "cost_hurdle_2x":
                    decision_payload, policy_diagnostics = choose_cost_hurdle(
                        state, candidates, decision_epoch=int(clock["decision_epoch"]),
                        terminal_clock=bool(clock["terminal_clock"]), policy=config["policy"],
                    )
                elif arm == "explicit_hold_vs_switch_2x":
                    decision_payload, policy_diagnostics = choose_explicit_hold_vs_switch(
                        state, candidates, eligible_2x(candidates, config["policy"]["minimum_entry_score_cost_ratio"]),
                        decision_epoch=int(clock["decision_epoch"]), terminal_clock=bool(clock["terminal_clock"]),
                        policy=config["policy"], costs=config["costs"], rationale_prefix="explicit hold-vs-switch 2x",
                    )
                elif arm == "factor_conflict_suppressed_2x":
                    eligible = eligible_2x(candidates, config["policy"]["minimum_entry_score_cost_ratio"])
                    accepted, votes, rejected = factor_consistent_candidates(
                        eligible, tie_epsilon=float(config["policy"]["factor_vote_tie_epsilon"]),
                    )
                    decision_payload, policy_diagnostics = choose_explicit_hold_vs_switch(
                        state, candidates, accepted, decision_epoch=int(clock["decision_epoch"]),
                        terminal_clock=bool(clock["terminal_clock"]), policy=config["policy"], costs=config["costs"],
                        rationale_prefix="factor-conflict-suppressed 2x",
                    )
                    policy_diagnostics.update({"factor_vote_scores": votes, "factor_conflict_rejections": rejected})
                else:
                    accepted, rejected = calibrate_candidates(
                        candidates, calibrations[session], float(config["policy"]["minimum_entry_score_cost_ratio"]),
                    )
                    decision_payload, policy_diagnostics = choose_explicit_hold_vs_switch(
                        state, candidates, accepted, decision_epoch=int(clock["decision_epoch"]),
                        terminal_clock=bool(clock["terminal_clock"]), policy=config["policy"], costs=config["costs"],
                        rationale_prefix="strict-prior-Wednesday OOF remaining-move calibration",
                    )
                    policy_diagnostics.update({
                        "training_session_keys": session_training[session],
                        "calibration_cells": calibrations[session],
                        "calibration_rejections": rejected,
                    })
                state, execution = apply_action(
                    state, decision_payload, markets[session], execution_epoch=int(clock["execution_epoch"]),
                    costs=config["costs"], clock_id=str(clock["clock_id"]), arm=arm,
                )
                status, equity = liquidation_equity(state, markets[session], int(clock["feedback_epoch"]), slippage)
                feedback = {"status": status, "liquidation_equity_pips": equity}
            actions[decision_payload["action"]] = actions.get(decision_payload["action"], 0) + 1
            legs += len(execution["legs"])
            if execution["status"] != "applied":
                reason = execution["rejection_reason"] or execution["status"]
                failures[reason] = failures.get(reason, 0) + 1
            arm_rows.append(seal_row({
                "arm_decision_id": "policyexpdecision_" + stable_hash(cohort_id, arm, clock["clock_id"], pre_state, decision_payload)[:28],
                "cohort_id": cohort_id,
                "arm": arm,
                "clock_id": clock["clock_id"],
                "source_clock_id": source_clock["clock_id"],
                "sequence_no": clock["sequence_no"],
                "session_key": session,
                "decision_epoch": clock["decision_epoch"],
                "historical_training": True,
                "prior_discovery": session in prior_discovery,
                "state_before": pre_state,
                "candidate_count": len(candidates),
                "source_candidate_root_sha256": stable_hash(candidates),
                "decision": decision_payload,
                "policy_diagnostics": policy_diagnostics,
                "execution": execution,
                "state_after": state,
                "feedback": feedback,
                "counts_as_market_repetition": 0,
                "counts_as_regime_repetition": 0,
                "proof_eligible": False,
                "execution_eligible": False,
            }))
            if clock["terminal_clock"]:
                retry = {"status": "not_needed", "legs": [], "state_after": state}
                if state.get("position") is not None and arm != "v1_baseline_reference":
                    retry_decision = {
                        "action": "exit", "instrument": state["position"]["instrument"], "side": None,
                        "units": 0, "confidence": None, "expected_move_pips": None, "horizon_min": None,
                        "entry_condition": "", "invalidation": "", "rationale": "exact session-end terminal retry",
                        "candidate_snapshot_id": None, "branch_label": "terminal_retry",
                    }
                    state, retry = apply_action(
                        state, retry_decision, markets[session], execution_epoch=int(clock["session_end_epoch"]),
                        costs=config["costs"], clock_id=str(clock["clock_id"]), arm=arm,
                    )
                    legs += len(retry["legs"])
                elif arm == "v1_baseline_reference":
                    retry = source_terminals[session]["retry"]
                    state = source_terminals[session]["state_after"]
                session_delta = float(state["realized_pips"]) - prior_terminal_realized
                session_realized[session] = session_delta
                prior_terminal_realized = float(state["realized_pips"])
                terminal_rows.append(seal_row({
                    "terminal_id": "policyexpterminal_" + stable_hash(cohort_id, arm, session)[:28],
                    "cohort_id": cohort_id,
                    "arm": arm,
                    "session_key": session,
                    "terminal_epoch": clock["session_end_epoch"],
                    "historical_training": True,
                    "prior_discovery": session in prior_discovery,
                    "session_realized_pips": session_delta,
                    "retry": retry,
                    "terminal_flat": state.get("position") is None,
                    "censored": state.get("position") is not None,
                    "state_after": state,
                    "counts_as_market_repetition": 0,
                    "counts_as_regime_repetition": 0,
                    "proof_eligible": False,
                }))
        positive_sessions = {key: value for key, value in session_realized.items() if value > 0.0}
        best_session = max(positive_sessions, key=positive_sessions.get) if positive_sessions else None
        best_session_pips = positive_sessions[best_session] if best_session is not None else 0.0
        arm_summaries[arm] = {
            "action_counts": actions,
            "non_wait_action_count": sum(value for key, value in actions.items() if key != "wait"),
            "execution_leg_count": legs,
            "failure_counts": failures,
            "realized_pips": float(state["realized_pips"]),
            "terminal_flat": state.get("position") is None,
            "session_realized_pips": session_realized,
            "positive_session_count": len(positive_sessions),
            "best_session_key": best_session,
            "best_session_pips": best_session_pips,
            "result_excluding_best_session_pips": float(state["realized_pips"]) - best_session_pips,
        }

    datasets = {
        "clocks": hardened.write_dataset(hardened.safe_relative(cohort_root, "global_clocks.jsonl.gz"), clock_rows, config["dataset_limits"]),
        "arm_decisions": hardened.write_dataset(hardened.safe_relative(cohort_root, "arm_decisions.jsonl.gz"), arm_rows, config["dataset_limits"]),
        "terminals": hardened.write_dataset(hardened.safe_relative(cohort_root, "arm_session_terminals.jsonl.gz"), terminal_rows, config["dataset_limits"]),
        "oof_training": hardened.write_dataset(hardened.safe_relative(cohort_root, "oof_training_observations.jsonl.gz"), training_rows, config["dataset_limits"]),
        "oof_calibrations": hardened.write_dataset(hardened.safe_relative(cohort_root, "oof_calibrations.jsonl.gz"), calibration_rows, config["dataset_limits"]),
    }
    generated_utc = datetime.fromtimestamp(max(int(row["feedback_epoch"]) for row in clock_rows), tz=timezone.utc).isoformat()
    state_doc = {
        **SAFETY,
        "schema_version": 1,
        "generated_utc": generated_utc,
        "cohort_id": cohort_id,
        "material_sha256": material_sha,
        "material_contract": material,
        "artifact_root": str(cohort_root),
        "source_binding": source_binding,
        "evidence_role": config["evidence_role"],
        "session_roles": config["session_roles"],
        "application_training_sessions": session_training,
        "arms": list(ARM_NAMES),
        "session_count": len(sessions),
        "global_clock_count": len(clock_rows),
        "market_repetition_count": len(clock_rows),
        "source_pair_context_count": int(source_state["pair_context_count"]),
        "source_candidate_context_count": int(source_state["candidate_context_count"]),
        "source_context_availability_counts": source_state["context_availability_counts"],
        "arm_decision_count": len(arm_rows),
        "arm_decisions_count_as_rep": 0,
        "training_observation_count": len(training_rows),
        "training_observations_count_as_rep": 0,
        "calibration_cell_count": len(calibration_rows),
        "calibrations_count_as_rep": 0,
        "independent_regime_count": None,
        "arm_summaries": arm_summaries,
        "paired_pips_vs_no_trade": {
            arm: float(summary["realized_pips"]) - float(arm_summaries["no_trade"]["realized_pips"])
            for arm, summary in arm_summaries.items()
        },
        "paired_pips_vs_v1": {
            arm: float(summary["realized_pips"]) - float(arm_summaries["v1_baseline_reference"]["realized_pips"])
            for arm, summary in arm_summaries.items()
        },
        "terminal_flat_all_arms": all(summary["terminal_flat"] for summary in arm_summaries.values()),
        "datasets": datasets,
    }
    state_path = hardened.safe_relative(cohort_root, "state.json")
    report_path = hardened.safe_relative(cohort_root, "report.md")
    material_path = hardened.safe_relative(cohort_root, "material_contract.json")
    hardened.immutable_bytes(state_path, hardened.json_bytes(state_doc))
    hardened.immutable_bytes(report_path, render_report(state_doc).encode("utf-8"))
    hardened.immutable_bytes(material_path, hardened.json_bytes(material))

    receipt_path = hardened.safe_relative(cohort_root, "verifier_receipt.json")
    completed = subprocess.run(
        [sys.executable, str(VERIFIER), "--config", str(config_path), "--state", str(state_path),
         "--material", str(material_path), "--output", str(receipt_path)],
        cwd=ROOT, text=True, capture_output=True,
    )
    if completed.returncode != 0:
        raise ValueError("independent policy-expansion verification failed: " + (completed.stdout + completed.stderr).strip())
    receipt = hardened.read_json(receipt_path)
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("independent policy-expansion verification failed closed")

    hardened.atomic_bytes(hardened.safe_relative(artifact_root, config["storage"]["state_name"]), state_path.read_bytes())
    hardened.atomic_bytes(hardened.safe_relative(artifact_root, config["storage"]["report_name"]), report_path.read_bytes())
    hardened.atomic_bytes(hardened.safe_relative(artifact_root, config["storage"]["verifier_name"]), receipt_path.read_bytes())
    return state_doc, state_path, receipt_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    args = parser.parse_args()
    state, state_path, receipt_path = run(args.config)
    print(json.dumps({
        "cohort_id": state["cohort_id"],
        "results": {arm: summary["realized_pips"] for arm, summary in state["arm_summaries"].items()},
        "verified": True,
    }, sort_keys=True))
    print(state_path)
    print(receipt_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
