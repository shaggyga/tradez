#!/usr/bin/env python3
"""Independent verifier for the seven-Wednesday all-68 policy expansion.

This runnable verifier imports no producer or policy core. It reuses only the
separately implemented challenger-verifier primitives, whose exact source hash
is itself frozen into the expansion material contract.
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Mapping

import oanda_sequential_all68_policy_challenger_verifier as independent


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_all68_policy_expansion_v1.json"
PRODUCER_PATH = ROOT / "oanda_sequential_all68_policy_expansion.py"
HARDENED_SUPPORT_RUNNER = ROOT / "oanda_sequential_all68_policy_challenger.py"
POLICY_CORE_PATH = ROOT / "src" / "forex_system" / "research" / "sequential_all68_policy_challenger_v1.py"
EXPANSION_CORE_PATH = ROOT / "src" / "forex_system" / "research" / "sequential_all68_policy_expansion_v1.py"
FOUNDATION_CORE_PATH = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
VERIFIER_FOUNDATION = ROOT / "oanda_sequential_all68_policy_challenger_verifier.py"
SAFETY = {
    "research_only": True,
    "execution_eligible": False,
    "proof_eligible": False,
    "can_promote": False,
    "can_place_orders": False,
    "can_authorize": False,
    "broker_access": False,
    "account_access": False,
    "signal_feed_write": False,
    "lifecycle_write": False,
    "supported_decision": "no_trade",
}
ARMS = (
    "no_trade",
    "v1_baseline_reference",
    "cost_hurdle_2x",
    "explicit_hold_vs_switch_2x",
    "factor_conflict_suppressed_2x",
    "oof_remaining_move_calibrated_2x",
)
ARTIFACT_TIME_CONTRACT = {
    "kind": "maximum_bound_source_feedback_epoch",
    "timezone": "UTC",
    "wall_clock_independent": True,
}


def ordered_sessions(manifest: Mapping[str, Any]) -> list[str]:
    rows = list(manifest["sessions"])
    starts = [str(row["start_utc"]) for row in rows]
    sessions = [str(row["session_key"]) for row in rows]
    if starts != sorted(starts) or len(starts) != len(set(starts)) or len(sessions) != len(set(sessions)):
        raise ValueError("source sessions are not uniquely time ordered")
    return sessions


def training_map(manifest: Mapping[str, Any]) -> dict[str, list[str]]:
    sessions = ordered_sessions(manifest)
    return {session: list(sessions[:index]) for index, session in enumerate(sessions)}


def _write_receipt(output_path: Path, receipt: Mapping[str, Any]) -> None:
    payload = (json.dumps(receipt, indent=2, sort_keys=True) + "\n").encode("utf-8")
    independent.reject_link_chain(output_path.parent)
    if output_path.exists() and independent.is_link_or_reparse(output_path):
        raise ValueError("linked verifier output rejected")
    if output_path.exists():
        if output_path.read_bytes() != payload:
            raise ValueError(f"immutable artifact conflict: {output_path}")
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.parent / f".tmp-{os.getpid()}-{independent.stable_hash(str(output_path))[:12]}"
    temporary.write_bytes(payload)
    os.replace(temporary, output_path)


def verify(config_path: Path, state_path: Path, material_path: Path, output_path: Path) -> dict[str, Any]:
    failures: list[str] = []
    checks: dict[str, Any] = {}
    try:
        for path in (config_path, state_path, material_path):
            independent.reject_link_chain(path)
        independent.reject_link_chain(output_path.parent)
        config = independent.read_json(config_path)
        state = independent.read_json(state_path)
        supplied_material = independent.read_json(material_path)
        source_state, manifest, source_data, source_binding = independent.source_contract(config)
        sessions = ordered_sessions(manifest)
        session_training = training_map(manifest)
        roles = config["session_roles"]
        if roles.get("all_sessions_are_historical_training") is not True:
            raise ValueError("sessions not frozen as historical training")
        if list(roles["prior_discovery_session_keys"]) != ["20260826_wed_overlap_prior_discovery"] or list(roles["prior_discovery_session_keys"]) != [sessions[-1]]:
            raise ValueError("prior-discovery role mismatch")
        if session_training[sessions[0]] != []:
            raise ValueError("first application session has training")
        for index, session in enumerate(sessions):
            if session_training[session] != sessions[:index]:
                raise ValueError("noncausal session training map")
        prior_discovery = set(roles["prior_discovery_session_keys"])

        expected_material = {
            "schema_version": 1,
            "config_sha256": independent.file_sha(config_path),
            "runner_sha256": independent.file_sha(PRODUCER_PATH),
            "hardened_support_runner_sha256": independent.file_sha(HARDENED_SUPPORT_RUNNER),
            "policy_core_sha256": independent.file_sha(POLICY_CORE_PATH),
            "expansion_core_sha256": independent.file_sha(EXPANSION_CORE_PATH),
            "foundation_core_sha256": independent.file_sha(FOUNDATION_CORE_PATH),
            "verifier_sha256": independent.file_sha(Path(__file__).resolve()),
            "independent_verifier_foundation_sha256": independent.file_sha(VERIFIER_FOUNDATION),
            "source_binding": source_binding,
            "arms": list(ARMS),
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
        if supplied_material != expected_material:
            failures.append("material_contract")
        expected_material_sha = independent.stable_hash(expected_material)
        expected_cohort = config["experiment_key"] + "." + expected_material_sha[:20]
        if state.get("cohort_id") != expected_cohort or state.get("material_sha256") != expected_material_sha:
            failures.append("cohort_identity")
        for key, expected in SAFETY.items():
            if state.get(key) != expected:
                failures.append(f"state_safety:{key}")
        if tuple(state.get("arms", ())) != ARMS:
            failures.append("arm_contract")
        if state_path.parent.resolve().name != expected_cohort:
            failures.append("cohort_path")
        expected_names = {"clocks", "arm_decisions", "terminals", "oof_training", "oof_calibrations"}
        if set(state.get("datasets", {})) != expected_names:
            failures.append("dataset_manifest")
        limits = config["dataset_limits"]
        if not (0 < int(limits["maximum_gzip_bytes"]) <= int(limits["maximum_raw_bytes"])):
            raise ValueError("invalid frozen dataset byte limits")
        if not (0 < int(limits["maximum_row_count"]) <= 100_000):
            raise ValueError("invalid frozen dataset row limit")
        root = state_path.parent.resolve()
        output = {key: independent.load_dataset(root, spec, limits) for key, spec in state["datasets"].items()}
        quotes = independent.load_quotes(manifest, config)
        src_clocks = source_data["clocks"]
        src_decisions = {row["clock_id"]: row for row in source_data["decisions"]}
        src_feedback = {row["clock_id"]: row for row in source_data["feedback"]}
        clock_ids = {row["clock_id"] for row in src_clocks}
        if len(src_clocks) != 336 or set(src_decisions) != clock_ids or set(src_feedback) != clock_ids:
            raise ValueError("source clock completeness failure")

        expected_clocks: list[dict[str, Any]] = []
        for src in src_clocks:
            session = str(src["session_key"])
            source_decision = src_decisions[src["clock_id"]]
            source_feedback = src_feedback[src["clock_id"]]
            expected_clocks.append(independent.seal({
                "clock_id": "policyexpclock_" + independent.stable_hash(expected_cohort, src["clock_id"])[:28],
                "cohort_id": expected_cohort,
                "source_clock_id": src["clock_id"],
                "source_clock_row_sha256": src["row_sha256"],
                "sequence_no": src["sequence_no"],
                "session_clock_ordinal": src.get("session_clock_ordinal"),
                "session_key": session,
                "decision_epoch": src["decision_epoch"],
                "execution_epoch": src["execution_epoch"],
                "feedback_epoch": src["feedback_epoch"],
                "session_end_epoch": src["session_end_epoch"],
                "terminal_clock": src["terminal_clock"],
                "historical_training": True,
                "prior_discovery": session in prior_discovery,
                "source_candidate_count": len(source_decision["candidate_rank"]),
                "source_causal_candidate_count": source_decision["causal_candidate_count"],
                "source_feedback_status": source_feedback["status"],
                "counts_as_market_repetition": 1,
                "counts_as_regime_repetition": 0,
                "proof_eligible": False,
                "execution_eligible": False,
            }))
        if output["clocks"] != expected_clocks:
            failures.append("clock_reconstruction")

        slip = float(config["costs"]["slippage_per_execution_leg_pips"])
        raw_training: dict[str, list[dict[str, Any]]] = {}
        expected_training: list[dict[str, Any]] = []
        for clock in src_clocks:
            session = str(clock["session_key"])
            for candidate in src_decisions[clock["clock_id"]]["candidate_rank"]:
                obs = independent.observation(candidate, quotes[session], clock, slip)
                raw_training.setdefault(session, []).append(obs)
                expected_training.append(independent.seal({
                    "training_observation_id": "policyexptrain_" + independent.stable_hash(expected_cohort, clock["clock_id"], candidate["snapshot_id"])[:28],
                    "cohort_id": expected_cohort,
                    "source_clock_id": clock["clock_id"],
                    "session_key": session,
                    "historical_training": True,
                    "prior_discovery": session in prior_discovery,
                    **obs,
                    "counts_as_market_repetition": 0,
                    "counts_as_regime_repetition": 0,
                    "proof_eligible": False,
                }))
        if output["oof_training"] != expected_training:
            failures.append("oof_training_reconstruction")

        session_cells: dict[str, dict[str, Any]] = {}
        expected_cal_rows: list[dict[str, Any]] = []
        for session in sessions:
            prior = session_training[session]
            observations = [row for key in prior for row in raw_training.get(key, [])]
            cells = independent.calibrations(observations, config["oof_calibration"])
            session_cells[session] = cells
            for bucket in sorted(set(cells) | {"liquid", "normal", "elevated"}):
                cell = cells.get(bucket, {
                    "observation_count": 0,
                    "mean_after_cost_pips": None,
                    "sample_standard_deviation_pips": None,
                    "lower_bound_after_cost_pips": None,
                    "sufficient_observations": False,
                    "eligible": False,
                })
                expected_cal_rows.append(independent.seal({
                    "calibration_id": "policyexpcal_" + independent.stable_hash(expected_cohort, session, bucket, prior, cell)[:28],
                    "cohort_id": expected_cohort,
                    "application_session_key": session,
                    "training_session_keys": prior,
                    "liquidity_bucket": bucket,
                    **cell,
                    "same_session_training": session in prior,
                    "future_session_training": any(sessions.index(item) >= sessions.index(session) for item in prior),
                    "historical_training": True,
                    "prior_discovery_application": session in prior_discovery,
                    "counts_as_market_repetition": 0,
                    "counts_as_regime_repetition": 0,
                    "proof_eligible": False,
                }))
        if output["oof_calibrations"] != expected_cal_rows:
            failures.append("oof_calibration_reconstruction")

        expected_arm_rows: list[dict[str, Any]] = []
        expected_terminals: list[dict[str, Any]] = []
        summaries: dict[str, dict[str, Any]] = {}
        src_terminals = {row["session_key"]: row for row in source_data["terminals"]}
        for arm in ARMS:
            state_chain = independent.flat_state()
            actions: dict[str, int] = {}
            error_counts: dict[str, int] = {}
            session_realized: dict[str, float] = {}
            prior_terminal_realized = 0.0
            legs = 0
            for clock, src_clock in zip(expected_clocks, src_clocks):
                session = str(clock["session_key"])
                src_decision = src_decisions[src_clock["clock_id"]]
                src_fb = src_feedback[src_clock["clock_id"]]
                before = copy.deepcopy(state_chain)
                candidates = src_decision["candidate_rank"]
                if arm == "v1_baseline_reference":
                    decision = src_decision["decision"]
                    execution = src_decision["execution"]
                    state_chain = src_decision["state_after"]
                    feedback = {"status": src_fb["status"], "liquidation_equity_pips": src_fb["liquidation_equity_pips"]}
                    diag = {
                        "reference_only": True,
                        "source_decision_id": src_decision["decision_id"],
                        "source_decision_row_sha256": src_decision["row_sha256"],
                        "source_feedback_row_sha256": src_fb["row_sha256"],
                    }
                else:
                    if arm == "no_trade":
                        if state_chain["position"] is not None:
                            raise ValueError("no-trade state not flat")
                        decision = independent.empty_decision(
                            "wait", "predeclared terminal boundary while flat" if clock["terminal_clock"] else "frozen no-trade comparator"
                        )
                        diag = {"policy_state": "flat_no_trade"}
                    elif arm == "cost_hurdle_2x":
                        decision, diag = independent.choose_cost(state_chain, candidates, clock, config)
                    elif arm == "explicit_hold_vs_switch_2x":
                        decision, diag = independent.choose_explicit(
                            state_chain, candidates, independent.eligible(candidates, 2.0), clock, config, "explicit hold-vs-switch 2x"
                        )
                    elif arm == "factor_conflict_suppressed_2x":
                        accepted, votes, rejected = independent.factor_filter(
                            independent.eligible(candidates, 2.0), float(config["policy"]["factor_vote_tie_epsilon"])
                        )
                        decision, diag = independent.choose_explicit(
                            state_chain, candidates, accepted, clock, config, "factor-conflict-suppressed 2x"
                        )
                        diag.update({"factor_vote_scores": votes, "factor_conflict_rejections": rejected})
                    else:
                        accepted, rejected = independent.calibrated_entries(candidates, session_cells[session], 2.0)
                        decision, diag = independent.choose_explicit(
                            state_chain, candidates, accepted, clock, config,
                            "strict-prior-Wednesday OOF remaining-move calibration",
                        )
                        diag.update({
                            "training_session_keys": session_training[session],
                            "calibration_cells": session_cells[session],
                            "calibration_rejections": rejected,
                        })
                    state_chain, execution = independent.apply(state_chain, decision, quotes[session], clock, config, arm)
                    status, liq = independent.equity(state_chain, quotes[session], int(clock["feedback_epoch"]), slip)
                    feedback = {"status": status, "liquidation_equity_pips": liq}
                actions[decision["action"]] = actions.get(decision["action"], 0) + 1
                legs += len(execution["legs"])
                if execution["status"] != "applied":
                    reason = execution["rejection_reason"] or execution["status"]
                    error_counts[reason] = error_counts.get(reason, 0) + 1
                expected_arm_rows.append(independent.seal({
                    "arm_decision_id": "policyexpdecision_" + independent.stable_hash(expected_cohort, arm, clock["clock_id"], before, decision)[:28],
                    "cohort_id": expected_cohort,
                    "arm": arm,
                    "clock_id": clock["clock_id"],
                    "source_clock_id": src_clock["clock_id"],
                    "sequence_no": clock["sequence_no"],
                    "session_key": session,
                    "decision_epoch": clock["decision_epoch"],
                    "historical_training": True,
                    "prior_discovery": session in prior_discovery,
                    "state_before": before,
                    "candidate_count": len(candidates),
                    "source_candidate_root_sha256": independent.stable_hash(candidates),
                    "decision": decision,
                    "policy_diagnostics": diag,
                    "execution": execution,
                    "state_after": state_chain,
                    "feedback": feedback,
                    "counts_as_market_repetition": 0,
                    "counts_as_regime_repetition": 0,
                    "proof_eligible": False,
                    "execution_eligible": False,
                }))
                if clock["terminal_clock"]:
                    retry = {"status": "not_needed", "legs": [], "state_after": state_chain}
                    if state_chain["position"] is not None and arm != "v1_baseline_reference":
                        retry_clock = dict(clock)
                        retry_clock["execution_epoch"] = clock["session_end_epoch"]
                        retry_decision = {
                            "action": "exit", "instrument": state_chain["position"]["instrument"], "side": None,
                            "units": 0, "confidence": None, "expected_move_pips": None, "horizon_min": None,
                            "entry_condition": "", "invalidation": "", "rationale": "exact session-end terminal retry",
                            "candidate_snapshot_id": None, "branch_label": "terminal_retry",
                        }
                        state_chain, retry = independent.apply(
                            state_chain, retry_decision, quotes[session], retry_clock, config, arm
                        )
                        legs += len(retry["legs"])
                    elif arm == "v1_baseline_reference":
                        retry = src_terminals[session]["retry"]
                        state_chain = src_terminals[session]["state_after"]
                    session_delta = float(state_chain["realized_pips"]) - prior_terminal_realized
                    session_realized[session] = session_delta
                    prior_terminal_realized = float(state_chain["realized_pips"])
                    expected_terminals.append(independent.seal({
                        "terminal_id": "policyexpterminal_" + independent.stable_hash(expected_cohort, arm, session)[:28],
                        "cohort_id": expected_cohort,
                        "arm": arm,
                        "session_key": session,
                        "terminal_epoch": clock["session_end_epoch"],
                        "historical_training": True,
                        "prior_discovery": session in prior_discovery,
                        "session_realized_pips": session_delta,
                        "retry": retry,
                        "terminal_flat": state_chain["position"] is None,
                        "censored": state_chain["position"] is not None,
                        "state_after": state_chain,
                        "counts_as_market_repetition": 0,
                        "counts_as_regime_repetition": 0,
                        "proof_eligible": False,
                    }))
            positive_sessions = {key: value for key, value in session_realized.items() if value > 0.0}
            best_session = max(positive_sessions, key=positive_sessions.get) if positive_sessions else None
            best_session_pips = positive_sessions[best_session] if best_session is not None else 0.0
            summaries[arm] = {
                "action_counts": actions,
                "non_wait_action_count": sum(value for key, value in actions.items() if key != "wait"),
                "execution_leg_count": legs,
                "failure_counts": error_counts,
                "realized_pips": float(state_chain["realized_pips"]),
                "terminal_flat": state_chain["position"] is None,
                "session_realized_pips": session_realized,
                "positive_session_count": len(positive_sessions),
                "best_session_key": best_session,
                "best_session_pips": best_session_pips,
                "result_excluding_best_session_pips": float(state_chain["realized_pips"]) - best_session_pips,
            }
        if output["arm_decisions"] != expected_arm_rows:
            failures.append("arm_decision_reconstruction")
        if output["terminals"] != expected_terminals:
            failures.append("terminal_reconstruction")
        if state.get("arm_summaries") != summaries:
            failures.append("arm_summary_reconstruction")

        expected_generated = datetime.fromtimestamp(
            max(int(row["feedback_epoch"]) for row in expected_clocks), tz=timezone.utc
        ).isoformat()
        checks = {
            "cohort_id": expected_cohort,
            "source_cohort_id": source_state["cohort_id"],
            "source_pack_id": manifest["pack_id"],
            "session_count": len(sessions),
            "global_clock_count": len(expected_clocks),
            "arm_decision_count": len(expected_arm_rows),
            "training_observation_count": len(expected_training),
            "calibration_cell_count": len(expected_cal_rows),
            "application_training_sessions": session_training,
            "arm_summaries": summaries,
        }
        expected_top = {
            "generated_utc": expected_generated,
            "evidence_role": config["evidence_role"],
            "session_roles": config["session_roles"],
            "application_training_sessions": session_training,
            "session_count": len(sessions),
            "global_clock_count": len(expected_clocks),
            "market_repetition_count": len(expected_clocks),
            "source_pair_context_count": int(source_state["pair_context_count"]),
            "source_candidate_context_count": int(source_state["candidate_context_count"]),
            "source_context_availability_counts": source_state["context_availability_counts"],
            "arm_decision_count": len(expected_arm_rows),
            "arm_decisions_count_as_rep": 0,
            "training_observation_count": len(expected_training),
            "training_observations_count_as_rep": 0,
            "calibration_cell_count": len(expected_cal_rows),
            "calibrations_count_as_rep": 0,
            "independent_regime_count": None,
            "paired_pips_vs_no_trade": {
                arm: summaries[arm]["realized_pips"] - summaries["no_trade"]["realized_pips"] for arm in ARMS
            },
            "paired_pips_vs_v1": {
                arm: summaries[arm]["realized_pips"] - summaries["v1_baseline_reference"]["realized_pips"] for arm in ARMS
            },
            "terminal_flat_all_arms": all(summaries[arm]["terminal_flat"] for arm in ARMS),
        }
        for key, expected in expected_top.items():
            if state.get(key) != expected:
                failures.append(f"state_summary:{key}")
        if state.get("source_binding") != source_binding:
            failures.append("state_source_binding")
    except Exception as exc:  # fail closed with the exact independent reason
        failures.append(f"exception:{type(exc).__name__}:{exc}")

    failures = sorted(set(failures))
    generated = None
    try:
        generated = independent.read_json(state_path).get("generated_utc")
    except Exception:
        generated = None
    receipt = {
        **SAFETY,
        "schema_version": 1,
        "generated_utc": generated,
        "verified": not failures,
        "failures": failures,
        "checks": checks,
    }
    _write_receipt(output_path, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--material", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = verify(args.config, args.state, args.material, args.output)
    print(json.dumps({"verified": receipt["verified"], "failures": receipt["failures"]}, sort_keys=True))
    return 0 if receipt["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
