"""Frozen, research-only policy challenger primitives for the all-68 replay.

This module has no broker, account, authorization, lifecycle, supervisor, or
signal-feed dependency.  It operates only on immutable historical replay rows
and archived executable quotes.
"""

from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
import math
from typing import Any, Callable, Mapping, Sequence


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

ARM_NAMES = (
    "no_trade",
    "v1_baseline_reference",
    "cost_hurdle_2x",
    "explicit_hold_vs_switch_2x",
    "factor_conflict_suppressed_2x",
    "oof_remaining_move_calibrated_2x",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def semantic_sha256(value: Mapping[str, Any], *volatile_fields: str) -> str:
    payload = deepcopy(dict(value))
    for field in volatile_fields:
        payload.pop(field, None)
    return stable_hash(payload)


def row_hash(row: Mapping[str, Any]) -> str:
    return stable_hash({key: value for key, value in row.items() if key != "row_sha256"})


def seal_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["row_sha256"] = row_hash(payload)
    return payload


def flat_state() -> dict[str, Any]:
    return {"realized_pips": 0.0, "position": None}


def candidate_resources(candidate: Mapping[str, Any]) -> tuple[str, str]:
    base, quote = str(candidate["instrument"]).upper().split("_", 1)
    side = int(candidate["side"])
    return (f"{base}:{side:+d}", f"{quote}:{-side:+d}")


def factor_vote_scores(candidates: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    scores: dict[str, float] = {}
    for candidate in candidates:
        base, quote = str(candidate["instrument"]).upper().split("_", 1)
        side = int(candidate["side"])
        weight = float(candidate["expected_move_pips"])
        scores[base] = scores.get(base, 0.0) + side * weight
        scores[quote] = scores.get(quote, 0.0) - side * weight
    return {key: scores[key] for key in sorted(scores)}


def factor_consistent_candidates(
    candidates: Sequence[Mapping[str, Any]], *, tie_epsilon: float
) -> tuple[list[dict[str, Any]], dict[str, float], list[dict[str, Any]]]:
    votes = factor_vote_scores(candidates)
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for original in candidates:
        candidate = dict(original)
        base, quote = str(candidate["instrument"]).upper().split("_", 1)
        side = int(candidate["side"])
        conflicts: list[str] = []
        for currency, exposure in ((base, side), (quote, -side)):
            vote = float(votes.get(currency, 0.0))
            if abs(vote) > float(tie_epsilon) and (vote > 0.0) != (exposure > 0):
                conflicts.append(currency)
        if conflicts:
            rejected.append({
                "snapshot_id": candidate["snapshot_id"],
                "instrument": candidate["instrument"],
                "side": candidate["side"],
                "conflicting_currencies": sorted(conflicts),
            })
        else:
            accepted.append(candidate)
    return accepted, votes, rejected


def rank_candidates(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (dict(row) for row in candidates),
        key=lambda row: (-float(row["score"]), str(row["instrument"]), -int(row["side"])),
    )


def eligible_2x(candidates: Sequence[Mapping[str, Any]], threshold: float) -> list[dict[str, Any]]:
    return rank_candidates(row for row in candidates if float(row["score"]) >= float(threshold))


def empty_decision(action: str, rationale: str, *, instrument: str | None = None) -> dict[str, Any]:
    return {
        "action": action,
        "instrument": instrument,
        "side": None,
        "units": 0,
        "confidence": None,
        "expected_move_pips": None,
        "horizon_min": None,
        "entry_condition": "",
        "invalidation": "",
        "rationale": rationale,
        "candidate_snapshot_id": None,
        "branch_label": "primary",
    }


def candidate_decision(candidate: Mapping[str, Any], action: str, policy: Mapping[str, Any], rationale: str) -> dict[str, Any]:
    return {
        "action": action,
        "instrument": str(candidate["instrument"]),
        "side": int(candidate["side"]),
        "units": int(policy["normalized_units"]),
        "confidence": float(candidate["confidence"]),
        "expected_move_pips": float(candidate["expected_move_pips"]),
        "horizon_min": int(policy["feedback_horizon_min"]),
        "entry_condition": f"frozen_score_cost_ratio>={policy['minimum_entry_score_cost_ratio']}",
        "invalidation": "frozen_rank_reversal_or_maximum_holding",
        "rationale": rationale,
        "candidate_snapshot_id": str(candidate["snapshot_id"]),
        "branch_label": "primary",
    }


def choose_no_trade(state: Mapping[str, Any], *, terminal_clock: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    if state.get("position") is not None:
        raise ValueError("no-trade arm must remain flat")
    reason = "predeclared terminal boundary while flat" if terminal_clock else "frozen no-trade comparator"
    return empty_decision("wait", reason), {"policy_state": "flat_no_trade"}


def choose_cost_hurdle(
    state: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    decision_epoch: int,
    terminal_clock: bool,
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    ranked = eligible_2x(candidates, float(policy["minimum_entry_score_cost_ratio"]))
    position = state.get("position")
    diagnostics = {"eligible_candidate_count": len(ranked), "incumbent_estimate_status": "not_applicable"}
    if terminal_clock:
        if position is None:
            return empty_decision("wait", "predeclared terminal boundary while flat"), diagnostics
        return empty_decision("exit", "predeclared terminal flattening", instrument=position["instrument"]), diagnostics
    best = ranked[0] if ranked else None
    if position is None:
        if best is None:
            return empty_decision("wait", "no candidate clears frozen 2x cost hurdle"), diagnostics
        return candidate_decision(best, "enter", policy, "highest frozen candidate clearing 2x cost hurdle"), diagnostics
    held_min = max(0, int(decision_epoch) - int(position["entry_epoch"])) // 60
    diagnostics["held_min"] = held_min
    if held_min >= int(policy["maximum_holding_min"]):
        return empty_decision("exit", "predeclared maximum holding time", instrument=position["instrument"]), diagnostics
    current = next(
        (row for row in ranked if row["instrument"] == position["instrument"] and int(row["side"]) == int(position["side"])),
        None,
    )
    if best is None:
        diagnostics["incumbent_estimate_status"] = "absent_below_new_entry_hurdle"
        return empty_decision("exit", "no continuing candidate clears frozen 2x hurdle", instrument=position["instrument"]), diagnostics
    if current is None:
        diagnostics["incumbent_estimate_status"] = "absent_below_new_entry_hurdle"
        return candidate_decision(best, "rotate", policy, "incumbent absent from 2x entry set; rotate to best"), diagnostics
    diagnostics["incumbent_estimate_status"] = "available_above_entry_hurdle"
    if best["instrument"] == current["instrument"] and int(best["side"]) == int(current["side"]):
        return empty_decision("hold", "incumbent remains highest ranked"), diagnostics
    improvement = float(best["score"]) / max(1e-12, float(current["score"]))
    diagnostics["rotation_score_multiple"] = improvement
    if improvement >= float(policy["rotation_improvement_multiple"]):
        return candidate_decision(best, "rotate", policy, "alternative clears frozen rotation multiple"), diagnostics
    return empty_decision("hold", "alternative does not clear frozen rotation multiple"), diagnostics


def _explicit_values(
    incumbent: Mapping[str, Any],
    best: Mapping[str, Any] | None,
    costs: Mapping[str, Any],
) -> dict[str, float | None]:
    slip = float(costs["slippage_per_execution_leg_pips"])
    liquidation_cost = float(incumbent["spread_pips"]) / 2.0 + slip
    hold = float(incumbent["expected_move_pips"]) - liquidation_cost
    exit_value = -liquidation_cost
    switch = None
    if best is not None:
        new_round_trip = float(best["spread_pips"]) + float(costs["round_trip_slippage_pips"])
        switch = float(best["expected_move_pips"]) - new_round_trip - liquidation_cost
    return {
        "estimated_liquidation_cost_pips": liquidation_cost,
        "hold_value_pips": hold,
        "exit_value_pips": exit_value,
        "switch_value_pips": switch,
    }


def choose_explicit_hold_vs_switch(
    state: Mapping[str, Any],
    all_candidates: Sequence[Mapping[str, Any]],
    new_entry_candidates: Sequence[Mapping[str, Any]],
    *,
    decision_epoch: int,
    terminal_clock: bool,
    policy: Mapping[str, Any],
    costs: Mapping[str, Any],
    rationale_prefix: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    ranked_all = rank_candidates(all_candidates)
    ranked_entry = rank_candidates(new_entry_candidates)
    position = state.get("position")
    diagnostics: dict[str, Any] = {
        "eligible_candidate_count": len(ranked_entry),
        "incumbent_estimate_status": "not_applicable",
    }
    if terminal_clock:
        if position is None:
            return empty_decision("wait", "predeclared terminal boundary while flat"), diagnostics
        return empty_decision("exit", "predeclared terminal flattening", instrument=position["instrument"]), diagnostics
    best = ranked_entry[0] if ranked_entry else None
    if position is None:
        if best is None:
            return empty_decision("wait", f"{rationale_prefix}: no candidate clears entry contract"), diagnostics
        return candidate_decision(best, "enter", policy, f"{rationale_prefix}: highest eligible new entry"), diagnostics
    held_min = max(0, int(decision_epoch) - int(position["entry_epoch"])) // 60
    diagnostics["held_min"] = held_min
    if held_min >= int(policy["maximum_holding_min"]):
        return empty_decision("exit", "predeclared maximum holding time", instrument=position["instrument"]), diagnostics
    incumbent = next(
        (row for row in ranked_all if row["instrument"] == position["instrument"] and int(row["side"]) == int(position["side"])),
        None,
    )
    if incumbent is None:
        diagnostics["incumbent_estimate_status"] = "unavailable_fail_closed"
        return empty_decision("exit", f"{rationale_prefix}: incumbent continuation estimate unavailable", instrument=position["instrument"]), diagnostics
    diagnostics["incumbent_estimate_status"] = "available_independent_of_new_entry_hurdle"
    if best is not None and best["instrument"] == incumbent["instrument"] and int(best["side"]) == int(incumbent["side"]):
        best = None
    values = _explicit_values(incumbent, best, costs)
    diagnostics.update(values)
    if best is not None:
        switch = float(values["switch_value_pips"])
        hurdle = float(policy.get("explicit_switch_incremental_hurdle_pips", 0.0))
        if switch > max(float(values["hold_value_pips"]) + hurdle, float(values["exit_value_pips"])):
            return candidate_decision(best, "rotate", policy, f"{rationale_prefix}: switch value exceeds explicit hold and exit"), diagnostics
    if float(values["hold_value_pips"]) >= float(values["exit_value_pips"]):
        return empty_decision("hold", f"{rationale_prefix}: explicit hold value dominates"), diagnostics
    return empty_decision("exit", f"{rationale_prefix}: explicit exit value dominates", instrument=position["instrument"]), diagnostics


def build_calibration(observations: Sequence[Mapping[str, Any]], config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[float]] = {}
    for row in observations:
        if row.get("status") != "available":
            continue
        groups.setdefault(str(row["liquidity_bucket"]), []).append(float(row["after_cost_pips"]))
    answer: dict[str, dict[str, Any]] = {}
    minimum = int(config["minimum_training_observations"])
    z = float(config["lower_bound_z"])
    for bucket in sorted(groups):
        values = groups[bucket]
        count = len(values)
        mean = sum(values) / count
        variance = sum((value - mean) ** 2 for value in values) / max(1, count - 1)
        standard_deviation = math.sqrt(max(0.0, variance))
        lower = mean - z * standard_deviation / math.sqrt(count)
        answer[bucket] = {
            "observation_count": count,
            "mean_after_cost_pips": mean,
            "sample_standard_deviation_pips": standard_deviation,
            "lower_bound_after_cost_pips": lower,
            "sufficient_observations": count >= minimum,
            "eligible": count >= minimum and (lower > 0.0 if config["require_positive_after_cost_lower_bound"] else True),
        }
    return answer


def calibrate_candidates(
    candidates: Sequence[Mapping[str, Any]], calibration: Mapping[str, Mapping[str, Any]], threshold: float
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for original in eligible_2x(candidates, threshold):
        candidate = dict(original)
        bucket = str(candidate["liquidity_bucket"])
        cell = calibration.get(bucket)
        reason = None
        if cell is None:
            reason = "missing_prior_session_calibration"
        elif not cell.get("sufficient_observations"):
            reason = "insufficient_prior_session_calibration"
        elif not cell.get("eligible"):
            reason = "nonpositive_prior_session_lower_bound"
        if reason:
            rejected.append({"snapshot_id": candidate["snapshot_id"], "reason": reason, "liquidity_bucket": bucket})
            continue
        candidate["calibrated_after_cost_pips"] = float(cell["mean_after_cost_pips"])
        candidate["calibrated_lower_bound_pips"] = float(cell["lower_bound_after_cost_pips"])
        accepted.append(candidate)
    accepted.sort(
        key=lambda row: (-float(row["calibrated_lower_bound_pips"]), -float(row["score"]), str(row["instrument"]), -int(row["side"]))
    )
    return accepted, rejected


def quote_at(market: Any, epoch: int) -> tuple[float, float, float, float]:
    index = market.exact_index(int(epoch))
    if index is None:
        raise ValueError(f"missing exact executable quote: {market.instrument}:{epoch}")
    bid = float(market.bid_open[index])
    ask = float(market.ask_open[index])
    spread = (ask - bid) / float(market.pip)
    if spread <= 0.0:
        raise ValueError(f"crossed executable quote: {market.instrument}:{epoch}")
    return bid, ask, spread, float(market.pip)


def _close(state: Mapping[str, Any], markets: Mapping[str, Any], epoch: int, slippage: float) -> tuple[dict[str, Any], dict[str, Any]]:
    position = state.get("position")
    if position is None:
        raise ValueError("cannot close flat state")
    bid, ask, spread, pip = quote_at(markets[position["instrument"]], epoch)
    side = int(position["side"])
    raw = bid if side > 0 else ask
    executed = raw - slippage * pip if side > 0 else raw + slippage * pip
    realized = side * (executed - float(position["entry_price"])) / pip * int(position["units"])
    next_state = {"realized_pips": float(state["realized_pips"]) + realized, "position": None}
    return next_state, {
        "leg_kind": "close", "instrument": position["instrument"], "side": side,
        "units": int(position["units"]), "execution_epoch": int(epoch), "raw_price": raw,
        "executed_price": executed, "spread_pips": spread, "slippage_pips": slippage,
        "realized_pips": realized, "thesis_id": position["thesis_id"],
    }


def _open(
    state: Mapping[str, Any], decision: Mapping[str, Any], markets: Mapping[str, Any], epoch: int,
    slippage: float, clock_id: str, arm: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    instrument = str(decision["instrument"])
    side = int(decision["side"])
    bid, ask, spread, pip = quote_at(markets[instrument], epoch)
    raw = ask if side > 0 else bid
    executed = raw + slippage * pip if side > 0 else raw - slippage * pip
    thesis = "challengerthesis_" + stable_hash(arm, instrument, side, clock_id)[:28]
    position = {
        "instrument": instrument, "side": side, "units": int(decision["units"]),
        "entry_epoch": int(epoch), "entry_price": executed, "entry_raw_price": raw,
        "entry_spread_pips": spread, "entry_slippage_pips": slippage,
        "entry_clock_id": clock_id, "thesis_id": thesis,
    }
    next_state = {"realized_pips": float(state["realized_pips"]), "position": position}
    return next_state, {
        "leg_kind": "open", "instrument": instrument, "side": side,
        "units": int(decision["units"]), "execution_epoch": int(epoch), "raw_price": raw,
        "executed_price": executed, "spread_pips": spread, "slippage_pips": slippage,
        "realized_pips": 0.0, "thesis_id": thesis,
    }


def apply_action(
    state: Mapping[str, Any], decision: Mapping[str, Any], markets: Mapping[str, Any], *,
    execution_epoch: int, costs: Mapping[str, Any], clock_id: str, arm: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    before = deepcopy(dict(state))
    action = str(decision["action"])
    slippage = float(costs["slippage_per_execution_leg_pips"])
    try:
        if action in {"wait", "hold"}:
            return before, {"status": "applied", "rejection_reason": "", "realized_delta_pips": 0.0, "legs": [], "state_after": before}
        if action in {"enter", "rotate"}:
            _, _, spread, _ = quote_at(markets[str(decision["instrument"])], execution_epoch)
            if spread > float(costs["maximum_entry_spread_pips"]):
                return before, {"status": "rejected", "rejection_reason": "execution_spread_above_limit", "realized_delta_pips": 0.0, "legs": [], "state_after": before}
        if action == "exit":
            after, close_leg = _close(before, markets, execution_epoch, slippage)
            return after, {"status": "applied", "rejection_reason": "", "realized_delta_pips": close_leg["realized_pips"], "legs": [close_leg], "state_after": after}
        if action == "enter":
            after, open_leg = _open(before, decision, markets, execution_epoch, slippage, clock_id, arm)
            return after, {"status": "applied", "rejection_reason": "", "realized_delta_pips": 0.0, "legs": [open_leg], "state_after": after}
        if action != "rotate":
            raise ValueError("invalid action")
        closed, close_leg = _close(before, markets, execution_epoch, slippage)
        after, open_leg = _open(closed, decision, markets, execution_epoch, slippage, clock_id, arm)
        return after, {"status": "applied", "rejection_reason": "", "realized_delta_pips": close_leg["realized_pips"], "legs": [close_leg, open_leg], "state_after": after}
    except (KeyError, TypeError, ValueError) as exc:
        reason = "missing_exact_execution_quote" if "missing exact" in str(exc).lower() else "invalid_exact_execution"
        return before, {"status": "rejected", "rejection_reason": reason, "realized_delta_pips": 0.0, "legs": [], "state_after": before}


def liquidation_equity(state: Mapping[str, Any], markets: Mapping[str, Any], epoch: int, slippage: float) -> tuple[str, float | None]:
    if state.get("position") is None:
        return "available_flat", float(state["realized_pips"])
    try:
        closed, _ = _close(state, markets, epoch, slippage)
        return "available", float(closed["realized_pips"])
    except (KeyError, TypeError, ValueError):
        return "missing_exact_feedback_quote", None


def candidate_roundtrip_observation(
    candidate: Mapping[str, Any], markets: Mapping[str, Any], *, execution_epoch: int,
    feedback_epoch: int, slippage: float,
) -> dict[str, Any]:
    try:
        instrument, side = str(candidate["instrument"]), int(candidate["side"])
        bid0, ask0, _, pip = quote_at(markets[instrument], execution_epoch)
        bid1, ask1, _, _ = quote_at(markets[instrument], feedback_epoch)
        entry = ask0 + slippage * pip if side > 0 else bid0 - slippage * pip
        exit_price = bid1 - slippage * pip if side > 0 else ask1 + slippage * pip
        net = side * (exit_price - entry) / pip
        return {
            "status": "available", "instrument": instrument, "side": side,
            "snapshot_id": candidate["snapshot_id"], "liquidity_bucket": candidate["liquidity_bucket"],
            "after_cost_pips": net,
        }
    except (KeyError, TypeError, ValueError):
        return {
            "status": "missing_exact_quote", "instrument": candidate.get("instrument"),
            "side": candidate.get("side"), "snapshot_id": candidate.get("snapshot_id"),
            "liquidity_bucket": candidate.get("liquidity_bucket"), "after_cost_pips": None,
        }


__all__ = [
    "ARM_NAMES", "SAFETY", "apply_action", "build_calibration", "calibrate_candidates",
    "candidate_resources", "candidate_roundtrip_observation", "canonical_json",
    "choose_cost_hurdle", "choose_explicit_hold_vs_switch", "choose_no_trade",
    "eligible_2x", "factor_consistent_candidates", "factor_vote_scores", "flat_state",
    "liquidation_equity", "rank_candidates", "row_hash", "seal_row", "semantic_sha256",
    "stable_hash",
]
