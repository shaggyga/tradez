"""Availability-aware all-68 sequential portfolio replay primitives.

This module is historical research infrastructure.  It has no broker, account,
authorization, lifecycle, or signal-publication dependency.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping, Sequence

from src.forex_system.research.sequential_portfolio_replay_v1 import (
    Candidate,
    Decision,
    MarketSeries,
    PortfolioState,
    apply_decision,
    candidate_for_market,
    choose_primary_decision,
    contiguous,
    decision_payload,
    legal_branches,
    liquidation_equity,
    stable_hash,
    state_payload,
)


SAFETY = {
    "research_only": True,
    "execution_eligible": False,
    "proof_eligible": False,
    "can_promote": False,
    "can_place_orders": False,
    "can_authorize": False,
    "broker_access": False,
    "account_access": False,
    "supported_decision": "no_trade",
}


def stable_aliases(instruments: Sequence[str]) -> dict[str, str]:
    ordered = sorted(str(value).upper() for value in instruments)
    if len(ordered) != 68 or len(set(ordered)) != 68:
        raise ValueError("all-68 alias contract requires 68 unique instruments")
    return {instrument: f"P{index:03d}" for index, instrument in enumerate(ordered, start=1)}


def scheduled_clocks(sessions: Sequence[Mapping[str, Any]], cadence_min: int) -> list[dict[str, Any]]:
    cadence = int(cadence_min) * 60
    rows: list[dict[str, Any]] = []
    sequence = 0
    for session in sorted(sessions, key=lambda row: int(row["session_start_epoch"])):
        start, end = int(session["session_start_epoch"]), int(session["session_end_epoch"])
        clocks = list(range(start, end, cadence))
        for ordinal, epoch in enumerate(clocks, start=1):
            sequence += 1
            rows.append({
                "sequence_no": sequence,
                "session_key": str(session["session_key"]),
                "session_clock_ordinal": ordinal,
                "decision_epoch": epoch,
                "execution_epoch": epoch + 60,
                "feedback_epoch": epoch + 300,
                "session_end_epoch": end,
                "terminal_clock": ordinal == len(clocks),
                "counts_as_market_repetition": 1,
                "counts_as_regime_repetition": 0,
            })
    return rows


def context_status(market: MarketSeries, decision_epoch: int, lookback_min: int) -> dict[str, Any]:
    causal_epoch = int(decision_epoch) - 60
    index = market.exact_index(causal_epoch)
    causal_ready = bool(index is not None and contiguous(market, int(index) - int(lookback_min), int(index)))
    execution_ready = market.exact_index(int(decision_epoch) + 60) is not None
    feedback_ready = market.exact_index(int(decision_epoch) + 300) is not None
    missing: list[str] = []
    if not causal_ready:
        missing.append("causal_context")
    if not execution_ready:
        missing.append("exact_execution_quote")
    if not feedback_ready:
        missing.append("exact_feedback_quote")
    return {
        "causal_epoch": causal_epoch,
        "causal_ready": causal_ready,
        "execution_ready": execution_ready,
        "feedback_ready": feedback_ready,
        "fully_ready": causal_ready and execution_ready and feedback_ready,
        "missing_reasons": missing,
    }


def causal_candidate(market: MarketSeries, decision_epoch: int, config: Mapping[str, Any]) -> Candidate | None:
    policy, costs = config["frozen_policy"], config["costs"]
    return candidate_for_market(
        market,
        int(decision_epoch),
        lookbacks_min=policy["lookbacks_min"],
        slope_weights=policy["slope_weights"],
        minimum_aligned_windows=int(policy["minimum_aligned_windows"]),
        minimum_score_cost_ratio=float(policy["minimum_score_cost_ratio"]),
        maximum_entry_spread_pips=float(costs["maximum_entry_spread_pips"]),
        round_trip_slippage_pips=float(costs["round_trip_slippage_pips"]),
        confidence_floor=float(policy["confidence_floor"]),
        confidence_ceiling=float(policy["confidence_ceiling"]),
        confidence_scale=float(policy["confidence_scale"]),
    )


def ranked_candidates(markets: Mapping[str, MarketSeries], decision_epoch: int, config: Mapping[str, Any]) -> list[Candidate]:
    lookback = max(int(value) for value in config["frozen_policy"]["lookbacks_min"])
    rows: list[Candidate] = []
    for instrument in sorted(markets):
        market = markets[instrument]
        if not context_status(market, decision_epoch, lookback)["causal_ready"]:
            continue
        candidate = causal_candidate(market, decision_epoch, config)
        if candidate is not None:
            rows.append(candidate)
    return sorted(rows, key=lambda row: (-row.score, row.instrument, -row.side))


def choose_decision(
    state: PortfolioState,
    candidates: Sequence[Candidate],
    clock: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    blocked: bool = False,
) -> Decision:
    if blocked:
        if state.flat:
            return Decision("wait", None, None, 0, None, None, None, "", "", "prior unresolved terminal state", None)
        return Decision("hold", None, None, 0, None, None, None, "", "unresolved_terminal_state", "prior unresolved terminal state", None)
    adapter = {
        "session": {
            "normalized_units": config["frozen_policy"]["normalized_units"],
            "feedback_horizon_min": config["schedule"]["feedback_horizon_min"],
        },
        "frozen_policy": config["frozen_policy"],
    }
    return choose_primary_decision(
        state,
        candidates,
        int(clock["decision_epoch"]),
        adapter,
        terminal_clock=bool(clock["terminal_clock"]),
        new_position_allowed=not bool(clock["terminal_clock"]),
    )


def apply_fail_closed(
    state: PortfolioState,
    decision: Decision,
    markets: Mapping[str, MarketSeries],
    clock: Mapping[str, Any],
    config: Mapping[str, Any],
    clock_id: str,
) -> tuple[PortfolioState, dict[str, Any]]:
    try:
        result = apply_decision(
            state,
            decision,
            markets,
            execution_epoch=int(clock["execution_epoch"]),
            slippage_per_leg_pips=float(config["costs"]["slippage_per_execution_leg_pips"]),
            decision_clock_id=clock_id,
            maximum_entry_spread_pips=float(config["costs"]["maximum_entry_spread_pips"]),
        )
        payload = {
            "status": result.status,
            "rejection_reason": result.rejection_reason,
            "realized_delta_pips": result.realized_delta_pips,
            "legs": [asdict(row) for row in result.legs],
            "state_after": state_payload(result.state),
        }
        return result.state, payload
    except (KeyError, ValueError) as exc:
        payload = {
            "status": "rejected",
            "rejection_reason": "missing_exact_execution_quote" if "missing exact" in str(exc).lower() else "invalid_exact_execution",
            "realized_delta_pips": 0.0,
            "legs": [],
            "state_after": state_payload(state),
        }
        return state, payload


def exact_feedback_equity(state: PortfolioState, markets: Mapping[str, MarketSeries], epoch: int, slippage: float) -> tuple[str, float | None]:
    if state.flat:
        return "available_flat", float(state.realized_pips)
    try:
        return "available", liquidation_equity(state, markets, epoch=int(epoch), slippage_per_leg_pips=float(slippage))
    except (KeyError, ValueError):
        return "missing_exact_feedback_quote", None


def terminal_retry(
    state: PortfolioState,
    markets: Mapping[str, MarketSeries],
    session_end_epoch: int,
    slippage: float,
    clock_id: str,
) -> tuple[PortfolioState, dict[str, Any]]:
    if state.flat:
        return state, {"status": "already_flat", "legs": [], "state_after": state_payload(state)}
    decision = Decision("exit", state.position.instrument, None, 0, None, None, None, "", "", "predeclared exact session-end terminal retry", None)
    clock = {"execution_epoch": int(session_end_epoch)}
    config = {"costs": {"slippage_per_execution_leg_pips": slippage, "maximum_entry_spread_pips": 1e9}}
    return apply_fail_closed(state, decision, markets, clock, config, clock_id)


def row_hash(row: Mapping[str, Any]) -> str:
    return stable_hash({key: value for key, value in row.items() if key != "row_sha256"})


def seal_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["row_sha256"] = row_hash(payload)
    return payload


__all__ = [
    "SAFETY", "PortfolioState", "apply_fail_closed", "causal_candidate", "choose_decision",
    "context_status", "decision_payload", "exact_feedback_equity", "legal_branches",
    "ranked_candidates", "scheduled_clocks", "seal_row", "stable_aliases", "stable_hash",
    "state_payload", "terminal_retry",
]
