"""Pure contracts for proof-ineligible sequential portfolio practice.

This module has no broker, account, authorization, lifecycle, or runtime
imports.  It schedules immutable historical cases and scores learner actions
only after the whole practice session has been precommitted.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence


POLICY = {
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


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def option_identity(action: Mapping[str, Any]) -> str:
    payload = {
        "action": action.get("action"),
        "instrument": action.get("instrument"),
        "side": action.get("side"),
        "units": int(action.get("units") or 0),
        "branch_label": action.get("branch_label"),
    }
    return "spcoption_" + stable_hash(payload)[:28]


def case_identity(
    source_cohort_id: str,
    source_session_id: str,
    clock_id: str,
    causal_sha256: str,
    options: Sequence[Mapping[str, Any]],
) -> str:
    option_ids = sorted(option_identity(row) for row in options)
    return "spccase_" + stable_hash(
        source_cohort_id, source_session_id, clock_id, causal_sha256, option_ids
    )[:28]


def exact_attempt_identity(
    cohort_id: str,
    learner_id: str,
    practice_session_id: str,
    assignment_id: str,
    case_id: str,
    attempt_ordinal_for_case: int,
    action: Mapping[str, Any],
) -> str:
    return "spcattempt_" + stable_hash(
        cohort_id,
        learner_id,
        practice_session_id,
        assignment_id,
        case_id,
        int(attempt_ordinal_for_case),
        option_identity(action),
        action,
    )[:28]


@dataclass(frozen=True)
class Memory:
    attempt_count: int = 0
    last_session_index: int = 0
    next_due_session: int = 1
    mastery_streak: int = 0
    last_regret_pips: float = 0.0
    last_correct: bool = False
    last_best_option_id: str | None = None


@dataclass(frozen=True)
class ScheduledCase:
    case_id: str
    selection_weight: float
    selection_reason: str
    attempt_ordinal_for_case: int
    counts_as_market_repetition: int
    counts_as_regime_repetition: int


def deterministic_tiebreak(seed: str, session_index: int, case_id: str) -> str:
    return stable_hash(seed, int(session_index), case_id)


def select_session_cases(
    cases: Sequence[Mapping[str, Any]],
    memory: Mapping[str, Memory],
    *,
    session_index: int,
    attempts_per_session: int,
    target_review_slots: int,
    seed: str,
    novelty_weight: float,
    due_error_weight: float,
    due_correct_weight: float,
    overdue_weight_per_session: float,
) -> list[ScheduledCase]:
    """Select a session using only memory available before that session."""
    by_id = {str(row["case_id"]): row for row in cases}
    unseen = [case_id for case_id in by_id if memory.get(case_id, Memory()).attempt_count == 0]
    due = [
        case_id
        for case_id in by_id
        if memory.get(case_id, Memory()).attempt_count > 0
        and memory[case_id].next_due_session <= int(session_index)
    ]

    def review_key(case_id: str) -> tuple[float, str]:
        state = memory[case_id]
        base = due_correct_weight if state.last_correct else due_error_weight
        overdue = max(0, int(session_index) - int(state.next_due_session))
        weight = float(base) + float(overdue_weight_per_session) * overdue
        return (-weight, deterministic_tiebreak(seed, session_index, case_id))

    due.sort(key=review_key)
    unseen.sort(key=lambda case_id: deterministic_tiebreak(seed, session_index, case_id))
    review_target = 0 if int(session_index) == 1 else min(int(target_review_slots), len(due))
    chosen_reviews = due[:review_target]
    remaining = max(0, int(attempts_per_session) - len(chosen_reviews))
    chosen_new = unseen[:remaining]
    remaining -= len(chosen_new)
    extra_reviews = [case_id for case_id in due if case_id not in chosen_reviews][:remaining]
    selected = chosen_reviews + chosen_new + extra_reviews
    if len(selected) < int(attempts_per_session):
        future_reviews = [
            case_id
            for case_id in by_id
            if memory.get(case_id, Memory()).attempt_count > 0 and case_id not in selected
        ]
        future_reviews.sort(
            key=lambda case_id: (
                memory[case_id].next_due_session,
                deterministic_tiebreak(seed, session_index, case_id),
            )
        )
        selected.extend(future_reviews[: int(attempts_per_session) - len(selected)])

    episode_seen_before = {
        str(by_id[case_id]["market_episode_id"])
        for case_id, state in memory.items()
        if state.attempt_count > 0 and case_id in by_id
    }
    result: list[ScheduledCase] = []
    for case_id in selected:
        state = memory.get(case_id, Memory())
        is_new = state.attempt_count == 0
        episode = str(by_id[case_id]["market_episode_id"])
        if is_new:
            weight = float(novelty_weight)
            reason = "novel_case"
        else:
            base = due_correct_weight if state.last_correct else due_error_weight
            overdue = max(0, int(session_index) - int(state.next_due_session))
            weight = float(base) + float(overdue_weight_per_session) * overdue
            reason = "due_correct_review" if state.last_correct else "due_error_review"
        result.append(
            ScheduledCase(
                case_id=case_id,
                selection_weight=weight,
                selection_reason=reason,
                attempt_ordinal_for_case=state.attempt_count + 1,
                counts_as_market_repetition=1 if is_new else 0,
                counts_as_regime_repetition=1 if is_new and episode not in episode_seen_before else 0,
            )
        )
        if is_new:
            episode_seen_before.add(episode)
    return result


def choose_precommitted_action(
    case: Mapping[str, Any],
    memory: Memory,
    *,
    learner_id: str,
    practice_session_id: str,
    assignment_id: str,
) -> Mapping[str, Any]:
    options = list(case["options"])
    if not options:
        raise ValueError("case has no legal actions")
    if memory.last_best_option_id:
        remembered = next(
            (row for row in options if option_identity(row) == memory.last_best_option_id),
            None,
        )
        if remembered is not None:
            return remembered
    index = int(stable_hash(learner_id, practice_session_id, assignment_id)[:16], 16) % len(options)
    return options[index]


def mistake_class(chosen: Mapping[str, Any], best: Mapping[str, Any], state_before: Mapping[str, Any]) -> str:
    if option_identity(chosen) == option_identity(best):
        return "none"
    chosen_action = str(chosen.get("action"))
    best_action = str(best.get("action"))
    position = state_before.get("position")
    if chosen_action == "wait" or best_action == "wait":
        return "opportunity_selection"
    if position is None and {chosen_action, best_action} & {"enter"}:
        if chosen.get("instrument") == best.get("instrument") and chosen.get("side") != best.get("side"):
            return "direction"
        return "entry"
    if "rotate" in {chosen_action, best_action}:
        return "rotation"
    if "exit" in {chosen_action, best_action}:
        return "exit"
    if "hold" in {chosen_action, best_action}:
        return "management"
    if chosen.get("side") != best.get("side"):
        return "direction"
    return "opportunity_selection"


def update_memory(
    before: Memory,
    *,
    session_index: int,
    regret_pips: float,
    correct: bool,
    best_option_id: str,
    incorrect_delay: int,
    correct_base_interval: int,
    maximum_interval: int,
) -> Memory:
    streak = before.mastery_streak + 1 if correct else 0
    if correct:
        interval = min(int(maximum_interval), int(correct_base_interval) * (2 ** max(0, streak - 1)))
    else:
        interval = int(incorrect_delay)
    return Memory(
        attempt_count=before.attempt_count + 1,
        last_session_index=int(session_index),
        next_due_session=int(session_index) + max(1, interval),
        mastery_streak=streak,
        last_regret_pips=float(regret_pips),
        last_correct=bool(correct),
        last_best_option_id=str(best_option_id),
    )


def memory_payload(value: Memory) -> dict[str, Any]:
    return {
        "attempt_count": value.attempt_count,
        "last_session_index": value.last_session_index,
        "next_due_session": value.next_due_session,
        "mastery_streak": value.mastery_streak,
        "last_regret_pips": value.last_regret_pips,
        "last_correct": value.last_correct,
        "last_best_option_id": value.last_best_option_id,
    }
