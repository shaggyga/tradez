"""Pure contracts for sequential, research-only deliberate market replay.

The high-volume SIM gym evaluates many actions at one market clock.  This
module defines the complementary unit needed for deliberate practice: one
primary portfolio decision at one global clock.  Alternative actions remain
nested counterfactuals and never increase the repetition count.

Nothing in this module can access a broker, publish a signal, promote a model,
or authorize execution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import AbstractSet, Any, Iterable, Mapping, Sequence


ALLOWED_ACTIONS = frozenset({"wait", "enter", "hold", "exit", "rotate"})
ALLOWED_SIDES = frozenset({"long", "short"})
POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_promote": False,
    "can_place_orders": False,
    "can_authorize": False,
    "supported_decision": "no_trade",
}

# Situation fingerprints are intentionally causal and coarse.  Outcome,
# future-path, realized-P/L, date labels and human review fields are excluded.
FINGERPRINT_FIELDS = (
    "session_bucket",
    "session",
    "liquidity_bucket",
    "spread_pips",
    "volatility_bucket",
    "regime_bucket",
    "level_state",
    "support_distance_pips",
    "resistance_distance_pips",
    "trend_state",
    "source_state",
    "news_state",
    "rate_state",
    "currency_factor",
    "signal_signature",
    "recent_path",
    "candidate_count_bucket",
    "portfolio_state",
)
FORBIDDEN_FINGERPRINT_FRAGMENTS = (
    "future",
    "outcome",
    "realized",
    "profit",
    "pnl",
    "mfe",
    "mae",
    "exit_price",
    "target_hit",
    "stop_hit",
)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def stable_hash(*parts: Any) -> str:
    encoded = canonical_json(parts).encode("utf-8")
    return sha256(encoded).hexdigest()


@dataclass(frozen=True)
class PortfolioState:
    position_pair: str | None = None
    side: str | None = None
    units: int = 0

    @property
    def flat(self) -> bool:
        return self.position_pair is None and self.side is None and self.units == 0


@dataclass(frozen=True)
class Decision:
    clock_id: str
    action: str
    pair: str | None = None
    side: str | None = None
    units: int = 0
    proof_partition: str = "practice"


@dataclass(frozen=True)
class TransitionResult:
    new_state: PortfolioState
    execution_legs: int
    closed_pair: str | None = None
    opened_pair: str | None = None


@dataclass(frozen=True)
class RepetitionCensus:
    counterfactual_variants: int
    practice_attempts: int
    unique_decision_clocks: int
    effective_independent_episodes: int


def _clean_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, Mapping):
        return {str(key): _clean_scalar(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple, set, frozenset)):
        cleaned = [_clean_scalar(item) for item in value]
        return sorted(cleaned, key=canonical_json)
    return str(value)


def build_situation_fingerprint(snapshot: Mapping[str, Any]) -> str:
    """Return a stable, future-free coarse situation identity.

    Unknown top-level fields are ignored.  A forbidden field nested inside an
    admitted causal field is rejected rather than silently hashed.
    """

    causal: dict[str, Any] = {}
    for field in FINGERPRINT_FIELDS:
        if field not in snapshot:
            continue
        value = snapshot[field]
        serialized = canonical_json(_clean_scalar(value)).lower()
        if any(fragment in serialized for fragment in FORBIDDEN_FINGERPRINT_FRAGMENTS):
            raise ValueError(f"future/outcome field leaked into situation fingerprint: {field}")
        causal[field] = _clean_scalar(value)
    if not causal:
        raise ValueError("situation fingerprint requires at least one causal field")
    return "situation_v1_" + stable_hash("sequential_deliberate_replay_v1", causal)[:28]


def valid_action(state: PortfolioState, decision: Decision) -> bool:
    action = str(decision.action).lower()
    if action not in ALLOWED_ACTIONS or not str(decision.clock_id):
        return False
    pair = str(decision.pair).upper() if decision.pair else None
    side = str(decision.side).lower() if decision.side else None
    units = int(decision.units)
    if state.flat:
        if action == "wait":
            return pair is None and side is None and units == 0
        if action == "enter":
            return bool(pair) and side in ALLOWED_SIDES and units > 0
        return False
    if state.position_pair is None or state.side not in ALLOWED_SIDES or state.units <= 0:
        return False
    if action == "hold":
        return pair is None and side is None and units == 0
    if action == "exit":
        return (pair is None or pair == state.position_pair.upper()) and side is None and units == 0
    if action == "rotate":
        if not pair or side not in ALLOWED_SIDES or units <= 0:
            return False
        return pair != state.position_pair.upper() or side != state.side or units != state.units
    return False


def transition_portfolio(state: PortfolioState, decision: Decision) -> TransitionResult:
    if not valid_action(state, decision):
        raise ValueError(f"invalid {decision.action!r} action for portfolio state")
    action = decision.action.lower()
    if action == "wait":
        return TransitionResult(state, 0)
    if action == "hold":
        return TransitionResult(state, 0)
    if action == "enter":
        pair = str(decision.pair).upper()
        new_state = PortfolioState(pair, str(decision.side).lower(), int(decision.units))
        return TransitionResult(new_state, 1, opened_pair=pair)
    if action == "exit":
        return TransitionResult(PortfolioState(), 1, closed_pair=state.position_pair)
    pair = str(decision.pair).upper()
    new_state = PortfolioState(pair, str(decision.side).lower(), int(decision.units))
    return TransitionResult(
        new_state,
        2,
        closed_pair=state.position_pair,
        opened_pair=pair,
    )


def validate_unique_primary_decisions(decisions: Sequence[Decision]) -> None:
    seen: set[str] = set()
    for decision in decisions:
        if decision.clock_id in seen:
            raise ValueError(f"duplicate primary decision clock: {decision.clock_id}")
        seen.add(decision.clock_id)


def make_repetition_keys(record: Mapping[str, Any]) -> dict[str, str]:
    """Normalize the hierarchy used to distinguish variants from repetitions."""

    aliases = {
        "decision_clock_id": ("decision_clock_id", "clock_id"),
        "market_episode_id": ("market_episode_id", "episode_id"),
        "currency_factor_id": ("currency_factor_id", "factor_id"),
        "price_path_id": ("price_path_id", "path_id"),
        "story_cluster_id": ("story_cluster_id",),
        "strategy_archetype_id": ("strategy_archetype_id", "archetype_id"),
        "position_thesis_id": ("position_thesis_id", "thesis_id"),
        "experiment_lineage_id": ("experiment_lineage_id", "lineage_id"),
    }
    output: dict[str, str] = {}
    for target, candidates in aliases.items():
        value = next((record.get(key) for key in candidates if record.get(key) not in (None, "")), None)
        output[target] = str(value or "unassigned")
    return output


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, index: int) -> int:
        while self.parent[index] != index:
            self.parent[index] = self.parent[self.parent[index]]
            index = self.parent[index]
        return index

    def union(self, left: int, right: int) -> None:
        a = self.find(left)
        b = self.find(right)
        if a != b:
            self.parent[b] = a


def _counts_as_primary_rep(record: Mapping[str, Any]) -> bool:
    if record.get("counts_as_rep") in (False, 0, "0"):
        return False
    if str(record.get("role") or "primary").lower() in {"counterfactual", "variant"}:
        return False
    if record.get("repeat_attempt") in (True, 1, "1"):
        return False
    return True


def effective_repetition_count(records: Iterable[Mapping[str, Any]]) -> int:
    """Collapse variants, repeats, shared paths, and factor-linked episodes.

    The graph is conservative: an exact physical path always links records;
    inside the same market episode, a shared unsigned currency resource links
    them even when the trade signs conflict.
    """

    unique: dict[str, dict[str, str]] = {}
    for record in records:
        if not _counts_as_primary_rep(record):
            continue
        keys = make_repetition_keys(record)
        clock = keys["decision_clock_id"]
        if clock == "unassigned":
            raise ValueError("primary repetition requires decision_clock_id")
        unique.setdefault(clock, keys)
    rows = list(unique.values())
    if not rows:
        return 0
    dsu = _DisjointSet(len(rows))
    for left in range(len(rows)):
        for right in range(left + 1, len(rows)):
            a = rows[left]
            b = rows[right]
            same_path = a["price_path_id"] != "unassigned" and a["price_path_id"] == b["price_path_id"]
            same_episode_factor = (
                a["market_episode_id"] != "unassigned"
                and a["market_episode_id"] == b["market_episode_id"]
                and a["currency_factor_id"] != "unassigned"
                and a["currency_factor_id"] == b["currency_factor_id"]
            )
            if same_path or same_episode_factor:
                dsu.union(left, right)
    return len({dsu.find(index) for index in range(len(rows))})


def repetition_census(records: Sequence[Mapping[str, Any]]) -> RepetitionCensus:
    attempts = {
        str(record.get("attempt_id"))
        for record in records
        if record.get("attempt_id") not in (None, "")
        and str(record.get("role") or "primary").lower() not in {"counterfactual", "variant"}
    }
    clocks = {
        make_repetition_keys(record)["decision_clock_id"]
        for record in records
        if _counts_as_primary_rep(record)
    }
    clocks.discard("unassigned")
    variants = sum(
        1
        for record in records
        if str(record.get("role") or "primary").lower() in {"counterfactual", "variant"}
    )
    return RepetitionCensus(
        counterfactual_variants=variants,
        practice_attempts=len(attempts),
        unique_decision_clocks=len(clocks),
        effective_independent_episodes=effective_repetition_count(records),
    )


def proof_record_allowed(
    record: Mapping[str, Any],
    quarantined_case_ids: AbstractSet[str],
) -> bool:
    case_id = str(record.get("case_id") or "")
    if not case_id or case_id in quarantined_case_ids:
        return False
    if str(
        record.get("evidence_role")
        or record.get("proof_partition")
        or record.get("partition")
        or ""
    ).lower() not in {
        "proof",
        "confirmation",
        "untouched_confirmation",
    }:
        return False
    blocking_flags = (
        "training_only",
        "used_for_training",
        "reviewed",
        "outcome_revealed",
        "feedback_revealed",
        "similar_case_revealed",
        "overlap_with_exposed",
        "factor_episode_exposed",
    )
    return not any(record.get(flag) in (True, 1, "1") for flag in blocking_flags)


def blind_case_alias(cohort_id: str, decision_clock_id: str, salt: str) -> str:
    if not cohort_id or not decision_clock_id or not salt:
        raise ValueError("blind alias requires cohort, clock, and frozen salt")
    return "blindcase_" + stable_hash(salt, cohort_id, decision_clock_id)[:20]


def portfolio_payload(state: PortfolioState) -> dict[str, Any]:
    return asdict(state)


__all__ = [
    "ALLOWED_ACTIONS",
    "Decision",
    "FINGERPRINT_FIELDS",
    "POLICY",
    "PortfolioState",
    "RepetitionCensus",
    "TransitionResult",
    "blind_case_alias",
    "build_situation_fingerprint",
    "canonical_json",
    "effective_repetition_count",
    "make_repetition_keys",
    "portfolio_payload",
    "proof_record_allowed",
    "repetition_census",
    "stable_hash",
    "transition_portfolio",
    "valid_action",
    "validate_unique_primary_decisions",
]
