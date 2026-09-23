"""Pure, research-only contracts for bar-by-bar portfolio replay.

The replay exposes one global portfolio decision every five completed M1 bars,
then executes that precommitted action one minute later against executable
bid/ask opens.  It is deliberately isolated from every broker, lifecycle,
authorization, and signal-publication surface.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from hashlib import sha256
import csv
import gzip
import io
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np


POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_promote": False,
    "can_place_orders": False,
    "can_authorize": False,
    "broker_access": False,
    "account_access": False,
    "signal_feed_write": False,
    "lifecycle_write": False,
    "supported_decision": "no_trade",
}
ALLOWED_ACTIONS = frozenset({"wait", "enter", "hold", "exit", "rotate"})
ALLOWED_SIDES = frozenset({1, -1})


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def parse_epoch(value: Any) -> int:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ValueError("missing timestamp")
    return int(datetime.fromisoformat(text).timestamp())


def pip_size(instrument: str) -> float:
    return 0.01 if str(instrument).upper().endswith("_JPY") else 0.0001


def liquidity_bucket(spread_pips: float) -> str:
    if spread_pips <= 2.0:
        return "liquid"
    if spread_pips <= 3.0:
        return "normal"
    if spread_pips <= 5.0:
        return "elevated"
    return "wide"


def session_bucket(epoch: int) -> str:
    hour = (int(epoch) // 3600) % 24
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "london_new_york_overlap"
    if 16 <= hour < 21:
        return "new_york"
    return "rollover"


def market_episode_id(epoch: int, minutes: int) -> str:
    width = max(1, int(minutes)) * 60
    return f"market_episode_{int(epoch) // width}"


@dataclass(frozen=True)
class MarketSeries:
    instrument: str
    pip: float
    epochs: np.ndarray
    bid_open: np.ndarray
    bid_high: np.ndarray
    bid_low: np.ndarray
    bid_close: np.ndarray
    ask_open: np.ndarray
    ask_high: np.ndarray
    ask_low: np.ndarray
    ask_close: np.ndarray
    mid_close: np.ndarray
    source_sha256: str

    def exact_index(self, epoch: int) -> int | None:
        index = int(np.searchsorted(self.epochs, int(epoch)))
        if index < len(self.epochs) and int(self.epochs[index]) == int(epoch):
            return index
        return None


@dataclass(frozen=True)
class Candidate:
    instrument: str
    side: int
    score: float
    confidence: float
    expected_move_pips: float
    spread_pips: float
    liquidity_bucket: str
    aligned_windows: int
    moves_pips: tuple[float, ...]
    snapshot_id: str


@dataclass(frozen=True)
class Position:
    instrument: str
    side: int
    units: int
    entry_epoch: int
    entry_price: float
    entry_raw_price: float
    entry_spread_pips: float
    entry_slippage_pips: float
    entry_clock_id: str
    thesis_id: str


@dataclass(frozen=True)
class PortfolioState:
    realized_pips: float = 0.0
    position: Position | None = None

    @property
    def flat(self) -> bool:
        return self.position is None


@dataclass(frozen=True)
class Decision:
    action: str
    instrument: str | None
    side: int | None
    units: int
    confidence: float | None
    expected_move_pips: float | None
    horizon_min: int | None
    entry_condition: str
    invalidation: str
    rationale: str
    candidate_snapshot_id: str | None
    branch_label: str = "primary"


@dataclass(frozen=True)
class ExecutionLeg:
    leg_kind: str
    instrument: str
    side: int
    units: int
    execution_epoch: int
    raw_price: float
    executed_price: float
    spread_pips: float
    slippage_pips: float
    realized_pips: float
    thesis_id: str


@dataclass(frozen=True)
class AppliedAction:
    state: PortfolioState
    legs: tuple[ExecutionLeg, ...]
    realized_delta_pips: float
    status: str = "applied"
    rejection_reason: str = ""


def _read_archive_bytes(path: Path, maximum_raw_bytes: int) -> bytes:
    with gzip.open(path, "rb") as handle:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = handle.read(min(1024 * 1024, maximum_raw_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_raw_bytes:
                raise ValueError(f"source archive exceeds limit: {path}")
            chunks.append(chunk)
    return b"".join(chunks)


def load_archived_market(
    path: Path,
    *,
    instrument: str,
    expected_raw_sha256: str,
    maximum_raw_bytes: int,
) -> MarketSeries:
    raw = _read_archive_bytes(path, int(maximum_raw_bytes))
    if sha256_bytes(raw) != str(expected_raw_sha256):
        raise ValueError(f"archived source hash mismatch: {instrument}")
    required = (
        "time",
        "datetime",
        "instrument",
        "granularity",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
    )
    rows: list[tuple[Any, ...]] = []
    with io.StringIO(raw.decode("utf-8"), newline="") as source:
        reader = csv.DictReader(source)
        missing = sorted(set(required) - set(reader.fieldnames or ()))
        if missing:
            raise ValueError(f"missing archived candle columns: {missing}")
        for number, row in enumerate(reader, start=2):
            try:
                epoch = parse_epoch(row["time"])
                duplicate_epoch = parse_epoch(row["datetime"])
                row_instrument = str(row["instrument"]).upper()
                granularity = str(row["granularity"])
                values = tuple(float(row[name]) for name in required[4:])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"malformed archived candle row {instrument}:{number}") from exc
            if (
                epoch != duplicate_epoch
                or epoch % 60
                or row_instrument != str(instrument).upper()
                or granularity != "M1"
                or any(not math.isfinite(value) or value <= 0.0 for value in values)
            ):
                raise ValueError(f"invalid archived candle row {instrument}:{number}")
            rows.append((epoch, *values))
    if not rows:
        raise ValueError(f"empty archived source: {instrument}")
    rows.sort(key=lambda row: int(row[0]))
    if len({int(row[0]) for row in rows}) != len(rows):
        raise ValueError(f"duplicate archived candle: {instrument}")
    epochs = np.asarray([row[0] for row in rows], dtype=np.int64)
    matrix = np.asarray([row[1:] for row in rows], dtype=np.float64)
    if np.any(matrix[:, 4:8] <= matrix[:, 0:4]):
        raise ValueError(f"crossed archived candle quote: {instrument}")
    for offset in (0, 4):
        opens = matrix[:, offset]
        highs = matrix[:, offset + 1]
        lows = matrix[:, offset + 2]
        closes = matrix[:, offset + 3]
        if np.any(highs < np.maximum(opens, closes)) or np.any(lows > np.minimum(opens, closes)):
            raise ValueError(f"invalid archived candle envelope: {instrument}")
    return MarketSeries(
        instrument=str(instrument).upper(),
        pip=pip_size(instrument),
        epochs=epochs,
        bid_open=matrix[:, 0],
        bid_high=matrix[:, 1],
        bid_low=matrix[:, 2],
        bid_close=matrix[:, 3],
        ask_open=matrix[:, 4],
        ask_high=matrix[:, 5],
        ask_low=matrix[:, 6],
        ask_close=matrix[:, 7],
        mid_close=(matrix[:, 3] + matrix[:, 7]) / 2.0,
        source_sha256=str(expected_raw_sha256),
    )


def contiguous(market: MarketSeries, first_index: int, last_index: int) -> bool:
    if first_index < 0 or last_index < first_index or last_index >= len(market.epochs):
        return False
    return int(market.epochs[last_index]) - int(market.epochs[first_index]) == (
        last_index - first_index
    ) * 60


def build_global_clocks(
    markets: Mapping[str, MarketSeries],
    *,
    start_epoch: int,
    end_epoch: int,
    cadence_min: int,
    feature_lookback_min: int,
    execution_delay_min: int,
    feedback_horizon_min: int,
) -> list[int]:
    cadence = max(1, int(cadence_min)) * 60
    first = ((int(start_epoch) + cadence - 1) // cadence) * cadence
    clocks: list[int] = []
    for decision_epoch in range(first, int(end_epoch), cadence):
        decision_candle_epoch = decision_epoch - 60
        valid = True
        for market in markets.values():
            index = market.exact_index(decision_candle_epoch)
            if (
                index is None
                or not contiguous(market, index - int(feature_lookback_min), index)
            ):
                valid = False
                break
        if valid:
            clocks.append(decision_epoch)
    return clocks


def candidate_for_market(
    market: MarketSeries,
    decision_epoch: int,
    *,
    lookbacks_min: Sequence[int],
    slope_weights: Sequence[float],
    minimum_aligned_windows: int,
    minimum_score_cost_ratio: float,
    maximum_entry_spread_pips: float,
    round_trip_slippage_pips: float,
    confidence_floor: float,
    confidence_ceiling: float,
    confidence_scale: float,
) -> Candidate | None:
    if len(lookbacks_min) != len(slope_weights) or not lookbacks_min:
        raise ValueError("lookback and weight vectors must be nonempty and matched")
    index = market.exact_index(int(decision_epoch) - 60)
    if index is None:
        return None
    current = float(market.mid_close[index])
    spread = (float(market.ask_close[index]) - float(market.bid_close[index])) / market.pip
    if spread <= 0.0 or spread > float(maximum_entry_spread_pips):
        return None
    moves: list[float] = []
    slopes: list[float] = []
    for lookback in lookbacks_min:
        prior = index - int(lookback)
        if not contiguous(market, prior, index):
            return None
        move = (current - float(market.mid_close[prior])) / market.pip
        moves.append(move)
        slopes.append(move / math.sqrt(max(1.0, float(lookback))))
    weighted = sum(float(weight) * slope for weight, slope in zip(slope_weights, slopes))
    if abs(weighted) <= 1e-12:
        return None
    side = 1 if weighted > 0.0 else -1
    aligned = sum(1 for move in moves if (move > 0.0) == (side > 0))
    if aligned < int(minimum_aligned_windows):
        return None
    total_cost = spread + max(0.0, float(round_trip_slippage_pips))
    score = abs(weighted) / max(0.01, total_cost)
    if score < float(minimum_score_cost_ratio):
        return None
    confidence = min(
        float(confidence_ceiling),
        max(float(confidence_floor), float(confidence_floor) + (score - 1.0) * float(confidence_scale)),
    )
    expected = abs(weighted) * math.sqrt(5.0)
    payload = {
        "instrument": market.instrument,
        "decision_epoch": int(decision_epoch),
        "source_sha256": market.source_sha256,
        "side": side,
        "score": round(score, 12),
        "moves_pips": [round(value, 12) for value in moves],
        "spread_pips": round(spread, 12),
    }
    return Candidate(
        instrument=market.instrument,
        side=side,
        score=score,
        confidence=confidence,
        expected_move_pips=expected,
        spread_pips=spread,
        liquidity_bucket=liquidity_bucket(spread),
        aligned_windows=aligned,
        moves_pips=tuple(moves),
        snapshot_id="candidate_" + stable_hash(payload)[:28],
    )


def candidate_set(
    markets: Mapping[str, MarketSeries],
    decision_epoch: int,
    config: Mapping[str, Any],
) -> list[Candidate]:
    policy = config["frozen_policy"]
    source = config["source"]
    costs = config["costs"]
    rows = [
        candidate_for_market(
            market,
            int(decision_epoch),
            lookbacks_min=policy["lookbacks_min"],
            slope_weights=policy["slope_weights"],
            minimum_aligned_windows=int(policy["minimum_aligned_windows"]),
            minimum_score_cost_ratio=float(policy["minimum_score_cost_ratio"]),
            maximum_entry_spread_pips=float(source["maximum_entry_spread_pips"]),
            round_trip_slippage_pips=float(costs["round_trip_slippage_pips"]),
            confidence_floor=float(policy["confidence_floor"]),
            confidence_ceiling=float(policy["confidence_ceiling"]),
            confidence_scale=float(policy["confidence_scale"]),
        )
        for market in markets.values()
    ]
    return sorted(
        (row for row in rows if row is not None),
        key=lambda row: (-row.score, row.instrument, -row.side),
    )


def _decision_from_candidate(candidate: Candidate, action: str, config: Mapping[str, Any], rationale: str) -> Decision:
    return Decision(
        action=action,
        instrument=candidate.instrument,
        side=candidate.side,
        units=int(config["session"]["normalized_units"]),
        confidence=candidate.confidence,
        expected_move_pips=candidate.expected_move_pips,
        horizon_min=int(config["session"]["feedback_horizon_min"]),
        entry_condition=f"frozen_rank_score>={config['frozen_policy']['minimum_score_cost_ratio']}",
        invalidation="frozen_rank_reversal_or_maximum_holding",
        rationale=rationale,
        candidate_snapshot_id=candidate.snapshot_id,
    )


def choose_primary_decision(
    state: PortfolioState,
    candidates: Sequence[Candidate],
    decision_epoch: int,
    config: Mapping[str, Any],
    *,
    terminal_clock: bool = False,
    new_position_allowed: bool = True,
) -> Decision:
    if terminal_clock:
        if state.flat:
            return Decision("wait", None, None, 0, None, None, None, "", "", "predeclared terminal boundary while flat", None)
        return Decision("exit", state.position.instrument, None, 0, None, None, None, "", "", "predeclared terminal flattening", None)
    best = candidates[0] if candidates else None
    if state.flat:
        if best is None or not new_position_allowed:
            return Decision("wait", None, None, 0, None, None, None, "", "", "no frozen-policy candidate", None)
        return _decision_from_candidate(best, "enter", config, "highest frozen cost-adjusted candidate")
    assert state.position is not None
    held_min = max(0, int(decision_epoch) - int(state.position.entry_epoch)) // 60
    if held_min >= int(config["frozen_policy"]["maximum_holding_min"]):
        return Decision("exit", state.position.instrument, None, 0, None, None, None, "", "", "predeclared maximum holding time", None)
    current = next(
        (
            row
            for row in candidates
            if row.instrument == state.position.instrument and row.side == state.position.side
        ),
        None,
    )
    if best is None:
        return Decision("exit", state.position.instrument, None, 0, None, None, None, "", "", "no continuing frozen-policy candidate", None)
    if current is None:
        if not new_position_allowed:
            return Decision("exit", state.position.instrument, None, 0, None, None, None, "", "", "new position cannot mature before administrative boundary", None)
        return _decision_from_candidate(best, "rotate", config, "current thesis invalidated; rotate to best candidate")
    if best.instrument == current.instrument and best.side == current.side:
        return Decision("hold", None, None, 0, current.confidence, current.expected_move_pips, int(config["session"]["feedback_horizon_min"]), "", "frozen_rank_reversal_or_maximum_holding", "current position remains highest-ranked", current.snapshot_id)
    improvement = best.score / max(1e-12, current.score)
    if improvement >= float(config["frozen_policy"]["rotation_improvement_multiple"]):
        if not new_position_allowed:
            return Decision("hold", None, None, 0, current.confidence, current.expected_move_pips, int(config["session"]["feedback_horizon_min"]), "", "administrative_session_boundary", "rotation cannot mature before administrative boundary", current.snapshot_id)
        return _decision_from_candidate(best, "rotate", config, "alternative clears frozen rotation-improvement multiple")
    return Decision("hold", None, None, 0, current.confidence, current.expected_move_pips, int(config["session"]["feedback_horizon_min"]), "", "frozen_rank_reversal_or_maximum_holding", "alternative does not clear rotation cost hurdle", current.snapshot_id)


def validate_decision(state: PortfolioState, decision: Decision) -> None:
    if decision.action not in ALLOWED_ACTIONS:
        raise ValueError(f"invalid action: {decision.action}")
    if state.flat:
        if decision.action == "wait" and decision.instrument is None and decision.side is None and decision.units == 0:
            return
        if decision.action == "enter" and decision.instrument and decision.side in ALLOWED_SIDES and decision.units > 0:
            return
        raise ValueError(f"invalid flat-state action: {decision.action}")
    if decision.action == "hold" and decision.instrument is None and decision.side is None and decision.units == 0:
        return
    if decision.action == "exit" and decision.instrument in (None, state.position.instrument) and decision.side is None and decision.units == 0:
        return
    if decision.action == "rotate" and decision.instrument and decision.side in ALLOWED_SIDES and decision.units > 0:
        if decision.instrument != state.position.instrument or decision.side != state.position.side or decision.units != state.position.units:
            return
    raise ValueError(f"invalid positioned-state action: {decision.action}")


def legal_branches(
    state: PortfolioState,
    primary: Decision,
    candidates: Sequence[Candidate],
    config: Mapping[str, Any],
) -> list[Decision]:
    branches: list[Decision] = []
    best = candidates[0] if candidates else None
    second = candidates[1] if len(candidates) > 1 else None
    if state.flat:
        branches.append(Decision("wait", None, None, 0, None, None, None, "", "", "depth-one no-trade branch", None, "wait"))
        for label, candidate in (("enter_best", best), ("enter_second", second)):
            if candidate is not None:
                branches.append(replace(_decision_from_candidate(candidate, "enter", config, f"depth-one {label} branch"), branch_label=label))
        if best is not None:
            flipped = replace(best, side=-best.side, snapshot_id="candidate_" + stable_hash(best.snapshot_id, "flipped")[:28])
            branches.append(replace(_decision_from_candidate(flipped, "enter", config, "depth-one flipped-best branch"), branch_label="enter_flipped_best"))
    else:
        branches.extend(
            [
                Decision("hold", None, None, 0, None, None, None, "", "", "depth-one hold branch", None, "hold"),
                Decision("exit", state.position.instrument, None, 0, None, None, None, "", "", "depth-one exit branch", None, "exit"),
            ]
        )
        if best is not None and (
            best.instrument != state.position.instrument or best.side != state.position.side
        ):
            branches.append(replace(_decision_from_candidate(best, "rotate", config, "depth-one rotate-best branch"), branch_label="rotate_best"))
    unique: dict[str, Decision] = {}
    primary_identity = (primary.action, primary.instrument, primary.side, primary.units)
    for branch in branches:
        validate_decision(state, branch)
        identity = (branch.action, branch.instrument, branch.side, branch.units)
        if identity == primary_identity:
            continue
        unique.setdefault(canonical_json(identity), branch)
    return [unique[key] for key in sorted(unique)]


def _quote(market: MarketSeries, epoch: int) -> tuple[float, float, float]:
    index = market.exact_index(int(epoch))
    if index is None:
        raise ValueError(f"missing exact executable quote: {market.instrument}:{epoch}")
    bid = float(market.bid_open[index])
    ask = float(market.ask_open[index])
    spread = (ask - bid) / market.pip
    if spread <= 0.0:
        raise ValueError(f"crossed executable quote: {market.instrument}:{epoch}")
    return bid, ask, spread


def _close_position(
    state: PortfolioState,
    markets: Mapping[str, MarketSeries],
    execution_epoch: int,
    slippage_per_leg_pips: float,
) -> tuple[PortfolioState, ExecutionLeg]:
    if state.position is None:
        raise ValueError("cannot close a flat portfolio")
    position = state.position
    market = markets[position.instrument]
    bid, ask, spread = _quote(market, execution_epoch)
    if position.side > 0:
        raw_price = bid
        executed = bid - float(slippage_per_leg_pips) * market.pip
    else:
        raw_price = ask
        executed = ask + float(slippage_per_leg_pips) * market.pip
    realized = position.side * (executed - position.entry_price) / market.pip * position.units
    leg = ExecutionLeg(
        "close",
        position.instrument,
        position.side,
        position.units,
        int(execution_epoch),
        raw_price,
        executed,
        spread,
        float(slippage_per_leg_pips),
        realized,
        position.thesis_id,
    )
    return PortfolioState(state.realized_pips + realized, None), leg


def _open_position(
    state: PortfolioState,
    decision: Decision,
    markets: Mapping[str, MarketSeries],
    execution_epoch: int,
    slippage_per_leg_pips: float,
    decision_clock_id: str,
) -> tuple[PortfolioState, ExecutionLeg]:
    if not state.flat or decision.instrument is None or decision.side not in ALLOWED_SIDES:
        raise ValueError("invalid open request")
    market = markets[decision.instrument]
    bid, ask, spread = _quote(market, execution_epoch)
    if decision.side > 0:
        raw_price = ask
        executed = ask + float(slippage_per_leg_pips) * market.pip
    else:
        raw_price = bid
        executed = bid - float(slippage_per_leg_pips) * market.pip
    thesis = "thesis_" + stable_hash(
        "aligned_multiscale_momentum_rank_v1",
        decision.instrument,
        decision.side,
        decision_clock_id,
    )[:28]
    position = Position(
        decision.instrument,
        int(decision.side),
        int(decision.units),
        int(execution_epoch),
        executed,
        raw_price,
        spread,
        float(slippage_per_leg_pips),
        decision_clock_id,
        thesis,
    )
    leg = ExecutionLeg(
        "open",
        decision.instrument,
        int(decision.side),
        int(decision.units),
        int(execution_epoch),
        raw_price,
        executed,
        spread,
        float(slippage_per_leg_pips),
        0.0,
        thesis,
    )
    return PortfolioState(state.realized_pips, position), leg


def apply_decision(
    state: PortfolioState,
    decision: Decision,
    markets: Mapping[str, MarketSeries],
    *,
    execution_epoch: int,
    slippage_per_leg_pips: float,
    decision_clock_id: str,
    maximum_entry_spread_pips: float | None = None,
) -> AppliedAction:
    validate_decision(state, decision)
    if decision.action in {"wait", "hold"}:
        return AppliedAction(state, (), 0.0)
    if decision.action in {"enter", "rotate"}:
        assert decision.instrument is not None
        _, _, opening_spread = _quote(markets[decision.instrument], execution_epoch)
        if (
            maximum_entry_spread_pips is not None
            and opening_spread > float(maximum_entry_spread_pips)
        ):
            return AppliedAction(
                state,
                (),
                0.0,
                "rejected",
                "execution_spread_above_limit",
            )
    if decision.action == "exit":
        new_state, close_leg = _close_position(state, markets, execution_epoch, slippage_per_leg_pips)
        return AppliedAction(new_state, (close_leg,), close_leg.realized_pips)
    if decision.action == "enter":
        new_state, open_leg = _open_position(state, decision, markets, execution_epoch, slippage_per_leg_pips, decision_clock_id)
        return AppliedAction(new_state, (open_leg,), 0.0)
    closed_state, close_leg = _close_position(state, markets, execution_epoch, slippage_per_leg_pips)
    opened_state, open_leg = _open_position(closed_state, decision, markets, execution_epoch, slippage_per_leg_pips, decision_clock_id)
    return AppliedAction(opened_state, (close_leg, open_leg), close_leg.realized_pips)


def liquidation_equity(
    state: PortfolioState,
    markets: Mapping[str, MarketSeries],
    *,
    epoch: int,
    slippage_per_leg_pips: float,
) -> float:
    if state.flat:
        return float(state.realized_pips)
    closed, _ = _close_position(state, markets, int(epoch), float(slippage_per_leg_pips))
    return float(closed.realized_pips)


def causal_snapshot(
    markets: Mapping[str, MarketSeries],
    candidates: Sequence[Candidate],
    state: PortfolioState,
    decision_epoch: int,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    candle_epoch = int(decision_epoch) - 60
    prices: dict[str, Any] = {}
    for instrument, market in sorted(markets.items()):
        index = market.exact_index(candle_epoch)
        if index is None:
            raise ValueError(f"missing causal candle: {instrument}:{candle_epoch}")
        prices[instrument] = {
            "bid_close": float(market.bid_close[index]),
            "ask_close": float(market.ask_close[index]),
            "source_sha256": market.source_sha256,
        }
    candidate_payload = [asdict(row) for row in candidates]
    availability = {
        "price": True,
        "news": config["source_snapshots"].get("news_snapshot_id") is not None,
        "rates": config["source_snapshots"].get("rates_snapshot_id") is not None,
        "levels": config["source_snapshots"].get("levels_snapshot_id") is not None,
        "positioning": config["source_snapshots"].get("positioning_snapshot_id") is not None,
    }
    price_snapshot_id = "pricesnapshot_" + stable_hash(candle_epoch, prices)[:28]
    exact_payload = {
        "decision_epoch": int(decision_epoch),
        "knowledge_cutoff_epoch": int(decision_epoch),
        "price_snapshot_id": price_snapshot_id,
        "prices": prices,
        "candidates": candidate_payload,
        "portfolio": asdict(state),
        "availability": availability,
        "news_snapshot_id": config["source_snapshots"].get("news_snapshot_id"),
        "rates_snapshot_id": config["source_snapshots"].get("rates_snapshot_id"),
        "levels_snapshot_id": config["source_snapshots"].get("levels_snapshot_id"),
        "positioning_snapshot_id": config["source_snapshots"].get("positioning_snapshot_id"),
    }
    coarse = {
        "session": session_bucket(decision_epoch),
        "candidate_count": len(candidates),
        "candidate_directions": [f"{row.instrument}:{row.side}" for row in candidates],
        "candidate_score_buckets": [round(row.score, 1) for row in candidates],
        "liquidity_buckets": [row.liquidity_bucket for row in candidates],
        "portfolio": "flat" if state.flat else f"{state.position.instrument}:{state.position.side}",
        "availability": availability,
    }
    exact_payload["exact_situation_id"] = "situation_exact_" + stable_hash(exact_payload)[:28]
    exact_payload["coarse_situation_id"] = "situation_coarse_" + stable_hash(coarse)[:28]
    return exact_payload


def feedback_components(
    *,
    primary: Decision,
    state_before: PortfolioState,
    primary_equity: float,
    alternative_equities: Mapping[str, float],
    candidate: Candidate | None,
) -> dict[str, Any]:
    best_alternative = max(alternative_equities.values()) if alternative_equities else primary_equity
    opportunity_delta = primary_equity - best_alternative
    cost_clear = primary_equity > state_before.realized_pips
    directional = None
    if primary.action in {"enter", "rotate"}:
        directional = 1.0 if cost_clear else 0.0
    entry = opportunity_delta if primary.action == "enter" else None
    management = opportunity_delta if primary.action == "hold" else None
    exit_value = opportunity_delta if primary.action == "exit" else None
    rotation = opportunity_delta if primary.action == "rotate" else None
    probability = primary.confidence
    brier = None
    if probability is not None and directional is not None:
        brier = (float(probability) - directional) ** 2
    return {
        "direction_score": directional,
        "entry_value_pips": entry,
        "management_value_pips": management,
        "exit_value_pips": exit_value,
        "rotation_value_pips": rotation,
        "opportunity_cost_pips": min(0.0, opportunity_delta),
        "delta_vs_best_alternative_pips": opportunity_delta,
        "cost_clear": cost_clear,
        "predicted_confidence": probability,
        "brier": brier,
        "candidate_score": candidate.score if candidate is not None else None,
    }


def decision_payload(decision: Decision) -> dict[str, Any]:
    return asdict(decision)


def state_payload(state: PortfolioState) -> dict[str, Any]:
    return asdict(state)


def leg_payload(leg: ExecutionLeg) -> dict[str, Any]:
    return asdict(leg)


def unsigned_currency_resources(instrument: str | None) -> tuple[str, ...]:
    if not instrument:
        return ()
    base, quote = str(instrument).upper().split("_", 1)
    return tuple(sorted((base, quote)))


def physical_path_id(
    instrument: str | None,
    execution_epoch: int,
    feedback_epoch: int,
    markets: Mapping[str, MarketSeries],
) -> str:
    if not instrument:
        return "unassigned"
    market = markets[str(instrument)]
    return "physical_path_" + stable_hash(
        market.instrument,
        int(execution_epoch),
        int(feedback_epoch),
        market.source_sha256,
    )[:28]


__all__ = [
    "ALLOWED_ACTIONS",
    "AppliedAction",
    "Candidate",
    "Decision",
    "ExecutionLeg",
    "MarketSeries",
    "POLICY",
    "PortfolioState",
    "Position",
    "apply_decision",
    "build_global_clocks",
    "candidate_set",
    "canonical_json",
    "causal_snapshot",
    "choose_primary_decision",
    "decision_payload",
    "feedback_components",
    "leg_payload",
    "legal_branches",
    "liquidation_equity",
    "load_archived_market",
    "market_episode_id",
    "physical_path_id",
    "session_bucket",
    "stable_hash",
    "state_payload",
    "unsigned_currency_resources",
    "validate_decision",
]
