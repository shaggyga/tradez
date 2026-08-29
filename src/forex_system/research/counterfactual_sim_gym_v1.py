"""Frozen primitives for the research-only counterfactual SIM gym.

The module contains no broker, account, lifecycle, authorization, promotion, or
signal-feed client.  It turns completed OANDA M1 bid/ask candles into causal
virtual order intents and executable after-cost outcomes.  Historical results
are diagnostics only and can never become prospective confirmation evidence.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

try:
    from oanda_instrument_pips import fallback_pip_size
except ModuleNotFoundError as exc:  # pragma: no cover - fail closed outside repo runtime.
    raise RuntimeError("authoritative OANDA instrument pip map is required") from exc


CONTRACT_ID = "counterfactual_sim_gym_v1_20260829"
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


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_hash(*parts: Any) -> str:
    return sha256_bytes("\x1f".join(canonical_json(part) for part in parts).encode("utf-8"))


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def file_prefix_sha256(
    path: Path, byte_count: int, chunk_size: int = 8 * 1024 * 1024
) -> str:
    """Hash an immutable captured prefix while permitting later appends."""

    remaining = int(byte_count)
    if remaining < 0:
        raise ValueError("negative prefix byte count")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while remaining:
            chunk = source.read(min(chunk_size, remaining))
            if not chunk:
                raise ValueError(f"file shorter than frozen prefix: {path}")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def finite(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def parse_epoch(value: Any) -> int:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("naive candle timestamp")
    return int(parsed.astimezone(timezone.utc).timestamp())


def iso(epoch: int | float) -> str:
    return datetime.fromtimestamp(float(epoch), timezone.utc).isoformat()


def liquidity_bucket(spread_pips: float) -> str:
    spread = float(spread_pips)
    if spread <= 2.0:
        return "liquid_le_2"
    if spread <= 3.0:
        return "normal_2_to_3"
    if spread <= 5.0:
        return "elevated_3_to_5"
    return "wide_gt_5"


def session_bucket(epoch: int) -> str:
    hour = datetime.fromtimestamp(int(epoch), timezone.utc).hour
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "london_new_york_overlap"
    if 16 <= hour < 21:
        return "new_york"
    if 21 <= hour or hour < 7:
        return "asia_or_rollover"
    return "other"


def signed_currency_factors(instrument: str, side: int) -> tuple[str, str]:
    base, quote = str(instrument).upper().split("_", 1)
    direction = 1 if int(side) > 0 else -1
    return (
        f"{base}:{'+' if direction > 0 else '-'}",
        f"{quote}:{'-' if direction > 0 else '+'}",
    )


def market_episode_id(decision_epoch: int, episode_min: int) -> str:
    width = max(1, int(episode_min)) * 60
    return f"market_episode_{int(decision_epoch) // width}"


@dataclass(frozen=True)
class PairMarket:
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
    source_prefix_bytes: int

    def exact_index(self, epoch: int) -> int | None:
        location = int(np.searchsorted(self.epochs, int(epoch)))
        if location < len(self.epochs) and int(self.epochs[location]) == int(epoch):
            return location
        return None


@dataclass(frozen=True)
class SignalObservation:
    rule_id: str
    side: int
    score: float
    feature_value_pips: float
    knowledge_cutoff_epoch: int
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class ArmDefinition:
    arm_id: str
    signal_rule_id: str
    comparator: str
    entry_delay_min: int
    horizon_min: int
    reentry_gap_min: int
    exit_policy: Mapping[str, Any]
    definition_sha256: str


@dataclass(frozen=True)
class SimulationResult:
    status: str
    exclusion_reason: str
    entry_epoch: int | None
    exit_epoch: int | None
    entry_bid: float | None
    entry_ask: float | None
    exit_bid: float | None
    exit_ask: float | None
    exit_price_kind: str
    observed_exit_bid: float | None
    observed_exit_ask: float | None
    entry_spread_pips: float | None
    exit_spread_pips: float | None
    gross_mid_pips: float | None
    executable_net_pips: float | None
    terminal_endpoint_net_pips: float | None
    moderate_stress_net_pips: float | None
    severe_stress_net_pips: float | None
    mfe_pips: float | None
    mae_pips: float | None
    first_positive_sec: int | None
    hold_sec: int | None
    exit_reason: str
    missed_entry_slippage_pips: float | None
    mistake_clusters: tuple[str, ...]


def _excluded_result(
    status: str,
    reason: str,
    *,
    entry_epoch: int | None = None,
    entry_bid: float | None = None,
    entry_ask: float | None = None,
    entry_spread_pips: float | None = None,
) -> SimulationResult:
    """Construct a fail-closed non-fill without positional-field drift."""

    return SimulationResult(
        status=status,
        exclusion_reason=reason,
        entry_epoch=entry_epoch,
        exit_epoch=None,
        entry_bid=entry_bid,
        entry_ask=entry_ask,
        exit_bid=None,
        exit_ask=None,
        exit_price_kind="unavailable",
        observed_exit_bid=None,
        observed_exit_ask=None,
        entry_spread_pips=entry_spread_pips,
        exit_spread_pips=None,
        gross_mid_pips=None,
        executable_net_pips=None,
        terminal_endpoint_net_pips=None,
        moderate_stress_net_pips=None,
        severe_stress_net_pips=None,
        mfe_pips=None,
        mae_pips=None,
        first_positive_sec=None,
        hold_sec=None,
        exit_reason=status,
        missed_entry_slippage_pips=None,
        mistake_clusters=(reason,),
    )


def load_pair_market(path: Path) -> PairMarket:
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
    captured_size = int(path.stat().st_size)
    with path.open("rb") as raw_source:
        captured = raw_source.read(captured_size)
    if len(captured) != captured_size:
        raise ValueError(f"source changed during prefix capture: {path}")
    if not captured.endswith((b"\n", b"\r")):
        boundary = max(captured.rfind(b"\n"), captured.rfind(b"\r"))
        if boundary < 0:
            raise ValueError(f"source has no complete CSV record: {path}")
        captured = captured[: boundary + 1]
    captured_sha256 = sha256_bytes(captured)
    rows: list[tuple[Any, ...]] = []
    with io.StringIO(captured.decode("utf-8"), newline="") as source:
        reader = csv.DictReader(source)
        missing = sorted(set(required) - set(reader.fieldnames or ()))
        if missing:
            raise ValueError(f"missing candle columns: {missing}")
        for row_number, raw in enumerate(reader, start=2):
            try:
                time_epoch = parse_epoch(raw.get("time"))
                datetime_epoch = parse_epoch(raw.get("datetime"))
                instrument = str(raw.get("instrument") or "")
                granularity = str(raw.get("granularity") or "")
                numeric = tuple(float(raw[name]) for name in required[4:])
            except (TypeError, ValueError, KeyError) as exc:
                raise ValueError(f"malformed candle row {path}:{row_number}") from exc
            if (
                time_epoch != datetime_epoch
                or time_epoch % 60
                or not instrument
                or granularity != "M1"
                or any(not math.isfinite(value) or value <= 0.0 for value in numeric)
            ):
                raise ValueError(f"invalid candle contract row {path}:{row_number}")
            rows.append((time_epoch, instrument, *numeric))
    if not rows:
        raise ValueError(f"empty market file: {path}")
    rows.sort(key=lambda value: int(value[0]))
    deduplicated: dict[int, tuple[Any, ...]] = {}
    for row in rows:
        epoch = int(row[0])
        previous = deduplicated.get(epoch)
        if previous is not None:
            raise ValueError(f"duplicate candle: {path}:{iso(epoch)}")
        deduplicated[epoch] = row
    ordered = [deduplicated[key] for key in sorted(deduplicated)]
    instruments = {str(row[1]) for row in ordered}
    expected = path.name.removesuffix("_M1.csv")
    if instruments != {expected}:
        raise ValueError(f"instrument identity mismatch: {expected}: {sorted(instruments)}")
    matrix = np.asarray([row[2:] for row in ordered], dtype=np.float64)
    epochs = np.asarray([row[0] for row in ordered], dtype=np.int64)
    if np.any(matrix[:, 4:8] <= matrix[:, 0:4]):
        raise ValueError(f"crossed or zero candle quote: {expected}")
    for offset in (0, 4):
        opens = matrix[:, offset]
        highs = matrix[:, offset + 1]
        lows = matrix[:, offset + 2]
        closes = matrix[:, offset + 3]
        if np.any(highs < np.maximum(opens, closes)) or np.any(lows > np.minimum(opens, closes)):
            raise ValueError(f"invalid candle OHLC envelope: {expected}")
    return PairMarket(
        instrument=expected,
        pip=float(fallback_pip_size(expected)),
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
        source_sha256=captured_sha256,
        source_prefix_bytes=len(captured),
    )


def _contiguous(market: PairMarket, start_index: int, end_index: int) -> bool:
    if start_index < 0 or end_index >= len(market.epochs) or end_index < start_index:
        return False
    expected = (end_index - start_index) * 60
    return int(market.epochs[end_index]) - int(market.epochs[start_index]) == expected


def _side(value: float) -> int:
    # A floating-point rounding artifact is not a market direction.  The
    # tolerance is many orders below the smallest executable pip move but
    # prevents an exact flat average from becoming a synthetic signal.
    if abs(float(value)) <= 1e-9:
        return 0
    return 1 if value > 0.0 else -1


def evaluate_signal(
    market: PairMarket,
    candle_index: int,
    rule: Mapping[str, Any],
    *,
    round_trip_slippage_pips: float,
) -> SignalObservation | None:
    """Evaluate one rule using only the completed candle and its past."""

    index = int(candle_index)
    if index < 1 or index >= len(market.epochs):
        return None
    decision_epoch = int(market.epochs[index]) + 60
    current = float(market.mid_close[index])
    spread = (float(market.ask_close[index]) - float(market.bid_close[index])) / market.pip
    cost = max(0.01, spread + max(0.0, float(round_trip_slippage_pips)))
    kind = str(rule.get("kind") or "")
    rule_id = str(rule.get("id") or kind)
    metadata: dict[str, Any] = {"kind": kind, "decision_candle_epoch": int(market.epochs[index])}

    if kind in {"momentum", "reversion"}:
        lookback = int(rule.get("lookback_min") or 0)
        prior_index = index - lookback
        if lookback <= 0 or not _contiguous(market, prior_index, index):
            return None
        move_pips = (current - float(market.mid_close[prior_index])) / market.pip
        signal_side = _side(move_pips)
        if kind == "reversion":
            signal_side *= -1
        ratio = abs(move_pips) / cost
        if signal_side == 0 or ratio < float(rule.get("minimum_move_cost_ratio") or 0.0):
            return None
        metadata.update({"lookback_min": lookback, "trailing_move_pips": move_pips})
        return SignalObservation(rule_id, signal_side, ratio, move_pips, decision_epoch, metadata)

    if kind == "moving_average_spread":
        fast = int(rule.get("fast_min") or 0)
        slow = int(rule.get("slow_min") or 0)
        if fast <= 0 or slow <= fast or not _contiguous(market, index - slow + 1, index):
            return None
        fast_mean = float(np.mean(market.mid_close[index - fast + 1 : index + 1]))
        slow_mean = float(np.mean(market.mid_close[index - slow + 1 : index + 1]))
        difference_pips = (fast_mean - slow_mean) / market.pip
        signal_side = _side(difference_pips)
        ratio = abs(difference_pips) / cost
        if signal_side == 0 or ratio < float(rule.get("minimum_move_cost_ratio") or 0.0):
            return None
        metadata.update({"fast_min": fast, "slow_min": slow, "ma_difference_pips": difference_pips})
        return SignalObservation(rule_id, signal_side, ratio, difference_pips, decision_epoch, metadata)

    if kind == "level_reaction":
        window = int(rule.get("window_min") or 0)
        approach = int(rule.get("approach_min") or 0)
        start = index - window
        approach_index = index - approach
        if window <= 1 or approach <= 0 or not _contiguous(market, start, index):
            return None
        history = market.mid_close[start:index]
        support = float(np.min(history))
        resistance = float(np.max(history))
        approach_move = (current - float(market.mid_close[approach_index])) / market.pip
        support_distance = abs(current - support) / market.pip
        resistance_distance = abs(resistance - current) / market.pip
        maximum_distance = cost * float(rule.get("maximum_distance_cost_multiple") or 1.0)
        physical_side = 0
        level_kind = ""
        distance = math.inf
        if approach_move < 0.0 and support_distance <= maximum_distance:
            physical_side, level_kind, distance = -1, "support", support_distance
        elif approach_move > 0.0 and resistance_distance <= maximum_distance:
            physical_side, level_kind, distance = 1, "resistance", resistance_distance
        if physical_side == 0:
            return None
        mode = str(rule.get("mode") or "bounce")
        signal_side = -physical_side if mode == "bounce" else physical_side
        score = (abs(approach_move) + max(0.0, maximum_distance - distance)) / cost
        metadata.update(
            {
                "window_min": window,
                "approach_min": approach,
                "mode": mode,
                "level_kind": level_kind,
                "level_price": support if level_kind == "support" else resistance,
                "distance_to_level_pips": distance,
                "approach_move_pips": approach_move,
            }
        )
        return SignalObservation(rule_id, signal_side, score, approach_move, decision_epoch, metadata)

    raise ValueError(f"unsupported signal kind: {kind}")


def _arm_definition(
    signal_rule_id: str,
    comparator: str,
    entry_delay_min: int,
    horizon_min: int,
    reentry_gap_min: int,
    exit_policy: Mapping[str, Any],
) -> ArmDefinition:
    definition = {
        "signal_rule_id": signal_rule_id,
        "comparator": comparator,
        "entry_delay_min": int(entry_delay_min),
        "horizon_min": int(horizon_min),
        "reentry_gap_min": int(reentry_gap_min),
        "exit_policy": dict(exit_policy),
    }
    digest = stable_hash(CONTRACT_ID, definition)
    return ArmDefinition(
        arm_id="simarm_" + digest[:28],
        definition_sha256=digest,
        **definition,
    )


def build_arm_definitions(config: Mapping[str, Any]) -> list[ArmDefinition]:
    grid = config.get("execution_grid") or {}
    rules = [str(row["id"]) for row in config.get("signal_rules") or []]
    delays = [int(value) for value in grid.get("entry_delay_min") or []]
    horizons = [int(value) for value in grid.get("horizon_min") or []]
    gaps = [int(value) for value in grid.get("reentry_gap_min") or [0]]
    policies = [dict(value) for value in grid.get("exit_policies") or []]
    arms: list[ArmDefinition] = []
    for rule_id in rules:
        for delay in delays:
            for horizon in horizons:
                for gap in gaps:
                    for policy in policies:
                        arms.append(_arm_definition(rule_id, "as_signaled", delay, horizon, gap, policy))
    comparator = config.get("comparators") or {}
    for comparator_id in ("flipped_direction", "deterministic_random_side"):
        if not (comparator.get(comparator_id) or {}).get("enabled"):
            continue
        for rule_id in rules:
            for delay in delays:
                for horizon in horizons:
                    for gap in gaps:
                        for policy in policies:
                            arms.append(
                                _arm_definition(
                                    rule_id,
                                    comparator_id,
                                    delay,
                                    horizon,
                                    gap,
                                    policy,
                                )
                            )
    unique = {arm.arm_id: arm for arm in arms}
    if len(unique) != len(arms):
        raise ValueError("duplicate arm definitions")
    return sorted(arms, key=lambda value: value.arm_id)


def deterministic_random_side(decision_id: str) -> int:
    return 1 if int(stable_hash("random_side", decision_id)[:16], 16) % 2 == 0 else -1


def historical_partition(epoch: int, first_epoch: int, last_epoch: int, config: Mapping[str, Any]) -> str:
    partition = config.get("historical_partitions") or {}
    labels = list(partition.get("labels") or [])
    fractions = [float(value) for value in partition.get("fractions") or []]
    if len(labels) != len(fractions) or not labels or abs(sum(fractions) - 1.0) > 1e-9:
        raise ValueError("invalid historical partition contract")
    if last_epoch <= first_epoch:
        return str(labels[0])
    location = min(1.0, max(0.0, (int(epoch) - int(first_epoch)) / (last_epoch - first_epoch)))
    cumulative = 0.0
    for label, fraction in zip(labels, fractions):
        cumulative += fraction
        if location <= cumulative + 1e-12:
            return str(label)
    return str(labels[-1])


def historical_partition_interval(
    interval_start_epoch: int,
    interval_end_epoch: int,
    first_epoch: int,
    last_epoch: int,
    config: Mapping[str, Any],
) -> str | None:
    """Assign a purged block using the full forecast influence interval.

    An observation crossing a split boundary is excluded.  Later blocks begin
    only after the frozen embargo, so their features and outcomes cannot share
    the preceding block's market path.
    """

    partition = config.get("historical_partitions") or {}
    labels = [str(value) for value in partition.get("labels") or []]
    fractions = [float(value) for value in partition.get("fractions") or []]
    if len(labels) != len(fractions) or not labels or abs(sum(fractions) - 1.0) > 1e-9:
        raise ValueError("invalid historical partition contract")
    start = int(interval_start_epoch)
    end = int(interval_end_epoch)
    first = int(first_epoch)
    last = int(last_epoch)
    if start < first or end <= start or end > last:
        return None
    span = last - first
    boundaries: list[int] = []
    cumulative = 0.0
    for fraction in fractions[:-1]:
        cumulative += fraction
        boundaries.append(first + int(span * cumulative))
    embargo = max(0, int(partition.get("embargo_min") or 0)) * 60
    block_starts = [first, *(boundary + embargo for boundary in boundaries)]
    block_ends = [*boundaries, last]
    for label, block_start, block_end in zip(labels, block_starts, block_ends):
        if start >= block_start and end <= block_end:
            return label
    return None


def simulate_order(
    market: PairMarket,
    *,
    decision_epoch: int,
    side: int,
    entry_delay_min: int,
    horizon_min: int,
    exit_policy: Mapping[str, Any],
    round_trip_slippage_pips: float,
    moderate_stress_slippage_pips: float,
    severe_stress_slippage_pips: float,
    maximum_entry_spread_pips: float | None = None,
    moderate_spread_multiplier: float = 1.25,
    severe_spread_multiplier: float = 1.5,
    latency_decay_threshold_pips: float = 1.0,
    extension_threshold_pips: float = 2.0,
) -> SimulationResult:
    direction = 1 if int(side) > 0 else -1
    immediate_index = market.exact_index(int(decision_epoch))
    entry_epoch = int(decision_epoch) + max(0, int(entry_delay_min)) * 60
    entry_index = market.exact_index(entry_epoch)
    if immediate_index is None or entry_index is None:
        return _excluded_result("not_filled", "missing_exact_entry_quote")
    horizon = max(1, int(horizon_min))
    final_index = entry_index + horizon - 1
    if not _contiguous(market, entry_index, final_index):
        return _excluded_result(
            "invalid", "incomplete_exact_m1_path", entry_epoch=entry_epoch
        )
    pip = market.pip
    entry_bid = float(market.bid_open[entry_index])
    entry_ask = float(market.ask_open[entry_index])
    entry_spread = (entry_ask - entry_bid) / pip
    if entry_spread <= 0.0:
        return _excluded_result(
            "invalid",
            "crossed_or_zero_entry_quote",
            entry_epoch=entry_epoch,
            entry_bid=entry_bid,
            entry_ask=entry_ask,
            entry_spread_pips=entry_spread,
        )
    if (
        maximum_entry_spread_pips is not None
        and entry_spread > float(maximum_entry_spread_pips)
    ):
        return _excluded_result(
            "invalid",
            "entry_spread_above_limit",
            entry_epoch=entry_epoch,
            entry_bid=entry_bid,
            entry_ask=entry_ask,
            entry_spread_pips=entry_spread,
        )
    indices = range(entry_index, final_index + 1)
    if direction > 0:
        executable_close = np.asarray(
            [(float(market.bid_close[i]) - entry_ask) / pip for i in indices], dtype=float
        )
        favorable = np.asarray(
            [(float(market.bid_high[i]) - entry_ask) / pip for i in indices], dtype=float
        )
        adverse = np.asarray(
            [(float(market.bid_low[i]) - entry_ask) / pip for i in indices], dtype=float
        )
        missed_entry_slippage = (
            entry_ask - float(market.ask_open[immediate_index])
        ) / pip
    else:
        executable_close = np.asarray(
            [(entry_bid - float(market.ask_close[i])) / pip for i in indices], dtype=float
        )
        favorable = np.asarray(
            [(entry_bid - float(market.ask_low[i])) / pip for i in indices], dtype=float
        )
        adverse = np.asarray(
            [(entry_bid - float(market.ask_high[i])) / pip for i in indices], dtype=float
        )
        missed_entry_slippage = (
            float(market.bid_open[immediate_index]) - entry_bid
        ) / pip
    slippage = max(0.0, float(round_trip_slippage_pips))
    executable_close -= slippage
    favorable -= slippage
    adverse -= slippage
    exit_index = final_index
    value = float(executable_close[-1])
    exit_reason = "endpoint"
    hold_sec = horizon * 60
    stop = finite(exit_policy.get("stop_pips"))
    if stop is not None and float(stop) <= entry_spread + slippage:
        return _excluded_result(
            "invalid",
            "stop_inside_entry_market",
            entry_epoch=entry_epoch,
            entry_bid=entry_bid,
            entry_ask=entry_ask,
            entry_spread_pips=entry_spread,
        )
    initial_stop_level = -max(0.0, float(stop)) if stop is not None else -math.inf
    stop_level = initial_stop_level
    target = finite(exit_policy.get("target_pips"))
    trailing = str(exit_policy.get("kind") or "") == "trailing"
    market_buffer = max(
        0.5,
        entry_spread * max(0.0, float(exit_policy.get("spread_multiple") or 0.0)),
    )
    for offset in range(horizon):
        if float(adverse[offset]) <= stop_level:
            value = float(stop_level)
            exit_index = entry_index + offset
            exit_reason = "trailing_stop" if trailing and stop_level > initial_stop_level else "stop"
            hold_sec = (offset + 1) * 60
            break
        if target is not None and float(favorable[offset]) >= target:
            value = float(target)
            exit_index = entry_index + offset
            exit_reason = "target"
            hold_sec = (offset + 1) * 60
            break
        if trailing:
            current = float(executable_close[offset])
            floor = max(0.0, float(exit_policy.get("floor_pips") or 0.0))
            trigger = max(0.0, float(exit_policy.get("activation_pips") or 0.0))
            distance = max(0.0, float(exit_policy.get("trail_distance_pips") or 0.0))
            step = max(0.0, float(exit_policy.get("step_pips") or 0.0))
            if current >= max(trigger, floor + market_buffer):
                desired = min(current - market_buffer, max(floor, current - distance))
                if desired >= floor and desired > stop_level + step:
                    stop_level = desired
    observed_exit_bid = float(market.bid_close[exit_index])
    observed_exit_ask = float(market.ask_close[exit_index])
    observed_exit_spread = (observed_exit_ask - observed_exit_bid) / pip
    if exit_reason == "endpoint":
        exit_bid, exit_ask = observed_exit_bid, observed_exit_ask
        exit_price_kind = "observed_m1_close"
    elif direction > 0:
        exit_bid = entry_ask + (value + slippage) * pip
        exit_ask = exit_bid + observed_exit_spread * pip
        exit_price_kind = "modeled_adverse_first_barrier_fill"
    else:
        exit_ask = entry_bid - (value + slippage) * pip
        exit_bid = exit_ask - observed_exit_spread * pip
        exit_price_kind = "modeled_adverse_first_barrier_fill"
    exit_spread = (exit_ask - exit_bid) / pip
    entry_mid = (entry_bid + entry_ask) / 2.0
    exit_mid = (exit_bid + exit_ask) / 2.0
    gross_mid = direction * (exit_mid - entry_mid) / pip
    terminal_endpoint = float(executable_close[-1])
    moderate = value - max(0.0, float(moderate_stress_slippage_pips) - slippage)
    moderate -= max(0.0, moderate_spread_multiplier - 1.0) * (entry_spread + exit_spread) / 2.0
    severe = value - max(0.0, float(severe_stress_slippage_pips) - slippage)
    severe -= max(0.0, severe_spread_multiplier - 1.0) * (entry_spread + exit_spread) / 2.0
    held_offsets = exit_index - entry_index + 1
    held_close = executable_close[:held_offsets]
    held_favorable = favorable[:held_offsets]
    held_adverse = adverse[:held_offsets]
    positive = np.flatnonzero(held_close > 0.0)
    first_positive = int((int(positive[0]) + 1) * 60) if positive.size else None
    clusters: list[str] = []
    if exit_reason in {"stop", "trailing_stop"} and terminal_endpoint > 0.0:
        clusters.append("stopped_then_recovered")
    if exit_reason in {"target", "trailing_stop"} and value > terminal_endpoint + extension_threshold_pips:
        clusters.append("exit_added_value")
    if gross_mid > 0.0 and value <= 0.0:
        clusters.append("cost_consumed_move")
    elif gross_mid <= 0.0:
        clusters.append("wrong_direction")
    elif value > 0.0:
        clusters.append("captured_after_cost")
    if missed_entry_slippage > latency_decay_threshold_pips:
        clusters.append("latency_decay")
    if not clusters:
        clusters.append("uncategorized")
    return SimulationResult(
        status="filled",
        exclusion_reason="",
        entry_epoch=entry_epoch,
        exit_epoch=int(market.epochs[exit_index]) + 60,
        entry_bid=entry_bid,
        entry_ask=entry_ask,
        exit_bid=exit_bid,
        exit_ask=exit_ask,
        exit_price_kind=exit_price_kind,
        observed_exit_bid=observed_exit_bid,
        observed_exit_ask=observed_exit_ask,
        entry_spread_pips=entry_spread,
        exit_spread_pips=exit_spread,
        gross_mid_pips=gross_mid,
        executable_net_pips=value,
        terminal_endpoint_net_pips=terminal_endpoint,
        moderate_stress_net_pips=moderate,
        severe_stress_net_pips=severe,
        mfe_pips=float(np.max(held_favorable)),
        mae_pips=float(np.min(held_adverse)),
        first_positive_sec=first_positive,
        hold_sec=hold_sec,
        exit_reason=exit_reason,
        missed_entry_slippage_pips=missed_entry_slippage,
        mistake_clusters=tuple(sorted(set(clusters))),
    )


def collapse_effective_n(rows: Iterable[Mapping[str, Any]]) -> int:
    """Collapse same-episode rows connected by a signed currency factor.

    A set of EUR_JPY, USD_JPY, and GBP_JPY expressions sharing ``JPY:+`` in
    one market episode therefore contributes one independent factor episode,
    even if thousands of virtual variants were evaluated.
    """

    by_episode: dict[str, list[set[str]]] = {}
    for row in rows:
        episode = str(row.get("market_episode_id") or "")
        raw = row.get("signed_currency_factors") or ()
        factors = {str(value) for value in raw if str(value)}
        if not episode or not factors:
            continue
        by_episode.setdefault(episode, []).append(factors)
    total = 0
    for groups in by_episode.values():
        components: list[set[str]] = []
        for factors in groups:
            matched = [index for index, value in enumerate(components) if not value.isdisjoint(factors)]
            if not matched:
                components.append(set(factors))
                continue
            merged = set(factors)
            for index in reversed(matched):
                merged.update(components.pop(index))
            changed = True
            while changed:
                changed = False
                for index in range(len(components) - 1, -1, -1):
                    if not components[index].isdisjoint(merged):
                        merged.update(components.pop(index))
                        changed = True
            components.append(merged)
        total += len(components)
    return total


def arm_as_dict(arm: ArmDefinition) -> dict[str, Any]:
    return asdict(arm)


__all__ = [
    "ArmDefinition",
    "CONTRACT_ID",
    "POLICY",
    "PairMarket",
    "SignalObservation",
    "SimulationResult",
    "arm_as_dict",
    "build_arm_definitions",
    "canonical_json",
    "collapse_effective_n",
    "deterministic_random_side",
    "evaluate_signal",
    "file_sha256",
    "file_prefix_sha256",
    "historical_partition",
    "historical_partition_interval",
    "iso",
    "liquidity_bucket",
    "load_pair_market",
    "market_episode_id",
    "session_bucket",
    "sha256_bytes",
    "signed_currency_factors",
    "simulate_order",
    "stable_hash",
]
