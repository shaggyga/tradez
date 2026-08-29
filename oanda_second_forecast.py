#!/usr/bin/env python3
"""Low-latency, causal FX forecasts built from one-second quote snapshots.

The live path performs no model fitting and no historical-data reads. A compact
JSON ridge snapshot is loaded into memory, while sampled forecasts and their
forward outcomes are written to a small independent SQLite ledger.
"""

from __future__ import annotations

import heapq
import json
import math
import os
import sqlite3
import statistics
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


SCHEMA_VERSION = 1
MODEL_FAMILY = "ridge_return"
INPUT_TIMEFRAME = "S1"
TRAINING_TIMEFRAME = "S5"
FEATURE_NAMES = (
    "return_5_pips",
    "return_10_pips",
    "return_30_pips",
    "return_60_pips",
    "acceleration_5_30",
    "volatility_30_pips",
    "volatility_60_pips",
    "range_30_pips",
    "spread_pips",
    "spread_ratio_60",
    "activity_30",
    "activity_ratio_30_60",
    "hour_sin",
    "hour_cos",
)
DEFAULT_HORIZONS_SEC = (
    15,
    30,
    60,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
    7200,
    10800,
    14400,
)
DEFAULT_EXECUTION_HORIZONS_SEC = (
    60,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
    7200,
    10800,
    14400,
)
PROFILE_EXECUTION = {
    "fast": {"stop_loss_pips": 4.0, "take_profit_r": 1.4},
    "balanced": {"stop_loss_pips": 5.0, "take_profit_r": 1.6},
    "strict": {"stop_loss_pips": 6.0, "take_profit_r": 1.8},
}


def matrix_lane_id(profile: str) -> str:
    """Return the horizon-independent lane key used by the unified matrix."""
    return f"{MODEL_FAMILY}.{INPUT_TIMEFRAME.lower()}.{profile}"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def population_std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    count = float(len(values))
    mean = sum(values) / count
    variance = sum(value * value for value in values) / count - mean * mean
    return math.sqrt(max(0.0, variance))


def _bounded_normalized_return(features: dict[str, Any], key: str) -> float:
    scale = max(0.1, safe_float(features.get("volatility_60_pips"), 0.1))
    return max(-6.0, min(6.0, safe_float(features.get(key)) / scale))


def _cross_breadth(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(
        1.0 if value > 0.0 else -1.0 if value < 0.0 else 0.0
        for value in values
    ) / len(values)


def _percentile_rank(value: float, values: list[float]) -> float:
    if not values:
        return 0.5
    below = sum(item < value for item in values)
    equal = sum(
        math.isclose(item, value, rel_tol=1e-12, abs_tol=1e-12)
        for item in values
    )
    return (below + 0.5 * equal) / len(values)


def augment_cross_sectional_microstructure(
    feature_cache: dict[str, dict[str, float]],
) -> None:
    """Add same-origin, leave-one-pair-out return and quote-flow context."""

    return_keys = {
        "r1_pips": "return_5_pips",
        "r3_pips": "return_30_pips",
        "r5_pips": "return_60_pips",
    }
    flow_keys = (
        "quote_flow_imbalance_1",
        "quote_flow_imbalance_5",
        "quote_flow_imbalance_30",
    )
    return_contributions: dict[str, dict[str, list[tuple[str, float]]]] = {
        key: {} for key in return_keys
    }
    flow_contributions: dict[str, dict[str, list[tuple[str, float]]]] = {
        key: {} for key in flow_keys
    }
    normalized_returns: dict[str, dict[str, float]] = {}
    normalized_flows: dict[str, dict[str, float]] = {}
    for instrument, features in feature_cache.items():
        parts = instrument.split("_")
        if len(parts) != 2:
            continue
        base, quote = parts
        normalized_returns[instrument] = {}
        normalized_flows[instrument] = {}
        for output_key, source_key in return_keys.items():
            value = _bounded_normalized_return(features, source_key)
            normalized_returns[instrument][output_key] = value
            return_contributions[output_key].setdefault(base, []).append(
                (instrument, value)
            )
            return_contributions[output_key].setdefault(quote, []).append(
                (instrument, -value)
            )
        for key in flow_keys:
            value = max(-6.0, min(6.0, safe_float(features.get(key))))
            normalized_flows[instrument][key] = value
            flow_contributions[key].setdefault(base, []).append(
                (instrument, value)
            )
            flow_contributions[key].setdefault(quote, []).append(
                (instrument, -value)
            )
    return_rank_values = {
        key: [values[key] for values in normalized_returns.values()]
        for key in return_keys
    }
    flow_rank_values = {
        key: [values[key] for values in normalized_flows.values()]
        for key in flow_keys
    }
    for instrument, features in feature_cache.items():
        normalized = normalized_returns.get(instrument)
        flows = normalized_flows.get(instrument)
        features.update(
            {
                "cross_sample_count": 0.0,
                "cross_pair_count": float(len(normalized_returns)),
                "cross_strength_r1": 0.0,
                "cross_strength_r3": 0.0,
                "cross_strength_r5": 0.0,
                "cross_breadth_r1": 0.0,
                "cross_breadth_r3": 0.0,
                "pair_norm_r1": 0.0 if normalized is None else normalized["r1_pips"],
                "pair_norm_r3": 0.0 if normalized is None else normalized["r3_pips"],
                "pair_norm_r5": 0.0 if normalized is None else normalized["r5_pips"],
                "pair_rank_r3": 0.5,
                "pair_rank_r5": 0.5,
                "relative_residual_r3": 0.0,
                "relative_residual_r5": 0.0,
                "cross_quote_flow_sample_count": 0.0,
                "cross_quote_flow_strength_1": 0.0,
                "cross_quote_flow_strength_5": 0.0,
                "cross_quote_flow_strength_30": 0.0,
                "cross_quote_flow_breadth_5": 0.0,
                "quote_flow_rank_5": 0.5,
                "quote_flow_rank_30": 0.5,
                "quote_flow_residual_5": 0.0,
                "quote_flow_residual_30": 0.0,
            }
        )
        parts = instrument.split("_")
        if len(parts) != 2 or normalized is None or flows is None:
            continue
        base, quote = parts

        def leave_one_out(
            contributions: dict[str, dict[str, list[tuple[str, float]]]],
            key: str,
        ) -> tuple[list[float], list[float]]:
            base_values = [
                value
                for pair, value in contributions[key].get(base, [])
                if pair != instrument
            ]
            quote_values = [
                value
                for pair, value in contributions[key].get(quote, [])
                if pair != instrument
            ]
            return base_values, quote_values

        return_peers = {
            key: leave_one_out(return_contributions, key)
            for key in return_keys
        }
        flow_peers = {
            key: leave_one_out(flow_contributions, key)
            for key in flow_keys
        }
        return_sample_count = min(
            (
                len(values)
                for peer_values in return_peers.values()
                for values in peer_values
            ),
            default=0,
        )
        flow_sample_count = min(
            (
                len(values)
                for peer_values in flow_peers.values()
                for values in peer_values
            ),
            default=0,
        )
        if return_sample_count > 0:
            strengths: dict[str, float] = {}
            for key, (base_values, quote_values) in return_peers.items():
                strengths[key] = 0.5 * (
                    statistics.median(base_values)
                    - statistics.median(quote_values)
                )
            r1_base, r1_quote = return_peers["r1_pips"]
            r3_base, r3_quote = return_peers["r3_pips"]
            features.update(
                {
                    "cross_sample_count": float(return_sample_count),
                    "cross_strength_r1": strengths["r1_pips"],
                    "cross_strength_r3": strengths["r3_pips"],
                    "cross_strength_r5": strengths["r5_pips"],
                    "cross_breadth_r1": 0.5
                    * (_cross_breadth(r1_base) - _cross_breadth(r1_quote)),
                    "cross_breadth_r3": 0.5
                    * (_cross_breadth(r3_base) - _cross_breadth(r3_quote)),
                    "pair_rank_r3": _percentile_rank(
                        normalized["r3_pips"],
                        return_rank_values["r3_pips"],
                    ),
                    "pair_rank_r5": _percentile_rank(
                        normalized["r5_pips"],
                        return_rank_values["r5_pips"],
                    ),
                    "relative_residual_r3": normalized["r3_pips"]
                    - strengths["r3_pips"],
                    "relative_residual_r5": normalized["r5_pips"]
                    - strengths["r5_pips"],
                }
            )
        if flow_sample_count > 0:
            flow_strengths: dict[str, float] = {}
            for key, (base_values, quote_values) in flow_peers.items():
                flow_strengths[key] = 0.5 * (
                    statistics.median(base_values)
                    - statistics.median(quote_values)
                )
            flow5_base, flow5_quote = flow_peers["quote_flow_imbalance_5"]
            features.update(
                {
                    "cross_quote_flow_sample_count": float(flow_sample_count),
                    "cross_quote_flow_strength_1": flow_strengths[
                        "quote_flow_imbalance_1"
                    ],
                    "cross_quote_flow_strength_5": flow_strengths[
                        "quote_flow_imbalance_5"
                    ],
                    "cross_quote_flow_strength_30": flow_strengths[
                        "quote_flow_imbalance_30"
                    ],
                    "cross_quote_flow_breadth_5": 0.5
                    * (
                        _cross_breadth(flow5_base)
                        - _cross_breadth(flow5_quote)
                    ),
                    "quote_flow_rank_5": _percentile_rank(
                        flows["quote_flow_imbalance_5"],
                        flow_rank_values["quote_flow_imbalance_5"],
                    ),
                    "quote_flow_rank_30": _percentile_rank(
                        flows["quote_flow_imbalance_30"],
                        flow_rank_values["quote_flow_imbalance_30"],
                    ),
                    "quote_flow_residual_5": flows["quote_flow_imbalance_5"]
                    - flow_strengths["quote_flow_imbalance_5"],
                    "quote_flow_residual_30": flows["quote_flow_imbalance_30"]
                    - flow_strengths["quote_flow_imbalance_30"],
                }
            )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    for attempt in range(5):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.01 * (2**attempt))


def theoretical_pips(
    direction: str,
    pip: float,
    entry_bid: float,
    entry_ask: float,
    exit_bid: float,
    exit_ask: float,
) -> float:
    if direction == "buy":
        return (exit_bid - entry_ask) / max(pip, 1e-12)
    return (entry_bid - exit_ask) / max(pip, 1e-12)


@dataclass(frozen=True)
class SecondPoint:
    bucket: int
    bid: float
    ask: float
    mid: float
    spread_pips: float
    pip: float
    activity: float
    depth_imbalance: float = 0.0
    microprice_offset_pips: float = 0.0
    liquidity_ratio: float = 1.0
    bid_liquidity: float = 0.0
    ask_liquidity: float = 0.0
    bid_total_liquidity: float = 0.0
    ask_total_liquidity: float = 0.0
    bid_levels: float = 0.0
    ask_levels: float = 0.0


class SecondFeatureEngine:
    """Convert asynchronous quote updates into a dense causal one-second grid."""

    def __init__(
        self,
        max_points: int = 420,
        max_fill_gap_sec: int = 60,
    ) -> None:
        self.max_points = max(90, max_points)
        self.max_fill_gap_sec = max(5, int(max_fill_gap_sec))
        self.points: dict[str, deque[SecondPoint]] = defaultdict(
            lambda: deque(maxlen=self.max_points)
        )
        self.last_versions: dict[str, int] = {}

    @staticmethod
    def _point(
        bucket: int,
        quote: Any,
        metadata: dict[str, Any],
        pip: float,
        activity: float,
    ) -> SecondPoint:
        bid = safe_float(quote.bid)
        ask = safe_float(quote.ask)
        bid_liquidity = safe_float(
            metadata.get("bid_top_liquidity"),
            safe_float(metadata.get("bid_total_liquidity")),
        )
        ask_liquidity = safe_float(
            metadata.get("ask_top_liquidity"),
            safe_float(metadata.get("ask_total_liquidity")),
        )
        bid_total = safe_float(
            metadata.get("bid_total_liquidity"),
            bid_liquidity,
        )
        ask_total = safe_float(
            metadata.get("ask_total_liquidity"),
            ask_liquidity,
        )
        liquidity_ratio = bid_total / max(ask_total, 1.0) if bid_total or ask_total else 1.0
        mid = (bid + ask) / 2.0
        microprice = safe_float(metadata.get("microprice"), mid)
        return SecondPoint(
            bucket=bucket,
            bid=bid,
            ask=ask,
            mid=mid,
            spread_pips=(ask - bid) / max(pip, 1e-12),
            pip=pip,
            activity=activity,
            depth_imbalance=safe_float(metadata.get("depth_imbalance")),
            microprice_offset_pips=(microprice - mid) / max(pip, 1e-12),
            liquidity_ratio=liquidity_ratio,
            bid_liquidity=bid_liquidity,
            ask_liquidity=ask_liquidity,
            bid_total_liquidity=bid_total,
            ask_total_liquidity=ask_total,
            bid_levels=safe_float(metadata.get("bid_levels")),
            ask_levels=safe_float(metadata.get("ask_levels")),
        )

    @staticmethod
    def _quote_flow_imbalance(previous: SecondPoint, current: SecondPoint) -> float:
        bid_flow = (
            (current.bid_liquidity if current.bid >= previous.bid else 0.0)
            - (previous.bid_liquidity if current.bid <= previous.bid else 0.0)
        )
        ask_flow = (
            -(current.ask_liquidity if current.ask <= previous.ask else 0.0)
            + (previous.ask_liquidity if current.ask >= previous.ask else 0.0)
        )
        return bid_flow + ask_flow

    def observe(
        self,
        prices: dict[str, Any],
        metadata: dict[str, dict[str, Any]],
        pip_sizes: dict[str, float],
        now_epoch: float | None = None,
    ) -> dict[str, dict[str, float]]:
        bucket = int(time.time() if now_epoch is None else now_epoch)
        ready: dict[str, dict[str, float]] = {}
        for instrument, quote in prices.items():
            pip = safe_float(pip_sizes.get(instrument))
            if pip <= 0.0 or not bool(getattr(quote, "tradeable", True)):
                continue
            rows = self.points[instrument]
            if rows and bucket <= rows[-1].bucket:
                features = self.features(instrument)
                if features is not None:
                    ready[instrument] = features
                continue
            info = metadata.get(instrument) or {}
            version = int(safe_float(info.get("version")))
            previous_version = self.last_versions.get(instrument, version)
            activity = max(0, version - previous_version)
            self.last_versions[instrument] = version
            if rows:
                gap = bucket - rows[-1].bucket
                if gap > self.max_fill_gap_sec:
                    rows.clear()
                else:
                    prior = rows[-1]
                    for missing_bucket in range(prior.bucket + 1, bucket):
                        rows.append(
                            SecondPoint(
                                bucket=missing_bucket,
                                bid=prior.bid,
                                ask=prior.ask,
                                mid=prior.mid,
                                spread_pips=prior.spread_pips,
                                pip=prior.pip,
                                activity=0.0,
                                depth_imbalance=prior.depth_imbalance,
                                microprice_offset_pips=prior.microprice_offset_pips,
                                liquidity_ratio=prior.liquidity_ratio,
                                bid_liquidity=prior.bid_liquidity,
                                ask_liquidity=prior.ask_liquidity,
                                bid_total_liquidity=prior.bid_total_liquidity,
                                ask_total_liquidity=prior.ask_total_liquidity,
                                bid_levels=prior.bid_levels,
                                ask_levels=prior.ask_levels,
                            )
                        )
            rows.append(self._point(bucket, quote, info, pip, float(activity)))
            features = self.features(instrument)
            if features is not None:
                ready[instrument] = features
        return ready

    def features(self, instrument: str) -> dict[str, float] | None:
        rows = self.points.get(instrument)
        if rows is None or len(rows) < 61:
            return None
        recent = list(rows)[-61:]
        if recent[-1].bucket - recent[0].bucket != 60:
            return None
        current = recent[-1]
        by_lag = {lag: recent[-1 - lag] for lag in (5, 10, 30, 60)}
        pip = max(current.pip, 1e-12)
        five_second_moves = [
            (recent[index].mid - recent[index - 5].mid) / pip
            for index in range(5, len(recent), 5)
        ]
        moves_30 = five_second_moves[-6:]
        moves_60 = five_second_moves[-12:]
        spreads_60 = [recent[index].spread_pips for index in range(5, len(recent), 5)]
        activity_30 = sum(row.activity for row in recent[-30:])
        activity_60 = sum(row.activity for row in recent[1:])
        activity_5 = sum(row.activity for row in recent[-5:])
        mids_30 = [row.mid for row in recent[-30:]]
        one_second_moves_30 = [
            recent[index].mid - recent[index - 1].mid
            for index in range(len(recent) - 29, len(recent))
        ]
        spreads_30 = [row.spread_pips for row in recent[-30:]]
        quote_flows = [
            self._quote_flow_imbalance(recent[index - 1], recent[index])
            for index in range(1, len(recent))
        ]

        def normalized_quote_flow(width: int) -> float:
            points = recent[-width:]
            liquidity_scale = statistics.fmean(
                max(0.0, row.bid_liquidity) + max(0.0, row.ask_liquidity)
                for row in points
            )
            if liquidity_scale <= 0.0:
                return 0.0
            return sum(quote_flows[-width:]) / liquidity_scale

        r5 = (current.mid - by_lag[5].mid) / pip
        r30 = (current.mid - by_lag[30].mid) / pip
        total_liquidity = (
            current.bid_total_liquidity + current.ask_total_liquidity
        )
        top_liquidity = current.bid_liquidity + current.ask_liquidity
        lag5_total_liquidity = (
            by_lag[5].bid_total_liquidity
            + by_lag[5].ask_total_liquidity
        )
        level_total = current.bid_levels + current.ask_levels

        def tick_imbalance(width: int) -> float:
            points = recent[-width:]
            moves = [
                points[index].mid - points[index - 1].mid
                for index in range(1, len(points))
            ]
            nonzero = [move for move in moves if move != 0.0]
            if not nonzero:
                return 0.0
            return sum(1.0 if move > 0.0 else -1.0 for move in nonzero) / len(
                nonzero
            )

        gross_path_30 = sum(abs(move) for move in one_second_moves_30)
        hour = datetime.fromtimestamp(current.bucket, timezone.utc).hour + datetime.fromtimestamp(
            current.bucket, timezone.utc
        ).minute / 60.0
        return {
            "return_5_pips": r5,
            "return_10_pips": (current.mid - by_lag[10].mid) / pip,
            "return_30_pips": r30,
            "return_60_pips": (current.mid - by_lag[60].mid) / pip,
            "acceleration_5_30": r5 - r30 / 6.0,
            "volatility_30_pips": population_std(moves_30),
            "volatility_60_pips": population_std(moves_60),
            "range_30_pips": (max(mids_30) - min(mids_30)) / pip,
            "spread_pips": current.spread_pips,
            "spread_ratio_60": current.spread_pips / max(statistics.fmean(spreads_60), 0.01),
            "spread_delta_1": current.spread_pips - recent[-2].spread_pips,
            "spread_delta_5": current.spread_pips - by_lag[5].spread_pips,
            "spread_volatility_30": population_std(spreads_30),
            "activity_5": activity_5,
            "activity_30": activity_30,
            "activity_ratio_30_60": activity_30 / max(activity_60 / 2.0, 1.0),
            "activity_acceleration_5_30": activity_5 / 5.0 - activity_30 / 30.0,
            "tick_imbalance_5": tick_imbalance(6),
            "tick_imbalance_30": tick_imbalance(30),
            "price_efficiency_30": abs(current.mid - recent[-30].mid)
            / max(gross_path_30, 1e-12),
            "hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
            "hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
            "bid_top_liquidity": current.bid_liquidity,
            "ask_top_liquidity": current.ask_liquidity,
            "bid_total_liquidity": current.bid_total_liquidity,
            "ask_total_liquidity": current.ask_total_liquidity,
            "bid_levels": current.bid_levels,
            "ask_levels": current.ask_levels,
            "depth_total_liquidity": total_liquidity,
            "depth_log_total_liquidity": math.log1p(max(0.0, total_liquidity)),
            "depth_log_total_liquidity_delta_5": math.log1p(
                max(0.0, total_liquidity)
            )
            - math.log1p(max(0.0, lag5_total_liquidity)),
            "depth_top_imbalance": (
                (current.bid_liquidity - current.ask_liquidity) / top_liquidity
                if top_liquidity > 0.0
                else 0.0
            ),
            "top_liquidity_share": (
                top_liquidity / total_liquidity
                if total_liquidity > 0.0
                else 0.0
            ),
            "level_imbalance": (
                (current.bid_levels - current.ask_levels) / level_total
                if level_total > 0.0
                else 0.0
            ),
            "depth_imbalance": current.depth_imbalance,
            "depth_imbalance_delta_5": (
                current.depth_imbalance - by_lag[5].depth_imbalance
            ),
            "depth_imbalance_delta_30": (
                current.depth_imbalance - by_lag[30].depth_imbalance
            ),
            "microprice_offset_pips": current.microprice_offset_pips,
            "microprice_offset_mean_5": statistics.fmean(
                row.microprice_offset_pips for row in recent[-5:]
            ),
            "microprice_offset_mean_30": statistics.fmean(
                row.microprice_offset_pips for row in recent[-30:]
            ),
            "liquidity_ratio": current.liquidity_ratio,
            "liquidity_ratio_log": math.log(max(current.liquidity_ratio, 1e-9)),
            "liquidity_ratio_delta_5": (
                current.liquidity_ratio - by_lag[5].liquidity_ratio
            ),
            "quote_flow_imbalance_1": normalized_quote_flow(1),
            "quote_flow_imbalance_5": normalized_quote_flow(5),
            "quote_flow_imbalance_30": normalized_quote_flow(30),
        }


class SecondRidgeSnapshot:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.state: dict[str, Any] = {}
        self.compiled: dict[str, list[dict[str, Any]]] = {}
        self.modified_ns = 0
        self.load()

    @property
    def ready(self) -> bool:
        return bool(self.state.get("models"))

    def load(self) -> bool:
        try:
            modified = self.path.stat().st_mtime_ns
            if modified == self.modified_ns and self.state:
                return False
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if int(payload.get("schema_version") or 0) != SCHEMA_VERSION:
                raise ValueError("unsupported second-forecast model schema")
            if tuple(payload.get("feature_names") or ()) != FEATURE_NAMES:
                raise ValueError("second-forecast feature schema does not match runtime")
        except (OSError, ValueError, json.JSONDecodeError):
            return False
        self.state = payload
        compiled: dict[str, list[dict[str, Any]]] = {}
        for instrument, pair_models in (payload.get("models") or {}).items():
            rows: list[dict[str, Any]] = []
            for horizon_text, model in (pair_models or {}).items():
                means = model.get("feature_means") or []
                scales = model.get("feature_scales") or []
                coefficients = model.get("coefficients") or []
                if not (len(means) == len(scales) == len(coefficients) == len(FEATURE_NAMES)):
                    continue
                weights = [
                    safe_float(coefficient) / max(safe_float(scale, 1.0), 1e-9)
                    for coefficient, scale in zip(coefficients, scales)
                ]
                adjusted_intercept = safe_float(model.get("intercept")) - sum(
                    safe_float(mean) * weight for mean, weight in zip(means, weights)
                )
                profile_gates = tuple(
                    (
                        str(profile),
                        max(0.0, safe_float(profile_fit.get("score_threshold"))),
                        max(
                            0.0,
                            safe_float(
                                profile_fit.get("max_spread_pips"),
                                3.0,
                            ),
                        ),
                        profile_fit,
                    )
                    for profile, profile_fit in (model.get("profiles") or {}).items()
                    if profile in PROFILE_EXECUTION
                    and isinstance(profile_fit, dict)
                )
                rows.append(
                    {
                        "horizon_sec": int(horizon_text),
                        "model": model,
                        "model_family": str(model.get("model_family") or MODEL_FAMILY),
                        "input_timeframe": str(model.get("input_timeframe") or INPUT_TIMEFRAME),
                        "training_timeframe": str(
                            model.get("training_timeframe") or TRAINING_TIMEFRAME
                        ),
                        "weights": weights,
                        "adjusted_intercept": adjusted_intercept,
                        "magnitude_calibration": max(
                            0.0,
                            min(
                                10.0,
                                safe_float(
                                    model.get("magnitude_calibration"),
                                    1.0,
                                ),
                            ),
                        ),
                        "residual_std_pips": max(
                            0.05,
                            safe_float(model.get("residual_std_pips"), 1.0),
                        ),
                        "profile_gates": profile_gates,
                    }
                )
            if rows:
                compiled[str(instrument)] = rows
        self.compiled = compiled
        self.modified_ns = modified
        return True

    def predict(self, instrument: str, features: dict[str, float]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        vector = [safe_float(features.get(name)) for name in FEATURE_NAMES]
        for compiled in self.compiled.get(instrument) or []:
            model = compiled["model"]
            raw = compiled["adjusted_intercept"] + sum(
                weight * value for weight, value in zip(compiled["weights"], vector)
            )
            predicted = raw * compiled["magnitude_calibration"]
            residual = compiled["residual_std_pips"]
            probability_up = 1.0 / (1.0 + math.exp(-max(-8.0, min(8.0, predicted / residual))))
            output.append(
                {
                    "model_id": str(
                        model.get("model_id")
                        or (
                            f"{MODEL_FAMILY}.{INPUT_TIMEFRAME.lower()}."
                            f"{instrument}.h{compiled['horizon_sec']}"
                        )
                    ),
                    "model_family": compiled["model_family"],
                    "input_timeframe": compiled["input_timeframe"],
                    "training_timeframe": compiled["training_timeframe"],
                    "instrument": instrument,
                    "horizon_sec": int(compiled["horizon_sec"]),
                    "raw_score": raw,
                    "predicted_signed_pips": predicted,
                    "probability_up": probability_up,
                    "profiles": model.get("profiles") or {},
                    "_profile_gates": compiled["profile_gates"],
                    "fit_metrics": model.get("fit_metrics") or {},
                    "smoothing": model.get("smoothing") or {},
                    "fit_provenance": str(
                        model.get("fit_provenance") or "pair_specific"
                    ),
                    "pair_specific_evidence": bool(
                        model.get("pair_specific_evidence", True)
                    ),
                    "account_eligible": bool(model.get("account_eligible", True)),
                }
            )
        return output


class SecondForecastStore:
    def __init__(self, database_path: Path, retention_days: int = 2) -> None:
        self.database_path = Path(database_path)
        self.retention_days = max(1, int(retention_days))
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.database = sqlite3.connect(self.database_path)
        self.database.execute("PRAGMA journal_mode=WAL")
        self.database.execute("PRAGMA synchronous=NORMAL")
        self.database.executescript(
            """
            CREATE TABLE IF NOT EXISTS forecasts (
                id TEXT PRIMARY KEY,
                origin_epoch INTEGER NOT NULL,
                target_epoch INTEGER NOT NULL,
                instrument TEXT NOT NULL,
                model_id TEXT NOT NULL,
                model_family TEXT NOT NULL DEFAULT 'ridge_return',
                input_timeframe TEXT NOT NULL DEFAULT 'S1',
                training_timeframe TEXT NOT NULL DEFAULT 'S5',
                horizon_sec INTEGER NOT NULL,
                predicted_signed_pips REAL NOT NULL,
                probability_up REAL NOT NULL,
                raw_score REAL NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                entry_mid REAL NOT NULL,
                spread_pips REAL NOT NULL,
                pip REAL NOT NULL,
                profiles_json TEXT NOT NULL,
                status TEXT NOT NULL,
                exit_time TEXT,
                actual_signed_pips REAL,
                chosen_theoretical_pips REAL,
                direction_correct INTEGER,
                brier_score REAL,
                outcome_delay_sec REAL
            );
            CREATE INDEX IF NOT EXISTS second_forecast_status_target
                ON forecasts(status, target_epoch);
            CREATE INDEX IF NOT EXISTS second_forecast_model_origin
                ON forecasts(model_id, origin_epoch);
            CREATE INDEX IF NOT EXISTS second_forecast_origin
                ON forecasts(origin_epoch);
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                origin_epoch INTEGER NOT NULL,
                instrument TEXT NOT NULL,
                features_json TEXT NOT NULL,
                PRIMARY KEY (origin_epoch, instrument)
            );
            CREATE INDEX IF NOT EXISTS second_feature_snapshot_origin
                ON feature_snapshots(origin_epoch);
            """
        )
        existing_columns = {
            str(row[1]) for row in self.database.execute("PRAGMA table_info(forecasts)")
        }
        for name, declaration in (
            ("model_family", "TEXT NOT NULL DEFAULT 'ridge_return'"),
            ("input_timeframe", "TEXT NOT NULL DEFAULT 'S1'"),
            ("training_timeframe", "TEXT NOT NULL DEFAULT 'S5'"),
        ):
            if name not in existing_columns:
                self.database.execute(f"ALTER TABLE forecasts ADD COLUMN {name} {declaration}")
        self.database.commit()
        self.pending: list[tuple[float, int, dict[str, Any]]] = []
        self.pending_sequence = 0
        self.inserts: list[tuple[Any, ...]] = []
        self.feature_inserts: list[tuple[Any, ...]] = []
        self.updates: list[tuple[Any, ...]] = []
        self.last_prune_epoch = time.time()

    def record(self, forecast: dict[str, Any], quote: Any, pip: float, origin_epoch: int) -> str:
        event_id = (
            f"{origin_epoch}-{forecast['instrument']}-{forecast['horizon_sec']}-"
            f"{forecast['model_id']}"
        )
        entry_mid = (safe_float(quote.bid) + safe_float(quote.ask)) / 2.0
        row = (
            event_id,
            origin_epoch,
            origin_epoch + int(forecast["horizon_sec"]),
            forecast["instrument"],
            forecast["model_id"],
            str(forecast.get("model_family") or MODEL_FAMILY),
            str(forecast.get("input_timeframe") or INPUT_TIMEFRAME),
            str(forecast.get("training_timeframe") or TRAINING_TIMEFRAME),
            int(forecast["horizon_sec"]),
            safe_float(forecast["predicted_signed_pips"]),
            safe_float(forecast["probability_up"]),
            safe_float(forecast["raw_score"]),
            safe_float(quote.bid),
            safe_float(quote.ask),
            entry_mid,
            safe_float(forecast["spread_pips"]),
            pip,
            json.dumps(forecast.get("profiles") or {}, separators=(",", ":")),
            "pending",
        )
        self.inserts.append(row)
        created_monotonic = time.monotonic()
        pending_item = {
            "id": event_id,
            "created_monotonic": created_monotonic,
            "instrument": forecast["instrument"],
            "horizon_sec": int(forecast["horizon_sec"]),
            "predicted_signed_pips": safe_float(forecast["predicted_signed_pips"]),
            "probability_up": safe_float(forecast["probability_up"]),
            "entry_bid": safe_float(quote.bid),
            "entry_ask": safe_float(quote.ask),
            "entry_mid": entry_mid,
            "pip": pip,
        }
        self.pending_sequence += 1
        heapq.heappush(
            self.pending,
            (
                created_monotonic + int(forecast["horizon_sec"]),
                self.pending_sequence,
                pending_item,
            ),
        )
        return event_id

    def record_feature_snapshot(
        self,
        origin_epoch: int,
        instrument: str,
        features: dict[str, Any],
    ) -> None:
        normalized = {
            str(name): safe_float(value)
            for name, value in sorted(features.items())
        }
        self.feature_inserts.append(
            (
                int(origin_epoch),
                str(instrument),
                json.dumps(normalized, separators=(",", ":"), sort_keys=True),
            )
        )

    def mature(self, prices: dict[str, Any]) -> list[dict[str, Any]]:
        now = time.monotonic()
        outcomes: list[dict[str, Any]] = []
        deferred: list[tuple[float, int, dict[str, Any]]] = []
        while self.pending and self.pending[0][0] <= now:
            _, sequence, item = heapq.heappop(self.pending)
            horizon = int(item["horizon_sec"])
            age = now - safe_float(item["created_monotonic"])
            quote = prices.get(str(item["instrument"]))
            if quote is None:
                deferred.append((now + 1.0, sequence, item))
                continue
            pip = max(1e-12, safe_float(item["pip"]))
            exit_mid = (safe_float(quote.bid) + safe_float(quote.ask)) / 2.0
            actual_signed = (exit_mid - safe_float(item["entry_mid"])) / pip
            direction = "buy" if safe_float(item["predicted_signed_pips"]) >= 0.0 else "sell"
            chosen = theoretical_pips(
                direction,
                pip,
                safe_float(item["entry_bid"]),
                safe_float(item["entry_ask"]),
                safe_float(quote.bid),
                safe_float(quote.ask),
            )
            correct = int((direction == "buy" and actual_signed > 0.0) or (direction == "sell" and actual_signed < 0.0))
            actual_up = 1.0 if actual_signed > 0.0 else 0.0
            brier = (safe_float(item["probability_up"], 0.5) - actual_up) ** 2
            delay = max(0.0, age - horizon)
            self.updates.append(
                (
                    str(getattr(quote, "time", "")),
                    actual_signed,
                    chosen,
                    correct,
                    brier,
                    delay,
                    item["id"],
                )
            )
            outcomes.append(
                {
                    **item,
                    "actual_signed_pips": actual_signed,
                    "chosen_theoretical_pips": chosen,
                    "direction_correct": bool(correct),
                    "brier_score": brier,
                    "outcome_delay_sec": delay,
                }
            )
        for pending_item in deferred:
            heapq.heappush(self.pending, pending_item)
        return outcomes

    def flush(self) -> None:
        if self.feature_inserts:
            self.database.executemany(
                """
                INSERT OR REPLACE INTO feature_snapshots (
                    origin_epoch, instrument, features_json
                ) VALUES (?, ?, ?)
                """,
                self.feature_inserts,
            )
            self.feature_inserts.clear()
        if self.inserts:
            self.database.executemany(
                """
                INSERT OR IGNORE INTO forecasts (
                    id, origin_epoch, target_epoch, instrument, model_id,
                    model_family, input_timeframe, training_timeframe, horizon_sec,
                    predicted_signed_pips, probability_up, raw_score, entry_bid,
                    entry_ask, entry_mid, spread_pips, pip, profiles_json, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self.inserts,
            )
            self.inserts.clear()
        if self.updates:
            self.database.executemany(
                """
                UPDATE forecasts SET status='matured', exit_time=?, actual_signed_pips=?,
                    chosen_theoretical_pips=?, direction_correct=?, brier_score=?,
                    outcome_delay_sec=? WHERE id=?
                """,
                self.updates,
            )
            self.updates.clear()
        now_epoch = time.time()
        if now_epoch - self.last_prune_epoch >= 3600.0:
            cutoff = int(now_epoch - self.retention_days * 86400.0)
            self.database.execute(
                "DELETE FROM forecasts WHERE origin_epoch < ?",
                (cutoff,),
            )
            self.database.execute(
                "DELETE FROM feature_snapshots WHERE origin_epoch < ?",
                (cutoff,),
            )
            self.last_prune_epoch = now_epoch
        self.database.commit()

    def close(self) -> None:
        self.flush()
        self.database.close()


class SecondForecastRuntime:
    """Evaluate compact models once per second and create promotion-gated lanes."""

    def __init__(
        self,
        model_path: Path,
        database_path: Path,
        state_path: Path,
        *,
        cadence_sec: int = 1,
        sample_sec: int = 5,
        signal_interval_sec: int = 5,
        execution_horizons: set[int] | None = None,
        research_enabled: bool = True,
        shadow_signal_tracking_enabled: bool | None = None,
    ) -> None:
        self.model = SecondRidgeSnapshot(model_path)
        self.research_enabled = research_enabled
        self.shadow_signal_tracking_enabled = (
            research_enabled
            if shadow_signal_tracking_enabled is None
            else bool(shadow_signal_tracking_enabled)
        )
        self.store = SecondForecastStore(database_path) if research_enabled else None
        self.state_path = state_path
        self.engine = SecondFeatureEngine()
        self.cadence_sec = max(1, cadence_sec)
        self.sample_sec = max(1, sample_sec)
        self.signal_interval_sec = max(1, signal_interval_sec)
        self.execution_horizons = execution_horizons or set(DEFAULT_EXECUTION_HORIZONS_SEC)
        self.last_bucket = -1
        self.last_signal_bucket: dict[tuple[str, int, str], int] = {}
        self.last_model_check = 0.0
        self.last_state_save = 0.0
        self.last_cycle_log = 0.0
        self.cycles = 0
        self.forecasts = 0
        self.sampled_forecasts = 0
        self.accepted_signals = 0
        self.matured_outcomes = 0
        self.latencies_ms: deque[float] = deque(maxlen=3600)
        self.top_forecasts: list[dict[str, Any]] = []
        self.forecast_curves: list[dict[str, Any]] = []
        self.top_forecasts_updated_utc = ""

    @property
    def ready(self) -> bool:
        return self.model.ready

    def evaluate(
        self,
        prices: dict[str, Any],
        metadata: dict[str, dict[str, Any]],
        pip_sizes: dict[str, float],
        pending: list[dict[str, Any]],
        log: Callable[..., None],
        now_epoch: float | None = None,
        candidate_callback: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> list[dict[str, Any]]:
        epoch = time.time() if now_epoch is None else now_epoch
        bucket = int(epoch)
        if bucket <= self.last_bucket or bucket % self.cadence_sec:
            return []
        self.last_bucket = bucket
        started = time.perf_counter()
        if time.monotonic() - self.last_model_check >= 60.0:
            self.model.load()
            self.last_model_check = time.monotonic()
        features_by_pair = self.engine.observe(prices, metadata, pip_sizes, epoch)
        augment_cross_sectional_microstructure(features_by_pair)
        if self.store is not None and bucket % self.sample_sec == 0:
            for instrument, features in features_by_pair.items():
                self.store.record_feature_snapshot(
                    bucket,
                    instrument,
                    features,
                )
        candidates: list[dict[str, Any]] = []
        ranked: list[dict[str, Any]] = []
        sampled: list[tuple[dict[str, Any], Any, float, int]] = []
        signal_logs: list[dict[str, Any]] = []
        for instrument, features in features_by_pair.items():
            quote = prices.get(instrument)
            pip = safe_float(pip_sizes.get(instrument))
            if quote is None or pip <= 0.0:
                continue
            for forecast in self.model.predict(instrument, features):
                predicted_signed_pips = float(forecast["predicted_signed_pips"])
                raw_score = float(forecast["raw_score"])
                probability_up = float(forecast["probability_up"])
                spread_pips = float(features.get("spread_pips") or 0.0)
                signal_strength_pips = abs(predicted_signed_pips)
                projected_net_pips = signal_strength_pips - spread_pips
                forecast["spread_pips"] = spread_pips
                forecast["depth_imbalance"] = float(
                    features.get("depth_imbalance") or 0.0
                )
                forecast["microprice_offset_pips"] = float(
                    features.get("microprice_offset_pips") or 0.0
                )
                forecast["projected_net_pips"] = projected_net_pips
                self.forecasts += 1
                ranked.append(forecast)
                forecast["accepted_profiles"] = []
                horizon_sec = int(forecast["horizon_sec"])
                research_sample_interval = self.sample_sec
                if horizon_sec >= 3600:
                    research_sample_interval = max(
                        self.sample_sec,
                        min(900, max(60, horizon_sec // 12)),
                    )
                if (
                    self.research_enabled
                    and bucket % research_sample_interval == 0
                ):
                    sampled.append((forecast, quote, pip, bucket))
                for (
                    profile,
                    threshold,
                    max_spread,
                    profile_fit,
                ) in forecast.get("_profile_gates") or ():
                    if abs(raw_score) < threshold or spread_pips > max_spread:
                        continue
                    forecast["accepted_profiles"].append(profile)
                    key = (instrument, int(forecast["horizon_sec"]), profile)
                    registration_interval = max(
                        self.signal_interval_sec,
                        min(
                            900,
                            max(
                                min(300, int(forecast["horizon_sec"])),
                                int(forecast["horizon_sec"]) // 12,
                            ),
                        ),
                    )
                    should_register = (
                        self.shadow_signal_tracking_enabled
                        and bucket - self.last_signal_bucket.get(key, -10**9) >= registration_interval
                    )
                    direction = "buy" if predicted_signed_pips >= 0.0 else "sell"
                    lane_id = matrix_lane_id(profile)
                    event_id = f"matrix-s1-{bucket}-{instrument}-h{forecast['horizon_sec']}-{profile}"
                    if should_register:
                        self.last_signal_bucket[key] = bucket
                        self.accepted_signals += 1
                        pending.append(
                            {
                                "id": event_id,
                                "lane_id": lane_id,
                                "family": MODEL_FAMILY,
                                "profile": profile,
                                "model_id": forecast["model_id"],
                                "input_timeframe": INPUT_TIMEFRAME,
                                "training_timeframe": TRAINING_TIMEFRAME,
                                "kind": "signal",
                                "instrument": instrument,
                                "direction": direction,
                                "blocked_reason": "",
                                "miss_class": "",
                                "entry_bid": safe_float(quote.bid),
                                "entry_ask": safe_float(quote.ask),
                                "entry_mid": (safe_float(quote.bid) + safe_float(quote.ask)) / 2.0,
                                "entry_time": str(getattr(quote, "time", "")),
                                "pip": pip,
                                "stop_loss_pips": PROFILE_EXECUTION[profile]["stop_loss_pips"],
                                "take_profit_r": PROFILE_EXECUTION[profile]["take_profit_r"],
                                "max_favorable_pips": 0.0,
                                "max_adverse_pips": 0.0,
                                "favorable_hits": {},
                                "adverse_hits": {},
                                "path_samples": 0,
                                "created_monotonic": time.monotonic(),
                                "remaining_horizons": [float(forecast["horizon_sec"])],
                            }
                        )
                        signal_logs.append(
                            {
                                "id": event_id,
                                "lane_id": lane_id,
                                "family": MODEL_FAMILY,
                                "profile": profile,
                                "model_id": forecast["model_id"],
                                "input_timeframe": INPUT_TIMEFRAME,
                                "training_timeframe": TRAINING_TIMEFRAME,
                                "instrument": instrument,
                                "direction": direction,
                                "horizon_sec": int(forecast["horizon_sec"]),
                                "raw_score": round(raw_score, 6),
                                "predicted_signed_pips": round(
                                    predicted_signed_pips,
                                    4,
                                ),
                                "spread_pips": round(spread_pips, 4),
                                "profile_threshold": threshold,
                                "registration_interval_sec": registration_interval,
                                "profile_fit": profile_fit,
                            }
                        )
                    if int(forecast["horizon_sec"]) not in self.execution_horizons:
                        continue
                    candidates.append(
                        {
                            "id": event_id,
                            "lane_id": lane_id,
                            "family": MODEL_FAMILY,
                            "profile": profile,
                            "model_id": forecast["model_id"],
                            "input_timeframe": INPUT_TIMEFRAME,
                            "training_timeframe": TRAINING_TIMEFRAME,
                            "signal_role": "entry_exit_timing",
                            "instrument": instrument,
                            "direction": direction,
                            "bid": safe_float(quote.bid),
                            "ask": safe_float(quote.ask),
                            "entry_time": str(getattr(quote, "time", "")),
                            "pip": pip,
                            "spread_pips": spread_pips,
                            "predicted_signed_pips": predicted_signed_pips,
                            "projected_net_pips": projected_net_pips,
                            "probability_up": probability_up,
                            "signal_strength_pips": signal_strength_pips,
                            "signal_to_spread": signal_strength_pips
                            / max(spread_pips, 0.01),
                            "stop_loss_pips": PROFILE_EXECUTION[profile]["stop_loss_pips"],
                            "take_profit_r": PROFILE_EXECUTION[profile]["take_profit_r"],
                            "account_eligible": bool(
                                forecast.get("account_eligible", True)
                            ),
                            "forecast_horizon_sec": int(forecast["horizon_sec"]),
                            "forecast_created_monotonic": time.monotonic(),
                            "historical_gate_passed": bool(profile_fit.get("historical_gate_passed")),
                        }
                    )
        inference_ms = (time.perf_counter() - started) * 1000.0
        self.latencies_ms.append(inference_ms)
        if candidates and candidate_callback is not None:
            candidate_callback(candidates)
        for forecast, quote, pip, origin_bucket in sampled:
            if self.store is not None:
                self.store.record(forecast, quote, pip, origin_bucket)
                self.sampled_forecasts += 1
        for fields in signal_logs:
            log("second_shadow_signal", **fields)
        outcomes = [] if self.store is None else self.store.mature(prices)
        self.matured_outcomes += len(outcomes)
        if self.store is not None:
            self.store.flush()
        self.cycles += 1
        ranked.sort(key=lambda row: safe_float(row.get("projected_net_pips")), reverse=True)
        if ranked:
            compact_forecasts = [
                {
                    "instrument": row["instrument"],
                    "model_id": row["model_id"],
                    "model_family": row.get("model_family") or MODEL_FAMILY,
                    "input_timeframe": row.get("input_timeframe") or INPUT_TIMEFRAME,
                    "training_timeframe": row.get("training_timeframe") or TRAINING_TIMEFRAME,
                    "horizon_sec": row["horizon_sec"],
                    "predicted_signed_pips": round(safe_float(row["predicted_signed_pips"]), 4),
                    "projected_net_pips": round(safe_float(row["projected_net_pips"]), 4),
                    "probability_up": round(safe_float(row["probability_up"]), 4),
                    "spread_pips": round(safe_float(row["spread_pips"]), 4),
                    "accepted_profiles": list(row.get("accepted_profiles") or []),
                    "fit_provenance": row.get("fit_provenance")
                    or "pair_specific",
                    "pair_specific_evidence": bool(
                        row.get("pair_specific_evidence", True)
                    ),
                    "account_eligible": bool(row.get("account_eligible", True)),
                    "smoothing_lambda": safe_float(
                        (row.get("smoothing") or {}).get("selected_lambda")
                    ),
                    "historical_gate_passed": any(
                        bool(profile.get("historical_gate_passed"))
                        for profile in (row.get("profiles") or {}).values()
                    ),
                    "holdout_n": max(
                        (
                            int(safe_float((profile.get("holdout") or {}).get("n")))
                            for profile in (row.get("profiles") or {}).values()
                        ),
                        default=0,
                    ),
                }
                for row in ranked
            ]
            self.top_forecasts = compact_forecasts[:12]
            curves: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in compact_forecasts:
                curves[str(row["instrument"])].append(row)
            self.forecast_curves = [
                {
                    "instrument": instrument,
                    "updated_utc": utc_now(),
                    "role": "entry_exit_timing",
                    "points": sorted(points, key=lambda item: int(item["horizon_sec"])),
                }
                for instrument, points in sorted(curves.items())
            ]
            self.top_forecasts_updated_utc = utc_now()
        if time.monotonic() - self.last_cycle_log >= 10.0:
            log("second_forecast_cycle", **self.summary(), ready_pairs=len(features_by_pair), candidates=len(candidates))
            self.last_cycle_log = time.monotonic()
        if time.monotonic() - self.last_state_save >= 10.0:
            atomic_json(self.state_path, self.summary())
            self.last_state_save = time.monotonic()
        return candidates

    def summary(self) -> dict[str, Any]:
        latency = sorted(self.latencies_ms)
        p50 = latency[len(latency) // 2] if latency else 0.0
        p95 = latency[min(len(latency) - 1, int(len(latency) * 0.95))] if latency else 0.0
        model_horizons = sorted(
            {
                int(row["horizon_sec"])
                for pair_rows in self.model.compiled.values()
                for row in pair_rows
            }
        )
        pair_model_count = sum(len(rows) for rows in self.model.compiled.values())
        return {
            "schema_version": SCHEMA_VERSION,
            "updated_utc": utc_now(),
            "model_path": str(self.model.path),
            "model_ready": self.model.ready,
            "research_enabled": self.research_enabled,
            "shadow_signal_tracking_enabled": self.shadow_signal_tracking_enabled,
            "model_fitted_utc": self.model.state.get("fitted_utc"),
            "model_pairs": len(self.model.state.get("models") or {}),
            "pairs_requested": int(
                self.model.state.get(
                    "pairs_requested",
                    len(self.model.state.get("models") or {}),
                )
            ),
            "complete_horizon_pair_count": int(
                self.model.state.get("complete_horizon_pair_count", 0)
            ),
            "pair_specific_model_count": int(
                self.model.state.get(
                    "pair_specific_model_count",
                    pair_model_count,
                )
            ),
            "proxy_model_count": int(
                self.model.state.get("proxy_model_count", 0)
            ),
            "model_family": MODEL_FAMILY,
            "input_timeframe": INPUT_TIMEFRAME,
            "training_timeframe": TRAINING_TIMEFRAME,
            "multi_horizon_smoothing": self.model.state.get(
                "multi_horizon_smoothing"
            )
            or {},
            "profiles": list(PROFILE_EXECUTION),
            "outcome_horizons_sec": model_horizons,
            "account_execution_horizons_sec": sorted(self.execution_horizons),
            "physical_lane_count": len(PROFILE_EXECUTION),
            "lane_horizon_surfaces": len(PROFILE_EXECUTION) * len(model_horizons),
            "pair_model_surfaces": pair_model_count,
            "pair_profile_surfaces": pair_model_count * len(PROFILE_EXECUTION),
            "cadence_sec": self.cadence_sec,
            "cycles": self.cycles,
            "forecasts": self.forecasts,
            "sampled_forecasts": self.sampled_forecasts,
            "accepted_signals": self.accepted_signals,
            "matured_outcomes": self.matured_outcomes,
            "pending_sampled_outcomes": 0 if self.store is None else len(self.store.pending),
            "latency_ms_p50": round(p50, 3),
            "latency_ms_p95": round(p95, 3),
            "latency_ms_latest": round(self.latencies_ms[-1], 3) if self.latencies_ms else 0.0,
            "top_forecasts": self.top_forecasts,
            "forecast_curves": self.forecast_curves,
            "top_forecasts_updated_utc": self.top_forecasts_updated_utc,
        }

    def close(self) -> None:
        atomic_json(self.state_path, self.summary())
        if self.store is not None:
            self.store.close()


__all__ = [
    "DEFAULT_EXECUTION_HORIZONS_SEC",
    "DEFAULT_HORIZONS_SEC",
    "FEATURE_NAMES",
    "INPUT_TIMEFRAME",
    "MODEL_FAMILY",
    "SecondFeatureEngine",
    "SecondForecastRuntime",
    "SecondForecastStore",
    "SecondPoint",
    "SecondRidgeSnapshot",
    "TRAINING_TIMEFRAME",
    "augment_cross_sectional_microstructure",
    "matrix_lane_id",
    "theoretical_pips",
]
