#!/usr/bin/env python3
"""Collect, mine, and score auditable fuzzy combinations of FX signals."""

from __future__ import annotations

import json
import math
import sqlite3
import statistics
import time
import zlib
from bisect import bisect_left
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

import numpy as np

try:
    from oanda_intrahour_forecast_contract import is_execution_only_feature
    from oanda_strategy_archetypes import strategy_archetype
except ModuleNotFoundError:
    from trad.oanda_intrahour_forecast_contract import is_execution_only_feature
    from trad.oanda_strategy_archetypes import strategy_archetype


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def feature_domain(feature: Any) -> str:
    """Return the independent information domain for a rule feature.

    This is intentionally diagnostic-only.  It makes a rule's raw condition
    depth comparable with its underlying informational breadth without
    changing mining, scoring, validation, or account eligibility.
    """

    name = str(feature or "").strip().lower()
    if not name:
        return "unclassified"
    if name.startswith("raw_"):
        family = name[4:]
        for suffix in ("_vote", "_activity"):
            if family.endswith(suffix):
                family = family[: -len(suffix)]
                break
        return strategy_archetype(family)
    if name.startswith(("cross_", "relative_residual_")) or "currency_strength" in name:
        return "cross_sectional_value"
    if name.startswith(("depth_", "order_book_", "position_book_", "volume_")):
        return "volatility_liquidity"
    if name.startswith("session_"):
        return "session_context"
    if "pattern" in name or name.startswith(("forecast_", "probability_")):
        return "statistical_forecast"
    if name.startswith(("regime_", "hurst_", "variance_ratio_")):
        return "regime_state"
    if name.startswith(("atr_", "spread_", "live_spread_", "candle_range_")):
        return "volatility_liquidity"
    if name.startswith(("bollinger_", "cci_", "stochastic_", "rsi_", "tick_vwap_", "vwap_", "candle_close_")):
        return "mean_reversion"
    if name.startswith(("ema", "macd_", "return_", "momentum_", "trend_")):
        return "trend_momentum"
    return "unclassified"


def rule_feature_domain_summary(conditions: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(conditions or [])
    domains = [feature_domain(row.get("feature")) for row in rows]
    unique_domains = sorted(set(domains))
    condition_count = len(rows)
    domain_count = len(unique_domains)
    return {
        "feature_domain_count": domain_count,
        "feature_domains": unique_domains,
        "redundant_condition_count": max(0, condition_count - domain_count),
        "independent_feature_ratio": round(domain_count / condition_count, 6) if condition_count else 0.0,
        "unclassified_features": sorted(
            str(row.get("feature") or "")
            for row, domain in zip(rows, domains)
            if domain == "unclassified"
        ),
        "feature_domain_shadow_only": True,
    }


def independence_adjusted_rule_score(rule: dict[str, Any]) -> float:
    """Return a shadow-only score that discounts redundant feature depth."""

    summary = rule_feature_domain_summary(rule.get("conditions") or [])
    ratio = finite(
        rule.get("independent_feature_ratio"),
        finite(summary.get("independent_feature_ratio")),
    )
    return round(finite(rule.get("score")) * max(0.0, min(1.0, ratio)), 8)


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    output = [float(values[0])]
    for value in values[1:]:
        output.append(alpha * float(value) + (1.0 - alpha) * output[-1])
    return output


def _rsi(values: list[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    changes = np.diff(np.asarray(values[-period - 1 :], dtype=float))
    gains = float(np.mean(np.maximum(changes, 0.0)))
    losses = float(np.mean(np.maximum(-changes, 0.0)))
    if losses <= 1e-12:
        return 100.0 if gains > 0.0 else 50.0
    return 100.0 - 100.0 / (1.0 + gains / losses)


def _cci(highs: list[float], lows: list[float], closes: list[float], period: int = 20) -> float:
    if len(closes) < period:
        return 0.0
    typical = np.asarray(
        [(highs[index] + lows[index] + closes[index]) / 3.0 for index in range(-period, 0)],
        dtype=float,
    )
    average = float(np.mean(typical))
    mean_deviation = float(np.mean(np.abs(typical - average)))
    return 0.0 if mean_deviation <= 1e-12 else float((typical[-1] - average) / (0.015 * mean_deviation))


def _stochastic(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
    if len(closes) < period:
        return 50.0
    highest = max(highs[-period:])
    lowest = min(lows[-period:])
    return 50.0 if math.isclose(highest, lowest) else 100.0 * (closes[-1] - lowest) / (highest - lowest)


def _zscore(values: list[float], period: int = 20) -> float:
    if len(values) < period:
        return 0.0
    sample = values[-period:]
    deviation = statistics.pstdev(sample)
    return 0.0 if deviation <= 1e-12 else (sample[-1] - statistics.fmean(sample)) / deviation


def _efficiency(values: list[float], period: int = 20) -> float:
    if len(values) < period:
        return 0.0
    sample = values[-period:]
    travel = sum(abs(sample[index] - sample[index - 1]) for index in range(1, len(sample)))
    return 0.0 if travel <= 1e-12 else abs(sample[-1] - sample[0]) / travel


def _normalized_gap(values: list[float], fast: int, slow: int, pip: float, atr_pips: float) -> float:
    if len(values) < slow:
        return 0.0
    denominator = max(0.1, atr_pips) * max(pip, 1e-12)
    return (_ema(values, fast)[-1] - _ema(values, slow)[-1]) / denominator


def _tick_vwap(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
    period: int,
) -> float:
    """Return a rolling typical-price VWAP using broker tick volume."""

    length = min(len(highs), len(lows), len(closes), len(volumes), max(1, int(period)))
    if length <= 0:
        return 0.0
    typical = np.asarray(
        [
            (highs[index] + lows[index] + closes[index]) / 3.0
            for index in range(-length, 0)
        ],
        dtype=float,
    )
    weights = np.maximum(
        0.0,
        np.asarray([finite(value) for value in volumes[-length:]], dtype=float),
    )
    total = float(np.sum(weights))
    return float(np.dot(typical, weights) / total) if total > 0.0 else float(np.mean(typical))


def _utc_hour(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    return parsed.hour + parsed.minute / 60.0 + parsed.second / 3600.0


def build_signal_vector(features: dict[str, Any]) -> dict[str, float]:
    """Create scale-stable numeric signals plus existing family vote outputs."""

    closes = [finite(value) for value in features.get("closes") or []]
    m5 = [finite(value) for value in features.get("m5_closes") or []]
    m15 = [finite(value) for value in features.get("m15_closes") or []]
    h1 = [finite(value) for value in features.get("h1_closes") or []]
    highs = [finite(value) for value in features.get("highs") or closes]
    lows = [finite(value) for value in features.get("lows") or closes]
    opens = [finite(value) for value in features.get("opens") or closes]
    volumes = [finite(value) for value in features.get("volumes") or [1.0] * len(closes)]
    if not closes:
        return {}
    pip = max(1e-12, finite(features.get("pip"), 0.0001))
    atr = max(0.1, finite(features.get("m1_atr14_pips"), 1.0))
    m5_atr = max(0.1, finite(features.get("m5_atr14_pips"), atr))
    macd_fast = _ema(closes, 12)
    macd_slow = _ema(closes, 26)
    macd = [left - right for left, right in zip(macd_fast, macd_slow)]
    macd_signal = _ema(macd, 9)
    histogram = [left - right for left, right in zip(macd, macd_signal)]
    ema_m1 = {period: _ema(closes, period) for period in (7, 8, 9)}
    ema_m5 = {period: _ema(m5, period) for period in (7, 8, 9)}
    ema7, ema8, ema9 = (ema_m1[period] for period in (7, 8, 9))
    ema7_m5, ema8_m5, ema9_m5 = (ema_m5[period] for period in (7, 8, 9))
    tick_vwap_2 = _tick_vwap(highs, lows, closes, volumes, 2)
    tick_vwap_5 = _tick_vwap(highs, lows, closes, volumes, 5)
    tick_vwap_15 = _tick_vwap(highs, lows, closes, volumes, 15)
    tick_vwap_30 = _tick_vwap(highs, lows, closes, volumes, 30)
    tick_vwap_60 = _tick_vwap(highs, lows, closes, volumes, 60)
    hour = _utc_hour(features.get("candle_time"))
    candle_range = max(1e-12, highs[-1] - lows[-1])
    body = closes[-1] - opens[-1]
    upper_tail = highs[-1] - max(opens[-1], closes[-1])
    lower_tail = min(opens[-1], closes[-1]) - lows[-1]
    vector = {
        "return_m1_atr": finite(features.get("r1_pips")) / atr,
        "return_m3_atr": finite(features.get("r3_pips")) / atr,
        "return_m5_atr": finite(features.get("r5_pips")) / atr,
        "return_m15_atr": finite(features.get("m5_r3_pips")) / m5_atr,
        "position_20": finite(features.get("pos20"), 0.5),
        "rsi_14": _rsi(closes),
        "stochastic_14": _stochastic(highs, lows, closes),
        "cci_20": _cci(highs, lows, closes),
        "bollinger_z_20": _zscore(closes),
        "efficiency_20": _efficiency(closes),
        "ema_gap_m1_5_13": _normalized_gap(closes, 5, 13, pip, atr),
        "ema_gap_m1_8_21": _normalized_gap(closes, 8, 21, pip, atr),
        "ema7_distance_m1_atr": (closes[-1] - ema7[-1]) / pip / atr if ema7 else 0.0,
        "ema8_distance_m1_atr": (closes[-1] - ema8[-1]) / pip / atr if ema8 else 0.0,
        "ema9_distance_m1_atr": (closes[-1] - ema9[-1]) / pip / atr if ema9 else 0.0,
        "ema7_slope_m1_atr": (ema7[-1] - ema7[-3]) / pip / atr if len(ema7) >= 3 else 0.0,
        "ema8_slope_m1_atr": (ema8[-1] - ema8[-3]) / pip / atr if len(ema8) >= 3 else 0.0,
        "ema9_slope_m1_atr": (ema9[-1] - ema9[-3]) / pip / atr if len(ema9) >= 3 else 0.0,
        "ema7_vs_ema8_m1_atr": (ema7[-1] - ema8[-1]) / pip / atr if ema7 and ema8 else 0.0,
        "ema8_vs_ema9_m1_atr": (ema8[-1] - ema9[-1]) / pip / atr if ema8 and ema9 else 0.0,
        "ema7_distance_m5_atr": (m5[-1] - ema7_m5[-1]) / pip / m5_atr if ema7_m5 else 0.0,
        "ema8_distance_m5_atr": (m5[-1] - ema8_m5[-1]) / pip / m5_atr if ema8_m5 else 0.0,
        "ema9_distance_m5_atr": (m5[-1] - ema9_m5[-1]) / pip / m5_atr if ema9_m5 else 0.0,
        "ema7_slope_m5_atr": (ema7_m5[-1] - ema7_m5[-3]) / pip / m5_atr if len(ema7_m5) >= 3 else 0.0,
        "ema8_slope_m5_atr": (ema8_m5[-1] - ema8_m5[-3]) / pip / m5_atr if len(ema8_m5) >= 3 else 0.0,
        "ema9_slope_m5_atr": (ema9_m5[-1] - ema9_m5[-3]) / pip / m5_atr if len(ema9_m5) >= 3 else 0.0,
        "ema_gap_m5_5_13": _normalized_gap(m5, 5, 13, pip, m5_atr),
        "ema_gap_m15_5_13": _normalized_gap(m15, 5, 13, pip, m5_atr),
        "ema_gap_h1_5_13": _normalized_gap(h1, 5, 13, pip, m5_atr),
        "macd_hist_atr": (histogram[-1] / pip / atr) if histogram else 0.0,
        "macd_hist_change_atr": ((histogram[-1] - histogram[-2]) / pip / atr) if len(histogram) > 1 else 0.0,
        "atr_m1_pips": atr,
        "atr_ratio_m1_m5": atr / m5_atr,
        "volume_ratio_12": finite(features.get("volume_ratio_12"), 1.0),
        "volume_ratio_30": finite(features.get("volume_ratio_30"), 1.0),
        "tick_vwap_distance_m1_2_atr": (closes[-1] - tick_vwap_2) / pip / atr,
        "tick_vwap_distance_m1_5_atr": (closes[-1] - tick_vwap_5) / pip / atr,
        "tick_vwap_distance_m1_15_atr": (closes[-1] - tick_vwap_15) / pip / atr,
        "tick_vwap_distance_m1_30_atr": (closes[-1] - tick_vwap_30) / pip / atr,
        "tick_vwap_distance_m1_60_atr": (closes[-1] - tick_vwap_60) / pip / atr,
        "ema7_vs_tick_vwap30_atr": (ema7[-1] - tick_vwap_30) / pip / atr if ema7 else 0.0,
        "ema8_vs_tick_vwap30_atr": (ema8[-1] - tick_vwap_30) / pip / atr if ema8 else 0.0,
        "ema9_vs_tick_vwap30_atr": (ema9[-1] - tick_vwap_30) / pip / atr if ema9 else 0.0,
        "spread_ratio_12": finite(features.get("spread_ratio_12"), 1.0),
        "spread_drop_atr": finite(features.get("spread_drop_3_pips")) / atr,
        "live_spread_atr": finite(features.get("live_spread_pips")) / atr,
        "candle_body_atr": body / pip / atr,
        "candle_range_atr": candle_range / pip / atr,
        "candle_close_location": (closes[-1] - lows[-1]) / candle_range,
        "upper_tail_atr": upper_tail / pip / atr,
        "lower_tail_atr": lower_tail / pip / atr,
        "cross_strength_m1": finite(features.get("cross_strength_r1")),
        "cross_strength_m3": finite(features.get("cross_strength_r3")),
        "cross_strength_m5": finite(features.get("cross_strength_r5")),
        "cross_breadth_m1": finite(features.get("cross_breadth_r1")),
        "cross_breadth_m3": finite(features.get("cross_breadth_r3")),
        "relative_residual_m3": finite(features.get("relative_residual_r3")),
        "relative_residual_m5": finite(features.get("relative_residual_r5")),
        "pair_rank_m3": finite(features.get("pair_rank_r3"), 0.5),
        "pair_rank_m5": finite(features.get("pair_rank_r5"), 0.5),
        "depth_imbalance": finite(features.get("depth_imbalance")),
        "depth_top_log_ratio": math.log(
            (1.0 + max(0.0, finite(features.get("bid_top_liquidity"))))
            / (1.0 + max(0.0, finite(features.get("ask_top_liquidity"))))
        ),
        "depth_total_log_ratio": math.log(
            (1.0 + max(0.0, finite(features.get("bid_total_liquidity"))))
            / (1.0 + max(0.0, finite(features.get("ask_total_liquidity"))))
        ),
        "depth_level_imbalance": (
            finite(features.get("bid_levels")) - finite(features.get("ask_levels"))
        )
        / max(1.0, finite(features.get("bid_levels")) + finite(features.get("ask_levels"))),
        "microprice_offset_atr": finite(features.get("microprice_offset_pips")) / atr,
        "depth_log_total_liquidity": finite(features.get("depth_log_total_liquidity")),
        "depth_top_imbalance": finite(features.get("depth_top_imbalance")),
        "order_book_near_5_imbalance": finite(features.get("order_book_near_5_imbalance")),
        "order_book_near_10_imbalance": finite(features.get("order_book_near_10_imbalance")),
        "order_book_near_25_imbalance": finite(features.get("order_book_near_25_imbalance")),
        "order_book_above_25_net": finite(features.get("order_book_above_25_net")),
        "order_book_below_25_net": finite(features.get("order_book_below_25_net")),
        "order_book_concentration": finite(features.get("order_book_concentration")),
        "position_book_near_5_imbalance": finite(features.get("position_book_near_5_imbalance")),
        "position_book_near_10_imbalance": finite(features.get("position_book_near_10_imbalance")),
        "position_book_near_25_imbalance": finite(features.get("position_book_near_25_imbalance")),
        "position_book_above_25_net": finite(features.get("position_book_above_25_net")),
        "position_book_below_25_net": finite(features.get("position_book_below_25_net")),
        "position_book_concentration": finite(features.get("position_book_concentration")),
        "order_book_available": finite(features.get("order_book_available")),
        "position_book_available": finite(features.get("position_book_available")),
        "session_asia": 1.0 if 0.0 <= hour < 8.0 else 0.0,
        "session_london": 1.0 if 7.0 <= hour < 16.0 else 0.0,
        "session_new_york": 1.0 if 12.0 <= hour < 21.0 else 0.0,
        "session_london_new_york_overlap": 1.0 if 12.0 <= hour < 16.0 else 0.0,
    }
    for name, value in (features.get("strategy_vote_features") or {}).items():
        vector[str(name)] = finite(value)
    return {
        name: round(value, 8)
        for name, value in vector.items()
        if math.isfinite(value) and not is_execution_only_feature(name)
    }


def fuzzy_membership(value: float, operator: str, threshold: float, width: float) -> float:
    width = max(abs(width), 1e-9)
    signed = (value - threshold) / width
    signed = max(-30.0, min(30.0, signed))
    if operator in {"=", "~=", "near"}:
        return math.exp(-0.5 * signed * signed)
    membership = 1.0 / (1.0 + math.exp(-signed))
    return membership if operator == ">=" else 1.0 - membership


class SignalCombinationStore:
    """Compressed feature snapshots and their future outcomes."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database_path, timeout=30.0)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id TEXT PRIMARY KEY,
                instrument TEXT NOT NULL,
                origin_time TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                feature_count INTEGER NOT NULL,
                features_zlib BLOB NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS outcomes (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                outcome_time TEXT NOT NULL,
                signed_move_pips REAL NOT NULL,
                long_net_pips REAL NOT NULL,
                short_net_pips REAL NOT NULL,
                UNIQUE(snapshot_id, horizon_sec),
                FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id)
            )
            """
        )
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_combo_horizon ON outcomes(horizon_sec, row_id)")
        self.connection.commit()
        self.pending_writes = 0

    def register_snapshot(
        self,
        snapshot_id: str,
        instrument: str,
        origin_time: str,
        features: dict[str, float],
    ) -> None:
        encoded = json.dumps(features, separators=(",", ":"), sort_keys=True).encode("utf-8")
        payload = sqlite3.Binary(zlib.compress(encoded, level=6))
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO snapshots
                (snapshot_id, instrument, origin_time, created_utc, feature_count, features_zlib)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, instrument, origin_time, utc_now(), len(features), payload),
        )
        self.pending_writes += max(0, cursor.rowcount)

    def observe(
        self,
        snapshot_id: str,
        horizon_sec: float,
        outcome_time: str,
        signed_move_pips: float,
        long_net_pips: float,
        short_net_pips: float,
    ) -> None:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO outcomes
                (snapshot_id, horizon_sec, outcome_time, signed_move_pips, long_net_pips, short_net_pips)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                int(horizon_sec),
                outcome_time,
                finite(signed_move_pips),
                finite(long_net_pips),
                finite(short_net_pips),
            ),
        )
        self.pending_writes += max(0, cursor.rowcount)

    def flush(self) -> None:
        if self.pending_writes:
            self.connection.commit()
            self.pending_writes = 0

    def close(self) -> None:
        self.flush()
        self.connection.close()


class SignalCombinationModel:
    """Hot-reload validated fuzzy rules and score the current market vector."""

    def __init__(
        self,
        state_path: Path,
        supplemental_state_paths: Iterable[Path] | None = None,
        maximum_state_age_sec: float | None = None,
    ) -> None:
        self.state_path = Path(state_path)
        self.state_paths = [
            self.state_path,
            *[
                Path(path)
                for path in (supplemental_state_paths or [])
                if Path(path) != self.state_path
            ],
        ]
        self.maximum_state_age_sec = (
            None
            if maximum_state_age_sec is None
            else max(1.0, float(maximum_state_age_sec))
        )
        self.modified_signature: tuple[tuple[str, int], ...] = ()
        self.state: dict[str, Any] = {}
        self._reload()

    def _reload(self) -> None:
        signature: list[tuple[str, int]] = []
        for path in self.state_paths:
            try:
                modified = path.stat().st_mtime_ns
            except OSError:
                modified = -1
            signature.append((str(path), modified))
        if self.maximum_state_age_sec is not None:
            # Re-evaluate a static artifact as wall-clock freshness changes,
            # without reparsing it on every strategy-lab scan.
            signature.append(("__freshness_hour__", int(time.time() // 3600)))
        frozen_signature = tuple(signature)
        if frozen_signature == self.modified_signature:
            return
        sources: list[dict[str, Any]] = []
        for path in self.state_paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                sources.append({"path": path, "payload": payload})
        if not sources:
            return
        multiple_sources = len(self.state_paths) > 1
        rules: list[dict[str, Any]] = []
        for source in sources:
            path = source["path"]
            payload = source["payload"]
            generated_text = str(payload.get("generated_at") or "").strip()
            try:
                generated_at = datetime.fromisoformat(
                    generated_text.replace("Z", "+00:00")
                )
                if generated_at.tzinfo is None:
                    generated_at = generated_at.replace(tzinfo=timezone.utc)
                source_age_sec = max(
                    0.0,
                    time.time() - generated_at.timestamp(),
                )
            except ValueError:
                source_age_sec = math.inf
            source["age_sec"] = source_age_sec
            source["fresh"] = bool(
                self.maximum_state_age_sec is None
                or source_age_sec <= self.maximum_state_age_sec
            )
            if not source["fresh"]:
                continue
            current_validation_schema = bool(
                int(finite(payload.get("schema_version"))) >= 3
                and int(
                    finite(
                        (payload.get("search_config") or {}).get(
                            "chronological_validation_blocks"
                        )
                    )
                )
                >= 2
            )
            for raw_rule in payload.get("rules") or []:
                if not isinstance(raw_rule, dict):
                    continue
                rule = dict(raw_rule)
                forward_confirmed = bool(rule.get("forward_refit_confirmed"))
                validation_policy_eligible = bool(
                    current_validation_schema and forward_confirmed
                )
                if not validation_policy_eligible:
                    rule["account_eligible"] = False
                    rule["validation_policy_reason"] = (
                        "awaiting_forward_refit_confirmation"
                        if current_validation_schema
                        else "legacy_combination_validation_schema"
                    )
                else:
                    rule["validation_policy_reason"] = ""
                rule["validation_policy_eligible"] = validation_policy_eligible
                original_id = str(rule.get("rule_id") or "")
                rule["original_rule_id"] = original_id
                rule["rule_source"] = path.name
                if multiple_sources:
                    rule["rule_id"] = f"{path.stem}:{original_id}"
                rules.append(rule)
        generated = [str(source["payload"].get("generated_at") or "") for source in sources]
        self.state = {
            "generated_at": max(generated, default=""),
            "rules": rules,
            "sources": [
                {
                    "path": str(source["path"]),
                    "generated_at": source["payload"].get("generated_at"),
                    "rule_count": len(source["payload"].get("rules") or []),
                    "age_sec": (
                        round(float(source["age_sec"]), 3)
                        if math.isfinite(float(source["age_sec"]))
                        else None
                    ),
                    "fresh": bool(source["fresh"]),
                    "retirement_reason": (
                        "" if source["fresh"] else "stale_model_state"
                    ),
                }
                for source in sources
            ],
        }
        self.modified_signature = frozen_signature

    def predict(self, vector: dict[str, float]) -> dict[str, Any]:
        self._reload()
        scored: list[dict[str, Any]] = []
        for rule in self.state.get("rules") or []:
            memberships: list[float] = []
            for condition in rule.get("conditions") or []:
                feature = str(condition.get("feature") or "")
                if feature not in vector:
                    memberships = []
                    break
                memberships.append(
                    fuzzy_membership(
                        vector[feature],
                        str(condition.get("operator") or ">="),
                        finite(condition.get("threshold")),
                        finite(condition.get("width"), 1.0),
                    )
                )
            if not memberships:
                continue
            membership = min(memberships)
            holdout = rule.get("holdout") or {}
            lower_edge = finite(holdout.get("lower_probability_edge"))
            support = finite(holdout.get("weighted_support"))
            score = membership * max(0.0, lower_edge) * math.log1p(max(0.0, support))
            scored.append(
                {
                    "ready": True,
                    "model_generated_at": self.state.get("generated_at"),
                    "rule_id": rule.get("rule_id"),
                    "rule_source": rule.get("rule_source"),
                    "original_rule_id": rule.get("original_rule_id"),
                    "rule_size": len(rule.get("conditions") or []),
                    "conditions": rule.get("conditions") or [],
                    "condition_text": rule.get("condition_text") or "",
                    "predicted_direction": rule.get("predicted_direction"),
                    "probability_up": finite(holdout.get("probability_up"), 0.5),
                    "expected_net_pips": finite(holdout.get("expected_net_pips")),
                    "expected_signed_move_pips": finite(holdout.get("expected_signed_move_pips")),
                    "train_support": finite((rule.get("training") or {}).get("weighted_support")),
                    "holdout_support": support,
                    "holdout_n": int(finite(holdout.get("n"))),
                    "holdout_lower_probability_edge": lower_edge,
                    "holdout_brier": finite(holdout.get("brier")),
                    "account_eligible": bool(rule.get("account_eligible", True)),
                    "validation_policy_eligible": bool(
                        rule.get("validation_policy_eligible")
                    ),
                    "validation_policy_reason": str(
                        rule.get("validation_policy_reason") or ""
                    ),
                    "forward_refit_confirmed": bool(
                        rule.get("forward_refit_confirmed")
                    ),
                    "shadow_reason": str(rule.get("shadow_reason") or ""),
                    "membership": round(membership, 6),
                    "score": round(score, 8),
                    "horizon_sec": int(finite(rule.get("horizon_sec"))),
                }
            )
        if not scored:
            return {
                "ready": False,
                "reason": (
                    "stale_combination_model_state"
                    if any(
                        not bool(row.get("fresh", True))
                        for row in self.state.get("sources") or []
                    )
                    else "no_validated_rule"
                ),
                "rules_available": len(self.state.get("rules") or []),
            }
        scored.sort(key=lambda item: (finite(item.get("score")), finite(item.get("membership"))), reverse=True)
        best = dict(scored[0])
        best["candidates"] = scored[:20]
        return best


def read_training_rows(
    database_path: Path,
    horizon_sec: int,
    max_rows: int,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(Path(database_path), timeout=30.0)
    connection.execute("PRAGMA query_only=ON")
    cursor = connection.execute(
        """
        SELECT o.row_id, s.instrument, s.origin_time, s.features_zlib,
               o.signed_move_pips, o.long_net_pips, o.short_net_pips
        FROM outcomes o
        JOIN snapshots s ON s.snapshot_id = o.snapshot_id
        WHERE o.horizon_sec = ?
        ORDER BY o.row_id DESC
        LIMIT ?
        """,
        (int(horizon_sec), int(max_rows)),
    )
    rows: list[dict[str, Any]] = []
    for row_id, instrument, origin_time, payload, signed_move, long_net, short_net in reversed(cursor.fetchall()):
        try:
            features = json.loads(zlib.decompress(payload).decode("utf-8"))
        except (TypeError, ValueError, zlib.error, json.JSONDecodeError):
            continue
        rows.append(
            {
                "row_id": row_id,
                "instrument": instrument,
                "origin_time": origin_time,
                "features": features,
                "signed_move_pips": finite(signed_move),
                "long_net_pips": finite(long_net),
                "short_net_pips": finite(short_net),
            }
        )
    connection.close()
    return rows


def _weighted_rule_metrics(
    membership: np.ndarray,
    signed_move: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
) -> dict[str, Any]:
    weights = np.clip(np.asarray(membership, dtype=float), 0.0, 1.0)
    support = float(np.sum(weights))
    n = int(np.sum(weights >= 0.5))
    if support <= 1e-9:
        return {
            "n": 0,
            "weighted_support": 0.0,
            "probability_up": 0.5,
            "expected_signed_move_pips": 0.0,
            "expected_net_pips": 0.0,
            "lower_probability_edge": 0.0,
            "brier": 0.25,
            "predicted_direction": "",
        }
    probability_up = float((np.dot(weights, signed_move > 0.0) + 5.0) / (support + 10.0))
    direction = "buy" if probability_up >= 0.5 else "sell"
    success_probability = probability_up if direction == "buy" else 1.0 - probability_up
    z = 1.645
    denominator = 1.0 + z * z / support
    center = (success_probability + z * z / (2.0 * support)) / denominator
    radius = z * math.sqrt(success_probability * (1.0 - success_probability) / support + z * z / (4.0 * support * support)) / denominator
    lower_edge = max(0.0, center - radius - 0.5)
    expected_signed = float(np.dot(weights, signed_move) / support)
    trade_values = long_net if direction == "buy" else short_net
    expected_net = float(np.dot(weights, trade_values) / support)
    actual_up = (signed_move > 0.0).astype(float)
    brier = float(np.dot(weights, np.square(actual_up - probability_up)) / support)
    return {
        "n": n,
        "weighted_support": round(support, 3),
        "probability_up": round(probability_up, 6),
        "expected_signed_move_pips": round(expected_signed, 4),
        "expected_net_pips": round(expected_net, 4),
        "lower_probability_edge": round(lower_edge, 6),
        "brier": round(brier, 6),
        "predicted_direction": direction,
    }


def _origin_epoch(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def chronological_partitions(
    rows: list[dict[str, Any]],
    horizon_sec: int,
    validation_blocks: int,
    train_fraction: float = 0.70,
) -> tuple[list[dict[str, Any]], slice, slice, slice, dict[str, Any]]:
    """Return timestamp-aligned, horizon-purged chronological partitions."""

    if not 0.5 <= float(train_fraction) < 0.95:
        raise ValueError("train_fraction must be in [0.5, 0.95)")
    ordered = list(rows)
    parsed = [_origin_epoch(row.get("origin_time")) for row in ordered]
    timestamp_aligned = bool(parsed and all(value is not None for value in parsed))
    if timestamp_aligned:
        ordered = [
            row
            for _, row in sorted(
                zip((float(value) for value in parsed), ordered),
                key=lambda item: (
                    item[0],
                    int(finite(item[1].get("row_id"))),
                ),
            )
        ]
        epochs = [float(_origin_epoch(row.get("origin_time")) or 0.0) for row in ordered]
        unique_epochs = sorted(set(epochs))
        timestamp_aligned = len(unique_epochs) >= 3
    else:
        epochs = []
        unique_epochs = []

    if timestamp_aligned:
        selection_epoch_index = max(
            1,
            min(
                len(unique_epochs) - 2,
                int(len(unique_epochs) * float(train_fraction)),
            ),
        )
        selection_epoch = unique_epochs[selection_epoch_index]
        selection_start = bisect_left(epochs, selection_epoch)
        train_end = bisect_left(epochs, selection_epoch - max(0, int(horizon_sec)))
        if validation_blocks == 1:
            selection_end = len(ordered)
            holdout_start = selection_start
            holdout_epoch = selection_epoch
        else:
            holdout_epoch_index = max(
                selection_epoch_index + 1,
                min(
                    len(unique_epochs) - 1,
                    int(
                        len(unique_epochs)
                        * (float(train_fraction) + (1.0 - float(train_fraction)) / 2.0)
                    ),
                ),
            )
            holdout_epoch = unique_epochs[holdout_epoch_index]
            holdout_start = bisect_left(epochs, holdout_epoch)
            selection_end = bisect_left(
                epochs,
                holdout_epoch - max(0, int(horizon_sec)),
            )
        if train_end > 0 and selection_end > selection_start and holdout_start < len(ordered):
            return (
                ordered,
                slice(0, train_end),
                slice(selection_start, selection_end),
                slice(holdout_start, len(ordered)),
                {
                    "timestamp_aligned": True,
                    "purge_sec": max(0, int(horizon_sec)),
                    "train_fraction": float(train_fraction),
                    "train_rows": train_end,
                    "selection_rows": selection_end - selection_start,
                    "holdout_rows": len(ordered) - holdout_start,
                    "selection_start_epoch": selection_epoch,
                    "holdout_start_epoch": holdout_epoch,
                },
            )

    split = max(
        1,
        min(len(ordered) - 1, int(len(ordered) * float(train_fraction))),
    )
    if validation_blocks == 1:
        selection_start = split
        selection_end = len(ordered)
        holdout_start = split
    else:
        selection_start = split
        selection_end = max(
            split + 1,
            min(len(ordered) - 1, split + (len(ordered) - split) // 2),
        )
        holdout_start = selection_end
    return (
        ordered,
        slice(0, split),
        slice(selection_start, selection_end),
        slice(holdout_start, len(ordered)),
        {
            "timestamp_aligned": False,
            "purge_sec": 0,
            "train_fraction": float(train_fraction),
            "train_rows": split,
            "selection_rows": selection_end - selection_start,
            "holdout_rows": len(ordered) - holdout_start,
        },
    )


def _rule_independence_audit(
    rows: list[dict[str, Any]],
    membership: np.ndarray,
    trade_values: np.ndarray,
    horizon_sec: int,
) -> dict[str, Any]:
    """Measure time- and instrument-level replication without counting every overlapping row."""

    weights = np.clip(np.asarray(membership, dtype=float), 0.0, 1.0)
    values = np.asarray(trade_values, dtype=float)
    active = np.flatnonzero(weights >= 0.5)
    timed = [
        (index, epoch)
        for index in active
        if (epoch := _origin_epoch(rows[int(index)].get("origin_time"))) is not None
    ]
    if not timed:
        return {
            "origin_span_sec": 0.0,
            "independent_time_bucket_count": 0,
            "positive_time_bucket_fraction": 0.0,
            "instrument_count": 0,
            "positive_instrument_fraction": 0.0,
            "maximum_instrument_weight_fraction": 1.0,
        }

    anchor = min(epoch for _, epoch in timed)
    bucket_width = max(1, int(horizon_sec))
    time_net: dict[int, float] = {}
    time_weight: dict[int, float] = {}
    instrument_net: dict[str, float] = {}
    instrument_weight: dict[str, float] = {}
    for index, epoch in timed:
        weight = float(weights[index])
        net = float(values[index]) * weight
        bucket = int((epoch - anchor) // bucket_width)
        instrument = str(rows[int(index)].get("instrument") or "unknown")
        time_net[bucket] = time_net.get(bucket, 0.0) + net
        time_weight[bucket] = time_weight.get(bucket, 0.0) + weight
        instrument_net[instrument] = instrument_net.get(instrument, 0.0) + net
        instrument_weight[instrument] = instrument_weight.get(instrument, 0.0) + weight

    time_means = [time_net[key] / max(time_weight[key], 1e-9) for key in time_net]
    instrument_means = [
        instrument_net[key] / max(instrument_weight[key], 1e-9) for key in instrument_net
    ]
    total_instrument_weight = sum(instrument_weight.values())
    return {
        "origin_span_sec": round(max(epoch for _, epoch in timed) - anchor, 3),
        "independent_time_bucket_count": len(time_means),
        "positive_time_bucket_fraction": round(
            sum(value > 0.0 for value in time_means) / max(1, len(time_means)),
            6,
        ),
        "instrument_count": len(instrument_means),
        "positive_instrument_fraction": round(
            sum(value > 0.0 for value in instrument_means) / max(1, len(instrument_means)),
            6,
        ),
        "maximum_instrument_weight_fraction": round(
            max(instrument_weight.values(), default=0.0) / max(total_instrument_weight, 1e-9),
            6,
        ),
    }


def _condition_membership(values: np.ndarray, operator: str, threshold: float, width: float) -> np.ndarray:
    signed = np.clip((values - threshold) / max(abs(width), 1e-9), -30.0, 30.0)
    if operator in {"=", "~=", "near"}:
        membership = np.exp(-0.5 * np.square(signed))
    else:
        membership = 1.0 / (1.0 + np.exp(-signed))
        membership = membership if operator == ">=" else 1.0 - membership
    membership[~np.isfinite(values)] = 0.0
    return membership


def mine_fuzzy_rules(
    rows: list[dict[str, Any]],
    horizon_sec: int,
    *,
    minimum_train_support: int = 80,
    minimum_holdout_support: int = 30,
    max_rules: int = 200,
    max_conditions: int = 10,
    beam_width: int = 24,
    expansion_conditions: int = 20,
    maximum_base_features: int = 24,
    conditions_per_feature: int = 2,
    minimum_independent_time_buckets: int = 3,
    minimum_instruments: int = 3,
    minimum_positive_time_bucket_fraction: float = 2.0 / 3.0,
    minimum_positive_instrument_fraction: float = 0.60,
    maximum_instrument_weight_fraction: float = 0.50,
    chronological_validation_blocks: int = 2,
) -> list[dict[str, Any]]:
    """Mine fuzzy interactions with independent chronological validation.

    Candidate discovery always uses the oldest 70% of observations.  The
    remaining observations are split into selection and final holdout blocks
    by default.  A rule must reproduce its direction and positive net edge in
    both blocks before it can be published.  ``chronological_validation_blocks
    = 1`` preserves the legacy single-holdout behavior for comparison tools.
    """

    if chronological_validation_blocks not in {1, 2}:
        raise ValueError("chronological_validation_blocks must be 1 or 2")
    validation_support = (
        int(minimum_holdout_support)
        if chronological_validation_blocks == 1
        else max(5, int(math.ceil(minimum_holdout_support / 2.0)))
    )
    required_support = minimum_train_support + validation_support * chronological_validation_blocks
    if len(rows) < max(200, required_support):
        return []
    if max_conditions < 2 or min(
        beam_width,
        expansion_conditions,
        maximum_base_features,
        conditions_per_feature,
    ) <= 0:
        raise ValueError("interaction search limits must be positive and max_conditions >= 2")
    rows, train_slice, selection_slice, holdout_slice, partition = (
        chronological_partitions(
            rows,
            horizon_sec,
            chronological_validation_blocks,
        )
    )
    selection_start = int(selection_slice.start or 0)
    selection_end = int(selection_slice.stop or len(rows))
    holdout_start = int(holdout_slice.start or 0)
    feature_names = sorted(
        {
            str(name)
            for row in rows
            for name, value in (row.get("features") or {}).items()
            if math.isfinite(finite(value, math.nan))
        }
    )
    matrix = np.full((len(rows), len(feature_names)), np.nan, dtype=np.float64)
    feature_index = {name: index for index, name in enumerate(feature_names)}
    for row_index, row in enumerate(rows):
        for name, value in (row.get("features") or {}).items():
            if name in feature_index:
                numeric = finite(value, math.nan)
                if math.isfinite(numeric):
                    matrix[row_index, feature_index[name]] = numeric
    signed_move = np.asarray([finite(row.get("signed_move_pips")) for row in rows], dtype=float)
    long_net = np.asarray([finite(row.get("long_net_pips")) for row in rows], dtype=float)
    short_net = np.asarray([finite(row.get("short_net_pips")) for row in rows], dtype=float)
    conditions: list[dict[str, Any]] = []
    memberships: dict[str, np.ndarray] = {}
    for name, index in feature_index.items():
        train_values = matrix[train_slice, index]
        valid = train_values[np.isfinite(train_values)]
        if len(valid) < minimum_train_support or float(np.ptp(valid)) <= 1e-12:
            continue
        q20, q35, q50, q65, q80 = np.quantile(valid, (0.20, 0.35, 0.50, 0.65, 0.80))
        width = max(float(q65 - q35) * 0.20, float(np.std(valid)) * 0.05, 1e-6)
        level_specs = [
            ("<=", float(q20), "very low"),
            ("<=", float(q35), "low"),
            ("~=", float(q50), "near median"),
            (">=", float(q65), "high"),
            (">=", float(q80), "very high"),
        ]
        if float(np.min(valid)) < 0.0 < float(np.max(valid)):
            level_specs.extend(
                [
                    ("<=", 0.0, "non-positive"),
                    ("~=", 0.0, "near zero"),
                    (">=", 0.0, "non-negative"),
                ]
            )
        seen_levels: set[tuple[str, float]] = set()
        for operator, threshold, label in level_specs:
            level_key = (operator, round(threshold, 10))
            if level_key in seen_levels:
                continue
            seen_levels.add(level_key)
            condition_id = f"{name}|{operator}|{threshold:.10g}"
            membership = _condition_membership(matrix[:, index], operator, threshold, width)
            metrics = _weighted_rule_metrics(
                membership[train_slice],
                signed_move[train_slice],
                long_net[train_slice],
                short_net[train_slice],
            )
            if metrics["weighted_support"] < minimum_train_support:
                continue
            edge = abs(finite(metrics["probability_up"], 0.5) - 0.5)
            score = edge * math.sqrt(metrics["weighted_support"]) * max(0.05, abs(finite(metrics["expected_net_pips"])))
            condition = {
                "id": condition_id,
                "feature": name,
                "operator": operator,
                "threshold": round(threshold, 8),
                "width": round(width, 8),
                "label": label,
                "training": metrics,
                "score": score,
            }
            conditions.append(condition)
            memberships[condition_id] = membership
    conditions.sort(key=lambda item: item["score"], reverse=True)
    by_feature: dict[str, list[dict[str, Any]]] = {}
    for condition in conditions:
        by_feature.setdefault(str(condition["feature"]), []).append(condition)
    feature_order = sorted(
        by_feature,
        key=lambda feature: by_feature[feature][0]["score"],
        reverse=True,
    )[:maximum_base_features]
    selected_by_feature = {
        feature: by_feature[feature][:conditions_per_feature] for feature in feature_order
    }
    top_conditions = sorted(
        [condition for values in selected_by_feature.values() for condition in values],
        key=lambda item: item["score"],
        reverse=True,
    )
    expansion_pool: list[dict[str, Any]] = []
    for condition_rank in range(conditions_per_feature):
        for feature in feature_order:
            values = selected_by_feature[feature]
            if condition_rank < len(values):
                expansion_pool.append(values[condition_rank])
                if len(expansion_pool) >= expansion_conditions:
                    break
        if len(expansion_pool) >= expansion_conditions:
            break

    seen_candidates: set[tuple[str, ...]] = set()

    def build_candidate(
        parts: tuple[dict[str, Any], ...],
        parent_membership: np.ndarray | None = None,
    ) -> dict[str, Any] | None:
        feature_names = {str(part["feature"]) for part in parts}
        if len(feature_names) != len(parts):
            return None
        identifiers = tuple(sorted(str(part["id"]) for part in parts))
        if identifiers in seen_candidates:
            return None
        seen_candidates.add(identifiers)
        membership = (
            np.minimum(parent_membership, memberships[str(parts[-1]["id"])])
            if parent_membership is not None
            else np.minimum.reduce([memberships[str(part["id"])] for part in parts])
        )
        train_metrics = _weighted_rule_metrics(
            membership[train_slice], signed_move[train_slice], long_net[train_slice], short_net[train_slice]
        )
        if train_metrics["weighted_support"] < minimum_train_support:
            return None
        edge = abs(finite(train_metrics["probability_up"], 0.5) - 0.5)
        if edge < 0.015 or finite(train_metrics["expected_net_pips"]) <= 0.0:
            return None
        raw_score = (
            finite(train_metrics["lower_probability_edge"])
            * math.log1p(finite(train_metrics["weighted_support"]))
            * max(0.05, finite(train_metrics["expected_net_pips"]))
        )
        complexity_penalty = 1.0 + 0.10 * max(0, len(parts) - 2)
        return {
            "parts": parts,
            "membership": membership,
            "training": train_metrics,
            "raw_training_score": raw_score,
            "training_score": raw_score / complexity_penalty,
            "condition_count": len(parts),
            "search_stage": "exhaustive_pair" if len(parts) == 2 else f"beam_depth_{len(parts)}",
        }

    pair_layer = [
        candidate
        for pair in combinations(top_conditions, 2)
        if (candidate := build_candidate(pair)) is not None
    ]
    pair_layer.sort(key=lambda item: item["training_score"], reverse=True)
    layers: dict[int, list[dict[str, Any]]] = {2: pair_layer}
    deepest = min(max_conditions, len(feature_order))
    for depth in range(3, deepest + 1):
        previous = layers.get(depth - 1) or []
        if not previous:
            break
        layer: list[dict[str, Any]] = []
        for parent in previous[:beam_width]:
            parent_features = {str(part["feature"]) for part in parent["parts"]}
            for condition in expansion_pool:
                if str(condition["feature"]) in parent_features:
                    continue
                candidate = build_candidate(
                    (*parent["parts"], condition),
                    parent["membership"],
                )
                if candidate is not None:
                    layer.append(candidate)
        layer.sort(key=lambda item: item["training_score"], reverse=True)
        layers[depth] = layer

    audited: list[dict[str, Any]] = []
    audit_per_depth = max(40, max_rules * 2)
    audit_candidates = [
        candidate
        for depth in sorted(layers)
        for candidate in layers[depth][:audit_per_depth]
    ]
    for candidate in audit_candidates:
        selection_metrics = _weighted_rule_metrics(
            candidate["membership"][selection_slice],
            signed_move[selection_slice],
            long_net[selection_slice],
            short_net[selection_slice],
        )
        if selection_metrics["weighted_support"] < validation_support:
            continue
        if (
            selection_metrics["lower_probability_edge"] <= 0.0
            or selection_metrics["expected_net_pips"] <= 0.0
        ):
            continue
        if (
            selection_metrics["predicted_direction"]
            != candidate["training"]["predicted_direction"]
        ):
            continue
        selection_trade_values = (
            long_net
            if selection_metrics["predicted_direction"] == "buy"
            else short_net
        )
        selection_independence = _rule_independence_audit(
            rows[selection_start:selection_end],
            candidate["membership"][selection_slice],
            selection_trade_values[selection_slice],
            horizon_sec,
        )
        final_holdout_metrics = _weighted_rule_metrics(
            candidate["membership"][holdout_slice],
            signed_move[holdout_slice],
            long_net[holdout_slice],
            short_net[holdout_slice],
        )
        if final_holdout_metrics["weighted_support"] < validation_support:
            continue
        if (
            final_holdout_metrics["lower_probability_edge"] <= 0.0
            or final_holdout_metrics["expected_net_pips"] <= 0.0
        ):
            continue
        if (
            final_holdout_metrics["predicted_direction"]
            != selection_metrics["predicted_direction"]
        ):
            continue
        holdout_trade_values = (
            long_net
            if final_holdout_metrics["predicted_direction"] == "buy"
            else short_net
        )
        final_holdout_independence = _rule_independence_audit(
            rows[holdout_start:],
            candidate["membership"][holdout_slice],
            holdout_trade_values[holdout_slice],
            horizon_sec,
        )
        # Preserve the legacy 30%-window calibration sample for consumers and
        # profile support thresholds, but only after each chronological half
        # has independently reproduced the rule.
        holdout_metrics = _weighted_rule_metrics(
            np.concatenate(
                (
                    candidate["membership"][selection_slice],
                    candidate["membership"][holdout_slice],
                )
            ),
            np.concatenate((signed_move[selection_slice], signed_move[holdout_slice])),
            np.concatenate((long_net[selection_slice], long_net[holdout_slice])),
            np.concatenate((short_net[selection_slice], short_net[holdout_slice])),
        )
        validation_rows = [
            *rows[selection_start:selection_end],
            *rows[holdout_start:],
        ]
        validation_membership = np.concatenate(
            (
                candidate["membership"][selection_slice],
                candidate["membership"][holdout_slice],
            )
        )
        validation_trade_values = (
            np.concatenate((long_net[selection_slice], long_net[holdout_slice]))
            if final_holdout_metrics["predicted_direction"] == "buy"
            else np.concatenate((short_net[selection_slice], short_net[holdout_slice]))
        )
        independence = _rule_independence_audit(
            validation_rows,
            validation_membership,
            validation_trade_values,
            horizon_sec,
        )
        eligibility_checks = {
            "independent_time_buckets": (
                independence["independent_time_bucket_count"] >= minimum_independent_time_buckets
            ),
            "time_bucket_profitability": (
                independence["positive_time_bucket_fraction"]
                >= minimum_positive_time_bucket_fraction
            ),
            "instrument_replication": independence["instrument_count"] >= minimum_instruments,
            "instrument_profitability": (
                independence["positive_instrument_fraction"]
                >= minimum_positive_instrument_fraction
            ),
            "instrument_concentration": (
                independence["maximum_instrument_weight_fraction"]
                <= maximum_instrument_weight_fraction
            ),
        }
        account_eligible = all(eligibility_checks.values())
        failed_checks = [name for name, passed in eligibility_checks.items() if not passed]
        conditions_out = [
            {
                "feature": part["feature"],
                "operator": part["operator"],
                "threshold": part["threshold"],
                "width": part["width"],
                "label": part["label"],
            }
            for part in candidate["parts"]
        ]
        condition_text = " AND ".join(
            f"{part['feature']} {part['operator']} {part['threshold']:.4g}" for part in conditions_out
        )
        selection_score = (
            selection_metrics["lower_probability_edge"]
            * math.log1p(selection_metrics["weighted_support"])
            * selection_metrics["expected_net_pips"]
        ) / (1.0 + 0.10 * max(0, len(candidate["parts"]) - 2))
        final_holdout_score = (
            final_holdout_metrics["lower_probability_edge"]
            * math.log1p(final_holdout_metrics["weighted_support"])
            * final_holdout_metrics["expected_net_pips"]
        ) / (1.0 + 0.10 * max(0, len(candidate["parts"]) - 2))
        holdout_score = (
            holdout_metrics["lower_probability_edge"]
            * math.log1p(holdout_metrics["weighted_support"])
            * holdout_metrics["expected_net_pips"]
        ) / (1.0 + 0.10 * max(0, len(candidate["parts"]) - 2))
        robust_score = min(selection_score, final_holdout_score)
        condition_count = len(candidate["parts"])
        feature_domain_summary = rule_feature_domain_summary(conditions_out)
        audited.append(
            {
                "rule_id": f"h{int(horizon_sec)}-r{len(audited) + 1}",
                "horizon_sec": int(horizon_sec),
                "condition_count": condition_count,
                **feature_domain_summary,
                "search_stage": candidate["search_stage"],
                "conditions": conditions_out,
                "condition_text": condition_text,
                "predicted_direction": final_holdout_metrics["predicted_direction"],
                "training": candidate["training"],
                "selection": selection_metrics,
                "holdout": holdout_metrics,
                "final_holdout": final_holdout_metrics,
                "selection_independence_audit": selection_independence,
                "final_holdout_independence_audit": final_holdout_independence,
                "independence_audit": independence,
                "chronological_partition": partition,
                "training_score": round(candidate["training_score"], 8),
                "selection_score": round(selection_score, 8),
                "holdout_score": round(holdout_score, 8),
                "final_holdout_score": round(final_holdout_score, 8),
                "score": round(robust_score, 8),
                "account_eligible": account_eligible,
                "experimental_depth": condition_count > 3,
                "shadow_reason": "" if account_eligible else "failed_" + ",".join(failed_checks),
            }
        )
    audited.sort(key=lambda item: (item["score"], item["holdout"]["weighted_support"]), reverse=True)

    # Preserve a small diagnostic sample from each validated depth, then fill by score.
    by_depth: dict[int, list[dict[str, Any]]] = {}
    for rule in audited:
        by_depth.setdefault(int(rule["condition_count"]), []).append(rule)
    reserve_per_depth = max(2, min(6, max_rules // max(1, 2 * len(by_depth))))
    selected: list[dict[str, Any]] = []
    selected_ids: set[int] = set()
    for depth in sorted(by_depth):
        for rule in by_depth[depth][:reserve_per_depth]:
            selected.append(rule)
            selected_ids.add(id(rule))
    for rule in audited:
        if len(selected) >= max_rules:
            break
        if id(rule) not in selected_ids:
            selected.append(rule)
            selected_ids.add(id(rule))
    selected.sort(key=lambda item: (item["score"], item["holdout"]["weighted_support"]), reverse=True)
    for index, rule in enumerate(selected[:max_rules], start=1):
        rule["rule_id"] = f"h{int(horizon_sec)}-r{index}"
    return selected[:max_rules]


def database_counts(database_path: Path) -> dict[str, Any]:
    if not Path(database_path).is_file():
        return {"snapshots": 0, "outcomes": 0, "horizons": {}}
    connection = sqlite3.connect(Path(database_path), timeout=30.0)
    snapshots = int(connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])
    outcomes = int(connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0])
    horizons = {
        str(int(horizon)): int(count)
        for horizon, count in connection.execute("SELECT horizon_sec, COUNT(*) FROM outcomes GROUP BY horizon_sec")
    }
    connection.close()
    return {"snapshots": snapshots, "outcomes": outcomes, "horizons": horizons}


def write_rule_state(
    state_path: Path,
    database_path: Path,
    rules: list[dict[str, Any]],
    horizon_rows: dict[str, int],
    search_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    counts = database_counts(database_path)
    config = dict(search_config or {})
    maximum_rule_size = int(config.get("max_conditions") or max((len(rule.get("conditions") or []) for rule in rules), default=3))
    depth_counts: dict[str, int] = {}
    feature_domain_depth_counts: dict[str, int] = {}
    rules_with_domains: list[dict[str, Any]] = []
    for rule in rules:
        rule_out = dict(rule)
        rule_out.update(rule_feature_domain_summary(rule.get("conditions") or []))
        rule_out["independence_adjusted_score"] = independence_adjusted_rule_score(
            rule_out
        )
        rule_out["independence_adjusted_shadow_only"] = True
        rules_with_domains.append(rule_out)
        depth = str(int(rule_out.get("condition_count") or len(rule_out.get("conditions") or [])))
        depth_counts[depth] = depth_counts.get(depth, 0) + 1
        domain_depth = str(int(rule_out.get("feature_domain_count") or 0))
        feature_domain_depth_counts[domain_depth] = feature_domain_depth_counts.get(domain_depth, 0) + 1
    independence_ranked = sorted(
        rules_with_domains,
        key=lambda row: (
            finite(row.get("independence_adjusted_score")),
            finite((row.get("holdout") or {}).get("weighted_support")),
        ),
        reverse=True,
    )
    independence_rank_by_id = {
        str(row.get("rule_id") or id(row)): index
        for index, row in enumerate(independence_ranked, start=1)
    }
    for row in rules_with_domains:
        row["independence_adjusted_rank"] = independence_rank_by_id[
            str(row.get("rule_id") or id(row))
        ]
    raw_top_ids = {
        str(row.get("rule_id") or id(row)) for row in rules_with_domains[:12]
    }
    adjusted_top_ids = {
        str(row.get("rule_id") or id(row)) for row in independence_ranked[:12]
    }
    payload = {
        "schema_version": 3,
        "generated_at": utc_now(),
        "status": "validated_rules_available" if rules else "collecting_support",
        "database": str(Path(database_path).resolve()),
        "method": (
            "exhaustive fuzzy 2-feature conjunctions plus complexity-penalized beam search "
            f"through {maximum_rule_size} features; discovered on oldest 70%, selected on the "
            "next 15%, and independently confirmed on the newest 15%; boundaries align to "
            "market timestamps and purge one forecast horizon"
        ),
        "minimum_rule_size": 2,
        "maximum_rule_size": maximum_rule_size,
        "validated_deep_rules_wired_to_signal_feed": True,
        "search_config": config,
        "counts": counts,
        "horizon_fit_rows": horizon_rows,
        "validated_rule_count": len(rules),
        "account_eligible_rule_count": sum(bool(rule.get("account_eligible", True)) for rule in rules),
        "rule_depth_counts": depth_counts,
        "feature_domain_depth_counts": feature_domain_depth_counts,
        "feature_domain_policy": "one diagnostic count per independent information domain; shadow-only",
        "independence_adjusted_ranking_policy": "raw_score_times_independent_feature_ratio_shadow_v1",
        "raw_top12_independence_adjusted_overlap_count": len(
            raw_top_ids.intersection(adjusted_top_ids)
        ),
        "rules": rules_with_domains,
        "top_rules": rules_with_domains[:12],
        "top_rules_independence_adjusted": independence_ranked[:12],
    }
    state_path = Path(state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(state_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(state_path)
    return payload


__all__ = [
    "SignalCombinationModel",
    "SignalCombinationStore",
    "build_signal_vector",
    "database_counts",
    "feature_domain",
    "fuzzy_membership",
    "chronological_partitions",
    "mine_fuzzy_rules",
    "independence_adjusted_rule_score",
    "read_training_rows",
    "rule_feature_domain_summary",
    "write_rule_state",
]
