#!/usr/bin/env python3
"""Backtest pre-spike lead prediction on the technical all-pair dataset.

This is research-only.  It asks a different question from the missed-move
report:

    At time T, can current technical/session/regime features predict that a
    major move will start soon, enter before it, and exit through existing
    stop/target/trailing path outcomes if the setup is wrong?

The script writes a compact JSON report plus fold-level CSV evidence.  It does
not place orders and does not modify any live account state.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings(
    "ignore",
    message=r"`sklearn\.utils\.parallel\.delayed` should be used",
    category=UserWarning,
)

from sklearn.ensemble import (
    ExtraTreesClassifier,
    GradientBoostingClassifier,
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score

import oanda_gpt_training_strategy_manager as manager


ROOT = Path(__file__).resolve().parent
RESEARCH_ROOT = ROOT / "data" / "oanda_training_manager" / "continuous_research"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_DATASET = RESEARCH_ROOT / "technical_spike_research.parquet"
DEFAULT_REPORT = REPORT_ROOT / "latest_pre_spike_lead_backtest.json"
DEFAULT_FOLDS_CSV = REPORT_ROOT / "latest_pre_spike_lead_backtest_folds.csv"

EXECUTION_HORIZONS = [30, 60, 120, 240]

EXPANDED_PRE_SPIKE_SOURCE_COLUMNS = [
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "momentum_60_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "ema8_minus_ema21_atr",
    "ema21_slope_15_atr",
    "ema55_slope_30_atr",
    "macd_hist_atr",
    "ma_stack_score",
    "rsi14_centered",
    "compression_30",
    "realized_vol_ratio_30_240",
    "range_position_60_centered",
    "range_position_240_centered",
    "donchian_60_position_centered",
    "donchian_60_breakout_atr",
    "donchian_240_position_centered",
    "donchian_240_breakout_atr",
    "trend_consistency_60",
    "spread_ratio_60",
    "spread_pips",
    "volume_z_30",
    "strength_gap_15",
    "strength_gap_60",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
    "atr240_pips",
    "shadow_rule_count",
    "is_asia_session",
    "is_london_session",
    "is_new_york_session",
    "is_london_ny_overlap",
    "is_rollover_hour",
    "is_volatile_or_exotic_pair",
    "base_is_risk",
    "quote_is_risk",
    "base_is_jpy",
    "quote_is_jpy",
    "base_is_chf",
    "quote_is_chf",
    "base_is_commodity",
    "quote_is_commodity",
    "carry_proxy_abs_diff",
    "regime_trend_strength",
    "regime_volatility_expansion_score",
    "regime_compression_score",
    "regime_spread_cost_score",
    "regime_usd_strength_15",
    "regime_usd_strength_60",
    "regime_jpy_strength_15",
    "regime_chf_strength_15",
    "regime_risk_on_score",
    "regime_commodity_score",
    "regime_acceleration_score",
    "regime_is_risk_off",
    "regime_is_low_vol_chop",
    "regime_is_high_vol_event",
    "regime_is_spread_impaired",
    "macro_pair_bias",
    "macro_news_risk",
    "macro_event_risk",
]

RULE_BASELINE_COLUMNS = [
    "rule_pre_breakout_long",
    "rule_pre_breakout_short",
    "rule_breakout_continuation_long",
    "rule_breakout_continuation_short",
    "rule_exhaustion_reversal_long",
    "rule_exhaustion_reversal_short",
    "rule_session_timing_long",
    "rule_session_timing_short",
    "rule_cross_pressure_long",
    "rule_cross_pressure_short",
    "rule_liquidity_ok",
    "rule_anti_chase_penalty",
    "rule_value_liquidity_ok",
    "rule_late_momentum_confirmation_long",
    "rule_late_momentum_confirmation_short",
    "rule_value_breakout_long",
    "rule_value_breakout_short",
    "rule_guarded_exhaustion_reversal_long",
    "rule_guarded_exhaustion_reversal_short",
    "rule_scout_value_long_score",
    "rule_scout_value_short_score",
    "rule_baseline_long_score",
    "rule_baseline_short_score",
    "rule_baseline_direction_score",
    "rule_baseline_strength_score",
    "rule_baseline_long_candidate",
    "rule_baseline_short_candidate",
]

RULE_PROFILE_SPECS: Dict[str, Dict[str, Any]] = {
    "baseline": {
        "long": {"rule_baseline_long_score": 1.0},
        "short": {"rule_baseline_short_score": 1.0},
    },
    "strict_breakout": {
        "long": {
            "rule_breakout_continuation_long": 2.00,
            "rule_pre_breakout_long": 0.85,
            "rule_cross_pressure_long": 0.75,
            "rule_session_timing_long": 0.45,
            "rule_liquidity_ok": 0.55,
            "rule_anti_chase_penalty": -2.20,
        },
        "short": {
            "rule_breakout_continuation_short": 2.00,
            "rule_pre_breakout_short": 0.85,
            "rule_cross_pressure_short": 0.75,
            "rule_session_timing_short": 0.45,
            "rule_liquidity_ok": 0.55,
            "rule_anti_chase_penalty": -2.20,
        },
    },
    "compression_breakout": {
        "long": {
            "rule_pre_breakout_long": 1.75,
            "rule_session_timing_long": 0.70,
            "rule_cross_pressure_long": 0.80,
            "rule_liquidity_ok": 0.70,
            "rule_anti_chase_penalty": -1.70,
        },
        "short": {
            "rule_pre_breakout_short": 1.75,
            "rule_session_timing_short": 0.70,
            "rule_cross_pressure_short": 0.80,
            "rule_liquidity_ok": 0.70,
            "rule_anti_chase_penalty": -1.70,
        },
    },
    "cross_pressure": {
        "long": {
            "rule_cross_pressure_long": 1.80,
            "rule_breakout_continuation_long": 0.95,
            "rule_session_timing_long": 0.75,
            "rule_liquidity_ok": 0.50,
            "rule_anti_chase_penalty": -1.60,
        },
        "short": {
            "rule_cross_pressure_short": 1.80,
            "rule_breakout_continuation_short": 0.95,
            "rule_session_timing_short": 0.75,
            "rule_liquidity_ok": 0.50,
            "rule_anti_chase_penalty": -1.60,
        },
    },
    "exhaustion_reversal": {
        "long": {
            "rule_exhaustion_reversal_long": 2.00,
            "rule_session_timing_long": 0.60,
            "rule_liquidity_ok": 0.60,
            "rule_cross_pressure_long": 0.35,
            "rule_anti_chase_penalty": -1.25,
        },
        "short": {
            "rule_exhaustion_reversal_short": 2.00,
            "rule_session_timing_short": 0.60,
            "rule_liquidity_ok": 0.60,
            "rule_cross_pressure_short": 0.35,
            "rule_anti_chase_penalty": -1.25,
        },
    },
    "value_breakout_confirmation": {
        "long": {
            "rule_value_breakout_long": 2.20,
            "rule_late_momentum_confirmation_long": 1.10,
            "rule_cross_pressure_long": 0.65,
            "rule_value_liquidity_ok": 0.85,
            "rule_anti_chase_penalty": -1.35,
        },
        "short": {
            "rule_value_breakout_short": 2.20,
            "rule_late_momentum_confirmation_short": 1.10,
            "rule_cross_pressure_short": 0.65,
            "rule_value_liquidity_ok": 0.85,
            "rule_anti_chase_penalty": -1.35,
        },
    },
    "late_momentum_scout": {
        "long": {
            "rule_late_momentum_confirmation_long": 2.25,
            "rule_session_timing_long": 0.65,
            "rule_value_liquidity_ok": 0.80,
            "rule_anti_chase_penalty": -1.10,
        },
        "short": {
            "rule_late_momentum_confirmation_short": 2.25,
            "rule_session_timing_short": 0.65,
            "rule_value_liquidity_ok": 0.80,
            "rule_anti_chase_penalty": -1.10,
        },
    },
    "guarded_exhaustion_value": {
        "long": {
            "rule_guarded_exhaustion_reversal_long": 2.25,
            "rule_value_liquidity_ok": 0.95,
            "rule_cross_pressure_long": 0.45,
            "rule_anti_chase_penalty": -1.70,
        },
        "short": {
            "rule_guarded_exhaustion_reversal_short": 2.25,
            "rule_value_liquidity_ok": 0.95,
            "rule_cross_pressure_short": 0.45,
            "rule_anti_chase_penalty": -1.70,
        },
    },
    "scout_value_hybrid": {
        "long": {
            "rule_scout_value_long_score": 1.00,
            "rule_value_liquidity_ok": 0.60,
            "rule_anti_chase_penalty": -1.25,
        },
        "short": {
            "rule_scout_value_short_score": 1.00,
            "rule_value_liquidity_ok": 0.60,
            "rule_anti_chase_penalty": -1.25,
        },
    },
    "hybrid_liquidity": {
        "long": {
            "rule_baseline_long_score": 0.85,
            "rule_liquidity_ok": 0.80,
            "rule_session_timing_long": 0.50,
            "rule_anti_chase_penalty": -1.90,
        },
        "short": {
            "rule_baseline_short_score": 0.85,
            "rule_liquidity_ok": 0.80,
            "rule_session_timing_short": 0.50,
            "rule_anti_chase_penalty": -1.90,
        },
    },
    "context_persistent_bucket": {
        "long": {
            "rule_baseline_long_score": 1.00,
            "rule_value_liquidity_ok": 0.25,
            "rule_anti_chase_penalty": -0.35,
        },
        "short": {
            "rule_baseline_short_score": 1.00,
            "rule_value_liquidity_ok": 0.25,
            "rule_anti_chase_penalty": -0.35,
        },
        "context_filter": {
            "group": "instrument_direction",
            "min_count": 2,
            "min_mean_pips": 0.0,
            "min_profit_factor": 1.15,
            "min_positive_rate": 0.40,
            "top_n_by_sum": 12,
            "exclude_regimes": ["jpy_unwind"],
            "fallback_to_unfiltered_when_empty": False,
        },
    },
    "context_persistent_value": {
        "long": {
            "rule_baseline_long_score": 0.75,
            "rule_scout_value_long_score": 0.35,
            "rule_value_liquidity_ok": 0.60,
            "rule_late_momentum_confirmation_long": 0.25,
            "rule_anti_chase_penalty": -0.60,
        },
        "short": {
            "rule_baseline_short_score": 0.75,
            "rule_scout_value_short_score": 0.35,
            "rule_value_liquidity_ok": 0.60,
            "rule_late_momentum_confirmation_short": 0.25,
            "rule_anti_chase_penalty": -0.60,
        },
        "context_filter": {
            "group": "instrument_direction",
            "min_count": 1,
            "min_mean_pips": 5.0,
            "min_profit_factor": 1.35,
            "min_positive_rate": 0.45,
            "top_n_by_sum": 10,
            "exclude_regimes": ["jpy_unwind", "low_vol_chop"],
            "fallback_to_unfiltered_when_empty": False,
        },
    },
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except Exception:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    replace_with_retry(tmp, path)


def replace_with_retry(tmp: Path, path: Path, *, attempts: int = 20) -> None:
    """Replace report artifacts while tolerating transient Windows locks.

    The live advisor can read report artifacts while long research runs are
    writing incremental updates.  On Windows that can briefly block
    ``os.replace`` with ``PermissionError``.  Retrying keeps research-only
    runs from dying because a monitor read raced an artifact update.
    """
    delay = 0.05
    for attempt in range(1, attempts + 1):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            if attempt >= attempts:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 1.0)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    if not rows:
        tmp.write_text("", encoding="utf-8")
        replace_with_retry(tmp, path)
        return
    fields = sorted({key for row in rows for key in row})
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    replace_with_retry(tmp, path)


def parquet_columns(path: Path) -> List[str]:
    try:
        import pyarrow.parquet as pq  # type: ignore

        return list(pq.ParquetFile(path).schema.names)
    except Exception:
        frame = pd.read_parquet(path)
        return list(frame.columns)


def execution_horizon(lead_minutes: int, move_horizon: int) -> int:
    needed = int(lead_minutes) + int(move_horizon)
    for horizon in EXECUTION_HORIZONS:
        if horizon >= needed:
            return horizon
    return EXECUTION_HORIZONS[-1]


def summarize(values: Iterable[float]) -> Dict[str, Any]:
    vals = [float(v) for v in values if math.isfinite(float(v))]
    if not vals:
        return {"count": 0}
    vals.sort()
    wins = [v for v in vals if v > 0]
    losses = [-v for v in vals if v < 0]
    return {
        "count": len(vals),
        "sum": float(sum(vals)),
        "mean": float(sum(vals) / len(vals)),
        "median": float(vals[len(vals) // 2]),
        "positive_count": len(wins),
        "positive_rate": float(len(wins) / len(vals)),
        "min": float(min(vals)),
        "max": float(max(vals)),
        "profit_factor": float(sum(wins) / max(sum(losses), 1e-9)),
    }


def feature_columns(feature_set: str) -> List[str]:
    if feature_set == "core":
        base = list(manager.TECHNICAL_CORE_FEATURES)
    else:
        base = list(manager.TECHNICAL_MODEL_FEATURES)
    return list(dict.fromkeys(base))


def add_expanded_pre_spike_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    """Add deterministic lead-entry features from already-known row values."""
    out = frame.copy()
    eps = 1e-9
    definitions: Dict[str, Any] = {}

    def col(name: str) -> pd.Series:
        if name not in out:
            return pd.Series(0.0, index=out.index)
        return pd.to_numeric(out[name], errors="coerce").fillna(0.0)

    definitions["abs_momentum_5_atr"] = col("momentum_5_atr").abs()
    definitions["abs_momentum_15_atr"] = col("momentum_15_atr").abs()
    definitions["abs_momentum_30_atr"] = col("momentum_30_atr").abs()
    definitions["abs_momentum_60_atr"] = col("momentum_60_atr").abs()
    definitions["momentum_5_15_delta_atr"] = col("momentum_5_atr") - col("momentum_15_atr")
    definitions["momentum_15_30_delta_atr"] = col("momentum_15_atr") - col("momentum_30_atr")
    definitions["momentum_30_60_delta_atr"] = col("momentum_30_atr") - col("momentum_60_atr")
    definitions["momentum_alignment_long"] = (
        (col("momentum_5_atr") > 0).astype(int)
        + (col("momentum_15_atr") > 0).astype(int)
        + (col("momentum_30_atr") > 0).astype(int)
        + (col("momentum_60_atr") > 0).astype(int)
    )
    definitions["momentum_alignment_short"] = (
        (col("momentum_5_atr") < 0).astype(int)
        + (col("momentum_15_atr") < 0).astype(int)
        + (col("momentum_30_atr") < 0).astype(int)
        + (col("momentum_60_atr") < 0).astype(int)
    )
    definitions["sma7_gt_sma8"] = (col("sma7_minus_8_atr") > 0).astype(int)
    definitions["sma7_lt_sma8"] = (col("sma7_minus_8_atr") < 0).astype(int)
    definitions["sma30_slope_gt_1_atr"] = (col("sma30_slope_5_atr") > 1.0).astype(int)
    definitions["sma30_slope_lt_minus_1_atr"] = (col("sma30_slope_5_atr") < -1.0).astype(int)
    definitions["ema8_gt_ema21"] = (col("ema8_minus_ema21_atr") > 0).astype(int)
    definitions["ema8_lt_ema21"] = (col("ema8_minus_ema21_atr") < 0).astype(int)
    definitions["macd_hist_positive"] = (col("macd_hist_atr") > 0).astype(int)
    definitions["macd_hist_negative"] = (col("macd_hist_atr") < 0).astype(int)
    definitions["donchian60_breakout_long"] = (col("donchian_60_breakout_atr") > 0.5).astype(int)
    definitions["donchian60_breakout_short"] = (col("donchian_60_breakout_atr") < -0.5).astype(int)
    definitions["donchian240_breakout_long"] = (col("donchian_240_breakout_atr") > 0.5).astype(int)
    definitions["donchian240_breakout_short"] = (col("donchian_240_breakout_atr") < -0.5).astype(int)
    definitions["range60_extreme_abs"] = col("range_position_60_centered").abs()
    definitions["range240_extreme_abs"] = col("range_position_240_centered").abs()
    definitions["strength_gap_abs_15"] = col("strength_gap_15").abs()
    definitions["strength_gap_abs_60"] = col("strength_gap_60").abs()
    definitions["atr_spread_efficiency"] = col("atr240_pips") / (col("spread_pips").abs() + eps)
    definitions["spread_cost_pressure"] = col("spread_ratio_60") * col("regime_spread_cost_score")
    definitions["compression_breakout_pressure"] = (
        (1.0 - col("compression_30").clip(lower=0.0, upper=1.0))
        * col("realized_vol_ratio_30_240").clip(lower=0.0)
    )
    definitions["london_volatility_pressure"] = col("is_london_session") * col("regime_volatility_expansion_score")
    definitions["ny_volatility_pressure"] = col("is_new_york_session") * col("regime_volatility_expansion_score")
    definitions["overlap_breakout_pressure"] = col("is_london_ny_overlap") * definitions["compression_breakout_pressure"]
    definitions["rollover_spread_penalty"] = col("is_rollover_hour") * col("spread_ratio_60")
    definitions["volatile_spread_penalty"] = col("is_volatile_or_exotic_pair") * col("spread_ratio_60")
    definitions["risk_on_long_pressure"] = col("regime_risk_on_score") * col("base_is_risk")
    definitions["risk_off_jpy_chf_pressure"] = col("regime_is_risk_off") * (
        col("quote_is_jpy") + col("quote_is_chf") + col("base_is_jpy") + col("base_is_chf")
    )
    definitions["carry_extreme_pressure"] = col("carry_proxy_abs_diff") * col("is_volatile_or_exotic_pair")
    definitions["macro_event_spread_pressure"] = col("macro_event_risk") * col("spread_ratio_60")

    # Interpretable rule-baseline families.  These are deliberately simple,
    # causal row-time signals that can be compared against ML candidates and
    # reused as features.  They are not live execution permissions.
    liquidity_ok = (
        (col("spread_ratio_60") <= 1.65)
        & (definitions["atr_spread_efficiency"] >= 2.0)
        & (col("is_rollover_hour") < 0.5)
    ).astype(int)
    anti_chase = (
        (col("spread_ratio_60") > 2.0)
        | (col("is_rollover_hour") > 0.5)
        | (col("regime_is_spread_impaired") > 0.5)
        | (col("momentum_60_atr").abs() > 3.25)
    ).astype(int)
    compression_ready = (
        (col("compression_30") <= 0.55)
        & (col("realized_vol_ratio_30_240") <= 1.15)
        & (col("spread_ratio_60") <= 1.5)
    ).astype(int)
    vol_expanding = (
        (col("realized_vol_ratio_30_240") >= 1.05)
        | (col("regime_volatility_expansion_score") >= 0.5)
        | (col("regime_is_high_vol_event") > 0.5)
    ).astype(int)
    active_session = (
        (col("is_london_session") > 0.5)
        | (col("is_new_york_session") > 0.5)
        | (col("is_london_ny_overlap") > 0.5)
    ).astype(int)
    near_upper_range = (
        (col("range_position_60_centered") >= 0.25)
        | (col("range_position_240_centered") >= 0.25)
        | (col("donchian_60_position_centered") >= 0.25)
    ).astype(int)
    near_lower_range = (
        (col("range_position_60_centered") <= -0.25)
        | (col("range_position_240_centered") <= -0.25)
        | (col("donchian_60_position_centered") <= -0.25)
    ).astype(int)
    trend_long = (
        (definitions["momentum_alignment_long"] >= 3)
        & (definitions["ema8_gt_ema21"] > 0)
        & ((col("ma_stack_score") > 0) | (definitions["macd_hist_positive"] > 0))
    ).astype(int)
    trend_short = (
        (definitions["momentum_alignment_short"] >= 3)
        & (definitions["ema8_lt_ema21"] > 0)
        & ((col("ma_stack_score") < 0) | (definitions["macd_hist_negative"] > 0))
    ).astype(int)
    cross_long = (
        (col("strength_gap_15") > 0)
        & (col("strength_gap_60") > -0.25)
        & (
            (col("macro_pair_bias") >= 0)
            | (col("regime_risk_on_score") > 0.25)
            | (col("regime_usd_strength_15") > 0)
        )
    ).astype(int)
    cross_short = (
        (col("strength_gap_15") < 0)
        & (col("strength_gap_60") < 0.25)
        & (
            (col("macro_pair_bias") <= 0)
            | (col("regime_is_risk_off") > 0.5)
            | (col("regime_usd_strength_15") < 0)
        )
    ).astype(int)
    exhaustion_long = (
        near_lower_range.astype(bool)
        & (col("momentum_30_atr") < -1.2)
        & (definitions["momentum_5_15_delta_atr"] > 0)
        & (col("rsi14_centered") <= -0.15)
    ).astype(int)
    exhaustion_short = (
        near_upper_range.astype(bool)
        & (col("momentum_30_atr") > 1.2)
        & (definitions["momentum_5_15_delta_atr"] < 0)
        & (col("rsi14_centered") >= 0.15)
    ).astype(int)

    definitions["rule_pre_breakout_long"] = (
        compression_ready.astype(bool)
        & near_upper_range.astype(bool)
        & (col("momentum_5_atr") > -0.20)
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_pre_breakout_short"] = (
        compression_ready.astype(bool)
        & near_lower_range.astype(bool)
        & (col("momentum_5_atr") < 0.20)
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_breakout_continuation_long"] = (
        trend_long.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_long"] > 0)
            | (definitions["donchian240_breakout_long"] > 0)
            | (col("regime_acceleration_score") > 0.3)
        )
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_breakout_continuation_short"] = (
        trend_short.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_short"] > 0)
            | (definitions["donchian240_breakout_short"] > 0)
            | (col("regime_acceleration_score") < -0.3)
        )
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_exhaustion_reversal_long"] = exhaustion_long
    definitions["rule_exhaustion_reversal_short"] = exhaustion_short
    definitions["rule_session_timing_long"] = (
        active_session.astype(bool)
        & (definitions["momentum_alignment_long"] >= 2)
        & (col("spread_ratio_60") <= 1.8)
    ).astype(int)
    definitions["rule_session_timing_short"] = (
        active_session.astype(bool)
        & (definitions["momentum_alignment_short"] >= 2)
        & (col("spread_ratio_60") <= 1.8)
    ).astype(int)
    definitions["rule_cross_pressure_long"] = cross_long
    definitions["rule_cross_pressure_short"] = cross_short
    definitions["rule_liquidity_ok"] = liquidity_ok
    definitions["rule_anti_chase_penalty"] = anti_chase
    value_liquidity_ok = (
        (definitions["atr_spread_efficiency"] >= 1.35)
        & (col("spread_ratio_60") <= 2.25)
        & (col("is_rollover_hour") < 0.5)
        & (col("spread_pips").abs() <= (col("atr240_pips").abs().clip(lower=0.1) * 0.90))
    ).astype(int)
    scout_session_ok = (
        active_session.astype(bool)
        | (col("is_asia_session") > 0.5)
        | (col("is_rollover_hour") < 0.5)
    ).astype(int)
    late_momentum_long = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & trend_long.astype(bool)
        & (col("momentum_5_atr") >= 0.30)
        & (col("momentum_15_atr") >= 0.45)
        & (col("momentum_60_atr").abs() <= 4.25)
    ).astype(int)
    late_momentum_short = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & trend_short.astype(bool)
        & (col("momentum_5_atr") <= -0.30)
        & (col("momentum_15_atr") <= -0.45)
        & (col("momentum_60_atr").abs() <= 4.25)
    ).astype(int)
    value_breakout_long = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_long"] > 0)
            | (definitions["donchian240_breakout_long"] > 0)
            | (col("regime_acceleration_score") > 0.35)
            | ((near_upper_range > 0) & (definitions["momentum_alignment_long"] >= 3))
        )
        & (col("momentum_5_atr") > -0.10)
    ).astype(int)
    value_breakout_short = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_short"] > 0)
            | (definitions["donchian240_breakout_short"] > 0)
            | (col("regime_acceleration_score") < -0.35)
            | ((near_lower_range > 0) & (definitions["momentum_alignment_short"] >= 3))
        )
        & (col("momentum_5_atr") < 0.10)
    ).astype(int)
    guarded_exhaustion_long = (
        exhaustion_long.astype(bool)
        & value_liquidity_ok.astype(bool)
        & (col("momentum_60_atr") > -4.25)
        & (col("spread_ratio_60") <= 2.00)
    ).astype(int)
    guarded_exhaustion_short = (
        exhaustion_short.astype(bool)
        & value_liquidity_ok.astype(bool)
        & (col("momentum_60_atr") < 4.25)
        & (col("spread_ratio_60") <= 2.00)
    ).astype(int)
    definitions["rule_value_liquidity_ok"] = value_liquidity_ok
    definitions["rule_late_momentum_confirmation_long"] = late_momentum_long
    definitions["rule_late_momentum_confirmation_short"] = late_momentum_short
    definitions["rule_value_breakout_long"] = value_breakout_long
    definitions["rule_value_breakout_short"] = value_breakout_short
    definitions["rule_guarded_exhaustion_reversal_long"] = guarded_exhaustion_long
    definitions["rule_guarded_exhaustion_reversal_short"] = guarded_exhaustion_short
    definitions["rule_scout_value_long_score"] = (
        value_breakout_long * 1.35
        + late_momentum_long * 1.15
        + guarded_exhaustion_long * 0.75
        + cross_long * 0.45
        + value_liquidity_ok * 0.40
        - anti_chase * 1.10
    )
    definitions["rule_scout_value_short_score"] = (
        value_breakout_short * 1.35
        + late_momentum_short * 1.15
        + guarded_exhaustion_short * 0.75
        + cross_short * 0.45
        + value_liquidity_ok * 0.40
        - anti_chase * 1.10
    )
    definitions["rule_baseline_long_score"] = (
        definitions["rule_pre_breakout_long"] * 1.15
        + definitions["rule_breakout_continuation_long"] * 1.35
        + definitions["rule_exhaustion_reversal_long"] * 0.85
        + definitions["rule_session_timing_long"] * 0.55
        + definitions["rule_cross_pressure_long"] * 0.90
        + definitions["rule_liquidity_ok"] * 0.35
        + definitions["rule_scout_value_long_score"] * 0.20
        - definitions["rule_anti_chase_penalty"] * 1.40
    )
    definitions["rule_baseline_short_score"] = (
        definitions["rule_pre_breakout_short"] * 1.15
        + definitions["rule_breakout_continuation_short"] * 1.35
        + definitions["rule_exhaustion_reversal_short"] * 0.85
        + definitions["rule_session_timing_short"] * 0.55
        + definitions["rule_cross_pressure_short"] * 0.90
        + definitions["rule_liquidity_ok"] * 0.35
        + definitions["rule_scout_value_short_score"] * 0.20
        - definitions["rule_anti_chase_penalty"] * 1.40
    )
    definitions["rule_baseline_direction_score"] = (
        definitions["rule_baseline_long_score"] - definitions["rule_baseline_short_score"]
    )
    definitions["rule_baseline_strength_score"] = np.maximum(
        definitions["rule_baseline_long_score"],
        definitions["rule_baseline_short_score"],
    )
    definitions["rule_baseline_long_candidate"] = (
        (definitions["rule_baseline_long_score"] >= 2.0)
        & (definitions["rule_baseline_long_score"] > definitions["rule_baseline_short_score"])
    ).astype(int)
    definitions["rule_baseline_short_candidate"] = (
        (definitions["rule_baseline_short_score"] >= 2.0)
        & (definitions["rule_baseline_short_score"] > definitions["rule_baseline_long_score"])
    ).astype(int)

    for name, values in definitions.items():
        out[name] = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out, list(definitions.keys())


def required_columns(features: Sequence[str], horizons: Sequence[int], leads: Sequence[int]) -> List[str]:
    exec_horizons = sorted({execution_horizon(lead, horizon) for lead in leads for horizon in horizons})
    columns = {
        "time_utc",
        "instrument",
        "pair_taxonomy_primary",
        "regime_primary",
        "atr240_pips",
        *EXPANDED_PRE_SPIKE_SOURCE_COLUMNS,
        *features,
    }
    for horizon in horizons:
        columns.add(f"major_event_{horizon}")
        columns.add(f"major_direction_up_{horizon}")
    for horizon in exec_horizons:
        columns.add(f"long_trailing_net_pips_{horizon}")
        columns.add(f"short_trailing_net_pips_{horizon}")
        columns.add(f"long_trailing_net_atr_{horizon}")
        columns.add(f"short_trailing_net_atr_{horizon}")
    return list(columns)


def apply_lead_stop_cap(outcome: np.ndarray, atr_pips: np.ndarray, stop_atr: float) -> np.ndarray:
    values = np.asarray(outcome, dtype=float)
    if stop_atr <= 0:
        return values
    stop = np.asarray(atr_pips, dtype=float)
    stop = np.where(np.isfinite(stop), np.maximum(stop, 0.1) * float(stop_atr), np.nan)
    return np.maximum(values, -stop)


def apply_subset(frame: pd.DataFrame, subset: str) -> pd.DataFrame:
    subset = str(subset or "volatile_exotic")
    try:
        return manager.apply_instrument_subset(frame, subset).copy()
    except Exception:
        return frame.copy()


def attach_next_event_labels(
    frame: pd.DataFrame,
    *,
    lead_minutes: int,
    move_horizon: int,
) -> pd.DataFrame:
    event_col = f"major_event_{move_horizon}"
    direction_col = f"major_direction_up_{move_horizon}"
    temp_columns = ["_row_index", "_event_time", "_event_direction_up"]
    working = frame.drop(
        columns=[column for column in temp_columns if column in frame.columns],
        errors="ignore",
    )
    pieces: List[pd.DataFrame] = []
    lead_event_col = f"lead_major_event_{lead_minutes}_{move_horizon}"
    lead_direction_col = f"lead_direction_up_{lead_minutes}_{move_horizon}"
    lead_minutes_col = f"lead_minutes_to_event_{lead_minutes}_{move_horizon}"
    for _, group in working.sort_values("time_utc").groupby(
        "instrument",
        sort=False,
        observed=False,
    ):
        rows = group.reset_index(drop=False).rename(columns={"index": "_row_index"})
        events = rows[pd.to_numeric(rows[event_col], errors="coerce").fillna(0).astype(int).eq(1)][
            ["time_utc", direction_col]
        ].rename(columns={"time_utc": "_event_time", direction_col: "_event_direction_up"})
        if events.empty:
            rows["_event_time"] = pd.NaT
            rows["_event_direction_up"] = np.nan
        else:
            merged = pd.merge_asof(
                rows.sort_values("time_utc"),
                events.sort_values("_event_time"),
                left_on="time_utc",
                right_on="_event_time",
                direction="forward",
                allow_exact_matches=False,
            )
            rows = merged
        rows["time_utc"] = pd.to_datetime(rows["time_utc"], errors="coerce", utc=True)
        rows["_event_time"] = pd.to_datetime(rows["_event_time"], errors="coerce", utc=True)
        within = (
            rows["_event_time"].notna()
            & (rows["_event_time"] <= rows["time_utc"] + pd.to_timedelta(lead_minutes, unit="m"))
        )
        rows[lead_event_col] = within.astype(int)
        rows[lead_direction_col] = np.where(
            within,
            pd.to_numeric(rows["_event_direction_up"], errors="coerce"),
            np.nan,
        )
        rows[lead_minutes_col] = np.where(
            within,
            (rows["_event_time"] - rows["time_utc"]).dt.total_seconds() / 60.0,
            np.nan,
        )
        pieces.append(
            rows.set_index("_row_index").drop(
                columns=[column for column in temp_columns if column != "_row_index"],
                errors="ignore",
            )
        )
    if not pieces:
        return working.copy()
    # Pandas warns that concatenating pieces with empty/all-NA columns will
    # change dtype inference in a future release.  The temporary event columns
    # are already dropped above; dropping all-NA columns from each piece before
    # concat preserves the current useful output while keeping future runs'
    # stderr reserved for real failures.
    cleaned_pieces = [piece.dropna(axis=1, how="all") for piece in pieces if not piece.empty]
    combined = pd.concat(cleaned_pieces, sort=False).sort_index() if cleaned_pieces else working.copy()
    for column, default in [
        (lead_event_col, 0),
        (lead_direction_col, np.nan),
        (lead_minutes_col, np.nan),
    ]:
        if column not in combined.columns:
            combined[column] = default
    for column in working.columns:
        if column not in combined.columns:
            combined[column] = np.nan
    return combined


def model_for_name(name: str, seed: int) -> Any:
    if name == "logistic":
        return LogisticRegression(
            C=0.75,
            class_weight="balanced",
            max_iter=500,
            random_state=seed,
        )
    if name == "extra_trees":
        return ExtraTreesClassifier(
            n_estimators=220,
            max_depth=10,
            min_samples_leaf=20,
            max_features="sqrt",
            class_weight="balanced",
            random_state=seed,
            n_jobs=-1,
        )
    if name == "random_forest":
        return RandomForestClassifier(
            n_estimators=240,
            max_depth=9,
            min_samples_leaf=20,
            max_features="sqrt",
            class_weight="balanced_subsample",
            random_state=seed,
            n_jobs=-1,
        )
    if name == "gradient_boosting":
        return GradientBoostingClassifier(
            n_estimators=140,
            learning_rate=0.04,
            max_depth=3,
            min_samples_leaf=35,
            subsample=0.75,
            random_state=seed,
        )
    return HistGradientBoostingClassifier(
        max_iter=140,
        learning_rate=0.05,
        max_leaf_nodes=31,
        min_samples_leaf=40,
        l2_regularization=0.02,
        random_state=seed,
    )


def rule_profile_scores(
    frame: pd.DataFrame,
    rule_profile: str,
) -> tuple[np.ndarray, np.ndarray]:
    profile = RULE_PROFILE_SPECS.get(rule_profile) or RULE_PROFILE_SPECS["baseline"]

    def weighted_score(weights: Dict[str, float]) -> np.ndarray:
        score = np.zeros(len(frame), dtype=float)
        for column, weight in weights.items():
            if column not in frame:
                values = np.zeros(len(frame), dtype=float)
            else:
                values = pd.to_numeric(
                    frame[column],
                    errors="coerce",
                ).fillna(0.0).to_numpy(dtype=float)
            score += values * float(weight)
        return score

    return weighted_score(profile["long"]), weighted_score(profile["short"])


def _profile_context_filter(rule_profile: str) -> Dict[str, Any]:
    profile = RULE_PROFILE_SPECS.get(rule_profile) or {}
    filter_spec = profile.get("context_filter")
    return dict(filter_spec) if isinstance(filter_spec, dict) else {}


def _instrument_direction_keys(frame: pd.DataFrame, positions: np.ndarray, is_long: np.ndarray) -> List[str]:
    if len(positions) == 0:
        return []
    selected = frame.iloc[positions]
    directions = np.where(is_long[positions], "LONG", "SHORT")
    return [
        f"{instrument}|{direction}"
        for instrument, direction in zip(selected["instrument"].astype(str).tolist(), directions)
    ]


def build_context_allowlist(
    frame: pd.DataFrame,
    positions: np.ndarray,
    is_long: np.ndarray,
    outcome: np.ndarray,
    filter_spec: Dict[str, Any],
) -> Dict[str, Any]:
    """Build a fold-safe pair/direction allowlist from calibration selections.

    This deliberately uses only the calibration window for the current fold.
    The resulting allowlist is then applied to the following test week, so the
    context filter can improve persistence without peeking at test outcomes.
    """
    if not filter_spec:
        return {"enabled": False, "allowed_groups": [], "group_stats": []}
    if str(filter_spec.get("group") or "instrument_direction") != "instrument_direction":
        return {"enabled": True, "allowed_groups": [], "group_stats": [], "unsupported_group": filter_spec.get("group")}
    if len(positions) == 0:
        return {"enabled": True, "allowed_groups": [], "group_stats": []}

    selected = frame.iloc[positions][["instrument", "regime_primary"]].copy()
    selected["_direction"] = np.where(is_long[positions], "LONG", "SHORT")
    selected["_outcome_pips"] = outcome[positions]
    exclude_regimes = {
        str(value)
        for value in filter_spec.get("exclude_regimes", [])
        if str(value)
    }
    if exclude_regimes:
        selected = selected[~selected["regime_primary"].astype(str).isin(exclude_regimes)]
    if selected.empty:
        return {"enabled": True, "allowed_groups": [], "group_stats": []}

    selected["_group"] = selected["instrument"].astype(str) + "|" + selected["_direction"].astype(str)
    min_count = safe_int(filter_spec.get("min_count"), 1)
    min_mean = safe_float(filter_spec.get("min_mean_pips"), 0.0)
    min_pf = safe_float(filter_spec.get("min_profit_factor"), 1.0)
    min_pos = safe_float(filter_spec.get("min_positive_rate"), 0.0)
    top_n = safe_int(filter_spec.get("top_n_by_sum"), 0)

    stats_rows: List[Dict[str, Any]] = []
    for group, rows in selected.groupby("_group", dropna=False):
        values = pd.to_numeric(rows["_outcome_pips"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=float)
        if len(values) < min_count:
            continue
        stats = summarize(values)
        if safe_float(stats.get("mean")) < min_mean:
            continue
        if safe_float(stats.get("profit_factor")) < min_pf:
            continue
        if safe_float(stats.get("positive_rate")) < min_pos:
            continue
        stats_rows.append({
            "group": str(group),
            "count": int(len(values)),
            "sum": safe_float(stats.get("sum")),
            "mean": safe_float(stats.get("mean")),
            "median": safe_float(stats.get("median")),
            "positive_rate": safe_float(stats.get("positive_rate")),
            "profit_factor": safe_float(stats.get("profit_factor")),
        })
    stats_rows = sorted(stats_rows, key=lambda row: row["sum"], reverse=True)
    if top_n > 0:
        stats_rows = stats_rows[:top_n]
    return {
        "enabled": True,
        "allowed_groups": [str(row["group"]) for row in stats_rows],
        "group_stats": stats_rows,
        "filter_spec": filter_spec,
    }


def apply_context_allowlist(
    frame: pd.DataFrame,
    positions: np.ndarray,
    is_long: np.ndarray,
    filter_spec: Dict[str, Any],
    allowlist: Dict[str, Any],
) -> tuple[np.ndarray, Dict[str, Any]]:
    if not filter_spec:
        return positions, {"enabled": False, "input_count": int(len(positions)), "output_count": int(len(positions))}
    if len(positions) == 0:
        return positions, {"enabled": True, "input_count": 0, "output_count": 0, "allowed_count": 0}
    allowed_groups = set(str(value) for value in allowlist.get("allowed_groups", []))
    if not allowed_groups and bool(filter_spec.get("fallback_to_unfiltered_when_empty", False)):
        return positions, {
            "enabled": True,
            "input_count": int(len(positions)),
            "output_count": int(len(positions)),
            "allowed_count": 0,
            "fallback": "unfiltered_when_empty",
        }
    if not allowed_groups:
        return np.asarray([], dtype=int), {
            "enabled": True,
            "input_count": int(len(positions)),
            "output_count": 0,
            "allowed_count": 0,
            "fallback": "none",
        }

    keys = _instrument_direction_keys(frame, positions, is_long)
    selected = frame.iloc[positions]
    exclude_regimes = {
        str(value)
        for value in filter_spec.get("exclude_regimes", [])
        if str(value)
    }
    keep_mask: List[bool] = []
    for key, (_, row) in zip(keys, selected.iterrows()):
        keep = key in allowed_groups
        if keep and exclude_regimes and str(row.get("regime_primary") or "") in exclude_regimes:
            keep = False
        keep_mask.append(bool(keep))
    filtered = positions[np.asarray(keep_mask, dtype=bool)]
    return filtered, {
        "enabled": True,
        "input_count": int(len(positions)),
        "output_count": int(len(filtered)),
        "allowed_count": int(len(allowed_groups)),
        "allowed_groups": sorted(allowed_groups)[:25],
    }


def sample_training(frame: pd.DataFrame, target: str, *, max_rows: int, negative_ratio: int, seed: int) -> pd.DataFrame:
    y = pd.to_numeric(frame[target], errors="coerce").fillna(0).astype(int)
    positives = frame[y.eq(1)]
    negatives = frame[y.eq(0)]
    if positives.empty or negatives.empty:
        return frame.iloc[0:0]
    max_pos = min(len(positives), max(2000, max_rows // max(negative_ratio + 1, 2)))
    pos = positives.sample(n=max_pos, random_state=seed) if len(positives) > max_pos else positives
    max_neg = min(len(negatives), max_rows - len(pos), len(pos) * negative_ratio)
    neg = negatives.sample(n=max_neg, random_state=seed + 17) if len(negatives) > max_neg else negatives
    return pd.concat([pos, neg], ignore_index=True).sample(frac=1.0, random_state=seed + 31)


def episode_positions(test: pd.DataFrame, probability: np.ndarray, threshold: float, cooldown_minutes: int) -> np.ndarray:
    selected = test.assign(_probability=probability)
    selected = selected[selected["_probability"] >= threshold].sort_values(
        ["time_utc", "_probability"],
        ascending=[True, False],
    )
    positions: List[int] = []
    last_by_instrument: Dict[str, pd.Timestamp] = {}
    for idx, row in selected.iterrows():
        instrument = str(row.get("instrument") or "")
        timestamp = pd.Timestamp(row["time_utc"])
        last = last_by_instrument.get(instrument)
        if last is not None and timestamp < last + pd.Timedelta(minutes=cooldown_minutes):
            continue
        positions.append(int(idx))
        last_by_instrument[instrument] = timestamp
    return np.asarray(positions, dtype=int)


def episode_candidate_frame(candidates: pd.DataFrame, cooldown_minutes: int) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    selected_rows: List[pd.Series] = []
    last_by_instrument: Dict[str, pd.Timestamp] = {}
    ordered = candidates.sort_values(
        ["time_utc", "probability"],
        ascending=[True, False],
    )
    for _, row in ordered.iterrows():
        instrument = str(row.get("instrument") or "")
        timestamp = pd.Timestamp(row["time_utc"])
        last = last_by_instrument.get(instrument)
        if last is not None and timestamp < last + pd.Timedelta(minutes=cooldown_minutes):
            continue
        selected_rows.append(row)
        last_by_instrument[instrument] = timestamp
    if not selected_rows:
        return candidates.iloc[0:0].copy()
    return pd.DataFrame(selected_rows).reset_index(drop=True)


def _record_time(record: Dict[str, Any]) -> pd.Timestamp:
    return pd.Timestamp(record.get("time_utc"))


def enrich_selected_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for raw in records:
        row = dict(raw)
        pips = safe_float(row.get("outcome_pips"), 0.0)
        atr = max(abs(safe_float(row.get("atr240_pips"), 0.0)), 1e-9)
        spread = max(abs(safe_float(row.get("spread_pips"), 0.0)), 1e-9)
        row["outcome_atr"] = pips / atr
        row["outcome_spread_units"] = pips / spread
        row["abs_outcome_pips"] = abs(pips)
        enriched.append(row)
    return enriched


def cluster_selected_records(records: List[Dict[str, Any]], gap_minutes: int) -> List[List[Dict[str, Any]]]:
    if not records:
        return []
    ordered = sorted(
        records,
        key=lambda row: (
            str(row.get("instrument") or ""),
            str(row.get("predicted_direction") or ""),
            _record_time(row),
        ),
    )
    clusters: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    last_key: tuple[str, str] | None = None
    last_time: pd.Timestamp | None = None
    gap = pd.Timedelta(minutes=max(1, int(gap_minutes)))
    for row in ordered:
        key = (str(row.get("instrument") or ""), str(row.get("predicted_direction") or ""))
        timestamp = _record_time(row)
        if (
            current
            and last_key == key
            and last_time is not None
            and timestamp <= last_time + gap
        ):
            current.append(row)
        else:
            if current:
                clusters.append(current)
            current = [row]
        last_key = key
        last_time = timestamp
    if current:
        clusters.append(current)
    return clusters


def concentration_summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {"count": 0}
    by_instrument: Dict[str, int] = {}
    by_taxonomy: Dict[str, int] = {}
    for row in records:
        inst = str(row.get("instrument") or "")
        tax = str(row.get("pair_taxonomy_primary") or "")
        by_instrument[inst] = by_instrument.get(inst, 0) + 1
        by_taxonomy[tax] = by_taxonomy.get(tax, 0) + 1
    top_inst = max(by_instrument.values()) if by_instrument else 0
    return {
        "count": len(records),
        "instrument_count": len(by_instrument),
        "top_instrument_share": float(top_inst / max(len(records), 1)),
        "top_instruments": dict(sorted(by_instrument.items(), key=lambda item: item[1], reverse=True)[:10]),
        "taxonomy_counts": dict(sorted(by_taxonomy.items(), key=lambda item: item[1], reverse=True)[:10]),
    }


def _group_value(row: Dict[str, Any], key: str) -> str:
    if key == "hour_utc":
        try:
            return f"{pd.Timestamp(row.get('time_utc')).hour:02d}"
        except Exception:
            return ""
    return str(row.get(key) or "")


def grouped_outcome_summary(
    records: List[Dict[str, Any]],
    group_keys: Sequence[str],
    *,
    min_count: int = 10,
    top: int = 10,
) -> Dict[str, Any]:
    """Compact group diagnostics for finding where selected-rule edge lives.

    The full selected-row table can be very large, so reports keep only the
    best/worst grouped summaries.  This is enough to spot whether apparent edge
    is concentrated in a single pair, session hour, taxonomy, or regime before
    considering a live/challenger promotion.
    """
    grouped: Dict[str, List[float]] = {}
    for row in records:
        key = "|".join(_group_value(row, part) for part in group_keys)
        if not key.strip("|"):
            key = "unknown"
        grouped.setdefault(key, []).append(safe_float(row.get("outcome_pips"), 0.0))

    rows: List[Dict[str, Any]] = []
    for key, values in grouped.items():
        if len(values) < min_count:
            continue
        stats = summarize(values)
        rows.append({
            "key": key,
            "count": int(len(values)),
            "sum": safe_float(stats.get("sum")),
            "mean": safe_float(stats.get("mean")),
            "median": safe_float(stats.get("median")),
            "positive_rate": safe_float(stats.get("positive_rate")),
            "profit_factor": safe_float(stats.get("profit_factor")),
        })
    return {
        "group_keys": list(group_keys),
        "min_count": int(min_count),
        "group_count": len(rows),
        "top_sum": sorted(rows, key=lambda row: row["sum"], reverse=True)[:top],
        "bottom_sum": sorted(rows, key=lambda row: row["sum"])[:top],
        "top_mean": sorted(rows, key=lambda row: row["mean"], reverse=True)[:top],
    }


def selected_risk_summary(records: List[Dict[str, Any]], *, cluster_gap_minutes: int) -> Dict[str, Any]:
    enriched = enrich_selected_records(records)
    clusters = cluster_selected_records(enriched, cluster_gap_minutes)
    first_reps = [cluster[0] for cluster in clusters]
    best_reps = [
        max(cluster, key=lambda row: safe_float(row.get("outcome_pips"), -1e9))
        for cluster in clusters
    ]

    def bundle(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "pips": summarize([safe_float(row.get("outcome_pips")) for row in rows]),
            "atr": summarize([safe_float(row.get("outcome_atr")) for row in rows]),
            "spread_units": summarize([safe_float(row.get("outcome_spread_units")) for row in rows]),
            "concentration": concentration_summary(rows),
        }

    return {
        "raw": bundle(enriched),
        "cluster_gap_minutes": int(cluster_gap_minutes),
        "cluster_count": len(clusters),
        "clustered_first": bundle(first_reps),
        "clustered_best": bundle(best_reps),
        "clustered_first_groups": {
            "instrument": grouped_outcome_summary(first_reps, ["instrument"], min_count=8),
            "instrument_direction": grouped_outcome_summary(first_reps, ["instrument", "predicted_direction"], min_count=8),
            "taxonomy": grouped_outcome_summary(first_reps, ["pair_taxonomy_primary"], min_count=8),
            "regime": grouped_outcome_summary(first_reps, ["regime_primary"], min_count=8),
            "hour_utc": grouped_outcome_summary(first_reps, ["hour_utc"], min_count=8),
        },
    }


def choose_threshold(
    calibration: pd.DataFrame,
    probability: np.ndarray,
    outcome: np.ndarray,
    *,
    cooldown_minutes: int,
) -> tuple[float, Dict[str, Any]]:
    candidates = sorted({
        0.50,
        0.60,
        0.70,
        0.80,
        0.90,
        *[float(np.quantile(probability, q)) for q in [0.90, 0.95, 0.975, 0.99] if len(probability)],
    })
    rows = []
    for threshold in candidates:
        positions = episode_positions(calibration, probability, threshold, cooldown_minutes)
        stats = summarize(outcome[positions])
        rows.append({"threshold": float(threshold), **stats})
    ranked = pd.DataFrame(rows)
    eligible = ranked[pd.to_numeric(ranked["count"], errors="coerce").fillna(0).ge(3)]
    if eligible.empty:
        eligible = ranked
    best = eligible.sort_values(
        ["mean", "profit_factor", "count"],
        ascending=[False, False, False],
    ).iloc[0]
    return float(best["threshold"]), best.to_dict()


def safe_metric(fn: Any, y_true: np.ndarray, score: np.ndarray) -> float:
    try:
        if len(np.unique(y_true)) < 2:
            return 0.0
        return float(fn(y_true, score))
    except Exception:
        return 0.0


def evaluate_combo(
    frame: pd.DataFrame,
    *,
    features: List[str],
    lead_minutes: int,
    move_horizon: int,
    model_name: str,
    max_train_rows: int,
    negative_ratio: int,
    max_test_weeks: int,
    lead_stop_atr: float,
    cluster_gap_minutes: int,
) -> Dict[str, Any]:
    exec_horizon = execution_horizon(lead_minutes, move_horizon)
    target = f"lead_major_event_{lead_minutes}_{move_horizon}"
    direction_target = f"lead_direction_up_{lead_minutes}_{move_horizon}"
    if target not in frame or direction_target not in frame:
        frame = attach_next_event_labels(
            frame,
            lead_minutes=lead_minutes,
            move_horizon=move_horizon,
        )
    for col in [
        target,
        direction_target,
        f"long_trailing_net_pips_{exec_horizon}",
        f"short_trailing_net_pips_{exec_horizon}",
        *features,
    ]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(
        subset=["time_utc", target, f"long_trailing_net_pips_{exec_horizon}", f"short_trailing_net_pips_{exec_horizon}", *features]
    ).sort_values("time_utc").reset_index(drop=True)
    frame["week_start"] = (
        frame["time_utc"].dt.normalize()
        - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
    )
    weeks = sorted(frame["week_start"].dropna().unique())
    if max_test_weeks > 0:
        weeks = weeks[-max_test_weeks:]
    folds: List[Dict[str, Any]] = []
    selected_values_all: List[float] = []
    selected_rows_all: List[Dict[str, Any]] = []
    for fold_number, test_week in enumerate(weeks, 1):
        test_week = pd.Timestamp(test_week)
        calibration_start = test_week - pd.Timedelta(days=7)
        train_end = calibration_start
        train = frame[frame["time_utc"] < train_end]
        calibration = frame[(frame["time_utc"] >= calibration_start) & (frame["time_utc"] < test_week)]
        test = frame[frame["week_start"].eq(test_week)].reset_index(drop=True)
        if len(train) < 5000 or len(calibration) < 250 or len(test) < 250:
            continue
        sampled = sample_training(
            train,
            target,
            max_rows=max_train_rows,
            negative_ratio=negative_ratio,
            seed=fold_number * 1009 + lead_minutes + move_horizon,
        )
        if len(sampled) < 1000 or sampled[target].nunique() < 2:
            continue
        y_train = sampled[target].astype(int)
        y_cal = calibration[target].astype(int).to_numpy()
        y_test = test[target].astype(int).to_numpy()
        if len(np.unique(y_test)) < 2:
            continue
        event_model = model_for_name(model_name, seed=fold_number * 7919)
        event_model.fit(sampled[features], y_train)
        cal_prob = event_model.predict_proba(calibration[features])[:, 1]
        test_prob = event_model.predict_proba(test[features])[:, 1]

        positive_train = train[
            train[target].astype(int).eq(1)
            & pd.to_numeric(train[direction_target], errors="coerce").notna()
        ]
        if len(positive_train) < 200 or positive_train[direction_target].nunique() < 2:
            continue
        if len(positive_train) > max_train_rows // 2:
            positive_train = positive_train.sample(n=max_train_rows // 2, random_state=fold_number * 1231)
        direction_model = model_for_name(model_name, seed=fold_number * 3571)
        direction_model.fit(
            positive_train[features],
            positive_train[direction_target].astype(int),
        )
        cal_dir = direction_model.predict_proba(calibration[features])[:, 1]
        test_dir = direction_model.predict_proba(test[features])[:, 1]
        cal_outcome = np.where(
            cal_dir >= 0.5,
            calibration[f"long_trailing_net_pips_{exec_horizon}"].to_numpy(dtype=float),
            calibration[f"short_trailing_net_pips_{exec_horizon}"].to_numpy(dtype=float),
        )
        cal_outcome = apply_lead_stop_cap(
            cal_outcome,
            calibration["atr240_pips"].to_numpy(dtype=float),
            lead_stop_atr,
        )
        test_outcome = np.where(
            test_dir >= 0.5,
            test[f"long_trailing_net_pips_{exec_horizon}"].to_numpy(dtype=float),
            test[f"short_trailing_net_pips_{exec_horizon}"].to_numpy(dtype=float),
        )
        test_outcome = apply_lead_stop_cap(
            test_outcome,
            test["atr240_pips"].to_numpy(dtype=float),
            lead_stop_atr,
        )
        threshold, calibration_summary = choose_threshold(
            calibration.reset_index(drop=True),
            cal_prob,
            cal_outcome,
            cooldown_minutes=max(lead_minutes, 15),
        )
        positions = episode_positions(
            test.reset_index(drop=True),
            test_prob,
            threshold,
            cooldown_minutes=max(lead_minutes, 15),
        )
        selected_outcome = test_outcome[positions]
        selected_targets = y_test[positions] if len(positions) else np.asarray([], dtype=int)
        selected_values_all.extend([float(v) for v in selected_outcome if math.isfinite(float(v))])
        if len(positions):
            selected_rows = test.iloc[positions][[
                "time_utc",
                "instrument",
                "pair_taxonomy_primary",
                "regime_primary",
                "atr240_pips",
                "spread_pips",
                target,
            ]].copy()
            selected_rows["fold"] = fold_number
            selected_rows["week_start"] = test_week.isoformat()
            selected_rows["outcome_pips"] = selected_outcome
            selected_rows["probability"] = test_prob[positions]
            selected_rows["predicted_direction"] = np.where(test_dir[positions] >= 0.5, "LONG", "SHORT")
            selected_rows["directional_target"] = selected_targets
            selected_rows_all.extend(selected_rows.to_dict("records"))
        event_auc = safe_metric(roc_auc_score, y_test, test_prob)
        event_ap = safe_metric(average_precision_score, y_test, test_prob)
        base_rate = float(np.mean(y_test)) if len(y_test) else 0.0
        direction_mask = y_test.astype(bool) & pd.to_numeric(test[direction_target], errors="coerce").notna().to_numpy()
        direction_accuracy = (
            float(((test_dir[direction_mask] >= 0.5).astype(int) == test.loc[direction_mask, direction_target].astype(int).to_numpy()).mean())
            if direction_mask.any()
            else 0.0
        )
        folds.append({
            "model": model_name,
            "lead_minutes": lead_minutes,
            "move_horizon": move_horizon,
            "execution_horizon": exec_horizon,
            "fold": fold_number,
            "week_start": test_week.isoformat(),
            "train_rows": int(len(train)),
            "sampled_train_rows": int(len(sampled)),
            "calibration_rows": int(len(calibration)),
            "test_rows": int(len(test)),
            "event_base_rate": base_rate,
            "event_auc": event_auc,
            "event_average_precision": event_ap,
            "event_ap_lift": event_ap / max(base_rate, 1e-9),
            "direction_accuracy_on_events": direction_accuracy,
            "selected_threshold": threshold,
            "selected_event_precision": float(selected_targets.mean()) if len(selected_targets) else 0.0,
            "selected_event_recall": float(selected_targets.sum() / max(y_test.sum(), 1)),
            "calibration_selected": calibration_summary,
            **{f"selected_{k}": v for k, v in summarize(selected_outcome).items()},
        })
    selected_summary = summarize(selected_values_all)
    risk_summary = selected_risk_summary(
        selected_rows_all,
        cluster_gap_minutes=cluster_gap_minutes,
    )
    fold_frame = pd.DataFrame(folds)
    if fold_frame.empty:
        return {
            "model": model_name,
            "mode": "two_stage",
            "lead_minutes": lead_minutes,
            "move_horizon": move_horizon,
            "execution_horizon": exec_horizon,
            "fold_count": 0,
            "status": "no_valid_folds",
            "score": -1e9,
            "folds": [],
        }
    positive_weeks = int((pd.to_numeric(fold_frame["selected_mean"], errors="coerce").fillna(0) > 0).sum())
    clustered_first = risk_summary.get("clustered_first") or {}
    clustered_pips = clustered_first.get("pips") if isinstance(clustered_first, dict) else {}
    clustered_spread = clustered_first.get("spread_units") if isinstance(clustered_first, dict) else {}
    score = (
        safe_float(fold_frame["event_auc"].mean()) * 30.0
        + min(12.0, safe_float(fold_frame["event_ap_lift"].mean())) * 4.0
        + safe_float(fold_frame["direction_accuracy_on_events"].mean()) * 20.0
        + np.clip(safe_float(selected_summary.get("mean")), -100.0, 100.0) * 0.35
        + np.clip(safe_float((clustered_pips or {}).get("mean")), -100.0, 100.0) * 0.20
        + np.clip(safe_float((clustered_spread or {}).get("mean")), -25.0, 25.0) * 1.25
        + safe_float((clustered_spread or {}).get("profit_factor")) * 2.0
        + safe_float(selected_summary.get("profit_factor")) * 2.5
        + positive_weeks * 2.0
    )
    return {
        "model": model_name,
        "mode": "two_stage",
        "lead_minutes": lead_minutes,
        "move_horizon": move_horizon,
        "execution_horizon": exec_horizon,
        "fold_count": int(len(fold_frame)),
        "status": "completed",
        "mean_event_auc": safe_float(fold_frame["event_auc"].mean()),
        "min_event_auc": safe_float(fold_frame["event_auc"].min()),
        "mean_event_ap_lift": safe_float(fold_frame["event_ap_lift"].mean()),
        "mean_direction_accuracy_on_events": safe_float(fold_frame["direction_accuracy_on_events"].mean()),
        "selected_summary": selected_summary,
        "selected_risk_summary": risk_summary,
        "positive_weeks": positive_weeks,
        "score": float(score),
        "folds": folds,
        "top_selected_examples": sorted(
            enrich_selected_records(selected_rows_all),
            key=lambda row: safe_float(row.get("outcome_pips")),
            reverse=True,
        )[:25],
    }


def choose_directional_threshold(
    calibration: pd.DataFrame,
    probability: np.ndarray,
    outcome: np.ndarray,
    *,
    cooldown_minutes: int,
) -> tuple[float, Dict[str, Any]]:
    if len(probability) == 0:
        return 1.0, {"count": 0}
    candidates = sorted({
        0.90,
        0.95,
        0.98,
        0.99,
        *[
            float(np.quantile(probability, q))
            for q in [0.975, 0.99, 0.995, 0.999, 0.9995]
        ],
    })
    rows = []
    for threshold in candidates:
        positions = episode_positions(
            calibration.reset_index(drop=True),
            probability,
            threshold,
            cooldown_minutes,
        )
        stats = summarize(outcome[positions])
        rows.append({"threshold": float(threshold), **stats})
    ranked = pd.DataFrame(rows)
    eligible = ranked[pd.to_numeric(ranked["count"], errors="coerce").fillna(0).ge(1)]
    if eligible.empty:
        eligible = ranked
    best = eligible.sort_values(
        ["mean", "profit_factor", "count"],
        ascending=[False, False, False],
    ).iloc[0]
    return float(best["threshold"]), best.to_dict()


def choose_score_threshold(
    calibration: pd.DataFrame,
    score: np.ndarray,
    outcome: np.ndarray,
    *,
    cooldown_minutes: int,
) -> tuple[float, Dict[str, Any]]:
    finite = np.asarray(score, dtype=float)
    finite = finite[np.isfinite(finite)]
    positive = finite[finite > 0]
    if len(positive) == 0:
        return float("inf"), {"count": 0}
    candidates = sorted({
        1.0,
        1.5,
        2.0,
        2.5,
        3.0,
        *[float(np.quantile(positive, q)) for q in [0.50, 0.70, 0.80, 0.90, 0.95, 0.98]],
    })
    rows = []
    for threshold in candidates:
        positions = episode_positions(
            calibration.reset_index(drop=True),
            score,
            threshold,
            cooldown_minutes,
        )
        stats = summarize(outcome[positions])
        rows.append({"threshold": float(threshold), **stats})
    ranked = pd.DataFrame(rows)
    eligible = ranked[pd.to_numeric(ranked["count"], errors="coerce").fillna(0).ge(3)]
    if eligible.empty:
        eligible = ranked
    best = eligible.sort_values(
        ["mean", "profit_factor", "count"],
        ascending=[False, False, False],
    ).iloc[0]
    return float(best["threshold"]), best.to_dict()


def evaluate_rule_baseline_combo(
    frame: pd.DataFrame,
    *,
    rule_profile: str,
    lead_minutes: int,
    move_horizon: int,
    max_test_weeks: int,
    lead_stop_atr: float,
    cluster_gap_minutes: int,
) -> Dict[str, Any]:
    exec_horizon = execution_horizon(lead_minutes, move_horizon)
    any_target = f"lead_major_event_{lead_minutes}_{move_horizon}"
    direction_target = f"lead_direction_up_{lead_minutes}_{move_horizon}"
    long_outcome_col = f"long_trailing_net_pips_{exec_horizon}"
    short_outcome_col = f"short_trailing_net_pips_{exec_horizon}"
    if any_target not in frame or direction_target not in frame:
        frame = attach_next_event_labels(
            frame,
            lead_minutes=lead_minutes,
            move_horizon=move_horizon,
        )
    needed_rule_cols = [
        any_target,
        direction_target,
        long_outcome_col,
        short_outcome_col,
        "atr240_pips",
        "spread_pips",
        *RULE_BASELINE_COLUMNS,
    ]
    compact_cols = list(dict.fromkeys([
        "time_utc",
        "instrument",
        "pair_taxonomy_primary",
        "regime_primary",
        *needed_rule_cols,
    ]))
    for meta_col in ("pair_taxonomy_primary", "regime_primary"):
        if meta_col not in frame:
            frame[meta_col] = ""
    for col in needed_rule_cols:
        if col not in frame:
            frame[col] = 0.0
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    # Keep rule-baseline validation memory bounded.  The full feature frame can
    # be millions of rows by 200+ columns; rule scoring only needs the compact
    # metadata/target/outcome/rule slice above.  Sorting/resetting the full
    # frame previously caused >1 GiB temporary allocations on Windows.
    frame = frame[[col for col in compact_cols if col in frame]].copy()
    frame = frame.dropna(
        subset=[
            "time_utc",
            any_target,
            long_outcome_col,
            short_outcome_col,
            "rule_baseline_long_score",
            "rule_baseline_short_score",
            "rule_baseline_strength_score",
        ]
    ).sort_values("time_utc").reset_index(drop=True)
    frame["week_start"] = (
        frame["time_utc"].dt.normalize()
        - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
    )
    weeks = sorted(frame["week_start"].dropna().unique())
    if max_test_weeks > 0:
        weeks = weeks[-max_test_weeks:]
    folds: List[Dict[str, Any]] = []
    selected_values_all: List[float] = []
    selected_rows_all: List[Dict[str, Any]] = []
    cooldown = max(lead_minutes, 15)
    model_label = f"rule_{rule_profile}"
    context_filter = _profile_context_filter(rule_profile)
    for fold_number, test_week in enumerate(weeks, 1):
        test_week = pd.Timestamp(test_week)
        calibration_start = test_week - pd.Timedelta(days=7)
        calibration = frame[(frame["time_utc"] >= calibration_start) & (frame["time_utc"] < test_week)].reset_index(drop=True)
        test = frame[frame["week_start"].eq(test_week)].reset_index(drop=True)
        if len(calibration) < 250 or len(test) < 250:
            continue
        cal_long_score, cal_short_score = rule_profile_scores(
            calibration,
            rule_profile,
        )
        cal_score = np.maximum(cal_long_score, cal_short_score)
        cal_is_long = cal_long_score >= cal_short_score
        cal_outcome = np.where(
            cal_is_long,
            calibration[long_outcome_col].to_numpy(dtype=float),
            calibration[short_outcome_col].to_numpy(dtype=float),
        )
        cal_outcome = apply_lead_stop_cap(
            cal_outcome,
            calibration["atr240_pips"].to_numpy(dtype=float),
            lead_stop_atr,
        )
        threshold, calibration_summary = choose_score_threshold(
            calibration,
            cal_score,
            cal_outcome,
            cooldown_minutes=cooldown,
        )
        cal_positions = episode_positions(
            calibration.reset_index(drop=True),
            cal_score,
            threshold,
            cooldown_minutes=cooldown,
        )
        context_allowlist = build_context_allowlist(
            calibration,
            cal_positions,
            cal_is_long,
            cal_outcome,
            context_filter,
        )
        test_long_score, test_short_score = rule_profile_scores(
            test,
            rule_profile,
        )
        test_score = np.maximum(test_long_score, test_short_score)
        test_is_long = test_long_score >= test_short_score
        test_outcome = np.where(
            test_is_long,
            test[long_outcome_col].to_numpy(dtype=float),
            test[short_outcome_col].to_numpy(dtype=float),
        )
        test_outcome = apply_lead_stop_cap(
            test_outcome,
            test["atr240_pips"].to_numpy(dtype=float),
            lead_stop_atr,
        )
        positions = episode_positions(
            test.reset_index(drop=True),
            test_score,
            threshold,
            cooldown_minutes=cooldown,
        )
        raw_positions_count = int(len(positions))
        positions, context_application = apply_context_allowlist(
            test,
            positions,
            test_is_long,
            context_filter,
            context_allowlist,
        )
        selected_outcome = test_outcome[positions]
        selected_values_all.extend([float(v) for v in selected_outcome if math.isfinite(float(v))])
        y_any = test[any_target].astype(int).to_numpy()
        actual_up = pd.to_numeric(test[direction_target], errors="coerce").to_numpy(dtype=float)
        predicted_up = test_is_long.astype(int)
        direction_known = np.isfinite(actual_up)
        actual_up_int = np.where(direction_known, actual_up, -1).astype(int)
        direction_match = (
            y_any.astype(bool)
            & direction_known
            & (predicted_up == actual_up_int)
        )
        selected_any_targets = y_any[positions] if len(positions) else np.asarray([], dtype=int)
        selected_directional_targets = direction_match[positions].astype(int) if len(positions) else np.asarray([], dtype=int)
        if len(positions):
            selected_rows = test.iloc[positions][[
                "time_utc",
                "instrument",
                "pair_taxonomy_primary",
                "regime_primary",
                "atr240_pips",
                "spread_pips",
                any_target,
                *RULE_BASELINE_COLUMNS,
            ]].copy()
            selected_rows["fold"] = fold_number
            selected_rows["week_start"] = test_week.isoformat()
            selected_rows["outcome_pips"] = selected_outcome
            selected_rows["probability"] = test_score[positions]
            selected_rows["predicted_direction"] = np.where(test_is_long[positions], "LONG", "SHORT")
            selected_rows["directional_target"] = selected_directional_targets
            selected_rows_all.extend(selected_rows.to_dict("records"))
        base_rate = float(np.mean(y_any)) if len(y_any) else 0.0
        event_auc = safe_metric(roc_auc_score, y_any, test_score)
        event_ap = safe_metric(average_precision_score, y_any, test_score)
        fold_summary = summarize(selected_outcome)
        folds.append({
            "model": model_label,
            "mode": "rules_directional",
            "rule_profile": rule_profile,
            "lead_minutes": lead_minutes,
            "move_horizon": move_horizon,
            "execution_horizon": exec_horizon,
            "fold": fold_number,
            "week_start": test_week.isoformat(),
            "train_rows": 0,
            "sampled_train_rows": 0,
            "calibration_rows": int(len(calibration)),
            "test_rows": int(len(test)),
            "event_base_rate": base_rate,
            "event_auc": event_auc,
            "event_average_precision": event_ap,
            "event_ap_lift": event_ap / max(base_rate, 1e-9),
            "selected_threshold": threshold,
            "raw_selected_count_before_context": raw_positions_count,
            "context_filter_enabled": bool(context_filter),
            "context_allowed_group_count": safe_int(context_application.get("allowed_count"), 0),
            "context_allowed_groups": ";".join(context_application.get("allowed_groups", [])[:25]) if isinstance(context_application.get("allowed_groups"), list) else "",
            "selected_event_precision": float(selected_any_targets.mean()) if len(selected_any_targets) else 0.0,
            "selected_directional_precision": float(selected_directional_targets.mean()) if len(selected_directional_targets) else 0.0,
            "selected_event_recall": float(selected_any_targets.sum() / max(y_any.sum(), 1)),
            "calibration_selected": calibration_summary,
            "context_filter": {
                "allowlist": context_allowlist,
                "application": context_application,
            },
            **{f"selected_{k}": v for k, v in fold_summary.items()},
        })
    selected_summary = summarize(selected_values_all)
    risk_summary = selected_risk_summary(
        selected_rows_all,
        cluster_gap_minutes=cluster_gap_minutes,
    )
    fold_frame = pd.DataFrame(folds)
    if fold_frame.empty:
        return {
            "model": model_label,
            "mode": "rules_directional",
            "rule_profile": rule_profile,
            "lead_minutes": lead_minutes,
            "move_horizon": move_horizon,
            "execution_horizon": exec_horizon,
            "fold_count": 0,
            "status": "no_valid_folds",
            "score": -1e9,
            "folds": [],
        }
    positive_weeks = int((pd.to_numeric(fold_frame["selected_mean"], errors="coerce").fillna(0) > 0).sum())
    clustered_first = risk_summary.get("clustered_first") or {}
    clustered_pips = clustered_first.get("pips") if isinstance(clustered_first, dict) else {}
    clustered_spread = clustered_first.get("spread_units") if isinstance(clustered_first, dict) else {}
    score = (
        safe_float(fold_frame["event_auc"].mean()) * 20.0
        + min(12.0, safe_float(fold_frame["event_ap_lift"].mean())) * 3.0
        + safe_float(fold_frame["selected_directional_precision"].mean()) * 15.0
        + np.clip(safe_float(selected_summary.get("mean")), -100.0, 100.0) * 0.25
        + np.clip(safe_float((clustered_pips or {}).get("mean")), -100.0, 100.0) * 0.20
        + np.clip(safe_float((clustered_spread or {}).get("mean")), -25.0, 25.0) * 1.15
        + safe_float((clustered_spread or {}).get("profit_factor")) * 2.0
        + safe_float(selected_summary.get("profit_factor")) * 2.5
        + positive_weeks * 2.0
    )
    return {
        "model": model_label,
        "mode": "rules_directional",
        "rule_profile": rule_profile,
        "lead_minutes": lead_minutes,
        "move_horizon": move_horizon,
        "execution_horizon": exec_horizon,
        "fold_count": int(len(fold_frame)),
        "status": "completed",
        "mean_event_auc": safe_float(fold_frame["event_auc"].mean()),
        "min_event_auc": safe_float(fold_frame["event_auc"].min()),
        "mean_event_ap_lift": safe_float(fold_frame["event_ap_lift"].mean()),
        "mean_direction_accuracy_on_events": safe_float(fold_frame["selected_directional_precision"].mean()),
        "mean_selected_event_precision": safe_float(fold_frame["selected_event_precision"].mean()),
        "selected_summary": selected_summary,
        "selected_risk_summary": risk_summary,
        "context_filter": context_filter,
        "positive_weeks": positive_weeks,
        "score": float(score),
        "folds": folds,
        "top_selected_examples": sorted(
            enrich_selected_records(selected_rows_all),
            key=lambda row: safe_float(row.get("outcome_pips")),
            reverse=True,
        )[:25],
    }


def train_binary_model(
    train: pd.DataFrame,
    *,
    features: List[str],
    target: str,
    model_name: str,
    max_train_rows: int,
    negative_ratio: int,
    seed: int,
) -> Any:
    sampled = sample_training(
        train,
        target,
        max_rows=max_train_rows,
        negative_ratio=negative_ratio,
        seed=seed,
    )
    if len(sampled) < 200 or sampled[target].nunique() < 2:
        return None
    model = model_for_name(model_name, seed=seed + 101)
    model.fit(sampled[features], sampled[target].astype(int))
    model._sampled_train_rows = len(sampled)  # type: ignore[attr-defined]
    return model


def evaluate_directional_combo(
    frame: pd.DataFrame,
    *,
    features: List[str],
    lead_minutes: int,
    move_horizon: int,
    model_name: str,
    max_train_rows: int,
    negative_ratio: int,
    max_test_weeks: int,
    lead_stop_atr: float,
    cluster_gap_minutes: int,
) -> Dict[str, Any]:
    exec_horizon = execution_horizon(lead_minutes, move_horizon)
    any_target = f"lead_major_event_{lead_minutes}_{move_horizon}"
    direction_target = f"lead_direction_up_{lead_minutes}_{move_horizon}"
    long_target = f"lead_long_event_{lead_minutes}_{move_horizon}"
    short_target = f"lead_short_event_{lead_minutes}_{move_horizon}"
    long_outcome_col = f"long_trailing_net_pips_{exec_horizon}"
    short_outcome_col = f"short_trailing_net_pips_{exec_horizon}"
    if any_target not in frame or direction_target not in frame:
        frame = attach_next_event_labels(
            frame,
            lead_minutes=lead_minutes,
            move_horizon=move_horizon,
        )
    frame[long_target] = (
        pd.to_numeric(frame[any_target], errors="coerce").fillna(0).astype(int).eq(1)
        & pd.to_numeric(frame[direction_target], errors="coerce").fillna(-1).astype(int).eq(1)
    ).astype(int)
    frame[short_target] = (
        pd.to_numeric(frame[any_target], errors="coerce").fillna(0).astype(int).eq(1)
        & pd.to_numeric(frame[direction_target], errors="coerce").fillna(-1).astype(int).eq(0)
    ).astype(int)
    for col in [
        any_target,
        long_target,
        short_target,
        long_outcome_col,
        short_outcome_col,
        *features,
    ]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(
        subset=["time_utc", any_target, long_target, short_target, long_outcome_col, short_outcome_col, *features]
    ).sort_values("time_utc").reset_index(drop=True)
    frame["week_start"] = (
        frame["time_utc"].dt.normalize()
        - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
    )
    weeks = sorted(frame["week_start"].dropna().unique())
    if max_test_weeks > 0:
        weeks = weeks[-max_test_weeks:]
    folds: List[Dict[str, Any]] = []
    selected_values_all: List[float] = []
    selected_rows_all: List[Dict[str, Any]] = []
    sampled_rows_all: List[int] = []
    cooldown = max(lead_minutes, 15)
    for fold_number, test_week in enumerate(weeks, 1):
        test_week = pd.Timestamp(test_week)
        calibration_start = test_week - pd.Timedelta(days=7)
        train_end = calibration_start
        train = frame[frame["time_utc"] < train_end]
        calibration = frame[(frame["time_utc"] >= calibration_start) & (frame["time_utc"] < test_week)].reset_index(drop=True)
        test = frame[frame["week_start"].eq(test_week)].reset_index(drop=True)
        if len(train) < 5000 or len(calibration) < 250 or len(test) < 250:
            continue
        long_model = train_binary_model(
            train,
            features=features,
            target=long_target,
            model_name=model_name,
            max_train_rows=max_train_rows,
            negative_ratio=negative_ratio,
            seed=fold_number * 4099 + lead_minutes,
        )
        short_model = train_binary_model(
            train,
            features=features,
            target=short_target,
            model_name=model_name,
            max_train_rows=max_train_rows,
            negative_ratio=negative_ratio,
            seed=fold_number * 6151 + move_horizon,
        )
        if long_model is None or short_model is None:
            continue
        sampled_rows_all.extend([
            safe_int(getattr(long_model, "_sampled_train_rows", 0)),
            safe_int(getattr(short_model, "_sampled_train_rows", 0)),
        ])
        long_cal_prob = long_model.predict_proba(calibration[features])[:, 1]
        short_cal_prob = short_model.predict_proba(calibration[features])[:, 1]
        long_test_prob = long_model.predict_proba(test[features])[:, 1]
        short_test_prob = short_model.predict_proba(test[features])[:, 1]
        long_threshold, long_calibration = choose_directional_threshold(
            calibration,
            long_cal_prob,
            apply_lead_stop_cap(
                calibration[long_outcome_col].to_numpy(dtype=float),
                calibration["atr240_pips"].to_numpy(dtype=float),
                lead_stop_atr,
            ),
            cooldown_minutes=cooldown,
        )
        short_threshold, short_calibration = choose_directional_threshold(
            calibration,
            short_cal_prob,
            apply_lead_stop_cap(
                calibration[short_outcome_col].to_numpy(dtype=float),
                calibration["atr240_pips"].to_numpy(dtype=float),
                lead_stop_atr,
            ),
            cooldown_minutes=cooldown,
        )
        candidates = []
        long_mask = long_test_prob >= long_threshold
        if long_mask.any():
            rows = test.loc[long_mask, [
                "time_utc",
                "instrument",
                "pair_taxonomy_primary",
                "regime_primary",
                "atr240_pips",
                "spread_pips",
                any_target,
                long_target,
            ]].copy()
            rows["probability"] = long_test_prob[long_mask]
            rows["predicted_direction"] = "LONG"
            rows["outcome_pips"] = apply_lead_stop_cap(
                test.loc[long_mask, long_outcome_col].to_numpy(dtype=float),
                test.loc[long_mask, "atr240_pips"].to_numpy(dtype=float),
                lead_stop_atr,
            )
            rows["directional_target"] = test.loc[long_mask, long_target].to_numpy(dtype=int)
            candidates.append(rows)
        short_mask = short_test_prob >= short_threshold
        if short_mask.any():
            rows = test.loc[short_mask, [
                "time_utc",
                "instrument",
                "pair_taxonomy_primary",
                "regime_primary",
                "atr240_pips",
                "spread_pips",
                any_target,
                short_target,
            ]].copy()
            rows["probability"] = short_test_prob[short_mask]
            rows["predicted_direction"] = "SHORT"
            rows["outcome_pips"] = apply_lead_stop_cap(
                test.loc[short_mask, short_outcome_col].to_numpy(dtype=float),
                test.loc[short_mask, "atr240_pips"].to_numpy(dtype=float),
                lead_stop_atr,
            )
            rows["directional_target"] = test.loc[short_mask, short_target].to_numpy(dtype=int)
            candidates.append(rows)
        candidate_frame = pd.concat(candidates, ignore_index=True) if candidates else test.iloc[0:0].copy()
        selected = episode_candidate_frame(candidate_frame, cooldown)
        selected_outcome = selected["outcome_pips"].to_numpy(dtype=float) if not selected.empty else np.asarray([], dtype=float)
        selected_values_all.extend([float(v) for v in selected_outcome if math.isfinite(float(v))])
        if not selected.empty:
            selected["fold"] = fold_number
            selected["week_start"] = test_week.isoformat()
            selected_rows_all.extend(selected.to_dict("records"))
        y_any = test[any_target].astype(int).to_numpy()
        max_event_probability = np.maximum(long_test_prob, short_test_prob)
        any_base_rate = float(y_any.mean()) if len(y_any) else 0.0
        long_y = test[long_target].astype(int).to_numpy()
        short_y = test[short_target].astype(int).to_numpy()
        selected_any_targets = selected[any_target].astype(int).to_numpy() if not selected.empty and any_target in selected else np.asarray([], dtype=int)
        selected_directional_targets = selected["directional_target"].astype(int).to_numpy() if not selected.empty and "directional_target" in selected else np.asarray([], dtype=int)
        fold_summary = summarize(selected_outcome)
        folds.append({
            "model": model_name,
            "mode": "directional",
            "lead_minutes": lead_minutes,
            "move_horizon": move_horizon,
            "execution_horizon": exec_horizon,
            "fold": fold_number,
            "week_start": test_week.isoformat(),
            "train_rows": int(len(train)),
            "sampled_train_rows": int(sum(sampled_rows_all[-2:])),
            "calibration_rows": int(len(calibration)),
            "test_rows": int(len(test)),
            "event_base_rate": any_base_rate,
            "event_auc": safe_metric(roc_auc_score, y_any, max_event_probability),
            "event_average_precision": safe_metric(average_precision_score, y_any, max_event_probability),
            "event_ap_lift": safe_metric(average_precision_score, y_any, max_event_probability) / max(any_base_rate, 1e-9),
            "long_auc": safe_metric(roc_auc_score, long_y, long_test_prob),
            "short_auc": safe_metric(roc_auc_score, short_y, short_test_prob),
            "long_threshold": long_threshold,
            "short_threshold": short_threshold,
            "selected_event_precision": float(selected_any_targets.mean()) if len(selected_any_targets) else 0.0,
            "selected_directional_precision": float(selected_directional_targets.mean()) if len(selected_directional_targets) else 0.0,
            "selected_event_recall": float(selected_any_targets.sum() / max(y_any.sum(), 1)),
            "long_calibration": long_calibration,
            "short_calibration": short_calibration,
            **{f"selected_{k}": v for k, v in fold_summary.items()},
        })
    selected_summary = summarize(selected_values_all)
    risk_summary = selected_risk_summary(
        selected_rows_all,
        cluster_gap_minutes=cluster_gap_minutes,
    )
    fold_frame = pd.DataFrame(folds)
    if fold_frame.empty:
        return {
            "model": model_name,
            "mode": "directional",
            "lead_minutes": lead_minutes,
            "move_horizon": move_horizon,
            "execution_horizon": exec_horizon,
            "fold_count": 0,
            "status": "no_valid_folds",
            "score": -1e9,
            "folds": [],
        }
    positive_weeks = int((pd.to_numeric(fold_frame["selected_mean"], errors="coerce").fillna(0) > 0).sum())
    clustered_first = risk_summary.get("clustered_first") or {}
    clustered_pips = clustered_first.get("pips") if isinstance(clustered_first, dict) else {}
    clustered_spread = clustered_first.get("spread_units") if isinstance(clustered_first, dict) else {}
    score = (
        safe_float(fold_frame["event_auc"].mean()) * 30.0
        + min(12.0, safe_float(fold_frame["event_ap_lift"].mean())) * 4.0
        + safe_float(fold_frame["selected_directional_precision"].mean()) * 25.0
        + np.clip(safe_float(selected_summary.get("mean")), -100.0, 100.0) * 0.35
        + np.clip(safe_float((clustered_pips or {}).get("mean")), -100.0, 100.0) * 0.20
        + np.clip(safe_float((clustered_spread or {}).get("mean")), -25.0, 25.0) * 1.25
        + safe_float((clustered_spread or {}).get("profit_factor")) * 2.0
        + safe_float(selected_summary.get("profit_factor")) * 3.0
        + positive_weeks * 2.0
    )
    return {
        "model": model_name,
        "mode": "directional",
        "lead_minutes": lead_minutes,
        "move_horizon": move_horizon,
        "execution_horizon": exec_horizon,
        "fold_count": int(len(fold_frame)),
        "status": "completed",
        "mean_event_auc": safe_float(fold_frame["event_auc"].mean()),
        "min_event_auc": safe_float(fold_frame["event_auc"].min()),
        "mean_event_ap_lift": safe_float(fold_frame["event_ap_lift"].mean()),
        "mean_direction_accuracy_on_events": safe_float(fold_frame["selected_directional_precision"].mean()),
        "mean_selected_event_precision": safe_float(fold_frame["selected_event_precision"].mean()),
        "selected_summary": selected_summary,
        "selected_risk_summary": risk_summary,
        "positive_weeks": positive_weeks,
        "score": float(score),
        "folds": folds,
        "top_selected_examples": sorted(
            enrich_selected_records(selected_rows_all),
            key=lambda row: safe_float(row.get("outcome_pips")),
            reverse=True,
        )[:25],
    }


def build_report(args: argparse.Namespace) -> Dict[str, Any]:
    feature_set = str(args.feature_set or "full").lower()
    base_features = feature_columns("core" if feature_set == "core" else "full")
    needed = required_columns(base_features, args.horizons, args.leads)
    available = set(parquet_columns(args.dataset))
    read_columns = [column for column in list(dict.fromkeys(needed)) if column in available]
    frame = pd.read_parquet(args.dataset, columns=read_columns)
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
    frame = apply_subset(frame, args.instrument_subset)
    if args.max_rows > 0 and len(frame) > args.max_rows:
        frame = frame.sort_values("time_utc").tail(args.max_rows).reset_index(drop=True)
    frame, expanded_features = add_expanded_pre_spike_features(frame)
    for lead in args.leads:
        for horizon in args.horizons:
            target = f"lead_major_event_{int(lead)}_{int(horizon)}"
            direction_target = f"lead_direction_up_{int(lead)}_{int(horizon)}"
            if target not in frame or direction_target not in frame:
                frame = attach_next_event_labels(
                    frame,
                    lead_minutes=int(lead),
                    move_horizon=int(horizon),
                )
    features = list(dict.fromkeys([*base_features, *expanded_features]))
    for feature in features:
        if feature not in frame:
            frame[feature] = 0.0
        frame[feature] = pd.to_numeric(frame[feature], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    results: List[Dict[str, Any]] = []
    fold_rows: List[Dict[str, Any]] = []
    rule_baseline_results: List[Dict[str, Any]] = []
    lead_stop_atrs = list(dict.fromkeys([
        *[safe_float(value, args.lead_stop_atr) for value in getattr(args, "lead_stop_atrs", [])],
        safe_float(args.lead_stop_atr, 1.5),
    ]))
    requested_rule_profiles = [
        str(profile)
        for profile in getattr(args, "rule_profiles", []) or []
        if str(profile) in RULE_PROFILE_SPECS
    ] or list(RULE_PROFILE_SPECS)
    rule_baseline_combos = [
        (profile, lead, horizon, stop_atr)
        for profile in requested_rule_profiles
        for lead in args.leads
        for horizon in args.horizons
        for stop_atr in lead_stop_atrs
    ]
    if not args.skip_rule_baselines:
        for profile, lead, horizon, stop_atr in rule_baseline_combos:
            result = evaluate_rule_baseline_combo(
                frame.copy(),
                rule_profile=str(profile),
                lead_minutes=int(lead),
                move_horizon=int(horizon),
                max_test_weeks=args.max_test_weeks,
                lead_stop_atr=float(stop_atr),
                cluster_gap_minutes=args.cluster_gap_minutes,
            )
            result["lead_stop_atr"] = float(stop_atr)
            clean_result = {key: value for key, value in result.items() if key not in {"folds"}}
            rule_baseline_results.append(clean_result)
            results.append(clean_result)
            for fold in result.get("folds", []):
                fold["lead_stop_atr"] = float(stop_atr)
                fold_rows.append(fold)
            if args.incremental:
                completed_now = [r for r in results if r.get("status") == "completed"]
                best_now = max(
                    completed_now,
                    key=lambda row: safe_float(row.get("score"), -1e9),
                    default={},
                )
                atomic_write_json(args.report, {
                    "generated_utc": utc_iso(),
                    "status": "partial",
                    "completed_combos": len(results),
                    "total_combos": len(rule_baseline_combos),
                    "model_combo_count": 0,
                    "dataset": str(args.dataset),
                    "dataset_rows_used": int(len(frame)),
                    "instrument_subset": args.instrument_subset,
                    "instrument_count": int(frame["instrument"].nunique()) if "instrument" in frame else 0,
                    "feature_set": feature_set,
                    "feature_count": len(features),
                    "rule_baseline_count": len(rule_baseline_results),
                    "leads": list(args.leads),
                    "horizons": list(args.horizons),
                    "models": [] if args.only_rule_baselines else list(args.models),
                    "modes": [] if args.only_rule_baselines else list(args.modes),
                    "lead_stop_atrs": lead_stop_atrs,
                    "cluster_gap_minutes": args.cluster_gap_minutes,
                    "results": sorted(results, key=lambda row: safe_float(row.get("score"), -1e9), reverse=True),
                    "rule_baselines": sorted(rule_baseline_results, key=lambda row: safe_float(row.get("score"), -1e9), reverse=True),
                    "best": best_now,
                    "fold_csv": str(args.folds_csv),
                })
                write_csv(args.folds_csv, fold_rows)
    combos = []
    if not args.only_rule_baselines:
        combos = [
            (mode, model, lead, horizon, stop_atr)
            for mode in args.modes
            for model in args.models
            for lead in args.leads
            for horizon in args.horizons
            for stop_atr in lead_stop_atrs
        ]
    if args.max_combos > 0:
        combos = combos[: args.max_combos]
    for mode, model, lead, horizon, stop_atr in combos:
        evaluator = evaluate_directional_combo if str(mode) == "directional" else evaluate_combo
        result = evaluator(
            frame.copy(),
            features=features,
            lead_minutes=int(lead),
            move_horizon=int(horizon),
            model_name=str(model),
            max_train_rows=args.max_train_rows,
            negative_ratio=args.negative_ratio,
            max_test_weeks=args.max_test_weeks,
            lead_stop_atr=float(stop_atr),
            cluster_gap_minutes=args.cluster_gap_minutes,
        )
        result["lead_stop_atr"] = float(stop_atr)
        results.append({key: value for key, value in result.items() if key not in {"folds"}})
        for fold in result.get("folds", []):
            fold["lead_stop_atr"] = float(stop_atr)
            fold_rows.append(fold)
        if args.incremental:
            completed_now = [r for r in results if r.get("status") == "completed"]
            best_now = max(
                completed_now,
                key=lambda row: safe_float(row.get("score"), -1e9),
                default={},
            )
            atomic_write_json(args.report, {
                "generated_utc": utc_iso(),
                "status": "partial",
                "completed_combos": len(results),
                "total_combos": len(combos) + (0 if args.skip_rule_baselines else len(rule_baseline_combos)),
                "model_combo_count": len(combos),
                "dataset": str(args.dataset),
                "dataset_rows_used": int(len(frame)),
                "instrument_subset": args.instrument_subset,
                "instrument_count": int(frame["instrument"].nunique()) if "instrument" in frame else 0,
                "feature_set": feature_set,
                "feature_count": len(features),
                "rule_baseline_count": len(rule_baseline_results),
                "leads": list(args.leads),
                "horizons": list(args.horizons),
                "models": list(args.models),
                "modes": list(args.modes),
                "lead_stop_atrs": lead_stop_atrs,
                "cluster_gap_minutes": args.cluster_gap_minutes,
                "results": sorted(results, key=lambda row: safe_float(row.get("score"), -1e9), reverse=True),
                "rule_baselines": sorted(rule_baseline_results, key=lambda row: safe_float(row.get("score"), -1e9), reverse=True),
                "best": best_now,
                "fold_csv": str(args.folds_csv),
            })
            write_csv(args.folds_csv, fold_rows)
    completed = [r for r in results if r.get("status") == "completed"]
    best = max(completed, key=lambda row: safe_float(row.get("score"), -1e9), default={})
    report = {
        "generated_utc": utc_iso(),
        "status": "completed",
        "completed_combos": len(results),
        "total_combos": len(combos) + (0 if args.skip_rule_baselines else len(rule_baseline_combos)),
        "model_combo_count": len(combos),
        "rule_baseline_count": len(rule_baseline_results),
        "dataset": str(args.dataset),
        "dataset_rows_used": int(len(frame)),
        "dataset_columns_requested": len(list(dict.fromkeys(needed))),
        "dataset_columns_loaded": len(read_columns),
        "optional_columns_missing": sorted(set(needed) - set(read_columns))[:50],
        "instrument_subset": args.instrument_subset,
        "instrument_count": int(frame["instrument"].nunique()) if "instrument" in frame else 0,
        "feature_set": feature_set,
        "base_feature_count": len(base_features),
        "expanded_feature_count": len(expanded_features),
        "feature_count": len(features),
        "expanded_features": expanded_features,
        "rule_baseline_features": RULE_BASELINE_COLUMNS,
        "rule_profiles": RULE_PROFILE_SPECS,
        "requested_rule_profiles": requested_rule_profiles,
        "leads": list(args.leads),
        "horizons": list(args.horizons),
        "models": list(args.models),
        "modes": list(args.modes),
        "max_test_weeks": args.max_test_weeks,
        "max_train_rows": args.max_train_rows,
        "negative_ratio": args.negative_ratio,
        "lead_stop_atrs": lead_stop_atrs,
        "cluster_gap_minutes": args.cluster_gap_minutes,
        "assumptions": {
            "entry": "enter at row time T when event probability exceeds calibrated threshold",
            "lead_label": "positive when a major-move onset starts within lead_minutes after T",
            "direction": "separate direction model predicts long/short for selected rows",
            "exit": "uses precomputed trailing path outcome from T over lead+move horizon, capped by lead_stop_atr as an early invalidation stop",
            "rule_baselines": "deterministic compression/breakout/exhaustion/session/cross-pressure/liquidity/anti-chase rules are evaluated as baselines and also fed as model features",
            "clustered_scoring": "selected_risk_summary reports raw and one-signal-per-instrument-direction-cluster pips, ATR units, spread units, and concentration",
            "live_execution": "none; research-only evidence for scout/trainer",
        },
        "results": sorted(results, key=lambda row: safe_float(row.get("score"), -1e9), reverse=True),
        "rule_baselines": sorted(rule_baseline_results, key=lambda row: safe_float(row.get("score"), -1e9), reverse=True),
        "best": best,
        "fold_csv": str(args.folds_csv),
    }
    atomic_write_json(args.report, report)
    write_csv(args.folds_csv, fold_rows)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--folds-csv", type=Path, default=DEFAULT_FOLDS_CSV)
    parser.add_argument("--instrument-subset", default="volatile_exotic")
    parser.add_argument("--feature-set", choices=["core", "full"], default="full")
    parser.add_argument("--leads", nargs="+", type=int, default=[15, 30, 60])
    parser.add_argument("--horizons", nargs="+", type=int, default=[60, 120])
    parser.add_argument("--models", nargs="+", default=["hist_gradient_boosting"])
    parser.add_argument("--modes", nargs="+", choices=["directional", "two_stage"], default=["directional"])
    parser.add_argument("--max-test-weeks", type=int, default=6)
    parser.add_argument("--max-train-rows", type=int, default=200_000)
    parser.add_argument("--negative-ratio", type=int, default=8)
    parser.add_argument("--lead-stop-atr", type=float, default=1.5)
    parser.add_argument("--lead-stop-atrs", nargs="+", type=float, default=[])
    parser.add_argument("--cluster-gap-minutes", type=int, default=60)
    parser.add_argument("--skip-rule-baselines", action="store_true")
    parser.add_argument("--only-rule-baselines", action="store_true")
    parser.add_argument("--rule-profiles", nargs="+", default=[])
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--max-combos", type=int, default=0)
    args = parser.parse_args()
    report = build_report(args)
    best = report.get("best") or {}
    best_risk = best.get("selected_risk_summary") if isinstance(best, dict) else {}
    clustered_first = (best_risk or {}).get("clustered_first") if isinstance(best_risk, dict) else {}
    clustered_pips = (clustered_first or {}).get("pips") if isinstance(clustered_first, dict) else {}
    print(json.dumps({
        "time_utc": report["generated_utc"],
        "dataset_rows_used": report["dataset_rows_used"],
        "instrument_subset": report["instrument_subset"],
        "instrument_count": report["instrument_count"],
        "feature_count": report["feature_count"],
        "result_count": len(report["results"]),
        "rule_baseline_count": report.get("rule_baseline_count", 0),
        "best": {
            key: best.get(key)
            for key in [
                "model",
                "mode",
                "lead_minutes",
                "move_horizon",
                "execution_horizon",
                "lead_stop_atr",
                "fold_count",
                "mean_event_auc",
                "mean_event_ap_lift",
                "mean_direction_accuracy_on_events",
                "selected_summary",
                "positive_weeks",
                "score",
            ]
        },
        "best_clustered_first": {
            "cluster_count": (best_risk or {}).get("cluster_count") if isinstance(best_risk, dict) else None,
            "sum_pips": (clustered_pips or {}).get("sum") if isinstance(clustered_pips, dict) else None,
            "mean_pips": (clustered_pips or {}).get("mean") if isinstance(clustered_pips, dict) else None,
            "positive_rate": (clustered_pips or {}).get("positive_rate") if isinstance(clustered_pips, dict) else None,
        },
        "report": str(args.report),
        "fold_csv": str(args.folds_csv),
    }, indent=2, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
