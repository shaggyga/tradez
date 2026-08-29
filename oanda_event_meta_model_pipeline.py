#!/usr/bin/env python3
"""Leakage-safe meta-model for OANDA technical event/pressure trade candidates."""

from __future__ import annotations

import argparse
from io import StringIO
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import joblib
import numpy as np
import pandas as pd
import requests
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

import oanda_gpt_training_strategy_manager as manager
from oanda_production_model_pipeline import load_bam


PIPELINE_VERSION = "event_meta_v1"
FRED_HISTORY_POINT_IN_TIME_SAFE = False
EVENT_WINDOWS = [5, 10, 15, 30]
STOP_BASE_PIPS = 14.0
TARGET_R_MULTIPLES = [1.0, 1.5, 2.0]


class SklearnFittedClassifierAdapter(ClassifierMixin, BaseEstimator):
    """Expose fitted-state metadata for third-party sklearn-style classifiers."""

    def __init__(self, estimator: Any) -> None:
        self.estimator = estimator

    def fit(self, features: Any, target: Any) -> "SklearnFittedClassifierAdapter":
        self.estimator_ = self.estimator.fit(features, target)
        self.classes_ = np.asarray(getattr(self.estimator_, "classes_", [0, 1]))
        return self

    def predict_proba(self, features: Any) -> np.ndarray:
        return np.asarray(self.estimator_.predict_proba(features), dtype=float)

    def predict(self, features: Any) -> np.ndarray:
        if hasattr(self.estimator_, "predict"):
            return np.asarray(self.estimator_.predict(features))
        return self.classes_[np.argmax(self.predict_proba(features), axis=1)]

    def __sklearn_is_fitted__(self) -> bool:
        return hasattr(self, "estimator_")
MAX_HOLD_MINUTES = 180
SLIPPAGE_RESERVE_PIPS = 0.20
PURGE_MINUTES = MAX_HOLD_MINUTES + 5

BASE_NUMERIC_FEATURES = [
    "spread_pips",
    "momentum_1_pips",
    "momentum_3_pips",
    "momentum_5_pips",
    "momentum_15_pips",
    "momentum_30_pips",
    "momentum_60_pips",
    "directional_momentum_1_pips",
    "directional_momentum_3_pips",
    "directional_momentum_5_pips",
    "directional_momentum_15_pips",
    "directional_momentum_30_pips",
    "directional_momentum_60_pips",
    "volatility_5_pips",
    "volatility_15_pips",
    "volatility_30_pips",
    "volatility_60_pips",
    "compression_score",
    "move_spread_ratio",
    "spread_to_volatility",
    "range_30_pips",
    "range_position_30",
    "directional_range_position_30",
    "directional_trend_alignment",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
    "pressure_weighted_pips",
    "directional_pressure_weighted_pips",
    "pressure_score",
    "pressure_accel_pips",
    "pressure_compression_ratio",
    "pressure_expansion_ratio",
    "event_ratio_5",
    "event_ratio_10",
    "event_ratio_15",
    "event_ratio_30",
    "directional_event_move_5",
    "directional_event_move_10",
    "directional_event_move_15",
    "directional_event_move_30",
    "event_cost_net_5",
    "event_cost_net_10",
    "event_cost_net_15",
    "event_cost_net_30",
    "directional_currency_strength_gap",
    "basket_confirmation_count",
]
META_NUMERIC_FEATURES = [
    "trigger_window",
    "trigger_abs_move_pips",
    "trigger_directional_move_pips",
    "trigger_ratio",
    "trigger_cost_net_pips",
    "pressure_ratio",
    "stop_pips",
    "target_pips",
    "target_r_multiple",
]
MACRO_FEATURES = [
    "macro_vix",
    "macro_vix_change_5d",
    "macro_fed_funds",
    "macro_us_2y",
    "macro_us_10y",
    "macro_curve_10y_2y",
    "macro_usd_index",
    "macro_usd_change_5d",
    "macro_sp500_return_1d",
    "macro_sp500_return_5d",
    "macro_oil_return_1d",
    "macro_oil_return_5d",
    "macro_base_rate",
    "macro_quote_rate",
    "macro_rate_differential",
]
COT_FEATURES = [
    "cot_directional_lev_money_net",
    "cot_directional_asset_manager_net",
    "cot_directional_dealer_net",
    "cot_directional_lev_money_change",
    "cot_directional_asset_manager_change",
    "cot_directional_open_interest_change",
    "cot_crowding_gap",
]
NUMERIC_FEATURES = BASE_NUMERIC_FEATURES + META_NUMERIC_FEATURES + MACRO_FEATURES + COT_FEATURES
CATEGORICAL_FEATURES = ["instrument", "direction", "theme", "setup_family", "trade_style"]
TARGET = "target_before_stop"

FRED_SERIES = {
    "macro_vix": ("VIXCLS", 1),
    "macro_fed_funds": ("DFF", 1),
    "macro_us_2y": ("DGS2", 1),
    "macro_us_10y": ("DGS10", 1),
    "macro_usd_index": ("DTWEXBGS", 1),
    "macro_sp500": ("SP500", 1),
    "macro_oil": ("DCOILWTICO", 1),
    "rate_EUR": ("ECBDFR", 1),
    "rate_GBP": ("IRSTCI01GBM156N", 45),
    "rate_JPY": ("IRSTCI01JPM156N", 45),
    "rate_CAD": ("IRSTCI01CAM156N", 45),
    "rate_AUD": ("IRSTCI01AUM156N", 45),
    "rate_NZD": ("IRSTCI01NZM156N", 45),
    "rate_CHF": ("IRSTCI01CHM156N", 45),
}

CFTC_CONTRACTS = {
    "EUR": "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "GBP": "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
    "JPY": "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "CAD": "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "AUD": "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "NZD": "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "CHF": "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
}


def latest_source_dataset() -> Path:
    paths = sorted(manager.DIRS["training_sets"].glob("production_training_set_h15_*.csv"))
    if not paths:
        raise FileNotFoundError("No one-year production H15 source dataset found.")
    return paths[-1]


def latest_labeled_dataset() -> Path:
    paths = sorted(manager.DIRS["training_sets"].glob("event_meta_training_set_*.csv"))
    if not paths:
        raise FileNotFoundError("No labeled event meta dataset found.")
    return paths[-1]


def fred_macro_frame(
    refresh: bool = False,
    *,
    allow_revised_history_for_research: bool = False,
) -> pd.DataFrame:
    if not allow_revised_history_for_research:
        raise RuntimeError(
            "FRED graph history is current/revised rather than vintage-causal; "
            "use an ALFRED/FRED vintage adapter before validation, or explicitly "
            "opt in for exploratory research that cannot be promoted"
        )
    macro_dir = manager.TRAINING_ROOT / "macro"
    macro_dir.mkdir(parents=True, exist_ok=True)
    cache_path = macro_dir / "fred_regime_features.csv"
    if cache_path.exists() and not refresh:
        cached = pd.read_csv(cache_path)
        cached["available_utc"] = pd.to_datetime(cached["available_utc"], utc=True)
        return cached
    frames = []
    for feature, (series_id, lag_days) in FRED_SERIES.items():
        response = requests.get(
            f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}",
            timeout=60,
        )
        response.raise_for_status()
        frame = pd.read_csv(StringIO(response.text))
        value_column = [column for column in frame.columns if column != "observation_date"][0]
        frame["observation_date"] = pd.to_datetime(frame["observation_date"], errors="coerce", utc=True)
        frame[feature] = pd.to_numeric(frame[value_column], errors="coerce")
        frame["available_utc"] = frame["observation_date"] + pd.Timedelta(days=lag_days)
        frames.append(frame[["available_utc", feature]].dropna())
    all_dates = pd.DataFrame({
        "available_utc": sorted(set().union(*[set(frame["available_utc"]) for frame in frames]))
    })
    for frame in frames:
        all_dates = pd.merge_asof(
            all_dates.sort_values("available_utc"),
            frame.sort_values("available_utc"),
            on="available_utc",
            direction="backward",
        )
    all_dates = all_dates.sort_values("available_utc").ffill()
    all_dates["macro_vix_change_5d"] = all_dates["macro_vix"].pct_change(5)
    all_dates["macro_curve_10y_2y"] = all_dates["macro_us_10y"] - all_dates["macro_us_2y"]
    all_dates["macro_usd_change_5d"] = all_dates["macro_usd_index"].pct_change(5)
    all_dates["macro_sp500_return_1d"] = all_dates["macro_sp500"].pct_change(1)
    all_dates["macro_sp500_return_5d"] = all_dates["macro_sp500"].pct_change(5)
    all_dates["macro_oil_return_1d"] = all_dates["macro_oil"].pct_change(1)
    all_dates["macro_oil_return_5d"] = all_dates["macro_oil"].pct_change(5)
    all_dates.to_csv(cache_path, index=False)
    return all_dates


def add_macro_features(
    df: pd.DataFrame,
    refresh: bool = False,
    *,
    allow_revised_history_for_research: bool = False,
) -> pd.DataFrame:
    macro = fred_macro_frame(
        refresh=refresh,
        allow_revised_history_for_research=allow_revised_history_for_research,
    )
    output = pd.merge_asof(
        df.sort_values("time_utc"),
        macro.sort_values("available_utc"),
        left_on="time_utc",
        right_on="available_utc",
        direction="backward",
    )
    output["base_currency"] = output["instrument"].str.slice(0, 3)
    output["quote_currency"] = output["instrument"].str.slice(-3)
    rate_columns = {
        "USD": "macro_fed_funds",
        "EUR": "rate_EUR",
        "GBP": "rate_GBP",
        "JPY": "rate_JPY",
        "CAD": "rate_CAD",
        "AUD": "rate_AUD",
        "NZD": "rate_NZD",
        "CHF": "rate_CHF",
    }
    output["macro_base_rate"] = [
        row.get(rate_columns.get(currency, ""), np.nan)
        for currency, (_, row) in zip(output["base_currency"], output.iterrows())
    ]
    output["macro_quote_rate"] = [
        row.get(rate_columns.get(currency, ""), np.nan)
        for currency, (_, row) in zip(output["quote_currency"], output.iterrows())
    ]
    output["macro_rate_differential"] = output["macro_base_rate"] - output["macro_quote_rate"]
    direction_multiplier = np.where(output["direction"].eq("LONG"), 1.0, -1.0)
    output["macro_rate_differential"] *= direction_multiplier
    output = output.drop(columns=[
        "available_utc",
        "base_currency",
        "quote_currency",
        "macro_sp500",
        "macro_oil",
        *[f"rate_{currency}" for currency in ["EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]],
    ], errors="ignore")
    for column in MACRO_FEATURES:
        output[column] = pd.to_numeric(output[column], errors="coerce")
    return output.dropna(subset=MACRO_FEATURES)


def cftc_position_frame(refresh: bool = False) -> pd.DataFrame:
    macro_dir = manager.TRAINING_ROOT / "macro"
    macro_dir.mkdir(parents=True, exist_ok=True)
    cache_path = macro_dir / "cftc_currency_positioning.csv"
    if cache_path.exists() and not refresh:
        cached = pd.read_csv(cache_path)
        cached["available_utc"] = pd.to_datetime(cached["available_utc"], utc=True)
        return cached
    base_url = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
    rows = []
    numeric_columns = [
        "open_interest_all",
        "lev_money_positions_long",
        "lev_money_positions_short",
        "asset_mgr_positions_long",
        "asset_mgr_positions_short",
        "dealer_positions_long_all",
        "dealer_positions_short_all",
        "change_in_open_interest_all",
        "change_in_lev_money_long",
        "change_in_lev_money_short",
        "change_in_asset_mgr_long",
        "change_in_asset_mgr_short",
    ]
    select_columns = ["report_date_as_yyyy_mm_dd", "market_and_exchange_names"] + numeric_columns
    for currency, market_name in CFTC_CONTRACTS.items():
        response = requests.get(
            base_url,
            params={
                "$select": ",".join(select_columns),
                "$where": (
                    f"market_and_exchange_names='{market_name}' "
                    "and report_date_as_yyyy_mm_dd >= '2024-01-01T00:00:00.000'"
                ),
                "$order": "report_date_as_yyyy_mm_dd asc",
                "$limit": "5000",
            },
            timeout=60,
        )
        response.raise_for_status()
        for item in response.json():
            row = {"currency": currency, "report_date": item.get("report_date_as_yyyy_mm_dd")}
            for column in numeric_columns:
                row[column] = manager.safe_float(item.get(column), float("nan"))
            rows.append(row)
    frame = pd.DataFrame(rows)
    frame["report_date"] = pd.to_datetime(frame["report_date"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["report_date", "open_interest_all"])
    frame["available_utc"] = frame["report_date"] + pd.Timedelta(days=4)
    open_interest = frame["open_interest_all"].clip(lower=1.0)
    frame["lev_net"] = (
        frame["lev_money_positions_long"] - frame["lev_money_positions_short"]
    ) / open_interest
    frame["asset_net"] = (
        frame["asset_mgr_positions_long"] - frame["asset_mgr_positions_short"]
    ) / open_interest
    frame["dealer_net"] = (
        frame["dealer_positions_long_all"] - frame["dealer_positions_short_all"]
    ) / open_interest
    frame["lev_change"] = (
        frame["change_in_lev_money_long"] - frame["change_in_lev_money_short"]
    ) / open_interest
    frame["asset_change"] = (
        frame["change_in_asset_mgr_long"] - frame["change_in_asset_mgr_short"]
    ) / open_interest
    frame["oi_change"] = frame["change_in_open_interest_all"] / open_interest
    wide = frame.pivot_table(
        index="available_utc",
        columns="currency",
        values=["lev_net", "asset_net", "dealer_net", "lev_change", "asset_change", "oi_change"],
        aggfunc="last",
    )
    wide.columns = [f"cot_{metric}_{currency}" for metric, currency in wide.columns]
    wide = wide.sort_index().ffill().reset_index()
    wide.to_csv(cache_path, index=False)
    return wide


def add_cftc_features(df: pd.DataFrame, refresh: bool = False) -> pd.DataFrame:
    cot = cftc_position_frame(refresh=refresh)
    output = pd.merge_asof(
        df.sort_values("time_utc"),
        cot.sort_values("available_utc"),
        left_on="time_utc",
        right_on="available_utc",
        direction="backward",
    )
    output["base_currency"] = output["instrument"].str.slice(0, 3)
    output["quote_currency"] = output["instrument"].str.slice(-3)
    direction_multiplier = np.where(output["direction"].eq("LONG"), 1.0, -1.0)
    def currency_value(row: pd.Series, metric: str, currency: str) -> float:
        if currency == "USD":
            return 0.0
        return manager.safe_float(row.get(f"cot_{metric}_{currency}"), 0.0)
    feature_specs = {
        "cot_directional_lev_money_net": "lev_net",
        "cot_directional_asset_manager_net": "asset_net",
        "cot_directional_dealer_net": "dealer_net",
        "cot_directional_lev_money_change": "lev_change",
        "cot_directional_asset_manager_change": "asset_change",
        "cot_directional_open_interest_change": "oi_change",
    }
    for feature, metric in feature_specs.items():
        values = []
        for _, row in output.iterrows():
            base = currency_value(row, metric, row["base_currency"])
            quote = currency_value(row, metric, row["quote_currency"])
            values.append(base - quote)
        output[feature] = np.asarray(values) * direction_multiplier
    output["cot_crowding_gap"] = (
        output["cot_directional_lev_money_net"] - output["cot_directional_asset_manager_net"]
    )
    output = output.drop(columns=[
        "available_utc",
        "base_currency",
        "quote_currency",
        *[column for column in output.columns if column.startswith("cot_") and column not in COT_FEATURES],
    ], errors="ignore")
    for column in COT_FEATURES:
        output[column] = pd.to_numeric(output[column], errors="coerce").fillna(0.0)
    return output


def select_candidates(source: pd.DataFrame) -> pd.DataFrame:
    df = source.copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True, errors="coerce")
    for column in BASE_NUMERIC_FEATURES:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["time_utc", "instrument", "direction"] + BASE_NUMERIC_FEATURES)
    event_moves = df[[f"directional_event_move_{window}" for window in EVENT_WINDOWS]].to_numpy(float)
    event_ratios = df[[f"event_ratio_{window}" for window in EVENT_WINDOWS]].to_numpy(float)
    event_costs = df[[f"event_cost_net_{window}" for window in EVENT_WINDOWS]].to_numpy(float)
    continuation_eligible = (event_moves >= 3.5) & (event_ratios >= 1.6)
    fade_eligible = (event_moves <= -3.5) & (event_ratios >= 1.6)
    continuation_score = np.where(continuation_eligible, event_ratios * np.sqrt(np.abs(event_moves)), -np.inf)
    fade_score = np.where(fade_eligible, event_ratios * np.sqrt(np.abs(event_moves)), -np.inf)
    continuation_index = np.argmax(continuation_score, axis=1)
    fade_index = np.argmax(fade_score, axis=1)
    continuation_any = np.isfinite(np.max(continuation_score, axis=1))
    fade_any = np.isfinite(np.max(fade_score, axis=1))
    pressure_ratio = (
        df["directional_pressure_weighted_pips"].abs()
        / df["spread_pips"].clip(lower=0.1)
    )
    pressure_continuation = (
        (df["directional_pressure_weighted_pips"] >= 1.225)
        & (df["pressure_score"] >= 78.0)
        & (pressure_ratio >= 0.80)
    )
    pressure_fade = (
        (df["directional_pressure_weighted_pips"] <= -1.225)
        & (df["pressure_score"] >= 78.0)
        & (pressure_ratio >= 0.80)
    )
    candidates: List[pd.DataFrame] = []
    for family, style, mask, index_values in [
        ("hard_event", "continuation", continuation_any, continuation_index),
        ("hard_event", "fade", fade_any, fade_index),
        ("pre_spike_pressure", "continuation", pressure_continuation.to_numpy(), continuation_index),
        ("pre_spike_pressure", "fade", pressure_fade.to_numpy(), fade_index),
    ]:
        if not np.any(mask):
            continue
        part = df.loc[mask].copy()
        selected_indices = index_values[mask]
        part["setup_family"] = family
        part["trade_style"] = style
        if family == "hard_event":
            part["trigger_window"] = [EVENT_WINDOWS[index] for index in selected_indices]
            part["trigger_directional_move_pips"] = [
                event_moves[row_index, window_index]
                for row_index, window_index in zip(np.flatnonzero(mask), selected_indices)
            ]
            part["trigger_ratio"] = [
                event_ratios[row_index, window_index]
                for row_index, window_index in zip(np.flatnonzero(mask), selected_indices)
            ]
            part["trigger_cost_net_pips"] = [
                event_costs[row_index, window_index]
                for row_index, window_index in zip(np.flatnonzero(mask), selected_indices)
            ]
        else:
            part["trigger_window"] = 5
            part["trigger_directional_move_pips"] = part["directional_pressure_weighted_pips"]
            part["trigger_ratio"] = pressure_ratio.loc[mask].to_numpy()
            part["trigger_cost_net_pips"] = (
                part["directional_pressure_weighted_pips"] - part["spread_pips"]
            )
        part["trigger_abs_move_pips"] = part["trigger_directional_move_pips"].abs()
        part["pressure_ratio"] = (
            part["directional_pressure_weighted_pips"].abs()
            / part["spread_pips"].clip(lower=0.1)
        )
        candidates.append(part)
    if not candidates:
        return pd.DataFrame()
    out = pd.concat(candidates, ignore_index=True)
    out = out[out["spread_pips"].between(0.01, 6.0)]
    out["candidate_strength"] = (
        out["trigger_ratio"].clip(lower=0)
        * np.sqrt(out["trigger_abs_move_pips"].clip(lower=0))
        + out["pressure_score"] / 20.0
        + out["basket_confirmation_count"].clip(lower=0)
    )
    out = out.sort_values(["instrument", "direction", "setup_family", "trade_style", "time_utc"])
    keep = []
    last_seen: Dict[Tuple[str, str, str, str], pd.Timestamp] = {}
    for row in out.itertuples():
        key = (row.instrument, row.direction, row.setup_family, row.trade_style)
        previous = last_seen.get(key)
        allowed = previous is None or row.time_utc - previous >= pd.Timedelta(minutes=30)
        keep.append(allowed)
        if allowed:
            last_seen[key] = row.time_utc
    return out.loc[keep].reset_index(drop=True)


def _path_label(row: pd.Series, candles: pd.DataFrame) -> Dict[str, Any] | None:
    timestamp = row["time_utc"]
    position = candles["dt"].searchsorted(timestamp)
    if position >= len(candles) or candles.iloc[position]["dt"] != timestamp:
        return None
    future = candles.iloc[position + 1:position + MAX_HOLD_MINUTES + 1]
    if len(future) < MAX_HOLD_MINUTES:
        return None
    if future.iloc[-1]["dt"] - timestamp > pd.Timedelta(minutes=MAX_HOLD_MINUTES + 1):
        return None
    if future["dt"].diff().dropna().max() > pd.Timedelta(minutes=2):
        return None
    required = ["bid_high", "bid_low", "bid_close", "ask_high", "ask_low", "ask_close"]
    if future[required].isna().any().any():
        return None
    multiplier = manager.pips_multiplier(row["instrument"])
    slippage_price = SLIPPAGE_RESERVE_PIPS / multiplier
    stop_pips = float(row["stop_pips"])
    target_pips = float(row["target_pips"])
    if row["direction"] == "LONG":
        entry = float(candles.iloc[position]["ask_close"]) + slippage_price
        stop_price = entry - stop_pips / multiplier
        target_price = entry + target_pips / multiplier
        for minute, bar in enumerate(future.itertuples(), 1):
            stop_hit = float(bar.bid_low) <= stop_price
            target_hit = float(bar.bid_high) >= target_price
            if stop_hit:
                return {"target_before_stop": 0, "realized_pips": -stop_pips, "exit_reason": "stop", "holding_minutes": minute}
            if target_hit:
                return {"target_before_stop": 1, "realized_pips": target_pips, "exit_reason": "target", "holding_minutes": minute}
        realized = (float(future.iloc[-1]["bid_close"]) - entry) * multiplier
    else:
        entry = float(candles.iloc[position]["bid_close"]) - slippage_price
        stop_price = entry + stop_pips / multiplier
        target_price = entry - target_pips / multiplier
        for minute, bar in enumerate(future.itertuples(), 1):
            stop_hit = float(bar.ask_high) >= stop_price
            target_hit = float(bar.ask_low) <= target_price
            if stop_hit:
                return {"target_before_stop": 0, "realized_pips": -stop_pips, "exit_reason": "stop", "holding_minutes": minute}
            if target_hit:
                return {"target_before_stop": 1, "realized_pips": target_pips, "exit_reason": "target", "holding_minutes": minute}
        realized = (entry - float(future.iloc[-1]["ask_close"])) * multiplier
    return {
        "target_before_stop": int(realized > 0),
        "realized_pips": float(realized),
        "exit_reason": "timeout",
        "holding_minutes": MAX_HOLD_MINUTES,
    }


def label_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    labeled_parts = []
    for instrument, group in candidates.groupby("instrument", sort=False):
        candles = load_bam(instrument)
        rows = []
        for row in group.to_dict("records"):
            stop_pips = max(STOP_BASE_PIPS, float(row["trigger_abs_move_pips"]) * 0.45)
            for target_r in TARGET_R_MULTIPLES:
                candidate = dict(row)
                candidate["stop_pips"] = stop_pips
                candidate["target_r_multiple"] = target_r
                candidate["target_pips"] = stop_pips * target_r
                label = _path_label(pd.Series(candidate), candles)
                if label is not None:
                    candidate.update(label)
                    candidate["realized_r"] = candidate["realized_pips"] / stop_pips
                    rows.append(candidate)
        print(f"[event-labels] {instrument}: candidates={len(group)} labeled={len(rows)}", flush=True)
        if rows:
            labeled_parts.append(pd.DataFrame(rows))
    if not labeled_parts:
        return pd.DataFrame()
    return (
        pd.concat(labeled_parts, ignore_index=True)
        .sort_values(["time_utc", "instrument", "direction", "target_r_multiple"])
        .reset_index(drop=True)
    )


def make_model(name: str) -> Pipeline:
    transformer = ColumnTransformer([
        ("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
        ("numeric", "passthrough", NUMERIC_FEATURES),
    ])
    if name == "hist_gradient_boosting":
        classifier = HistGradientBoostingClassifier(
            learning_rate=0.04,
            max_iter=300,
            max_leaf_nodes=31,
            min_samples_leaf=40,
            l2_regularization=2.0,
            random_state=42,
        )
    elif name == "extra_trees":
        classifier = ExtraTreesClassifier(
            n_estimators=400,
            max_depth=16,
            min_samples_leaf=15,
            class_weight="balanced",
            n_jobs=-1,
            random_state=42,
        )
    elif name == "catboost":
        classifier = object.__new__(manager.ContinuousResearchEngine).estimator(
            {
                "model_type": "catboost",
                "parameters": {
                    "iterations": 240,
                    "learning_rate": 0.04,
                    "depth": 6,
                    "l2_leaf_reg": 4.0,
                    "random_strength": 0.75,
                },
            }
        )
    elif name == "ngboost":
        classifier = SklearnFittedClassifierAdapter(
            object.__new__(manager.ContinuousResearchEngine).estimator(
                {
                    "model_type": "ngboost",
                    "parameters": {
                        "n_estimators": 160,
                        "learning_rate": 0.025,
                        "minibatch_frac": 0.8,
                        "col_sample": 0.8,
                    },
                }
            )
        )
    else:
        raise ValueError(f"Unsupported event meta-model: {name}")
    return Pipeline([("features", transformer), ("classifier", classifier)])


def rolling_folds(df: pd.DataFrame) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    boundaries = [df["time_utc"].quantile(q) for q in [0.50, 0.625, 0.75, 0.875, 1.0]]
    folds = []
    for start, end in zip(boundaries[:-1], boundaries[1:]):
        train = df[df["time_utc"] < start - pd.Timedelta(minutes=PURGE_MINUTES)]
        test = df[(df["time_utc"] >= start) & (df["time_utc"] <= end)]
        if len(train) >= 5_000 and len(test) >= 1_000:
            folds.append((train, test))
    return folds


def classification_metrics(y: np.ndarray, probability: np.ndarray) -> Dict[str, float]:
    return {
        "auc": float(roc_auc_score(y, probability)),
        "average_precision": float(average_precision_score(y, probability)),
        "positive_rate": float(np.mean(y)),
        "brier": float(brier_score_loss(y, probability)),
        "log_loss": float(log_loss(y, probability)),
    }


def choose_one_action(frame: pd.DataFrame, probability: np.ndarray) -> pd.DataFrame:
    selected = frame.copy()
    selected["probability"] = probability
    return (
        selected.sort_values("probability", ascending=False)
        .drop_duplicates(["time_utc", "instrument"])
        .sort_values("time_utc")
    )


def trade_metrics(frame: pd.DataFrame) -> Dict[str, float]:
    pips = frame["realized_pips"].to_numpy(float)
    r_values = frame["realized_r"].to_numpy(float)
    gross_win = float(np.sum(pips[pips > 0]))
    gross_loss = float(abs(np.sum(pips[pips < 0])))
    equity = np.cumsum(r_values)
    drawdown = np.maximum.accumulate(np.insert(equity, 0, 0.0))[1:] - equity
    return {
        "trades": len(frame),
        "win_rate": float(np.mean(pips > 0)) if len(frame) else 0.0,
        "mean_net_pips": float(np.mean(pips)) if len(frame) else 0.0,
        "median_net_pips": float(np.median(pips)) if len(frame) else 0.0,
        "mean_r": float(np.mean(r_values)) if len(frame) else 0.0,
        "profit_factor": gross_win / gross_loss if gross_loss > 0 else 0.0,
        "max_drawdown_r": float(np.max(drawdown)) if len(drawdown) else 0.0,
    }


def threshold_table(frame: pd.DataFrame, probability: np.ndarray) -> List[Dict[str, Any]]:
    unique = choose_one_action(frame, probability)
    rows = []
    for threshold in np.arange(0.10, 0.91, 0.025):
        trades = unique[unique["probability"] >= threshold]
        rows.append({"threshold": float(threshold), **trade_metrics(trades)})
    return rows


def block_bootstrap_mean_r_lower(frame: pd.DataFrame, samples: int = 500) -> float:
    if frame.empty:
        return -math.inf
    data = frame.copy()
    data["day"] = data["time_utc"].dt.floor("D")
    days = list(data["day"].unique())
    if len(days) < 10:
        return -math.inf
    rng = np.random.default_rng(42)
    means = []
    by_day = {day: data.loc[data["day"] == day, "realized_r"].to_numpy(float) for day in days}
    for _ in range(samples):
        sampled_days = rng.choice(days, size=len(days), replace=True)
        sample = np.concatenate([by_day[day] for day in sampled_days])
        means.append(float(np.mean(sample)))
    return float(np.quantile(means, 0.025))


def evaluate_candidate(name: str, df: pd.DataFrame) -> Dict[str, Any]:
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    folds = []
    for number, (train, test) in enumerate(rolling_folds(df), 1):
        model = make_model(name)
        model.fit(train[features], train[TARGET].astype(int))
        probability = model.predict_proba(test[features])[:, 1]
        metrics = classification_metrics(test[TARGET].to_numpy(int), probability)
        metrics.update({"fold": number, "train_rows": len(train), "test_rows": len(test)})
        folds.append(metrics)
        print(f"[event-validation] {name} fold={number} auc={metrics['auc']:.4f} ap={metrics['average_precision']:.4f}", flush=True)
    return {
        "model": name,
        "folds": folds,
        "mean_auc": float(np.mean([fold["auc"] for fold in folds])),
        "minimum_auc": float(np.min([fold["auc"] for fold in folds])),
        "mean_average_precision": float(np.mean([fold["average_precision"] for fold in folds])),
        "mean_brier": float(np.mean([fold["brier"] for fold in folds])),
    }


def final_fit(name: str, df: pd.DataFrame) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    q70 = df["time_utc"].quantile(0.70)
    q82 = df["time_utc"].quantile(0.82)
    train = df[df["time_utc"] < q70 - pd.Timedelta(minutes=PURGE_MINUTES)]
    calibration = df[(df["time_utc"] >= q70) & (df["time_utc"] < q82 - pd.Timedelta(minutes=PURGE_MINUTES))]
    holdout = df[df["time_utc"] >= q82]
    base = make_model(name)
    base.fit(train[features], train[TARGET].astype(int))
    calibrated = CalibratedClassifierCV(FrozenEstimator(base), method="sigmoid")
    calibrated.fit(calibration[features], calibration[TARGET].astype(int))
    calibration_probability = calibrated.predict_proba(calibration[features])[:, 1]
    calibration_thresholds = threshold_table(calibration, calibration_probability)
    viable = [
        row for row in calibration_thresholds
        if row["trades"] >= 100
        and row["mean_r"] > 0
        and row["profit_factor"] >= 1.10
    ]
    selected_threshold = (
        max(viable, key=lambda row: row["mean_r"] * math.sqrt(row["trades"]))["threshold"]
        if viable else 0.70
    )
    holdout_probability = calibrated.predict_proba(holdout[features])[:, 1]
    holdout_classification = classification_metrics(holdout[TARGET].to_numpy(int), holdout_probability)
    holdout_thresholds = threshold_table(holdout, holdout_probability)
    holdout_unique = choose_one_action(holdout, holdout_probability)
    selected_trades = holdout_unique[holdout_unique["probability"] >= selected_threshold]
    selected_metrics = trade_metrics(selected_trades)
    selected_metrics["bootstrap_mean_r_lower_95"] = block_bootstrap_mean_r_lower(selected_trades)
    per_instrument = []
    for instrument, group in holdout.groupby("instrument"):
        if len(group) >= 100 and group[TARGET].nunique() > 1:
            positions = holdout.index.get_indexer(group.index)
            per_instrument.append({
                "instrument": instrument,
                "rows": len(group),
                "auc": float(roc_auc_score(group[TARGET], holdout_probability[positions])),
            })
    monthly = []
    month_values = holdout["time_utc"].dt.to_period("M").astype(str)
    for month, group in holdout.groupby(month_values):
        if len(group) >= 100 and group[TARGET].nunique() > 1:
            positions = holdout.index.get_indexer(group.index)
            monthly.append({
                "month": month,
                "rows": len(group),
                "auc": float(roc_auc_score(group[TARGET], holdout_probability[positions])),
            })
    median_instrument_auc = float(np.median([row["auc"] for row in per_instrument])) if per_instrument else 0.0
    minimum_month_auc = min([row["auc"] for row in monthly], default=0.0)
    gate = {
        "auc_at_least_0_65": holdout_classification["auc"] >= 0.65,
        "average_precision_lift_at_least_0_08": (
            holdout_classification["average_precision"] - holdout_classification["positive_rate"] >= 0.08
        ),
        "median_instrument_auc_at_least_0_58": median_instrument_auc >= 0.58,
        "minimum_month_auc_at_least_0_52": minimum_month_auc >= 0.52,
        "selected_trades_at_least_100": selected_metrics["trades"] >= 100,
        "selected_profit_factor_at_least_1_20": selected_metrics["profit_factor"] >= 1.20,
        "selected_mean_r_positive": selected_metrics["mean_r"] > 0,
        "selected_bootstrap_lower_mean_r_positive": selected_metrics["bootstrap_mean_r_lower_95"] > 0,
        "selected_max_drawdown_r_at_most_20": selected_metrics["max_drawdown_r"] <= 20,
    }
    gate["passed"] = all(gate.values())
    report = {
        "train_rows": len(train),
        "calibration_rows": len(calibration),
        "holdout_rows": len(holdout),
        "selected_threshold": selected_threshold,
        "calibration_thresholds": calibration_thresholds,
        "holdout_classification": holdout_classification,
        "holdout_thresholds": holdout_thresholds,
        "selected_trade_metrics": selected_metrics,
        "median_instrument_auc": median_instrument_auc,
        "minimum_month_auc": minimum_month_auc,
        "per_instrument": per_instrument,
        "monthly": monthly,
        "production_gate": gate,
    }
    artifact = {
        "model": calibrated,
        "features": features,
        "threshold": selected_threshold,
        "pipeline_version": PIPELINE_VERSION,
        "report": report,
        "trained_utc": manager.iso_utc(),
    }
    return artifact, report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reuse-labeled",
        action="store_true",
        help="Reuse the latest path-labeled event dataset and rerun feature/model validation.",
    )
    parser.add_argument(
        "--refresh-macro",
        action="store_true",
        help="Refresh cached FRED regime series before validation.",
    )
    parser.add_argument(
        "--refresh-cftc",
        action="store_true",
        help="Refresh cached CFTC currency positioning before validation.",
    )
    parser.add_argument(
        "--allow-revised-macro-research",
        action="store_true",
        help=(
            "Explicitly permit current/revised FRED history for exploratory "
            "research only. Resulting artifacts are marked non-promotable."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manager.ensure_dirs()
    source_path = latest_source_dataset()
    if args.reuse_labeled:
        dataset_path = latest_labeled_dataset()
        required = list(dict.fromkeys(
            ["time_utc", "instrument", "direction", "theme", "setup_family", "trade_style", TARGET,
             "realized_pips", "realized_r"] + BASE_NUMERIC_FEATURES + META_NUMERIC_FEATURES
        ))
        labeled = pd.read_csv(dataset_path, usecols=required)
        labeled["time_utc"] = pd.to_datetime(labeled["time_utc"], utc=True)
        print(f"[event-reuse] dataset={dataset_path.name} rows={len(labeled)}", flush=True)
    else:
        use_columns = list(dict.fromkeys(
            ["time_utc", "instrument", "direction", "theme"] + BASE_NUMERIC_FEATURES
        ))
        source = pd.read_csv(source_path, usecols=use_columns)
        candidates = select_candidates(source)
        print(f"[event-candidates] source_rows={len(source)} candidates={len(candidates)}", flush=True)
        labeled = label_candidates(candidates)
        if labeled.empty:
            raise SystemExit("No event candidates could be labeled.")
        dataset_path = manager.DIRS["training_sets"] / f"event_meta_training_set_{manager.utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
        labeled.to_csv(dataset_path, index=False)
    labeled = add_macro_features(
        labeled,
        refresh=args.refresh_macro,
        allow_revised_history_for_research=args.allow_revised_macro_research,
    )
    labeled = add_cftc_features(labeled, refresh=args.refresh_cftc)
    print(f"[event-macro] rows_after_lagged_macro_join={len(labeled)}", flush=True)
    validations = [
        evaluate_candidate("hist_gradient_boosting", labeled),
        evaluate_candidate("extra_trees", labeled),
    ]
    best = max(validations, key=lambda row: (row["mean_auc"], row["minimum_auc"], -row["mean_brier"]))
    artifact, final_report = final_fit(best["model"], labeled)
    artifact["research_only"] = True
    artifact["can_promote"] = False
    artifact["macro_data_contract"] = {
        "source": "FRED graph current/revised history",
        "point_in_time_safe": False,
        "allowed_use": "exploratory_research_only",
        "required_replacement": "ALFRED/FRED vintage observations with causal availability timestamps",
    }
    if isinstance(final_report.get("production_gate"), dict):
        final_report["production_gate"]["passed"] = False
        final_report["production_gate"]["blocked_by"] = list(dict.fromkeys(
            list(final_report["production_gate"].get("blocked_by") or [])
            + ["non_point_in_time_macro_history"]
        ))
    artifact_path = manager.DIRS["models"] / f"event_meta_model_{manager.utc_now().strftime('%Y%m%d_%H%M%S')}.joblib"
    joblib.dump(artifact, artifact_path)
    report = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_utc": manager.iso_utc(),
        "source_dataset": str(source_path),
        "dataset": str(dataset_path),
        "rows": len(labeled),
        "start": labeled["time_utc"].min().isoformat(),
        "end": labeled["time_utc"].max().isoformat(),
        "positive_rate": float(labeled[TARGET].mean()),
        "candidate_validation": validations,
        "selected_model": best["model"],
        "artifact": str(artifact_path),
        "final": final_report,
        "macro_data_contract": artifact["macro_data_contract"],
    }
    report_path = manager.DIRS["reports"] / "latest_event_meta_model_report.json"
    manager.save_json(report_path, report)
    print(json.dumps({
        "rows": len(labeled),
        "selected_model": best["model"],
        "artifact": str(artifact_path),
        "holdout": final_report["holdout_classification"],
        "selected_threshold": final_report["selected_threshold"],
        "selected_trade_metrics": final_report["selected_trade_metrics"],
        "production_gate": final_report["production_gate"],
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
