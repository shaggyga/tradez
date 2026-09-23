from __future__ import annotations

import itertools
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import roc_auc_score

from .common import ROOT, fallback_spread_pips, pair_meta, pip_size_for, tier_for, write_json
from .htf_validation_replay import (
    DEFAULT_FIXED_UNITS,
    INITIAL_EQUITY,
    MAX_OPEN_POSITIONS,
    _OpenPriceCache,
    _aligned_positions,
    _cost_stress,
    _exclusive_end,
    _fold_metrics,
    _max_drawdown_pct,
    _profit_factor,
    _quote_to_usd_rates,
    _rich_metrics,
    _timestamp_ns,
    _utc,
    _viability,
    allocate_fixed_exposure,
)
from .htf_arima_features import (
    ARIMA_FEATURE_COLUMNS,
    add_causal_h1_arima_features,
)
from .replay_metrics import rank_ic, write_report_pair


H1_MINUTES = 60
HORIZON_MINUTES = 120
EARLY_MINUTES = 30
MAX_ALIGNMENT_DELAY_MINUTES = 10
LABEL_EMBARGO_MINUTES = H1_MINUTES + HORIZON_MINUTES
CURVE_BEST_WEIGHT = 0.55
CURVE_ENDPOINT_WEIGHT = 0.25
CURVE_EARLY_WEIGHT = 0.20
CURVE_ADVERSE_PENALTY = 0.35
SCORE_QUANTILES = (0.90, 0.95, 0.98, 0.99)
NEW_TRADE_QUOTAS = (1, 2, 3)
SOURCE_DATASET = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "technical_spike_research_1h_step1.parquet"
)
EXPERIMENT_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "experiments"
    / "exp_20260702_022306_e09b53ec8d.json"
)
UNSAFE_GLOBAL_FEATURES = {
    "instrument_volatility_percentile",
    "instrument_median_abs_60_pips",
    "instrument_q995_abs_60_pips",
    "instrument_q995_move_to_spread",
    "is_volatile_pair",
    "is_volatile_or_exotic_pair",
    "is_non_usd_volatile_pair",
}
MOTION_FEATURE_TOKENS = (
    "momentum",
    "atr",
    "range",
    "donchian",
    "trend",
    "ema",
    "sma",
    "macd",
    "rsi",
    "compression",
    "spread",
    "volume",
    "strength",
    "acceleration",
    "liquidity",
    "session",
    "hour_",
    "weekday",
)


def _experiment_payload() -> dict[str, Any]:
    return json.loads(EXPERIMENT_PATH.read_text(encoding="utf-8"))


def corrected_feature_families() -> dict[str, list[str]]:
    features = [str(value) for value in _experiment_payload()["result"]["features"]]
    safe = [feature for feature in features if feature not in UNSAFE_GLOBAL_FEATURES]
    motion = [
        feature
        for feature in safe
        if any(token in feature.lower() for token in MOTION_FEATURE_TOKENS)
    ]
    arima = list(ARIMA_FEATURE_COLUMNS)
    return {
        "safe_full_220": safe,
        "motion_cost_117": motion,
        "arima_causal": arima,
        "safe_full_220_plus_arima": [*safe, *arima],
    }


def _selected_pairs(
    cfg: dict[str, Any],
    tier: str,
    explicit_pairs: list[str] | None,
) -> list[str]:
    available = {
        path.name.removesuffix("_M1.csv")
        for path in (ROOT / cfg["candles_dir"]).glob("*_M1.csv")
    }
    if explicit_pairs:
        pairs = [pair for pair in explicit_pairs if pair in available]
    elif tier == "tier1":
        pairs = [pair for pair in cfg["tier1_pairs"] if pair in available]
    elif tier == "tier2":
        pairs = [
            pair
            for pair in cfg["tier1_pairs"] + cfg["tier2_pairs"]
            if pair in available
        ]
    else:
        raise ValueError("Corrected HTF rebuild requires tier1 or tier2; all-tier is not automatic")
    if not pairs:
        raise ValueError("No pairs selected for corrected H1 rebuild")
    return sorted(set(pairs))


def _load_h1_features(
    cfg: dict[str, Any],
    tier: str,
    explicit_pairs: list[str] | None,
    max_rows_per_pair: int | None,
) -> tuple[pd.DataFrame, dict[str, list[str]], dict[str, Any]]:
    families = corrected_feature_families()
    safe_features = families["safe_full_220"]
    pairs = _selected_pairs(cfg, tier, explicit_pairs)
    needed = ["time_utc", "instrument", *safe_features]
    frame = pd.read_parquet(SOURCE_DATASET, columns=needed)
    frame = frame[frame["instrument"].astype(str).isin(pairs)].copy()
    frame["raw_bar_time_utc"] = pd.to_datetime(frame.pop("time_utc"), utc=True, errors="coerce")
    frame["decision_time_utc"] = frame["raw_bar_time_utc"] + pd.Timedelta(minutes=H1_MINUTES)
    frame = frame.dropna(subset=["raw_bar_time_utc", "decision_time_utc", "instrument"])
    for column in safe_features:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float32")
    frame = frame.replace([np.inf, -np.inf], np.nan)
    rows_before_limit = int(len(frame))
    frame, arima_audit = add_causal_h1_arima_features(
        frame,
        ROOT / cfg["candles_dir"],
    )
    if max_rows_per_pair:
        frame = (
            frame.sort_values("decision_time_utc")
            .groupby("instrument", group_keys=False)
            .tail(int(max_rows_per_pair))
        )
    manifest = {
        "source_dataset": str(SOURCE_DATASET),
        "source_rows_selected": int(len(frame)),
        "source_rows_before_optional_limit": rows_before_limit,
        "pairs": pairs,
        "pair_count": int(len(pairs)),
        "source_start": frame["raw_bar_time_utc"].min(),
        "source_end": frame["raw_bar_time_utc"].max(),
        "decision_start": frame["decision_time_utc"].min(),
        "decision_end": frame["decision_time_utc"].max(),
        "original_feature_count": 227,
        "safe_feature_count": int(len(safe_features)),
        "motion_feature_count": int(len(families["motion_cost_117"])),
        "arima_feature_count": int(len(families["arima_causal"])),
        "safe_plus_arima_feature_count": int(
            len(families["safe_full_220_plus_arima"])
        ),
        "arima_feature_audit": arima_audit,
        "excluded_global_features": sorted(UNSAFE_GLOBAL_FEATURES),
    }
    return frame.sort_values(["decision_time_utc", "instrument"]), families, manifest


def _numeric_candle_column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def _pair_corrected_labels(
    feature_rows: pd.DataFrame,
    instrument: str,
    cfg: dict[str, Any],
    fixed_units: int,
    price_cache: _OpenPriceCache,
) -> pd.DataFrame:
    path = ROOT / cfg["candles_dir"] / f"{instrument}_M1.csv"
    candle_columns = {
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "spread_pips",
    }
    candles = pd.read_csv(path, usecols=lambda column: column in candle_columns)
    candles["datetime"] = pd.to_datetime(candles["datetime"], utc=True, errors="coerce")
    candles = candles[
        (candles["datetime"] >= price_cache.start)
        & (candles["datetime"] <= price_cache.end)
    ].copy()
    mid_open = _numeric_candle_column(candles, "open").fillna(_numeric_candle_column(candles, "close"))
    mid_high = _numeric_candle_column(candles, "high").fillna(mid_open)
    mid_low = _numeric_candle_column(candles, "low").fillna(mid_open)
    observed_spread = _numeric_candle_column(candles, "spread_pips")
    pip = pip_size_for(instrument)
    derived_spread = (
        _numeric_candle_column(candles, "ask_open")
        - _numeric_candle_column(candles, "bid_open")
    ).abs() / pip
    observed_spread = observed_spread.fillna(derived_spread)
    feature_spread = pd.to_numeric(feature_rows["spread_pips"], errors="coerce")
    default_spread = float(feature_spread.median()) if feature_spread.notna().any() else fallback_spread_pips(pair_meta(instrument, cfg), cfg)
    observed_spread = observed_spread.fillna(default_spread).clip(lower=0.0)
    half = observed_spread * pip / 2.0

    candles["mid_open"] = mid_open
    candles["mid_high"] = mid_high
    candles["mid_low"] = mid_low
    candles["bid_open_eff"] = _numeric_candle_column(candles, "bid_open").fillna(mid_open - half)
    candles["ask_open_eff"] = _numeric_candle_column(candles, "ask_open").fillna(mid_open + half)
    candles["bid_high_eff"] = _numeric_candle_column(candles, "bid_high").fillna(mid_high - half)
    candles["bid_low_eff"] = _numeric_candle_column(candles, "bid_low").fillna(mid_low - half)
    candles["ask_high_eff"] = _numeric_candle_column(candles, "ask_high").fillna(mid_high + half)
    candles["ask_low_eff"] = _numeric_candle_column(candles, "ask_low").fillna(mid_low + half)
    candles = candles.dropna(subset=["datetime", "mid_open", "mid_high", "mid_low"])
    candles = candles.sort_values("datetime").drop_duplicates("datetime", keep="last")
    if candles.empty:
        return pd.DataFrame({"_row_id": feature_rows["_row_id"], "valid_corrected_label": False})

    times = _timestamp_ns(candles["datetime"])
    decision_ns = _timestamp_ns(feature_rows["decision_time_utc"])
    entry_pos, entry_valid = _aligned_positions(times, decision_ns)
    entry_ns = times[entry_pos]
    early_targets = entry_ns + EARLY_MINUTES * pd.Timedelta(minutes=1).value
    exit_targets = entry_ns + HORIZON_MINUTES * pd.Timedelta(minutes=1).value
    early_pos, early_valid = _aligned_positions(times, early_targets)
    exit_pos, exit_valid = _aligned_positions(times, exit_targets)
    entry_delay_ns = entry_ns - decision_ns
    early_delay_ns = times[early_pos] - early_targets
    exit_delay_ns = times[exit_pos] - exit_targets
    max_delay_ns = MAX_ALIGNMENT_DELAY_MINUTES * pd.Timedelta(minutes=1).value
    path_rows = exit_pos - entry_pos
    valid = (
        entry_valid
        & early_valid
        & exit_valid
        & (entry_delay_ns >= 0)
        & (entry_delay_ns <= max_delay_ns)
        & (early_delay_ns >= 0)
        & (early_delay_ns <= max_delay_ns)
        & (exit_delay_ns >= 0)
        & (exit_delay_ns <= max_delay_ns)
        & (path_rows >= 90)
    )

    arrays = {
        column: candles[column].to_numpy(dtype=float)
        for column in [
            "mid_open",
            "mid_high",
            "mid_low",
            "bid_open_eff",
            "ask_open_eff",
            "bid_high_eff",
            "bid_low_eff",
            "ask_high_eff",
            "ask_low_eff",
        ]
    }
    count = len(feature_rows)
    long_gross = np.full(count, np.nan)
    short_gross = np.full(count, np.nan)
    long_spread_drag = np.full(count, np.nan)
    short_spread_drag = np.full(count, np.nan)
    long_endpoint = np.full(count, np.nan)
    short_endpoint = np.full(count, np.nan)
    long_mfe = np.full(count, np.nan)
    short_mfe = np.full(count, np.nan)
    long_mae = np.full(count, np.nan)
    short_mae = np.full(count, np.nan)
    long_curve = np.full(count, np.nan)
    short_curve = np.full(count, np.nan)
    path_range = np.full(count, np.nan)
    slippage = float(cfg["costs"][f"slippage_pips_{tier_for(instrument, cfg)}"])

    for offset in np.flatnonzero(valid):
        entry = int(entry_pos[offset])
        early = int(early_pos[offset])
        exit_ = int(exit_pos[offset])
        path_slice = slice(entry, exit_)
        entry_mid = arrays["mid_open"][entry]
        exit_mid = arrays["mid_open"][exit_]
        entry_bid = arrays["bid_open_eff"][entry]
        entry_ask = arrays["ask_open_eff"][entry]
        exit_bid = arrays["bid_open_eff"][exit_]
        exit_ask = arrays["ask_open_eff"][exit_]
        early_bid = arrays["bid_open_eff"][early]
        early_ask = arrays["ask_open_eff"][early]

        gross_long = (exit_mid - entry_mid) / pip
        gross_short = -gross_long
        endpoint_long_before_slip = (exit_bid - entry_ask) / pip
        endpoint_short_before_slip = (entry_bid - exit_ask) / pip
        endpoint_long = endpoint_long_before_slip - slippage
        endpoint_short = endpoint_short_before_slip - slippage
        early_long = (early_bid - entry_ask) / pip - slippage
        early_short = (entry_bid - early_ask) / pip - slippage
        best_long = (np.nanmax(arrays["bid_high_eff"][path_slice]) - entry_ask) / pip - slippage
        best_short = (entry_bid - np.nanmin(arrays["ask_low_eff"][path_slice])) / pip - slippage
        adverse_long = (np.nanmin(arrays["bid_low_eff"][path_slice]) - entry_ask) / pip
        adverse_short = (entry_bid - np.nanmax(arrays["ask_high_eff"][path_slice])) / pip
        curve_long = (
            CURVE_BEST_WEIGHT * best_long
            + CURVE_ENDPOINT_WEIGHT * endpoint_long
            + CURVE_EARLY_WEIGHT * early_long
            - CURVE_ADVERSE_PENALTY * max(0.0, -adverse_long)
        )
        curve_short = (
            CURVE_BEST_WEIGHT * best_short
            + CURVE_ENDPOINT_WEIGHT * endpoint_short
            + CURVE_EARLY_WEIGHT * early_short
            - CURVE_ADVERSE_PENALTY * max(0.0, -adverse_short)
        )

        long_gross[offset] = gross_long
        short_gross[offset] = gross_short
        long_spread_drag[offset] = max(0.0, gross_long - endpoint_long_before_slip)
        short_spread_drag[offset] = max(0.0, gross_short - endpoint_short_before_slip)
        long_endpoint[offset] = endpoint_long
        short_endpoint[offset] = endpoint_short
        long_mfe[offset] = best_long
        short_mfe[offset] = best_short
        long_mae[offset] = adverse_long
        short_mae[offset] = adverse_short
        long_curve[offset] = curve_long
        short_curve[offset] = curve_short
        path_range[offset] = (
            np.nanmax(arrays["mid_high"][path_slice])
            - np.nanmin(arrays["mid_low"][path_slice])
        ) / pip

    exit_ns = times[exit_pos]
    exit_times = pd.to_datetime(exit_ns, unit="ns", utc=True)
    exit_mid = arrays["mid_open"][exit_pos]
    quote_rates = _quote_to_usd_rates(
        instrument,
        pd.Series(exit_times),
        exit_mid,
        price_cache,
    )
    pip_value = pip * quote_rates
    valid &= np.isfinite(pip_value) & (pip_value > 0.0)
    momentum_positive = pd.to_numeric(
        feature_rows["momentum_30_atr"], errors="coerce"
    ).fillna(0.0).to_numpy() >= 0.0
    reversal_long = ~momentum_positive

    result = pd.DataFrame({
        "_row_id": feature_rows["_row_id"].to_numpy(),
        "valid_corrected_label": valid,
        "entry_time_utc": pd.to_datetime(entry_ns, unit="ns", utc=True),
        "label_end_time_utc": exit_times,
        "entry_delay_minutes": entry_delay_ns / pd.Timedelta(minutes=1).value,
        "early_alignment_delay_minutes": early_delay_ns / pd.Timedelta(minutes=1).value,
        "endpoint_alignment_delay_minutes": exit_delay_ns / pd.Timedelta(minutes=1).value,
        "path_row_count": path_rows,
        "pip_value_usd_per_unit": pip_value,
        "fixed_units": int(fixed_units),
        "slippage_pips_round_trip": slippage,
        "path_range_pips_120m": path_range,
        "long_gross_endpoint_pips": long_gross,
        "short_gross_endpoint_pips": short_gross,
        "long_spread_drag_pips": long_spread_drag,
        "short_spread_drag_pips": short_spread_drag,
        "long_endpoint_net_pips": long_endpoint,
        "short_endpoint_net_pips": short_endpoint,
        "long_mfe_net_pips": long_mfe,
        "short_mfe_net_pips": short_mfe,
        "long_mae_pips": long_mae,
        "short_mae_pips": short_mae,
        "long_curve_net_pips": long_curve,
        "short_curve_net_pips": short_curve,
        "reversal_direction": np.where(reversal_long, "LONG", "SHORT"),
        "continuation_direction": np.where(reversal_long, "SHORT", "LONG"),
        "reversal_gross_endpoint_pips": np.where(reversal_long, long_gross, short_gross),
        "continuation_gross_endpoint_pips": np.where(reversal_long, short_gross, long_gross),
        "reversal_spread_drag_pips": np.where(reversal_long, long_spread_drag, short_spread_drag),
        "continuation_spread_drag_pips": np.where(reversal_long, short_spread_drag, long_spread_drag),
        "reversal_endpoint_net_pips": np.where(reversal_long, long_endpoint, short_endpoint),
        "continuation_endpoint_net_pips": np.where(reversal_long, short_endpoint, long_endpoint),
        "reversal_curve_net_pips": np.where(reversal_long, long_curve, short_curve),
        "continuation_curve_net_pips": np.where(reversal_long, short_curve, long_curve),
        "reversal_mfe_net_pips": np.where(reversal_long, long_mfe, short_mfe),
        "continuation_mfe_net_pips": np.where(reversal_long, short_mfe, long_mfe),
    })
    for direction in ("reversal", "continuation"):
        result[f"{direction}_endpoint_pnl_usd"] = (
            result[f"{direction}_endpoint_net_pips"] * pip_value * fixed_units
        )
        result[f"{direction}_curve_profit"] = (
            result[f"{direction}_curve_net_pips"] > 0.0
        ).astype(float)
    numeric = [
        column
        for column in result.columns
        if column
        not in {
            "_row_id",
            "valid_corrected_label",
            "entry_time_utc",
            "label_end_time_utc",
            "reversal_direction",
            "continuation_direction",
        }
    ]
    result.loc[~result["valid_corrected_label"], numeric] = np.nan
    return result


def build_corrected_h1_labels(
    features: pd.DataFrame,
    cfg: dict[str, Any],
    fixed_units: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = features.reset_index(drop=True).copy()
    work["_row_id"] = np.arange(len(work), dtype=int)
    cache = _OpenPriceCache(
        ROOT / cfg["candles_dir"],
        work["decision_time_utc"].min(),
        work["decision_time_utc"].max() + pd.Timedelta(minutes=HORIZON_MINUTES),
    )
    labels = []
    for instrument, pair_frame in work.groupby("instrument", sort=True):
        labels.append(
            _pair_corrected_labels(pair_frame, str(instrument), cfg, fixed_units, cache)
        )
    label_frame = pd.concat(labels, ignore_index=True)
    merged = work.merge(label_frame, on="_row_id", how="left")
    valid = merged["valid_corrected_label"].fillna(False)
    corrected = merged[valid].copy()
    audit = {
        "source_rows": int(len(merged)),
        "valid_label_rows": int(len(corrected)),
        "invalid_rows": int((~valid).sum()),
        "valid_fraction": float(valid.mean()),
        "entry_contract": "first M1 open at or after completed H1 candle",
        "early_mark_minutes": EARLY_MINUTES,
        "path_horizon_minutes": HORIZON_MINUTES,
        "maximum_alignment_delay_minutes": MAX_ALIGNMENT_DELAY_MINUTES,
        "minimum_path_rows": 90,
        "entry_delay_max_minutes": float(corrected["entry_delay_minutes"].max()),
        "entry_delay_mean_minutes": float(corrected["entry_delay_minutes"].mean()),
        "early_alignment_delay_max_minutes": float(
            corrected["early_alignment_delay_minutes"].max()
        ),
        "endpoint_alignment_delay_max_minutes": float(
            corrected["endpoint_alignment_delay_minutes"].max()
        ),
        "endpoint_alignment_delay_mean_minutes": float(
            corrected["endpoint_alignment_delay_minutes"].mean()
        ),
        "all_alignment_delays_within_limit": bool(
            corrected[
                [
                    "entry_delay_minutes",
                    "early_alignment_delay_minutes",
                    "endpoint_alignment_delay_minutes",
                ]
            ].max().max()
            <= MAX_ALIGNMENT_DELAY_MINUTES
        ),
        "path_rows_min": int(corrected["path_row_count"].min()),
        "path_rows_median": float(corrected["path_row_count"].median()),
        "path_rows_max": int(corrected["path_row_count"].max()),
        "label_end_strictly_after_decision": bool(
            (corrected["label_end_time_utc"] > corrected["decision_time_utc"]).all()
        ),
        "old_curve_targets_used": False,
        "old_realized_outcomes_used": False,
        "fixed_units": int(fixed_units),
        "compounding": False,
    }
    return corrected, audit


def corrected_candidate_specs() -> list[dict[str, Any]]:
    specs = []
    for feature_family, direction, objective in itertools.product(
        (
            "safe_full_220",
            "motion_cost_117",
            "arima_causal",
            "safe_full_220_plus_arima",
        ),
        ("reversal", "continuation"),
        ("curve_classifier", "curve_regressor", "endpoint_regressor"),
    ):
        specs.append({
            "name": f"HGB|{feature_family}|{direction}|{objective}",
            "model_family": "HistGradientBoosting",
            "feature_family": feature_family,
            "direction": direction,
            "objective": objective,
        })
    return specs


def _estimator(spec: dict[str, Any]) -> Any:
    common = {
        "learning_rate": 0.05,
        "max_iter": 120,
        "max_leaf_nodes": 31,
        "min_samples_leaf": 40,
        "l2_regularization": 0.05,
        "early_stopping": True,
        "validation_fraction": 0.10,
        "n_iter_no_change": 10,
        "random_state": 42,
    }
    if spec["objective"] == "curve_classifier":
        return HistGradientBoostingClassifier(**common)
    return HistGradientBoostingRegressor(loss="squared_error", **common)


def _target_column(spec: dict[str, Any]) -> str:
    direction = spec["direction"]
    if spec["objective"] == "curve_classifier":
        return f"{direction}_curve_profit"
    if spec["objective"] == "curve_regressor":
        return f"{direction}_curve_net_pips"
    return f"{direction}_endpoint_net_pips"


def _fit_and_predict(
    train: pd.DataFrame,
    test: pd.DataFrame,
    spec: dict[str, Any],
    feature_families: dict[str, list[str]],
    max_train_rows: int,
) -> tuple[np.ndarray, dict[str, Any], Any]:
    features = feature_families[spec["feature_family"]]
    target_column = _target_column(spec)
    train = train.dropna(subset=[target_column]).sort_values("decision_time_utc")
    if len(train) > max_train_rows:
        train = train.tail(max_train_rows)
    if len(train) < 5000 or test.empty:
        raise ValueError(f"insufficient rows train={len(train)} test={len(test)}")
    x_train = train[features].to_numpy(dtype=np.float32, copy=False)
    x_test = test[features].to_numpy(dtype=np.float32, copy=False)
    y_train = pd.to_numeric(train[target_column], errors="coerce").to_numpy(dtype=float)
    model = _estimator(spec)
    if spec["objective"] == "curve_classifier":
        if np.unique(y_train).size < 2:
            raise ValueError("classifier target has one class")
        model.fit(x_train, y_train.astype(int))
        prediction = model.predict_proba(x_test)[:, 1]
    else:
        lower, upper = np.nanquantile(y_train, [0.01, 0.99])
        model.fit(x_train, np.clip(y_train, lower, upper))
        prediction = model.predict(x_test)
    diagnostics = {
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "feature_count": int(len(features)),
        "target_column": target_column,
        "train_target_mean": float(np.nanmean(y_train)),
        "model_iterations": int(getattr(model, "n_iter_", 0)),
    }
    if spec["objective"] == "curve_classifier":
        y_test = pd.to_numeric(test[target_column], errors="coerce")
        if y_test.nunique() >= 2:
            diagnostics["test_auc"] = float(roc_auc_score(y_test, prediction))
    return prediction, diagnostics, model


def _prediction_surface(
    rows: pd.DataFrame,
    prediction: np.ndarray,
    spec: dict[str, Any],
    fold: str,
) -> pd.DataFrame:
    direction = spec["direction"]
    keep = [
        "_row_id",
        "instrument",
        "raw_bar_time_utc",
        "decision_time_utc",
        "entry_time_utc",
        "label_end_time_utc",
        "momentum_30_atr",
        "spread_pips",
        "atr240_pips",
        "pip_value_usd_per_unit",
        "fixed_units",
        "slippage_pips_round_trip",
        "path_range_pips_120m",
        "long_endpoint_net_pips",
        "short_endpoint_net_pips",
        "long_gross_endpoint_pips",
        "short_gross_endpoint_pips",
        "long_spread_drag_pips",
        "short_spread_drag_pips",
        "long_curve_net_pips",
        "short_curve_net_pips",
    ]
    surface = rows[keep].copy()
    surface["exit_time_utc"] = surface["label_end_time_utc"]
    surface["candidate"] = spec["name"]
    surface["feature_family"] = spec["feature_family"]
    surface["direction_family"] = direction
    surface["objective"] = spec["objective"]
    surface["fold"] = fold
    surface["timeframe"] = "h1"
    surface["selected_score"] = np.asarray(prediction, dtype=float)
    surface["selected_direction"] = rows[f"{direction}_direction"].astype(str).to_numpy()
    use_long = surface["selected_direction"].eq("LONG").to_numpy()
    surface["selected_gross_pips"] = np.where(
        use_long,
        surface["long_gross_endpoint_pips"],
        surface["short_gross_endpoint_pips"],
    )
    surface["selected_spread_drag_pips"] = np.where(
        use_long,
        surface["long_spread_drag_pips"],
        surface["short_spread_drag_pips"],
    )
    surface["selected_net_pips"] = np.where(
        use_long,
        surface["long_endpoint_net_pips"],
        surface["short_endpoint_net_pips"],
    )
    surface["selected_curve_net_pips"] = np.where(
        use_long,
        surface["long_curve_net_pips"],
        surface["short_curve_net_pips"],
    )
    surface["selected_pnl_usd"] = (
        surface["selected_net_pips"]
        * surface["pip_value_usd_per_unit"]
        * surface["fixed_units"]
    )
    return surface.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["selected_score", "selected_net_pips", "selected_pnl_usd"]
    )


def _development_quarters() -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    return [
        ("2025Q1", pd.Timestamp("2025-01-01T00:00:00Z"), pd.Timestamp("2025-04-01T00:00:00Z")),
        ("2025Q2", pd.Timestamp("2025-04-01T00:00:00Z"), pd.Timestamp("2025-07-01T00:00:00Z")),
        ("2025Q3", pd.Timestamp("2025-07-01T00:00:00Z"), pd.Timestamp("2025-10-01T00:00:00Z")),
        ("2025Q4", pd.Timestamp("2025-10-01T00:00:00Z"), pd.Timestamp("2026-01-01T00:00:00Z")),
    ]


def build_development_oos_predictions(
    dataset: pd.DataFrame,
    feature_families: dict[str, list[str]],
    max_train_rows: int,
) -> tuple[dict[str, pd.DataFrame], list[dict[str, Any]], list[dict[str, Any]]]:
    surfaces: dict[str, list[pd.DataFrame]] = {
        spec["name"]: [] for spec in corrected_candidate_specs()
    }
    fold_rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for fold_name, test_start, test_end in _development_quarters():
        train = dataset[dataset["label_end_time_utc"] < test_start].copy()
        test = dataset[
            (dataset["decision_time_utc"] >= test_start)
            & (dataset["decision_time_utc"] < test_end)
        ].copy()
        fold_rows.append({
            "fold": fold_name,
            "train_start": train["decision_time_utc"].min(),
            "train_end": train["decision_time_utc"].max(),
            "test_start": test_start,
            "test_end": test_end,
            "train_rows_available": int(len(train)),
            "test_rows": int(len(test)),
            "label_purge_contract": "train label_end_time strictly before test start",
        })
        for spec in corrected_candidate_specs():
            try:
                prediction, diagnostics, _model = _fit_and_predict(
                    train,
                    test,
                    spec,
                    feature_families,
                    max_train_rows,
                )
                surfaces[spec["name"]].append(
                    _prediction_surface(test, prediction, spec, fold_name)
                )
                fold_rows.append({
                    "fold": fold_name,
                    "candidate": spec["name"],
                    **diagnostics,
                })
            except Exception as exc:
                skipped.append({
                    "fold": fold_name,
                    "candidate": spec["name"],
                    "reason": f"{type(exc).__name__}: {exc}",
                })
    combined = {
        name: pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame()
        for name, parts in surfaces.items()
    }
    return combined, fold_rows, skipped


def _threshold_for(surface: pd.DataFrame, spec: dict[str, Any], quantile: float) -> float:
    threshold = float(surface["selected_score"].quantile(float(quantile)))
    if spec["objective"] != "curve_classifier":
        threshold = max(0.0, threshold)
    return threshold


def _apply_surface_policy(
    surface: pd.DataFrame,
    threshold: float,
    quota: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    eligible = surface[surface["selected_score"] >= float(threshold)].copy()
    selected, attribution = allocate_fixed_exposure(
        eligible,
        max_new_per_timestamp=int(quota),
        max_open_positions=MAX_OPEN_POSITIONS,
    )
    return eligible, selected, {
        "surface_rows": int(len(surface)),
        "rejected_by_score_threshold": int(len(surface) - len(eligible)),
        **attribution,
    }


def _selection_metrics(selected: pd.DataFrame, eligible: pd.DataFrame) -> dict[str, Any]:
    if selected.empty:
        return {
            "pnl_usd": 0.0,
            "trades": 0,
            "mean_net_pips": None,
            "profit_factor": None,
            "rank_ic": rank_ic(eligible, "selected_score", "selected_pnl_usd"),
            "max_drawdown_pct": 0.0,
            "top_pair_trade_share": None,
        }
    realized = selected.sort_values(["label_end_time_utc", "entry_time_utc"])
    pnl = realized["selected_pnl_usd"]
    pair_counts = realized["instrument"].value_counts()
    return {
        "pnl_usd": float(pnl.sum()),
        "trades": int(len(realized)),
        "mean_net_pips": float(realized["selected_net_pips"].mean()),
        "profit_factor": _profit_factor(pnl),
        "rank_ic": rank_ic(eligible, "selected_score", "selected_pnl_usd"),
        "max_drawdown_pct": _max_drawdown_pct(pnl),
        "top_pair_trade_share": float(pair_counts.iloc[0] / len(realized)),
    }


def _corrected_viability(
    metrics: dict[str, Any],
    folds: list[dict[str, Any]],
) -> dict[str, Any]:
    base = _viability(metrics, folds)
    trades_ok = int(metrics.get("trades", 0)) >= 100
    pnl_ok = float(metrics.get("pnl_usd", 0.0)) > 0.0
    mean_ok = float(metrics.get("mean_net_pips") or -math.inf) > 0.0
    folds_ok = int(base["positive_folds"]) >= int(base["required_positive_folds"])
    drawdown_ok = float(metrics.get("max_drawdown_pct", math.inf)) <= 35.0
    concentration = metrics.get("top_pair_trade_share")
    concentration_ok = concentration is not None and float(concentration) <= 0.40
    checks = {
        "minimum_100_trades": bool(trades_ok),
        "positive_development_pnl": bool(pnl_ok),
        "positive_mean_net_pips": bool(mean_ok),
        "positive_fold_majority": bool(folds_ok),
        "max_drawdown_at_most_35pct": bool(drawdown_ok),
        "top_pair_trade_share_at_most_40pct": bool(concentration_ok),
    }
    base.update({
        **checks,
        "passed_check_count": int(sum(checks.values())),
        "total_check_count": int(len(checks)),
        "viable_before_risk_concentration_checks": bool(base["viable"]),
        "viable": bool(base["viable"] and drawdown_ok and concentration_ok),
    })
    return base


def _selection_rank_key(report: dict[str, Any]) -> tuple[Any, ...]:
    viability = report["viability"]
    return (
        bool(viability["viable"]),
        int(viability.get("passed_check_count", 0)),
        int(viability["positive_folds"]),
        float(report["development"]["pnl_usd"]),
        float(report["development"].get("mean_net_pips") or -math.inf),
    )


def select_development_candidate(
    surfaces: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    specs = {spec["name"]: spec for spec in corrected_candidate_specs()}
    reports = []
    for candidate, surface in surfaces.items():
        if surface.empty:
            continue
        spec = specs[candidate]
        for quantile, quota in itertools.product(SCORE_QUANTILES, NEW_TRADE_QUOTAS):
            threshold = _threshold_for(surface, spec, quantile)
            eligible, selected, attribution = _apply_surface_policy(
                surface, threshold, quota
            )
            metrics = _selection_metrics(selected, eligible)
            folds = _fold_metrics(selected)
            viability = _corrected_viability(metrics, folds)
            reports.append({
                "candidate": candidate,
                "spec": spec,
                "score_quantile": float(quantile),
                "frozen_score_threshold": float(threshold),
                "max_new_per_timestamp": int(quota),
                "development": metrics,
                "folds": folds,
                "viability": viability,
                "gate_attribution": attribution,
            })
    viable = [report for report in reports if report["viability"]["viable"]]
    ranked = viable or reports
    best = max(ranked, key=_selection_rank_key)
    return {
        "best": best,
        "reports": reports,
        "declared_configs": int(len(reports)),
        "viable_configs": int(len(viable)),
    }


def _simple_surface(rows: pd.DataFrame, direction: str) -> pd.DataFrame:
    spec = {
        "name": f"simple_abs_momentum|{direction}",
        "feature_family": "simple_abs_momentum",
        "direction": direction,
        "objective": "simple_abs_momentum",
    }
    score = pd.to_numeric(rows["momentum_30_atr"], errors="coerce").abs().to_numpy()
    return _prediction_surface(rows, score, spec, "development")


def select_simple_baseline(development: pd.DataFrame) -> dict[str, Any]:
    reports = []
    surfaces = {
        direction: _simple_surface(development, direction)
        for direction in ("reversal", "continuation")
    }
    for direction, quantile, quota in itertools.product(
        ("reversal", "continuation"), SCORE_QUANTILES, NEW_TRADE_QUOTAS
    ):
        surface = surfaces[direction]
        threshold = float(surface["selected_score"].quantile(float(quantile)))
        eligible, selected, attribution = _apply_surface_policy(surface, threshold, quota)
        metrics = _selection_metrics(selected, eligible)
        folds = _fold_metrics(selected)
        reports.append({
            "candidate": f"simple_abs_momentum|{direction}",
            "direction": direction,
            "score_quantile": float(quantile),
            "frozen_score_threshold": threshold,
            "max_new_per_timestamp": int(quota),
            "development": metrics,
            "folds": folds,
            "viability": _corrected_viability(metrics, folds),
            "gate_attribution": attribution,
        })
    viable = [report for report in reports if report["viability"]["viable"]]
    ranked = viable or reports
    best = max(ranked, key=_selection_rank_key)
    return {
        "best": best,
        "reports": reports,
        "surfaces": surfaces,
        "declared_configs": int(len(reports)),
        "viable_configs": int(len(viable)),
    }


def _random_direction_baseline(trades: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if trades.empty:
        return trades.copy(), _rich_metrics(trades, trades)
    out = trades.copy()
    choose_long = out["_row_id"].astype(int).map(
        lambda value: (value * 1103515245 + 12345) % 2 == 0
    ).to_numpy()
    out["selected_direction"] = np.where(choose_long, "LONG", "SHORT")
    out["selected_gross_pips"] = np.where(
        choose_long, out["long_gross_endpoint_pips"], out["short_gross_endpoint_pips"]
    )
    out["selected_spread_drag_pips"] = np.where(
        choose_long, out["long_spread_drag_pips"], out["short_spread_drag_pips"]
    )
    out["selected_net_pips"] = np.where(
        choose_long, out["long_endpoint_net_pips"], out["short_endpoint_net_pips"]
    )
    out["selected_pnl_usd"] = (
        out["selected_net_pips"] * out["pip_value_usd_per_unit"] * out["fixed_units"]
    )
    return out, _rich_metrics(out, out)


def _fit_frozen_candidate(
    dataset: pd.DataFrame,
    spec: dict[str, Any],
    feature_families: dict[str, list[str]],
    evaluation_start: pd.Timestamp,
    evaluation_end: pd.Timestamp,
    max_train_rows: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    train = dataset[dataset["label_end_time_utc"] < evaluation_start].copy()
    evaluation = dataset[
        (dataset["decision_time_utc"] >= evaluation_start)
        & (dataset["decision_time_utc"] < evaluation_end)
    ].copy()
    prediction, diagnostics, _model = _fit_and_predict(
        train,
        evaluation,
        spec,
        feature_families,
        max_train_rows,
    )
    return _prediction_surface(evaluation, prediction, spec, "diagnostic_evaluation"), diagnostics


def _evaluate_simple_frozen(
    evaluation: pd.DataFrame,
    selected: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    surface = _simple_surface(evaluation, selected["direction"])
    return _apply_surface_policy(
        surface,
        float(selected["frozen_score_threshold"]),
        int(selected["max_new_per_timestamp"]),
    )


def _label_summary(dataset: pd.DataFrame) -> dict[str, Any]:
    directions = {}
    for direction in ("reversal", "continuation"):
        endpoint = dataset[f"{direction}_endpoint_net_pips"]
        curve = dataset[f"{direction}_curve_net_pips"]
        mfe = dataset[f"{direction}_mfe_net_pips"]
        directions[direction] = {
            "endpoint_mean_pips": float(endpoint.mean()),
            "endpoint_positive_rate": float((endpoint > 0.0).mean()),
            "curve_mean_pips": float(curve.mean()),
            "curve_positive_rate": float((curve > 0.0).mean()),
            "mfe_mean_pips": float(mfe.mean()),
            "endpoint_rank_ic_vs_curve": rank_ic(
                dataset.assign(_endpoint=endpoint, _curve=curve), "_curve", "_endpoint"
            ),
        }
    year_rows = []
    years = dataset["decision_time_utc"].dt.year
    for year, group in dataset.groupby(years, sort=True):
        year_rows.append({
            "year": int(year),
            "rows": int(len(group)),
            "reversal_endpoint_mean_pips": float(group["reversal_endpoint_net_pips"].mean()),
            "continuation_endpoint_mean_pips": float(group["continuation_endpoint_net_pips"].mean()),
            "path_range_mean_pips": float(group["path_range_pips_120m"].mean()),
        })
    return {"directions": directions, "by_year": year_rows}


def _feature_audit(feature_families: dict[str, list[str]]) -> dict[str, Any]:
    forbidden_tokens = (
        "future_",
        "_target",
        "target_",
        "_outcome",
        "outcome_",
        "_curve_",
        "_profit_",
        "_mfe_",
        "_mae_",
        "giveback",
    )
    all_features = sorted({
        feature
        for features in feature_families.values()
        for feature in features
    })
    forbidden = [
        feature
        for feature in all_features
        if any(token in feature.lower() for token in forbidden_tokens)
    ]
    return {
        "source_feature_count": 227,
        "safe_full_feature_count": int(len(feature_families["safe_full_220"])),
        "motion_cost_feature_count": int(len(feature_families["motion_cost_117"])),
        "arima_feature_count": int(len(feature_families["arima_causal"])),
        "safe_plus_arima_feature_count": int(
            len(feature_families["safe_full_220_plus_arima"])
        ),
        "feature_family_counts": {
            name: int(len(features))
            for name, features in feature_families.items()
        },
        "arima_feature_columns": list(feature_families["arima_causal"]),
        "excluded_global_full_history_features": sorted(UNSAFE_GLOBAL_FEATURES),
        "forbidden_label_like_features": forbidden,
        "feature_name_audit_passed": not forbidden,
        "arima_features_use_completed_candles_only": True,
        "arima_features_use_forward_labels": False,
        "arima_family_preserves_safe_full_220_control": True,
        "features_observed_through_completed_h1_bar": True,
        "decision_time_shift_minutes": H1_MINUTES,
        "old_target_columns_loaded_as_features": False,
        "old_curve_targets_used": False,
        "full_history_volatility_metadata_used": False,
    }


def _top_scorecards(selection: dict[str, Any], limit: int = 40) -> list[dict[str, Any]]:
    return sorted(
        selection["reports"],
        key=_selection_rank_key,
        reverse=True,
    )[:limit]


def run_corrected_h1_rebuild(
    cfg: dict[str, Any],
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
    fixed_units: int = DEFAULT_FIXED_UNITS,
    max_train_rows: int = 100_000,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    evaluation_start = _utc(start)
    evaluation_end = _exclusive_end(end)
    features, feature_families, source_manifest = _load_h1_features(
        cfg,
        tier,
        pairs,
        max_rows_per_pair,
    )
    dataset, label_audit = build_corrected_h1_labels(features, cfg, fixed_units)
    feature_audit = _feature_audit(feature_families)
    label_summary = _label_summary(dataset)

    model_feature_columns = {
        feature
        for features_for_family in feature_families.values()
        for feature in features_for_family
    }
    label_columns = [
        column
        for column in dataset.columns
        if column not in model_feature_columns
    ]
    dataset[label_columns].to_parquet(
        output_dir / "corrected_h1_labels.parquet", index=False
    )

    surfaces, fold_report, skipped = build_development_oos_predictions(
        dataset,
        feature_families,
        max_train_rows,
    )
    selection = select_development_candidate(surfaces)
    selected = selection["best"]
    selected_spec = selected["spec"]
    selected_development_surface = surfaces[selected["candidate"]]
    dev_eligible, dev_trades, dev_gates = _apply_surface_policy(
        selected_development_surface,
        float(selected["frozen_score_threshold"]),
        int(selected["max_new_per_timestamp"]),
    )
    model_viable = bool(selected["viability"]["viable"])
    selected_dev_trades = dev_trades if model_viable else dev_trades.iloc[0:0].copy()
    development_metrics = _rich_metrics(selected_dev_trades, dev_eligible)
    research_development_metrics = _rich_metrics(dev_trades, dev_eligible)

    evaluation_surface, final_fit = _fit_frozen_candidate(
        dataset,
        selected_spec,
        feature_families,
        evaluation_start,
        evaluation_end,
        max_train_rows,
    )
    eval_eligible, eval_trades, eval_gates = _apply_surface_policy(
        evaluation_surface,
        float(selected["frozen_score_threshold"]),
        int(selected["max_new_per_timestamp"]),
    )
    selected_eval_trades = eval_trades if model_viable else eval_trades.iloc[0:0].copy()
    evaluation_metrics = _rich_metrics(selected_eval_trades, eval_eligible)
    research_evaluation_metrics = _rich_metrics(eval_trades, eval_eligible)

    development = dataset[
        (dataset["decision_time_utc"] >= pd.Timestamp("2025-01-01T00:00:00Z"))
        & (dataset["decision_time_utc"] < pd.Timestamp("2026-01-01T00:00:00Z"))
    ].copy()
    simple_selection = select_simple_baseline(development)
    simple_best = simple_selection["best"]
    evaluation_rows = dataset[
        (dataset["decision_time_utc"] >= evaluation_start)
        & (dataset["decision_time_utc"] < evaluation_end)
    ].copy()
    simple_eligible, simple_trades, simple_gates = _evaluate_simple_frozen(
        evaluation_rows, simple_best
    )
    simple_viable = bool(simple_best["viability"]["viable"])
    selected_simple_trades = simple_trades if simple_viable else simple_trades.iloc[0:0].copy()
    simple_metrics = _rich_metrics(selected_simple_trades, simple_eligible)
    research_simple_metrics = _rich_metrics(simple_trades, simple_eligible)

    random_trades, random_metrics = _random_direction_baseline(eval_trades)
    stress = _cost_stress(eval_trades)
    stress.update({
        "research_only": bool(not model_viable),
        "model_selected_for_evaluation": model_viable,
    })
    selected_dev_trades.to_csv(output_dir / "corrected_h1_selected_development_trades.csv", index=False)
    selected_eval_trades.to_csv(output_dir / "corrected_h1_selected_evaluation_trades.csv", index=False)
    dev_trades.to_csv(output_dir / "corrected_h1_research_candidate_development_trades.csv", index=False)
    eval_trades.to_csv(output_dir / "corrected_h1_research_candidate_evaluation_trades.csv", index=False)
    simple_trades.to_csv(output_dir / "corrected_h1_research_simple_evaluation_trades.csv", index=False)
    selected_development_surface.to_parquet(
        output_dir / "corrected_h1_selected_candidate_oos_predictions.parquet",
        index=False,
    )
    evaluation_surface.to_parquet(
        output_dir / "corrected_h1_frozen_evaluation_predictions.parquet",
        index=False,
    )

    selected_pnl = float(evaluation_metrics["pnl_usd"])
    simple_pnl = float(simple_metrics["pnl_usd"])
    research_selected_pnl = float(research_evaluation_metrics["pnl_usd"])
    research_simple_pnl = float(research_simple_metrics["pnl_usd"])
    random_pnl = float(random_metrics["pnl_usd"])
    signal_checks = {
        "development_candidate_viable": model_viable,
        "positive_diagnostic_evaluation_pnl_after_costs": research_selected_pnl > 0.0,
        "beats_no_trade": model_viable and selected_pnl > 0.0,
        "beats_selected_simple_baseline": model_viable and selected_pnl > simple_pnl,
        "beats_random_direction_baseline": model_viable and selected_pnl > random_pnl,
        "top_score_decile_actual_ev_positive": (
            research_evaluation_metrics.get("top_score_decile_actual_ev_usd") or -math.inf
        ) > 0.0,
        "mild_cost_stress_survives": bool(stress["edge_survives_mild_stress"]),
        "diagnostic_max_drawdown_at_most_35pct": float(
            research_evaluation_metrics["max_drawdown_pct"]
        ) <= 35.0,
        "diagnostic_top_pair_share_at_most_40pct": (
            research_evaluation_metrics.get("top_pair_trade_share") is not None
            and float(research_evaluation_metrics["top_pair_trade_share"]) <= 0.40
        ),
        "feature_audit_passed": bool(
            feature_audit["feature_name_audit_passed"]
            and feature_audit["arima_features_use_completed_candles_only"]
            and not feature_audit["arima_features_use_forward_labels"]
        ),
        "label_timing_audit_passed": bool(
            label_audit["label_end_strictly_after_decision"]
            and label_audit["all_alignment_delays_within_limit"]
        ),
    }
    gate_diagnostics = {
        "viability_gate_suppressed_development_trades": int(len(dev_trades) - len(selected_dev_trades)),
        "viability_gate_suppressed_evaluation_trades": int(len(eval_trades) - len(selected_eval_trades)),
        "suppressed_positive_development_candidate": bool(
            not model_viable and float(research_development_metrics["pnl_usd"]) > 0.0
        ),
        "suppressed_positive_diagnostic_candidate": bool(
            not model_viable and research_selected_pnl > 0.0
        ),
        "research_candidate_beats_raw_simple_baseline": bool(
            research_selected_pnl > research_simple_pnl
        ),
        "research_candidate_beats_random_direction_baseline": bool(
            research_selected_pnl > random_pnl
        ),
    }
    salvage_verdict = "PASS" if all(signal_checks.values()) else "FAIL"

    label_report = {
        "source_manifest": source_manifest,
        "feature_audit": feature_audit,
        "label_audit": label_audit,
        "label_summary": label_summary,
    }
    write_report_pair(output_dir, "HTF_CORRECTED_LABEL_AUDIT", label_report)

    walk_forward_report = {
        "protocol": {
            "development_period": "2025-01-01 through 2025-12-31",
            "folds": "calendar quarters",
            "training_contract": "expanding rows whose corrected label end precedes fold start",
            "candidate_grid_declared_before_evaluation": True,
            "configuration_and_threshold_selected_only_on_2025_oos_predictions": True,
            "selection_rule": (
                "prefer fully viable configurations; when none are viable, choose the "
                "research-only fallback with the most viability checks passed, then "
                "positive folds, development P/L, and mean net pips"
            ),
            "evaluation_period_previously_inspected": True,
        },
        "candidate_specs": corrected_candidate_specs(),
        "fold_report": fold_report,
        "skipped": skipped,
        "declared_selection_configs": selection["declared_configs"],
        "viable_selection_configs": selection["viable_configs"],
        "selected": selected,
        "top_development_scorecards": _top_scorecards(selection),
    }
    write_report_pair(output_dir, "HTF_CORRECTED_WALK_FORWARD", walk_forward_report)

    report = {
        "run_id": output_dir.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "htf-corrected-rebuild",
        "tier": tier,
        "evaluation_start": evaluation_start,
        "evaluation_end_exclusive": evaluation_end,
        "evidence_status": "corrected_label_and_execution_diagnostic_not_fresh_holdout",
        "selected_model": {
            "selected_for_evaluation": model_viable,
            "candidate": selected["candidate"],
            "spec": selected_spec,
            "score_quantile": selected["score_quantile"],
            "frozen_score_threshold": selected["frozen_score_threshold"],
            "max_new_per_timestamp": selected["max_new_per_timestamp"],
            "viability": selected["viability"],
            "development_folds": selected["folds"],
            "development": development_metrics,
            "research_development_before_viability_gate": research_development_metrics,
            "diagnostic_evaluation": evaluation_metrics,
            "research_evaluation_before_viability_gate": research_evaluation_metrics,
            "development_gate_attribution": dev_gates,
            "evaluation_gate_attribution": eval_gates,
            "final_fit": final_fit,
        },
        "baselines": {
            "no_trade": {"pnl_usd": 0.0, "return_pct": 0.0, "trades": 0},
            "random_direction": random_metrics,
            "simple_abs_momentum": {
                "selected_for_evaluation": simple_viable,
                "selection": simple_best,
                "diagnostic_evaluation": simple_metrics,
                "research_evaluation_before_viability_gate": research_simple_metrics,
                "gate_attribution": simple_gates,
            },
        },
        "signal_checks": signal_checks,
        "gate_diagnostics": gate_diagnostics,
        "diagnostic_salvage_verdict": salvage_verdict,
        "production_readiness_verdict": "FAIL",
        "production_blockers": [
            "evaluation period was previously inspected",
            "no local candles after 2026-07-07 are available for a fresh holdout",
        ],
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    }
    write_report_pair(output_dir, "HTF_CORRECTED_REBUILD_REPORT", report)
    write_report_pair(output_dir, "HTF_CORRECTED_COST_STRESS", stress)

    final_summary = {
        "verdict": "FAIL",
        "diagnostic_salvage_verdict": salvage_verdict,
        "signal_checks": signal_checks,
        "gate_diagnostics": gate_diagnostics,
        "selected_model": report["selected_model"],
        "model_development_result": development_metrics,
        "model_research_development_before_viability_gate": research_development_metrics,
        "model_diagnostic_evaluation_result": evaluation_metrics,
        "model_research_evaluation_before_viability_gate": research_evaluation_metrics,
        "best_simple_baseline_result": simple_metrics,
        "best_simple_research_result_before_viability_gate": research_simple_metrics,
        "random_baseline_result": random_metrics,
        "no_trade_result": {"pnl_usd": 0.0, "return_pct": 0.0, "trades": 0},
        "cost_stress_survived": bool(stress["edge_survives_mild_stress"]),
        "old_curve_target_used": False,
        "old_unshifted_entry_used": False,
        "fresh_untouched_holdout_available": False,
        "biggest_remaining_blocker": "collect post-2026-07-07 candles and evaluate this frozen protocol once",
        "next_recommended_step": (
            "freeze the corrected H1 candidate and wait for post-2026-07-07 candles"
            if salvage_verdict == "PASS"
            else (
                "retain this exact H1 configuration as a frozen research-only watchlist "
                "candidate and evaluate it once on post-2026-07-07 candles; do not tune "
                "on the inspected 2026 period or promote it"
                if gate_diagnostics["suppressed_positive_development_candidate"]
                and gate_diagnostics["suppressed_positive_diagnostic_candidate"]
                and stress["edge_survives_mild_stress"]
                else "stop the corrected H1 family; do not promote the archived 227-feature ensemble"
            )
        ),
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
        "live_execution_confirmation": "Research/backtest only; no OANDA bot, broker call, order placement, credential edit, or account-file modification was invoked.",
    }
    write_report_pair(output_dir, "HTF_CORRECTED_FINAL_SUMMARY", final_summary)
    write_json(output_dir / "run_manifest.json", {
        "mode": "htf-corrected-rebuild",
        "run_dir": str(output_dir),
        "reports": [
            "HTF_CORRECTED_LABEL_AUDIT.json",
            "HTF_CORRECTED_WALK_FORWARD.json",
            "HTF_CORRECTED_REBUILD_REPORT.json",
            "HTF_CORRECTED_COST_STRESS.json",
            "HTF_CORRECTED_FINAL_SUMMARY.json",
        ],
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
    })
    return output_dir / "HTF_CORRECTED_FINAL_SUMMARY.json"
