#!/usr/bin/env python3
"""Leakage-resistant OANDA M1 research pipeline for production model gating."""

from __future__ import annotations

import argparse
from datetime import timedelta
import json
import math
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

import oanda_gpt_training_strategy_manager as manager


PIPELINE_VERSION = "production_quality_v2_return_curve_labels"
LIQUID_UNIVERSE = manager.PREFERRED_MAJOR_INSTRUMENTS[:12]
DEFAULT_HISTORY_DAYS = 90
DEFAULT_STEP_MINUTES = 15
DEFAULT_HORIZON_MINUTES = 30
SLIPPAGE_RESERVE_PIPS = 0.20
RETURN_CURVE_BEST_WEIGHT = 0.55
RETURN_CURVE_ENDPOINT_WEIGHT = 0.25
RETURN_CURVE_EARLY_WEIGHT = 0.20
RETURN_CURVE_ADVERSE_PENALTY = 0.35

NUMERIC_FEATURES = [
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
    "accel_pips",
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
    "trend_alignment",
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
    "currency_strength_gap",
    "directional_currency_strength_gap",
    "basket_confirmation_count",
]
CATEGORICAL_FEATURES = ["instrument", "direction", "theme"]
TARGET = "would_profit_horizon"


def iso_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, sort_keys=True, default=str)


def bam_path(instrument: str) -> Path:
    return manager.DIRS["candles_bam"] / f"{instrument}_M1.csv"


def load_bam(instrument: str) -> pd.DataFrame:
    path = bam_path(instrument)
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    df["dt"] = pd.to_datetime(df.get("datetime", df.get("time")), errors="coerce", utc=True)
    for column in [
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
    ]:
        df[column] = pd.to_numeric(df.get(column), errors="coerce")
    required = ["dt", "close", "bid_close", "ask_close", "spread_pips"]
    return (
        df.dropna(subset=required)
        .sort_values("dt")
        .drop_duplicates("dt")
        .reset_index(drop=True)
    )


def backfill_bam(
    client: manager.OandaClient,
    instruments: Iterable[str],
    days: int,
    batch_size: int = 5000,
) -> Dict[str, Any]:
    cutoff = manager.utc_now() - timedelta(days=max(1, days))
    summary: Dict[str, Any] = {
        "pipeline_version": PIPELINE_VERSION,
        "days": days,
        "cutoff_utc": cutoff.isoformat(),
        "started_utc": manager.iso_utc(),
        "instruments": {},
    }
    instruments = list(instruments)
    for number, instrument in enumerate(instruments, 1):
        batches: List[pd.DataFrame] = []
        end_time = manager.utc_now()
        previous_earliest = None
        requests = 0
        error = ""
        while True:
            payload = client.candles(
                instrument,
                granularity="M1",
                count=batch_size,
                end_time=end_time,
                price="BAM",
            )
            requests += 1
            if payload.get("_error"):
                error = manager.json_dumps(payload)[:1000]
                break
            batch = manager.candle_df_from_oanda(payload, instrument, "M1")
            if batch.empty:
                break
            batches.append(batch)
            earliest = manager.parse_oanda_time(str(batch.iloc[0]["datetime"]))
            if earliest <= cutoff:
                break
            if previous_earliest is not None and earliest >= previous_earliest:
                error = "pagination made no backward progress"
                break
            previous_earliest = earliest
            end_time = earliest - timedelta(seconds=1)
            time.sleep(0.08)
        before = len(load_bam(instrument))
        after = before
        if batches:
            combined = pd.concat(batches, ignore_index=True)
            combined["filter_dt"] = pd.to_datetime(combined["datetime"], errors="coerce", utc=True)
            combined = combined[combined["filter_dt"] >= pd.Timestamp(cutoff)].drop(columns=["filter_dt"])
            if not combined.empty:
                manager.save_candles_csv(
                    combined,
                    instrument,
                    "M1",
                    root=manager.DIRS["candles_bam"],
                )
                after = len(load_bam(instrument))
        summary["instruments"][instrument] = {
            "requests": requests,
            "rows_before": before,
            "rows_after": after,
            "rows_added": max(0, after - before),
            "error": error,
        }
        print(
            f"[bam-backfill] {number}/{len(instruments)} {instrument}: "
            f"requests={requests} rows={after} added={max(0, after - before)}"
            + (f" error={error}" if error else ""),
            flush=True,
        )
    summary["completed_utc"] = manager.iso_utc()
    summary["total_rows_added"] = sum(
        manager.safe_int(item["rows_added"]) for item in summary["instruments"].values()
    )
    report_path = manager.DIRS["reports"] / "latest_bam_backfill_summary.json"
    manager.save_json(report_path, summary)
    return summary


def _momentum(close: np.ndarray, periods: int, multiplier: float) -> float:
    if len(close) <= periods:
        return 0.0
    return float((close[-1] - close[-1 - periods]) * multiplier)


def infer_theme(instrument: str, direction: str) -> str:
    theme = "MOMENTUM"
    if "JPY" in instrument:
        theme = "JPY_STRENGTH" if direction == "SHORT" and instrument.endswith("JPY") else "JPY_WEAKNESS"
    if instrument.startswith("USD") or instrument.endswith("USD"):
        if (instrument.startswith("USD") and direction == "LONG") or (
            instrument.endswith("USD") and direction == "SHORT"
        ):
            theme = "USD_RALLY"
        else:
            theme = "USD_SELLOFF"
    if instrument[:3] in {"AUD", "NZD", "CAD"} or instrument[-3:] in {"AUD", "NZD", "CAD"}:
        theme = "COMMODITY_FX_" + ("STRENGTH" if direction == "LONG" else "WEAKNESS")
    return theme


def recent_event_features(
    history: pd.DataFrame,
    instrument: str,
    direction: str,
    minutes: int,
) -> Dict[str, float]:
    window = history.iloc[-minutes:]
    if len(window) < minutes:
        return {"ratio": 0.0, "directional_move": 0.0, "cost_net": 0.0}
    multiplier = manager.pips_multiplier(instrument)
    start = window.iloc[0]
    end = window.iloc[-1]
    mid_move = (float(end["close"]) - float(start["open"])) * multiplier
    average_spread = float(window["spread_pips"].mean())
    if direction == "LONG":
        cost_net = (float(end["bid_close"]) - float(start["ask_close"])) * multiplier
        directional_move = mid_move
    else:
        cost_net = (float(start["bid_close"]) - float(end["ask_close"])) * multiplier
        directional_move = -mid_move
    return {
        "ratio": abs(mid_move) / max(average_spread, 0.1),
        "directional_move": directional_move,
        "cost_net": cost_net,
    }


def pressure_features(history: pd.DataFrame, instrument: str) -> Dict[str, float]:
    multiplier = manager.pips_multiplier(instrument)
    close = history["close"].to_numpy(dtype=float)
    def move(bars: int) -> float:
        return float((close[-1] - close[-1 - bars]) * multiplier)
    def average_range(frame: pd.DataFrame) -> float:
        return float(((frame["high"] - frame["low"]) * multiplier).mean())
    m3, m5, m10, m15 = move(3), move(5), move(10), move(15)
    weighted = m3 * 0.40 + m5 * 0.30 + m10 * 0.20 + m15 * 0.10
    recent_range = average_range(history.iloc[-5:])
    prior_range = average_range(history.iloc[-35:-10])
    longer_range = average_range(history.iloc[-60:-35])
    compression_ratio = prior_range / max(longer_range, 0.1)
    expansion_ratio = recent_range / max(prior_range, 0.1)
    accel = max(0.0, abs(m3) - abs(move(8) - move(5)))
    spread = max(float(history.iloc[-1]["spread_pips"]), 0.1)
    threshold = 3.5
    ratio = abs(weighted) / spread
    move_score = float(np.clip(abs(weighted) / threshold * 45.0, 0.0, 45.0))
    accel_score = float(np.clip(accel / max(threshold * 0.25, 0.5) * 20.0, 0.0, 20.0))
    compression_score = float(np.clip((1.10 - compression_ratio) * 30.0, 0.0, 15.0))
    expansion_score = float(np.clip((expansion_ratio - 1.0) * 14.0, 0.0, 15.0))
    spread_score = float(np.clip((ratio - 0.50) * 12.0, 0.0, 10.0))
    return {
        "weighted": weighted,
        "score": float(np.clip(
            move_score + accel_score + compression_score + expansion_score + spread_score,
            0.0,
            100.0,
        )),
        "accel": accel,
        "compression_ratio": compression_ratio,
        "expansion_ratio": expansion_ratio,
    }


def build_rows_for_instrument(
    instrument: str,
    profile: manager.StrategyProfile,
    step_minutes: int,
    horizon_minutes: int,
) -> List[Dict[str, Any]]:
    df = load_bam(instrument)
    if len(df) < 120:
        return []
    multiplier = manager.pips_multiplier(instrument)
    rows: List[Dict[str, Any]] = []
    horizon_minutes = max(5, int(horizon_minutes))
    for i in range(60, len(df) - horizon_minutes - 1):
        now = df.iloc[i]
        timestamp = now["dt"]
        if timestamp.minute % max(1, step_minutes) != 0:
            continue
        past_30 = df.iloc[i - 30]
        future_horizon = df.iloc[i + horizon_minutes]
        past_delta = now["dt"] - past_30["dt"]
        future_delta = future_horizon["dt"] - now["dt"]
        if not (timedelta(minutes=29) <= past_delta <= timedelta(minutes=31)):
            continue
        if not (
            timedelta(minutes=horizon_minutes - 1)
            <= future_delta
            <= timedelta(minutes=horizon_minutes + 1)
        ):
            continue
        history = df.iloc[i - 60:i + 1]
        if history["dt"].diff().dropna().max() > timedelta(minutes=2):
            continue
        future = df.iloc[i:i + horizon_minutes + 1]
        if future["dt"].diff().dropna().max() > timedelta(minutes=2):
            continue
        spread = float(now["spread_pips"])
        if not math.isfinite(spread) or spread <= 0 or spread > 12:
            continue
        mid = float(now["close"])
        bid = float(now["bid_close"])
        ask = float(now["ask_close"])
        signal = manager.compute_signal(
            instrument,
            history,
            {"mid": mid, "bid": bid, "ask": ask, "spread_pips": spread},
            profile,
        )
        if signal is None:
            continue
        signal.time_utc = now["dt"].isoformat()
        close = history["close"].to_numpy(dtype=float)
        diffs = np.diff(close) * multiplier
        high_30 = float(history["high"].iloc[-30:].max())
        low_30 = float(history["low"].iloc[-30:].min())
        range_30 = max((high_30 - low_30) * multiplier, 0.0)
        range_position = (mid - low_30) / max(high_30 - low_30, 1e-12)
        m1 = _momentum(close, 1, multiplier)
        m3 = _momentum(close, 3, multiplier)
        m60 = _momentum(close, 60, multiplier)
        vol5 = float(np.std(diffs[-5:])) if len(diffs) >= 5 else 0.0
        vol15 = float(np.std(diffs[-15:])) if len(diffs) >= 15 else 0.0
        vol60 = float(np.std(diffs[-60:])) if len(diffs) >= 60 else 0.0
        trend_alignment = float(
            np.sign(signal.momentum_5_pips)
            + np.sign(signal.momentum_15_pips)
            + np.sign(signal.momentum_30_pips)
        ) / 3.0
        future_bid = future["bid_close"].to_numpy(dtype=float)
        future_ask = future["ask_close"].to_numpy(dtype=float)
        pressure = pressure_features(history, instrument)
        hour = timestamp.hour + timestamp.minute / 60.0
        weekday = timestamp.weekday()
        base = signal.to_row()
        for direction in ["LONG", "SHORT"]:
            direction_mult = 1.0 if direction == "LONG" else -1.0
            if direction == "LONG":
                net_path = (future_bid - ask) * multiplier - SLIPPAGE_RESERVE_PIPS
                directional_position = range_position
            else:
                net_path = (bid - future_ask) * multiplier - SLIPPAGE_RESERVE_PIPS
                directional_position = 1.0 - range_position
            net_30 = float(net_path[-1])
            best_net = float(np.max(net_path))
            worst_net = float(np.min(net_path))
            early_index = min(
                len(net_path) - 1,
                max(1, int(round(min(horizon_minutes, 30) / 1.0))),
            )
            early_net = float(net_path[early_index])
            adverse_cost = max(0.0, -worst_net)
            curve_net = (
                RETURN_CURVE_BEST_WEIGHT * best_net
                + RETURN_CURVE_ENDPOINT_WEIGHT * net_30
                + RETURN_CURVE_EARLY_WEIGHT * early_net
                - RETURN_CURVE_ADVERSE_PENALTY * adverse_cost
            )
            row = dict(base)
            row["direction"] = direction
            row["theme"] = infer_theme(instrument, direction)
            events = {
                minutes: recent_event_features(history, instrument, direction, minutes)
                for minutes in [5, 10, 15, 30]
            }
            rows.append({
                **row,
                "momentum_1_pips": m1,
                "momentum_3_pips": m3,
                "momentum_60_pips": m60,
                "directional_momentum_1_pips": m1 * direction_mult,
                "directional_momentum_3_pips": m3 * direction_mult,
                "directional_momentum_5_pips": signal.momentum_5_pips * direction_mult,
                "directional_momentum_15_pips": signal.momentum_15_pips * direction_mult,
                "directional_momentum_30_pips": signal.momentum_30_pips * direction_mult,
                "directional_momentum_60_pips": m60 * direction_mult,
                "volatility_5_pips": vol5,
                "volatility_15_pips": vol15,
                "volatility_60_pips": vol60,
                "spread_to_volatility": spread / max(signal.volatility_30_pips, 0.05),
                "range_30_pips": range_30,
                "range_position_30": float(np.clip(range_position, 0.0, 1.0)),
                "directional_range_position_30": float(np.clip(directional_position, 0.0, 1.0)),
                "trend_alignment": trend_alignment,
                "directional_trend_alignment": trend_alignment * direction_mult,
                "hour_sin": math.sin(2 * math.pi * hour / 24.0),
                "hour_cos": math.cos(2 * math.pi * hour / 24.0),
                "weekday_sin": math.sin(2 * math.pi * weekday / 5.0),
                "weekday_cos": math.cos(2 * math.pi * weekday / 5.0),
                "pressure_weighted_pips": pressure["weighted"],
                "directional_pressure_weighted_pips": pressure["weighted"] * direction_mult,
                "pressure_score": pressure["score"],
                "pressure_accel_pips": pressure["accel"],
                "pressure_compression_ratio": pressure["compression_ratio"],
                "pressure_expansion_ratio": pressure["expansion_ratio"],
                **{
                    f"event_ratio_{minutes}": events[minutes]["ratio"]
                    for minutes in [5, 10, 15, 30]
                },
                **{
                    f"directional_event_move_{minutes}": events[minutes]["directional_move"]
                    for minutes in [5, 10, 15, 30]
                },
                **{
                    f"event_cost_net_{minutes}": events[minutes]["cost_net"]
                    for minutes in [5, 10, 15, 30]
                },
                "currency_strength_gap": 0.0,
                "directional_currency_strength_gap": 0.0,
                "basket_confirmation_count": 0.0,
                "net_horizon_pips": net_30,
                "mfe_horizon_pips": float(np.max(net_path)),
                "mae_horizon_pips": float(np.min(net_path)),
                "curve_best_net_pips": best_net,
                "curve_early_net_pips": early_net,
                "curve_endpoint_net_pips": net_30,
                "curve_mae_pips": worst_net,
                "curve_giveback_pips": max(0.0, best_net - net_30),
                "curve_efficiency": curve_net / best_net if best_net > 0 else 0.0,
                "curve_net_pips": curve_net,
                "would_profit_curve": int(curve_net > 0),
                "horizon_minutes": horizon_minutes,
                TARGET: int(net_30 > 0),
                "dataset_source": "oanda_bam_exact_time_dual_direction",
            })
    return rows


def add_currency_strength_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    output = df.copy()
    output["base_currency"] = output["instrument"].str.slice(0, 3)
    output["quote_currency"] = output["instrument"].str.slice(-3)
    output["_direction_mult"] = np.where(output["direction"].eq("LONG"), 1.0, -1.0)
    source = output.drop_duplicates(["time_utc", "instrument"]).copy()
    source["vote_direction"] = np.sign(source["pressure_weighted_pips"]).replace(0, 1)
    source["vote_weight"] = (
        source["pressure_weighted_pips"].abs()
        / source["spread_pips"].clip(lower=0.1)
    ).clip(upper=5.0)
    feature_rows: List[Dict[str, Any]] = []
    for timestamp, group in source.groupby("time_utc", sort=False):
        strength: Dict[str, float] = {}
        confirmations: Dict[Tuple[str, int], int] = {}
        for row in group.itertuples():
            vote = float(row.vote_direction) * float(row.vote_weight)
            strength[row.base_currency] = strength.get(row.base_currency, 0.0) + vote
            strength[row.quote_currency] = strength.get(row.quote_currency, 0.0) - vote
            confirmations[(row.base_currency, 1 if vote > 0 else -1)] = confirmations.get(
                (row.base_currency, 1 if vote > 0 else -1), 0
            ) + 1
            confirmations[(row.quote_currency, -1 if vote > 0 else 1)] = confirmations.get(
                (row.quote_currency, -1 if vote > 0 else 1), 0
            ) + 1
        for row in group.itertuples():
            gap = strength.get(row.base_currency, 0.0) - strength.get(row.quote_currency, 0.0)
            feature_rows.append({
                "time_utc": timestamp,
                "instrument": row.instrument,
                "_strength_gap": gap,
                "_long_confirmations": confirmations.get((row.base_currency, 1), 0)
                + confirmations.get((row.quote_currency, -1), 0),
                "_short_confirmations": confirmations.get((row.base_currency, -1), 0)
                + confirmations.get((row.quote_currency, 1), 0),
            })
    feature_frame = pd.DataFrame(feature_rows)
    output = output.merge(feature_frame, on=["time_utc", "instrument"], how="left")
    output["currency_strength_gap"] = output["_strength_gap"].fillna(0.0)
    output["directional_currency_strength_gap"] = (
        output["currency_strength_gap"] * output["_direction_mult"]
    )
    output["basket_confirmation_count"] = np.where(
        output["direction"].eq("LONG"),
        output["_long_confirmations"],
        output["_short_confirmations"],
    )
    return output.drop(columns=[
        "base_currency",
        "quote_currency",
        "_direction_mult",
        "_strength_gap",
        "_long_confirmations",
        "_short_confirmations",
    ])


def build_dataset(
    instruments: Iterable[str],
    profile: manager.StrategyProfile,
    step_minutes: int,
    horizon_minutes: int,
) -> pd.DataFrame:
    all_rows: List[Dict[str, Any]] = []
    instruments = list(instruments)
    for number, instrument in enumerate(instruments, 1):
        rows = build_rows_for_instrument(instrument, profile, step_minutes, horizon_minutes)
        all_rows.extend(rows)
        print(
            f"[production-dataset] {number}/{len(instruments)} {instrument}: rows={len(rows)}",
            flush=True,
        )
    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True, errors="coerce")
        df = add_currency_strength_features(df)
        df = (
            df.dropna(subset=["time_utc"] + NUMERIC_FEATURES + CATEGORICAL_FEATURES + [TARGET, "net_horizon_pips"])
            .drop_duplicates(["time_utc", "instrument", "direction"])
            .sort_values(["time_utc", "instrument"])
            .reset_index(drop=True)
        )
    return df


def make_estimator(name: str) -> Pipeline:
    transformer = ColumnTransformer(
        [
            ("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
            ("numeric", "passthrough", NUMERIC_FEATURES),
        ],
        remainder="drop",
    )
    if name == "hist_gradient_boosting":
        classifier = HistGradientBoostingClassifier(
            learning_rate=0.05,
            max_iter=250,
            max_leaf_nodes=31,
            min_samples_leaf=40,
            l2_regularization=1.0,
            random_state=42,
        )
    elif name == "extra_trees":
        classifier = ExtraTreesClassifier(
            n_estimators=300,
            max_depth=14,
            min_samples_leaf=20,
            class_weight="balanced",
            n_jobs=-1,
            random_state=42,
        )
    else:
        raise ValueError(f"Unknown estimator: {name}")
    return Pipeline([("features", transformer), ("classifier", classifier)])


def metric_block(y: np.ndarray, probability: np.ndarray) -> Dict[str, float]:
    prediction = (probability >= 0.5).astype(int)
    return {
        "auc": float(roc_auc_score(y, probability)),
        "average_precision": float(average_precision_score(y, probability)),
        "accuracy": float(accuracy_score(y, prediction)),
        "precision_050": float(precision_score(y, prediction, zero_division=0)),
        "recall_050": float(recall_score(y, prediction, zero_division=0)),
        "brier": float(brier_score_loss(y, probability)),
        "log_loss": float(log_loss(y, probability)),
        "positive_rate": float(np.mean(y)),
    }


def rolling_folds(df: pd.DataFrame) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    times = df["time_utc"]
    quantiles = [0.50, 0.625, 0.75, 0.875, 1.0]
    boundaries = [times.quantile(q) for q in quantiles]
    folds = []
    for test_start, test_end in zip(boundaries[:-1], boundaries[1:]):
        train = df[times < test_start - pd.Timedelta(minutes=31)]
        test = df[(times >= test_start) & (times <= test_end)]
        if len(train) >= 10_000 and len(test) >= 2_000:
            folds.append((train, test))
    return folds


def evaluate_model(name: str, df: pd.DataFrame) -> Dict[str, Any]:
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    fold_results = []
    for number, (train, test) in enumerate(rolling_folds(df), 1):
        model = make_estimator(name)
        model.fit(train[features], train[TARGET].astype(int))
        probability = model.predict_proba(test[features])[:, 1]
        metrics = metric_block(test[TARGET].astype(int).to_numpy(), probability)
        metrics.update({
            "fold": number,
            "train_rows": len(train),
            "test_rows": len(test),
            "test_start": test["time_utc"].min().isoformat(),
            "test_end": test["time_utc"].max().isoformat(),
        })
        fold_results.append(metrics)
        print(
            f"[validation] {name} fold={number} auc={metrics['auc']:.4f} "
            f"ap={metrics['average_precision']:.4f} brier={metrics['brier']:.4f}",
            flush=True,
        )
    return {
        "model": name,
        "folds": fold_results,
        "mean_auc": float(np.mean([row["auc"] for row in fold_results])),
        "min_auc": float(np.min([row["auc"] for row in fold_results])),
        "mean_average_precision": float(np.mean([row["average_precision"] for row in fold_results])),
        "mean_brier": float(np.mean([row["brier"] for row in fold_results])),
    }


def threshold_table(
    df: pd.DataFrame,
    probability: np.ndarray,
    thresholds: Iterable[float],
) -> List[Dict[str, Any]]:
    result = []
    net = df["net_horizon_pips"].to_numpy(dtype=float)
    target = df[TARGET].to_numpy(dtype=int)
    for threshold in thresholds:
        selected = probability >= threshold
        result.append({
            "threshold": threshold,
            "trades": int(np.sum(selected)),
            "coverage": float(np.mean(selected)),
            "win_rate": float(np.mean(target[selected])) if np.any(selected) else None,
            "mean_net_pips": float(np.mean(net[selected])) if np.any(selected) else None,
            "median_net_pips": float(np.median(net[selected])) if np.any(selected) else None,
        })
    return result


def train_final(name: str, df: pd.DataFrame) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    q75 = df["time_utc"].quantile(0.75)
    q85 = df["time_utc"].quantile(0.85)
    train = df[df["time_utc"] < q75 - pd.Timedelta(minutes=31)]
    calibration = df[(df["time_utc"] >= q75) & (df["time_utc"] < q85 - pd.Timedelta(minutes=31))]
    holdout = df[df["time_utc"] >= q85]
    base = make_estimator(name)
    base.fit(train[features], train[TARGET].astype(int))
    calibrated = CalibratedClassifierCV(FrozenEstimator(base), method="sigmoid")
    calibrated.fit(calibration[features], calibration[TARGET].astype(int))
    calibration_probability = calibrated.predict_proba(calibration[features])[:, 1]
    calibration_table = threshold_table(
        calibration,
        calibration_probability,
        np.arange(0.20, 0.81, 0.025),
    )
    viable = [
        row for row in calibration_table
        if row["trades"] >= 300
        and row["coverage"] >= 0.005
        and row["mean_net_pips"] is not None
        and row["mean_net_pips"] > 0
    ]
    selected_threshold = (
        max(viable, key=lambda row: row["mean_net_pips"] * math.sqrt(row["trades"]))["threshold"]
        if viable else 0.70
    )
    holdout_probability = calibrated.predict_proba(holdout[features])[:, 1]
    holdout_metrics = metric_block(holdout[TARGET].astype(int).to_numpy(), holdout_probability)
    holdout_thresholds = threshold_table(
        holdout,
        holdout_probability,
        sorted(set([0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.60, selected_threshold])),
    )
    selected = next(
        row for row in holdout_thresholds
        if abs(row["threshold"] - selected_threshold) < 1e-9
    )
    per_instrument = []
    for instrument, group in holdout.groupby("instrument"):
        if len(group) < 100 or group[TARGET].nunique() < 2:
            continue
        idx = group.index.to_numpy()
        position = holdout.index.get_indexer(idx)
        per_instrument.append({
            "instrument": instrument,
            "rows": len(group),
            "auc": float(roc_auc_score(group[TARGET].astype(int), holdout_probability[position])),
        })
    weekly = []
    week_values = holdout["time_utc"].dt.to_period("W").astype(str)
    for week, group in holdout.groupby(week_values):
        position = holdout.index.get_indexer(group.index)
        if group[TARGET].nunique() < 2:
            continue
        weekly.append({
            "week": week,
            "rows": len(group),
            "auc": float(roc_auc_score(group[TARGET].astype(int), holdout_probability[position])),
        })
    median_instrument_auc = (
        float(np.median([row["auc"] for row in per_instrument]))
        if per_instrument else 0.0
    )
    min_week_auc = min([row["auc"] for row in weekly], default=0.0)
    production_gate = {
        "auc_at_least_0_72": holdout_metrics["auc"] >= 0.72,
        "average_precision_lift_at_least_0_10": (
            holdout_metrics["average_precision"] - holdout_metrics["positive_rate"] >= 0.10
        ),
        "median_instrument_auc_at_least_0_60": median_instrument_auc >= 0.60,
        "minimum_week_auc_at_least_0_60": min_week_auc >= 0.60,
        "selected_threshold_trades_at_least_300": selected["trades"] >= 300,
        "selected_threshold_mean_net_pips_positive": (
            selected["mean_net_pips"] is not None and selected["mean_net_pips"] > 0
        ),
    }
    production_gate["passed"] = all(production_gate.values())
    report = {
        "model": name,
        "pipeline_version": PIPELINE_VERSION,
        "train_rows": len(train),
        "calibration_rows": len(calibration),
        "holdout_rows": len(holdout),
        "selected_threshold": selected_threshold,
        "calibration_thresholds": calibration_table,
        "holdout_metrics": holdout_metrics,
        "holdout_thresholds": holdout_thresholds,
        "median_instrument_auc": median_instrument_auc,
        "per_instrument": per_instrument,
        "minimum_week_auc": min_week_auc,
        "weekly": weekly,
        "production_gate": production_gate,
    }
    artifact = {
        "model": calibrated,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "threshold": selected_threshold,
        "universe": LIQUID_UNIVERSE,
        "pipeline_version": PIPELINE_VERSION,
        "trained_utc": manager.iso_utc(),
        "report": report,
    }
    return artifact, report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-days", type=int, default=DEFAULT_HISTORY_DAYS)
    parser.add_argument("--step-minutes", type=int, default=DEFAULT_STEP_MINUTES)
    parser.add_argument("--horizon-minutes", type=int, default=DEFAULT_HORIZON_MINUTES)
    parser.add_argument("--skip-backfill", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manager.ensure_dirs()
    training_manager = manager.TrainingStrategyManager()
    if not args.skip_backfill:
        backfill_bam(
            training_manager.client,
            LIQUID_UNIVERSE,
            days=args.history_days,
        )
    dataset = build_dataset(
        LIQUID_UNIVERSE,
        training_manager.active_strategy,
        step_minutes=args.step_minutes,
        horizon_minutes=args.horizon_minutes,
    )
    if dataset.empty:
        raise SystemExit("Production dataset is empty.")
    dataset_path = (
        manager.DIRS["training_sets"]
        / f"production_training_set_h{args.horizon_minutes}_{manager.utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    dataset.to_csv(dataset_path, index=False)
    candidates = [
        evaluate_model("hist_gradient_boosting", dataset),
        evaluate_model("extra_trees", dataset),
    ]
    best = max(
        candidates,
        key=lambda row: (row["mean_auc"], row["min_auc"], -row["mean_brier"]),
    )
    artifact, final_report = train_final(best["model"], dataset)
    artifact_path = (
        manager.DIRS["models"]
        / f"production_trade_quality_h{args.horizon_minutes}_{manager.utc_now().strftime('%Y%m%d_%H%M%S')}.joblib"
    )
    joblib.dump(artifact, artifact_path)
    report = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_utc": manager.iso_utc(),
        "universe": LIQUID_UNIVERSE,
        "dataset": str(dataset_path),
        "dataset_rows": len(dataset),
        "dataset_start": dataset["time_utc"].min().isoformat(),
        "dataset_end": dataset["time_utc"].max().isoformat(),
        "horizon_minutes": args.horizon_minutes,
        "positive_rate": float(dataset[TARGET].mean()),
        "candidate_validation": candidates,
        "selected_model": best["model"],
        "artifact": str(artifact_path),
        "final": final_report,
    }
    report_path = manager.DIRS["reports"] / f"production_model_report_h{args.horizon_minutes}.json"
    manager.save_json(report_path, report)
    manager.save_json(manager.DIRS["reports"] / "latest_production_model_report.json", report)
    print(iso_json({
        "dataset_rows": len(dataset),
        "selected_model": best["model"],
        "artifact": str(artifact_path),
        "holdout": final_report["holdout_metrics"],
        "selected_threshold": final_report["selected_threshold"],
        "production_gate": final_report["production_gate"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
