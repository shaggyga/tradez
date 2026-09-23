#!/usr/bin/env python3
"""Fetch OANDA S5 bid/ask history and test event meta-models with microstructure features."""

from __future__ import annotations

import argparse
from datetime import timedelta
import json
import math
import os
from pathlib import Path
import shutil
import time
from typing import Any, Dict, Iterable, List

import joblib
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

import oanda_gpt_training_strategy_manager as manager
from oanda_event_meta_model_pipeline import (
    BASE_NUMERIC_FEATURES,
    META_NUMERIC_FEATURES,
    MACRO_FEATURES,
    COT_FEATURES,
    CATEGORICAL_FEATURES,
    TARGET,
    add_macro_features,
    add_cftc_features,
    choose_one_action,
    classification_metrics,
    latest_labeled_dataset,
    trade_metrics,
    block_bootstrap_mean_r_lower,
)


PIPELINE_VERSION = "s5_microstructure_event_meta_v1"
DEFAULT_PAIRS = ["GBP_JPY", "USD_JPY"]
DEFAULT_DAYS = 180
S5_ROOT = Path(
    os.environ.get(
        "OANDA_S5_ROOT",
        str(manager.TRAINING_ROOT / "candles_s5_bam"),
    )
)
MICRO_FEATURES = [
    "micro_spread_last",
    "micro_spread_mean_60s",
    "micro_spread_mean_300s",
    "micro_spread_std_300s",
    "micro_spread_expansion",
    "micro_directional_momentum_15s",
    "micro_directional_momentum_30s",
    "micro_directional_momentum_60s",
    "micro_directional_momentum_180s",
    "micro_directional_momentum_300s",
    "micro_return_std_60s",
    "micro_return_std_300s",
    "micro_range_60s",
    "micro_range_300s",
    "micro_volume_30s",
    "micro_volume_60s",
    "micro_volume_300s",
    "micro_volume_acceleration",
    "micro_direction_agreement_60s",
    "micro_direction_agreement_300s",
]
NUMERIC_FEATURES = BASE_NUMERIC_FEATURES + META_NUMERIC_FEATURES + MACRO_FEATURES + COT_FEATURES + MICRO_FEATURES


def s5_path(instrument: str) -> Path:
    return S5_ROOT / f"{instrument}_S5.parquet"


def parse_s5(payload: Dict[str, Any], instrument: str) -> pd.DataFrame:
    rows = []
    multiplier = manager.pips_multiplier(instrument)
    for candle in payload.get("candles", []) or []:
        if not candle.get("complete", True):
            continue
        bid = candle.get("bid") or {}
        ask = candle.get("ask") or {}
        try:
            bid_open = float(bid["o"])
            bid_high = float(bid["h"])
            bid_low = float(bid["l"])
            bid_close = float(bid["c"])
            ask_open = float(ask["o"])
            ask_high = float(ask["h"])
            ask_low = float(ask["l"])
            ask_close = float(ask["c"])
        except Exception:
            continue
        rows.append({
            "time": candle.get("time"),
            "bid_open": bid_open,
            "bid_high": bid_high,
            "bid_low": bid_low,
            "bid_close": bid_close,
            "ask_open": ask_open,
            "ask_high": ask_high,
            "ask_low": ask_low,
            "ask_close": ask_close,
            "mid_open": (bid_open + ask_open) / 2.0,
            "mid_high": (bid_high + ask_high) / 2.0,
            "mid_low": (bid_low + ask_low) / 2.0,
            "mid_close": (bid_close + ask_close) / 2.0,
            "spread_pips": (ask_close - bid_close) * multiplier,
            "volume": manager.safe_int(candle.get("volume")),
        })
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["dt"] = pd.to_datetime(frame["time"], errors="coerce", utc=True)
        frame = frame.dropna(subset=["dt"]).sort_values("dt").drop_duplicates("dt", keep="last")
    return frame


def backfill_s5(client: manager.OandaClient, instrument: str, days: int) -> Dict[str, Any]:
    S5_ROOT.mkdir(parents=True, exist_ok=True)
    cutoff = manager.utc_now() - timedelta(days=days)
    temp_dir = S5_ROOT / f".{instrument}_parts"
    root_resolved = S5_ROOT.resolve()
    temp_resolved = temp_dir.resolve()
    try:
        temp_resolved.relative_to(root_resolved)
    except ValueError as error:
        raise RuntimeError(f"temporary backfill path escapes S5 root: {temp_resolved}") from error
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    end_time = manager.utc_now()
    previous_earliest = None
    part_paths = []
    requests = 0
    error = ""
    while True:
        payload = client.candles(
            instrument,
            granularity="S5",
            count=5000,
            end_time=end_time,
            price="BA",
        )
        requests += 1
        if payload.get("_error"):
            error = manager.json_dumps(payload)[:1000]
            break
        batch = parse_s5(payload, instrument)
        if batch.empty:
            break
        earliest = batch.iloc[0]["dt"].to_pydatetime()
        batch = batch[batch["dt"] >= pd.Timestamp(cutoff)]
        if not batch.empty:
            part = temp_dir / f"part_{requests:05d}.parquet"
            batch.to_parquet(part, index=False, compression="zstd")
            part_paths.append(part)
        if earliest <= cutoff:
            break
        if previous_earliest is not None and earliest >= previous_earliest:
            error = "pagination made no backward progress"
            break
        previous_earliest = earliest
        end_time = earliest - timedelta(seconds=1)
        if requests % 50 == 0:
            print(f"[s5-backfill] {instrument}: requests={requests}", flush=True)
        time.sleep(0.06)
    if not part_paths:
        return {"instrument": instrument, "requests": requests, "rows": 0, "error": error}
    frames = [pd.read_parquet(path) for path in part_paths]
    combined = (
        pd.concat(frames, ignore_index=True)
        .sort_values("dt")
        .drop_duplicates("dt", keep="last")
        .reset_index(drop=True)
    )
    combined.to_parquet(s5_path(instrument), index=False, compression="zstd")
    shutil.rmtree(temp_dir)
    print(
        f"[s5-backfill] {instrument}: complete requests={requests} rows={len(combined)} "
        f"size_mb={s5_path(instrument).stat().st_size / 1e6:.1f}",
        flush=True,
    )
    return {
        "instrument": instrument,
        "requests": requests,
        "rows": len(combined),
        "start": str(combined["dt"].min()),
        "end": str(combined["dt"].max()),
        "size_bytes": s5_path(instrument).stat().st_size,
        "error": error,
    }


def load_s5(instrument: str) -> pd.DataFrame:
    frame = pd.read_parquet(s5_path(instrument))
    frame["dt"] = pd.to_datetime(frame["dt"], errors="coerce", utc=True)
    return frame.sort_values("dt").drop_duplicates("dt").reset_index(drop=True)


def micro_features_at(
    s5: pd.DataFrame,
    timestamp: pd.Timestamp,
    direction: str,
    instrument: str,
) -> Dict[str, float] | None:
    right = s5["dt"].searchsorted(timestamp, side="right")
    if right < 60:
        return None
    window = s5.iloc[right - 60:right]
    if timestamp - window.iloc[-1]["dt"] > pd.Timedelta(seconds=10):
        return None
    if window["dt"].diff().dropna().max() > pd.Timedelta(seconds=10):
        return None
    multiplier = manager.pips_multiplier(instrument)
    direction_multiplier = 1.0 if direction == "LONG" else -1.0
    close = window["mid_close"].to_numpy(float)
    returns = np.diff(close) * multiplier
    spread = window["spread_pips"].to_numpy(float)
    volume = window["volume"].to_numpy(float)
    def momentum(seconds: int) -> float:
        bars = max(1, seconds // 5)
        return float((close[-1] - close[-1 - bars]) * multiplier * direction_multiplier)
    def range_pips(seconds: int) -> float:
        bars = max(1, seconds // 5)
        recent = window.iloc[-bars:]
        return float((recent["mid_high"].max() - recent["mid_low"].min()) * multiplier)
    def agreement(seconds: int) -> float:
        bars = max(2, seconds // 5)
        signed = np.sign(np.diff(close[-bars:])) * direction_multiplier
        return float(np.mean(signed > 0))
    return {
        "micro_spread_last": float(spread[-1]),
        "micro_spread_mean_60s": float(np.mean(spread[-12:])),
        "micro_spread_mean_300s": float(np.mean(spread)),
        "micro_spread_std_300s": float(np.std(spread)),
        "micro_spread_expansion": float(spread[-1] / max(np.mean(spread), 0.05)),
        "micro_directional_momentum_15s": momentum(15),
        "micro_directional_momentum_30s": momentum(30),
        "micro_directional_momentum_60s": momentum(60),
        "micro_directional_momentum_180s": momentum(180),
        "micro_directional_momentum_300s": momentum(295),
        "micro_return_std_60s": float(np.std(returns[-12:])),
        "micro_return_std_300s": float(np.std(returns)),
        "micro_range_60s": range_pips(60),
        "micro_range_300s": range_pips(300),
        "micro_volume_30s": float(np.sum(volume[-6:])),
        "micro_volume_60s": float(np.sum(volume[-12:])),
        "micro_volume_300s": float(np.sum(volume)),
        "micro_volume_acceleration": float(
            np.mean(volume[-12:]) / max(np.mean(volume[-60:-12]), 0.1)
        ),
        "micro_direction_agreement_60s": agreement(60),
        "micro_direction_agreement_300s": agreement(300),
    }


def enrich_dataset(labeled: pd.DataFrame, pairs: Iterable[str]) -> pd.DataFrame:
    parts = []
    for instrument in pairs:
        group = labeled[labeled["instrument"] == instrument].copy()
        if group.empty:
            continue
        s5 = load_s5(instrument)
        feature_rows = []
        for timestamp, direction in zip(group["time_utc"], group["direction"]):
            feature_rows.append(micro_features_at(s5, timestamp, direction, instrument))
        valid = pd.Series([row is not None for row in feature_rows], index=group.index)
        group = group.loc[valid].copy()
        feature_frame = pd.DataFrame([row for row in feature_rows if row is not None], index=group.index)
        group = pd.concat([group, feature_frame], axis=1)
        parts.append(group)
        print(f"[s5-enrich] {instrument}: source={len(valid)} enriched={len(group)}", flush=True)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).sort_values("time_utc").reset_index(drop=True)


def make_model() -> Pipeline:
    transformer = ColumnTransformer([
        ("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
        ("numeric", "passthrough", NUMERIC_FEATURES),
    ])
    classifier = HistGradientBoostingClassifier(
        learning_rate=0.035,
        max_iter=350,
        max_leaf_nodes=31,
        min_samples_leaf=50,
        l2_regularization=3.0,
        random_state=42,
    )
    return Pipeline([("features", transformer), ("classifier", classifier)])


def rolling_validation(df: pd.DataFrame) -> List[Dict[str, Any]]:
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    boundaries = [df["time_utc"].quantile(q) for q in [0.50, 0.625, 0.75, 0.875, 1.0]]
    results = []
    for number, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1):
        train = df[df["time_utc"] < start - pd.Timedelta(minutes=185)]
        test = df[(df["time_utc"] >= start) & (df["time_utc"] <= end)]
        model = make_model()
        model.fit(train[features], train[TARGET].astype(int))
        probability = model.predict_proba(test[features])[:, 1]
        metrics = classification_metrics(test[TARGET].to_numpy(int), probability)
        metrics.update({"fold": number, "train_rows": len(train), "test_rows": len(test)})
        results.append(metrics)
        print(f"[s5-validation] fold={number} auc={metrics['auc']:.4f} ap={metrics['average_precision']:.4f}", flush=True)
    return results


def threshold_metrics(frame: pd.DataFrame, probability: np.ndarray) -> List[Dict[str, Any]]:
    unique = choose_one_action(frame, probability)
    rows = []
    for threshold in np.arange(0.20, 0.81, 0.025):
        trades = unique[unique["probability"] >= threshold]
        rows.append({"threshold": float(threshold), **trade_metrics(trades)})
    return rows


def final_fit(df: pd.DataFrame) -> tuple[Dict[str, Any], Dict[str, Any]]:
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    q70 = df["time_utc"].quantile(0.70)
    q82 = df["time_utc"].quantile(0.82)
    train = df[df["time_utc"] < q70 - pd.Timedelta(minutes=185)]
    calibration = df[(df["time_utc"] >= q70) & (df["time_utc"] < q82 - pd.Timedelta(minutes=185))]
    holdout = df[df["time_utc"] >= q82]
    base = make_model()
    base.fit(train[features], train[TARGET].astype(int))
    calibrated = CalibratedClassifierCV(FrozenEstimator(base), method="sigmoid")
    calibrated.fit(calibration[features], calibration[TARGET].astype(int))
    calibration_probability = calibrated.predict_proba(calibration[features])[:, 1]
    calibration_thresholds = threshold_metrics(calibration, calibration_probability)
    viable = [
        row for row in calibration_thresholds
        if row["trades"] >= 75 and row["mean_r"] > 0 and row["profit_factor"] >= 1.10
    ]
    threshold = (
        max(viable, key=lambda row: row["mean_r"] * math.sqrt(row["trades"]))["threshold"]
        if viable else 0.70
    )
    holdout_probability = calibrated.predict_proba(holdout[features])[:, 1]
    holdout_classification = classification_metrics(holdout[TARGET].to_numpy(int), holdout_probability)
    holdout_thresholds = threshold_metrics(holdout, holdout_probability)
    unique = choose_one_action(holdout, holdout_probability)
    selected = unique[unique["probability"] >= threshold]
    selected_metrics = trade_metrics(selected)
    selected_metrics["bootstrap_mean_r_lower_95"] = block_bootstrap_mean_r_lower(selected)
    monthly = []
    month_values = holdout["time_utc"].dt.to_period("M").astype(str)
    for month, group in holdout.groupby(month_values):
        if group[TARGET].nunique() > 1:
            positions = holdout.index.get_indexer(group.index)
            monthly.append({
                "month": month,
                "rows": len(group),
                "auc": float(roc_auc_score(group[TARGET], holdout_probability[positions])),
            })
    gate = {
        "auc_at_least_0_65": holdout_classification["auc"] >= 0.65,
        "average_precision_lift_at_least_0_08": (
            holdout_classification["average_precision"] - holdout_classification["positive_rate"] >= 0.08
        ),
        "minimum_month_auc_at_least_0_52": min([row["auc"] for row in monthly], default=0) >= 0.52,
        "selected_trades_at_least_75": selected_metrics["trades"] >= 75,
        "selected_profit_factor_at_least_1_20": selected_metrics["profit_factor"] >= 1.20,
        "selected_mean_r_positive": selected_metrics["mean_r"] > 0,
        "selected_bootstrap_lower_mean_r_positive": selected_metrics["bootstrap_mean_r_lower_95"] > 0,
        "selected_max_drawdown_r_at_most_15": selected_metrics["max_drawdown_r"] <= 15,
    }
    gate["passed"] = all(gate.values())
    report = {
        "train_rows": len(train),
        "calibration_rows": len(calibration),
        "holdout_rows": len(holdout),
        "selected_threshold": threshold,
        "calibration_thresholds": calibration_thresholds,
        "holdout_classification": holdout_classification,
        "holdout_thresholds": holdout_thresholds,
        "selected_trade_metrics": selected_metrics,
        "monthly": monthly,
        "production_gate": gate,
    }
    artifact = {
        "model": calibrated,
        "features": features,
        "threshold": threshold,
        "pipeline_version": PIPELINE_VERSION,
        "report": report,
        "trained_utc": manager.iso_utc(),
    }
    return artifact, report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--pairs", nargs="+", default=DEFAULT_PAIRS)
    parser.add_argument("--skip-backfill", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manager.ensure_dirs()
    training_manager = manager.TrainingStrategyManager()
    summaries = []
    if not args.skip_backfill:
        for instrument in args.pairs:
            summaries.append(backfill_s5(training_manager.client, instrument, args.days))
        manager.save_json(manager.DIRS["reports"] / "latest_s5_backfill_summary.json", {
            "pipeline_version": PIPELINE_VERSION,
            "days": args.days,
            "instruments": summaries,
            "generated_utc": manager.iso_utc(),
        })
    labeled_path = latest_labeled_dataset()
    use_columns = list(dict.fromkeys(
        ["time_utc", "instrument", "direction", "theme", "setup_family", "trade_style", TARGET,
         "realized_pips", "realized_r"] + BASE_NUMERIC_FEATURES + META_NUMERIC_FEATURES
    ))
    labeled = pd.read_csv(labeled_path, usecols=use_columns)
    labeled["time_utc"] = pd.to_datetime(labeled["time_utc"], utc=True)
    labeled = labeled[labeled["instrument"].isin(args.pairs)]
    labeled = add_macro_features(labeled)
    labeled = add_cftc_features(labeled)
    enriched = enrich_dataset(labeled, args.pairs)
    if enriched.empty:
        raise SystemExit("No S5-enriched candidates available.")
    dataset_path = manager.DIRS["training_sets"] / f"s5_event_meta_training_set_{manager.utc_now().strftime('%Y%m%d_%H%M%S')}.parquet"
    enriched.to_parquet(dataset_path, index=False, compression="zstd")
    folds = rolling_validation(enriched)
    artifact, final_report = final_fit(enriched)
    artifact_path = manager.DIRS["models"] / f"s5_event_meta_model_{manager.utc_now().strftime('%Y%m%d_%H%M%S')}.joblib"
    joblib.dump(artifact, artifact_path)
    report = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_utc": manager.iso_utc(),
        "pairs": args.pairs,
        "days": args.days,
        "dataset": str(dataset_path),
        "rows": len(enriched),
        "start": enriched["time_utc"].min().isoformat(),
        "end": enriched["time_utc"].max().isoformat(),
        "rolling_validation": folds,
        "artifact": str(artifact_path),
        "final": final_report,
    }
    manager.save_json(manager.DIRS["reports"] / "latest_s5_event_meta_model_report.json", report)
    print(json.dumps({
        "rows": len(enriched),
        "pairs": args.pairs,
        "holdout": final_report["holdout_classification"],
        "selected_threshold": final_report["selected_threshold"],
        "selected_trade_metrics": final_report["selected_trade_metrics"],
        "production_gate": final_report["production_gate"],
        "artifact": str(artifact_path),
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
