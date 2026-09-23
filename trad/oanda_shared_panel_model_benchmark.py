#!/usr/bin/env python3
"""Benchmark tabular model-gap challengers on the shared FX panel."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

try:
    import oanda_gpt_training_strategy_manager as manager
    import oanda_shared_timeframe_horizon_panel as panel
    from oanda_event_meta_model_pipeline import SklearnFittedClassifierAdapter
except ModuleNotFoundError:
    from trad import oanda_gpt_training_strategy_manager as manager
    from trad import oanda_shared_timeframe_horizon_panel as panel
    from trad.oanda_event_meta_model_pipeline import SklearnFittedClassifierAdapter


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "modern_model_gap"
MODEL_ROOT = ROOT / "data" / "oanda_training_manager" / "models" / "modern_model_gap"
DEFAULT_MODELS = ("logistic_baseline", "hist_gradient_boosting", "catboost", "ngboost")
MODEL_ALIASES = {
    "hgb": "hist_gradient_boosting",
    "logistic": "logistic_baseline",
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def timeframe_seconds(value: str) -> int:
    text = str(value).strip().upper()
    multipliers = {"S": 1, "M": 60, "H": 3600}
    if text[:1] in multipliers:
        seconds = int(text[1:]) * multipliers[text[0]]
    else:
        seconds = int(text)
    if seconds <= 0:
        raise ValueError("timeframe must be positive")
    return seconds


def canonical_model_names(models: Iterable[str]) -> list[str]:
    return list(
        dict.fromkeys(
            MODEL_ALIASES.get(str(name).strip().lower(), str(name).strip().lower())
            for name in models
        )
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def distribution_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return ""


def atomic_json_dump(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_joblib_dump(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        joblib.dump(value, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _event_tail(frame: pd.DataFrame, max_events: int) -> pd.DataFrame:
    if max_events <= 0 or frame["event_id"].nunique() <= max_events:
        return frame
    events = (
        frame[
            [
                "event_id",
                "prediction_time_utc",
                "input_timeframe",
                "horizon_sec",
            ]
        ]
        .drop_duplicates("event_id")
        .sort_values("prediction_time_utc")
    )
    groups = list(
        events.groupby(["input_timeframe", "horizon_sec"], observed=True, sort=False)
    )
    base, remainder = divmod(max_events, len(groups))
    selected: set[str] = set()
    for index, (_, group) in enumerate(groups):
        limit = base + int(index < remainder)
        if limit <= 0:
            continue
        if len(group) <= limit:
            selected.update(group["event_id"].astype(str))
            continue
        positions = np.linspace(0, len(group) - 1, num=limit, dtype=int)
        selected.update(group.iloc[np.unique(positions)]["event_id"].astype(str))
    return frame[frame["event_id"].isin(selected)]


def _panel_event_limits(paths: list[Path], max_events: int) -> dict[tuple[str, int], int]:
    cells: set[tuple[str, int]] = set()
    for path in paths:
        frame = pd.read_parquet(
            path,
            columns=["input_timeframe", "horizon_sec"],
        )
        cells.update(
            (str(row.input_timeframe), int(row.horizon_sec))
            for row in frame.drop_duplicates().itertuples(index=False)
        )
    ordered = sorted(cells)
    if not ordered:
        return {}
    base, remainder = divmod(max_events, len(ordered))
    return {
        cell: base + int(index < remainder)
        for index, cell in enumerate(ordered)
    }


def _event_sample_with_limits(
    frame: pd.DataFrame,
    remaining: dict[tuple[str, int], int],
) -> pd.DataFrame:
    events = (
        frame[
            ["event_id", "prediction_time_utc", "input_timeframe", "horizon_sec"]
        ]
        .drop_duplicates("event_id")
        .sort_values("prediction_time_utc")
    )
    selected: set[str] = set()
    for raw_cell, group in events.groupby(
        ["input_timeframe", "horizon_sec"], observed=True, sort=False
    ):
        cell = (str(raw_cell[0]), int(raw_cell[1]))
        limit = max(0, int(remaining.get(cell, 0)))
        if limit <= 0:
            continue
        if len(group) <= limit:
            chosen = group
        else:
            positions = np.linspace(0, len(group) - 1, num=limit, dtype=int)
            chosen = group.iloc[np.unique(positions)]
        identifiers = chosen["event_id"].astype(str)
        selected.update(identifiers)
        remaining[cell] = limit - len(identifiers)
    return frame[frame["event_id"].astype(str).isin(selected)]


def load_panels(paths: Iterable[Path], max_events: int = 0) -> tuple[pd.DataFrame, dict[str, Any]]:
    resolved_paths = [Path(path).resolve() for path in paths]
    remaining = (
        _panel_event_limits(resolved_paths, max_events) if max_events > 0 else {}
    )
    parts: list[pd.DataFrame] = []
    sources: list[dict[str, Any]] = []
    for path in resolved_paths:
        frame = pd.read_parquet(path)
        panel.validate_panel(frame)
        sources.append(
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "rows": len(frame),
                "events": frame["event_id"].nunique(),
            }
        )
        parts.append(
            _event_sample_with_limits(frame, remaining)
            if max_events > 0
            else frame
        )
    if not parts:
        raise ValueError("at least one shared-panel parquet is required")
    frame = pd.concat(parts, ignore_index=True)
    if frame["side_id"].duplicated().any():
        raise ValueError("input panels overlap on side_id")
    frame = _event_tail(frame, max_events)
    frame = frame.sort_values(
        ["prediction_time_utc", "instrument", "horizon_sec", "direction"]
    ).reset_index(drop=True)
    if frame[panel.TARGET_COLUMN].nunique() < 2:
        raise ValueError("shared panel has no binary target variation")
    optional_coverage = {
        name: float(frame[name].notna().mean())
        for name in panel.OPTIONAL_MICROSTRUCTURE_FEATURES
        if name in frame.columns
    }
    return frame, {
        "sources": sources,
        "rows": len(frame),
        "events": frame["event_id"].nunique(),
        "instruments": frame["instrument"].nunique(),
        "timeframes": sorted(frame["input_timeframe"].unique().tolist()),
        "horizons_sec": sorted(int(value) for value in frame["horizon_sec"].unique()),
        "start_utc": frame["prediction_time_utc"].min().isoformat(),
        "end_utc": frame["prediction_time_utc"].max().isoformat(),
        "positive_rate": float(frame[panel.TARGET_COLUMN].mean()),
        "optional_feature_coverage": optional_coverage,
    }


def model_features(frame: pd.DataFrame) -> tuple[list[str], list[str]]:
    numeric = [
        name
        for name in panel.MODEL_NUMERIC_FEATURES
        if name in frame.columns and frame[name].notna().any()
    ]
    categorical = [name for name in panel.MODEL_CATEGORICAL_FEATURES if name in frame.columns]
    required_numeric = set(panel.REQUIRED_MODEL_NUMERIC_FEATURES)
    missing = sorted(
        (required_numeric | set(panel.MODEL_CATEGORICAL_FEATURES))
        - set(frame.columns)
    )
    if missing:
        raise ValueError(f"panel is missing model features: {', '.join(missing)}")
    return numeric, categorical


def _preprocessor(numeric: list[str], categorical: list[str], scale: bool) -> ColumnTransformer:
    numeric_steps: list[tuple[str, Any]] = [
        (
            "impute",
            SimpleImputer(strategy="median", keep_empty_features=True),
        )
    ]
    if scale:
        numeric_steps.append(("scale", StandardScaler()))
    return ColumnTransformer(
        [
            ("numeric", Pipeline(numeric_steps), numeric),
            (
                "categorical",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        ("one_hot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical,
            ),
        ],
        sparse_threshold=0.0,
    )


def make_model(name: str, numeric: list[str], categorical: list[str]) -> Pipeline:
    if name == "logistic_baseline":
        classifier: Any = LogisticRegression(
            C=0.5,
            class_weight="balanced",
            max_iter=500,
            random_state=42,
        )
        scale = True
    elif name == "hist_gradient_boosting":
        classifier = HistGradientBoostingClassifier(
            learning_rate=0.04,
            max_iter=220,
            max_leaf_nodes=31,
            min_samples_leaf=35,
            l2_regularization=2.0,
            random_state=42,
        )
        scale = False
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
        scale = False
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
        scale = False
    else:
        raise ValueError(f"unsupported shared-panel model: {name}")
    return Pipeline(
        [
            ("features", _preprocessor(numeric, categorical, scale=scale)),
            ("classifier", classifier),
        ]
    )


def _time_at_fraction(frame: pd.DataFrame, fraction: float) -> pd.Timestamp:
    times = frame["prediction_time_utc"].drop_duplicates().sort_values().reset_index(drop=True)
    if times.empty:
        raise ValueError("no prediction times are available")
    index = min(len(times) - 1, max(0, int(math.floor(fraction * len(times)))))
    return pd.Timestamp(times.iloc[index])


def purged_walk_forward_folds(
    frame: pd.DataFrame,
    min_train_events: int = 500,
    min_test_events: int = 100,
) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    boundaries = [_time_at_fraction(frame, value) for value in (0.50, 0.625, 0.75, 0.875)]
    final_end = frame["prediction_time_utc"].max() + timedelta(microseconds=1)
    folds: list[tuple[pd.DataFrame, pd.DataFrame]] = []
    for start, end in zip(boundaries, [*boundaries[1:], final_end]):
        train = frame[frame["maturity_time_utc"] < start]
        test = frame[
            (frame["prediction_time_utc"] >= start)
            & (frame["prediction_time_utc"] < end)
        ]
        if (
            train["event_id"].nunique() >= min_train_events
            and test["event_id"].nunique() >= min_test_events
            and train[panel.TARGET_COLUMN].nunique() == 2
            and test[panel.TARGET_COLUMN].nunique() == 2
        ):
            folds.append((train, test))
    return folds


def classification_metrics(target: np.ndarray, probability: np.ndarray) -> dict[str, float]:
    return {
        "auc": float(roc_auc_score(target, probability)),
        "average_precision": float(average_precision_score(target, probability)),
        "positive_rate": float(np.mean(target)),
        "brier": float(brier_score_loss(target, probability)),
        "log_loss": float(log_loss(target, probability, labels=[0, 1])),
    }


def choose_one_action(frame: pd.DataFrame, probability: np.ndarray) -> pd.DataFrame:
    selected = frame.copy()
    selected["probability"] = probability
    return (
        selected.sort_values(
            ["event_id", "probability", "direction"],
            ascending=[True, False, True],
        )
        .drop_duplicates("event_id")
        .sort_values(["prediction_time_utc", "instrument", "horizon_sec"])
    )


def trade_metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    pips = frame["realized_net_pips"].to_numpy(float)
    gross_win = float(np.sum(pips[pips > 0.0]))
    gross_loss = float(abs(np.sum(pips[pips < 0.0])))
    equity = np.cumsum(pips)
    drawdown = np.maximum.accumulate(np.insert(equity, 0, 0.0))[1:] - equity
    elapsed_hours = 0.0
    if len(frame) > 1:
        elapsed_hours = max(
            0.0,
            (frame["prediction_time_utc"].max() - frame["prediction_time_utc"].min()).total_seconds()
            / 3600.0,
        )
    return {
        "trades": int(len(frame)),
        "win_rate": float(np.mean(pips > 0.0)) if len(frame) else 0.0,
        "best_side_rate": float(frame["target_best_side"].mean()) if len(frame) else 0.0,
        "mean_net_pips": float(np.mean(pips)) if len(frame) else 0.0,
        "median_net_pips": float(np.median(pips)) if len(frame) else 0.0,
        "sum_net_pips": float(np.sum(pips)) if len(frame) else 0.0,
        "pips_per_hour": float(np.sum(pips) / elapsed_hours) if elapsed_hours > 0.0 else 0.0,
        "profit_factor": (
            gross_win / gross_loss
            if gross_loss > 0.0
            else (math.inf if gross_win > 0.0 else 0.0)
        ),
        "max_drawdown_pips": float(np.max(drawdown)) if len(drawdown) else 0.0,
    }


def candidate_probability_thresholds(probability: np.ndarray) -> list[float]:
    values = np.asarray(probability, dtype=float)
    values = values[np.isfinite(values)]
    if not len(values):
        return []
    quantiles = np.quantile(
        values,
        [0.0, 0.25, 0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.925, 0.95, 0.975, 0.99],
    )
    fixed = np.arange(0.50, 0.901, 0.025)
    return sorted(
        {
            float(np.clip(value, 0.0, 1.0))
            for value in np.concatenate((quantiles, fixed))
        }
    )


def threshold_table(frame: pd.DataFrame, probability: np.ndarray) -> list[dict[str, Any]]:
    unique = choose_one_action(frame, probability)
    thresholds = candidate_probability_thresholds(unique["probability"].to_numpy(float))
    return [
        {
            "threshold": float(threshold),
            "trade_fraction": float(np.mean(unique["probability"] >= threshold)),
            **trade_metrics(unique[unique["probability"] >= threshold]),
        }
        for threshold in thresholds
    ]


def _bootstrap_mean_lower(frame: pd.DataFrame, samples: int = 400) -> float:
    if frame.empty:
        return -math.inf
    data = frame.copy()
    data["day"] = data["prediction_time_utc"].dt.floor("D")
    days = list(data["day"].unique())
    if len(days) < 8:
        return -math.inf
    by_day = {
        day: data.loc[data["day"] == day, "realized_net_pips"].to_numpy(float)
        for day in days
    }
    rng = np.random.default_rng(42)
    means: list[float] = []
    for _ in range(samples):
        sampled = rng.choice(days, size=len(days), replace=True)
        values = np.concatenate([by_day[day] for day in sampled])
        means.append(float(np.mean(values)))
    return float(np.quantile(means, 0.025))


def _fit_calibrator(probability: np.ndarray, target: np.ndarray) -> LogisticRegression | None:
    if len(np.unique(target)) < 2:
        return None
    calibrator = LogisticRegression(C=1.0, max_iter=200, random_state=42)
    calibrator.fit(probability.reshape(-1, 1), target)
    return calibrator


def _calibrated_probability(model: Pipeline, calibrator: LogisticRegression | None, frame: pd.DataFrame, features: list[str]) -> np.ndarray:
    raw = np.asarray(model.predict_proba(frame[features])[:, 1], dtype=float)
    if calibrator is None:
        return raw
    return np.asarray(calibrator.predict_proba(raw.reshape(-1, 1))[:, 1], dtype=float)


def cell_metrics(actions: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (timeframe, horizon), group in actions.groupby(
        ["input_timeframe", "horizon_sec"], observed=True
    ):
        rows.append(
            {
                "input_timeframe": timeframe,
                "horizon_sec": int(horizon),
                **trade_metrics(group),
                "mean_confidence": float(group["probability"].mean()),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            timeframe_seconds(row["input_timeframe"]),
            row["horizon_sec"],
        ),
    )


def prediction_cell_metrics(frame: pd.DataFrame, probability: np.ndarray) -> list[dict[str, Any]]:
    scored = frame.copy()
    scored["probability"] = np.asarray(probability, dtype=float)
    rows: list[dict[str, Any]] = []
    for (timeframe, horizon), group in scored.groupby(
        ["input_timeframe", "horizon_sec"], observed=True
    ):
        target = group[panel.TARGET_COLUMN].to_numpy(int)
        predicted = group["probability"].to_numpy(float)
        actions = choose_one_action(group, predicted)
        classification: dict[str, float | None] = {
            "auc": (
                float(roc_auc_score(target, predicted))
                if len(np.unique(target)) == 2
                else None
            ),
            "brier": float(brier_score_loss(target, predicted)),
            "accuracy_at_0_5": float(np.mean((predicted >= 0.5) == target)),
        }
        rows.append(
            {
                "input_timeframe": str(timeframe),
                "horizon_sec": int(horizon),
                "side_rows": int(len(group)),
                "events": int(group["event_id"].nunique()),
                "mean_confidence": float(actions["probability"].mean()),
                **classification,
                **trade_metrics(actions),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            timeframe_seconds(row["input_timeframe"]),
            row["horizon_sec"],
        ),
    )


def evaluate_model(
    name: str,
    frame: pd.DataFrame,
    min_train_events: int,
    min_test_events: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    numeric, categorical = model_features(frame)
    features = [*numeric, *categorical]
    folds: list[dict[str, Any]] = []
    for number, (train, test) in enumerate(
        purged_walk_forward_folds(frame, min_train_events, min_test_events), start=1
    ):
        model = make_model(name, numeric, categorical)
        model.fit(train[features], train[panel.TARGET_COLUMN].astype(int))
        probability = np.asarray(model.predict_proba(test[features])[:, 1], dtype=float)
        fold = {
            "fold": number,
            "train_rows": len(train),
            "train_events": train["event_id"].nunique(),
            "test_rows": len(test),
            "test_events": test["event_id"].nunique(),
            "train_maturity_end_utc": train["maturity_time_utc"].max().isoformat(),
            "test_prediction_start_utc": test["prediction_time_utc"].min().isoformat(),
            **classification_metrics(test[panel.TARGET_COLUMN].to_numpy(int), probability),
            "all_action_metrics": trade_metrics(choose_one_action(test, probability)),
        }
        folds.append(fold)
        print(
            f"[shared-benchmark] {name} fold={number} auc={fold['auc']:.4f} "
            f"net={fold['all_action_metrics']['mean_net_pips']:.4f}",
            flush=True,
        )
    if not folds:
        raise ValueError("no walk-forward fold satisfies the requested minimum event counts")

    calibration_start = _time_at_fraction(frame, 0.70)
    holdout_start = _time_at_fraction(frame, 0.82)
    train = frame[frame["maturity_time_utc"] < calibration_start]
    calibration = frame[
        (frame["prediction_time_utc"] >= calibration_start)
        & (frame["maturity_time_utc"] < holdout_start)
    ]
    holdout = frame[frame["prediction_time_utc"] >= holdout_start]
    for split_name, split in (("train", train), ("calibration", calibration), ("holdout", holdout)):
        if split["event_id"].nunique() < max(10, min_test_events if split_name != "train" else min_train_events):
            raise ValueError(f"{split_name} split is too small after maturity purge")
        if split[panel.TARGET_COLUMN].nunique() < 2:
            raise ValueError(f"{split_name} split has no binary target variation")

    final_model = make_model(name, numeric, categorical)
    final_model.fit(train[features], train[panel.TARGET_COLUMN].astype(int))
    calibration_raw = np.asarray(final_model.predict_proba(calibration[features])[:, 1], dtype=float)
    calibrator = _fit_calibrator(
        calibration_raw,
        calibration[panel.TARGET_COLUMN].to_numpy(int),
    )
    calibration_probability = _calibrated_probability(final_model, calibrator, calibration, features)
    calibration_thresholds = threshold_table(calibration, calibration_probability)
    viable = [
        row
        for row in calibration_thresholds
        if row["trades"] >= min_test_events
        and row["mean_net_pips"] > 0.0
        and row["profit_factor"] >= 1.05
    ]
    selected_threshold = (
        max(
            viable,
            key=lambda row: row["mean_net_pips"] * math.sqrt(row["trades"]),
        )["threshold"]
        if viable
        else 0.70
    )
    threshold_selection = {
        "status": "selected_on_calibration" if viable else "no_viable_calibration_threshold",
        "candidate_count": len(calibration_thresholds),
        "viable_candidate_count": len(viable),
        "fallback_threshold": 0.70 if not viable else None,
        "candidates": calibration_thresholds,
    }
    holdout_probability = _calibrated_probability(final_model, calibrator, holdout, features)
    holdout_actions = choose_one_action(holdout, holdout_probability)
    selected = holdout_actions[holdout_actions["probability"] >= selected_threshold]
    selected_metrics = trade_metrics(selected)
    bootstrap_lower = _bootstrap_mean_lower(selected)
    selected_metrics["bootstrap_mean_net_pips_lower_95"] = bootstrap_lower
    holdout_classification = classification_metrics(
        holdout[panel.TARGET_COLUMN].to_numpy(int), holdout_probability
    )
    gate_checks = {
        "minimum_trades": selected_metrics["trades"] >= max(100, min_test_events),
        "positive_mean_net_pips": selected_metrics["mean_net_pips"] > 0.0,
        "profit_factor": selected_metrics["profit_factor"] >= 1.10,
        "auc": holdout_classification["auc"] >= 0.52,
        "bootstrap_lower_positive": bootstrap_lower > 0.0,
    }
    report = {
        "status": "evaluated",
        "model": name,
        "walk_forward": {
            "folds": folds,
            "mean_auc": float(np.mean([fold["auc"] for fold in folds])),
            "minimum_auc": float(np.min([fold["auc"] for fold in folds])),
            "mean_brier": float(np.mean([fold["brier"] for fold in folds])),
            "mean_net_pips": float(
                np.mean([fold["all_action_metrics"]["mean_net_pips"] for fold in folds])
            ),
        },
        "holdout": {
            "train_events": train["event_id"].nunique(),
            "calibration_events": calibration["event_id"].nunique(),
            "holdout_events": holdout["event_id"].nunique(),
            "selected_threshold": selected_threshold,
            "threshold_selection": threshold_selection,
            "classification": holdout_classification,
            "all_actions": trade_metrics(holdout_actions),
            "all_prediction_cell_metrics": prediction_cell_metrics(
                holdout, holdout_probability
            ),
            "selected": selected_metrics,
            "cell_metrics": cell_metrics(selected),
            "gate_checks_before_baseline_comparison": gate_checks,
            "passed_before_baseline_comparison": all(gate_checks.values()),
        },
    }
    artifact = {
        "schema_version": 1,
        "model": name,
        "feature_version": panel.FEATURE_VERSION,
        "numeric_features": numeric,
        "categorical_features": categorical,
        "target": panel.TARGET_COLUMN,
        "selected_threshold": selected_threshold,
        "estimator": final_model,
        "calibrator": calibrator,
        "trained_utc": utc_iso(),
        "execution_policy": "shadow_only",
        "training_coverage": {
            "input_timeframes": sorted(frame["input_timeframe"].unique().tolist()),
            "horizons_sec": sorted(int(value) for value in frame["horizon_sec"].unique()),
            "cells": [
                {
                    "input_timeframe": str(timeframe),
                    "horizon_sec": int(horizon),
                }
                for timeframe, horizon in sorted(
                    {
                        (str(row.input_timeframe), int(row.horizon_sec))
                        for row in frame[["input_timeframe", "horizon_sec"]]
                        .drop_duplicates()
                        .itertuples(index=False)
                    }
                )
            ],
            "instruments": sorted(frame["instrument"].unique().tolist()),
            "start_utc": frame["prediction_time_utc"].min().isoformat(),
            "end_utc": frame["prediction_time_utc"].max().isoformat(),
        },
    }
    return artifact, report


def run_benchmark(
    panel_paths: list[Path],
    models: list[str],
    max_events: int = 0,
    min_train_events: int = 500,
    min_test_events: int = 100,
    persist_artifacts: bool = True,
    report_root: Path | None = None,
    model_root: Path | None = None,
) -> dict[str, Any]:
    report_root = Path(REPORT_ROOT if report_root is None else report_root)
    model_root = Path(MODEL_ROOT if model_root is None else model_root)
    frame, dataset = load_panels(panel_paths, max_events=max_events)
    results: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    requested_models = list(dict.fromkeys(models))
    canonical_models = canonical_model_names(requested_models)
    for name in canonical_models:
        print(f"[shared-benchmark] evaluating={name} events={dataset['events']}", flush=True)
        try:
            artifact, result = evaluate_model(
                name,
                frame,
                min_train_events=min_train_events,
                min_test_events=min_test_events,
            )
            if persist_artifacts:
                artifact_path = model_root / f"{name}_shared_panel_latest.joblib"
                atomic_joblib_dump(artifact, artifact_path)
                artifact_record = {
                    "model": name,
                    "path": str(artifact_path),
                    "bytes": artifact_path.stat().st_size,
                    "sha256": sha256_file(artifact_path),
                }
                result["artifact"] = artifact_record
                artifacts.append(artifact_record)
        except Exception as exc:
            result = {
                "status": "error",
                "model": name,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        results.append(result)

    evaluated = [row for row in results if row["status"] == "evaluated"]
    baseline = next((row for row in evaluated if row["model"] == "logistic_baseline"), None)
    baseline_net = (
        baseline["holdout"]["selected"]["mean_net_pips"] if baseline else None
    )
    for row in evaluated:
        selected_net = row["holdout"]["selected"]["mean_net_pips"]
        beats_baseline = baseline_net is not None and (
            row["model"] == "logistic_baseline" or selected_net > baseline_net
        )
        row["holdout"]["matched_baseline"] = {
            "model": "logistic_baseline" if baseline else None,
            "baseline_mean_net_pips": baseline_net,
            "delta_mean_net_pips": selected_net - baseline_net if baseline_net is not None else None,
            "passed": beats_baseline,
        }
        row["holdout"]["production_gate"] = {
            "passed": bool(
                row["holdout"]["passed_before_baseline_comparison"] and beats_baseline
            ),
            "account_eligible": False,
            "reason": "shared-panel challengers remain shadow-only pending independent live-paper outcomes",
        }
    ranked = sorted(
        evaluated,
        key=lambda row: (
            row["holdout"]["production_gate"]["passed"],
            row["holdout"]["selected"]["mean_net_pips"],
            row["holdout"]["classification"]["auc"],
            -row["holdout"]["classification"]["brier"],
        ),
        reverse=True,
    )
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "run_id": stamp,
        "execution_policy": "shadow_only",
        "account_wired": False,
        "dataset": dataset,
        "validation_contract": {
            "split": "time-ordered expanding walk-forward",
            "purge": "training maturity precedes test prediction start",
            "calibration": "Platt scaling and threshold selection on pre-holdout calibration rows only",
            "action_consolidation": "highest side probability per event_id",
            "costs": "observed bid/ask net pips persisted by shared panel",
        },
        "dependencies": {
            "catboost": distribution_version("catboost"),
            "ngboost": distribution_version("ngboost"),
            "pandas": distribution_version("pandas"),
            "scikit_learn": distribution_version("scikit-learn"),
        },
        "requested_models": requested_models,
        "canonical_models": canonical_models,
        "results": results,
        "ranking": [row["model"] for row in ranked],
        "research_winner": ranked[0]["model"] if ranked else None,
        "all_requested_evaluated": len(evaluated) == len(canonical_models),
        "any_production_gate_passed": any(
            row["holdout"]["production_gate"]["passed"] for row in evaluated
        ),
        "artifacts": artifacts,
    }
    report_root.mkdir(parents=True, exist_ok=True)
    timestamped = report_root / f"shared_panel_model_benchmark_{stamp}.json"
    latest = report_root / "shared_panel_model_benchmark_latest.json"
    atomic_json_dump(report, timestamped)
    atomic_json_dump(report, latest)
    report["report_paths"] = [str(timestamped), str(latest)]
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panels", type=Path, nargs="+", required=True)
    parser.add_argument("--models", nargs="+", default=list(DEFAULT_MODELS))
    parser.add_argument(
        "--target",
        choices=(panel.TARGET_COLUMN, "target_best_side"),
        default=panel.TARGET_COLUMN,
        help=(
            "Classification target. target_best_side is a direction-ranking "
            "experiment; executable net-pip gates still apply."
        ),
    )
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--min-train-events", type=int, default=500)
    parser.add_argument("--min-test-events", type=int, default=100)
    parser.add_argument("--no-artifacts", action="store_true")
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--model-root", type=Path, default=MODEL_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    panel.TARGET_COLUMN = args.target
    report = run_benchmark(
        args.panels,
        args.models,
        max_events=args.max_events,
        min_train_events=args.min_train_events,
        min_test_events=args.min_test_events,
        persist_artifacts=not args.no_artifacts,
        report_root=args.report_root,
        model_root=args.model_root,
    )
    print(
        json.dumps(
            {
                "dataset_events": report["dataset"]["events"],
                "ranking": report["ranking"],
                "all_requested_evaluated": report["all_requested_evaluated"],
                "any_production_gate_passed": report["any_production_gate_passed"],
                "reports": report["report_paths"],
            },
            indent=2,
        )
    )
    return 0 if report["all_requested_evaluated"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
