#!/usr/bin/env python3
"""Backtest runnable model-gap adapters on observed, spread-aware FX outcomes.

This runner is deliberately shadow-only.  A completed backtest is evidence that
an adapter ran under the common market contract; it is not permission to trade.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    from oanda_profit_factor_contract_v2 import CONTRACT, factor_fields, profit_factor_at_least, pooled_profit_factor
except ModuleNotFoundError:
    from trad.oanda_profit_factor_contract_v2 import CONTRACT, factor_fields, profit_factor_at_least, pooled_profit_factor

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

try:
    import oanda_cross_pair_graph_adapter as graph_adapter
    import oanda_decision_model_adapters as decision_adapter
    import oanda_neuralforecast_family_adapters as neural_adapter
    import oanda_representation_learning_adapters as representation_adapter
    import oanda_shared_panel_model_benchmark as shared_benchmark
    import oanda_shared_timeframe_horizon_panel as panel
    import oanda_state_space_adapters as state_space_adapter
except ModuleNotFoundError:
    from trad import oanda_cross_pair_graph_adapter as graph_adapter
    from trad import oanda_decision_model_adapters as decision_adapter
    from trad import oanda_neuralforecast_family_adapters as neural_adapter
    from trad import oanda_representation_learning_adapters as representation_adapter
    from trad import oanda_shared_panel_model_benchmark as shared_benchmark
    from trad import oanda_shared_timeframe_horizon_panel as panel
    from trad import oanda_state_space_adapters as state_space_adapter


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "modern_model_gap"
DEFAULT_REPORT = REPORT_ROOT / "remaining_market_validation_latest.json"

FORECAST_MODELS = ("patchtst", "nbeats", "nhits", "lstm", "gru", "tcn")
GRAPH_MODELS = ("temporal_cross_pair_gnn",)
STATE_SPACE_MODELS = ("s4",)
DECISION_MODELS = ("contextual_bandit", "ppo", "sac", "dqn")
REPRESENTATION_MODELS = (
    "self_supervised_pretraining",
    "contrastive_learning",
    "multi_task_learning",
    "transfer_learning",
    "knowledge_distillation",
)
RUNNABLE_MODELS = (
    *FORECAST_MODELS,
    *GRAPH_MODELS,
    *STATE_SPACE_MODELS,
    *DECISION_MODELS,
    *REPRESENTATION_MODELS,
)

SMOKE_FORECAST_CELLS = (
    ("S5", 30),
    ("M1", 300),
    ("M15", 3600),
    *panel.REQUIRED_LONG_CELLS,
)
DEFAULT_FORECAST_CELLS = tuple(
    (timeframe, horizon)
    for timeframe in panel.CANONICAL_TIMEFRAMES
    for horizon in panel.CANONICAL_HORIZONS_SEC
)


def parse_forecast_cells(value: str) -> tuple[tuple[str, int], ...]:
    cells: list[tuple[str, int]] = []
    for item in value.split(","):
        timeframe, separator, horizon = item.strip().partition(":")
        if not separator:
            raise argparse.ArgumentTypeError(
                "forecast cells must use TIMEFRAME:HORIZON_SEC"
            )
        cell = (timeframe.upper(), int(horizon))
        if cell[0] not in panel.CANONICAL_TIMEFRAMES:
            raise argparse.ArgumentTypeError(f"unsupported timeframe: {cell[0]}")
        if cell[1] not in panel.CANONICAL_HORIZONS_SEC:
            raise argparse.ArgumentTypeError(f"unsupported horizon: {cell[1]}")
        if cell not in cells:
            cells.append(cell)
    if not cells:
        raise argparse.ArgumentTypeError("at least one forecast cell is required")
    return tuple(cells)

MARKET_COLUMNS = tuple(
    dict.fromkeys(
        (
            "event_id",
            "side_id",
            "decision_candle_utc",
            "prediction_time_utc",
            "maturity_time_utc",
            "max_feature_origin_utc",
            "instrument",
            "input_timeframe",
            "input_timeframe_seconds",
            "horizon_sec",
            "direction",
            "side_sign",
            "source",
            "spread_mode",
            "feature_version",
            "entry_spread_pips",
            "realized_net_pips",
            "opposite_net_pips",
            "realized_edge_pips",
            "market_mid_move_pips",
            "realized_directional_move_pips",
            "target_best_side",
            panel.TARGET_COLUMN,
            *panel.MODEL_NUMERIC_FEATURES,
            *panel.MODEL_CATEGORICAL_FEATURES,
        )
    )
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def distribution_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json_dump(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _manifest_sha(path: Path) -> str:
    manifest = path.with_suffix(path.suffix + ".manifest.json")
    if manifest.is_file():
        payload = json.loads(manifest.read_text(encoding="utf-8-sig"))
        value = str((payload.get("panel") or {}).get("sha256") or "")
        if value:
            return value
    return sha256_file(path)


def _canonical_cell_count(frame: pd.DataFrame) -> int:
    return int(frame[["input_timeframe", "horizon_sec"]].drop_duplicates().shape[0])


def _pair_cell_count(frame: pd.DataFrame) -> int:
    return int(
        frame[["instrument", "input_timeframe", "horizon_sec"]]
        .drop_duplicates()
        .shape[0]
    )


def load_bounded_market_panel(
    paths: Iterable[Path],
    events_per_pair_cell: int = 64,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load contiguous tails per pair/cell without retaining the full 8M-row panel."""
    if events_per_pair_cell < 16:
        raise ValueError("events_per_pair_cell must be at least 16")
    parts: list[pd.DataFrame] = []
    sources: list[dict[str, Any]] = []
    for raw_path in paths:
        path = raw_path.resolve()
        frame = pd.read_parquet(path, columns=list(MARKET_COLUMNS))
        panel.validate_panel(frame)
        long_rows = frame[frame["direction"] == "LONG"].sort_values(
            ["prediction_time_utc", "instrument", "horizon_sec"]
        )
        selected = (
            long_rows.groupby(
                ["instrument", "input_timeframe", "horizon_sec"],
                observed=True,
                group_keys=False,
            )
            .tail(events_per_pair_cell)["event_id"]
            .astype(str)
        )
        selected_ids = set(selected)
        bounded = frame[frame["event_id"].astype(str).isin(selected_ids)].copy()
        parts.append(bounded)
        sources.append(
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": _manifest_sha(path),
                "source_rows": int(len(frame)),
                "sampled_rows": int(len(bounded)),
                "sampled_events": int(bounded["event_id"].nunique()),
            }
        )
        del frame, bounded
    if not parts:
        raise ValueError("at least one shared-panel parquet is required")
    combined = pd.concat(parts, ignore_index=True)
    combined = combined.drop_duplicates("side_id", keep="last")
    combined = combined.sort_values(
        ["prediction_time_utc", "instrument", "input_timeframe", "horizon_sec", "direction"]
    ).reset_index(drop=True)
    panel.validate_panel(combined)
    dataset = {
        "sampling": "contiguous tail per instrument/timeframe/horizon cell",
        "events_per_pair_cell_cap": int(events_per_pair_cell),
        "rows": int(len(combined)),
        "events": int(combined["event_id"].nunique()),
        "instruments": int(combined["instrument"].nunique()),
        "timeframe_horizon_cells": _canonical_cell_count(combined),
        "pair_cells": _pair_cell_count(combined),
        "expected_timeframe_horizon_cells": len(panel.CANONICAL_TIMEFRAMES)
        * len(panel.CANONICAL_HORIZONS_SEC),
        "expected_pair_cells": 68
        * len(panel.CANONICAL_TIMEFRAMES)
        * len(panel.CANONICAL_HORIZONS_SEC),
        "timeframes": sorted(combined["input_timeframe"].astype(str).unique()),
        "horizons_sec": sorted(int(value) for value in combined["horizon_sec"].unique()),
        "start_utc": pd.Timestamp(combined["prediction_time_utc"].min()).isoformat(),
        "end_utc": pd.Timestamp(combined["prediction_time_utc"].max()).isoformat(),
        "sources": sources,
    }
    return combined, dataset


def event_table(frame: pd.DataFrame) -> pd.DataFrame:
    events = frame[frame["direction"] == "LONG"].copy()
    if events["event_id"].duplicated().any():
        raise ValueError("event table contains duplicate event_id values")
    events = events.rename(
        columns={"realized_net_pips": "long_net_pips", "opposite_net_pips": "short_net_pips"}
    )
    events["target_up"] = (events["market_mid_move_pips"].astype(float) > 0.0).astype(int)
    return events.sort_values(["prediction_time_utc", "event_id"]).reset_index(drop=True)


def _time_at_fraction(frame: pd.DataFrame, fraction: float) -> pd.Timestamp:
    times = (
        frame["prediction_time_utc"].drop_duplicates().sort_values().reset_index(drop=True)
    )
    if times.empty:
        raise ValueError("cannot split an empty event table")
    return pd.Timestamp(times.iloc[min(len(times) - 1, int(len(times) * fraction))])


def temporal_event_split(events: pd.DataFrame) -> dict[str, pd.DataFrame]:
    calibration_start = _time_at_fraction(events, 0.70)
    holdout_start = _time_at_fraction(events, 0.84)
    train = events[events["maturity_time_utc"] < calibration_start].copy()
    calibration = events[
        (events["prediction_time_utc"] >= calibration_start)
        & (events["maturity_time_utc"] < holdout_start)
    ].copy()
    holdout = events[events["prediction_time_utc"] >= holdout_start].copy()
    if min(len(train), len(calibration), len(holdout)) < 32:
        raise ValueError("maturity-purged train/calibration/holdout partitions are too small")
    if not train["maturity_time_utc"].max() < calibration["prediction_time_utc"].min():
        raise AssertionError("training labels overlap calibration prediction time")
    return {
        "train": train,
        "calibration": calibration,
        "holdout": holdout,
        "calibration_start_utc": calibration_start,
        "holdout_start_utc": holdout_start,
    }


def _sigmoid(value: np.ndarray) -> np.ndarray:
    clipped = np.clip(np.asarray(value, dtype=float), -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def classification_metrics(target: np.ndarray, probability: np.ndarray) -> dict[str, Any]:
    target = np.asarray(target, dtype=int)
    probability = np.clip(np.asarray(probability, dtype=float), 1e-6, 1.0 - 1e-6)
    return {
        "auc": float(roc_auc_score(target, probability)) if len(np.unique(target)) == 2 else None,
        "brier": float(brier_score_loss(target, probability)),
        "log_loss": float(log_loss(target, probability, labels=[0, 1])),
        "accuracy_at_0_5": float(np.mean((probability >= 0.5) == target)),
        "positive_rate": float(np.mean(target)),
    }


def score_exposure(
    events: pd.DataFrame,
    exposure: np.ndarray,
    confidence: np.ndarray | None = None,
) -> pd.DataFrame:
    scored = events.copy().reset_index(drop=True)
    values = np.clip(np.asarray(exposure, dtype=float).reshape(-1), -1.0, 1.0)
    if len(values) != len(scored):
        raise ValueError("exposure length does not match events")
    scored["exposure"] = values
    scored["confidence"] = (
        np.abs(values)
        if confidence is None
        else np.clip(np.asarray(confidence, dtype=float).reshape(-1), 0.0, 1.0)
    )
    long_reward = scored["long_net_pips"].to_numpy(float)
    short_reward = scored["short_net_pips"].to_numpy(float)
    scored["realized_net_pips"] = np.where(
        values >= 0.0,
        values * long_reward,
        (-values) * short_reward,
    )
    scored["predicted_direction"] = np.sign(values)
    return scored


def execution_metrics(scored: pd.DataFrame) -> dict[str, Any]:
    for column in ("exposure", "realized_net_pips", "market_mid_move_pips", "predicted_direction"):
        if not np.isfinite(scored[column].to_numpy(float)).all():
            raise ValueError("nonfinite_execution_metric_input")
    active = scored[np.abs(scored["exposure"].to_numpy(float)) > 1e-9]
    pips = active["realized_net_pips"].to_numpy(float)
    if len(active):
        equity = np.cumsum(pips)
        drawdown = np.maximum.accumulate(np.insert(equity, 0, 0.0))[1:] - equity
        truth = np.sign(active["market_mid_move_pips"].to_numpy(float))
        direction_accuracy = float(np.mean(active["predicted_direction"].to_numpy(float) == truth))
    else:
        drawdown = np.asarray([], dtype=float)
        direction_accuracy = 0.0
    gross_win = float(np.sum(pips[pips > 0.0]))
    gross_loss = float(abs(np.sum(pips[pips < 0.0])))
    return {
        "events": int(len(scored)),
        "trades": int(len(active)),
        "trade_rate": float(len(active) / len(scored)) if len(scored) else 0.0,
        "direction_accuracy": direction_accuracy,
        "win_rate": float(np.mean(pips > 0.0)) if len(pips) else 0.0,
        "mean_net_pips": float(np.mean(pips)) if len(pips) else 0.0,
        "median_net_pips": float(np.median(pips)) if len(pips) else 0.0,
        "sum_net_pips": float(np.sum(pips)) if len(pips) else 0.0,
        **factor_fields(gross_win, gross_loss),
        "max_drawdown_pips": float(np.max(drawdown)) if len(drawdown) else 0.0,
        "mean_abs_exposure": float(np.mean(np.abs(active["exposure"]))) if len(active) else 0.0,
    }


def execution_cell_metrics(scored: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (timeframe, horizon), group in scored.groupby(
        ["input_timeframe", "horizon_sec"], observed=True
    ):
        rows.append(
            {
                "input_timeframe": str(timeframe),
                "horizon_sec": int(horizon),
                **execution_metrics(group),
            }
        )
    return sorted(
        rows,
        key=lambda row: (panel.parse_timeframe(row["input_timeframe"]), row["horizon_sec"]),
    )


def execution_stability_metrics(scored: pd.DataFrame) -> dict[str, Any]:
    cell_rows = execution_cell_metrics(scored)
    active_cells = [row for row in cell_rows if row["trades"] > 0]
    positive_cells = [row for row in active_cells if row["sum_net_pips"] > 0.0]
    timeframe_rows = [
        {
            "input_timeframe": str(timeframe),
            **execution_metrics(group),
        }
        for timeframe, group in scored.groupby("input_timeframe", observed=True)
    ]
    active_timeframes = [row for row in timeframe_rows if row["trades"] > 0]
    positive_timeframes = [
        row for row in active_timeframes if row["sum_net_pips"] > 0.0
    ]
    positive_sums = sorted(
        (float(row["sum_net_pips"]) for row in positive_cells), reverse=True
    )
    positive_total = float(sum(positive_sums))
    top_five_positive = float(sum(positive_sums[:5]))
    return {
        "cells": len(cell_rows),
        "active_cells": len(active_cells),
        "active_cell_fraction": (
            float(len(active_cells) / len(cell_rows)) if cell_rows else 0.0
        ),
        "positive_active_cells": len(positive_cells),
        "positive_active_cell_fraction": (
            float(len(positive_cells) / len(active_cells)) if active_cells else 0.0
        ),
        "median_active_cell_mean_net_pips": (
            float(np.median([row["mean_net_pips"] for row in active_cells]))
            if active_cells
            else 0.0
        ),
        "timeframes": len(timeframe_rows),
        "active_timeframes": len(active_timeframes),
        "positive_active_timeframes": len(positive_timeframes),
        "positive_active_timeframe_fraction": (
            float(len(positive_timeframes) / len(active_timeframes))
            if active_timeframes
            else 0.0
        ),
        "top_five_positive_net_concentration": (
            top_five_positive / positive_total if positive_total > 0.0 else 0.0
        ),
    }


def choose_confidence_threshold(
    events: pd.DataFrame,
    exposure: np.ndarray,
    confidence: np.ndarray,
    minimum_trades: int = 32,
) -> tuple[float, list[dict[str, Any]]]:
    base = score_exposure(events, exposure, confidence)
    rows: list[dict[str, Any]] = []
    for threshold in np.arange(0.50, 0.901, 0.05):
        candidate = base.copy()
        candidate.loc[candidate["confidence"] < threshold, "exposure"] = 0.0
        candidate.loc[candidate["confidence"] < threshold, "predicted_direction"] = 0.0
        candidate.loc[candidate["confidence"] < threshold, "realized_net_pips"] = 0.0
        rows.append({"threshold": float(threshold), **execution_metrics(candidate)})
    viable = [
        row
        for row in rows
        if row["trades"] >= minimum_trades
        and row["mean_net_pips"] > 0.0
        and profit_factor_at_least(row, 1.05)
    ]
    if not viable:
        return 0.70, rows
    winner = max(viable, key=lambda row: row["mean_net_pips"] * math.sqrt(row["trades"]))
    return float(winner["threshold"]), rows


def apply_confidence_threshold(scored: pd.DataFrame, threshold: float) -> pd.DataFrame:
    selected = scored.copy()
    blocked = selected["confidence"] < threshold
    selected.loc[blocked, "exposure"] = 0.0
    selected.loc[blocked, "predicted_direction"] = 0.0
    selected.loc[blocked, "realized_net_pips"] = 0.0
    return selected


def _validation_block(
    scored: pd.DataFrame,
    classification: dict[str, Any] | None,
    *,
    minimum_events: int,
    minimum_instruments: int,
    minimum_cells: int,
) -> dict[str, Any]:
    metrics = execution_metrics(scored)
    stability = execution_stability_metrics(scored)
    integrity = {
        "minimum_events": len(scored) >= minimum_events,
        "minimum_instruments": scored["instrument"].nunique() >= minimum_instruments,
        "minimum_cells": _canonical_cell_count(scored) >= minimum_cells,
        "finite_costed_outcomes": bool(np.isfinite(scored["realized_net_pips"]).all()),
        "feature_provenance": bool(
            (scored["max_feature_origin_utc"] <= scored["prediction_time_utc"]).all()
        ),
    }
    performance = {
        "minimum_trades": metrics["trades"] >= max(100, minimum_events // 5),
        "positive_mean_net_pips": metrics["mean_net_pips"] > 0.0,
        "profit_factor": profit_factor_at_least(metrics, 1.10),
        "auc": bool(classification and classification.get("auc") is not None and classification["auc"] >= 0.52),
        "broad_cell_activity": stability["active_cell_fraction"] >= 0.75,
        "positive_cell_majority": stability["positive_active_cell_fraction"] >= 0.55,
        "positive_timeframe_majority": (
            stability["positive_active_timeframe_fraction"] >= 0.60
        ),
    }
    return {
        "integrity_checks": integrity,
        "backtest_valid": all(integrity.values()),
        "performance_checks": performance,
        "stability": stability,
        "performance_passed": all(performance.values()),
        "production_eligible": False,
        "account_wired": False,
        "reason": "bounded market evidence remains shadow-only pending independent live-paper validation",
    }


@dataclass(frozen=True)
class ForecastFixture:
    neural_frame: pd.DataFrame
    metadata: pd.DataFrame
    train_rows: int
    forecast_steps: int
    calibration_ds_min: int
    holdout_ds_min: int
    input_size: int
    cells: tuple[tuple[str, int], ...]


def prepare_forecast_fixture(
    events: pd.DataFrame,
    cells: Sequence[tuple[str, int]] = DEFAULT_FORECAST_CELLS,
    series_points: int = 48,
    score_steps: int = 4,
    input_size: int = 12,
) -> ForecastFixture:
    requested_index = pd.MultiIndex.from_tuples(
        [(str(timeframe), int(horizon)) for timeframe, horizon in cells],
        names=["input_timeframe", "horizon_sec"],
    )
    event_index = pd.MultiIndex.from_arrays(
        [
            events["input_timeframe"].astype(str),
            pd.to_numeric(events["horizon_sec"], errors="coerce").fillna(0).astype(int),
        ],
        names=requested_index.names,
    )
    selected = events[event_index.isin(requested_index)].copy()
    selected["unique_id"] = (
        selected["instrument"].astype(str)
        + "|"
        + selected["input_timeframe"].astype(str)
        + "|"
        + selected["horizon_sec"].astype(str)
    )
    selected = selected.sort_values(["unique_id", "prediction_time_utc"])
    eligible = selected.groupby("unique_id", observed=True).size()
    eligible_ids = set(eligible[eligible >= series_points].index.astype(str))
    selected = selected[selected["unique_id"].isin(eligible_ids)]
    selected = (
        selected.groupby("unique_id", observed=True, group_keys=False)
        .tail(series_points)
        .sort_values(["unique_id", "prediction_time_utc"])
        .reset_index(drop=True)
    )
    if selected.empty:
        raise ValueError("no forecast series has enough contiguous market observations")
    represented_cells = set(
        zip(selected["input_timeframe"].astype(str), selected["horizon_sec"].astype(int))
    )
    missing_cells = [cell for cell in cells if cell not in represented_cells]
    if missing_cells:
        raise ValueError(f"forecast fixture is missing cells: {missing_cells}")
    selected["ds"] = selected.groupby("unique_id", observed=True).cumcount().astype(int)
    score_start = series_points - score_steps
    train_counts: list[int] = []
    for _, group in selected.groupby("unique_id", observed=True, sort=False):
        score_time = pd.Timestamp(group.loc[group["ds"] == score_start, "prediction_time_utc"].iloc[0])
        matured = group[group["maturity_time_utc"] < score_time]
        train_counts.append(int(matured["ds"].max()) + 1 if not matured.empty else 0)
    train_rows = min(train_counts)
    forecast_steps = series_points - train_rows
    if train_rows <= input_size + 4:
        raise ValueError("maturity purge leaves too little forecast training history")
    if forecast_steps < score_steps:
        raise AssertionError("forecast bridge is shorter than scored horizon")
    train = selected[selected["ds"] < train_rows]
    scored = selected[selected["ds"] >= score_start]
    scored_starts = scored.groupby("unique_id", observed=True)["prediction_time_utc"].min()
    train_ends = train.groupby("unique_id", observed=True)["maturity_time_utc"].max()
    aligned = pd.concat(
        [train_ends.rename("train_maturity"), scored_starts.rename("score_prediction")],
        axis=1,
        join="inner",
    )
    if len(aligned) != selected["unique_id"].nunique() or not (
        aligned["train_maturity"] < aligned["score_prediction"]
    ).all():
        raise AssertionError("forecast training labels are not mature before scored predictions")
    neural_frame = selected[["unique_id", "ds", "market_mid_move_pips"]].rename(
        columns={"market_mid_move_pips": "y"}
    )
    return ForecastFixture(
        neural_frame=neural_frame,
        metadata=selected,
        train_rows=train_rows,
        forecast_steps=forecast_steps,
        calibration_ds_min=score_start,
        holdout_ds_min=series_points - score_steps // 2,
        input_size=input_size,
        cells=tuple(cells),
    )


def _forecast_probability(prediction: np.ndarray, train_values: np.ndarray) -> np.ndarray:
    scale = float(np.nanmedian(np.abs(train_values)))
    if not math.isfinite(scale) or scale < 0.1:
        scale = max(0.1, float(np.nanstd(train_values)))
    return _sigmoid(np.asarray(prediction, dtype=float) / scale)


def _forecast_result(
    model_name: str,
    fixture: ForecastFixture,
    prediction: pd.DataFrame,
    prediction_column: str,
    *,
    provider_runtime: str,
    minimum_events: int = 300,
    minimum_instruments: int = 50,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scored = fixture.metadata.merge(
        prediction[["unique_id", "ds", prediction_column]],
        on=["unique_id", "ds"],
        how="inner",
        validate="one_to_one",
    )
    scored = scored[scored["ds"] >= fixture.calibration_ds_min].copy()
    expected = fixture.metadata[fixture.metadata["ds"] >= fixture.calibration_ds_min]
    if len(scored) != len(expected):
        raise RuntimeError("forecast output does not cover every scored market event")
    train_values = fixture.neural_frame[fixture.neural_frame["ds"] < fixture.train_rows]["y"].to_numpy(float)
    probability = _forecast_probability(scored[prediction_column].to_numpy(float), train_values)
    exposure = np.where(probability >= 0.5, 1.0, -1.0)
    confidence = np.maximum(probability, 1.0 - probability)
    calibration_mask = scored["ds"] < fixture.holdout_ds_min
    threshold, threshold_table = choose_confidence_threshold(
        scored[calibration_mask], exposure[calibration_mask], confidence[calibration_mask]
    )
    holdout = scored[~calibration_mask].copy()
    holdout_probability = probability[~calibration_mask]
    holdout_scored = apply_confidence_threshold(
        score_exposure(holdout, exposure[~calibration_mask], confidence[~calibration_mask]),
        threshold,
    )
    classification = classification_metrics(holdout["target_up"], holdout_probability)
    validation = _validation_block(
        holdout_scored,
        classification,
        minimum_events=minimum_events,
        minimum_instruments=minimum_instruments,
        minimum_cells=len(fixture.cells),
    )
    return {
        "model": model_name,
        "status": "bounded_market_scored" if validation["backtest_valid"] else "market_validation_failed",
        "provider_runtime": provider_runtime,
        "backtest_scope": {
            "cells": [
                {"input_timeframe": timeframe, "horizon_sec": horizon}
                for timeframe, horizon in fixture.cells
            ],
            "series": int(fixture.metadata["unique_id"].nunique()),
            "train_rows_per_series": fixture.train_rows,
            "forecast_bridge_steps": fixture.forecast_steps,
            "calibration_events": int(calibration_mask.sum()),
            "holdout_events": int((~calibration_mask).sum()),
            "maturity_purged": True,
        },
        "classification": classification,
        "selected_threshold": threshold,
        "calibration_thresholds": threshold_table,
        "all_actions": execution_metrics(
            score_exposure(holdout, exposure[~calibration_mask], confidence[~calibration_mask])
        ),
        "selected": execution_metrics(holdout_scored),
        "cells": execution_cell_metrics(holdout_scored),
        "validation": validation,
        "production_gate": {
            "passed": False,
            "account_eligible": False,
            "reason": validation["reason"],
        },
        "account_wired": False,
        "production_eligible": False,
        **(extra or {}),
    }


def evaluate_neural_forecaster(
    name: str,
    fixture: ForecastFixture,
    max_steps: int,
) -> dict[str, Any]:
    from neuralforecast import NeuralForecast

    train = fixture.neural_frame[fixture.neural_frame["ds"] < fixture.train_rows]
    model = neural_adapter.make_model(
        name,
        horizon=fixture.forecast_steps,
        input_size=fixture.input_size,
        max_steps=max_steps,
    )
    engine = NeuralForecast(models=[model], freq=1)
    engine.fit(
        df=train,
        val_size=max(fixture.forecast_steps, min(4, fixture.train_rows // 5)),
    )
    forecast = engine.predict().reset_index()
    prediction_column = name
    if prediction_column not in forecast:
        candidates = [column for column in forecast if column not in {"unique_id", "ds"}]
        if len(candidates) != 1:
            raise RuntimeError(f"cannot identify forecast column for {name}: {candidates}")
        prediction_column = candidates[0]
    return _forecast_result(
        name,
        fixture,
        forecast,
        prediction_column,
        provider_runtime="neuralforecast",
        extra={"max_steps": int(max_steps)},
    )


def evaluate_cross_pair_graph(
    events: pd.DataFrame,
    max_steps: int,
    cell: tuple[str, int] = ("M1", 300),
) -> dict[str, Any]:
    from neuralforecast import NeuralForecast

    fixture = prepare_forecast_fixture(events, cells=(cell,))
    train_meta = fixture.metadata[fixture.metadata["ds"] < fixture.train_rows]
    returns = train_meta[
        ["prediction_time_utc", "instrument", "market_mid_move_pips"]
    ].rename(
        columns={"prediction_time_utc": "timestamp_utc", "market_mid_move_pips": "return_pips"}
    )
    as_of = fixture.metadata[
        fixture.metadata["ds"] >= fixture.calibration_ds_min
    ]["prediction_time_utc"].min()
    snapshot = graph_adapter.build_lagged_graph(
        returns,
        pd.Timestamp(as_of),
        instruments=sorted(train_meta["instrument"].unique()),
        lookback_rows=max(24, fixture.train_rows),
        min_observations=min(24, fixture.train_rows - 1),
    )
    train = fixture.neural_frame[fixture.neural_frame["ds"] < fixture.train_rows].copy()
    train["unique_id"] = train["unique_id"].str.split("|", regex=False).str[0]
    metadata = fixture.metadata.copy()
    metadata["unique_id"] = metadata["instrument"].astype(str)
    graph_fixture = ForecastFixture(
        neural_frame=pd.concat(
            [
                train,
                fixture.neural_frame[fixture.neural_frame["ds"] >= fixture.train_rows].assign(
                    unique_id=lambda value: value["unique_id"].str.split("|", regex=False).str[0]
                ),
            ],
            ignore_index=True,
        ),
        metadata=metadata,
        train_rows=fixture.train_rows,
        forecast_steps=fixture.forecast_steps,
        calibration_ds_min=fixture.calibration_ds_min,
        holdout_ds_min=fixture.holdout_ds_min,
        input_size=fixture.input_size,
        cells=fixture.cells,
    )
    model = graph_adapter.make_stemgnn_model(
        sorted(train_meta["instrument"].unique()),
        horizon=fixture.forecast_steps,
        input_size=fixture.input_size,
        max_steps=max_steps,
    )
    engine = NeuralForecast(models=[model], freq=1)
    engine.fit(
        df=train,
        val_size=max(fixture.forecast_steps, min(4, fixture.train_rows // 5)),
    )
    forecast = engine.predict().reset_index()
    prediction_column = "stemgnn"
    if prediction_column not in forecast:
        candidates = [column for column in forecast if column not in {"unique_id", "ds"}]
        if len(candidates) != 1:
            raise RuntimeError(f"cannot identify StemGNN forecast column: {candidates}")
        prediction_column = candidates[0]
    return _forecast_result(
        "temporal_cross_pair_gnn",
        graph_fixture,
        forecast,
        prediction_column,
        provider_runtime="neuralforecast.StemGNN",
        minimum_events=100,
        minimum_instruments=60,
        extra={
            "max_steps": int(max_steps),
            "lagged_graph": snapshot.to_dict(),
            "adjacency_shape": list(graph_adapter.adjacency_matrix(snapshot).shape),
        },
    )


def combine_cell_results(model: str, results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Combine independently fitted cell runs without hiding a failed cell."""
    cells = [cell for result in results for cell in result.get("cells") or []]
    valid = bool(results) and all(
        result.get("status") == "bounded_market_scored"
        and (result.get("validation") or {}).get("backtest_valid")
        for result in results
    )
    trades = sum(int(cell.get("trades") or 0) for cell in cells)
    events = sum(int(cell.get("events") or 0) for cell in cells)
    sum_net = sum(float(cell.get("sum_net_pips") or 0.0) for cell in cells)

    def weighted(field: str, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return float(
            sum(
                float(cell.get(field) or 0.0) * int(cell.get("trades") or 0)
                for cell in cells
            )
            / denominator
        )

    selected = {
        "events": events,
        "trades": trades,
        "trade_rate": float(trades / events) if events else 0.0,
        "direction_accuracy": weighted("direction_accuracy", trades),
        "win_rate": weighted("win_rate", trades),
        "mean_net_pips": float(sum_net / trades) if trades else 0.0,
        "median_net_pips": None,
        "median_net_pips_status": "unavailable_without_pooled_outcomes",
        "sum_net_pips": float(sum_net),
        **pooled_profit_factor(cells),
        "max_drawdown_pips": None,
        "max_drawdown_pips_status": "unavailable_without_ordered_portfolio_path",
        "maximum_cell_drawdown_pips": max(
            (float(cell.get("max_drawdown_pips") or 0.0) for cell in cells),
            default=0.0,
        ),
    }
    return {
        "model": model,
        "status": "bounded_market_scored" if valid else "market_validation_failed",
        "provider_runtime": "neuralforecast.StemGNN",
        "backtest_scope": {
            "cells": [
                {
                    "input_timeframe": cell["input_timeframe"],
                    "horizon_sec": int(cell["horizon_sec"]),
                }
                for cell in cells
            ],
            "independent_cell_fits": len(results),
            "maturity_purged": True,
        },
        "selected": selected,
        "cells": cells,
        "validation": {
            "backtest_valid": valid,
            "performance_passed": False,
            "production_eligible": False,
            "account_wired": False,
            "reason": (
                "all required cells completed; aggregate remains shadow-only"
                if valid
                else "one or more required cells failed bounded market validation"
            ),
        },
        "cell_runs": [
            {
                "status": result.get("status"),
                "backtest_scope": result.get("backtest_scope"),
                "selected": result.get("selected"),
                "validation": result.get("validation"),
            }
            for result in results
        ],
        "production_gate": {
            "passed": False,
            "account_eligible": False,
            "reason": "bounded long-horizon grid remains shadow-only",
        },
        "account_wired": False,
        "production_eligible": False,
    }


def evaluate_cross_pair_graph_matrix(
    events: pd.DataFrame,
    max_steps: int,
    cells: Sequence[tuple[str, int]],
) -> dict[str, Any]:
    """Evaluate graph cells from one grouped index without repeated panel scans."""

    grouped = events.groupby(
        ["input_timeframe", "horizon_sec"], observed=True, sort=False
    )
    results: list[dict[str, Any]] = []
    total_cells = len(cells)
    for cell_index, (timeframe, horizon) in enumerate(cells, start=1):
        cell = (str(timeframe), int(horizon))
        cell_started = time.monotonic()
        try:
            cell_events = grouped.get_group(cell)
            result = evaluate_cross_pair_graph(cell_events, max_steps, cell=cell)
        except Exception as exc:
            result = {
                "model": "temporal_cross_pair_gnn",
                "status": "market_validation_error",
                "backtest_scope": {
                    "cells": [
                        {"input_timeframe": cell[0], "horizon_sec": cell[1]}
                    ]
                },
                "cells": [],
                "validation": {
                    "backtest_valid": False,
                    "performance_passed": False,
                    "production_eligible": False,
                    "account_wired": False,
                    "reason": f"{type(exc).__name__}: {exc}",
                },
            }
        results.append(result)
        print(
            "[gap-market] graph_cell="
            f"{cell_index}/{total_cells} cell={cell[0]}:{cell[1]} "
            f"status={result['status']} elapsed={time.monotonic() - cell_started:.1f}s",
            flush=True,
        )
    return combine_cell_results("temporal_cross_pair_gnn", results)


@dataclass(frozen=True)
class WindowFixture:
    train_x: np.ndarray
    train_return: np.ndarray
    train_meta: pd.DataFrame
    calibration_x: np.ndarray
    calibration_return: np.ndarray
    calibration_meta: pd.DataFrame
    holdout_x: np.ndarray
    holdout_return: np.ndarray
    holdout_meta: pd.DataFrame
    numeric_features: tuple[str, ...]
    normalization_mean: np.ndarray
    normalization_std: np.ndarray
    context_steps: int
    split: dict[str, Any]


def prepare_window_fixture(
    events: pd.DataFrame,
    context_steps: int = 8,
    windows_per_series: int = 12,
) -> WindowFixture:
    if context_steps < 4 or windows_per_series < 3:
        raise ValueError("window context and per-series sample limits are too small")
    numeric = tuple(
        name
        for name in panel.MODEL_NUMERIC_FEATURES
        if name != "side_sign" and name in events and events[name].notna().any()
    )
    if not numeric:
        raise ValueError("market panel has no numeric model features")
    ordered = events.sort_values(
        ["instrument", "input_timeframe", "horizon_sec", "prediction_time_utc"]
    ).reset_index(drop=True)
    raw = ordered[list(numeric)].to_numpy(dtype=np.float32)
    windows: list[np.ndarray] = []
    target_indices: list[int] = []
    for _, group in ordered.groupby(
        ["instrument", "input_timeframe", "horizon_sec"], observed=True, sort=False
    ):
        indices = group.index.to_numpy(dtype=int)
        if len(indices) < context_steps + 2:
            continue
        candidates = np.arange(context_steps - 1, len(indices), dtype=int)
        if len(candidates) > windows_per_series:
            candidates = np.unique(
                np.linspace(0, len(candidates) - 1, windows_per_series, dtype=int)
            )
            candidates = np.arange(context_steps - 1, len(indices), dtype=int)[candidates]
        for position in candidates:
            source = indices[position - context_steps + 1 : position + 1]
            if len(source) != context_steps:
                continue
            windows.append(raw[source])
            target_indices.append(int(indices[position]))
    if not windows:
        raise ValueError("no chronological market windows could be constructed")
    x = np.stack(windows).astype(np.float32, copy=False)
    metadata = ordered.loc[target_indices].reset_index(drop=True)
    returns = metadata["market_mid_move_pips"].to_numpy(dtype=np.float32)
    split = temporal_event_split(metadata)
    train_ids = set(split["train"]["event_id"].astype(str))
    calibration_ids = set(split["calibration"]["event_id"].astype(str))
    holdout_ids = set(split["holdout"]["event_id"].astype(str))
    event_ids = metadata["event_id"].astype(str)
    train_mask = event_ids.isin(train_ids).to_numpy()
    calibration_mask = event_ids.isin(calibration_ids).to_numpy()
    holdout_mask = event_ids.isin(holdout_ids).to_numpy()
    if min(train_mask.sum(), calibration_mask.sum(), holdout_mask.sum()) < 100:
        raise ValueError("windowed market partitions do not have sufficient support")
    flattened_train = x[train_mask].reshape(-1, x.shape[-1])
    median = np.nanmedian(flattened_train, axis=0)
    median[~np.isfinite(median)] = 0.0
    x = np.where(np.isfinite(x), x, median[None, None, :])
    mean = x[train_mask].mean(axis=(0, 1), keepdims=True)
    std = x[train_mask].std(axis=(0, 1), keepdims=True)
    std[std < 1e-6] = 1.0
    x = ((x - mean) / std).astype(np.float32, copy=False)
    return WindowFixture(
        train_x=x[train_mask],
        train_return=returns[train_mask],
        train_meta=metadata[train_mask].reset_index(drop=True),
        calibration_x=x[calibration_mask],
        calibration_return=returns[calibration_mask],
        calibration_meta=metadata[calibration_mask].reset_index(drop=True),
        holdout_x=x[holdout_mask],
        holdout_return=returns[holdout_mask],
        holdout_meta=metadata[holdout_mask].reset_index(drop=True),
        numeric_features=numeric,
        normalization_mean=mean.reshape(-1),
        normalization_std=std.reshape(-1),
        context_steps=context_steps,
        split={
            "train_samples": int(train_mask.sum()),
            "calibration_samples": int(calibration_mask.sum()),
            "holdout_samples": int(holdout_mask.sum()),
            "train_maturity_end_utc": pd.Timestamp(
                metadata.loc[train_mask, "maturity_time_utc"].max()
            ).isoformat(),
            "calibration_prediction_start_utc": pd.Timestamp(
                metadata.loc[calibration_mask, "prediction_time_utc"].min()
            ).isoformat(),
            "holdout_prediction_start_utc": pd.Timestamp(
                metadata.loc[holdout_mask, "prediction_time_utc"].min()
            ).isoformat(),
            "maturity_purged": True,
            "normalization_fit": "training windows only",
        },
    )


def _bounded_indices(length: int, maximum: int) -> np.ndarray:
    if maximum <= 0 or length <= maximum:
        return np.arange(length, dtype=int)
    return np.unique(np.linspace(0, length - 1, maximum, dtype=int))


def _torch_runtime():
    import torch
    from torch import nn

    torch.manual_seed(42)
    torch.set_num_threads(max(1, min(4, os.cpu_count() or 1)))
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return torch, nn, device


def _batch_indices(length: int, batch_size: int, seed: int) -> Iterable[np.ndarray]:
    rng = np.random.default_rng(seed)
    order = rng.permutation(length)
    for start in range(0, length, batch_size):
        yield order[start : start + batch_size]


def _tensor(value: np.ndarray, torch: Any, device: Any):
    return torch.as_tensor(value, dtype=torch.float32, device=device)


def _finite_objective(value: Any) -> float:
    result = float(value.detach().cpu())
    if not math.isfinite(result):
        raise RuntimeError("market training objective became non-finite")
    return result


def _fit_direction_head(
    encoder: Any,
    train_x: np.ndarray,
    train_return: np.ndarray,
    *,
    epochs: int,
    device: Any,
    freeze_encoder: bool,
) -> tuple[Any, Any, float]:
    torch, nn, _ = _torch_runtime()
    encoder = encoder.to(device)
    if freeze_encoder:
        encoder.eval()
        for parameter in encoder.parameters():
            parameter.requires_grad = False
    hidden = int(encoder(_tensor(train_x[:1], torch, device))[1].shape[-1])
    head = nn.Linear(hidden, 1).to(device)
    parameters = list(head.parameters())
    if not freeze_encoder:
        parameters += list(encoder.parameters())
    optimizer = torch.optim.Adam(parameters, lr=1e-3)
    last_loss = torch.tensor(float("nan"), device=device)
    for epoch in range(max(1, epochs)):
        for indices in _batch_indices(len(train_x), 128, 1100 + epoch):
            x = _tensor(train_x[indices], torch, device)
            y = _tensor((train_return[indices] > 0.0).astype(np.float32), torch, device)
            if freeze_encoder:
                with torch.no_grad():
                    _, embedding = encoder(x)
            else:
                _, embedding = encoder(x)
            logits = head(embedding).squeeze(-1)
            last_loss = nn.functional.binary_cross_entropy_with_logits(logits, y)
            optimizer.zero_grad()
            last_loss.backward()
            optimizer.step()
    return encoder, head, _finite_objective(last_loss)


def _predict_direction_logits(
    encoder: Any,
    head: Any,
    values: np.ndarray,
    device: Any,
) -> np.ndarray:
    torch, _, _ = _torch_runtime()
    encoder.eval()
    head.eval()
    output: list[np.ndarray] = []
    with torch.no_grad():
        for start in range(0, len(values), 512):
            x = _tensor(values[start : start + 512], torch, device)
            _, embedding = encoder(x)
            output.append(head(embedding).squeeze(-1).detach().cpu().numpy())
    return np.concatenate(output) if output else np.asarray([], dtype=float)


def _score_probability_model(
    name: str,
    fixture: WindowFixture,
    calibration_logits: np.ndarray,
    holdout_logits: np.ndarray,
    *,
    runtime: str,
    training: dict[str, Any],
    transfer_target_only: bool = False,
) -> dict[str, Any]:
    calibration_meta = fixture.calibration_meta
    holdout_meta = fixture.holdout_meta
    if transfer_target_only:
        calibration_mask = calibration_meta["instrument"].map(_is_transfer_target).to_numpy()
        holdout_mask = holdout_meta["instrument"].map(_is_transfer_target).to_numpy()
        calibration_meta = calibration_meta[calibration_mask].reset_index(drop=True)
        holdout_meta = holdout_meta[holdout_mask].reset_index(drop=True)
        calibration_logits = calibration_logits[calibration_mask]
        holdout_logits = holdout_logits[holdout_mask]
    calibration_probability = _sigmoid(calibration_logits)
    holdout_probability = _sigmoid(holdout_logits)
    calibration_exposure = np.where(calibration_probability >= 0.5, 1.0, -1.0)
    calibration_confidence = np.maximum(calibration_probability, 1.0 - calibration_probability)
    threshold, threshold_table = choose_confidence_threshold(
        calibration_meta,
        calibration_exposure,
        calibration_confidence,
        minimum_trades=max(32, len(calibration_meta) // 20),
    )
    holdout_exposure = np.where(holdout_probability >= 0.5, 1.0, -1.0)
    holdout_confidence = np.maximum(holdout_probability, 1.0 - holdout_probability)
    all_scored = score_exposure(holdout_meta, holdout_exposure, holdout_confidence)
    selected = apply_confidence_threshold(all_scored, threshold)
    classification = classification_metrics(holdout_meta["target_up"], holdout_probability)
    validation = _validation_block(
        selected,
        classification,
        minimum_events=500,
        minimum_instruments=20 if transfer_target_only else 50,
        minimum_cells=_canonical_cell_count(fixture.train_meta),
    )
    return {
        "model": name,
        "status": "bounded_market_scored" if validation["backtest_valid"] else "market_validation_failed",
        "provider_runtime": runtime,
        "backtest_scope": {
            **fixture.split,
            "features": len(fixture.numeric_features),
            "context_steps": fixture.context_steps,
            "holdout_events_scored": int(len(holdout_meta)),
            "holdout_instruments": int(holdout_meta["instrument"].nunique()),
            "holdout_cells": _canonical_cell_count(holdout_meta),
            "transfer_target_only": transfer_target_only,
        },
        "training": training,
        "classification": classification,
        "selected_threshold": threshold,
        "calibration_thresholds": threshold_table,
        "all_actions": execution_metrics(all_scored),
        "selected": execution_metrics(selected),
        "cells": execution_cell_metrics(selected),
        "validation": validation,
        "production_gate": {
            "passed": False,
            "account_eligible": False,
            "reason": validation["reason"],
        },
        "account_wired": False,
        "production_eligible": False,
    }


def _is_transfer_target(instrument: str) -> bool:
    return sum(str(instrument).encode("ascii", errors="ignore")) % 3 == 0


def evaluate_representation_model(
    name: str,
    fixture: WindowFixture,
    *,
    epochs: int,
    max_train_samples: int,
) -> dict[str, Any]:
    torch, nn, device = _torch_runtime()
    train_indices = _bounded_indices(len(fixture.train_x), max_train_samples)
    train_x = fixture.train_x[train_indices]
    train_return = fixture.train_return[train_indices]
    features = train_x.shape[-1]
    training: dict[str, Any] = {
        "epochs": int(epochs),
        "bounded_train_samples": int(len(train_x)),
        "device": str(device),
    }

    if name == "self_supervised_pretraining":
        encoder = representation_adapter.SequenceEncoder.build(features).to(device)
        decoder = nn.Linear(16, features).to(device)
        optimizer = torch.optim.Adam([*encoder.parameters(), *decoder.parameters()], lr=1e-3)
        last_loss = torch.tensor(float("nan"), device=device)
        for epoch in range(max(1, epochs)):
            for indices in _batch_indices(len(train_x), 128, 2100 + epoch):
                x = _tensor(train_x[indices], torch, device)
                generator = torch.Generator(device="cpu").manual_seed(42 + epoch)
                mask = torch.rand(x.shape[:2], generator=generator).to(device) < 0.2
                corrupted = x.clone()
                corrupted[mask] = 0.0
                sequence, _ = encoder(corrupted)
                reconstruction = decoder(sequence)
                last_loss = (reconstruction[mask] - x[mask]).pow(2).mean()
                optimizer.zero_grad()
                last_loss.backward()
                optimizer.step()
        training["pretraining_loss"] = _finite_objective(last_loss)
        encoder, head, downstream_loss = _fit_direction_head(
            encoder,
            train_x,
            train_return,
            epochs=epochs,
            device=device,
            freeze_encoder=True,
        )
        training["downstream_loss"] = downstream_loss
    elif name == "contrastive_learning":
        encoder = representation_adapter.SequenceEncoder.build(features).to(device)
        optimizer = torch.optim.Adam(encoder.parameters(), lr=1e-3)
        last_loss = torch.tensor(float("nan"), device=device)
        for epoch in range(max(1, epochs)):
            for indices in _batch_indices(len(train_x), 64, 3100 + epoch):
                x = _tensor(train_x[indices], torch, device)
                left = x + 0.01 * torch.randn_like(x)
                right = x + 0.01 * torch.randn_like(x)
                _, left_embedding = encoder(left)
                _, right_embedding = encoder(right)
                left_embedding = nn.functional.normalize(left_embedding, dim=1)
                right_embedding = nn.functional.normalize(right_embedding, dim=1)
                logits = left_embedding @ right_embedding.T / 0.1
                labels = torch.arange(len(indices), device=device)
                last_loss = 0.5 * (
                    nn.functional.cross_entropy(logits, labels)
                    + nn.functional.cross_entropy(logits.T, labels)
                )
                optimizer.zero_grad()
                last_loss.backward()
                optimizer.step()
        training["pretraining_loss"] = _finite_objective(last_loss)
        encoder, head, downstream_loss = _fit_direction_head(
            encoder,
            train_x,
            train_return,
            epochs=epochs,
            device=device,
            freeze_encoder=True,
        )
        training["downstream_loss"] = downstream_loss
    elif name == "multi_task_learning":
        model = representation_adapter._multitask_model(features).to(device)
        optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
        return_scale = max(0.1, float(np.nanmedian(np.abs(train_return))))
        last_loss = torch.tensor(float("nan"), device=device)
        for epoch in range(max(1, epochs)):
            for indices in _batch_indices(len(train_x), 128, 4100 + epoch):
                x = _tensor(train_x[indices], torch, device)
                y = _tensor(train_return[indices] / return_scale, torch, device)
                direction, predicted_return, movement = model(x)
                last_loss = (
                    nn.functional.binary_cross_entropy_with_logits(direction, (y > 0.0).float())
                    + nn.functional.smooth_l1_loss(predicted_return, y)
                    + nn.functional.smooth_l1_loss(movement, y.abs())
                )
                optimizer.zero_grad()
                last_loss.backward()
                optimizer.step()
        training.update(
            {"objective_loss": _finite_objective(last_loss), "return_scale_pips": return_scale}
        )
        encoder = model.encoder
        head = model.direction
    elif name == "transfer_learning":
        source_mask = ~fixture.train_meta.iloc[train_indices]["instrument"].map(_is_transfer_target).to_numpy()
        target_mask = ~source_mask
        if min(source_mask.sum(), target_mask.sum()) < 100:
            raise ValueError("transfer source/target market partitions are too small")
        encoder = representation_adapter.SequenceEncoder.build(features).to(device)
        encoder, source_head, source_loss = _fit_direction_head(
            encoder,
            train_x[source_mask],
            train_return[source_mask],
            epochs=epochs,
            device=device,
            freeze_encoder=False,
        )
        encoder, head, target_loss = _fit_direction_head(
            encoder,
            train_x[target_mask],
            train_return[target_mask],
            epochs=epochs,
            device=device,
            freeze_encoder=True,
        )
        training.update(
            {
                "source_samples": int(source_mask.sum()),
                "target_samples": int(target_mask.sum()),
                "source_loss": source_loss,
                "target_loss": target_loss,
                "encoder_frozen_for_target_fit": True,
            }
        )
    elif name == "knowledge_distillation":
        teacher = representation_adapter._multitask_model(features, hidden=32).to(device)
        teacher_optimizer = torch.optim.Adam(teacher.parameters(), lr=1e-3)
        return_scale = max(0.1, float(np.nanmedian(np.abs(train_return))))
        teacher_loss = torch.tensor(float("nan"), device=device)
        for epoch in range(max(1, epochs)):
            for indices in _batch_indices(len(train_x), 128, 5100 + epoch):
                x = _tensor(train_x[indices], torch, device)
                y = _tensor(train_return[indices] / return_scale, torch, device)
                logits, predicted_return, movement = teacher(x)
                teacher_loss = (
                    nn.functional.binary_cross_entropy_with_logits(logits, (y > 0.0).float())
                    + nn.functional.smooth_l1_loss(predicted_return, y)
                    + nn.functional.smooth_l1_loss(movement, y.abs())
                )
                teacher_optimizer.zero_grad()
                teacher_loss.backward()
                teacher_optimizer.step()
        teacher.eval()
        for parameter in teacher.parameters():
            parameter.requires_grad = False
        student = representation_adapter._multitask_model(features, hidden=8).to(device)
        student_optimizer = torch.optim.Adam(student.parameters(), lr=1e-3)
        student_loss = torch.tensor(float("nan"), device=device)
        temperature = 2.0
        for epoch in range(max(1, epochs)):
            for indices in _batch_indices(len(train_x), 128, 6100 + epoch):
                x = _tensor(train_x[indices], torch, device)
                y = _tensor(train_return[indices] / return_scale, torch, device)
                with torch.no_grad():
                    teacher_logits, teacher_return, _ = teacher(x)
                student_logits, student_return, _ = student(x)
                soft = torch.sigmoid(teacher_logits / temperature)
                student_loss = (
                    nn.functional.binary_cross_entropy_with_logits(
                        student_logits / temperature, soft
                    )
                    * temperature**2
                    + nn.functional.smooth_l1_loss(student_return, teacher_return)
                    + 0.25
                    * nn.functional.binary_cross_entropy_with_logits(
                        student_logits, (y > 0.0).float()
                    )
                )
                student_optimizer.zero_grad()
                student_loss.backward()
                student_optimizer.step()
        training.update(
            {
                "teacher_loss": _finite_objective(teacher_loss),
                "student_loss": _finite_objective(student_loss),
                "teacher_frozen": True,
                "return_scale_pips": return_scale,
            }
        )
        encoder = student.encoder
        head = student.direction
    else:
        raise ValueError(f"unsupported representation model: {name}")

    calibration_logits = _predict_direction_logits(
        encoder, head, fixture.calibration_x, device
    )
    holdout_logits = _predict_direction_logits(encoder, head, fixture.holdout_x, device)
    return _score_probability_model(
        name,
        fixture,
        calibration_logits,
        holdout_logits,
        runtime="torch",
        training=training,
        transfer_target_only=name == "transfer_learning",
    )


def evaluate_s4(
    fixture: WindowFixture,
    *,
    epochs: int,
    max_train_samples: int,
) -> dict[str, Any]:
    torch, nn, device = _torch_runtime()
    train_indices = _bounded_indices(len(fixture.train_x), max_train_samples)
    train_x = fixture.train_x[train_indices]
    train_return = fixture.train_return[train_indices]
    d_model = 16
    official = state_space_adapter.S4OfficialAdapter()

    class S4DirectionModel(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.input_projection = nn.Linear(train_x.shape[-1], d_model)
            self.s4 = official.build_layer(d_model)
            self.head = nn.Linear(d_model, 1)

        def forward(self, value):
            hidden = self.input_projection(value).transpose(1, 2)
            output = self.s4(hidden)
            sequence = output[0] if isinstance(output, tuple) else output
            if sequence.ndim != 3:
                raise RuntimeError(f"official S4 returned shape {tuple(sequence.shape)}")
            if sequence.shape[1] == d_model:
                embedding = sequence[:, :, -1]
            elif sequence.shape[2] == d_model:
                embedding = sequence[:, -1, :]
            else:
                raise RuntimeError(f"official S4 changed d_model axis: {tuple(sequence.shape)}")
            return self.head(embedding).squeeze(-1)

    model = S4DirectionModel().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    last_loss = torch.tensor(float("nan"), device=device)
    for epoch in range(max(1, epochs)):
        for indices in _batch_indices(len(train_x), 128, 7100 + epoch):
            x = _tensor(train_x[indices], torch, device)
            target = _tensor((train_return[indices] > 0.0).astype(np.float32), torch, device)
            last_loss = nn.functional.binary_cross_entropy_with_logits(model(x), target)
            optimizer.zero_grad()
            last_loss.backward()
            optimizer.step()

    def predict(values: np.ndarray) -> np.ndarray:
        model.eval()
        output: list[np.ndarray] = []
        with torch.no_grad():
            for start in range(0, len(values), 512):
                output.append(
                    model(_tensor(values[start : start + 512], torch, device))
                    .detach()
                    .cpu()
                    .numpy()
                )
        return np.concatenate(output)

    result = _score_probability_model(
        "s4",
        fixture,
        predict(fixture.calibration_x),
        predict(fixture.holdout_x),
        runtime="official_state-spaces/s4",
        training={
            "epochs": int(epochs),
            "bounded_train_samples": int(len(train_x)),
            "device": str(device),
            "objective_loss": _finite_objective(last_loss),
            "checkout": str(official.checkout),
            "d_model": d_model,
        },
    )
    return result


def _uniform_event_limit(frame: pd.DataFrame, maximum: int) -> pd.DataFrame:
    if maximum <= 0 or len(frame) <= maximum:
        return frame.reset_index(drop=True)
    groups = list(
        frame.groupby(["input_timeframe", "horizon_sec"], observed=True, sort=False)
    )
    base, remainder = divmod(maximum, len(groups))
    selected: list[pd.DataFrame] = []
    for index, (_, group) in enumerate(groups):
        limit = base + int(index < remainder)
        if limit <= 0:
            continue
        positions = np.unique(np.linspace(0, len(group) - 1, min(limit, len(group)), dtype=int))
        selected.append(group.iloc[positions])
    if not selected:
        return frame.iloc[0:0].copy()
    return (
        pd.concat(selected, ignore_index=True)
        .sort_values(["prediction_time_utc", "event_id"])
        .reset_index(drop=True)
    )


@dataclass(frozen=True)
class DecisionFixture:
    train_x: np.ndarray
    train_rewards: np.ndarray
    train_meta: pd.DataFrame
    calibration_x: np.ndarray
    calibration_rewards: np.ndarray
    calibration_meta: pd.DataFrame
    holdout_x: np.ndarray
    holdout_rewards: np.ndarray
    holdout_meta: pd.DataFrame
    features: tuple[str, ...]
    split: dict[str, Any]


def _action_rewards(events: pd.DataFrame) -> np.ndarray:
    return np.column_stack(
        [
            events["short_net_pips"].to_numpy(float),
            np.zeros(len(events), dtype=float),
            events["long_net_pips"].to_numpy(float),
        ]
    ).astype(np.float32)


def prepare_decision_fixture(
    events: pd.DataFrame,
    *,
    max_train_rows: int = 20000,
    max_calibration_rows: int = 6000,
    max_holdout_rows: int = 12000,
) -> DecisionFixture:
    split = temporal_event_split(events)
    train = _uniform_event_limit(split["train"], max_train_rows)
    calibration = _uniform_event_limit(split["calibration"], max_calibration_rows)
    holdout = _uniform_event_limit(split["holdout"], max_holdout_rows)
    numeric, categorical = shared_benchmark.model_features(train)
    numeric = [name for name in numeric if name != "side_sign"]
    categorical = [name for name in categorical if name != "direction"]
    features = tuple([*numeric, *categorical])
    transformer = shared_benchmark._preprocessor(numeric, categorical, scale=True)
    train_x = np.asarray(transformer.fit_transform(train[list(features)]), dtype=np.float32)
    calibration_x = np.asarray(
        transformer.transform(calibration[list(features)]), dtype=np.float32
    )
    holdout_x = np.asarray(transformer.transform(holdout[list(features)]), dtype=np.float32)
    for label, values in (
        ("train", train_x),
        ("calibration", calibration_x),
        ("holdout", holdout_x),
    ):
        if not np.isfinite(values).all():
            raise ValueError(f"{label} decision contexts contain non-finite values")
    return DecisionFixture(
        train_x=train_x,
        train_rewards=_action_rewards(train),
        train_meta=train,
        calibration_x=calibration_x,
        calibration_rewards=_action_rewards(calibration),
        calibration_meta=calibration,
        holdout_x=holdout_x,
        holdout_rewards=_action_rewards(holdout),
        holdout_meta=holdout,
        features=features,
        split={
            "train_rows": int(len(train)),
            "calibration_rows": int(len(calibration)),
            "holdout_rows": int(len(holdout)),
            "encoded_features": int(train_x.shape[1]),
            "maturity_purged": True,
            "train_maturity_end_utc": pd.Timestamp(train["maturity_time_utc"].max()).isoformat(),
            "calibration_prediction_start_utc": pd.Timestamp(
                calibration["prediction_time_utc"].min()
            ).isoformat(),
            "holdout_prediction_start_utc": pd.Timestamp(
                holdout["prediction_time_utc"].min()
            ).isoformat(),
        },
    )


def _static_action_baseline(fixture: DecisionFixture) -> dict[str, Any]:
    means = fixture.train_rewards.mean(axis=0)
    action = int(np.argmax(means))
    exposure = float(action - 1)
    scored = score_exposure(
        fixture.holdout_meta,
        np.full(len(fixture.holdout_meta), exposure),
    )
    return {
        "selected_action": decision_adapter.ACTION_NAMES[action],
        "train_action_mean_pips": [float(value) for value in means],
        **execution_metrics(scored),
    }


def _decision_result(
    name: str,
    fixture: DecisionFixture,
    exposure: np.ndarray,
    *,
    runtime: str,
    training: dict[str, Any],
) -> dict[str, Any]:
    exposure = np.asarray(exposure, dtype=float).reshape(-1)
    probability = np.clip((exposure + 1.0) / 2.0, 0.0, 1.0)
    confidence = np.abs(exposure)
    scored = score_exposure(fixture.holdout_meta, exposure, confidence)
    classification = classification_metrics(fixture.holdout_meta["target_up"], probability)
    validation = _validation_block(
        scored,
        classification,
        minimum_events=3000,
        minimum_instruments=50,
        minimum_cells=_canonical_cell_count(fixture.train_meta),
    )
    baseline = _static_action_baseline(fixture)
    selected_metrics = execution_metrics(scored)
    performance_delta = selected_metrics["mean_net_pips"] - baseline["mean_net_pips"]
    validation["performance_checks"]["beats_train_selected_static_action"] = performance_delta > 0.0
    validation["performance_passed"] = all(validation["performance_checks"].values())
    return {
        "model": name,
        "status": "bounded_market_scored" if validation["backtest_valid"] else "market_validation_failed",
        "provider_runtime": runtime,
        "backtest_scope": {
            **fixture.split,
            "holdout_instruments": int(fixture.holdout_meta["instrument"].nunique()),
            "holdout_cells": _canonical_cell_count(fixture.holdout_meta),
            "action_contract": list(decision_adapter.ACTION_NAMES),
            "observed_counterfactual_rewards": "SHORT and LONG use observed bid/ask net pips; FLAT is zero",
        },
        "training": training,
        "classification": classification,
        "selected": selected_metrics,
        "all_actions": selected_metrics,
        "cells": execution_cell_metrics(scored),
        "matched_baseline": {
            **baseline,
            "delta_mean_net_pips": performance_delta,
        },
        "validation": validation,
        "production_gate": {
            "passed": False,
            "account_eligible": False,
            "reason": validation["reason"],
        },
        "account_wired": False,
        "production_eligible": False,
    }


def evaluate_decision_model(
    name: str,
    fixture: DecisionFixture,
    *,
    total_timesteps: int,
) -> dict[str, Any]:
    if name == "contextual_bandit":
        model = decision_adapter.VowpalContextualBanditAdapter(epsilon=0.05)
        for index, (context, rewards) in enumerate(
            zip(fixture.train_x, fixture.train_rewards)
        ):
            action = index % len(decision_adapter.ACTION_NAMES)
            model.learn(
                context,
                action,
                float(rewards[action]),
                1.0 / len(decision_adapter.ACTION_NAMES),
            )
        actions = np.asarray(
            [int(np.argmax(model.predict(context))) for context in fixture.holdout_x],
            dtype=float,
        )
        return _decision_result(
            name,
            fixture,
            actions - 1.0,
            runtime="vowpalwabbit.CB-ADF",
            training={
                "logged_rows": int(len(fixture.train_x)),
                "logging_policy": "deterministic rotating action with propensity 1/3",
                "epsilon": 0.05,
            },
        )

    continuous = name == "sac"
    train_env = decision_adapter.OfflineActionEnv(
        fixture.train_x,
        fixture.train_rewards,
        continuous=continuous,
        max_drawdown=1e9,
    )
    agent = decision_adapter.make_sb3_agent(name, train_env)
    agent.learn(total_timesteps=total_timesteps, progress_bar=False)
    exposures: list[float] = []
    for context in fixture.holdout_x:
        action, _ = agent.predict(context, deterministic=True)
        if continuous:
            exposures.append(float(np.clip(np.asarray(action).reshape(-1)[0], -1.0, 1.0)))
        else:
            exposures.append(float(int(np.asarray(action).item()) - 1))
    return _decision_result(
        name,
        fixture,
        np.asarray(exposures),
        runtime="stable_baselines3",
        training={
            "total_timesteps": int(total_timesteps),
            "continuous_exposure": continuous,
            "drawdown_stop_disabled_for_fixed_holdout_comparability": True,
        },
    )


def _capture(model: str, function: Any) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    try:
        result = function()
    except Exception as exc:
        result = {
            "model": model,
            "status": "market_validation_error",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "account_wired": False,
            "production_eligible": False,
        }
    result["elapsed_sec"] = (datetime.now(timezone.utc) - started).total_seconds()
    return result


def run_validation(
    panel_paths: Sequence[Path],
    models: Sequence[str] = RUNNABLE_MODELS,
    *,
    events_per_pair_cell: int = 64,
    neural_max_steps: int = 5,
    representation_epochs: int = 2,
    max_train_samples: int = 20000,
    decision_timesteps: int = 2000,
    forecast_cells: Sequence[tuple[str, int]] = DEFAULT_FORECAST_CELLS,
    output: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    requested = tuple(dict.fromkeys(str(model) for model in models))
    unsupported = sorted(set(requested) - set(RUNNABLE_MODELS))
    if unsupported:
        raise ValueError(f"unsupported remaining-gap models: {', '.join(unsupported)}")
    frame, dataset = load_bounded_market_panel(
        panel_paths, events_per_pair_cell=events_per_pair_cell
    )
    events = event_table(frame)
    results: list[dict[str, Any]] = []
    forecast_fixture: ForecastFixture | None = None
    window_fixture: WindowFixture | None = None
    decision_fixture: DecisionFixture | None = None
    for name in requested:
        print(f"[gap-market] evaluating={name}", flush=True)
        if name in FORECAST_MODELS:
            if forecast_fixture is None:
                forecast_fixture = prepare_forecast_fixture(
                    events, cells=forecast_cells
                )
            result = _capture(
                name,
                lambda name=name: evaluate_neural_forecaster(
                    name, forecast_fixture, neural_max_steps
                ),
            )
        elif name in GRAPH_MODELS:
            result = _capture(
                name,
                lambda: evaluate_cross_pair_graph_matrix(
                    events,
                    neural_max_steps,
                    forecast_cells,
                ),
            )
        elif name in (*REPRESENTATION_MODELS, *STATE_SPACE_MODELS):
            if window_fixture is None:
                window_fixture = prepare_window_fixture(events)
            if name == "s4":
                result = _capture(
                    name,
                    lambda: evaluate_s4(
                        window_fixture,
                        epochs=representation_epochs,
                        max_train_samples=max_train_samples,
                    ),
                )
            else:
                result = _capture(
                    name,
                    lambda name=name: evaluate_representation_model(
                        name,
                        window_fixture,
                        epochs=representation_epochs,
                        max_train_samples=max_train_samples,
                    ),
                )
        elif name in DECISION_MODELS:
            if decision_fixture is None:
                decision_fixture = prepare_decision_fixture(events)
            result = _capture(
                name,
                lambda name=name: evaluate_decision_model(
                    name,
                    decision_fixture,
                    total_timesteps=decision_timesteps,
                ),
            )
        else:
            raise AssertionError(f"unrouted model: {name}")
        results.append(result)
        print(
            f"[gap-market] model={name} status={result['status']} elapsed={result['elapsed_sec']:.1f}s",
            flush=True,
        )

    valid = [row for row in results if row["status"] == "bounded_market_scored"]
    performance_passed = [
        row for row in valid if (row.get("validation") or {}).get("performance_passed")
    ]
    report = {
        "schema_version": 2,
        "generated_utc": utc_iso(),
        "scope": "market backtests for runnable models previously limited to synthetic evidence",
        "execution_metrics_contract": CONTRACT,
        "execution_policy": "shadow_only_no_account_wiring",
        "account_wired": False,
        "dataset": dataset,
        "validation_contract": {
            "feature_provenance": "max_feature_origin_utc must not exceed prediction_time_utc",
            "temporal_split": "chronological train/calibration/holdout with maturity purge",
            "costs": "observed bid/ask entry and exit prices persisted as realized net pips",
            "selection": "thresholds use calibration only; holdout is untouched until final scoring",
            "coverage": "bounded contiguous pair/cell tails; per-model scope is reported explicitly",
            "decision_models": "fixed offline replay; no broker or market interaction",
            "completion_semantics": "backtest_valid is separate from performance_passed",
        },
        "dependencies": {
            "neuralforecast": distribution_version("neuralforecast"),
            "torch": distribution_version("torch"),
            "stable_baselines3": distribution_version("stable-baselines3"),
            "vowpalwabbit": distribution_version("vowpalwabbit"),
            "pandas": distribution_version("pandas"),
            "scikit_learn": distribution_version("scikit-learn"),
        },
        "configuration": {
            "requested_models": list(requested),
            "events_per_pair_cell": int(events_per_pair_cell),
            "neural_max_steps": int(neural_max_steps),
            "representation_epochs": int(representation_epochs),
            "max_train_samples": int(max_train_samples),
            "decision_timesteps": int(decision_timesteps),
            "forecast_cells": [
                {"input_timeframe": timeframe, "horizon_sec": int(horizon)}
                for timeframe, horizon in forecast_cells
            ],
            "random_seed": 42,
        },
        "results": results,
        "summary": {
            "requested": len(requested),
            "backtest_valid": len(valid),
            "backtest_failed_or_error": len(requested) - len(valid),
            "performance_passed": len(performance_passed),
            "all_requested_backtested": len(valid) == len(requested),
            "production_eligible": 0,
            "account_wired": 0,
        },
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    timestamped = output.with_name(f"remaining_market_validation_{stamp}.json")
    atomic_json_dump(report, timestamped)
    atomic_json_dump(report, output)
    report["report_paths"] = [str(timestamped.resolve()), str(output.resolve())]
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panels", type=Path, nargs="+", required=True)
    parser.add_argument("--models", nargs="+", default=list(RUNNABLE_MODELS))
    parser.add_argument("--events-per-pair-cell", type=int, default=64)
    parser.add_argument("--neural-max-steps", type=int, default=5)
    parser.add_argument("--representation-epochs", type=int, default=2)
    parser.add_argument("--max-train-samples", type=int, default=20000)
    parser.add_argument("--decision-timesteps", type=int, default=2000)
    parser.add_argument(
        "--forecast-cells",
        type=parse_forecast_cells,
        default=DEFAULT_FORECAST_CELLS,
        help="comma-separated TIMEFRAME:HORIZON_SEC cells for forecast models",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--require-all-backtested", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_validation(
        args.panels,
        args.models,
        events_per_pair_cell=args.events_per_pair_cell,
        neural_max_steps=args.neural_max_steps,
        representation_epochs=args.representation_epochs,
        max_train_samples=args.max_train_samples,
        decision_timesteps=args.decision_timesteps,
        forecast_cells=args.forecast_cells,
        output=args.output,
    )
    print(json.dumps(report["summary"], indent=2), flush=True)
    if args.require_all_backtested and not report["summary"]["all_requested_backtested"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
