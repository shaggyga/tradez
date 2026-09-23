#!/usr/bin/env python3
"""Qualify TFT and DeepAR adapters on maturity-safe shared-panel sequences."""

from __future__ import annotations

import argparse
import heapq
import json
import math
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

try:
    import oanda_shared_panel_model_benchmark as tabular
    import oanda_shared_timeframe_horizon_panel as panel
except ModuleNotFoundError:
    from trad import oanda_shared_panel_model_benchmark as tabular
    from trad import oanda_shared_timeframe_horizon_panel as panel


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "modern_model_gap"
MODEL_ROOT = ROOT / "data" / "oanda_training_manager" / "models" / "modern_model_gap"
DEFAULT_MODELS = ("tft", "deepar")
TRACK_GROUPS = ("instrument", "input_timeframe", "horizon_sec", "direction")

warnings.filterwarnings(
    "ignore",
    message="X does not have valid feature names, but StandardScaler was fitted with feature names",
    category=UserWarning,
)
warnings.filterwarnings(
    "once",
    message="upsample_linear1d_backward_out_cuda does not have a deterministic implementation.*",
    category=UserWarning,
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def trainer_accelerator() -> str:
    requested = os.environ.get("OANDA_NEURAL_ACCELERATOR", "auto").strip().lower()
    if requested not in {"auto", "cpu", "gpu"}:
        raise ValueError("OANDA_NEURAL_ACCELERATOR must be auto, cpu, or gpu")
    if requested == "cpu":
        return "cpu"
    import torch

    available = bool(torch.cuda.is_available())
    if requested == "gpu" and not available:
        raise RuntimeError("GPU acceleration was required but CUDA is unavailable")
    return "gpu" if available else "cpu"


def trainer_determinism(name: str, accelerator: str) -> bool | str:
    # CUDA has no deterministic backward kernel for TFT's linear upsampling op.
    # Keep the fixed seed and surface that single limitation instead of silently
    # falling back or disabling deterministic checks for every neural model.
    if name == "tft" and accelerator == "gpu":
        return "warn"
    return True


def assign_maturity_tracks(frame: pd.DataFrame) -> pd.DataFrame:
    """Color overlapping forecast intervals into target-safe sequences."""
    ordered = frame.sort_values([*TRACK_GROUPS, "prediction_time_utc"]).copy()
    ordered["maturity_track"] = -1
    for _, group in ordered.groupby(list(TRACK_GROUPS), observed=True, sort=False):
        available: list[tuple[int, int]] = []
        next_track = 0
        for index, row in group.iterrows():
            prediction_ns = int(pd.Timestamp(row["prediction_time_utc"]).value)
            maturity_ns = int(pd.Timestamp(row["maturity_time_utc"]).value)
            if available and available[0][0] <= prediction_ns:
                _, track = heapq.heappop(available)
            else:
                track = next_track
                next_track += 1
            ordered.at[index, "maturity_track"] = track
            heapq.heappush(available, (maturity_ns, track))
    ordered["sequence_id"] = (
        ordered["instrument"].astype(str)
        + "|"
        + ordered["input_timeframe"].astype(str)
        + "|"
        + ordered["horizon_sec"].astype(str)
        + "|"
        + ordered["direction"].astype(str)
        + "|"
        + ordered["maturity_track"].astype(str)
    )
    ordered["sequence_step"] = ordered.groupby("sequence_id", observed=True).cumcount()
    validate_maturity_tracks(ordered)
    return ordered.sort_values(["prediction_time_utc", "event_id", "direction"]).reset_index(drop=True)


def validate_maturity_tracks(frame: pd.DataFrame) -> None:
    for sequence_id, group in frame.groupby("sequence_id", observed=True):
        ordered = group.sort_values("prediction_time_utc")
        previous_maturity = ordered["maturity_time_utc"].shift(1)
        invalid = previous_maturity.notna() & (
            previous_maturity > ordered["prediction_time_utc"]
        )
        if invalid.any():
            raise ValueError(f"overlapping outcomes remain in maturity track {sequence_id}")


def build_window_samples(
    frame: pd.DataFrame,
    encoder_length: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if encoder_length < 2:
        raise ValueError("encoder_length must be at least 2")
    windows: list[pd.DataFrame] = []
    targets: list[pd.Series] = []
    for sequence_id, group in frame.groupby("sequence_id", observed=True):
        ordered = group.sort_values("prediction_time_utc").reset_index(drop=True)
        for target_index in range(encoder_length, len(ordered)):
            sample = ordered.iloc[target_index - encoder_length : target_index + 1].copy()
            target = sample.iloc[-1].copy()
            sample_id = f"{sequence_id}|{target['side_id']}"
            sample["sample_id"] = sample_id
            sample["sample_time_idx"] = np.arange(len(sample), dtype=np.int64)
            sample["horizon_label"] = "T" + sample["horizon_sec"].astype(str)
            windows.append(sample)
            target["sample_id"] = sample_id
            targets.append(target)
    if not windows:
        raise ValueError("no maturity-safe sequence is long enough for the encoder")
    window_frame = pd.concat(windows, ignore_index=True)
    target_frame = pd.DataFrame(targets).reset_index(drop=True)
    if window_frame.groupby("sample_id", observed=True).size().ne(encoder_length + 1).any():
        raise ValueError("neural sample window length is inconsistent")
    return window_frame, target_frame


def _stratified_time_limit(part: pd.DataFrame, maximum: int) -> pd.DataFrame:
    """Retain pair/timeframe/horizon coverage before filling a time-uniform cap."""
    ordered = part.sort_values("prediction_time_utc")
    if maximum <= 0 or len(ordered) <= maximum:
        return ordered
    strata = ["instrument", "input_timeframe", "horizon_sec"]
    groups = [group for _, group in ordered.groupby(strata, observed=True, sort=True)]
    if len(groups) > maximum:
        positions = np.linspace(0, len(groups) - 1, maximum).round().astype(int)
        groups = [groups[index] for index in np.unique(positions)]
    mandatory = [group.index[len(group) // 2] for group in groups]
    selected = set(int(index) for index in mandatory)
    remaining_slots = maximum - len(selected)
    if remaining_slots > 0:
        remaining = ordered.loc[~ordered.index.isin(selected)]
        positions = np.linspace(
            0, len(remaining) - 1, remaining_slots
        ).round().astype(int)
        selected.update(int(index) for index in remaining.iloc[np.unique(positions)].index)
    return ordered.loc[ordered.index.isin(selected)].sort_values("prediction_time_utc")


def build_limited_window_samples(
    frame: pd.DataFrame,
    encoder_length: int,
    max_samples_per_split: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, set[str]]]:
    """Select target rows before materializing repeated encoder windows."""
    if max_samples_per_split <= 0:
        windows, targets = build_window_samples(frame, encoder_length)
        return windows, targets, chronological_sample_split(targets)

    candidate_parts: list[pd.DataFrame] = []
    for _, group in frame.groupby("sequence_id", observed=True, sort=False):
        ordered = group.sort_values("prediction_time_utc")
        if len(ordered) <= encoder_length:
            continue
        eligible = ordered.iloc[encoder_length:][
            [
                "prediction_time_utc",
                "maturity_time_utc",
                "instrument",
                "input_timeframe",
                "horizon_sec",
            ]
        ].copy()
        eligible["tracked_row_id"] = eligible.index.to_numpy(dtype=np.int64)
        candidate_parts.append(eligible)
    if not candidate_parts:
        raise ValueError("no maturity-safe sequence is long enough for the encoder")
    candidates = pd.concat(candidate_parts, ignore_index=True)
    calibration_start = tabular._time_at_fraction(candidates, 0.70)
    holdout_start = tabular._time_at_fraction(candidates, 0.82)
    candidate_splits = {
        "train": candidates[candidates["maturity_time_utc"] < calibration_start],
        "calibration": candidates[
            (candidates["prediction_time_utc"] >= calibration_start)
            & (candidates["maturity_time_utc"] < holdout_start)
        ],
        "holdout": candidates[candidates["prediction_time_utc"] >= holdout_start],
    }
    if min(len(part) for part in candidate_splits.values()) == 0:
        raise ValueError("neural chronological split is empty after maturity purge")

    selected_rows: dict[int, str] = {}
    for split_name, part in candidate_splits.items():
        ordered = _stratified_time_limit(part, max_samples_per_split)
        for row_id in ordered["tracked_row_id"]:
            selected_rows[int(row_id)] = split_name

    windows: list[pd.DataFrame] = []
    targets: list[pd.Series] = []
    for sequence_id, group in frame.groupby("sequence_id", observed=True, sort=False):
        ordered = group.sort_values("prediction_time_utc")
        for target_index in range(encoder_length, len(ordered)):
            row_id = int(ordered.index[target_index])
            split_name = selected_rows.get(row_id)
            if split_name is None:
                continue
            sample = ordered.iloc[
                target_index - encoder_length : target_index + 1
            ].copy()
            target = sample.iloc[-1].copy()
            sample_id = f"{sequence_id}|{target['side_id']}"
            sample["sample_id"] = sample_id
            sample["sample_time_idx"] = np.arange(len(sample), dtype=np.int64)
            sample["horizon_label"] = "T" + sample["horizon_sec"].astype(str)
            windows.append(sample)
            target["sample_id"] = sample_id
            target["sample_split"] = split_name
            targets.append(target)
    if not windows:
        raise ValueError("no selected neural windows could be materialized")
    window_frame = pd.concat(windows, ignore_index=True)
    target_frame = pd.DataFrame(targets).reset_index(drop=True)
    split_ids = {
        name: set(target_frame.loc[target_frame["sample_split"] == name, "sample_id"])
        for name in candidate_splits
    }
    expected = sum(len(ids) for ids in split_ids.values())
    if expected != len(target_frame):
        raise ValueError("selected neural targets were duplicated across splits")
    if window_frame.groupby("sample_id", observed=True).size().ne(encoder_length + 1).any():
        raise ValueError("neural sample window length is inconsistent")
    return window_frame, target_frame, split_ids


def chronological_sample_split(targets: pd.DataFrame) -> dict[str, set[str]]:
    calibration_start = tabular._time_at_fraction(targets, 0.70)
    holdout_start = tabular._time_at_fraction(targets, 0.82)
    train = targets[targets["maturity_time_utc"] < calibration_start]
    calibration = targets[
        (targets["prediction_time_utc"] >= calibration_start)
        & (targets["maturity_time_utc"] < holdout_start)
    ]
    holdout = targets[targets["prediction_time_utc"] >= holdout_start]
    if min(len(train), len(calibration), len(holdout)) == 0:
        raise ValueError("neural chronological split is empty after maturity purge")
    return {
        "train": set(train["sample_id"]),
        "calibration": set(calibration["sample_id"]),
        "holdout": set(holdout["sample_id"]),
    }


def _uniform_limit(ids: set[str], targets: pd.DataFrame, maximum: int) -> set[str]:
    if maximum <= 0 or len(ids) <= maximum:
        return ids
    selected = targets[targets["sample_id"].isin(ids)].sort_values("prediction_time_utc")
    positions = np.linspace(0, len(selected) - 1, maximum).round().astype(int)
    return set(selected.iloc[np.unique(positions)]["sample_id"])


def prepare_neural_frames(
    frame: pd.DataFrame,
    encoder_length: int,
    max_samples_per_split: int = 0,
    already_tracked: bool = False,
) -> dict[str, Any]:
    tracked = frame if already_tracked else assign_maturity_tracks(frame)
    windows, targets, split_ids = build_limited_window_samples(
        tracked,
        encoder_length,
        max_samples_per_split,
    )
    numeric, _ = tabular.model_features(tracked)
    neural_known_reals = [
        name
        for name in numeric
        if name not in {"input_timeframe_seconds", "horizon_sec", "side_sign"}
    ]
    train_windows = windows[windows["sample_id"].isin(split_ids["train"])]
    medians = train_windows[neural_known_reals].median(numeric_only=True).fillna(0.0)
    lower = float(train_windows["realized_net_pips"].quantile(0.01))
    upper = float(train_windows["realized_net_pips"].quantile(0.99))
    prepared: dict[str, pd.DataFrame] = {}
    target_splits: dict[str, pd.DataFrame] = {}
    for name, ids in split_ids.items():
        part = windows[windows["sample_id"].isin(ids)].copy()
        part[neural_known_reals] = part[neural_known_reals].fillna(medians).fillna(0.0)
        part["neural_target_pips"] = part["realized_net_pips"].clip(lower, upper)
        for column in ("sample_id", "instrument", "input_timeframe", "direction", "horizon_label"):
            part[column] = part[column].astype(str)
        prepared[name] = part
        target_splits[name] = (
            targets[targets["sample_id"].isin(ids)]
            .sort_values(["prediction_time_utc", "event_id", "direction"])
            .reset_index(drop=True)
        )
    return {
        "windows": prepared,
        "targets": target_splits,
        "known_reals": neural_known_reals,
        "encoder_length": encoder_length,
        "target_clip_pips": [lower, upper],
        "track_count": tracked["sequence_id"].nunique(),
        "sample_counts": {name: len(ids) for name, ids in split_ids.items()},
    }


def build_datasets(prepared: dict[str, Any]) -> dict[str, Any]:
    from pytorch_forecasting import TimeSeriesDataSet
    from pytorch_forecasting.data import EncoderNormalizer, NaNLabelEncoder

    all_sample_ids = pd.concat(
        [part["sample_id"] for part in prepared["windows"].values()],
        ignore_index=True,
    )
    sample_encoder = NaNLabelEncoder(add_nan=True, warn=False).fit(all_sample_ids)
    encoder_length = prepared["encoder_length"]
    train = TimeSeriesDataSet(
        prepared["windows"]["train"],
        time_idx="sample_time_idx",
        target="neural_target_pips",
        group_ids=["sample_id"],
        min_encoder_length=encoder_length,
        max_encoder_length=encoder_length,
        min_prediction_length=1,
        max_prediction_length=1,
        static_categoricals=["instrument", "input_timeframe", "direction", "horizon_label"],
        static_reals=["input_timeframe_seconds", "horizon_sec", "side_sign"],
        time_varying_known_reals=["sample_time_idx", *prepared["known_reals"]],
        time_varying_unknown_reals=["neural_target_pips"],
        target_normalizer=EncoderNormalizer(
            method="robust",
            max_length=encoder_length,
        ),
        categorical_encoders={"sample_id": sample_encoder},
        add_relative_time_idx=True,
        add_target_scales=True,
        add_encoder_length=True,
        randomize_length=False,
        allow_missing_timesteps=False,
    )
    calibration = TimeSeriesDataSet.from_dataset(
        train,
        prepared["windows"]["calibration"],
        predict=True,
        stop_randomization=True,
    )
    holdout = TimeSeriesDataSet.from_dataset(
        train,
        prepared["windows"]["holdout"],
        predict=True,
        stop_randomization=True,
    )
    return {"train": train, "calibration": calibration, "holdout": holdout}


def make_neural_model(name: str, dataset: Any, learning_rate: float) -> Any:
    if name == "tft":
        from pytorch_forecasting import TemporalFusionTransformer
        from pytorch_forecasting.metrics import QuantileLoss

        return TemporalFusionTransformer.from_dataset(
            dataset,
            learning_rate=learning_rate,
            hidden_size=12,
            attention_head_size=1,
            dropout=0.10,
            hidden_continuous_size=8,
            output_size=7,
            loss=QuantileLoss(),
            log_interval=-1,
            reduce_on_plateau_patience=2,
        )
    if name == "deepar":
        from pytorch_forecasting import DeepAR
        from pytorch_forecasting.metrics import NormalDistributionLoss

        return DeepAR.from_dataset(
            dataset,
            learning_rate=learning_rate,
            hidden_size=16,
            rnn_layers=1,
            dropout=0.10,
            loss=NormalDistributionLoss(),
            log_interval=-1,
            reduce_on_plateau_patience=2,
        )
    raise ValueError(f"unsupported neural shared-panel model: {name}")


def _prediction_scores(model: Any, loader: Any) -> pd.DataFrame:
    result = model.predict(
        loader,
        mode="prediction",
        return_index=True,
        trainer_kwargs={
            "accelerator": trainer_accelerator(),
            "devices": 1,
            "logger": False,
            "enable_checkpointing": False,
            "enable_progress_bar": False,
        },
    )
    values = np.asarray(result.output.detach().cpu(), dtype=float).reshape(len(result.index), -1)
    scores = values[:, 0]
    return pd.DataFrame(
        {
            "sample_id": result.index["sample_id"].astype(str).to_numpy(),
            "score_net_pips": scores,
        }
    )


def _join_scores(targets: pd.DataFrame, scores: pd.DataFrame) -> pd.DataFrame:
    joined = targets.merge(scores, on="sample_id", how="inner", validate="one_to_one")
    if len(joined) != len(targets):
        raise ValueError("neural predictions do not cover every requested sample")
    return joined


def _score_calibrator(frame: pd.DataFrame) -> LogisticRegression:
    target = frame[panel.TARGET_COLUMN].to_numpy(int)
    if len(np.unique(target)) < 2:
        raise ValueError("calibration split has no target variation")
    calibrator = LogisticRegression(C=1.0, max_iter=200, random_state=42)
    calibrator.fit(frame[["score_net_pips"]], target)
    return calibrator


def _probability(calibrator: LogisticRegression, frame: pd.DataFrame) -> np.ndarray:
    return np.asarray(calibrator.predict_proba(frame[["score_net_pips"]])[:, 1], dtype=float)


def evaluate_matched_logistic(prepared: dict[str, Any]) -> dict[str, Any]:
    train = prepared["targets"]["train"]
    calibration = prepared["targets"]["calibration"]
    holdout = prepared["targets"]["holdout"]
    combined = pd.concat([train, calibration, holdout], ignore_index=True)
    numeric, categorical = tabular.model_features(combined)
    features = [*numeric, *categorical]
    model = tabular.make_model("logistic_baseline", numeric, categorical)
    model.fit(train[features], train[panel.TARGET_COLUMN].astype(int))
    calibration_raw = np.asarray(model.predict_proba(calibration[features])[:, 1], dtype=float)
    calibrator = tabular._fit_calibrator(
        calibration_raw,
        calibration[panel.TARGET_COLUMN].to_numpy(int),
    )
    calibration_probability = tabular._calibrated_probability(
        model, calibrator, calibration, features
    )
    minimum = max(10, min(100, len(calibration) // 4))
    viable = [
        row
        for row in tabular.threshold_table(calibration, calibration_probability)
        if row["trades"] >= minimum
        and row["mean_net_pips"] > 0.0
        and row["profit_factor"] >= 1.05
    ]
    threshold = (
        max(viable, key=lambda row: row["mean_net_pips"] * math.sqrt(row["trades"]))["threshold"]
        if viable
        else 0.70
    )
    holdout_probability = tabular._calibrated_probability(
        model, calibrator, holdout, features
    )
    actions = tabular.choose_one_action(holdout, holdout_probability)
    selected = actions[actions["probability"] >= threshold]
    return {
        "model": "logistic_baseline",
        "selected_threshold": threshold,
        "classification": tabular.classification_metrics(
            holdout[panel.TARGET_COLUMN].to_numpy(int), holdout_probability
        ),
        "all_actions": tabular.trade_metrics(actions),
        "selected": tabular.trade_metrics(selected),
    }


def evaluate_neural_model(
    name: str,
    prepared: dict[str, Any],
    datasets: dict[str, Any],
    max_epochs: int,
    batch_size: int,
    learning_rate: float,
    limit_train_batches: int,
) -> tuple[Any, dict[str, Any]]:
    import lightning.pytorch as lightning

    lightning.seed_everything(42, workers=True)
    train_loader = datasets["train"].to_dataloader(
        train=True, batch_size=batch_size, num_workers=0
    )
    calibration_loader = datasets["calibration"].to_dataloader(
        train=False, batch_size=batch_size, num_workers=0
    )
    holdout_loader = datasets["holdout"].to_dataloader(
        train=False, batch_size=batch_size, num_workers=0
    )
    model = make_neural_model(name, datasets["train"], learning_rate)
    accelerator = trainer_accelerator()
    deterministic = trainer_determinism(name, accelerator)
    trainer_kwargs: dict[str, Any] = {
        "max_epochs": max_epochs,
        "accelerator": accelerator,
        "devices": 1,
        "gradient_clip_val": 0.1,
        "logger": False,
        "enable_checkpointing": False,
        "enable_model_summary": False,
        "enable_progress_bar": False,
        "deterministic": deterministic,
    }
    if limit_train_batches > 0:
        trainer_kwargs["limit_train_batches"] = limit_train_batches
    trainer = lightning.Trainer(**trainer_kwargs)
    trainer.fit(model, train_loader, calibration_loader)

    calibration = _join_scores(
        prepared["targets"]["calibration"],
        _prediction_scores(model, calibration_loader),
    )
    holdout = _join_scores(
        prepared["targets"]["holdout"],
        _prediction_scores(model, holdout_loader),
    )
    calibrator = _score_calibrator(calibration)
    calibration_probability = _probability(calibrator, calibration)
    thresholds = tabular.threshold_table(calibration, calibration_probability)
    minimum = max(10, min(100, len(calibration) // 4))
    viable = [
        row
        for row in thresholds
        if row["trades"] >= minimum
        and row["mean_net_pips"] > 0.0
        and row["profit_factor"] >= 1.05
    ]
    selected_threshold = (
        max(viable, key=lambda row: row["mean_net_pips"] * math.sqrt(row["trades"]))["threshold"]
        if viable
        else 0.70
    )
    holdout_probability = _probability(calibrator, holdout)
    actions = tabular.choose_one_action(holdout, holdout_probability)
    selected = actions[actions["probability"] >= selected_threshold]
    report = {
        "status": "adapter_qualified",
        "model": name,
        "accelerator": accelerator,
        "determinism_policy": deterministic,
        "epochs": max_epochs,
        "train_samples": len(prepared["targets"]["train"]),
        "calibration_samples": len(calibration),
        "holdout_samples": len(holdout),
        "selected_threshold": selected_threshold,
        "classification": tabular.classification_metrics(
            holdout[panel.TARGET_COLUMN].to_numpy(int), holdout_probability
        ),
        "all_actions": tabular.trade_metrics(actions),
        "all_prediction_cell_metrics": tabular.cell_metrics(actions),
        "selected": tabular.trade_metrics(selected),
        "cell_metrics": tabular.cell_metrics(selected),
        "production_gate": {
            "passed": False,
            "account_eligible": False,
            "reason": "adapter qualification is not purged multi-fold production validation",
        },
    }
    artifact = {
        "model": model,
        "calibrator": calibrator,
        "selected_threshold": selected_threshold,
        "dataset_parameters": datasets["train"].get_parameters(),
    }
    return artifact, report


def atomic_torch_dump(value: Any, path: Path) -> None:
    import torch

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        payload = {
            "state_dict": value["model"].state_dict(),
            "calibrator": value["calibrator"],
            "selected_threshold": value["selected_threshold"],
            "dataset_parameters": value["dataset_parameters"],
            "feature_version": panel.FEATURE_VERSION,
            "execution_policy": "shadow_only",
        }
        torch.save(payload, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def run_benchmark(
    panel_paths: list[Path],
    models: list[str],
    max_events: int = 0,
    encoder_length: int = 8,
    max_samples_per_split: int = 0,
    max_epochs: int = 2,
    batch_size: int = 64,
    learning_rate: float = 0.01,
    limit_train_batches: int = 0,
    persist_artifacts: bool = True,
    artifact_tag: str = "latest",
    report_output: Path | None = None,
) -> dict[str, Any]:
    artifact_tag = artifact_tag.strip()
    if not artifact_tag or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        for character in artifact_tag
    ):
        raise ValueError("artifact_tag must contain only letters, numbers, '_' or '-'")
    frame, dataset_summary = tabular.load_panels(
        panel_paths,
        max_events=max_events,
    )
    tracked = assign_maturity_tracks(frame)
    del frame
    prepared = prepare_neural_frames(
        tracked,
        encoder_length=encoder_length,
        max_samples_per_split=max_samples_per_split,
        already_tracked=True,
    )
    del tracked
    datasets = build_datasets(prepared)
    matched_baseline = evaluate_matched_logistic(prepared)
    results: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for name in list(dict.fromkeys(models)):
        print(f"[neural-gap] evaluating={name}", flush=True)
        try:
            artifact, result = evaluate_neural_model(
                name,
                prepared,
                datasets,
                max_epochs=max_epochs,
                batch_size=batch_size,
                learning_rate=learning_rate,
                limit_train_batches=limit_train_batches,
            )
            if persist_artifacts:
                path = MODEL_ROOT / f"{name}_shared_panel_{artifact_tag}.pt"
                atomic_torch_dump(artifact, path)
                record = {
                    "model": name,
                    "path": str(path),
                    "bytes": path.stat().st_size,
                    "sha256": tabular.sha256_file(path),
                }
                result["artifact"] = record
                artifacts.append(record)
        except Exception as exc:
            result = {
                "status": "error",
                "model": name,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        results.append(result)
    for result in results:
        if result["status"] != "adapter_qualified":
            continue
        result["matched_baseline"] = {
            "model": matched_baseline["model"],
            "baseline_auc": matched_baseline["classification"]["auc"],
            "baseline_all_action_mean_net_pips": matched_baseline["all_actions"]["mean_net_pips"],
            "delta_auc": result["classification"]["auc"] - matched_baseline["classification"]["auc"],
            "delta_all_action_mean_net_pips": (
                result["all_actions"]["mean_net_pips"]
                - matched_baseline["all_actions"]["mean_net_pips"]
            ),
        }
    ranked = sorted(
        [row for row in results if row["status"] == "adapter_qualified"],
        key=lambda row: (
            row["selected"]["mean_net_pips"],
            row["classification"]["auc"],
            -row["classification"]["brier"],
        ),
        reverse=True,
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "run_id": stamp,
        "execution_policy": "shadow_only",
        "account_wired": False,
        "validation_level": "runtime_adapter_qualification_chronological_holdout",
        "artifact_tag": artifact_tag,
        "dataset": dataset_summary,
        "sequence_contract": {
            "encoder_length": encoder_length,
            "maturity_tracks": prepared["track_count"],
            "sample_counts": prepared["sample_counts"],
            "target_clip_pips": prepared["target_clip_pips"],
            "leakage_rule": "prior encoder outcome maturity must not exceed the next prediction time",
        },
        "matched_baseline": matched_baseline,
        "requested_models": models,
        "results": results,
        "ranking": [row["model"] for row in ranked],
        "all_requested_qualified": len(ranked) == len(set(models)),
        "any_production_gate_passed": False,
        "artifacts": artifacts,
    }
    latest = (
        report_output.resolve()
        if report_output is not None
        else REPORT_ROOT / "shared_panel_neural_benchmark_latest.json"
    )
    latest.parent.mkdir(parents=True, exist_ok=True)
    timestamped = latest.with_name(f"{latest.stem}_{stamp}{latest.suffix}")
    tabular.atomic_json_dump(report, timestamped)
    tabular.atomic_json_dump(report, latest)
    report["report_paths"] = [str(timestamped), str(latest)]
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panels", type=Path, nargs="+", required=True)
    parser.add_argument("--models", nargs="+", default=list(DEFAULT_MODELS))
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--encoder-length", type=int, default=8)
    parser.add_argument("--max-samples-per-split", type=int, default=0)
    parser.add_argument("--max-epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--learning-rate", type=float, default=0.01)
    parser.add_argument("--limit-train-batches", type=int, default=0)
    parser.add_argument("--no-artifacts", action="store_true")
    parser.add_argument("--artifact-tag", default="latest")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_benchmark(
        args.panels,
        args.models,
        max_events=args.max_events,
        encoder_length=args.encoder_length,
        max_samples_per_split=args.max_samples_per_split,
        max_epochs=args.max_epochs,
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        limit_train_batches=args.limit_train_batches,
        persist_artifacts=not args.no_artifacts,
        artifact_tag=args.artifact_tag,
        report_output=args.output,
    )
    print(
        json.dumps(
            {
                "ranking": report["ranking"],
                "all_requested_qualified": report["all_requested_qualified"],
                "reports": report["report_paths"],
            },
            indent=2,
        )
    )
    return 0 if report["all_requested_qualified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
