#!/usr/bin/env python3
"""Shadow-only NeuralForecast adapters for the remaining neural model families."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


MODEL_NAMES = ("patchtst", "nbeats", "nhits", "lstm", "gru", "tcn")
ALL_MODEL_NAMES = (*MODEL_NAMES, "stemgnn")


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


def _trainer_kwargs() -> dict[str, Any]:
    return {
        "accelerator": trainer_accelerator(),
        "devices": 1,
        "enable_checkpointing": False,
        "enable_progress_bar": False,
        "logger": False,
        "num_sanity_val_steps": 0,
    }


def make_model(
    name: str,
    horizon: int,
    input_size: int,
    max_steps: int,
    n_series: int = 1,
) -> Any:
    """Build a small but real model from the maintained Nixtla implementation."""
    from neuralforecast import models

    common = {
        "h": horizon,
        "input_size": input_size,
        "max_steps": max_steps,
        "learning_rate": 1e-3,
        "batch_size": 8,
        "windows_batch_size": 32,
        "val_check_steps": max(1, max_steps),
        "random_seed": 42,
        **_trainer_kwargs(),
    }
    if name == "patchtst":
        return models.PatchTST(
            encoder_layers=1,
            n_heads=2,
            hidden_size=16,
            linear_hidden_size=32,
            patch_len=min(4, input_size),
            stride=2,
            dropout=0.0,
            fc_dropout=0.0,
            head_dropout=0.0,
            attn_dropout=0.0,
            alias=name,
            **common,
        )
    if name == "nbeats":
        return models.NBEATS(
            stack_types=["identity"],
            n_blocks=[1],
            mlp_units=[[32, 32]],
            n_basis=2,
            alias=name,
            **common,
        )
    if name == "nhits":
        return models.NHITS(
            stack_types=["identity"],
            n_blocks=[1],
            mlp_units=[[32, 32]],
            n_pool_kernel_size=[2],
            n_freq_downsample=[1],
            alias=name,
            **common,
        )
    if name == "lstm":
        return models.LSTM(
            encoder_n_layers=1,
            encoder_hidden_size=16,
            decoder_hidden_size=16,
            decoder_layers=1,
            alias=name,
            **common,
        )
    if name == "gru":
        return models.GRU(
            encoder_n_layers=1,
            encoder_hidden_size=16,
            decoder_hidden_size=16,
            decoder_layers=1,
            alias=name,
            **common,
        )
    if name == "tcn":
        return models.TCN(
            kernel_size=2,
            dilations=[1, 2],
            encoder_hidden_size=16,
            context_size=min(4, input_size),
            decoder_hidden_size=16,
            decoder_layers=1,
            alias=name,
            **common,
        )
    if name == "stemgnn":
        return models.StemGNN(
            n_series=n_series,
            n_stacks=2,
            multi_layer=2,
            dropout_rate=0.0,
            alias=name,
            **common,
        )
    raise ValueError(f"unsupported NeuralForecast family: {name}")


def synthetic_series(n_series: int = 4, points: int = 72) -> pd.DataFrame:
    if n_series < 1 or points < 24:
        raise ValueError("synthetic qualification needs at least one series and 24 points")
    rows: list[pd.DataFrame] = []
    index = pd.date_range("2026-01-01", periods=points, freq="min", tz="UTC")
    for series in range(n_series):
        phase = series * 0.41
        trend = np.linspace(0.0, 0.25 + series * 0.02, points)
        values = np.sin(np.arange(points) / 5.0 + phase) + trend
        rows.append(pd.DataFrame({"unique_id": f"S{series}", "ds": index, "y": values}))
    return pd.concat(rows, ignore_index=True)


def panel_to_series(panel_frame: pd.DataFrame) -> pd.DataFrame:
    required = {"instrument", "prediction_time_utc", "event_id", "last"}
    missing = sorted(required - set(panel_frame.columns))
    if missing:
        raise ValueError(f"shared panel is missing: {', '.join(missing)}")
    frame = panel_frame.drop_duplicates("event_id").copy()
    frame["ds"] = pd.to_datetime(frame["prediction_time_utc"], utc=True)
    frame["unique_id"] = frame["instrument"].astype(str)
    frame["y"] = pd.to_numeric(frame["last"], errors="coerce")
    frame = frame.dropna(subset=["y"]).sort_values(["unique_id", "ds"])
    duplicate = frame.duplicated(["unique_id", "ds"])
    if duplicate.any():
        frame = frame[~duplicate]
    counts = frame.groupby("unique_id", observed=True).size()
    if counts.empty or counts.min() < 24:
        raise ValueError("each panel series needs at least 24 unique observations")
    return frame[["unique_id", "ds", "y"]].reset_index(drop=True)


def sequence_index_axis(frame: pd.DataFrame) -> pd.DataFrame:
    """Preserve order while replacing an irregular audit sample's clock with an index."""
    ordered = frame.sort_values(["unique_id", "ds"]).copy()
    ordered["source_ds"] = ordered["ds"]
    ordered["ds"] = ordered.groupby("unique_id", observed=True).cumcount()
    return ordered.reset_index(drop=True)


def _balanced_tail(frame: pd.DataFrame) -> pd.DataFrame:
    counts = frame.groupby("unique_id", observed=True).size()
    minimum = int(counts.min())
    return (
        frame.groupby("unique_id", observed=True, group_keys=False)
        .tail(minimum)
        .sort_values(["unique_id", "ds"])
        .reset_index(drop=True)
    )


def qualify_model(
    name: str,
    frame: pd.DataFrame,
    horizon: int = 2,
    input_size: int = 12,
    max_steps: int = 1,
    frequency: str | int = "min",
) -> dict[str, Any]:
    from neuralforecast import NeuralForecast

    if name not in ALL_MODEL_NAMES:
        raise ValueError(f"unsupported model: {name}")
    balanced = _balanced_tail(frame)
    n_series = balanced["unique_id"].nunique()
    minimum = int(balanced.groupby("unique_id", observed=True).size().min())
    if minimum <= input_size + horizon + 4:
        raise ValueError("series is too short for the requested encoder and horizon")
    train = balanced.groupby("unique_id", observed=True, group_keys=False).head(minimum - horizon)
    truth = balanced.groupby("unique_id", observed=True, group_keys=False).tail(horizon)
    model = make_model(name, horizon, input_size, max_steps, n_series=n_series)
    engine = NeuralForecast(models=[model], freq=frequency)
    engine.fit(df=train, val_size=max(horizon, 2))
    forecast = engine.predict().reset_index()
    prediction_column = name
    if prediction_column not in forecast.columns:
        candidates = [column for column in forecast if column not in {"unique_id", "ds"}]
        if len(candidates) != 1:
            raise RuntimeError(f"cannot identify forecast column for {name}: {candidates}")
        prediction_column = candidates[0]
    scored = truth.merge(
        forecast[["unique_id", "ds", prediction_column]],
        on=["unique_id", "ds"],
        how="inner",
        validate="one_to_one",
    )
    if len(scored) != n_series * horizon:
        raise RuntimeError("forecast did not cover every held-out timestamp")
    predicted = scored[prediction_column].to_numpy(dtype=float)
    actual = scored["y"].to_numpy(dtype=float)
    if not np.isfinite(predicted).all():
        raise RuntimeError("model returned non-finite forecasts")
    last_train = train.groupby("unique_id", observed=True)["y"].last()
    bases = scored["unique_id"].map(last_train).to_numpy(dtype=float)
    direction = np.sign(predicted - bases) == np.sign(actual - bases)
    return {
        "model": name,
        "status": "synthetic_qualified",
        "provider_runtime": "neuralforecast",
        "accelerator": trainer_accelerator(),
        "samples": len(scored),
        "series": n_series,
        "horizon_steps": horizon,
        "input_steps": input_size,
        "max_steps": max_steps,
        "mae": float(np.mean(np.abs(predicted - actual))),
        "direction_accuracy": float(np.mean(direction)),
        "prediction_min": float(np.min(predicted)),
        "prediction_max": float(np.max(predicted)),
        "account_wired": False,
        "production_eligible": False,
        "time_axis": "sequence_index" if frequency == 1 else str(frequency),
    }


def run_qualification(
    names: Iterable[str],
    frame: pd.DataFrame,
    horizon: int,
    input_size: int,
    max_steps: int,
    source: str,
    frequency: str | int = "min",
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for name in names:
        try:
            result = qualify_model(
                name,
                frame,
                horizon,
                input_size,
                max_steps,
                frequency=frequency,
            )
            if frequency == 1:
                result["status"] = "bounded_vault_smoke_qualified"
                result["evidence_level"] = "synthetic_qualified"
                result["performance_interpretation"] = (
                    "adapter compatibility only; sparse canary rows and no cost scoring"
                )
            results.append(result)
        except Exception as exc:
            results.append(
                {
                    "model": name,
                    "status": "qualification_failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "account_wired": False,
                    "production_eligible": False,
                }
            )
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "source": source,
        "execution_policy": "shadow_only_no_account_wiring",
        "time_axis": "sequence_index" if frequency == 1 else str(frequency),
        "results": results,
        "summary": {
            "requested": len(results),
            "qualified": sum(
                row["status"]
                in {"synthetic_qualified", "bounded_vault_smoke_qualified"}
                for row in results
            ),
            "failed": sum(row["status"] == "qualification_failed" for row in results),
            "account_wired": 0,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models", nargs="+", choices=ALL_MODEL_NAMES, default=list(MODEL_NAMES))
    parser.add_argument("--panel", type=Path)
    parser.add_argument("--horizon", type=int, default=2)
    parser.add_argument("--input-size", type=int, default=12)
    parser.add_argument("--max-steps", type=int, default=1)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.panel:
        frame = panel_to_series(pd.read_parquet(args.panel))
        source_start = frame["ds"].min().isoformat()
        source_end = frame["ds"].max().isoformat()
        frame = sequence_index_axis(frame).drop(columns="source_ds")
        frequency: str | int = 1
        source = str(args.panel.resolve())
    else:
        frame = synthetic_series()
        source_start = frame["ds"].min().isoformat()
        source_end = frame["ds"].max().isoformat()
        frequency = "min"
        source = "deterministic_synthetic_series_v1"
    report = run_qualification(
        args.models,
        frame,
        args.horizon,
        args.input_size,
        args.max_steps,
        source,
        frequency,
    )
    report["source_start"] = source_start
    report["source_end"] = source_end
    report["cost_qualified"] = False
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + f".{os.getpid()}.tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        os.replace(temporary, args.output)
    print(payload)
    return 0 if report["summary"]["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
