#!/usr/bin/env python3
"""Score one locally frozen foundation model on the shared M1 holdout fixture."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from oanda_foundation_model_adapters import ForecastRequest, get_adapter


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_device(requested: str) -> str:
    normalized = requested.strip().lower()
    if normalized not in {"auto", "cpu", "cuda"}:
        raise ValueError("foundation device must be auto, cpu, or cuda")
    if normalized == "cpu":
        return "cpu"
    try:
        import torch
    except ImportError:
        if normalized == "cuda":
            raise RuntimeError("CUDA was required but PyTorch is unavailable")
        return "cpu"
    available = bool(torch.cuda.is_available())
    if normalized == "cuda" and not available:
        raise RuntimeError("CUDA was required but no CUDA device is available")
    return "cuda" if available else "cpu"


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--weights", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--device",
        choices=("auto", "cpu", "cuda"),
        default=os.environ.get("OANDA_FOUNDATION_DEVICE", "auto"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fixture = np.load(args.fixture)
    contexts = fixture["contexts"]
    horizons = fixture["horizons_sec"].astype(int)
    interval_seconds = int(
        fixture["input_timeframe_seconds"][0]
        if "input_timeframe_seconds" in fixture
        else 60
    )
    input_timeframe = str(
        fixture["input_timeframe"][0]
        if "input_timeframe" in fixture
        else "M1"
    )
    steps = horizons // interval_seconds
    max_steps = int(steps.max())
    actual_mid = fixture["actual_mid"]
    long_net = fixture["long_net_pips"]
    short_net = fixture["short_net_pips"]
    adapter = get_adapter(args.model)
    device = resolve_device(args.device)
    forecasts: list[np.ndarray] = []
    for number, context in enumerate(contexts, start=1):
        result = adapter.forecast(
            ForecastRequest(
                context=context,
                horizon=max_steps,
                model_ref=str(args.weights.resolve()),
                allow_download=False,
                interval_seconds=interval_seconds,
                device=device,
            )
        )
        median = np.asarray(result.median, dtype=float).squeeze()
        if median.ndim != 1 or len(median) != max_steps:
            raise RuntimeError(
                f"unexpected {args.model} forecast shape: {result.median.shape}"
            )
        forecasts.append(median)
        print(f"[foundation-market] {args.model} {number}/{len(contexts)}", flush=True)
    forecast = np.stack(forecasts)
    point = forecast[:, steps - 1]
    base = contexts[:, -1, None]
    predicted_move = point - base
    actual_move = actual_mid - base
    choose_long = predicted_move >= 0.0
    selected_net = np.where(choose_long, long_net, short_net)
    direction_correct = np.sign(predicted_move) == np.sign(actual_move)
    cells: list[dict[str, Any]] = []
    for index, horizon in enumerate(horizons):
        net = selected_net[:, index]
        cells.append(
            {
                "input_timeframe": input_timeframe,
                "horizon_sec": int(horizon),
                "n": int(len(net)),
                "direction_accuracy": float(direction_correct[:, index].mean()),
                "win_rate": float((net > 0.0).mean()),
                "avg_net_pips": float(net.mean()),
                "sum_net_pips": float(net.sum()),
                "long_rate": float(choose_long[:, index].mean()),
                "mae_price": float(np.abs(point[:, index] - actual_mid[:, index]).mean()),
            }
        )
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "model": args.model,
        "status": "bounded_market_scored",
        "validation_level": "frozen_zero_shot_chronological_holdout",
        "weights": str(args.weights.resolve()),
        "fixture": str(args.fixture.resolve()),
        "input_timeframe": input_timeframe,
        "input_timeframe_seconds": interval_seconds,
        "requested_device": args.device,
        "device": device,
        "windows": int(len(contexts)),
        "cells": cells,
        "summary": {
            "n": int(selected_net.size),
            "direction_accuracy": float(direction_correct.mean()),
            "win_rate": float((selected_net > 0.0).mean()),
            "avg_net_pips": float(selected_net.mean()),
            "sum_net_pips": float(selected_net.sum()),
        },
        "production_eligible": False,
        "account_wired": False,
    }
    write_json_atomic(args.output, report)
    print(json.dumps(report["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
