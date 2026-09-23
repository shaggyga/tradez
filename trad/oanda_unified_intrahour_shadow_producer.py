#!/usr/bin/env python3
"""Create current account-ineligible unified FX forecast curves from a snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from scipy.stats import norm


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "fresh_m1_intrahour"))

from src.unified_forecast import live_matrix_from_snapshot  # noqa: E402


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_forecasts(
    artifact_path: Path,
    validation_path: Path,
    snapshot_path: Path,
    max_snapshot_age_sec: float,
) -> dict[str, Any]:
    validation = read_json(validation_path)
    if not validation.get("forecast_validation_pass"):
        raise RuntimeError("validated forecast gate has not passed")
    artifact_hash = sha256(artifact_path)
    if artifact_hash != str(validation.get("artifact_sha256") or ""):
        raise RuntimeError("artifact hash does not match validation receipt")
    bundle = joblib.load(artifact_path)
    if bundle.get("account_eligible") or bundle.get("live_execution_enabled"):
        raise RuntimeError("unified artifact must remain shadow-only")
    snapshot = read_json(snapshot_path)
    generated_epoch = float(snapshot.get("generated_epoch") or 0.0)
    age = max(0.0, time.time() - generated_epoch)
    if generated_epoch <= 0.0 or age > max_snapshot_age_sec:
        raise RuntimeError(f"feature snapshot is stale: {age:.1f}s")
    numeric = list(bundle["numeric_features"])
    categorical = list(bundle["categorical_features"])
    pip_sizes = {
        str(key): float(value)
        for key, value in (bundle.get("pip_sizes") or {}).items()
    }
    matrix = live_matrix_from_snapshot(snapshot, numeric, pip_sizes)
    predicted = np.asarray(
        bundle["model"].predict(matrix[[*numeric, *categorical]]),
        dtype=float,
    )
    opportunity_heads = bundle.get("opportunity_heads") or {}
    opportunity_predictions = {
        kind: np.asarray(
            state["model"].predict(matrix[[*numeric, *categorical]]),
            dtype=float,
        )
        for kind, state in opportunity_heads.items()
        if isinstance(state, dict) and state.get("model") is not None
    }
    horizons = [int(value) for value in bundle["horizons_minutes"]]
    residual = bundle["residual_quantiles"]
    forecasts: list[dict[str, Any]] = []
    for position, row in matrix.reset_index(drop=True).iterrows():
        curve: dict[str, Any] = {}
        for column, horizon in enumerate(horizons):
            point = float(predicted[position, column])
            state = residual[str(horizon)]
            scale = max(1e-9, float(state["std"]))
            curve[str(horizon * 60)] = {
                "predicted_signed_pips": point,
                "probability_up": float(norm.cdf(point / scale)),
                "quantile_low_pips": point + float(state["q10"]),
                "quantile_high_pips": point + float(state["q90"]),
                "account_eligible": False,
                "filter_reasons": ["unified_forecast_shadow_only"],
            }
        for kind, prediction in opportunity_predictions.items():
            targets = list(opportunity_heads[kind].get("targets") or [])
            for column, target in enumerate(targets):
                horizon = int(str(target).rsplit("_", 1)[-1])
                cell = curve.get(str(horizon * 60))
                if cell is None:
                    continue
                value = float(prediction[position, column])
                value = (
                    float(np.clip(value, 0.0, 1.0))
                    if kind == "probability"
                    else max(0.0, value)
                )
                output_name = str(target).removeprefix(
                    "target_"
                ).removesuffix(f"_{horizon}")
                cell[output_name] = value
        instrument = str(row["instrument"])
        source = (snapshot.get("instruments") or {}).get(instrument) or {}
        quote = source.get("quote") or {}
        forecasts.append(
            {
                "model_id": f"unified_intrahour_{artifact_hash[:12]}",
                "instrument": instrument,
                "input_timeframe": "MULTI",
                "generated_epoch": generated_epoch,
                "generated_utc": snapshot.get("generated_utc"),
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "pip": float(
                    pip_sizes.get(
                        instrument,
                        0.01 if instrument.endswith("_JPY") else 0.0001,
                    )
                ),
                "forecast_curve": curve,
                "account_eligible": False,
                "producer_metadata": {
                    "artifact_sha256": artifact_hash,
                    "validation_receipt": str(validation_path.resolve()),
                    "context_timeframes": ["M1", "M30", "H1", "H4"],
                    "one_curve_per_pair": True,
                    "historical_rows_replayed_as_live": False,
                    "oanda_api_called": False,
                    "orders_supported": False,
                },
            }
        )
    return {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snapshot.get("snapshot_id"),
        "snapshot_age_sec": age,
        "artifact": str(artifact_path.resolve()),
        "artifact_sha256": artifact_hash,
        "account_eligible": False,
        "execution_enabled": False,
        "one_curve_per_pair": True,
        "forecasts": forecasts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--validation", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-snapshot-age-sec", type=float, default=120.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_forecasts(
        args.artifact,
        args.validation,
        args.snapshot,
        args.max_snapshot_age_sec,
    )
    atomic_json(args.output, payload)
    print(args.output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
