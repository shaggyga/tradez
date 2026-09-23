#!/usr/bin/env python3
"""Build a compact interactive-explorer dataset from a historical backtest."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from oanda_strategy_lab_ensemble_replay import atomic_write_json, safe_float
except ModuleNotFoundError:
    from trad.oanda_strategy_lab_ensemble_replay import atomic_write_json, safe_float


ROOT = Path(__file__).resolve().parent
DEFAULT_CANDLE_DIR = ROOT / "data" / "oanda_training_manager" / "candles"


def epoch_ms(value: Any) -> int:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return int(timestamp.timestamp() * 1000)


def read_events(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def lane_predictions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    outcomes: dict[str, dict[str, float]] = defaultdict(dict)
    pattern_outcomes: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        if row.get("event") == "shadow_outcome":
            event_id = str(row.get("id") or "")
            horizon = str(int(safe_float(row.get("horizon_sec"))))
            outcomes[event_id][horizon] = safe_float(row.get("theoretical_pips"))
            if isinstance(row.get("pattern_prediction"), dict):
                pattern_outcomes[event_id][horizon] = {
                    "actual_signed_move_pips": row.get("actual_signed_move_pips"),
                    "actual_abs_move_pips": row.get("actual_abs_move_pips"),
                    "actual_movement_coefficient": row.get("actual_movement_coefficient"),
                    "direction_correct": row.get("direction_correct"),
                    "probability_brier_score": row.get("probability_brier_score"),
                }
    predictions: list[dict[str, Any]] = []
    for row in rows:
        if row.get("event") not in {"shadow_signal", "shadow_miss"}:
            continue
        event_id = str(row.get("id") or "")
        lane_id = str(row.get("lane_id") or "")
        if not event_id or not lane_id:
            continue
        miss_class = str(row.get("miss_class") or "")
        status = "accepted" if row.get("event") == "shadow_signal" else miss_class or "rejected"
        bid = safe_float(row.get("entry_bid"))
        ask = safe_float(row.get("entry_ask"))
        gates = row.get("gates") or {}
        signal = row.get("signal") or {}
        prediction = {
                "id": event_id,
                "model_key": f"lane:{lane_id}",
                "model_name": lane_id,
                "model_type": "lane",
                "family": str(row.get("family") or ""),
                "profile": str(row.get("profile") or ""),
                "instrument": str(row.get("instrument") or ""),
                "time_ms": epoch_ms(row.get("entry_time") or row.get("decision_time") or row.get("time")),
                "direction": str(row.get("direction") or ""),
                "status": status,
                "entry_mid": round((bid + ask) / 2.0, 8),
                "spread_pips": safe_float(gates.get("spread_pips")),
                "signal_to_spread": safe_float(gates.get("signal_to_spread")),
                "outcomes": outcomes.get(event_id) or {},
                "signal": signal,
            }
        pattern_forecast = signal.get("pattern_forecast")
        if isinstance(pattern_forecast, dict):
            prediction.update(
                {
                    "pattern_forecast": pattern_forecast,
                    "pattern": pattern_forecast.get("pattern"),
                    "probability_up": pattern_forecast.get("probability_up"),
                    "expected_signed_move_pips": pattern_forecast.get("expected_signed_move_pips"),
                    "expected_abs_move_pips": pattern_forecast.get("expected_abs_move_pips"),
                    "movement_coefficient": pattern_forecast.get("movement_coefficient"),
                    "historical_pattern_count": pattern_forecast.get("historical_pattern_count"),
                    "live_pattern_count": pattern_forecast.get("live_pattern_count"),
                    "pattern_outcomes": pattern_outcomes.get(event_id) or {},
                }
            )
        predictions.append(prediction)
    return predictions


def ensemble_predictions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    outcomes: dict[str, dict[str, float]] = defaultdict(dict)
    for row in rows:
        if row.get("event") == "ensemble_outcome":
            outcomes[str(row.get("ensemble_id") or "")][str(int(safe_float(row.get("horizon_sec"))))] = safe_float(
                row.get("theoretical_pips")
            )
    predictions: list[dict[str, Any]] = []
    for row in rows:
        if row.get("event") != "ensemble_signal":
            continue
        event_id = str(row.get("id") or "")
        ensemble = str(row.get("ensemble") or "")
        if not event_id or not ensemble:
            continue
        bid = safe_float(row.get("entry_bid"))
        ask = safe_float(row.get("entry_ask"))
        predictions.append(
            {
                "id": event_id,
                "model_key": f"ensemble:{ensemble}",
                "model_name": ensemble,
                "model_type": "ensemble",
                "family": "ensemble",
                "profile": str(row.get("vote_level") or "vote"),
                "instrument": str(row.get("instrument") or ""),
                "time_ms": epoch_ms(row.get("entry_time") or row.get("decision_time")),
                "direction": str(row.get("direction") or ""),
                "status": "accepted" if not str(ensemble).startswith("near_") else "near_inclusive",
                "entry_mid": round((bid + ask) / 2.0, 8),
                "spread_pips": round((ask - bid), 8),
                "signal_to_spread": 0.0,
                "agreement": safe_float(row.get("agreement")),
                "voter_count": int(safe_float(row.get("voter_count"))),
                "outcomes": outcomes.get(event_id) or {},
            }
        )
    return predictions


def load_prices(
    candle_dir: Path,
    instrument: str,
    start_ms: int,
    end_ms: int,
) -> list[list[float | int]]:
    path = candle_dir / f"{instrument}_M1.csv"
    columns = ["datetime", "close", "bid_close", "ask_close"]
    try:
        frame = pd.read_csv(path, usecols=columns, engine="pyarrow")
    except (ImportError, ValueError):
        frame = pd.read_csv(path, usecols=columns)
    frame["time"] = pd.to_datetime(frame.pop("datetime"), errors="coerce", utc=True)
    for column in columns[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["time", "close"])
    frame["time_ms"] = (frame["time"].astype("int64") // 1_000_000).astype("int64")
    frame = frame[(frame["time_ms"] >= start_ms) & (frame["time_ms"] <= end_ms)]
    output: list[list[float | int]] = []
    for row in frame.itertuples(index=False):
        bid = safe_float(row.bid_close, safe_float(row.close))
        ask = safe_float(row.ask_close, safe_float(row.close))
        output.append([int(row.time_ms), round(safe_float(row.close), 8), round(bid, 8), round(ask, 8)])
    return output


def build_explorer(report_path: Path, events_path: Path, candle_dir: Path) -> dict[str, Any]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    rows = read_events(events_path)
    predictions = lane_predictions(rows) + ensemble_predictions(rows)
    predictions.sort(key=lambda row: (row["time_ms"], row["model_key"], row["instrument"]))
    instruments = sorted({str(item) for item in report.get("instruments") or []})
    horizons = [str(int(value)) for value in report.get("horizons_sec") or [60, 180, 300]]
    start_ms = epoch_ms(report.get("first_decision_candle"))
    end_ms = epoch_ms(report.get("last_decision_candle")) + max(int(value) for value in horizons) * 1000
    prices = {
        instrument: load_prices(candle_dir, instrument, start_ms, end_ms)
        for instrument in instruments
    }
    model_map: dict[str, dict[str, Any]] = {}
    for prediction in predictions:
        model_map.setdefault(
            prediction["model_key"],
            {
                "model_key": prediction["model_key"],
                "name": prediction["model_name"],
                "type": prediction["model_type"],
                "family": prediction["family"],
                "profile": prediction["profile"],
            },
        )
    return {
        "schema_version": 1,
        "source_report": str(report_path.resolve()),
        "source_events": str(events_path.resolve()),
        "evaluation_mode": report.get("evaluation_mode"),
        "decision_uses_future_data": bool(report.get("decision_uses_future_data")),
        "instruments": instruments,
        "horizons_sec": [int(value) for value in horizons],
        "start_ms": start_ms,
        "end_ms": end_ms,
        "models": sorted(model_map.values(), key=lambda row: (row["type"], row["family"], row["name"])),
        "prices": prices,
        "predictions": predictions,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--events", type=Path, default=None)
    parser.add_argument("--candle-dir", type=Path, default=DEFAULT_CANDLE_DIR)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    events = args.events or args.report.with_name(args.report.stem + "_events.jsonl")
    output = args.output or args.report.with_name(args.report.stem + "_explorer.json")
    payload = build_explorer(args.report, events, args.candle_dir)
    atomic_write_json(output, payload)
    print(
        json.dumps(
            {
                "output": str(output.resolve()),
                "models": len(payload["models"]),
                "predictions": len(payload["predictions"]),
                "price_points": sum(len(rows) for rows in payload["prices"].values()),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
