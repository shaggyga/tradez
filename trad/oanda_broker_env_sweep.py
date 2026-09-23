#!/usr/bin/env python3
"""Sweep model streams inside the OANDA-style broker replay."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from oanda_broker_style_portfolio_replay import (
    BrokerReplay,
    CandleStore,
    DEFAULT_OUTPUT as BROKER_OUTPUT,
    PRIMARY_CONFIG,
    CANDLE_ROOT,
    direction_int,
    load_candidates,
    load_margin_rates,
    objective,
    read_json,
    safe_float,
    safe_int,
)
from oanda_h1_h4_guard_recalibration import PROFILES, SCORE_GATES, STREAMS as H1_H4_STREAMS


DEFAULT_OUTPUT = BROKER_OUTPUT.parent / "h1_h4_oanda_broker_sweep"

EXPANDED_STREAMS = {
    "m30": BROKER_OUTPUT.parent / "all68_latest_hgb_reversal_m30_full_trader_style_backtest" / "candidate_rows.csv",
    "h1": BROKER_OUTPUT.parent / "all68_latest_hgb_reversal_h1_full_trader_style_backtest" / "candidate_rows.csv",
    "h4": BROKER_OUTPUT.parent / "all68_latest_hgb_reversal_h4_full_trader_style_backtest" / "candidate_rows.csv",
}


EXECUTION_PROFILES: Dict[str, Dict[str, Any]] = {
    "live_current": {
        "atr_stop_multiplier": 0.85,
        "take_profit_edge_capture": 0.45,
        "take_profit_min_r_multiple": 0.35,
        "trailing_stop_r_multiple": 0.35,
        "min_trailing_spread_multiple": 2.0,
        "min_edge_pips": 0.25,
        "max_spread_to_edge_ratio": 1.0,
        "min_profit_per_margin": 0.0035,
    },
    "wider_stop": {
        "atr_stop_multiplier": 1.25,
        "take_profit_edge_capture": 0.50,
        "take_profit_min_r_multiple": 0.40,
        "trailing_stop_r_multiple": 0.80,
        "min_trailing_spread_multiple": 2.5,
        "min_edge_pips": 0.50,
        "max_spread_to_edge_ratio": 0.80,
        "min_profit_per_margin": 0.0035,
    },
    "wide_stop_slow_trail": {
        "atr_stop_multiplier": 1.60,
        "take_profit_edge_capture": 0.55,
        "take_profit_min_r_multiple": 0.35,
        "trailing_stop_r_multiple": 1.25,
        "min_trailing_spread_multiple": 3.0,
        "min_edge_pips": 0.75,
        "max_spread_to_edge_ratio": 0.65,
        "min_profit_per_margin": 0.0035,
    },
    "very_wide_stop": {
        "atr_stop_multiplier": 2.10,
        "take_profit_edge_capture": 0.70,
        "take_profit_min_r_multiple": 0.30,
        "trailing_stop_r_multiple": 1.75,
        "min_trailing_spread_multiple": 3.5,
        "min_edge_pips": 1.00,
        "max_spread_to_edge_ratio": 0.50,
        "min_profit_per_margin": 0.0035,
    },
    "quick_profit": {
        "atr_stop_multiplier": 1.10,
        "take_profit_edge_capture": 0.25,
        "take_profit_min_r_multiple": 0.20,
        "trailing_stop_r_multiple": 0.65,
        "min_trailing_spread_multiple": 2.5,
        "min_edge_pips": 0.50,
        "max_spread_to_edge_ratio": 0.75,
        "min_profit_per_margin": 0.0035,
    },
    "quality_strict": {
        "atr_stop_multiplier": 1.50,
        "take_profit_edge_capture": 0.60,
        "take_profit_min_r_multiple": 0.35,
        "trailing_stop_r_multiple": 1.20,
        "min_trailing_spread_multiple": 3.0,
        "min_edge_pips": 1.50,
        "max_spread_to_edge_ratio": 0.35,
        "min_profit_per_margin": 0.0060,
        "min_score_percentile": 0.90,
    },
}


GUARD_PROFILE_NAMES = ["h_tf_conservative", "h_tf_balanced", "h_tf_growth"]
STREAM_GATE_ROWS = [
    ("h1", "all"),
    ("h1", "score_ge_012"),
    ("h1", "score_ge_019"),
    ("h4", "all"),
    ("h4", "score_ge_012"),
    ("h4", "score_ge_019"),
]

EXPANDED_STREAM_GATE_ROWS = [
    ("m30", "all"),
    ("m30", "score_ge_012"),
    ("m30", "score_ge_019"),
    ("h1", "all"),
    ("h1", "score_ge_012"),
    ("h1", "score_ge_019"),
    ("h4", "all"),
    ("h4", "score_ge_012"),
    ("h4", "score_ge_019"),
    ("ensemble_m30_h1_h4", "all"),
    ("ensemble_m30_h1_h4", "score_ge_012"),
    ("ensemble_m30_h1_h4", "score_ge_019"),
]


def merged_profile(guard_name: str, exec_cfg: Dict[str, Any]) -> Dict[str, Any]:
    profile = {**PROFILES[guard_name], "start_equity": 100000.0}
    if "min_score_percentile" in exec_cfg:
        profile["min_score_percentile"] = safe_float(exec_cfg["min_score_percentile"], 0.0)
    return profile


def merged_live_cfg(base_cfg: Dict[str, Any], exec_cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base_cfg)
    out["live_new_entries_enabled"] = True
    for key, value in exec_cfg.items():
        if key == "min_score_percentile":
            continue
        out[key] = value
    return out


def transform_candidates(candidates: pd.DataFrame, direction_mode: str) -> pd.DataFrame:
    if direction_mode == "normal":
        return candidates
    out = candidates.copy()
    out["direction"] = out["direction"].map(lambda value: "SHORT" if direction_int(value) > 0 else "LONG")
    if "campaign" in out.columns:
        out["campaign"] = out["instrument"].astype(str) + ":" + out["direction"].astype(str)
    return out


def stream_paths(stream_set: str) -> Dict[str, Path]:
    if stream_set == "h1h4":
        return dict(H1_H4_STREAMS)
    return dict(EXPANDED_STREAMS)


def load_stream_candidates(
    stream: str,
    gate: str,
    paths: Dict[str, Path],
    cache: Dict[Tuple[str, str], pd.DataFrame],
) -> pd.DataFrame:
    key = (stream, gate)
    if key in cache:
        return cache[key]
    if stream == "ensemble_m30_h1_h4":
        frames: List[pd.DataFrame] = []
        for source in ["m30", "h1", "h4"]:
            one = load_stream_candidates(source, gate, paths, cache).copy()
            one["source_stream"] = source
            frames.append(one)
        out = pd.concat(frames, ignore_index=True)
        out = out.sort_values(["time_utc", "rank_score", "probability"], ascending=[True, False, False]).reset_index(drop=True)
    else:
        out = load_candidates(paths[stream], SCORE_GATES[gate])
        out["source_stream"] = stream
    cache[key] = out
    return out


def selected_rows(mode: str, stream_set: str) -> List[Tuple[str, str, str, str]]:
    rows: List[Tuple[str, str, str, str]] = []
    exec_names = list(EXECUTION_PROFILES)
    guard_names = GUARD_PROFILE_NAMES
    stream_gates = STREAM_GATE_ROWS if stream_set == "h1h4" else EXPANDED_STREAM_GATE_ROWS
    if mode == "fast":
        exec_names = ["wider_stop", "wide_stop_slow_trail", "very_wide_stop", "quality_strict"]
        guard_names = ["h_tf_balanced", "h_tf_growth"]
        if stream_set == "h1h4":
            stream_gates = [("h1", "all"), ("h1", "score_ge_012"), ("h4", "all"), ("h4", "score_ge_012")]
        else:
            stream_gates = [
                ("m30", "all"),
                ("m30", "score_ge_012"),
                ("h1", "all"),
                ("h1", "score_ge_012"),
                ("h4", "all"),
                ("h4", "score_ge_012"),
                ("ensemble_m30_h1_h4", "all"),
                ("ensemble_m30_h1_h4", "score_ge_012"),
            ]
    for stream, gate in stream_gates:
        for guard_name in guard_names:
            for exec_name in exec_names:
                rows.append((stream, gate, guard_name, exec_name))
    return rows


def run(output_root: Path, mode: str, direction_mode: str, stream_set: str) -> pd.DataFrame:
    selected = selected_rows(mode, stream_set)
    paths = stream_paths(stream_set)
    live_cfg_base = read_json(PRIMARY_CONFIG, {})
    loaded_candidates: Dict[Tuple[str, str], pd.DataFrame] = {}
    all_times: List[pd.Timestamp] = []
    all_instruments: set[str] = set()
    end_time = pd.Timestamp.min.tz_localize("UTC")
    for stream, gate, _guard, _exec in selected:
        key = (stream, gate)
        if key not in loaded_candidates:
            candidates = load_stream_candidates(stream, gate, paths, loaded_candidates)
            all_times.extend(pd.to_datetime(candidates["time_utc"], utc=True).tolist())
            all_instruments.update(candidates["instrument"].astype(str).unique())
            end_time = max(end_time, pd.to_datetime(candidates["planned_exit_time"], utc=True).max())
    for path in CANDLE_ROOT.glob("*_M1.csv"):
        name = path.name.removesuffix("_M1.csv")
        base, quote = name.split("_", 1)
        if base == "USD" or quote == "USD":
            all_instruments.add(name)
    candles = CandleStore(CANDLE_ROOT, all_times, end_time)
    candles.load_all(all_instruments)
    margin_rates = load_margin_rates()
    output_root.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    for stream, gate, guard_name, exec_name in selected:
        candidates = transform_candidates(loaded_candidates[(stream, gate)], direction_mode)
        exec_cfg = EXECUTION_PROFILES[exec_name]
        profile = merged_profile(guard_name, exec_cfg)
        live_cfg = merged_live_cfg(live_cfg_base, exec_cfg)
        replay = BrokerReplay(
            profile,
            candles,
            margin_rates=margin_rates,
            live_cfg=live_cfg,
            max_new_positions_per_timestamp=safe_int(live_cfg_base.get("max_new_positions_per_cycle"), 2),
        )
        out_dir = output_root / f"{stream}_{gate}_{guard_name}_{exec_name}"
        summary = replay.run(candidates)
        row = {
            "stream": stream,
            "stream_set": stream_set,
            "score_gate": gate,
            "direction_mode": direction_mode,
            "guard_profile": guard_name,
            "execution_profile": exec_name,
            "objective": objective(summary),
            **summary,
            **{f"exec_{key}": value for key, value in exec_cfg.items()},
            "guard_target_margin_pct": profile.get("target_margin_pct"),
            "guard_hard_margin_pct": profile.get("hard_margin_pct"),
            "guard_max_open_risk_pct": profile.get("max_open_risk_pct"),
            "output_dir": str(out_dir),
        }
        replay.write_outputs(out_dir, row, candidates)
        rows.append(row)
        print(json.dumps(row, sort_keys=True, default=str), flush=True)
    frame = pd.DataFrame(rows).sort_values("objective", ascending=False)
    frame.to_csv(output_root / "oanda_broker_env_sweep_summary.csv", index=False)
    (output_root / "oanda_broker_env_sweep_summary.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--mode", choices=["fast", "full"], default="fast")
    parser.add_argument("--direction-mode", choices=["normal", "inverted"], default="normal")
    parser.add_argument("--stream-set", choices=["h1h4", "expanded"], default="h1h4")
    args = parser.parse_args()
    frame = run(args.output_root, args.mode, args.direction_mode, args.stream_set)
    print(frame.head(30).to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
