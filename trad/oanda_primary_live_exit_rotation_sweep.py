#!/usr/bin/env python3
"""Sweep live-style exit and stale-rotation settings for the primary OANDA bot."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from oanda_broker_style_portfolio_replay import (
    BrokerReplay,
    CandleStore,
    CANDLE_ROOT,
    PRIMARY_CONFIG,
    REPORTS,
    instrument_parts,
    load_candidates,
    load_margin_rates,
    objective,
    read_json,
    safe_float,
    safe_int,
)
from oanda_primary_live_exact_backtest import selected_profile


DEFAULT_OUTPUT = REPORTS / "primary_live_exit_rotation_sweep"


def signal_variants() -> List[Dict[str, Any]]:
    return [
        {
            "signal_name": "sg_off",
            "signal_gone_exit_minutes": 100000.0,
            "signal_gone_min_horizon_fraction": 1000.0,
        },
        {
            "signal_name": "sg_60_no_hfloor",
            "signal_gone_exit_minutes": 60.0,
            "signal_gone_min_horizon_fraction": 0.0,
        },
        {
            "signal_name": "sg_90_075h",
            "signal_gone_exit_minutes": 90.0,
            "signal_gone_min_horizon_fraction": 0.75,
        },
        {
            "signal_name": "sg_120_1h",
            "signal_gone_exit_minutes": 120.0,
            "signal_gone_min_horizon_fraction": 1.0,
        },
        {
            "signal_name": "sg_180_1h",
            "signal_gone_exit_minutes": 180.0,
            "signal_gone_min_horizon_fraction": 1.0,
        },
        {
            "signal_name": "sg_240_1h",
            "signal_gone_exit_minutes": 240.0,
            "signal_gone_min_horizon_fraction": 1.0,
        },
    ]


def stale_variants() -> List[Dict[str, Any]]:
    return [
        {
            "stale_name": "stale_90_minrot45",
            "stale_minutes": 90.0,
            "min_rotation_age_minutes": 45.0,
        },
        {
            "stale_name": "stale_120_minrot60",
            "stale_minutes": 120.0,
            "min_rotation_age_minutes": 60.0,
        },
        {
            "stale_name": "stale_180_minrot75",
            "stale_minutes": 180.0,
            "min_rotation_age_minutes": 75.0,
        },
        {
            "stale_name": "stale_off",
            "stale_minutes": 100000.0,
            "min_rotation_age_minutes": 60.0,
        },
    ]


def rotation_variants() -> List[Dict[str, Any]]:
    return [
        {
            "rotation_name": "replace_live",
            "rotation_score_multiplier": 1.15,
            "replacement_min_score_multiplier": 1.15,
            "replacement_min_score_improvement": 0.003,
            "max_rotations_per_timestamp": 2,
            "max_replacements_per_hour": 4,
        },
        {
            "rotation_name": "replace_strict",
            "rotation_score_multiplier": 1.50,
            "replacement_min_score_multiplier": 1.50,
            "replacement_min_score_improvement": 0.015,
            "max_rotations_per_timestamp": 1,
            "max_replacements_per_hour": 2,
        },
        {
            "rotation_name": "replace_very_strict",
            "rotation_score_multiplier": 1.75,
            "replacement_min_score_multiplier": 1.75,
            "replacement_min_score_improvement": 0.025,
            "max_rotations_per_timestamp": 1,
            "max_replacements_per_hour": 1,
        },
        {
            "rotation_name": "replace_off",
            "rotation_score_multiplier": 999.0,
            "replacement_min_score_multiplier": 999.0,
            "replacement_min_score_improvement": 999.0,
            "max_rotations_per_timestamp": 0,
            "max_replacements_per_hour": 0,
        },
    ]


def profit_lock_variants(mode: str) -> List[Dict[str, Any]]:
    current = {
        "profit_lock_name": "profit_lock_current",
        "profit_lock_min_age_minutes": 1.0,
        "profit_lock_min_pips": 0.5,
        "profit_lock_r_multiple": 0.2,
    }
    if mode == "fast":
        return [current]
    return [
        current,
        {
            "profit_lock_name": "profit_lock_delayed",
            "profit_lock_min_age_minutes": 30.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.2,
        },
        {
            "profit_lock_name": "profit_lock_30m_035r",
            "profit_lock_min_age_minutes": 30.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.35,
        },
        {
            "profit_lock_name": "profit_lock_45m_050r",
            "profit_lock_min_age_minutes": 45.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.50,
        },
        {
            "profit_lock_name": "profit_lock_off",
            "profit_lock_min_age_minutes": 100000.0,
            "profit_lock_min_pips": 100000.0,
            "profit_lock_r_multiple": 100000.0,
        },
    ]


def exit_guard_variants(mode: str) -> List[Dict[str, Any]]:
    current = {
        "exit_guard_name": "exit_current",
        "exit_fade_confirm_cycles": 1,
        "signal_gone_loser_policy": "allow",
        "signal_gone_loser_buffer_pips": 0.0,
    }
    if mode == "fast":
        return [current]
    return [
        current,
        {
            "exit_guard_name": "exit_confirm2",
            "exit_fade_confirm_cycles": 2,
            "signal_gone_loser_policy": "allow",
            "signal_gone_loser_buffer_pips": 0.0,
        },
        {
            "exit_guard_name": "exit_defer_losers",
            "exit_fade_confirm_cycles": 1,
            "signal_gone_loser_policy": "defer_losers_unless_opposite",
            "signal_gone_loser_buffer_pips": 0.0,
        },
        {
            "exit_guard_name": "exit_defer_losers_confirm2",
            "exit_fade_confirm_cycles": 2,
            "signal_gone_loser_policy": "defer_losers_unless_opposite",
            "signal_gone_loser_buffer_pips": 0.0,
        },
        {
            "exit_guard_name": "exit_defer_losers_buffer1_confirm2",
            "exit_fade_confirm_cycles": 2,
            "signal_gone_loser_policy": "defer_losers_unless_opposite",
            "signal_gone_loser_buffer_pips": 1.0,
        },
    ]


def build_variants(mode: str) -> List[Dict[str, Any]]:
    variants: List[Dict[str, Any]] = []
    for signal in signal_variants():
        for stale in stale_variants():
            for rotation in rotation_variants():
                for profit_lock in profit_lock_variants(mode):
                    for exit_guard in exit_guard_variants(mode):
                        variant = {**signal, **stale, **rotation, **profit_lock, **exit_guard}
                        variant["variant_name"] = "_".join(
                            [
                                str(variant["signal_name"]),
                                str(variant["stale_name"]),
                                str(variant["rotation_name"]),
                                str(variant["profit_lock_name"]),
                                str(variant["exit_guard_name"]),
                            ]
                        )
                        variants.append(variant)
    return variants


def load_replay_inputs(config_path: Path, candidate_rows_csv: Path | None) -> tuple[Dict[str, Any], pd.DataFrame, CandleStore]:
    live_cfg = read_json(config_path, {})
    stream_cfg = live_cfg.get("model_stream") if isinstance(live_cfg.get("model_stream"), dict) else {}
    candidate_path = candidate_rows_csv or Path(str(stream_cfg.get("candidate_rows_csv") or ""))
    if not candidate_path.exists():
        raise FileNotFoundError(f"Missing candidate rows CSV: {candidate_path}")
    candidates = load_candidates(candidate_path, safe_float(live_cfg.get("min_rank_score"), 0.0))
    all_times = pd.to_datetime(candidates["time_utc"], utc=True).tolist()
    all_instruments = set(candidates["instrument"].astype(str).unique())
    end_time = pd.to_datetime(candidates["planned_exit_time"], utc=True).max()
    for path in CANDLE_ROOT.glob("*_M1.csv"):
        name = path.name.removesuffix("_M1.csv")
        base, quote = instrument_parts(name)
        if base == "USD" or quote == "USD":
            all_instruments.add(name)
    candles = CandleStore(CANDLE_ROOT, all_times, end_time)
    candles.load_all(all_instruments)
    live_cfg["_sweep_candidate_rows_csv"] = str(candidate_path)
    return live_cfg, candidates, candles


def run_variant(
    *,
    base_cfg: Dict[str, Any],
    base_profile: Dict[str, Any],
    candidates: pd.DataFrame,
    candles: CandleStore,
    margin_rates: Dict[str, float],
    variant: Dict[str, Any],
    run_dir: Path,
    max_new_positions_per_timestamp: int,
) -> Dict[str, Any]:
    live_cfg = dict(base_cfg)
    live_cfg["simulate_live_exit_checks"] = True
    for key in [
        "signal_gone_exit_minutes",
        "signal_gone_min_horizon_fraction",
        "profit_lock_min_age_minutes",
        "profit_lock_min_pips",
        "profit_lock_r_multiple",
        "exit_fade_confirm_cycles",
        "signal_gone_loser_policy",
        "signal_gone_loser_buffer_pips",
        "replacement_min_score_multiplier",
        "replacement_min_score_improvement",
        "max_replacements_per_hour",
    ]:
        live_cfg[key] = variant[key]
    live_cfg["replacement_min_age_minutes"] = variant["min_rotation_age_minutes"]
    profile = dict(base_profile)
    for key in [
        "stale_minutes",
        "min_rotation_age_minutes",
        "rotation_score_multiplier",
        "max_rotations_per_timestamp",
        "replacement_min_score_improvement",
        "max_replacements_per_hour",
    ]:
        profile[key] = variant[key]
    replay = BrokerReplay(
        profile,
        candles,
        margin_rates=margin_rates,
        live_cfg=live_cfg,
        max_new_positions_per_timestamp=max_new_positions_per_timestamp,
    )
    start = time.perf_counter()
    summary = replay.run(candidates)
    elapsed = time.perf_counter() - start
    row = {
        "variant_name": variant["variant_name"],
        "objective": objective(summary),
        "elapsed_seconds": round(elapsed, 3),
        **{key: value for key, value in variant.items() if key != "variant_name"},
        **summary,
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(json.dumps(row, indent=2, sort_keys=True, default=str), encoding="utf-8")
    pd.DataFrame(replay.closed_rows).to_csv(run_dir / "trades.csv", index=False)
    pd.DataFrame(replay.curve_rows).to_csv(run_dir / "equity_curve.csv", index=False)
    return row


def run(
    output_root: Path,
    *,
    mode: str,
    config_path: Path,
    candidate_rows_csv: Path | None,
    profile_name: str,
    start_equity: float,
    max_runs: int,
    only_signal: str,
    only_stale: str,
    only_rotation: str,
    only_profit_lock: str,
    only_exit_guard: str,
) -> pd.DataFrame:
    output_root.mkdir(parents=True, exist_ok=True)
    live_cfg, candidates, candles = load_replay_inputs(config_path, candidate_rows_csv)
    (output_root / "candidate_rows_source.txt").write_text(
        str(live_cfg.get("_sweep_candidate_rows_csv") or ""),
        encoding="utf-8",
    )
    base_profile = selected_profile(live_cfg, profile_name, start_equity)
    margin_rates = load_margin_rates()
    variants = build_variants(mode)
    filters = {
        "signal_name": only_signal,
        "stale_name": only_stale,
        "rotation_name": only_rotation,
        "profit_lock_name": only_profit_lock,
        "exit_guard_name": only_exit_guard,
    }
    for key, allowed_text in filters.items():
        allowed = {item.strip() for item in str(allowed_text or "").split(",") if item.strip()}
        if allowed:
            variants = [variant for variant in variants if str(variant.get(key)) in allowed]
    if max_runs > 0:
        variants = variants[:max_runs]
    rows: List[Dict[str, Any]] = []
    max_new = safe_int(live_cfg.get("max_new_positions_per_cycle"), 2)
    for idx, variant in enumerate(variants, start=1):
        run_dir = output_root / "runs" / f"{idx:04d}_{variant['variant_name']}"
        row = run_variant(
            base_cfg=live_cfg,
            base_profile=base_profile,
            candidates=candidates,
            candles=candles,
            margin_rates=margin_rates,
            variant=variant,
            run_dir=run_dir,
            max_new_positions_per_timestamp=max_new,
        )
        row["run_index"] = idx
        row["run_dir"] = str(run_dir)
        rows.append(row)
        print(json.dumps(row, sort_keys=True, default=str), flush=True)
    frame = pd.DataFrame(rows).sort_values("objective", ascending=False)
    frame.to_csv(output_root / "sweep_summary.csv", index=False)
    (output_root / "sweep_summary.json").write_text(
        json.dumps(frame.to_dict(orient="records"), indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--mode", choices=["fast", "full"], default="fast")
    parser.add_argument("--config", type=Path, default=PRIMARY_CONFIG)
    parser.add_argument("--candidate-rows-csv", type=Path, default=None)
    parser.add_argument("--profile-name", default="h_tf_growth")
    parser.add_argument("--start-equity", type=float, default=100000.0)
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("--only-signal", default="", help="Comma-separated signal_name filters.")
    parser.add_argument("--only-stale", default="", help="Comma-separated stale_name filters.")
    parser.add_argument("--only-rotation", default="", help="Comma-separated rotation_name filters.")
    parser.add_argument("--only-profit-lock", default="", help="Comma-separated profit_lock_name filters.")
    parser.add_argument("--only-exit-guard", default="", help="Comma-separated exit_guard_name filters.")
    args = parser.parse_args()
    frame = run(
        args.output_root,
        mode=args.mode,
        config_path=args.config,
        candidate_rows_csv=args.candidate_rows_csv,
        profile_name=args.profile_name,
        start_equity=args.start_equity,
        max_runs=args.max_runs,
        only_signal=args.only_signal,
        only_stale=args.only_stale,
        only_rotation=args.only_rotation,
        only_profit_lock=args.only_profit_lock,
        only_exit_guard=args.only_exit_guard,
    )
    print(frame.head(30).to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
