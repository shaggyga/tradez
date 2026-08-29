#!/usr/bin/env python3
"""Replay H1/H4 HGB streams under model-specific guard profiles."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from oanda_live_style_pair_portfolio_backtest import (
    PortfolioReplay,
    build_arg_parser as build_replay_arg_parser,
    write_outputs,
)


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_OUTPUT = REPORTS / "h1_h4_guard_recalibration"

STREAMS = {
    "h1": REPORTS
    / "all68_latest_hgb_reversal_h1_full_trader_style_backtest"
    / "candidate_rows.csv",
    "h4": REPORTS
    / "all68_latest_hgb_reversal_h4_full_trader_style_backtest"
    / "candidate_rows.csv",
}

SCORE_GATES = {
    "all": 0.0,
    "score_ge_012": 0.12,
    "score_ge_019": 0.19,
}

BASE_GUARDS: Dict[str, Any] = {
    "start_equity": 100000.0,
    "train_row_cap": 30000,
    "n_jobs": 4,
    "min_train_rows": 5000,
    "min_calibration_rows": 250,
    "min_test_rows": 250,
    "min_pair_calibration_rows": 20,
    "min_pair_calibration_trades": 3,
    "min_expected_move_atr": 0.0,
    "min_rank_score": 0.0,
    "min_score_percentile": 0.0,
    "min_probability": 0.0,
    "margin_pct_per_risk_pct": 8.0,
    "hard_margin_pct": 74.0,
    "max_same_campaign_positions": 1,
    "min_campaign_entry_gap_minutes": 45.0,
    "min_pair_entry_gap_minutes": 20.0,
    "min_after_close_gap_minutes": 10.0,
    "stale_rotation_min_score_percentile": 0.90,
    "stale_rotation_open_share": 0.70,
    "max_rotations_per_timestamp": 2,
    "drawdown_entry_halt_pct": 0.0,
    "drawdown_halt_bypass_score_percentile": 0.98,
    "daily_loss_bypass_score_percentile": 0.99,
}

PROFILES: Dict[str, Dict[str, Any]] = {
    "native_replay": {
        "min_risk_pct": 0.18,
        "low_risk_pct": 0.28,
        "medium_risk_pct": 0.40,
        "high_risk_pct": 0.52,
        "max_risk_pct": 0.65,
        "min_throttled_risk_pct": 0.05,
        "target_margin_pct": 62.0,
        "hard_margin_pct": 74.0,
        "emergency_margin_pct": 88.0,
        "target_open_risk_pct": 0.0,
        "max_open_risk_pct": 0.0,
        "max_currency_risk_pct": 0.0,
        "above_target_min_score_percentile": 0.80,
        "drawdown_risk_throttle_start_pct": 10.0,
        "drawdown_risk_throttle_full_pct": 25.0,
        "drawdown_risk_min_multiplier": 0.25,
        "drawdown_open_risk_min_multiplier": 0.45,
        "max_daily_loss_pct": 0.0,
        "max_pair_daily_loss_pct": 0.0,
        "max_pair_total_loss_pct": 0.0,
        "pair_loss_streak_cooldown_trades": 0,
        "pair_loss_cooldown_minutes": 240.0,
        "pair_large_loss_cooldown_pct": 0.0,
        "max_open_positions": 18,
        "max_same_pair_positions": 2,
        "stale_minutes": 90.0,
        "min_rotation_age_minutes": 45.0,
        "min_opposite_rotation_age_minutes": 30.0,
        "rotation_score_multiplier": 1.25,
        "opposite_rotation_multiplier": 1.15,
    },
    "primary_strict": {
        "min_risk_pct": 0.15,
        "low_risk_pct": 0.30,
        "medium_risk_pct": 0.50,
        "high_risk_pct": 0.65,
        "max_risk_pct": 0.75,
        "min_throttled_risk_pct": 0.05,
        "target_margin_pct": 58.0,
        "hard_margin_pct": 74.0,
        "emergency_margin_pct": 86.0,
        "target_open_risk_pct": 4.8,
        "max_open_risk_pct": 6.5,
        "max_currency_risk_pct": 3.0,
        "above_target_min_score_percentile": 0.90,
        "drawdown_risk_throttle_start_pct": 10.0,
        "drawdown_risk_throttle_full_pct": 22.0,
        "drawdown_risk_min_multiplier": 0.30,
        "drawdown_open_risk_min_multiplier": 0.55,
        "max_daily_loss_pct": 5.0,
        "max_pair_daily_loss_pct": 1.8,
        "max_pair_total_loss_pct": 6.5,
        "pair_loss_streak_cooldown_trades": 3,
        "pair_loss_cooldown_minutes": 300.0,
        "pair_large_loss_cooldown_pct": 1.2,
        "max_open_positions": 24,
        "max_same_pair_positions": 1,
        "stale_minutes": 96.0,
        "min_rotation_age_minutes": 60.0,
        "min_opposite_rotation_age_minutes": 30.0,
        "rotation_score_multiplier": 1.35,
        "opposite_rotation_multiplier": 1.20,
    },
    "h_tf_balanced": {
        "min_risk_pct": 0.16,
        "low_risk_pct": 0.30,
        "medium_risk_pct": 0.46,
        "high_risk_pct": 0.60,
        "max_risk_pct": 0.72,
        "min_throttled_risk_pct": 0.06,
        "target_margin_pct": 62.0,
        "hard_margin_pct": 76.0,
        "emergency_margin_pct": 88.0,
        "target_open_risk_pct": 6.0,
        "max_open_risk_pct": 8.0,
        "max_currency_risk_pct": 4.0,
        "above_target_min_score_percentile": 0.86,
        "drawdown_risk_throttle_start_pct": 9.0,
        "drawdown_risk_throttle_full_pct": 18.0,
        "drawdown_risk_min_multiplier": 0.35,
        "drawdown_open_risk_min_multiplier": 0.60,
        "max_daily_loss_pct": 6.0,
        "max_pair_daily_loss_pct": 2.6,
        "max_pair_total_loss_pct": 9.0,
        "pair_loss_streak_cooldown_trades": 4,
        "pair_loss_cooldown_minutes": 240.0,
        "pair_large_loss_cooldown_pct": 1.8,
        "max_open_positions": 24,
        "max_same_pair_positions": 1,
        "stale_minutes": 120.0,
        "min_rotation_age_minutes": 60.0,
        "min_opposite_rotation_age_minutes": 45.0,
        "rotation_score_multiplier": 1.30,
        "opposite_rotation_multiplier": 1.18,
    },
    "h_tf_growth": {
        "min_risk_pct": 0.18,
        "low_risk_pct": 0.34,
        "medium_risk_pct": 0.52,
        "high_risk_pct": 0.68,
        "max_risk_pct": 0.82,
        "min_throttled_risk_pct": 0.06,
        "target_margin_pct": 68.0,
        "hard_margin_pct": 82.0,
        "emergency_margin_pct": 90.0,
        "target_open_risk_pct": 7.2,
        "max_open_risk_pct": 9.5,
        "max_currency_risk_pct": 5.0,
        "above_target_min_score_percentile": 0.84,
        "drawdown_risk_throttle_start_pct": 10.0,
        "drawdown_risk_throttle_full_pct": 22.0,
        "drawdown_risk_min_multiplier": 0.30,
        "drawdown_open_risk_min_multiplier": 0.55,
        "max_daily_loss_pct": 7.0,
        "max_pair_daily_loss_pct": 3.0,
        "max_pair_total_loss_pct": 11.0,
        "pair_loss_streak_cooldown_trades": 4,
        "pair_loss_cooldown_minutes": 210.0,
        "pair_large_loss_cooldown_pct": 2.2,
        "max_open_positions": 30,
        "max_same_pair_positions": 1,
        "stale_minutes": 120.0,
        "min_rotation_age_minutes": 60.0,
        "min_opposite_rotation_age_minutes": 45.0,
        "rotation_score_multiplier": 1.25,
        "opposite_rotation_multiplier": 1.15,
    },
    "h_tf_conservative": {
        "min_risk_pct": 0.12,
        "low_risk_pct": 0.24,
        "medium_risk_pct": 0.38,
        "high_risk_pct": 0.52,
        "max_risk_pct": 0.62,
        "min_throttled_risk_pct": 0.04,
        "target_margin_pct": 50.0,
        "hard_margin_pct": 66.0,
        "emergency_margin_pct": 80.0,
        "target_open_risk_pct": 4.2,
        "max_open_risk_pct": 5.8,
        "max_currency_risk_pct": 2.6,
        "above_target_min_score_percentile": 0.90,
        "drawdown_risk_throttle_start_pct": 7.0,
        "drawdown_risk_throttle_full_pct": 15.0,
        "drawdown_risk_min_multiplier": 0.35,
        "drawdown_open_risk_min_multiplier": 0.60,
        "max_daily_loss_pct": 4.5,
        "max_pair_daily_loss_pct": 1.8,
        "max_pair_total_loss_pct": 6.5,
        "pair_loss_streak_cooldown_trades": 3,
        "pair_loss_cooldown_minutes": 360.0,
        "pair_large_loss_cooldown_pct": 1.2,
        "max_open_positions": 18,
        "max_same_pair_positions": 1,
        "stale_minutes": 150.0,
        "min_rotation_age_minutes": 75.0,
        "min_opposite_rotation_age_minutes": 60.0,
        "rotation_score_multiplier": 1.40,
        "opposite_rotation_multiplier": 1.25,
    },
}


def load_candidates(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True)
    frame["planned_exit_time"] = pd.to_datetime(frame["planned_exit_time"], utc=True)
    if "score_percentile" not in frame:
        frame["score_percentile"] = frame["rank_score"].rank(method="average", pct=True)
    return frame


def replay_args(output_dir: Path, profile: Dict[str, Any]) -> argparse.Namespace:
    args = build_replay_arg_parser().parse_args([])
    for key, value in {**BASE_GUARDS, **profile}.items():
        setattr(args, key, value)
    args.output_dir = output_dir
    args.candidates_csv = None
    args.settings_csv = Path("")
    args.skip_unsupported = True
    args.max_pairs = 0
    args.thresholds = ""
    return args


def objective(summary: Dict[str, Any]) -> float:
    ret = float(summary.get("return_pct", 0.0))
    dd = float(summary.get("max_drawdown_pct", 0.0))
    pf = float(summary.get("profit_factor", 0.0))
    trades = float(summary.get("opened_trades", 0.0))
    if trades < 100:
        return -9999.0
    return (ret / max(1.0, dd)) * max(0.0, min(pf, 3.0))


def run(output_root: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    output_root.mkdir(parents=True, exist_ok=True)
    for stream, source in STREAMS.items():
        candidates_all = load_candidates(source)
        for gate_name, min_score in SCORE_GATES.items():
            candidates = candidates_all[candidates_all["rank_score"] >= min_score].copy()
            if candidates.empty:
                continue
            for profile_name, profile in PROFILES.items():
                out_dir = output_root / f"{stream}_{gate_name}_{profile_name}"
                args = replay_args(out_dir, profile)
                replay = PortfolioReplay(args)
                summary = replay.run(candidates)
                summary_row = {
                    "stream": stream,
                    "score_gate": gate_name,
                    "min_rank_score_filter": min_score,
                    "profile": profile_name,
                    "objective": objective(summary),
                    **{
                        key: summary.get(key)
                        for key in [
                            "candidate_rows",
                            "opened_trades",
                            "blocked_candidates",
                            "unique_pairs_traded",
                            "return_pct",
                            "profit_factor",
                            "max_drawdown_pct",
                            "win_rate",
                            "avg_margin_used_pct",
                            "max_margin_used_pct",
                            "avg_open_risk_pct",
                            "max_open_risk_pct",
                            "stale_rotations",
                            "opposite_rotations",
                            "scheduled_exits",
                        ]
                    },
                    **{f"guard_{key}": value for key, value in profile.items()},
                    "output_dir": str(out_dir),
                }
                rows.append(summary_row)
                write_outputs(
                    out_dir,
                    summary,
                    candidates,
                    pd.DataFrame(),
                    pd.DataFrame(),
                    [],
                    replay,
                    args,
                )
                print(json.dumps(summary_row, sort_keys=True), flush=True)
    summary_frame = pd.DataFrame(rows).sort_values(
        ["stream", "objective"],
        ascending=[True, False],
    )
    summary_frame.to_csv(output_root / "guard_recalibration_summary.csv", index=False)
    (output_root / "guard_recalibration_summary.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return summary_frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    summary = run(args.output_root)
    print(summary.head(20).to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
