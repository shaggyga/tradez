#!/usr/bin/env python3
"""Focused OANDA replay for the currently wired primary forecast-rotation bot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

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
from oanda_h1_h4_guard_recalibration import PROFILES


DEFAULT_OUTPUT = REPORTS / "primary_live_exact_backtest"


def selected_profile(live_cfg: Dict[str, Any], profile_name: str, start_equity: float) -> Dict[str, Any]:
    profile = {**PROFILES[profile_name], "start_equity": start_equity}
    # Keep the normalized guard profile aligned with the currently deployed live config.
    profile.update(
        {
            "target_margin_pct": safe_float(live_cfg.get("target_margin_used_pct"), profile.get("target_margin_pct")),
            "hard_margin_pct": safe_float(live_cfg.get("max_margin_used_pct"), profile.get("hard_margin_pct")),
            "emergency_margin_pct": safe_float(live_cfg.get("emergency_margin_used_pct"), profile.get("emergency_margin_pct")),
            "target_open_risk_pct": safe_float(live_cfg.get("target_open_risk_pct"), profile.get("target_open_risk_pct")),
            "max_open_risk_pct": safe_float(live_cfg.get("max_open_risk_pct"), profile.get("max_open_risk_pct")),
            "max_currency_risk_pct": safe_float(live_cfg.get("max_currency_risk_pct"), profile.get("max_currency_risk_pct")),
            "max_open_positions": safe_int(live_cfg.get("max_open_positions"), profile.get("max_open_positions")),
            "max_same_pair_positions": safe_int(live_cfg.get("max_same_pair_positions"), profile.get("max_same_pair_positions")),
            "max_risk_pct": safe_float(live_cfg.get("risk_pct_max"), profile.get("max_risk_pct")),
            "min_risk_pct": safe_float(live_cfg.get("risk_pct_min"), profile.get("min_risk_pct")),
            "drawdown_risk_throttle_start_pct": safe_float(
                live_cfg.get("drawdown_risk_throttle_start_pct"),
                profile.get("drawdown_risk_throttle_start_pct"),
            ),
            "drawdown_risk_throttle_full_pct": safe_float(
                live_cfg.get("drawdown_risk_throttle_full_pct"),
                profile.get("drawdown_risk_throttle_full_pct"),
            ),
            "drawdown_risk_min_multiplier": safe_float(
                live_cfg.get("drawdown_risk_min_multiplier"),
                profile.get("drawdown_risk_min_multiplier"),
            ),
            "drawdown_open_risk_min_multiplier": safe_float(
                live_cfg.get("drawdown_open_risk_min_multiplier"),
                profile.get("drawdown_open_risk_min_multiplier"),
            ),
            "max_pair_daily_loss_pct": safe_float(live_cfg.get("max_pair_daily_loss_pct"), profile.get("max_pair_daily_loss_pct")),
            "max_pair_total_loss_pct": safe_float(live_cfg.get("max_pair_total_loss_pct"), profile.get("max_pair_total_loss_pct")),
            "pair_loss_streak_cooldown_trades": safe_int(
                live_cfg.get("pair_loss_streak_cooldown_trades"),
                profile.get("pair_loss_streak_cooldown_trades"),
            ),
            "pair_loss_cooldown_minutes": safe_float(
                live_cfg.get("pair_loss_cooldown_minutes"),
                profile.get("pair_loss_cooldown_minutes"),
            ),
            "pair_large_loss_cooldown_pct": safe_float(
                live_cfg.get("pair_large_loss_cooldown_pct"),
                profile.get("pair_large_loss_cooldown_pct"),
            ),
            "stale_minutes": safe_float(live_cfg.get("signal_gone_exit_minutes"), profile.get("stale_minutes")),
            "min_rotation_age_minutes": safe_float(
                live_cfg.get("replacement_min_age_minutes"),
                safe_float(live_cfg.get("min_hold_minutes"), profile.get("min_rotation_age_minutes")),
            ),
            "opposite_rotation_multiplier": safe_float(
                live_cfg.get("opposite_min_score_multiplier"),
                profile.get("opposite_rotation_multiplier"),
            ),
            "rotation_score_multiplier": safe_float(
                live_cfg.get("replacement_min_score_multiplier"),
                profile.get("rotation_score_multiplier"),
            ),
            "replacement_min_score_improvement": safe_float(
                live_cfg.get("replacement_min_score_improvement"),
                profile.get("replacement_min_score_improvement"),
            ),
            "max_replacements_per_hour": safe_int(
                live_cfg.get("max_replacements_per_hour"),
                profile.get("max_replacements_per_hour"),
            ),
        }
    )
    return profile


def run(
    output_dir: Path,
    *,
    config_path: Path,
    candidate_rows_csv: Path | None,
    profile_name: str,
    start_equity: float,
) -> Dict[str, Any]:
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
    replay_cfg = dict(live_cfg)
    replay_cfg["simulate_live_exit_checks"] = True
    profile = selected_profile(live_cfg, profile_name, start_equity)
    replay = BrokerReplay(
        profile,
        candles,
        margin_rates=load_margin_rates(),
        live_cfg=replay_cfg,
        max_new_positions_per_timestamp=safe_int(live_cfg.get("max_new_positions_per_cycle"), 2),
    )
    summary = replay.run(candidates)
    summary_row = {
        "scenario": "primary_live_exact",
        "candidate_rows_csv": str(candidate_path),
        "config_path": str(config_path),
        "profile_name": profile_name,
        "objective": objective(summary),
        **summary,
        "signal_gone_exit_minutes": safe_float(live_cfg.get("signal_gone_exit_minutes"), 0.0),
        "signal_gone_min_horizon_fraction": safe_float(live_cfg.get("signal_gone_min_horizon_fraction"), 0.0),
        "economics_exit_min_horizon_fraction": safe_float(live_cfg.get("economics_exit_min_horizon_fraction"), 0.0),
        "soft_loss_exit_min_horizon_fraction": safe_float(live_cfg.get("soft_loss_exit_min_horizon_fraction"), 0.0),
        "hold_same_signal_past_horizon": bool(live_cfg.get("hold_same_signal_past_horizon", False)),
        "max_new_positions_per_timestamp": safe_int(live_cfg.get("max_new_positions_per_cycle"), 2),
    }
    replay.write_outputs(output_dir, summary_row, candidates)
    (output_dir / "live_config_snapshot.json").write_text(
        json.dumps(live_cfg, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "profile_snapshot.json").write_text(
        json.dumps(profile, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return summary_row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--config", type=Path, default=PRIMARY_CONFIG)
    parser.add_argument("--candidate-rows-csv", type=Path, default=None)
    parser.add_argument("--profile-name", choices=sorted(PROFILES), default="h_tf_growth")
    parser.add_argument("--start-equity", type=float, default=100000.0)
    args = parser.parse_args()
    summary = run(
        args.output_dir,
        config_path=args.config,
        candidate_rows_csv=args.candidate_rows_csv,
        profile_name=args.profile_name,
        start_equity=args.start_equity,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
