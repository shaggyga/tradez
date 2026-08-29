#!/usr/bin/env python3
"""Extensive live-style backtests for the currently wired primary forecast stream."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd

from oanda_broker_style_portfolio_replay import (
    BrokerReplay,
    PRIMARY_CONFIG,
    REPORTS,
    load_margin_rates,
    objective,
    safe_float,
    safe_int,
)
from oanda_primary_live_exact_backtest import selected_profile
from oanda_primary_live_exit_rotation_sweep import load_replay_inputs


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def scenario(
    name: str,
    group: str,
    *,
    cfg_updates: Dict[str, Any] | None = None,
    profile_updates: Dict[str, Any] | None = None,
    filter_kind: str = "",
    filter_value: str = "",
    notes: str = "",
) -> Dict[str, Any]:
    return {
        "scenario_name": name,
        "scenario_group": group,
        "cfg_updates": dict(cfg_updates or {}),
        "profile_updates": dict(profile_updates or {}),
        "filter_kind": filter_kind,
        "filter_value": filter_value,
        "notes": notes,
    }


def scale_profile(profile: Dict[str, Any], multiplier: float) -> Dict[str, Any]:
    keys = [
        "max_risk_pct",
        "min_risk_pct",
        "high_risk_pct",
        "medium_risk_pct",
        "low_risk_pct",
        "target_open_risk_pct",
        "max_open_risk_pct",
        "max_currency_risk_pct",
    ]
    return {key: safe_float(profile.get(key), 0.0) * multiplier for key in keys if key in profile}


def build_scenarios(base_cfg: Dict[str, Any], base_profile: Dict[str, Any], start_equity: float) -> List[Dict[str, Any]]:
    cfg_slip_default = safe_float(base_cfg.get("slippage_pips_default"), 0.05)
    cfg_slip_exotic = safe_float(base_cfg.get("slippage_pips_exotic"), 0.5)
    spread_cost = safe_float(base_cfg.get("spread_cost_multiplier"), 1.0)
    margin_target = safe_float(base_cfg.get("target_margin_used_pct"), 68.0)
    margin_hard = safe_float(base_cfg.get("max_margin_used_pct"), 82.0)
    margin_emergency = safe_float(base_cfg.get("emergency_margin_used_pct"), 90.0)
    scenarios: List[Dict[str, Any]] = [
        scenario("live_current_exact", "baseline", notes="Current primary config and current primary model stream."),
        scenario(
            "live_nav_half_risk",
            "risk_sizing",
            profile_updates=scale_profile(base_profile, 0.5),
            notes="Half live risk and open-risk budget.",
        ),
        scenario(
            "live_nav_75pct_risk",
            "risk_sizing",
            profile_updates=scale_profile(base_profile, 0.75),
            notes="75% live risk and open-risk budget.",
        ),
        scenario(
            "live_nav_125pct_risk",
            "risk_sizing",
            profile_updates=scale_profile(base_profile, 1.25),
            notes="125% live risk and open-risk budget.",
        ),
        scenario(
            "margin_lower_55_72_86",
            "margin",
            cfg_updates={
                "target_margin_used_pct": min(margin_target, 55.0),
                "max_margin_used_pct": min(margin_hard, 72.0),
                "emergency_margin_used_pct": min(margin_emergency, 86.0),
            },
            profile_updates={
                "target_margin_pct": min(margin_target, 55.0),
                "hard_margin_pct": min(margin_hard, 72.0),
                "emergency_margin_pct": min(margin_emergency, 86.0),
            },
            notes="Older/lower margin cap style.",
        ),
        scenario(
            "margin_tight_45_65_75",
            "margin",
            cfg_updates={
                "target_margin_used_pct": 45.0,
                "max_margin_used_pct": 65.0,
                "emergency_margin_used_pct": 75.0,
            },
            profile_updates={
                "target_margin_pct": 45.0,
                "hard_margin_pct": 65.0,
                "emergency_margin_pct": 75.0,
            },
            notes="Conservative margin cap stress.",
        ),
        scenario(
            "slippage_2x",
            "cost_stress",
            cfg_updates={
                "slippage_pips_default": cfg_slip_default * 2.0,
                "slippage_pips_exotic": cfg_slip_exotic * 2.0,
            },
        ),
        scenario(
            "slippage_4x",
            "cost_stress",
            cfg_updates={
                "slippage_pips_default": cfg_slip_default * 4.0,
                "slippage_pips_exotic": cfg_slip_exotic * 4.0,
            },
        ),
        scenario(
            "spread_cost_1_5x",
            "cost_stress",
            cfg_updates={"spread_cost_multiplier": spread_cost * 1.5},
        ),
        scenario(
            "spread_cost_2x",
            "cost_stress",
            cfg_updates={"spread_cost_multiplier": spread_cost * 2.0},
        ),
        scenario(
            "signal_gone_off",
            "exit",
            cfg_updates={"signal_gone_exit_minutes": 100000.0, "signal_gone_min_horizon_fraction": 1000.0},
        ),
        scenario(
            "signal_gone_90_075h",
            "exit",
            cfg_updates={"signal_gone_exit_minutes": 90.0, "signal_gone_min_horizon_fraction": 0.75},
        ),
        scenario(
            "signal_gone_180_1h",
            "exit",
            cfg_updates={"signal_gone_exit_minutes": 180.0, "signal_gone_min_horizon_fraction": 1.0},
        ),
        scenario(
            "signal_gone_240_1h",
            "exit",
            cfg_updates={"signal_gone_exit_minutes": 240.0, "signal_gone_min_horizon_fraction": 1.0},
        ),
        scenario(
            "profit_lock_delayed",
            "exit",
            cfg_updates={"profit_lock_min_age_minutes": 30.0, "profit_lock_min_pips": 0.5, "profit_lock_r_multiple": 0.2},
        ),
        scenario(
            "profit_lock_off",
            "exit",
            cfg_updates={
                "profit_lock_min_age_minutes": 100000.0,
                "profit_lock_min_pips": 100000.0,
                "profit_lock_r_multiple": 100000.0,
            },
        ),
        scenario(
            "replacement_strict",
            "rotation",
            cfg_updates={
                "replacement_min_score_multiplier": 1.50,
                "replacement_min_score_improvement": 0.015,
                "max_replacements_per_hour": 2,
            },
            profile_updates={
                "rotation_score_multiplier": 1.50,
                "replacement_min_score_improvement": 0.015,
                "max_rotations_per_timestamp": 1,
                "max_replacements_per_hour": 2,
            },
        ),
        scenario(
            "replacement_very_strict",
            "rotation",
            cfg_updates={
                "replacement_min_score_multiplier": 1.75,
                "replacement_min_score_improvement": 0.025,
                "max_replacements_per_hour": 1,
            },
            profile_updates={
                "rotation_score_multiplier": 1.75,
                "replacement_min_score_improvement": 0.025,
                "max_rotations_per_timestamp": 1,
                "max_replacements_per_hour": 1,
            },
        ),
        scenario(
            "replacement_off",
            "rotation",
            cfg_updates={
                "replacement_min_score_multiplier": 999.0,
                "replacement_min_score_improvement": 999.0,
                "max_replacements_per_hour": 0,
            },
            profile_updates={
                "rotation_score_multiplier": 999.0,
                "replacement_min_score_improvement": 999.0,
                "max_rotations_per_timestamp": 0,
                "max_replacements_per_hour": 0,
            },
        ),
        scenario(
            "min_rotation_age_45",
            "rotation",
            profile_updates={"min_rotation_age_minutes": 45.0},
        ),
        scenario(
            "min_rotation_age_120",
            "rotation",
            profile_updates={"min_rotation_age_minutes": 120.0},
        ),
        scenario("source_m30_only", "model_member", filter_kind="source_stream", filter_value="m30"),
        scenario("source_h1_only", "model_member", filter_kind="source_stream", filter_value="h1"),
        scenario("source_h4_only", "model_member", filter_kind="source_stream", filter_value="h4"),
    ]

    for equity in [25.0, start_equity, 50.0, 100.0, 1000.0, 100000.0]:
        name = f"start_equity_{str(round(equity, 4)).replace('.', 'p')}"
        if any(item["scenario_name"] == name for item in scenarios):
            continue
        scenarios.append(
            scenario(
                name,
                "starting_equity",
                profile_updates={"start_equity": equity},
                notes="Same settings at a different account size.",
            )
        )
    return scenarios


def add_time_scenarios(scenarios: List[Dict[str, Any]], candidates: pd.DataFrame, folds: int) -> None:
    if folds <= 1 or candidates.empty:
        return
    times = pd.to_datetime(candidates["time_utc"], utc=True)
    start = times.min()
    end = times.max()
    if pd.isna(start) or pd.isna(end) or start >= end:
        return
    edges = pd.date_range(start=start, end=end, periods=folds + 1)
    for idx in range(folds):
        left = edges[idx]
        right = edges[idx + 1]
        scenarios.append(
            scenario(
                f"time_fold_{idx + 1:02d}",
                "time_split",
                filter_kind="time_window",
                filter_value=json.dumps({"start": left.isoformat(), "end": right.isoformat(), "right_inclusive": idx == folds - 1}),
                notes="Fresh-account replay on a chronological slice.",
            )
        )


def filter_candidates(candidates: pd.DataFrame, item: Dict[str, Any]) -> pd.DataFrame:
    kind = str(item.get("filter_kind") or "")
    value = str(item.get("filter_value") or "")
    if not kind:
        return candidates.copy()
    if kind == "source_stream":
        return candidates[candidates["source_stream"].astype(str) == value].copy()
    if kind == "time_window":
        payload = json.loads(value)
        times = pd.to_datetime(candidates["time_utc"], utc=True)
        start = pd.Timestamp(payload["start"])
        end = pd.Timestamp(payload["end"])
        mask = (times >= start) & (times <= end if payload.get("right_inclusive") else times < end)
        return candidates[mask].copy()
    raise ValueError(f"Unknown filter_kind: {kind}")


def run_one(
    *,
    item: Dict[str, Any],
    base_cfg: Dict[str, Any],
    base_profile: Dict[str, Any],
    candidates: pd.DataFrame,
    margin_rates: Dict[str, float],
    candles: Any,
    output_root: Path,
    write_details: bool,
) -> Dict[str, Any]:
    scenario_candidates = filter_candidates(candidates, item)
    cfg = dict(base_cfg)
    cfg["simulate_live_exit_checks"] = True
    cfg.update(item.get("cfg_updates") or {})
    profile = dict(base_profile)
    profile.update(item.get("profile_updates") or {})
    # Keep profile margin fields in sync when the config scenario mutates them.
    if "target_margin_used_pct" in cfg:
        profile["target_margin_pct"] = safe_float(cfg["target_margin_used_pct"], profile.get("target_margin_pct"))
    if "max_margin_used_pct" in cfg:
        profile["hard_margin_pct"] = safe_float(cfg["max_margin_used_pct"], profile.get("hard_margin_pct"))
    if "emergency_margin_used_pct" in cfg:
        profile["emergency_margin_pct"] = safe_float(cfg["emergency_margin_used_pct"], profile.get("emergency_margin_pct"))
    replay = BrokerReplay(
        profile,
        candles,
        margin_rates=margin_rates,
        live_cfg=cfg,
        max_new_positions_per_timestamp=safe_int(cfg.get("max_new_positions_per_cycle"), 2),
    )
    started = time.perf_counter()
    summary = replay.run(scenario_candidates)
    elapsed = time.perf_counter() - started
    row = {
        "scenario_name": item["scenario_name"],
        "scenario_group": item["scenario_group"],
        "filter_kind": item.get("filter_kind", ""),
        "filter_value": item.get("filter_value", ""),
        "notes": item.get("notes", ""),
        "objective": objective(summary),
        "elapsed_seconds": round(elapsed, 3),
        "start_equity": safe_float(profile.get("start_equity"), 0.0),
        "signal_gone_exit_minutes": safe_float(cfg.get("signal_gone_exit_minutes"), 0.0),
        "signal_gone_min_horizon_fraction": safe_float(cfg.get("signal_gone_min_horizon_fraction"), 0.0),
        "profit_lock_min_age_minutes": safe_float(cfg.get("profit_lock_min_age_minutes"), 0.0),
        "rotation_score_multiplier": safe_float(profile.get("rotation_score_multiplier"), 0.0),
        "replacement_min_score_multiplier": safe_float(cfg.get("replacement_min_score_multiplier"), 0.0),
        "replacement_min_score_improvement": safe_float(cfg.get("replacement_min_score_improvement"), 0.0),
        "max_replacements_per_hour": safe_int(cfg.get("max_replacements_per_hour"), 0),
        "min_rotation_age_minutes": safe_float(profile.get("min_rotation_age_minutes"), 0.0),
        "target_margin_pct": safe_float(profile.get("target_margin_pct"), 0.0),
        "hard_margin_pct": safe_float(profile.get("hard_margin_pct"), 0.0),
        "slippage_pips_default": safe_float(cfg.get("slippage_pips_default"), 0.0),
        "slippage_pips_exotic": safe_float(cfg.get("slippage_pips_exotic"), 0.0),
        "spread_cost_multiplier": safe_float(cfg.get("spread_cost_multiplier"), 0.0),
        **summary,
    }
    if write_details:
        run_dir = output_root / "runs" / item["scenario_name"]
        replay.write_outputs(run_dir, row, scenario_candidates)
    return row


def write_report(output_root: Path, summary: pd.DataFrame) -> None:
    if summary.empty:
        return
    best = summary.sort_values("objective", ascending=False).head(10)
    by_group = (
        summary.groupby("scenario_group", dropna=False)
        .agg(
            scenarios=("scenario_name", "count"),
            best_return_pct=("return_pct", "max"),
            median_return_pct=("return_pct", "median"),
            worst_return_pct=("return_pct", "min"),
            best_profit_factor=("profit_factor", "max"),
            worst_max_drawdown_pct=("max_drawdown_pct", "max"),
            max_margin_call_rows=("margin_call_rows", "max"),
        )
        .reset_index()
    )
    by_group.to_csv(output_root / "summary_by_group.csv", index=False)
    lines = [
        "# Primary Forecast Extensive Backtest",
        "",
        "## Top scenarios",
        "",
        best[
            [
                "scenario_name",
                "scenario_group",
                "return_pct",
                "profit_factor",
                "max_drawdown_pct",
                "max_margin_used_pct",
                "margin_call_rows",
                "opened_trades",
            ]
        ].to_markdown(index=False),
        "",
        "## Group robustness",
        "",
        by_group.to_markdown(index=False),
        "",
    ]
    (output_root / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--config", type=Path, default=PRIMARY_CONFIG)
    parser.add_argument("--candidate-rows-csv", type=Path, default=None)
    parser.add_argument("--profile-name", default="h_tf_growth")
    parser.add_argument("--start-equity", type=float, default=28.2155)
    parser.add_argument("--time-folds", type=int, default=4)
    parser.add_argument("--max-scenarios", type=int, default=0)
    parser.add_argument("--no-details", action="store_true")
    args = parser.parse_args()

    output_root = args.output_root or REPORTS / f"primary_forecast_extensive_backtest_{utc_stamp()}"
    output_root.mkdir(parents=True, exist_ok=True)
    live_cfg, candidates, candles = load_replay_inputs(args.config, args.candidate_rows_csv)
    base_profile = selected_profile(live_cfg, args.profile_name, args.start_equity)
    scenarios = build_scenarios(live_cfg, base_profile, args.start_equity)
    add_time_scenarios(scenarios, candidates, args.time_folds)
    if args.max_scenarios > 0:
        scenarios = scenarios[: args.max_scenarios]
    margin_rates = load_margin_rates()
    rows: List[Dict[str, Any]] = []
    metadata = {
        "config": str(args.config),
        "candidate_rows_csv": str(live_cfg.get("_sweep_candidate_rows_csv") or ""),
        "candidate_rows": int(len(candidates)),
        "candidate_min_time_utc": pd.to_datetime(candidates["time_utc"], utc=True).min().isoformat(),
        "candidate_max_time_utc": pd.to_datetime(candidates["time_utc"], utc=True).max().isoformat(),
        "profile_name": args.profile_name,
        "start_equity": args.start_equity,
        "scenario_count": len(scenarios),
    }
    (output_root / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    (output_root / "live_config_snapshot.json").write_text(
        json.dumps(live_cfg, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_root / "base_profile_snapshot.json").write_text(
        json.dumps(base_profile, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    for index, item in enumerate(scenarios, start=1):
        row = run_one(
            item=item,
            base_cfg=live_cfg,
            base_profile=base_profile,
            candidates=candidates,
            margin_rates=margin_rates,
            candles=candles,
            output_root=output_root,
            write_details=not args.no_details,
        )
        row["run_index"] = index
        rows.append(row)
        print(json.dumps(row, sort_keys=True, default=str), flush=True)
    summary = pd.DataFrame(rows).sort_values(["objective", "return_pct"], ascending=[False, False])
    summary.to_csv(output_root / "scenario_summary.csv", index=False)
    (output_root / "scenario_summary.json").write_text(
        json.dumps(summary.to_dict(orient="records"), indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    write_report(output_root, summary)
    print(json.dumps({"output_root": str(output_root), "scenarios": len(summary)}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
