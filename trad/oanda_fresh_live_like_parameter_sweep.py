#!/usr/bin/env python3
"""Intensive live-like parameter sweep for rebuilt OANDA ensemble candidates.

This keeps the same BrokerReplay engine used by the primary live exact backtest,
but sweeps the execution environment around it:

- stream composition: all, single timeframe, and pairwise timeframe ensembles;
- signal-gone, stale, replacement, profit-lock, and exit-fade controls;
- spread/slippage stress;
- stop/take-profit/trailing geometry;
- margin/risk posture and max-new-per-cycle pacing.

The default "intensive" preset is staged and bounded. Use --preset exit_full to
run the full legacy exit/rotation grid, --preset all to run exit_full plus the
staged grid, or --preset cartesian only for a very large overnight product.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

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
from oanda_primary_live_exit_rotation_sweep import build_variants as build_exit_grid


DEFAULT_OUTPUT = REPORTS / "fresh_live_like_parameter_sweep"


def path_quality_objective(summary: Dict[str, Any]) -> float:
    trades = safe_float(summary.get("opened_trades"), 0.0)
    if trades < 100 or safe_float(summary.get("margin_call_rows"), 0.0) > 0:
        return -1e9
    path_return = safe_float(summary.get("path_adjusted_return_pct"), 0.0)
    multiple = 1.0 + path_return / 100.0
    if multiple > 0:
        log_return_score = math.log(multiple) * 100.0
    else:
        log_return_score = -1000.0 + multiple * 100.0
    path_pf = min(safe_float(summary.get("path_adjusted_profit_factor"), 0.0), 10.0)
    clean_win = safe_float(summary.get("clean_win_rate"), 0.0)
    deep_share = safe_float(summary.get("deep_adverse_trade_share"), 0.0)
    rescued_share = safe_float(summary.get("rescued_winner_share"), 0.0)
    max_dd = safe_float(summary.get("max_drawdown_pct"), 0.0)
    p95_mae_r = safe_float(summary.get("p95_mae_r"), 0.0)
    return (
        log_return_score
        + path_pf * 20.0
        + clean_win * 100.0
        - max_dd * 8.0
        - deep_share * 250.0
        - rescued_share * 350.0
        - p95_mae_r * 25.0
    )


def list_variants(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(item) for item in items]


def stream_variants() -> List[Dict[str, Any]]:
    return list_variants(
        [
            {"stream_name": "all", "source_streams": []},
            {"stream_name": "m30_only", "source_streams": ["m30"]},
            {"stream_name": "h1_only", "source_streams": ["h1"]},
            {"stream_name": "h4_only", "source_streams": ["h4"]},
            {"stream_name": "m30_h1", "source_streams": ["m30", "h1"]},
            {"stream_name": "m30_h4", "source_streams": ["m30", "h4"]},
            {"stream_name": "h1_h4", "source_streams": ["h1", "h4"]},
        ]
    )


def candidate_gate_variants() -> List[Dict[str, Any]]:
    return list_variants(
        [
            {"candidate_gate_name": "gate_live", "min_score_percentile": 0.0, "min_probability": 0.0, "min_edge_pips": None},
            {"candidate_gate_name": "score_p50", "min_score_percentile": 0.50, "min_probability": 0.0, "min_edge_pips": None},
            {"candidate_gate_name": "score_p75", "min_score_percentile": 0.75, "min_probability": 0.0, "min_edge_pips": None},
            {"candidate_gate_name": "prob_060", "min_score_percentile": 0.0, "min_probability": 0.60, "min_edge_pips": None},
            {"candidate_gate_name": "prob_065", "min_score_percentile": 0.0, "min_probability": 0.65, "min_edge_pips": None},
            {"candidate_gate_name": "edge_1p0", "min_score_percentile": 0.0, "min_probability": 0.0, "min_edge_pips": 1.0},
            {"candidate_gate_name": "edge_2p0", "min_score_percentile": 0.0, "min_probability": 0.0, "min_edge_pips": 2.0},
        ]
    )


def friction_variants() -> List[Dict[str, Any]]:
    return list_variants(
        [
            {"friction_name": "friction_live", "spread_multiplier": 1.00, "slippage_multiplier": 1.00},
            {"friction_name": "spread_125", "spread_multiplier": 1.25, "slippage_multiplier": 1.00},
            {"friction_name": "spread_150", "spread_multiplier": 1.50, "slippage_multiplier": 1.00},
            {"friction_name": "slip_2x", "spread_multiplier": 1.00, "slippage_multiplier": 2.00},
            {"friction_name": "slip_4x", "spread_multiplier": 1.00, "slippage_multiplier": 4.00},
            {"friction_name": "spread125_slip2", "spread_multiplier": 1.25, "slippage_multiplier": 2.00},
        ]
    )


def stop_variants() -> List[Dict[str, Any]]:
    return list_variants(
        [
            {
                "stop_name": "stops_live",
                "atr_stop_multiplier": None,
                "take_profit_edge_capture": None,
                "take_profit_min_r_multiple": None,
                "trailing_stop_r_multiple": None,
            },
            {
                "stop_name": "stops_wide",
                "atr_stop_multiplier": 1.50,
                "take_profit_edge_capture": 0.60,
                "take_profit_min_r_multiple": 0.50,
                "trailing_stop_r_multiple": 1.00,
            },
            {
                "stop_name": "stops_very_wide",
                "atr_stop_multiplier": 1.80,
                "take_profit_edge_capture": 0.70,
                "take_profit_min_r_multiple": 0.60,
                "trailing_stop_r_multiple": 1.20,
            },
            {
                "stop_name": "stops_tight_fast",
                "atr_stop_multiplier": 1.00,
                "take_profit_edge_capture": 0.45,
                "take_profit_min_r_multiple": 0.35,
                "trailing_stop_r_multiple": 0.65,
            },
        ]
    )


def risk_variants() -> List[Dict[str, Any]]:
    return list_variants(
        [
            {
                "risk_name": "risk_live",
                "target_margin_pct": None,
                "hard_margin_pct": None,
                "target_open_risk_pct": None,
                "max_open_risk_pct": None,
                "max_risk_pct": None,
                "min_risk_pct": None,
            },
            {
                "risk_name": "risk_safer_margin",
                "target_margin_pct": 55.0,
                "hard_margin_pct": 70.0,
                "target_open_risk_pct": 6.0,
                "max_open_risk_pct": 8.0,
                "max_risk_pct": None,
                "min_risk_pct": None,
            },
            {
                "risk_name": "risk_conservative",
                "target_margin_pct": 45.0,
                "hard_margin_pct": 62.0,
                "target_open_risk_pct": 4.5,
                "max_open_risk_pct": 6.5,
                "max_risk_pct": 0.55,
                "min_risk_pct": 0.12,
            },
        ]
    )


def pacing_variants() -> List[Dict[str, Any]]:
    return list_variants(
        [
            {"pacing_name": "new_1", "max_new_positions_per_timestamp": 1},
            {"pacing_name": "new_2_live", "max_new_positions_per_timestamp": 2},
            {"pacing_name": "new_3", "max_new_positions_per_timestamp": 3},
            {"pacing_name": "new_4", "max_new_positions_per_timestamp": 4},
            {"pacing_name": "new_5", "max_new_positions_per_timestamp": 5},
            {"pacing_name": "new_6", "max_new_positions_per_timestamp": 6},
            {"pacing_name": "new_7", "max_new_positions_per_timestamp": 7},
            {"pacing_name": "new_8", "max_new_positions_per_timestamp": 8},
            {"pacing_name": "new_9", "max_new_positions_per_timestamp": 9},
            {"pacing_name": "new_10", "max_new_positions_per_timestamp": 10},
            {"pacing_name": "new_11", "max_new_positions_per_timestamp": 11},
            {"pacing_name": "new_12", "max_new_positions_per_timestamp": 12},
            {"pacing_name": "new_13", "max_new_positions_per_timestamp": 13},
            {"pacing_name": "new_14", "max_new_positions_per_timestamp": 14},
            {"pacing_name": "new_15", "max_new_positions_per_timestamp": 15},
            {"pacing_name": "new_16", "max_new_positions_per_timestamp": 16},
        ]
    )


def compact_exit_variants() -> List[Dict[str, Any]]:
    rows = [
        {
            "exit_name": "exit_exact_config",
        },
        {
            "exit_name": "exit_exact_no_signal_gone",
            "signal_gone_exit_minutes": 100000.0,
            "signal_gone_min_horizon_fraction": 1000.0,
        },
        {
            "exit_name": "exit_exact_slow_signal",
            "signal_gone_exit_minutes": 240.0,
            "signal_gone_min_horizon_fraction": 1.0,
        },
        {
            "exit_name": "exit_exact_instant_replace",
            "min_rotation_age_minutes": 0.0,
            "rotation_score_multiplier": 1.05,
            "replacement_min_score_multiplier": 1.05,
            "replacement_min_score_improvement": 0.0,
            "max_rotations_per_timestamp": 8,
            "max_replacements_per_hour": 60,
        },
        {
            "exit_name": "exit_live",
            "signal_gone_exit_minutes": 120.0,
            "signal_gone_min_horizon_fraction": 1.0,
            "stale_minutes": 120.0,
            "min_rotation_age_minutes": 60.0,
            "rotation_score_multiplier": 1.15,
            "replacement_min_score_multiplier": 1.15,
            "replacement_min_score_improvement": 0.003,
            "max_rotations_per_timestamp": 2,
            "max_replacements_per_hour": 4,
            "profit_lock_min_age_minutes": 1.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.2,
            "exit_fade_confirm_cycles": 1,
            "signal_gone_loser_policy": "allow",
            "signal_gone_loser_buffer_pips": 0.0,
        },
        {
            "exit_name": "exit_no_signal_gone",
            "signal_gone_exit_minutes": 100000.0,
            "signal_gone_min_horizon_fraction": 1000.0,
            "stale_minutes": 180.0,
            "min_rotation_age_minutes": 75.0,
            "rotation_score_multiplier": 1.50,
            "replacement_min_score_multiplier": 1.50,
            "replacement_min_score_improvement": 0.015,
            "max_rotations_per_timestamp": 1,
            "max_replacements_per_hour": 2,
            "profit_lock_min_age_minutes": 30.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.35,
            "exit_fade_confirm_cycles": 2,
            "signal_gone_loser_policy": "defer_losers_unless_opposite",
            "signal_gone_loser_buffer_pips": 1.0,
        },
        {
            "exit_name": "exit_slow_signal",
            "signal_gone_exit_minutes": 240.0,
            "signal_gone_min_horizon_fraction": 1.0,
            "stale_minutes": 180.0,
            "min_rotation_age_minutes": 75.0,
            "rotation_score_multiplier": 1.50,
            "replacement_min_score_multiplier": 1.50,
            "replacement_min_score_improvement": 0.015,
            "max_rotations_per_timestamp": 1,
            "max_replacements_per_hour": 2,
            "profit_lock_min_age_minutes": 30.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.35,
            "exit_fade_confirm_cycles": 2,
            "signal_gone_loser_policy": "defer_losers_unless_opposite",
            "signal_gone_loser_buffer_pips": 1.0,
        },
        {
            "exit_name": "exit_strict_replace",
            "signal_gone_exit_minutes": 180.0,
            "signal_gone_min_horizon_fraction": 1.0,
            "stale_minutes": 180.0,
            "min_rotation_age_minutes": 75.0,
            "rotation_score_multiplier": 1.75,
            "replacement_min_score_multiplier": 1.75,
            "replacement_min_score_improvement": 0.025,
            "max_rotations_per_timestamp": 1,
            "max_replacements_per_hour": 1,
            "profit_lock_min_age_minutes": 45.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.50,
            "exit_fade_confirm_cycles": 2,
            "signal_gone_loser_policy": "defer_losers_unless_opposite",
            "signal_gone_loser_buffer_pips": 1.0,
        },
        {
            "exit_name": "exit_replacement_off",
            "signal_gone_exit_minutes": 180.0,
            "signal_gone_min_horizon_fraction": 1.0,
            "stale_minutes": 100000.0,
            "min_rotation_age_minutes": 60.0,
            "rotation_score_multiplier": 999.0,
            "replacement_min_score_multiplier": 999.0,
            "replacement_min_score_improvement": 999.0,
            "max_rotations_per_timestamp": 0,
            "max_replacements_per_hour": 0,
            "profit_lock_min_age_minutes": 30.0,
            "profit_lock_min_pips": 0.5,
            "profit_lock_r_multiple": 0.35,
            "exit_fade_confirm_cycles": 2,
            "signal_gone_loser_policy": "defer_losers_unless_opposite",
            "signal_gone_loser_buffer_pips": 1.0,
        },
    ]
    return list_variants(rows)


def name_from(parts: Sequence[str]) -> str:
    clean = [str(part).replace(" ", "_").replace("/", "_") for part in parts if str(part)]
    return "__".join(clean)


def merge_variant(
    *,
    stream: Dict[str, Any] | None = None,
    gate: Dict[str, Any] | None = None,
    friction: Dict[str, Any] | None = None,
    stop: Dict[str, Any] | None = None,
    risk: Dict[str, Any] | None = None,
    pacing: Dict[str, Any] | None = None,
    exit_cfg: Dict[str, Any] | None = None,
    family: str = "intensive",
) -> Dict[str, Any]:
    variant = {
        **stream_variants()[0],
        **candidate_gate_variants()[0],
        **friction_variants()[0],
        **stop_variants()[0],
        **risk_variants()[0],
        **{"pacing_name": "new_2_live", "max_new_positions_per_timestamp": 2},
        **compact_exit_variants()[0],
    }
    for patch in [stream, gate, friction, stop, risk, pacing, exit_cfg]:
        if patch:
            variant.update(patch)
    variant["variant_family"] = family
    variant["variant_name"] = name_from(
        [
            variant["stream_name"],
            variant["candidate_gate_name"],
            variant["friction_name"],
            variant["stop_name"],
            variant["risk_name"],
            variant["pacing_name"],
            variant["exit_name"],
        ]
    )
    return variant


def build_cartesian_variants() -> List[Dict[str, Any]]:
    variants: List[Dict[str, Any]] = []
    for stream in stream_variants():
        for gate in candidate_gate_variants():
            for friction in friction_variants():
                for stop in stop_variants():
                    for risk in risk_variants():
                        for pacing in pacing_variants():
                            for exit_cfg in compact_exit_variants():
                                variant = {
                                    **stream,
                                    **gate,
                                    **friction,
                                    **stop,
                                    **risk,
                                    **pacing,
                                    **exit_cfg,
                                }
                                variant["variant_family"] = "cartesian"
                                variant["variant_name"] = name_from(
                                    [
                                        variant["stream_name"],
                                        variant["candidate_gate_name"],
                                        variant["friction_name"],
                                        variant["stop_name"],
                                        variant["risk_name"],
                                        variant["pacing_name"],
                                        variant["exit_name"],
                                    ]
                                )
                                variants.append(variant)
    return variants


def build_intensive_variants() -> List[Dict[str, Any]]:
    """Build a broad staged grid that is runnable on exact replay.

    The full Cartesian product is useful for overnight jobs, but exact OANDA
    replay on a multi-month candidate file can take more than a minute per
    variant. This staged grid covers each control axis plus selected two-axis
    stress combinations that are most relevant to live/backtest mismatch.
    """

    variants_by_name: Dict[str, Dict[str, Any]] = {}

    def add(variant: Dict[str, Any]) -> None:
        variants_by_name.setdefault(variant["variant_name"], variant)

    exits = compact_exit_variants()
    for exit_cfg in exits:
        add(merge_variant(exit_cfg=exit_cfg))

    for axis_values, key in [
        (stream_variants(), "stream"),
        (candidate_gate_variants(), "gate"),
        (friction_variants(), "friction"),
        (stop_variants(), "stop"),
        (risk_variants(), "risk"),
        (pacing_variants(), "pacing"),
    ]:
        for value in axis_values:
            for exit_cfg in exits:
                add(merge_variant(**{key: value}, exit_cfg=exit_cfg))

    for stream in stream_variants():
        for friction in friction_variants():
            add(merge_variant(stream=stream, friction=friction, exit_cfg=exits[0]))
            add(merge_variant(stream=stream, friction=friction, exit_cfg=exits[2]))

    for gate in candidate_gate_variants():
        for friction in friction_variants():
            add(merge_variant(gate=gate, friction=friction, exit_cfg=exits[0]))
            add(merge_variant(gate=gate, friction=friction, exit_cfg=exits[2]))

    for stop in stop_variants():
        for risk in risk_variants():
            add(merge_variant(stop=stop, risk=risk, exit_cfg=exits[0]))
            add(merge_variant(stop=stop, risk=risk, exit_cfg=exits[2]))

    return list(variants_by_name.values())


def build_exit_full_variants() -> List[Dict[str, Any]]:
    variants: List[Dict[str, Any]] = []
    for legacy in build_exit_grid("full"):
        variant = {
            **stream_variants()[0],
            **candidate_gate_variants()[0],
            **friction_variants()[0],
            **stop_variants()[0],
            **risk_variants()[0],
            **{"pacing_name": "new_2_live", "max_new_positions_per_timestamp": 2},
            **legacy,
            "exit_name": str(legacy.get("variant_name", "legacy_exit")),
            "variant_family": "exit_full",
        }
        variant["variant_name"] = name_from(["exit_full", variant["exit_name"]])
        variants.append(variant)
    return variants


def build_variants(preset: str) -> List[Dict[str, Any]]:
    if preset == "intensive":
        return build_intensive_variants()
    if preset == "cartesian":
        return build_cartesian_variants()
    if preset == "exit_full":
        return build_exit_full_variants()
    if preset == "all":
        return build_exit_full_variants() + build_intensive_variants()
    raise ValueError(f"Unknown preset: {preset}")


def load_replay_inputs(config_path: Path, candidate_rows_csv: Path) -> tuple[Dict[str, Any], pd.DataFrame, CandleStore]:
    live_cfg = read_json(config_path, {})
    if not candidate_rows_csv.exists():
        raise FileNotFoundError(f"Missing candidate rows CSV: {candidate_rows_csv}")
    candidates = load_candidates(candidate_rows_csv, safe_float(live_cfg.get("min_rank_score"), 0.0))
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
    return live_cfg, candidates, candles


def apply_candidate_variant(candidates: pd.DataFrame, variant: Dict[str, Any]) -> pd.DataFrame:
    frame = candidates
    streams = [str(item) for item in variant.get("source_streams") or []]
    if streams and "source_stream" in frame.columns:
        frame = frame[frame["source_stream"].astype(str).isin(streams)]
    min_percentile = safe_float(variant.get("min_score_percentile"), 0.0)
    if min_percentile > 0 and "score_percentile" in frame.columns:
        frame = frame[pd.to_numeric(frame["score_percentile"], errors="coerce").fillna(0.0) >= min_percentile]
    min_probability = safe_float(variant.get("min_probability"), 0.0)
    if min_probability > 0 and "probability" in frame.columns:
        frame = frame[pd.to_numeric(frame["probability"], errors="coerce").fillna(0.0) >= min_probability]
    spread_multiplier = safe_float(variant.get("spread_multiplier"), 1.0)
    if spread_multiplier != 1.0 and "spread_pips" in frame.columns:
        frame = frame.copy()
        frame["spread_pips"] = pd.to_numeric(frame["spread_pips"], errors="coerce").fillna(0.0) * spread_multiplier
    return frame.sort_values(["time_utc", "rank_score"], ascending=[True, False]).reset_index(drop=True)


def apply_cfg_variant(base_cfg: Dict[str, Any], variant: Dict[str, Any]) -> Dict[str, Any]:
    live_cfg = copy.deepcopy(base_cfg)
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
        if key in variant:
            live_cfg[key] = variant[key]
    live_cfg["replacement_min_age_minutes"] = variant.get("min_rotation_age_minutes", live_cfg.get("replacement_min_age_minutes"))
    for key in ["atr_stop_multiplier", "take_profit_edge_capture", "take_profit_min_r_multiple", "trailing_stop_r_multiple"]:
        value = variant.get(key)
        if value is not None:
            live_cfg[key] = value
    min_edge = variant.get("min_edge_pips")
    if min_edge is not None:
        live_cfg["min_edge_pips"] = min_edge
    slippage_multiplier = safe_float(variant.get("slippage_multiplier"), 1.0)
    if slippage_multiplier != 1.0:
        for key in ["slippage_pips_default", "slippage_pips_exotic"]:
            live_cfg[key] = safe_float(live_cfg.get(key), 0.0) * slippage_multiplier
    return live_cfg


def apply_profile_variant(base_profile: Dict[str, Any], variant: Dict[str, Any]) -> Dict[str, Any]:
    profile = dict(base_profile)
    for key in [
        "stale_minutes",
        "min_rotation_age_minutes",
        "rotation_score_multiplier",
        "max_rotations_per_timestamp",
        "replacement_min_score_improvement",
        "max_replacements_per_hour",
    ]:
        if key in variant:
            profile[key] = variant[key]
    for key in [
        "target_margin_pct",
        "hard_margin_pct",
        "target_open_risk_pct",
        "max_open_risk_pct",
        "max_risk_pct",
        "min_risk_pct",
    ]:
        value = variant.get(key)
        if value is not None:
            profile[key] = value
    return profile


def run_variant(
    *,
    base_cfg: Dict[str, Any],
    base_profile: Dict[str, Any],
    candidates: pd.DataFrame,
    candles: CandleStore,
    margin_rates: Dict[str, float],
    variant: Dict[str, Any],
    write_dir: Path | None = None,
) -> Dict[str, Any]:
    run_candidates = apply_candidate_variant(candidates, variant)
    live_cfg = apply_cfg_variant(base_cfg, variant)
    profile = apply_profile_variant(base_profile, variant)
    replay = BrokerReplay(
        profile,
        candles,
        margin_rates=margin_rates,
        live_cfg=live_cfg,
        max_new_positions_per_timestamp=safe_int(variant.get("max_new_positions_per_timestamp"), 2),
    )
    start = time.perf_counter()
    summary = replay.run(run_candidates)
    elapsed = time.perf_counter() - start
    row = {
        "variant_name": variant["variant_name"],
        "variant_family": variant.get("variant_family", ""),
        "objective": objective(summary),
        "path_quality_objective": path_quality_objective(summary),
        "elapsed_seconds": round(elapsed, 3),
        "filtered_candidate_rows": int(len(run_candidates)),
        **{key: value for key, value in variant.items() if key != "variant_name"},
        **summary,
    }
    if write_dir is not None:
        replay.write_outputs(write_dir, row, run_candidates)
    return row


def maybe_filter_variants(
    variants: List[Dict[str, Any]],
    *,
    only_family: str,
    only_stream: str,
    only_gate: str,
    only_friction: str,
    only_stop: str,
    only_risk: str,
    only_pacing: str,
    only_exit: str,
) -> List[Dict[str, Any]]:
    filters = {
        "variant_family": only_family,
        "stream_name": only_stream,
        "candidate_gate_name": only_gate,
        "friction_name": only_friction,
        "stop_name": only_stop,
        "risk_name": only_risk,
        "pacing_name": only_pacing,
        "exit_name": only_exit,
    }
    out = variants
    for key, allowed_text in filters.items():
        allowed = {item.strip() for item in str(allowed_text or "").split(",") if item.strip()}
        if allowed:
            out = [variant for variant in out if str(variant.get(key)) in allowed]
    return out


def run(
    output_root: Path,
    *,
    preset: str,
    config_path: Path,
    candidate_rows_csv: Path,
    profile_name: str,
    start_equity: float,
    max_runs: int,
    write_top_n: int,
    only_family: str,
    only_stream: str,
    only_gate: str,
    only_friction: str,
    only_stop: str,
    only_risk: str,
    only_pacing: str,
    only_exit: str,
    path_strict: bool,
    deep_adverse_r_threshold: float,
    rank_by: str,
) -> pd.DataFrame:
    output_root.mkdir(parents=True, exist_ok=True)
    live_cfg, candidates, candles = load_replay_inputs(config_path, candidate_rows_csv)
    live_cfg["conservative_intrabar_order"] = bool(path_strict)
    live_cfg["deep_adverse_r_threshold"] = float(deep_adverse_r_threshold)
    base_profile = selected_profile(live_cfg, profile_name, start_equity)
    margin_rates = load_margin_rates()
    variants = build_variants(preset)
    variants = maybe_filter_variants(
        variants,
        only_family=only_family,
        only_stream=only_stream,
        only_gate=only_gate,
        only_friction=only_friction,
        only_stop=only_stop,
        only_risk=only_risk,
        only_pacing=only_pacing,
        only_exit=only_exit,
    )
    if max_runs > 0:
        variants = variants[:max_runs]
    metadata = {
        "preset": preset,
        "config_path": str(config_path),
        "candidate_rows_csv": str(candidate_rows_csv),
        "profile_name": profile_name,
        "start_equity": start_equity,
        "variant_count": len(variants),
        "input_candidate_rows": int(len(candidates)),
        "input_time_start": pd.to_datetime(candidates["time_utc"], utc=True).min().isoformat() if not candidates.empty else "",
        "input_time_end": pd.to_datetime(candidates["time_utc"], utc=True).max().isoformat() if not candidates.empty else "",
        "path_strict": bool(path_strict),
        "deep_adverse_r_threshold": float(deep_adverse_r_threshold),
        "rank_by": str(rank_by),
    }
    (output_root / "sweep_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    rows: List[Dict[str, Any]] = []
    for idx, variant in enumerate(variants, start=1):
        row = run_variant(
            base_cfg=live_cfg,
            base_profile=base_profile,
            candidates=candidates,
            candles=candles,
            margin_rates=margin_rates,
            variant=variant,
        )
        row["run_index"] = idx
        rows.append(row)
        print(json.dumps(row, sort_keys=True, default=str), flush=True)
    frame = pd.DataFrame(rows)
    if not frame.empty:
        primary_rank = "path_quality_objective" if rank_by == "path_quality" else "objective"
        frame = frame.sort_values(
            [primary_rank, "path_adjusted_profit_factor", "clean_win_rate", "return_pct"],
            ascending=[False, False, False, False],
        )
    frame.to_csv(output_root / "sweep_summary.csv", index=False)
    (output_root / "sweep_summary.json").write_text(
        json.dumps(frame.to_dict(orient="records"), indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    if write_top_n > 0 and not frame.empty:
        top_root = output_root / "top_runs"
        for rank, row in enumerate(frame.head(write_top_n).to_dict(orient="records"), start=1):
            variant = variants[safe_int(row["run_index"], 1) - 1]
            digest = hashlib.sha1(str(variant["variant_name"]).encode("utf-8")).hexdigest()[:10]
            short_name = str(variant["variant_name"])[:72].rstrip("_")
            run_dir = top_root / f"{rank:03d}_{short_name}_{digest}"
            detailed = run_variant(
                base_cfg=live_cfg,
                base_profile=base_profile,
                candidates=candidates,
                candles=candles,
                margin_rates=margin_rates,
                variant=variant,
                write_dir=run_dir,
            )
            detailed["rank"] = rank
            detailed["run_dir"] = str(run_dir)
            (run_dir / "variant.json").write_text(json.dumps(variant, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps({"top_run_written": detailed}, sort_keys=True, default=str), flush=True)
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--preset", choices=["intensive", "cartesian", "exit_full", "all"], default="intensive")
    parser.add_argument("--config", type=Path, default=PRIMARY_CONFIG)
    parser.add_argument("--candidate-rows-csv", type=Path, required=True)
    parser.add_argument("--profile-name", default="h_tf_growth")
    parser.add_argument("--start-equity", type=float, default=1000.0)
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("--write-top-n", type=int, default=10)
    parser.add_argument("--only-family", default="")
    parser.add_argument("--only-stream", default="")
    parser.add_argument("--only-gate", default="")
    parser.add_argument("--only-friction", default="")
    parser.add_argument("--only-stop", default="")
    parser.add_argument("--only-risk", default="")
    parser.add_argument("--only-pacing", default="")
    parser.add_argument("--only-exit", default="")
    parser.add_argument("--path-strict", action="store_true")
    parser.add_argument("--deep-adverse-r-threshold", type=float, default=0.5)
    parser.add_argument("--rank-by", choices=["objective", "path_quality"], default="objective")
    args = parser.parse_args()
    frame = run(
        args.output_root,
        preset=args.preset,
        config_path=args.config,
        candidate_rows_csv=args.candidate_rows_csv,
        profile_name=args.profile_name,
        start_equity=args.start_equity,
        max_runs=args.max_runs,
        write_top_n=args.write_top_n,
        only_family=args.only_family,
        only_stream=args.only_stream,
        only_gate=args.only_gate,
        only_friction=args.only_friction,
        only_stop=args.only_stop,
        only_risk=args.only_risk,
        only_pacing=args.only_pacing,
        only_exit=args.only_exit,
        path_strict=args.path_strict,
        deep_adverse_r_threshold=args.deep_adverse_r_threshold,
        rank_by=args.rank_by,
    )
    if frame.empty:
        print("No variants ran.", flush=True)
    else:
        print(frame.head(30).to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
