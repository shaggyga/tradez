#!/usr/bin/env python3
"""Research-only tests for genuinely distinct FX strategy archetypes.

The harness deliberately does not expose an execution adapter.  Every cutoff is
estimated from data before 2025-01-01 and the later six quarters are reported
separately.  Outcomes use the stored bid/ask entry and exit series produced by
the all-68 feature build.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from oanda_all68_pattern_research import (
    BLOCKS,
    PHASES,
    atomic_csv,
    atomic_json,
    atomic_text,
    bh_adjust,
    block_definitions,
    independent_transition_indices,
    mean_ci,
    metadata_row,
    pair_group,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_FEATURE_DIR = ROOT / "data" / "all68_weekly_move_study" / "features"
DEFAULT_OUTPUT_DIR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "distinct_strategy_backtest_20260802"
)
DEFAULT_BASELINE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "all68_pattern_research_20260802"
    / "candidate_summary.csv"
)
DEFAULT_NEWS = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "better_models_20260802"
    / "news_event_reaction_audit.json"
)

HORIZONS = (15, 30, 60, 120, 240)
GRID_MINUTES = 15
FEATURE_COLUMNS = (
    "high",
    "low",
    "close",
    "spread_pips",
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_60_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "atr15_to_atr240",
    "compression_30",
    "range_position_60_centered",
    "spread_ratio_60",
    "volume_z_30",
    "atr240_pips",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
)


@dataclass(frozen=True)
class StrategySpec:
    name: str
    description: str
    horizons: tuple[int, ...]


STRATEGIES = (
    StrategySpec(
        "confirmed_pullback_resume",
        "60m trend, opposing 15m pullback, then 5m resumption with trend-stack confirmation",
        (60, 120, 240),
    ),
    StrategySpec(
        "compression_confirmed_breakout",
        "low compression followed by aligned 5m/15m range-edge confirmation",
        (30, 60, 120, 240),
    ),
    StrategySpec(
        "expansion_strength_direction",
        "volatility expansion times the move; cross-currency strength supplies direction",
        (15, 30, 60, 120),
    ),
    StrategySpec(
        "liquidity_sweep_reclaim",
        "trade against a 60m high/low sweep only after the close reclaims the prior boundary",
        (15, 30, 60, 120),
    ),
    StrategySpec(
        "london_asian_range_breakout",
        "first London-morning close beyond the completed local Asian-session range",
        (60, 120, 240),
    ),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sign(values: np.ndarray) -> np.ndarray:
    output = np.sign(values)
    output[~np.isfinite(values)] = 0
    return output.astype(np.int8)


def threshold(thresholds: Mapping[str, float], name: str, fallback: float) -> float:
    value = float(thresholds.get(name, fallback))
    return value if math.isfinite(value) else fallback


def derive_distinct_thresholds(discovery: pd.DataFrame) -> dict[str, float]:
    """Fit only the predeclared cutoffs used by this harness."""
    output: dict[str, float] = {}
    for column in ("momentum_5_atr", "momentum_15_atr", "momentum_60_atr", "strength_gap_rank_15"):
        values = pd.to_numeric(discovery[column], errors="coerce").abs().dropna()
        output[f"abs_{column}_q70"] = float(values.quantile(0.70))
    range_values = pd.to_numeric(
        discovery["range_position_60_centered"], errors="coerce"
    ).abs().dropna()
    output["abs_range_position_60_centered_q80"] = float(range_values.quantile(0.80))
    output["atr15_to_atr240_q90"] = float(
        pd.to_numeric(discovery["atr15_to_atr240"], errors="coerce").dropna().quantile(0.90)
    )
    output["compression_30_q20"] = float(
        pd.to_numeric(discovery["compression_30"], errors="coerce").dropna().quantile(0.20)
    )
    return output


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def rolling_sweep_direction(frame: pd.DataFrame, instrument: str) -> np.ndarray:
    """Return a causal sweep/reclaim direction on the native five-minute rows."""
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    atr = pd.to_numeric(frame["atr240_pips"], errors="coerce").clip(lower=1e-9)
    spread = pd.to_numeric(frame["spread_pips"], errors="coerce").clip(lower=0.0)
    volume = pd.to_numeric(frame["volume_z_30"], errors="coerce")
    spread_ratio = pd.to_numeric(frame["spread_ratio_60"], errors="coerce")
    prior_high = high.rolling(12, min_periods=12).max().shift(1)
    prior_low = low.rolling(12, min_periods=12).min().shift(1)
    scale = pip_size(instrument)
    upper_sweep = (high - prior_high) / scale
    upper_reclaim = (prior_high - close) / scale
    lower_sweep = (prior_low - low) / scale
    lower_reclaim = (close - prior_low) / scale
    minimum_excursion = np.maximum(spread.to_numpy(dtype=float), 0.05 * atr.to_numpy(dtype=float))
    minimum_reclaim = 0.02 * atr.to_numpy(dtype=float)
    quality = (volume.to_numpy(dtype=float) >= 0.0) & (spread_ratio.to_numpy(dtype=float) <= 1.25)
    upper = (
        (upper_sweep.to_numpy(dtype=float) >= minimum_excursion)
        & (upper_reclaim.to_numpy(dtype=float) >= minimum_reclaim)
        & quality
    )
    lower = (
        (lower_sweep.to_numpy(dtype=float) >= minimum_excursion)
        & (lower_reclaim.to_numpy(dtype=float) >= minimum_reclaim)
        & quality
    )
    output = np.zeros(len(frame), dtype=np.int8)
    output[upper & ~lower] = -1
    output[lower & ~upper] = 1
    both = upper & lower
    if both.any():
        upper_quality = upper_sweep.to_numpy(dtype=float) + upper_reclaim.to_numpy(dtype=float)
        lower_quality = lower_sweep.to_numpy(dtype=float) + lower_reclaim.to_numpy(dtype=float)
        output[both] = np.where(upper_quality[both] >= lower_quality[both], -1, 1)
    return output


def london_breakout_direction(frame: pd.DataFrame, instrument: str) -> np.ndarray:
    """Break the completed 00:00-08:00 Europe/London range after 08:00 local."""
    del instrument
    local = frame.index.tz_convert("Europe/London")
    local_date = pd.Series(local.date, index=frame.index)
    before_open = np.asarray(local.hour < 8)
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    session_high = high.where(before_open).groupby(local_date).transform("max")
    session_low = low.where(before_open).groupby(local_date).transform("min")
    entry_window = np.asarray((local.hour >= 8) & (local.hour < 11))
    low_cost = pd.to_numeric(frame["spread_ratio_60"], errors="coerce").to_numpy(dtype=float) <= 1.0
    upper = entry_window & low_cost & (close.to_numpy(dtype=float) > session_high.to_numpy(dtype=float))
    lower = entry_window & low_cost & (close.to_numpy(dtype=float) < session_low.to_numpy(dtype=float))
    output = np.zeros(len(frame), dtype=np.int8)
    output[upper & ~lower] = 1
    output[lower & ~upper] = -1
    return output


def strategy_direction(
    frame: pd.DataFrame,
    strategy: str,
    thresholds: Mapping[str, float],
    instrument: str,
) -> np.ndarray:
    arrays = {
        column: pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
        for column in FEATURE_COLUMNS
        if column in frame.columns
    }
    m5 = arrays["momentum_5_atr"]
    m15 = arrays["momentum_15_atr"]
    m60 = arrays["momentum_60_atr"]
    slow = sign(m60)
    fast = sign(m5)
    middle = sign(m15)
    strength = sign(arrays["strength_gap_rank_15"])
    output = np.zeros(len(frame), dtype=np.int8)

    if strategy == "confirmed_pullback_resume":
        mask = (
            (np.abs(m60) >= threshold(thresholds, "abs_momentum_60_atr_q70", 0.0))
            & (np.abs(m15) >= threshold(thresholds, "abs_momentum_15_atr_q70", 0.0))
            & (np.abs(m5) >= threshold(thresholds, "abs_momentum_5_atr_q70", 0.0))
            & (middle == -slow)
            & (fast == slow)
            & (sign(arrays["sma7_minus_8_atr"]) == slow)
            & (sign(arrays["sma30_slope_5_atr"]) == slow)
            & (arrays["spread_ratio_60"] <= 1.0)
        )
        output[mask & (slow != 0)] = slow[mask & (slow != 0)]
    elif strategy == "compression_confirmed_breakout":
        edge = sign(arrays["range_position_60_centered"])
        # Compression is a setup state.  Requiring the break and compression on
        # the same bar suppresses the very transition being tested, so use the
        # fully known state from 15 minutes earlier on the five-minute panel.
        prior_compression = pd.to_numeric(
            frame["compression_30"], errors="coerce"
        ).shift(3).to_numpy(dtype=float)
        mask = (
            (prior_compression <= threshold(thresholds, "compression_30_q20", 0.0))
            & (np.abs(arrays["range_position_60_centered"]) >= threshold(
                thresholds, "abs_range_position_60_centered_q80", 0.0
            ))
            & (np.abs(m15) >= threshold(thresholds, "abs_momentum_15_atr_q70", 0.0))
            & (middle == edge)
            & (arrays["spread_ratio_60"] <= 1.0)
        )
        output[mask & (edge != 0)] = edge[mask & (edge != 0)]
    elif strategy == "expansion_strength_direction":
        mask = (
            (arrays["atr15_to_atr240"] >= threshold(thresholds, "atr15_to_atr240_q90", math.inf))
            & (np.abs(arrays["strength_gap_rank_15"]) >= threshold(
                thresholds, "abs_strength_gap_rank_15_q70", 0.0
            ))
            & (np.abs(m15) >= threshold(thresholds, "abs_momentum_15_atr_q70", 0.0))
            & (arrays["volume_z_30"] >= 0.0)
            & (arrays["spread_ratio_60"] <= 1.0)
        )
        output[mask & (strength != 0)] = strength[mask & (strength != 0)]
    elif strategy == "liquidity_sweep_reclaim":
        return rolling_sweep_direction(frame, instrument)
    elif strategy == "london_asian_range_breakout":
        return london_breakout_direction(frame, instrument)
    else:
        raise KeyError(f"unknown strategy: {strategy}")
    return output


def update_accumulator(target: dict[Any, list[float]], key: Any, values: Iterable[float]) -> None:
    incoming = list(values)
    current = target.setdefault(key, [0.0] * len(incoming))
    for index, value in enumerate(incoming):
        current[index] += float(value)


def phase_name_for_block(block: str) -> str:
    for phase, blocks in PHASES.items():
        if block in blocks:
            return phase
    raise KeyError(block)


def aggregate_results(
    pair_rows: list[dict[str, Any]],
    cluster_acc: Mapping[tuple[str, int, str, int], list[float]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pair_frame = pd.DataFrame(pair_rows)
    block_rows: list[dict[str, Any]] = []
    phase_rows: list[dict[str, Any]] = []
    cluster_map: dict[tuple[str, int, str], list[float]] = defaultdict(list)
    for (strategy, horizon, block, _bucket), (count, net_sum) in cluster_acc.items():
        if count:
            cluster_map[(strategy, horizon, block)].append(net_sum / count)

    for spec in STRATEGIES:
        for horizon in spec.horizons:
            for block, _start, _end in block_definitions():
                subset = pair_frame[
                    (pair_frame["strategy"] == spec.name)
                    & (pair_frame["horizon_minutes"] == horizon)
                    & (pair_frame["block"] == block)
                ] if not pair_frame.empty else pd.DataFrame()
                count = int(subset["trigger_count"].sum()) if not subset.empty else 0
                cluster_values = np.asarray(cluster_map.get((spec.name, horizon, block), []), dtype=float)
                cluster_mean, ci_low, se = mean_ci(cluster_values)
                pair_means = (
                    subset["net_units_sum"].to_numpy(dtype=float)
                    / subset["trigger_count"].to_numpy(dtype=float)
                    if count else np.empty(0)
                )
                block_rows.append({
                    "strategy": spec.name,
                    "description": spec.description,
                    "horizon_minutes": horizon,
                    "block": block,
                    "trigger_count": count,
                    "pair_count": int(len(subset)),
                    "mean_net_units": float(subset["net_units_sum"].sum() / count) if count else math.nan,
                    "mean_gross_units": float(subset["gross_units_sum"].sum() / count) if count else math.nan,
                    "mean_cost_units": float(subset["cost_units_sum"].sum() / count) if count else math.nan,
                    "win_rate": float(subset["wins"].sum() / count) if count else math.nan,
                    "direction_accuracy": float(subset["direction_correct"].sum() / count) if count else math.nan,
                    "positive_pair_fraction": float((pair_means > 0).mean()) if len(pair_means) else math.nan,
                    "cluster_count": int(len(cluster_values)),
                    "cluster_mean_net_units": cluster_mean,
                    "cluster_ci95_low": ci_low,
                    "cluster_standard_error": se,
                })

            for phase, phase_blocks in PHASES.items():
                relevant_blocks = [
                    row for row in block_rows
                    if row["strategy"] == spec.name
                    and row["horizon_minutes"] == horizon
                    and row["block"] in phase_blocks
                ]
                pair_subset = pair_frame[
                    (pair_frame["strategy"] == spec.name)
                    & (pair_frame["horizon_minutes"] == horizon)
                    & (pair_frame["block"].isin(phase_blocks))
                ] if not pair_frame.empty else pd.DataFrame()
                cluster_values = np.asarray([
                    net_sum / count
                    for (name, item_horizon, block, _bucket), (count, net_sum) in cluster_acc.items()
                    if name == spec.name and item_horizon == horizon and block in phase_blocks and count
                ], dtype=float)
                mean, ci_low, se = mean_ci(cluster_values)
                count = sum(int(row["trigger_count"]) for row in relevant_blocks)
                net_sum = sum(
                    float(row["mean_net_units"]) * int(row["trigger_count"])
                    for row in relevant_blocks if int(row["trigger_count"])
                )
                gross_sum = float(pair_subset["gross_units_sum"].sum()) if count else 0.0
                cost_sum = float(pair_subset["cost_units_sum"].sum()) if count else 0.0
                wins = int(pair_subset["wins"].sum()) if count else 0
                correct = int(pair_subset["direction_correct"].sum()) if count else 0
                pair_agg = (
                    pair_subset.groupby("instrument", observed=True)[["trigger_count", "net_units_sum"]].sum()
                    if not pair_subset.empty else pd.DataFrame()
                )
                z = mean / se if math.isfinite(mean) and math.isfinite(se) and se > 0 else math.nan
                p_value = 0.5 * math.erfc(z / math.sqrt(2.0)) if math.isfinite(z) else math.nan
                phase_rows.append({
                    "strategy": spec.name,
                    "description": spec.description,
                    "horizon_minutes": horizon,
                    "phase": phase,
                    "trigger_count": count,
                    "pair_count": int(len(pair_agg)),
                    "mean_net_units": net_sum / count if count else math.nan,
                    "mean_gross_units": gross_sum / count if count else math.nan,
                    "mean_cost_units": cost_sum / count if count else math.nan,
                    "win_rate": wins / count if count else math.nan,
                    "direction_accuracy": correct / count if count else math.nan,
                    "cluster_count": int(len(cluster_values)),
                    "cluster_mean_net_units": mean,
                    "cluster_ci95_low": ci_low,
                    "cluster_standard_error": se,
                    "cluster_one_sided_p": p_value,
                    "positive_pair_fraction": (
                        float((pair_agg["net_units_sum"] > 0).mean())
                        if not pair_agg.empty else math.nan
                    ),
                })

    final_indices = [index for index, row in enumerate(phase_rows) if row["phase"] == "final_2026H1"]
    adjusted = bh_adjust([phase_rows[index]["cluster_one_sided_p"] for index in final_indices])
    for index, q_value in zip(final_indices, adjusted):
        phase_rows[index]["final_bh_q"] = q_value
    return block_rows, phase_rows


def candidate_summary(
    block_rows: list[dict[str, Any]], phase_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    blocks = pd.DataFrame(block_rows)
    phases = pd.DataFrame(phase_rows)
    output: list[dict[str, Any]] = []
    for spec in STRATEGIES:
        for horizon in spec.horizons:
            selected = phases[(phases["strategy"] == spec.name) & (phases["horizon_minutes"] == horizon)]
            by_phase = selected.set_index("phase")
            if any(name not in by_phase.index for name in PHASES):
                continue
            values = {name: float(by_phase.loc[name, "cluster_mean_net_units"]) for name in PHASES}
            final = by_phase.loc["final_2026H1"]
            item_blocks = blocks[(blocks["strategy"] == spec.name) & (blocks["horizon_minutes"] == horizon)]
            positive_quarters = int((item_blocks["cluster_mean_net_units"] > 0).sum())
            final_q = float(final.get("final_bh_q", math.nan))
            promoted = bool(
                all(value > 0 for value in values.values())
                and positive_quarters >= 4
                and int(final["trigger_count"]) >= 100
                and int(final["pair_count"]) >= 8
                and float(final["positive_pair_fraction"]) >= 0.55
                and float(final["cluster_ci95_low"]) > 0
                and math.isfinite(final_q)
                and final_q <= 0.10
            )
            output.append({
                "strategy": spec.name,
                "description": spec.description,
                "horizon_minutes": horizon,
                **values,
                "minimum_phase_cluster_net_units": min(values.values()),
                "positive_quarter_count": positive_quarters,
                "final_trigger_count": int(final["trigger_count"]),
                "final_pair_count": int(final["pair_count"]),
                "final_positive_pair_fraction": float(final["positive_pair_fraction"]),
                "final_cluster_ci95_low": float(final["cluster_ci95_low"]),
                "final_mean_net_units": float(final["mean_net_units"]),
                "final_mean_gross_units": float(final["mean_gross_units"]),
                "final_mean_cost_units": float(final["mean_cost_units"]),
                "final_win_rate": float(final["win_rate"]),
                "final_direction_accuracy": float(final["direction_accuracy"]),
                "final_bh_q": final_q,
                "promoted": promoted,
            })
    return sorted(
        output,
        key=lambda row: (row["promoted"], row["minimum_phase_cluster_net_units"], row["final_2026H1"]),
        reverse=True,
    )


def load_prior_baselines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    wanted = {
        "pullback_trend_continuation",
        "compressed_micro_break",
        "cross_strength_continuation",
    }
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [row for row in csv.DictReader(handle) if row.get("pattern") in wanted]


def load_news(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def pair_group_diagnostics(pair_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not pair_rows:
        return []
    frame = pd.DataFrame(pair_rows)
    frame["phase"] = frame["block"].map(phase_name_for_block)
    output: list[dict[str, Any]] = []
    for keys, subset in frame.groupby(
        ["strategy", "horizon_minutes", "phase", "pair_group"], observed=True
    ):
        strategy, horizon, phase, group = keys
        count = int(subset["trigger_count"].sum())
        pair_net = subset.groupby("instrument", observed=True)["net_units_sum"].sum()
        output.append({
            "strategy": strategy,
            "horizon_minutes": int(horizon),
            "phase": phase,
            "pair_group": group,
            "trigger_count": count,
            "pair_count": int(len(pair_net)),
            "mean_net_units": float(subset["net_units_sum"].sum() / count),
            "mean_gross_units": float(subset["gross_units_sum"].sum() / count),
            "mean_cost_units": float(subset["cost_units_sum"].sum() / count),
            "win_rate": float(subset["wins"].sum() / count),
            "direction_accuracy": float(subset["direction_correct"].sum() / count),
            "positive_pair_fraction": float((pair_net > 0).mean()),
        })
    return output


def render_report(
    inventory: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    prior: list[dict[str, Any]],
    news: Mapping[str, Any],
    group_rows: list[dict[str, Any]],
) -> str:
    promoted = [row for row in candidates if row["promoted"]]
    lines = [
        "# Distinct Strategy Backtest — 2026-08-02",
        "",
        f"Generated: `{utc_now()}`.",
        f"Coverage: `{len(inventory)}` pairs and `{sum(int(row['rows']) for row in inventory):,}` stored five-minute rows.",
        "All thresholds were frozen before 2025-01-01. Six later quarters use transition-only entries, full-horizon cooldowns, boundary purges, stored bid/ask costs, synchronized-time clustering, pair breadth, and final false-discovery control.",
        "",
        "## Decision",
        "",
        f"Promoted strategies: `{len(promoted)}` of `{len(candidates)}` strategy/horizon tests.",
        "Every result remains research-only; this harness has no account or execution adapter.",
        "",
        "## New independent tests",
        "",
        "| Strategy | Horizon | 2025 H1 | 2025 H2 | 2026 H1 | Positive quarters | Final pairs positive | Final CI low | q | Promote |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in candidates:
        q_value = row["final_bh_q"]
        lines.append(
            "| {strategy} | {horizon_minutes}m | {selection_2025H1:+.4f} | {confirmation_2025H2:+.4f} | {final_2026H1:+.4f} | {positive_quarter_count}/6 | {final_positive_pair_fraction:.1%} | {final_cluster_ci95_low:+.4f} | {q} | {promoted} |".format(
                **row, q=(f"{q_value:.4g}" if math.isfinite(q_value) else "n/a")
            )
        )
    lines.extend([
        "",
        "## Final-period economics",
        "",
        "| Strategy | Horizon | Direction accuracy | Gross units | Cost units | Net units | Win rate |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for row in candidates:
        lines.append(
            f"| {row['strategy']} | {row['horizon_minutes']}m | {row['final_direction_accuracy']:.1%} | {row['final_mean_gross_units']:+.4f} | {row['final_mean_cost_units']:+.4f} | {row['final_mean_net_units']:+.4f} | {row['final_win_rate']:.1%} |"
        )
    final_groups = [row for row in group_rows if row["phase"] == "final_2026H1"]
    final_groups.sort(key=lambda row: row["mean_net_units"], reverse=True)
    lines.extend([
        "",
        "## Best final-period universe segments (diagnostic only)",
        "",
        "These segment rows were not separate pre-registered promotion tests and therefore cannot promote a strategy.",
        "",
        "| Strategy | Horizon | Universe | Trades | Net units | Direction accuracy | Positive pairs |",
        "|---|---:|---|---:|---:|---:|---:|",
    ])
    for row in final_groups[:12]:
        lines.append(
            f"| {row['strategy']} | {row['horizon_minutes']}m | {row['pair_group']} | {row['trigger_count']} | {row['mean_net_units']:+.4f} | {row['direction_accuracy']:.1%} | {row['positive_pair_fraction']:.1%} |"
        )
    lines.extend(["", "## Existing baselines retained, not duplicated", ""])
    if prior:
        lines.extend([
            "| Baseline | Horizon | 2025 H1 | 2025 H2 | 2026 H1 | Replicated |",
            "|---|---:|---:|---:|---:|---|",
        ])
        for row in sorted(prior, key=lambda item: (item["pattern"], int(item["horizon_minutes"]))):
            lines.append(
                f"| {row['pattern']} | {row['horizon_minutes']}m | {float(row['selection_2025H1']):+.4f} | {float(row['confirmation_2025H2']):+.4f} | {float(row['final_2026H1']):+.4f} | {row['replicated_profitable']} |"
            )
    else:
        lines.append("Prior all-68 baseline report was unavailable.")
    lines.extend(["", "## News-reaction arm", ""])
    if news:
        horizons = news.get("horizons", {})
        lines.append(
            f"The causal news replay contains `{news.get('calls', 0):,}` scored calls but only `{news.get('episodes', 0)}` episode-horizon rows. True actual-plus-consensus coverage is `{float(news.get('true_surprise_coverage', {}).get('actual_and_consensus_coverage', 0.0)):.1%}`."
        )
        lines.append("")
        lines.append("| Horizon | Independent episodes | Follow net pips | Fade net pips | Decision |")
        lines.append("|---:|---:|---:|---:|---|")
        for key in sorted(horizons, key=lambda item: int(item)):
            item = horizons[key]
            lines.append(
                f"| {key}m | {item.get('episodes', 0)} | {float(item.get('naive_follow', {}).get('average_net_pips', math.nan)):+.3f} | {float(item.get('naive_fade', {}).get('average_net_pips', math.nan)):+.3f} | {item.get('shadow_decision', 'unknown')} |"
            )
    else:
        lines.append("Causal news audit was unavailable.")
    lines.extend([
        "",
        "## Guardrails",
        "",
        "- No result is allowed to change account 007, candidate routing, or order policy.",
        "- A positive isolated quarter is not promotion; every phase, breadth, confidence, sample, and multiplicity condition must pass.",
        "- Stored bid/ask outcomes are used directly; raw midpoint direction accuracy is reported only as a diagnostic.",
        "- London session times use Europe/London local time, including daylight-saving changes.",
    ])
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict[str, Any]:
    feature_dir = Path(args.feature_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    discovery_end = pd.Timestamp(args.discovery_end, tz="UTC")
    paths = sorted(feature_dir.glob("*.parquet"))
    if args.pair_group != "all":
        paths = [path for path in paths if pair_group(path.stem) == args.pair_group]
    if args.max_pairs:
        paths = paths[: int(args.max_pairs)]
    if not paths:
        raise FileNotFoundError(f"no feature parquet files under {feature_dir}")

    outcome_columns = [
        column
        for horizon in HORIZONS
        for column in (
            f"future_move_pips_{horizon}",
            f"future_long_net_pips_{horizon}",
            f"future_short_net_pips_{horizon}",
        )
    ]
    inventory = [metadata_row(path) for path in paths]
    pair_rows: list[dict[str, Any]] = []
    cluster_acc: dict[tuple[str, int, str, int], list[float]] = {}
    thresholds_by_pair: dict[str, dict[str, float]] = {}

    for number, path in enumerate(paths, 1):
        frame = pd.read_parquet(path, columns=[*FEATURE_COLUMNS, *outcome_columns])
        frame.index = pd.to_datetime(frame.index, utc=True)
        frame = frame.replace([np.inf, -np.inf], np.nan)
        discovery_source = frame.loc[frame.index < discovery_end].iloc[:: max(1, int(args.discovery_stride))]
        thresholds = derive_distinct_thresholds(discovery_source)
        thresholds_by_pair[path.stem] = thresholds
        scales = {
            horizon: max(
                1e-9,
                float(pd.to_numeric(
                    discovery_source[f"future_move_pips_{horizon}"], errors="coerce"
                ).abs().quantile(0.50)),
            )
            for horizon in HORIZONS
        }
        grid_mask = np.asarray((frame.index.minute % GRID_MINUTES) == 0)

        for spec in STRATEGIES:
            full_direction = strategy_direction(frame, spec.name, thresholds, path.stem)
            directions = full_direction[grid_mask]
            grid_frame = frame.loc[grid_mask]
            times_ns = grid_frame.index.asi8
            for horizon in spec.horizons:
                selected = independent_transition_indices(directions, times_ns, horizon)
                if not len(selected):
                    continue
                move = pd.to_numeric(grid_frame[f"future_move_pips_{horizon}"], errors="coerce").to_numpy(dtype=float)
                long_net = pd.to_numeric(grid_frame[f"future_long_net_pips_{horizon}"], errors="coerce").to_numpy(dtype=float)
                short_net = pd.to_numeric(grid_frame[f"future_short_net_pips_{horizon}"], errors="coerce").to_numpy(dtype=float)
                for block, start, end in block_definitions():
                    chosen = selected[
                        (grid_frame.index[selected] >= start)
                        & (grid_frame.index[selected] + pd.Timedelta(minutes=horizon) < end)
                    ]
                    if not len(chosen):
                        continue
                    chosen_direction = directions[chosen]
                    net = np.where(chosen_direction > 0, long_net[chosen], short_net[chosen])
                    raw = move[chosen]
                    valid = np.isfinite(net) & np.isfinite(raw)
                    chosen = chosen[valid]
                    chosen_direction = chosen_direction[valid]
                    net = net[valid]
                    raw = raw[valid]
                    if not len(chosen):
                        continue
                    units = net / scales[horizon]
                    pair_rows.append({
                        "strategy": spec.name,
                        "horizon_minutes": horizon,
                        "block": block,
                        "instrument": path.stem,
                        "pair_group": pair_group(path.stem),
                        "trigger_count": int(len(chosen)),
                        "net_units_sum": float(units.sum()),
                        "gross_units_sum": float((chosen_direction * raw / scales[horizon]).sum()),
                        "cost_units_sum": float(((chosen_direction * raw - net) / scales[horizon]).sum()),
                        "wins": int((net > 0).sum()),
                        "direction_correct": int((chosen_direction * raw > 0).sum()),
                    })
                    buckets = times_ns[chosen] // (horizon * 60 * 1_000_000_000)
                    for bucket in np.unique(buckets):
                        values = units[buckets == bucket]
                        update_accumulator(
                            cluster_acc,
                            (spec.name, horizon, block, int(bucket)),
                            (len(values), float(values.sum())),
                        )
        print(f"[distinct] {number:02d}/{len(paths)} {path.stem}", flush=True)

    block_rows, phase_rows = aggregate_results(pair_rows, cluster_acc)
    candidates = candidate_summary(block_rows, phase_rows)
    group_rows = pair_group_diagnostics(pair_rows)
    prior = load_prior_baselines(Path(args.baseline))
    news = load_news(Path(args.news_audit))
    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_csv(output_dir / "pair_metrics.csv", pair_rows)
    atomic_csv(output_dir / "block_metrics.csv", block_rows)
    atomic_csv(output_dir / "phase_metrics.csv", phase_rows)
    atomic_csv(output_dir / "candidate_summary.csv", candidates)
    atomic_csv(output_dir / "pair_group_diagnostics.csv", group_rows)
    atomic_json(output_dir / "thresholds.json", thresholds_by_pair)
    atomic_text(
        output_dir / "DISTINCT_STRATEGY_BACKTEST_20260802.md",
        render_report(inventory, candidates, prior, news, group_rows),
    )
    summary = {
        "generated_utc": utc_now(),
        "schema_version": 1,
        "research_only": True,
        "shadow_only": True,
        "execution_adapter": False,
        "account_eligible": False,
        "pair_count": len(paths),
        "source_rows": sum(int(row["rows"]) for row in inventory),
        "discovery_end": discovery_end.isoformat(),
        "evaluation_blocks": [name for name, _start, _end in BLOCKS],
        "strategy_count": len(STRATEGIES),
        "strategy_horizon_tests": len(candidates),
        "promoted_count": sum(bool(row["promoted"]) for row in candidates),
        "best_candidates": candidates[:10],
        "prior_baselines_included": len(prior),
        "news_audit_included": bool(news),
    }
    atomic_json(output_dir / "summary.json", summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-dir", type=Path, default=DEFAULT_FEATURE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--news-audit", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--discovery-end", default="2025-01-01")
    parser.add_argument("--discovery-stride", type=int, default=4)
    parser.add_argument("--pair-group", choices=("all", "usd_related", "major_cross", "exotic_or_regional"), default="all")
    parser.add_argument("--max-pairs", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = run(args)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
