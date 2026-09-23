#!/usr/bin/env python3
"""Leakage-safe, research-only pattern study over the all-68 FX archive.

The study deliberately separates two questions:

1. Does a state concentrate unusually large future moves?
2. Does its inferred direction earn positive bid/ask net movement?

Feature cutoffs and pair/horizon normalizers are frozen from data before
2025-01-01.  Six later quarters are then used as untouched replication blocks.
No output from this script is consumed by an executor.
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
from typing import Any, Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
DEFAULT_FEATURE_DIR = ROOT / "data" / "all68_weekly_move_study" / "features"
DEFAULT_OUTPUT_DIR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "all68_pattern_research_20260802"
)

HORIZONS = (15, 30, 60, 120, 240)
FEATURE_COLUMNS = (
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_60_atr",
    "acceleration_15_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "rsi14_centered",
    "atr15_to_atr240",
    "compression_30",
    "range_position_60_centered",
    "range_position_240_centered",
    "spread_ratio_60",
    "volume_z_30",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
)
ABS_QUANTILE_FEATURES = (
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_60_atr",
    "acceleration_15_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "rsi14_centered",
    "range_position_60_centered",
    "range_position_240_centered",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
)
QUANTILES = (0.20, 0.25, 0.70, 0.80, 0.90)
BLOCKS = (
    ("2025Q1", "2025-01-01", "2025-04-01"),
    ("2025Q2", "2025-04-01", "2025-07-01"),
    ("2025Q3", "2025-07-01", "2025-10-01"),
    ("2025Q4", "2025-10-01", "2026-01-01"),
    ("2026Q1", "2026-01-01", "2026-04-01"),
    ("2026Q2", "2026-04-01", "2026-07-01"),
)
PHASES = {
    "selection_2025H1": ("2025Q1", "2025Q2"),
    "confirmation_2025H2": ("2025Q3", "2025Q4"),
    "final_2026H1": ("2026Q1", "2026Q2"),
}
EXOTIC_CURRENCIES = {
    "CNH", "CZK", "DKK", "HKD", "HUF", "MXN", "NOK", "PLN",
    "SEK", "SGD", "THB", "TRY", "ZAR",
}


@dataclass(frozen=True)
class PatternSpec:
    name: str
    description: str


PATTERNS = (
    PatternSpec("fast_momentum_continuation", "extreme 15m momentum, follow"),
    PatternSpec("slow_momentum_continuation", "extreme 60m momentum, follow"),
    PatternSpec("momentum_alignment", "15m and 60m momentum agree"),
    PatternSpec("trend_stack_continuation", "momentum and two trend slopes agree"),
    PatternSpec("cross_strength_continuation", "extreme cross-sectional currency rank gap"),
    PatternSpec("price_strength_agreement", "price momentum and currency strength agree"),
    PatternSpec("price_strength_disagreement_fade", "follow strength when price disagrees"),
    PatternSpec("rsi_range_reversion", "fade aligned RSI and range-location extremes"),
    PatternSpec("rsi_range_breakout", "follow aligned RSI and range-location extremes"),
    PatternSpec("acceleration_continuation", "follow aligned short momentum and acceleration"),
    PatternSpec("acceleration_reversal", "fade aligned short momentum and acceleration"),
    PatternSpec("volatility_expansion_momentum", "follow momentum during fast ATR expansion"),
    PatternSpec("compressed_micro_break", "follow a small break from low range compression"),
    PatternSpec("high_volume_momentum", "follow momentum on unusually high tick volume"),
    PatternSpec("pullback_trend_continuation", "follow 60m trend after an opposing 5m pullback"),
    PatternSpec("low_cost_momentum_alignment", "momentum alignment in a low spread-ratio state"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temp.write_text(value, encoding="utf-8")
    os.replace(temp, path)


def atomic_json(path: Path, value: Any) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True, default=str))


def atomic_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    if not materialized:
        temp.write_text("", encoding="utf-8")
    else:
        fields = sorted({key for row in materialized for key in row})
        with temp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(materialized)
    os.replace(temp, path)


def pair_group(pair: str) -> str:
    base, quote = pair.split("_", 1)
    if base in EXOTIC_CURRENCIES or quote in EXOTIC_CURRENCIES:
        return "exotic_or_regional"
    if "USD" in (base, quote):
        return "usd_related"
    return "major_cross"


def finite_series(frame: pd.DataFrame, column: str) -> np.ndarray:
    return pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)


def threshold_key(column: str, quantile: float, absolute: bool = False) -> str:
    prefix = "abs_" if absolute else ""
    return f"{prefix}{column}_q{int(round(quantile * 100)):02d}"


def derive_thresholds(discovery: pd.DataFrame) -> dict[str, float]:
    """Derive every pattern cutoff from the discovery window only."""
    thresholds: dict[str, float] = {}
    for column in ABS_QUANTILE_FEATURES:
        values = pd.to_numeric(discovery[column], errors="coerce").abs().dropna()
        for quantile in (0.70, 0.80, 0.90):
            thresholds[threshold_key(column, quantile, True)] = float(
                values.quantile(quantile)
            )
    for column, quantiles in {
        "atr15_to_atr240": (0.90,),
        "compression_30": (0.20, 0.90),
        "spread_ratio_60": (0.25,),
        "volume_z_30": (0.90,),
    }.items():
        values = pd.to_numeric(discovery[column], errors="coerce").dropna()
        for quantile in quantiles:
            thresholds[threshold_key(column, quantile)] = float(
                values.quantile(quantile)
            )
    return thresholds


def direction_from_pattern(
    frame: pd.DataFrame,
    pattern: str,
    thresholds: dict[str, float],
) -> np.ndarray:
    """Return -1/0/+1 for a predeclared symmetric pattern."""
    values = {column: finite_series(frame, column) for column in FEATURE_COLUMNS}
    signs = {
        column: np.where(np.isfinite(value), np.sign(value), 0).astype(np.int8)
        for column, value in values.items()
    }

    def aq(column: str, q: float) -> float:
        return thresholds[threshold_key(column, q, True)]

    def q(column: str, level: float) -> float:
        return thresholds[threshold_key(column, level)]

    m5 = values["momentum_5_atr"]
    m15 = values["momentum_15_atr"]
    m60 = values["momentum_60_atr"]
    accel = values["acceleration_15_atr"]
    fast_slope = values["sma7_minus_8_atr"]
    slow_slope = values["sma30_slope_5_atr"]
    rsi = values["rsi14_centered"]
    range60 = values["range_position_60_centered"]
    strength15 = values["strength_gap_rank_15"]
    strength60 = values["strength_gap_rank_60"]
    direction = np.zeros(len(frame), dtype=np.int8)

    if pattern == "fast_momentum_continuation":
        mask = np.abs(m15) >= aq("momentum_15_atr", 0.90)
        source = signs["momentum_15_atr"]
    elif pattern == "slow_momentum_continuation":
        mask = np.abs(m60) >= aq("momentum_60_atr", 0.90)
        source = signs["momentum_60_atr"]
    elif pattern == "momentum_alignment":
        mask = (
            (np.abs(m15) >= aq("momentum_15_atr", 0.80))
            & (np.abs(m60) >= aq("momentum_60_atr", 0.70))
            & (signs["momentum_15_atr"] == signs["momentum_60_atr"])
        )
        source = signs["momentum_15_atr"]
    elif pattern == "trend_stack_continuation":
        source = signs["momentum_60_atr"]
        mask = (
            (np.abs(m60) >= aq("momentum_60_atr", 0.70))
            & (np.abs(fast_slope) >= aq("sma7_minus_8_atr", 0.70))
            & (np.abs(slow_slope) >= aq("sma30_slope_5_atr", 0.70))
            & (source == signs["sma7_minus_8_atr"])
            & (source == signs["sma30_slope_5_atr"])
        )
    elif pattern == "cross_strength_continuation":
        mask = np.abs(strength15) >= aq("strength_gap_rank_15", 0.90)
        source = signs["strength_gap_rank_15"]
    elif pattern == "price_strength_agreement":
        source = signs["momentum_15_atr"]
        mask = (
            (np.abs(m15) >= aq("momentum_15_atr", 0.70))
            & (np.abs(strength15) >= aq("strength_gap_rank_15", 0.70))
            & (source == signs["strength_gap_rank_15"])
        )
    elif pattern == "price_strength_disagreement_fade":
        source = signs["strength_gap_rank_15"]
        mask = (
            (np.abs(m15) >= aq("momentum_15_atr", 0.80))
            & (np.abs(strength15) >= aq("strength_gap_rank_15", 0.70))
            & (source == -signs["momentum_15_atr"])
        )
    elif pattern in {"rsi_range_reversion", "rsi_range_breakout"}:
        source = signs["range_position_60_centered"]
        mask = (
            (np.abs(rsi) >= aq("rsi14_centered", 0.80))
            & (np.abs(range60) >= aq("range_position_60_centered", 0.80))
            & (source == signs["rsi14_centered"])
        )
        if pattern.endswith("reversion"):
            source = -source
    elif pattern in {"acceleration_continuation", "acceleration_reversal"}:
        source = signs["acceleration_15_atr"]
        mask = (
            (np.abs(accel) >= aq("acceleration_15_atr", 0.90))
            & (np.abs(m5) >= aq("momentum_5_atr", 0.70))
            & (source == signs["momentum_5_atr"])
        )
        if pattern.endswith("reversal"):
            source = -source
    elif pattern == "volatility_expansion_momentum":
        source = signs["momentum_15_atr"]
        mask = (
            (values["atr15_to_atr240"] >= q("atr15_to_atr240", 0.90))
            & (np.abs(m15) >= aq("momentum_15_atr", 0.80))
        )
    elif pattern == "compressed_micro_break":
        source = signs["momentum_5_atr"]
        mask = (
            (values["compression_30"] <= q("compression_30", 0.20))
            & (np.abs(m5) >= aq("momentum_5_atr", 0.70))
        )
    elif pattern == "high_volume_momentum":
        source = signs["momentum_15_atr"]
        mask = (
            (values["volume_z_30"] >= q("volume_z_30", 0.90))
            & (np.abs(m15) >= aq("momentum_15_atr", 0.80))
        )
    elif pattern == "pullback_trend_continuation":
        source = signs["momentum_60_atr"]
        mask = (
            (np.abs(m60) >= aq("momentum_60_atr", 0.80))
            & (np.abs(m5) >= aq("momentum_5_atr", 0.70))
            & (np.abs(m5) <= aq("momentum_5_atr", 0.90))
            & (source == -signs["momentum_5_atr"])
        )
    elif pattern == "low_cost_momentum_alignment":
        source = signs["momentum_15_atr"]
        mask = (
            (values["spread_ratio_60"] <= q("spread_ratio_60", 0.25))
            & (np.abs(m15) >= aq("momentum_15_atr", 0.80))
            & (np.abs(m60) >= aq("momentum_60_atr", 0.70))
            & (source == signs["momentum_60_atr"])
        )
    else:
        raise KeyError(f"unknown pattern: {pattern}")

    valid = mask & np.isfinite(np.asarray(source, dtype=float)) & (source != 0)
    direction[valid] = np.asarray(source, dtype=np.int8)[valid]
    return direction


def independent_transition_indices(
    directions: np.ndarray,
    times_ns: np.ndarray,
    horizon_minutes: int,
) -> np.ndarray:
    """Take state transitions and enforce a full-horizon cooldown."""
    if len(directions) == 0:
        return np.empty(0, dtype=np.int64)
    transition = (directions != 0) & np.r_[True, directions[1:] != directions[:-1]]
    candidates = np.flatnonzero(transition)
    if len(candidates) < 2:
        return candidates.astype(np.int64)
    gap = int(horizon_minutes) * 60 * 1_000_000_000
    selected: list[int] = []
    next_allowed = -2**63
    for index in candidates:
        timestamp = int(times_ns[index])
        if timestamp >= next_allowed:
            selected.append(int(index))
            next_allowed = timestamp + gap
    return np.asarray(selected, dtype=np.int64)


def session_name(timestamp: pd.Timestamp) -> str:
    hour = timestamp.hour + timestamp.minute / 60.0
    if hour >= 22.0 or hour < 7.0:
        return "asia"
    if hour < 12.0:
        return "london"
    if hour < 16.0:
        return "london_ny_overlap"
    if hour < 21.0:
        return "new_york_late"
    return "rollover"


def metadata_row(path: Path) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    index = parquet.schema.names.index("time_utc")
    stats = parquet.metadata.row_group(0).column(index).statistics
    return {
        "instrument": path.stem,
        "rows": int(parquet.metadata.num_rows),
        "start": str(stats.min) if stats else "",
        "end": str(stats.max) if stats else "",
        "bytes": int(path.stat().st_size),
        "pair_group": pair_group(path.stem),
    }


def block_definitions() -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    return [
        (name, pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC"))
        for name, start, end in BLOCKS
    ]


def _update_accumulator(target: dict[Any, list[float]], key: Any, values: Iterable[float]) -> None:
    incoming = list(values)
    row = target.setdefault(key, [0.0] * len(incoming))
    for index, value in enumerate(incoming):
        row[index] += float(value)


def mean_ci(values: np.ndarray) -> tuple[float, float, float]:
    values = values[np.isfinite(values)]
    if len(values) == 0:
        return math.nan, math.nan, math.nan
    mean = float(values.mean())
    if len(values) < 2:
        return mean, math.nan, math.nan
    se = float(values.std(ddof=1) / math.sqrt(len(values)))
    return mean, mean - 1.96 * se, se


def bh_adjust(p_values: list[float]) -> list[float]:
    """Benjamini-Hochberg adjustment with NaNs preserved."""
    output = [math.nan] * len(p_values)
    valid = [(index, value) for index, value in enumerate(p_values) if math.isfinite(value)]
    if not valid:
        return output
    ordered = sorted(valid, key=lambda item: item[1])
    running = 1.0
    count = len(ordered)
    for rank in range(count, 0, -1):
        index, value = ordered[rank - 1]
        running = min(running, value * count / rank)
        output[index] = min(1.0, running)
    return output


def aggregate_rows(
    pair_rows: list[dict[str, Any]],
    cluster_acc: dict[tuple[str, int, str, int], list[float]],
    baseline_acc: dict[tuple[str, int], list[float]],
) -> list[dict[str, Any]]:
    pair_frame = pd.DataFrame(pair_rows)
    output: list[dict[str, Any]] = []
    clusters_by_key: dict[tuple[str, int, str], list[float]] = defaultdict(list)
    for (pattern, horizon, block, _bucket), (count, net_sum) in cluster_acc.items():
        if count > 0:
            clusters_by_key[(pattern, horizon, block)].append(net_sum / count)

    for spec in PATTERNS:
        for horizon in HORIZONS:
            for block, _start, _end in block_definitions():
                subset = pair_frame[
                    (pair_frame["pattern"] == spec.name)
                    & (pair_frame["horizon_minutes"] == horizon)
                    & (pair_frame["block"] == block)
                ]
                count = int(subset["trigger_count"].sum()) if not subset.empty else 0
                net_sum = float(subset["net_units_sum"].sum()) if count else 0.0
                wins = float(subset["wins"].sum()) if count else 0.0
                correct = float(subset["direction_correct"].sum()) if count else 0.0
                spikes = float(subset["spikes"].sum()) if count else 0.0
                pair_means = (
                    subset["net_units_sum"].to_numpy(dtype=float)
                    / subset["trigger_count"].to_numpy(dtype=float)
                    if count
                    else np.empty(0)
                )
                cluster_values = np.asarray(
                    clusters_by_key.get((spec.name, horizon, block), []), dtype=float
                )
                cluster_mean, cluster_ci_low, cluster_se = mean_ci(cluster_values)
                baseline_rows, baseline_spikes = baseline_acc.get((block, horizon), [0.0, 0.0])
                base_rate = baseline_spikes / baseline_rows if baseline_rows else math.nan
                spike_rate = spikes / count if count else math.nan
                output.append(
                    {
                        "pattern": spec.name,
                        "description": spec.description,
                        "horizon_minutes": horizon,
                        "block": block,
                        "trigger_count": count,
                        "pair_count": int(len(subset)),
                        "mean_net_units": net_sum / count if count else math.nan,
                        "win_rate": wins / count if count else math.nan,
                        "direction_accuracy": correct / count if count else math.nan,
                        "spike_rate": spike_rate,
                        "baseline_spike_rate": base_rate,
                        "spike_lift": spike_rate / base_rate if count and base_rate else math.nan,
                        "positive_pair_fraction": float((pair_means > 0).mean()) if len(pair_means) else math.nan,
                        "max_pair_trigger_share": float(subset["trigger_count"].max() / count) if count else math.nan,
                        "cluster_count": int(len(cluster_values)),
                        "cluster_mean_net_units": cluster_mean,
                        "cluster_ci95_low": cluster_ci_low,
                        "cluster_standard_error": cluster_se,
                        "positive_cluster_fraction": float((cluster_values > 0).mean()) if len(cluster_values) else math.nan,
                    }
                )
    return output


def phase_metrics(
    block_rows: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
    cluster_acc: dict[tuple[str, int, str, int], list[float]],
) -> list[dict[str, Any]]:
    block_frame = pd.DataFrame(block_rows)
    pair_frame = pd.DataFrame(pair_rows)
    output: list[dict[str, Any]] = []
    for spec in PATTERNS:
        for horizon in HORIZONS:
            pattern_blocks = block_frame[
                (block_frame["pattern"] == spec.name)
                & (block_frame["horizon_minutes"] == horizon)
            ]
            for phase, blocks in PHASES.items():
                block_subset = pattern_blocks[pattern_blocks["block"].isin(blocks)]
                pairs = pair_frame[
                    (pair_frame["pattern"] == spec.name)
                    & (pair_frame["horizon_minutes"] == horizon)
                    & (pair_frame["block"].isin(blocks))
                ]
                pair_agg = (
                    pairs.groupby("instrument", observed=True)[["trigger_count", "net_units_sum"]]
                    .sum()
                    .reset_index()
                    if not pairs.empty
                    else pd.DataFrame()
                )
                clusters = []
                for (name, item_horizon, block, _bucket), (count, net_sum) in cluster_acc.items():
                    if name == spec.name and item_horizon == horizon and block in blocks and count:
                        clusters.append(net_sum / count)
                cluster_values = np.asarray(clusters, dtype=float)
                mean, ci_low, se = mean_ci(cluster_values)
                count = int(block_subset["trigger_count"].sum()) if not block_subset.empty else 0
                weighted_net = float(
                    (block_subset["mean_net_units"] * block_subset["trigger_count"]).sum()
                    / count
                ) if count else math.nan
                z = mean / se if math.isfinite(mean) and math.isfinite(se) and se > 0 else math.nan
                one_sided_p = 0.5 * math.erfc(z / math.sqrt(2.0)) if math.isfinite(z) else math.nan
                output.append(
                    {
                        "pattern": spec.name,
                        "description": spec.description,
                        "horizon_minutes": horizon,
                        "phase": phase,
                        "trigger_count": count,
                        "pair_count": int(len(pair_agg)),
                        "mean_net_units": weighted_net,
                        "cluster_count": int(len(cluster_values)),
                        "cluster_mean_net_units": mean,
                        "cluster_ci95_low": ci_low,
                        "cluster_standard_error": se,
                        "cluster_one_sided_p": one_sided_p,
                        "positive_pair_fraction": (
                            float((pair_agg["net_units_sum"] > 0).mean())
                            if not pair_agg.empty else math.nan
                        ),
                    }
                )
    final_indices = [index for index, row in enumerate(output) if row["phase"] == "final_2026H1"]
    adjusted = bh_adjust([output[index]["cluster_one_sided_p"] for index in final_indices])
    for index, q_value in zip(final_indices, adjusted):
        output[index]["final_bh_q"] = q_value
    return output


def summarize_candidates(
    block_rows: list[dict[str, Any]], phase_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    block_frame = pd.DataFrame(block_rows)
    phase_frame = pd.DataFrame(phase_rows)
    output: list[dict[str, Any]] = []
    for spec in PATTERNS:
        for horizon in HORIZONS:
            blocks = block_frame[
                (block_frame["pattern"] == spec.name)
                & (block_frame["horizon_minutes"] == horizon)
            ]
            phases = phase_frame[
                (phase_frame["pattern"] == spec.name)
                & (phase_frame["horizon_minutes"] == horizon)
            ].set_index("phase")
            if len(phases) != len(PHASES):
                continue
            phase_values = {
                name: float(phases.loc[name, "cluster_mean_net_units"])
                for name in PHASES
            }
            final = phases.loc["final_2026H1"]
            positive_blocks = int((blocks["cluster_mean_net_units"] > 0).sum())
            min_phase = min(phase_values.values())
            final_q = float(final.get("final_bh_q", math.nan))
            replicated = bool(
                all(value > 0 for value in phase_values.values())
                and positive_blocks >= 4
                and int(final["trigger_count"]) >= 100
                and int(final["pair_count"]) >= 8
                and float(final["positive_pair_fraction"]) >= 0.55
                and math.isfinite(final_q)
                and final_q <= 0.10
            )
            output.append(
                {
                    "pattern": spec.name,
                    "description": spec.description,
                    "horizon_minutes": horizon,
                    **phase_values,
                    "minimum_phase_cluster_net_units": min_phase,
                    "positive_quarter_count": positive_blocks,
                    "final_trigger_count": int(final["trigger_count"]),
                    "final_pair_count": int(final["pair_count"]),
                    "final_positive_pair_fraction": float(final["positive_pair_fraction"]),
                    "final_bh_q": final_q,
                    "replicated_profitable": replicated,
                }
            )
    return sorted(
        output,
        key=lambda row: (
            bool(row["replicated_profitable"]),
            row["minimum_phase_cluster_net_units"],
            row["final_2026H1"],
        ),
        reverse=True,
    )


def render_report(
    inventory: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    block_rows: list[dict[str, Any]],
    thresholds: dict[str, float],
    discovery_end: pd.Timestamp,
) -> str:
    replicated = [row for row in candidates if row["replicated_profitable"]]
    best = candidates[:12]
    block_frame = pd.DataFrame(block_rows)
    volatility = (
        block_frame.groupby(["pattern", "horizon_minutes"], observed=True)
        .agg(spike_lift=("spike_lift", "mean"), mean_net=("cluster_mean_net_units", "mean"))
        .reset_index()
        .sort_values("spike_lift", ascending=False)
        .head(10)
    )
    lines = [
        "# All-68 FX Pattern Research",
        "",
        f"Generated: `{utc_now()}`.",
        f"Pairs: `{len(inventory)}`; source rows: `{sum(row['rows'] for row in inventory):,}`.",
        f"Archive range: `{min(row['start'] for row in inventory)}` through `{max(row['end'] for row in inventory)}`.",
        f"Discovery cutoff: `{discovery_end.isoformat()}`. All thresholds and pair normalizers were frozen before this time.",
        "Evaluation: six untouched quarters from 2025Q1 through 2026Q2, with a 15-minute decision grid, transition-only entries, full-horizon cooldowns, boundary purges, stored bid/ask costs, pair-level breadth, and synchronized time-cluster inference.",
        "",
        "## Decision",
        "",
        f"Strictly replicated profitable pattern/horizon combinations: `{len(replicated)}` of `{len(candidates)}`.",
    ]
    if replicated:
        lines.append("These combinations cleared every phase plus final false-discovery control; they remain research-only until execution-policy review.")
    else:
        lines.append("No combination cleared the strict promotion bar. The leading rows below are research leads, not permission to loosen execution gates.")
    lines.extend([
        "",
        "## Best Directional Leads",
        "",
        "| Pattern | Horizon | 2025 H1 | 2025 H2 | 2026 H1 | Positive quarters | Final positive pairs | Final q | Replicated |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ])
    for row in best:
        q_value = row["final_bh_q"]
        lines.append(
            "| {pattern} | {horizon_minutes}m | {selection_2025H1:+.4f} | {confirmation_2025H2:+.4f} | {final_2026H1:+.4f} | {positive_quarter_count}/6 | {final_positive_pair_fraction:.1%} | {q} | {replicated_profitable} |".format(
                **row,
                q=(f"{q_value:.4g}" if math.isfinite(q_value) else "n/a"),
            )
        )
    lines.extend([
        "",
        "`Net` is bid/ask-adjusted movement divided by that pair/horizon's discovery-period median absolute move. Time-cluster means prevent 20 correlated pair signals at one timestamp from masquerading as 20 independent observations.",
        "",
        "## Volatility Concentration (Not Directional Profit)",
        "",
        "| Pattern | Horizon | Mean spike lift | Mean directional net |",
        "|---|---:|---:|---:|",
    ])
    for row in volatility.itertuples(index=False):
        lines.append(f"| {row.pattern} | {int(row.horizon_minutes)}m | {row.spike_lift:.2f}x | {row.mean_net:+.4f} |")
    lines.extend([
        "",
        "## Interpretation Guardrails",
        "",
        "- Spike lift answers whether a state precedes a large move; it does not establish direction or profit.",
        "- A positive average with poor pair breadth or one synchronized episode is treated as concentration, not replication.",
        "- The final 2026 H1 phase is adjusted across all tested pattern/horizon combinations with Benjamini-Hochberg control.",
        "- This script and every artifact are observation-only and are not wired to account 007 or any executor.",
        "",
        "## Artifacts",
        "",
        "- `inventory.csv`: all pair coverage and date ranges",
        "- `thresholds.json`: discovery-only frozen cutoffs",
        "- `block_metrics.csv`: quarter-level evidence",
        "- `phase_metrics.csv`: selection/confirmation/final evidence",
        "- `pair_metrics.csv`: pair-level breadth evidence",
        "- `candidate_summary.csv`: ranked pattern/horizon conclusions",
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
        raise FileNotFoundError(f"no parquet files found under {feature_dir}")

    print(f"[inventory] {len(paths)} pair files", flush=True)
    inventory = [metadata_row(path) for path in paths]
    pair_rows: list[dict[str, Any]] = []
    thresholds_by_pair: dict[str, dict[str, float]] = {}
    baseline_acc: dict[tuple[str, int], list[float]] = {}
    cluster_acc: dict[tuple[str, int, str, int], list[float]] = {}
    session_acc: dict[tuple[str, int, str, str], list[float]] = {}
    group_acc: dict[tuple[str, int, str, str], list[float]] = {}
    blocks = block_definitions()
    eval_columns = [
        *FEATURE_COLUMNS,
        *[
            column
            for horizon in HORIZONS
            for column in (
                f"future_move_pips_{horizon}",
                f"future_long_net_pips_{horizon}",
                f"future_short_net_pips_{horizon}",
            )
        ],
    ]
    for number, path in enumerate(paths, 1):
        frame = pd.read_parquet(path, columns=eval_columns)
        frame.index = pd.to_datetime(frame.index, utc=True)
        grid = (frame.index.minute % int(args.decision_minutes)) == 0
        frame = frame.loc[grid].replace([np.inf, -np.inf], np.nan)
        discovery = frame.loc[frame.index < discovery_end]
        if args.discovery_stride > 1:
            threshold_source = discovery.loc[:, FEATURE_COLUMNS].iloc[:: args.discovery_stride]
        else:
            threshold_source = discovery.loc[:, FEATURE_COLUMNS]
        thresholds = derive_thresholds(threshold_source)
        thresholds_by_pair[path.stem] = thresholds
        pair_scales: dict[int, tuple[float, float]] = {}
        for horizon in HORIZONS:
            moves = pd.to_numeric(
                discovery[f"future_move_pips_{horizon}"], errors="coerce"
            ).abs().dropna()
            pair_scales[horizon] = (
                float(moves.quantile(0.50)),
                float(moves.quantile(0.90)),
            )
        times_ns = frame.index.asi8
        group = pair_group(path.stem)

        for horizon in HORIZONS:
            move = finite_series(frame, f"future_move_pips_{horizon}")
            median_scale, spike_scale = pair_scales[horizon]
            median_scale = max(median_scale, 1e-9)
            for block, start, end in blocks:
                valid = (
                    (frame.index >= start)
                    & (frame.index + pd.Timedelta(minutes=horizon) < end)
                    & np.isfinite(move)
                )
                _update_accumulator(
                    baseline_acc,
                    (block, horizon),
                    (int(valid.sum()), int((np.abs(move[valid]) >= spike_scale).sum())),
                )

        for spec in PATTERNS:
            directions = direction_from_pattern(frame, spec.name, thresholds)
            for horizon in HORIZONS:
                selected = independent_transition_indices(directions, times_ns, horizon)
                if len(selected) == 0:
                    continue
                move = finite_series(frame, f"future_move_pips_{horizon}")
                long_net = finite_series(frame, f"future_long_net_pips_{horizon}")
                short_net = finite_series(frame, f"future_short_net_pips_{horizon}")
                median_scale, spike_scale = pair_scales[horizon]
                median_scale = max(median_scale, 1e-9)
                for block, start, end in blocks:
                    chosen = selected[
                        (frame.index[selected] >= start)
                        & (frame.index[selected] + pd.Timedelta(minutes=horizon) < end)
                    ]
                    if len(chosen) == 0:
                        continue
                    chosen_direction = directions[chosen]
                    net = np.where(chosen_direction > 0, long_net[chosen], short_net[chosen])
                    raw_move = move[chosen]
                    valid = np.isfinite(net) & np.isfinite(raw_move)
                    chosen = chosen[valid]
                    chosen_direction = chosen_direction[valid]
                    net = net[valid]
                    raw_move = raw_move[valid]
                    if len(chosen) == 0:
                        continue
                    net_units = net / median_scale
                    wins = net > 0
                    correct = chosen_direction * raw_move > 0
                    spikes = np.abs(raw_move) >= spike_scale
                    pair_rows.append(
                        {
                            "pattern": spec.name,
                            "horizon_minutes": horizon,
                            "block": block,
                            "instrument": path.stem,
                            "pair_group": group,
                            "trigger_count": int(len(chosen)),
                            "net_units_sum": float(net_units.sum()),
                            "wins": int(wins.sum()),
                            "direction_correct": int(correct.sum()),
                            "spikes": int(spikes.sum()),
                        }
                    )
                    bucket_ns = horizon * 60 * 1_000_000_000
                    buckets = times_ns[chosen] // bucket_ns
                    for bucket in np.unique(buckets):
                        bucket_values = net_units[buckets == bucket]
                        _update_accumulator(
                            cluster_acc,
                            (spec.name, horizon, block, int(bucket)),
                            (len(bucket_values), float(bucket_values.sum())),
                        )
                    sessions = np.asarray([session_name(frame.index[index]) for index in chosen])
                    for session in np.unique(sessions):
                        values = net_units[sessions == session]
                        _update_accumulator(
                            session_acc,
                            (spec.name, horizon, block, str(session)),
                            (len(values), float(values.sum()), int((values > 0).sum())),
                        )
                    _update_accumulator(
                        group_acc,
                        (spec.name, horizon, block, group),
                        (len(net_units), float(net_units.sum()), int(wins.sum())),
                    )
        print(f"[evaluate] {number:02d}/{len(paths)} {path.stem}", flush=True)

    block_rows = aggregate_rows(pair_rows, cluster_acc, baseline_acc)
    phases = phase_metrics(block_rows, pair_rows, cluster_acc)
    candidates = summarize_candidates(block_rows, phases)
    session_rows = [
        {
            "pattern": key[0], "horizon_minutes": key[1], "block": key[2],
            "session": key[3], "trigger_count": int(value[0]),
            "mean_net_units": value[1] / value[0], "win_rate": value[2] / value[0],
        }
        for key, value in session_acc.items() if value[0]
    ]
    group_rows = [
        {
            "pattern": key[0], "horizon_minutes": key[1], "block": key[2],
            "pair_group": key[3], "trigger_count": int(value[0]),
            "mean_net_units": value[1] / value[0], "win_rate": value[2] / value[0],
        }
        for key, value in group_acc.items() if value[0]
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_csv(output_dir / "inventory.csv", inventory)
    atomic_json(output_dir / "thresholds.json", thresholds_by_pair)
    atomic_csv(output_dir / "pair_metrics.csv", pair_rows)
    atomic_csv(output_dir / "block_metrics.csv", block_rows)
    atomic_csv(output_dir / "phase_metrics.csv", phases)
    atomic_csv(output_dir / "session_metrics.csv", session_rows)
    atomic_csv(output_dir / "pair_group_metrics.csv", group_rows)
    atomic_csv(output_dir / "candidate_summary.csv", candidates)
    report = render_report(inventory, candidates, block_rows, thresholds_by_pair, discovery_end)
    atomic_text(output_dir / "PATTERN_RESEARCH_REPORT_20260802.md", report)
    summary = {
        "generated_utc": utc_now(),
        "research_only": True,
        "execution_wiring": False,
        "pair_count": len(paths),
        "source_rows": sum(row["rows"] for row in inventory),
        "discovery_end": discovery_end.isoformat(),
        "evaluation_blocks": [name for name, _start, _end in blocks],
        "pattern_count": len(PATTERNS),
        "horizons_minutes": list(HORIZONS),
        "combinations_tested": len(candidates),
        "replicated_profitable_count": sum(bool(row["replicated_profitable"]) for row in candidates),
        "best_candidates": candidates[:12],
    }
    atomic_json(output_dir / "summary.json", summary)
    print(f"[done] {output_dir}", flush=True)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-dir", default=str(DEFAULT_FEATURE_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--discovery-end", default="2025-01-01")
    parser.add_argument("--decision-minutes", type=int, default=15)
    parser.add_argument("--discovery-stride", type=int, default=1)
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument(
        "--pair-group",
        choices=("all", "major_cross", "usd_related", "exotic_or_regional"),
        default="all",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    run(parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
