#!/usr/bin/env python3
"""Find large all-pair FX spikes and rank interpretable pre-spike rules.

This is research-only. It consumes the existing all-68 feature parquet files
under ``data/all68_weekly_move_study/features`` and writes a standalone report.

Large spike definition:
- pair/horizon-specific absolute future move >= the training-period quantile,
- move >= a minimum ATR-scaled size,
- move/spread >= a minimum value threshold.

Rules are simple conjunctions of train-derived feature/session/pair-class
conditions. They are scored out of sample by first-trigger, cooldown-adjusted
movement in the predicted direction.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_FEATURE_DIR = ROOT / "data" / "all68_weekly_move_study" / "features"
DEFAULT_OUTPUT_DIR = (
    ROOT / "data" / "all68_weekly_move_study" / "large_spike_ruleset_search"
)

HORIZONS = (15, 30, 60, 120, 240)
BASE_FEATURE_COLUMNS = (
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
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
    "strength_gap_15",
    "strength_gap_60",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
)
DERIVED_FEATURE_COLUMNS = (
    "is_asia_session",
    "is_london_session",
    "is_new_york_session",
    "is_london_ny_overlap",
    "is_rollover_hour",
    "pair_group_usd_related",
    "pair_group_non_usd_cross",
    "pair_group_non_usd_exotic",
    "is_jpy_cross",
    "is_exotic_pair",
)
EXOTIC_CURRENCIES = {
    "CNH",
    "CZK",
    "DKK",
    "HKD",
    "HUF",
    "MXN",
    "NOK",
    "PLN",
    "SEK",
    "SGD",
    "THB",
    "TRY",
    "ZAR",
}


@dataclass(frozen=True)
class Condition:
    name: str
    column: str
    op: str
    value: float

    def mask(self, frame: pd.DataFrame) -> np.ndarray:
        values = pd.to_numeric(frame[self.column], errors="coerce").to_numpy(
            dtype=float
        )
        if self.op == ">=":
            return np.isfinite(values) & (values >= self.value)
        if self.op == "<=":
            return np.isfinite(values) & (values <= self.value)
        if self.op == "==":
            return np.isfinite(values) & (values == self.value)
        raise ValueError(f"unsupported condition operator: {self.op}")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    horizon: int
    direction: str
    condition_names: tuple[str, ...]

    @property
    def description(self) -> str:
        return " AND ".join(self.condition_names)


def parse_utc(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def pair_parts(pair: str) -> tuple[str, str]:
    base, quote = pair.split("_", 1)
    return base, quote


def pair_group(pair: str) -> str:
    base, quote = pair_parts(pair)
    if "USD" in (base, quote):
        return "usd_related"
    if base in EXOTIC_CURRENCIES or quote in EXOTIC_CURRENCIES:
        return "non_usd_exotic"
    return "non_usd_cross"


def read_pair(path: Path, horizons: Iterable[int]) -> pd.DataFrame:
    columns = [
        *BASE_FEATURE_COLUMNS,
        "close",
        "atr240_pips",
        "spread_pips",
    ]
    for horizon in horizons:
        columns.extend(
            [
                f"future_move_pips_{horizon}",
                f"future_long_net_pips_{horizon}",
                f"future_short_net_pips_{horizon}",
            ]
        )
    frame = pd.read_parquet(path, columns=columns)
    frame.index = pd.to_datetime(frame.index, utc=True)
    return frame.replace([np.inf, -np.inf], np.nan)


def add_derived_features(frame: pd.DataFrame, pair: str) -> pd.DataFrame:
    out = frame.copy()
    index = pd.DatetimeIndex(out.index)
    hours = index.hour + index.minute / 60.0
    group = pair_group(pair)
    base, quote = pair_parts(pair)
    out["is_asia_session"] = ((hours >= 22.0) | (hours < 7.0)).astype(float)
    out["is_london_session"] = ((hours >= 7.0) & (hours < 16.0)).astype(float)
    out["is_new_york_session"] = ((hours >= 12.0) & (hours < 21.0)).astype(float)
    out["is_london_ny_overlap"] = ((hours >= 12.0) & (hours < 16.0)).astype(float)
    out["is_rollover_hour"] = ((hours >= 21.0) & (hours < 22.0)).astype(float)
    out["pair_group_usd_related"] = float(group == "usd_related")
    out["pair_group_non_usd_cross"] = float(group == "non_usd_cross")
    out["pair_group_non_usd_exotic"] = float(group == "non_usd_exotic")
    out["is_jpy_cross"] = float("JPY" in (base, quote))
    out["is_exotic_pair"] = float(base in EXOTIC_CURRENCIES or quote in EXOTIC_CURRENCIES)
    return out


def load_paths(feature_dir: Path) -> dict[str, Path]:
    paths = {
        path.stem: path
        for path in sorted(feature_dir.glob("*.parquet"))
        if path.is_file()
    }
    if not paths:
        raise FileNotFoundError(f"no parquet features found under {feature_dir}")
    return paths


def compute_thresholds(
    paths: dict[str, Path],
    horizons: Iterable[int],
    split_date: pd.Timestamp,
    spike_quantile: float,
) -> dict[tuple[str, int], float]:
    thresholds: dict[tuple[str, int], float] = {}
    columns = [f"future_move_pips_{horizon}" for horizon in horizons]
    for number, (pair, path) in enumerate(paths.items(), 1):
        print(f"[thresholds] {number}/{len(paths)} {pair}", flush=True)
        frame = pd.read_parquet(path, columns=columns)
        frame.index = pd.to_datetime(frame.index, utc=True)
        train = frame.loc[frame.index < split_date]
        for horizon in horizons:
            series = train[f"future_move_pips_{horizon}"].abs().dropna()
            if len(series) < 1_000:
                series = frame[f"future_move_pips_{horizon}"].abs().dropna()
            threshold = float(series.quantile(spike_quantile)) if len(series) else math.nan
            if not math.isfinite(threshold) or threshold <= 0:
                continue
            thresholds[(pair, int(horizon))] = threshold
    return thresholds


def collect_quantile_sample(
    paths: dict[str, Path],
    horizons: Iterable[int],
    split_date: pd.Timestamp,
    max_rows_per_pair: int,
) -> pd.DataFrame:
    samples: list[pd.DataFrame] = []
    for number, (pair, path) in enumerate(paths.items(), 1):
        print(f"[sample] {number}/{len(paths)} {pair}", flush=True)
        frame = read_pair(path, horizons)
        frame = add_derived_features(frame, pair)
        train = frame.loc[frame.index < split_date, [*BASE_FEATURE_COLUMNS, *DERIVED_FEATURE_COLUMNS]]
        train = train.dropna(how="all")
        if train.empty:
            continue
        step = max(1, len(train) // max_rows_per_pair)
        samples.append(train.iloc[::step].head(max_rows_per_pair))
    if not samples:
        raise RuntimeError("no training rows available for condition quantiles")
    return pd.concat(samples, axis=0, ignore_index=True)


def build_conditions(sample: pd.DataFrame) -> list[Condition]:
    conditions: list[Condition] = []
    quantiles = (0.05, 0.10, 0.20, 0.80, 0.90, 0.95)
    for column in BASE_FEATURE_COLUMNS:
        series = pd.to_numeric(sample[column], errors="coerce").replace(
            [np.inf, -np.inf], np.nan
        )
        if series.dropna().nunique() < 8:
            continue
        values = series.quantile(quantiles).to_dict()
        for quantile in (0.05, 0.10, 0.20):
            bound = safe_float(values.get(quantile), math.nan)
            if math.isfinite(bound):
                label = f"{column}<=q{int(quantile * 100):02d}({bound:.4g})"
                conditions.append(Condition(label, column, "<=", bound))
        for quantile in (0.80, 0.90, 0.95):
            bound = safe_float(values.get(quantile), math.nan)
            if math.isfinite(bound):
                label = f"{column}>=q{int(quantile * 100):02d}({bound:.4g})"
                conditions.append(Condition(label, column, ">=", bound))
    for column in DERIVED_FEATURE_COLUMNS:
        if column == "is_rollover_hour":
            conditions.append(Condition(f"{column}==0", column, "==", 0.0))
        conditions.append(Condition(f"{column}==1", column, "==", 1.0))
    return conditions


def valid_large_spike_arrays(
    frame: pd.DataFrame,
    pair: str,
    horizon: int,
    threshold: float,
    min_atr_units: float,
    min_move_to_spread: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    move = pd.to_numeric(frame[f"future_move_pips_{horizon}"], errors="coerce").to_numpy(
        dtype=float
    )
    atr = pd.to_numeric(frame["atr240_pips"], errors="coerce").to_numpy(dtype=float)
    spread = pd.to_numeric(frame["spread_pips"], errors="coerce").to_numpy(dtype=float)
    expected = atr * math.sqrt(horizon / 5.0)
    move_atr_units = np.divide(
        np.abs(move),
        expected,
        out=np.full_like(move, np.nan, dtype=float),
        where=np.isfinite(expected) & (expected > 0),
    )
    move_to_spread = np.divide(
        np.abs(move),
        np.clip(spread, 0.1, None),
        out=np.full_like(move, np.nan, dtype=float),
        where=np.isfinite(spread),
    )
    large = (
        np.isfinite(move)
        & (np.abs(move) >= threshold)
        & np.isfinite(move_atr_units)
        & (move_atr_units >= min_atr_units)
        & np.isfinite(move_to_spread)
        & (move_to_spread >= min_move_to_spread)
    )
    return large, move_atr_units, move_to_spread


def collect_large_spikes(
    paths: dict[str, Path],
    thresholds: dict[tuple[str, int], float],
    horizons: Iterable[int],
    min_atr_units: float,
    min_move_to_spread: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    feature_output = [
        "momentum_5_atr",
        "momentum_15_atr",
        "momentum_30_atr",
        "momentum_60_atr",
        "acceleration_15_atr",
        "range_position_60_centered",
        "range_position_240_centered",
        "atr15_to_atr240",
        "compression_30",
        "spread_ratio_60",
        "volume_z_30",
        "strength_gap_15",
        "strength_gap_60",
        "strength_gap_rank_15",
        "strength_gap_rank_60",
    ]
    for number, (pair, path) in enumerate(paths.items(), 1):
        print(f"[spikes] {number}/{len(paths)} {pair}", flush=True)
        frame = read_pair(path, horizons)
        frame = add_derived_features(frame, pair)
        for horizon in horizons:
            threshold = thresholds.get((pair, int(horizon)))
            if threshold is None:
                continue
            large, move_atr_units, move_to_spread = valid_large_spike_arrays(
                frame, pair, int(horizon), threshold, min_atr_units, min_move_to_spread
            )
            move = pd.to_numeric(frame[f"future_move_pips_{horizon}"], errors="coerce").to_numpy(
                dtype=float
            )
            positions = np.flatnonzero(large)
            if len(positions) == 0:
                continue
            candidates = []
            for pos in positions:
                candidates.append(
                    {
                        "position": int(pos),
                        "start_utc": frame.index[pos],
                        "end_utc": frame.index[pos] + pd.Timedelta(minutes=int(horizon)),
                        "severity_vs_threshold": abs(move[pos]) / threshold,
                        "move_atr_units": move_atr_units[pos],
                        "abs_move_pips": abs(move[pos]),
                    }
                )
            selected = cluster_event_positions(candidates, int(horizon))
            for item in selected:
                pos = int(item["position"])
                record: dict[str, Any] = {
                    "instrument": pair,
                    "group": pair_group(pair),
                    "start_utc": frame.index[pos].isoformat(),
                    "end_utc": (
                        frame.index[pos] + pd.Timedelta(minutes=int(horizon))
                    ).isoformat(),
                    "horizon_minutes": int(horizon),
                    "direction": "LONG" if move[pos] > 0 else "SHORT",
                    "move_pips": float(move[pos]),
                    "abs_move_pips": float(abs(move[pos])),
                    "threshold_pips": float(threshold),
                    "severity_vs_threshold": float(abs(move[pos]) / threshold),
                    "move_atr_units": float(move_atr_units[pos]),
                    "move_to_spread": float(move_to_spread[pos]),
                    "spread_pips": safe_float(frame["spread_pips"].iloc[pos], math.nan),
                }
                for column in feature_output:
                    record[column] = safe_float(frame[column].iloc[pos], math.nan)
                rows.append(record)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        ["start_utc", "instrument", "horizon_minutes"]
    )


def cluster_event_positions(candidates: list[dict[str, Any]], horizon: int) -> list[dict[str, Any]]:
    if not candidates:
        return []
    ordered = sorted(candidates, key=lambda item: item["start_utc"])
    max_gap = pd.Timedelta(minutes=0.70 * horizon)
    clusters: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = [ordered[0]]
    prior_time = ordered[0]["start_utc"]
    for item in ordered[1:]:
        if item["start_utc"] - prior_time <= max_gap:
            current.append(item)
        else:
            clusters.append(current)
            current = [item]
        prior_time = item["start_utc"]
    clusters.append(current)
    return [
        max(
            cluster,
            key=lambda item: (
                item["severity_vs_threshold"],
                item["move_atr_units"],
                item["abs_move_pips"],
            ),
        )
        for cluster in clusters
    ]


def empty_base_stats() -> dict[str, float]:
    return {"rows": 0.0, "spike_rows": 0.0}


def empty_rule_stats() -> dict[str, float]:
    return {
        "selected_rows": 0.0,
        "selected_spike_rows": 0.0,
        "trigger_count": 0.0,
        "trigger_spikes": 0.0,
        "trigger_wins": 0.0,
        "trigger_net_pips": 0.0,
        "trigger_net_units": 0.0,
        "trigger_positive_units": 0.0,
        "trigger_spike_abs_units": 0.0,
    }


def update_row_stats(
    stats: dict[str, float],
    selected: np.ndarray,
    target: np.ndarray,
    outcome_units: np.ndarray,
) -> None:
    valid = selected & np.isfinite(outcome_units)
    stats["selected_rows"] += float(valid.sum())
    stats["selected_spike_rows"] += float((valid & target).sum())
    if valid.any():
        stats["trigger_net_units"] += float(np.nansum(outcome_units[valid]))


def first_trigger_positions(
    index: pd.DatetimeIndex,
    mask: np.ndarray,
    cooldown_minutes: int,
) -> np.ndarray:
    if len(mask) == 0:
        return np.array([], dtype=int)
    edge = mask.copy()
    edge[1:] = edge[1:] & ~mask[:-1]
    candidates = np.flatnonzero(edge)
    if len(candidates) == 0:
        return candidates
    selected: list[int] = []
    next_allowed: pd.Timestamp | None = None
    cooldown = pd.Timedelta(minutes=cooldown_minutes)
    for pos in candidates:
        timestamp = index[pos]
        if next_allowed is None or timestamp >= next_allowed:
            selected.append(int(pos))
            next_allowed = timestamp + cooldown
    return np.asarray(selected, dtype=int)


def update_trigger_stats(
    stats: dict[str, float],
    index: pd.DatetimeIndex,
    selected_rows: np.ndarray,
    target: np.ndarray,
    outcome_pips: np.ndarray,
    outcome_units: np.ndarray,
    abs_move_units: np.ndarray,
    cooldown_minutes: int,
) -> None:
    valid = selected_rows & np.isfinite(outcome_pips) & np.isfinite(outcome_units)
    stats["selected_rows"] += float(valid.sum())
    stats["selected_spike_rows"] += float((valid & target).sum())
    positions = first_trigger_positions(index, valid, cooldown_minutes)
    if len(positions) == 0:
        return
    trigger_target = target[positions]
    trigger_units = outcome_units[positions]
    trigger_pips = outcome_pips[positions]
    stats["trigger_count"] += float(len(positions))
    stats["trigger_spikes"] += float(trigger_target.sum())
    stats["trigger_wins"] += float((trigger_pips > 0).sum())
    stats["trigger_net_pips"] += float(np.nansum(trigger_pips))
    stats["trigger_net_units"] += float(np.nansum(trigger_units))
    stats["trigger_positive_units"] += float(np.nansum(np.clip(trigger_units, 0, None)))
    if trigger_target.any():
        stats["trigger_spike_abs_units"] += float(
            np.nansum(abs_move_units[positions][trigger_target])
        )


def score_single_conditions(
    paths: dict[str, Path],
    thresholds: dict[tuple[str, int], float],
    conditions: list[Condition],
    horizons: Iterable[int],
    split_date: pd.Timestamp,
    min_atr_units: float,
    min_move_to_spread: float,
) -> pd.DataFrame:
    base_stats: dict[tuple[int, str], dict[str, float]] = {}
    stats: dict[tuple[int, str, str], dict[str, float]] = {}
    for horizon in horizons:
        for direction in ("LONG", "SHORT"):
            base_stats[(int(horizon), direction)] = empty_base_stats()
            for condition in conditions:
                stats[(int(horizon), direction, condition.name)] = empty_rule_stats()

    for number, (pair, path) in enumerate(paths.items(), 1):
        print(f"[single-rules] {number}/{len(paths)} {pair}", flush=True)
        frame = add_derived_features(read_pair(path, horizons), pair)
        train_mask = np.asarray(frame.index < split_date, dtype=bool)
        condition_masks = {condition.name: condition.mask(frame) for condition in conditions}
        for horizon in horizons:
            threshold = thresholds.get((pair, int(horizon)))
            if threshold is None:
                continue
            large, _, _ = valid_large_spike_arrays(
                frame, pair, int(horizon), threshold, min_atr_units, min_move_to_spread
            )
            move = pd.to_numeric(frame[f"future_move_pips_{horizon}"], errors="coerce").to_numpy(
                dtype=float
            )
            for direction in ("LONG", "SHORT"):
                target = large & ((move > 0) if direction == "LONG" else (move < 0))
                outcome_col = (
                    f"future_long_net_pips_{horizon}"
                    if direction == "LONG"
                    else f"future_short_net_pips_{horizon}"
                )
                outcome = pd.to_numeric(frame[outcome_col], errors="coerce").to_numpy(
                    dtype=float
                )
                outcome_units = outcome / threshold
                valid = train_mask & np.isfinite(outcome_units)
                base = base_stats[(int(horizon), direction)]
                base["rows"] += float(valid.sum())
                base["spike_rows"] += float((valid & target).sum())
                for condition in conditions:
                    update_row_stats(
                        stats[(int(horizon), direction, condition.name)],
                        valid & condition_masks[condition.name],
                        target,
                        outcome_units,
                    )

    rows: list[dict[str, Any]] = []
    for (horizon, direction, condition_name), row in stats.items():
        base = base_stats[(horizon, direction)]
        base_rate = base["spike_rows"] / max(base["rows"], 1.0)
        selected_rate = row["selected_spike_rows"] / max(row["selected_rows"], 1.0)
        lift = selected_rate / max(base_rate, 1e-12)
        rows.append(
            {
                "horizon_minutes": horizon,
                "direction": direction,
                "condition": condition_name,
                "base_rows": int(base["rows"]),
                "base_spike_rows": int(base["spike_rows"]),
                "base_spike_rate": base_rate,
                "selected_rows": int(row["selected_rows"]),
                "selected_spike_rows": int(row["selected_spike_rows"]),
                "selected_spike_rate": selected_rate,
                "row_lift": lift,
                "row_net_units": row["trigger_net_units"],
                "single_score": lift
                * math.log1p(max(row["selected_spike_rows"], 0.0))
                * max(selected_rate, 1e-12),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["single_score", "row_lift", "selected_spike_rows"],
        ascending=[False, False, False],
    )


def choose_top_conditions(
    single_scores: pd.DataFrame,
    top_conditions: int,
    min_selected_spikes: int,
) -> dict[tuple[int, str], list[str]]:
    output: dict[tuple[int, str], list[str]] = {}
    for (horizon, direction), group in single_scores.groupby(
        ["horizon_minutes", "direction"], sort=False
    ):
        filtered = group[
            (group["selected_spike_rows"] >= min_selected_spikes)
            & (group["row_lift"] >= 1.0)
            & (group["selected_rows"] >= max(500, min_selected_spikes * 10))
        ]
        if filtered.empty:
            filtered = group[group["selected_rows"] >= 500]
        output[(int(horizon), str(direction))] = (
            filtered.sort_values(
                ["single_score", "row_lift", "selected_spike_rows"],
                ascending=[False, False, False],
            )["condition"]
            .drop_duplicates()
            .head(top_conditions)
            .tolist()
        )
    return output


def rule_is_consistent(condition_names: tuple[str, ...], condition_by_name: dict[str, Condition]) -> bool:
    bounds: dict[str, dict[str, float]] = {}
    equals: dict[str, float] = {}
    for name in condition_names:
        condition = condition_by_name[name]
        if condition.op == "==":
            prior = equals.get(condition.column)
            if prior is not None and prior != condition.value:
                return False
            equals[condition.column] = condition.value
            continue
        item = bounds.setdefault(condition.column, {"min": -math.inf, "max": math.inf})
        if condition.op == ">=":
            item["min"] = max(item["min"], condition.value)
        elif condition.op == "<=":
            item["max"] = min(item["max"], condition.value)
        if item["min"] > item["max"]:
            return False
    return True


def build_rules(
    top_conditions: dict[tuple[int, str], list[str]],
    condition_by_name: dict[str, Condition],
    max_rule_size: int,
) -> list[Rule]:
    rules: list[Rule] = []
    seen: set[str] = set()
    for (horizon, direction), names in sorted(top_conditions.items()):
        for size in range(1, max_rule_size + 1):
            for combo in itertools.combinations(names, size):
                combo = tuple(sorted(combo))
                if not rule_is_consistent(combo, condition_by_name):
                    continue
                rule_id = f"h{horizon}_{direction.lower()}__" + "__AND__".join(combo)
                if rule_id in seen:
                    continue
                seen.add(rule_id)
                rules.append(Rule(rule_id, int(horizon), direction, combo))
    return rules


def evaluate_rules(
    paths: dict[str, Path],
    thresholds: dict[tuple[str, int], float],
    conditions: list[Condition],
    rules: list[Rule],
    horizons: Iterable[int],
    split_date: pd.Timestamp,
    min_atr_units: float,
    min_move_to_spread: float,
) -> tuple[pd.DataFrame, dict[str, dict[str, float]], dict[tuple[int, str, str], dict[str, float]]]:
    condition_by_name = {condition.name: condition for condition in conditions}
    rules_by_context: dict[tuple[int, str], list[Rule]] = {}
    for rule in rules:
        rules_by_context.setdefault((rule.horizon, rule.direction), []).append(rule)

    base_stats: dict[tuple[int, str, str], dict[str, float]] = {}
    rule_stats: dict[tuple[str, str], dict[str, float]] = {}
    for horizon in horizons:
        for direction in ("LONG", "SHORT"):
            for segment in ("train", "validation"):
                base_stats[(int(horizon), direction, segment)] = empty_base_stats()
    for rule in rules:
        for segment in ("train", "validation"):
            rule_stats[(rule.rule_id, segment)] = empty_rule_stats()

    used_conditions = sorted({name for rule in rules for name in rule.condition_names})
    for number, (pair, path) in enumerate(paths.items(), 1):
        print(f"[evaluate-rules] {number}/{len(paths)} {pair}", flush=True)
        frame = add_derived_features(read_pair(path, horizons), pair)
        index = pd.DatetimeIndex(frame.index)
        train_mask = np.asarray(index < split_date, dtype=bool)
        validation_mask = ~train_mask
        condition_masks = {
            name: condition_by_name[name].mask(frame)
            for name in used_conditions
        }
        rule_masks: dict[str, np.ndarray] = {}
        for rule in rules:
            mask = np.ones(len(frame), dtype=bool)
            for name in rule.condition_names:
                mask &= condition_masks[name]
            rule_masks[rule.rule_id] = mask

        for horizon in horizons:
            threshold = thresholds.get((pair, int(horizon)))
            if threshold is None:
                continue
            large, _, _ = valid_large_spike_arrays(
                frame, pair, int(horizon), threshold, min_atr_units, min_move_to_spread
            )
            move = pd.to_numeric(frame[f"future_move_pips_{horizon}"], errors="coerce").to_numpy(
                dtype=float
            )
            abs_move_units = np.abs(move) / threshold
            for direction in ("LONG", "SHORT"):
                context_rules = rules_by_context.get((int(horizon), direction), [])
                if not context_rules:
                    continue
                target = large & ((move > 0) if direction == "LONG" else (move < 0))
                outcome_col = (
                    f"future_long_net_pips_{horizon}"
                    if direction == "LONG"
                    else f"future_short_net_pips_{horizon}"
                )
                outcome = pd.to_numeric(frame[outcome_col], errors="coerce").to_numpy(
                    dtype=float
                )
                outcome_units = outcome / threshold
                for segment, segment_mask in (
                    ("train", train_mask),
                    ("validation", validation_mask),
                ):
                    valid = segment_mask & np.isfinite(outcome_units)
                    base = base_stats[(int(horizon), direction, segment)]
                    base["rows"] += float(valid.sum())
                    base["spike_rows"] += float((valid & target).sum())
                    for rule in context_rules:
                        update_trigger_stats(
                            rule_stats[(rule.rule_id, segment)],
                            index,
                            valid & rule_masks[rule.rule_id],
                            target,
                            outcome,
                            outcome_units,
                            abs_move_units,
                            int(horizon),
                        )

    rows: list[dict[str, Any]] = []
    rule_by_id = {rule.rule_id: rule for rule in rules}
    for rule in rules:
        row: dict[str, Any] = {
            "rule_id": rule.rule_id,
            "horizon_minutes": rule.horizon,
            "direction": rule.direction,
            "rule": rule.description,
            "condition_count": len(rule.condition_names),
        }
        for segment in ("train", "validation"):
            stats = rule_stats[(rule.rule_id, segment)]
            base = base_stats[(rule.horizon, rule.direction, segment)]
            base_rate = base["spike_rows"] / max(base["rows"], 1.0)
            trigger_precision = stats["trigger_spikes"] / max(
                stats["trigger_count"], 1.0
            )
            trigger_lift = trigger_precision / max(base_rate, 1e-12)
            row.update(
                {
                    f"{segment}_base_rows": int(base["rows"]),
                    f"{segment}_base_spike_rows": int(base["spike_rows"]),
                    f"{segment}_base_spike_rate": base_rate,
                    f"{segment}_selected_rows": int(stats["selected_rows"]),
                    f"{segment}_selected_spike_rows": int(stats["selected_spike_rows"]),
                    f"{segment}_trigger_count": int(stats["trigger_count"]),
                    f"{segment}_trigger_spikes": int(stats["trigger_spikes"]),
                    f"{segment}_trigger_precision": trigger_precision,
                    f"{segment}_trigger_lift": trigger_lift,
                    f"{segment}_trigger_win_rate": stats["trigger_wins"]
                    / max(stats["trigger_count"], 1.0),
                    f"{segment}_trigger_net_pips": stats["trigger_net_pips"],
                    f"{segment}_trigger_net_units": stats["trigger_net_units"],
                    f"{segment}_trigger_mean_units": stats["trigger_net_units"]
                    / max(stats["trigger_count"], 1.0),
                    f"{segment}_trigger_positive_units": stats[
                        "trigger_positive_units"
                    ],
                    f"{segment}_trigger_spike_abs_units": stats[
                        "trigger_spike_abs_units"
                    ],
                }
            )
        eligible = (
            row["validation_trigger_count"] >= 100
            and row["validation_trigger_lift"] >= 1.15
            and row["validation_trigger_mean_units"] > 0
            and row["validation_trigger_spikes"] >= 5
        )
        row["eligible_positive_predictor"] = bool(eligible)
        row["rank_score"] = (
            row["validation_trigger_net_units"]
            if eligible
            else row["validation_trigger_spike_abs_units"]
            * max(row["validation_trigger_lift"], 0.0)
            * max(row["validation_trigger_precision"], 1e-9)
        )
        rows.append(row)
    results = pd.DataFrame(rows).sort_values(
        [
            "eligible_positive_predictor",
            "rank_score",
            "validation_trigger_net_units",
            "validation_trigger_spike_abs_units",
            "validation_trigger_lift",
        ],
        ascending=[False, False, False, False, False],
    )
    return results, {rule_id: rule.__dict__ for rule_id, rule in rule_by_id.items()}, base_stats


def collect_rule_triggers(
    paths: dict[str, Path],
    thresholds: dict[tuple[str, int], float],
    conditions: list[Condition],
    rule: Rule,
    horizons: Iterable[int],
    split_date: pd.Timestamp,
    min_atr_units: float,
    min_move_to_spread: float,
) -> pd.DataFrame:
    condition_by_name = {condition.name: condition for condition in conditions}
    rows: list[dict[str, Any]] = []
    for number, (pair, path) in enumerate(paths.items(), 1):
        print(f"[best-triggers] {number}/{len(paths)} {pair}", flush=True)
        threshold = thresholds.get((pair, rule.horizon))
        if threshold is None:
            continue
        frame = add_derived_features(read_pair(path, horizons), pair)
        index = pd.DatetimeIndex(frame.index)
        validation_mask = np.asarray(index >= split_date, dtype=bool)
        mask = validation_mask.copy()
        for name in rule.condition_names:
            mask &= condition_by_name[name].mask(frame)
        large, move_atr_units, move_to_spread = valid_large_spike_arrays(
            frame,
            pair,
            rule.horizon,
            threshold,
            min_atr_units,
            min_move_to_spread,
        )
        move = pd.to_numeric(frame[f"future_move_pips_{rule.horizon}"], errors="coerce").to_numpy(
            dtype=float
        )
        target = large & ((move > 0) if rule.direction == "LONG" else (move < 0))
        outcome_col = (
            f"future_long_net_pips_{rule.horizon}"
            if rule.direction == "LONG"
            else f"future_short_net_pips_{rule.horizon}"
        )
        outcome = pd.to_numeric(frame[outcome_col], errors="coerce").to_numpy(dtype=float)
        positions = first_trigger_positions(
            index,
            mask & np.isfinite(outcome),
            rule.horizon,
        )
        for pos in positions:
            rows.append(
                {
                    "instrument": pair,
                    "time_utc": index[pos].isoformat(),
                    "horizon_minutes": rule.horizon,
                    "direction": rule.direction,
                    "future_move_pips": float(move[pos]),
                    "net_pips_in_direction": float(outcome[pos]),
                    "net_threshold_units": float(outcome[pos] / threshold),
                    "is_large_spike": bool(target[pos]),
                    "threshold_pips": float(threshold),
                    "move_atr_units": float(move_atr_units[pos]),
                    "move_to_spread": float(move_to_spread[pos]),
                    "rule": rule.description,
                }
            )
    return pd.DataFrame(rows).sort_values("time_utc") if rows else pd.DataFrame()


def render_report(
    args: argparse.Namespace,
    paths: dict[str, Path],
    spikes: pd.DataFrame,
    single_scores: pd.DataFrame,
    results: pd.DataFrame,
    output_dir: Path,
) -> str:
    lines: list[str] = [
        "# All-68 Large Spike Ruleset Search",
        "",
        f"Generated from `{args.feature_dir}`.",
        f"Pairs scanned: `{len(paths)}`.",
        f"Split date: `{parse_utc(args.split_date).isoformat()}`.",
        (
            "Large spike filter: "
            f"q{args.spike_quantile:.3f} pair/horizon threshold, "
            f">={args.min_atr_units:.2f} ATR units, "
            f">={args.min_move_to_spread:.2f} move/spread."
        ),
        "",
        "## Spike Inventory",
        "",
        f"Deduplicated large spike events: `{len(spikes)}`.",
    ]
    if not spikes.empty:
        by_horizon = spikes.groupby("horizon_minutes").size().sort_index()
        by_direction = spikes.groupby("direction").size().sort_index()
        lines.append(
            "By horizon: "
            + ", ".join(f"{int(k)}m={int(v)}" for k, v in by_horizon.items())
            + "."
        )
        lines.append(
            "By direction: "
            + ", ".join(f"{k}={int(v)}" for k, v in by_direction.items())
            + "."
        )
        largest = spikes.loc[spikes["abs_move_pips"].idxmax()]
        lines.append(
            f"Largest move: `{largest.instrument}` {largest.direction} "
            f"{largest.abs_move_pips:.1f} pips."
        )
    lines += [
        "",
        "## Best Validated Rules",
        "",
        "| Rank | Horizon | Direction | Rule | Triggers | Spike precision | Lift | Win rate | Net q-units | Captured spike q-units |",
        "|---:|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, (_, row) in enumerate(results.head(15).iterrows(), 1):
        lines.append(
            f"| {rank} | {int(row.horizon_minutes)}m | {row.direction} | "
            f"{row.rule} | {int(row.validation_trigger_count)} | "
            f"{row.validation_trigger_precision:.2%} | "
            f"{row.validation_trigger_lift:.2f} | "
            f"{row.validation_trigger_win_rate:.2%} | "
            f"{row.validation_trigger_net_units:.2f} | "
            f"{row.validation_trigger_spike_abs_units:.2f} |"
        )
    best = results.iloc[0] if not results.empty else None
    if best is not None:
        lines += [
            "",
            "## Selected Ruleset",
            "",
            f"`{best.rule}`",
            "",
            (
                f"Validation: {int(best.validation_trigger_count)} first triggers, "
                f"{best.validation_trigger_precision:.2%} large-spike precision, "
                f"{best.validation_trigger_lift:.2f}x base spike rate, "
                f"{best.validation_trigger_win_rate:.2%} win rate, "
                f"{best.validation_trigger_net_units:.2f} q-threshold units net."
            ),
        ]
    lines += [
        "",
        "## Files",
        "",
        f"- `{output_dir / 'all_large_spikes.csv'}`",
        f"- `{output_dir / 'single_condition_scores.csv'}`",
        f"- `{output_dir / 'rule_search_results.csv'}`",
        f"- `{output_dir / 'best_rule_validation_triggers.csv'}`",
        f"- `{output_dir / 'summary.json'}`",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-dir", type=Path, default=DEFAULT_FEATURE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--split-date", default="2026-01-01T00:00:00Z")
    parser.add_argument("--horizons", nargs="+", type=int, default=list(HORIZONS))
    parser.add_argument("--spike-quantile", type=float, default=0.995)
    parser.add_argument("--min-atr-units", type=float, default=1.5)
    parser.add_argument("--min-move-to-spread", type=float, default=3.0)
    parser.add_argument("--max-sample-rows-per-pair", type=int, default=5_000)
    parser.add_argument("--top-conditions", type=int, default=10)
    parser.add_argument("--max-rule-size", type=int, default=2)
    parser.add_argument("--min-single-spikes", type=int, default=50)
    args = parser.parse_args()

    split_date = parse_utc(args.split_date)
    horizons = tuple(sorted({int(horizon) for horizon in args.horizons}))
    paths = load_paths(args.feature_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    thresholds = compute_thresholds(paths, horizons, split_date, args.spike_quantile)
    sample = collect_quantile_sample(
        paths, horizons, split_date, args.max_sample_rows_per_pair
    )
    conditions = build_conditions(sample)
    conditions_path = args.output_dir / "conditions.csv"
    pd.DataFrame([condition.__dict__ for condition in conditions]).to_csv(
        conditions_path, index=False
    )

    spikes = collect_large_spikes(
        paths,
        thresholds,
        horizons,
        args.min_atr_units,
        args.min_move_to_spread,
    )
    spikes.to_csv(args.output_dir / "all_large_spikes.csv", index=False)

    single_scores = score_single_conditions(
        paths,
        thresholds,
        conditions,
        horizons,
        split_date,
        args.min_atr_units,
        args.min_move_to_spread,
    )
    single_scores.to_csv(args.output_dir / "single_condition_scores.csv", index=False)

    top_conditions = choose_top_conditions(
        single_scores, args.top_conditions, args.min_single_spikes
    )
    condition_by_name = {condition.name: condition for condition in conditions}
    rules = build_rules(top_conditions, condition_by_name, args.max_rule_size)
    pd.DataFrame(
        [
            {
                "rule_id": rule.rule_id,
                "horizon_minutes": rule.horizon,
                "direction": rule.direction,
                "rule": rule.description,
                "condition_count": len(rule.condition_names),
            }
            for rule in rules
        ]
    ).to_csv(args.output_dir / "candidate_rules.csv", index=False)

    results, rule_payload, _ = evaluate_rules(
        paths,
        thresholds,
        conditions,
        rules,
        horizons,
        split_date,
        args.min_atr_units,
        args.min_move_to_spread,
    )
    results.to_csv(args.output_dir / "rule_search_results.csv", index=False)

    best_rule = None
    best_triggers = pd.DataFrame()
    if not results.empty:
        best_rule_payload = rule_payload[str(results.iloc[0]["rule_id"])]
        best_rule = Rule(
            rule_id=best_rule_payload["rule_id"],
            horizon=int(best_rule_payload["horizon"]),
            direction=str(best_rule_payload["direction"]),
            condition_names=tuple(best_rule_payload["condition_names"]),
        )
        best_triggers = collect_rule_triggers(
            paths,
            thresholds,
            conditions,
            best_rule,
            horizons,
            split_date,
            args.min_atr_units,
            args.min_move_to_spread,
        )
    best_triggers.to_csv(args.output_dir / "best_rule_validation_triggers.csv", index=False)

    summary = {
        "feature_dir": str(args.feature_dir),
        "output_dir": str(args.output_dir),
        "pairs": len(paths),
        "split_date": split_date.isoformat(),
        "horizons": list(horizons),
        "spike_quantile": args.spike_quantile,
        "min_atr_units": args.min_atr_units,
        "min_move_to_spread": args.min_move_to_spread,
        "large_spike_events": int(len(spikes)),
        "candidate_rules": int(len(rules)),
        "best_rule": results.head(1).to_dict("records")[0] if not results.empty else {},
        "best_rule_validation_triggers": int(len(best_triggers)),
        "top_conditions": {
            f"{horizon}_{direction}": names
            for (horizon, direction), names in top_conditions.items()
        },
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    report = render_report(args, paths, spikes, single_scores, results, args.output_dir)
    (args.output_dir / "report.md").write_text(report, encoding="utf-8")
    print(json.dumps(summary["best_rule"], indent=2), flush=True)
    print(f"[done] wrote {args.output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
