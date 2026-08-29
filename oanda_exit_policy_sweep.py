#!/usr/bin/env python3
"""Walk-forward S5 replay for fixed and trailing Forex exit policies.

The replay uses executable OANDA bid/ask paths. Buys enter at ask and exit on
bid; sells enter at bid and exit on ask. Stops take precedence when a stop and
target occur in the same S5 candle, and trailing stops only tighten after an
observed candle close so the replay does not use an intrabar high with hindsight.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import random
import time
from typing import Any, Iterable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from oanda_statistical_validation import multiple_testing_metrics
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_statistical_validation import multiple_testing_metrics


ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_EVENTS = (
    TRAINING_ROOT
    / "reports"
    / "strategy_lab_current92_s5_liquid20_30d_step5_allcycles_20260714_events.jsonl"
)
DEFAULT_CANDLES = TRAINING_ROOT / "candles_s5_bam"
DEFAULT_OUTPUT = TRAINING_ROOT / "reports" / "exit_policy_s5_walkforward_v1.json"
SIGNAL_MARKER = b'"event": "shadow_signal"'


def parse_int_csv(value: str) -> list[int]:
    values = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("values must be positive integers")
    return values


def timestamp_ns(value: str) -> int:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1_000_000_000)


def pip_size(instrument: str) -> float:
    quote = str(instrument).split("_")[-1].upper()
    return 0.01 if quote == "JPY" else 0.0001


def _round(value: float | np.floating[Any] | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    return round(float(value), digits)


def reservoir_signals(path: Path, maximum: int, seed: int) -> tuple[list[dict[str, Any]], int]:
    """Uniformly sample accepted signals without loading the JSONL ledger."""

    rng = random.Random(seed)
    sample: list[dict[str, Any]] = []
    seen = 0
    with path.open("rb", buffering=8 * 1024 * 1024) as source:
        for line in source:
            if SIGNAL_MARKER not in line:
                continue
            row = json.loads(line)
            if row.get("event") != "shadow_signal":
                continue
            direction = str(row.get("direction") or "").lower()
            instrument = str(row.get("instrument") or "")
            entry_bid = float(row.get("entry_bid") or 0.0)
            entry_ask = float(row.get("entry_ask") or 0.0)
            entry_time = str(row.get("entry_time") or row.get("time") or "")
            if (
                direction not in {"buy", "sell"}
                or not instrument
                or entry_bid <= 0.0
                or entry_ask <= entry_bid
                or not entry_time
            ):
                continue
            record = {
                "id": str(row.get("id") or ""),
                "instrument": instrument,
                "direction": direction,
                "family": str(row.get("family") or "unknown"),
                "lane_id": str(row.get("lane_id") or "unknown"),
                "entry_bid": entry_bid,
                "entry_ask": entry_ask,
                "entry_time": entry_time,
                "entry_ns": timestamp_ns(entry_time),
            }
            seen += 1
            if len(sample) < maximum:
                sample.append(record)
                continue
            position = rng.randrange(seen)
            if position < maximum:
                sample[position] = record
    sample.sort(key=lambda row: (row["entry_ns"], row["id"]))
    return sample, seen


def _column_numpy(table: pa.Table, name: str) -> np.ndarray:
    return table.column(name).combine_chunks().to_numpy(zero_copy_only=False)


def build_executable_paths(
    records: list[dict[str, Any]], candles_dir: Path, maximum_horizon_sec: int
) -> tuple[list[dict[str, Any]], dict[str, np.ndarray], dict[str, Any]]:
    """Build spread-paid signed P/L paths from future S5 bid/ask candles."""

    maximum_rows = int(math.ceil(maximum_horizon_sec / 5.0)) + 16
    count = len(records)
    favorable = np.full((count, maximum_rows), np.nan, dtype=np.float32)
    adverse = np.full((count, maximum_rows), np.nan, dtype=np.float32)
    close = np.full((count, maximum_rows), np.nan, dtype=np.float32)
    elapsed = np.full((count, maximum_rows), np.nan, dtype=np.float32)
    spread = np.full(count, np.nan, dtype=np.float32)
    usable = np.zeros(count, dtype=bool)
    truncated = 0
    missing_pairs: list[str] = []

    pair_positions: dict[str, list[int]] = {}
    for position, record in enumerate(records):
        pair_positions.setdefault(record["instrument"], []).append(position)

    needed_columns = [
        "dt",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_high",
        "ask_low",
        "ask_close",
    ]
    horizon_ns = int(maximum_horizon_sec * 1_000_000_000)
    for instrument, positions in sorted(pair_positions.items()):
        source = candles_dir / f"{instrument}_S5.parquet"
        if not source.exists():
            missing_pairs.append(instrument)
            continue
        table = pq.read_table(source, columns=needed_columns)
        times = table.column("dt").combine_chunks().cast(pa.int64()).to_numpy(
            zero_copy_only=False
        )
        columns = {name: _column_numpy(table, name).astype(np.float64) for name in needed_columns[1:]}
        if len(times) > 1 and np.any(times[1:] < times[:-1]):
            order = np.argsort(times, kind="stable")
            times = times[order]
            columns = {name: values[order] for name, values in columns.items()}
        pip = pip_size(instrument)
        for position in positions:
            record = records[position]
            start = int(np.searchsorted(times, record["entry_ns"], side="right"))
            end = int(
                np.searchsorted(times, record["entry_ns"] + horizon_ns, side="right")
            )
            available = max(0, end - start)
            if available <= 0:
                continue
            rows = min(available, maximum_rows)
            if available > maximum_rows:
                truncated += 1
            selection = slice(start, start + rows)
            elapsed[position, :rows] = (
                times[selection].astype(np.float64) - float(record["entry_ns"])
            ) / 1_000_000_000.0
            if record["direction"] == "buy":
                entry = record["entry_ask"]
                favorable[position, :rows] = (columns["bid_high"][selection] - entry) / pip
                adverse[position, :rows] = (columns["bid_low"][selection] - entry) / pip
                close[position, :rows] = (columns["bid_close"][selection] - entry) / pip
            else:
                entry = record["entry_bid"]
                favorable[position, :rows] = (entry - columns["ask_low"][selection]) / pip
                adverse[position, :rows] = (entry - columns["ask_high"][selection]) / pip
                close[position, :rows] = (entry - columns["ask_close"][selection]) / pip
            spread[position] = (record["entry_ask"] - record["entry_bid"]) / pip
            usable[position] = True

    kept = np.flatnonzero(usable)
    kept_records = [records[int(position)] for position in kept]
    paths = {
        "favorable": favorable[kept].astype(np.float64),
        "adverse": adverse[kept].astype(np.float64),
        "close": close[kept].astype(np.float64),
        "elapsed": elapsed[kept].astype(np.float64),
        "spread": spread[kept].astype(np.float64),
    }
    diagnostics = {
        "requested_records": count,
        "usable_records": len(kept_records),
        "missing_pairs": missing_pairs,
        "truncated_paths": truncated,
        "maximum_rows_per_path": maximum_rows,
    }
    return kept_records, paths, diagnostics


def policy_grid() -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = [{"id": "endpoint", "kind": "endpoint"}]
    for stop in (1.5, 2.0, 3.0, 4.0, 6.0, 8.0):
        for target in (0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0):
            policies.append(
                {
                    "id": f"fixed_s{stop:g}_t{target:g}",
                    "kind": "fixed",
                    "stop_pips": stop,
                    "target_pips": target,
                }
            )
    trailing_shapes = (
        (0.75, 0.5, 0.0),
        (1.0, 0.5, 0.1),
        (1.0, 0.75, 0.0),
        (1.5, 1.0, 0.2),
        (2.0, 1.0, 0.2),
        (2.0, 1.5, 0.2),
        (2.0, 2.0, 0.2),
        (3.0, 1.5, 0.5),
        (3.0, 2.0, 0.5),
        (3.0, 3.0, 0.5),
    )
    for stop in (2.0, 4.0, 6.0):
        for activation, distance, floor in trailing_shapes:
            for spread_multiple in (0.0, 1.5):
                policies.append(
                    {
                        "id": (
                            f"trail_s{stop:g}_a{activation:g}_d{distance:g}_"
                            f"f{floor:g}_m{spread_multiple:g}"
                        ),
                        "kind": "trailing",
                        "stop_pips": stop,
                        "activation_pips": activation,
                        "trail_distance_pips": distance,
                        "floor_pips": floor,
                        "spread_multiple": spread_multiple,
                        "step_pips": 0.25,
                    }
                )
    policies.append(
        {
            "id": "trail_deployed_reference",
            "kind": "trailing",
            "stop_pips": 4.0,
            "activation_pips": 2.0,
            "trail_distance_pips": 2.0,
            "floor_pips": 0.2,
            "spread_multiple": 1.5,
            "step_pips": 0.25,
            "diagnostic_note": "Approximation of the current runtime profit lock with a 4-pip initial stop.",
            }
        )
    for stop in (2.0, 4.0, 6.0):
        for fraction in (0.25, 0.50, 0.75):
            policies.append(
                {
                    "id": f"time_s{stop:g}_f{fraction:g}",
                    "kind": "time_stop",
                    "stop_pips": stop,
                    "horizon_fraction": fraction,
                }
            )
    return policies


def simulate_policy(
    paths: dict[str, np.ndarray], policy: dict[str, Any], horizon_sec: int
) -> tuple[np.ndarray, np.ndarray]:
    """Simulate one policy across every path without intrabar lookahead."""

    close = paths["close"]
    favorable = paths["favorable"]
    adverse = paths["adverse"]
    elapsed = paths["elapsed"]
    spread = paths["spread"]
    policy_horizon_sec = float(horizon_sec)
    if policy.get("kind") == "time_stop":
        policy_horizon_sec *= max(
            0.05,
            min(1.0, float(policy.get("horizon_fraction") or 1.0)),
        )
    valid = (
        np.isfinite(close)
        & np.isfinite(elapsed)
        & (elapsed > 0.0)
        & (elapsed <= policy_horizon_sec)
    )
    count = close.shape[0]
    values = np.full(count, np.nan, dtype=np.float64)
    hold_seconds = np.full(count, np.nan, dtype=np.float64)
    active = np.any(valid, axis=1)
    last_close = np.full(count, np.nan, dtype=np.float64)
    last_elapsed = np.full(count, np.nan, dtype=np.float64)

    stop = policy.get("stop_pips")
    stop_level = (
        -np.maximum(float(stop), spread + 0.5)
        if stop is not None
        else np.full(count, -np.inf)
    )
    target = policy.get("target_pips")
    trailing = policy.get("kind") == "trailing"
    market_buffer = np.maximum(
        0.5, spread * max(0.0, float(policy.get("spread_multiple") or 0.0))
    )

    for column in range(close.shape[1]):
        bar = active & valid[:, column]
        if not np.any(bar):
            continue
        last_close[bar] = close[bar, column]
        last_elapsed[bar] = elapsed[bar, column]

        hit_stop = bar & np.isfinite(stop_level) & (adverse[:, column] <= stop_level)
        if np.any(hit_stop):
            values[hit_stop] = stop_level[hit_stop]
            hold_seconds[hit_stop] = elapsed[hit_stop, column]
            active[hit_stop] = False

        if target is not None:
            hit_target = (
                active
                & valid[:, column]
                & (favorable[:, column] >= float(target))
            )
            if np.any(hit_target):
                values[hit_target] = float(target)
                hold_seconds[hit_target] = elapsed[hit_target, column]
                active[hit_target] = False

        if trailing:
            survivors = active & valid[:, column]
            if not np.any(survivors):
                continue
            current = close[:, column]
            floor = max(0.0, float(policy.get("floor_pips") or 0.0))
            trigger = max(0.0, float(policy.get("activation_pips") or 0.0))
            distance = max(0.0, float(policy.get("trail_distance_pips") or 0.0))
            step = max(0.0, float(policy.get("step_pips") or 0.0))
            eligible = survivors & (current >= np.maximum(trigger, floor + market_buffer))
            desired = np.minimum(current - market_buffer, np.maximum(floor, current - distance))
            tighten = eligible & (desired >= floor) & (desired > stop_level + step)
            stop_level[tighten] = desired[tighten]

    values[active] = last_close[active]
    hold_seconds[active] = last_elapsed[active]
    return values, hold_seconds


def metrics(
    values: np.ndarray, hold_seconds: np.ndarray, positions: Iterable[int]
) -> dict[str, Any]:
    selected = np.asarray(list(positions), dtype=np.int64)
    if selected.size == 0:
        return {"n": 0}
    selected_values = values[selected]
    selected_holds = hold_seconds[selected]
    valid = np.isfinite(selected_values) & np.isfinite(selected_holds)
    selected_values = selected_values[valid]
    selected_holds = selected_holds[valid]
    count = int(selected_values.size)
    if count == 0:
        return {"n": 0}
    average = float(np.mean(selected_values))
    deviation = float(np.std(selected_values, ddof=1)) if count > 1 else 0.0
    lower = average - 1.96 * deviation / math.sqrt(count)
    wins = selected_values[selected_values > 0.0]
    losses = selected_values[selected_values < 0.0]
    gross_profit = float(np.sum(wins))
    gross_loss = float(-np.sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0.0 else None
    equity = np.cumsum(selected_values)
    peaks = np.maximum.accumulate(np.concatenate(([0.0], equity)))
    max_drawdown = float(np.max(peaks[1:] - equity)) if equity.size else 0.0
    exposure_hours = float(np.sum(selected_holds)) / 3600.0
    return {
        "n": count,
        "average_net_pips": _round(average),
        "median_net_pips": _round(float(np.median(selected_values))),
        "total_net_pips": _round(float(np.sum(selected_values))),
        "win_rate_pct": _round(100.0 * float(np.mean(selected_values > 0.0)), 3),
        "lower_95_mean_pips": _round(lower),
        "standard_deviation_pips": _round(deviation),
        "profit_factor": _round(profit_factor),
        "max_drawdown_pips": _round(max_drawdown),
        "average_hold_sec": _round(float(np.mean(selected_holds)), 3),
        "pips_per_exposure_hour": _round(
            float(np.sum(selected_values)) / exposure_hours if exposure_hours > 0.0 else None
        ),
    }


def _rank_key(record: dict[str, Any], metric_name: str = "train") -> tuple[float, ...]:
    result = record[metric_name]
    lower = result.get("lower_95_mean_pips")
    average = result.get("average_net_pips")
    factor = result.get("profit_factor")
    drawdown = result.get("max_drawdown_pips")
    hold = result.get("average_hold_sec")
    return (
        float(lower) if lower is not None else -math.inf,
        float(average) if average is not None else -math.inf,
        float(factor) if factor is not None else 1_000_000.0,
        -float(drawdown) if drawdown is not None else -math.inf,
        -float(hold) if hold is not None else -math.inf,
    )


def evaluate_scope(
    policies: list[dict[str, Any]],
    simulations: dict[str, tuple[np.ndarray, np.ndarray]],
    positions: list[int],
    minimum_samples: int,
    *,
    records: list[dict[str, Any]] | None = None,
    horizon_sec: int = 300,
) -> dict[str, Any] | None:
    if len(positions) < minimum_samples:
        return None
    split = max(1, min(len(positions) - 1, int(len(positions) * 0.70)))
    development_positions = positions[:split]
    holdout_positions = positions[split:]
    if records and holdout_positions:
        holdout_start_ns = int(records[holdout_positions[0]].get("entry_ns") or 0)
        if holdout_start_ns > 0:
            purge_ns = max(1, int(horizon_sec)) * 1_000_000_000
            development_positions = [
                position
                for position in development_positions
                if int(records[position].get("entry_ns") or 0) + purge_ns
                < holdout_start_ns
            ]
    fold_bounds = ((0.40, 0.50), (0.50, 0.60), (0.60, 1.00))
    selection_positions: list[int] = []
    for train_fraction, validation_fraction in fold_bounds:
        train_end = int(len(development_positions) * train_fraction)
        validation_end = int(len(development_positions) * validation_fraction)
        if train_end >= 10 and validation_end - train_end >= 5:
            selection_positions.extend(development_positions[train_end:validation_end])
    if not selection_positions:
        inner_split = max(1, int(len(development_positions) * 0.70))
        selection_positions = development_positions[inner_split:]
    policy_trial_count = max(1, len(policies))

    def independent_policy_values(values: np.ndarray) -> list[float]:
        if not records:
            return [float(values[position]) for position in selection_positions]
        buckets: dict[int, list[float]] = {}
        for position in selection_positions:
            value = float(values[position])
            entry_ns = int(records[position].get("entry_ns") or 0)
            if not math.isfinite(value) or entry_ns <= 0:
                continue
            bucket = entry_ns // (max(1, int(horizon_sec)) * 1_000_000_000)
            buckets.setdefault(int(bucket), []).append(value)
        return [float(np.mean(buckets[key])) for key in sorted(buckets)]

    candidates: list[dict[str, Any]] = []
    for policy in policies:
        values, holds = simulations[policy["id"]]
        selection_adjustment = multiple_testing_metrics(
            independent_policy_values(values),
            trial_count=policy_trial_count,
        )
        candidates.append(
            {
                "policy": policy,
                "train": metrics(values, holds, selection_positions),
                "selection_walkforward": {
                    **metrics(values, holds, selection_positions),
                    "multiple_testing": selection_adjustment,
                },
                "development": metrics(values, holds, development_positions),
                "holdout": metrics(values, holds, holdout_positions),
                "all": metrics(values, holds, positions),
            }
        )
    candidates.sort(
        key=lambda row: (
            float(
                (row["selection_walkforward"].get("multiple_testing") or {}).get(
                    "selection_adjusted_lower_mean_pips"
                )
                or -math.inf
            ),
            float(
                (row["selection_walkforward"].get("multiple_testing") or {}).get(
                    "deflated_sharpe_probability"
                )
                or 0.0
            ),
            *_rank_key(row),
        ),
        reverse=True,
    )
    selected = candidates[0]
    selected_adjustment = selected["selection_walkforward"]["multiple_testing"]
    selected_holdout = selected["holdout"]
    blockers: list[str] = []
    if (
        selected_adjustment.get("selection_adjusted_lower_mean_pips") is None
        or float(selected_adjustment["selection_adjusted_lower_mean_pips"]) <= 0.0
    ):
        blockers.append("multiple_testing_net_confidence")
    if float(selected_adjustment.get("deflated_sharpe_probability") or 0.0) < 0.90:
        blockers.append("deflated_sharpe_probability")
    if float(selected_holdout.get("average_net_pips") or 0.0) <= 0.0:
        blockers.append("outer_holdout_net_edge")
    if float(selected_holdout.get("lower_95_mean_pips") or -math.inf) <= 0.0:
        blockers.append("outer_holdout_net_confidence")
    if float(selected_holdout.get("profit_factor") or 0.0) < 1.05:
        blockers.append("outer_holdout_profit_factor")
    selected["eligible"] = not blockers
    selected["blocked_by"] = blockers
    holdout_oracle = max(candidates, key=lambda row: _rank_key(row, "holdout"))
    by_id = {row["policy"]["id"]: row for row in candidates}
    return {
        "n": len(positions),
        "split": (
            "purged three-fold expanding inner walk-forward on oldest 70% / "
            "newest 30% untouched outer holdout"
        ),
        "policy_trial_count": policy_trial_count,
        "inner_fold_count": 3,
        "purge_horizon_sec": int(horizon_sec),
        "selected_on_training": selected,
        "selected_on_inner_walkforward": selected,
        "endpoint_baseline": by_id["endpoint"],
        "deployed_reference": by_id["trail_deployed_reference"],
        "top_training_candidates": candidates[:10],
        "holdout_oracle_diagnostic_only": {
            "warning": "Selected on holdout and therefore not valid for deployment.",
            **holdout_oracle,
        },
    }


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--candles-dir", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--horizons-sec", type=parse_int_csv, default=parse_int_csv("60,180,300"))
    parser.add_argument("--max-signals", type=int, default=20_000)
    parser.add_argument("--minimum-samples", type=int, default=500)
    parser.add_argument("--family-minimum-samples", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20260717)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_signals < 100:
        raise SystemExit("--max-signals must be at least 100")
    started = time.time()
    sampled, total_signals = reservoir_signals(args.events, args.max_signals, args.seed)
    records, paths, path_diagnostics = build_executable_paths(
        sampled, args.candles_dir, max(args.horizons_sec)
    )
    if len(records) < args.minimum_samples:
        raise SystemExit(
            f"only {len(records)} playable paths; need at least {args.minimum_samples}"
        )
    policies = policy_grid()
    all_positions = list(range(len(records)))
    family_positions: dict[str, list[int]] = {}
    for position, record in enumerate(records):
        family_positions.setdefault(record["family"], []).append(position)

    horizons: dict[str, Any] = {}
    summary: list[dict[str, Any]] = []
    for horizon in args.horizons_sec:
        simulations = {
            policy["id"]: simulate_policy(paths, policy, horizon) for policy in policies
        }
        overall = evaluate_scope(
            policies,
            simulations,
            all_positions,
            args.minimum_samples,
            records=records,
            horizon_sec=horizon,
        )
        assert overall is not None
        families = {
            family: result
            for family, positions in sorted(family_positions.items())
            if (
                result := evaluate_scope(
                    policies,
                    simulations,
                    positions,
                    args.family_minimum_samples,
                    records=records,
                    horizon_sec=horizon,
                )
            )
            is not None
        }
        horizons[str(horizon)] = {"overall": overall, "families": families}
        selected = overall["selected_on_training"]
        summary.append(
            {
                "horizon_sec": horizon,
                "selected_policy": selected["policy"]["id"],
                "eligible": bool(selected.get("eligible")),
                "blocked_by": list(selected.get("blocked_by") or []),
                "train_average_net_pips": selected["train"].get("average_net_pips"),
                "holdout_average_net_pips": selected["holdout"].get("average_net_pips"),
                "holdout_win_rate_pct": selected["holdout"].get("win_rate_pct"),
                "holdout_pips_per_exposure_hour": selected["holdout"].get(
                    "pips_per_exposure_hour"
                ),
            }
        )

    report = {
        "schema_version": "exit_policy_s5_walkforward_v2",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "elapsed_sec": _round(time.time() - started, 3),
        "inputs": {
            "events": str(args.events),
            "candles_dir": str(args.candles_dir),
            "event_type": "shadow_signal",
            "sample_method": "deterministic uniform reservoir; chronological sort after sampling",
            "sample_seed": args.seed,
            "accepted_signals_seen": total_signals,
            "sampled_signals": len(sampled),
            "playable_signals": len(records),
            "horizons_sec": args.horizons_sec,
            "policy_count": len(policies),
        },
        "path_diagnostics": path_diagnostics,
        "methodology": {
            "prices": "buy ask->bid; sell bid->ask; spread paid in every path",
            "same_bar_rule": "stop before target",
            "initial_stop_rule": "configured stop is widened to at least entry spread + 0.5 pip",
            "trailing_rule": "test existing stop first; tighten from S5 close for next candle",
            "selection": (
                "purged three-fold expanding inner walk-forward, ranked by "
                "multiple-testing-adjusted lower mean and DSR-style probability"
            ),
            "validation": (
                "newest 30% is untouched until the inner-walk-forward policy "
                "is selected; policy eligibility requires positive outer evidence"
            ),
            "trial_control": (
                "Bonferroni family-wise lower mean plus DSR-style expected-maximum "
                "Sharpe correction across every exit policy"
            ),
            "limitations": [
                "S5 candles cannot establish tick-level ordering inside a candle.",
                "Stop fills use the stop price and do not add gap/slippage beyond recorded spread.",
                "Signals overlap; pips per exposure hour is not an account-equity simulation.",
                "The deployed reference uses a fixed 4-pip initial stop as an approximation.",
            ],
        },
        "summary": summary,
        "horizons": horizons,
    }
    atomic_write_json(args.output, report)
    print(json.dumps({"output": str(args.output), **report["inputs"], "summary": summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
