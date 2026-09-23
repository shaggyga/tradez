#!/usr/bin/env python3
"""Causal ordinal time-irreversibility gate for shadow FX research."""

from __future__ import annotations

import argparse
import itertools
import json
import math
from collections import deque
from pathlib import Path
from typing import Any, Iterable

import numpy as np

try:
    from oanda_directional_change_shadow import (
        HORIZONS,
        DirectionalChangeDataset,
        chronological_splits,
        cost_bucket_diagnostics,
        direction_metrics,
        directional_change_states,
        load_dataset,
        utc_now,
    )
except ModuleNotFoundError:  # pragma: no cover - package import fallback
    from trad.oanda_directional_change_shadow import (
        HORIZONS,
        DirectionalChangeDataset,
        chronological_splits,
        cost_bucket_diagnostics,
        direction_metrics,
        directional_change_states,
        load_dataset,
        utc_now,
    )


PATTERNS = tuple(itertools.permutations(range(3)))
PATTERN_INDEX = {pattern: index for index, pattern in enumerate(PATTERNS)}
ARM_NAMES = (
    "irreversible_continuation",
    "irreversible_reversal",
    "reversible_continuation",
    "reversible_reversal",
)


def ordinal_pattern(values: tuple[float, float, float]) -> tuple[int, int, int]:
    return tuple(sorted(range(3), key=lambda index: (values[index], index)))


def ordinal_irreversibility_states(
    dataset: DirectionalChangeDataset,
    window: int = 60,
) -> dict[str, np.ndarray]:
    """Measure forward/reversed ordinal-pattern distance from completed history."""
    score = np.full(dataset.rows, math.nan, dtype=np.float64)
    monotonic_bias = np.full(dataset.rows, math.nan, dtype=np.float64)
    warm = np.zeros(dataset.rows, dtype=bool)
    by_instrument: dict[str, list[int]] = {}
    for index, instrument in enumerate(dataset.instruments):
        by_instrument.setdefault(str(instrument), []).append(index)

    for indices in by_instrument.values():
        path: deque[float] = deque(maxlen=max(12, int(window)))
        cumulative = 0.0
        previous_forward: float | None = None
        previous_epoch: float | None = None
        for index in indices:
            epoch = float(dataset.epochs[index])
            gap = epoch - previous_epoch if previous_epoch is not None else math.inf
            if previous_forward is not None and 0.0 < gap <= 120.0:
                cumulative += previous_forward
                path.append(cumulative)
            elif previous_epoch is not None:
                path.clear()
                cumulative = 0.0
            if len(path) >= max(12, window // 2):
                counts = np.zeros(len(PATTERNS), dtype=np.float64)
                reverse_counts = np.zeros(len(PATTERNS), dtype=np.float64)
                values = list(path)
                for offset in range(len(values) - 2):
                    triple = (values[offset], values[offset + 1], values[offset + 2])
                    counts[PATTERN_INDEX[ordinal_pattern(triple)]] += 1.0
                    reverse_counts[
                        PATTERN_INDEX[ordinal_pattern(tuple(reversed(triple)))]
                    ] += 1.0
                total = float(np.sum(counts))
                if total > 0.0:
                    score[index] = 0.5 * float(
                        np.sum(np.abs(counts / total - reverse_counts / total))
                    )
                    monotonic_bias[index] = (
                        counts[PATTERN_INDEX[(0, 1, 2)]]
                        - counts[PATTERN_INDEX[(2, 1, 0)]]
                    ) / total
                    warm[index] = True
            # The current row's forward return enters only the next row's path.
            previous_forward = float(dataset.forward_m1_pips[index])
            previous_epoch = epoch
    return {
        "irreversibility_score": score,
        "monotonic_bias": monotonic_bias,
        "warm": warm,
    }


def gated_directions(
    dc_states: dict[str, np.ndarray],
    ordinal_states: dict[str, np.ndarray],
) -> dict[str, np.ndarray]:
    mode = np.asarray(dc_states["mode"], dtype=np.int8)
    score = np.asarray(ordinal_states["irreversibility_score"], dtype=np.float64)
    active = (mode != 0) & np.isfinite(score)
    high = active & (score >= 0.15)
    low = active & (score <= 0.05)
    return {
        "irreversible_continuation": np.where(high, mode, 0).astype(np.int8),
        "irreversible_reversal": np.where(high, -mode, 0).astype(np.int8),
        "reversible_continuation": np.where(low, mode, 0).astype(np.int8),
        "reversible_reversal": np.where(low, -mode, 0).astype(np.int8),
    }


def run_horizon(database: Path, horizon_sec: int, max_rows: int) -> dict[str, Any]:
    dataset = load_dataset(database, horizon_sec, max_rows=max_rows)
    dc_states = directional_change_states(dataset)
    ordinal_states = ordinal_irreversibility_states(dataset)
    directions = gated_directions(dc_states, ordinal_states)
    selection, holdout, split = chronological_splits(dataset.epochs, horizon_sec)
    selection_reports = {
        name: direction_metrics(dataset, values, selection, horizon_sec)
        for name, values in directions.items()
    }
    ranked = sorted(
        ARM_NAMES,
        key=lambda name: (
            selection_reports[name]["block_ci95_lower_pips"],
            selection_reports[name]["independent_time_blocks"],
        ),
        reverse=True,
    )
    selected_arm = ranked[0]
    holdout_reports = {
        name: direction_metrics(dataset, values, holdout, horizon_sec)
        for name, values in directions.items()
    }
    selected = holdout_reports[selected_arm]
    checks = {
        "selection_positive_ci95_lower": (
            selection_reports[selected_arm]["block_ci95_lower_pips"] > 0.0
        ),
        "holdout_positive_average": selected["average_net_pips"] > 0.0,
        "holdout_positive_ci95_lower": selected["block_ci95_lower_pips"] > 0.0,
        "holdout_minimum_20_blocks": selected["independent_time_blocks"] >= 20,
        "holdout_robust_without_largest": (
            selected["average_without_largest_abs_pips"] > 0.0
        ),
    }
    return {
        "rows": dataset.rows,
        "horizon_sec": int(horizon_sec),
        "transform_contract": {
            "past_move_shift_rows": 1,
            "ordinal_dimension": 3,
            "rolling_path_points": 60,
            "irreversible_threshold": 0.15,
            "reversible_threshold": 0.05,
            "thresholds_selected_on_data": False,
        },
        "warm_rows": int(np.sum(ordinal_states["warm"])),
        "split": split,
        "selection": selection_reports,
        "selected_arm": selected_arm,
        "untouched_holdout_all_arms": holdout_reports,
        "selected_arm_holdout_by_cost_bucket": cost_bucket_diagnostics(
            dataset, directions[selected_arm], holdout, horizon_sec
        ),
        "promotion_checks": checks,
        "promotion_ready": all(checks.values()),
        "shadow_decision": (
            "forward_observe_only" if all(checks.values()) else "reject_historical_candidate"
        ),
    }


def run_audit(
    database: Path,
    output: Path,
    horizons: Iterable[str] = HORIZONS,
    max_rows: int = 120_000,
) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    failures: dict[str, str] = {}
    for label in horizons:
        name = str(label).upper()
        if name not in HORIZONS:
            failures[name] = "unsupported_horizon"
            continue
        try:
            reports[name] = run_horizon(database, HORIZONS[name], max_rows)
        except (OSError, ValueError) as exc:
            failures[name] = f"{type(exc).__name__}: {exc}"
    payload = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "model": "ordinal_time_irreversibility_gate_v1",
        "database": str(Path(database).resolve()),
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "execution_adapter": False,
        "holdout_used_for_selection": False,
        "horizons": reports,
        "failures": failures,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--horizons", nargs="+", default=list(HORIZONS))
    parser.add_argument("--max-rows", type=int, default=120_000)
    args = parser.parse_args()
    payload = run_audit(
        args.database,
        args.output,
        horizons=args.horizons,
        max_rows=max(5_000, int(args.max_rows)),
    )
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "horizons": sorted(payload["horizons"]),
                "failures": payload["failures"],
            },
            indent=2,
        )
    )
    return 0 if payload["horizons"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARM_NAMES",
    "gated_directions",
    "ordinal_irreversibility_states",
    "ordinal_pattern",
    "run_audit",
    "run_horizon",
]
