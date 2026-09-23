#!/usr/bin/env python3
"""Research-only directional-change continuation/reversal audit.

This audit reconstructs an intrinsic-time state from *past* one-minute price
moves and evaluates a small preregistered set of continuation/reversal rules
against realised, after-spread long/short outcomes.  The current row's forward
one-minute move is never used to form its state.  There is no execution adapter.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import zlib
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np

try:
    from oanda_multihorizon_panel_model import origin_epoch
except ModuleNotFoundError:  # pragma: no cover - package import fallback
    from trad.oanda_multihorizon_panel_model import origin_epoch


HORIZONS = {"M5": 300, "M15": 900}
ARM_NAMES = (
    "dc_regime_continuation",
    "dc_regime_reversal",
    "fresh_change_continuation",
    "mature_overshoot_reversal",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


@dataclass
class DirectionalChangeDataset:
    instruments: np.ndarray
    epochs: np.ndarray
    forward_m1_pips: np.ndarray
    long_net: np.ndarray
    short_net: np.ndarray
    spread_pips: np.ndarray

    @property
    def rows(self) -> int:
        return int(len(self.epochs))


def load_dataset(
    database: Path,
    horizon_sec: int,
    max_rows: int = 120_000,
) -> DirectionalChangeDataset:
    uri = f"file:{Path(database).resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    connection.execute("PRAGMA query_only=ON")
    rows = connection.execute(
        """
        SELECT s.instrument, s.origin_time, s.features_zlib,
               minute.signed_move_pips,
               target.long_net_pips, target.short_net_pips
        FROM outcomes target INDEXED BY idx_combo_horizon
        JOIN snapshots s ON s.snapshot_id=target.snapshot_id
        JOIN outcomes minute
          ON minute.snapshot_id=target.snapshot_id AND minute.horizon_sec=60
        WHERE target.horizon_sec=?
        ORDER BY target.row_id DESC
        LIMIT ?
        """,
        (int(horizon_sec), int(max_rows)),
    ).fetchall()
    connection.close()
    parsed: list[tuple[str, float, float, float, float, float]] = []
    for instrument, origin_time, payload, m1_move, long_net, short_net in rows:
        epoch = origin_epoch(origin_time)
        if epoch is None:
            continue
        spread = math.nan
        try:
            features = json.loads(zlib.decompress(payload).decode("utf-8"))
            atr = float(features.get("atr_m1_pips"))
            ratio = float(features.get("live_spread_atr"))
            candidate = atr * ratio
            if math.isfinite(candidate) and candidate >= 0.0:
                spread = candidate
        except (TypeError, ValueError, zlib.error, json.JSONDecodeError):
            pass
        parsed.append(
            (
                str(instrument or ""),
                epoch,
                finite(m1_move),
                finite(long_net),
                finite(short_net),
                spread,
            )
        )
    parsed.sort(key=lambda row: (row[1], row[0]))
    return DirectionalChangeDataset(
        instruments=np.asarray([row[0] for row in parsed], dtype=object),
        epochs=np.asarray([row[1] for row in parsed], dtype=np.float64),
        forward_m1_pips=np.asarray([row[2] for row in parsed], dtype=np.float64),
        long_net=np.asarray([row[3] for row in parsed], dtype=np.float64),
        short_net=np.asarray([row[4] for row in parsed], dtype=np.float64),
        spread_pips=np.asarray([row[5] for row in parsed], dtype=np.float64),
    )


def directional_change_states(
    dataset: DirectionalChangeDataset,
    lookback: int = 60,
    threshold_multiple: float = 3.0,
    minimum_threshold_pips: float = 0.5,
) -> dict[str, np.ndarray]:
    """Build adaptive directional-change state using only completed past moves."""
    mode = np.zeros(dataset.rows, dtype=np.int8)
    event_age = np.full(dataset.rows, math.nan, dtype=np.float64)
    overshoot = np.full(dataset.rows, math.nan, dtype=np.float64)
    threshold_out = np.full(dataset.rows, math.nan, dtype=np.float64)
    warm = np.zeros(dataset.rows, dtype=bool)
    by_instrument: dict[str, list[int]] = {}
    for index, instrument in enumerate(dataset.instruments):
        by_instrument.setdefault(str(instrument), []).append(index)

    for indices in by_instrument.values():
        history: deque[float] = deque(maxlen=max(10, int(lookback)))
        cumulative = 0.0
        state = 0
        extreme = 0.0
        event_level = 0.0
        age = 0
        previous_forward: float | None = None
        previous_epoch: float | None = None
        for index in indices:
            epoch = float(dataset.epochs[index])
            gap = epoch - previous_epoch if previous_epoch is not None else math.inf
            if previous_forward is not None and 0.0 < gap <= 120.0:
                cumulative += previous_forward
                history.append(abs(previous_forward))
            elif previous_epoch is not None:
                history.clear()
                cumulative = 0.0
                state = 0
                extreme = 0.0
                event_level = 0.0
                age = 0

            if len(history) >= max(10, lookback // 2):
                threshold = max(
                    float(minimum_threshold_pips),
                    float(threshold_multiple) * statistics.median(history),
                )
                warm[index] = True
                if state == 0:
                    if cumulative >= threshold:
                        state = 1
                        extreme = cumulative
                        event_level = cumulative
                        age = 0
                    elif cumulative <= -threshold:
                        state = -1
                        extreme = cumulative
                        event_level = cumulative
                        age = 0
                elif state > 0:
                    extreme = max(extreme, cumulative)
                    if extreme - cumulative >= threshold:
                        state = -1
                        extreme = cumulative
                        event_level = cumulative
                        age = 0
                else:
                    extreme = min(extreme, cumulative)
                    if cumulative - extreme >= threshold:
                        state = 1
                        extreme = cumulative
                        event_level = cumulative
                        age = 0
                if state:
                    signed_overshoot = (
                        cumulative - event_level
                        if state > 0
                        else event_level - cumulative
                    )
                    mode[index] = state
                    event_age[index] = float(age)
                    overshoot[index] = signed_overshoot / threshold
                    age += 1
                threshold_out[index] = threshold

            # This forward move becomes available only to the next timestamp.
            previous_forward = float(dataset.forward_m1_pips[index])
            previous_epoch = epoch

    return {
        "mode": mode,
        "event_age_minutes": event_age,
        "overshoot_ratio": overshoot,
        "threshold_pips": threshold_out,
        "warm": warm,
    }


def arm_directions(states: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    mode = np.asarray(states["mode"], dtype=np.int8)
    age = np.asarray(states["event_age_minutes"], dtype=np.float64)
    overshoot = np.asarray(states["overshoot_ratio"], dtype=np.float64)
    active = mode != 0
    return {
        "dc_regime_continuation": np.where(active, mode, 0).astype(np.int8),
        "dc_regime_reversal": np.where(active, -mode, 0).astype(np.int8),
        "fresh_change_continuation": np.where(active & (age <= 5.0), mode, 0).astype(
            np.int8
        ),
        "mature_overshoot_reversal": np.where(
            active & (age >= 10.0) & (overshoot >= 1.0), -mode, 0
        ).astype(np.int8),
    }


def chronological_splits(
    epochs: np.ndarray,
    horizon_sec: int,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    unique = np.unique(epochs)
    if len(unique) < 30:
        raise ValueError("at least 30 timestamps are required")
    selection_start = float(unique[int(len(unique) * 0.70)])
    holdout_start = float(unique[int(len(unique) * 0.85)])
    selection = np.flatnonzero(
        (epochs >= selection_start) & (epochs < holdout_start - horizon_sec)
    )
    holdout = np.flatnonzero(epochs >= holdout_start)
    if not len(selection) or not len(holdout):
        raise ValueError("empty purged split")
    return selection, holdout, {
        "selection_start": datetime.fromtimestamp(
            selection_start, timezone.utc
        ).isoformat(),
        "holdout_start": datetime.fromtimestamp(holdout_start, timezone.utc).isoformat(),
        "horizon_purge_sec": int(horizon_sec),
        "selection_rows": int(len(selection)),
        "holdout_rows": int(len(holdout)),
    }


def direction_metrics(
    dataset: DirectionalChangeDataset,
    directions: np.ndarray,
    indices: np.ndarray,
    horizon_sec: int,
) -> dict[str, Any]:
    selected = indices[np.asarray(directions[indices]) != 0]
    if not len(selected):
        return {
            "signals": 0,
            "independent_time_blocks": 0,
            "average_net_pips": 0.0,
            "win_rate": 0.0,
            "block_ci95_lower_pips": 0.0,
        }
    chosen_direction = directions[selected]
    realised = np.where(
        chosen_direction > 0,
        dataset.long_net[selected],
        dataset.short_net[selected],
    )
    blocks = np.floor(dataset.epochs[selected] / max(1, horizon_sec)).astype(np.int64)
    block_means = np.asarray(
        [np.mean(realised[blocks == block]) for block in np.unique(blocks)],
        dtype=np.float64,
    )
    block_mean = float(np.mean(block_means))
    lower = block_mean
    if len(block_means) > 1:
        lower -= 1.96 * float(
            np.std(block_means, ddof=1) / math.sqrt(len(block_means))
        )
    without_largest = realised
    if len(realised) > 1:
        without_largest = np.delete(realised, int(np.argmax(np.abs(realised))))
    return {
        "signals": int(len(realised)),
        "coverage": round(len(realised) / max(1, len(indices)), 6),
        "independent_time_blocks": int(len(block_means)),
        "average_net_pips": round(float(np.mean(realised)), 6),
        "median_net_pips": round(float(np.median(realised)), 6),
        "average_without_largest_abs_pips": round(
            float(np.mean(without_largest)), 6
        ),
        "win_rate": round(float(np.mean(realised > 0.0)), 6),
        "long_fraction": round(float(np.mean(chosen_direction > 0)), 6),
        "block_mean_net_pips": round(block_mean, 6),
        "block_ci95_lower_pips": round(lower, 6),
    }


def cost_bucket_diagnostics(
    dataset: DirectionalChangeDataset,
    directions: np.ndarray,
    indices: np.ndarray,
    horizon_sec: int,
) -> dict[str, dict[str, Any]]:
    spread = dataset.spread_pips
    masks = {
        "liquid_le_3_pips": np.isfinite(spread) & (spread <= 3.0),
        "medium_gt_3_le_10_pips": (
            np.isfinite(spread) & (spread > 3.0) & (spread <= 10.0)
        ),
        "wide_gt_10_pips": np.isfinite(spread) & (spread > 10.0),
        "spread_unknown": ~np.isfinite(spread),
    }
    return {
        name: {
            "population_rows": int(np.sum(mask[indices])),
            **direction_metrics(dataset, directions, indices[mask[indices]], horizon_sec),
        }
        for name, mask in masks.items()
    }


def run_horizon(
    database: Path,
    horizon_sec: int,
    max_rows: int,
) -> dict[str, Any]:
    dataset = load_dataset(database, horizon_sec, max_rows=max_rows)
    states = directional_change_states(dataset)
    directions = arm_directions(states)
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
    selected_holdout = holdout_reports[selected_arm]
    checks = {
        "selection_positive_ci95_lower": (
            selection_reports[selected_arm]["block_ci95_lower_pips"] > 0.0
        ),
        "holdout_positive_average": selected_holdout["average_net_pips"] > 0.0,
        "holdout_positive_ci95_lower": (
            selected_holdout["block_ci95_lower_pips"] > 0.0
        ),
        "holdout_minimum_20_blocks": (
            selected_holdout["independent_time_blocks"] >= 20
        ),
        "holdout_robust_without_largest": (
            selected_holdout["average_without_largest_abs_pips"] > 0.0
        ),
    }
    return {
        "rows": dataset.rows,
        "horizon_sec": int(horizon_sec),
        "state_contract": {
            "past_move_shift_rows": 1,
            "gap_reset_sec": 120,
            "rolling_abs_move_lookback": 60,
            "threshold_multiple": 3.0,
            "minimum_threshold_pips": 0.5,
        },
        "warm_rows": int(np.sum(states["warm"])),
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
        except (OSError, sqlite3.Error, ValueError) as exc:
            failures[name] = f"{type(exc).__name__}: {exc}"
    payload = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "model": "adaptive_directional_change_intrinsic_time_v1",
        "database": str(Path(database).resolve()),
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "execution_adapter": False,
        "holdout_used_for_selection": False,
        "horizons": reports,
        "failures": failures,
    }
    output = Path(output)
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
    "DirectionalChangeDataset",
    "arm_directions",
    "chronological_splits",
    "direction_metrics",
    "directional_change_states",
    "load_dataset",
    "run_audit",
    "run_horizon",
]
