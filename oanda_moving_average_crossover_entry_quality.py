#!/usr/bin/env python3
"""Evaluate moving-average crossovers as executable entry-timing events.

Signals and entries use the leakage-safe machinery in
``oanda_moving_average_crossover_sweep.py``. Instead of asking where price ends
at a fixed horizon, this study measures the executable path after entry:

- maximum favorable and adverse excursion;
- favorable excursion versus the opposite direction at the same timestamp;
- first touch of symmetric profit and loss barriers.

If both barriers are touched in the same M1 candle, the loss barrier wins. This
is conservative because M1 OHLC cannot establish intrabar ordering.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gc
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

import oanda_moving_average_crossover_sweep as crossover


ROOT = Path(__file__).resolve().parent
DEFAULT_REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "moving_average_crossover_entry_quality"
)
DEFAULT_PATH_WINDOWS_MIN = (5, 15, 30, 60)
DEFAULT_BARRIER_PIPS = (0.5, 1.0, 2.0, 3.0, 5.0)
EXTENDED_INPUT_COLUMNS = (
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "spread_pips",
)


@dataclass
class EntryPathBatch:
    valid: np.ndarray
    mfe_pips: np.ndarray
    mae_pips: np.ndarray
    reverse_mfe_pips: np.ndarray
    target_hits: np.ndarray
    stop_hits: np.ndarray
    first_touch_wins: np.ndarray
    first_touch_losses: np.ndarray


class EntryAggregateCube:
    """Pooled and pair-consistency statistics for each entry rule."""

    BASE_ARRAYS = (
        "n",
        "mfe_sum",
        "mae_sum",
        "reverse_mfe_sum",
        "ever_positive_n",
        "direction_dominant_n",
        "direction_pair_n",
        "direction_positive_pair_n",
    )
    BARRIER_ARRAYS = (
        "target_hit_n",
        "stop_hit_n",
        "first_touch_win_n",
        "first_touch_loss_n",
        "barrier_pair_n",
        "barrier_positive_pair_n",
    )

    def __init__(
        self,
        config_count: int,
        window_count: int,
        barrier_count: int,
    ) -> None:
        base_shape = (config_count, window_count, len(crossover.SEGMENTS))
        barrier_shape = (
            config_count,
            window_count,
            barrier_count,
            len(crossover.SEGMENTS),
        )
        self.n = np.zeros(base_shape, dtype=np.int64)
        self.mfe_sum = np.zeros(base_shape, dtype=np.float64)
        self.mae_sum = np.zeros(base_shape, dtype=np.float64)
        self.reverse_mfe_sum = np.zeros(base_shape, dtype=np.float64)
        self.ever_positive_n = np.zeros(base_shape, dtype=np.int64)
        self.direction_dominant_n = np.zeros(base_shape, dtype=np.int64)
        self.direction_pair_n = np.zeros(base_shape, dtype=np.int32)
        self.direction_positive_pair_n = np.zeros(base_shape, dtype=np.int32)
        self.target_hit_n = np.zeros(barrier_shape, dtype=np.int64)
        self.stop_hit_n = np.zeros(barrier_shape, dtype=np.int64)
        self.first_touch_win_n = np.zeros(barrier_shape, dtype=np.int64)
        self.first_touch_loss_n = np.zeros(barrier_shape, dtype=np.int64)
        self.barrier_pair_n = np.zeros(barrier_shape, dtype=np.int32)
        self.barrier_positive_pair_n = np.zeros(barrier_shape, dtype=np.int32)

    def update(
        self,
        config_id: int,
        window_index: int,
        segment_index: int,
        batch: EntryPathBatch,
        mask: np.ndarray,
        minimum_pair_events: int,
    ) -> None:
        finite = (
            mask
            & batch.valid[:, window_index]
            & np.isfinite(batch.mfe_pips[:, window_index])
            & np.isfinite(batch.mae_pips[:, window_index])
            & np.isfinite(batch.reverse_mfe_pips[:, window_index])
        )
        if not finite.any():
            return
        key = (config_id, window_index, segment_index)
        mfe = batch.mfe_pips[finite, window_index]
        mae = batch.mae_pips[finite, window_index]
        reverse_mfe = batch.reverse_mfe_pips[finite, window_index]
        count = int(mfe.size)
        self.n[key] += count
        self.mfe_sum[key] += float(mfe.sum())
        self.mae_sum[key] += float(mae.sum())
        self.reverse_mfe_sum[key] += float(reverse_mfe.sum())
        self.ever_positive_n[key] += int(np.count_nonzero(mfe > 0.0))
        self.direction_dominant_n[key] += int(np.count_nonzero(mfe > reverse_mfe))

        if count >= minimum_pair_events:
            self.direction_pair_n[key] += 1
            direction_edge = float(np.mean(mfe - reverse_mfe))
            dominance = float(np.mean(mfe > reverse_mfe))
            self.direction_positive_pair_n[key] += int(
                direction_edge > 0.0 and dominance > 0.50
            )

        for barrier_index in range(batch.first_touch_wins.shape[2]):
            barrier_key = (
                config_id,
                window_index,
                barrier_index,
                segment_index,
            )
            wins = batch.first_touch_wins[finite, window_index, barrier_index]
            losses = batch.first_touch_losses[finite, window_index, barrier_index]
            target_hits = batch.target_hits[finite, window_index, barrier_index]
            stop_hits = batch.stop_hits[finite, window_index, barrier_index]
            win_count = int(np.count_nonzero(wins))
            loss_count = int(np.count_nonzero(losses))
            self.target_hit_n[barrier_key] += int(np.count_nonzero(target_hits))
            self.stop_hit_n[barrier_key] += int(np.count_nonzero(stop_hits))
            self.first_touch_win_n[barrier_key] += win_count
            self.first_touch_loss_n[barrier_key] += loss_count
            if count >= minimum_pair_events and win_count + loss_count >= 3:
                self.barrier_pair_n[barrier_key] += 1
                self.barrier_positive_pair_n[barrier_key] += int(win_count > loss_count)

    def merge_payload(self, payload: dict[str, np.ndarray]) -> None:
        for name in self.BASE_ARRAYS + self.BARRIER_ARRAYS:
            getattr(self, name)[:] += payload[name]

    def payload(self) -> dict[str, np.ndarray]:
        return {
            name: getattr(self, name)
            for name in self.BASE_ARRAYS + self.BARRIER_ARRAYS
        }

    def frame(
        self,
        configurations: Sequence[crossover.Configuration],
        path_windows_min: Sequence[int],
        barrier_pips: Sequence[float],
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for config in configurations:
            for window_index, window_min in enumerate(path_windows_min):
                for barrier_index, barrier in enumerate(barrier_pips):
                    for segment_index, segment in enumerate(crossover.SEGMENTS):
                        base_key = (config.config_id, window_index, segment_index)
                        barrier_key = (
                            config.config_id,
                            window_index,
                            barrier_index,
                            segment_index,
                        )
                        count = int(self.n[base_key])
                        if count <= 0:
                            continue
                        wins = int(self.first_touch_win_n[barrier_key])
                        losses = int(self.first_touch_loss_n[barrier_key])
                        resolved = wins + losses
                        pair_count = int(self.barrier_pair_n[barrier_key])
                        mfe_mean = float(self.mfe_sum[base_key] / count)
                        mae_mean = float(self.mae_sum[base_key] / count)
                        reverse_mean = float(self.reverse_mfe_sum[base_key] / count)
                        row = config.as_dict()
                        row.update(
                            {
                                "path_window_min": int(window_min),
                                "barrier_pips": float(barrier),
                                "segment": segment,
                                "n": count,
                                "avg_mfe_pips": mfe_mean,
                                "avg_mae_pips": mae_mean,
                                "avg_reverse_mfe_pips": reverse_mean,
                                "mfe_advantage_pips": mfe_mean - reverse_mean,
                                "ever_positive_rate": float(
                                    self.ever_positive_n[base_key] / count
                                ),
                                "direction_dominance_rate": float(
                                    self.direction_dominant_n[base_key] / count
                                ),
                                "direction_pair_count": int(
                                    self.direction_pair_n[base_key]
                                ),
                                "direction_positive_pair_fraction": (
                                    float(
                                        self.direction_positive_pair_n[base_key]
                                        / self.direction_pair_n[base_key]
                                    )
                                    if self.direction_pair_n[base_key]
                                    else math.nan
                                ),
                                "target_hit_rate": float(
                                    self.target_hit_n[barrier_key] / count
                                ),
                                "stop_hit_rate": float(
                                    self.stop_hit_n[barrier_key] / count
                                ),
                                "resolved_rate": float(resolved / count),
                                "first_touch_win_rate": (
                                    float(wins / resolved) if resolved else math.nan
                                ),
                                "barrier_expectancy_per_signal_pips": float(
                                    barrier * (wins - losses) / count
                                ),
                                "barrier_pair_count": pair_count,
                                "barrier_positive_pair_fraction": (
                                    float(
                                        self.barrier_positive_pair_n[barrier_key]
                                        / pair_count
                                    )
                                    if pair_count
                                    else math.nan
                                ),
                            }
                        )
                        rows.append(row)
        return pd.DataFrame(rows)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_csv_floats(value: str) -> list[float]:
    values = [float(item.strip()) for item in value.split(",") if item.strip()]
    if not values or any(item <= 0.0 for item in values):
        raise argparse.ArgumentTypeError("values must be positive comma-separated numbers")
    return sorted(set(values))


def read_entry_frame(
    path: Path,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    exact_only: bool,
    attempts: int = 5,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            frame = pd.read_parquet(path, columns=list(EXTENDED_INPUT_COLUMNS))
            break
        except (OSError, PermissionError) as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                raise
            time.sleep(0.25 * (attempt + 1))
    else:
        raise RuntimeError(f"Unable to read {path}: {last_error}")

    frame["time"] = pd.to_datetime(frame["time"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["time", "open", "high", "low", "close"])
    frame = frame.set_index("time").sort_index()
    frame = frame[~frame.index.duplicated(keep="last")]
    if start is not None:
        frame = frame.loc[frame.index >= start]
    if end is not None:
        frame = frame.loc[frame.index <= end]
    for column in EXTENDED_INPUT_COLUMNS:
        if column != "time":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if exact_only:
        exact_columns = (
            "bid_open",
            "bid_high",
            "bid_low",
            "ask_open",
            "ask_high",
            "ask_low",
        )
        frame = frame.dropna(subset=list(exact_columns))
    if frame.empty:
        raise ValueError(f"No usable entry-path rows in {path}")
    return frame


def executable_price_arrays(
    frame: pd.DataFrame,
    pip_size: float,
    proxy_spread_pips: float,
) -> dict[str, np.ndarray]:
    spread = frame["spread_pips"].to_numpy(dtype=np.float64, copy=False)
    spread = np.where(
        np.isfinite(spread) & (spread > 0.0),
        spread,
        proxy_spread_pips,
    )
    half_spread_price = spread * pip_size * 0.5
    output: dict[str, np.ndarray] = {}
    for prefix, sign in (("bid", -1.0), ("ask", 1.0)):
        for field in ("open", "high", "low"):
            exact = frame[f"{prefix}_{field}"].to_numpy(
                dtype=np.float64,
                copy=False,
            )
            midpoint = frame[field].to_numpy(dtype=np.float64, copy=False)
            proxy = midpoint + sign * half_spread_price
            output[f"{prefix}_{field}"] = np.where(
                np.isfinite(exact),
                exact,
                proxy,
            )
    return output


def entry_path_outcomes(
    frame: pd.DataFrame,
    events: crossover.SignalEvents,
    path_windows_min: Sequence[int],
    barrier_pips: Sequence[float],
    pip_size: float,
    proxy_spread_pips: float,
    maximum_target_delay_minutes: int = 2,
) -> EntryPathBatch:
    count = int(events.entry_positions.size)
    window_count = len(path_windows_min)
    barrier_count = len(barrier_pips)
    shape = (count, window_count)
    barrier_shape = (count, window_count, barrier_count)
    if count == 0:
        return EntryPathBatch(
            valid=np.zeros(shape, dtype=bool),
            mfe_pips=np.full(shape, np.nan),
            mae_pips=np.full(shape, np.nan),
            reverse_mfe_pips=np.full(shape, np.nan),
            target_hits=np.zeros(barrier_shape, dtype=bool),
            stop_hits=np.zeros(barrier_shape, dtype=bool),
            first_touch_wins=np.zeros(barrier_shape, dtype=bool),
            first_touch_losses=np.zeros(barrier_shape, dtype=bool),
        )

    maximum_window = max(path_windows_min)
    offsets = np.arange(maximum_window, dtype=np.int64)
    raw_positions = events.entry_positions[:, None] + offsets[None, :]
    in_bounds = raw_positions < len(frame)
    path_positions = np.minimum(raw_positions, len(frame) - 1)
    time_ns = frame.index.asi8
    path_time_ns = time_ns[path_positions]
    entry_time_ns = time_ns[events.entry_positions]
    prices = executable_price_arrays(frame, pip_size, proxy_spread_pips)
    bid_open = prices["bid_open"][events.entry_positions]
    ask_open = prices["ask_open"][events.entry_positions]
    directions = events.directions
    long_mask = directions > 0

    bid_high = prices["bid_high"][path_positions]
    bid_low = prices["bid_low"][path_positions]
    ask_high = prices["ask_high"][path_positions]
    ask_low = prices["ask_low"][path_positions]
    favorable_path = np.where(
        long_mask[:, None],
        (bid_high - ask_open[:, None]) / pip_size,
        (bid_open[:, None] - ask_low) / pip_size,
    )
    adverse_path = np.where(
        long_mask[:, None],
        (ask_open[:, None] - bid_low) / pip_size,
        (ask_high - bid_open[:, None]) / pip_size,
    )
    reverse_favorable_path = np.where(
        long_mask[:, None],
        (bid_open[:, None] - ask_low) / pip_size,
        (bid_high - ask_open[:, None]) / pip_size,
    )

    valid = np.zeros(shape, dtype=bool)
    mfe = np.full(shape, np.nan, dtype=np.float64)
    mae = np.full(shape, np.nan, dtype=np.float64)
    reverse_mfe = np.full(shape, np.nan, dtype=np.float64)
    target_hits = np.zeros(barrier_shape, dtype=bool)
    stop_hits = np.zeros(barrier_shape, dtype=bool)
    first_touch_wins = np.zeros(barrier_shape, dtype=bool)
    first_touch_losses = np.zeros(barrier_shape, dtype=bool)

    for window_index, window_min in enumerate(path_windows_min):
        target_ns = entry_time_ns + int(window_min) * 60 * 1_000_000_000
        exit_positions = np.searchsorted(time_ns, target_ns, side="left")
        valid_window = exit_positions < len(frame)
        if valid_window.any():
            selected = np.flatnonzero(valid_window)
            delays = time_ns[exit_positions[selected]] - target_ns[selected]
            valid_window[selected] &= (
                (delays >= 0)
                & (
                    delays
                    <= maximum_target_delay_minutes * 60 * 1_000_000_000
                )
            )
        path_mask = (
            in_bounds
            & (path_time_ns >= entry_time_ns[:, None])
            & (path_time_ns < target_ns[:, None])
        )
        valid_window &= path_mask.any(axis=1)
        valid[:, window_index] = valid_window
        if not valid_window.any():
            continue

        favorable = np.where(path_mask, favorable_path, -np.inf)
        adverse = np.where(path_mask, adverse_path, -np.inf)
        reverse_favorable = np.where(
            path_mask,
            reverse_favorable_path,
            -np.inf,
        )
        mfe[:, window_index] = np.maximum(0.0, np.max(favorable, axis=1))
        mae[:, window_index] = np.maximum(0.0, np.max(adverse, axis=1))
        reverse_mfe[:, window_index] = np.maximum(
            0.0,
            np.max(reverse_favorable, axis=1),
        )
        mfe[~valid_window, window_index] = np.nan
        mae[~valid_window, window_index] = np.nan
        reverse_mfe[~valid_window, window_index] = np.nan

        for barrier_index, barrier in enumerate(barrier_pips):
            comparison_floor = barrier - 1e-9
            target_matrix = path_mask & (favorable_path >= comparison_floor)
            stop_matrix = path_mask & (adverse_path >= comparison_floor)
            target = target_matrix.any(axis=1)
            stop = stop_matrix.any(axis=1)
            target_index = np.where(
                target,
                np.argmax(target_matrix, axis=1),
                maximum_window + 1,
            )
            stop_index = np.where(
                stop,
                np.argmax(stop_matrix, axis=1),
                maximum_window + 1,
            )
            wins = valid_window & target & ((~stop) | (target_index < stop_index))
            losses = valid_window & stop & ((~target) | (stop_index <= target_index))
            target_hits[:, window_index, barrier_index] = valid_window & target
            stop_hits[:, window_index, barrier_index] = valid_window & stop
            first_touch_wins[:, window_index, barrier_index] = wins
            first_touch_losses[:, window_index, barrier_index] = losses

    return EntryPathBatch(
        valid=valid,
        mfe_pips=mfe,
        mae_pips=mae,
        reverse_mfe_pips=reverse_mfe,
        target_hits=target_hits,
        stop_hits=stop_hits,
        first_touch_wins=first_touch_wins,
        first_touch_losses=first_touch_losses,
    )


def evaluate_pair(
    instrument: str,
    frame: pd.DataFrame,
    configurations_by_timeframe: dict[str, list[crossover.Configuration]],
    path_windows_min: Sequence[int],
    barrier_pips: Sequence[float],
    cube: EntryAggregateCube,
    development_fraction: float,
    validation_fraction: float,
    minimum_pair_events: int,
) -> dict[str, Any]:
    pip_size = crossover.infer_pip_size(instrument)
    proxy_spread, spread_sample_count = crossover.estimate_spread_pips(
        frame,
        pip_size,
    )
    required_timeframes = set(configurations_by_timeframe)
    for timeframe, configs in configurations_by_timeframe.items():
        if any(config.confirmation == "higher_tf_same" for config in configs):
            required_timeframes.add(crossover.HIGHER_TIMEFRAME[timeframe])
    bar_cache = crossover.prepare_bar_cache(frame, required_timeframes)

    evaluated_configs = 0
    mapped_events = 0
    for timeframe, configs in configurations_by_timeframe.items():
        bars = bar_cache[timeframe]
        if len(bars) < 25:
            continue
        required_ma = {
            (config.fast_kind, config.fast_window)
            for config in configs
        } | {
            (config.slow_kind, config.slow_window)
            for config in configs
        }
        ma_cache = crossover.prepare_ma_cache(bars, required_ma)
        close = bars["close"].to_numpy(dtype=np.float64, copy=False)
        volume = bars["volume"].to_numpy(dtype=np.float64, copy=False)
        volume_median = (
            pd.Series(volume, copy=False)
            .rolling(window=20, min_periods=10)
            .median()
            .to_numpy(dtype=np.float64, copy=False)
        )

        higher_cache: dict[tuple[str, int], np.ndarray] = {}
        higher_bars: pd.DataFrame | None = None
        higher_minutes = 0
        if any(config.confirmation == "higher_tf_same" for config in configs):
            higher_name = crossover.HIGHER_TIMEFRAME[timeframe]
            higher_bars = bar_cache[higher_name]
            higher_minutes = crossover.TIMEFRAME_MINUTES[higher_name]
            higher_cache = crossover.prepare_ma_cache(higher_bars, required_ma)

        grouped: dict[tuple[str, str, int, int], list[crossover.Configuration]] = {}
        for config in configs:
            grouped.setdefault(
                (
                    config.fast_kind,
                    config.slow_kind,
                    config.fast_window,
                    config.slow_window,
                ),
                [],
            ).append(config)

        for cross_key, cross_configs in grouped.items():
            fast_kind, slow_kind, fast_window, slow_window = cross_key
            fast = ma_cache[(fast_kind, fast_window)]
            slow = ma_cache[(slow_kind, slow_window)]
            positions, directions = crossover.detect_crosses(fast, slow)
            evaluated_configs += len(cross_configs)
            if positions.size == 0:
                continue

            higher_alignment: np.ndarray | None = None
            if higher_bars is not None:
                decision_ns = (
                    bars.index.asi8[positions]
                    + crossover.TIMEFRAME_MINUTES[timeframe]
                    * 60
                    * 1_000_000_000
                )
                higher_alignment = crossover.completed_higher_tf_alignment(
                    decision_ns,
                    directions,
                    higher_bars.index,
                    higher_minutes,
                    higher_cache[(fast_kind, fast_window)],
                    higher_cache[(slow_kind, slow_window)],
                )

            base_events = crossover.map_signal_entries(
                bars.index,
                positions,
                directions,
                crossover.TIMEFRAME_MINUTES[timeframe],
                frame.index,
            )
            if (
                base_events.entry_positions.size == 0
                or base_events.source_indices is None
            ):
                continue
            source_indices = base_events.source_indices
            base_segments = crossover.split_for_entries(
                base_events.entry_positions,
                len(frame),
                development_fraction,
                validation_fraction,
            )
            paths = entry_path_outcomes(
                frame,
                base_events,
                path_windows_min,
                barrier_pips,
                pip_size,
                proxy_spread,
            )

            for config in cross_configs:
                raw_confirmation = crossover.apply_confirmation(
                    config.confirmation,
                    positions,
                    directions,
                    close,
                    volume,
                    volume_median,
                    fast,
                    slow,
                    higher_alignment,
                )
                confirmation = raw_confirmation[source_indices]
                mapped_events += int(np.count_nonzero(confirmation))
                if not confirmation.any():
                    continue
                for window_index in range(len(path_windows_min)):
                    for segment_index in range(len(crossover.SEGMENTS)):
                        mask = confirmation & (base_segments == segment_index)
                        cube.update(
                            config.config_id,
                            window_index,
                            segment_index,
                            paths,
                            mask,
                            minimum_pair_events,
                        )
            del paths

        del ma_cache, higher_cache, grouped
        gc.collect()

    exact_columns = [
        "bid_open",
        "bid_high",
        "bid_low",
        "ask_open",
        "ask_high",
        "ask_low",
    ]
    exact = frame[exact_columns].notna().all(axis=1)
    return {
        "instrument": instrument,
        "rows": int(len(frame)),
        "start": frame.index.min().isoformat(),
        "end": frame.index.max().isoformat(),
        "exact_path_rows": int(exact.sum()),
        "exact_path_fraction": float(exact.mean()),
        "proxy_spread_pips": proxy_spread,
        "spread_sample_count": spread_sample_count,
        "evaluated_configs": evaluated_configs,
        "mapped_signal_events_across_configs": mapped_events,
    }


def build_selected_candidates(
    aggregate: pd.DataFrame,
    minimum_segment_events: int,
    minimum_pairs: int,
    minimum_resolved_rate: float,
) -> pd.DataFrame:
    if aggregate.empty:
        return pd.DataFrame()
    index_columns = [
        "config_id",
        "timeframe",
        "fast_kind",
        "slow_kind",
        "fast_window",
        "slow_window",
        "confirmation",
        "path_window_min",
        "barrier_pips",
    ]
    value_columns = [
        "n",
        "mfe_advantage_pips",
        "direction_dominance_rate",
        "direction_pair_count",
        "direction_positive_pair_fraction",
        "resolved_rate",
        "first_touch_win_rate",
        "barrier_expectancy_per_signal_pips",
        "barrier_pair_count",
        "barrier_positive_pair_fraction",
        "avg_mfe_pips",
        "avg_mae_pips",
        "ever_positive_rate",
    ]
    wide = aggregate.pivot(
        index=index_columns,
        columns="segment",
        values=value_columns,
    )
    wide.columns = [f"{segment}_{metric}" for metric, segment in wide.columns]
    wide = wide.reset_index()
    for segment in crossover.SEGMENTS:
        for metric in value_columns:
            column = f"{segment}_{metric}"
            if column not in wide:
                wide[column] = math.nan

    eligible = np.ones(len(wide), dtype=bool)
    for segment in ("development", "validation"):
        eligible &= wide[f"{segment}_n"].fillna(0) >= minimum_segment_events
        eligible &= (
            wide[f"{segment}_direction_pair_count"].fillna(0) >= minimum_pairs
        )
        eligible &= wide[f"{segment}_barrier_pair_count"].fillna(0) >= minimum_pairs
        eligible &= (
            wide[f"{segment}_resolved_rate"].fillna(0.0) >= minimum_resolved_rate
        )
        eligible &= wide[f"{segment}_mfe_advantage_pips"].fillna(-math.inf) > 0.0
        eligible &= (
            wide[f"{segment}_direction_dominance_rate"].fillna(0.0) > 0.50
        )
        eligible &= (
            wide[f"{segment}_direction_positive_pair_fraction"].fillna(0.0)
            >= 0.50
        )
        eligible &= (
            wide[f"{segment}_first_touch_win_rate"].fillna(0.0) > 0.50
        )
        eligible &= (
            wide[f"{segment}_barrier_expectancy_per_signal_pips"].fillna(-math.inf)
            > 0.0
        )
        eligible &= (
            wide[f"{segment}_barrier_positive_pair_fraction"].fillna(0.0)
            >= 0.50
        )
    selected = wide.loc[eligible].copy()
    if selected.empty:
        return selected

    holdout_pass = np.ones(len(selected), dtype=bool)
    segment = "holdout"
    holdout_pass &= selected[f"{segment}_n"].fillna(0) >= minimum_segment_events
    holdout_pass &= (
        selected[f"{segment}_direction_pair_count"].fillna(0) >= minimum_pairs
    )
    holdout_pass &= (
        selected[f"{segment}_barrier_pair_count"].fillna(0) >= minimum_pairs
    )
    holdout_pass &= (
        selected[f"{segment}_resolved_rate"].fillna(0.0) >= minimum_resolved_rate
    )
    holdout_pass &= (
        selected[f"{segment}_mfe_advantage_pips"].fillna(-math.inf) > 0.0
    )
    holdout_pass &= (
        selected[f"{segment}_direction_dominance_rate"].fillna(0.0) > 0.50
    )
    holdout_pass &= (
        selected[f"{segment}_direction_positive_pair_fraction"].fillna(0.0)
        >= 0.50
    )
    holdout_pass &= (
        selected[f"{segment}_first_touch_win_rate"].fillna(0.0) > 0.50
    )
    holdout_pass &= (
        selected[f"{segment}_barrier_expectancy_per_signal_pips"].fillna(-math.inf)
        > 0.0
    )
    holdout_pass &= (
        selected[f"{segment}_barrier_positive_pair_fraction"].fillna(0.0)
        >= 0.50
    )
    selected["holdout_passed"] = holdout_pass
    selected["selection_floor_expectancy_pips"] = selected[
        [
            "development_barrier_expectancy_per_signal_pips",
            "validation_barrier_expectancy_per_signal_pips",
        ]
    ].min(axis=1)
    selected["selection_floor_win_rate"] = selected[
        [
            "development_first_touch_win_rate",
            "validation_first_touch_win_rate",
        ]
    ].min(axis=1)
    selected["selection_floor_mfe_advantage_pips"] = selected[
        [
            "development_mfe_advantage_pips",
            "validation_mfe_advantage_pips",
        ]
    ].min(axis=1)
    selected["selection_score"] = (
        selected["selection_floor_expectancy_pips"]
        + selected["barrier_pips"]
        * (selected["selection_floor_win_rate"] - 0.50)
        + 0.10 * selected["selection_floor_mfe_advantage_pips"]
    )
    return selected.sort_values(
        ["selection_score", "validation_n"],
        ascending=[False, False],
    ).reset_index(drop=True)


_WORKER_CONTEXT: dict[str, Any] | None = None


def initialize_worker(context: dict[str, Any]) -> None:
    global _WORKER_CONTEXT
    _WORKER_CONTEXT = context


def evaluate_worker(instrument: str) -> dict[str, Any]:
    if _WORKER_CONTEXT is None:
        raise RuntimeError("worker context was not initialized")
    context = _WORKER_CONTEXT
    configurations = context["configurations"]
    cube = EntryAggregateCube(
        len(configurations),
        len(context["path_windows_min"]),
        len(context["barrier_pips"]),
    )
    path = Path(context["source_dir"]) / f"{instrument}_M1.parquet"
    try:
        frame = read_entry_frame(
            path,
            context["start"],
            context["end"],
            context["exact_only"],
        )
        inventory = evaluate_pair(
            instrument,
            frame,
            context["configurations_by_timeframe"],
            context["path_windows_min"],
            context["barrier_pips"],
            cube,
            context["development_fraction"],
            context["validation_fraction"],
            context["minimum_pair_events"],
        )
        return {
            "instrument": instrument,
            "inventory": inventory,
            "aggregate": cube.payload(),
            "error": None,
        }
    except Exception as exc:
        return {
            "instrument": instrument,
            "inventory": None,
            "aggregate": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def render_summary(
    manifest: dict[str, Any],
    selected: pd.DataFrame,
) -> str:
    lines = [
        "# Moving-Average Crossover Entry-Quality Sweep",
        "",
        "This study evaluates executable path quality after a crossover entry. It is",
        "not a fixed-endpoint forecast or account-equity simulation.",
        "",
        "## Coverage",
        "",
        f"- Loaded pairs: {manifest['loaded_pair_count']}.",
        f"- Configurations: {manifest['configuration_count']}.",
        f"- Path windows: {', '.join(str(value) for value in manifest['path_windows_min'])} minutes.",
        f"- Symmetric barriers: {', '.join(str(value) for value in manifest['barrier_pips'])} pips.",
        f"- Development/validation candidates: {manifest['candidate_count']}.",
        f"- Holdout-confirmed candidates: {manifest['holdout_confirmed_count']}.",
        "",
        "A first-touch win requires the profit barrier to be observed in an earlier",
        "M1 candle than the loss barrier. Same-candle collisions count as losses.",
        "Unresolved paths contribute zero to barrier expectancy.",
        "",
        "## Top Candidates",
        "",
    ]
    if selected.empty:
        lines.append("No configuration passed the development and validation entry-quality gate.")
    else:
        columns = [
            "timeframe",
            "fast_kind",
            "fast_window",
            "slow_kind",
            "slow_window",
            "confirmation",
            "path_window_min",
            "barrier_pips",
            "selection_score",
            "validation_first_touch_win_rate",
            "holdout_first_touch_win_rate",
            "holdout_mfe_advantage_pips",
            "holdout_passed",
        ]
        display = selected[columns].head(20).copy()
        lines.append(crossover.markdown_table(display))
    lines.extend(
        [
            "",
            "Read `manifest.json` for the complete run contract and selection gates.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=crossover.DEFAULT_SOURCE_DIR,
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--instrument", default="all")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--preset", choices=("smoke", "wide", "exhaustive"), default="wide")
    parser.add_argument("--window-pairs", default="")
    parser.add_argument(
        "--timeframes",
        default=",".join(crossover.DEFAULT_TIMEFRAMES),
    )
    parser.add_argument("--ma-pairs", default=",".join(crossover.DEFAULT_MA_PAIRS))
    parser.add_argument(
        "--confirmations",
        default=",".join(crossover.DEFAULT_CONFIRMATIONS),
    )
    parser.add_argument(
        "--path-windows-min",
        type=parse_csv_floats,
        default=list(DEFAULT_PATH_WINDOWS_MIN),
    )
    parser.add_argument(
        "--barrier-pips",
        type=parse_csv_floats,
        default=list(DEFAULT_BARRIER_PIPS),
    )
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--exact-only", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--development-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--minimum-segment-events", type=int, default=100)
    parser.add_argument("--minimum-pairs", type=int, default=10)
    parser.add_argument("--minimum-pair-events", type=int, default=5)
    parser.add_argument("--minimum-resolved-rate", type=float, default=0.25)
    parser.add_argument("--top-n", type=int, default=500)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = utc_now()
    start = crossover.parse_utc(args.start or "")
    end = crossover.parse_utc(args.end or "")
    path_windows_min = [int(value) for value in args.path_windows_min]
    if any(float(value) != int(value) for value in args.path_windows_min):
        raise ValueError("path windows must be whole minutes")
    if max(path_windows_min) > 240:
        raise ValueError("entry-quality path windows are capped at 240 minutes")
    barrier_pips = [float(value) for value in args.barrier_pips]
    if not 0.0 < args.development_fraction < 1.0:
        raise ValueError("development fraction must be between zero and one")
    if not 0.0 < args.validation_fraction < 1.0:
        raise ValueError("validation fraction must be between zero and one")
    if args.development_fraction + args.validation_fraction >= 1.0:
        raise ValueError("development plus validation fraction must be below one")

    timeframes = crossover.parse_csv_strings(args.timeframes)
    invalid_timeframes = sorted(set(timeframes) - set(crossover.TIMEFRAME_MINUTES))
    if invalid_timeframes:
        raise ValueError(f"Unsupported timeframes: {', '.join(invalid_timeframes)}")
    ma_pairs = crossover.parse_ma_pairs(args.ma_pairs)
    confirmations = crossover.parse_csv_strings(args.confirmations)
    invalid_confirmations = sorted(
        set(confirmations) - set(crossover.DEFAULT_CONFIRMATIONS)
    )
    if invalid_confirmations:
        raise ValueError(
            f"Unsupported confirmations: {', '.join(invalid_confirmations)}"
        )
    window_pairs = (
        crossover.parse_window_pairs(args.window_pairs)
        if args.window_pairs
        else crossover.window_pairs_for_preset(args.preset)
    )
    configurations = crossover.build_configurations(
        timeframes,
        ma_pairs,
        window_pairs,
        confirmations,
    )
    by_timeframe: dict[str, list[crossover.Configuration]] = {}
    for config in configurations:
        by_timeframe.setdefault(config.timeframe, []).append(config)

    instruments = crossover.discover_instruments(
        args.source_dir,
        args.instrument,
        args.max_pairs,
    )
    output_dir = args.output_dir or (
        DEFAULT_REPORT_ROOT
        / f"entry_quality_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    context = {
        "source_dir": str(args.source_dir.resolve()),
        "configurations": configurations,
        "configurations_by_timeframe": by_timeframe,
        "path_windows_min": path_windows_min,
        "barrier_pips": barrier_pips,
        "start": start,
        "end": end,
        "exact_only": bool(args.exact_only),
        "development_fraction": args.development_fraction,
        "validation_fraction": args.validation_fraction,
        "minimum_pair_events": args.minimum_pair_events,
    }
    cube = EntryAggregateCube(
        len(configurations),
        len(path_windows_min),
        len(barrier_pips),
    )
    inventories: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    workers = max(1, min(args.workers, len(instruments)))
    completed = 0
    if workers == 1:
        initialize_worker(context)
        result_iterator = (evaluate_worker(instrument) for instrument in instruments)
        for result in result_iterator:
            completed += 1
            if result["error"]:
                errors.append(
                    {"instrument": result["instrument"], "error": result["error"]}
                )
            else:
                cube.merge_payload(result["aggregate"])
                inventories.append(result["inventory"])
            print(
                f"[{completed}/{len(instruments)}] {result['instrument']} "
                f"{'ERROR ' + result['error'] if result['error'] else 'ok'}",
                flush=True,
            )
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers,
            initializer=initialize_worker,
            initargs=(context,),
        ) as executor:
            futures = {
                executor.submit(evaluate_worker, instrument): instrument
                for instrument in instruments
            }
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                completed += 1
                if result["error"]:
                    errors.append(
                        {
                            "instrument": result["instrument"],
                            "error": result["error"],
                        }
                    )
                else:
                    cube.merge_payload(result["aggregate"])
                    inventories.append(result["inventory"])
                print(
                    f"[{completed}/{len(instruments)}] {result['instrument']} "
                    f"{'ERROR ' + result['error'] if result['error'] else 'ok'}",
                    flush=True,
                )

    aggregate = cube.frame(configurations, path_windows_min, barrier_pips)
    selected = build_selected_candidates(
        aggregate,
        args.minimum_segment_events,
        args.minimum_pairs,
        args.minimum_resolved_rate,
    )
    aggregate_path = output_dir / "entry_quality_aggregate.parquet"
    selected_path = output_dir / "entry_quality_candidates.csv"
    inventory_path = output_dir / "data_inventory.csv"
    aggregate.to_parquet(aggregate_path, index=False)
    selected.head(args.top_n).to_csv(selected_path, index=False)
    pd.DataFrame(inventories).sort_values("instrument").to_csv(
        inventory_path,
        index=False,
    )

    manifest = {
        "schema_version": 1,
        "run_type": "moving_average_crossover_entry_quality",
        "started_at_utc": started,
        "finished_at_utc": utc_now(),
        "source_dir": str(args.source_dir.resolve()),
        "requested_pair_count": len(instruments),
        "loaded_pair_count": len(inventories),
        "load_errors": errors,
        "configuration_count": len(configurations),
        "observed_configuration_count": (
            int(aggregate["config_id"].nunique()) if not aggregate.empty else 0
        ),
        "workers": workers,
        "preset": args.preset,
        "window_pairs": window_pairs,
        "timeframes": timeframes,
        "ma_kind_pairs": [f"{fast}-{slow}" for fast, slow in ma_pairs],
        "confirmations": confirmations,
        "path_windows_min": path_windows_min,
        "barrier_pips": barrier_pips,
        "exact_only": bool(args.exact_only),
        "date_range": {
            "start": start.isoformat() if start is not None else None,
            "end": end.isoformat() if end is not None else None,
        },
        "splits": {
            "development_fraction": args.development_fraction,
            "validation_fraction": args.validation_fraction,
            "holdout_fraction": (
                1.0 - args.development_fraction - args.validation_fraction
            ),
        },
        "selection_gates": {
            "minimum_segment_events": args.minimum_segment_events,
            "minimum_pairs": args.minimum_pairs,
            "minimum_pair_events": args.minimum_pair_events,
            "minimum_resolved_rate": args.minimum_resolved_rate,
            "development_and_validation_requirements": {
                "mfe_advantage_pips": "> 0",
                "direction_dominance_rate": "> 0.50",
                "direction_positive_pair_fraction": ">= 0.50",
                "first_touch_win_rate": "> 0.50",
                "barrier_expectancy_per_signal_pips": "> 0",
                "barrier_positive_pair_fraction": ">= 0.50",
            },
        },
        "candidate_count": int(len(selected)),
        "holdout_confirmed_count": (
            int(selected["holdout_passed"].sum()) if not selected.empty else 0
        ),
        "method": {
            "signal": "completed resampled bars only",
            "entry": "next observed M1 open, maximum mapping delay 2 minutes",
            "path": "executable bid/ask OHLC extrema from entry until path-window end",
            "proxy": "midpoint OHLC plus pair median observed spread when bid/ask is missing",
            "first_touch": (
                "profit target must precede loss barrier; same-M1-candle collision "
                "counts as loss"
            ),
            "opposite_direction_control": (
                "maximum favorable excursion in the reverse direction at the "
                "same entry timestamp"
            ),
        },
        "outputs": {
            "aggregate": str(aggregate_path.resolve()),
            "candidates": str(selected_path.resolve()),
            "inventory": str(inventory_path.resolve()),
        },
        "top_candidates": crossover.dataframe_records(selected, limit=20),
    }
    crossover.atomic_write_json(output_dir / "manifest.json", manifest)
    (output_dir / "SUMMARY.md").write_text(
        render_summary(manifest, selected),
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2, default=str), flush=True)
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
