#!/usr/bin/env python3
"""Run a broad, leakage-safe moving-average crossover event study.

Signals are calculated from completed resampled bars. Entry is the next observed
M1 open, and each fixed-horizon result exits at the first M1 close at the target
wall-clock time. Recorded bid/ask prices are used when available. Older rows that
only contain midpoint OHLC use the pair's median recorded spread and are tagged
as proxy-cost outcomes.

This is an exploratory signal study, not an account-equity simulation. Signals
can overlap, so summed pips must not be interpreted as realizable portfolio P/L.
Candidate ranking uses development and validation data only; holdout results are
reported after selection.
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
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_SOURCE_DIR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "candles_m1_parquet_recovered_20260719"
)
DEFAULT_REPORT_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "reports" / "moving_average_crossover_sweep"
)

TIMEFRAME_MINUTES: dict[str, int] = {
    "M1": 1,
    "M2": 2,
    "M3": 3,
    "M4": 4,
    "M5": 5,
    "M6": 6,
    "M8": 8,
    "M10": 10,
    "M12": 12,
    "M15": 15,
    "M20": 20,
    "M30": 30,
    "M45": 45,
    "H1": 60,
    "H2": 120,
    "H3": 180,
    "H4": 240,
    "H6": 360,
    "H8": 480,
    "H12": 720,
    "D1": 1440,
}

HIGHER_TIMEFRAME: dict[str, str] = {
    "M1": "M5",
    "M2": "M5",
    "M3": "M10",
    "M4": "M15",
    "M5": "M15",
    "M6": "M20",
    "M8": "M30",
    "M10": "M30",
    "M12": "H1",
    "M15": "H1",
    "M20": "H2",
    "M30": "H2",
    "M45": "H3",
    "H1": "H4",
    "H2": "H6",
    "H3": "H8",
    "H4": "H12",
    "H6": "H12",
    "H8": "D1",
    "H12": "D1",
}

DEFAULT_TIMEFRAMES = ("M1", "M5", "M10", "M15", "M30", "H1", "H2", "H3", "H4")
DEFAULT_MA_PAIRS = ("ema-ema", "sma-sma", "ema-sma", "sma-ema")
DEFAULT_CONFIRMATIONS = (
    "cross_only",
    "slow_slope",
    "dual_slope",
    "slow_slope_volume",
    "higher_tf_same",
)
DEFAULT_HORIZONS_MIN = (1, 3, 5, 10, 15, 30, 60, 120, 240, 1440)

# This grid intentionally includes common retail pairs and denser short-period
# combinations without expanding into every mathematically possible pair.
WIDE_WINDOW_PAIRS: tuple[tuple[int, int], ...] = (
    (2, 3),
    (2, 5),
    (2, 8),
    (3, 5),
    (3, 8),
    (3, 9),
    (3, 10),
    (3, 13),
    (3, 21),
    (4, 8),
    (4, 9),
    (4, 12),
    (4, 20),
    (5, 8),
    (5, 9),
    (5, 10),
    (5, 13),
    (5, 15),
    (5, 20),
    (5, 21),
    (5, 30),
    (7, 14),
    (7, 21),
    (7, 30),
    (8, 13),
    (8, 20),
    (8, 21),
    (8, 26),
    (8, 30),
    (9, 15),
    (9, 20),
    (9, 21),
    (9, 26),
    (9, 30),
    (9, 50),
    (10, 20),
    (10, 30),
    (10, 50),
    (12, 20),
    (12, 26),
    (12, 30),
    (12, 50),
    (15, 30),
    (15, 50),
    (20, 40),
    (20, 50),
    (20, 100),
    (21, 50),
    (21, 100),
    (30, 50),
    (30, 75),
    (30, 100),
    (40, 100),
    (50, 100),
    (50, 150),
    (50, 200),
    (75, 150),
    (100, 200),
)

SMOKE_WINDOW_PAIRS: tuple[tuple[int, int], ...] = (
    (3, 9),
    (5, 13),
    (8, 21),
    (9, 20),
    (12, 26),
    (20, 50),
    (50, 200),
)

FAST_EXHAUSTIVE = (2, 3, 4, 5, 7, 8, 9, 10, 12, 15, 20, 21, 30, 40, 50, 75, 100)
SLOW_EXHAUSTIVE = (3, 5, 8, 9, 10, 12, 13, 15, 18, 20, 21, 26, 30, 40, 50, 75, 100, 150, 200)

INPUT_COLUMNS = (
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "bid_open",
    "bid_close",
    "ask_open",
    "ask_close",
    "spread_pips",
)

SEGMENTS = ("development", "validation", "holdout")
SEGMENT_INDEX = {name: index for index, name in enumerate(SEGMENTS)}


@dataclass(frozen=True)
class Configuration:
    config_id: int
    timeframe: str
    fast_kind: str
    slow_kind: str
    fast_window: int
    slow_window: int
    confirmation: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "config_id": self.config_id,
            "timeframe": self.timeframe,
            "fast_kind": self.fast_kind,
            "slow_kind": self.slow_kind,
            "fast_window": self.fast_window,
            "slow_window": self.slow_window,
            "confirmation": self.confirmation,
        }


@dataclass
class SignalEvents:
    entry_positions: np.ndarray
    directions: np.ndarray
    source_indices: np.ndarray | None = None


@dataclass
class OutcomeBatch:
    gross_pips: np.ndarray
    net_pips: np.ndarray
    exact_cost: np.ndarray
    valid: np.ndarray


class AggregateCube:
    """Compact pooled sufficient statistics for every config/horizon/split."""

    def __init__(self, config_count: int, horizon_count: int) -> None:
        shape = (config_count, horizon_count, len(SEGMENTS))
        self.n = np.zeros(shape, dtype=np.int64)
        self.wins = np.zeros(shape, dtype=np.int64)
        self.exact_n = np.zeros(shape, dtype=np.int64)
        self.long_n = np.zeros(shape, dtype=np.int64)
        self.gross_sum = np.zeros(shape, dtype=np.float64)
        self.net_sum = np.zeros(shape, dtype=np.float64)
        self.net_sumsq = np.zeros(shape, dtype=np.float64)
        self.positive_sum = np.zeros(shape, dtype=np.float64)
        self.negative_abs_sum = np.zeros(shape, dtype=np.float64)
        self.pair_n = np.zeros(shape, dtype=np.int32)
        self.positive_pair_n = np.zeros(shape, dtype=np.int32)
        self.pair_mean_sum = np.zeros(shape, dtype=np.float64)

    def update(
        self,
        config_id: int,
        horizon_index: int,
        segment_index: int,
        gross: np.ndarray,
        net: np.ndarray,
        exact: np.ndarray,
        directions: np.ndarray,
    ) -> None:
        finite = np.isfinite(gross) & np.isfinite(net)
        if not finite.any():
            return
        gross = gross[finite]
        net = net[finite]
        exact = exact[finite]
        directions = directions[finite]
        key = (config_id, horizon_index, segment_index)
        count = int(net.size)
        mean_net = float(net.mean())
        self.n[key] += count
        self.wins[key] += int(np.count_nonzero(net > 0.0))
        self.exact_n[key] += int(np.count_nonzero(exact))
        self.long_n[key] += int(np.count_nonzero(directions > 0))
        self.gross_sum[key] += float(gross.sum())
        self.net_sum[key] += float(net.sum())
        self.net_sumsq[key] += float(np.square(net).sum())
        self.positive_sum[key] += float(net[net > 0.0].sum())
        self.negative_abs_sum[key] += float(-net[net < 0.0].sum())
        self.pair_n[key] += 1
        self.positive_pair_n[key] += int(mean_net > 0.0)
        self.pair_mean_sum[key] += mean_net

    def frame(
        self,
        configurations: Sequence[Configuration],
        horizons_min: Sequence[int],
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for config in configurations:
            for horizon_index, horizon_min in enumerate(horizons_min):
                for segment_index, segment in enumerate(SEGMENTS):
                    key = (config.config_id, horizon_index, segment_index)
                    count = int(self.n[key])
                    if count <= 0:
                        continue
                    net_sum = float(self.net_sum[key])
                    mean_net = net_sum / count
                    variance = max(
                        0.0,
                        float(self.net_sumsq[key]) / count - mean_net * mean_net,
                    )
                    std_net = math.sqrt(variance)
                    standard_error = std_net / math.sqrt(count) if count > 1 else float("nan")
                    t_stat = (
                        mean_net / standard_error
                        if standard_error > 0.0 and math.isfinite(standard_error)
                        else float("nan")
                    )
                    negative_abs = float(self.negative_abs_sum[key])
                    profit_factor = (
                        float(self.positive_sum[key]) / negative_abs
                        if negative_abs > 0.0
                        else float("inf")
                    )
                    pair_count = int(self.pair_n[key])
                    row = config.as_dict()
                    row.update(
                        {
                            "horizon_min": int(horizon_min),
                            "segment": segment,
                            "n": count,
                            "win_rate": float(self.wins[key]) / count,
                            "avg_gross_pips": float(self.gross_sum[key]) / count,
                            "avg_net_pips": mean_net,
                            "total_net_signal_pips": net_sum,
                            "std_net_pips": std_net,
                            "t_stat": t_stat,
                            "profit_factor": profit_factor,
                            "exact_cost_fraction": float(self.exact_n[key]) / count,
                            "long_fraction": float(self.long_n[key]) / count,
                            "pair_count": pair_count,
                            "positive_pair_fraction": (
                                float(self.positive_pair_n[key]) / pair_count
                                if pair_count
                                else float("nan")
                            ),
                            "mean_pair_avg_net_pips": (
                                float(self.pair_mean_sum[key]) / pair_count
                                if pair_count
                                else float("nan")
                            ),
                        }
                    )
                    rows.append(row)
        return pd.DataFrame(rows)

    def payload(self) -> dict[str, np.ndarray]:
        return {
            name: getattr(self, name)
            for name in (
                "n",
                "wins",
                "exact_n",
                "long_n",
                "gross_sum",
                "net_sum",
                "net_sumsq",
                "positive_sum",
                "negative_abs_sum",
                "pair_n",
                "positive_pair_n",
                "pair_mean_sum",
            )
        }

    def merge_payload(self, payload: dict[str, np.ndarray]) -> None:
        for name, values in payload.items():
            target = getattr(self, name)
            if target.shape != values.shape:
                raise ValueError(
                    f"Aggregate shape mismatch for {name}: {target.shape} != {values.shape}"
                )
            target += values


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def infer_pip_size(instrument: str, frame: pd.DataFrame | None = None) -> float:
    """Infer the venue pip from recorded BAM spread metadata when available.

    JPY is not the only OANDA quote currency whose display pip is 0.01.  HUF
    and THB instruments in the priced universe use the same increment.  The
    previous symbol-only fallback therefore overstated their costs and returns
    by 100x.  A recorded spread in pips plus executable bid/ask supplies the
    instrument-specific contract without a hard-coded currency list.
    """
    if frame is not None and {
        "spread_pips", "ask_open", "bid_open"
    }.issubset(frame.columns):
        declared = pd.to_numeric(frame["spread_pips"], errors="coerce")
        raw_spread = (
            pd.to_numeric(frame["ask_open"], errors="coerce")
            - pd.to_numeric(frame["bid_open"], errors="coerce")
        )
        valid = (
            declared.gt(0.0)
            & raw_spread.gt(0.0)
            & np.isfinite(declared)
            & np.isfinite(raw_spread)
        )
        if int(valid.sum()) >= 10:
            observed = float((raw_spread[valid] / declared[valid]).median())
            candidates = np.asarray(
                [0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1],
                dtype=np.float64,
            )
            if np.isfinite(observed) and observed > 0.0:
                distance = np.abs(np.log10(candidates) - math.log10(observed))
                selected = float(candidates[int(np.argmin(distance))])
                if 0.5 <= observed / selected <= 2.0:
                    return selected
    quote = instrument.rsplit("_", 1)[-1].upper()
    return 0.01 if quote == "JPY" else 0.0001


def parse_csv_strings(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_csv_ints(value: str) -> list[int]:
    output = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not output or any(item <= 0 for item in output):
        raise argparse.ArgumentTypeError("values must be positive comma-separated integers")
    return output


def parse_ma_pairs(value: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in parse_csv_strings(value.lower()):
        parts = item.split("-", 1)
        if len(parts) != 2 or any(part not in {"ema", "sma"} for part in parts):
            raise argparse.ArgumentTypeError(
                "MA pairs must be comma-separated fast-slow kinds such as ema-ema,sma-sma"
            )
        pairs.append((parts[0], parts[1]))
    if not pairs:
        raise argparse.ArgumentTypeError("provide at least one MA kind pair")
    return pairs


def window_pairs_for_preset(preset: str) -> list[tuple[int, int]]:
    if preset == "smoke":
        return list(SMOKE_WINDOW_PAIRS)
    if preset == "wide":
        return list(WIDE_WINDOW_PAIRS)
    return [
        (fast, slow)
        for fast in FAST_EXHAUSTIVE
        for slow in SLOW_EXHAUSTIVE
        if fast < slow
    ]


def parse_window_pairs(value: str) -> list[tuple[int, int]]:
    pairs: set[tuple[int, int]] = set()
    for item in parse_csv_strings(value):
        normalized = item.replace("/", ":").replace("-", ":")
        parts = normalized.split(":")
        if len(parts) != 2:
            raise argparse.ArgumentTypeError(
                "window pairs must use fast:slow notation, for example 5:13,8:21"
            )
        fast, slow = (int(part.strip()) for part in parts)
        if fast <= 0 or slow <= fast:
            raise argparse.ArgumentTypeError(
                "every window pair must contain positive integers with fast < slow"
            )
        pairs.add((fast, slow))
    if not pairs:
        raise argparse.ArgumentTypeError("provide at least one window pair")
    return sorted(pairs)


def discover_instruments(source_dir: Path, requested: str, max_pairs: int) -> list[str]:
    available = sorted(
        {
            path.name.rsplit("_M1.", 1)[0]
            for pattern in ("*_M1.parquet", "*_M1.csv")
            for path in source_dir.glob(pattern)
        }
    )
    if requested.strip().lower() in {"", "all", "*"}:
        selected = available
    else:
        wanted = {
            item.upper().replace("/", "_").replace("-", "_")
            for item in parse_csv_strings(requested)
        }
        missing = sorted(wanted - set(available))
        if missing:
            raise FileNotFoundError(f"Missing pair parquet files: {', '.join(missing)}")
        selected = sorted(wanted)
    return selected[:max_pairs] if max_pairs > 0 else selected


def instrument_source_path(source_dir: Path, instrument: str) -> Path:
    parquet = source_dir / f"{instrument}_M1.parquet"
    if parquet.is_file():
        return parquet
    csv_path = source_dir / f"{instrument}_M1.csv"
    if csv_path.is_file():
        return csv_path
    raise FileNotFoundError(f"Missing M1 candle file for {instrument} in {source_dir}")


def build_configurations(
    timeframes: Sequence[str],
    ma_pairs: Sequence[tuple[str, str]],
    window_pairs: Sequence[tuple[int, int]],
    confirmations: Sequence[str],
) -> list[Configuration]:
    configurations: list[Configuration] = []
    for timeframe in timeframes:
        for fast_kind, slow_kind in ma_pairs:
            for fast_window, slow_window in window_pairs:
                for confirmation in confirmations:
                    configurations.append(
                        Configuration(
                            config_id=len(configurations),
                            timeframe=timeframe,
                            fast_kind=fast_kind,
                            slow_kind=slow_kind,
                            fast_window=fast_window,
                            slow_window=slow_window,
                            confirmation=confirmation,
                        )
                    )
    return configurations


def read_pair_frame(
    path: Path,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    attempts: int = 5,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            if path.suffix.lower() == ".csv":
                requested = set(INPUT_COLUMNS) | {"datetime"}
                frame = pd.read_csv(
                    path,
                    usecols=lambda column: column in requested,
                )
                if "time" not in frame and "datetime" in frame:
                    frame["time"] = frame["datetime"]
            else:
                frame = pd.read_parquet(path, columns=list(INPUT_COLUMNS))
            break
        except (OSError, PermissionError) as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                raise
            time.sleep(0.25 * (attempt + 1))
    else:
        raise RuntimeError(f"Unable to read {path}: {last_error}")

    for column in INPUT_COLUMNS:
        if column not in frame:
            frame[column] = np.nan
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["time", "open", "close"]).set_index("time").sort_index()
    frame = frame[~frame.index.duplicated(keep="last")]
    if start is not None:
        frame = frame.loc[frame.index >= start]
    if end is not None:
        frame = frame.loc[frame.index <= end]
    for column in INPUT_COLUMNS:
        if column != "time" and column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["open", "close"])
    if frame.empty:
        raise ValueError(f"No usable M1 rows in {path}")
    return frame


def estimate_spread_pips(frame: pd.DataFrame, pip_size: float) -> tuple[float, int]:
    observed = pd.to_numeric(frame["spread_pips"], errors="coerce")
    observed = observed[(observed > 0.0) & np.isfinite(observed)]
    if observed.empty:
        calculated = (frame["ask_open"] - frame["bid_open"]) / pip_size
        observed = calculated[(calculated > 0.0) & np.isfinite(calculated)]
    if observed.empty:
        return 2.0, 0
    return float(observed.median()), int(observed.size)


def resample_signal_bars(frame: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if minutes == 1:
        bars = frame[["close", "volume"]].copy()
        bars["source_count"] = 1
        return bars.dropna(subset=["close"])

    rule = f"{minutes}min"
    bars = frame.resample(rule, label="left", closed="left", origin="epoch").agg(
        close=("close", "last"),
        volume=("volume", "sum"),
        source_count=("close", "count"),
    )
    minimum_count = max(1, int(math.ceil(minutes * 0.80)))
    bars = bars.loc[bars["source_count"] >= minimum_count]
    return bars.dropna(subset=["close"])


def moving_average(values: np.ndarray, kind: str, window: int) -> np.ndarray:
    series = pd.Series(values, copy=False)
    if kind == "ema":
        result = series.ewm(span=window, adjust=False, min_periods=window).mean()
    elif kind == "sma":
        result = series.rolling(window=window, min_periods=window).mean()
    else:
        raise ValueError(f"Unsupported moving-average kind: {kind}")
    return result.to_numpy(dtype=np.float64, copy=False)


def detect_crosses(fast: np.ndarray, slow: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if fast.shape != slow.shape:
        raise ValueError("fast and slow arrays must have the same shape")
    difference = fast - slow
    finite = np.isfinite(difference[1:]) & np.isfinite(difference[:-1])
    up = finite & (difference[:-1] <= 0.0) & (difference[1:] > 0.0)
    down = finite & (difference[:-1] >= 0.0) & (difference[1:] < 0.0)
    positions = np.flatnonzero(up | down) + 1
    directions = np.where(up[positions - 1], 1, -1).astype(np.int8)
    return positions.astype(np.int64, copy=False), directions


def datetime_index_nanoseconds(index: pd.DatetimeIndex) -> np.ndarray:
    """Return epoch nanoseconds across pandas 2.x and 3.x resolutions."""
    if hasattr(index, "as_unit"):
        return index.as_unit("ns").asi8
    return index.asi8


def normalize_epoch_nanoseconds(values: np.ndarray) -> np.ndarray:
    """Accept pandas-produced seconds/ms/us/ns integer epochs defensively."""
    output = np.asarray(values, dtype=np.int64)
    if output.size == 0:
        return output
    magnitude = int(np.max(np.abs(output)))
    if magnitude < 10**12:
        return output * 1_000_000_000
    if magnitude < 10**15:
        return output * 1_000_000
    if magnitude < 10**18:
        return output * 1_000
    return output


def completed_higher_tf_alignment(
    decision_ns: np.ndarray,
    directions: np.ndarray,
    higher_bar_index: pd.DatetimeIndex,
    higher_minutes: int,
    higher_fast: np.ndarray,
    higher_slow: np.ndarray,
) -> np.ndarray:
    decision_ns = normalize_epoch_nanoseconds(decision_ns)
    completed_ns = datetime_index_nanoseconds(higher_bar_index) + higher_minutes * 60 * 1_000_000_000
    positions = np.searchsorted(completed_ns, decision_ns, side="right") - 1
    valid = positions >= 0
    output = np.zeros(decision_ns.shape, dtype=bool)
    if not valid.any():
        return output
    selected = positions[valid]
    gaps = higher_fast[selected] - higher_slow[selected]
    finite = np.isfinite(gaps)
    aligned = finite & (directions[valid] * gaps > 0.0)
    output[np.flatnonzero(valid)] = aligned
    return output


def apply_confirmation(
    confirmation: str,
    positions: np.ndarray,
    directions: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
    volume_median: np.ndarray,
    fast: np.ndarray,
    slow: np.ndarray,
    higher_alignment: np.ndarray | None = None,
) -> np.ndarray:
    if positions.size == 0 or confirmation == "cross_only":
        return np.ones(positions.shape, dtype=bool)
    slow_slope = slow[positions] - slow[positions - 1]
    if confirmation == "slow_slope":
        return directions * slow_slope > 0.0
    if confirmation == "dual_slope":
        fast_slope = fast[positions] - fast[positions - 1]
        return (directions * slow_slope > 0.0) & (directions * fast_slope > 0.0)
    if confirmation == "slow_slope_volume":
        return (
            (directions * slow_slope > 0.0)
            & np.isfinite(volume_median[positions])
            & (volume[positions] >= volume_median[positions])
        )
    if confirmation == "higher_tf_same":
        if higher_alignment is None:
            return np.zeros(positions.shape, dtype=bool)
        return higher_alignment
    raise ValueError(f"Unsupported confirmation: {confirmation}")


def map_signal_entries(
    signal_bar_index: pd.DatetimeIndex,
    signal_positions: np.ndarray,
    directions: np.ndarray,
    timeframe_minutes: int,
    m1_index: pd.DatetimeIndex,
    maximum_entry_delay_minutes: int = 2,
) -> SignalEvents:
    decision_ns = (
        datetime_index_nanoseconds(signal_bar_index)[signal_positions]
        + timeframe_minutes * 60 * 1_000_000_000
    )
    m1_ns = datetime_index_nanoseconds(m1_index)
    entry_positions = np.searchsorted(m1_ns, decision_ns, side="left")
    in_range = entry_positions < m1_ns.size
    delay_ok = np.zeros(in_range.shape, dtype=bool)
    if in_range.any():
        valid_positions = entry_positions[in_range]
        delay = m1_ns[valid_positions] - decision_ns[in_range]
        delay_ok[in_range] = (
            (delay >= 0)
            & (delay <= maximum_entry_delay_minutes * 60 * 1_000_000_000)
        )
    keep = in_range & delay_ok
    return SignalEvents(
        entry_positions=entry_positions[keep].astype(np.int64, copy=False),
        directions=directions[keep].astype(np.int8, copy=False),
        source_indices=np.flatnonzero(keep).astype(np.int64, copy=False),
    )


def fixed_horizon_outcomes(
    frame: pd.DataFrame,
    events: SignalEvents,
    horizon_minutes: int,
    pip_size: float,
    proxy_spread_pips: float,
    maximum_exit_delay_minutes: int = 2,
) -> OutcomeBatch:
    count = events.entry_positions.size
    empty_float = np.full(count, np.nan, dtype=np.float64)
    empty_bool = np.zeros(count, dtype=bool)
    if count == 0:
        return OutcomeBatch(empty_float, empty_float.copy(), empty_bool, empty_bool.copy())

    m1_ns = datetime_index_nanoseconds(frame.index)
    target_ns = m1_ns[events.entry_positions] + horizon_minutes * 60 * 1_000_000_000
    exit_positions = np.searchsorted(m1_ns, target_ns, side="left")
    valid = exit_positions < m1_ns.size
    if valid.any():
        valid_exit = exit_positions[valid]
        delay = m1_ns[valid_exit] - target_ns[valid]
        valid_indices = np.flatnonzero(valid)
        valid[valid_indices] &= (
            (delay >= 0)
            & (delay <= maximum_exit_delay_minutes * 60 * 1_000_000_000)
        )

    gross = empty_float.copy()
    net = empty_float.copy()
    exact = empty_bool.copy()
    if not valid.any():
        return OutcomeBatch(gross, net, exact, valid)

    event_indices = np.flatnonzero(valid)
    entries = events.entry_positions[event_indices]
    exits = exit_positions[event_indices]
    directions = events.directions[event_indices].astype(np.float64)

    mid_entry = frame["open"].to_numpy(dtype=np.float64, copy=False)[entries]
    mid_exit = frame["close"].to_numpy(dtype=np.float64, copy=False)[exits]
    gross_values = directions * (mid_exit - mid_entry) / pip_size
    gross[event_indices] = gross_values
    net[event_indices] = gross_values - proxy_spread_pips

    bid_open = frame["bid_open"].to_numpy(dtype=np.float64, copy=False)[entries]
    ask_open = frame["ask_open"].to_numpy(dtype=np.float64, copy=False)[entries]
    bid_close = frame["bid_close"].to_numpy(dtype=np.float64, copy=False)[exits]
    ask_close = frame["ask_close"].to_numpy(dtype=np.float64, copy=False)[exits]
    exact_values = (
        np.isfinite(bid_open)
        & np.isfinite(ask_open)
        & np.isfinite(bid_close)
        & np.isfinite(ask_close)
    )
    if exact_values.any():
        selected = event_indices[exact_values]
        selected_directions = events.directions[selected]
        long_mask = selected_directions > 0
        exact_net = np.empty(selected.size, dtype=np.float64)
        exact_entries = events.entry_positions[selected]
        exact_exits = exit_positions[selected]
        if long_mask.any():
            exact_net[long_mask] = (
                frame["bid_close"].to_numpy(dtype=np.float64, copy=False)[
                    exact_exits[long_mask]
                ]
                - frame["ask_open"].to_numpy(dtype=np.float64, copy=False)[
                    exact_entries[long_mask]
                ]
            ) / pip_size
        if (~long_mask).any():
            exact_net[~long_mask] = (
                frame["bid_open"].to_numpy(dtype=np.float64, copy=False)[
                    exact_entries[~long_mask]
                ]
                - frame["ask_close"].to_numpy(dtype=np.float64, copy=False)[
                    exact_exits[~long_mask]
                ]
            ) / pip_size
        net[selected] = exact_net
        exact[selected] = True
    return OutcomeBatch(gross, net, exact, valid)


def split_for_entries(
    entry_positions: np.ndarray,
    total_rows: int,
    development_fraction: float,
    validation_fraction: float,
) -> np.ndarray:
    development_end = int(total_rows * development_fraction)
    validation_end = int(total_rows * (development_fraction + validation_fraction))
    return np.where(
        entry_positions < development_end,
        SEGMENT_INDEX["development"],
        np.where(
            entry_positions < validation_end,
            SEGMENT_INDEX["validation"],
            SEGMENT_INDEX["holdout"],
        ),
    ).astype(np.int8)


def prepare_bar_cache(
    frame: pd.DataFrame,
    timeframe_names: Iterable[str],
) -> dict[str, pd.DataFrame]:
    return {
        timeframe: resample_signal_bars(frame, TIMEFRAME_MINUTES[timeframe])
        for timeframe in sorted(set(timeframe_names), key=lambda item: TIMEFRAME_MINUTES[item])
    }


def prepare_ma_cache(
    bars: pd.DataFrame,
    required: Iterable[tuple[str, int]],
) -> dict[tuple[str, int], np.ndarray]:
    close = bars["close"].to_numpy(dtype=np.float64, copy=False)
    return {
        key: moving_average(close, key[0], key[1])
        for key in sorted(set(required), key=lambda item: (item[0], item[1]))
    }


def evaluate_pair(
    instrument: str,
    frame: pd.DataFrame,
    configurations_by_timeframe: dict[str, list[Configuration]],
    horizons_min: Sequence[int],
    cube: AggregateCube,
    development_fraction: float,
    validation_fraction: float,
    direction_mode: str = "trend",
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pip_size = infer_pip_size(instrument, frame)
    proxy_spread, spread_sample_count = estimate_spread_pips(frame, pip_size)
    required_timeframes = set(configurations_by_timeframe)
    for timeframe in configurations_by_timeframe:
        if any(
            config.confirmation == "higher_tf_same"
            for config in configurations_by_timeframe[timeframe]
        ):
            required_timeframes.add(HIGHER_TIMEFRAME[timeframe])
    bar_cache = prepare_bar_cache(frame, required_timeframes)

    pair_candidate_rows: list[dict[str, Any]] = []
    evaluated_configs = 0
    signal_count = 0
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
        ma_cache = prepare_ma_cache(bars, required_ma)
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
            higher_name = HIGHER_TIMEFRAME[timeframe]
            higher_bars = bar_cache[higher_name]
            higher_minutes = TIMEFRAME_MINUTES[higher_name]
            higher_cache = prepare_ma_cache(higher_bars, required_ma)

        grouped_configs: dict[tuple[str, str, int, int], list[Configuration]] = {}
        for config in configs:
            cross_key = (
                config.fast_kind,
                config.slow_kind,
                config.fast_window,
                config.slow_window,
            )
            grouped_configs.setdefault(cross_key, []).append(config)

        for cross_key, cross_configs in grouped_configs.items():
            fast_kind, slow_kind, fast_window, slow_window = cross_key
            fast = ma_cache[(fast_kind, fast_window)]
            slow = ma_cache[(slow_kind, slow_window)]
            positions, directions = detect_crosses(fast, slow)
            if direction_mode == "fade":
                directions = -directions
            evaluated_configs += len(cross_configs)
            if positions.size == 0:
                continue

            higher_alignment: np.ndarray | None = None
            if higher_bars is not None:
                decision_ns = (
                    bars.index.asi8[positions]
                    + TIMEFRAME_MINUTES[timeframe] * 60 * 1_000_000_000
                )
                higher_alignment = completed_higher_tf_alignment(
                    decision_ns,
                    directions,
                    higher_bars.index,
                    higher_minutes,
                    higher_cache[(fast_kind, fast_window)],
                    higher_cache[(slow_kind, slow_window)],
                )

            base_events = map_signal_entries(
                bars.index,
                positions,
                directions,
                TIMEFRAME_MINUTES[timeframe],
                frame.index,
            )
            if base_events.entry_positions.size == 0 or base_events.source_indices is None:
                continue
            source_indices = base_events.source_indices
            base_segments = split_for_entries(
                base_events.entry_positions,
                len(frame),
                development_fraction,
                validation_fraction,
            )
            outcomes_by_horizon = [
                fixed_horizon_outcomes(
                    frame,
                    base_events,
                    horizon_min,
                    pip_size,
                    proxy_spread,
                )
                for horizon_min in horizons_min
            ]

            for config in cross_configs:
                raw_confirmation_mask = apply_confirmation(
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
                confirmation_mask = raw_confirmation_mask[source_indices]
                config_signal_count = int(np.count_nonzero(confirmation_mask))
                signal_count += config_signal_count
                if config_signal_count == 0:
                    continue

                local_segment_metrics: dict[tuple[int, int], dict[str, Any]] = {}
                for horizon_index, outcomes in enumerate(outcomes_by_horizon):
                    for segment_index in range(len(SEGMENTS)):
                        mask = (
                            outcomes.valid
                            & confirmation_mask
                            & (base_segments == segment_index)
                        )
                        if not mask.any():
                            continue
                        gross = outcomes.gross_pips[mask]
                        net = outcomes.net_pips[mask]
                        exact = outcomes.exact_cost[mask]
                        selected_directions = base_events.directions[mask]
                        cube.update(
                            config.config_id,
                            horizon_index,
                            segment_index,
                            gross,
                            net,
                            exact,
                            selected_directions,
                        )
                        count = int(net.size)
                        local_segment_metrics[(horizon_index, segment_index)] = {
                            "n": count,
                            "avg_net_pips": float(net.mean()),
                            "win_rate": float(np.mean(net > 0.0)),
                            "exact_cost_fraction": float(np.mean(exact)),
                        }

                for horizon_index, horizon_min in enumerate(horizons_min):
                    validation = local_segment_metrics.get(
                        (horizon_index, SEGMENT_INDEX["validation"])
                    )
                    holdout = local_segment_metrics.get(
                        (horizon_index, SEGMENT_INDEX["holdout"])
                    )
                    if not validation or not holdout:
                        continue
                    row = config.as_dict()
                    row.update(
                        {
                            "instrument": instrument,
                            "direction_mode": direction_mode,
                            "horizon_min": int(horizon_min),
                            "validation_n": validation["n"],
                            "validation_avg_net_pips": validation["avg_net_pips"],
                            "validation_win_rate": validation["win_rate"],
                            "holdout_n": holdout["n"],
                            "holdout_avg_net_pips": holdout["avg_net_pips"],
                            "holdout_win_rate": holdout["win_rate"],
                            "holdout_exact_cost_fraction": holdout[
                                "exact_cost_fraction"
                            ],
                        }
                    )
                    pair_candidate_rows.append(row)

            del outcomes_by_horizon

        del ma_cache, higher_cache, grouped_configs
        gc.collect()

    valid_bid_ask = frame[["bid_open", "bid_close", "ask_open", "ask_close"]].notna().all(axis=1)
    inventory = {
        "instrument": instrument,
        "rows": int(len(frame)),
        "start": frame.index.min().isoformat(),
        "end": frame.index.max().isoformat(),
        "exact_bid_ask_rows": int(valid_bid_ask.sum()),
        "exact_bid_ask_fraction": float(valid_bid_ask.mean()),
        "proxy_spread_pips": proxy_spread,
        "spread_sample_count": spread_sample_count,
        "evaluated_configs": evaluated_configs,
        "mapped_signal_events_across_configs": signal_count,
    }
    return inventory, pair_candidate_rows


def select_pair_rows(
    rows: Sequence[dict[str, Any]],
    minimum_validation_events: int,
    minimum_holdout_events: int,
    limit: int,
) -> list[dict[str, Any]]:
    eligible = [
        dict(row)
        for row in rows
        if int(row["validation_n"]) >= minimum_validation_events
        and int(row["holdout_n"]) >= minimum_holdout_events
    ]
    for row in eligible:
        validation_avg = float(row["validation_avg_net_pips"])
        validation_win = float(row["validation_win_rate"])
        row["selection_score"] = (
            validation_avg
            * math.sqrt(float(row["validation_n"]))
            * max(0.0, 2.0 * validation_win - 0.8)
        )
    eligible.sort(
        key=lambda row: (
            row["selection_score"],
            row["validation_avg_net_pips"],
            row["validation_n"],
        ),
        reverse=True,
    )
    return eligible[:limit]


_WORKER_CONTEXT: dict[str, Any] | None = None


def initialize_pair_worker(context: dict[str, Any]) -> None:
    global _WORKER_CONTEXT
    _WORKER_CONTEXT = context


def evaluate_pair_worker(instrument: str) -> dict[str, Any]:
    if _WORKER_CONTEXT is None:
        raise RuntimeError("Pair worker was not initialized")
    context = _WORKER_CONTEXT
    path = instrument_source_path(Path(context["source_dir"]), instrument)
    started = time.perf_counter()
    try:
        frame = read_pair_frame(
            path,
            start=context["start"],
            end=context["end"],
        )
        if context["exact_only"]:
            exact_rows = frame[
                ["bid_open", "bid_close", "ask_open", "ask_close"]
            ].notna().all(axis=1)
            frame = frame.loc[exact_rows]
            if frame.empty:
                raise ValueError("No exact bid/ask rows matched the requested range")
        cube = AggregateCube(
            context["configuration_count"],
            len(context["horizons_min"]),
        )
        inventory, candidate_rows = evaluate_pair(
            instrument,
            frame,
            context["configs_by_timeframe"],
            context["horizons_min"],
            cube,
            context["development_fraction"],
            context["validation_fraction"],
            context["direction_mode"],
        )
        inventory["source_path"] = str(path.resolve())
        inventory["source_size_bytes"] = path.stat().st_size
        inventory["elapsed_sec"] = round(time.perf_counter() - started, 3)
        pair_top = select_pair_rows(
            candidate_rows,
            context["pair_minimum_validation_events"],
            context["pair_minimum_holdout_events"],
            context["pair_top_n"],
        )
        return {
            "ok": True,
            "instrument": instrument,
            "inventory": inventory,
            "pair_top": pair_top,
            "aggregate": cube.payload(),
        }
    except Exception as exc:
        return {
            "ok": False,
            "instrument": instrument,
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_sec": round(time.perf_counter() - started, 3),
        }


def build_selected_candidates(
    aggregate: pd.DataFrame,
    minimum_segment_events: int,
    minimum_pairs: int,
    top_n: int,
) -> pd.DataFrame:
    value_columns = [
        "n",
        "win_rate",
        "avg_gross_pips",
        "avg_net_pips",
        "total_net_signal_pips",
        "std_net_pips",
        "t_stat",
        "profit_factor",
        "exact_cost_fraction",
        "pair_count",
        "positive_pair_fraction",
        "mean_pair_avg_net_pips",
    ]
    index_columns = [
        "config_id",
        "timeframe",
        "fast_kind",
        "slow_kind",
        "fast_window",
        "slow_window",
        "confirmation",
        "horizon_min",
    ]
    wide = aggregate.pivot(index=index_columns, columns="segment", values=value_columns)
    wide.columns = [f"{segment}_{metric}" for metric, segment in wide.columns]
    wide = wide.reset_index()

    required = (
        (wide["development_n"] >= minimum_segment_events)
        & (wide["validation_n"] >= minimum_segment_events)
        & (wide["holdout_n"] >= minimum_segment_events)
        & (wide["development_pair_count"] >= minimum_pairs)
        & (wide["validation_pair_count"] >= minimum_pairs)
        & (wide["holdout_pair_count"] >= minimum_pairs)
    )
    eligible = wide.loc[required].copy()
    if eligible.empty:
        return eligible
    eligible["selection_floor_avg_net_pips"] = eligible[
        ["development_avg_net_pips", "validation_avg_net_pips"]
    ].min(axis=1)
    eligible["selection_floor_win_rate"] = eligible[
        ["development_win_rate", "validation_win_rate"]
    ].min(axis=1)
    eligible["selection_floor_positive_pair_fraction"] = eligible[
        ["development_positive_pair_fraction", "validation_positive_pair_fraction"]
    ].min(axis=1)
    eligible["selection_instability_pips"] = (
        eligible["development_avg_net_pips"] - eligible["validation_avg_net_pips"]
    ).abs()
    support = np.sqrt(
        np.minimum(eligible["development_n"], eligible["validation_n"]).astype(float)
    )
    eligible["selection_score"] = (
        eligible["selection_floor_avg_net_pips"]
        * support
        * eligible["selection_floor_positive_pair_fraction"]
        - 0.25 * eligible["selection_instability_pips"]
    )
    eligible = eligible.sort_values(
        [
            "selection_score",
            "selection_floor_avg_net_pips",
            "validation_positive_pair_fraction",
        ],
        ascending=False,
    )
    return eligible.head(top_n).reset_index(drop=True)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def safe_json_value(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def dataframe_records(frame: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    selected = frame.head(limit) if limit is not None else frame
    return [
        {key: safe_json_value(value) for key, value in row.items()}
        for row in selected.to_dict(orient="records")
    ]


def markdown_table(frame: pd.DataFrame) -> str:
    columns = [str(column) for column in frame.columns]

    def cell(value: Any) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ")

    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for values in frame.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(cell(value) for value in values) + " |")
    return "\n".join(lines)


def render_summary(
    manifest: dict[str, Any],
    selected: pd.DataFrame,
    output_path: Path,
) -> None:
    lines = [
        "# Moving-Average Crossover Sweep",
        "",
        f"- Generated: `{manifest['finished_at_utc']}`",
        f"- Source: `{manifest['source_dir']}`",
        f"- Pairs loaded: `{manifest['loaded_pair_count']}` / `{manifest['requested_pair_count']}`",
        f"- Configurations: `{manifest['configuration_count']:,}`",
        f"- Timeframes: `{', '.join(manifest['timeframes'])}`",
        f"- Horizons (minutes): `{', '.join(map(str, manifest['horizons_min']))}`",
        f"- MA kind pairs: `{', '.join(manifest['ma_kind_pairs'])}`",
        f"- Window pairs: `{manifest['window_pair_count']}`",
        f"- Confirmations: `{', '.join(manifest['confirmations'])}`",
        "",
        "## Method",
        "",
        "- Signal uses only a completed resampled candle.",
        "- Entry uses the next observed M1 open, with a maximum two-minute mapping delay.",
        "- Exit uses the first M1 close at the requested wall-clock horizon, with a maximum two-minute delay.",
        "- Recorded bid/ask is used when present; older midpoint-only history pays the pair's median recorded spread.",
        "- Development and validation select candidates. Holdout is not included in the selection score.",
        "- Outcomes are overlapping signal events, not an account-equity simulation.",
        "",
        "## Top Validation-Selected Candidates",
        "",
    ]
    if selected.empty:
        lines.append("No configuration met the requested support gates.")
    else:
        columns = [
            "timeframe",
            "fast_kind",
            "slow_kind",
            "fast_window",
            "slow_window",
            "confirmation",
            "horizon_min",
            "validation_n",
            "validation_avg_net_pips",
            "validation_win_rate",
            "holdout_n",
            "holdout_avg_net_pips",
            "holdout_win_rate",
            "holdout_positive_pair_fraction",
            "selection_score",
        ]
        visible = selected[columns].head(30).copy()
        for column in visible.select_dtypes(include=[np.number]).columns:
            visible[column] = visible[column].map(
                lambda value: f"{value:.4f}" if pd.notna(value) else ""
            )
        lines.append(markdown_table(visible))
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_utc(value: str) -> pd.Timestamp | None:
    if not value.strip():
        return None
    return pd.Timestamp(value).tz_localize("UTC") if pd.Timestamp(value).tzinfo is None else pd.Timestamp(value).tz_convert("UTC")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--instruments", default="all")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--preset", choices=("smoke", "wide", "exhaustive"), default="wide")
    parser.add_argument(
        "--window-pairs",
        type=parse_window_pairs,
        default=None,
        help="Optional fast:slow pairs that override the preset window grid.",
    )
    parser.add_argument("--timeframes", default=",".join(DEFAULT_TIMEFRAMES))
    parser.add_argument("--ma-pairs", default=",".join(DEFAULT_MA_PAIRS))
    parser.add_argument("--confirmations", default=",".join(DEFAULT_CONFIRMATIONS))
    parser.add_argument(
        "--horizons-min",
        type=parse_csv_ints,
        default=list(DEFAULT_HORIZONS_MIN),
    )
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument(
        "--exact-only",
        action="store_true",
        help="Keep only M1 rows with complete recorded bid/ask fields.",
    )
    parser.add_argument("--development-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--minimum-segment-events", type=int, default=100)
    parser.add_argument("--minimum-pairs", type=int, default=10)
    parser.add_argument("--pair-minimum-validation-events", type=int, default=20)
    parser.add_argument("--pair-minimum-holdout-events", type=int, default=20)
    parser.add_argument("--pair-top-n", type=int, default=20)
    parser.add_argument("--top-n", type=int, default=500)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--progress-every", type=int, default=1)
    parser.add_argument(
        "--direction-mode",
        choices=("trend", "fade"),
        default="trend",
        help="Trade with the crossover direction or use the exact inverse as a predeclared negative/control arm.",
    )
    args = parser.parse_args(argv)

    args.timeframes = [item.upper() for item in parse_csv_strings(args.timeframes)]
    invalid_timeframes = sorted(set(args.timeframes) - set(DEFAULT_TIMEFRAMES))
    if invalid_timeframes:
        parser.error(f"unsupported signal timeframes: {', '.join(invalid_timeframes)}")
    args.ma_pairs = parse_ma_pairs(args.ma_pairs)
    args.confirmations = parse_csv_strings(args.confirmations.lower())
    invalid_confirmations = sorted(set(args.confirmations) - set(DEFAULT_CONFIRMATIONS))
    if invalid_confirmations:
        parser.error(f"unsupported confirmations: {', '.join(invalid_confirmations)}")
    if not 0.0 < args.development_fraction < 1.0:
        parser.error("development fraction must be between zero and one")
    if not 0.0 < args.validation_fraction < 1.0:
        parser.error("validation fraction must be between zero and one")
    if args.development_fraction + args.validation_fraction >= 1.0:
        parser.error("development plus validation fractions must be less than one")
    if (
        args.max_pairs < 0
        or args.top_n <= 0
        or args.pair_top_n <= 0
        or args.workers <= 0
    ):
        parser.error("pair limit must be non-negative and result limits must be positive")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started_at = utc_now()
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or (DEFAULT_REPORT_ROOT / f"{args.preset}_{run_stamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    instruments = discover_instruments(args.source_dir, args.instruments, args.max_pairs)
    if not instruments:
        raise SystemExit(f"No *_M1.parquet or *_M1.csv files found in {args.source_dir}")
    window_pairs = args.window_pairs or window_pairs_for_preset(args.preset)
    configurations = build_configurations(
        args.timeframes,
        args.ma_pairs,
        window_pairs,
        args.confirmations,
    )
    configs_by_timeframe = {
        timeframe: [config for config in configurations if config.timeframe == timeframe]
        for timeframe in args.timeframes
    }
    cube = AggregateCube(len(configurations), len(args.horizons_min))
    start = parse_utc(args.start)
    end = parse_utc(args.end)

    print(
        json.dumps(
            {
                "event": "setup",
                "source_dir": str(args.source_dir.resolve()),
                "pair_count": len(instruments),
                "configuration_count": len(configurations),
                "window_pair_count": len(window_pairs),
                "horizons_min": args.horizons_min,
                "direction_mode": args.direction_mode,
            }
        ),
        flush=True,
    )

    inventory_rows: list[dict[str, Any]] = []
    pair_top_rows: list[dict[str, Any]] = []
    load_errors: dict[str, str] = {}
    worker_context = {
        "source_dir": str(args.source_dir.resolve()),
        "start": start,
        "end": end,
        "configuration_count": len(configurations),
        "configs_by_timeframe": configs_by_timeframe,
        "horizons_min": args.horizons_min,
        "development_fraction": args.development_fraction,
        "validation_fraction": args.validation_fraction,
        "pair_minimum_validation_events": args.pair_minimum_validation_events,
        "pair_minimum_holdout_events": args.pair_minimum_holdout_events,
        "pair_top_n": args.pair_top_n,
        "exact_only": args.exact_only,
        "direction_mode": args.direction_mode,
    }

    if args.workers == 1:
        initialize_pair_worker(worker_context)
        results: Iterable[dict[str, Any]] = (
            evaluate_pair_worker(instrument) for instrument in instruments
        )
        executor: concurrent.futures.ProcessPoolExecutor | None = None
    else:
        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=min(args.workers, len(instruments)),
            initializer=initialize_pair_worker,
            initargs=(worker_context,),
        )
        futures = {
            executor.submit(evaluate_pair_worker, instrument): instrument
            for instrument in instruments
        }
        results = (
            future.result()
            for future in concurrent.futures.as_completed(futures)
        )

    try:
        for pair_index, result in enumerate(results, start=1):
            instrument = str(result["instrument"])
            if result["ok"]:
                inventory_rows.append(result["inventory"])
                pair_top_rows.extend(result["pair_top"])
                cube.merge_payload(result["aggregate"])
                pair_elapsed = result["inventory"]["elapsed_sec"]
            else:
                load_errors[instrument] = str(result["error"])
                pair_elapsed = result["elapsed_sec"]
                print(
                    json.dumps(
                        {
                            "event": "pair_error",
                            "pair": instrument,
                            "error": load_errors[instrument],
                        }
                    ),
                    flush=True,
                )
            gc.collect()
            if args.progress_every > 0 and (
                pair_index % args.progress_every == 0 or pair_index == len(instruments)
            ):
                print(
                    json.dumps(
                        {
                            "event": "progress",
                            "pair_index": pair_index,
                            "pair_count": len(instruments),
                            "pair": instrument,
                            "loaded": len(inventory_rows),
                            "errors": len(load_errors),
                            "elapsed_sec": pair_elapsed,
                        }
                    ),
                    flush=True,
                )
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)

    inventory_rows.sort(key=lambda row: str(row["instrument"]))
    pair_top_rows.sort(
        key=lambda row: (
            str(row["instrument"]),
            -float(row["selection_score"]),
        )
    )

    if not inventory_rows:
        raise SystemExit("No pair histories were evaluated")

    aggregate = cube.frame(configurations, args.horizons_min)
    aggregate.insert(1, "direction_mode", args.direction_mode)
    selected = build_selected_candidates(
        aggregate,
        args.minimum_segment_events,
        args.minimum_pairs,
        args.top_n,
    )
    if not selected.empty:
        selected.insert(1, "direction_mode", args.direction_mode)
    pair_top = pd.DataFrame(pair_top_rows)
    inventory_frame = pd.DataFrame(inventory_rows)
    finished_at = utc_now()

    aggregate_path = output_dir / "aggregate_results.parquet"
    selected_path = output_dir / "top_validation_selected_candidates.csv"
    pair_top_path = output_dir / "pair_validation_selected_candidates.csv"
    inventory_path = output_dir / "data_inventory.csv"
    summary_path = output_dir / "SUMMARY.md"
    manifest_path = output_dir / "manifest.json"

    aggregate.to_parquet(aggregate_path, index=False)
    selected.to_csv(selected_path, index=False)
    pair_top.to_csv(pair_top_path, index=False)
    inventory_frame.to_csv(inventory_path, index=False)

    if selected.empty:
        selected_positive = selected.copy()
    else:
        selected_positive = selected.loc[
            (selected["holdout_avg_net_pips"] > 0.0)
            & (selected["holdout_win_rate"] > 0.50)
        ]
    manifest = {
        "schema_version": 1,
        "run_type": "moving_average_crossover_sweep",
        "preset": args.preset,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "source_dir": str(args.source_dir.resolve()),
        "requested_pair_count": len(instruments),
        "loaded_pair_count": len(inventory_rows),
        "load_errors": load_errors,
        "configuration_count": len(configurations),
        "direction_mode": args.direction_mode,
        "workers": args.workers,
        "window_pair_count": len(window_pairs),
        "window_pairs": [list(pair) for pair in window_pairs],
        "exact_only": args.exact_only,
        "timeframes": args.timeframes,
        "ma_kind_pairs": [f"{fast}-{slow}" for fast, slow in args.ma_pairs],
        "confirmations": args.confirmations,
        "horizons_min": args.horizons_min,
        "splits": {
            "development_fraction": args.development_fraction,
            "validation_fraction": args.validation_fraction,
            "holdout_fraction": 1.0
            - args.development_fraction
            - args.validation_fraction,
        },
        "selection_gates": {
            "minimum_segment_events": args.minimum_segment_events,
            "minimum_pairs": args.minimum_pairs,
            "top_n": args.top_n,
        },
        "candidate_count": int(len(selected)),
        "holdout_positive_candidate_count": int(len(selected_positive)),
        "holdout_positive_candidate_fraction": (
            float(len(selected_positive) / len(selected)) if len(selected) else None
        ),
        "method": {
            "signal": "completed resampled timeframe bar",
            "direction": args.direction_mode,
            "entry": "next observed M1 open, maximum mapping delay 2 minutes",
            "exit": "first M1 close at requested wall-clock horizon, maximum delay 2 minutes",
            "cost": "exact recorded bid/ask where available; otherwise pair median recorded spread proxy",
            "selection": "development and validation only; holdout excluded from selection score",
            "overlap": "signal events may overlap; total signal pips are not account P/L",
        },
        "outputs": {
            "aggregate_results": str(aggregate_path.resolve()),
            "selected_candidates": str(selected_path.resolve()),
            "pair_candidates": str(pair_top_path.resolve()),
            "data_inventory": str(inventory_path.resolve()),
            "summary": str(summary_path.resolve()),
        },
        "top_candidates": dataframe_records(selected, limit=25),
    }
    atomic_write_json(manifest_path, manifest)
    render_summary(manifest, selected, summary_path)
    print(
        json.dumps(
            {
                "event": "complete",
                "output_dir": str(output_dir.resolve()),
                "loaded_pairs": len(inventory_rows),
                "configurations": len(configurations),
                "aggregate_rows": len(aggregate),
                "selected_candidates": len(selected),
                "holdout_positive_candidates": len(selected_positive),
            }
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
