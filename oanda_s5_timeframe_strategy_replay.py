#!/usr/bin/env python3
"""Replay current strategy-lab lanes across S5-derived decision timeframes.

This is intentionally close to ``oanda_strategy_lab_historical_backtest`` but
lets the primary candle be S5/S10/S15/S30/M1/M5. The strategy lab still sees
the primary timeframe as ``M1`` because its feature names are historical; the
report records the actual timeframe used.
"""

from __future__ import annotations

import argparse
import bisect
import io
import json
import os
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    from oanda_practice_shadow_strategy_lab import (
        LaneSpec,
        augment_cross_sectional_features,
        build_features,
        classify_miss,
        default_lanes,
        economic_gates,
        evaluate_family,
        filter_lanes,
        infer_pip_size,
        theoretical_pips,
    )
except ModuleNotFoundError:
    from trad.oanda_practice_shadow_strategy_lab import (
        LaneSpec,
        augment_cross_sectional_features,
        build_features,
        classify_miss,
        default_lanes,
        economic_gates,
        evaluate_family,
        filter_lanes,
        infer_pip_size,
        theoretical_pips,
    )


ROOT = Path(__file__).resolve().parent
DEFAULT_S5_DIR = ROOT / "data" / "oanda_training_manager" / "candles_s5_bam"
DEFAULT_M1_DIR = ROOT / "data" / "oanda_training_manager" / "candles"
DEFAULT_M1_CACHE_DIR = (
    ROOT / "data" / "oanda_training_manager" / "candles_m1_parquet"
)
DEFAULT_REPORT_DIR = ROOT / "data" / "oanda_training_manager" / "reports"


@dataclass
class TimeframeHistory:
    instrument: str
    pip: float
    primary_seconds: int
    primary: pd.DataFrame
    higher: pd.DataFrame
    primary_times: list[pd.Timestamp]
    higher_times: list[pd.Timestamp]
    primary_candles: list[dict[str, Any]]
    higher_candles: list[dict[str, Any]]
    positions: dict[pd.Timestamp, int]
    execution: pd.DataFrame
    execution_seconds: int
    execution_times: list[pd.Timestamp]
    execution_positions: dict[pd.Timestamp, int]
    spread_mode: str
    max_exit_delay_seconds: int = 0

    def windows(self, decision_time: pd.Timestamp) -> dict[str, list[dict[str, Any]]] | None:
        position = self.positions.get(decision_time)
        if position is None and not self.positions:
            located = self.primary.index.get_indexer([decision_time])[0]
            position = int(located) if located >= 0 else None
        if position is None:
            return None
        primary_start = max(0, position - 359)
        primary = (
            self.primary_candles[primary_start : position + 1]
            if self.primary_candles
            else frame_to_candles(self.primary.iloc[primary_start : position + 1])
        )
        higher_cutoff = decision_time.floor("5min")
        if higher_cutoff + timedelta(minutes=5) > decision_time + timedelta(
            seconds=int(self.primary_seconds)
        ):
            higher_cutoff -= timedelta(minutes=5)
        higher_end = bisect.bisect_right(self.higher_times, higher_cutoff)
        higher_start = max(0, higher_end - 360)
        higher = (
            self.higher_candles[higher_start:higher_end]
            if self.higher_candles
            else frame_to_candles(self.higher.iloc[higher_start:higher_end])
        )
        return {"M1": primary, "M5": higher}

    def entry_position(self, decision_time: pd.Timestamp) -> int | None:
        entry_time = decision_time + timedelta(seconds=int(self.primary_seconds))
        position = self.execution_positions.get(entry_time)
        if position is None and not self.execution_positions:
            located = self.execution.index.get_indexer([entry_time])[0]
            return int(located) if located >= 0 else None
        return position

    def exit_position(self, entry_position: int, horizon_sec: int) -> int | None:
        if horizon_sec < self.execution_seconds or horizon_sec % self.execution_seconds:
            return None
        entry_time = self.execution_times[entry_position]
        exit_time = entry_time + timedelta(
            seconds=int(horizon_sec - self.execution_seconds)
        )
        position = self.execution_positions.get(exit_time)
        if position is None and not self.execution_positions:
            located = self.execution.index.get_indexer([exit_time])[0]
            position = int(located) if located >= 0 else None
        if position is not None or self.max_exit_delay_seconds <= 0:
            return position
        located = bisect.bisect_left(self.execution_times, exit_time)
        if located >= len(self.execution_times):
            return None
        candidate = pd.Timestamp(self.execution_times[located])
        delay_seconds = (candidate - pd.Timestamp(exit_time)).total_seconds()
        if 0.0 <= delay_seconds <= float(self.max_exit_delay_seconds):
            return int(located)
        return position


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def candle_time(value: pd.Timestamp) -> str:
    return value.isoformat().replace("+00:00", "Z")


def parse_timeframe(value: str) -> int:
    text = value.strip().lower()
    if text.startswith("s"):
        seconds = int(text[1:])
    elif text.startswith("m"):
        seconds = int(text[1:]) * 60
    elif text.startswith("h"):
        seconds = int(text[1:]) * 3600
    else:
        seconds = int(text)
    if seconds < 5 or seconds % 5:
        raise argparse.ArgumentTypeError("S5 historical replay supports multiples of 5 seconds")
    return seconds


def seconds_label(seconds: int) -> str:
    if seconds < 60:
        return f"S{seconds}"
    if seconds >= 3600 and seconds % 3600 == 0:
        return f"H{seconds // 3600}"
    return f"M{seconds // 60}"


def sample_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"n": 0, "avg": 0.0, "median": 0.0, "win_rate": 0.0, "best": 0.0, "worst": 0.0}
    return {
        "n": len(values),
        "avg": round(statistics.fmean(values), 3),
        "median": round(statistics.median(values), 3),
        "win_rate": round(100.0 * sum(1 for value in values if value > 0.0) / len(values), 1),
        "best": round(max(values), 3),
        "worst": round(min(values), 3),
    }


def frame_to_candles(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in frame.itertuples():
        rows.append(
            {
                "time": candle_time(pd.Timestamp(row.Index)),
                "complete": True,
                "volume": float(row.volume),
                "mid": {"o": float(row.open), "h": float(row.high), "l": float(row.low), "c": float(row.close)},
                "bid": {
                    "o": float(row.bid_open),
                    "h": float(row.bid_high),
                    "l": float(row.bid_low),
                    "c": float(row.bid_close),
                },
                "ask": {
                    "o": float(row.ask_open),
                    "h": float(row.ask_high),
                    "l": float(row.ask_low),
                    "c": float(row.ask_close),
                },
            }
        )
    return rows


def resample_s5(frame: pd.DataFrame, seconds: int) -> pd.DataFrame:
    rule = f"{seconds}s"
    out = frame.resample(rule, label="left", closed="left").agg(
        {
            "mid_open": "first",
            "mid_high": "max",
            "mid_low": "min",
            "mid_close": "last",
            "volume": "sum",
            "bid_open": "first",
            "bid_high": "max",
            "bid_low": "min",
            "bid_close": "last",
            "ask_open": "first",
            "ask_high": "max",
            "ask_low": "min",
            "ask_close": "last",
        }
    )
    out = out.rename(columns={"mid_open": "open", "mid_high": "high", "mid_low": "low", "mid_close": "close"})
    return out.dropna(subset=["open", "high", "low", "close", "bid_open", "bid_close", "ask_open", "ask_close"])


def read_csv_tail(path: Path, usecols: list[str], row_count: int) -> pd.DataFrame:
    """Read a bounded CSV tail without materializing a multi-year file first."""
    if row_count <= 0:
        return pd.read_csv(path, usecols=usecols)

    block_size = 4 * 1024 * 1024
    with path.open("rb") as handle:
        header = handle.readline().rstrip(b"\r\n")
        data_start = handle.tell()
        handle.seek(0, 2)
        position = handle.tell()
        blocks: list[bytes] = []
        newline_count = 0
        while position > data_start and newline_count <= row_count:
            size = min(block_size, position - data_start)
            position -= size
            handle.seek(position)
            block = handle.read(size)
            blocks.append(block)
            newline_count += block.count(b"\n")

    payload = b"".join(reversed(blocks))
    if position > data_start:
        first_newline = payload.find(b"\n")
        payload = payload[first_newline + 1 :] if first_newline >= 0 else b""
    rows = payload.splitlines()[-row_count:]
    bounded_csv = io.BytesIO(header + b"\n" + b"\n".join(rows) + b"\n")
    return pd.read_csv(bounded_csv, usecols=usecols)


def read_parquet_tail(path: Path, row_count: int) -> pd.DataFrame:
    """Read only the parquet row groups needed for a bounded tail."""
    if row_count <= 0:
        return pd.read_parquet(path)
    import pyarrow.parquet as parquet

    source = parquet.ParquetFile(path)
    selected: list[int] = []
    selected_rows = 0
    for index in range(source.num_row_groups - 1, -1, -1):
        selected.append(index)
        selected_rows += source.metadata.row_group(index).num_rows
        if selected_rows >= row_count:
            break
    table = source.read_row_groups(sorted(selected))
    return table.to_pandas().tail(row_count).copy()


def load_history(
    s5_dir: Path,
    instrument: str,
    primary_seconds: int,
    tail_rows: int,
    compact: bool = False,
) -> TimeframeHistory:
    path = s5_dir / f"{instrument}_S5.parquet"
    if not path.is_file():
        raise FileNotFoundError(path)
    if tail_rows > 0:
        raw_rows = max(
            tail_rows * max(1, primary_seconds // 5) + 10_000,
            tail_rows,
        )
        frame = read_parquet_tail(path, raw_rows)
    else:
        frame = pd.read_parquet(path)
    frame["time_utc"] = pd.to_datetime(frame["dt"] if "dt" in frame.columns else frame["time"], errors="coerce", utc=True)
    needed = [
        "mid_open",
        "mid_high",
        "mid_low",
        "mid_close",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "volume",
    ]
    for column in needed:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["time_utc", *needed]).sort_values("time_utc").drop_duplicates("time_utc", keep="last")
    frame = frame.set_index("time_utc")
    execution = resample_s5(frame, 5)
    primary = resample_s5(frame, primary_seconds)
    higher = resample_s5(frame, 300)
    if tail_rows > 0:
        primary = primary.tail(tail_rows)
    if len(primary) < 120 or len(higher) < 40:
        raise ValueError("insufficient resampled candles")
    primary_times = primary.index if compact else list(primary.index)
    higher_times = higher.index if compact else list(higher.index)
    execution_times = execution.index if compact else list(execution.index)
    return TimeframeHistory(
        instrument=instrument,
        pip=infer_pip_size(instrument),
        primary_seconds=primary_seconds,
        primary=primary,
        higher=higher,
        primary_times=primary_times,
        higher_times=higher_times,
        primary_candles=[] if compact else frame_to_candles(primary),
        higher_candles=[] if compact else frame_to_candles(higher),
        positions={} if compact else {timestamp: index for index, timestamp in enumerate(primary_times)},
        execution=execution,
        execution_seconds=5,
        execution_times=execution_times,
        execution_positions=(
            {}
            if compact
            else {timestamp: index for index, timestamp in enumerate(execution_times)}
        ),
        spread_mode="observed_bid_ask",
    )


def fallback_spread_pips(instrument: str) -> float:
    base, quote = instrument.upper().split("_", 1)
    liquid = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
    if base in liquid and quote in liquid:
        return 1.5 if "USD" in {base, quote} else 2.5
    if {base, quote} & {"HKD", "SGD", "CNH", "PLN", "CZK", "HUF"}:
        return 8.0
    return 15.0


def load_m1_history(
    m1_dir: Path,
    instrument: str,
    primary_seconds: int,
    tail_rows: int,
    cache_dir: Path | None = None,
    compact: bool = False,
) -> TimeframeHistory:
    if primary_seconds < 60 or primary_seconds % 60:
        raise ValueError("deep M1 history supports M1 and slower input timeframes")
    path = m1_dir / f"{instrument}_M1.csv"
    cache_path = (
        None
        if cache_dir is None
        else Path(cache_dir) / f"{instrument}_M1.parquet"
    )
    cache_current = bool(
        cache_path is not None
        and cache_path.is_file()
        and (
            not path.is_file()
            or cache_path.stat().st_mtime_ns >= path.stat().st_mtime_ns
        )
    )
    requested = [
        "time",
        "datetime",
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
    ]
    if cache_current:
        if tail_rows > 0:
            raw_rows = tail_rows * max(1, primary_seconds // 60) + 10_000
            frame = read_parquet_tail(cache_path, raw_rows)
        else:
            frame = pd.read_parquet(cache_path)
    else:
        if not path.is_file():
            raise FileNotFoundError(path)
        available = set(pd.read_csv(path, nrows=0).columns)
        usecols = [column for column in requested if column in available]
        if tail_rows > 0:
            raw_rows = tail_rows * max(1, primary_seconds // 60) + 10_000
            frame = read_csv_tail(path, usecols, raw_rows)
        else:
            frame = pd.read_csv(path, usecols=usecols)
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = cache_path.with_suffix(
                    cache_path.suffix + f".{os.getpid()}.tmp"
                )
                try:
                    frame.to_parquet(
                        temporary,
                        index=False,
                        compression="zstd",
                    )
                    os.replace(temporary, cache_path)
                except (OSError, ValueError):
                    temporary.unlink(missing_ok=True)
    time_column = "datetime" if "datetime" in frame else "time"
    frame["time_utc"] = pd.to_datetime(frame[time_column], errors="coerce", utc=True)
    numeric_columns = [column for column in requested if column not in {"time", "datetime"}]
    for column in numeric_columns:
        if column not in frame:
            frame[column] = math.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["time_utc", "open", "high", "low", "close", "volume"])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    pip = infer_pip_size(instrument)
    observed_spread = (frame["ask_open"] - frame["bid_open"]) / pip
    provided_spread = frame["spread_pips"].where(frame["spread_pips"] > 0.0)
    valid_spreads = pd.concat(
        [observed_spread.where(observed_spread > 0.0), provided_spread],
        ignore_index=True,
    ).dropna()
    modeled_spread = (
        float(valid_spreads.median()) if not valid_spreads.empty else fallback_spread_pips(instrument)
    )
    spread = provided_spread.fillna(modeled_spread).clip(lower=0.1)
    half_spread = spread * pip / 2.0
    for side, sign in (("bid", -1.0), ("ask", 1.0)):
        for field in ("open", "high", "low", "close"):
            column = f"{side}_{field}"
            frame[column] = frame[column].fillna(frame[field] + sign * half_spread)
    frame = frame.rename(
        columns={
            "open": "mid_open",
            "high": "mid_high",
            "low": "mid_low",
            "close": "mid_close",
        }
    )
    execution = resample_s5(frame, 60)
    primary = execution if primary_seconds == 60 else resample_s5(frame, primary_seconds)
    higher = primary if primary_seconds == 300 else resample_s5(frame, 300)
    if tail_rows > 0:
        primary = primary.tail(tail_rows)
    if len(primary) < 120 or len(higher) < 40:
        raise ValueError("insufficient resampled candles")
    primary_times = primary.index if compact else list(primary.index)
    higher_times = higher.index if compact else list(higher.index)
    execution_times = execution.index if compact else list(execution.index)
    observed_rows = int((observed_spread > 0.0).sum())
    return TimeframeHistory(
        instrument=instrument,
        pip=pip,
        primary_seconds=primary_seconds,
        primary=primary,
        higher=higher,
        primary_times=primary_times,
        higher_times=higher_times,
        primary_candles=[] if compact else frame_to_candles(primary),
        higher_candles=[] if compact else frame_to_candles(higher),
        positions={} if compact else {timestamp: index for index, timestamp in enumerate(primary_times)},
        execution=execution,
        execution_seconds=60,
        execution_times=execution_times,
        execution_positions=(
            {}
            if compact
            else {timestamp: index for index, timestamp in enumerate(execution_times)}
        ),
        spread_mode=(
            f"observed_and_median_modeled:{observed_rows}/{len(frame)}:median={modeled_spread:.3f}"
        ),
    )


def load_history_with_fallback(
    *,
    source: str,
    m1_dir: Path,
    m1_cache_dir: Path,
    s5_dir: Path,
    instrument: str,
    timeframe_seconds: int,
    tail_rows: int,
    compact: bool = False,
) -> tuple[TimeframeHistory, str, str]:
    if source != "m1":
        return (
            load_history(
                s5_dir,
                instrument,
                timeframe_seconds,
                tail_rows,
                **({"compact": True} if compact else {}),
            ),
            "s5_observed",
            "",
        )
    last_error: Exception | None = None
    attempts = 0
    for attempts in range(1, 3):
        try:
            return (
                load_m1_history(
                    m1_dir,
                    instrument,
                    timeframe_seconds,
                    tail_rows,
                    m1_cache_dir,
                    **({"compact": True} if compact else {}),
                ),
                "m1_deep",
                "",
            )
        except PermissionError as error:
            last_error = error
            if attempts == 1:
                time.sleep(0.5)
                continue
            break
        except (OSError, ValueError, EOFError, pd.errors.ParserError) as error:
            last_error = error
            break
    assert last_error is not None
    history = load_history(
        s5_dir,
        instrument,
        timeframe_seconds,
        tail_rows,
        **({"compact": True} if compact else {}),
    )
    warning = f"{type(last_error).__name__}: {last_error}; m1_attempts={attempts}"
    return history, "s5_integrity_fallback", warning


def parse_instruments(value: str) -> list[str]:
    return sorted({item.strip().upper().replace("/", "_") for item in value.split(",") if item.strip()})


def parse_horizons(value: str) -> list[int]:
    return sorted({int(item.strip()) for item in value.split(",") if item.strip()})


def select_times(
    histories: dict[str, TimeframeHistory],
    step_seconds: int,
    max_cycles: int,
    max_horizon_sec: int = 0,
    sampling: str = "uniform",
) -> list[pd.Timestamp]:
    times = sorted({timestamp for history in histories.values() for timestamp in history.primary_times})
    latest_complete = min(
        history.execution_times[-1]
        - timedelta(seconds=int(history.primary_seconds + max_horizon_sec))
        for history in histories.values()
    )
    selected = [
        timestamp
        for timestamp in times
        if timestamp <= latest_complete
        and int(timestamp.timestamp()) % step_seconds == 0
    ]
    if max_cycles <= 0 or len(selected) <= max_cycles:
        return selected
    if sampling == "latest":
        return selected[-max_cycles:]
    if sampling != "uniform":
        raise ValueError(f"unsupported decision-time sampling mode: {sampling}")
    if max_cycles == 1:
        return [selected[-1]]
    indices = {
        round(index * (len(selected) - 1) / (max_cycles - 1))
        for index in range(max_cycles)
    }
    return [selected[index] for index in sorted(indices)]


def setup_outcomes(history: TimeframeHistory, entry_position: int, direction: str, horizons: Iterable[int]) -> dict[int, float]:
    entry = history.execution.iloc[entry_position]
    output: dict[int, float] = {}
    for horizon in horizons:
        if horizon < history.execution_seconds or horizon % history.execution_seconds:
            continue
        exit_position = history.exit_position(entry_position, horizon)
        if exit_position is None:
            continue
        exit_row = history.execution.iloc[exit_position]
        output[horizon] = round(
            theoretical_pips(
                direction,
                history.pip,
                float(entry.bid_open),
                float(entry.ask_open),
                float(exit_row.bid_close),
                float(exit_row.ask_close),
            ),
            3,
        )
    return output


def run(histories: dict[str, TimeframeHistory], lanes: list[LaneSpec], times: list[pd.Timestamp], horizons: list[int], progress_every: int) -> dict[str, Any]:
    lane_counts: dict[str, Counter[str]] = defaultdict(Counter)
    lane_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    lane_values: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    family_values: dict[tuple[str, int], list[float]] = defaultdict(list)
    feature_errors: Counter[str] = Counter()
    for cycle, decision_time in enumerate(times, start=1):
        feature_cache: dict[str, dict[str, Any]] = {}
        entries: dict[str, int] = {}
        for instrument, history in histories.items():
            windows = history.windows(decision_time)
            entry = history.entry_position(decision_time)
            if windows is None or entry is None:
                continue
            features, reason = build_features(instrument, windows, history.pip)
            if features is None:
                feature_errors[reason] += 1
                continue
            feature_cache[instrument] = features
            entries[instrument] = entry
        augment_cross_sectional_features(feature_cache)
        for lane in lanes:
            counts = lane_counts[lane.lane_id]
            reasons = lane_reasons[lane.lane_id]
            for instrument, features in feature_cache.items():
                counts["evaluated"] += 1
                setup = evaluate_family(lane, features)
                if setup.direction is None:
                    reasons[setup.reason] += 1
                    continue
                counts["raw_setup"] += 1
                history = histories[instrument]
                entry = history.execution.iloc[entries[instrument]]
                blockers, gates = economic_gates(lane, features, float(entry.bid_open), float(entry.ask_open), setup.signal)
                category = classify_miss(lane, gates) if blockers else "signal"
                counts[category] += 1
                reasons.update(blockers)
                for horizon, pips in setup_outcomes(history, entries[instrument], setup.direction, horizons).items():
                    lane_values[(lane.lane_id, category, horizon)].append(pips)
                    if category == "signal":
                        family_values[(lane.family, horizon)].append(pips)
        if progress_every and (cycle % progress_every == 0 or cycle == len(times)):
            print(f"[timeframe] {cycle:,}/{len(times):,} cycles", flush=True)

    lane_summaries = []
    for lane in lanes:
        counts = lane_counts[lane.lane_id]
        lane_summaries.append(
            {
                "lane_id": lane.lane_id,
                "family": lane.family,
                "profile": lane.profile,
                "evaluated": counts["evaluated"],
                "raw_setups": counts["raw_setup"],
                "accepted": counts["signal"],
                "near_threshold": counts["near_threshold"],
                "hard_reject": counts["hard_reject"],
                "top_reasons": lane_reasons[lane.lane_id].most_common(8),
                "horizons": {
                    str(h): {
                        "accepted": sample_stats(lane_values[(lane.lane_id, "signal", h)]),
                        "near_threshold": sample_stats(lane_values[(lane.lane_id, "near_threshold", h)]),
                        "hard_reject": sample_stats(lane_values[(lane.lane_id, "hard_reject", h)]),
                    }
                    for h in horizons
                },
            }
        )
    lane_summaries.sort(key=lambda row: (row["horizons"][str(max(horizons))]["accepted"]["avg"], row["accepted"]), reverse=True)
    families = [
        {"family": family, "horizons": {str(h): sample_stats(family_values[(family, h)]) for h in horizons}}
        for family in sorted({lane.family for lane in lanes})
    ]
    return {
        "generated_utc": utc_now(),
        "instrument_count": len(histories),
        "instruments": sorted(histories),
        "cycle_count": len(times),
        "first_decision_candle": candle_time(times[0]) if times else "",
        "last_decision_candle": candle_time(times[-1]) if times else "",
        "lane_count": len(lanes),
        "horizons_sec": horizons,
        "feature_errors": dict(feature_errors),
        "raw_setup_count": sum(row["raw_setups"] for row in lane_summaries),
        "accepted_count": sum(row["accepted"] for row in lane_summaries),
        "near_threshold_count": sum(row["near_threshold"] for row in lane_summaries),
        "hard_reject_count": sum(row["hard_reject"] for row in lane_summaries),
        "lane_summaries": lane_summaries,
        "family_summaries": families,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--s5-dir", type=Path, default=DEFAULT_S5_DIR)
    parser.add_argument("--m1-dir", type=Path, default=DEFAULT_M1_DIR)
    parser.add_argument(
        "--m1-cache-dir",
        type=Path,
        default=DEFAULT_M1_CACHE_DIR,
        help="Compact full-history cache reused across M1-and-slower matrix runs",
    )
    parser.add_argument(
        "--source",
        choices=("auto", "s5", "m1"),
        default="auto",
        help="auto uses deep M1 history for M1+ inputs and observed S5 for subminute inputs",
    )
    parser.add_argument("--instruments", type=parse_instruments, required=True)
    parser.add_argument("--timeframe", type=parse_timeframe, required=True)
    parser.add_argument("--families", default="")
    parser.add_argument("--profiles", default="strict,balanced,fast,loose")
    parser.add_argument(
        "--horizons-sec",
        type=parse_horizons,
        default=parse_horizons(
            "30,60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400"
        ),
    )
    parser.add_argument("--step-seconds", type=int, default=0)
    parser.add_argument("--max-cycles", type=int, default=1000)
    parser.add_argument(
        "--sampling",
        choices=("uniform", "latest"),
        default="uniform",
        help="uniform spans the full available history; latest is a recency-only diagnostic",
    )
    parser.add_argument("--tail-rows", type=int, default=0)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--progress-every", type=int, default=100)
    args = parser.parse_args()
    source = args.source
    if source == "auto":
        source = "m1" if args.timeframe >= 60 else "s5"
    execution_seconds = 60 if source == "m1" else 5
    horizons = [
        horizon
        for horizon in args.horizons_sec
        if horizon >= execution_seconds and horizon % execution_seconds == 0
    ]
    omitted_horizons = sorted(set(args.horizons_sec) - set(horizons))
    if not horizons:
        raise SystemExit(
            f"no horizons can be scored from the {seconds_label(execution_seconds)} execution clock"
        )
    lanes = filter_lanes(default_lanes(), args.families or ",".join(sorted({lane.family for lane in default_lanes()})), args.profiles)
    histories = {}
    history_sources: dict[str, str] = {}
    history_load_warnings: dict[str, str] = {}
    for index, instrument in enumerate(args.instruments, start=1):
        print(f"[load] {index}/{len(args.instruments)} {instrument}", flush=True)
        history, history_source, warning = load_history_with_fallback(
            source=source,
            m1_dir=args.m1_dir,
            m1_cache_dir=args.m1_cache_dir,
            s5_dir=args.s5_dir,
            instrument=instrument,
            timeframe_seconds=args.timeframe,
            tail_rows=args.tail_rows,
        )
        histories[instrument] = history
        history_sources[instrument] = history_source
        if warning:
            history_load_warnings[instrument] = warning
            print(
                f"[load-warning] {instrument} source={history_source} {warning}",
                flush=True,
            )
    step = args.step_seconds or args.timeframe
    times = select_times(
        histories,
        step,
        args.max_cycles,
        max(horizons),
        sampling=args.sampling,
    )
    report = run(histories, lanes, times, horizons, args.progress_every)
    report["sampling_mode"] = args.sampling
    report["history_coverage"] = "full_span" if args.sampling == "uniform" else "latest_tail"
    report["source"] = "m1_csv_deep_history" if source == "m1" else "s5_parquet_resampled"
    report["m1_cache_dir"] = str(args.m1_cache_dir.resolve())
    report["history_sources"] = history_sources
    report["history_load_warnings"] = history_load_warnings
    report["execution_seconds"] = execution_seconds
    report["requested_horizons_sec"] = args.horizons_sec
    report["omitted_horizons_sec"] = omitted_horizons
    report["spread_modes"] = {
        instrument: history.spread_mode for instrument, history in histories.items()
    }
    report["timeframe"] = seconds_label(args.timeframe)
    report["timeframe_seconds"] = args.timeframe
    report["step_seconds"] = step
    output = args.output or DEFAULT_REPORT_DIR / f"strategy_lab_timeframe_{seconds_label(args.timeframe).lower()}_{utc_now().replace(':', '').replace('-', '')}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output": str(output.resolve()), "timeframe": report["timeframe"], "cycles": report["cycle_count"], "accepted": report["accepted_count"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
