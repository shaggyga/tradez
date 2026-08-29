#!/usr/bin/env python3
"""Backtest strategy-lab lanes and ensembles on local OANDA bid/ask data.

No account, token, or broker call is required. Decisions use completed candles
through time T, entries use the exact next M1 bid/ask open, and mark-to-market
outcomes use exact future M1 bid/ask closes. This mirrors the live shadow
outcome convention without pretending that minute data is tick-level replay.
"""

from __future__ import annotations

import argparse
import bisect
import gc
import json
import math
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    from oanda_practice_shadow_strategy_lab import (
        LaneSpec,
        augment_cross_sectional_features,
        augment_supervised_return_features,
        build_features,
        classify_miss,
        default_lanes,
        economic_gates,
        evaluate_family,
        filter_lanes,
        infer_pip_size,
        theoretical_pips,
    )
    from oanda_strategy_lab_ensemble_replay import (
        DEFAULT_ENSEMBLES,
        EnsembleSpec,
        atomic_write_json,
        atomic_write_jsonl,
        attach_outcomes,
        candidate_for_group,
        sample_stats,
    )
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_practice_shadow_strategy_lab import (
        LaneSpec,
        augment_cross_sectional_features,
        augment_supervised_return_features,
        build_features,
        classify_miss,
        default_lanes,
        economic_gates,
        evaluate_family,
        filter_lanes,
        infer_pip_size,
        theoretical_pips,
    )
    from trad.oanda_strategy_lab_ensemble_replay import (
        DEFAULT_ENSEMBLES,
        EnsembleSpec,
        atomic_write_json,
        atomic_write_jsonl,
        attach_outcomes,
        candidate_for_group,
        sample_stats,
    )


ROOT = Path(__file__).resolve().parent
DEFAULT_CANDLE_DIR = ROOT / "data" / "oanda_training_manager" / "candles"
DEFAULT_S5_DIR = ROOT / "data" / "oanda_training_manager" / "candles_s5_bam"
DEFAULT_REPORT_DIR = ROOT / "data" / "oanda_training_manager" / "reports"
M1_COLUMNS = (
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
)


@dataclass
class PairHistory:
    instrument: str
    pip: float
    frame: pd.DataFrame
    m1_times: list[pd.Timestamp]
    m1_candles: list[dict[str, Any]]
    m5_times: list[pd.Timestamp]
    m5_candles: list[dict[str, Any]]
    positions: dict[pd.Timestamp, int]

    def candle_windows(self, decision_time: pd.Timestamp) -> dict[str, list[dict[str, Any]]] | None:
        position = self.positions.get(decision_time)
        if position is None:
            return None
        m1_start = max(0, position - 119)
        m1 = self.m1_candles[m1_start : position + 1]
        # At the close of M1 candle T, an M5 candle is complete only when its
        # start is at or before T-4 minutes.
        m5_cutoff = decision_time - pd.Timedelta(minutes=4)
        m5_end = bisect.bisect_right(self.m5_times, m5_cutoff)
        m5 = self.m5_candles[max(0, m5_end - 360) : m5_end]
        return {"M1": m1, "M5": m5}

    def entry_position(self, decision_time: pd.Timestamp) -> int | None:
        return self.positions.get(decision_time + pd.Timedelta(minutes=1))

    def exit_position(self, entry_position: int, horizon_sec: int) -> int | None:
        if horizon_sec <= 0 or horizon_sec % 60:
            return None
        entry_time = self.m1_times[entry_position]
        exit_bar_time = entry_time + pd.Timedelta(seconds=horizon_sec - 60)
        return self.positions.get(exit_bar_time)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def candle_time(value: pd.Timestamp) -> str:
    return value.isoformat().replace("+00:00", "Z")


def frame_to_candles(frame: pd.DataFrame) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    for row in frame.itertuples():
        bid_open = float(getattr(row, "bid_open", float("nan")))
        bid_close = float(getattr(row, "bid_close", float("nan")))
        ask_open = float(getattr(row, "ask_open", float("nan")))
        ask_close = float(getattr(row, "ask_close", float("nan")))
        candles.append(
            {
                "time": candle_time(pd.Timestamp(row.Index)),
                "complete": True,
                "volume": float(row.volume),
                "mid": {
                    "o": float(row.open),
                    "h": float(row.high),
                    "l": float(row.low),
                    "c": float(row.close),
                },
                "bid": {
                    "o": bid_open,
                    "h": float(getattr(row, "bid_high", bid_open)),
                    "l": float(getattr(row, "bid_low", bid_close)),
                    "c": bid_close,
                },
                "ask": {
                    "o": ask_open,
                    "h": float(getattr(row, "ask_high", ask_close)),
                    "l": float(getattr(row, "ask_low", ask_open)),
                    "c": ask_close,
                },
            }
        )
    return candles


def build_pair_history(instrument: str, frame: pd.DataFrame) -> PairHistory:
    clean = frame.copy()
    clean.index = pd.to_datetime(clean.index, errors="coerce", utc=True)
    clean = clean[~clean.index.isna()].sort_index()
    clean = clean[~clean.index.duplicated(keep="last")]
    if "bid_high" not in clean.columns and {"bid_open", "bid_close"} <= set(clean.columns):
        clean["bid_high"] = clean[["bid_open", "bid_close"]].max(axis=1)
    if "bid_low" not in clean.columns and {"bid_open", "bid_close"} <= set(clean.columns):
        clean["bid_low"] = clean[["bid_open", "bid_close"]].min(axis=1)
    if "ask_high" not in clean.columns and {"ask_open", "ask_close"} <= set(clean.columns):
        clean["ask_high"] = clean[["ask_open", "ask_close"]].max(axis=1)
    if "ask_low" not in clean.columns and {"ask_open", "ask_close"} <= set(clean.columns):
        clean["ask_low"] = clean[["ask_open", "ask_close"]].min(axis=1)
    required = [column for column in M1_COLUMNS if column != "datetime"]
    clean = clean.dropna(subset=required)
    if clean.empty:
        raise ValueError(f"No executable M1 rows for {instrument}")

    m5 = clean.resample("5min", label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
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
    m5 = m5.dropna(subset=["open", "high", "low", "close"])
    m1_times = list(clean.index)
    return PairHistory(
        instrument=instrument,
        pip=infer_pip_size(instrument),
        frame=clean,
        m1_times=m1_times,
        m1_candles=frame_to_candles(clean),
        m5_times=list(m5.index),
        m5_candles=frame_to_candles(m5),
        positions={timestamp: index for index, timestamp in enumerate(m1_times)},
    )


def read_csv_tail(path: Path, columns: list[str], tail_rows: int) -> pd.DataFrame:
    if tail_rows <= 0:
        try:
            return pd.read_csv(path, usecols=columns, engine="pyarrow")
        except (ImportError, OSError, PermissionError, ValueError):
            return pd.read_csv(path, usecols=columns)
    chunks: list[pd.DataFrame] = []
    total = 0
    with pd.read_csv(path, usecols=columns, chunksize=max(10_000, tail_rows)) as reader:
        for chunk in reader:
            chunks.append(chunk)
            total += len(chunk)
            while len(chunks) > 3 and total - len(chunks[0]) >= tail_rows:
                total -= len(chunks[0])
                chunks.pop(0)
    if not chunks:
        return pd.DataFrame(columns=columns)
    return pd.concat(chunks, ignore_index=True).tail(tail_rows).reset_index(drop=True)


def available_csv_columns(path: Path) -> list[str]:
    return list(pd.read_csv(path, nrows=0).columns)


def load_pair_history(candle_dir: Path, instrument: str, tail_rows: int = 0) -> PairHistory:
    path = candle_dir / f"{instrument}_M1.csv"
    if not path.is_file():
        raise FileNotFoundError(f"Missing M1 candle file: {path}")
    last_error: PermissionError | None = None
    for attempt in range(5):
        try:
            columns = [column for column in M1_COLUMNS if column in available_csv_columns(path)]
            missing = {"datetime", "open", "high", "low", "close", "volume", "bid_open", "bid_close", "ask_open", "ask_close"} - set(columns)
            if missing:
                raise ValueError(f"{path.name} missing required columns: {sorted(missing)}")
            frame = read_csv_tail(path, columns, tail_rows)
            break
        except PermissionError as exc:
            last_error = exc
            if attempt == 4:
                raise
            time.sleep(0.25 * (attempt + 1))
    else:  # pragma: no cover - the loop either loads or raises.
        raise last_error or PermissionError(path)
    frame["time_utc"] = pd.to_datetime(frame.pop("datetime"), errors="coerce", utc=True)
    for column in columns:
        if column != "datetime":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.set_index("time_utc")
    return build_pair_history(instrument, frame)


def load_pair_history_s5(s5_dir: Path, instrument: str, tail_rows: int = 0) -> PairHistory:
    path = s5_dir / f"{instrument}_S5.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"Missing S5 candle file: {path}")
    frame = pd.read_parquet(path)
    if tail_rows > 0:
        # Keep enough S5 rows to form the requested number of M1 rows plus the
        # lookback windows used by the strategy lab.
        frame = frame.tail(max(tail_rows * 12 + 1500, tail_rows))
    time_column = "dt" if "dt" in frame.columns else "time"
    frame["time_utc"] = pd.to_datetime(frame[time_column], errors="coerce", utc=True)
    numeric_columns = [
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
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["time_utc", "mid_open", "mid_high", "mid_low", "mid_close"])
    if frame.empty:
        raise ValueError(f"No executable S5 rows for {instrument}")
    frame = frame.sort_values("time_utc").drop_duplicates("time_utc", keep="last").set_index("time_utc")
    m1 = frame.resample("1min", label="left", closed="left").agg(
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
    m1 = m1.rename(
        columns={
            "mid_open": "open",
            "mid_high": "high",
            "mid_low": "low",
            "mid_close": "close",
        }
    )
    m1 = m1.dropna(subset=["open", "high", "low", "close", "bid_open", "bid_close", "ask_open", "ask_close"])
    if tail_rows > 0:
        m1 = m1.tail(tail_rows)
    return build_pair_history(instrument, m1)


def parse_utc(value: str) -> pd.Timestamp | None:
    if not value:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def parse_horizons(value: str) -> list[int]:
    horizons = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not horizons or any(horizon <= 0 or horizon % 60 for horizon in horizons):
        raise argparse.ArgumentTypeError("horizons must be positive whole-minute seconds")
    return horizons


def parse_instruments(value: str) -> list[str]:
    instruments = sorted({item.strip().upper().replace("/", "_") for item in value.split(",") if item.strip()})
    if not instruments or any("_" not in instrument for instrument in instruments):
        raise argparse.ArgumentTypeError("provide one or more comma-separated FX instruments")
    return instruments


def select_cycle_times(
    histories: dict[str, PairHistory],
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    step_minutes: int,
    max_cycles: int,
) -> list[pd.Timestamp]:
    times = sorted({timestamp for history in histories.values() for timestamp in history.m1_times})
    selected = [
        timestamp
        for timestamp in times
        if (start is None or timestamp >= start)
        and (end is None or timestamp <= end)
        and int(timestamp.timestamp() // 60) % step_minutes == 0
    ]
    return selected[-max_cycles:] if max_cycles > 0 else selected


def setup_outcomes(
    history: PairHistory,
    entry_position: int,
    direction: str,
    horizons: Iterable[int],
) -> dict[int, dict[str, Any]]:
    entry = history.frame.iloc[entry_position]
    output: dict[int, dict[str, Any]] = {}
    for horizon in horizons:
        exit_position = history.exit_position(entry_position, horizon)
        if exit_position is None:
            continue
        exit_row = history.frame.iloc[exit_position]
        pips = theoretical_pips(
            direction,
            history.pip,
            float(entry.bid_open),
            float(entry.ask_open),
            float(exit_row.bid_close),
            float(exit_row.ask_close),
        )
        output[horizon] = {
            "theoretical_pips": round(pips, 3),
            "exit_time": candle_time(history.m1_times[exit_position] + pd.Timedelta(minutes=1)),
            "exit_bid": float(exit_row.bid_close),
            "exit_ask": float(exit_row.ask_close),
        }
    return output


def outcome_summary(
    values: dict[tuple[str, str, int], list[float]],
    lane_id: str,
    horizons: Iterable[int],
) -> dict[str, Any]:
    return {
        str(horizon): {
            "accepted": sample_stats(values[(lane_id, "signal", horizon)]),
            "near_threshold": sample_stats(values[(lane_id, "near_threshold", horizon)]),
            "hard_reject": sample_stats(values[(lane_id, "hard_reject", horizon)]),
        }
        for horizon in horizons
    }


def run_backtest(
    histories: dict[str, PairHistory],
    lanes: list[LaneSpec],
    cycle_times: list[pd.Timestamp],
    horizons: list[int],
    ensemble_specs: Iterable[EnsembleSpec] = DEFAULT_ENSEMBLES,
    enable_supervised: bool = False,
    progress_every: int = 0,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ensemble_specs = tuple(ensemble_specs)
    lane_counts: dict[str, Counter[str]] = defaultdict(Counter)
    lane_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    lane_values: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    family_values: dict[tuple[str, int], list[float]] = defaultdict(list)
    source_outcomes: dict[tuple[str, int], float] = {}
    setup_events: list[dict[str, Any]] = []
    outcome_events: list[dict[str, Any]] = []
    ensemble_candidates: list[dict[str, Any]] = []
    feature_errors: Counter[str] = Counter()

    for cycle_number, decision_time in enumerate(cycle_times, start=1):
        feature_cache: dict[str, dict[str, Any]] = {}
        candle_sets: dict[str, dict[str, list[dict[str, Any]]]] = {}
        entries: dict[str, int] = {}
        for instrument, history in histories.items():
            windows = history.candle_windows(decision_time)
            entry_position = history.entry_position(decision_time)
            if windows is None or entry_position is None:
                continue
            features, reason = build_features(instrument, windows, history.pip)
            if features is None:
                feature_errors[reason] += 1
                continue
            feature_cache[instrument] = features
            candle_sets[instrument] = windows
            entries[instrument] = entry_position
        augment_cross_sectional_features(feature_cache)
        if enable_supervised:
            augment_supervised_return_features(feature_cache, candle_sets)
        else:
            for features in feature_cache.values():
                features["supervised_ready"] = False

        cycle_key = decision_time.strftime("%Y%m%dT%H%M%S")
        ensemble_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
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
                entry_position = entries[instrument]
                entry = history.frame.iloc[entry_position]
                bid = float(entry.bid_open)
                ask = float(entry.ask_open)
                blockers, gate_metrics = economic_gates(lane, features, bid, ask, setup.signal)
                miss_class = classify_miss(lane, gate_metrics) if blockers else ""
                event = "shadow_miss" if blockers else "shadow_signal"
                category = miss_class if blockers else "signal"
                counts[category] += 1
                reasons.update(blockers)
                event_id = f"{cycle_key}-{lane.lane_id}-{instrument}"
                entry_time = candle_time(history.m1_times[entry_position])
                setup_row = {
                    "event": event,
                    "id": event_id,
                    "cycle_id": cycle_key,
                    "lane_id": lane.lane_id,
                    "family": lane.family,
                    "profile": lane.profile,
                    "instrument": instrument,
                    "direction": setup.direction,
                    "time": entry_time,
                    "decision_time": candle_time(decision_time + pd.Timedelta(minutes=1)),
                    "signal_candle_time": candle_time(decision_time),
                    "entry_time": entry_time,
                    "entry_bid": bid,
                    "entry_ask": ask,
                    "blocked_reason": blockers[0] if blockers else "",
                    "blocked_reasons": blockers,
                    "miss_class": miss_class,
                    "gates": gate_metrics,
                    "signal": setup.signal,
                }
                setup_events.append(setup_row)
                if not blockers or miss_class == "near_threshold":
                    ensemble_groups[instrument].append(setup_row)

                future = setup_outcomes(history, entry_position, setup.direction, horizons)
                for horizon, outcome in future.items():
                    pips = float(outcome["theoretical_pips"])
                    lane_values[(lane.lane_id, category, horizon)].append(pips)
                    if category == "signal":
                        family_values[(lane.family, horizon)].append(pips)
                    source_outcomes[(event_id, horizon)] = pips
                    outcome_events.append(
                        {
                            "event": "shadow_outcome",
                            "id": event_id,
                            "lane_id": lane.lane_id,
                            "family": lane.family,
                            "profile": lane.profile,
                            "kind": "signal" if category == "signal" else "miss",
                            "instrument": instrument,
                            "direction": setup.direction,
                            "miss_class": miss_class,
                            "horizon_sec": horizon,
                            **outcome,
                        }
                    )

        for instrument, rows in ensemble_groups.items():
            for spec in ensemble_specs:
                candidate = candidate_for_group(spec, (cycle_key, instrument), rows)
                if candidate is not None:
                    ensemble_candidates.append(candidate)
        if progress_every and (cycle_number % progress_every == 0 or cycle_number == len(cycle_times)):
            print(
                f"[backtest] {cycle_number:,}/{len(cycle_times):,} cycles; "
                f"setups={len(setup_events):,}; ensembles={len(ensemble_candidates):,}",
                flush=True,
            )

    ensemble_outcomes = attach_outcomes(ensemble_candidates, source_outcomes, horizons)
    ensemble_values: dict[tuple[str, int], list[float]] = defaultdict(list)
    for row in ensemble_outcomes:
        ensemble_values[(str(row["ensemble"]), int(row["horizon_sec"]))].append(
            float(row["theoretical_pips"])
        )

    lane_summaries: list[dict[str, Any]] = []
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
                "horizons": outcome_summary(lane_values, lane.lane_id, horizons),
            }
        )
    lane_summaries.sort(
        key=lambda row: (
            row["horizons"][str(max(horizons))]["accepted"]["avg"],
            row["horizons"][str(max(horizons))]["accepted"]["n"],
        ),
        reverse=True,
    )

    family_summaries = [
        {
            "family": family,
            "horizons": {
                str(horizon): sample_stats(family_values[(family, horizon)])
                for horizon in horizons
            },
        }
        for family in sorted({lane.family for lane in lanes})
    ]
    ensemble_signal_counts = Counter(row["ensemble"] for row in ensemble_candidates)
    ensemble_summaries = [
        {
            "ensemble": spec.name,
            "signals": ensemble_signal_counts[spec.name],
            "horizons": {
                str(horizon): sample_stats(ensemble_values[(spec.name, horizon)])
                for horizon in horizons
            },
        }
        for spec in ensemble_specs
    ]
    report = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "evaluation_mode": "historical_bid_ask_replay",
        "account_required": False,
        "token_required": False,
        "places_orders": False,
        "decision_uses_future_data": False,
        "instrument_count": len(histories),
        "instruments": sorted(histories),
        "lane_count": len(lanes),
        "cycle_count": len(cycle_times),
        "first_decision_candle": candle_time(cycle_times[0]) if cycle_times else "",
        "last_decision_candle": candle_time(cycle_times[-1]) if cycle_times else "",
        "horizons_sec": horizons,
        "feature_errors": dict(feature_errors),
        "raw_setup_count": sum(row["raw_setups"] for row in lane_summaries),
        "accepted_count": sum(row["accepted"] for row in lane_summaries),
        "near_threshold_count": sum(row["near_threshold"] for row in lane_summaries),
        "hard_reject_count": sum(row["hard_reject"] for row in lane_summaries),
        "outcome_count": len(outcome_events),
        "ensemble_candidate_count": len(ensemble_candidates),
        "ensemble_outcome_count": len(ensemble_outcomes),
        "lane_summaries": lane_summaries,
        "family_summaries": family_summaries,
        "ensemble_summaries": ensemble_summaries,
        "method": {
            "feature_cutoff": "completed M1 candle T and completed M5 candles through T-4m",
            "entry": "next exact M1 bid/ask open at T+1m",
            "exit": "exact future M1 bid/ask close at each horizon",
            "cost_model": "historical executable bid/ask; no synthetic spread",
            "position_model": "independent mark-to-market signals; no account margin or netting",
            "supervised_return_rank": (
                "rolling production refits enabled"
                if enable_supervised
                else "disabled; pass --enable-supervised for rolling cross-pair model refits"
            ),
        },
    }
    event_rows: list[dict[str, Any]] = [
        {
            "event": "backtest_start",
            "evaluation_mode": report["evaluation_mode"],
            "instruments": report["instruments"],
            "lane_count": len(lanes),
            "horizons_sec": horizons,
            "decision_uses_future_data": False,
        }
    ]
    event_rows.extend(setup_events)
    event_rows.extend(outcome_events)
    event_rows.extend({"event": "ensemble_signal", **row} for row in ensemble_candidates)
    event_rows.extend(ensemble_outcomes)
    return report, event_rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candle-dir", type=Path, default=DEFAULT_CANDLE_DIR)
    parser.add_argument("--s5-dir", type=Path, default=DEFAULT_S5_DIR)
    parser.add_argument(
        "--source",
        choices=("m1", "s5"),
        default="m1",
        help="Use local M1 CSV candles or resample local S5 bid/ask parquet candles.",
    )
    parser.add_argument("--instruments", type=parse_instruments, default=parse_instruments("EUR_USD"))
    parser.add_argument("--families", default="")
    parser.add_argument("--profiles", default="strict,balanced,fast,loose")
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--step-minutes", type=int, default=1)
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=1000,
        help="Use the latest N eligible cycles; zero runs the full available period",
    )
    parser.add_argument(
        "--horizons-sec",
        type=parse_horizons,
        default=parse_horizons(
            "60,180,300,600,900,1800,3600,7200,10800,14400"
        ),
    )
    parser.add_argument(
        "--tail-rows",
        type=int,
        default=0,
        help="Load only the latest N M1 rows per instrument; zero loads full files.",
    )
    parser.add_argument("--enable-supervised", action="store_true")
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT_DIR / "latest_strategy_lab_historical.json")
    parser.add_argument("--events-output", type=Path, default=None)
    parser.add_argument("--progress-every", type=int, default=100)
    args = parser.parse_args(argv)
    if args.step_minutes <= 0 or args.max_cycles < 0 or args.progress_every < 0 or args.tail_rows < 0:
        raise SystemExit("step, max-cycle, and progress values must be non-negative with a positive step")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    all_lanes = default_lanes()
    families = args.families or ",".join(sorted({lane.family for lane in all_lanes}))
    lanes = filter_lanes(all_lanes, families, args.profiles)
    histories: dict[str, PairHistory] = {}
    load_errors: dict[str, str] = {}
    for index, instrument in enumerate(args.instruments, start=1):
        print(f"[load] {index}/{len(args.instruments)} {instrument}", flush=True)
        try:
            if args.source == "s5":
                histories[instrument] = load_pair_history_s5(args.s5_dir, instrument, args.tail_rows)
            else:
                histories[instrument] = load_pair_history(args.candle_dir, instrument, args.tail_rows)
        except Exception as exc:
            load_errors[instrument] = str(exc)
            print(f"[load-skip] {instrument}: {exc}", flush=True)
        if index % 8 == 0:
            gc.collect()
    if not histories:
        raise SystemExit("No instruments loaded")
    cycles = select_cycle_times(
        histories,
        parse_utc(args.start),
        parse_utc(args.end),
        args.step_minutes,
        args.max_cycles,
    )
    if not cycles:
        raise SystemExit("No historical cycles matched the requested range")
    report, events = run_backtest(
        histories,
        lanes,
        cycles,
        args.horizons_sec,
        enable_supervised=args.enable_supervised,
        progress_every=args.progress_every,
    )
    report["source"] = args.source
    report["source_dir"] = str((args.s5_dir if args.source == "s5" else args.candle_dir).resolve())
    report["load_errors"] = load_errors
    events_output = args.events_output or args.output.with_name(args.output.stem + "_events.jsonl")
    atomic_write_json(args.output, report)
    atomic_write_jsonl(events_output, events)
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "events": str(events_output.resolve()),
                "cycles": report["cycle_count"],
                "accepted": report["accepted_count"],
                "near_threshold": report["near_threshold_count"],
                "hard_reject": report["hard_reject_count"],
                "ensemble_candidates": report["ensemble_candidate_count"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
