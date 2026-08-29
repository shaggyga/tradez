#!/usr/bin/env python3
"""Fit and validate the canonical moving-average-only forecast grid.

Each input-timeframe model has three multi-horizon heads:

* direction score, calibrated to probability-up on validation;
* signed midpoint pips, affine-calibrated on validation;
* absolute midpoint movement, affine-calibrated on validation.

The untouched holdout is scored for direction, pip error, magnitude error, and
executable bid/ask economics. No result is authorized for account execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import joblib
import numpy as np
import pandas as pd
from scipy.stats import binomtest, spearmanr
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    balanced_accuracy_score,
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from oanda_ma_feature_grid import (
        FAMILY,
        FORECAST_HORIZONS_SEC,
        SCHEMA_VERSION,
        TIMEFRAME_SECONDS,
        artifact_checksum,
        build_ma_feature_matrix,
        contract_payload,
        infer_pip_size,
        ma_feature_names,
        parse_timeframe,
        periods_for_timeframe,
        timeframe_label,
        utc_now,
    )
except ModuleNotFoundError:
    from trad.oanda_ma_feature_grid import (
        FAMILY,
        FORECAST_HORIZONS_SEC,
        SCHEMA_VERSION,
        TIMEFRAME_SECONDS,
        artifact_checksum,
        build_ma_feature_matrix,
        contract_payload,
        infer_pip_size,
        ma_feature_names,
        parse_timeframe,
        periods_for_timeframe,
        timeframe_label,
        utc_now,
    )


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_M1_DIR = DATA_ROOT / "candles_m1_parquet_recovered_20260719"
DEFAULT_S5_DIR = DATA_ROOT / "candles_s5_bam"
DEFAULT_MODEL_DIR = DATA_ROOT / "models" / "ma_feature_grid"
DEFAULT_REPORT_DIR = DATA_ROOT / "reports" / "ma_feature_grid"
DEFAULT_ARTIFACT = DEFAULT_MODEL_DIR / "ma_feature_grid_latest.joblib"
DEFAULT_REPORT = DEFAULT_REPORT_DIR / "ma_feature_grid_latest.json"
DEFAULT_GRID = DEFAULT_REPORT_DIR / "ma_feature_grid_metrics_latest.csv"
DEFAULT_SUMMARY = DEFAULT_REPORT_DIR / "MA_FEATURE_GRID_LATEST.md"

SOURCE_COLUMNS = (
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
)
SPLITS = ("development", "validation", "holdout")
PAIR_CONTEXT_MODES = ("none", "currencies", "pair_and_currencies")
SPLIT_POLICIES = ("pair_fraction", "pair_time_purged", "global_time_purged")
DATASET_CACHE_SCHEMA_VERSION = 1


@dataclass
class TimeframeDataset:
    timeframe: str
    features: np.ndarray
    feature_names: tuple[str, ...]
    signed_pips: np.ndarray
    long_net_pips: np.ndarray
    short_net_pips: np.ndarray
    decision_cost_pips: np.ndarray
    exact_cost: np.ndarray
    instruments: np.ndarray
    timestamps_ns: np.ndarray
    splits: np.ndarray
    source_rows: int
    sampled_rows: int
    pair_inventory: list[dict[str, Any]]


def add_pair_context(
    dataset: TimeframeDataset,
    mode: str,
) -> TimeframeDataset:
    """Append static instrument identity without adding non-MA market inputs."""

    normalized_mode = str(mode or "none").lower()
    if normalized_mode not in PAIR_CONTEXT_MODES:
        raise ValueError(f"unsupported pair context mode: {mode}")
    if normalized_mode == "none":
        return dataset
    instruments = np.asarray(dataset.instruments, dtype=str)
    pairs = tuple(sorted(set(instruments.tolist())))
    currencies = tuple(
        sorted(
            {
                currency
                for instrument in pairs
                for currency in instrument.split("_", 1)
                if currency
            }
        )
    )
    columns: list[np.ndarray] = []
    names: list[str] = []
    if normalized_mode == "pair_and_currencies":
        for instrument in pairs:
            columns.append((instruments == instrument).astype(np.float32))
            names.append(f"context__pair__{instrument}")
    bases = np.asarray(
        [instrument.split("_", 1)[0] for instrument in instruments],
        dtype=str,
    )
    quotes = np.asarray(
        [
            instrument.split("_", 1)[1] if "_" in instrument else ""
            for instrument in instruments
        ],
        dtype=str,
    )
    for currency in currencies:
        columns.append((bases == currency).astype(np.float32))
        names.append(f"context__base__{currency}")
        columns.append((quotes == currency).astype(np.float32))
        names.append(f"context__quote__{currency}")
    if columns:
        dataset.features = np.column_stack(
            (dataset.features, np.column_stack(columns))
        ).astype(np.float32, copy=False)
        dataset.feature_names = (*dataset.feature_names, *names)
    return dataset


@dataclass
class DatasetAccumulator:
    timeframe: str
    feature_names: tuple[str, ...]
    feature_parts: list[np.ndarray]
    signed_parts: list[np.ndarray]
    long_parts: list[np.ndarray]
    short_parts: list[np.ndarray]
    decision_cost_parts: list[np.ndarray]
    exact_parts: list[np.ndarray]
    instrument_parts: list[np.ndarray]
    timestamp_parts: list[np.ndarray]
    split_parts: list[np.ndarray]
    source_rows: int
    sampled_rows: int
    pair_inventory: list[dict[str, Any]]


def dataset_payload(dataset: TimeframeDataset | None) -> dict[str, Any] | None:
    if dataset is None:
        return None
    return {
        "timeframe": dataset.timeframe,
        "features": dataset.features,
        "feature_names": dataset.feature_names,
        "signed_pips": dataset.signed_pips,
        "long_net_pips": dataset.long_net_pips,
        "short_net_pips": dataset.short_net_pips,
        "decision_cost_pips": dataset.decision_cost_pips,
        "exact_cost": dataset.exact_cost,
        "instruments": dataset.instruments,
        "timestamps_ns": dataset.timestamps_ns,
        "splits": dataset.splits,
        "source_rows": dataset.source_rows,
        "sampled_rows": dataset.sampled_rows,
        "pair_inventory": dataset.pair_inventory,
    }


def dataset_from_payload(
    payload: Mapping[str, Any] | None,
) -> TimeframeDataset | None:
    if payload is None:
        return None
    return TimeframeDataset(
        timeframe=str(payload["timeframe"]),
        features=np.asarray(payload["features"]),
        feature_names=tuple(payload["feature_names"]),
        signed_pips=np.asarray(payload["signed_pips"]),
        long_net_pips=np.asarray(payload["long_net_pips"]),
        short_net_pips=np.asarray(payload["short_net_pips"]),
        decision_cost_pips=np.asarray(payload["decision_cost_pips"]),
        exact_cost=np.asarray(payload["exact_cost"]),
        instruments=np.asarray(payload["instruments"]),
        timestamps_ns=np.asarray(payload["timestamps_ns"]),
        splits=np.asarray(payload["splits"]),
        source_rows=int(payload["source_rows"]),
        sampled_rows=int(payload["sampled_rows"]),
        pair_inventory=list(payload["pair_inventory"]),
    )


def finite(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric if math.isfinite(numeric) else default


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(type(value).__name__)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=_json_default,
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def write_joblib_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    joblib.dump(payload, temporary, compress=3)
    temporary.replace(path)


def write_dataset_cache_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    joblib.dump(payload, temporary, compress=1)
    temporary.replace(path)


def parse_csv_ints(value: str) -> tuple[int, ...]:
    output = tuple(
        sorted(
            {
                int(item)
                for item in str(value).replace(" ", ",").split(",")
                if item.strip()
            }
        )
    )
    if not output or output[0] <= 0:
        raise argparse.ArgumentTypeError("values must be positive integers")
    return output


def parse_timeframes(value: str) -> tuple[str, ...]:
    labels: list[str] = []
    for item in str(value).replace(" ", ",").split(","):
        if not item.strip():
            continue
        label = timeframe_label(parse_timeframe(item.strip()))
        if label not in TIMEFRAME_SECONDS:
            raise argparse.ArgumentTypeError(f"unsupported timeframe: {item}")
        if label not in labels:
            labels.append(label)
    if not labels:
        raise argparse.ArgumentTypeError("at least one timeframe is required")
    return tuple(labels)


def discover_instruments(
    m1_dir: Path,
    s5_dir: Path,
    requested: str,
    max_pairs: int,
) -> list[str]:
    if requested.strip():
        instruments = sorted(
            {
                item.strip().upper().replace("/", "_")
                for item in requested.split(",")
                if item.strip()
            }
        )
    else:
        instruments = sorted(
            path.name.removesuffix("_M1.parquet")
            for path in m1_dir.glob("*_M1.parquet")
        )
        if not instruments:
            instruments = sorted(
                path.name.removesuffix("_M1.csv")
                for path in m1_dir.glob("*_M1.csv")
            )
    if max_pairs > 0:
        instruments = instruments[:max_pairs]
    return instruments


def source_path(
    instrument: str,
    timeframe: str,
    m1_dir: Path,
    s5_dir: Path,
) -> tuple[Path | None, int]:
    if TIMEFRAME_SECONDS[timeframe] < 60:
        candidates = (
            s5_dir / f"{instrument}_S5.parquet",
            s5_dir / f"{instrument}_S5.csv",
        )
        base_seconds = 5
    else:
        candidates = (
            m1_dir / f"{instrument}_M1.parquet",
            m1_dir / f"{instrument}_M1.csv",
        )
        base_seconds = 60
    return next((path for path in candidates if path.is_file()), None), base_seconds


def dataset_cache_contract(
    timeframes: tuple[str, ...],
    instruments: list[str],
    horizons_by_timeframe: Mapping[str, tuple[int, ...]],
    m1_dir: Path,
    s5_dir: Path,
    samples_per_pair: int,
) -> dict[str, Any]:
    sources: dict[str, dict[str, Any]] = {}
    for instrument in instruments:
        for timeframe in timeframes:
            path, base_seconds = source_path(
                instrument,
                timeframe,
                m1_dir,
                s5_dir,
            )
            if path is None:
                continue
            resolved = str(path.resolve())
            if resolved in sources:
                continue
            stat = path.stat()
            sources[resolved] = {
                "path": resolved,
                "base_seconds": base_seconds,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
    feature_schema = {
        timeframe: list(ma_feature_names(timeframe))
        for timeframe in timeframes
    }
    feature_schema_sha256 = hashlib.sha256(
        json.dumps(
            feature_schema,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": DATASET_CACHE_SCHEMA_VERSION,
        "timeframes": list(timeframes),
        "instruments": list(instruments),
        "horizons_by_timeframe": {
            timeframe: list(horizons_by_timeframe[timeframe])
            for timeframe in timeframes
        },
        "m1_dir": str(m1_dir.resolve()),
        "s5_dir": str(s5_dir.resolve()),
        "samples_per_pair_timeframe": (
            "all" if samples_per_pair == 0 else samples_per_pair
        ),
        "feature_schema_sha256": feature_schema_sha256,
        "sources": [sources[key] for key in sorted(sources)],
    }


def load_source_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_csv(path, low_memory=False)
    aliases = {
        "datetime": "dt",
        "open": "mid_open",
        "high": "mid_high",
        "low": "mid_low",
        "close": "mid_close",
    }
    for canonical, source in aliases.items():
        if canonical not in frame and source in frame:
            frame[canonical] = frame[source]
    if "time" not in frame and "datetime" in frame:
        frame["time"] = frame["datetime"]
    missing = {"time", "open", "close"} - set(frame)
    if missing:
        raise ValueError(f"{path.name} missing columns: {sorted(missing)}")
    for column in SOURCE_COLUMNS:
        if column not in frame:
            frame[column] = np.nan
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce", utc=True)
    for column in SOURCE_COLUMNS:
        if column not in ("time", "datetime"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["time", "open", "close"])
        .set_index("time")
        .sort_index()
    )
    frame = frame[~frame.index.duplicated(keep="last")]
    return frame


def estimate_proxy_spread(frame: pd.DataFrame, pip: float) -> tuple[float, int]:
    observed = pd.to_numeric(frame["spread_pips"], errors="coerce")
    observed = observed[np.isfinite(observed) & (observed > 0.0)]
    if observed.empty:
        calculated = (frame["ask_open"] - frame["bid_open"]) / pip
        observed = calculated[np.isfinite(calculated) & (calculated > 0.0)]
    return (
        (float(observed.median()), int(observed.size))
        if not observed.empty
        else (2.0, 0)
    )


def resample_bars(
    frame: pd.DataFrame,
    timeframe_seconds: int,
    base_seconds: int,
) -> pd.DataFrame:
    if timeframe_seconds == base_seconds:
        bars = frame[["close"]].copy()
        bars["source_count"] = 1
        return bars
    if timeframe_seconds < base_seconds or timeframe_seconds % base_seconds:
        raise ValueError(
            f"timeframe {timeframe_seconds}s is not supported by {base_seconds}s source"
        )
    expected = timeframe_seconds // base_seconds
    bars = frame.resample(
        f"{timeframe_seconds}s",
        label="left",
        closed="left",
        origin="epoch",
    ).agg(close=("close", "last"), source_count=("close", "count"))
    bars = bars[bars["source_count"] >= max(1, math.ceil(expected * 0.80))]
    return bars.dropna(subset=["close"])


def uniform_positions(
    bars: pd.DataFrame,
    minimum_position: int,
    samples: int,
) -> np.ndarray:
    available = np.arange(minimum_position, len(bars), dtype=np.int64)
    if samples <= 0 or available.size <= samples:
        return available
    indexes = np.linspace(0, available.size - 1, samples, dtype=np.int64)
    return np.unique(available[indexes])


def outcome_targets(
    frame: pd.DataFrame,
    bars: pd.DataFrame,
    positions: np.ndarray,
    timeframe_seconds: int,
    horizons_sec: tuple[int, ...],
    pip: float,
    proxy_spread: float,
    base_seconds: int,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    count = positions.size
    horizon_count = len(horizons_sec)
    signed = np.full((count, horizon_count), np.nan, dtype=np.float32)
    long_net = np.full_like(signed, np.nan)
    short_net = np.full_like(signed, np.nan)
    exact = np.zeros((count, horizon_count), dtype=bool)
    source_ns = frame.index.asi8
    decision_ns = bars.index.asi8[positions] + int(timeframe_seconds * 1e9)
    entry_positions = np.searchsorted(source_ns, decision_ns, side="left")
    entry_valid = entry_positions < source_ns.size
    entry_delay = np.full(count, np.iinfo(np.int64).max, dtype=np.int64)
    if entry_valid.any():
        indexes = np.flatnonzero(entry_valid)
        entry_delay[indexes] = source_ns[entry_positions[indexes]] - decision_ns[indexes]
    maximum_delay_ns = int(max(2 * base_seconds, 10) * 1e9)
    entry_valid &= (entry_delay >= 0) & (entry_delay <= maximum_delay_ns)

    open_mid = frame["open"].to_numpy(dtype=np.float64, copy=False)
    close_mid = frame["close"].to_numpy(dtype=np.float64, copy=False)
    bid_open = frame["bid_open"].to_numpy(dtype=np.float64, copy=False)
    ask_open = frame["ask_open"].to_numpy(dtype=np.float64, copy=False)
    bid_close = frame["bid_close"].to_numpy(dtype=np.float64, copy=False)
    ask_close = frame["ask_close"].to_numpy(dtype=np.float64, copy=False)
    entry_times_ns = np.zeros(count, dtype=np.int64)
    valid_entry_indexes = np.flatnonzero(entry_valid)
    entry_times_ns[valid_entry_indexes] = source_ns[
        entry_positions[valid_entry_indexes]
    ]
    decision_cost_pips = np.full(count, proxy_spread, dtype=np.float32)
    if valid_entry_indexes.size:
        entries = entry_positions[valid_entry_indexes]
        observed_entry_spread = (ask_open[entries] - bid_open[entries]) / pip
        usable_entry_spread = (
            np.isfinite(observed_entry_spread) & (observed_entry_spread > 0.0)
        )
        decision_cost_pips[valid_entry_indexes[usable_entry_spread]] = (
            observed_entry_spread[usable_entry_spread].astype(np.float32)
        )

    for horizon_index, horizon_sec in enumerate(horizons_sec):
        target_ns = entry_times_ns + int(horizon_sec * 1e9)
        exit_positions = np.searchsorted(source_ns, target_ns, side="left")
        valid = entry_valid & (exit_positions < source_ns.size)
        delay = np.full(count, np.iinfo(np.int64).max, dtype=np.int64)
        if valid.any():
            indexes = np.flatnonzero(valid)
            delay[indexes] = source_ns[exit_positions[indexes]] - target_ns[indexes]
        valid &= (delay >= 0) & (delay <= maximum_delay_ns)
        indexes = np.flatnonzero(valid)
        if not indexes.size:
            continue
        entries = entry_positions[indexes]
        exits = exit_positions[indexes]
        gross = (close_mid[exits] - open_mid[entries]) / pip
        signed[indexes, horizon_index] = gross.astype(np.float32)
        long_values = gross - proxy_spread
        short_values = -gross - proxy_spread
        exact_rows = (
            np.isfinite(bid_open[entries])
            & np.isfinite(ask_open[entries])
            & np.isfinite(bid_close[exits])
            & np.isfinite(ask_close[exits])
        )
        if exact_rows.any():
            long_values[exact_rows] = (
                bid_close[exits[exact_rows]] - ask_open[entries[exact_rows]]
            ) / pip
            short_values[exact_rows] = (
                bid_open[entries[exact_rows]] - ask_close[exits[exact_rows]]
            ) / pip
        long_net[indexes, horizon_index] = long_values.astype(np.float32)
        short_net[indexes, horizon_index] = short_values.astype(np.float32)
        exact[indexes, horizon_index] = exact_rows
    return (
        signed,
        long_net,
        short_net,
        decision_cost_pips,
        exact,
        decision_ns,
        entry_valid,
    )


def split_codes(positions: np.ndarray, bar_count: int) -> np.ndarray:
    development_end = int(bar_count * 0.60)
    validation_end = int(bar_count * 0.80)
    return np.where(
        positions < development_end,
        0,
        np.where(positions < validation_end, 1, 2),
    ).astype(np.int8)


def apply_time_split_policy(
    dataset: TimeframeDataset,
    policy: str,
    purge_horizon_sec: int,
) -> dict[str, Any]:
    """Apply chronological splits whose labels cannot cross split boundaries."""

    normalized = str(policy or "pair_fraction").lower()
    if normalized not in SPLIT_POLICIES:
        raise ValueError(f"unsupported split policy: {policy}")
    input_rows = int(dataset.splits.size)
    if normalized == "pair_fraction":
        return {
            "policy": normalized,
            "purge_horizon_sec": 0,
            "input_rows": input_rows,
            "purged_rows": 0,
            "retained_rows": input_rows,
        }
    timestamps = np.asarray(dataset.timestamps_ns, dtype=np.int64)
    instruments = np.asarray(dataset.instruments, dtype=str)
    output = np.full(timestamps.size, -1, dtype=np.int8)
    purge_ns = max(0, int(purge_horizon_sec)) * 1_000_000_000
    scopes = (
        [np.arange(timestamps.size, dtype=np.int64)]
        if normalized == "global_time_purged"
        else [
            np.flatnonzero(instruments == instrument)
            for instrument in np.unique(instruments)
        ]
    )
    boundaries: list[dict[str, Any]] = []
    for indexes in scopes:
        if indexes.size < 5:
            continue
        unique_times = np.unique(timestamps[indexes])
        if unique_times.size < 5:
            continue
        development_index = min(
            unique_times.size - 2,
            max(1, int(math.floor(unique_times.size * 0.60))),
        )
        validation_index = min(
            unique_times.size - 1,
            max(
                development_index + 1,
                int(math.floor(unique_times.size * 0.80)),
            ),
        )
        development_end = int(unique_times[development_index])
        validation_end = int(unique_times[validation_index])
        scope_times = timestamps[indexes]
        development = (
            (scope_times < development_end)
            & (scope_times + purge_ns < development_end)
        )
        validation = (
            (scope_times >= development_end)
            & (scope_times < validation_end)
            & (scope_times + purge_ns < validation_end)
        )
        holdout = scope_times >= validation_end
        output[indexes[development]] = 0
        output[indexes[validation]] = 1
        output[indexes[holdout]] = 2
        boundaries.append(
            {
                "scope": (
                    "global"
                    if normalized == "global_time_purged"
                    else str(instruments[indexes[0]])
                ),
                "development_end_ns": development_end,
                "validation_end_ns": validation_end,
            }
        )
    retained = output >= 0
    purged_rows = int((~retained).sum())
    dataset.features = dataset.features[retained]
    dataset.signed_pips = dataset.signed_pips[retained]
    dataset.long_net_pips = dataset.long_net_pips[retained]
    dataset.short_net_pips = dataset.short_net_pips[retained]
    dataset.decision_cost_pips = dataset.decision_cost_pips[retained]
    dataset.exact_cost = dataset.exact_cost[retained]
    dataset.instruments = dataset.instruments[retained]
    dataset.timestamps_ns = dataset.timestamps_ns[retained]
    dataset.splits = output[retained]
    dataset.sampled_rows = int(retained.sum())
    return {
        "policy": normalized,
        "purge_horizon_sec": int(purge_horizon_sec),
        "input_rows": input_rows,
        "purged_rows": purged_rows,
        "retained_rows": int(retained.sum()),
        "boundary_count": len(boundaries),
        "boundaries": boundaries[:10],
    }


def build_timeframe_dataset(
    timeframe: str,
    instruments: list[str],
    horizons_sec: tuple[int, ...],
    m1_dir: Path,
    s5_dir: Path,
    samples_per_pair: int,
) -> TimeframeDataset | None:
    timeframe_seconds = TIMEFRAME_SECONDS[timeframe]
    feature_parts: list[np.ndarray] = []
    signed_parts: list[np.ndarray] = []
    long_parts: list[np.ndarray] = []
    short_parts: list[np.ndarray] = []
    decision_cost_parts: list[np.ndarray] = []
    exact_parts: list[np.ndarray] = []
    instrument_parts: list[np.ndarray] = []
    timestamp_parts: list[np.ndarray] = []
    split_parts: list[np.ndarray] = []
    source_rows = 0
    sampled_rows = 0
    inventory: list[dict[str, Any]] = []
    expected_names = ma_feature_names(timeframe)
    warmup = max(periods_for_timeframe(timeframe)) + 5

    for instrument in instruments:
        path, base_seconds = source_path(instrument, timeframe, m1_dir, s5_dir)
        if path is None:
            inventory.append(
                {
                    "instrument": instrument,
                    "status": "source_missing",
                    "timeframe": timeframe,
                }
            )
            continue
        try:
            frame = load_source_frame(path)
            bars = resample_bars(frame, timeframe_seconds, base_seconds)
        except Exception as exc:
            inventory.append(
                {
                    "instrument": instrument,
                    "status": f"load_error:{type(exc).__name__}",
                    "error": str(exc)[:300],
                    "path": str(path),
                }
            )
            continue
        source_rows += len(frame)
        if len(bars) <= warmup + 10:
            inventory.append(
                {
                    "instrument": instrument,
                    "status": "insufficient_bars",
                    "source_rows": len(frame),
                    "bars": len(bars),
                    "path": str(path),
                }
            )
            continue
        pip = infer_pip_size(instrument)
        proxy_spread, spread_samples = estimate_proxy_spread(frame, pip)
        positions = uniform_positions(bars, warmup, samples_per_pair)
        (
            signed,
            long_net,
            short_net,
            decision_cost_pips,
            exact,
            decision_ns,
            entry_valid,
        ) = outcome_targets(
            frame,
            bars,
            positions,
            timeframe_seconds,
            horizons_sec,
            pip,
            proxy_spread,
            base_seconds,
        )
        target_valid = (
            entry_valid
            & np.all(np.isfinite(signed), axis=1)
            & np.all(np.isfinite(long_net), axis=1)
            & np.all(np.isfinite(short_net), axis=1)
        )
        positions = positions[target_valid]
        signed = signed[target_valid]
        long_net = long_net[target_valid]
        short_net = short_net[target_valid]
        decision_cost_pips = decision_cost_pips[target_valid]
        exact = exact[target_valid]
        decision_ns = decision_ns[target_valid]
        if not positions.size:
            inventory.append(
                {
                    "instrument": instrument,
                    "status": "no_complete_horizon_rows",
                    "source_rows": len(frame),
                    "bars": len(bars),
                    "path": str(path),
                }
            )
            continue
        matrix, names = build_ma_feature_matrix(
            bars["close"].to_numpy(dtype=np.float64, copy=False),
            positions,
            pip,
            timeframe,
        )
        if names != expected_names:
            raise RuntimeError("MA feature schema changed during one fit")
        finite_rows = np.any(np.isfinite(matrix), axis=1)
        matrix = matrix[finite_rows]
        signed = signed[finite_rows]
        long_net = long_net[finite_rows]
        short_net = short_net[finite_rows]
        decision_cost_pips = decision_cost_pips[finite_rows]
        exact = exact[finite_rows]
        decision_ns = decision_ns[finite_rows]
        kept_positions = positions[finite_rows]
        rows = matrix.shape[0]
        if not rows:
            continue
        sampled_rows += rows
        feature_parts.append(matrix)
        signed_parts.append(signed)
        long_parts.append(long_net)
        short_parts.append(short_net)
        decision_cost_parts.append(decision_cost_pips)
        exact_parts.append(exact)
        instrument_parts.append(np.full(rows, instrument, dtype=object))
        timestamp_parts.append(decision_ns)
        split_parts.append(split_codes(kept_positions, len(bars)))
        inventory.append(
            {
                "instrument": instrument,
                "status": "loaded",
                "path": str(path),
                "source": f"S{base_seconds}",
                "source_rows": len(frame),
                "bars": len(bars),
                "sampled_rows": rows,
                "first_utc": frame.index[0].isoformat(),
                "last_utc": frame.index[-1].isoformat(),
                "proxy_spread_pips": round(proxy_spread, 6),
                "observed_spread_samples": spread_samples,
                "exact_cost_fraction": round(float(np.mean(exact)), 6),
            }
        )

    if not feature_parts:
        return None
    return TimeframeDataset(
        timeframe=timeframe,
        features=np.vstack(feature_parts).astype(np.float32, copy=False),
        feature_names=expected_names,
        signed_pips=np.vstack(signed_parts).astype(np.float32, copy=False),
        long_net_pips=np.vstack(long_parts).astype(np.float32, copy=False),
        short_net_pips=np.vstack(short_parts).astype(np.float32, copy=False),
        decision_cost_pips=np.concatenate(decision_cost_parts).astype(
            np.float32,
            copy=False,
        ),
        exact_cost=np.vstack(exact_parts),
        instruments=np.concatenate(instrument_parts),
        timestamps_ns=np.concatenate(timestamp_parts),
        splits=np.concatenate(split_parts),
        source_rows=source_rows,
        sampled_rows=sampled_rows,
        pair_inventory=inventory,
    )


def _empty_accumulator(timeframe: str) -> DatasetAccumulator:
    return DatasetAccumulator(
        timeframe=timeframe,
        feature_names=ma_feature_names(timeframe),
        feature_parts=[],
        signed_parts=[],
        long_parts=[],
        short_parts=[],
        decision_cost_parts=[],
        exact_parts=[],
        instrument_parts=[],
        timestamp_parts=[],
        split_parts=[],
        source_rows=0,
        sampled_rows=0,
        pair_inventory=[],
    )


def _append_pair_sample(
    accumulator: DatasetAccumulator,
    instrument: str,
    frame: pd.DataFrame,
    path: Path,
    base_seconds: int,
    horizons_sec: tuple[int, ...],
    samples_per_pair: int,
) -> None:
    timeframe = accumulator.timeframe
    timeframe_seconds = TIMEFRAME_SECONDS[timeframe]
    try:
        bars = resample_bars(frame, timeframe_seconds, base_seconds)
    except Exception as exc:
        accumulator.pair_inventory.append(
            {
                "instrument": instrument,
                "status": f"resample_error:{type(exc).__name__}",
                "error": str(exc)[:300],
                "path": str(path),
            }
        )
        return
    accumulator.source_rows += len(frame)
    warmup = max(periods_for_timeframe(timeframe)) + 5
    if len(bars) <= warmup + 10:
        accumulator.pair_inventory.append(
            {
                "instrument": instrument,
                "status": "insufficient_bars",
                "source_rows": len(frame),
                "bars": len(bars),
                "path": str(path),
            }
        )
        return
    pip = infer_pip_size(instrument)
    proxy_spread, spread_samples = estimate_proxy_spread(frame, pip)
    positions = uniform_positions(bars, warmup, samples_per_pair)
    (
        signed,
        long_net,
        short_net,
        decision_cost_pips,
        exact,
        decision_ns,
        entry_valid,
    ) = outcome_targets(
        frame,
        bars,
        positions,
        timeframe_seconds,
        horizons_sec,
        pip,
        proxy_spread,
        base_seconds,
    )
    target_valid = (
        entry_valid
        & np.all(np.isfinite(signed), axis=1)
        & np.all(np.isfinite(long_net), axis=1)
        & np.all(np.isfinite(short_net), axis=1)
    )
    positions = positions[target_valid]
    signed = signed[target_valid]
    long_net = long_net[target_valid]
    short_net = short_net[target_valid]
    decision_cost_pips = decision_cost_pips[target_valid]
    exact = exact[target_valid]
    decision_ns = decision_ns[target_valid]
    if not positions.size:
        accumulator.pair_inventory.append(
            {
                "instrument": instrument,
                "status": "no_complete_horizon_rows",
                "source_rows": len(frame),
                "bars": len(bars),
                "path": str(path),
            }
        )
        return
    matrix, names = build_ma_feature_matrix(
        bars["close"].to_numpy(dtype=np.float64, copy=False),
        positions,
        pip,
        timeframe,
    )
    if names != accumulator.feature_names:
        raise RuntimeError("MA feature schema changed during one fit")
    finite_rows = np.any(np.isfinite(matrix), axis=1)
    matrix = matrix[finite_rows]
    signed = signed[finite_rows]
    long_net = long_net[finite_rows]
    short_net = short_net[finite_rows]
    decision_cost_pips = decision_cost_pips[finite_rows]
    exact = exact[finite_rows]
    decision_ns = decision_ns[finite_rows]
    kept_positions = positions[finite_rows]
    rows = matrix.shape[0]
    if not rows:
        return
    accumulator.sampled_rows += rows
    accumulator.feature_parts.append(matrix)
    accumulator.signed_parts.append(signed)
    accumulator.long_parts.append(long_net)
    accumulator.short_parts.append(short_net)
    accumulator.decision_cost_parts.append(decision_cost_pips)
    accumulator.exact_parts.append(exact)
    accumulator.instrument_parts.append(np.full(rows, instrument, dtype=object))
    accumulator.timestamp_parts.append(decision_ns)
    accumulator.split_parts.append(split_codes(kept_positions, len(bars)))
    accumulator.pair_inventory.append(
        {
            "instrument": instrument,
            "status": "loaded",
            "path": str(path),
            "source": f"S{base_seconds}",
            "source_rows": len(frame),
            "bars": len(bars),
            "sampled_rows": rows,
            "first_utc": frame.index[0].isoformat(),
            "last_utc": frame.index[-1].isoformat(),
            "proxy_spread_pips": round(proxy_spread, 6),
            "observed_spread_samples": spread_samples,
            "exact_cost_fraction": round(float(np.mean(exact)), 6),
        }
    )


def _finalize_accumulator(
    accumulator: DatasetAccumulator,
) -> TimeframeDataset | None:
    if not accumulator.feature_parts:
        return None
    return TimeframeDataset(
        timeframe=accumulator.timeframe,
        features=np.vstack(accumulator.feature_parts).astype(np.float32, copy=False),
        feature_names=accumulator.feature_names,
        signed_pips=np.vstack(accumulator.signed_parts).astype(np.float32, copy=False),
        long_net_pips=np.vstack(accumulator.long_parts).astype(np.float32, copy=False),
        short_net_pips=np.vstack(accumulator.short_parts).astype(np.float32, copy=False),
        decision_cost_pips=np.concatenate(
            accumulator.decision_cost_parts
        ).astype(np.float32, copy=False),
        exact_cost=np.vstack(accumulator.exact_parts),
        instruments=np.concatenate(accumulator.instrument_parts),
        timestamps_ns=np.concatenate(accumulator.timestamp_parts),
        splits=np.concatenate(accumulator.split_parts),
        source_rows=accumulator.source_rows,
        sampled_rows=accumulator.sampled_rows,
        pair_inventory=accumulator.pair_inventory,
    )


def build_all_timeframe_datasets(
    timeframes: tuple[str, ...],
    instruments: list[str],
    horizons_by_timeframe: dict[str, tuple[int, ...]],
    m1_dir: Path,
    s5_dir: Path,
    samples_per_pair: int,
) -> dict[str, TimeframeDataset | None]:
    """Load each physical source once per pair, then derive every requested frame."""

    accumulators = {
        timeframe: _empty_accumulator(timeframe) for timeframe in timeframes
    }
    source_groups = {
        5: tuple(
            timeframe
            for timeframe in timeframes
            if TIMEFRAME_SECONDS[timeframe] < 60
        ),
        60: tuple(
            timeframe
            for timeframe in timeframes
            if TIMEFRAME_SECONDS[timeframe] >= 60
        ),
    }
    for instrument_index, instrument in enumerate(instruments, start=1):
        for base_seconds, group in source_groups.items():
            if not group:
                continue
            path, observed_base = source_path(
                instrument,
                group[0],
                m1_dir,
                s5_dir,
            )
            if path is None:
                for timeframe in group:
                    accumulators[timeframe].pair_inventory.append(
                        {
                            "instrument": instrument,
                            "status": "source_missing",
                            "timeframe": timeframe,
                        }
                    )
                continue
            try:
                frame = load_source_frame(path)
            except Exception as exc:
                for timeframe in group:
                    accumulators[timeframe].pair_inventory.append(
                        {
                            "instrument": instrument,
                            "status": f"load_error:{type(exc).__name__}",
                            "error": str(exc)[:300],
                            "path": str(path),
                        }
                    )
                continue
            if observed_base != base_seconds:
                raise RuntimeError("source resolution contract mismatch")
            for timeframe in group:
                _append_pair_sample(
                    accumulators[timeframe],
                    instrument,
                    frame,
                    path,
                    base_seconds,
                    horizons_by_timeframe[timeframe],
                    samples_per_pair,
                )
        print(
            json.dumps(
                {
                    "event": "ma_grid_pair_complete",
                    "instrument": instrument,
                    "pair_index": instrument_index,
                    "pair_count": len(instruments),
                }
            ),
            flush=True,
        )
    return {
        timeframe: _finalize_accumulator(accumulator)
        for timeframe, accumulator in accumulators.items()
    }


def fit_pipeline(
    alpha: float,
    estimator_type: str = "ridge",
    *,
    xgb_device: str = "cuda",
    xgb_estimators: int = 240,
    xgb_max_depth: int = 4,
    xgb_learning_rate: float = 0.035,
) -> Any:
    if estimator_type == "xgboost":
        from xgboost import XGBRegressor

        return XGBRegressor(
            objective="reg:squarederror",
            n_estimators=max(10, int(xgb_estimators)),
            max_depth=max(1, int(xgb_max_depth)),
            learning_rate=float(xgb_learning_rate),
            min_child_weight=20.0,
            subsample=0.80,
            colsample_bytree=0.65,
            reg_alpha=1.0,
            reg_lambda=20.0,
            max_bin=256,
            tree_method="hist",
            device=str(xgb_device),
            multi_strategy="one_output_per_tree",
            n_jobs=4,
            random_state=17,
            verbosity=0,
        )
    return Pipeline(
        (
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            ("ridge", Ridge(alpha=float(alpha))),
        )
    )


def affine_calibration(
    raw: np.ndarray,
    target: np.ndarray,
    *,
    nonnegative: bool = False,
) -> dict[str, float]:
    valid = np.isfinite(raw) & np.isfinite(target)
    if valid.sum() < 20 or float(np.std(raw[valid])) <= 1e-10:
        slope = 0.0
        intercept = float(np.mean(target[valid])) if valid.any() else 0.0
    else:
        slope, intercept = np.polyfit(raw[valid], target[valid], 1)
        slope = float(np.clip(slope, -3.0, 3.0))
        intercept = float(intercept)
    if nonnegative:
        intercept = max(0.0, intercept)
    return {"slope": round(slope, 12), "intercept": round(intercept, 12)}


def direction_calibration(raw: np.ndarray, target_up: np.ndarray) -> dict[str, float]:
    valid = np.isfinite(raw) & np.isfinite(target_up)
    labels = target_up[valid].astype(np.int8)
    if valid.sum() < 30 or np.unique(labels).size < 2:
        return {"slope": 1.0, "intercept": 0.0}
    model = LogisticRegression(C=1.0, max_iter=300, solver="lbfgs")
    model.fit(raw[valid].reshape(-1, 1), labels)
    return {
        "slope": round(float(model.coef_[0, 0]), 12),
        "intercept": round(float(model.intercept_[0]), 12),
    }


def direction_threshold(
    target_up: np.ndarray,
    probability_up: np.ndarray,
    *,
    minimum_side_fraction: float = 0.05,
) -> float:
    valid = np.isfinite(target_up) & np.isfinite(probability_up)
    labels = target_up[valid].astype(np.int8)
    probabilities = probability_up[valid].astype(np.float64)
    if valid.sum() < 30 or np.unique(labels).size < 2:
        return 0.5
    false_positive, true_positive, thresholds = roc_curve(labels, probabilities)
    candidates: list[tuple[float, float, float]] = []
    for fpr, tpr, threshold in zip(false_positive, true_positive, thresholds):
        threshold = float(threshold)
        if not math.isfinite(threshold) or not 0.001 <= threshold <= 0.999:
            continue
        predicted_up_fraction = float(np.mean(probabilities >= threshold))
        if not (
            minimum_side_fraction
            <= predicted_up_fraction
            <= 1.0 - minimum_side_fraction
        ):
            continue
        candidates.append(
            (
                float(tpr - fpr),
                -abs(threshold - 0.5),
                threshold,
            )
        )
    if not candidates:
        return 0.5
    return round(max(candidates)[2], 12)


def decision_centered_probability(
    probability_up: np.ndarray,
    threshold: float,
) -> np.ndarray:
    probability = np.clip(
        np.asarray(probability_up, dtype=np.float64),
        1e-6,
        1.0 - 1e-6,
    )
    threshold = float(np.clip(threshold, 1e-6, 1.0 - 1e-6))
    logit = np.log(probability / (1.0 - probability))
    threshold_logit = math.log(threshold / (1.0 - threshold))
    return 1.0 / (1.0 + np.exp(-np.clip(logit - threshold_logit, -30.0, 30.0)))


def apply_calibration(
    raw: np.ndarray,
    rows: list[dict[str, float]],
    *,
    probability: bool = False,
    nonnegative: bool = False,
) -> np.ndarray:
    output = raw.astype(np.float64, copy=True)
    for index, row in enumerate(rows):
        output[:, index] = (
            float(row["slope"]) * output[:, index] + float(row["intercept"])
        )
    if probability:
        output = 1.0 / (1.0 + np.exp(-np.clip(output, -30.0, 30.0)))
    if nonnegative:
        output = np.maximum(0.0, output)
    return output


def _safe_correlation(left: np.ndarray, right: np.ndarray) -> float | None:
    valid = np.isfinite(left) & np.isfinite(right)
    if valid.sum() < 3 or np.std(left[valid]) <= 1e-12 or np.std(right[valid]) <= 1e-12:
        return None
    return float(np.corrcoef(left[valid], right[valid])[0, 1])


def _profit_factor(values: np.ndarray) -> float | None:
    gains = float(values[values > 0.0].sum())
    losses = float(-values[values < 0.0].sum())
    if losses <= 1e-12:
        return None if gains <= 0.0 else 999.0
    return gains / losses


def _lower_95_mean(values: np.ndarray) -> float | None:
    finite_values = np.asarray(values, dtype=np.float64)
    finite_values = finite_values[np.isfinite(finite_values)]
    if finite_values.size < 2:
        return None
    return float(
        np.mean(finite_values)
        - 1.96
        * np.std(finite_values, ddof=1)
        / math.sqrt(finite_values.size)
    )


def _pair_centered_correlation(
    actual: np.ndarray,
    predicted: np.ndarray,
    instruments: np.ndarray,
) -> float | None:
    actual_values = np.asarray(actual, dtype=np.float64)
    predicted_values = np.asarray(predicted, dtype=np.float64)
    pairs = np.asarray(instruments, dtype=str)
    valid = np.isfinite(actual_values) & np.isfinite(predicted_values)
    centered_actual: list[np.ndarray] = []
    centered_predicted: list[np.ndarray] = []
    for pair in np.unique(pairs[valid]):
        mask = valid & (pairs == pair)
        if int(mask.sum()) < 2:
            continue
        centered_actual.append(actual_values[mask] - np.mean(actual_values[mask]))
        centered_predicted.append(
            predicted_values[mask] - np.mean(predicted_values[mask])
        )
    if not centered_actual:
        return None
    return _safe_correlation(
        np.concatenate(centered_actual),
        np.concatenate(centered_predicted),
    )


def _selected_group_means(
    values: np.ndarray,
    group_codes: np.ndarray,
    group_count: int,
    selected: np.ndarray,
) -> np.ndarray:
    selected_codes = group_codes[selected]
    counts = np.bincount(selected_codes, minlength=group_count)
    active = counts > 0
    if not active.any():
        return np.asarray([], dtype=np.float64)
    sums = np.bincount(
        selected_codes,
        weights=np.asarray(values, dtype=np.float64)[selected],
        minlength=group_count,
    )
    return sums[active] / counts[active]


def confidence_bins(
    actual_up: np.ndarray,
    probability_up: np.ndarray,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    edges = np.linspace(0.0, 1.0, 11)
    for lower, upper in zip(edges[:-1], edges[1:]):
        mask = (probability_up >= lower) & (
            probability_up <= upper if math.isclose(upper, 1.0) else probability_up < upper
        )
        if not mask.any():
            continue
        output.append(
            {
                "lower": round(float(lower), 2),
                "upper": round(float(upper), 2),
                "n": int(mask.sum()),
                "mean_probability_up": round(float(np.mean(probability_up[mask])), 6),
                "observed_up_rate": round(float(np.mean(actual_up[mask])), 6),
            }
        )
    return output


def executable_edge_threshold(
    actual_long_net: np.ndarray,
    actual_short_net: np.ndarray,
    probability_up: np.ndarray,
    predicted_long_net: np.ndarray,
    predicted_short_net: np.ndarray,
    direction_threshold_value: float,
    *,
    minimum_fraction: float = 0.05,
    minimum_rows: int = 30,
) -> float:
    """Choose a validation-only entry gate for predicted executable edge."""

    decision_probability = decision_centered_probability(
        probability_up,
        direction_threshold_value,
    )
    predicted_up = decision_probability >= 0.5
    actual_selected = np.where(
        predicted_up,
        actual_long_net,
        actual_short_net,
    )
    predicted_selected = np.where(
        predicted_up,
        predicted_long_net,
        predicted_short_net,
    )
    valid = np.isfinite(actual_selected) & np.isfinite(predicted_selected)
    if not valid.any():
        return math.inf
    actual = actual_selected[valid].astype(np.float64)
    predicted = predicted_selected[valid].astype(np.float64)
    required = max(
        int(minimum_rows),
        int(math.ceil(len(predicted) * max(0.0, minimum_fraction))),
    )
    quantiles = np.linspace(0.0, 0.95, 40)
    candidates = sorted(
        {
            0.0,
            *(
                max(0.0, float(value))
                for value in np.quantile(predicted, quantiles)
                if math.isfinite(float(value))
            ),
        }
    )
    best: tuple[tuple[float, float, int], float] | None = None
    for threshold in candidates:
        selected = predicted >= threshold
        count = int(selected.sum())
        if count < required:
            continue
        values = actual[selected]
        average = float(np.mean(values))
        standard_error = (
            float(np.std(values, ddof=1)) / math.sqrt(count)
            if count > 1
            else math.inf
        )
        lower_95 = (
            average - 1.96 * standard_error
            if math.isfinite(standard_error)
            else -math.inf
        )
        rank = (lower_95, average, count)
        if best is None or rank > best[0]:
            best = (rank, float(threshold))
    if best is not None:
        return best[1]
    return math.inf


def executable_edge_metrics(
    actual_long_net: np.ndarray,
    actual_short_net: np.ndarray,
    instruments: np.ndarray,
    probability_up: np.ndarray,
    predicted_long_net: np.ndarray,
    predicted_short_net: np.ndarray,
    direction_threshold_value: float,
    entry_threshold: float,
) -> dict[str, Any]:
    decision_probability = decision_centered_probability(
        probability_up,
        direction_threshold_value,
    )
    predicted_up = decision_probability >= 0.5
    actual_selected = np.where(
        predicted_up,
        actual_long_net,
        actual_short_net,
    )
    predicted_selected = np.where(
        predicted_up,
        predicted_long_net,
        predicted_short_net,
    )
    valid = np.isfinite(actual_selected) & np.isfinite(predicted_selected)
    selected = valid & (predicted_selected >= entry_threshold)
    count = int(selected.sum())
    base = {
        "executable_edge_threshold": (
            None if not math.isfinite(entry_threshold) else round(entry_threshold, 8)
        ),
        "executable_edge_entry_n": count,
        "executable_edge_entry_fraction": round(
            count / max(1, int(valid.sum())),
            6,
        ),
    }
    if not count:
        return {
            **base,
            "executable_edge_average_net_pips": None,
            "executable_edge_median_net_pips": None,
            "executable_edge_total_net_pips": 0.0,
            "executable_edge_win_rate": None,
            "executable_edge_lower_95_net_pips": None,
            "executable_edge_profit_factor": None,
            "executable_edge_positive_pair_fraction": None,
            "executable_edge_prediction_mae": None,
            "executable_edge_prediction_bias": None,
        }
    actual = actual_selected[selected].astype(np.float64)
    predicted = predicted_selected[selected].astype(np.float64)
    average = float(np.mean(actual))
    standard_error = (
        float(np.std(actual, ddof=1)) / math.sqrt(count)
        if count > 1
        else math.inf
    )
    pair_averages = [
        float(np.mean(actual_selected[selected & (instruments == pair)]))
        for pair in np.unique(instruments[selected])
    ]
    return {
        **base,
        "executable_edge_average_net_pips": round(average, 6),
        "executable_edge_median_net_pips": round(float(np.median(actual)), 6),
        "executable_edge_total_net_pips": round(float(np.sum(actual)), 4),
        "executable_edge_win_rate": round(float(np.mean(actual > 0.0)), 6),
        "executable_edge_lower_95_net_pips": (
            round(average - 1.96 * standard_error, 6)
            if math.isfinite(standard_error)
            else None
        ),
        "executable_edge_profit_factor": (
            None
            if (value := _profit_factor(actual)) is None
            else round(value, 6)
        ),
        "executable_edge_positive_pair_fraction": round(
            float(np.mean(np.asarray(pair_averages) > 0.0)),
            6,
        ),
        "executable_edge_prediction_mae": round(
            float(np.mean(np.abs(predicted - actual))),
            6,
        ),
        "executable_edge_prediction_bias": round(
            float(np.mean(predicted - actual)),
            6,
        ),
    }


def movement_cost_gate_threshold(
    actual_long_net: np.ndarray,
    actual_short_net: np.ndarray,
    decision_cost_pips: np.ndarray,
    instruments: np.ndarray,
    probability_up: np.ndarray,
    predicted_magnitude: np.ndarray,
    direction_threshold_value: float,
    *,
    minimum_fraction: float = 0.02,
    minimum_rows: int = 50,
    minimum_pairs: int = 8,
    minimum_pair_fraction: float = 0.25,
) -> dict[str, Any]:
    """Fit a validation-only magnitude/cost and confidence entry gate."""

    decision_probability = decision_centered_probability(
        probability_up,
        direction_threshold_value,
    )
    predicted_up = decision_probability >= 0.5
    actual_selected = np.where(
        predicted_up,
        actual_long_net,
        actual_short_net,
    ).astype(np.float64)
    estimated_cost = np.maximum(
        0.05,
        np.asarray(decision_cost_pips, dtype=np.float64),
    )
    normalized_actual_selected = actual_selected / estimated_cost
    magnitude = np.maximum(
        0.0,
        np.asarray(predicted_magnitude, dtype=np.float64),
    )
    ratio = magnitude / estimated_cost
    confidence = np.maximum(decision_probability, 1.0 - decision_probability)
    pairs = np.asarray(instruments, dtype=str)
    pair_labels, pair_codes = np.unique(pairs, return_inverse=True)
    valid = (
        np.isfinite(actual_selected)
        & np.isfinite(ratio)
        & np.isfinite(confidence)
    )
    valid_count = int(valid.sum())
    if not valid_count:
        return {
            "minimum_magnitude_to_cost": None,
            "minimum_confidence": None,
            "validation_entry_n": 0,
            "validation_pair_count": 0,
            "validation_pair_average_net_lower_95": None,
            "validation_pair_average_net_cost_units_lower_95": None,
            "candidate_count": 0,
        }
    required_rows = max(
        int(minimum_rows),
        int(math.ceil(valid_count * max(0.0, minimum_fraction))),
    )
    total_pairs = int(np.unique(pair_codes[valid]).size)
    required_pairs = min(
        total_pairs,
        max(
            1,
            int(minimum_pairs),
            int(math.ceil(total_pairs * max(0.0, minimum_pair_fraction))),
        ),
    )
    ratio_candidates = sorted(
        {
            1.0,
            *(
                max(1.0, float(value))
                for value in np.quantile(
                    ratio[valid],
                    np.linspace(0.50, 0.98, 17),
                )
                if math.isfinite(float(value))
            ),
        }
    )
    confidence_candidates = sorted(
        {
            0.50,
            0.525,
            0.55,
            0.575,
            0.60,
            0.625,
            0.65,
            *(
                max(0.50, min(0.90, float(value)))
                for value in np.quantile(
                    confidence[valid],
                    (0.50, 0.65, 0.75, 0.85, 0.90, 0.95),
                )
                if math.isfinite(float(value))
            ),
        }
    )
    best: tuple[tuple[float, ...], dict[str, Any]] | None = None
    candidate_count = 0
    for minimum_ratio in ratio_candidates:
        for minimum_confidence_value in confidence_candidates:
            selected = (
                valid
                & (ratio >= minimum_ratio)
                & (confidence >= minimum_confidence_value)
            )
            count = int(selected.sum())
            if count < required_rows:
                continue
            selected_pair_codes = np.unique(pair_codes[selected])
            if selected_pair_codes.size < required_pairs:
                continue
            candidate_count += 1
            values = actual_selected[selected]
            normalized_values = normalized_actual_selected[selected]
            pair_averages = _selected_group_means(
                actual_selected,
                pair_codes,
                len(pair_labels),
                selected,
            )
            pair_normalized_averages = _selected_group_means(
                normalized_actual_selected,
                pair_codes,
                len(pair_labels),
                selected,
            )
            pair_lower = _lower_95_mean(pair_averages)
            row_lower = _lower_95_mean(values)
            normalized_pair_lower = _lower_95_mean(
                pair_normalized_averages
            )
            normalized_row_lower = _lower_95_mean(normalized_values)
            average = float(np.mean(values))
            positive_pair_fraction = float(np.mean(pair_averages > 0.0))
            rank = (
                -math.inf
                if normalized_pair_lower is None
                else normalized_pair_lower,
                -math.inf if pair_lower is None else pair_lower,
                -math.inf
                if normalized_row_lower is None
                else normalized_row_lower,
                positive_pair_fraction,
                average,
                count,
            )
            payload = {
                "minimum_magnitude_to_cost": round(float(minimum_ratio), 8),
                "minimum_confidence": round(
                    float(minimum_confidence_value),
                    8,
                ),
                "validation_entry_n": count,
                "validation_pair_count": int(selected_pair_codes.size),
                "validation_average_net_pips": round(average, 6),
                "validation_net_lower_95": (
                    None if row_lower is None else round(row_lower, 6)
                ),
                "validation_pair_average_net_lower_95": (
                    None if pair_lower is None else round(pair_lower, 6)
                ),
                "validation_average_net_cost_units": round(
                    float(np.mean(normalized_values)),
                    6,
                ),
                "validation_net_cost_units_lower_95": (
                    None
                    if normalized_row_lower is None
                    else round(normalized_row_lower, 6)
                ),
                "validation_pair_average_net_cost_units_lower_95": (
                    None
                    if normalized_pair_lower is None
                    else round(normalized_pair_lower, 6)
                ),
                "validation_positive_pair_fraction": round(
                    positive_pair_fraction,
                    6,
                ),
            }
            if best is None or rank > best[0]:
                best = (rank, payload)
    if best is None:
        return {
            "minimum_magnitude_to_cost": None,
            "minimum_confidence": None,
            "validation_entry_n": 0,
            "validation_pair_count": 0,
            "validation_pair_average_net_lower_95": None,
            "validation_pair_average_net_cost_units_lower_95": None,
            "candidate_count": candidate_count,
            "required_rows": required_rows,
            "required_pairs": required_pairs,
        }
    return {
        **best[1],
        "candidate_count": candidate_count,
        "required_rows": required_rows,
        "required_pairs": required_pairs,
    }


def movement_cost_gate_metrics(
    actual_signed: np.ndarray,
    actual_long_net: np.ndarray,
    actual_short_net: np.ndarray,
    decision_cost_pips: np.ndarray,
    exact_cost: np.ndarray,
    instruments: np.ndarray,
    probability_up: np.ndarray,
    predicted_magnitude: np.ndarray,
    direction_threshold_value: float,
    gate: Mapping[str, Any],
) -> dict[str, Any]:
    minimum_ratio = finite(gate.get("minimum_magnitude_to_cost"), math.inf)
    minimum_confidence_value = finite(gate.get("minimum_confidence"), math.inf)
    decision_probability = decision_centered_probability(
        probability_up,
        direction_threshold_value,
    )
    predicted_up = decision_probability >= 0.5
    actual_up = np.asarray(actual_signed, dtype=np.float64) > 0.0
    actual_selected = np.where(
        predicted_up,
        actual_long_net,
        actual_short_net,
    ).astype(np.float64)
    estimated_cost = np.maximum(
        0.05,
        np.asarray(decision_cost_pips, dtype=np.float64),
    )
    normalized_actual_selected = actual_selected / estimated_cost
    magnitude = np.maximum(
        0.0,
        np.asarray(predicted_magnitude, dtype=np.float64),
    )
    ratio = magnitude / estimated_cost
    confidence = np.maximum(decision_probability, 1.0 - decision_probability)
    pairs = np.asarray(instruments, dtype=str)
    pair_labels, pair_codes = np.unique(pairs, return_inverse=True)
    valid = (
        np.isfinite(actual_selected)
        & np.isfinite(ratio)
        & np.isfinite(confidence)
    )
    selected = (
        valid
        & (ratio >= minimum_ratio)
        & (confidence >= minimum_confidence_value)
    )
    count = int(selected.sum())
    base = {
        "movement_gate_minimum_magnitude_to_cost": (
            None if not math.isfinite(minimum_ratio) else round(minimum_ratio, 8)
        ),
        "movement_gate_minimum_confidence": (
            None
            if not math.isfinite(minimum_confidence_value)
            else round(minimum_confidence_value, 8)
        ),
        "movement_gate_entry_n": count,
        "movement_gate_entry_fraction": round(
            count / max(1, int(valid.sum())),
            6,
        ),
    }
    if not count:
        return {
            **base,
            "movement_gate_pair_count": 0,
            "movement_gate_average_net_pips": None,
            "movement_gate_net_lower_95": None,
            "movement_gate_pair_average_net_lower_95": None,
            "movement_gate_average_net_cost_units": None,
            "movement_gate_net_cost_units_lower_95": None,
            "movement_gate_pair_average_net_cost_units_lower_95": None,
            "movement_gate_median_net_pips": None,
            "movement_gate_median_net_cost_units": None,
            "movement_gate_total_net_pips": 0.0,
            "movement_gate_win_rate": None,
            "movement_gate_profit_factor": None,
            "movement_gate_positive_pair_fraction": None,
            "movement_gate_direction_accuracy": None,
            "movement_gate_direction_accuracy_lower_95": None,
            "movement_gate_macro_direction_accuracy": None,
            "movement_gate_macro_direction_accuracy_lower_95": None,
            "movement_gate_balanced_accuracy": None,
            "movement_gate_exact_cost_fraction": None,
            "movement_gate_mean_predicted_magnitude_to_cost": None,
            "movement_gate_mean_decision_cost_pips": None,
        }
    values = actual_selected[selected]
    normalized_values = normalized_actual_selected[selected]
    selected_pair_codes = np.unique(pair_codes[selected])
    pair_averages = _selected_group_means(
        actual_selected,
        pair_codes,
        len(pair_labels),
        selected,
    )
    pair_normalized_averages = _selected_group_means(
        normalized_actual_selected,
        pair_codes,
        len(pair_labels),
        selected,
    )
    direction_correct = predicted_up[selected] == actual_up[selected]
    direction_accuracy = float(np.mean(direction_correct))
    direction_standard_error = math.sqrt(
        max(1e-12, direction_accuracy * (1.0 - direction_accuracy)) / count
    )
    pair_direction_accuracies = _selected_group_means(
        (predicted_up == actual_up).astype(np.float64),
        pair_codes,
        len(pair_labels),
        selected,
    )
    balanced = (
        float(
            balanced_accuracy_score(
                actual_up[selected],
                predicted_up[selected],
            )
        )
        if np.unique(actual_up[selected]).size > 1
        else None
    )
    row_lower = _lower_95_mean(values)
    pair_lower = _lower_95_mean(pair_averages)
    normalized_row_lower = _lower_95_mean(normalized_values)
    normalized_pair_lower = _lower_95_mean(pair_normalized_averages)
    pair_direction_lower = _lower_95_mean(pair_direction_accuracies)
    return {
        **base,
        "movement_gate_pair_count": int(selected_pair_codes.size),
        "movement_gate_average_net_pips": round(float(np.mean(values)), 6),
        "movement_gate_net_lower_95": (
            None if row_lower is None else round(row_lower, 6)
        ),
        "movement_gate_pair_average_net_lower_95": (
            None if pair_lower is None else round(pair_lower, 6)
        ),
        "movement_gate_average_net_cost_units": round(
            float(np.mean(normalized_values)),
            6,
        ),
        "movement_gate_net_cost_units_lower_95": (
            None
            if normalized_row_lower is None
            else round(normalized_row_lower, 6)
        ),
        "movement_gate_pair_average_net_cost_units_lower_95": (
            None
            if normalized_pair_lower is None
            else round(normalized_pair_lower, 6)
        ),
        "movement_gate_median_net_pips": round(float(np.median(values)), 6),
        "movement_gate_median_net_cost_units": round(
            float(np.median(normalized_values)),
            6,
        ),
        "movement_gate_total_net_pips": round(float(np.sum(values)), 4),
        "movement_gate_win_rate": round(float(np.mean(values > 0.0)), 6),
        "movement_gate_profit_factor": (
            None
            if (value := _profit_factor(values)) is None
            else round(value, 6)
        ),
        "movement_gate_positive_pair_fraction": round(
            float(np.mean(pair_averages > 0.0)),
            6,
        ),
        "movement_gate_direction_accuracy": round(direction_accuracy, 6),
        "movement_gate_direction_accuracy_lower_95": round(
            direction_accuracy - 1.96 * direction_standard_error,
            6,
        ),
        "movement_gate_macro_direction_accuracy": round(
            float(np.mean(pair_direction_accuracies)),
            6,
        ),
        "movement_gate_macro_direction_accuracy_lower_95": (
            None
            if pair_direction_lower is None
            else round(pair_direction_lower, 6)
        ),
        "movement_gate_balanced_accuracy": (
            None if balanced is None else round(balanced, 6)
        ),
        "movement_gate_exact_cost_fraction": round(
            float(np.mean(np.asarray(exact_cost, dtype=bool)[selected])),
            6,
        ),
        "movement_gate_mean_predicted_magnitude_to_cost": round(
            float(np.mean(ratio[selected])),
            6,
        ),
        "movement_gate_mean_decision_cost_pips": round(
            float(np.mean(estimated_cost[selected])),
            6,
        ),
    }


def movement_cost_gate_pass(metrics: Mapping[str, Any]) -> bool:
    return bool(
        int(metrics.get("movement_gate_entry_n") or 0) >= 50
        and int(metrics.get("movement_gate_pair_count") or 0) >= 8
        and finite(
            metrics.get("movement_gate_direction_accuracy_lower_95"),
            -math.inf,
        )
        > 0.50
        and finite(
            metrics.get("movement_gate_macro_direction_accuracy_lower_95"),
            -math.inf,
        )
        > 0.50
        and finite(
            metrics.get("movement_gate_pair_average_net_lower_95"),
            -math.inf,
        )
        > 0.0
        and finite(
            metrics.get(
                "movement_gate_pair_average_net_cost_units_lower_95"
            ),
            -math.inf,
        )
        > 0.0
        and finite(
            metrics.get("movement_gate_positive_pair_fraction"),
            -math.inf,
        )
        >= 0.50
        and finite(metrics.get("movement_gate_profit_factor"), 0.0) > 1.0
    )


def cell_metrics(
    actual_signed: np.ndarray,
    actual_long_net: np.ndarray,
    actual_short_net: np.ndarray,
    decision_cost_pips: np.ndarray,
    exact_cost: np.ndarray,
    instruments: np.ndarray,
    probability_up: np.ndarray,
    predicted_signed: np.ndarray,
    predicted_magnitude: np.ndarray,
    direction_threshold_value: float = 0.5,
) -> dict[str, Any]:
    actual_up = actual_signed > 0.0
    decision_probability = decision_centered_probability(
        probability_up,
        direction_threshold_value,
    )
    predicted_up = decision_probability >= 0.5
    selected_net = np.where(predicted_up, actual_long_net, actual_short_net)
    decision_cost = np.maximum(
        0.05,
        np.asarray(decision_cost_pips, dtype=np.float64),
    )
    selected_net_cost_units = selected_net / decision_cost
    absolute_actual = np.abs(actual_signed)
    pair_averages = {
        pair: float(np.mean(selected_net[instruments == pair]))
        for pair in np.unique(instruments)
    }
    pair_cost_unit_averages = {
        pair: float(
            np.mean(selected_net_cost_units[instruments == pair])
        )
        for pair in np.unique(instruments)
    }
    try:
        auc = (
            float(roc_auc_score(actual_up, probability_up))
            if np.unique(actual_up).size > 1
            else None
        )
    except ValueError:
        auc = None
    try:
        rank = spearmanr(actual_signed, predicted_signed).statistic
        rank_correlation = float(rank) if math.isfinite(float(rank)) else None
    except (ValueError, TypeError):
        rank_correlation = None
    direction_correct = predicted_up == actual_up
    unique_pairs = np.unique(instruments)
    pair_direction_accuracies = np.asarray(
        [
            np.mean(direction_correct[instruments == pair])
            for pair in unique_pairs
        ],
        dtype=np.float64,
    )
    pair_balanced_accuracies = np.asarray(
        [
            balanced_accuracy_score(
                actual_up[instruments == pair],
                predicted_up[instruments == pair],
            )
            for pair in unique_pairs
            if np.unique(actual_up[instruments == pair]).size > 1
        ],
        dtype=np.float64,
    )
    macro_direction_lower = _lower_95_mean(pair_direction_accuracies)
    macro_balanced_lower = _lower_95_mean(pair_balanced_accuracies)
    confidence = np.maximum(decision_probability, 1.0 - decision_probability)
    high_confidence = confidence >= 0.60
    n = len(actual_signed)
    standard_error = (
        math.sqrt(max(1e-12, float(direction_correct.mean()) * (1.0 - float(direction_correct.mean()))) / n)
        if n
        else math.inf
    )
    return {
        "n": n,
        "pair_count": len(pair_averages),
        "exact_cost_fraction": round(float(np.mean(exact_cost)), 6),
        "direction_accuracy": round(float(np.mean(direction_correct)), 6),
        "direction_accuracy_pvalue": round(
            float(
                binomtest(
                    int(direction_correct.sum()),
                    n=max(1, n),
                    p=0.5,
                    alternative="greater",
                ).pvalue
            ),
            10,
        ),
        "direction_accuracy_pvalue_unclustered": round(
            float(
                binomtest(
                    int(direction_correct.sum()),
                    n=max(1, n),
                    p=0.5,
                    alternative="greater",
                ).pvalue
            ),
            10,
        ),
        "direction_threshold": round(float(direction_threshold_value), 8),
        "direction_accuracy_lower_95": round(
            float(np.mean(direction_correct)) - 1.96 * standard_error,
            6,
        ),
        "macro_direction_accuracy": round(
            float(np.mean(pair_direction_accuracies)),
            6,
        ),
        "macro_direction_accuracy_lower_95": (
            None
            if macro_direction_lower is None
            else round(macro_direction_lower, 6)
        ),
        "direction_positive_pair_fraction": round(
            float(np.mean(pair_direction_accuracies > 0.50)),
            6,
        ),
        "balanced_accuracy": round(
            float(balanced_accuracy_score(actual_up, predicted_up)),
            6,
        ),
        "macro_balanced_accuracy": (
            round(float(np.mean(pair_balanced_accuracies)), 6)
            if pair_balanced_accuracies.size
            else None
        ),
        "macro_balanced_accuracy_lower_95": (
            None
            if macro_balanced_lower is None
            else round(macro_balanced_lower, 6)
        ),
        "roc_auc": None if auc is None else round(auc, 6),
        "brier_score": round(
            float(brier_score_loss(actual_up.astype(np.int8), probability_up)),
            6,
        ),
        "log_loss": round(
            float(
                log_loss(
                    actual_up.astype(np.int8),
                    np.column_stack((1.0 - probability_up, probability_up)),
                    labels=(0, 1),
                )
            ),
            6,
        ),
        "predicted_up_fraction": round(float(np.mean(predicted_up)), 6),
        "high_confidence_fraction": round(float(np.mean(high_confidence)), 6),
        "high_confidence_accuracy": (
            round(float(np.mean(direction_correct[high_confidence])), 6)
            if high_confidence.any()
            else None
        ),
        "signed_pip_mae": round(
            float(mean_absolute_error(actual_signed, predicted_signed)),
            6,
        ),
        "signed_pip_rmse": round(
            math.sqrt(float(mean_squared_error(actual_signed, predicted_signed))),
            6,
        ),
        "signed_pip_bias": round(float(np.mean(predicted_signed - actual_signed)), 6),
        "signed_pip_r2": round(float(r2_score(actual_signed, predicted_signed)), 6),
        "signed_pip_correlation": (
            None
            if (value := _safe_correlation(actual_signed, predicted_signed)) is None
            else round(value, 6)
        ),
        "within_pair_signed_pip_correlation": (
            None
            if (
                value := _pair_centered_correlation(
                    actual_signed,
                    predicted_signed,
                    instruments,
                )
            )
            is None
            else round(value, 6)
        ),
        "signed_pip_rank_correlation": (
            None if rank_correlation is None else round(rank_correlation, 6)
        ),
        "magnitude_pip_mae": round(
            float(mean_absolute_error(absolute_actual, predicted_magnitude)),
            6,
        ),
        "magnitude_pip_rmse": round(
            math.sqrt(float(mean_squared_error(absolute_actual, predicted_magnitude))),
            6,
        ),
        "magnitude_pip_bias": round(
            float(np.mean(predicted_magnitude - absolute_actual)),
            6,
        ),
        "magnitude_pip_correlation": (
            None
            if (value := _safe_correlation(absolute_actual, predicted_magnitude)) is None
            else round(value, 6)
        ),
        "within_pair_magnitude_pip_correlation": (
            None
            if (
                value := _pair_centered_correlation(
                    absolute_actual,
                    predicted_magnitude,
                    instruments,
                )
            )
            is None
            else round(value, 6)
        ),
        "executable_average_net_pips": round(float(np.mean(selected_net)), 6),
        "executable_net_lower_95_pips": (
            None
            if (value := _lower_95_mean(selected_net)) is None
            else round(value, 6)
        ),
        "executable_median_net_pips": round(float(np.median(selected_net)), 6),
        "executable_total_net_pips": round(float(np.sum(selected_net)), 4),
        "executable_win_rate": round(float(np.mean(selected_net > 0.0)), 6),
        "executable_profit_factor": (
            None
            if (value := _profit_factor(selected_net)) is None
            else round(value, 6)
        ),
        "executable_average_net_cost_units": round(
            float(np.mean(selected_net_cost_units)),
            6,
        ),
        "executable_net_cost_units_lower_95": (
            None
            if (value := _lower_95_mean(selected_net_cost_units)) is None
            else round(value, 6)
        ),
        "executable_median_net_cost_units": round(
            float(np.median(selected_net_cost_units)),
            6,
        ),
        "pair_average_net_cost_units_lower_95": (
            None
            if (
                value := _lower_95_mean(
                    np.asarray(
                        list(pair_cost_unit_averages.values()),
                        dtype=np.float64,
                    )
                )
            )
            is None
            else round(value, 6)
        ),
        "mean_pair_average_net_cost_units": round(
            float(np.mean(list(pair_cost_unit_averages.values()))),
            6,
        ),
        "positive_pair_fraction": round(
            float(np.mean(np.asarray(list(pair_averages.values())) > 0.0)),
            6,
        ),
        "mean_pair_average_net_pips": round(
            float(np.mean(list(pair_averages.values()))),
            6,
        ),
        "pair_average_net_lower_95_pips": (
            None
            if (
                value := _lower_95_mean(
                    np.asarray(list(pair_averages.values()), dtype=np.float64)
                )
            )
            is None
            else round(value, 6)
        ),
        "mean_actual_magnitude_pips": round(float(np.mean(absolute_actual)), 6),
        "confidence_bins": confidence_bins(actual_up, probability_up),
    }


def top_coefficients(
    estimator: Any,
    feature_names: tuple[str, ...],
    horizons_sec: tuple[int, ...],
    top_n: int = 12,
) -> dict[str, list[dict[str, Any]]]:
    if isinstance(estimator, Pipeline):
        coefficients = np.asarray(estimator.named_steps["ridge"].coef_)
    else:
        try:
            importance = np.asarray(estimator.feature_importances_)
        except (AttributeError, ValueError):
            return {}
        coefficients = np.repeat(
            importance.reshape(1, -1),
            len(horizons_sec),
            axis=0,
        )
    if coefficients.ndim == 1:
        coefficients = coefficients.reshape(1, -1)
    output: dict[str, list[dict[str, Any]]] = {}
    for horizon_index, horizon in enumerate(horizons_sec):
        if horizon_index >= coefficients.shape[0]:
            continue
        row = coefficients[horizon_index]
        indexes = np.argsort(np.abs(row))[-top_n:][::-1]
        output[str(horizon)] = [
            {
                "feature": feature_names[index],
                "coefficient": round(float(row[index]), 8),
            }
            for index in indexes
        ]
    return output


def fit_timeframe(
    dataset: TimeframeDataset,
    horizons_sec: tuple[int, ...],
    alpha: float,
    minimum_rows: int,
    target_space: str = "raw_pips",
    target_clip_quantile: float = 0.995,
    estimator_type: str = "ridge",
    xgb_device: str = "cuda",
    xgb_estimators: int = 240,
    xgb_max_depth: int = 4,
    xgb_learning_rate: float = 0.035,
    pair_context: str = "none",
    executable_edge_head: bool = True,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    split_masks = {name: dataset.splits == index for index, name in enumerate(SPLITS)}
    counts = {name: int(mask.sum()) for name, mask in split_masks.items()}
    if min(counts.values()) < minimum_rows:
        return None, {
            "timeframe": dataset.timeframe,
            "status": "insufficient_split_rows",
            "split_rows": counts,
            "feature_count": len(dataset.feature_names),
        }
    development = split_masks["development"]
    validation = split_masks["validation"]
    direction_target = np.where(dataset.signed_pips > 0.0, 1.0, -1.0)
    if target_space == "local_scale":
        try:
            scale_index = dataset.feature_names.index("ma__scale_pips")
        except ValueError as exc:
            raise RuntimeError("local-scale target requires ma__scale_pips") from exc
        target_scale = np.maximum(
            0.1,
            np.asarray(dataset.features[:, scale_index], dtype=np.float64),
        )
        normalized_signed = (
            np.asarray(dataset.signed_pips, dtype=np.float64)
            / target_scale[:, None]
        )
        clip_quantile = float(np.clip(target_clip_quantile, 0.90, 1.0))
        target_clip_abs = np.maximum(
            0.5,
            np.nanquantile(
                np.abs(normalized_signed[development]),
                clip_quantile,
                axis=0,
            ),
        )
        fit_signed_target = np.clip(
            normalized_signed,
            -target_clip_abs,
            target_clip_abs,
        )
        fit_magnitude_target = np.clip(
            np.abs(normalized_signed),
            0.0,
            target_clip_abs,
        )
        fit_long_net_target = np.clip(
            np.asarray(dataset.long_net_pips, dtype=np.float64)
            / target_scale[:, None],
            -target_clip_abs,
            target_clip_abs,
        )
        fit_short_net_target = np.clip(
            np.asarray(dataset.short_net_pips, dtype=np.float64)
            / target_scale[:, None],
            -target_clip_abs,
            target_clip_abs,
        )
    else:
        target_space = "raw_pips"
        target_scale = np.ones(dataset.features.shape[0], dtype=np.float64)
        target_clip_abs = np.full(len(horizons_sec), np.inf, dtype=np.float64)
        fit_signed_target = np.asarray(dataset.signed_pips, dtype=np.float64)
        fit_magnitude_target = np.abs(fit_signed_target)
        fit_long_net_target = np.asarray(
            dataset.long_net_pips,
            dtype=np.float64,
        )
        fit_short_net_target = np.asarray(
            dataset.short_net_pips,
            dtype=np.float64,
        )
    fit_edge_target = np.column_stack(
        (fit_long_net_target, fit_short_net_target)
    )
    estimator_options = {
        "estimator_type": estimator_type,
        "xgb_device": xgb_device,
        "xgb_estimators": xgb_estimators,
        "xgb_max_depth": xgb_max_depth,
        "xgb_learning_rate": xgb_learning_rate,
    }
    direction_estimator = fit_pipeline(alpha, **estimator_options)
    pip_estimator = fit_pipeline(alpha, **estimator_options)
    magnitude_estimator = fit_pipeline(alpha, **estimator_options)
    executable_edge_estimator = (
        fit_pipeline(alpha, **estimator_options)
        if executable_edge_head
        else None
    )
    direction_estimator.fit(dataset.features[development], direction_target[development])
    pip_estimator.fit(dataset.features[development], fit_signed_target[development])
    magnitude_estimator.fit(
        dataset.features[development],
        fit_magnitude_target[development],
    )
    if executable_edge_estimator is not None:
        executable_edge_estimator.fit(
            dataset.features[development],
            fit_edge_target[development],
        )
    validation_direction_raw = np.asarray(
        direction_estimator.predict(dataset.features[validation])
    )
    validation_signed_raw = np.asarray(pip_estimator.predict(dataset.features[validation]))
    validation_magnitude_raw = np.asarray(
        magnitude_estimator.predict(dataset.features[validation])
    )
    validation_edge_raw = (
        np.asarray(
            executable_edge_estimator.predict(dataset.features[validation])
        )
        if executable_edge_estimator is not None
        else None
    )
    if validation_direction_raw.ndim == 1:
        validation_direction_raw = validation_direction_raw.reshape(-1, 1)
        validation_signed_raw = validation_signed_raw.reshape(-1, 1)
        validation_magnitude_raw = validation_magnitude_raw.reshape(-1, 1)
    if validation_edge_raw is not None and validation_edge_raw.ndim == 1:
        validation_edge_raw = validation_edge_raw.reshape(-1, 1)
    direction_calibrators = [
        direction_calibration(
            validation_direction_raw[:, index],
            dataset.signed_pips[validation, index] > 0.0,
        )
        for index in range(len(horizons_sec))
    ]
    pip_calibrators = [
        affine_calibration(
            validation_signed_raw[:, index],
            fit_signed_target[validation, index],
        )
        for index in range(len(horizons_sec))
    ]
    magnitude_calibrators = [
        affine_calibration(
            validation_magnitude_raw[:, index],
            fit_magnitude_target[validation, index],
            nonnegative=True,
        )
        for index in range(len(horizons_sec))
    ]
    executable_edge_calibrators = (
        [
            affine_calibration(
                validation_edge_raw[:, index],
                fit_edge_target[validation, index],
            )
            for index in range(2 * len(horizons_sec))
        ]
        if validation_edge_raw is not None
        else []
    )
    validation_probability = apply_calibration(
        validation_direction_raw,
        direction_calibrators,
        probability=True,
    )
    validation_magnitude = apply_calibration(
        validation_magnitude_raw,
        magnitude_calibrators,
        nonnegative=True,
    )
    if target_space == "local_scale":
        validation_magnitude = np.clip(
            validation_magnitude,
            0.0,
            target_clip_abs,
        ) * target_scale[validation, None]
    direction_thresholds = [
        direction_threshold(
            dataset.signed_pips[validation, index] > 0.0,
            validation_probability[:, index],
        )
        for index in range(len(horizons_sec))
    ]
    validation_edge = (
        apply_calibration(
            validation_edge_raw,
            executable_edge_calibrators,
        )
        if validation_edge_raw is not None
        else None
    )
    if validation_edge is not None and target_space == "local_scale":
        validation_edge = np.clip(
            validation_edge,
            np.tile(-target_clip_abs, 2),
            np.tile(target_clip_abs, 2),
        ) * target_scale[validation, None]
    executable_edge_thresholds = (
        [
            executable_edge_threshold(
                dataset.long_net_pips[validation, index],
                dataset.short_net_pips[validation, index],
                validation_probability[:, index],
                validation_edge[:, index],
                validation_edge[:, index + len(horizons_sec)],
                direction_thresholds[index],
            )
            for index in range(len(horizons_sec))
        ]
        if validation_edge is not None
        else []
    )
    movement_cost_gates = [
        movement_cost_gate_threshold(
            dataset.long_net_pips[validation, index],
            dataset.short_net_pips[validation, index],
            dataset.decision_cost_pips[validation],
            dataset.instruments[validation],
            validation_probability[:, index],
            validation_magnitude[:, index],
            direction_thresholds[index],
        )
        for index in range(len(horizons_sec))
    ]

    split_metrics: dict[str, dict[str, Any]] = {}
    for split_name, mask in split_masks.items():
        raw_direction = np.asarray(direction_estimator.predict(dataset.features[mask]))
        raw_signed = np.asarray(pip_estimator.predict(dataset.features[mask]))
        raw_magnitude = np.asarray(magnitude_estimator.predict(dataset.features[mask]))
        raw_edge = (
            np.asarray(
                executable_edge_estimator.predict(dataset.features[mask])
            )
            if executable_edge_estimator is not None
            else None
        )
        if raw_direction.ndim == 1:
            raw_direction = raw_direction.reshape(-1, 1)
            raw_signed = raw_signed.reshape(-1, 1)
            raw_magnitude = raw_magnitude.reshape(-1, 1)
        if raw_edge is not None and raw_edge.ndim == 1:
            raw_edge = raw_edge.reshape(-1, 1)
        probability = apply_calibration(
            raw_direction,
            direction_calibrators,
            probability=True,
        )
        predicted_signed = apply_calibration(raw_signed, pip_calibrators)
        predicted_magnitude = apply_calibration(
            raw_magnitude,
            magnitude_calibrators,
            nonnegative=True,
        )
        predicted_edge = (
            apply_calibration(raw_edge, executable_edge_calibrators)
            if raw_edge is not None
            else None
        )
        if target_space == "local_scale":
            predicted_signed = np.clip(
                predicted_signed,
                -target_clip_abs,
                target_clip_abs,
            ) * target_scale[mask, None]
            predicted_magnitude = np.clip(
                predicted_magnitude,
                0.0,
                target_clip_abs,
            ) * target_scale[mask, None]
            if predicted_edge is not None:
                predicted_edge = np.clip(
                    predicted_edge,
                    np.tile(-target_clip_abs, 2),
                    np.tile(target_clip_abs, 2),
                ) * target_scale[mask, None]
        split_metrics[split_name] = {}
        for index, horizon in enumerate(horizons_sec):
            metrics = cell_metrics(
                dataset.signed_pips[mask, index],
                dataset.long_net_pips[mask, index],
                dataset.short_net_pips[mask, index],
                dataset.decision_cost_pips[mask],
                dataset.exact_cost[mask, index],
                dataset.instruments[mask],
                probability[:, index],
                predicted_signed[:, index],
                predicted_magnitude[:, index],
                direction_thresholds[index],
            )
            metrics.update(
                movement_cost_gate_metrics(
                    dataset.signed_pips[mask, index],
                    dataset.long_net_pips[mask, index],
                    dataset.short_net_pips[mask, index],
                    dataset.decision_cost_pips[mask],
                    dataset.exact_cost[mask, index],
                    dataset.instruments[mask],
                    probability[:, index],
                    predicted_magnitude[:, index],
                    direction_thresholds[index],
                    movement_cost_gates[index],
                )
            )
            if predicted_edge is not None:
                metrics.update(
                    executable_edge_metrics(
                        dataset.long_net_pips[mask, index],
                        dataset.short_net_pips[mask, index],
                        dataset.instruments[mask],
                        probability[:, index],
                        predicted_edge[:, index],
                        predicted_edge[:, index + len(horizons_sec)],
                        direction_thresholds[index],
                        executable_edge_thresholds[index],
                    )
                )
            split_metrics[split_name][str(horizon)] = metrics

    holdout = split_metrics["holdout"]
    validation_metrics = split_metrics["validation"]
    validation_passes = {
        horizon: (
            row["direction_accuracy_lower_95"] > 0.50
            and row["executable_average_net_pips"] > 0.0
            and row["positive_pair_fraction"] >= 0.50
        )
        for horizon, row in validation_metrics.items()
    }
    holdout_passes = {
        horizon: (
            row["direction_accuracy_lower_95"] > 0.50
            and row["executable_average_net_pips"] > 0.0
            and row["positive_pair_fraction"] >= 0.50
        )
        for horizon, row in holdout.items()
    }
    validation_movement_gate_passes = {
        horizon: movement_cost_gate_pass(row)
        for horizon, row in validation_metrics.items()
    }
    holdout_movement_gate_passes = {
        horizon: movement_cost_gate_pass(row)
        for horizon, row in holdout.items()
    }
    replicated_movement_gate_horizons = [
        int(horizon)
        for horizon in validation_metrics
        if validation_movement_gate_passes[horizon]
        and holdout_movement_gate_passes[horizon]
    ]
    report = {
        "timeframe": dataset.timeframe,
        "status": "fitted",
        "source_rows_read": dataset.source_rows,
        "sampled_rows": dataset.sampled_rows,
        "feature_count": len(dataset.feature_names),
        "estimator_type": estimator_type,
        "pair_context": pair_context,
        "target_space": target_space,
        "executable_edge_head": bool(executable_edge_estimator is not None),
        "target_clip_quantile": (
            round(float(target_clip_quantile), 6)
            if target_space == "local_scale"
            else None
        ),
        "target_clip_abs": (
            [round(float(value), 8) for value in target_clip_abs]
            if target_space == "local_scale"
            else []
        ),
        "split_rows": counts,
        "metrics": split_metrics,
        "validation_pass_horizons": [
            int(horizon) for horizon, passed in validation_passes.items() if passed
        ],
        "holdout_pass_horizons": [
            int(horizon) for horizon, passed in holdout_passes.items() if passed
        ],
        "validation_movement_gate_pass_horizons": [
            int(horizon)
            for horizon, passed in validation_movement_gate_passes.items()
            if passed
        ],
        "holdout_movement_gate_pass_horizons": [
            int(horizon)
            for horizon, passed in holdout_movement_gate_passes.items()
            if passed
        ],
        "replicated_movement_gate_horizons": replicated_movement_gate_horizons,
        "three_split_execution_authorized": False,
        "top_direction_coefficients": top_coefficients(
            direction_estimator,
            dataset.feature_names,
            horizons_sec,
        ),
        "top_pip_coefficients": top_coefficients(
            pip_estimator,
            dataset.feature_names,
            horizons_sec,
        ),
        "top_executable_edge_coefficients": (
            top_coefficients(
                executable_edge_estimator,
                dataset.feature_names,
                (*horizons_sec, *horizons_sec),
            )
            if executable_edge_estimator is not None
            else {}
        ),
        "pair_inventory": dataset.pair_inventory,
    }
    model = {
        "timeframe": dataset.timeframe,
        "horizons_sec": list(horizons_sec),
        "feature_names": list(dataset.feature_names),
        "estimator_type": estimator_type,
        "pair_context": pair_context,
        "direction_estimator": direction_estimator,
        "direction_thresholds": direction_thresholds,
        "pip_estimator": pip_estimator,
        "magnitude_estimator": magnitude_estimator,
        "executable_edge_estimator": executable_edge_estimator,
        "target_space": target_space,
        "target_scale_feature": (
            "ma__scale_pips" if target_space == "local_scale" else ""
        ),
        "target_clip_abs": (
            [float(value) for value in target_clip_abs]
            if target_space == "local_scale"
            else []
        ),
        "direction_calibration": direction_calibrators,
        "pip_calibration": pip_calibrators,
        "magnitude_calibration": magnitude_calibrators,
        "movement_cost_gates": movement_cost_gates,
        "executable_edge_calibration": executable_edge_calibrators,
        "executable_edge_thresholds": executable_edge_thresholds,
        "holdout_metrics": holdout,
        "validation_metrics": validation_metrics,
        "account_eligible_horizons": [],
        "execution_policy": "shadow_only",
    }
    return model, report


def grid_rows(report_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for report in report_rows:
        timeframe = str(report.get("timeframe") or "")
        for horizon in report.get("unsupported_horizons_sec") or []:
            output.append(
                {
                    "timeframe": timeframe,
                    "horizon_sec": int(horizon),
                    "split": None,
                    "status": "unsupported_source_resolution",
                    "n": 0,
                }
            )
        if report.get("status") != "fitted":
            output.append(
                {
                    "timeframe": timeframe,
                    "horizon_sec": None,
                    "split": None,
                    "status": report.get("status"),
                    "n": 0,
                }
            )
            continue
        for split, horizons in (report.get("metrics") or {}).items():
            for horizon, metrics in horizons.items():
                output.append(
                    {
                        "timeframe": timeframe,
                        "horizon_sec": int(horizon),
                        "split": split,
                        "status": "fitted",
                        **{
                            key: value
                            for key, value in metrics.items()
                            if key != "confidence_bins"
                        },
                    }
                )
    return output


def markdown_summary(manifest: dict[str, Any]) -> str:
    lines = [
        "# Moving-Average Feature Grid",
        "",
        f"- Generated: `{manifest['generated_at']}`",
        f"- Instruments requested: `{manifest['instrument_count']}`",
        f"- Timeframes requested: `{len(manifest['timeframes'])}`",
        f"- Horizons requested: `{len(manifest['horizons_sec'])}`",
        f"- Planned cells: `{manifest['planned_cell_count']}`",
        f"- Fitted cells: `{manifest['fitted_cell_count']}`",
        f"- Strict feature family: `moving averages only`",
        f"- Estimator: `{manifest.get('fit', {}).get('estimator_type') or 'ridge'}`",
        f"- Split policy: `{manifest.get('fit', {}).get('split_policy') or 'pair_fraction'}`",
        f"- Execution policy: `shadow_only`",
        "",
        "## Holdout Grid",
        "",
        "| Timeframe | Horizon | N | Direction | Pip MAE | Magnitude MAE | Avg executable net | Win | Positive pairs |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    rows = [
        row
        for row in manifest.get("grid") or []
        if row.get("split") == "holdout" and row.get("status") == "fitted"
    ]
    rows.sort(
        key=lambda row: (
            TIMEFRAME_SECONDS.get(str(row.get("timeframe")), 10**12),
            int(row.get("horizon_sec") or 0),
        )
    )
    for row in rows:
        lines.append(
            "| {timeframe} | {horizon}s | {n:,} | {accuracy:.2%} | "
            "{pip_mae:.3f} | {magnitude_mae:.3f} | {net:+.3f} | "
            "{win:.2%} | {pairs:.2%} |".format(
                timeframe=row["timeframe"],
                horizon=int(row["horizon_sec"]),
                n=int(row.get("n") or 0),
                accuracy=float(row.get("direction_accuracy") or 0.0),
                pip_mae=float(row.get("signed_pip_mae") or 0.0),
                magnitude_mae=float(row.get("magnitude_pip_mae") or 0.0),
                net=float(row.get("executable_average_net_pips") or 0.0),
                win=float(row.get("executable_win_rate") or 0.0),
                pairs=float(row.get("positive_pair_fraction") or 0.0),
            )
        )
    lines.extend(
        (
            "",
            "## Interpretation",
            "",
            "Direction accuracy measures the sign of the future midpoint move. Pip MAE",
            "measures signed forecast error. Magnitude MAE measures absolute-move error.",
            "Executable net selects the predicted side and uses recorded bid/ask where",
            "available, otherwise the pair's median observed spread. The movement gate",
            "is fitted on validation from predicted magnitude/current spread and",
            "direction confidence, then evaluated unchanged on holdout. It never uses",
            "the future exit spread to select a row.",
            "",
            "Row-pooled direction bounds are diagnostic because neighboring forecasts",
            "are dependent. Macro metrics average each pair equally; pair-level lower",
            "bounds and within-pair correlations are the stricter evidence. A positive",
            "pooled average alone is not a promotion: pair breadth, confidence bounds,",
            "chronological replication, and prospective paper outcomes remain required.",
            "",
        )
    )
    return "\n".join(lines)


def copy_latest_to_stamp(path: Path, stamp: str) -> Path:
    stamped = path.with_name(f"{path.stem.removesuffix('_latest')}_{stamp}{path.suffix}")
    shutil.copy2(path, stamped)
    return stamped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--m1-dir", type=Path, default=DEFAULT_M1_DIR)
    parser.add_argument("--s5-dir", type=Path, default=DEFAULT_S5_DIR)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument(
        "--timeframes",
        type=parse_timeframes,
        default=tuple(TIMEFRAME_SECONDS),
    )
    parser.add_argument(
        "--horizons-sec",
        type=parse_csv_ints,
        default=FORECAST_HORIZONS_SEC,
    )
    parser.add_argument("--instruments", default="")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--samples-per-pair-timeframe", type=int, default=1000)
    parser.add_argument("--minimum-split-rows", type=int, default=150)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    parser.add_argument(
        "--estimator",
        choices=("ridge", "xgboost"),
        default="ridge",
    )
    parser.add_argument("--xgb-device", default="cuda")
    parser.add_argument("--xgb-estimators", type=int, default=240)
    parser.add_argument("--xgb-max-depth", type=int, default=4)
    parser.add_argument("--xgb-learning-rate", type=float, default=0.035)
    parser.add_argument(
        "--pair-context",
        choices=PAIR_CONTEXT_MODES,
        default="none",
        help="append static pair/base/quote identity to the strict MA market features",
    )
    parser.add_argument(
        "--target-space",
        choices=("raw_pips", "local_scale"),
        default="raw_pips",
    )
    parser.add_argument("--target-clip-quantile", type=float, default=0.995)
    parser.add_argument(
        "--dataset-cache",
        type=Path,
        default=None,
        help="optional fingerprinted pre-split MA feature/target panel cache",
    )
    parser.add_argument(
        "--refresh-dataset-cache",
        action="store_true",
        help="ignore and replace an existing derived-dataset cache",
    )
    parser.add_argument(
        "--split-policy",
        choices=SPLIT_POLICIES,
        default="pair_fraction",
        help=(
            "chronological split policy; purged policies remove rows whose "
            "maximum-horizon label crosses a split boundary"
        ),
    )
    parser.add_argument(
        "--executable-edge-head",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "fit a validation-gated bid/ask net-pip head alongside midpoint "
            "direction and movement"
        ),
    )
    parser.add_argument(
        "--merge-existing",
        action="store_true",
        help="replace requested timeframe models inside the existing latest artifact/report",
    )
    parser.add_argument("--no-stamped-copy", action="store_true")
    args = parser.parse_args(argv)

    timeframes = tuple(args.timeframes)
    horizons = tuple(int(value) for value in args.horizons_sec)
    instruments = discover_instruments(
        args.m1_dir,
        args.s5_dir,
        args.instruments,
        args.max_pairs,
    )
    if not instruments:
        raise SystemExit("no instruments found")
    if args.samples_per_pair_timeframe == 0 or args.samples_per_pair_timeframe < -1:
        raise SystemExit("samples per pair must be -1 for all rows or a positive integer")
    if (
        args.xgb_estimators <= 0
        or args.xgb_max_depth <= 0
        or args.xgb_learning_rate <= 0.0
    ):
        raise SystemExit("XGBoost fit parameters must be positive")
    samples = 0 if args.samples_per_pair_timeframe == -1 else args.samples_per_pair_timeframe
    generated_at = utc_now()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    models: dict[str, Any] = {}
    reports: list[dict[str, Any]] = []
    horizons_by_timeframe: dict[str, tuple[int, ...]] = {}
    unsupported_by_timeframe: dict[str, tuple[int, ...]] = {}
    for timeframe in timeframes:
        source_resolution_sec = 5 if TIMEFRAME_SECONDS[timeframe] < 60 else 60
        horizons_by_timeframe[timeframe] = tuple(
            horizon for horizon in horizons if horizon >= source_resolution_sec
        )
        unsupported_by_timeframe[timeframe] = tuple(
            horizon for horizon in horizons if horizon < source_resolution_sec
        )
    print(
        json.dumps(
            {
                "event": "ma_grid_dataset_build_start",
                "instruments": len(instruments),
                "timeframes": len(timeframes),
                "samples_per_pair_timeframe": "all" if samples == 0 else samples,
            }
        ),
        flush=True,
    )
    cache_contract = dataset_cache_contract(
        timeframes,
        instruments,
        horizons_by_timeframe,
        args.m1_dir,
        args.s5_dir,
        samples,
    )
    cache_status = "disabled"
    datasets: dict[str, TimeframeDataset | None] | None = None
    if (
        args.dataset_cache is not None
        and args.dataset_cache.is_file()
        and not args.refresh_dataset_cache
    ):
        try:
            cached = joblib.load(args.dataset_cache)
            if cached.get("contract") == cache_contract:
                datasets = {
                    timeframe: dataset_from_payload(
                        (cached.get("datasets") or {}).get(timeframe)
                    )
                    for timeframe in timeframes
                }
                cache_status = "hit"
            else:
                cache_status = "stale"
        except (
            OSError,
            EOFError,
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
        ):
            cache_status = "invalid"
    if datasets is None:
        datasets = build_all_timeframe_datasets(
            timeframes,
            instruments,
            horizons_by_timeframe,
            args.m1_dir,
            args.s5_dir,
            samples,
        )
        if args.dataset_cache is not None:
            write_dataset_cache_atomic(
                args.dataset_cache,
                {
                    "schema_version": DATASET_CACHE_SCHEMA_VERSION,
                    "generated_at": generated_at,
                    "contract": cache_contract,
                    "datasets": {
                        timeframe: dataset_payload(datasets.get(timeframe))
                        for timeframe in timeframes
                    },
                },
            )
            cache_status = (
                "refreshed"
                if args.refresh_dataset_cache
                else "written"
            )
    print(
        json.dumps(
            {
                "event": "ma_grid_dataset_cache",
                "status": cache_status,
                "path": (
                    str(args.dataset_cache.resolve())
                    if args.dataset_cache is not None
                    else None
                ),
            }
        ),
        flush=True,
    )

    for timeframe in timeframes:
        supported_horizons = horizons_by_timeframe[timeframe]
        unsupported_horizons = unsupported_by_timeframe[timeframe]
        print(
            json.dumps(
                {
                    "event": "ma_grid_timeframe_start",
                    "timeframe": timeframe,
                    "instruments": len(instruments),
                    "horizons": len(supported_horizons),
                    "unsupported_horizons": list(unsupported_horizons),
                }
            ),
            flush=True,
        )
        dataset = datasets.pop(timeframe, None)
        if dataset is None:
            report = {
                "timeframe": timeframe,
                "status": "no_usable_rows",
                "feature_count": len(ma_feature_names(timeframe)),
                "pair_inventory": [],
                "unsupported_horizons_sec": list(unsupported_horizons),
            }
            reports.append(report)
            continue
        split_audit = apply_time_split_policy(
            dataset,
            args.split_policy,
            max(supported_horizons, default=0),
        )
        dataset = add_pair_context(dataset, args.pair_context)
        model, report = fit_timeframe(
            dataset,
            supported_horizons,
            args.ridge_alpha,
            args.minimum_split_rows,
            args.target_space,
            args.target_clip_quantile,
            args.estimator,
            args.xgb_device,
            args.xgb_estimators,
            args.xgb_max_depth,
            args.xgb_learning_rate,
            args.pair_context,
            args.executable_edge_head,
        )
        reports.append(report)
        report["split_policy"] = args.split_policy
        report["split_audit"] = split_audit
        report["unsupported_horizons_sec"] = list(unsupported_horizons)
        if model is not None:
            models[timeframe] = model
        print(
            json.dumps(
                {
                    "event": "ma_grid_timeframe_complete",
                    "timeframe": timeframe,
                    "status": report["status"],
                    "sampled_rows": dataset.sampled_rows,
                    "split_rows": report.get("split_rows"),
                }
            ),
            flush=True,
        )

    output_timeframes = list(timeframes)
    output_horizons = list(horizons)
    output_instruments = list(instruments)
    previous_generated_at = ""
    artifact_path = args.model_dir / DEFAULT_ARTIFACT.name
    report_path = args.report_dir / DEFAULT_REPORT.name
    if args.merge_existing and artifact_path.is_file():
        previous = joblib.load(artifact_path)
        if not isinstance(previous, dict) or previous.get("family") != FAMILY:
            raise RuntimeError("existing artifact is not an MA feature grid")
        previous_generated_at = str(previous.get("generated_at") or "")
        models = {
            **dict(previous.get("models") or {}),
            **models,
        }
        output_timeframes = [
            timeframe
            for timeframe in TIMEFRAME_SECONDS
            if timeframe in set(previous.get("timeframes") or ()) | set(timeframes)
        ]
        output_horizons = sorted(
            {
                int(value)
                for value in (
                    *(previous.get("horizons_sec") or ()),
                    *horizons,
                )
            }
        )
        if report_path.is_file():
            previous_report = json.loads(report_path.read_text(encoding="utf-8"))
            previous_reports = {
                str(row.get("timeframe") or ""): row
                for row in previous_report.get("timeframe_reports") or []
                if isinstance(row, dict)
            }
            current_reports = {
                str(row.get("timeframe") or ""): row for row in reports
            }
            reports = [
                {**previous_reports, **current_reports}[timeframe]
                for timeframe in output_timeframes
                if timeframe in {**previous_reports, **current_reports}
            ]
            output_instruments = sorted(
                set(previous_report.get("instruments") or ()) | set(instruments)
            )

    artifact = {
        "schema_version": SCHEMA_VERSION,
        "family": FAMILY,
        "generated_at": generated_at,
        "timeframes": output_timeframes,
        "horizons_sec": output_horizons,
        "models": models,
        "execution_policy": "shadow_only",
        "account_scope": "none",
        "real_account_authorized": False,
        "training_contract": contract_payload(),
        "fit": {
            "estimator": (
                "xgboost_multioutput_regressor"
                if args.estimator == "xgboost"
                else "median_imputer_standard_scaler_multioutput_ridge"
            ),
            "estimator_type": args.estimator,
            "pair_context": args.pair_context,
            "ridge_alpha": args.ridge_alpha,
            "xgboost": (
                {
                    "device": args.xgb_device,
                    "n_estimators": args.xgb_estimators,
                    "max_depth": args.xgb_max_depth,
                    "learning_rate": args.xgb_learning_rate,
                    "multi_strategy": "one_output_per_tree",
                }
                if args.estimator == "xgboost"
                else {}
            ),
            "direction_calibration": "validation_logistic",
            "direction_threshold": "validation_youden_j_minimum_5pct_per_side",
            "pip_calibration": "validation_affine_clipped_slope",
            "magnitude_calibration": "validation_affine_nonnegative",
            "executable_edge_head": bool(args.executable_edge_head),
            "executable_edge_calibration": (
                "validation_affine_bid_ask_net_pips"
                if args.executable_edge_head
                else "disabled"
            ),
            "executable_edge_threshold": (
                "validation_lower_95_rank_minimum_5pct_nonnegative_score"
                if args.executable_edge_head
                else "disabled"
            ),
            "target_space": args.target_space,
            "target_clip_quantile": (
                args.target_clip_quantile
                if args.target_space == "local_scale"
                else None
            ),
            "split_policy": args.split_policy,
            "split_purge": "maximum_supported_horizon",
            "split_scope": (
                "global_calendar"
                if args.split_policy == "global_time_purged"
                else "per_pair"
            ),
            "development_fraction": 0.60,
            "validation_fraction": 0.20,
            "holdout_fraction": 0.20,
            "development_fraction_per_pair": (
                None
                if args.split_policy == "global_time_purged"
                else 0.60
            ),
            "validation_fraction_per_pair": (
                None
                if args.split_policy == "global_time_purged"
                else 0.20
            ),
            "holdout_fraction_per_pair": (
                None
                if args.split_policy == "global_time_purged"
                else 0.20
            ),
            "samples_per_pair_timeframe": (
                "all" if samples == 0 else samples
            ),
            "dataset_cache": {
                "status": cache_status,
                "path": (
                    str(args.dataset_cache.resolve())
                    if args.dataset_cache is not None
                    else None
                ),
                "contract_sha256": hashlib.sha256(
                    json.dumps(
                        cache_contract,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            },
            "merge_existing": bool(args.merge_existing),
            "previous_generated_at": previous_generated_at,
        },
    }
    grid_path = args.report_dir / DEFAULT_GRID.name
    summary_path = args.report_dir / DEFAULT_SUMMARY.name
    write_joblib_atomic(artifact_path, artifact)
    grid = grid_rows(reports)
    grid_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(grid).to_csv(grid_path, index=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "family": FAMILY,
        "generated_at": generated_at,
        "instrument_count": len(output_instruments),
        "instruments": output_instruments,
        "timeframes": output_timeframes,
        "horizons_sec": output_horizons,
        "planned_cell_count": len(output_timeframes) * len(output_horizons),
        "fitted_cell_count": sum(
            len(model.get("horizons_sec") or []) for model in models.values()
        ),
        "unsupported_cell_count": (
            len(output_timeframes) * len(output_horizons)
            - sum(len(model.get("horizons_sec") or []) for model in models.values())
        ),
        "execution_policy": "shadow_only",
        "real_account_authorized": False,
        "artifact": str(artifact_path.resolve()),
        "artifact_sha256": artifact_checksum(artifact_path),
        "grid_csv": str(grid_path.resolve()),
        "fit": artifact["fit"],
        "contract": contract_payload(),
        "timeframe_reports": reports,
        "grid": grid,
    }
    write_json_atomic(report_path, manifest)
    summary_path.write_text(markdown_summary(manifest), encoding="utf-8")
    files = [artifact_path, report_path, grid_path, summary_path]
    if not args.no_stamped_copy:
        files.extend(copy_latest_to_stamp(path, stamp) for path in list(files))
    completion = {
        "event": "ma_grid_complete",
        "generated_at": generated_at,
        "planned_cells": manifest["planned_cell_count"],
        "fitted_cells": manifest["fitted_cell_count"],
        "artifact": str(artifact_path.resolve()),
        "report": str(report_path.resolve()),
        "grid": str(grid_path.resolve()),
        "summary": str(summary_path.resolve()),
        "files": [str(path.resolve()) for path in files],
    }
    print(json.dumps(completion, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
