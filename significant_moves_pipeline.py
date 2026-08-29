#!/usr/bin/env python3
"""Build the permanent, resumable, all-pair two-hour FX move catalog.

The detector is deliberately retrospective.  It scans timestamp-bounded
endpoint displacements at configurable horizons around two hours, records a
broad tail sample, and then performs auditable cross-horizon deduplication.
Nothing in this file should be interpreted as a causal entry signal.
"""

from __future__ import annotations

import argparse
import bisect
import concurrent.futures
import hashlib
import json
import math
import os
import sqlite3
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = ROOT / "config" / "significant_moves_2h.json"
SCHEMA_VERSION = 1

# Validated against the OANDA instrument metadata used by the training manager
# and trad/test_training_manager_weekend.py.  In particular, HKD_JPY is not in
# this set and therefore uses a 0.0001 pip, despite its quote currency.
PIP_LOCATION_MINUS2 = {
    "AUD_JPY",
    "CAD_JPY",
    "CHF_JPY",
    "EUR_HUF",
    "EUR_JPY",
    "GBP_JPY",
    "NZD_JPY",
    "SGD_JPY",
    "TRY_JPY",
    "USD_HUF",
    "USD_JPY",
    "USD_THB",
    "ZAR_JPY",
}

EVENT_LINK_COLUMNS = [
    "link_id",
    "move_id",
    "event_id",
    "event_timestamp",
    "macro_event_category",
    "central_bank_event",
    "rate_decision",
    "inflation_release",
    "employment_release",
    "gdp_release",
    "fiscal_event",
    "political_event",
    "geopolitical_event",
    "commodity_shock",
    "risk_sentiment_shock",
    "intervention",
    "policy_speech",
    "surprise_direction",
    "event_to_move_lead_minutes",
    "event_confidence",
    "source",
    "notes",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def normalize_instrument(value: str) -> str:
    return str(value or "").strip().upper().replace("/", "_")


def pair_parts(instrument: str) -> tuple[str, str]:
    normalized = normalize_instrument(instrument)
    if "_" not in normalized:
        raise ValueError(f"invalid FX instrument name: {instrument!r}")
    base, quote = normalized.split("_", 1)
    if len(base) != 3 or len(quote) != 3:
        raise ValueError(f"invalid FX instrument name: {instrument!r}")
    return base, quote


def pip_multiplier(instrument: str) -> float:
    return 100.0 if normalize_instrument(instrument) in PIP_LOCATION_MINUS2 else 10_000.0


def pip_size(instrument: str) -> float:
    return 1.0 / pip_multiplier(instrument)


def stable_id(prefix: str, *values: Any, length: int = 20) -> str:
    payload = "|".join(str(value) for value in values).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()[:length]}"


def json_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def resolve_from_root(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (ROOT / path).resolve()


def load_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "source_glob",
        "output_root",
        "horizons_minutes",
        "bar_minutes",
        "broad_quantile",
        "strict_quantile",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"config is missing required keys: {', '.join(missing)}")
    horizons = sorted({int(value) for value in payload["horizons_minutes"]})
    bar_minutes = int(payload["bar_minutes"])
    if not horizons or any(value <= 0 or value % bar_minutes for value in horizons):
        raise ValueError("every horizon must be a positive multiple of bar_minutes")
    if not 0.0 < float(payload["broad_quantile"]) < float(payload["strict_quantile"]) < 1.0:
        raise ValueError("expected 0 < broad_quantile < strict_quantile < 1")
    if int(payload.get("scan_stride_minutes", bar_minutes)) % bar_minutes:
        raise ValueError("scan_stride_minutes must be a multiple of bar_minutes")
    payload["horizons_minutes"] = horizons
    payload["bar_minutes"] = bar_minutes
    payload["config_path"] = str(path.resolve())
    return payload


def atomic_write_json(payload: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


def atomic_write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=False)
    os.replace(temporary, path)


def atomic_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False, compression="zstd")
    os.replace(temporary, path)


def source_stat(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"path": str(path or ""), "exists": False, "size": 0, "mtime_ns": 0}
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "exists": True,
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def checkpoint_matches(checkpoint: dict[str, Any], fingerprint: dict[str, Any]) -> bool:
    prior = dict(checkpoint.get("fingerprint", {}))
    current = dict(fingerprint)
    # Assembly/reporting-only code changes must not force a 49M-row rescan.
    # Any data-changing detector edit must bump pipeline_version in the config.
    prior.pop("code_hash", None)
    current.pop("code_hash", None)
    return prior == current and all(
        Path(value).exists() for value in checkpoint.get("output_files", {}).values()
    )


def numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def read_price_csv(path: Path, *, overlay: bool = False) -> pd.DataFrame:
    wanted = {
        "time",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "bid_close",
        "ask_close",
        "spread_pips",
    }
    frame = pd.read_csv(path, usecols=lambda column: column in wanted, low_memory=False)
    time_values = frame["datetime"] if "datetime" in frame else frame.get("time")
    frame["time_utc"] = pd.to_datetime(time_values, errors="coerce", utc=True)
    keep = ["time_utc"]
    if not overlay:
        keep.extend(["open", "high", "low", "close", "volume"])
    keep.extend(["bid_close", "ask_close", "spread_pips"])
    for column in keep:
        if column != "time_utc":
            frame[column] = numeric_series(frame, column)
    return frame[keep]


def load_and_normalize_prices(
    source_path: Path,
    overlay_path: Path | None,
    instrument: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    raw = read_price_csv(source_path)
    source_rows = int(len(raw))
    invalid_timestamps = int(raw["time_utc"].isna().sum())
    raw = raw.dropna(subset=["time_utc"]).sort_values("time_utc")
    duplicate_timestamps = int(raw.duplicated("time_utc", keep="last").sum())
    raw = raw.drop_duplicates("time_utc", keep="last").set_index("time_utc")

    required_price = raw[["open", "high", "low", "close"]]
    missing_mid_ohlc = int(required_price.isna().any(axis=1).sum())
    invalid_ohlc_mask = (
        (raw["high"] < raw[["open", "close"]].max(axis=1))
        | (raw["low"] > raw[["open", "close"]].min(axis=1))
        | (raw["high"] < raw["low"])
    )
    invalid_ohlc_rows = int(invalid_ohlc_mask.fillna(True).sum())
    raw = raw.loc[~invalid_ohlc_mask.fillna(True)].copy()

    observed_ba = (
        raw["bid_close"].notna()
        & raw["ask_close"].notna()
        & (raw["ask_close"] >= raw["bid_close"])
    )
    observed_spread = raw["spread_pips"].notna() & (raw["spread_pips"] > 0)
    overlay_rows = 0
    if overlay_path is not None and overlay_path.exists():
        overlay_frame = read_price_csv(overlay_path, overlay=True)
        overlay_frame = (
            overlay_frame.dropna(subset=["time_utc"])
            .sort_values("time_utc")
            .drop_duplicates("time_utc", keep="last")
            .set_index("time_utc")
            .reindex(raw.index)
        )
        valid_overlay_ba = (
            overlay_frame["bid_close"].notna()
            & overlay_frame["ask_close"].notna()
            & (overlay_frame["ask_close"] >= overlay_frame["bid_close"])
        )
        use_overlay_ba = ~observed_ba & valid_overlay_ba
        raw.loc[use_overlay_ba, "bid_close"] = overlay_frame.loc[use_overlay_ba, "bid_close"]
        raw.loc[use_overlay_ba, "ask_close"] = overlay_frame.loc[use_overlay_ba, "ask_close"]
        valid_overlay_spread = overlay_frame["spread_pips"].notna() & (
            overlay_frame["spread_pips"] > 0
        )
        use_overlay_spread = ~observed_spread & valid_overlay_spread
        raw.loc[use_overlay_spread, "spread_pips"] = overlay_frame.loc[
            use_overlay_spread, "spread_pips"
        ]
        overlay_rows = int((use_overlay_ba | use_overlay_spread).sum())
        observed_ba = observed_ba | use_overlay_ba
        observed_spread = observed_spread | use_overlay_spread

    multiplier = pip_multiplier(instrument)
    usable_observed_spreads = raw.loc[observed_spread, "spread_pips"]
    median_spread = float(usable_observed_spreads.median()) if len(usable_observed_spreads) else math.nan
    if not math.isfinite(median_spread) or median_spread <= 0:
        median_spread = 2.0 if instrument in PIP_LOCATION_MINUS2 else 2.5
    spread_missing = raw["spread_pips"].isna() | (raw["spread_pips"] <= 0)
    raw.loc[spread_missing, "spread_pips"] = median_spread
    half_spread_price = raw["spread_pips"] / multiplier / 2.0
    bid_missing = raw["bid_close"].isna()
    ask_missing = raw["ask_close"].isna()
    raw.loc[bid_missing, "bid_close"] = raw.loc[bid_missing, "close"] - half_spread_price[bid_missing]
    raw.loc[ask_missing, "ask_close"] = raw.loc[ask_missing, "close"] + half_spread_price[ask_missing]
    raw["spread_is_estimated"] = ~(observed_ba & observed_spread)
    raw["source_m1_observations"] = 1

    differences = raw.index.to_series().diff().dt.total_seconds().div(60.0)
    missing_minutes = np.maximum(np.floor(differences.fillna(1.0).to_numpy()) - 1.0, 0.0)
    quality = {
        "instrument": instrument,
        "base_currency": pair_parts(instrument)[0],
        "quote_currency": pair_parts(instrument)[1],
        "pip_size": pip_size(instrument),
        "pip_multiplier": multiplier,
        "source_path": str(source_path.resolve()),
        "bam_overlay_path": str(overlay_path.resolve()) if overlay_path and overlay_path.exists() else "",
        "source_rows": source_rows,
        "usable_rows": int(len(raw)),
        "start_timestamp": raw.index.min().isoformat() if len(raw) else "",
        "end_timestamp": raw.index.max().isoformat() if len(raw) else "",
        "invalid_timestamp_rows": invalid_timestamps,
        "duplicate_timestamp_rows": duplicate_timestamps,
        "missing_mid_ohlc_rows": missing_mid_ohlc,
        "invalid_ohlc_rows": invalid_ohlc_rows,
        "gap_events_over_1_minute": int((differences > 1.0).sum()),
        "gap_events_2_to_5_minutes": int(((differences > 1.0) & (differences <= 5.0)).sum()),
        "gap_events_5_to_120_minutes": int(((differences > 5.0) & (differences <= 120.0)).sum()),
        "gap_events_over_120_minutes": int((differences > 120.0).sum()),
        "estimated_missing_source_minutes": int(np.nansum(missing_minutes)),
        "maximum_gap_minutes": float(differences.max()) if len(differences) else math.nan,
        "observed_bid_ask_spread_rows": int((observed_ba & observed_spread).sum()),
        "bam_overlay_rows": overlay_rows,
        "estimated_cost_rows": int(raw["spread_is_estimated"].sum()),
        "estimated_cost_fraction": float(raw["spread_is_estimated"].mean()),
        "median_spread_pips": median_spread,
    }
    return raw, quality


def resample_prices(raw: pd.DataFrame, bar_minutes: int) -> pd.DataFrame:
    rule = f"{int(bar_minutes)}min"
    grouped = raw.resample(rule, label="right", closed="right")
    bars = grouped.agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        bid_close=("bid_close", "last"),
        ask_close=("ask_close", "last"),
        spread_pips=("spread_pips", "median"),
        volume=("volume", "sum"),
        source_m1_observations=("source_m1_observations", "sum"),
        spread_estimated_fraction=("spread_is_estimated", "mean"),
    )
    bars.loc[bars["close"].isna(), "volume"] = np.nan
    bars["source_m1_observations"] = bars["source_m1_observations"].fillna(0).astype(int)
    bars.index.name = "time_utc"
    return bars


def safe_number(value: Any, default: float = math.nan) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def interval_overlap_fraction(
    left_start: pd.Timestamp,
    left_end: pd.Timestamp,
    right_start: pd.Timestamp,
    right_end: pd.Timestamp,
) -> float:
    overlap_start = max(left_start, right_start)
    overlap_end = min(left_end, right_end)
    if overlap_end <= overlap_start:
        return 0.0
    overlap = (overlap_end - overlap_start).total_seconds()
    shortest = min(
        (left_end - left_start).total_seconds(),
        (right_end - right_start).total_seconds(),
    )
    return float(overlap / shortest) if shortest > 0 else 0.0


def classify_start_session(timestamp: pd.Timestamp) -> str:
    hour = timestamp.hour + timestamp.minute / 60.0
    if 12.0 <= hour < 16.0:
        return "london_new_york_overlap"
    if 7.0 <= hour < 16.0:
        return "london"
    if 12.0 <= hour < 21.0:
        return "new_york"
    if hour >= 22.0 or hour < 7.0:
        return "asia"
    return "other"


def session_fractions(index: pd.DatetimeIndex) -> dict[str, float]:
    if len(index) == 0:
        return {
            "asia_fraction": 0.0,
            "london_fraction": 0.0,
            "new_york_fraction": 0.0,
            "london_new_york_overlap_fraction": 0.0,
        }
    hours = index.hour + index.minute / 60.0
    return {
        "asia_fraction": float(np.mean((hours >= 22.0) | (hours < 7.0))),
        "london_fraction": float(np.mean((hours >= 7.0) & (hours < 16.0))),
        "new_york_fraction": float(np.mean((hours >= 12.0) & (hours < 21.0))),
        "london_new_york_overlap_fraction": float(
            np.mean((hours >= 12.0) & (hours < 16.0))
        ),
    }


def _path_record(
    *,
    bars: pd.DataFrame,
    instrument: str,
    horizon: int,
    start_position: int,
    end_position: int,
    move_percentile: float,
    broad_threshold: float,
    strict_threshold: float,
    atr_at_start: float,
    config: dict[str, Any],
) -> dict[str, Any]:
    multiplier = pip_multiplier(instrument)
    base_currency, quote_currency = pair_parts(instrument)
    index = pd.DatetimeIndex(bars.index)
    start_timestamp = index[start_position]
    end_timestamp = index[end_position]

    close_array = bars["close"].to_numpy(dtype=float)
    high_array = bars["high"].to_numpy(dtype=float)
    low_array = bars["low"].to_numpy(dtype=float)
    bid_array = bars["bid_close"].to_numpy(dtype=float)
    ask_array = bars["ask_close"].to_numpy(dtype=float)
    spread_array = bars["spread_pips"].to_numpy(dtype=float)
    m1_array = bars["source_m1_observations"].to_numpy(dtype=float)
    estimated_array = bars["spread_estimated_fraction"].to_numpy(dtype=float)

    start_mid = float(close_array[start_position])
    end_mid = float(close_array[end_position])
    signed_price_change = end_mid - start_mid
    signed_pips = signed_price_change * multiplier
    absolute_pips = abs(signed_pips)
    sign = 1.0 if signed_pips >= 0 else -1.0
    direction = "up" if sign > 0 else "down"
    position_direction = "long" if sign > 0 else "short"

    path_slice = slice(start_position + 1, end_position + 1)
    path_high = high_array[path_slice]
    path_low = low_array[path_slice]
    finite_high = np.isfinite(path_high)
    finite_low = np.isfinite(path_low)
    highest_mid = max(
        start_mid,
        float(np.nanmax(path_high)) if finite_high.any() else start_mid,
    )
    lowest_mid = min(
        start_mid,
        float(np.nanmin(path_low)) if finite_low.any() else start_mid,
    )
    if finite_high.any():
        high_offset = int(np.nanargmax(path_high)) + start_position + 1
        highest_timestamp = index[high_offset]
    else:
        highest_timestamp = start_timestamp
    if finite_low.any():
        low_offset = int(np.nanargmin(path_low)) + start_position + 1
        lowest_timestamp = index[low_offset]
    else:
        lowest_timestamp = start_timestamp

    upward_excursion = max(0.0, (highest_mid - start_mid) * multiplier)
    downward_excursion = max(0.0, (start_mid - lowest_mid) * multiplier)
    if direction == "up":
        maximum_favorable = upward_excursion
        maximum_adverse = downward_excursion
        favorable_timestamp = highest_timestamp
        adverse_timestamp = lowest_timestamp
    else:
        maximum_favorable = downward_excursion
        maximum_adverse = upward_excursion
        favorable_timestamp = lowest_timestamp
        adverse_timestamp = highest_timestamp

    path_closes = close_array[start_position : end_position + 1]
    finite_close = np.isfinite(path_closes)
    observed_closes = path_closes[finite_close]
    increments = np.diff(observed_closes) * multiplier if len(observed_closes) > 1 else np.array([])
    path_distance = float(np.sum(np.abs(increments))) if len(increments) else 0.0
    efficiency = min(1.0, absolute_pips / path_distance) if path_distance > 0 else 0.0
    meaningful = increments[np.abs(increments) >= float(config.get("reversal_noise_pips", 0.1))]
    trend_consistency = (
        float(np.mean(np.sign(meaningful) == sign)) if len(meaningful) else 0.0
    )
    reversal_count = (
        int(np.sum(np.sign(meaningful[1:]) != np.sign(meaningful[:-1])))
        if len(meaningful) > 1
        else 0
    )
    observed_logs = np.log(observed_closes[observed_closes > 0])
    log_increments = np.diff(observed_logs) if len(observed_logs) > 1 else np.array([])
    realized_volatility_pct = (
        float(np.std(log_increments, ddof=1) * math.sqrt(len(log_increments)) * 100.0)
        if len(log_increments) > 1
        else 0.0
    )
    realized_volatility_pips = (
        float(np.std(increments, ddof=1) * math.sqrt(len(increments)))
        if len(increments) > 1
        else 0.0
    )

    expected_move = atr_at_start * math.sqrt(horizon / int(config["bar_minutes"]))
    atr_normalized = absolute_pips / expected_move if expected_move > 0 else math.nan
    range_price = highest_mid - lowest_mid
    range_pips = range_price * multiplier
    close_location = (end_mid - lowest_mid) / range_price if range_price > 0 else 0.5
    directional_close_location = close_location if direction == "up" else 1.0 - close_location
    retained_fraction = min(1.0, absolute_pips / maximum_favorable) if maximum_favorable > 0 else 0.0
    giveback_pips = max(0.0, maximum_favorable - absolute_pips)

    start_bid = float(bid_array[start_position])
    start_ask = float(ask_array[start_position])
    end_bid = float(bid_array[end_position])
    end_ask = float(ask_array[end_position])
    slippage = float(config.get("slippage_pips_per_side", 0.0))
    if direction == "up":
        entry_cost = max(0.0, (start_ask - start_mid) * multiplier) + slippage
        exit_cost = max(0.0, (end_mid - end_bid) * multiplier) + slippage
        executable_net = (end_bid - start_ask) * multiplier - 2.0 * slippage
    else:
        entry_cost = max(0.0, (start_mid - start_bid) * multiplier) + slippage
        exit_cost = max(0.0, (end_ask - end_mid) * multiplier) + slippage
        executable_net = (start_bid - end_ask) * multiplier - 2.0 * slippage
    round_trip_cost = max(0.0, entry_cost + exit_cost)
    move_to_cost = absolute_pips / max(round_trip_cost, 0.1)

    path_bar_observed = np.isfinite(path_closes[1:])
    observed_bar_count = int(path_bar_observed.sum())
    expected_bar_count = int(horizon // int(config["bar_minutes"]))
    bar_coverage = observed_bar_count / max(expected_bar_count, 1)
    source_m1_observations = int(np.nansum(m1_array[path_slice]))
    missing_source_minutes = max(0, int(horizon - source_m1_observations))
    estimated_cost_fraction = safe_number(np.nanmean(estimated_array[start_position : end_position + 1]), 1.0)
    spread_estimate = safe_number(
        np.nanmean([spread_array[start_position], spread_array[end_position]]),
        math.nan,
    )

    observed_positions = np.flatnonzero(finite_close)
    midpoint_offset = (end_position - start_position) / 2.0
    if len(observed_positions) >= 2:
        midpoint_price = float(np.interp(midpoint_offset, observed_positions, path_closes[finite_close]))
    else:
        midpoint_price = (start_mid + end_mid) / 2.0
    half_hours = max(horizon / 120.0, 1e-9)
    first_half_velocity = sign * (midpoint_price - start_mid) * multiplier / half_hours
    second_half_velocity = sign * (end_mid - midpoint_price) * multiplier / half_hours
    acceleration = (second_half_velocity - first_half_velocity) / half_hours

    observed_index = index[start_position + 1 : end_position + 1][path_bar_observed]
    sessions = session_fractions(observed_index)
    weekend_exposure = bool(np.any(observed_index.dayofweek >= 5)) if len(observed_index) else False

    minimum_atr = float(config.get("strict_minimum_atr_units", 1.5))
    minimum_cost = float(config.get("strict_minimum_move_to_cost", 3.0))
    minimum_efficiency = float(config.get("strict_minimum_path_efficiency", 0.1))
    minimum_coverage = float(config.get("minimum_window_bar_coverage", 0.5))
    strict_checks = {
        "instrument_percentile": absolute_pips >= strict_threshold,
        "atr_normalized": math.isfinite(atr_normalized) and atr_normalized >= minimum_atr,
        "move_to_cost": move_to_cost >= minimum_cost,
        "path_efficiency": efficiency >= minimum_efficiency,
        "bar_coverage": bar_coverage >= minimum_coverage,
        "positive_after_cost": executable_net > 0,
    }
    strict_eligible = all(strict_checks.values())
    strict_rejections = [name for name, passed in strict_checks.items() if not passed]

    tail_score = float(
        np.clip(
            (move_percentile - float(config["broad_quantile"]))
            / (1.0 - float(config["broad_quantile"])),
            0.0,
            1.0,
        )
    )
    severity_score = min(1.0, absolute_pips / max(strict_threshold * 2.0, 0.1))
    atr_score = (
        min(1.0, atr_normalized / max(minimum_atr * 2.0, 0.1))
        if math.isfinite(atr_normalized)
        else 0.0
    )
    cost_score = min(1.0, move_to_cost / max(minimum_cost * 3.0, 0.1))
    significance_score = 100.0 * (
        0.35 * tail_score
        + 0.20 * severity_score
        + 0.20 * atr_score
        + 0.10 * efficiency
        + 0.10 * retained_fraction
        + 0.05 * cost_score
    )

    quality_flags: list[str] = []
    if estimated_cost_fraction > 0:
        quality_flags.append("transaction_cost_partly_estimated")
    if estimated_cost_fraction >= 0.999:
        quality_flags.append("transaction_cost_fully_estimated")
    if bar_coverage < float(config.get("quality_warning_bar_coverage", 0.9)):
        quality_flags.append("sparse_window")
    if not math.isfinite(atr_normalized):
        quality_flags.append("insufficient_atr_history")
    if weekend_exposure:
        quality_flags.append("calendar_weekend_exposure")

    signed_percentage_return = signed_price_change / start_mid * 100.0
    record = {
        "move_id": stable_id(
            "move",
            instrument,
            start_timestamp.isoformat(),
            end_timestamp.isoformat(),
            horizon,
            direction,
        ),
        "instrument": instrument,
        "base_currency": base_currency,
        "quote_currency": quote_currency,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "duration_minutes": int(horizon),
        "duration_hours": float(horizon / 60.0),
        "duration_days": float(horizon / 1_440.0),
        "horizon_minutes": int(horizon),
        "is_primary_horizon": bool(horizon == int(config["primary_horizon_minutes"])),
        "direction": direction,
        "position_direction": position_direction,
        "start_bid": start_bid,
        "start_ask": start_ask,
        "start_mid": start_mid,
        "end_bid": end_bid,
        "end_ask": end_ask,
        "end_mid": end_mid,
        "highest_mid": highest_mid,
        "highest_mid_timestamp": highest_timestamp,
        "lowest_mid": lowest_mid,
        "lowest_mid_timestamp": lowest_timestamp,
        "favorable_extreme_timestamp": favorable_timestamp,
        "adverse_extreme_timestamp": adverse_timestamp,
        "net_signed_price_change": signed_price_change,
        "absolute_price_change": abs(signed_price_change),
        "signed_pip_change": signed_pips,
        "absolute_pip_change": absolute_pips,
        "signed_percentage_return": signed_percentage_return,
        "absolute_percentage_return": abs(signed_percentage_return),
        "log_return": math.log(end_mid / start_mid),
        "maximum_favorable_excursion_pips": maximum_favorable,
        "maximum_adverse_excursion_pips": maximum_adverse,
        "maximum_adverse_excursion_signed_pips": -maximum_adverse,
        "time_to_mfe_minutes": (favorable_timestamp - start_timestamp).total_seconds() / 60.0,
        "time_to_mae_minutes": (adverse_timestamp - start_timestamp).total_seconds() / 60.0,
        "in_window_range_pips": range_pips,
        "close_location_within_range": close_location,
        "directional_close_location": directional_close_location,
        "path_efficiency_ratio": efficiency,
        "trend_consistency": trend_consistency,
        "reversal_count": reversal_count,
        "choppiness_estimate": 1.0 - efficiency,
        "realized_volatility_pips": realized_volatility_pips,
        "realized_volatility_pct": realized_volatility_pct,
        "atr_baseline_pips_per_bar": atr_at_start,
        "atr_expected_move_pips": expected_move,
        "atr_normalized_move": atr_normalized,
        "start_spread_pips": safe_number(spread_array[start_position]),
        "end_spread_pips": safe_number(spread_array[end_position]),
        "spread_estimate_pips": spread_estimate,
        "estimated_entry_cost_pips": entry_cost,
        "estimated_exit_cost_pips": exit_cost,
        "estimated_round_trip_cost_pips": round_trip_cost,
        "net_move_after_cost_pips": executable_net,
        "move_to_cost_ratio": move_to_cost,
        "move_velocity_pips_per_hour": absolute_pips / (horizon / 60.0),
        "move_acceleration_pips_per_hour2": acceleration,
        "first_half_velocity_pips_per_hour": first_half_velocity,
        "second_half_velocity_pips_per_hour": second_half_velocity,
        "giveback_from_mfe_pips": giveback_pips,
        "endpoint_capture_of_mfe": retained_fraction,
        "start_session": classify_start_session(start_timestamp),
        **sessions,
        "weekend_exposure": weekend_exposure,
        "observation_count": observed_bar_count,
        "expected_observation_count": expected_bar_count,
        "bar_coverage_fraction": bar_coverage,
        "source_m1_observation_count": source_m1_observations,
        "missing_source_minute_count": missing_source_minutes,
        "estimated_transaction_cost_fraction": estimated_cost_fraction,
        "data_quality_flags": "|".join(quality_flags),
        "broad_threshold_pips": broad_threshold,
        "strict_threshold_pips": strict_threshold,
        "instrument_move_percentile": move_percentile,
        "severity_vs_strict_threshold": absolute_pips / max(strict_threshold, 0.1),
        "strict_eligible": bool(strict_eligible),
        "strict_rejection_reasons": "|".join(strict_rejections),
        "threshold_systems_passed": "|".join(name for name, passed in strict_checks.items() if passed),
        "significance_score": significance_score,
        "threshold_scope": "pair_horizon_full_sample_retrospective",
        "threshold_is_causal": False,
        "hindsight_selected": True,
        "timestamp_resolution_minutes": int(config["bar_minutes"]),
        "pipeline_version": str(config["pipeline_version"]),
    }
    return record


def detect_move_candidates(
    bars: pd.DataFrame,
    instrument: str,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if bars.empty:
        return pd.DataFrame(), []
    multiplier = pip_multiplier(instrument)
    bar_minutes = int(config["bar_minutes"])
    scan_step = max(1, int(config.get("scan_stride_minutes", bar_minutes)) // bar_minutes)
    close = bars["close"].to_numpy(dtype=float)
    high = bars["high"].to_numpy(dtype=float)
    low = bars["low"].to_numpy(dtype=float)
    observed = np.isfinite(close).astype(np.int64)
    observed_prefix = np.concatenate([[0], np.cumsum(observed)])

    close_series = bars["close"].astype(float)
    previous_close = close_series.shift(1)
    true_range = pd.concat(
        [
            bars["high"] - bars["low"],
            (bars["high"] - previous_close).abs(),
            (bars["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1, skipna=True) * multiplier
    true_range.loc[bars["close"].isna()] = np.nan
    atr_bars = max(2, int(config.get("atr_lookback_minutes", 240)) // bar_minutes)
    atr_minimum = min(
        atr_bars,
        max(2, int(config.get("atr_minimum_observations", atr_bars // 2))),
    )
    atr = true_range.rolling(atr_bars, min_periods=atr_minimum).mean().to_numpy(dtype=float)

    rows: list[dict[str, Any]] = []
    threshold_rows: list[dict[str, Any]] = []
    minimum_coverage = float(config.get("minimum_window_bar_coverage", 0.5))
    minimum_history = int(config.get("minimum_threshold_history_windows", 1_000))
    fixed_minimum = float(config.get("fixed_minimum_pips", 0.0))
    for horizon in config["horizons_minutes"]:
        horizon = int(horizon)
        horizon_bars = horizon // bar_minutes
        if len(bars) <= horizon_bars:
            threshold_rows.append(
                {
                    "instrument": instrument,
                    "horizon_minutes": horizon,
                    "valid_windows": 0,
                    "status": "insufficient_rows",
                }
            )
            continue
        starts = np.arange(0, len(bars) - horizon_bars, scan_step, dtype=np.int64)
        ends = starts + horizon_bars
        observed_in_window = observed_prefix[ends + 1] - observed_prefix[starts + 1]
        coverage = observed_in_window / max(horizon_bars, 1)
        start_close = close[starts]
        end_close = close[ends]
        valid = (
            np.isfinite(start_close)
            & np.isfinite(end_close)
            & (coverage >= minimum_coverage)
        )
        signed_moves = (end_close - start_close) * multiplier
        absolute_moves = np.abs(signed_moves)
        valid_moves = absolute_moves[valid]
        threshold_record: dict[str, Any] = {
            "instrument": instrument,
            "horizon_minutes": horizon,
            "valid_windows": int(valid.sum()),
            "broad_quantile": float(config["broad_quantile"]),
            "strict_quantile": float(config["strict_quantile"]),
            "fixed_minimum_pips": fixed_minimum,
        }
        if len(valid_moves) < minimum_history:
            threshold_record["status"] = "insufficient_valid_windows"
            threshold_rows.append(threshold_record)
            continue
        broad_threshold = max(
            fixed_minimum,
            float(np.quantile(valid_moves, float(config["broad_quantile"]))),
        )
        strict_threshold = max(
            broad_threshold,
            fixed_minimum,
            float(np.quantile(valid_moves, float(config["strict_quantile"]))),
        )
        threshold_record.update(
            {
                "status": "ok",
                "median_absolute_move_pips": float(np.median(valid_moves)),
                "q95_absolute_move_pips": float(np.quantile(valid_moves, 0.95)),
                "broad_threshold_pips": broad_threshold,
                "strict_threshold_pips": strict_threshold,
                "maximum_absolute_move_pips": float(np.max(valid_moves)),
            }
        )
        threshold_rows.append(threshold_record)
        sorted_moves = np.sort(valid_moves)
        broad_positions = np.flatnonzero(valid & (absolute_moves >= broad_threshold))
        for local_position in broad_positions:
            start_position = int(starts[local_position])
            end_position = int(ends[local_position])
            absolute_move = float(absolute_moves[local_position])
            percentile = float(
                np.searchsorted(sorted_moves, absolute_move, side="right") / len(sorted_moves)
            )
            rows.append(
                _path_record(
                    bars=bars,
                    instrument=instrument,
                    horizon=horizon,
                    start_position=start_position,
                    end_position=end_position,
                    move_percentile=percentile,
                    broad_threshold=broad_threshold,
                    strict_threshold=strict_threshold,
                    atr_at_start=float(atr[start_position]),
                    config=config,
                )
            )
    candidates = pd.DataFrame(rows)
    if not candidates.empty:
        candidates = candidates.sort_values(
            ["start_timestamp", "horizon_minutes", "direction"]
        ).reset_index(drop=True)
    return candidates, threshold_rows


def select_non_overlapping_candidates(
    candidates: pd.DataFrame,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Assign auditable overlap groups and select strict non-overlapping moves."""
    if candidates.empty:
        return candidates.copy(), candidates.copy(), candidates.copy()
    output = candidates.copy().reset_index(drop=True)
    output["start_timestamp"] = pd.to_datetime(output["start_timestamp"], utc=True)
    output["end_timestamp"] = pd.to_datetime(output["end_timestamp"], utc=True)
    output["overlap_group_id"] = ""
    minimum_overlap = float(config.get("overlap_group_minimum_fraction", 0.3))
    start_tolerance = float(config.get("overlap_start_tolerance_minutes", 45.0))
    maximum_horizon = max(int(value) for value in config["horizons_minutes"])

    for direction, direction_frame in output.groupby("direction", sort=True):
        prototypes: list[dict[str, Any]] = []
        prototype_start_values: list[int] = []
        order = direction_frame.sort_values(
            ["significance_score", "absolute_pip_change", "is_primary_horizon"],
            ascending=[False, False, False],
        ).index
        for row_index in order:
            row = output.loc[row_index]
            start = row["start_timestamp"]
            end = row["end_timestamp"]
            lower_value = int((start - pd.Timedelta(minutes=maximum_horizon)).value)
            upper_value = int(end.value)
            left = bisect.bisect_left(prototype_start_values, lower_value)
            right = bisect.bisect_right(prototype_start_values, upper_value)
            best_match: dict[str, Any] | None = None
            best_key = (-1.0, -math.inf)
            for prototype in prototypes[left:right]:
                overlap = interval_overlap_fraction(
                    start,
                    end,
                    prototype["start"],
                    prototype["end"],
                )
                start_delta = abs((start - prototype["start"]).total_seconds()) / 60.0
                matches = overlap >= minimum_overlap or (
                    overlap > 0.0 and start_delta <= start_tolerance
                )
                key = (overlap, -start_delta)
                if matches and key > best_key:
                    best_key = key
                    best_match = prototype
            if best_match is None:
                group_id = stable_id(
                    "overlap",
                    row["instrument"],
                    direction,
                    row["move_id"],
                )
                prototype = {
                    "start": start,
                    "end": end,
                    "group_id": group_id,
                    "move_id": row["move_id"],
                }
                insert_at = bisect.bisect_right(prototype_start_values, int(start.value))
                prototype_start_values.insert(insert_at, int(start.value))
                prototypes.insert(insert_at, prototype)
            else:
                group_id = str(best_match["group_id"])
            output.at[row_index, "overlap_group_id"] = group_id

    output["rank_within_overlap_group"] = (
        output.groupby("overlap_group_id")["significance_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    output["overlap_group_size"] = output.groupby("overlap_group_id")[
        "move_id"
    ].transform("size").astype(int)
    output["deduplicated_selected"] = output["rank_within_overlap_group"].eq(1)
    output["parent_trend_id"] = ""
    output["root_trend_id"] = ""
    output["rejected_by_move_id"] = ""
    output["final_selected"] = False
    output["selection_reason"] = np.where(
        output["deduplicated_selected"],
        "pending_strict_selection",
        "rejected_overlap_group_lower_score",
    )
    output["selection_method"] = "greedy_significance_non_maximum_suppression"

    deduplicated_indices = output.index[output["deduplicated_selected"]].tolist()
    parent_gap = pd.Timedelta(minutes=float(config.get("parent_trend_gap_minutes", 15.0)))
    ordered_deduplicated = sorted(
        deduplicated_indices,
        key=lambda index: (
            output.at[index, "start_timestamp"],
            output.at[index, "end_timestamp"],
        ),
    )
    current_members: list[int] = []
    current_end: pd.Timestamp | None = None
    parent_clusters: list[list[int]] = []
    for row_index in ordered_deduplicated:
        start = output.at[row_index, "start_timestamp"]
        end = output.at[row_index, "end_timestamp"]
        if current_end is None or start <= current_end + parent_gap:
            current_members.append(row_index)
            current_end = end if current_end is None else max(current_end, end)
        else:
            parent_clusters.append(current_members)
            current_members = [row_index]
            current_end = end
    if current_members:
        parent_clusters.append(current_members)
    group_to_parent: dict[str, str] = {}
    for members in parent_clusters:
        first = min(output.at[index, "start_timestamp"] for index in members)
        last = max(output.at[index, "end_timestamp"] for index in members)
        instrument = str(output.at[members[0], "instrument"])
        parent_id = stable_id("trend", instrument, first.isoformat(), last.isoformat())
        for row_index in members:
            output.at[row_index, "parent_trend_id"] = parent_id
            output.at[row_index, "root_trend_id"] = parent_id
            group_to_parent[str(output.at[row_index, "overlap_group_id"])] = parent_id
    for row_index in output.index:
        group_id = str(output.at[row_index, "overlap_group_id"])
        parent_id = group_to_parent.get(group_id, "")
        output.at[row_index, "parent_trend_id"] = parent_id
        output.at[row_index, "root_trend_id"] = parent_id

    strict_winners = output[
        output["deduplicated_selected"] & output["strict_eligible"].astype(bool)
    ].sort_values(
        ["significance_score", "absolute_pip_change", "is_primary_horizon"],
        ascending=[False, False, False],
    )
    accepted: list[int] = []
    final_maximum_overlap = float(config.get("final_maximum_overlap_fraction", 0.0))
    for row_index, row in strict_winners.iterrows():
        blocker: int | None = None
        for accepted_index in accepted:
            overlap = interval_overlap_fraction(
                row["start_timestamp"],
                row["end_timestamp"],
                output.at[accepted_index, "start_timestamp"],
                output.at[accepted_index, "end_timestamp"],
            )
            if overlap > final_maximum_overlap:
                blocker = accepted_index
                break
        if blocker is None:
            accepted.append(int(row_index))
            output.at[row_index, "final_selected"] = True
            output.at[row_index, "selection_reason"] = "selected_strict_non_overlapping"
        else:
            output.at[row_index, "selection_reason"] = "rejected_strict_interval_overlap"
            output.at[row_index, "rejected_by_move_id"] = output.at[blocker, "move_id"]

    below_strict = output["deduplicated_selected"] & ~output["strict_eligible"].astype(bool)
    output.loc[below_strict, "selection_reason"] = "rejected_below_strict_threshold"
    output["is_primary_move"] = output["final_selected"]
    output["is_submove"] = False
    output["hierarchy_level"] = 0
    output["parent_move_id"] = ""

    overlap_groups = output.sort_values(
        ["overlap_group_id", "rank_within_overlap_group"]
    ).reset_index(drop=True)
    deduplicated = output[output["deduplicated_selected"]].copy()
    deduplicated = deduplicated.sort_values("start_timestamp").reset_index(drop=True)
    final = output[output["final_selected"]].copy()
    final = final.sort_values("start_timestamp").reset_index(drop=True)
    return overlap_groups, deduplicated, final


def instrument_output_paths(output_root: Path, instrument: str) -> dict[str, Path]:
    return {
        "raw": output_root / "raw_candidates" / "by_instrument" / f"{instrument}.parquet",
        "overlap": output_root / "deduplicated" / "by_instrument" / f"{instrument}_overlap.parquet",
        "deduplicated": output_root / "deduplicated" / "by_instrument" / f"{instrument}.parquet",
        "final": output_root / "final" / "by_instrument" / f"{instrument}.parquet",
        "bars": output_root / "cache" / "bars" / f"{instrument}.parquet",
        "checkpoint": output_root / "checkpoints" / f"{instrument}.json",
    }


def process_instrument(
    source_path_text: str,
    config: dict[str, Any],
    config_hash: str,
    force: bool,
) -> dict[str, Any]:
    source_path = Path(source_path_text)
    instrument = normalize_instrument(source_path.stem.removesuffix("_M1"))
    pair_parts(instrument)
    output_root = resolve_from_root(config["output_root"])
    overlay_dir = resolve_from_root(config["bam_overlay_dir"])
    overlay_path = overlay_dir / source_path.name
    paths = instrument_output_paths(output_root, instrument)
    code_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    fingerprint = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": str(config["pipeline_version"]),
        "config_hash": config_hash,
        "code_hash": code_hash,
        "source": source_stat(source_path),
        "bam_overlay": source_stat(overlay_path if overlay_path.exists() else None),
    }
    if not force and paths["checkpoint"].exists():
        try:
            checkpoint = json.loads(paths["checkpoint"].read_text(encoding="utf-8"))
            if checkpoint_matches(checkpoint, fingerprint):
                checkpoint["runtime_status"] = "cached"
                return checkpoint
        except Exception:
            pass

    started = utc_now()
    raw, quality = load_and_normalize_prices(
        source_path,
        overlay_path if overlay_path.exists() else None,
        instrument,
    )
    bars = resample_prices(raw, int(config["bar_minutes"]))
    del raw
    candidates, thresholds = detect_move_candidates(bars, instrument, config)
    overlap_groups, deduplicated, final = select_non_overlapping_candidates(
        candidates, config
    )
    bars_output = bars.reset_index()
    atomic_write_parquet(bars_output, paths["bars"])
    # The broad table retains every overlapping candidate together with its
    # overlap group, rank, and final acceptance/rejection reason.
    atomic_write_parquet(overlap_groups, paths["raw"])
    atomic_write_parquet(overlap_groups, paths["overlap"])
    atomic_write_parquet(deduplicated, paths["deduplicated"])
    atomic_write_parquet(final, paths["final"])

    warnings: list[str] = []
    if quality["estimated_cost_fraction"] > 0.5:
        warnings.append("most_transaction_costs_estimated")
    if quality["gap_events_5_to_120_minutes"] > 0:
        warnings.append("intraday_source_gaps_present")
    if quality["invalid_timestamp_rows"] or quality["invalid_ohlc_rows"]:
        warnings.append("invalid_source_rows_removed")
    if not thresholds or not any(row.get("status") == "ok" for row in thresholds):
        warnings.append("no_horizon_had_enough_valid_windows")
    quality["data_quality_warnings"] = "|".join(warnings)
    inventory = {
        "instrument": instrument,
        "base_currency": pair_parts(instrument)[0],
        "quote_currency": pair_parts(instrument)[1],
        "source_path": str(source_path.resolve()),
        "status": "usable",
        "skip_reason": "",
        "available_start_timestamp": quality["start_timestamp"],
        "available_end_timestamp": quality["end_timestamp"],
        "row_count": quality["usable_rows"],
        "candidate_count": int(len(candidates)),
        "deduplicated_count": int(len(deduplicated)),
        "final_move_count": int(len(final)),
        "data_quality_warnings": quality["data_quality_warnings"],
    }
    completed = utc_now()
    checkpoint = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": str(config["pipeline_version"]),
        "instrument": instrument,
        "fingerprint": fingerprint,
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "elapsed_seconds": (completed - started).total_seconds(),
        "runtime_status": "processed",
        "inventory": inventory,
        "quality": quality,
        "thresholds": thresholds,
        "output_files": {key: str(path.resolve()) for key, path in paths.items() if key != "checkpoint"},
    }
    atomic_write_json(checkpoint, paths["checkpoint"])
    return checkpoint


def discover_source_files(config: dict[str, Any]) -> list[Path]:
    pattern = Path(str(config["source_glob"]))
    pattern = pattern if pattern.is_absolute() else ROOT / pattern
    files = sorted(path.resolve() for path in pattern.parent.glob(pattern.name) if path.is_file())
    return files


def load_current_universe_checkpoints(
    *,
    output_root: Path,
    source_files: Sequence[Path],
    config: dict[str, Any],
    config_hash: str,
) -> dict[str, dict[str, Any]]:
    """Load only checkpoints whose inputs and research configuration are current."""
    overlay_dir = resolve_from_root(config["bam_overlay_dir"])
    source_by_instrument = {
        normalize_instrument(path.stem.removesuffix("_M1")): path for path in source_files
    }
    checkpoints: dict[str, dict[str, Any]] = {}
    for instrument, source_path in source_by_instrument.items():
        checkpoint_path = output_root / "checkpoints" / f"{instrument}.json"
        if not checkpoint_path.exists():
            continue
        try:
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        overlay_path = overlay_dir / source_path.name
        expected = {
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": str(config["pipeline_version"]),
            "config_hash": config_hash,
            "source": source_stat(source_path),
            "bam_overlay": source_stat(overlay_path if overlay_path.exists() else None),
        }
        actual = dict(checkpoint.get("fingerprint", {}))
        actual.pop("code_hash", None)
        if actual != expected:
            continue
        if not all(
            Path(value).exists()
            for value in checkpoint.get("output_files", {}).values()
        ):
            continue
        checkpoint["runtime_status"] = "cached"
        checkpoints[instrument] = checkpoint
    return checkpoints


def concatenate_checkpoint_outputs(
    checkpoints: Sequence[dict[str, Any]],
    output_key: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for checkpoint in checkpoints:
        path_text = checkpoint.get("output_files", {}).get(output_key)
        if not path_text:
            continue
        path = Path(path_text)
        if not path.exists():
            continue
        frame = pd.read_parquet(path)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=0, ignore_index=True, sort=False)


def build_currency_strength_context(
    final: pd.DataFrame,
    checkpoints: Sequence[dict[str, Any]],
) -> pd.DataFrame:
    """Build contemporaneous, hindsight currency decomposition for each move."""
    if final.empty:
        return pd.DataFrame()
    starts = pd.to_datetime(final["start_timestamp"], utc=True)
    ends = pd.to_datetime(final["end_timestamp"], utc=True)
    needed_times = pd.DatetimeIndex(sorted(set(starts.tolist() + ends.tolist())))
    close_columns: dict[str, pd.Series] = {}
    for checkpoint in checkpoints:
        instrument = str(checkpoint["instrument"])
        path_text = checkpoint.get("output_files", {}).get("bars")
        if not path_text or not Path(path_text).exists():
            continue
        bars = pd.read_parquet(path_text, columns=["time_utc", "close"])
        bars["time_utc"] = pd.to_datetime(bars["time_utc"], utc=True)
        series = bars.drop_duplicates("time_utc", keep="last").set_index("time_utc")["close"]
        close_columns[instrument] = pd.to_numeric(series, errors="coerce").reindex(needed_times)
    if not close_columns:
        return pd.DataFrame()
    wide = pd.DataFrame(close_columns, index=needed_times)
    all_pairs = sorted(wide.columns)
    currencies = sorted({currency for pair in all_pairs for currency in pair_parts(pair)})
    rows: list[dict[str, Any]] = []
    for move in final.itertuples(index=False):
        start = pd.Timestamp(move.start_timestamp)
        end = pd.Timestamp(move.end_timestamp)
        if start.tzinfo is None:
            start = start.tz_localize("UTC")
        else:
            start = start.tz_convert("UTC")
        if end.tzinfo is None:
            end = end.tz_localize("UTC")
        else:
            end = end.tz_convert("UTC")
        start_prices = wide.loc[start]
        end_prices = wide.loc[end]
        valid = (
            start_prices.notna()
            & end_prices.notna()
            & (start_prices > 0)
            & (end_prices > 0)
        )
        pair_returns = np.log(end_prices[valid] / start_prices[valid])
        contributions: dict[str, list[float]] = {currency: [] for currency in currencies}
        for pair, value in pair_returns.items():
            base, quote = pair_parts(pair)
            contributions[base].append(float(value))
            contributions[quote].append(float(-value))
        strength = pd.Series(
            {
                currency: (
                    float(np.median(values)) if values else math.nan
                )
                for currency, values in contributions.items()
            },
            dtype=float,
        )
        strength_rank = strength.rank(pct=True)
        base = str(move.base_currency)
        quote = str(move.quote_currency)
        base_strength = safe_number(strength.get(base))
        quote_strength = safe_number(strength.get(quote))
        differential = base_strength - quote_strength
        actual = safe_number(pair_returns.get(str(move.instrument)), safe_number(move.log_return))
        residual = actual - differential if math.isfinite(differential) else math.nan
        if len(pair_returns):
            pair_rank = float(pair_returns.rank(pct=True).get(str(move.instrument), math.nan))
        else:
            pair_rank = math.nan
        valid_strength = strength.dropna()
        strongest = str(valid_strength.idxmax()) if len(valid_strength) else "unknown"
        weakest = str(valid_strength.idxmin()) if len(valid_strength) else "unknown"
        if actual and math.isfinite(differential):
            alignment = math.copysign(
                min(1.0, abs(differential) / max(abs(actual), 1e-12)),
                actual * differential,
            )
        else:
            alignment = 0.0
        rows.append(
            {
                "move_id": str(move.move_id),
                "instrument": str(move.instrument),
                "start_timestamp": start,
                "end_timestamp": end,
                "cross_sectional_pair_count": int(len(pair_returns)),
                "base_currency_strength_score": base_strength * 10_000.0,
                "quote_currency_strength_score": quote_strength * 10_000.0,
                "relative_strength_differential": differential * 10_000.0,
                "base_currency_strength_rank": safe_number(strength_rank.get(base)),
                "quote_currency_strength_rank": safe_number(strength_rank.get(quote)),
                "pair_cross_sectional_rank": pair_rank,
                "pair_residual": residual * 10_000.0,
                "broad_market_alignment_score": alignment,
                "strongest_currency": strongest,
                "weakest_currency": weakest,
                "currency_theme_cluster_id": f"{strongest}_strong__{weakest}_weak",
                "currency_context_quality_flag": (
                    "" if len(pair_returns) >= 60 else "partial_cross_sectional_coverage"
                ),
                "currency_context_is_causal": False,
                "currency_context_note": "contemporaneous full-move decomposition; hindsight context only",
            }
        )
    return pd.DataFrame(rows)


def audit_legacy_catalog(
    final: pd.DataFrame,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    legacy_path = resolve_from_root(config["legacy_catalog"])
    summary: dict[str, Any] = {
        "legacy_path": str(legacy_path),
        "legacy_exists": legacy_path.exists(),
        "legacy_rows": 0,
        "legacy_comparison_rows": 0,
        "legacy_residual_overlap_violations": 0,
        "final_moves_with_legacy_match": 0,
    }
    if not legacy_path.exists():
        return pd.DataFrame(), summary
    legacy = pd.read_parquet(legacy_path)
    summary["legacy_rows"] = int(len(legacy))
    start_column = "start_utc" if "start_utc" in legacy else "start_timestamp"
    end_column = "end_utc" if "end_utc" in legacy else "end_timestamp"
    if start_column not in legacy or end_column not in legacy or "instrument" not in legacy:
        summary["legacy_error"] = "required interval columns missing"
        return pd.DataFrame(), summary
    legacy = legacy.copy()
    legacy["legacy_start_timestamp"] = pd.to_datetime(legacy[start_column], errors="coerce", utc=True)
    legacy["legacy_end_timestamp"] = pd.to_datetime(legacy[end_column], errors="coerce", utc=True)
    if "horizon_minutes" not in legacy:
        legacy["horizon_minutes"] = (
            legacy["legacy_end_timestamp"] - legacy["legacy_start_timestamp"]
        ).dt.total_seconds().div(60.0)
    if "direction" not in legacy:
        legacy["direction"] = ""
    legacy["normalized_direction"] = (
        legacy["direction"].astype(str).str.lower().replace({"long": "up", "short": "down"})
    )
    violations = 0
    for _, group in legacy.dropna(
        subset=["legacy_start_timestamp", "legacy_end_timestamp"]
    ).groupby("instrument"):
        running_end: pd.Timestamp | None = None
        for row in group.sort_values("legacy_start_timestamp").itertuples(index=False):
            start = row.legacy_start_timestamp
            end = row.legacy_end_timestamp
            if running_end is not None and start < running_end:
                violations += 1
            running_end = end if running_end is None else max(running_end, end)
    summary["legacy_residual_overlap_violations"] = int(violations)

    comparison = legacy[
        pd.to_numeric(legacy["horizon_minutes"], errors="coerce").eq(
            int(config["primary_horizon_minutes"])
        )
    ].copy()
    summary["legacy_comparison_rows"] = int(len(comparison))

    tolerance = float(config.get("legacy_match_tolerance_minutes", 45.0))
    rows: list[dict[str, Any]] = []
    for move in final.itertuples(index=False):
        group = comparison[
            comparison["instrument"].astype(str).eq(str(move.instrument))
        ].copy()
        if group.empty:
            rows.append(
                {
                    "move_id": move.move_id,
                    "legacy_match_count": 0,
                    "legacy_nearest_start_delta_minutes": math.nan,
                    "legacy_nearest_start_timestamp": pd.NaT,
                    "legacy_nearest_horizon_minutes": math.nan,
                    "legacy_direction_agrees": False,
                }
            )
            continue
        start = pd.Timestamp(move.start_timestamp)
        end = pd.Timestamp(move.end_timestamp)
        group["start_delta_minutes"] = (
            group["legacy_start_timestamp"] - start
        ).abs().dt.total_seconds().div(60.0)
        group["overlap_fraction"] = [
            interval_overlap_fraction(start, end, row.legacy_start_timestamp, row.legacy_end_timestamp)
            for row in group.itertuples(index=False)
        ]
        matches = group[
            (group["start_delta_minutes"] <= tolerance)
            | (group["overlap_fraction"] >= 0.3)
        ]
        nearest = group.sort_values(
            ["start_delta_minutes", "overlap_fraction"], ascending=[True, False]
        ).iloc[0]
        rows.append(
            {
                "move_id": move.move_id,
                "legacy_match_count": int(len(matches)),
                "legacy_nearest_start_delta_minutes": float(nearest["start_delta_minutes"]),
                "legacy_nearest_start_timestamp": nearest["legacy_start_timestamp"],
                "legacy_nearest_horizon_minutes": safe_number(nearest["horizon_minutes"]),
                "legacy_direction_agrees": str(nearest["normalized_direction"]) == str(move.direction),
            }
        )
    crosswalk = pd.DataFrame(rows)
    if not crosswalk.empty:
        summary["final_moves_with_legacy_match"] = int((crosswalk["legacy_match_count"] > 0).sum())
    return crosswalk, summary


def add_rankings(final: pd.DataFrame) -> pd.DataFrame:
    if final.empty:
        return final.copy()
    ranked = final.copy()
    ranked["start_timestamp"] = pd.to_datetime(ranked["start_timestamp"], utc=True)
    ranked["end_timestamp"] = pd.to_datetime(ranked["end_timestamp"], utc=True)
    specifications = {
        "global_significance_rank": "significance_score",
        "global_pip_rank": "absolute_pip_change",
        "global_percentage_rank": "absolute_percentage_return",
        "global_atr_rank": "atr_normalized_move",
        "global_efficiency_rank": "path_efficiency_ratio",
        "global_velocity_rank": "move_velocity_pips_per_hour",
        "global_duration_rank": "duration_minutes",
        "global_low_choppiness_rank": "choppiness_estimate",
        "global_volatility_rank": "realized_volatility_pct",
    }
    for rank_column, value_column in specifications.items():
        ascending = rank_column == "global_low_choppiness_rank"
        ranked[rank_column] = (
            pd.to_numeric(ranked[value_column], errors="coerce")
            .rank(method="min", ascending=ascending, na_option="bottom")
            .astype(int)
        )
    ranked["instrument_rank"] = (
        ranked.groupby("instrument")["significance_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    ranked["direction_rank"] = (
        ranked.groupby("direction")["significance_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    ranked["year"] = ranked["start_timestamp"].dt.year.astype(int)
    ranked["month"] = ranked["start_timestamp"].dt.strftime("%Y-%m")
    ranked["year_rank"] = (
        ranked.groupby("year")["significance_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    ranked["month_rank"] = (
        ranked.groupby("month")["significance_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    return ranked.sort_values("global_significance_rank").reset_index(drop=True)


def sqlite_ready(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in output.columns:
        if isinstance(output[column].dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_any_dtype(
            output[column]
        ):
            output[column] = pd.to_datetime(output[column], errors="coerce", utc=True).map(
                lambda value: value.isoformat() if not pd.isna(value) else None
            )
        elif output[column].dtype == object:
            output[column] = output[column].map(
                lambda value: (
                    value.isoformat()
                    if isinstance(value, (pd.Timestamp, datetime))
                    else value
                )
            )
    return output


def write_sqlite_database(
    path: Path,
    *,
    final: pd.DataFrame,
    deduplicated: pd.DataFrame,
    candidates: pd.DataFrame,
    inventory: pd.DataFrame,
    quality: pd.DataFrame,
    event_links: pd.DataFrame,
    include_candidates: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()
    connection = sqlite3.connect(temporary)
    try:
        sqlite_ready(final).to_sql("significant_moves", connection, if_exists="replace", index=False)
        sqlite_ready(deduplicated).to_sql(
            "deduplicated_moves", connection, if_exists="replace", index=False
        )
        sqlite_ready(inventory).to_sql(
            "instrument_inventory", connection, if_exists="replace", index=False
        )
        sqlite_ready(quality).to_sql("data_quality", connection, if_exists="replace", index=False)
        sqlite_ready(event_links).to_sql("move_event_links", connection, if_exists="replace", index=False)
        if include_candidates:
            sqlite_ready(candidates).to_sql(
                "rolling_move_candidates", connection, if_exists="replace", index=False
            )
        if not final.empty:
            connection.execute(
                "CREATE INDEX idx_significant_moves_instrument_start "
                "ON significant_moves(instrument, start_timestamp)"
            )
            connection.execute(
                "CREATE INDEX idx_significant_moves_currencies "
                "ON significant_moves(base_currency, quote_currency)"
            )
            connection.execute(
                "CREATE INDEX idx_significant_moves_score "
                "ON significant_moves(significance_score DESC)"
            )
        connection.commit()
    finally:
        connection.close()
    os.replace(temporary, path)


def render_report(
    *,
    config: dict[str, Any],
    inventory: pd.DataFrame,
    quality: pd.DataFrame,
    candidates: pd.DataFrame,
    deduplicated: pd.DataFrame,
    final: pd.DataFrame,
    legacy_summary: dict[str, Any],
    run_summary: dict[str, Any],
) -> str:
    lines = [
        "# Significant Moves: roughly two-hour FX catalog",
        "",
        f"Generated: `{run_summary['completed_at']}`.",
        f"Pipeline: `{config['pipeline_version']}`.",
        "",
        "## Outcome",
        "",
        f"- Instruments discovered: **{len(inventory)}**",
        f"- Instruments usable: **{int((inventory['status'] == 'usable').sum()) if len(inventory) else 0}**",
        f"- Broad overlapping candidates: **{len(candidates):,}**",
        f"- Deduplicated directional candidates: **{len(deduplicated):,}**",
        f"- Strict, non-overlapping final moves: **{len(final):,}**",
        f"- Horizons: **{', '.join(str(value) for value in config['horizons_minutes'])} minutes** (primary {config['primary_horizon_minutes']}m)",
        "",
        "Strict events pass the pair/horizon full-sample q"
        f"{float(config['strict_quantile']):.3f} tail, at least "
        f"{float(config['strict_minimum_atr_units']):.2f} ATR units, "
        f"{float(config['strict_minimum_move_to_cost']):.2f}x estimated round-trip cost, "
        "the configured path-efficiency and observation-coverage floors, then a "
        "greedy largest-significance strict interval selection.",
        "",
        "## Important interpretation",
        "",
        "This is a retrospective/hindsight movement catalog, not a trading signal or a performance claim. "
        "Full-sample percentile thresholds and overlap selection are explicitly marked non-causal. "
        "Most historical pairs lack observed bid/ask data; the catalog overlays the project's 12-pair "
        "BAM archive where possible and flags every remaining estimated-cost window.",
        "",
        "The scan uses a regular wall-clock five-minute grid and exact 90/120/150-minute timestamps. "
        "It does not make the legacy mistake of treating the next N sparse observations as N minutes.",
        "",
        "## Data quality",
        "",
    ]
    if not quality.empty:
        total_rows = int(quality["usable_rows"].sum())
        estimated_rows = int(quality["estimated_cost_rows"].sum())
        estimated_fraction = estimated_rows / max(total_rows, 1)
        latest = pd.to_datetime(quality["end_timestamp"], errors="coerce", utc=True).max()
        lines.extend(
            [
                f"- Canonical usable M1 rows: **{total_rows:,}**",
                f"- Rows whose transaction cost remains estimated: **{estimated_rows:,} ({estimated_fraction:.2%})**",
                f"- Latest source timestamp: **{latest.isoformat() if not pd.isna(latest) else 'unknown'}**",
                f"- Instruments with quality warnings: **{int(quality['data_quality_warnings'].fillna('').ne('').sum())}**",
            ]
        )
    if not final.empty:
        fully_estimated = int(
            (pd.to_numeric(final["estimated_transaction_cost_fraction"], errors="coerce") >= 0.999).sum()
        )
        sparse_windows = int(final["data_quality_flags"].fillna("").str.contains("sparse_window").sum())
        weekend_windows = int(final["weekend_exposure"].astype(bool).sum())
        partial_context = int(
            (pd.to_numeric(final.get("cross_sectional_pair_count"), errors="coerce") < 60).sum()
        )
        lines.extend(
            [
                f"- Final moves with fully estimated costs: **{fully_estimated:,} / {len(final):,}**",
                f"- Final moves with sparse-window warnings: **{sparse_windows:,}**",
                f"- Final moves with calendar-weekend exposure: **{weekend_windows:,}**",
                f"- Final moves with fewer than 60 cross-sectional context pairs: **{partial_context:,}**",
            ]
        )
    lines += [
        "",
        "## Legacy catalog audit",
        "",
        f"- Legacy rows: **{int(legacy_summary.get('legacy_rows', 0)):,}**",
        f"- Legacy {int(config['primary_horizon_minutes'])}m comparison rows: **{int(legacy_summary.get('legacy_comparison_rows', 0)):,}**",
        f"- Residual same-pair legacy overlap violations: **{int(legacy_summary.get('legacy_residual_overlap_violations', 0)):,}**",
        f"- New final moves with a nearby/overlapping legacy match: **{int(legacy_summary.get('final_moves_with_legacy_match', 0)):,}**",
        "",
        "The legacy table remains a benchmark only; it is not imported into the new final set.",
        "",
        "## Top moves",
        "",
        "| Rank | Instrument | Start UTC | Horizon | Direction | Pips | ATR units | Efficiency | Score |",
        "|---:|---|---|---:|---|---:|---:|---:|---:|",
    ]
    for row in final.sort_values("significance_score", ascending=False).head(15).itertuples(
        index=False
    ):
        lines.append(
            f"| {int(row.global_significance_rank)} | {row.instrument} | {pd.Timestamp(row.start_timestamp).isoformat()} | "
            f"{int(row.horizon_minutes)}m | {row.direction} | {float(row.absolute_pip_change):.1f} | "
            f"{float(row.atr_normalized_move):.2f} | {float(row.path_efficiency_ratio):.2f} | "
            f"{float(row.significance_score):.1f} |"
        )
    lines += [
        "",
        "## Querying",
        "",
        "```powershell",
        "python trad/query_significant_moves.py --instrument EUR_USD --top 20",
        "python trad/query_significant_moves.py --currency USD --direction down --group-by year",
        "```",
        "",
        "See `trad/SIGNIFICANT_MOVES.md` for rerun, output, and query details.",
    ]
    return "\n".join(lines) + "\n"


def assemble_outputs(
    *,
    config: dict[str, Any],
    config_hash: str,
    checkpoints: Sequence[dict[str, Any]],
    failed_inventory: Sequence[dict[str, Any]],
    run_started: datetime,
    selected_source_count: int,
) -> dict[str, Any]:
    output_root = resolve_from_root(config["output_root"])
    inventory_rows = [dict(checkpoint["inventory"]) for checkpoint in checkpoints]
    inventory_rows.extend(dict(row) for row in failed_inventory)
    inventory = pd.DataFrame(inventory_rows)
    if not inventory.empty:
        inventory = inventory.sort_values("instrument").reset_index(drop=True)
        ends = pd.to_datetime(inventory["available_end_timestamp"], errors="coerce", utc=True)
        inventory["data_age_days_at_run"] = (
            pd.Timestamp(utc_now()) - ends
        ).dt.total_seconds().div(86_400.0)
        stale = inventory["data_age_days_at_run"] > 2.0
        inventory.loc[stale, "data_quality_warnings"] = inventory.loc[
            stale, "data_quality_warnings"
        ].fillna("").map(lambda value: "|".join(filter(None, [value, "source_data_stale_over_2_days"])))
    quality = pd.DataFrame([dict(checkpoint["quality"]) for checkpoint in checkpoints])
    if not quality.empty:
        quality = quality.sort_values("instrument").reset_index(drop=True)
        quality["data_age_days_at_run"] = (
            pd.Timestamp(utc_now())
            - pd.to_datetime(quality["end_timestamp"], errors="coerce", utc=True)
        ).dt.total_seconds().div(86_400.0)
        stale = quality["data_age_days_at_run"] > 2.0
        quality.loc[stale, "data_quality_warnings"] = quality.loc[
            stale, "data_quality_warnings"
        ].fillna("").map(lambda value: "|".join(filter(None, [value, "source_data_stale_over_2_days"])))
    thresholds = pd.DataFrame(
        [row for checkpoint in checkpoints for row in checkpoint.get("thresholds", [])]
    )

    candidates = concatenate_checkpoint_outputs(checkpoints, "raw")
    overlap_groups = concatenate_checkpoint_outputs(checkpoints, "overlap")
    deduplicated = concatenate_checkpoint_outputs(checkpoints, "deduplicated")
    final = concatenate_checkpoint_outputs(checkpoints, "final")
    for frame in (candidates, overlap_groups, deduplicated, final):
        if not frame.empty:
            frame["start_timestamp"] = pd.to_datetime(frame["start_timestamp"], utc=True)
            frame["end_timestamp"] = pd.to_datetime(frame["end_timestamp"], utc=True)

    currency_context = build_currency_strength_context(final, checkpoints)
    if not currency_context.empty:
        context_columns = [
            column
            for column in currency_context.columns
            if column not in {"instrument", "start_timestamp", "end_timestamp"}
        ]
        final = final.merge(currency_context[context_columns], on="move_id", how="left")
    legacy_crosswalk, legacy_summary = audit_legacy_catalog(final, config)
    if not legacy_crosswalk.empty:
        final = final.merge(legacy_crosswalk, on="move_id", how="left")
    final = add_rankings(final)

    atomic_write_csv(inventory, output_root / "instrument_inventory.csv")
    atomic_write_csv(quality, output_root / "data_quality_report.csv")
    atomic_write_csv(thresholds, output_root / "manifests" / "instrument_thresholds.csv")
    atomic_write_parquet(
        candidates,
        output_root / "raw_candidates" / "rolling_move_candidates.parquet",
    )
    atomic_write_parquet(
        overlap_groups,
        output_root / "deduplicated" / "overlap_groups.parquet",
    )
    atomic_write_parquet(
        deduplicated,
        output_root / "deduplicated" / "deduplicated_moves.parquet",
    )
    atomic_write_parquet(final, output_root / "final" / "significant_moves_final.parquet")
    atomic_write_csv(final, output_root / "final" / "significant_moves_final.csv")
    atomic_write_parquet(
        currency_context,
        output_root / "currency_strength_context.parquet",
    )
    atomic_write_csv(
        legacy_crosswalk,
        output_root / "reports" / "legacy_catalog_crosswalk.csv",
    )

    top20 = final[final["instrument_rank"] <= 20].copy() if not final.empty else final.copy()
    top50 = final[final["instrument_rank"] <= 50].copy() if not final.empty else final.copy()
    top100 = final[final["instrument_rank"] <= 100].copy() if not final.empty else final.copy()
    atomic_write_csv(top20, output_root / "final" / "significant_moves_top20_per_pair.csv")
    atomic_write_csv(top50, output_root / "final" / "significant_moves_top50_per_pair.csv")
    atomic_write_csv(top100, output_root / "final" / "significant_moves_top100_per_pair.csv")
    atomic_write_csv(final, output_root / "final" / "significant_moves_global_rankings.csv")

    event_links_path = output_root / "event_links" / "move_event_links.csv"
    if event_links_path.exists():
        event_links = pd.read_csv(event_links_path)
    else:
        event_links = pd.DataFrame(columns=EVENT_LINK_COLUMNS)
        atomic_write_csv(event_links, event_links_path)
    write_sqlite_database(
        output_root / "significant_moves.sqlite",
        final=final,
        deduplicated=deduplicated,
        candidates=candidates,
        inventory=inventory,
        quality=quality,
        event_links=event_links,
        include_candidates=bool(config.get("sqlite_include_raw_candidates", False)),
    )

    completed = utc_now()
    run_summary = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": str(config["pipeline_version"]),
        "config_hash": config_hash,
        "config_path": str(config["config_path"]),
        "output_root": str(output_root),
        "started_at": run_started.isoformat(),
        "completed_at": completed.isoformat(),
        "elapsed_seconds": (completed - run_started).total_seconds(),
        "selected_source_files": int(selected_source_count),
        "usable_instruments": int(len(checkpoints)),
        "failed_instruments": int(len(failed_inventory)),
        "processed_instruments": int(
            sum(checkpoint.get("runtime_status") == "processed" for checkpoint in checkpoints)
        ),
        "cached_instruments": int(
            sum(checkpoint.get("runtime_status") == "cached" for checkpoint in checkpoints)
        ),
        "broad_candidate_count": int(len(candidates)),
        "overlap_group_count": int(
            overlap_groups["overlap_group_id"].nunique()
            if not overlap_groups.empty and "overlap_group_id" in overlap_groups
            else 0
        ),
        "deduplicated_candidate_count": int(len(deduplicated)),
        "strict_final_move_count": int(len(final)),
        "legacy_audit": legacy_summary,
        "hindsight_warning": (
            "Retrospective full-sample thresholds, extrema/path diagnostics, currency context, "
            "and overlap selection are not causal trading signals."
        ),
    }
    report = render_report(
        config=config,
        inventory=inventory,
        quality=quality,
        candidates=candidates,
        deduplicated=deduplicated,
        final=final,
        legacy_summary=legacy_summary,
        run_summary=run_summary,
    )
    report_path = output_root / "reports" / "latest_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")
    stamp = completed.strftime("%Y%m%d_%H%M%S")
    manifest_path = output_root / "manifests" / f"run_{stamp}.json"
    run_summary["report_path"] = str(report_path)
    run_summary["manifest_path"] = str(manifest_path)
    atomic_write_json(run_summary, manifest_path)
    atomic_write_json(run_summary, output_root / "manifests" / "latest_manifest.json")
    atomic_write_json(run_summary, output_root / "run_manifest.json")
    return run_summary


def prepare_output_directories(output_root: Path) -> None:
    for relative in [
        "config",
        "manifests",
        "raw_candidates/by_instrument",
        "deduplicated/by_instrument",
        "final/by_instrument",
        "event_links",
        "reports",
        "checkpoints",
        "cache/bars",
    ]:
        (output_root / relative).mkdir(parents=True, exist_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--instrument", action="append", default=[])
    parser.add_argument("--max-instruments", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--assemble-only", action="store_true")
    args = parser.parse_args(argv)

    config_path = args.config.resolve()
    config = load_config(config_path)
    if args.output_root is not None:
        config["output_root"] = str(args.output_root.resolve())
    if args.workers is not None:
        config["workers"] = max(1, int(args.workers))
    output_root = resolve_from_root(config["output_root"])
    prepare_output_directories(output_root)
    hash_payload = {
        key: value
        for key, value in config.items()
        if key not in {"config_path", "workers", "output_root"}
    }
    config_hash = json_hash(hash_payload)
    effective_config = {**config, "config_hash": config_hash, "schema_version": SCHEMA_VERSION}
    atomic_write_json(effective_config, output_root / "config" / "effective_config.json")
    run_started = utc_now()

    requested = {normalize_instrument(value) for value in args.instrument}
    all_source_files = discover_source_files(config)
    source_files = list(all_source_files)
    if requested:
        source_files = [
            path
            for path in source_files
            if normalize_instrument(path.stem.removesuffix("_M1")) in requested
        ]
    if args.max_instruments is not None:
        source_files = source_files[: max(0, int(args.max_instruments))]
    if not source_files and not args.assemble_only:
        raise FileNotFoundError(f"no source files matched {config['source_glob']!r}")

    checkpoints: list[dict[str, Any]] = []
    failed_inventory: list[dict[str, Any]] = []
    if args.assemble_only:
        checkpoints = list(
            load_current_universe_checkpoints(
                output_root=output_root,
                source_files=all_source_files,
                config=config,
                config_hash=config_hash,
            ).values()
        )
        selected_source_count = len(checkpoints)
    else:
        selected_source_count = len(source_files)
        workers = max(1, int(config.get("workers", 1)))
        print(
            f"[start] {len(source_files)} instruments; workers={workers}; output={output_root}",
            flush=True,
        )
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    process_instrument,
                    str(source_path),
                    config,
                    config_hash,
                    bool(args.force),
                ): source_path
                for source_path in source_files
            }
            completed_count = 0
            for future in concurrent.futures.as_completed(futures):
                source_path = futures[future]
                instrument = normalize_instrument(source_path.stem.removesuffix("_M1"))
                completed_count += 1
                try:
                    checkpoint = future.result()
                    checkpoints.append(checkpoint)
                    inventory = checkpoint["inventory"]
                    print(
                        f"[{completed_count}/{len(source_files)}] {instrument} "
                        f"{checkpoint['runtime_status']} candidates={inventory['candidate_count']} "
                        f"final={inventory['final_move_count']} "
                        f"seconds={float(checkpoint.get('elapsed_seconds', 0.0)):.1f}",
                        flush=True,
                    )
                except Exception as error:
                    failed_inventory.append(
                        {
                            "instrument": instrument,
                            "base_currency": pair_parts(instrument)[0],
                            "quote_currency": pair_parts(instrument)[1],
                            "source_path": str(source_path),
                            "status": "skipped",
                            "skip_reason": f"{type(error).__name__}: {error}",
                            "available_start_timestamp": "",
                            "available_end_timestamp": "",
                            "row_count": 0,
                            "candidate_count": 0,
                            "deduplicated_count": 0,
                            "final_move_count": 0,
                            "data_quality_warnings": "processing_failed",
                        }
                    )
                    print(
                        f"[{completed_count}/{len(source_files)}] {instrument} FAILED: {error}",
                        file=sys.stderr,
                        flush=True,
                    )
                    traceback.print_exception(error, file=sys.stderr)

    targeted_run = bool(requested) or args.max_instruments is not None
    if targeted_run and not args.assemble_only:
        # A one-pair refresh must not replace the permanent universe catalog
        # with a one-pair assembly. Merge the refreshed partitions into every
        # other still-current checkpoint before writing global artifacts.
        universe_checkpoints = load_current_universe_checkpoints(
            output_root=output_root,
            source_files=all_source_files,
            config=config,
            config_hash=config_hash,
        )
        failed_instruments = {str(row["instrument"]) for row in failed_inventory}
        for instrument in failed_instruments:
            universe_checkpoints.pop(instrument, None)
        universe_checkpoints.update(
            {str(checkpoint["instrument"]): checkpoint for checkpoint in checkpoints}
        )
        checkpoints = list(universe_checkpoints.values())
        expected_instruments = {
            normalize_instrument(path.stem.removesuffix("_M1"))
            for path in all_source_files
        }
        missing_instruments = sorted(expected_instruments - set(universe_checkpoints))
        if args.output_root is None and missing_instruments:
            raise RuntimeError(
                "refusing to overwrite the production catalog with a partial universe; "
                "missing or stale checkpoints: " + ", ".join(missing_instruments)
            )
    if args.assemble_only and args.output_root is None:
        expected_instruments = {
            normalize_instrument(path.stem.removesuffix("_M1"))
            for path in all_source_files
        }
        available_instruments = {
            str(checkpoint["instrument"]) for checkpoint in checkpoints
        }
        missing_instruments = sorted(expected_instruments - available_instruments)
        if missing_instruments:
            raise RuntimeError(
                "refusing to overwrite the production catalog from an incomplete "
                "assemble-only checkpoint set; missing or stale checkpoints: "
                + ", ".join(missing_instruments)
            )
    checkpoints.sort(key=lambda checkpoint: checkpoint["instrument"])
    summary = assemble_outputs(
        config=config,
        config_hash=config_hash,
        checkpoints=checkpoints,
        failed_inventory=failed_inventory,
        run_started=run_started,
        selected_source_count=selected_source_count,
    )
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if not failed_inventory else 2


if __name__ == "__main__":
    raise SystemExit(main())
