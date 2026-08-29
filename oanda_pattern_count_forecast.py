#!/usr/bin/env python3
"""Causal pattern-count forecasts built from historical and completed live M1 bars.

The cache builder scans local OANDA M1 CSVs once. The runtime model combines
those historical pattern statistics with newer completed candles observed from
OANDA. It never updates a pattern until the corresponding forecast horizon has
matured.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np


SCHEMA_VERSION = 1
MINUTE_NS = 60 * 1_000_000_000
HORIZONS_MIN = (1, 3, 5)
PATTERN_SPECS = (
    ("sign", 3),
    ("sign", 5),
    ("sign", 7),
    ("magnitude", 3),
)
MAGNITUDE_LABELS = ("D2", "D1", "F", "U1", "U2")
ROOT = Path(__file__).resolve().parent
DEFAULT_CANDLE_DIR = ROOT / "data" / "oanda_training_manager" / "candles"
DEFAULT_CACHE = ROOT / "data" / "oanda_training_manager" / "state" / "pattern_count_history_v1.json.gz"
DEFAULT_LIVE_STATE = ROOT / "data" / "oanda_training_manager" / "state" / "pattern_count_live_v1.json.gz"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def parse_time_ns(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        prefix, suffix = text.split(".", 1)
        zone_index = max(suffix.find("+"), suffix.find("-"))
        if zone_index >= 0:
            fraction, zone = suffix[:zone_index], suffix[zone_index:]
        else:
            fraction, zone = suffix, ""
        text = f"{prefix}.{fraction[:6]}{zone}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1_000_000_000)


def format_time_ns(value: int | float | None) -> str:
    if value is None or safe_float(value) <= 0.0:
        return ""
    return datetime.fromtimestamp(int(value) / 1_000_000_000, timezone.utc).isoformat()


def empty_stats() -> dict[str, float]:
    return {"count": 0.0, "up": 0.0, "zero": 0.0, "sum": 0.0, "abs_sum": 0.0}


def add_observation(stats: dict[str, Any], value: float) -> None:
    stats["count"] = safe_float(stats.get("count")) + 1.0
    stats["up"] = safe_float(stats.get("up")) + float(value > 0.0)
    stats["zero"] = safe_float(stats.get("zero")) + float(math.isclose(value, 0.0, abs_tol=1e-12))
    stats["sum"] = safe_float(stats.get("sum")) + value
    stats["abs_sum"] = safe_float(stats.get("abs_sum")) + abs(value)


def stats_rate(stats: dict[str, Any] | None, key: str) -> float:
    source = stats or {}
    count = safe_float(source.get("count"))
    if count <= 0.0:
        return 0.0
    if key == "probability_up":
        return (safe_float(source.get("up")) + 0.5 * safe_float(source.get("zero"))) / count
    if key == "mean":
        return safe_float(source.get("sum")) / count
    if key == "mean_abs":
        return safe_float(source.get("abs_sum")) / count
    raise KeyError(key)


def stats_from_groups(codes: np.ndarray, values: np.ndarray, size: int) -> dict[str, dict[str, float]]:
    valid = (codes >= 0) & np.isfinite(values)
    if not np.any(valid):
        return {}
    local_codes = codes[valid].astype(np.int64, copy=False)
    local_values = values[valid].astype(np.float64, copy=False)
    counts = np.bincount(local_codes, minlength=size).astype(np.float64)
    ups = np.bincount(local_codes, weights=(local_values > 0.0), minlength=size)
    zeros = np.bincount(local_codes, weights=np.isclose(local_values, 0.0), minlength=size)
    sums = np.bincount(local_codes, weights=local_values, minlength=size)
    abs_sums = np.bincount(local_codes, weights=np.abs(local_values), minlength=size)
    output: dict[str, dict[str, float]] = {}
    for code in np.flatnonzero(counts):
        output[str(int(code))] = {
            "count": int(counts[code]),
            "up": int(ups[code]),
            "zero": int(zeros[code]),
            "sum": round(float(sums[code]), 10),
            "abs_sum": round(float(abs_sums[code]), 10),
        }
    return output


def sign_codes(returns: np.ndarray, order: int) -> np.ndarray:
    source = np.asarray(returns, dtype=np.float64)
    codes = np.zeros(len(source), dtype=np.int32)
    valid = np.ones(len(source), dtype=bool)
    for position in range(order):
        offset = order - 1 - position
        shifted = np.full(len(source), np.nan)
        if offset == 0:
            shifted[:] = source
        else:
            shifted[offset:] = source[:-offset]
        valid &= np.isfinite(shifted)
        codes += (shifted >= 0.0).astype(np.int32) << (order - 1 - position)
    codes[~valid] = -1
    return codes


def magnitude_categories(returns: np.ndarray, scales: np.ndarray) -> np.ndarray:
    source = np.asarray(returns, dtype=np.float64)
    local_scales = np.asarray(scales, dtype=np.float64)
    valid = np.isfinite(source) & np.isfinite(local_scales) & (local_scales > 0.0)
    ratios = np.zeros(len(source), dtype=np.float64)
    ratios[valid] = source[valid] / local_scales[valid]
    categories = np.full(len(source), -1, dtype=np.int16)
    categories[valid & (ratios <= -1.5)] = 0
    categories[valid & (ratios > -1.5) & (ratios < -0.1)] = 1
    categories[valid & (np.abs(ratios) <= 0.1)] = 2
    categories[valid & (ratios > 0.1) & (ratios < 1.5)] = 3
    categories[valid & (ratios >= 1.5)] = 4
    return categories


def base_codes(categories: np.ndarray, order: int, base: int) -> np.ndarray:
    source = np.asarray(categories, dtype=np.int16)
    codes = np.zeros(len(source), dtype=np.int32)
    valid = np.ones(len(source), dtype=bool)
    for position in range(order):
        offset = order - 1 - position
        shifted = np.full(len(source), -1, dtype=np.int16)
        if offset == 0:
            shifted[:] = source
        else:
            shifted[offset:] = source[:-offset]
        valid &= shifted >= 0
        codes = codes * base + np.maximum(shifted, 0)
    codes[~valid] = -1
    return codes


def rolling_median_scale(returns: np.ndarray, window: int = 60, minimum: int = 20) -> np.ndarray:
    source = np.abs(np.asarray(returns, dtype=np.float64))
    source[~np.isfinite(source) | (source <= 0.0)] = np.nan
    try:
        import pandas as pd

        # Shift keeps the current return out of its own magnitude category.
        return (
            pd.Series(source, copy=False)
            .rolling(window=window, min_periods=minimum)
            .median()
            .shift(1)
            .to_numpy(dtype=np.float64)
        )
    except ImportError:
        output = np.full(len(source), np.nan)
        for index in range(len(source)):
            prior = source[max(0, index - window) : index]
            prior = prior[np.isfinite(prior)]
            if len(prior) >= minimum:
                output[index] = float(np.median(prior))
        return output


def pattern_label(mode: str, order: int, code: int) -> str:
    if mode == "sign":
        return " ".join("U" if code & (1 << (order - 1 - index)) else "D" for index in range(order))
    digits = [0] * order
    remainder = int(code)
    for index in range(order - 1, -1, -1):
        digits[index] = remainder % 5
        remainder //= 5
    return " ".join(MAGNITUDE_LABELS[digit] for digit in digits)


def _pattern_key(mode: str, order: int, horizon_min: int) -> str:
    return f"{mode}:{order}:{horizon_min}"


def build_instrument_history(times_ns: np.ndarray, closes: np.ndarray, pip: float) -> dict[str, Any]:
    times = np.asarray(times_ns, dtype=np.int64)
    prices = np.asarray(closes, dtype=np.float64)
    if len(times) != len(prices) or len(times) < 20:
        raise ValueError("pattern history requires aligned time and close arrays")
    order_index = np.argsort(times, kind="stable")
    times = times[order_index]
    prices = prices[order_index]
    keep = np.r_[times[1:] != times[:-1], True]
    times = times[keep]
    prices = prices[keep]
    returns = np.full(len(prices), np.nan)
    consecutive = (times[1:] - times[:-1]) == MINUTE_NS
    returns[1:] = (prices[1:] - prices[:-1]) / pip
    returns[1:][~consecutive] = np.nan
    scales = rolling_median_scale(returns)
    code_map: dict[tuple[str, int], np.ndarray] = {}
    for mode, order in PATTERN_SPECS:
        if mode == "sign":
            code_map[(mode, order)] = sign_codes(returns, order)
        else:
            code_map[(mode, order)] = base_codes(magnitude_categories(returns, scales), order, 5)

    baseline: dict[str, dict[str, float]] = {}
    patterns: dict[str, dict[str, dict[str, float]]] = {}
    for horizon in HORIZONS_MIN:
        targets = np.full(len(prices), np.nan)
        valid_future = (times[horizon:] - times[:-horizon]) == horizon * MINUTE_NS
        local_targets = (prices[horizon:] - prices[:-horizon]) / pip
        local_targets[~valid_future] = np.nan
        targets[:-horizon] = local_targets
        finite_targets = targets[np.isfinite(targets)]
        baseline[str(horizon)] = {
            "count": int(len(finite_targets)),
            "up": int(np.sum(finite_targets > 0.0)),
            "zero": int(np.sum(np.isclose(finite_targets, 0.0))),
            "sum": round(float(np.sum(finite_targets)), 10),
            "abs_sum": round(float(np.sum(np.abs(finite_targets))), 10),
        }
        for mode, order in PATTERN_SPECS:
            size = (2**order) if mode == "sign" else (5**order)
            patterns[_pattern_key(mode, order, horizon)] = stats_from_groups(
                code_map[(mode, order)], targets, size
            )
    return {
        "row_count": int(len(times)),
        "start_time_ns": int(times[0]),
        "end_time_ns": int(times[-1]),
        "pip_size": pip,
        "baseline": baseline,
        "patterns": patterns,
    }


def _open_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = ".json.gz" if path.suffix.lower() == ".gz" else ".json"
    descriptor, temporary = tempfile.mkstemp(prefix=path.stem + ".", suffix=suffix, dir=path.parent)
    os.close(descriptor)
    temporary_path = Path(temporary)
    try:
        opener = gzip.open if path.suffix.lower() == ".gz" else open
        with opener(temporary_path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _infer_pip(frame: Any, instrument: str) -> float:
    if {"bid_close", "ask_close", "spread_pips"}.issubset(frame.columns):
        spread = np.asarray(frame["spread_pips"], dtype=np.float64)
        distance = np.asarray(frame["ask_close"] - frame["bid_close"], dtype=np.float64)
        valid = np.isfinite(spread) & np.isfinite(distance) & (spread > 0.0) & (distance > 0.0)
        if np.any(valid):
            estimate = float(np.median(distance[valid] / spread[valid]))
            if estimate > 0.0:
                return 10.0 ** round(math.log10(estimate))
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def _build_history_file(path: Path, tail_rows: int) -> tuple[str, dict[str, Any]]:
    import pandas as pd

    instrument = path.stem.removesuffix("_M1")
    columns = ["datetime", "close", "bid_close", "ask_close", "spread_pips"]
    try:
        frame = pd.read_csv(path, usecols=columns, engine="pyarrow")
    except (ImportError, ValueError):
        frame = pd.read_csv(path, usecols=columns)
    if tail_rows > 0:
        frame = frame.tail(tail_rows).copy()
    frame["timestamp"] = pd.to_datetime(frame.pop("datetime"), errors="coerce", utc=True)
    for column in columns[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"])
    pip = _infer_pip(frame, instrument)
    instrument_history = build_instrument_history(
        frame["timestamp"].dt.as_unit("ns").astype("int64").to_numpy(),
        frame["close"].to_numpy(dtype=np.float64),
        pip,
    )
    if sum(int(row.get("count") or 0) for row in instrument_history["baseline"].values()) <= 0:
        raise ValueError(f"No continuous M1 outcomes were built for {instrument}")
    return instrument, instrument_history


def build_cache(
    candle_dir: Path,
    output: Path,
    instruments: Iterable[str] | None = None,
    tail_rows: int = 0,
    workers: int = 4,
) -> dict[str, Any]:
    requested = {str(item).upper().replace("/", "_") for item in instruments or []}
    paths = sorted(candle_dir.glob("*_M1.csv"))
    if requested:
        paths = [path for path in paths if path.stem.removesuffix("_M1") in requested]
    if not paths:
        raise FileNotFoundError(f"No M1 candle files found in {candle_dir}")
    history: dict[str, Any] = {}
    worker_count = min(max(1, workers), len(paths))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="pattern-cache") as pool:
        futures = {pool.submit(_build_history_file, path, tail_rows): path for path in paths}
        for index, future in enumerate(as_completed(futures), start=1):
            instrument, instrument_history = future.result()
            history[instrument] = instrument_history
            print(f"[pattern-cache] {index}/{len(paths)} {instrument}", flush=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": utc_now(),
        "source_dir": str(candle_dir.resolve()),
        "tail_rows": int(tail_rows),
        "pattern_specs": [{"mode": mode, "order": order} for mode, order in PATTERN_SPECS],
        "horizons_min": list(HORIZONS_MIN),
        "instruments": history,
    }
    _atomic_json(output, payload)
    return payload


def _candle_arrays(candles: list[dict[str, Any]], pip: float) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    rows: dict[int, float] = {}
    for candle in candles:
        timestamp = parse_time_ns(candle.get("time"))
        close = safe_float((candle.get("mid") or {}).get("c"), math.nan)
        if timestamp is not None and math.isfinite(close):
            rows[timestamp] = close
    if not rows:
        return tuple(np.asarray([], dtype=np.float64) for _ in range(4))  # type: ignore[return-value]
    times = np.asarray(sorted(rows), dtype=np.int64)
    closes = np.asarray([rows[int(value)] for value in times], dtype=np.float64)
    returns = np.full(len(closes), np.nan)
    if len(closes) > 1:
        returns[1:] = (closes[1:] - closes[:-1]) / pip
        returns[1:][(times[1:] - times[:-1]) != MINUTE_NS] = np.nan
    scales = rolling_median_scale(returns)
    return times, closes, returns, scales


class PatternCountForecaster:
    """Blend a fixed historical cache with causally matured live observations."""

    def __init__(
        self,
        history_cache: Path = DEFAULT_CACHE,
        live_state: Path = DEFAULT_LIVE_STATE,
        *,
        historical_effective_cap: float = 1000.0,
        live_weight: float = 4.0,
        prior_count: float = 20.0,
    ) -> None:
        self.history_cache = history_cache
        self.live_state_path = live_state
        self.historical_effective_cap = max(1.0, historical_effective_cap)
        self.live_weight = max(0.0, live_weight)
        self.prior_count = max(0.0, prior_count)
        self.history = _open_json(history_cache)
        self.live = _open_json(live_state)
        if int(self.live.get("schema_version") or 0) != SCHEMA_VERSION:
            self.live = {"schema_version": SCHEMA_VERSION, "instruments": {}}
        self.live.setdefault("instruments", {})
        self.dirty = False

    @property
    def ready(self) -> bool:
        for source in (self.history, self.live):
            if int(source.get("schema_version") or 0) != SCHEMA_VERSION:
                continue
            for instrument in (source.get("instruments") or {}).values():
                baseline = instrument.get("baseline") or {}
                if any(safe_float(row.get("count")) > 0.0 for row in baseline.values()):
                    return True
        return False

    def metadata(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "history_cache": str(self.history_cache.resolve()),
            "history_generated_utc": str(self.history.get("generated_utc") or ""),
            "historical_instruments": len(self.history.get("instruments") or {}),
            "live_instruments": len(self.live.get("instruments") or {}),
            "historical_effective_cap": self.historical_effective_cap,
            "live_weight": self.live_weight,
            "prior_count": self.prior_count,
            "movement_coefficient_definition": "pattern expected absolute move / pair baseline absolute move at the same horizon",
        }

    def _history_instrument(self, instrument: str) -> dict[str, Any]:
        value = (self.history.get("instruments") or {}).get(instrument)
        return value if isinstance(value, dict) else {}

    def _live_instrument(self, instrument: str) -> dict[str, Any]:
        instruments = self.live.setdefault("instruments", {})
        value = instruments.setdefault(
            instrument,
            {"last_origin_ns": {}, "baseline": {}, "patterns": {}},
        )
        value.setdefault("last_origin_ns", {})
        value.setdefault("baseline", {})
        value.setdefault("patterns", {})
        return value

    def observe_candles(self, instrument: str, candles: list[dict[str, Any]], pip: float) -> int:
        times, closes, returns, scales = _candle_arrays(candles, pip)
        if len(times) < max(HORIZONS_MIN) + 8:
            return 0
        codes: dict[tuple[str, int], np.ndarray] = {}
        for mode, order in PATTERN_SPECS:
            if mode == "sign":
                codes[(mode, order)] = sign_codes(returns, order)
            else:
                codes[(mode, order)] = base_codes(magnitude_categories(returns, scales), order, 5)
        history_row = self._history_instrument(instrument)
        live_row = self._live_instrument(instrument)
        historical_end = int(safe_float(history_row.get("end_time_ns")))
        observed = 0
        for horizon in HORIZONS_MIN:
            last_origin = max(
                historical_end,
                int(safe_float(live_row["last_origin_ns"].get(str(horizon)))),
            )
            newest_origin = last_origin
            for origin in range(0, len(times) - horizon):
                origin_time = int(times[origin])
                if origin_time <= last_origin:
                    continue
                if int(times[origin + horizon] - times[origin]) != horizon * MINUTE_NS:
                    continue
                target = float((closes[origin + horizon] - closes[origin]) / pip)
                baseline = live_row["baseline"].setdefault(str(horizon), empty_stats())
                add_observation(baseline, target)
                for mode, order in PATTERN_SPECS:
                    code = int(codes[(mode, order)][origin])
                    if code < 0:
                        continue
                    key = _pattern_key(mode, order, horizon)
                    pattern_stats = live_row["patterns"].setdefault(key, {}).setdefault(str(code), empty_stats())
                    add_observation(pattern_stats, target)
                newest_origin = max(newest_origin, origin_time)
                observed += 1
            if newest_origin > last_origin:
                live_row["last_origin_ns"][str(horizon)] = newest_origin
                self.dirty = True
        return observed

    def observe_candle_sets(
        self,
        candle_sets: dict[str, dict[str, list[dict[str, Any]]]],
        pip_sizes: dict[str, float],
    ) -> int:
        observed = 0
        for instrument, granularities in candle_sets.items():
            observed += self.observe_candles(
                instrument,
                granularities.get("M1") or [],
                safe_float(pip_sizes.get(instrument), 0.01 if instrument.endswith("_JPY") else 0.0001),
            )
        if self.dirty:
            self.save()
        return observed

    def save(self) -> None:
        if not self.dirty:
            return
        self.live["schema_version"] = SCHEMA_VERSION
        self.live["updated_utc"] = utc_now()
        _atomic_json(self.live_state_path, self.live)
        self.dirty = False

    def _blend_baseline(self, historical: dict[str, Any], live: dict[str, Any]) -> dict[str, float]:
        hist_count = safe_float(historical.get("count"))
        live_count = safe_float(live.get("count"))
        hist_weight = min(hist_count, self.historical_effective_cap * 8.0)
        live_weight = live_count * self.live_weight
        total = hist_weight + live_weight
        if total <= 0.0:
            return {"probability_up": 0.5, "mean": 0.0, "mean_abs": 0.0, "effective_count": 0.0}
        return {
            "probability_up": (
                stats_rate(historical, "probability_up") * hist_weight
                + stats_rate(live, "probability_up") * live_weight
            )
            / total,
            "mean": (stats_rate(historical, "mean") * hist_weight + stats_rate(live, "mean") * live_weight)
            / total,
            "mean_abs": (
                stats_rate(historical, "mean_abs") * hist_weight
                + stats_rate(live, "mean_abs") * live_weight
            )
            / total,
            "effective_count": total,
        }

    def forecast(
        self,
        instrument: str,
        candles: list[dict[str, Any]],
        pip: float,
        *,
        mode: str,
        order: int,
        horizon_min: int,
    ) -> dict[str, Any] | None:
        if (mode, order) not in PATTERN_SPECS or horizon_min not in HORIZONS_MIN:
            return None
        history_row = self._history_instrument(instrument)
        live_row = self._live_instrument(instrument)
        if not history_row and not live_row.get("baseline"):
            return None
        times, _closes, returns, scales = _candle_arrays(candles, pip)
        if len(times) < order + 20:
            return None
        if mode == "sign":
            code = int(sign_codes(returns, order)[-1])
        else:
            code = int(base_codes(magnitude_categories(returns, scales), order, 5)[-1])
        if code < 0:
            return None
        key = _pattern_key(mode, order, horizon_min)
        historical = ((history_row.get("patterns") or {}).get(key) or {}).get(str(code)) or {}
        live = ((live_row.get("patterns") or {}).get(key) or {}).get(str(code)) or {}
        historical_baseline = (history_row.get("baseline") or {}).get(str(horizon_min)) or {}
        live_baseline = (live_row.get("baseline") or {}).get(str(horizon_min)) or {}
        baseline = self._blend_baseline(historical_baseline, live_baseline)
        hist_count = safe_float(historical.get("count"))
        live_count = safe_float(live.get("count"))
        hist_weight = min(hist_count, self.historical_effective_cap)
        live_effective = live_count * self.live_weight
        prior = self.prior_count
        effective = hist_weight + live_effective + prior
        if effective <= 0.0:
            return None
        probability_up = (
            stats_rate(historical, "probability_up") * hist_weight
            + stats_rate(live, "probability_up") * live_effective
            + baseline["probability_up"] * prior
        ) / effective
        expected_signed = (
            stats_rate(historical, "mean") * hist_weight
            + stats_rate(live, "mean") * live_effective
            + baseline["mean"] * prior
        ) / effective
        expected_abs = (
            stats_rate(historical, "mean_abs") * hist_weight
            + stats_rate(live, "mean_abs") * live_effective
            + baseline["mean_abs"] * prior
        ) / effective
        baseline_abs = max(1e-9, baseline["mean_abs"])
        coefficient = expected_abs / baseline_abs
        recent_sequence = [round(float(value), 3) for value in returns[-order:]]
        direction = "buy" if probability_up >= 0.5 else "sell"
        return {
            "model": "pattern_count_forecast",
            "pattern_mode": mode,
            "pattern_order": order,
            "pattern_code": code,
            "pattern": pattern_label(mode, order, code),
            "sequence_pips": recent_sequence,
            "decision_candle_time": format_time_ns(int(times[-1])),
            "target_horizon_sec": horizon_min * 60,
            "probability_up": round(probability_up, 6),
            "probability_down": round(1.0 - probability_up, 6),
            "predicted_direction": direction,
            "direction_edge": round(abs(probability_up - 0.5), 6),
            "expected_signed_move_pips": round(expected_signed, 4),
            "expected_abs_move_pips": round(expected_abs, 4),
            "baseline_abs_move_pips": round(baseline_abs, 4),
            "movement_coefficient": round(coefficient, 6),
            "coefficient_of_movement": round(coefficient, 6),
            "historical_pattern_count": int(hist_count),
            "live_pattern_count": int(live_count),
            "combined_pattern_count": int(hist_count + live_count),
            "effective_sample_count": round(effective, 3),
            "historical_probability_up": round(stats_rate(historical, "probability_up"), 6),
            "live_probability_up": None if live_count <= 0 else round(stats_rate(live, "probability_up"), 6),
            "historical_end_time": format_time_ns(int(safe_float(history_row.get("end_time_ns")))),
            "live_last_origin_time": format_time_ns(
                int(safe_float((live_row.get("last_origin_ns") or {}).get(str(horizon_min))))
            ),
            "data_sources": [
                source
                for source, present in (
                    ("historical_m1_cache", bool(history_row)),
                    ("completed_live_m1", safe_float(live_baseline.get("count")) > 0.0),
                )
                if present
            ],
        }


def parse_instruments(value: str) -> list[str]:
    return sorted({item.strip().upper().replace("/", "_") for item in value.split(",") if item.strip()})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candle-dir", type=Path, default=DEFAULT_CANDLE_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--instruments", type=parse_instruments, default=[])
    parser.add_argument("--tail-rows", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    if args.tail_rows < 0 or args.workers <= 0:
        raise SystemExit("--tail-rows must be non-negative and --workers must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_cache(args.candle_dir, args.output, args.instruments, args.tail_rows, args.workers)
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "instruments": len(payload["instruments"]),
                "rows": sum(int(row["row_count"]) for row in payload["instruments"].values()),
                "generated_utc": payload["generated_utc"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
