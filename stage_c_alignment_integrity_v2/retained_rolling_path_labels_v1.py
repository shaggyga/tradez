"""Retrospective, row-preserving M1 outcomes for the technical research dataset.

This module labels every supplied origin. It never selects training examples,
predictors, instruments, trades or profitable cases. Origin ``time`` is the
original completed minute START in UTC seconds. A horizon is close-to-close:
the h-minute target is at time + h*60 and its assumed maturity is target + 60.
That bar-end assumption is NOT evidence of historical receipt/publication.

States describe exact-minute temporal support. ``available`` means the entire
origin-to-target clock path is present; separate validity flags identify usable
midpoint, bid/ask endpoint and bid/ask excursion values. Missing price inputs
are never imputed. ``pending_right_edge`` means the target is at/beyond the
exclusive historical coverage boundary, not necessarily beyond the real-world
current time. An explicit closed-query boundary distinguishes missing tail
candles from targets whose query coverage is genuinely still unavailable.

Bid/ask labels match the live TechnicalStore's close-endpoint definitions.
Excursions use future minute CLOSES only, excluding the origin. They cannot
reconstruct intraminute stops, entry timing, actual fills or position management.
No slippage, financing or commissions beyond the quoted spread are modeled.

Pandas' reused exact-key index and compiled time-window rolling operations
scale linearly in rows per horizon. No dense calendar or N-by-horizon-window
array is constructed. Inputs are read-only and the module performs no I/O.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd


SCHEMA_VERSION = "rolling_m1_technical_labels_v1_20260915"
DEFAULT_HORIZONS = (5, 15, 30, 60)
OUTCOME_SCOPE = "historical_bidask_close_endpoint_proxy_no_fills_or_slippage"
AVAILABILITY_BASIS = (
    "retrospective_target_bar_end_assumption; actual receipt/publication "
    "availability is unknown and must be retained separately when available"
)
STATES = ("pending_right_edge", "missing_target", "gap_in_path", "available")
NUMERIC_OUTCOMES = (
    "return_bps", "absolute_return_bps", "direction", "long_net_bps",
    "short_net_bps", "max_favorable_long_close_bps",
    "max_adverse_long_close_bps", "max_favorable_short_close_bps",
    "max_adverse_short_close_bps",
)
FIELDS = (
    "target_bar_start_epoch", "target_bar_end_epoch", "assumed_available_epoch",
    "state", "path_expected_bars", "path_missing_bars", "midpoint_valid",
    "bidask_endpoint_valid", "bidask_excursion_valid", *NUMERIC_OUTCOMES,
)


def _horizons(horizons) -> tuple[int, ...]:
    try:
        values = tuple(horizons)
    except TypeError as exc:
        raise ValueError("positive_unique_integer_horizons_required") from exc
    if (not values or any(isinstance(h, (bool, np.bool_)) or
            not isinstance(h, (int, np.integer)) or h <= 0 or h > 525600
            for h in values) or len(set(values)) != len(values)):
        raise ValueError("positive_unique_integer_horizons_required_max_one_year")
    return tuple(int(h) for h in values)


def _normalize(data: Mapping[str, Any]) -> dict[str, np.ndarray]:
    if not isinstance(data, Mapping) or "time" not in data or "close" not in data:
        raise ValueError("time_and_close_arrays_required")
    raw = np.asarray(data["time"])
    if raw.ndim != 1 or raw.dtype.kind not in "iuf":
        raise ValueError("one_dimensional_numeric_time_required")
    if np.any(~np.isfinite(raw)) or np.any(raw < 0) or np.any(raw > 253402300799) or np.any(raw % 60):
        raise ValueError("original_aligned_utc_minute_start_required")
    times = raw.astype(np.int64)
    if np.any(np.diff(times) <= 0):
        raise ValueError("unique_sorted_minutes_required")
    values = {"time": times}
    for name in ("close", "bid_close", "ask_close"):
        array = np.asarray(data.get(name, np.full(len(times), np.nan)), dtype=np.float64)
        if array.ndim != 1 or len(array) != len(times):
            raise ValueError("candle_array_shape_mismatch:" + name)
        array = array.copy()
        array[~np.isfinite(array) | (array <= 0)] = np.nan
        values[name] = array
    crossed = values["ask_close"] < values["bid_close"]
    values["ask_close"][crossed] = np.nan
    values["bid_close"][crossed] = np.nan
    return values


def _finite(value: np.ndarray, valid: np.ndarray) -> np.ndarray:
    value = np.asarray(value, dtype=np.float64)
    value[~valid | ~np.isfinite(value)] = np.nan
    return value


def compute_outcomes(data: Mapping[str, Any], horizons=DEFAULT_HORIZONS, *, coverage_end_epoch=None) -> OrderedDict[str, np.ndarray]:
    """Return registry-ordered arrays, exactly one element per original row.

    OHLC highs/lows and volume, if present, are deliberately unused. All price
    numbers are bps of origin midpoint. Numerical outcomes are NaN when their
    required price support or exact clock path is unavailable. Metadata target
    clocks remain populated for pending rows; missing-bar counts are NaN beyond
    the coverage boundary because the unseen suffix cannot be called a gap.

    ``coverage_end_epoch`` is the exclusive aligned candle-START boundary of a
    completed historical query. The caller must establish that coverage; this
    pure function does not consult the wall clock. It must be at least the last
    supplied candle start + 60. Omission uses precisely that default boundary.
    A target below an explicit boundary is missing_target when absent, even if
    later than the final returned candle. No origin rows are filtered.
    """
    horizons = _horizons(horizons)
    values = _normalize(data)
    times, close, bid, ask = (values[name] for name in ("time", "close", "bid_close", "ask_close"))
    n = len(times)
    if coverage_end_epoch is None:
        coverage_end = int(times[-1]) + 60 if n else 0
    else:
        if (isinstance(coverage_end_epoch, (bool, np.bool_)) or
                not isinstance(coverage_end_epoch, (int, float, np.integer, np.floating)) or
                not np.isfinite(coverage_end_epoch) or coverage_end_epoch < 0 or
                coverage_end_epoch > 253402300800 or coverage_end_epoch % 60):
            raise ValueError("aligned_finite_exclusive_coverage_end_epoch_required")
        coverage_end = int(coverage_end_epoch)
        if n and coverage_end < int(times[-1]) + 60:
            raise ValueError("coverage_end_precedes_last_completed_candle")
    result: OrderedDict[str, np.ndarray] = OrderedDict()
    # Reverse and negate the clock: trailing windows become future windows.
    # Second-resolution datetime64 avoids pandas' narrower nanosecond year range.
    reverse_clock = pd.DatetimeIndex((-times[::-1]).astype("datetime64[s]"))
    counts = pd.Series(np.ones(n), index=reverse_clock)
    future_bid = pd.Series(bid[::-1], index=reverse_clock)
    future_ask = pd.Series(ask[::-1], index=reverse_clock)
    exact_index = pd.Index(times)
    indexes = np.arange(n)
    origin_mid_valid = np.isfinite(close)
    origin_quotes_valid = np.isfinite(bid) & np.isfinite(ask)

    for h in horizons:
        target = times + h * 60
        target_index = exact_index.get_indexer(target)
        target_present = target_index >= 0
        pending = target >= coverage_end
        window = pd.Timedelta(h * 60, unit="s")
        present_count = counts.rolling(window, closed="both", min_periods=1).sum().to_numpy()[::-1]
        missing = h + 1 - present_count
        missing[pending] = np.nan
        available = target_present & (missing == 0)
        state = np.full(n, "pending_right_edge", dtype=object)
        state[~pending & ~target_present] = "missing_target"
        state[target_present & ~available] = "gap_in_path"
        state[available] = "available"
        # Strict ordering/alignment imply a complete path ends h row positions
        # after its origin. Retain this independent invariant on rolling counts.
        if np.any(available & (target_index != indexes + h)):
            raise AssertionError("exact_minute_path_invariant_failed")
        safe_index = np.maximum(target_index, 0)
        endpoint_mid = close[safe_index]
        endpoint_bid, endpoint_ask = bid[safe_index], ask[safe_index]
        mid_valid = available & origin_mid_valid & np.isfinite(endpoint_mid)
        net_valid = available & origin_mid_valid & origin_quotes_valid & np.isfinite(endpoint_bid) & np.isfinite(endpoint_ask)

        # The left-closed reversed window is (origin, target] in forward time.
        # Requiring h non-null quotes also prevents partial-window extrema.
        bid_window = future_bid.rolling(window, closed="left", min_periods=h)
        ask_window = future_ask.rolling(window, closed="left", min_periods=h)
        bid_max, bid_min = (getattr(bid_window, op)().to_numpy()[::-1] for op in ("max", "min"))
        ask_max, ask_min = (getattr(ask_window, op)().to_numpy()[::-1] for op in ("max", "min"))
        excursion_valid = net_valid & np.isfinite(bid_max) & np.isfinite(bid_min) & np.isfinite(ask_max) & np.isfinite(ask_min)
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            ret = _finite((endpoint_mid / close - 1.) * 10000., mid_valid)
            outcome = {
                "target_bar_start_epoch": target,
                "target_bar_end_epoch": target + 60,
                "assumed_available_epoch": target + 60,
                "state": state,
                "path_expected_bars": np.full(n, h + 1, dtype=np.int64),
                "path_missing_bars": missing,
                "midpoint_valid": mid_valid & np.isfinite(ret),
                "bidask_endpoint_valid": net_valid,
                "bidask_excursion_valid": excursion_valid,
                "return_bps": ret,
                "absolute_return_bps": np.abs(ret),
                "direction": np.sign(ret),
                "long_net_bps": _finite((endpoint_bid - ask) / close * 10000., net_valid),
                "short_net_bps": _finite((bid - endpoint_ask) / close * 10000., net_valid),
                "max_favorable_long_close_bps": _finite((bid_max - ask) / close * 10000., excursion_valid),
                "max_adverse_long_close_bps": _finite((bid_min - ask) / close * 10000., excursion_valid),
                "max_favorable_short_close_bps": _finite((bid - ask_min) / close * 10000., excursion_valid),
                "max_adverse_short_close_bps": _finite((bid - ask_max) / close * 10000., excursion_valid),
            }
        # Extreme finite input magnitudes may overflow arithmetic; validity must
        # describe the published finite values, not merely finite source inputs.
        outcome["bidask_endpoint_valid"] = net_valid & np.isfinite(outcome["long_net_bps"]) & np.isfinite(outcome["short_net_bps"])
        outcome["bidask_excursion_valid"] = excursion_valid & np.logical_and.reduce([
            np.isfinite(outcome[field]) for field in NUMERIC_OUTCOMES if "close_bps" in field])
        for field in ("long_net_bps", "short_net_bps"):
            outcome[field][~outcome["bidask_endpoint_valid"]] = np.nan
        for field in NUMERIC_OUTCOMES:
            if "close_bps" in field:
                outcome[field][~outcome["bidask_excursion_valid"]] = np.nan
        for field in FIELDS:
            result[f"label__{h}m__{field}"] = outcome[field]
    return result


def label_registry(horizons=DEFAULT_HORIZONS) -> list[dict[str, Any]]:
    """Describe every output column in the same deterministic order as compute."""
    formulas = {
        "target_bar_start_epoch": "origin_bar_start_epoch+horizon_minutes*60",
        "target_bar_end_epoch": "target_bar_start_epoch+60",
        "assumed_available_epoch": "target_bar_end_epoch; assumption only, not historical receipt evidence",
        "state": "pending if target>=exclusive_coverage_end_epoch; else missing_target if endpoint absent; else gap_in_path if any minute absent; else available",
        "path_expected_bars": "horizon_minutes+1; includes origin and target",
        "path_missing_bars": "expected bars minus present exact minutes in [origin,target]; NaN at/beyond exclusive coverage end",
        "midpoint_valid": "exact clock path and finite positive midpoint endpoints yield finite midpoint return",
        "bidask_endpoint_valid": "exact clock path, positive origin midpoint and noncrossed positive endpoint bid/ask yield finite net outcomes",
        "bidask_excursion_valid": "valid bidask endpoints plus every future minute's bid/ask close yields finite excursions",
        "return_bps": "10000*(mid_close[target]/mid_close[origin]-1)",
        "absolute_return_bps": "abs(return_bps)",
        "direction": "sign(return_bps); -1=down, 0=exactly flat, +1=up",
        "long_net_bps": "10000*(bid_close[target]-ask_close[origin])/mid_close[origin]",
        "short_net_bps": "10000*(bid_close[origin]-ask_close[target])/mid_close[origin]",
        "max_favorable_long_close_bps": "max(10000*(bid_close[u]-ask_close[origin])/mid_close[origin]) for origin<u<=target",
        "max_adverse_long_close_bps": "min(10000*(bid_close[u]-ask_close[origin])/mid_close[origin]) for origin<u<=target",
        "max_favorable_short_close_bps": "max(10000*(bid_close[origin]-ask_close[u])/mid_close[origin]) for origin<u<=target",
        "max_adverse_short_close_bps": "min(10000*(bid_close[origin]-ask_close[u])/mid_close[origin]) for origin<u<=target",
    }
    registry = []
    for h in _horizons(horizons):
        prefix = f"label__{h}m__"
        for field in FIELDS:
            unit = "utc_epoch_seconds" if field.endswith("epoch") else "state" if field == "state" else "boolean" if field.endswith("valid") else "bars" if field.startswith("path_") else "sign" if field == "direction" else "bps"
            registry.append({
                "name": prefix + field, "schema_version": SCHEMA_VERSION,
                "role": "label" if field in NUMERIC_OUTCOMES else "label_metadata",
                "model_input": False, "horizon_minutes": h, "unit": unit,
                "formula": formulas[field], "target_clock_column": prefix + "target_bar_end_epoch",
                "availability_assumption_column": prefix + "assumed_available_epoch",
                "availability_basis": AVAILABILITY_BASIS, "outcome_scope": OUTCOME_SCOPE,
                "coverage_boundary_policy": "exclusive candle-start boundary of caller-established closed historical query when supplied; otherwise last supplied candle start+60; this module does not establish real-world query completion",
                "gap_policy": "every exact UTC minute in inclusive origin-to-target path required",
                "missing_policy": "NaN numerical outcomes; no imputation or origin-row filtering",
                "state_semantics": "clock support only; numerical validity is separately reported",
                "states": list(STATES), "future_information": True,
            })
    return registry
