"""Pure causal derived M1/M5 technical features with explicit assumed rows.

Original source rows and the frozen 216-column M1 contract are never changed.
A missing bar may use only a strictly prior actual close and bid/ask; its flat
OHLC and zero activity are assumptions. First N missing bars are filled without
examining any future endpoint. The remainder of a long gap stays absent, so a
weekend is never bridged into a continuous feature window.

Callers authenticate REQUIRED_SOURCE_BINDINGS before using this pure module.
This module performs no file/network/clock I/O and makes no compatibility claim
for existing model weights. Returned timestamps always retain original UTC.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any, Mapping

import numpy as np

import oanda_rolling_technical_features_v1 as kernel

SCHEMA = "causal_derived_technical_features_v1_20260916"
REQUIRED_SOURCE_BINDINGS = {
    "oanda_rolling_technical_features_v1.py": "074a7b4fc138ef25a1e99b503ea693e0c31bc2e51a2e8f3753aab7556a6fb657"
}
DEFAULT_MAX_CONSECUTIVE_FILL = {60: 5, 300: 3}
MAX_INPUT_ROWS = 65536
MAX_OUTPUT_ROWS = 65536
MAX_CONSECUTIVE_FILL = 64
FIELDS = kernel.INPUT_COLUMNS


def _step(bar_seconds):
    if type(bar_seconds) is not int or bar_seconds not in DEFAULT_MAX_CONSECUTIVE_FILL:
        raise ValueError("explicit_m1_or_m5_bar_seconds_required")
    return bar_seconds


def _finite(value, name):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, np.integer, np.floating)) or not math.isfinite(value):
        raise ValueError("finite_" + name + "_required")
    return float(value)


def _bound(value, ceiling, name):
    if type(value) is not int or not 0 <= value <= ceiling:
        raise ValueError("bounded_" + name + "_required")
    return value


def _validated(data, pair, pip, step):
    if not isinstance(pair, str) or not re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", pair):
        raise ValueError("explicit_pair_required")
    if not 1e-8 <= _finite(pip, "pip") <= .1:
        raise ValueError("explicit_positive_pip_required")
    if not isinstance(data, Mapping) or "time" not in data:
        raise ValueError("original_candle_arrays_required")
    raw = np.asarray(data["time"])
    if raw.ndim != 1 or raw.dtype.kind not in "iuf" or len(raw) > MAX_INPUT_ROWS:
        raise ValueError("bounded_numeric_original_time_required")
    if np.any(~np.isfinite(raw)) or np.any(raw < 0) or np.any(raw > 253402300799-step) or np.any(raw % step):
        raise ValueError("original_step_aligned_time_required")
    times = raw.astype(np.int64, copy=True)
    if np.any(np.diff(times) <= 0):
        raise ValueError("unique_sorted_original_bars_required")
    result = {"time": times}
    for name in FIELDS[1:]:
        if name not in data:
            if name not in ("bid_close", "ask_close", "volume"):
                raise ValueError("required_source_column_missing:" + name)
            values = np.full(len(times), np.nan)
        else:
            values = np.asarray(data[name], dtype=np.float64)
            if values.ndim != 1 or len(values) != len(times):
                raise ValueError("original_array_shape_mismatch:" + name)
            if np.any(np.isinf(values)):
                raise ValueError("infinite_source_value:" + name)
            values = values.copy()
        result[name] = values
    return result


def regularize(data: Mapping[str, Any], pair: str, pip: float, *, bar_seconds=60,
               as_of_epoch=None, max_consecutive_fill=None, max_output_rows=8192):
    """Return original rows plus bounded strictly-prior flat assumptions.

    ``as_of_epoch`` is a caller-supplied completed-bar eligibility cutoff, not a
    provider observation or publication timestamp. If omitted, it is the end
    of the last actual bar. Actual rows after that cutoff are refused. Leading
    gaps are never filled. An invalid prior close also prevents filling.
    """
    step = _step(bar_seconds)
    fill_bound = DEFAULT_MAX_CONSECUTIVE_FILL[step] if max_consecutive_fill is None else max_consecutive_fill
    fill_bound = _bound(fill_bound, MAX_CONSECUTIVE_FILL, "consecutive_fill")
    capacity = _bound(max_output_rows, MAX_OUTPUT_ROWS, "output_rows")
    if capacity == 0:
        raise ValueError("positive_output_rows_required")
    original = _validated(data, pair, pip, step)
    times = original["time"]
    if as_of_epoch is None:
        cutoff = float(times[-1]+step) if len(times) else None
    else:
        cutoff = _finite(as_of_epoch, "as_of_epoch")
        if not 0 <= cutoff <= 253402300799:
            raise ValueError("bounded_as_of_epoch_required")
    if len(times) and (cutoff is None or times[-1]+step > cutoff):
        raise ValueError("source_bar_incomplete_at_cutoff")
    target = int(cutoff//step)*step-step if cutoff is not None else None
    output = {name: [] for name in FIELDS}
    observed, imputed, actual_start, fill_run = [], [], [], []

    def append_actual(index):
        if len(observed) >= capacity:
            raise ValueError("regularized_output_row_bound")
        for name in FIELDS:
            output[name].append(original[name][index])
        observed.append(True); imputed.append(False)
        actual_start.append(int(times[index])); fill_run.append(0)

    def append_gap(index, through):
        previous = int(times[index])
        close = original["close"][index]
        if through <= previous or not np.isfinite(close) or close <= 0:
            return
        count = min(fill_bound, (through-previous)//step)
        if len(observed)+count > capacity:
            raise ValueError("regularized_output_row_bound")
        bid, ask = original["bid_close"][index], original["ask_close"][index]
        if not (np.isfinite(bid) and np.isfinite(ask) and 0 < bid <= ask):
            bid = ask = np.nan
        for distance in range(1, count+1):
            output["time"].append(previous+distance*step)
            for name in ("open", "high", "low", "close"):
                output[name].append(close)
            output["bid_close"].append(bid); output["ask_close"].append(ask)
            output["volume"].append(0.)
            observed.append(False); imputed.append(True)
            actual_start.append(previous); fill_run.append(distance)

    for index in range(len(times)):
        if index:
            append_gap(index-1, int(times[index])-step)
        append_actual(index)
    if len(times) and target is not None:
        append_gap(len(times)-1, target)
    arrays = {name: np.asarray(values, dtype=np.int64 if name == "time" else np.float64)
              for name, values in output.items()}
    stamps = arrays["time"]
    gap_before = np.r_[0, np.diff(stamps)//step-1].astype(np.int64) if len(stamps) else np.array([], dtype=np.int64)
    last_actual = int(times[-1]) if len(times) else None
    return {"schema": SCHEMA, "pair": pair, "bar_seconds": step, "as_of_epoch": cutoff,
            "data": arrays, "observed_mask": np.asarray(observed, dtype=bool),
            "imputed_mask": np.asarray(imputed, dtype=bool),
            "consecutive_imputed_bars": np.asarray(fill_run, dtype=np.int64),
            "last_actual_bar_start_epoch": np.asarray(actual_start, dtype=np.int64),
            "last_actual_bar_end_epoch": np.asarray(actual_start, dtype=np.int64)+step,
            "unfilled_gap_before_bars": gap_before, "gap_before_mask": gap_before > 0,
            "latest_requested_bar_start_epoch": target,
            "trailing_unfilled_bars": max(0, (target-int(stamps[-1]))//step) if len(stamps) and target is not None else 0,
            "latest_actual_bar_start_epoch": last_actual,
            "latest_actual_bar_end_epoch": last_actual+step if last_actual is not None else None,
            "latest_actual_bar_age_seconds": cutoff-last_actual-step if last_actual is not None else None,
            "max_consecutive_fill": fill_bound,
            "assumption": "strictly prior actual close flat OHLC; prior actual bid/ask when valid; activity zero assumed",
            "original_rows_replaced": False, "future_endpoint_used": False}


def feature_registry(bar_seconds=60):
    step = _step(bar_seconds)
    prefix = "m1_derived__" if step == 60 else "m5_derived__"
    result = []
    for definition in kernel.feature_registry():
        row = dict(definition)
        row.update(name=prefix+definition["name"].removeprefix("m1__"),
            aliases=[], original_kernel_feature_name=definition["name"],
            schema_version=SCHEMA, timeframe="M1_DERIVED" if step == 60 else "M5_DERIVED",
            actual_bar_seconds=step, lookback_seconds=definition["lookback_bars"]*step,
            period_semantics="Numeric periods count actual bars; one bar is %d elapsed seconds." % step,
            gap_policy="Exact %d-second support after explicit causal flat assumptions; longer gaps remain absent." % step,
            missing_policy="No numeric missing-to-zero replacement; unsupported or undefined feature values remain NaN.",
            regularization="First N missing bars use strictly prior flat assumptions; longer remainder absent.",
            availability="Actual caller publication required; original bar end is not a first-seen or publication clock.",
            original_weight_compatibility=False, source_origin="Frozen kernel numerics on explicitly derived grid; " + definition["source_origin"],
            kernel_source_sha256=REQUIRED_SOURCE_BINDINGS["oanda_rolling_technical_features_v1.py"])
        if row["family"] == "calendar":
            row["formula"] = "Original UTC bar end (original_time + actual_bar_seconds); " + definition["formula"]
            row["source_origin"] = "Original UTC completed derived bar end, never internal nominal kernel coordinates"
        result.append(row)
    return result


def schema_metadata(bar_seconds=60):
    step = _step(bar_seconds)
    registry = feature_registry(step)
    return {"schema_version": SCHEMA, "feature_count": len(registry), "actual_bar_seconds": step,
            "registry_sha256": hashlib.sha256(json.dumps(registry, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "max_lookback_bars": kernel.max_lookback_bars(),
            "max_lookback_seconds": kernel.max_lookback_bars()*step,
            "required_source_bindings": dict(REQUIRED_SOURCE_BINDINGS),
            "source_authentication_is_callers_responsibility": True,
            "input_and_output_time": "Original UTC bar START epoch seconds",
            "internal_kernel_time": "(original_time // actual_bar_seconds) * 60; never exposed as original time",
            "calendar": "Recomputed from original_time + actual_bar_seconds",
            "quality_policy": "Counts and fractions only; no arbitrary imputation-fraction eligibility cap",
            "missing_numeric_values": "NaN arrays / None latest; mathematical undefined values remain missing",
            "original_weight_compatibility": False, "research_only": True,
            "can_place_orders": False, "can_authorize": False, "can_promote": False}


def compute_features(data: Mapping[str, Any], pair: str, pip: float, **kwargs):
    """Compute the distinct derived feature family and causal window quality."""
    regularized = regularize(data, pair, pip, **kwargs)
    step = regularized["bar_seconds"]
    actual = regularized["data"]
    nominal = dict(actual, time=(actual["time"]//step)*60)
    original_features = kernel.compute_features(nominal, pair, pip)
    end = actual["time"]+step
    hour = (end % 86400)/3600.
    weekday = ((end//86400+3) % 7).astype(float)
    for label, values, period in (("hour", hour, 24.), ("weekday", weekday, 7.)):
        for operation in ("sin", "cos"):
            original_features[f"m1__utc_{label}_{operation}"] = getattr(np, operation)(2*math.pi*values/period)
    definitions = feature_registry(step)
    features = {row["name"]: original_features[row["original_kernel_feature_name"]] for row in definitions}
    times = actual["time"]
    indexes = np.arange(len(times))
    imputed_prefix = np.r_[0, np.cumsum(regularized["imputed_mask"], dtype=np.int64)]
    observed_prefix = np.r_[0, np.cumsum(regularized["observed_mask"], dtype=np.int64)]
    by_window, quality = {}, {}
    for definition in definitions:
        width = definition["lookback_bars"]
        if width not in by_window:
            left = np.searchsorted(times, times-(width-1)*step)
            observed = observed_prefix[indexes+1]-observed_prefix[left]
            imputed = imputed_prefix[indexes+1]-imputed_prefix[left]
            missing = width-observed-imputed
            by_window[width] = {"required_bars": width, "required_elapsed_seconds": width*step,
                "observed_count": observed, "imputed_count": imputed,
                "missing_grid_count": missing, "imputed_fraction": imputed.astype(float)/width,
                "fraction_denominator": "full required feature window, including missing/leading bars",
                "complete_grid_support": missing == 0, "gap_mask": missing > 0}
        quality[definition["name"]] = by_window[width]
    return {**regularized, "metadata": schema_metadata(step), "features": features,
            "feature_quality": quality, "numeric_features_retain_undefined": True}


def latest_features(data: Mapping[str, Any], pair: str, pip: float, **kwargs):
    """JSON-compatible latest view with actual ages and per-feature provenance.

    Latest values prefer the exact current kernel value.  If a sparse pair lacks
    current support for a long-window feature, publish a causal fallback instead
    of leaving the live all-pair matrix structurally missing: first the last
    prior finite value of the same feature, then the nearest shorter feature in
    the same named family.  Fallbacks are recorded separately and do not change
    the per-feature support masks, so consumers can distinguish exact support
    from continuity values.
    """
    neutralize_undefined_shape = bool(kwargs.pop("neutralize_undefined_shape_fallbacks", False))
    result = compute_features(data, pair, pip, **kwargs)
    times = result["data"]["time"]

    def latest_value(array):
        return float(array[-1]) if len(array) and np.isfinite(array[-1]) else None

    values = {name: latest_value(array) for name, array in result["features"].items()}
    fallbacks = {}

    def last_prior_finite(name, array):
        if len(array) <= 1:
            return None
        finite = np.where(np.isfinite(array[:-1]))[0]
        if not len(finite):
            return None
        index = int(finite[-1])
        return float(array[index]), len(array)-1-index

    def lacks_grid_support(name):
        detail = result["feature_quality"].get(name)
        if not detail:
            return False
        missing = detail.get("missing_grid_count")
        if isinstance(missing, np.ndarray):
            return bool(len(missing) and missing[-1] > 0)
        return bool(missing)

    for name, array in result["features"].items():
        if values[name] is not None or not lacks_grid_support(name):
            continue
        prior = last_prior_finite(name, array)
        if prior is not None:
            values[name] = prior[0]
            fallbacks[name] = {"method": "last_prior_finite", "source_feature": name,
                               "age_bars": prior[1], "age_seconds": prior[1]*result["bar_seconds"]}

    period_pattern = re.compile(r"_(\d+)(?=_(?:pips|bps)$|$)")
    periods = (200, 120, 100, 60, 50, 30, 21, 20, 15, 14, 13, 12, 8, 5, 3, 2, 1)

    def shorter_candidates(name):
        matches = list(period_pattern.finditer(name))
        if not matches:
            return []
        match = matches[-1]
        period = int(match.group(1))
        result_names = []
        for candidate in periods:
            if candidate >= period:
                continue
            candidate_name = name[:match.start(1)] + str(candidate) + name[match.end(1):]
            if candidate_name in values:
                result_names.append(candidate_name)
        return result_names

    for name in list(values):
        if values[name] is not None or not lacks_grid_support(name):
            continue
        for candidate in shorter_candidates(name):
            candidate_value = values.get(candidate)
            if candidate_value is not None and math.isfinite(candidate_value):
                values[name] = float(candidate_value)
                fallbacks[name] = {"method": "nearest_shorter_same_family", "source_feature": candidate}
                break

    if len(times):
        for name in list(values):
            if values[name] is not None:
                continue
            if name.startswith(("m1_derived__tick_activity_ratio_", "m5_derived__tick_activity_ratio_")):
                values[name] = 1.0
                fallbacks[name] = {"method": "neutral_activity_ratio_for_undefined_baseline", "neutral_value": 1.0}
            elif name.startswith(("m1_derived__tick_activity_z_", "m5_derived__tick_activity_z_",
                                  "m1_derived__spread_z_prior_", "m5_derived__spread_z_prior_")):
                values[name] = 0.0
                fallbacks[name] = {"method": "neutral_z_score_for_undefined_baseline", "neutral_value": 0.0}
            elif neutralize_undefined_shape and name.startswith((
                "m1_derived__body_to_range", "m5_derived__body_to_range",
                "m1_derived__lower_wick_fraction", "m5_derived__lower_wick_fraction",
                "m1_derived__upper_wick_fraction", "m5_derived__upper_wick_fraction",
            )):
                values[name] = 0.0
                fallbacks[name] = {"method": "neutral_candle_shape_for_flat_or_undefined_range", "neutral_value": 0.0}

    quality = {}
    for name, detail in result["feature_quality"].items():
        quality[name] = {key: (item[-1].item() if len(item) else None) if isinstance(item, np.ndarray) else item
                         for key, item in detail.items()}
    return {"schema": SCHEMA, "pair": pair, "bar_seconds": result["bar_seconds"],
            "as_of_epoch": result["as_of_epoch"], "metadata": result["metadata"],
            "bar_start_epoch": int(times[-1]) if len(times) else None,
            "bar_end_epoch": int(times[-1])+result["bar_seconds"] if len(times) else None,
            "latest_actual_bar_start_epoch": result["latest_actual_bar_start_epoch"],
            "latest_actual_bar_end_epoch": result["latest_actual_bar_end_epoch"],
            "latest_actual_bar_age_seconds": result["latest_actual_bar_age_seconds"],
            "latest_requested_bar_start_epoch": result["latest_requested_bar_start_epoch"],
            "trailing_unfilled_bars": result["trailing_unfilled_bars"],
            "observed": bool(result["observed_mask"][-1]) if len(times) else None,
            "imputed": bool(result["imputed_mask"][-1]) if len(times) else None,
            "consecutive_imputed_bars": int(result["consecutive_imputed_bars"][-1]) if len(times) else None,
            "max_consecutive_fill": result["max_consecutive_fill"],
            "values": values, "value_fallbacks": fallbacks,
            "feature_quality": quality, "original_rows_replaced": False, "future_endpoint_used": False,
            "publication_epoch": None, "publication_must_be_recorded_by_caller": True,
            "quote_age_is_separate_and_not_inferred": True}
