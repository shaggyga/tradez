"""Pure per-pair ridge/EWMA research models; no peer data dependency.

Input keys and cutoff are actual UTC M1 START epochs. A close at key t is
complete at t+60. The caller verifies actual observation and later issuance;
this module never equates a market timestamp with availability. Missing bars
are never filled or compressed. Existing seven-pair study sources are untouched.

No files, databases, broker, worker, calendar, registration or execution code
is imported or invoked. No forecast is published and no order is authorized.
"""
from collections.abc import Mapping
import math
import re
from types import MappingProxyType

import numpy as np


MODEL_VERSION = "pair_local_ridge_ewma_v1_20260907"
FAMILIES = ("ridge_return_repaired", "probabilistic_state_space")
FEATURE_WINDOWS = (1, 3, 5, 15, 30, 60)
PARAMETERS = MappingProxyType({
    "horizon_sec": 3600,
    "maximum_real_rows": 1024, "current_pair_closes": 61,
    "feature_windows_minutes": FEATURE_WINDOWS,
    "ridge_alpha": 10., "ridge_minimum_rows": 24, "ridge_stride_minutes": 3,
    "state_alpha": .08, "state_maximum_prices": 256, "state_minimum_returns": 60,
    "sampling_phase": "epoch_minute_modulo_3_equals_zero",
    "training_gap_policy": "all 121 feature_and_target_prices_must_exist",
})


def _epoch(value, name):
    if (type(value) not in (int, float) or not 0 < value < 32503680000
            or not math.isfinite(value) or value % 60 != 0):
        raise ValueError(name + ": positive exact UTC minute START required")
    return int(value)


def validate_identity(instrument, pip_size):
    if (type(instrument) is not str or not re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", instrument)
            or instrument[:3] == instrument[4:]):
        raise ValueError("valid_distinct_currency_pair_required")
    if (type(pip_size) not in (int, float) or pip_size not in (.0001, .001, .01)
            or not math.isfinite(pip_size)):
        raise ValueError("bounded_positive_finite_pip_size_required")
    return instrument, float(pip_size)


def _validated(rows_by_epoch, cutoff_epoch, instrument):
    cutoff = _epoch(cutoff_epoch, "cutoff_epoch")
    if not isinstance(rows_by_epoch, Mapping) or not 1 <= len(rows_by_epoch) <= 1024:
        raise ValueError("bounded_nonempty_pair_real_rows_required")
    rows = {}
    for raw_epoch, value in rows_by_epoch.items():
        epoch = _epoch(raw_epoch, instrument + ".epoch")
        if epoch > cutoff:
            raise ValueError("future_bar_after_market_cutoff")
        if type(value) not in (int, float):
            raise ValueError("positive_finite_pair_price_required")
        try:
            price = float(value)
        except (ValueError, OverflowError) as exc:
            raise ValueError("positive_finite_pair_price_required") from exc
        if not math.isfinite(price) or price <= 0:
            raise ValueError("positive_finite_pair_price_required")
        rows[epoch] = price
    if any(cutoff - lag * 60 not in rows for lag in range(61)):
        raise ValueError("current_pair_feature_window_requires_61_closes")
    return dict(sorted(rows.items())), cutoff


def _runs(epochs):
    output, previous, length = {}, None, 0
    for epoch in sorted(epochs):
        length = length + 1 if previous is not None and epoch - previous == 60 else 1
        output[epoch] = length
        previous = epoch
    return output


def _training_epochs(rows, cutoff):
    runs = _runs(rows)
    return [t for t in sorted(rows) if t // 60 % 3 == 0 and t + 3600 <= cutoff
            and runs.get(t + 3600, 0) >= 121]


def _feature(rows, epoch, pip_size):
    prices = np.asarray([rows[epoch - lag * 60] for lag in range(60, -1, -1)])
    values = [(prices[-1] - prices[-1 - window]) / pip_size for window in FEATURE_WINDOWS]
    changes = np.diff(prices) / pip_size
    values.extend(float(np.std(changes[-window:])) for window in (5, 15, 60))
    tail = prices[-20:]
    width = float(np.max(tail) - np.min(tail))
    values.append(.5 if width <= 0 else float((prices[-1] - np.min(tail)) / width))
    return np.asarray(values, dtype=float)


def _ridge(x, y, current):
    if len(y) < 24:
        raise ValueError("insufficient_ridge_training_rows")
    mean, scale = np.mean(x, axis=0), np.std(x, axis=0)
    scale[scale < 1e-9] = 1.
    design = np.column_stack([np.ones(len(x)), (x - mean) / scale])
    penalty = np.eye(design.shape[1]) * 10.
    penalty[0, 0] = 0.
    beta = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    expected = float(np.r_[1., (current - mean) / scale] @ beta)
    sigma = max(1e-6, float(np.std(y - design @ beta)))
    return expected, sigma


def _probability(expected, sigma):
    return min(.999, max(.001, .5 * (1 + math.erf(expected / max(1e-9, sigma) / math.sqrt(2)))))


def _diagnostics(model, rows, cutoff, training_epochs, instrument, pip_size):
    return {"model_version": MODEL_VERSION, "model": model, "instrument": instrument, "pip_size": pip_size,
        "feature_cutoff_epoch": cutoff + 60, "training_rows": len(training_epochs),
        "training_row_start_epochs_by_pair": {instrument: training_epochs} if training_epochs else {},
        "training_label_maturity_max_epoch": max((t + 3660 for t in training_epochs), default=None),
        "exact_target_offset_sec": 3600, "retained_real_rows": len(rows),
        "epoch_semantics": "UTC_M1_start; price_close_and_label_maturity=start+60",
        "sampling_phase": PARAMETERS["sampling_phase"], "gap_policy": PARAMETERS["training_gap_policy"],
        "input_scope": "single_pair_only_no_peer_inputs", "research_only": True,
        "can_place_orders": False, "can_authorize": False, "can_promote": False,
        "account_eligible": False, "proof_eligible": False}


def _predict(rows, cutoff, instrument, pip_size):
    output = {}
    epochs = _training_epochs(rows, cutoff)
    if len(epochs) >= 24:
        x = np.asarray([_feature(rows, epoch, pip_size) for epoch in epochs])
        y = np.asarray([(rows[epoch + 3600] - rows[epoch]) / pip_size for epoch in epochs])
        expected, sigma = _ridge(x, y, _feature(rows, cutoff, pip_size))
        diagnostics = _diagnostics("per_pair_standardized_ridge_timestamp_v1", rows, cutoff, epochs, instrument, pip_size)
        diagnostics.update(alpha=10., stride_minutes=3, residual_sigma_pips=sigma)
        output[FAMILIES[0]] = (expected, _probability(expected, sigma), diagnostics)

    length = min(256, _runs(rows)[cutoff])
    prices = np.asarray([rows[cutoff - lag * 60] for lag in range(length - 1, -1, -1)])
    changes = np.diff(prices) / pip_size
    if len(changes) >= 60:
        trend, variance = 0., 1.
        for value in changes:
            innovation = float(value) - trend
            trend += .08 * innovation
            variance = .92 * variance + .08 * innovation * innovation
        sigma = max(1e-6, math.sqrt(variance * 60))
        expected = max(-3 * sigma, min(3 * sigma, trend * 60))
        diagnostics = _diagnostics("local_level_drift_ewma_contiguous_segment_v1", rows, cutoff, [], instrument, pip_size)
        diagnostics.update(observations=len(changes), alpha=.08,
            state_segment_start_epoch=cutoff - (length - 1) * 60,
            state_reset_policy="restart_at_latest_gap; at_most_256_consecutive_prices",
            state_trend_pips_per_minute=trend, forecast_sigma_pips=sigma)
        output[FAMILIES[1]] = (expected, _probability(expected, sigma), diagnostics)
    return output


def predict_all(rows_by_epoch, cutoff_epoch, *, instrument, pip_size):
    """Return available single-pair family tuples; caller requires both or abstains.

    The direct pair map has <=1024 actual UTC minute START keys and positive
    finite close prices. cutoff_epoch must be its latest key. Sixty complete
    current return intervals and 24 mature ridge labels are still required.
    Valid Friday training segments are retained without bridging weekend gaps.

    The caller must validate instrument/source/duplicate identities before
    constructing this map, attest actual capture/completion clocks, and keep
    the original separately published H1 target. This helper cannot observe
    availability, enforce issuance, publish a forecast or authorize an order.
    """
    instrument, pip_size = validate_identity(instrument, pip_size)
    rows, cutoff = _validated(rows_by_epoch, cutoff_epoch, instrument)
    try:
        with np.errstate(over="raise", divide="raise", invalid="raise"):
            output = _predict(rows, cutoff, instrument, pip_size)
    except (FloatingPointError, np.linalg.LinAlgError) as exc:
        raise ValueError("numerical_failure_requires_abstention") from exc
    for expected, probability, _ in output.values():
        if not math.isfinite(expected) or not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("nonfinite_prediction_requires_abstention")
    return output
