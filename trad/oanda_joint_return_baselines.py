"""Pure fixed-universe, direct-horizon multivariate return baselines.

These are regularized lag regressions (VAR-style predictors), not a recursive
VAR system, cointegration/VECM, a regime model, or an execution strategy. Nothing
in this module reads files, imports project workers, starts a runtime, or trades.
Caller-supplied availability clocks are checked but never independently attested.
"""

from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
from typing import Any

import numpy as np


CONTRACT_ID = "joint_return_direct_horizon_baselines_v1_20260906"
PAIRS = ("EUR_USD", "GBP_USD", "AUD_USD", "NZD_USD", "USD_JPY", "USD_CHF", "USD_CAD")
TARGET_PAIR = "EUR_USD"
PEERS = PAIRS[1:]
ORIENTATION = {pair: (1 if pair.endswith("_USD") else -1) for pair in PAIRS}
ARMS = ("own_price_lags", "own_and_peer_lags", "own_and_peer_usd_factor")
MAX_INPUT_MINUTES = 20_000
DEFAULT_LAG_MINUTES = (1, 5, 15, 60)


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field}: finite numeric value required")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{field}: finite numeric value required") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field}: finite numeric value required")
    return number


def _minute(value: Any, field: str) -> int:
    number = _finite(value, field)
    if number % 60 != 0:
        raise ValueError(f"{field}: UTC minute boundary required")
    return int(number)


def _integer(value: Any, field: str, *, minimum: int = 1, maximum: int = 20_000) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field}: integer in [{minimum}, {maximum}] required")
    return value


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode("utf-8")).hexdigest()


def _parameters(horizon_sec, lag_minutes, alpha, min_training_rows, max_training_rows) -> dict:
    horizon = _integer(horizon_sec, "horizon_sec", minimum=60, maximum=86_400)
    if horizon % 60:
        raise ValueError("horizon_sec: whole minutes required")
    if not isinstance(lag_minutes, (tuple, list)) or not 1 <= len(lag_minutes) <= 8:
        raise ValueError("lag_minutes: one to eight fixed lookbacks required")
    lags = [_integer(value, "lag_minutes", maximum=240) for value in lag_minutes]
    if lags != sorted(set(lags)):
        raise ValueError("lag_minutes: strictly increasing unique lookbacks required")
    penalty = _finite(alpha, "alpha")
    if not 1e-6 <= penalty <= 1e6:
        raise ValueError("alpha: ridge penalty in [1e-6, 1e6] required")
    minimum = _integer(min_training_rows, "min_training_rows", minimum=2)
    maximum = _integer(max_training_rows, "max_training_rows", minimum=minimum)
    return {"horizon_sec": horizon, "lag_minutes": lags, "alpha": penalty,
            "min_training_rows": minimum, "max_training_rows": maximum,
            "training_stride_minutes": 1,
            "features": "trailing_compounded_log_returns_in_basis_points",
            "regression": "ridge_direct_target_return_no_recursive_VAR",
            "scaling": "training_rows_only_population_mean_and_std_per_arm",
            "regularization": "sum_squared_error_plus_alpha_times_squared_standardized_coefficients",
            "intercept": "fitted_unpenalized", "training_subset": "most_recent_valid_rows",
            "selection": "caller_fixed_parameters_no_internal_search"}


def _parse_bars(bars: list[dict], decision: float, reference: int) -> tuple[list[int], np.ndarray, np.ndarray, str]:
    if not isinstance(bars, list):
        raise ValueError("bars: list of explicitly observed minute-bar dictionaries required")
    if len(bars) > MAX_INPUT_MINUTES * len(PAIRS):
        raise ValueError("bars: bounded input limit exceeded")
    grouped: dict[int, dict[str, tuple[float, float]]] = {}
    for row in bars:
        if not isinstance(row, dict):
            raise ValueError("bar: dictionary required")
        pair = row.get("instrument")
        if not isinstance(pair, str) or pair not in PAIRS:
            raise ValueError("bar instrument: exact fixed seven-pair universe required")
        start = _minute(row.get("start_epoch"), "start_epoch")
        end = _minute(row.get("end_epoch"), "end_epoch")
        available = _finite(row.get("available_epoch"), "available_epoch")
        price = _finite(row.get("close_mid"), "close_mid")
        if end != start + 60:
            raise ValueError("bar: exact completed sixty-second interval required")
        if price <= 0:
            raise ValueError("close_mid: strictly positive price required")
        if available < end:
            raise ValueError("bar availability precedes its completed interval")
        if end > reference:
            raise ValueError("bar after original reference cannot enter this forecast")
        if available >= decision:
            raise ValueError("bar must be actually available strictly before decision")
        if row.get("complete", True) is not True:
            raise ValueError("bar: incomplete interval rejected")
        minute_rows = grouped.setdefault(end, {})
        if pair in minute_rows:
            raise ValueError("duplicate instrument-minute rejected, including identical copies")
        minute_rows[pair] = (price, available)
    for minute_rows in grouped.values():
        if set(minute_rows) != set(PAIRS):
            raise ValueError("partial cross-pair minute rejected; every retained minute needs all seven pairs")
    times = sorted(grouped)
    mids = np.asarray([[grouped[stamp][pair][0] for pair in PAIRS] for stamp in times], dtype=float).reshape(-1, len(PAIRS))
    available = np.asarray([[grouped[stamp][pair][1] for pair in PAIRS] for stamp in times], dtype=float).reshape(-1, len(PAIRS))
    canonical = [[stamp, [[pair, *grouped[stamp][pair]] for pair in PAIRS]] for stamp in times]
    return times, mids, available, _hash(canonical)


def _feature_rows(log_prices: np.ndarray, index: int, lags: list[int]) -> dict[str, np.ndarray]:
    # Shared non-USD strength orientation makes USD/JPY comparable with EUR/USD.
    signs = np.asarray([ORIENTATION[pair] for pair in PAIRS], dtype=float)
    returns = np.asarray([(log_prices[index] - log_prices[index-lag]) * 10_000.0 * signs
                          for lag in lags], dtype=float)
    own = returns[:, 0]
    peers = returns[:, 1:]
    factor = np.mean(peers, axis=1)  # No EUR/USD value enters this factor.
    return {"own_price_lags": own,
            "own_and_peer_lags": np.r_[own, peers.T.reshape(-1)],
            "own_and_peer_usd_factor": np.r_[own, factor]}


def _feature_names(lags: list[int]) -> dict[str, list[str]]:
    own = [f"EUR_USD_trailing_{lag}m_log_bps" for lag in lags]
    peers = [f"{pair}_non_USD_strength_trailing_{lag}m_log_bps" for pair in PEERS for lag in lags]
    factor = [f"leave_EUR_USD_out_peer_mean_trailing_{lag}m_log_bps" for lag in lags]
    return {"own_price_lags": own, "own_and_peer_lags": own + peers,
            "own_and_peer_usd_factor": own + factor}


def _fit_arm(x: np.ndarray, y: np.ndarray, current: np.ndarray, alpha: float, names: list[str]) -> dict:
    mean = np.mean(x, axis=0)
    scale = np.std(x, axis=0, ddof=0)
    # A constant feature conveys no training variation. Keep it at zero in both
    # fit and prediction, even if the current observation later changes it.
    constant = scale <= 1e-12
    scale[constant] = 1.0
    z = (x - mean) / scale
    current_z = (current - mean) / scale
    z[:, constant] = 0.0
    current_z[constant] = 0.0
    design = np.column_stack((np.ones(len(z)), z))
    penalty = np.eye(design.shape[1]) * alpha
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    prediction = float(np.r_[1.0, current_z] @ beta)
    if not (np.isfinite(beta).all() and math.isfinite(prediction)):
        raise ValueError("nonfinite_regression_result")
    return {"status": "predicted", "predicted_return_bps": prediction,
            "side": 1 if prediction > 0 else -1 if prediction < 0 else 0,
            "feature_names": names, "training_scaler_mean": mean.tolist(),
            "training_scaler_scale": scale.tolist(),
            "constant_training_feature_names": [name for name, flag in zip(names, constant) if flag],
            "intercept_bps": float(beta[0]), "standardized_coefficients_bps": beta[1:].tolist()}


def fit_joint_return_baselines(
    bars: list[dict], *, decision_epoch: float, reference_epoch: float,
    horizon_sec: int = 3600, lag_minutes: tuple[int, ...] = DEFAULT_LAG_MINUTES,
    alpha: float = 10.0, min_training_rows: int = 120, max_training_rows: int = 4096,
) -> dict:
    """Produce three frozen direct-horizon forecasts from synchronized past bars.

    Each bar requires instrument, start_epoch, end_epoch, close_mid, and an actual
    caller-observed available_epoch. UTC minute intervals must be exactly sixty
    seconds; prices are positive finite midpoints. Every retained minute must
    contain exactly the seven PAIRS; duplicates, partial minutes, unknown pairs,
    post-reference bars, and unavailable-at-decision rows fail closed with
    ValueError. Unsorted valid input is accepted deterministically. Missing whole
    minutes are retained as gaps, never filled or treated as zero returns.

    Lag k means the compounded trailing k-minute log return, not a shifted
    one-minute return. All feature and label intervals must be gap-free. The
    target is fixed at reference_epoch + horizon_sec, never decision time plus
    horizon; a decision at/after target is invalid. Training labels end at their
    original exact horizon and both maturity and actual label availability must
    precede decision strictly. No label is inferred from a later nearest quote.

    A fixed most-recent subset trains all three arms on identical rows. Scalers
    and coefficients use only that subset; the current feature row is excluded
    from fitting. No hyperparameter search occurs. Insufficient usable history
    returns an explicit all-arm abstention. Prices are normalized as log basis
    points, with USD-first pairs inverted before forming peer features and a
    simple mean factor that excludes EUR/USD. The output forecasts EUR/USD log
    return in basis points; it does not provide calibrated probabilities or P/L.

    Real availability cannot be reconstructed from historical candle timestamps.
    Callers must not invent it for retrospective performance. This pure helper
    also cannot attest the wall time at which fitting/issue/commit occurred.
    Every result remains offline, unregistered, and ineligible for proof/trading.
    """
    decision = _finite(decision_epoch, "decision_epoch")
    reference = _minute(reference_epoch, "reference_epoch")
    parameters = _parameters(horizon_sec, lag_minutes, alpha, min_training_rows, max_training_rows)
    target = reference + parameters["horizon_sec"]
    if not reference <= decision < target:
        raise ValueError("require original reference <= decision < unchanged original target")
    times, mids, available, input_hash = _parse_bars(bars, decision, reference)
    names = _feature_names(parameters["lag_minutes"])
    result = {
        "contract_id": CONTRACT_ID, "status": "abstain", "reasons": [],
        "instrument": TARGET_PAIR, "pairs": list(PAIRS),
        "decision_epoch": decision, "reference_epoch": reference, "target_epoch": target,
        "parameters": parameters, "parameters_sha256": _hash(parameters),
        "input_sha256": input_hash, "common_minute_count": len(times),
        "availability_evidence": "caller_observed_clocks_not_independently_attested",
        "issue_and_commit_clocks_attested": False,
        "orientation": "positive_is_non_USD_strength_against_USD",
        "pair_orientation_signs": ORIENTATION.copy(),
        "peer_factor_components": {pair: ORIENTATION[pair] / len(PEERS) for pair in PEERS},
        "factor_excludes_target_pair": True,
        "research_only": True, "collection_enabled": False, "can_place_orders": False,
        "can_promote": False, "can_authorize": False, "account_eligible": False,
        "proof_eligible": False, "independent_sample_count": None,
        "overlapping_training_labels": parameters["horizon_sec"] > 60,
        "eligible_training_rows": 0, "selected_training_rows": 0,
        "training_rows": [], "training_exclusion_counts": {},
        "arms": {arm: {"status": "abstain", "predicted_return_bps": None, "side": 0,
                       "feature_names": names[arm]} for arm in ARMS},
    }
    if not times or times[-1] != reference:
        result["reasons"] = ["missing_synchronized_reference_bar"]
        return result
    lookback = max(parameters["lag_minutes"])
    segments = np.r_[0, np.cumsum(np.diff(times) != 60)]
    result["whole_minute_gap_count"] = int(np.sum(np.diff(times) != 60))
    if len(times) <= lookback or segments[-1] != segments[-1-lookback]:
        result["reasons"] = ["insufficient_contiguous_current_feature_history"]
        return result
    lookup = {stamp: index for index, stamp in enumerate(times)}
    log_prices = np.log(mids)
    eligible = []
    exclusions = Counter()
    for index, stamp in enumerate(times):
        if index < lookback or segments[index] != segments[index-lookback]:
            exclusions["insufficient_or_gapped_feature_history"] += 1
            continue
        maturity = stamp + parameters["horizon_sec"]
        target_index = lookup.get(maturity)
        if target_index is None:
            exclusions["missing_exact_training_target"] += 1
            continue
        if segments[index] != segments[target_index]:
            exclusions["gap_in_training_label_interval"] += 1
            continue
        label_available = float(max(available[index, 0], available[target_index, 0]))
        # Explicit even though the strict input checks normally imply these.
        if maturity >= decision or label_available >= decision:
            exclusions["training_label_not_known_before_decision"] += 1
            continue
        features_available = float(np.max(available[index-lookback:index+1]))
        if features_available >= decision:
            exclusions["training_features_not_known_before_decision"] += 1
            continue
        eligible.append((index, target_index, label_available, features_available))
    selected = eligible[-parameters["max_training_rows"]:]
    result.update(
        eligible_training_rows=len(eligible), selected_training_rows=len(selected),
        training_exclusion_counts=dict(sorted(exclusions.items())),
        current_features_available_epoch=float(np.max(available[-lookback-1:])),
        all_supplied_data_available_max_epoch=float(np.max(available)),
        training_rows=[{"reference_epoch": times[index], "target_epoch": times[target_index],
                        "label_available_epoch": label_available,
                        "features_available_epoch": features_available}
                       for index, target_index, label_available, features_available in selected],
        training_label_maturity_max_epoch=max((times[row[1]] for row in selected), default=None),
        training_labels_available_max_epoch=max((row[2] for row in selected), default=None),
        training_features_available_max_epoch=max((row[3] for row in selected), default=None),
    )
    if len(selected) < parameters["min_training_rows"]:
        result["reasons"] = ["insufficient_mature_available_training_rows"]
        return result
    current = _feature_rows(log_prices, len(times)-1, parameters["lag_minutes"])
    matrices = {arm: [] for arm in ARMS}
    labels = []
    for index, target_index, _, _ in selected:
        row = _feature_rows(log_prices, index, parameters["lag_minutes"])
        for arm in ARMS:
            matrices[arm].append(row[arm])
        labels.append(float((log_prices[target_index, 0] - log_prices[index, 0]) * 10_000.0))
    y = np.asarray(labels)
    try:
        fitted = {arm: _fit_arm(np.asarray(matrices[arm]), y, current[arm], parameters["alpha"], names[arm])
                  for arm in ARMS}
    except (np.linalg.LinAlgError, ValueError, FloatingPointError):
        result["reasons"] = ["numeric_regression_failure"]
        return result
    result.update(status="predicted", arms=fitted,
                  current_feature_values={arm: current[arm].tolist() for arm in ARMS})
    return result
