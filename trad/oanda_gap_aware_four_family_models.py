"""Pure timestamp-addressed revision of the four EURUSD research models.

Rows use actual UTC M1 START epochs. A price/label at t matures at t+60.
Missing minutes are never filled, compressed or treated as flat returns.
Training uses valid historical segments, including retained Friday segments;
only current features require 61 consecutive shared closes (60 intervals).

This module does not read archives, observe availability, issue forecasts,
persist weights, register cohorts, open databases or authorize execution. The
caller must independently capture inputs before fitting and bind a later
completion/issue/publication clock; cutoff_epoch is a market clock, not an
availability claim. Existing registered source files remain untouched.
"""
from collections.abc import Mapping
import math
from types import MappingProxyType

import numpy as np


MODEL_VERSION = "timestamp_gap_aware_four_family_models_v1_20260907"
PAIRS = ("EUR_USD", "GBP_USD", "AUD_USD", "NZD_USD", "USD_JPY", "USD_CHF", "USD_CAD")
FAMILIES = ("ridge_return_repaired", "modern_tabular_probabilistic_repaired",
            "cross_pair_graph_transfer", "probabilistic_state_space")
FEATURE_WINDOWS = (1, 3, 5, 15, 30, 60)
PARAMETERS = MappingProxyType({
    "horizon_sec": 3600, "maximum_real_rows_per_pair": 1024,
    "current_common_closes": 61, "feature_windows_minutes": FEATURE_WINDOWS,
    "ridge_alpha": 10.0, "ridge_minimum_rows": 24, "ridge_stride_minutes": 3,
    "pooled_stride_minutes": 5, "pooled_minimum_rows_per_pair": 24,
    "pooled_minimum_rows": 300, "hgb_max_iter": 40, "hgb_max_leaf_nodes": 15,
    "hgb_min_samples_leaf": 40, "hgb_learning_rate": 0.06,
    "hgb_l2_regularization": 1.0, "hgb_random_state": 20260806,
    "graph_alpha": 25.0, "graph_stride_minutes": 3,
    "graph_factor_windows_minutes": (1, 5, 15, 60), "graph_minimum_rows": 32,
    "state_alpha": 0.08, "state_maximum_prices": 256, "state_minimum_returns": 60,
    "sampling_phase": "epoch_minute_modulo_stride_equals_zero",
    "training_gap_policy": "all feature and 60-minute target intervals must exist",
})


def _epoch(value, name):
    if (type(value) not in (int, float) or not 0 < value < 32503680000
            or not math.isfinite(value) or value % 60 != 0):
        raise ValueError(name + ": positive exact UTC minute START required")
    return int(value)


def _validated(rows_by_pair, cutoff_epoch):
    cutoff = _epoch(cutoff_epoch, "cutoff_epoch")
    if not isinstance(rows_by_pair, Mapping) or set(rows_by_pair) != set(PAIRS):
        raise ValueError("exact_seven_pair_maps_required")
    rows = {}
    for pair in PAIRS:
        source = rows_by_pair[pair]
        if not isinstance(source, Mapping) or not 1 <= len(source) <= 1024:
            raise ValueError("bounded_nonempty_real_rows_required:" + pair)
        clean = {}
        for raw_epoch, value in source.items():
            epoch = _epoch(raw_epoch, pair + ".epoch")
            if epoch > cutoff:
                raise ValueError("future_bar_after_market_cutoff:" + pair)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("positive_finite_price_required:" + pair)
            clean[epoch] = float(value)
        rows[pair] = dict(sorted(clean.items()))
    if any(any(cutoff - lag * 60 not in rows[pair] for lag in range(61)) for pair in PAIRS):
        raise ValueError("current_common_feature_window_requires_61_closes")
    return rows, cutoff


def _runs(epochs):
    result, previous, length = {}, None, 0
    for epoch in sorted(epochs):
        length = length + 1 if previous is not None and epoch - previous == 60 else 1
        result[epoch] = length
        previous = epoch
    return result


class _Data:
    def __init__(self, rows, cutoff):
        self.rows, self.cutoff = rows, cutoff
        self.runs = {pair: _runs(rows[pair]) for pair in PAIRS}
        self.common_runs = _runs(set.intersection(*(set(rows[pair]) for pair in PAIRS)))
        self.pips = {pair: .01 if pair.endswith("_JPY") else .0001 for pair in PAIRS}
        self.features = {}
        self.currencies = tuple(sorted({currency for pair in PAIRS for currency in pair.split("_")}))
        self.positions = {currency: index for index, currency in enumerate(self.currencies)}
        self.factors = {}

    def training_epochs(self, pair, stride):
        # A full 121-price segment covers both feature history and exact H1
        # target path. A deleted interior minute invalidates affected rows.
        return [t for t in self.rows[pair] if t // 60 % stride == 0
                and t + 3600 <= self.cutoff and self.runs[pair].get(t + 3600, 0) >= 121]

    def feature(self, pair, epoch):
        key = (pair, epoch)
        if key not in self.features:
            prices = np.asarray([self.rows[pair][epoch - lag * 60] for lag in range(60, -1, -1)])
            pip = self.pips[pair]
            values = [(prices[-1] - prices[-1 - window]) / pip for window in FEATURE_WINDOWS]
            changes = np.diff(prices) / pip
            values.extend(float(np.std(changes[-window:])) for window in (5, 15, 60))
            tail = prices[-20:]
            width = float(np.max(tail) - np.min(tail))
            values.append(.5 if width <= 0 else float((prices[-1] - np.min(tail)) / width))
            self.features[key] = np.asarray(values, dtype=float)
        return self.features[key]

    def target(self, pair, epoch):
        return (self.rows[pair][epoch + 3600] - self.rows[pair][epoch]) / self.pips[pair]

    def current_prices(self, pair, maximum):
        length = min(maximum, self.runs[pair][self.cutoff])
        return np.asarray([self.rows[pair][self.cutoff - lag * 60] for lag in range(length - 1, -1, -1)])

    def factor(self, epoch):
        # epoch identifies the endpoint of a real one-minute return.
        if epoch not in self.factors:
            factors = np.zeros(len(self.currencies))
            counts = np.zeros(len(self.currencies))
            for pair in PAIRS:
                base, quote = pair.split("_")
                change = (self.rows[pair][epoch] - self.rows[pair][epoch - 60]) / self.pips[pair]
                factors[self.positions[base]] += change
                factors[self.positions[quote]] -= change
                counts[self.positions[base]] += 1
                counts[self.positions[quote]] += 1
            self.factors[epoch] = factors / np.maximum(1., counts)
        return self.factors[epoch]

    def graph_feature(self, epoch):
        factors = np.asarray([self.factor(epoch - lag * 60) for lag in range(59, -1, -1)])
        base, quote = self.positions["EUR"], self.positions["USD"]
        output = []
        for window in (1, 5, 15, 60):
            window_sum = np.sum(factors[-window:], axis=0)
            output.extend([window_sum[base], window_sum[quote], np.mean(np.abs(window_sum))])
        output.extend((self.rows["EUR_USD"][epoch] - self.rows["EUR_USD"][epoch - window * 60]) / .0001
                      for window in (1, 5, 15, 60))
        return np.asarray(output, dtype=float)

    def diagnostics(self, model, training_by_pair):
        return {"model_version": MODEL_VERSION, "model": model,
            "feature_cutoff_epoch": self.cutoff + 60,
            "training_rows": sum(map(len, training_by_pair.values())),
            "training_row_start_epochs_by_pair": training_by_pair,
            "training_label_maturity_max_epoch": max(
                (t + 3660 for epochs in training_by_pair.values() for t in epochs), default=None),
            "exact_target_offset_sec": 3600,
            "epoch_semantics": "UTC_M1_start; price_close_and_label_maturity=start+60",
            "gap_policy": PARAMETERS["training_gap_policy"],
            "sampling_phase": PARAMETERS["sampling_phase"],
            "retained_real_rows_by_pair": {pair: len(self.rows[pair]) for pair in PAIRS},
            "research_only": True, "can_place_orders": False, "proof_eligible": False}


def _ridge(x, y, current, alpha):
    if len(y) < max(24, x.shape[1] * 2):
        raise ValueError("insufficient_ridge_training_rows")
    mean, scale = np.mean(x, axis=0), np.std(x, axis=0)
    scale[scale < 1e-9] = 1.
    z = (x - mean) / scale
    design = np.column_stack([np.ones(len(z)), z])
    penalty = np.eye(design.shape[1]) * alpha
    penalty[0, 0] = 0.
    beta = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    prediction = float(np.r_[1., (current - mean) / scale] @ beta)
    sigma = max(1e-6, float(np.std(y - design @ beta)))
    return prediction, sigma


def _probability(expected, sigma):
    return min(.999, max(.001, .5 * (1 + math.erf(expected / max(1e-9, sigma) / math.sqrt(2)))))


def _predict(data):
    output = {}
    epochs = data.training_epochs("EUR_USD", 3)
    if len(epochs) >= 24:
        x = np.asarray([data.feature("EUR_USD", t) for t in epochs])
        y = np.asarray([data.target("EUR_USD", t) for t in epochs])
        expected, sigma = _ridge(x, y, data.feature("EUR_USD", data.cutoff), 10.)
        diagnostics = data.diagnostics("per_pair_standardized_ridge_timestamp_v1", {"EUR_USD": epochs})
        diagnostics.update(alpha=10., stride_minutes=3, residual_sigma_pips=sigma)
        output[FAMILIES[0]] = (expected, _probability(expected, sigma), diagnostics)

    pooled_epochs = {pair: data.training_epochs(pair, 5) for pair in PAIRS}
    pooled_epochs = {pair: ts for pair, ts in pooled_epochs.items() if len(ts) >= 24}
    if "EUR_USD" in pooled_epochs and sum(map(len, pooled_epochs.values())) >= 300:
        from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
        features, targets, scales = [], [], {}
        for pair, times in pooled_epochs.items():
            volatility = max(1., float(np.std(np.diff(data.current_prices(pair, 120)) / data.pips[pair])) * math.sqrt(60))
            scales[pair] = volatility
            features.extend(data.feature(pair, t) / volatility for t in times)
            targets.extend(data.target(pair, t) / volatility for t in times)
        x, y = np.asarray(features), np.asarray(targets)
        if len(np.unique(y > 0)) >= 2:
            kwargs = dict(max_iter=40, max_leaf_nodes=15, min_samples_leaf=40,
                          learning_rate=.06, l2_regularization=1., random_state=20260806)
            regressor = HistGradientBoostingRegressor(**kwargs).fit(x, y)
            classifier = HistGradientBoostingClassifier(**kwargs).fit(x, y > 0)
            current = (data.feature("EUR_USD", data.cutoff) / scales["EUR_USD"]).reshape(1, -1)
            expected = float(regressor.predict(current)[0]) * scales["EUR_USD"]
            probability = float(classifier.predict_proba(current)[0, 1])
            diagnostics = data.diagnostics("pooled_hist_gradient_boosting_timestamp_v1", pooled_epochs)
            diagnostics.update(pooled_training_rows=len(y), stride_minutes=5,
                               current_normalization_scale_by_pair=scales, **kwargs)
            output[FAMILIES[1]] = (expected, probability, diagnostics)

    graph_epochs = [t for t in data.training_epochs("EUR_USD", 3) if data.common_runs.get(t, 0) >= 61]
    if len(graph_epochs) >= 32:
        x = np.asarray([data.graph_feature(t) for t in graph_epochs])
        y = np.asarray([data.target("EUR_USD", t) for t in graph_epochs])
        expected, sigma = _ridge(x, y, data.graph_feature(data.cutoff), 25.)
        diagnostics = data.diagnostics("lagged_base_quote_factor_graph_ridge_timestamp_v1", {"EUR_USD": graph_epochs})
        diagnostics.update(alpha=25., stride_minutes=3, currency_nodes=len(data.currencies),
            residual_sigma_pips=sigma, factor_price_units="legacy_per_pair_pips_not_normalized_currency_returns")
        output[FAMILIES[2]] = (expected, _probability(expected, sigma), diagnostics)

    prices = data.current_prices("EUR_USD", 256)
    changes = np.diff(prices) / .0001
    if len(changes) >= 60:
        trend, variance = 0., 1.
        for value in changes:
            innovation = float(value) - trend
            trend += .08 * innovation
            variance = .92 * variance + .08 * innovation * innovation
        sigma = max(1e-6, math.sqrt(variance * 60))
        expected = max(-3 * sigma, min(3 * sigma, trend * 60))
        diagnostics = data.diagnostics("local_level_drift_ewma_contiguous_segment_v1", {})
        diagnostics.update(observations=len(changes), alpha=.08,
            state_segment_start_epoch=data.cutoff - (len(prices) - 1) * 60,
            state_reset_policy="restart_at_latest_gap; at_most_256_consecutive_prices",
            state_trend_pips_per_minute=trend, forecast_sigma_pips=sigma)
        output[FAMILIES[3]] = (expected, _probability(expected, sigma), diagnostics)
    return output


def predict_all(rows_by_pair, cutoff_epoch):
    """Return available EURUSD family tuples; caller abstains if any are absent.

    Inputs must be seven maps of <=1024 real rows each, keyed by actual UTC M1
    START epochs, with cutoff_epoch equal to the latest shared start. Future
    rows are rejected, rather than silently filtered. Source parsers must reject
    duplicates before creating maps; a dict cannot expose already-lost rows.
    Learning availability is not inferred here from cutoff or bar timestamps.
    """
    rows, cutoff = _validated(rows_by_pair, cutoff_epoch)
    try:
        with np.errstate(over="raise", divide="raise", invalid="raise"):
            output = _predict(_Data(rows, cutoff))
    except (FloatingPointError, np.linalg.LinAlgError) as exc:
        raise ValueError("numerical_failure_requires_abstention") from exc
    for expected, probability, _ in output.values():
        if not math.isfinite(expected) or not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("nonfinite_prediction_requires_abstention")
    return output
