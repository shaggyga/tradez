#!/usr/bin/env python3
"""Leakage-safe, research-only direct horizon models for FX signal snapshots.

The existing supervised strategy predicts one M5 candle and is then consumed by
several horizon lanes.  This module instead fits one panel model per requested
horizon against realised *net* long and short outcomes.  It intentionally has
no execution adapter: its output is evidence for shadow evaluation only.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sqlite3
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np

try:
    from oanda_signal_combination_audit import feature_domain
except ModuleNotFoundError:
    from trad.oanda_signal_combination_audit import feature_domain


HORIZONS: dict[str, int] = {
    "M5": 300,
    "M15": 900,
    "M30": 1_800,
    "H1": 3_600,
    "H2": 7_200,
}
RIDGE_ALPHAS: tuple[float, ...] = (1.0, 10.0, 100.0, 1_000.0)
EDGE_QUANTILES: tuple[float, ...] = (0.0, 0.50, 0.65, 0.75, 0.85, 0.90)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def origin_epoch(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


@dataclass
class PanelDataset:
    feature_names: list[str]
    raw_features: np.ndarray
    instruments: np.ndarray
    epochs: np.ndarray
    row_ids: np.ndarray
    long_net: np.ndarray
    short_net: np.ndarray
    schema_missing_rate: float

    @property
    def rows(self) -> int:
        return int(len(self.row_ids))


def _numeric_schema(connection: sqlite3.Connection, horizon_sec: int) -> list[str]:
    rows = connection.execute(
        """
        SELECT s.features_zlib
        FROM outcomes o INDEXED BY idx_combo_horizon
        JOIN snapshots s ON s.snapshot_id = o.snapshot_id
        WHERE o.horizon_sec = ?
        ORDER BY o.row_id DESC
        LIMIT 256
        """,
        (int(horizon_sec),),
    ).fetchall()
    keys: set[str] = set()
    for (payload,) in rows:
        try:
            features = json.loads(zlib.decompress(payload).decode("utf-8"))
        except (TypeError, ValueError, zlib.error, json.JSONDecodeError):
            continue
        for key, value in features.items():
            if isinstance(value, (bool, int, float)) and math.isfinite(float(value)):
                keys.add(str(key))
    return sorted(keys)


def load_panel_dataset(
    database_path: Path,
    horizon_sec: int,
    max_rows: int = 60_000,
) -> PanelDataset:
    """Load a compact numeric panel from the newest mature outcome rows."""

    database_path = Path(database_path)
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    connection.execute("PRAGMA query_only=ON")
    feature_names = _numeric_schema(connection, horizon_sec)
    if not feature_names:
        connection.close()
        raise ValueError(f"no numeric feature schema for horizon {horizon_sec}")
    rows = connection.execute(
        """
        SELECT o.row_id, s.instrument, s.origin_time, s.features_zlib,
               o.long_net_pips, o.short_net_pips
        FROM outcomes o INDEXED BY idx_combo_horizon
        JOIN snapshots s ON s.snapshot_id = o.snapshot_id
        WHERE o.horizon_sec = ?
        ORDER BY o.row_id DESC
        LIMIT ?
        """,
        (int(horizon_sec), int(max_rows)),
    ).fetchall()
    connection.close()
    rows.reverse()

    key_index = {key: index for index, key in enumerate(feature_names)}
    matrix: list[list[float]] = []
    instruments: list[str] = []
    epochs: list[float] = []
    row_ids: list[int] = []
    long_net: list[float] = []
    short_net: list[float] = []
    missing_cells = 0
    for row_id, instrument, origin_time, payload, long_value, short_value in rows:
        epoch = origin_epoch(origin_time)
        if epoch is None:
            continue
        try:
            features = json.loads(zlib.decompress(payload).decode("utf-8"))
        except (TypeError, ValueError, zlib.error, json.JSONDecodeError):
            continue
        vector = [math.nan] * len(feature_names)
        present = 0
        for key, value in features.items():
            index = key_index.get(str(key))
            if index is None or not isinstance(value, (bool, int, float)):
                continue
            numeric = float(value)
            if math.isfinite(numeric):
                vector[index] = numeric
                present += 1
        missing_cells += len(feature_names) - present
        matrix.append(vector)
        instruments.append(str(instrument or ""))
        epochs.append(epoch)
        row_ids.append(int(row_id))
        long_net.append(finite(long_value))
        short_net.append(finite(short_value))

    order = np.lexsort((np.asarray(row_ids, dtype=np.int64), np.asarray(epochs)))
    raw = np.asarray(matrix, dtype=np.float64)[order]
    total_cells = max(1, raw.shape[0] * raw.shape[1])
    return PanelDataset(
        feature_names=feature_names,
        raw_features=raw,
        instruments=np.asarray(instruments, dtype=object)[order],
        epochs=np.asarray(epochs, dtype=np.float64)[order],
        row_ids=np.asarray(row_ids, dtype=np.int64)[order],
        long_net=np.asarray(long_net, dtype=np.float64)[order],
        short_net=np.asarray(short_net, dtype=np.float64)[order],
        schema_missing_rate=round(missing_cells / total_cells, 8),
    )


def purged_partition_indices(
    epochs: np.ndarray,
    horizon_sec: int,
    train_fraction: float = 0.70,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    """Make timestamp-aligned 70/15/15 partitions with a horizon purge."""

    values = np.asarray(epochs, dtype=np.float64)
    unique = np.unique(values)
    if len(unique) < 10:
        raise ValueError("at least ten unique timestamps are required")
    selection_position = max(1, min(len(unique) - 2, int(len(unique) * train_fraction)))
    holdout_position = max(
        selection_position + 1,
        min(len(unique) - 1, int(len(unique) * (train_fraction + (1.0 - train_fraction) / 2.0))),
    )
    selection_epoch = float(unique[selection_position])
    holdout_epoch = float(unique[holdout_position])
    purge = max(0, int(horizon_sec))
    train = np.flatnonzero(values < selection_epoch - purge)
    selection = np.flatnonzero((values >= selection_epoch) & (values < holdout_epoch - purge))
    holdout = np.flatnonzero(values >= holdout_epoch)
    if min(len(train), len(selection), len(holdout)) == 0:
        raise ValueError("purged partition produced an empty split")
    used = len(train) + len(selection) + len(holdout)
    return train, selection, holdout, {
        "timestamp_aligned": True,
        "horizon_purge_seconds": purge,
        "selection_start": datetime.fromtimestamp(selection_epoch, timezone.utc).isoformat(),
        "holdout_start": datetime.fromtimestamp(holdout_epoch, timezone.utc).isoformat(),
        "train_rows": int(len(train)),
        "selection_rows": int(len(selection)),
        "holdout_rows": int(len(holdout)),
        "purged_rows": int(len(values) - used),
        "unique_timestamps": int(len(unique)),
    }


def _currency_exposures(instruments: np.ndarray, currencies: list[str]) -> np.ndarray:
    index = {currency: column for column, currency in enumerate(currencies)}
    result = np.zeros((len(instruments), len(currencies)), dtype=np.float64)
    for row, instrument in enumerate(instruments):
        parts = str(instrument).upper().split("_")
        if len(parts) != 2:
            continue
        if parts[0] in index:
            result[row, index[parts[0]]] += 1.0
        if parts[1] in index:
            result[row, index[parts[1]]] -= 1.0
    return result


def design_matrix(
    dataset: PanelDataset,
    train_indices: np.ndarray,
    minimum_coverage: float = 0.90,
) -> tuple[np.ndarray, list[str], dict[str, Any]]:
    """Build train-defined technical, currency, and cyclical context features."""

    train_raw = dataset.raw_features[train_indices]
    coverage = np.mean(np.isfinite(train_raw), axis=0)
    train_medians = np.nanmedian(train_raw, axis=0)
    filled_train = np.where(np.isfinite(train_raw), train_raw, train_medians)
    variance = np.var(filled_train, axis=0)
    keep = np.flatnonzero((coverage >= minimum_coverage) & np.isfinite(variance) & (variance > 1e-10))
    if len(keep) == 0:
        raise ValueError("no sufficiently covered, non-constant features")
    raw = dataset.raw_features[:, keep]
    medians = train_medians[keep]
    raw = np.where(np.isfinite(raw), raw, medians)
    names = [dataset.feature_names[index] for index in keep]

    # Reconstruct spread in pips.  `live_spread_atr` alone makes a one-ATR
    # spread on a major look equivalent to one on an exotic; the actual net
    # outcomes show that this missing scale interaction is material.
    if "atr_m1_pips" in names and "live_spread_atr" in names:
        atr_column = names.index("atr_m1_pips")
        spread_column = names.index("live_spread_atr")
        raw = np.column_stack((raw, raw[:, atr_column] * raw[:, spread_column]))
        names.append("spread_pips_proxy")

    train_instruments = dataset.instruments[train_indices]
    currencies = sorted(
        {
            currency
            for instrument in train_instruments
            for currency in str(instrument).upper().split("_")
            if len(currency) == 3
        }
    )
    exposures = _currency_exposures(dataset.instruments, currencies)
    seconds_day = np.mod(dataset.epochs, 86_400.0)
    day_angle = seconds_day * (2.0 * math.pi / 86_400.0)
    week_angle = np.mod(dataset.epochs, 604_800.0) * (2.0 * math.pi / 604_800.0)
    cyclical = np.column_stack(
        (np.sin(day_angle), np.cos(day_angle), np.sin(week_angle), np.cos(week_angle))
    )
    market_feature_count = len(names)
    matrix = np.column_stack((raw, exposures, cyclical))
    names.extend(f"currency_exposure_{currency}" for currency in currencies)
    names.extend(("utc_time_sin", "utc_time_cos", "utc_week_sin", "utc_week_cos"))

    means = np.mean(matrix[train_indices], axis=0)
    scales = np.std(matrix[train_indices], axis=0)
    scales = np.where(scales > 1e-8, scales, 1.0)
    matrix = np.clip((matrix - means) / scales, -8.0, 8.0)
    # A small deterministic nonlinear basis gives ridge models threshold and
    # saturation behaviour without adding a heavyweight ML runtime.  It is
    # derived entirely from origin-time features and normalised on train only.
    market = matrix[:, :market_feature_count]
    nonlinear = np.column_stack((np.abs(market), market * np.abs(market)))
    nonlinear_names = [f"absolute__{name}" for name in names[:market_feature_count]]
    nonlinear_names.extend(
        f"signed_square__{name}" for name in names[:market_feature_count]
    )
    matrix = np.column_stack((matrix, nonlinear))
    names.extend(nonlinear_names)
    final_means = np.mean(matrix[train_indices], axis=0)
    final_scales = np.std(matrix[train_indices], axis=0)
    final_scales = np.where(final_scales > 1e-8, final_scales, 1.0)
    matrix = np.clip((matrix - final_means) / final_scales, -8.0, 8.0)
    domains: dict[str, int] = {}
    for name in names:
        domain_name = name.split("__", 1)[-1]
        if domain_name.startswith("currency_exposure_"):
            domain = "cross_currency_identity"
        elif domain_name.startswith("utc_"):
            domain = "cyclical_time_context"
        elif domain_name == "spread_pips_proxy":
            domain = "volatility_liquidity"
        else:
            domain = feature_domain(domain_name)
        domains[domain] = domains.get(domain, 0) + 1
    return matrix, names, {
        "input_feature_count": int(dataset.raw_features.shape[1]),
        "retained_market_feature_count": int(len(keep)),
        "engineered_spread_scale_feature": "spread_pips_proxy" in names,
        "nonlinear_market_basis_count": int(2 * market_feature_count),
        "currency_feature_count": int(len(currencies)),
        "total_feature_count": int(matrix.shape[1]),
        "feature_domains": dict(sorted(domains.items())),
        "minimum_train_coverage": float(minimum_coverage),
        "schema_missing_rate": dataset.schema_missing_rate,
    }


def _winsorised(values: np.ndarray, reference: np.ndarray) -> np.ndarray:
    low, high = np.quantile(reference, (0.005, 0.995))
    return np.clip(values, low, high)


def ridge_fit_predict(
    train_x: np.ndarray,
    train_y: np.ndarray,
    evaluation_x: np.ndarray,
    alpha: float,
) -> np.ndarray:
    y_mean = float(np.mean(train_y))
    target = train_y - y_mean
    gram = train_x.T @ train_x
    penalty = np.eye(train_x.shape[1], dtype=np.float64) * float(alpha)
    try:
        coefficients = np.linalg.solve(gram + penalty, train_x.T @ target)
    except np.linalg.LinAlgError:
        coefficients = np.linalg.pinv(gram + penalty) @ train_x.T @ target
    return evaluation_x @ coefficients + y_mean


def project_currency_factor_moves(
    instruments: np.ndarray,
    epochs: np.ndarray,
    predicted_pair_moves: np.ndarray,
    ridge: float = 1e-3,
) -> np.ndarray:
    """Project pair forecasts onto a base-minus-quote currency factor space."""

    instruments = np.asarray(instruments, dtype=object)
    epochs = np.asarray(epochs, dtype=np.float64)
    predicted = np.asarray(predicted_pair_moves, dtype=np.float64)
    output = predicted.copy()
    for timestamp in np.unique(epochs):
        rows = np.flatnonzero(epochs == timestamp)
        parsed = [str(instruments[index]).upper().split("_") for index in rows]
        currencies = sorted(
            {currency for pair in parsed if len(pair) == 2 for currency in pair}
        )
        if len(currencies) < 2 or len(rows) < 2:
            continue
        currency_index = {currency: column for column, currency in enumerate(currencies)}
        exposure = np.zeros((len(rows), len(currencies)), dtype=np.float64)
        valid = np.zeros(len(rows), dtype=bool)
        for local_row, pair in enumerate(parsed):
            if len(pair) != 2:
                continue
            exposure[local_row, currency_index[pair[0]]] = 1.0
            exposure[local_row, currency_index[pair[1]]] = -1.0
            valid[local_row] = True
        if np.sum(valid) < 2:
            continue
        active = exposure[valid]
        values = predicted[rows][valid]
        penalty = np.eye(len(currencies), dtype=np.float64) * float(ridge)
        strengths = np.linalg.solve(active.T @ active + penalty, active.T @ values)
        reconstructed = active @ strengths
        local_output = output[rows]
        local_output[valid] = reconstructed
        output[rows] = local_output
    return output


def outcome_metrics(
    epochs: np.ndarray,
    predicted_long: np.ndarray,
    predicted_short: np.ndarray,
    actual_long: np.ndarray,
    actual_short: np.ndarray,
    horizon_sec: int,
    threshold: float,
) -> dict[str, Any]:
    long_direction = predicted_long >= predicted_short
    predicted_edge = np.where(long_direction, predicted_long, predicted_short)
    selected = predicted_edge >= float(threshold)
    realised = np.where(long_direction, actual_long, actual_short)
    chosen = realised[selected]
    chosen_epochs = epochs[selected]
    if len(chosen) == 0:
        return {
            "trades": 0,
            "coverage": 0.0,
            "independent_time_blocks": 0,
            "average_net_pips": 0.0,
            "win_rate": 0.0,
            "block_mean_net_pips": 0.0,
            "block_ci90_lower_pips": 0.0,
            "long_fraction": 0.0,
        }
    block_ids = np.floor(chosen_epochs / max(1, int(horizon_sec))).astype(np.int64)
    block_means = np.asarray(
        [np.mean(chosen[block_ids == block]) for block in np.unique(block_ids)],
        dtype=np.float64,
    )
    block_mean = float(np.mean(block_means))
    if len(block_means) > 1:
        standard_error = float(np.std(block_means, ddof=1) / math.sqrt(len(block_means)))
        lower = block_mean - 1.645 * standard_error
    else:
        lower = block_mean
    return {
        "trades": int(len(chosen)),
        "coverage": round(len(chosen) / max(1, len(epochs)), 6),
        "independent_time_blocks": int(len(block_means)),
        "average_net_pips": round(float(np.mean(chosen)), 6),
        "median_net_pips": round(float(np.median(chosen)), 6),
        "total_net_pips": round(float(np.sum(chosen)), 6),
        "win_rate": round(float(np.mean(chosen > 0.0)), 6),
        "block_mean_net_pips": round(block_mean, 6),
        "block_ci90_lower_pips": round(float(lower), 6),
        "long_fraction": round(float(np.mean(long_direction[selected])), 6),
        "threshold_pips": round(float(threshold), 6),
    }


def spread_pips_proxy(dataset: PanelDataset) -> np.ndarray:
    """Reconstruct the collection-time spread scale for reporting buckets."""
    required = ("atr_m1_pips", "live_spread_atr")
    if not all(name in dataset.feature_names for name in required):
        return np.full(dataset.rows, math.nan, dtype=np.float64)
    atr = dataset.raw_features[:, dataset.feature_names.index(required[0])]
    ratio = dataset.raw_features[:, dataset.feature_names.index(required[1])]
    result = atr * ratio
    return np.where(np.isfinite(result) & (result >= 0.0), result, math.nan)


def cost_bucket_metrics(
    spread_pips: np.ndarray,
    epochs: np.ndarray,
    predicted_long: np.ndarray,
    predicted_short: np.ndarray,
    actual_long: np.ndarray,
    actual_short: np.ndarray,
    horizon_sec: int,
    threshold: float,
) -> dict[str, dict[str, Any]]:
    """Report fixed, predeclared liquidity buckets without selecting on them."""
    spreads = np.asarray(spread_pips, dtype=np.float64)
    buckets = {
        "liquid_le_3_pips": np.isfinite(spreads) & (spreads <= 3.0),
        "medium_gt_3_le_10_pips": (
            np.isfinite(spreads) & (spreads > 3.0) & (spreads <= 10.0)
        ),
        "wide_gt_10_pips": np.isfinite(spreads) & (spreads > 10.0),
        "spread_unknown": ~np.isfinite(spreads),
    }
    result: dict[str, dict[str, Any]] = {}
    for name, selected in buckets.items():
        metrics = outcome_metrics(
            epochs[selected],
            predicted_long[selected],
            predicted_short[selected],
            actual_long[selected],
            actual_short[selected],
            horizon_sec,
            threshold,
        )
        metrics["population_rows"] = int(np.sum(selected))
        result[name] = metrics
    return result


def _selection_score(metrics: dict[str, Any]) -> float:
    trades = int(metrics.get("trades") or 0)
    blocks = int(metrics.get("independent_time_blocks") or 0)
    if trades < 30 or blocks < 5:
        return -1e9 + trades + blocks
    # The lower block confidence bound discourages overlapping rows from
    # masquerading as thousands of independent observations.
    return finite(metrics.get("block_ci90_lower_pips"))


def fit_horizon_model(
    dataset: PanelDataset,
    horizon_sec: int,
    alphas: Iterable[float] = RIDGE_ALPHAS,
    edge_quantiles: Iterable[float] = EDGE_QUANTILES,
) -> dict[str, Any]:
    train, selection, holdout, partition = purged_partition_indices(
        dataset.epochs, horizon_sec
    )
    matrix, feature_names, feature_audit = design_matrix(dataset, train)
    train_long = _winsorised(dataset.long_net[train], dataset.long_net[train])
    train_short = _winsorised(dataset.short_net[train], dataset.short_net[train])

    atr_name = "atr_m1_pips"
    if atr_name in dataset.feature_names:
        atr_raw = dataset.raw_features[:, dataset.feature_names.index(atr_name)]
        positive_train_atr = atr_raw[train][np.isfinite(atr_raw[train]) & (atr_raw[train] > 0.0)]
        atr_floor = max(0.01, float(np.quantile(positive_train_atr, 0.01))) if len(positive_train_atr) else 0.01
        atr_fill = float(np.median(positive_train_atr)) if len(positive_train_atr) else 1.0
        atr_scale = np.where(np.isfinite(atr_raw) & (atr_raw > 0.0), atr_raw, atr_fill)
        atr_scale = np.maximum(atr_scale, atr_floor)
    else:
        atr_scale = np.ones(dataset.rows, dtype=np.float64)
        atr_floor = 1.0
    move_net = (dataset.long_net - dataset.short_net) / (2.0 * atr_scale)
    cost_net = -(dataset.long_net + dataset.short_net) / (2.0 * atr_scale)
    train_move = _winsorised(move_net[train], move_net[train])
    train_cost = _winsorised(cost_net[train], cost_net[train])

    candidates: list[dict[str, Any]] = []
    cached_predictions: dict[tuple[str, float], tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
    for alpha in alphas:
        prediction_sets: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
        direct_selection_long = ridge_fit_predict(matrix[train], train_long, matrix[selection], alpha)
        direct_selection_short = ridge_fit_predict(matrix[train], train_short, matrix[selection], alpha)
        direct_holdout_long = ridge_fit_predict(matrix[train], train_long, matrix[holdout], alpha)
        direct_holdout_short = ridge_fit_predict(matrix[train], train_short, matrix[holdout], alpha)
        prediction_sets["direct_net"] = (
            direct_selection_long,
            direct_selection_short,
            direct_holdout_long,
            direct_holdout_short,
        )

        selection_move = ridge_fit_predict(matrix[train], train_move, matrix[selection], alpha)
        selection_cost = np.maximum(
            0.0,
            ridge_fit_predict(matrix[train], train_cost, matrix[selection], alpha),
        )
        holdout_move = ridge_fit_predict(matrix[train], train_move, matrix[holdout], alpha)
        holdout_cost = np.maximum(
            0.0,
            ridge_fit_predict(matrix[train], train_cost, matrix[holdout], alpha),
        )
        prediction_sets["structural_atr_move_cost"] = (
            (selection_move - selection_cost) * atr_scale[selection],
            (-selection_move - selection_cost) * atr_scale[selection],
            (holdout_move - holdout_cost) * atr_scale[holdout],
            (-holdout_move - holdout_cost) * atr_scale[holdout],
        )
        selection_factor_move = project_currency_factor_moves(
            dataset.instruments[selection],
            dataset.epochs[selection],
            selection_move,
        )
        holdout_factor_move = project_currency_factor_moves(
            dataset.instruments[holdout],
            dataset.epochs[holdout],
            holdout_move,
        )
        prediction_sets["currency_factor_atr_move_cost"] = (
            (selection_factor_move - selection_cost) * atr_scale[selection],
            (-selection_factor_move - selection_cost) * atr_scale[selection],
            (holdout_factor_move - holdout_cost) * atr_scale[holdout],
            (-holdout_factor_move - holdout_cost) * atr_scale[holdout],
        )

        for method, predictions in prediction_sets.items():
            selection_long, selection_short, holdout_long, holdout_short = predictions
            cached_predictions[(method, float(alpha))] = predictions
            edges = np.maximum(selection_long, selection_short)
            for quantile in edge_quantiles:
                threshold = max(0.0, float(np.quantile(edges, float(quantile))))
                metrics = outcome_metrics(
                    dataset.epochs[selection],
                    selection_long,
                    selection_short,
                    dataset.long_net[selection],
                    dataset.short_net[selection],
                    horizon_sec,
                    threshold,
                )
                candidates.append(
                    {
                        "method": method,
                        "alpha": float(alpha),
                        "edge_quantile": float(quantile),
                        "threshold_pips": threshold,
                        "score": _selection_score(metrics),
                        "metrics": metrics,
                    }
                )
    candidates.sort(
        key=lambda row: (
            finite(row.get("score"), -1e12),
            int((row.get("metrics") or {}).get("independent_time_blocks") or 0),
            -finite(row.get("edge_quantile")),
        ),
        reverse=True,
    )
    method_selection_leaders: dict[str, dict[str, Any]] = {}
    for row in candidates:
        candidate_method = str(row.get("method") or "")
        if candidate_method and candidate_method not in method_selection_leaders:
            method_selection_leaders[candidate_method] = {
                "ridge_alpha": row["alpha"],
                "edge_quantile": row["edge_quantile"],
                "threshold_pips": round(float(row["threshold_pips"]), 6),
                "score": round(float(row["score"]), 6),
                "selection": row["metrics"],
            }
    for candidate_method, leader in method_selection_leaders.items():
        _, _, diagnostic_holdout_long, diagnostic_holdout_short = cached_predictions[
            (candidate_method, float(leader["ridge_alpha"]))
        ]
        leader["untouched_holdout_diagnostic"] = outcome_metrics(
            dataset.epochs[holdout],
            diagnostic_holdout_long,
            diagnostic_holdout_short,
            dataset.long_net[holdout],
            dataset.short_net[holdout],
            horizon_sec,
            float(leader["threshold_pips"]),
        )
        leader["holdout_used_for_selection"] = False
    best = candidates[0]
    method = str(best["method"])
    alpha = float(best["alpha"])
    selection_long, selection_short, holdout_long, holdout_short = cached_predictions[(method, alpha)]
    holdout_metrics = outcome_metrics(
        dataset.epochs[holdout],
        holdout_long,
        holdout_short,
        dataset.long_net[holdout],
        dataset.short_net[holdout],
        horizon_sec,
        float(best["threshold_pips"]),
    )
    holdout_inverse_metrics = outcome_metrics(
        dataset.epochs[holdout],
        holdout_short,
        holdout_long,
        dataset.long_net[holdout],
        dataset.short_net[holdout],
        horizon_sec,
        float(best["threshold_pips"]),
    )
    holdout_spreads = spread_pips_proxy(dataset)[holdout]
    holdout_cost_buckets = cost_bucket_metrics(
        holdout_spreads,
        dataset.epochs[holdout],
        holdout_long,
        holdout_short,
        dataset.long_net[holdout],
        dataset.short_net[holdout],
        horizon_sec,
        float(best["threshold_pips"]),
    )
    holdout_inverse_cost_buckets = cost_bucket_metrics(
        holdout_spreads,
        dataset.epochs[holdout],
        holdout_short,
        holdout_long,
        dataset.long_net[holdout],
        dataset.short_net[holdout],
        horizon_sec,
        float(best["threshold_pips"]),
    )

    # A simple direction-only baseline reveals whether the model improves on
    # the recent M5 return rather than merely harvesting a directional drift.
    baseline_name = "return_m5_atr"
    if baseline_name in dataset.feature_names:
        raw_index = dataset.feature_names.index(baseline_name)
        trend_long = dataset.raw_features[holdout, raw_index] >= 0.0
        baseline_long = np.where(trend_long, 1.0, -1.0)
        baseline_short = -baseline_long
        baseline = outcome_metrics(
            dataset.epochs[holdout],
            baseline_long,
            baseline_short,
            dataset.long_net[holdout],
            dataset.short_net[holdout],
            horizon_sec,
            0.0,
        )
    else:
        baseline = {"available": False}

    promotion_checks = {
        "selection_positive_average_net": finite(best["metrics"].get("average_net_pips")) > 0.0,
        "selection_positive_block_ci90_lower": finite(best["metrics"].get("block_ci90_lower_pips")) > 0.0,
        "minimum_200_holdout_trades": int(holdout_metrics.get("trades") or 0) >= 200,
        "minimum_20_independent_blocks": int(holdout_metrics.get("independent_time_blocks") or 0) >= 20,
        "positive_average_net": finite(holdout_metrics.get("average_net_pips")) > 0.0,
        "positive_block_ci90_lower": finite(holdout_metrics.get("block_ci90_lower_pips")) > 0.0,
        "beats_m5_trend_baseline": finite(holdout_metrics.get("block_mean_net_pips"))
        > finite(baseline.get("block_mean_net_pips")),
    }
    return {
        "schema_version": 1,
        "model": "direct_multihorizon_currency_panel_ridge_v3",
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "execution_adapter": False,
        "target": "separate realised long_net_pips and short_net_pips",
        "rows_loaded": dataset.rows,
        "horizon_sec": int(horizon_sec),
        "partition": partition,
        "feature_audit": feature_audit,
        "feature_names": feature_names,
        "selected_hyperparameters": {
            "target_method": method,
            "ridge_alpha": alpha,
            "edge_quantile": float(best["edge_quantile"]),
            "absolute_edge_threshold_pips": round(float(best["threshold_pips"]), 6),
        },
        "selection": best["metrics"],
        "method_selection_leaders": method_selection_leaders,
        "untouched_holdout": holdout_metrics,
        "untouched_holdout_by_cost_bucket": holdout_cost_buckets,
        "inverse_direction_holdout_diagnostic": holdout_inverse_metrics,
        "inverse_direction_holdout_by_cost_bucket": holdout_inverse_cost_buckets,
        "inverse_direction_policy": (
            "diagnostic_only; never selected on or connected to execution"
        ),
        "m5_trend_baseline_holdout": baseline,
        "promotion_checks": promotion_checks,
        "promotion_ready": all(promotion_checks.values()),
        "shadow_decision": (
            "forward_observe_only"
            if finite(best["metrics"].get("block_ci90_lower_pips")) > 0.0
            else "no_trade_failed_purged_selection"
        ),
        "promotion_policy": "shadow forward validation remains required even if historical checks pass",
        "selection_candidates": [
            {
                "alpha": row["alpha"],
                "target_method": row["method"],
                "edge_quantile": row["edge_quantile"],
                "threshold_pips": round(float(row["threshold_pips"]), 6),
                "score": round(float(row["score"]), 6),
                "metrics": row["metrics"],
            }
            for row in candidates[:12]
        ],
    }


def write_markdown_report(payload: dict[str, Any], path: Path) -> None:
    lines = [
        "# Direct Multi-Horizon Panel Model Audit",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "This is research-only. It cannot place orders and is not account eligible.",
        "",
        "| Horizon | Holdout trades | Blocks | Avg net pips | Block CI90 lower | Win rate | Baseline block mean | Promotion ready |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for label, model in payload.get("horizons", {}).items():
        holdout = model.get("untouched_holdout") or {}
        baseline = model.get("m5_trend_baseline_holdout") or {}
        lines.append(
            f"| {label} | {int(holdout.get('trades') or 0)} | "
            f"{int(holdout.get('independent_time_blocks') or 0)} | "
            f"{finite(holdout.get('average_net_pips')):.3f} | "
            f"{finite(holdout.get('block_ci90_lower_pips')):.3f} | "
            f"{100.0 * finite(holdout.get('win_rate')):.1f}% | "
            f"{finite(baseline.get('block_mean_net_pips')):.3f} | "
            f"{'yes' if model.get('promotion_ready') else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Each horizon is fitted directly; H1/H2 are not scaled-up M5 forecasts.",
            "- Hyperparameters and the abstention threshold are chosen only on the purged selection window.",
            "- The newest holdout is read once for reporting and promotion checks.",
            "- Confidence is calculated over horizon-sized time blocks, not overlapping pair rows.",
            "- Currency-exposure and cyclical-time features share evidence across pairs without pair-ID memorisation.",
            "- A non-positive purged-selection confidence bound produces an explicit no-trade decision.",
            "",
            "Historical success alone cannot enable execution. A separate forward shadow sample is required.",
        ]
    )
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_audit(
    database_path: Path,
    output_json: Path,
    output_markdown: Path,
    horizons: Iterable[str] = HORIZONS,
    max_rows: int = 60_000,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    failures: dict[str, str] = {}
    for label in horizons:
        label = str(label).upper()
        if label not in HORIZONS:
            failures[label] = "unsupported_horizon"
            continue
        print(
            json.dumps(
                {
                    "status": "horizon_started",
                    "horizon": label,
                    "horizon_sec": HORIZONS[label],
                    "max_rows": int(max_rows),
                },
                separators=(",", ":"),
            ),
            flush=True,
        )
        try:
            dataset = load_panel_dataset(database_path, HORIZONS[label], max_rows=max_rows)
            results[label] = fit_horizon_model(dataset, HORIZONS[label])
            print(
                json.dumps(
                    {
                        "status": "horizon_completed",
                        "horizon": label,
                        "rows": dataset.rows,
                        "promotion_ready": bool(results[label]["promotion_ready"]),
                        "shadow_decision": results[label]["shadow_decision"],
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )
        except (OSError, sqlite3.Error, ValueError) as exc:
            failures[label] = f"{type(exc).__name__}: {exc}"
        finally:
            gc.collect()
    payload = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "database": str(Path(database_path).resolve()),
        "model": "direct_multihorizon_currency_panel_ridge_v3",
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "horizons": results,
        "failures": failures,
    }
    output_json = Path(output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_json.with_suffix(output_json.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output_json)
    write_markdown_report(payload, output_markdown)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    parser.add_argument("--horizons", nargs="+", default=list(HORIZONS))
    parser.add_argument("--max-rows", type=int, default=60_000)
    args = parser.parse_args()
    payload = run_audit(
        args.database,
        args.output_json,
        args.output_markdown,
        horizons=args.horizons,
        max_rows=max(1_000, int(args.max_rows)),
    )
    print(json.dumps({
        "output_json": str(args.output_json.resolve()),
        "horizons": sorted(payload["horizons"]),
        "failures": payload["failures"],
    }, indent=2))
    return 0 if payload["horizons"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "HORIZONS",
    "PanelDataset",
    "design_matrix",
    "fit_horizon_model",
    "load_panel_dataset",
    "outcome_metrics",
    "project_currency_factor_moves",
    "purged_partition_indices",
    "ridge_fit_predict",
    "spread_pips_proxy",
    "cost_bucket_metrics",
    "run_audit",
]
