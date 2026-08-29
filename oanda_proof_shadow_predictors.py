#!/usr/bin/env python3
"""Prospective proof-only predictors for the canonical FX evidence ledger.

This worker deliberately publishes nowhere near the execution feed.  It reads
the already-materialized completed-bar feature snapshot, builds four genuinely
different H1 hypotheses, and records each forecast before its outcome exists:

* a repaired per-pair autoregressive ridge;
* a pooled probabilistic tabular model shared across pairs;
* a currency-factor graph/transfer ridge;
* a probabilistic local-trend state-space baseline.

Every row is research-only, account-ineligible, deterministic for a data cutoff,
and matured by ``oanda_canonical_outcome_worker.py`` from executable bid/ask
quotes.  This module has no broker or order API imports.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

try:
    from oanda_proof_cohort_registry import ensure_cohorts
    from oanda_shadow_outcome_store import ShadowOutcomeStore
except ModuleNotFoundError:
    from trad.oanda_proof_cohort_registry import ensure_cohorts
    from trad.oanda_shadow_outcome_store import ShadowOutcomeStore


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_SNAPSHOT = STATE / "live_model_feature_snapshot_v1.json"
DEFAULT_DATABASE = STATE / "strategy_shadow_outcomes_v1.sqlite"
DEFAULT_HEARTBEAT = STATE / "proof_shadow_predictors_v1.json"
FROZEN_PREDICTION_SOURCE_SHA256 = (
    "1bca3b3b583dcad31d25af7dff44e005c417e81d2e6392d763ade02f04cb0752"
)
DEFAULT_COHORT_DATABASE = STATE / "proof_cohort_registry_v1.sqlite"
DEFAULT_COHORT_STATE = STATE / "proof_cohort_registry_v1.json"
DEFAULT_GOVERNANCE_CONFIG = ROOT / "config" / "prospective_replication_governance_v1.json"
HORIZON_SEC = 3600
HORIZON_STEPS = 60
CADENCE_SEC = 900
FEATURE_WINDOWS = (1, 3, 5, 15, 30, 60)
FAMILIES = (
    "ridge_return_repaired",
    "modern_tabular_probabilistic_repaired",
    "cross_pair_graph_transfer",
    "probabilistic_state_space",
)
# The original Aug-06 material contracts were inadvertently reactivated after
# Aug-10 supersession.  This explicit generation opens clean prospective
# activation instances without rewriting or crediting the contaminated legacy
# rows.  It is intentionally stable across process restarts.
PROOF_COHORT_GENERATION_ID = "lineage_repair_20260829_v1"
FORECAST_CONTRACT_VERSION = "proof_forecast_contract_v2_cohort_bound"
COST_MODEL_VERSION = "executable_bid_ask_plus_0.25pip_modeled_slippage_v1"
MODEL_SPECIFICATIONS = {
    "ridge_return_repaired": {
        "model": "per_pair_standardized_ridge",
        "alpha": 10.0,
        "stride": 3,
    },
    "modern_tabular_probabilistic_repaired": {
        "model": "pooled_hist_gradient_boosting_regressor_classifier",
        "max_iter": 40,
        "max_leaf_nodes": 15,
        "min_samples_leaf": 40,
        "learning_rate": 0.06,
        "l2_regularization": 1.0,
        "random_state": 20260806,
    },
    "cross_pair_graph_transfer": {
        "model": "lagged_base_quote_factor_graph_ridge_v2",
        "alpha": 25.0,
        "factor_windows": [1, 5, 15, 60],
    },
    "probabilistic_state_space": {
        "model": "local_level_drift_ewma_state_space_v1",
        "alpha": 0.08,
        "history": 256,
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(
        path.suffix + f".{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temporary.unlink(missing_ok=True)


def code_hash() -> str:
    """Return the frozen prediction contract, excluding operational plumbing.

    Heartbeat, logging, and Windows-safe file-publication changes must not mint
    a new proof cohort.  Deliberate changes to prediction semantics must update
    this value and therefore start a new immutable cohort.
    """
    return FROZEN_PREDICTION_SOURCE_SHA256


def run_with_progress_heartbeat(
    heartbeat: Path,
    payload: dict[str, Any],
    operation: Any,
    *,
    interval_sec: float = 15.0,
) -> Any:
    """Run a slow proof build while keeping supervision causally informed."""
    state = dict(payload)
    state["generated_utc"] = utc_now()
    state["progress_sequence"] = 0
    atomic_json(heartbeat, state)
    stop = threading.Event()
    write_errors = 0

    def pulse() -> None:
        nonlocal write_errors
        sequence = 0
        while not stop.wait(max(0.01, interval_sec)):
            sequence += 1
            current = dict(state)
            current["generated_utc"] = utc_now()
            current["progress_sequence"] = sequence
            current["progress_write_errors"] = write_errors
            try:
                atomic_json(heartbeat, current)
            except OSError:
                write_errors += 1

    thread = threading.Thread(
        target=pulse,
        name="proof-progress-heartbeat",
        daemon=True,
    )
    thread.start()
    try:
        return operation()
    finally:
        stop.set()
        thread.join(timeout=max(2.0, interval_sec + 1.0))


def training_dataset_fingerprints(
    series_map: dict[str, np.ndarray], generated: str
) -> tuple[dict[str, str], str]:
    pair_hashes: dict[str, str] = {}
    combined = hashlib.sha256()
    combined.update(str(generated).encode("utf-8"))
    for instrument, values in sorted(series_map.items()):
        digest = hashlib.sha256()
        digest.update(instrument.encode("utf-8"))
        digest.update(np.asarray(values, dtype="<f8").tobytes())
        pair_hashes[instrument] = digest.hexdigest()
        combined.update(instrument.encode("utf-8"))
        combined.update(bytes.fromhex(pair_hashes[instrument]))
    return pair_hashes, combined.hexdigest()


def pre_governance_counts(database: Path) -> dict[str, int]:
    if not database.exists():
        return {family: 0 for family in FAMILIES}
    connection = sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    try:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "canonical_forecasts" not in tables:
            return {family: 0 for family in FAMILIES}
        counts = dict(
            connection.execute(
                "SELECT family,COUNT(*) FROM canonical_forecasts WHERE family IN (?,?,?,?) GROUP BY family",
                FAMILIES,
            ).fetchall()
        )
        return {family: int(counts.get(family, 0)) for family in FAMILIES}
    finally:
        connection.close()


def durable_proof_counts(
    database: Path,
    cohort_ids: dict[str, str],
    evidence_state: Path | None = None,
) -> dict[str, dict[str, int]]:
    """Read cohort-specific durable counts from the canonical evidence census.

    The evidence census is generated from the immutable ledger by the governed
    evaluator.  Reading its compact cohort rows avoids repeatedly applying
    ``json_extract`` across the multi-gigabyte forecast ledger from this
    latency-sensitive producer.
    """
    output = {
        family: {"forecasts": 0, "matured": 0}
        for family in FAMILIES
    }
    active = {str(value) for value in cohort_ids.values() if value}
    if not active:
        return output
    state_path = evidence_state or database.with_name("edge_evidence_v1.json")
    evidence = read_json(state_path)
    governance = evidence.get("prospective_governance") or {}
    proof = governance.get("proof_cohorts") or {}
    rows = proof.get("cohorts") or []
    for row in rows:
        if not isinstance(row, dict) or str(row.get("cohort_id") or "") not in active:
            continue
        family = str(row.get("family") or "")
        if family not in output:
            continue
        output[family] = {
            "forecasts": int(row.get("forecast_count") or 0),
            "matured": int(row.get("matured_outcome_count") or 0),
        }
    return output


def cohort_specifications(
    *,
    source_sha256: str,
    feature_version: str,
    training_cutoff_utc: str,
    training_dataset_sha256: str,
    governance: dict[str, Any],
    prior_counts: dict[str, int],
) -> list[dict[str, Any]]:
    promotion = {
        "discovery": governance.get("discovery") or {},
        "sequential": governance.get("sequential") or {},
        "power": governance.get("power") or {},
        "minimum_economic_edge_pips": governance.get("minimum_economic_edge_pips") or {},
        "graduation": governance.get("graduation") or {},
    }
    return [
        {
            "family": family,
            "cohort_generation_id": PROOF_COHORT_GENERATION_ID,
            "source_sha256": f"sha256:{source_sha256}",
            "model_specification": MODEL_SPECIFICATIONS[family],
            "hyperparameters": MODEL_SPECIFICATIONS[family],
            "feature_schema_version": f"sha256:{feature_version}",
            "training_policy": {
                "mode": "causal_rolling_completed_m1_v1",
                "per_forecast_cutoff_and_dataset_hash_required": True,
                "lookback_max_bars": 512,
                "horizon_steps": HORIZON_STEPS,
            },
            "forecast_contract_version": FORECAST_CONTRACT_VERSION,
            "cost_model_version": COST_MODEL_VERSION,
            "dimensions": {
                "pairs": "all snapshot pairs with >=180 completed M1 observations",
                "horizon_sec": HORIZON_SEC,
                "sessions": ["asia", "london", "overlap", "new_york", "rollover"],
                "liquidity": ["liquid_le_2", "normal_2_to_3", "elevated_3_to_5", "wide_gt_5"],
            },
            "deduplication_rules": {
                "unit": "family_pair_horizon_session_liquidity",
                "episode": "shared_signed_currency_factor_within_horizon",
            },
            "promotion_thresholds": promotion,
            "initial_training_cutoff_utc": training_cutoff_utc,
            "initial_training_dataset_sha256": f"sha256:{training_dataset_sha256}",
            "pre_governance_forecast_count": int(prior_counts.get(family, 0)),
        }
        for family in FAMILIES
    ]


def split_pair(instrument: str) -> tuple[str, str]:
    parts = str(instrument).upper().split("_")
    if len(parts) != 2 or any(len(part) != 3 for part in parts):
        raise ValueError(f"invalid instrument {instrument}")
    return parts[0], parts[1]


def pip_for(instrument: str, raw: dict[str, Any]) -> float:
    quote = raw.get("quote") or {}
    value = finite(quote.get("pip"))
    if value > 0.0:
        return value
    return 0.01 if str(instrument).endswith("_JPY") else 0.0001


def m1_series(raw: dict[str, Any]) -> np.ndarray:
    series = raw.get("series") or {}
    values = series.get("M1") if isinstance(series, dict) else None
    if not isinstance(values, list):
        return np.asarray([], dtype=float)
    output = np.asarray([finite(value, math.nan) for value in values], dtype=float)
    return output[np.isfinite(output) & (output > 0.0)]


def feature_vector(prices: np.ndarray, index: int, pip: float) -> np.ndarray:
    if index < max(FEATURE_WINDOWS):
        raise ValueError("insufficient feature history")
    features = [(prices[index] - prices[index - window]) / pip for window in FEATURE_WINDOWS]
    one_step = np.diff(prices[: index + 1]) / pip
    for window in (5, 15, 60):
        tail = one_step[-window:]
        features.append(float(np.std(tail)) if tail.size else 0.0)
    tail20 = prices[max(0, index - 19) : index + 1]
    width = float(np.max(tail20) - np.min(tail20))
    features.append(0.5 if width <= 0.0 else float((prices[index] - np.min(tail20)) / width))
    return np.asarray(features, dtype=float)


def supervised_rows(
    prices: np.ndarray, pip: float, horizon_steps: int = HORIZON_STEPS, stride: int = 3
) -> tuple[np.ndarray, np.ndarray]:
    start = max(FEATURE_WINDOWS)
    stop = len(prices) - int(horizon_steps)
    if stop <= start:
        return np.empty((0, 10)), np.empty(0)
    indices = range(start, stop, max(1, int(stride)))
    x = np.asarray([feature_vector(prices, index, pip) for index in indices], dtype=float)
    y = np.asarray(
        [(prices[index + horizon_steps] - prices[index]) / pip for index in indices],
        dtype=float,
    )
    return x, y


def ridge_fit_predict(
    x: np.ndarray, y: np.ndarray, current: np.ndarray, alpha: float = 10.0
) -> tuple[float, float]:
    if len(y) < max(24, x.shape[1] * 2):
        raise ValueError("insufficient ridge rows")
    mean = np.nanmean(x, axis=0)
    scale = np.nanstd(x, axis=0)
    scale[~np.isfinite(scale) | (scale < 1e-9)] = 1.0
    z = np.nan_to_num((x - mean) / scale)
    current_z = np.nan_to_num((current - mean) / scale)
    design = np.column_stack([np.ones(len(z)), z])
    penalty = np.eye(design.shape[1]) * float(alpha)
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    prediction = float(np.r_[1.0, current_z] @ beta)
    residual = y - design @ beta
    sigma = max(1e-6, float(np.std(residual)))
    return prediction, sigma


def normal_probability_up(mean: float, sigma: float) -> float:
    value = mean / max(1e-9, sigma)
    return min(0.999, max(0.001, 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))))


def ridge_predictions(
    series_map: dict[str, np.ndarray], pips: dict[str, float]
) -> dict[str, tuple[float, float, dict[str, Any]]]:
    output = {}
    for instrument, prices in series_map.items():
        x, y = supervised_rows(prices, pips[instrument])
        if len(y) < 24:
            continue
        expected, sigma = ridge_fit_predict(
            x, y, feature_vector(prices, len(prices) - 1, pips[instrument])
        )
        output[instrument] = (
            expected,
            normal_probability_up(expected, sigma),
            {"training_rows": len(y), "residual_sigma_pips": round(sigma, 6)},
        )
    return output


def pooled_tabular_predictions(
    series_map: dict[str, np.ndarray], pips: dict[str, float]
) -> dict[str, tuple[float, float, dict[str, Any]]]:
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
    except ImportError:
        return {}
    train_x: list[np.ndarray] = []
    train_y: list[np.ndarray] = []
    current: dict[str, np.ndarray] = {}
    scales: dict[str, float] = {}
    for instrument, prices in series_map.items():
        x, y = supervised_rows(prices, pips[instrument], stride=5)
        if len(y) < 24:
            continue
        volatility = max(1.0, float(np.std(np.diff(prices[-120:]) / pips[instrument])) * math.sqrt(HORIZON_STEPS))
        train_x.append(x / volatility)
        train_y.append(y / volatility)
        current[instrument] = feature_vector(prices, len(prices) - 1, pips[instrument]) / volatility
        scales[instrument] = volatility
    if not train_x:
        return {}
    x_all = np.vstack(train_x)
    y_all = np.concatenate(train_y)
    if len(y_all) < 300 or len(np.unique(y_all > 0.0)) < 2:
        return {}
    regressor = HistGradientBoostingRegressor(
        max_iter=40, max_leaf_nodes=15, min_samples_leaf=40, learning_rate=0.06,
        l2_regularization=1.0, random_state=20260806,
    ).fit(x_all, y_all)
    classifier = HistGradientBoostingClassifier(
        max_iter=40, max_leaf_nodes=15, min_samples_leaf=40, learning_rate=0.06,
        l2_regularization=1.0, random_state=20260806,
    ).fit(x_all, y_all > 0.0)
    output = {}
    for instrument, vector in current.items():
        frame = vector.reshape(1, -1)
        expected = float(regressor.predict(frame)[0]) * scales[instrument]
        probability = float(classifier.predict_proba(frame)[0, 1])
        output[instrument] = (
            expected,
            probability,
            {"pooled_training_rows": len(y_all), "model": "hist_gradient_boosting"},
        )
    return output


def currency_factor_matrix(
    series_map: dict[str, np.ndarray], pips: dict[str, float]
) -> tuple[tuple[str, ...], np.ndarray, int]:
    minimum = min((len(values) for values in series_map.values()), default=0)
    if minimum < 180:
        return (), np.empty((0, 0)), 0
    currencies = tuple(sorted({currency for pair in series_map for currency in split_pair(pair)}))
    positions = {currency: index for index, currency in enumerate(currencies)}
    factors = np.zeros((minimum - 1, len(currencies)), dtype=float)
    counts = np.zeros_like(factors)
    for pair, values in series_map.items():
        base, quote = split_pair(pair)
        returns = np.diff(values[-minimum:]) / pips[pair]
        factors[:, positions[base]] += returns
        factors[:, positions[quote]] -= returns
        counts[:, positions[base]] += 1.0
        counts[:, positions[quote]] += 1.0
    factors /= np.maximum(1.0, counts)
    return currencies, factors, minimum


def graph_predictions(
    series_map: dict[str, np.ndarray], pips: dict[str, float]
) -> dict[str, tuple[float, float, dict[str, Any]]]:
    currencies, factors, minimum = currency_factor_matrix(series_map, pips)
    if not currencies:
        return {}
    positions = {currency: index for index, currency in enumerate(currencies)}
    output = {}
    start = max(FEATURE_WINDOWS)
    stop = minimum - 1 - HORIZON_STEPS
    for pair, values in series_map.items():
        if stop <= start:
            continue
        base, quote = split_pair(pair)
        aligned = values[-minimum:]
        pair_returns = np.diff(aligned) / pips[pair]
        rows = []
        targets = []
        for index in range(start, stop, 3):
            factor_features = []
            for window in (1, 5, 15, 60):
                window_factors = factors[index - window : index]
                factor_features.extend(
                    [
                        np.sum(window_factors[:, positions[base]]),
                        np.sum(window_factors[:, positions[quote]]),
                        np.mean(np.abs(np.sum(window_factors, axis=0))),
                    ]
                )
            factor_features.extend(
                [
                    np.sum(pair_returns[index - window : index])
                    for window in (1, 5, 15, 60)
                ]
            )
            rows.append(factor_features)
            targets.append((aligned[index + HORIZON_STEPS] - aligned[index]) / pips[pair])
        current = []
        for window in (1, 5, 15, 60):
            window_factors = factors[-window:]
            current.extend(
                [
                    np.sum(window_factors[:, positions[base]]),
                    np.sum(window_factors[:, positions[quote]]),
                    np.mean(np.abs(np.sum(window_factors, axis=0))),
                ]
            )
        current.extend([np.sum(pair_returns[-window:]) for window in (1, 5, 15, 60)])
        try:
            expected, sigma = ridge_fit_predict(
                np.asarray(rows, dtype=float), np.asarray(targets, dtype=float),
                np.asarray(current, dtype=float), alpha=25.0,
            )
        except (ValueError, np.linalg.LinAlgError):
            continue
        output[pair] = (
            expected,
            normal_probability_up(expected, sigma),
            {
                "training_rows": len(targets),
                "currency_nodes": len(currencies),
                "base_node": base,
                "quote_node": quote,
                "residual_sigma_pips": round(sigma, 6),
                "construction": "strictly_lagged_base_quote_factor_graph_ridge_v2",
            },
        )
    return output


def state_space_predictions(
    series_map: dict[str, np.ndarray], pips: dict[str, float]
) -> dict[str, tuple[float, float, dict[str, Any]]]:
    output = {}
    alpha = 0.08
    for pair, prices in series_map.items():
        returns = np.diff(prices[-256:]) / pips[pair]
        if len(returns) < 60:
            continue
        trend = 0.0
        variance = 1.0
        for value in returns:
            innovation = float(value) - trend
            trend += alpha * innovation
            variance = (1.0 - alpha) * variance + alpha * innovation * innovation
        sigma = max(1e-6, math.sqrt(variance * HORIZON_STEPS))
        raw = trend * HORIZON_STEPS
        expected = max(-3.0 * sigma, min(3.0 * sigma, raw))
        output[pair] = (
            expected,
            normal_probability_up(expected, sigma),
            {
                "observations": len(returns),
                "state_trend_pips_per_minute": round(trend, 6),
                "forecast_sigma_pips": round(sigma, 6),
                "model": "local_level_drift_ewma_state_space_v1",
            },
        )
    return output


def build_forecasts(
    snapshot: dict[str, Any], cohort_ids: dict[str, str] | None = None
) -> list[dict[str, Any]]:
    instruments = snapshot.get("instruments") or {}
    if not isinstance(instruments, dict):
        return []
    generated = str(snapshot.get("generated_utc") or utc_now())
    series_map: dict[str, np.ndarray] = {}
    pips: dict[str, float] = {}
    usable: dict[str, dict[str, Any]] = {}
    for instrument, raw in sorted(instruments.items()):
        if not isinstance(raw, dict):
            continue
        quote = raw.get("quote") or {}
        bid, ask = finite(quote.get("bid")), finite(quote.get("ask"))
        prices = m1_series(raw)
        if bid <= 0.0 or ask < bid or len(prices) < 180:
            continue
        series_map[str(instrument)] = prices
        pips[str(instrument)] = pip_for(str(instrument), raw)
        usable[str(instrument)] = raw
    predictions = {
        "ridge_return_repaired": ridge_predictions(series_map, pips),
        "modern_tabular_probabilistic_repaired": pooled_tabular_predictions(series_map, pips),
        "cross_pair_graph_transfer": graph_predictions(series_map, pips),
        "probabilistic_state_space": state_space_predictions(series_map, pips),
    }
    pair_training_hashes, pooled_training_hash = training_dataset_fingerprints(
        series_map, generated
    )
    source_version = code_hash()
    feature_version = hashlib.sha256(
        json.dumps(
            {"windows": FEATURE_WINDOWS, "horizon_steps": HORIZON_STEPS, "timeframe": "M1"},
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    output = []
    for family, family_predictions in predictions.items():
        for instrument, (expected, probability, diagnostics) in sorted(family_predictions.items()):
            raw = usable[instrument]
            quote = raw.get("quote") or {}
            bid, ask, pip = finite(quote.get("bid")), finite(quote.get("ask")), pips[instrument]
            data_cutoff = str(raw.get("feature_origin_utc") or generated)
            training_hash = (
                pooled_training_hash
                if family in {"modern_tabular_probabilistic_repaired", "cross_pair_graph_transfer"}
                else pair_training_hashes[instrument]
            )
            direction = "buy" if expected >= 0.0 else "sell"
            identity = hashlib.sha256(
                f"{family}|{instrument}|{HORIZON_SEC}|{data_cutoff}".encode("utf-8")
            ).hexdigest()
            spread = max(0.0, (ask - bid) / pip)
            output.append(
                {
                    "id": f"proof_{identity}",
                    "lane_id": f"{family}.h1.proof",
                    "family": family,
                    "profile": "canonical_proof",
                    "model_id": f"{family}.h1",
                    "model_version": f"sha256:{source_version}",
                    "feature_version": f"sha256:{feature_version}",
                    "cohort_id": (cohort_ids or {}).get(family, "unregistered_test_cohort"),
                    "forecast_contract_version": FORECAST_CONTRACT_VERSION,
                    "cost_model_version": COST_MODEL_VERSION,
                    "training_cutoff_utc": data_cutoff,
                    "training_dataset_sha256": f"sha256:{training_hash}",
                    "data_cutoff_utc": data_cutoff,
                    "input_timeframe": "M1",
                    "training_timeframe": "M1",
                    "kind": "signal",
                    "instrument": instrument,
                    "direction": direction,
                    "entry_time": generated,
                    "entry_bid": bid,
                    "entry_ask": ask,
                    "pip": pip,
                    "entry_spread_pips": round(spread, 6),
                    "predicted_magnitude_pips": round(abs(expected), 6),
                    "probability_up": round(probability, 8),
                    "expected_signed_pips": round(expected, 6),
                    "expected_after_spread_pips": round(abs(expected) - spread, 6),
                    "stop_loss_pips": max(2.0 * spread, 5.0),
                    "take_profit_r": 1.5,
                    "blocked_reason": "proof_shadow_only",
                    "miss_class": "proof_predictor",
                    "research_only": True,
                    "account_eligible": False,
                    "can_place_orders": False,
                    "provider_quote_time": quote.get("time"),
                    "proof_diagnostics": diagnostics,
                }
            )
    return output


def run(args: argparse.Namespace) -> None:
    store = ShadowOutcomeStore(args.database, batch_size=512, flush_sec=1.0)
    last_bucket = -1
    totals = {family: 0 for family in FAMILIES}
    cycles = errors = 0
    cohort_ids: dict[str, str] = {}
    governance = read_json(args.governance_config)
    prior_counts = pre_governance_counts(args.database)
    started = time.monotonic()
    try:
        while args.duration_sec <= 0 or time.monotonic() - started < args.duration_sec:
            snapshot = read_json(args.snapshot)
            generated_epoch = finite(snapshot.get("generated_epoch"))
            age = time.time() - generated_epoch if generated_epoch > 0.0 else math.inf
            bucket = int(generated_epoch // max(60, args.cadence_sec)) if generated_epoch > 0.0 else -1
            last_error = ""
            forecasts: list[dict[str, Any]] = []
            if age <= args.max_snapshot_age_sec and bucket > last_bucket:
                try:
                    if not cohort_ids:
                        instruments = snapshot.get("instruments") or {}
                        series_map = {
                            str(pair): m1_series(raw)
                            for pair, raw in instruments.items()
                            if isinstance(raw, dict) and len(m1_series(raw)) >= 180
                        }
                        _, pooled_hash = training_dataset_fingerprints(
                            series_map, str(snapshot.get("generated_utc") or utc_now())
                        )
                        feature_version = hashlib.sha256(
                            json.dumps(
                                {"windows": FEATURE_WINDOWS, "horizon_steps": HORIZON_STEPS, "timeframe": "M1"},
                                sort_keys=True,
                            ).encode("utf-8")
                        ).hexdigest()
                        cohort_ids = ensure_cohorts(
                            args.cohort_database,
                            args.cohort_state,
                            cohort_specifications(
                                source_sha256=code_hash(),
                                feature_version=feature_version,
                                training_cutoff_utc=str(snapshot.get("generated_utc") or utc_now()),
                                training_dataset_sha256=pooled_hash,
                                governance=governance,
                                prior_counts=prior_counts,
                            ),
                        )
                    forecasts = run_with_progress_heartbeat(
                        args.heartbeat,
                        {
                            "schema_version": 1,
                            "observation_only": True,
                            "broker_access": False,
                            "can_place_orders": False,
                            "account_eligible": False,
                            "phase": "building_forecasts",
                            "snapshot_age_sec": round(max(0.0, age), 3),
                            "last_snapshot_bucket": last_bucket,
                            "last_forecast_count": 0,
                            "cycles": cycles,
                            "errors": errors,
                            "last_error": "",
                            "totals": totals,
                            "totals_scope": "process_lifetime_since_worker_start",
                            "process_totals": totals,
                            "durable_totals": durable_proof_counts(
                                args.database, cohort_ids
                            ),
                            "durable_totals_source": "canonical_edge_evidence_census",
                            "families": list(FAMILIES),
                            "horizon_sec": HORIZON_SEC,
                            "active_cohorts": cohort_ids,
                            "cohort_registry": str(args.cohort_database),
                        },
                        lambda: build_forecasts(snapshot, cohort_ids=cohort_ids),
                    )
                    for forecast in forecasts:
                        store.observe_forecast(
                            forecast, horizons=[HORIZON_SEC], track_outcome=True
                        )
                        totals[str(forecast["family"])] += 1
                    store.flush(force=True)
                    last_bucket = bucket
                    cycles += 1
                except Exception as exc:
                    errors += 1
                    last_error = f"{type(exc).__name__}: {exc}"
            atomic_json(
                args.heartbeat,
                {
                    "schema_version": 1,
                    "generated_utc": utc_now(),
                    "observation_only": True,
                    "broker_access": False,
                    "can_place_orders": False,
                    "account_eligible": False,
                    "phase": "collecting" if generated_epoch > 0.0 else "waiting_for_snapshot",
                    "snapshot_age_sec": None if math.isinf(age) else round(max(0.0, age), 3),
                    "last_snapshot_bucket": last_bucket,
                    "last_forecast_count": len(forecasts),
                    "cycles": cycles,
                    "errors": errors,
                    "last_error": last_error,
                    "totals": totals,
                    "totals_scope": "process_lifetime_since_worker_start",
                    "process_totals": totals,
                    "durable_totals": durable_proof_counts(
                        args.database, cohort_ids
                    ),
                    "durable_totals_scope": "active_immutable_proof_cohort_ledger",
                    "durable_totals_source": "canonical_edge_evidence_census",
                    "families": list(FAMILIES),
                    "horizon_sec": HORIZON_SEC,
                    "active_cohorts": cohort_ids,
                    "cohort_registry": str(args.cohort_database),
                },
            )
            time.sleep(max(1.0, args.poll_sec))
    finally:
        store.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--cohort-database", type=Path, default=DEFAULT_COHORT_DATABASE)
    parser.add_argument("--cohort-state", type=Path, default=DEFAULT_COHORT_STATE)
    parser.add_argument("--governance-config", type=Path, default=DEFAULT_GOVERNANCE_CONFIG)
    parser.add_argument("--cadence-sec", type=int, default=CADENCE_SEC)
    parser.add_argument("--max-snapshot-age-sec", type=float, default=300.0)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    return parser


def main() -> None:
    run(build_parser().parse_args())


if __name__ == "__main__":
    main()


__all__ = [
    "FAMILIES",
    "FROZEN_PREDICTION_SOURCE_SHA256",
    "atomic_json",
    "build_forecasts",
    "code_hash",
    "currency_factor_matrix",
    "feature_vector",
    "graph_predictions",
    "normal_probability_up",
    "ridge_fit_predict",
    "run_with_progress_heartbeat",
    "state_space_predictions",
    "supervised_rows",
]
