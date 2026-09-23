#!/usr/bin/env python3
"""Research-only validation programme for the mandatory forex model registry."""

from __future__ import annotations

from pathlib import Path
import site


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
LEGACY_SITE_PACKAGES = PROJECT_ROOT / "..venv" / "Lib" / "site-packages"
if LEGACY_SITE_PACKAGES.exists():
    site.addsitedir(str(LEGACY_SITE_PACKAGES))

import argparse
import hashlib
import json
import math
import os
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd
import scipy
import sklearn
import statsmodels
from scipy.stats import spearmanr
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from statsmodels.tsa.statespace.sarimax import SARIMAX

from src.mandatory_model_registry import (
    ARIMA_ORDERS,
    MANDATORY_MODEL_NAMES,
    MandatoryModelSpec,
    build_mandatory_registry,
)


warnings.filterwarnings("ignore")

PIPELINE_VERSION = "mandatory_model_validation_v1"
CALIBRATION_START = pd.Timestamp("2025-09-01T00:00:00Z")
VALIDATION_START = pd.Timestamp("2025-11-01T00:00:00Z")
FINAL_START = pd.Timestamp("2026-01-01T00:00:00Z")
INITIAL_EQUITY_USD = 1_000.0
FIXED_UNITS = 1_000
ROUND_TRIP_SLIPPAGE_PIPS = 0.4
MAJOR_PAIRS = {
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
    "AUD_USD",
    "NZD_USD",
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(json_safe(payload), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def reproducibility_contract(
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: list[str],
    workers: int,
    maxiter: int,
    quick: bool,
) -> dict[str, Any]:
    run_name = output_dir.name
    command = (
        ".\\data\\oanda_training_manager\\.research_py313\\Scripts\\python.exe "
        ".\\fresh_m1_intrahour\\run_pipeline.py mandatory-model-validation "
        f"--start {start} --end {end} --tier {tier} --pairs {','.join(pairs)} "
        f"--workers {workers} --maxiter {maxiter} --run-name {run_name}"
    )
    if quick:
        command += " --quick"
    report_relative = output_dir.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    return {
        "command": command,
        "entrypoint_path": "fresh_m1_intrahour/run_pipeline.py",
        "worker_source_path": "fresh_m1_intrahour/mandatory_model_validation_worker.py",
        "registry_source_path": "fresh_m1_intrahour/src/mandatory_model_registry.py",
        "launcher_source_path": "fresh_m1_intrahour/src/mandatory_model_launcher.py",
        "config_path": "fresh_m1_intrahour/config.json",
        "data_manifest_path": f"{report_relative}/MANDATORY_MODEL_DATA_MANIFEST.json",
        "start": start,
        "end": end,
        "tier": tier,
        "pairs": pairs,
        "workers": int(workers),
        "maxiter": int(maxiter),
        "quick": bool(quick),
        "random_seed": 42,
        "calibration_start": CALIBRATION_START.isoformat(),
        "validation_start": VALIDATION_START.isoformat(),
        "final_diagnostic_start": FINAL_START.isoformat(),
        "final_is_previously_available": True,
        "canonical_holdout_required_after": "2026-07-07T23:59:59Z",
        "execution_mode": "offline_research",
    }


def build_data_manifest(
    *,
    start: str,
    end: str,
    pairs: list[str],
    pair_diagnostics: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for pair in pairs:
        source = PROJECT_ROOT / "data" / "oanda_training_manager" / "candles" / f"{pair}_M1.csv"
        stat = source.stat()
        diagnostics = (pair_diagnostics or {}).get(pair, {})
        files.append(
            {
                "pair": pair,
                "source_path": source.relative_to(PROJECT_ROOT).as_posix(),
                "bytes": int(stat.st_size),
                "modified_time_ns": int(stat.st_mtime_ns),
                "sha256": file_sha256(source),
                "loaded_rows": diagnostics.get("rows_m1"),
            }
        )
    return {
        "manifest_version": "mandatory_model_data_v1",
        "source_type": "local_oanda_training_m1_candles",
        "requested_start": start,
        "requested_end": end,
        "pair_count": len(pairs),
        "pairs": pairs,
        "files": files,
        "hash_algorithm": "sha256",
        "large_files_copied_to_vault": False,
        "replication_contract": "Use the exact source paths and SHA-256 values; the vault catalogues these candle files by reference.",
    }


def pip_size(pair: str) -> float:
    return 0.01 if pair.endswith("_JPY") else 0.0001


def fallback_spread(pair: str) -> float:
    defaults = {
        "EUR_USD": 1.0,
        "GBP_USD": 1.3,
        "USD_JPY": 1.2,
        "USD_CHF": 1.4,
        "USD_CAD": 1.4,
        "AUD_USD": 1.2,
        "NZD_USD": 1.4,
    }
    return defaults.get(pair, 1.8)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def load_m1(pair: str, start: str, end: str) -> pd.DataFrame:
    path = PROJECT_ROOT / "data" / "oanda_training_manager" / "candles" / f"{pair}_M1.csv"
    columns = [
        "time",
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
    ]
    frame = pd.read_csv(path, usecols=lambda col: col in columns, low_memory=False)
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["time", "open", "high", "low", "close"])
    frame = frame[(frame["time"] >= pd.Timestamp(start, tz="UTC")) & (frame["time"] <= pd.Timestamp(end, tz="UTC"))]
    frame = frame.sort_values("time").drop_duplicates("time", keep="last").set_index("time")
    pip = pip_size(pair)
    spread = _numeric(frame, "spread_pips")
    native = (_numeric(frame, "ask_close") - _numeric(frame, "bid_close")) / pip
    spread = spread.where(spread.notna(), native)
    spread = spread.replace([np.inf, -np.inf], np.nan)
    spread = spread.where(spread > 0.0)
    spread = spread.fillna(spread.rolling(240, min_periods=20).median()).fillna(fallback_spread(pair)).clip(0.1, 20.0)
    frame["spread_pips"] = spread
    for suffix in ("open", "high", "low", "close"):
        mid = _numeric(frame, suffix)
        bid = _numeric(frame, f"bid_{suffix}")
        ask = _numeric(frame, f"ask_{suffix}")
        half = spread * pip / 2.0
        frame[f"exec_bid_{suffix}"] = bid.where(bid.notna() & ask.notna(), mid - half)
        frame[f"exec_ask_{suffix}"] = ask.where(bid.notna() & ask.notna(), mid + half)
    return frame


def resample_exec(frame: pd.DataFrame, rule: str, pair: str) -> pd.DataFrame:
    aggregate: dict[str, str] = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "spread_pips": "mean",
        "exec_bid_open": "first",
        "exec_bid_high": "max",
        "exec_bid_low": "min",
        "exec_bid_close": "last",
        "exec_ask_open": "first",
        "exec_ask_high": "max",
        "exec_ask_low": "min",
        "exec_ask_close": "last",
    }
    out = frame.resample(rule, label="left", closed="left").agg(aggregate)
    out = out.dropna(subset=["open", "high", "low", "close", "exec_bid_open", "exec_ask_open"])
    pip = pip_size(pair)
    out["actual_mid_pips"] = (out["close"] - out["open"]) / pip
    out["long_net_pips"] = (out["exec_bid_close"] - out["exec_ask_open"]) / pip - ROUND_TRIP_SLIPPAGE_PIPS
    out["short_net_pips"] = (out["exec_bid_open"] - out["exec_ask_close"]) / pip - ROUND_TRIP_SLIPPAGE_PIPS
    out["long_gross_pips"] = out["actual_mid_pips"]
    out["short_gross_pips"] = -out["actual_mid_pips"]
    out["range_pips"] = (out["high"] - out["low"]) / pip
    return out


def add_lagged_features(frame: pd.DataFrame, pair: str, train_end: pd.Timestamp) -> pd.DataFrame:
    out = frame.copy()
    pip = pip_size(pair)
    previous_close = out["close"].shift(1)
    true_range = pd.concat(
        [
            (out["high"] - out["low"]).abs(),
            (out["high"] - previous_close).abs(),
            (out["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr_pips"] = (true_range.rolling(14, min_periods=7).mean().shift(1) / pip).clip(lower=0.01)
    returns_pips = out["close"].diff() / pip
    out["lag1_pips"] = returns_pips.shift(1)
    out["cum6_pips"] = (out["close"].shift(1) - out["close"].shift(7)) / pip
    out["cum24_pips"] = (out["close"].shift(1) - out["close"].shift(25)) / pip
    out["vol24_pips"] = returns_pips.rolling(24, min_periods=12).std().shift(1).clip(lower=0.01)
    out["vol120_pips"] = returns_pips.rolling(120, min_periods=40).std().shift(1).clip(lower=0.01)
    daily = out.resample("1D").agg({"high": "max", "low": "min", "close": "last"})
    daily["adr_pips"] = ((daily["high"] - daily["low"]) / pip).rolling(20, min_periods=5).mean().shift(1)
    out["adr_pips"] = daily["adr_pips"].reindex(out.index, method="ffill")
    train = out[out.index < train_end]
    low_vol = float(train["vol24_pips"].quantile(0.33)) if len(train) else 0.0
    high_vol = float(train["vol24_pips"].quantile(0.67)) if len(train) else math.inf
    out["volatility_regime"] = np.select(
        [out["vol24_pips"] <= low_vol, out["vol24_pips"] >= high_vol],
        [0, 2],
        default=1,
    )
    out["trend_regime"] = np.sign(out["cum24_pips"]).astype(float)
    out["event_score"] = (out["cum6_pips"].abs() / out["atr_pips"].replace(0.0, np.nan)).clip(0.0, 20.0)
    out["range_failure_flag"] = ((out["volatility_regime"].shift(1) == 0) & (out["volatility_regime"] >= 1)).astype(int)
    return out


def path_exit_outcomes(m1: pd.DataFrame, h1: pd.DataFrame, pair: str) -> pd.DataFrame:
    pip = pip_size(pair)
    results: dict[pd.Timestamp, dict[str, float]] = {}
    for timestamp, group in m1.groupby(m1.index.floor("h"), sort=False):
        if timestamp not in h1.index or timestamp < CALIBRATION_START:
            continue
        first = group.iloc[0]
        last = group.iloc[-1]
        values: dict[str, float] = {}
        for side in ("long", "short"):
            if side == "long":
                entry = float(first["exec_ask_open"])
                endpoint = (float(last["exec_bid_close"]) - entry) / pip - ROUND_TRIP_SLIPPAGE_PIPS
                favorable = group["exec_bid_high"].to_numpy(dtype=float)
                adverse = group["exec_bid_low"].to_numpy(dtype=float)
                tp_result = endpoint
                for high, low in zip(favorable, adverse):
                    if (entry - low) / pip >= 12.0:
                        tp_result = -12.0 - ROUND_TRIP_SLIPPAGE_PIPS
                        break
                    if (high - entry) / pip >= 8.0:
                        tp_result = 8.0 - ROUND_TRIP_SLIPPAGE_PIPS
                        break
                peak = entry
                trail_result = endpoint
                for high, low in zip(favorable, adverse):
                    stop = peak - 8.0 * pip
                    if low <= stop:
                        trail_result = (stop - entry) / pip - ROUND_TRIP_SLIPPAGE_PIPS
                        break
                    peak = max(peak, high)
            else:
                entry = float(first["exec_bid_open"])
                endpoint = (entry - float(last["exec_ask_close"])) / pip - ROUND_TRIP_SLIPPAGE_PIPS
                favorable = group["exec_ask_low"].to_numpy(dtype=float)
                adverse = group["exec_ask_high"].to_numpy(dtype=float)
                tp_result = endpoint
                for low, high in zip(favorable, adverse):
                    if (high - entry) / pip >= 12.0:
                        tp_result = -12.0 - ROUND_TRIP_SLIPPAGE_PIPS
                        break
                    if (entry - low) / pip >= 8.0:
                        tp_result = 8.0 - ROUND_TRIP_SLIPPAGE_PIPS
                        break
                trough = entry
                trail_result = endpoint
                for low, high in zip(favorable, adverse):
                    stop = trough + 8.0 * pip
                    if high >= stop:
                        trail_result = (entry - stop) / pip - ROUND_TRIP_SLIPPAGE_PIPS
                        break
                    trough = min(trough, low)
            values[f"{side}_tp_sl_pips"] = float(tp_result)
            values[f"{side}_trailing_pips"] = float(trail_result)
        results[timestamp] = values
    return pd.DataFrame.from_dict(results, orient="index")


def fallback_ar_signal(close: pd.Series, order: tuple[int, int, int], pip: float) -> pd.Series:
    p, d, _ = order
    values = np.log(close.astype(float)).to_numpy()
    diff = np.diff(values, n=max(d, 1))
    lag = max(p, 1)
    if len(diff) <= lag + 20:
        return pd.Series(0.0, index=close.index)
    x = np.column_stack([diff[lag - offset - 1 : -offset - 1 if offset >= 0 else None] for offset in range(lag)])
    y = diff[lag:]
    train_positions = np.flatnonzero(close.index[lag + max(d, 1) :] < CALIBRATION_START)
    n_train = int(train_positions[-1] + 1) if len(train_positions) else min(len(y), 500)
    beta, *_ = np.linalg.lstsq(np.c_[np.ones(n_train), x[:n_train]], y[:n_train], rcond=None)
    predicted = np.zeros(len(close))
    start = lag + max(d, 1)
    predicted[start : start + len(x)] = np.c_[np.ones(len(x)), x] @ beta
    return pd.Series(predicted * close.shift(1).to_numpy(dtype=float) / pip, index=close.index).replace([np.inf, -np.inf], np.nan)


def fit_sarimax_model(model: SARIMAX, maxiter: int) -> tuple[Any, list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    fit = model.fit(disp=False, maxiter=maxiter, method="lbfgs")
    attempts.append(
        {
            "method": "lbfgs",
            "converged": bool(getattr(fit, "mle_retvals", {}).get("converged", True)),
            "iterations": int(getattr(fit, "mle_retvals", {}).get("iterations", 0)),
        }
    )
    if attempts[-1]["converged"]:
        return fit, attempts
    retry = model.fit(
        disp=False,
        maxiter=max(100, maxiter),
        method="powell",
        start_params=fit.params,
    )
    attempts.append(
        {
            "method": "powell",
            "converged": bool(getattr(retry, "mle_retvals", {}).get("converged", True)),
            "iterations": int(getattr(retry, "mle_retvals", {}).get("iterations", 0)),
        }
    )
    return retry, attempts


def forecast_stability(signal: pd.Series, actual: pd.Series) -> dict[str, Any]:
    aligned = pd.concat([signal.rename("signal"), actual.rename("actual")], axis=1).dropna()
    if aligned.empty:
        return {"stable": False, "reason": "no finite aligned forecasts"}
    max_abs = float(aligned["signal"].abs().max())
    actual_q99 = float(aligned["actual"].abs().quantile(0.99))
    stability_limit = max(100.0, 10.0 * actual_q99)
    rmse = float(np.sqrt(np.mean(np.square(aligned["actual"] - aligned["signal"]))))
    baseline_rmse = float(np.sqrt(np.mean(np.square(aligned["actual"]))))
    stable = bool(
        np.isfinite(max_abs)
        and np.isfinite(rmse)
        and max_abs <= stability_limit
        and rmse <= max(1e-9, 5.0 * baseline_rmse)
    )
    return {
        "stable": stable,
        "max_abs_prediction_pips": max_abs,
        "actual_abs_q99_pips": actual_q99,
        "stability_limit_pips": stability_limit,
        "rmse_pips": rmse,
        "zero_baseline_rmse_pips": baseline_rmse,
        "reason": None if stable else "forecast exceeded causal stability limits",
    }


def arima_signal(
    frame: pd.DataFrame,
    pair: str,
    order: tuple[int, int, int],
    max_train_rows: int,
    maxiter: int,
) -> tuple[pd.Series, dict[str, Any]]:
    close = frame["close"].astype(float)
    log_close = np.log(close)
    scale = 10_000.0
    if order[1] == 0:
        modeled = log_close.diff() * scale
        transform = "scaled_log_returns"
    else:
        anchor = float(log_close.iloc[0])
        modeled = (log_close - anchor) * scale
        transform = "centered_scaled_log_price"
    train = modeled[modeled.index < CALIBRATION_START].dropna().tail(max_train_rows)
    diagnostic: dict[str, Any] = {
        "order": list(order),
        "converged": False,
        "fallback": False,
        "transform": transform,
        "scale": scale,
    }
    try:
        fit_model = SARIMAX(
            train,
            order=order,
            trend="n",
            enforce_stationarity=True,
            enforce_invertibility=True,
        )
        fit, attempts = fit_sarimax_model(fit_model, maxiter)
        full_model = SARIMAX(
            modeled.fillna(0.0),
            order=order,
            trend="n",
            enforce_stationarity=True,
            enforce_invertibility=True,
        )
        filtered = full_model.filter(fit.params)
        predicted = filtered.get_prediction(start=1, end=len(modeled) - 1, dynamic=False).predicted_mean
        predicted = predicted.reindex(log_close.index)
        if order[1] == 0:
            signal = predicted / scale * close.shift(1) / pip_size(pair)
        else:
            predicted_level = predicted / scale + anchor
            signal = (np.exp(predicted_level) - close.shift(1)) / pip_size(pair)
        stability = forecast_stability(signal, frame["actual_mid_pips"])
        diagnostic.update(
            {
                "converged": bool(getattr(fit, "mle_retvals", {}).get("converged", True)),
                "aic": float(fit.aic) if np.isfinite(fit.aic) else None,
                "bic": float(fit.bic) if np.isfinite(fit.bic) else None,
                "train_rows": int(len(train)),
                "attempts": attempts,
                "stability": stability,
            }
        )
        if not diagnostic["converged"]:
            diagnostic["error"] = "optimizer did not converge after retry"
        elif not stability["stable"]:
            diagnostic["error"] = str(stability["reason"])
        return signal.replace([np.inf, -np.inf], np.nan), diagnostic
    except Exception as exc:
        diagnostic.update({"fallback": True, "error": f"{type(exc).__name__}: {exc}"})
        return fallback_ar_signal(close, order, pip_size(pair)), diagnostic


def sarimax_exog_signal(
    frame: pd.DataFrame,
    pair: str,
    seasonal: bool,
    maxiter: int,
) -> tuple[pd.Series, dict[str, Any]]:
    pip = pip_size(pair)
    scale = 10_000.0
    returns = np.log(frame["close"].astype(float)).diff() * scale
    hour = frame.index.hour.to_numpy(dtype=float)
    exog = pd.DataFrame(
        {
            "spread": frame["spread_pips"].shift(1),
            "atr": frame["atr_pips"],
            "momentum": frame["cum6_pips"],
            "hour_sin": np.sin(2.0 * np.pi * hour / 24.0),
            "hour_cos": np.cos(2.0 * np.pi * hour / 24.0),
        },
        index=frame.index,
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    train_mask = (frame.index < CALIBRATION_START) & returns.notna()
    train_y = returns[train_mask].tail(5000)
    raw_train_x = exog.loc[train_y.index]
    exog_median = raw_train_x.median()
    exog_scale = (raw_train_x.quantile(0.75) - raw_train_x.quantile(0.25)).clip(lower=1.0)
    exog = (exog - exog_median) / exog_scale
    train_x = exog.loc[train_y.index]
    seasonal_order = (1, 0, 0, 24) if seasonal else (0, 0, 0, 0)
    diagnostic: dict[str, Any] = {
        "seasonal_order": list(seasonal_order),
        "converged": False,
        "target_transform": "scaled_log_returns",
        "target_scale": scale,
        "exog_scaling": "calibration_median_iqr_min_1",
    }
    try:
        fit_model = SARIMAX(
            train_y,
            exog=train_x,
            order=(1, 0, 1),
            seasonal_order=seasonal_order,
            trend="n",
            enforce_stationarity=True,
            enforce_invertibility=True,
        )
        fit, attempts = fit_sarimax_model(fit_model, maxiter)
        full_y = returns.fillna(0.0)
        full_model = SARIMAX(
            full_y,
            exog=exog,
            order=(1, 0, 1),
            seasonal_order=seasonal_order,
            trend="n",
            enforce_stationarity=True,
            enforce_invertibility=True,
        )
        filtered = full_model.filter(fit.params)
        predicted_return = filtered.get_prediction(start=1, end=len(full_y) - 1, dynamic=False).predicted_mean.reindex(frame.index)
        signal = predicted_return / scale * frame["close"].shift(1) / pip
        stability = forecast_stability(signal, frame["actual_mid_pips"])
        diagnostic.update(
            {
                "converged": bool(getattr(fit, "mle_retvals", {}).get("converged", True)),
                "aic": float(fit.aic) if np.isfinite(fit.aic) else None,
                "train_rows": int(len(train_y)),
                "attempts": attempts,
                "stability": stability,
            }
        )
        if not diagnostic["converged"]:
            diagnostic["error"] = "optimizer did not converge after retry"
        elif not stability["stable"]:
            diagnostic["error"] = str(stability["reason"])
        return signal.replace([np.inf, -np.inf], np.nan), diagnostic
    except Exception as exc:
        diagnostic["error"] = f"{type(exc).__name__}: {exc}"
        return pd.Series(0.0, index=frame.index), diagnostic


def alpha_beta_signal(
    close: pd.Series,
    pair: str,
    *,
    alpha: float,
    beta: float,
    adaptive: bool = False,
    regime: pd.Series | None = None,
) -> pd.Series:
    values = close.to_numpy(dtype=float)
    forecasts = np.full(len(values), np.nan)
    level = float(values[0])
    trend = 0.0
    scale = float(np.nanstd(np.diff(np.log(values[close.index < CALIBRATION_START]))))
    scale = max(scale, 1e-9)
    for index in range(1, len(values)):
        prediction = level + trend
        forecasts[index] = prediction
        residual = values[index] - prediction
        a = alpha
        b = beta
        if adaptive:
            ratio = min(abs(residual / max(abs(prediction), 1e-9)) / scale, 4.0)
            a = min(0.65, max(0.05, alpha * (0.5 + ratio)))
            b = min(0.25, max(0.005, beta * (0.5 + ratio)))
        if regime is not None and index > 0:
            state = int(regime.iloc[index - 1])
            a *= {0: 0.6, 1: 1.0, 2: 1.5}.get(state, 1.0)
            b *= {0: 0.5, 1: 1.0, 2: 1.5}.get(state, 1.0)
        level = prediction + a * residual
        trend = trend + b * residual
    return pd.Series((forecasts - close.shift(1).to_numpy(dtype=float)) / pip_size(pair), index=close.index)


def hidden_markov_signal(frame: pd.DataFrame, pair: str) -> tuple[pd.Series, dict[str, Any]]:
    returns = (frame["close"].diff() / pip_size(pair)).fillna(0.0)
    train = returns[returns.index < CALIBRATION_START].to_numpy(dtype=float)
    if len(train) < 100:
        return pd.Series(0.0, index=frame.index), {"states": 0}
    kmeans = KMeans(n_clusters=2, random_state=42, n_init=20).fit(train.reshape(-1, 1))
    labels = kmeans.labels_
    means = np.array([train[labels == state].mean() for state in range(2)])
    stds = np.array([max(train[labels == state].std(), 0.1) for state in range(2)])
    transition = np.ones((2, 2), dtype=float)
    for previous, current in zip(labels[:-1], labels[1:]):
        transition[int(previous), int(current)] += 1.0
    transition /= transition.sum(axis=1, keepdims=True)
    probability = np.array([0.5, 0.5], dtype=float)
    forecast = np.zeros(len(returns), dtype=float)
    for index, observed in enumerate(returns.to_numpy(dtype=float)):
        prior = probability @ transition
        forecast[index] = float(prior @ means)
        likelihood = np.exp(-0.5 * ((observed - means) / stds) ** 2) / stds
        posterior = prior * likelihood
        probability = posterior / posterior.sum() if posterior.sum() > 0 else prior
    return pd.Series(forecast, index=frame.index), {"states": 2, "means": means.tolist(), "transition": transition.tolist()}


def markov_regime_signal(frame: pd.DataFrame, pair: str) -> tuple[pd.Series, dict[str, Any]]:
    returns = (frame["close"].diff() / pip_size(pair)).fillna(0.0)
    train_mask = frame.index < CALIBRATION_START
    threshold = float(returns[train_mask].abs().quantile(0.60))
    states = np.select([returns < -threshold, returns > threshold], [0, 2], default=1).astype(int)
    transition = np.ones((3, 3), dtype=float)
    train_states = states[train_mask]
    for previous, current in zip(train_states[:-1], train_states[1:]):
        transition[int(previous), int(current)] += 1.0
    transition /= transition.sum(axis=1, keepdims=True)
    state_means = np.array(
        [
            float(returns[train_mask][train_states == state].mean()) if np.any(train_states == state) else 0.0
            for state in range(3)
        ]
    )
    forecast = np.zeros(len(frame), dtype=float)
    for index in range(1, len(frame)):
        forecast[index] = float(transition[states[index - 1]] @ state_means)
    return pd.Series(forecast, index=frame.index), {"states": 3, "transition": transition.tolist(), "means": state_means.tolist()}


def trend_classifier_signal(frame: pd.DataFrame, pair: str) -> tuple[pd.Series, dict[str, Any]]:
    feature_columns = ["lag1_pips", "cum6_pips", "cum24_pips", "vol24_pips", "atr_pips", "event_score"]
    features = frame[feature_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    target = (frame["actual_mid_pips"] > 0.0).astype(int)
    train_mask = (frame.index < CALIBRATION_START) & frame[feature_columns].notna().all(axis=1)
    if int(train_mask.sum()) < 200 or target[train_mask].nunique() < 2:
        return pd.Series(0.0, index=frame.index), {"fit": False}
    model = LogisticRegression(C=0.5, max_iter=500, random_state=42)
    model.fit(features[train_mask], target[train_mask])
    probability = model.predict_proba(features)[:, 1]
    magnitude = frame["atr_pips"].fillna(frame["atr_pips"].median()).to_numpy(dtype=float) * 0.25
    signal = (probability - 0.5) * 2.0 * magnitude
    return pd.Series(signal, index=frame.index), {"fit": True, "coefficients": model.coef_[0].tolist()}


def _surface_columns(frame: pd.DataFrame) -> list[str]:
    base = [
        "timestamp",
        "pair",
        "timeframe",
        "open",
        "close",
        "spread_pips",
        "actual_mid_pips",
        "long_net_pips",
        "short_net_pips",
        "long_gross_pips",
        "short_gross_pips",
        "range_pips",
        "atr_pips",
        "adr_pips",
        "cum6_pips",
        "cum24_pips",
        "vol24_pips",
        "vol120_pips",
        "volatility_regime",
        "trend_regime",
        "event_score",
        "range_failure_flag",
        "long_tp_sl_pips",
        "short_tp_sl_pips",
        "long_trailing_pips",
        "short_trailing_pips",
    ]
    signals = [column for column in frame if column.startswith("signal_")]
    return [column for column in base + signals if column in frame]


def build_pair_surfaces(
    pair: str,
    start: str,
    end: str,
    output_dir: str,
    quick: bool,
    maxiter: int,
) -> dict[str, Any]:
    pair_start = time.perf_counter()
    m1 = load_m1(pair, start, end)
    surfaces: dict[str, str] = {}
    diagnostics: dict[str, Any] = {"pair": pair, "rows_m1": int(len(m1)), "fits": {}}
    h1 = add_lagged_features(resample_exec(m1, "1h", pair), pair, CALIBRATION_START)
    path_outcomes = path_exit_outcomes(m1, h1, pair)
    h1 = h1.join(path_outcomes)
    for column in ("long_tp_sl_pips", "short_tp_sl_pips", "long_trailing_pips", "short_trailing_pips"):
        if column not in h1:
            h1[column] = np.nan

    orders = ARIMA_ORDERS
    for order in orders:
        name = f"signal_arima_{order[0]}{order[1]}{order[2]}"
        signal, diag = arima_signal(h1, pair, order, max_train_rows=2500 if quick else 5000, maxiter=maxiter)
        h1[name] = signal
        diagnostics["fits"][name] = diag
    atr_base_signal, atr_base_diag = arima_signal(
        h1,
        pair,
        (2, 0, 1),
        max_train_rows=2500 if quick else 5000,
        maxiter=maxiter,
    )
    h1["signal_arima_201"] = atr_base_signal
    diagnostics["fits"]["signal_arima_201"] = atr_base_diag
    for seasonal, name in ((False, "signal_sarimax_exog"), (True, "signal_sarimax_daily")):
        signal, diag = sarimax_exog_signal(h1, pair, seasonal=seasonal, maxiter=maxiter)
        h1[name] = signal
        diagnostics["fits"][name] = diag

    h1["signal_atr_momentum"] = np.sign(h1["cum6_pips"]) * h1["atr_pips"] * 0.35
    h1["signal_adr_momentum"] = np.sign(h1["cum24_pips"]) * h1["adr_pips"] / 24.0
    h1["signal_cumulative_pips"] = h1["cum6_pips"] / 6.0
    h1["signal_volatility_regime"] = np.sign(h1["cum6_pips"]) * h1["vol24_pips"] * np.where(h1["volatility_regime"] == 2, 1.0, 0.4)
    h1["signal_kalman"] = alpha_beta_signal(h1["close"], pair, alpha=0.20, beta=0.04)
    h1["signal_adaptive_kalman"] = alpha_beta_signal(h1["close"], pair, alpha=0.15, beta=0.03, adaptive=True)
    h1["signal_state_space"] = alpha_beta_signal(h1["close"], pair, alpha=0.10, beta=0.01)
    h1["signal_linear_state_space"] = h1["cum6_pips"] / 6.0
    h1["signal_regime_switching_state"] = alpha_beta_signal(
        h1["close"], pair, alpha=0.12, beta=0.02, regime=h1["volatility_regime"]
    )
    hmm, hmm_diag = hidden_markov_signal(h1, pair)
    h1["signal_hidden_markov"] = hmm
    diagnostics["fits"]["signal_hidden_markov"] = hmm_diag
    markov, markov_diag = markov_regime_signal(h1, pair)
    h1["signal_markov_regime"] = markov
    diagnostics["fits"]["signal_markov_regime"] = markov_diag
    trend, trend_diag = trend_classifier_signal(h1, pair)
    h1["signal_trend_classifier"] = trend
    diagnostics["fits"]["signal_trend_classifier"] = trend_diag
    h1["signal_atr_032"] = h1["signal_arima_201"]

    h1 = h1[h1.index >= CALIBRATION_START].copy()
    h1["timestamp"] = h1.index
    h1["pair"] = pair
    h1["timeframe"] = "H1"
    h1_path = Path(output_dir) / f"surface_{pair}_H1.parquet"
    h1[_surface_columns(h1)].to_parquet(h1_path, index=False, compression="zstd")
    surfaces["H1"] = str(h1_path)

    if pair in MAJOR_PAIRS:
        for timeframe, rule, order, signal_name in (
            ("M5", "5min", (1, 1, 0), "signal_m5_arima"),
            ("M15", "15min", (1, 1, 1), "signal_m15_arima"),
        ):
            tf = add_lagged_features(resample_exec(m1, rule, pair), pair, CALIBRATION_START)
            signal, diag = arima_signal(
                tf,
                pair,
                order,
                max_train_rows=4000 if quick else 20000,
                maxiter=maxiter,
            )
            tf[signal_name] = signal
            diagnostics["fits"][signal_name] = diag
            if timeframe == "M15":
                atr_signal, atr_diag = arima_signal(
                    tf,
                    pair,
                    (2, 0, 1),
                    max_train_rows=4000 if quick else 20000,
                    maxiter=maxiter,
                )
                tf["signal_m15_atr_032"] = atr_signal
                diagnostics["fits"]["signal_m15_atr_032"] = atr_diag
            tf = tf[(tf.index >= CALIBRATION_START) & (tf.index.minute == 0)].copy()
            tf["timestamp"] = tf.index
            tf["pair"] = pair
            tf["timeframe"] = timeframe
            path = Path(output_dir) / f"surface_{pair}_{timeframe}.parquet"
            tf[_surface_columns(tf)].to_parquet(path, index=False, compression="zstd")
            surfaces[timeframe] = str(path)

    diagnostics["elapsed_seconds"] = time.perf_counter() - pair_start
    diagnostics_path = Path(output_dir) / f"diagnostics_{pair}.json"
    write_json(diagnostics_path, diagnostics)
    return {"pair": pair, "surfaces": surfaces, "diagnostics": str(diagnostics_path)}


def rank_ic(predicted: pd.Series, actual: pd.Series) -> float | None:
    valid = pd.DataFrame({"predicted": predicted, "actual": actual}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(valid) < 3 or valid["predicted"].nunique() < 2 or valid["actual"].nunique() < 2:
        return None
    value = spearmanr(valid["predicted"], valid["actual"]).statistic
    return float(value) if np.isfinite(value) else None


def forecast_metrics(frame: pd.DataFrame, signal_column: str) -> dict[str, Any]:
    valid = frame[[signal_column, "actual_mid_pips"]].replace([np.inf, -np.inf], np.nan).dropna()
    if valid.empty:
        return {
            "rows": 0,
            "mae_pips": None,
            "rmse_pips": None,
            "direction_accuracy": None,
            "rank_ic": None,
            "movement_mae_pips": None,
            "prediction_std_pips": None,
            "nonzero_prediction_rate": None,
            "max_abs_prediction_pips": None,
            "actual_abs_q99_pips": None,
            "forecast_stable": False,
        }
    predicted = valid[signal_column].astype(float)
    actual = valid["actual_mid_pips"].astype(float)
    stability = forecast_stability(predicted, actual)
    return {
        "rows": int(len(valid)),
        "mae_pips": float(mean_absolute_error(actual, predicted)),
        "rmse_pips": float(math.sqrt(mean_squared_error(actual, predicted))),
        "zero_baseline_rmse_pips": float(math.sqrt(np.mean(np.square(actual)))),
        "direction_accuracy": float((np.sign(predicted) == np.sign(actual)).mean()),
        "rank_ic": rank_ic(predicted, actual),
        "movement_mae_pips": float(mean_absolute_error(actual.abs(), predicted.abs())),
        "prediction_bias_pips": float((predicted - actual).mean()),
        "prediction_std_pips": float(predicted.std()),
        "nonzero_prediction_rate": float((predicted.abs() > 1e-12).mean()),
        "max_abs_prediction_pips": stability["max_abs_prediction_pips"],
        "actual_abs_q99_pips": stability["actual_abs_q99_pips"],
        "forecast_stability_limit_pips": stability["stability_limit_pips"],
        "forecast_stable": stability["stable"],
    }


def split_name(timestamp: pd.Timestamp) -> str:
    if timestamp < VALIDATION_START:
        return "calibration"
    if timestamp < FINAL_START:
        return "validation"
    return "final_diagnostic"


def add_account_currency_values(surface: pd.DataFrame) -> pd.DataFrame:
    out = surface.copy()
    h1 = out[out["timeframe"] == "H1"]
    pivot = h1.pivot_table(index="timestamp", columns="pair", values="close", aggfunc="last").sort_index()
    quote_rates: dict[str, pd.Series] = {"USD": pd.Series(1.0, index=pivot.index)}
    for currency in ("JPY", "CHF", "CAD"):
        pair = f"USD_{currency}"
        if pair in pivot:
            quote_rates[currency] = 1.0 / pivot[pair]
    for currency in ("EUR", "GBP", "AUD", "NZD"):
        pair = f"{currency}_USD"
        if pair in pivot:
            quote_rates[currency] = pivot[pair]
    rate_lookup: dict[tuple[pd.Timestamp, str], float] = {}
    for currency, series in quote_rates.items():
        for timestamp, value in series.dropna().items():
            rate_lookup[(pd.Timestamp(timestamp), currency)] = float(value)

    def rate(row: pd.Series) -> float:
        quote = str(row["pair"]).split("_")[1]
        timestamp = pd.Timestamp(row["timestamp"]).floor("h")
        value = rate_lookup.get((timestamp, quote))
        if value is not None and np.isfinite(value) and value > 0:
            return value
        if quote == "USD":
            return 1.0
        if str(row["pair"]).startswith("USD_") and float(row["close"]) > 0:
            return 1.0 / float(row["close"])
        return np.nan

    out["quote_to_usd"] = out.apply(rate, axis=1)
    out["pip_value_usd_per_unit"] = [pip_size(pair) for pair in out["pair"]] * out["quote_to_usd"]
    out["split"] = [split_name(pd.Timestamp(timestamp)) for timestamp in out["timestamp"]]
    return out


def choose_signal_by_validation(surface: pd.DataFrame, columns: list[str]) -> tuple[str, list[dict[str, Any]]]:
    validation = surface[(surface["timeframe"] == "H1") & (surface["split"] == "validation")]
    ranking: list[dict[str, Any]] = []
    for column in columns:
        metrics = forecast_metrics(validation, column)
        rank_value = metrics.get("rank_ic") or 0.0
        direction = metrics.get("direction_accuracy") or 0.0
        rmse = metrics.get("rmse_pips") or math.inf
        baseline = metrics.get("zero_baseline_rmse_pips") or 1.0
        score = rank_value + (direction - 0.5) - 0.10 * (rmse / max(baseline, 1e-9))
        ranking.append({"signal": column, "selection_score": float(score), "metrics": metrics})
    ranking.sort(key=lambda row: (row["selection_score"], row["signal"]), reverse=True)
    return ranking[0]["signal"], ranking


def add_derived_signals(surface: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = surface.copy()
    arima_columns = [f"signal_arima_{p}{d}{q}" for p, d, q in ARIMA_ORDERS]
    selected_arima, arima_ranking = choose_signal_by_validation(out, arima_columns)
    selected_sarimax, sarimax_ranking = choose_signal_by_validation(out, ["signal_sarimax_exog", "signal_sarimax_daily"])
    out["signal_arima_selected"] = out[selected_arima]
    out["signal_sarimax_selected"] = out[selected_sarimax]

    h1_mask = out["timeframe"] == "H1"
    validation_mask = h1_mask & (out["split"] == "validation")
    inverse_errors: dict[str, float] = {}
    for column in arima_columns:
        metrics = forecast_metrics(out[validation_mask], column)
        inverse_errors[column] = 1.0 / max(float(metrics.get("rmse_pips") or 1e9), 1e-9)
    weight_total = sum(inverse_errors.values())
    weights = {column: value / weight_total for column, value in inverse_errors.items()}
    out["signal_arima_ensemble"] = out[arima_columns].mean(axis=1)
    out["signal_voting_arima"] = np.sign(out[arima_columns]).sum(axis=1).pipe(np.sign) * out[arima_columns].abs().median(axis=1)
    out["signal_confidence_weighted_ensemble"] = sum(out[column] * weight for column, weight in weights.items())
    out["signal_arima_kalman"] = (out["signal_arima_selected"] + out["signal_kalman"]) / 2.0
    out["signal_arima_state_space"] = (out["signal_arima_selected"] + out["signal_state_space"]) / 2.0

    base_candidates = [
        "signal_arima_selected",
        "signal_sarimax_selected",
        "signal_kalman",
        "signal_adaptive_kalman",
        "signal_trend_classifier",
    ]
    selected_performance, performance_ranking = choose_signal_by_validation(out, base_candidates)
    out["signal_performance_ranked"] = out[selected_performance]

    h1 = out[h1_mask].copy()
    features = [
        "signal_arima_selected",
        "signal_sarimax_selected",
        "signal_kalman",
        "signal_adaptive_kalman",
        "signal_hidden_markov",
        "signal_trend_classifier",
    ]
    calibration = h1[h1["split"] == "calibration"].replace([np.inf, -np.inf], np.nan).dropna(subset=features + ["actual_mid_pips"])
    validation = h1[h1["split"] == "validation"].replace([np.inf, -np.inf], np.nan).dropna(subset=features + ["actual_mid_pips"])
    alphas = [0.1, 1.0, 10.0, 100.0]
    alpha_rows: list[dict[str, Any]] = []
    best_alpha = 10.0
    if len(calibration) >= 200 and len(validation) >= 100:
        for alpha in alphas:
            model = Ridge(alpha=alpha)
            model.fit(calibration[features], calibration["actual_mid_pips"])
            pred = model.predict(validation[features])
            rmse = float(math.sqrt(mean_squared_error(validation["actual_mid_pips"], pred)))
            alpha_rows.append({"alpha": alpha, "validation_rmse_pips": rmse})
        best_alpha = min(alpha_rows, key=lambda row: row["validation_rmse_pips"])["alpha"]
        validation_model = Ridge(alpha=best_alpha).fit(calibration[features], calibration["actual_mid_pips"])
        dev = h1[h1["split"].isin(["calibration", "validation"])].replace([np.inf, -np.inf], np.nan).dropna(subset=features + ["actual_mid_pips"])
        final_model = Ridge(alpha=best_alpha).fit(dev[features], dev["actual_mid_pips"])
        valid_rows = out[h1_mask].replace([np.inf, -np.inf], np.nan).dropna(subset=features)
        meta = pd.Series(np.nan, index=out.index)
        validation_rows = valid_rows[valid_rows["split"] == "validation"]
        final_rows = valid_rows[valid_rows["split"] == "final_diagnostic"]
        meta.loc[validation_rows.index] = validation_model.predict(validation_rows[features])
        meta.loc[final_rows.index] = final_model.predict(final_rows[features])
        out["signal_arima_meta"] = meta
    else:
        out["signal_arima_meta"] = out["signal_confidence_weighted_ensemble"]

    for timeframe, source_column, output_column in (
        ("M5", "signal_m5_arima", "aligned_m5"),
        ("M15", "signal_m15_arima", "aligned_m15"),
    ):
        timeframe_rows = out[out["timeframe"] == timeframe]
        if timeframe_rows.empty or source_column not in timeframe_rows:
            raise ValueError(
                f"mandatory {timeframe} surface is missing; include at least one supported major pair"
            )
        source = timeframe_rows[["timestamp", "pair", source_column]].drop_duplicates(["timestamp", "pair"])
        source = source.rename(columns={source_column: output_column})
        out = out.merge(source, on=["timestamp", "pair"], how="left")
    agreement = (
        (np.sign(out["signal_arima_selected"]) == np.sign(out["aligned_m5"]))
        & (np.sign(out["signal_arima_selected"]) == np.sign(out["aligned_m15"]))
    )
    out["signal_multi_timeframe_consensus"] = np.where(
        agreement,
        out[["signal_arima_selected", "aligned_m5", "aligned_m15"]].mean(axis=1),
        0.0,
    )
    health_hit = (np.sign(out["signal_arima_selected"]) == np.sign(out["actual_mid_pips"])).astype(float)
    out["model_health_score"] = (
        health_hit.groupby(out["pair"]).transform(lambda series: series.rolling(100, min_periods=20).mean().shift(1)).fillna(0.5)
    )
    out["signal_model_health"] = out["signal_arima_selected"] * (2.0 * out["model_health_score"] - 1.0)
    out["signal_arima_122"] = out["signal_arima_122"]
    out["signal_m15_atr_032"] = out.get("signal_m15_atr_032", np.nan)
    selection = {
        "selected_arima_signal": selected_arima,
        "arima_validation_ranking": arima_ranking,
        "selected_sarimax_signal": selected_sarimax,
        "sarimax_validation_ranking": sarimax_ranking,
        "selected_performance_signal": selected_performance,
        "performance_validation_ranking": performance_ranking,
        "arima_ensemble_weights": weights,
        "meta_model_alpha": best_alpha,
        "meta_model_alpha_validation": alpha_rows,
    }
    return out, selection


def signal_column_for(spec: MandatoryModelSpec) -> str:
    mapping = {
        "arima_selected": "signal_arima_selected",
        "sarimax_selected": "signal_sarimax_selected",
        "atr_momentum": "signal_atr_momentum",
        "adr_momentum": "signal_adr_momentum",
        "cumulative_pips": "signal_cumulative_pips",
        "volatility_regime": "signal_volatility_regime",
        "m5_arima": "signal_m5_arima",
        "m15_arima": "signal_m15_arima",
        "m15_atr_032": "signal_m15_atr_032",
        "atr_032": "signal_atr_032",
        "kalman": "signal_kalman",
        "adaptive_kalman": "signal_adaptive_kalman",
        "state_space": "signal_state_space",
        "linear_state_space": "signal_linear_state_space",
        "regime_switching_state": "signal_regime_switching_state",
        "hidden_markov": "signal_hidden_markov",
        "markov_regime": "signal_markov_regime",
        "trend_classifier": "signal_trend_classifier",
        "arima_kalman": "signal_arima_kalman",
        "arima_state_space": "signal_arima_state_space",
        "arima_meta": "signal_arima_meta",
        "arima_ensemble": "signal_arima_ensemble",
        "voting_arima": "signal_voting_arima",
        "confidence_weighted_ensemble": "signal_confidence_weighted_ensemble",
        "performance_ranked": "signal_performance_ranked",
        "multi_timeframe_consensus": "signal_multi_timeframe_consensus",
        "model_health": "signal_model_health",
    }
    if spec.signal.startswith("arima_") and spec.signal[6:].isdigit():
        return f"signal_{spec.signal}"
    if spec.signal not in mapping:
        raise KeyError(f"no signal mapping for {spec.model_name}: {spec.signal}")
    return mapping[spec.signal]


def apply_static_policy(frame: pd.DataFrame, spec: MandatoryModelSpec, signal_column: str) -> pd.DataFrame:
    work = frame.copy()
    work["signal"] = pd.to_numeric(work[signal_column], errors="coerce")
    work["eligible"] = work["signal"].notna() & (work["signal"].abs() > 1e-12)
    work["size_multiplier"] = 1.0
    policy = spec.policy
    parameters = spec.parameters
    policy_selection: dict[str, Any] = {}
    if policy == "event_gate":
        work["eligible"] &= work["event_score"] >= float(parameters.get("event_threshold", 0.91))
    elif policy == "atr_gate":
        work["eligible"] &= work["signal"].abs() >= float(parameters.get("atr_multiplier", 0.7)) * work["atr_pips"]
    elif policy == "edge_gate":
        work["eligible"] &= work["signal"].abs() >= 1.25 * (work["spread_pips"] + ROUND_TRIP_SLIPPAGE_PIPS)
    elif policy == "agreement_gate":
        work["eligible"] &= work["signal"].abs() > 0.0
    elif policy == "regime_gate":
        work["eligible"] &= np.sign(work["signal"]) == np.sign(work["trend_regime"])
    elif policy == "volatility_gate":
        work["eligible"] &= work["volatility_regime"] != 2
    elif policy == "regime_controller":
        work["eligible"] &= work["volatility_regime"] >= 1
        work["size_multiplier"] = np.where(work["volatility_regime"] == 2, 0.5, 1.0)
    elif policy == "trend_gate":
        work["eligible"] &= np.sign(work["signal"]) == np.sign(work["cum24_pips"])
    elif policy == "range_failure":
        work["eligible"] &= work["range_failure_flag"] > 0
    elif policy == "volatility_scaled":
        work["size_multiplier"] = (work["atr_pips"].median() / work["atr_pips"].replace(0.0, np.nan)).clip(0.25, 1.5).fillna(0.25)
    elif policy == "reverse_edge":
        work["signal"] *= -1.0
    elif policy == "lifecycle_throttle":
        work["size_multiplier"] = 0.5
    elif policy == "lifecycle_rehab":
        threshold = float(work.loc[work["split"] == "validation", "signal"].abs().quantile(0.80))
        work["eligible"] &= work["signal"].abs() >= threshold
        work["size_multiplier"] = 0.25
    elif policy == "lifecycle_hard_kill":
        threshold = float(work.loc[work["split"] == "validation", "signal"].abs().quantile(0.95))
        work["eligible"] &= work["signal"].abs() >= threshold
        work["size_multiplier"] = 0.1
    elif policy == "equity_gate":
        work["eligible"] &= work["model_health_score"] >= 0.5
    elif policy == "health_score":
        work["eligible"] &= work["model_health_score"] >= 0.5
    elif policy == "oanda_mirror_simulation":
        work["eligible"] &= work["spread_pips"] <= work.loc[work["split"] == "validation", "spread_pips"].quantile(0.95)
    elif policy == "mt5_simulation":
        work["eligible"] &= work["spread_pips"] <= work.loc[work["split"] == "validation", "spread_pips"].quantile(0.90)
    elif policy == "randomized_validation_selector":
        rng = np.random.default_rng(int(parameters.get("seed", 42)))
        candidates: list[dict[str, Any]] = []
        validation = work[work["split"] == "validation"].copy()
        for _ in range(40):
            quantile = float(rng.choice([0.0, 0.50, 0.70, 0.80, 0.90, 0.95]))
            edge_multiplier = float(rng.choice([0.0, 0.5, 1.0, 1.25, 1.5, 2.0]))
            threshold = float(validation["signal"].abs().quantile(quantile))
            eligible = validation[
                (validation["signal"].abs() >= threshold)
                & (validation["signal"].abs() >= edge_multiplier * (validation["spread_pips"] + ROUND_TRIP_SLIPPAGE_PIPS))
            ].copy()
            eligible["score"] = eligible["signal"].abs() / (eligible["spread_pips"] + ROUND_TRIP_SLIPPAGE_PIPS)
            selected = eligible.sort_values(["timestamp", "score"], ascending=[True, False]).groupby("timestamp").head(1)
            actual = np.where(selected["signal"] > 0, selected["long_net_pips"], selected["short_net_pips"])
            pnl = float(
                (actual * selected["pip_value_usd_per_unit"] * FIXED_UNITS).sum()
            )
            candidates.append(
                {
                    "quantile": quantile,
                    "threshold": threshold,
                    "edge_multiplier": edge_multiplier,
                    "validation_pnl_usd": pnl,
                    "validation_trades": int(len(selected)),
                }
            )
        candidates.sort(key=lambda row: (row["validation_pnl_usd"], row["validation_trades"]), reverse=True)
        selected_config = candidates[0]
        work["eligible"] &= work["signal"].abs() >= selected_config["threshold"]
        work["eligible"] &= work["signal"].abs() >= selected_config["edge_multiplier"] * (
            work["spread_pips"] + ROUND_TRIP_SLIPPAGE_PIPS
        )
        policy_selection = {"selected": selected_config, "attempted": candidates}
    elif policy == "top_n_cluster":
        validation = work[(work["split"] == "validation") & work["eligible"]].copy()
        validation["score"] = validation["signal"].abs() / (validation["spread_pips"] + ROUND_TRIP_SLIPPAGE_PIPS)
        candidates = []
        for top_n in (1, 2, 3, 5):
            selected = validation.sort_values(["timestamp", "score"], ascending=[True, False]).groupby("timestamp").head(top_n)
            actual = np.where(selected["signal"] > 0, selected["long_net_pips"], selected["short_net_pips"])
            pnl = float((actual * selected["pip_value_usd_per_unit"] * FIXED_UNITS / top_n).sum())
            candidates.append({"top_n": top_n, "validation_pnl_usd": pnl, "validation_trades": int(len(selected))})
        candidates.sort(key=lambda row: row["validation_pnl_usd"], reverse=True)
        policy_selection = {"selected": candidates[0], "attempted": candidates}
    work["score"] = work["signal"].abs() / (work["spread_pips"] + ROUND_TRIP_SLIPPAGE_PIPS).clip(lower=0.1)
    work.attrs["policy_selection"] = policy_selection
    return work


def currency_exposure_ok(selected_pairs: list[str], pair: str) -> bool:
    used = {currency for selected in selected_pairs for currency in selected.split("_")}
    return not any(currency in used for currency in pair.split("_"))


def select_candidates(work: pd.DataFrame, spec: MandatoryModelSpec) -> tuple[pd.DataFrame, dict[str, int]]:
    eligible = work[work["eligible"]].copy()
    policy = spec.policy
    top_n = 1
    if policy in {"top_n_cluster", "split_capital"}:
        top_n = int(
            work.attrs.get("policy_selection", {}).get("selected", {}).get(
                "top_n", spec.parameters.get("top_n", 3)
            )
        )
    elif policy == "forecast_flood":
        top_n = int(spec.parameters.get("top_n", 5))
    selected_rows: list[pd.Series] = []
    rejected_exposure = 0
    for _, group in eligible.groupby("timestamp", sort=True):
        ordered = group.sort_values(["score", "pair"], ascending=[False, True])
        selected_pairs: list[str] = []
        selected_clusters: set[str] = set()
        for _, row in ordered.iterrows():
            pair = str(row["pair"])
            if policy == "top_n_cluster":
                cluster = "JPY" if pair.endswith("JPY") else ("USD" if "USD" in pair else "CROSS")
                if cluster in selected_clusters:
                    continue
                selected_clusters.add(cluster)
            if policy in {"exposure_cap", "top_n_cluster"} and not currency_exposure_ok(selected_pairs, pair):
                rejected_exposure += 1
                continue
            selected_rows.append(row)
            selected_pairs.append(pair)
            if len(selected_pairs) >= top_n:
                break
    if not selected_rows:
        return eligible.iloc[0:0].copy(), {"eligible": int(len(eligible)), "rejected_exposure": rejected_exposure}
    selected = pd.DataFrame(selected_rows).reset_index(drop=True)
    if policy in {"split_capital", "forecast_flood", "top_n_cluster"}:
        counts = selected.groupby("timestamp")["pair"].transform("count").clip(lower=1)
        selected["size_multiplier"] = selected["size_multiplier"] / counts
    return selected, {"eligible": int(len(eligible)), "rejected_exposure": int(rejected_exposure)}


def apply_sequential_controls(selected: pd.DataFrame, spec: MandatoryModelSpec) -> tuple[pd.DataFrame, dict[str, int]]:
    if selected.empty:
        return selected, {"rejected_control": 0}
    selected = selected.sort_values(["timestamp", "score"], ascending=[True, False]).copy()
    keep = np.ones(len(selected), dtype=bool)
    last_side: dict[str, int] = {}
    last_trade_index: dict[str, int] = {}
    equity = INITIAL_EQUITY_USD
    peak = equity
    policy = spec.policy
    rejected = 0
    provisional_pips = realized_pips(selected, spec).to_numpy(dtype=float)
    for position, (_, row) in enumerate(selected.iterrows()):
        pair = str(row["pair"])
        side = 1 if float(row["signal"]) > 0 else -1
        previous = last_side.get(pair)
        if policy == "no_reverse" and previous is not None and side != previous:
            keep[position] = False
        elif policy == "minimum_hold" and previous is not None and side != previous:
            minimum = int(spec.parameters.get("minimum_hold_bars", 3))
            if position - last_trade_index.get(pair, -10_000) < minimum:
                keep[position] = False
        elif policy == "cooldown":
            cooldown = int(spec.parameters.get("cooldown_bars", 3))
            if position - last_trade_index.get(pair, -10_000) < cooldown:
                keep[position] = False
        drawdown = (peak - equity) / max(peak, 1e-9)
        if policy in {"equity_preserving", "drawdown_controlled", "lifecycle_dynamic"}:
            if drawdown >= 0.10:
                keep[position] = False
            elif drawdown >= 0.05:
                selected.iloc[position, selected.columns.get_loc("size_multiplier")] *= 0.25
            elif drawdown >= 0.02:
                selected.iloc[position, selected.columns.get_loc("size_multiplier")] *= 0.50
        if not keep[position]:
            rejected += 1
            continue
        last_side[pair] = side
        last_trade_index[pair] = position
        current_size = float(selected.iloc[position]["size_multiplier"])
        provisional = (
            float(provisional_pips[position])
            * float(row["pip_value_usd_per_unit"])
            * FIXED_UNITS
            * current_size
        )
        equity += provisional
        peak = max(peak, equity)
    return selected.loc[keep].reset_index(drop=True), {"rejected_control": int(rejected)}


def realized_pips(selected: pd.DataFrame, spec: MandatoryModelSpec) -> pd.Series:
    side_long = selected["signal"] > 0.0
    if spec.policy == "gross_pre_cost":
        return pd.Series(np.where(side_long, selected["long_gross_pips"], selected["short_gross_pips"]), index=selected.index)
    if spec.policy == "tp_sl":
        return pd.Series(np.where(side_long, selected["long_tp_sl_pips"], selected["short_tp_sl_pips"]), index=selected.index)
    if spec.policy == "trailing_stop":
        return pd.Series(np.where(side_long, selected["long_trailing_pips"], selected["short_trailing_pips"]), index=selected.index)
    base = pd.Series(np.where(side_long, selected["long_net_pips"], selected["short_net_pips"]), index=selected.index)
    if spec.policy == "mt5_simulation":
        base -= float(spec.parameters.get("extra_slippage_pips", 0.1))
    return base


def trading_metrics(selected: pd.DataFrame, spec: MandatoryModelSpec) -> dict[str, Any]:
    if selected.empty:
        return {
            "pnl_usd": 0.0,
            "return_pct": 0.0,
            "trades": 0,
            "win_rate": None,
            "profit_factor": None,
            "max_drawdown_pct": 0.0,
            "mean_net_pips": None,
            "top_bucket_actual_ev_usd": None,
            "pair_concentration": None,
        }
    pips = realized_pips(selected, spec).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    pnl = pips * selected["pip_value_usd_per_unit"].astype(float) * FIXED_UNITS * selected["size_multiplier"].astype(float)
    equity = INITIAL_EQUITY_USD + pnl.cumsum()
    drawdown = (equity.cummax() - equity) / equity.cummax().replace(0.0, np.nan)
    positive = pnl[pnl > 0.0].sum()
    negative = -pnl[pnl < 0.0].sum()
    threshold = selected["score"].quantile(0.90)
    top = pnl[selected["score"] >= threshold]
    pair_pnl = pnl.groupby(selected["pair"]).sum().abs()
    pair_concentration = float(pair_pnl.max() / pair_pnl.sum()) if pair_pnl.sum() > 0 else None
    return {
        "pnl_usd": float(pnl.sum()),
        "return_pct": float(100.0 * pnl.sum() / INITIAL_EQUITY_USD),
        "trades": int(len(selected)),
        "win_rate": float((pnl > 0.0).mean()),
        "profit_factor": float(positive / negative) if negative > 0 else (math.inf if positive > 0 else None),
        "max_drawdown_pct": float(100.0 * drawdown.max()) if len(drawdown) else 0.0,
        "mean_net_pips": float(pips.mean()),
        "mean_pnl_usd": float(pnl.mean()),
        "top_bucket_actual_ev_usd": float(top.mean()) if len(top) else None,
        "pair_concentration": pair_concentration,
        "cost_drag_usd": float(
            (
                (np.where(selected["signal"] > 0, selected["long_gross_pips"], selected["short_gross_pips"]) - pips)
                * selected["pip_value_usd_per_unit"]
                * FIXED_UNITS
                * selected["size_multiplier"]
            ).sum()
        ),
        "pair_breakdown": {
            str(pair): {
                "pnl_usd": float(pnl[group.index].sum()),
                "trades": int(len(group)),
            }
            for pair, group in selected.groupby("pair")
        },
        "session_breakdown": {
            str(session): {
                "pnl_usd": float(pnl[group.index].sum()),
                "trades": int(len(group)),
            }
            for session, group in selected.assign(
                session=np.select(
                    [selected["timestamp"].dt.hour < 7, selected["timestamp"].dt.hour < 13, selected["timestamp"].dt.hour < 21],
                    ["asia", "london", "new_york"],
                    default="late",
                )
            ).groupby("session")
        },
    }


def cost_stress(selected: pd.DataFrame, spec: MandatoryModelSpec) -> list[dict[str, Any]]:
    if selected.empty:
        return []
    side_long = selected["signal"] > 0
    endpoint_gross = pd.Series(
        np.where(side_long, selected["long_gross_pips"], selected["short_gross_pips"]),
        index=selected.index,
    )
    endpoint_net = pd.Series(
        np.where(side_long, selected["long_net_pips"], selected["short_net_pips"]),
        index=selected.index,
    )
    base_realized = realized_pips(selected, spec)
    execution_friction = (
        float(spec.parameters.get("extra_slippage_pips", 0.1))
        if spec.policy == "mt5_simulation"
        else 0.0
    )
    if spec.policy == "gross_pre_cost":
        embedded_spread = selected["spread_pips"].astype(float)
        pre_cost = base_realized
    elif spec.policy in {"tp_sl", "trailing_stop"}:
        embedded_spread = selected["spread_pips"].astype(float)
        pre_cost = base_realized + embedded_spread + ROUND_TRIP_SLIPPAGE_PIPS
    else:
        embedded_spread = (
            endpoint_gross - endpoint_net - ROUND_TRIP_SLIPPAGE_PIPS
        ).clip(lower=0.0)
        pre_cost = base_realized + embedded_spread + ROUND_TRIP_SLIPPAGE_PIPS + execution_friction
    rows: list[dict[str, Any]] = []
    scenarios = [
        ("zero_cost_fantasy", 0.0, 0.0),
        ("spread_0p5x", 0.5, ROUND_TRIP_SLIPPAGE_PIPS),
        ("base", 1.0, ROUND_TRIP_SLIPPAGE_PIPS),
        ("spread_1p25x", 1.25, ROUND_TRIP_SLIPPAGE_PIPS),
        ("spread_1p5x", 1.5, ROUND_TRIP_SLIPPAGE_PIPS),
        ("spread_2x", 2.0, ROUND_TRIP_SLIPPAGE_PIPS),
        ("slippage_plus_0p1", 1.0, ROUND_TRIP_SLIPPAGE_PIPS + 0.1),
        ("slippage_plus_0p3", 1.0, ROUND_TRIP_SLIPPAGE_PIPS + 0.3),
    ]
    for name, spread_multiplier, slippage in scenarios:
        stressed_pips = pre_cost - embedded_spread * spread_multiplier - slippage
        if name != "zero_cost_fantasy":
            stressed_pips -= execution_friction
        pnl = stressed_pips * selected["pip_value_usd_per_unit"] * FIXED_UNITS * selected["size_multiplier"]
        rows.append({"scenario": name, "pnl_usd": float(pnl.sum()), "survives": bool(pnl.sum() > 0.0)})
    return rows


def evaluate_model(surface: pd.DataFrame, spec: MandatoryModelSpec) -> dict[str, Any]:
    signal_column = signal_column_for(spec)
    timeframe = spec.timeframe
    source = surface[surface["timeframe"] == timeframe].copy()
    if signal_column not in source:
        raise KeyError(f"{spec.model_name}: missing {signal_column} on {timeframe}")
    validation_source = source[source["split"] == "validation"]
    final_source = source[source["split"] == "final_diagnostic"]
    work = apply_static_policy(source, spec, signal_column)
    policy_selection = work.attrs.get("policy_selection", {})
    selected, gate = select_candidates(work, spec)
    selected, control = apply_sequential_controls(selected, spec)
    validation_trades = selected[selected["split"] == "validation"].copy()
    final_trades = selected[selected["split"] == "final_diagnostic"].copy()
    validation_trading = trading_metrics(validation_trades, spec)
    final_trading = trading_metrics(final_trades, spec)
    validation_forecast = forecast_metrics(validation_source, signal_column)
    final_forecast = forecast_metrics(final_source, signal_column)
    stress = cost_stress(final_trades, spec)
    stress_map = {row["scenario"]: row for row in stress}
    final_positive = float(final_trading["pnl_usd"]) > 0.0
    validation_positive = float(validation_trading["pnl_usd"]) > 0.0
    enough_trades = int(final_trading["trades"]) >= 50
    top_positive = (final_trading.get("top_bucket_actual_ev_usd") or 0.0) > 0.0
    mild_stress = bool(stress_map.get("spread_1p25x", {}).get("survives", False))
    concentration_ok = (final_trading.get("pair_concentration") or 0.0) <= 0.60
    implementation_ok = bool(
        int(validation_forecast.get("rows") or 0) >= 100
        and int(final_forecast.get("rows") or 0) >= 100
        and float(validation_forecast.get("prediction_std_pips") or 0.0) > 1e-9
        and float(final_forecast.get("prediction_std_pips") or 0.0) > 1e-9
        and float(validation_forecast.get("nonzero_prediction_rate") or 0.0) > 0.001
        and float(final_forecast.get("nonzero_prediction_rate") or 0.0) > 0.001
        and bool(validation_forecast.get("forecast_stable", False))
        and bool(final_forecast.get("forecast_stable", False))
    )
    research_lead = implementation_ok and validation_positive and final_positive and enough_trades and top_positive and mild_stress and concentration_ok
    return {
        "model_name": spec.model_name,
        "model_family": spec.family,
        "model_type": spec.category,
        "variant": f"{spec.signal}|{spec.policy}|{spec.timeframe}",
        "specification": asdict(spec),
        "pair": "tier1_15" if timeframe == "H1" else "tier1_major_7",
        "timeframe": timeframe,
        "date_coverage": {
            "calibration": [CALIBRATION_START.isoformat(), VALIDATION_START.isoformat()],
            "validation": [VALIDATION_START.isoformat(), FINAL_START.isoformat()],
            "final_diagnostic": [FINAL_START.isoformat(), str(source["timestamp"].max())],
        },
        "test_status": "PASSED" if implementation_ok else "FAILED",
        "validation_status": "RESEARCH_LEAD" if research_lead else ("FAILED" if implementation_ok else "INVALIDATED"),
        "canonical_eligible": False,
        "canonical_blocker": "The 2026 diagnostic period existed and was inspected before this mandatory programme; new post-2026-07-07 data is required for a canonical untouched holdout.",
        "validation": {"forecast": validation_forecast, "trading": validation_trading},
        "final_diagnostic": {"forecast": final_forecast, "trading": final_trading},
        "cost_stress": stress,
        "gate_attribution": {
            "total_candidates": int(len(source)),
            "with_predictions": int(source[signal_column].notna().sum()),
            "eligible": gate.get("eligible", 0),
            "rejected_exposure": gate.get("rejected_exposure", 0),
            "rejected_control": control.get("rejected_control", 0),
            "traded": int(len(selected)),
        },
        "policy_selection": policy_selection,
        "viability_checks": {
            "validation_pnl_positive": validation_positive,
            "final_diagnostic_pnl_positive": final_positive,
            "minimum_final_trades": enough_trades,
            "top_bucket_positive": top_positive,
            "spread_1p25x_survives": mild_stress,
            "pair_concentration_le_60pct": concentration_ok,
            "implementation_surface_valid": implementation_ok,
        },
        "live_execution_enabled": False,
        "demo_execution_enabled": False,
        "oanda_execution_enabled": False,
        "mt5_execution_enabled": False,
    }


def report_markdown(results: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    lines = [
        f"# Mandatory Model Validation - {summary['verdict']}",
        "",
        f"- Required models: `{summary['required_models']}`",
        f"- Models run: `{summary['models_run']}`",
        f"- Test-complete: `{summary['test_complete']}`",
        f"- Research leads: `{summary['research_leads']}`",
        f"- Validated failures: `{summary['validated_failures']}`",
        f"- Canonical models: `0`",
        f"- Invariant violations: `{summary['invariant_violations']}`",
        f"- Live/demo/OANDA/MT5 execution: `disabled`",
        "",
        "The suite PASS means every mandatory implementation was present and completed its causal historical test contract. It does not mean every model was profitable. The 2026 period is a previously available diagnostic, so no result is canonical.",
        "",
        "## Results",
        "",
        "| Model | Status | Val P/L | Final P/L | Trades | Final direction | Final RMSE | Mild stress |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for result in results:
        validation = result["validation"]["trading"]
        final = result["final_diagnostic"]
        lines.append(
            "| {name} | {status} | {vp:.2f} | {fp:.2f} | {trades} | {direction} | {rmse} | {stress} |".format(
                name=result["model_name"].replace("|", "/"),
                status=result["validation_status"],
                vp=float(validation["pnl_usd"]),
                fp=float(final["trading"]["pnl_usd"]),
                trades=int(final["trading"]["trades"]),
                direction="n/a" if final["forecast"]["direction_accuracy"] is None else f"{100.0 * final['forecast']['direction_accuracy']:.1f}%",
                rmse="n/a" if final["forecast"]["rmse_pips"] is None else f"{final['forecast']['rmse_pips']:.3f}",
                stress="yes" if result["viability_checks"]["spread_1p25x_survives"] else "no",
            )
        )
    return "\n".join(lines)


def enrich_existing_report_reproducibility(
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: list[str],
    workers: int,
    maxiter: int,
    quick: bool = False,
) -> Path:
    output_dir = output_dir.resolve()
    reproduction = reproducibility_contract(
        output_dir,
        start=start,
        end=end,
        tier=tier,
        pairs=pairs,
        workers=workers,
        maxiter=maxiter,
        quick=quick,
    )
    pair_diagnostics: dict[str, dict[str, Any]] = {}
    for path in (output_dir / "surfaces").glob("diagnostics_*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        pair_diagnostics[str(payload.get("pair"))] = payload
    write_json(
        output_dir / "MANDATORY_MODEL_DATA_MANIFEST.json",
        build_data_manifest(
            start=start,
            end=end,
            pairs=pairs,
            pair_diagnostics=pair_diagnostics,
        ),
    )
    results_path = output_dir / "MANDATORY_MODEL_RESULTS.json"
    results = json.loads(results_path.read_text(encoding="utf-8"))
    for result in results:
        result["reproducibility"] = reproduction
    write_json(results_path, results)
    with (output_dir / "MANDATORY_MODEL_RESULTS.jsonl").open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(json_safe(result), sort_keys=True, allow_nan=False) + "\n")

    validation_path = output_dir / "MANDATORY_MODEL_VALIDATION_REPORT.json"
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    validation["results"] = results
    validation.setdefault("summary", {})["reproducibility"] = reproduction
    write_json(validation_path, validation)

    summary_path = output_dir / "MANDATORY_MODEL_FINAL_SUMMARY.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["reproducibility"] = reproduction
    write_json(summary_path, summary)

    manifest_path = output_dir / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    reports = list(manifest.get("reports", []))
    if "MANDATORY_MODEL_DATA_MANIFEST.json" not in reports:
        reports.insert(1, "MANDATORY_MODEL_DATA_MANIFEST.json")
    manifest["reports"] = reports
    manifest["reproducibility"] = reproduction
    write_json(manifest_path, manifest)
    return output_dir / "MANDATORY_MODEL_DATA_MANIFEST.json"


def run_validation(args: argparse.Namespace) -> Path:
    started = time.perf_counter()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    surfaces_dir = output_dir / "surfaces"
    surfaces_dir.mkdir(parents=True, exist_ok=True)
    registry = build_mandatory_registry()
    pairs = [part.strip().upper() for part in args.pairs.split(",") if part.strip()]
    if args.tier != "tier1":
        raise ValueError("mandatory validation is bounded to tier1; all-tier execution is prohibited")
    tasks: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
        futures = {
            pool.submit(
                build_pair_surfaces,
                pair,
                args.start,
                args.end,
                str(surfaces_dir),
                bool(args.quick),
                int(args.maxiter),
            ): pair
            for pair in pairs
        }
        for future in as_completed(futures):
            pair = futures[future]
            result = future.result()
            tasks.append(result)
            print(f"[mandatory] pair={pair} surfaces={sorted(result['surfaces'])}", flush=True)

    surface_paths = [path for task in tasks for path in task["surfaces"].values()]
    fit_records: list[dict[str, Any]] = []
    pair_diagnostics: dict[str, dict[str, Any]] = {}
    for task in tasks:
        diagnostics = json.loads(Path(task["diagnostics"]).read_text(encoding="utf-8"))
        pair_diagnostics[str(diagnostics.get("pair"))] = diagnostics
        for fit_name, fit_payload in diagnostics.get("fits", {}).items():
            fit_records.append({"pair": diagnostics.get("pair"), "fit": fit_name, **fit_payload})
    fit_error_count = sum(bool(record.get("error")) for record in fit_records)
    fallback_count = sum(bool(record.get("fallback")) for record in fit_records)
    convergence_records = [record for record in fit_records if "converged" in record]
    converged_count = sum(bool(record.get("converged")) for record in convergence_records)
    not_converged_count = len(convergence_records) - converged_count
    surface = pd.concat([pd.read_parquet(path) for path in surface_paths], ignore_index=True)
    surface["timestamp"] = pd.to_datetime(surface["timestamp"], utc=True)
    surface = add_account_currency_values(surface)
    surface, selection = add_derived_signals(surface)
    final_surface_path = output_dir / "mandatory_model_surface.parquet"
    surface.to_parquet(final_surface_path, index=False, compression="zstd")
    reproduction = reproducibility_contract(
        output_dir,
        start=args.start,
        end=args.end,
        tier=args.tier,
        pairs=pairs,
        workers=args.workers,
        maxiter=args.maxiter,
        quick=args.quick,
    )
    data_manifest = build_data_manifest(
        start=args.start,
        end=args.end,
        pairs=pairs,
        pair_diagnostics=pair_diagnostics,
    )
    write_json(output_dir / "MANDATORY_MODEL_DATA_MANIFEST.json", data_manifest)

    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for name in MANDATORY_MODEL_NAMES:
        try:
            result = evaluate_model(surface, registry[name])
            result["reproducibility"] = reproduction
            results.append(result)
            print(
                f"[mandatory] {name}: {result['validation_status']} final_pnl={result['final_diagnostic']['trading']['pnl_usd']:.2f}",
                flush=True,
            )
        except Exception as exc:
            errors.append({"model_name": name, "error": f"{type(exc).__name__}: {exc}"})
            print(f"[mandatory] ERROR {name}: {type(exc).__name__}: {exc}", flush=True)

    result_names = {result["model_name"] for result in results}
    missing = [name for name in MANDATORY_MODEL_NAMES if name not in result_names]
    duplicate_count = len(results) - len(result_names)
    invariant_violations = 0
    for result in results:
        invariant_violations += int(result["gate_attribution"]["traded"] < 0)
        invariant_violations += int(result["test_status"] != "PASSED")
        invariant_violations += int(result["live_execution_enabled"])
        invariant_violations += int(result["oanda_execution_enabled"])
    suite_pass = (
        not errors
        and not missing
        and duplicate_count == 0
        and invariant_violations == 0
        and fit_error_count == 0
        and fallback_count == 0
    )
    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "verdict": "PASS" if suite_pass else "FAIL",
        "required_models": len(MANDATORY_MODEL_NAMES),
        "models_run": len(results),
        "test_complete": sum(result["test_status"] == "PASSED" for result in results),
        "research_leads": sum(result["validation_status"] == "RESEARCH_LEAD" for result in results),
        "validated_failures": sum(result["validation_status"] == "FAILED" for result in results),
        "canonical_models": 0,
        "missing_models": missing,
        "duplicate_results": duplicate_count,
        "errors": errors,
        "invariant_violations": invariant_violations,
        "fit_diagnostics": {
            "fits": len(fit_records),
            "optimizer_tracked_fits": len(convergence_records),
            "converged": converged_count,
            "not_converged": not_converged_count,
            "errors": fit_error_count,
            "fallbacks": fallback_count,
        },
        "pairs": pairs,
        "pair_count": len(pairs),
        "date_contract": {
            "source_start": args.start,
            "source_end": args.end,
            "calibration_start": CALIBRATION_START,
            "validation_start": VALIDATION_START,
            "final_diagnostic_start": FINAL_START,
            "final_is_previously_available": True,
        },
        "selection": selection,
        "surface_rows": int(len(surface)),
        "surface_path": str(final_surface_path),
        "reproducibility": reproduction,
        "elapsed_minutes": (time.perf_counter() - started) / 60.0,
        "runtime": {
            "python": os.sys.version,
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "scipy": scipy.__version__,
            "sklearn": sklearn.__version__,
            "statsmodels": statsmodels.__version__,
        },
        "safety": {
            "live_execution_enabled": False,
            "demo_execution_enabled": False,
            "oanda_execution_enabled": False,
            "mt5_execution_enabled": False,
            "order_placement_used": False,
            "serialized_models_loaded": False,
        },
    }
    write_json(output_dir / "MANDATORY_MODEL_REGISTRY.json", [asdict(spec) for spec in registry.values()])
    write_json(output_dir / "MANDATORY_MODEL_FIT_DIAGNOSTICS.json", fit_records)
    write_json(output_dir / "MANDATORY_MODEL_RESULTS.json", results)
    with (output_dir / "MANDATORY_MODEL_RESULTS.jsonl").open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(json_safe(result), sort_keys=True, allow_nan=False) + "\n")
    compact_rows = []
    for result in results:
        compact_rows.append(
            {
                "model_name": result["model_name"],
                "model_family": result["model_family"],
                "category": result["model_type"],
                "timeframe": result["timeframe"],
                "validation_status": result["validation_status"],
                "validation_pnl_usd": result["validation"]["trading"]["pnl_usd"],
                "final_pnl_usd": result["final_diagnostic"]["trading"]["pnl_usd"],
                "final_trades": result["final_diagnostic"]["trading"]["trades"],
                "final_direction_accuracy": result["final_diagnostic"]["forecast"]["direction_accuracy"],
                "final_rmse_pips": result["final_diagnostic"]["forecast"]["rmse_pips"],
                "mild_cost_stress_survives": result["viability_checks"]["spread_1p25x_survives"],
            }
        )
    pd.DataFrame(compact_rows).to_csv(output_dir / "MANDATORY_MODEL_RESULTS.csv", index=False)
    write_json(output_dir / "MANDATORY_MODEL_VALIDATION_REPORT.json", {"summary": summary, "results": results})
    write_text(output_dir / "MANDATORY_MODEL_VALIDATION_REPORT.md", report_markdown(results, summary))
    write_json(output_dir / "MANDATORY_MODEL_FINAL_SUMMARY.json", summary)
    write_text(
        output_dir / "MANDATORY_MODEL_FINAL_SUMMARY.md",
        "\n".join(
            [
                f"# {summary['verdict']} - Mandatory Model Programme",
                "",
                f"- Required models: `{summary['required_models']}`",
                f"- Models run: `{summary['models_run']}`",
                f"- Research leads: `{summary['research_leads']}`",
                f"- Validated failures: `{summary['validated_failures']}`",
                f"- Missing: `{summary['missing_models']}`",
                f"- Errors: `{summary['errors']}`",
                f"- Invariant violations: `{summary['invariant_violations']}`",
                "- Canonical models: `0` (2026 was previously available)",
                "- Live/demo/OANDA/MT5 execution: `disabled`",
            ]
        ),
    )
    write_json(
        output_dir / "run_manifest.json",
        {
            "mode": "mandatory-model-validation",
            "run_dir": str(output_dir),
            "start": args.start,
            "end": args.end,
            "tier": args.tier,
            "pairs": pairs,
            "workers": args.workers,
            "maxiter": args.maxiter,
            "quick": args.quick,
            "reports": [
                "MANDATORY_MODEL_REGISTRY.json",
                "MANDATORY_MODEL_DATA_MANIFEST.json",
                "MANDATORY_MODEL_FIT_DIAGNOSTICS.json",
                "MANDATORY_MODEL_RESULTS.json",
                "MANDATORY_MODEL_VALIDATION_REPORT.json",
                "MANDATORY_MODEL_FINAL_SUMMARY.json",
            ],
            "live_execution_enabled": False,
            "demo_execution_enabled": False,
            "oanda_execution_enabled": False,
            "mt5_execution_enabled": False,
            "order_placement_used": False,
            "reproducibility": reproduction,
        },
    )
    if not suite_pass:
        raise RuntimeError(f"mandatory model programme failed: missing={missing} errors={errors}")
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--tier", default="tier1", choices=["tier1"])
    parser.add_argument("--pairs", required=True)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--maxiter", type=int, default=35)
    parser.add_argument("--quick", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    print(run_validation(parse_args()))
