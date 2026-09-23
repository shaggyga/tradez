#!/usr/bin/env python3
"""Research-only full SARIMA sweep using the project's dormant Python 3.12 stack."""

from __future__ import annotations

from pathlib import Path
import site


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
LEGACY_SITE_PACKAGES = PROJECT_ROOT / "..venv" / "Lib" / "site-packages"
if LEGACY_SITE_PACKAGES.exists():
    site.addsitedir(str(LEGACY_SITE_PACKAGES))

import argparse
import gc
import itertools
import json
import math
import os
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import scipy
import statsmodels
from statsmodels.tsa.statespace.sarimax import SARIMAX


GRID_VERSION = "tier1_h2_sarima_full_v1"
INITIAL_EQUITY = 1_000.0
BAR_MINUTES = 120
MIN_CONVERGED_PAIR_FITS = 12
SEASONAL_PERIODS = (12, 60)
P_VALUES = (0, 1, 2)
D_VALUES = (0, 1)
Q_VALUES = (0, 1, 2)
SEASONAL_AR_VALUES = (0, 1)
SEASONAL_DIFF_VALUES = (0, 1)
SEASONAL_MA_VALUES = (0, 1)
THRESHOLD_QUANTILES = (0.0, 0.50, 0.70, 0.80, 0.90, 0.95, 0.98, 0.99)
CALIBRATION_START = pd.Timestamp("2025-09-01T00:00:00Z")
VALIDATION_START = pd.Timestamp("2025-11-01T00:00:00Z")
FINAL_START = pd.Timestamp("2026-01-01T00:00:00Z")


@dataclass(frozen=True)
class SarimaSpec:
    p: int
    d: int
    q: int
    seasonal_p: int
    seasonal_d: int
    seasonal_q: int
    seasonal_period: int
    trend: str = "n"

    @property
    def order(self) -> tuple[int, int, int]:
        return (self.p, self.d, self.q)

    @property
    def seasonal_order(self) -> tuple[int, int, int, int]:
        if self.seasonal_period <= 0:
            return (0, 0, 0, 0)
        return (
            self.seasonal_p,
            self.seasonal_d,
            self.seasonal_q,
            self.seasonal_period,
        )

    @property
    def is_seasonal(self) -> bool:
        return self.seasonal_period > 0

    @property
    def spec_id(self) -> str:
        base = f"p{self.p}_d{self.d}_q{self.q}"
        if not self.is_seasonal:
            return f"arima_{base}_trend_{self.trend}"
        seasonal = (
            f"P{self.seasonal_p}_D{self.seasonal_d}_Q{self.seasonal_q}"
            f"_s{self.seasonal_period}"
        )
        return f"sarima_{base}_{seasonal}_trend_{self.trend}"


def declared_specs() -> list[SarimaSpec]:
    specs: list[SarimaSpec] = []
    for p, d, q in itertools.product(P_VALUES, D_VALUES, Q_VALUES):
        specs.append(SarimaSpec(p, d, q, 0, 0, 0, 0))
        for seasonal_period in SEASONAL_PERIODS:
            for seasonal_p, seasonal_d, seasonal_q in itertools.product(
                SEASONAL_AR_VALUES,
                SEASONAL_DIFF_VALUES,
                SEASONAL_MA_VALUES,
            ):
                if seasonal_p == seasonal_d == seasonal_q == 0:
                    continue
                specs.append(
                    SarimaSpec(
                        p,
                        d,
                        q,
                        seasonal_p,
                        seasonal_d,
                        seasonal_q,
                        seasonal_period,
                    )
                )
    unique = {spec.spec_id: spec for spec in specs}
    return [unique[key] for key in sorted(unique)]


def canonical_stationary_spec(spec: SarimaSpec) -> SarimaSpec:
    if spec.d == 0:
        return spec
    return SarimaSpec(
        spec.p,
        0,
        spec.q,
        spec.seasonal_p,
        spec.seasonal_d,
        spec.seasonal_q,
        spec.seasonal_period,
        spec.trend,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            _json_safe(payload),
            indent=2,
            sort_keys=True,
            default=_json_default,
            allow_nan=False,
        ),
        encoding="utf-8",
    )


def _write_report_pair(output_dir: Path, stem: str, payload: dict[str, Any]) -> None:
    _write_json(output_dir / f"{stem}.json", payload)
    body = json.dumps(
        _json_safe(payload),
        indent=2,
        sort_keys=True,
        default=_json_default,
        allow_nan=False,
    )
    (output_dir / f"{stem}.md").write_text(
        f"# {stem.replace('_', ' ').title()}\n\n```json\n{body}\n```\n",
        encoding="utf-8",
    )


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _exclusive_end(value: str) -> pd.Timestamp:
    ts = _utc(value)
    if len(value) <= 10:
        ts += pd.Timedelta(days=1)
    return ts


def _load_config() -> dict[str, Any]:
    return json.loads((ENGINE_ROOT / "config.json").read_text(encoding="utf-8"))


def _pair_pip_size(pair: str, cfg: dict[str, Any]) -> float:
    metadata = cfg.get("pairs", {}).get(pair, {})
    if "pip_size" in metadata:
        return float(metadata["pip_size"])
    return 0.01 if pair.endswith("_JPY") else 0.0001


def _discover_corrected_labels(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit).resolve()
        if not path.exists():
            raise FileNotFoundError(path)
        return path
    candidates = []
    for path in (ENGINE_ROOT / "reports").glob(
        "htf_corrected_rebuild*/corrected_h1_labels.parquet"
    ):
        parent = path.parent
        if (parent / "RUN_INVALID.md").exists() or (parent / "SMOKE_ONLY.md").exists():
            continue
        candidates.append(path)
    if not candidates:
        raise FileNotFoundError("no accepted corrected_h1_labels.parquet found")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _prepare_pair_data(
    output_dir: Path,
    labels_path: Path,
    pairs: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: dict[str, Any],
    force: bool,
) -> tuple[Path, dict[str, Any]]:
    data_dir = output_dir / "sarima_h2_pair_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    columns = [
        "instrument",
        "decision_time_utc",
        "entry_time_utc",
        "label_end_time_utc",
        "pip_value_usd_per_unit",
        "fixed_units",
        "slippage_pips_round_trip",
        "long_gross_endpoint_pips",
        "short_gross_endpoint_pips",
        "long_spread_drag_pips",
        "short_spread_drag_pips",
        "long_endpoint_net_pips",
        "short_endpoint_net_pips",
    ]
    labels = pd.read_parquet(labels_path, columns=columns)
    labels["decision_time_utc"] = pd.to_datetime(labels["decision_time_utc"], utc=True)
    labels = labels[
        (labels["decision_time_utc"] >= start)
        & (labels["decision_time_utc"] < end)
        & labels["instrument"].astype(str).isin(pairs)
    ].copy()
    decision = labels["decision_time_utc"]
    labels = labels[(decision.dt.minute == 0) & (decision.dt.hour.mod(2) == 0)].copy()
    audit_rows = []
    for pair in pairs:
        output_path = data_dir / f"{pair}.parquet"
        if output_path.exists() and not force:
            cached = pd.read_parquet(
                output_path,
                columns=["decision_time_utc", "label_end_time_utc"],
            )
            audit_rows.append(
                {
                    "pair": pair,
                    "rows": int(len(cached)),
                    "start": pd.to_datetime(cached["decision_time_utc"], utc=True).min(),
                    "end": pd.to_datetime(cached["decision_time_utc"], utc=True).max(),
                    "cache_reused": True,
                }
            )
            continue
        pair_labels = labels[labels["instrument"].eq(pair)].copy()
        candle_path = PROJECT_ROOT / cfg["candles_dir"] / f"{pair}_M1.csv"
        candles = pd.read_csv(candle_path, usecols=["datetime", "close"])
        candles["datetime"] = pd.to_datetime(candles["datetime"], utc=True, errors="coerce")
        candles["close"] = pd.to_numeric(candles["close"], errors="coerce")
        candles = candles[
            (candles["datetime"] >= start - pd.Timedelta(hours=4))
            & (candles["datetime"] < end + pd.Timedelta(hours=4))
        ].dropna()
        h2 = (
            candles.set_index("datetime")["close"]
            .resample("2h", label="right", closed="left")
            .last()
            .dropna()
            .rename("h2_close")
            .reset_index()
            .rename(columns={"datetime": "decision_time_utc"})
        )
        pair_frame = pair_labels.merge(h2, on="decision_time_utc", how="inner")
        pair_frame = pair_frame.sort_values("decision_time_utc").drop_duplicates(
            "decision_time_utc", keep="last"
        )
        pair_frame["log_close"] = np.log(pair_frame["h2_close"].astype(float))
        pair_frame["pip_size"] = _pair_pip_size(pair, cfg)
        pre_calibration = pair_frame["decision_time_utc"] < CALIBRATION_START
        observed_cost = (
            pair_frame["long_spread_drag_pips"].astype(float)
            + pair_frame["short_spread_drag_pips"].astype(float)
        ) / 2.0 + pair_frame["slippage_pips_round_trip"].astype(float)
        train_cost = observed_cost[pre_calibration].median()
        train_abs_move = pair_frame.loc[
            pre_calibration, "long_gross_endpoint_pips"
        ].astype(float).abs()
        forecast_cap = max(10.0, float(train_abs_move.quantile(0.999)) * 2.0)
        pair_frame["estimated_round_trip_cost_pips"] = float(train_cost)
        pair_frame["forecast_cap_pips"] = float(forecast_cap)
        pair_frame.to_parquet(output_path, index=False)
        audit_rows.append(
            {
                "pair": pair,
                "rows": int(len(pair_frame)),
                "start": pair_frame["decision_time_utc"].min(),
                "end": pair_frame["decision_time_utc"].max(),
                "median_train_round_trip_cost_pips": float(train_cost),
                "training_only_forecast_cap_pips": float(forecast_cap),
                "cache_reused": False,
            }
        )
    audit = {
        "labels_source": str(labels_path),
        "bar_minutes": BAR_MINUTES,
        "bar_contract": "H2 close uses M1 rows strictly before the decision timestamp",
        "entry_contract": "corrected first M1 open at or after completed H1/H2 decision",
        "exit_contract": "corrected executable endpoint 120 minutes after entry",
        "pair_count": len(pairs),
        "pairs": audit_rows,
        "start": start,
        "end_exclusive": end,
        "old_archived_targets_used": False,
        "fixed_units": 10_000,
    }
    return data_dir, audit


_WORKER_DATA_DIR: Path | None = None
_WORKER_CACHE: dict[str, pd.DataFrame] = {}


def _worker_init(data_dir: str) -> None:
    global _WORKER_DATA_DIR, _WORKER_CACHE
    _WORKER_DATA_DIR = Path(data_dir)
    _WORKER_CACHE = {}


def _worker_pair_data(pair: str) -> pd.DataFrame:
    if pair not in _WORKER_CACHE:
        if _WORKER_DATA_DIR is None:
            raise RuntimeError("worker data directory not initialized")
        frame = pd.read_parquet(_WORKER_DATA_DIR / f"{pair}.parquet")
        for column in ["decision_time_utc", "entry_time_utc", "label_end_time_utc"]:
            frame[column] = pd.to_datetime(frame[column], utc=True)
        _WORKER_CACHE[pair] = frame.sort_values("decision_time_utc").reset_index(drop=True)
    return _WORKER_CACHE[pair]


def _sarimax_model(endog: np.ndarray, spec: SarimaSpec) -> SARIMAX:
    return SARIMAX(
        endog,
        order=spec.order,
        seasonal_order=spec.seasonal_order,
        trend=spec.trend,
        concentrate_scale=True,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )


def _fit_pair_task(
    pair: str,
    spec_payload: dict[str, Any],
    fit_end_iso: str,
    predict_end_iso: str,
    maxiter: int,
) -> dict[str, Any]:
    spec = SarimaSpec(**spec_payload)
    fit_end = _utc(fit_end_iso)
    predict_end = _utc(predict_end_iso)
    frame = _worker_pair_data(pair)
    usable = frame[frame["decision_time_utc"] < predict_end + pd.Timedelta(hours=2)].copy()
    price_train_count = int((usable["decision_time_utc"] < fit_end).sum())
    price_log = usable["log_close"].to_numpy(dtype=float)
    if spec.d == 0:
        model_endog = np.diff(price_log)
        model_times = usable["decision_time_utc"].iloc[1:].reset_index(drop=True)
        train_count = int((model_times < fit_end).sum())
        endog_transform = "stationary_log_returns"
    else:
        model_endog = price_log
        train_count = price_train_count
        endog_transform = "log_price_levels_with_model_differencing"
    started = time.perf_counter()
    diagnostics: dict[str, Any] = {
        "pair": pair,
        "spec_id": spec.spec_id,
        "train_rows": train_count,
        "fit_end": fit_end,
        "maxiter": int(maxiter),
        "endog_transform": endog_transform,
        "success": False,
    }
    if train_count < 1_000 or len(model_endog) <= train_count + 2:
        diagnostics["error"] = "insufficient rows"
        return {"pair": pair, "diagnostics": diagnostics, "predictions": None}
    y_train = model_endog[:train_count]
    try:
        caught: list[warnings.WarningMessage]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            model = _sarimax_model(y_train, spec)
            if model.k_params == 0:
                fitted = model.filter(np.empty(0, dtype=float))
                converged = True
                iterations = 0
            else:
                fitted = model.fit(
                    disp=False,
                    maxiter=int(maxiter),
                    method="lbfgs",
                    low_memory=True,
                )
                converged = bool(fitted.mle_retvals.get("converged", False))
                iterations = int(fitted.mle_retvals.get("iterations", 0) or 0)
            full_model = _sarimax_model(model_endog, spec)
            filtered = full_model.filter(np.asarray(fitted.params, dtype=float))
            if spec.d == 0:
                predicted_returns = np.asarray(
                    filtered.get_prediction(
                        start=0,
                        end=len(model_endog) - 1,
                        dynamic=False,
                    ).predicted_mean,
                    dtype=float,
                )
            else:
                predicted_targets = np.asarray(
                    filtered.get_prediction(
                        start=1,
                        end=len(model_endog) - 1,
                        dynamic=False,
                    ).predicted_mean,
                    dtype=float,
                )
        origins = usable.iloc[:-1].copy()
        next_times = usable["decision_time_utc"].iloc[1:].reset_index(drop=True)
        origins = origins.reset_index(drop=True)
        if spec.d == 0:
            forecast_pips = (
                origins["h2_close"].to_numpy(dtype=float)
                * np.expm1(predicted_returns)
                / origins["pip_size"].to_numpy(dtype=float)
            )
        else:
            current_log = price_log[:-1]
            forecast_pips = (
                np.exp(predicted_targets) - np.exp(current_log)
            ) / origins["pip_size"].to_numpy(dtype=float)
        origins["forecast_pips"] = forecast_pips
        origins["next_model_time_utc"] = next_times
        origins = origins[
            (origins["decision_time_utc"] >= fit_end)
            & (origins["decision_time_utc"] < predict_end)
            & (
                (origins["next_model_time_utc"] - origins["decision_time_utc"])
                == pd.Timedelta(hours=2)
            )
            & np.isfinite(origins["forecast_pips"])
            & (
                origins["forecast_pips"].abs()
                <= origins["forecast_cap_pips"].astype(float)
            )
        ].copy()
        keep = [
            "instrument",
            "decision_time_utc",
            "entry_time_utc",
            "label_end_time_utc",
            "pip_value_usd_per_unit",
            "fixed_units",
            "slippage_pips_round_trip",
            "estimated_round_trip_cost_pips",
            "forecast_cap_pips",
            "long_gross_endpoint_pips",
            "short_gross_endpoint_pips",
            "long_spread_drag_pips",
            "short_spread_drag_pips",
            "long_endpoint_net_pips",
            "short_endpoint_net_pips",
            "forecast_pips",
        ]
        diagnostics.update(
            {
                "success": True,
                "converged": converged,
                "iterations": iterations,
                "fit_seconds": float(time.perf_counter() - started),
                "aic": float(fitted.aic) if np.isfinite(fitted.aic) else None,
                "bic": float(fitted.bic) if np.isfinite(fitted.bic) else None,
                "warning_count": len(caught),
                "prediction_rows": int(len(origins)),
            }
        )
        del filtered, full_model, fitted, model
        gc.collect()
        return {
            "pair": pair,
            "diagnostics": diagnostics,
            "predictions": origins[keep],
        }
    except Exception as exc:
        diagnostics.update(
            {
                "fit_seconds": float(time.perf_counter() - started),
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        return {"pair": pair, "diagnostics": diagnostics, "predictions": None}


def _candidate_surface(predictions: pd.DataFrame) -> pd.DataFrame:
    out = predictions.copy()
    use_long = out["forecast_pips"].astype(float) >= 0.0
    out["selected_direction"] = np.where(use_long, "LONG", "SHORT")
    for output, long_column, short_column in [
        ("selected_gross_pips", "long_gross_endpoint_pips", "short_gross_endpoint_pips"),
        ("selected_spread_drag_pips", "long_spread_drag_pips", "short_spread_drag_pips"),
        ("selected_net_pips", "long_endpoint_net_pips", "short_endpoint_net_pips"),
    ]:
        out[output] = np.where(use_long, out[long_column], out[short_column])
    usd_per_pip = out["pip_value_usd_per_unit"].astype(float) * out["fixed_units"].astype(float)
    out["predicted_score_usd"] = (
        out["forecast_pips"].abs().astype(float)
        - out["estimated_round_trip_cost_pips"].astype(float)
    ) * usd_per_pip
    out["selected_pnl_usd"] = out["selected_net_pips"].astype(float) * usd_per_pip
    out["forecast_error_pips"] = (
        out["forecast_pips"].astype(float) - out["long_gross_endpoint_pips"].astype(float)
    )
    return out.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["predicted_score_usd", "selected_pnl_usd", "selected_net_pips"]
    )


def _select_top1(surface: pd.DataFrame, threshold: float) -> pd.DataFrame:
    eligible = surface[surface["predicted_score_usd"] >= float(threshold)].copy()
    if eligible.empty:
        return eligible
    return (
        eligible.sort_values(
            ["decision_time_utc", "predicted_score_usd", "instrument"],
            ascending=[True, False, True],
        )
        .groupby("decision_time_utc", sort=False, as_index=False)
        .head(1)
        .sort_values(["label_end_time_utc", "entry_time_utc"])
    )


def _profit_factor(pnl: pd.Series) -> float | None:
    wins = float(pnl[pnl > 0.0].sum())
    losses = float(abs(pnl[pnl < 0.0].sum()))
    if losses <= 0.0:
        return math.inf if wins > 0.0 else None
    return wins / losses


def _max_drawdown_pct(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = INITIAL_EQUITY + pnl.cumsum().to_numpy(dtype=float)
    equity = np.concatenate([[INITIAL_EQUITY], equity])
    peaks = np.maximum.accumulate(equity)
    drawdown = (equity - peaks) / np.where(peaks == 0.0, np.nan, peaks) * 100.0
    return float(abs(np.nanmin(drawdown)))


def _metrics(trades: pd.DataFrame, surface: pd.DataFrame | None = None) -> dict[str, Any]:
    if trades.empty:
        return {
            "pnl_usd": 0.0,
            "return_pct": 0.0,
            "trades": 0,
            "win_rate": None,
            "profit_factor": None,
            "max_drawdown_pct": 0.0,
            "mean_net_pips": None,
            "top_pair_trade_share": None,
            "positive_months": 0,
            "months": 0,
            "forecast_rmse_pips": None,
            "forecast_direction_accuracy": None,
            "forecast_correlation": None,
        }
    realized = trades.sort_values(["label_end_time_utc", "entry_time_utc"])
    pnl = realized["selected_pnl_usd"].astype(float)
    month = realized["entry_time_utc"].dt.tz_localize(None).dt.to_period("M").astype(str)
    month_pnl = realized.assign(month=month).groupby("month")["selected_pnl_usd"].sum()
    pair_counts = realized["instrument"].value_counts()
    forecast_source = surface if surface is not None and not surface.empty else realized
    forecast = forecast_source["forecast_pips"].astype(float)
    actual = forecast_source["long_gross_endpoint_pips"].astype(float)
    correlation = forecast.corr(actual) if forecast.nunique() > 1 and actual.nunique() > 1 else None
    pair_breakdown = []
    for pair, group in realized.groupby("instrument", sort=True, observed=True):
        pair_breakdown.append(
            {
                "pair": str(pair),
                "trades": int(len(group)),
                "pnl_usd": float(group["selected_pnl_usd"].sum()),
                "mean_net_pips": float(group["selected_net_pips"].mean()),
                "win_rate": float((group["selected_pnl_usd"] > 0.0).mean()),
            }
        )
    return {
        "pnl_usd": float(pnl.sum()),
        "return_pct": float(pnl.sum() / INITIAL_EQUITY * 100.0),
        "trades": int(len(realized)),
        "win_rate": float((pnl > 0.0).mean()),
        "profit_factor": _profit_factor(pnl),
        "max_drawdown_pct": _max_drawdown_pct(pnl),
        "mean_net_pips": float(realized["selected_net_pips"].mean()),
        "mean_gross_pips": float(realized["selected_gross_pips"].mean()),
        "total_net_pips": float(realized["selected_net_pips"].sum()),
        "top_pair_trade_share": float(pair_counts.iloc[0] / len(realized)),
        "positive_months": int((month_pnl > 0.0).sum()),
        "months": int(len(month_pnl)),
        "forecast_rmse_pips": float(np.sqrt(np.mean(np.square(forecast - actual)))),
        "forecast_direction_accuracy": float((np.sign(forecast) == np.sign(actual)).mean()),
        "forecast_correlation": float(correlation) if correlation is not None else None,
        "pair_breakdown": pair_breakdown,
        "monthly_pnl": [
            {"month": str(key), "pnl_usd": float(value)}
            for key, value in month_pnl.items()
        ],
    }


def _select_calibration_threshold(
    calibration: pd.DataFrame,
    min_trades: int,
) -> dict[str, Any]:
    if calibration.empty:
        return {
            "quantile": None,
            "threshold_score_usd": math.inf,
            "calibration": _metrics(calibration),
        }
    scores = calibration["predicted_score_usd"].dropna()
    candidates = []
    for quantile in THRESHOLD_QUANTILES:
        threshold = float(scores.quantile(quantile))
        trades = _select_top1(calibration, threshold)
        metrics = _metrics(trades, calibration)
        candidates.append(
            {
                "quantile": float(quantile),
                "threshold_score_usd": threshold,
                "calibration": metrics,
            }
        )
    eligible = [row for row in candidates if row["calibration"]["trades"] >= min_trades]
    ranked = eligible or candidates
    best = max(
        ranked,
        key=lambda row: (
            float(row["calibration"]["pnl_usd"]),
            float(row["calibration"].get("profit_factor") or -math.inf),
            int(row["calibration"]["trades"]),
        ),
    )
    return {**best, "all_thresholds": candidates}


def _viability(
    metrics: dict[str, Any],
    successful_pairs: int,
    converged_pairs: int,
) -> dict[str, Any]:
    months = int(metrics.get("months", 0))
    checks = {
        "at_least_12_successful_pair_fits": successful_pairs >= 12,
        "at_least_12_converged_pair_fits": converged_pairs >= MIN_CONVERGED_PAIR_FITS,
        "at_least_60_validation_trades": int(metrics.get("trades", 0)) >= 60,
        "positive_validation_pnl": float(metrics.get("pnl_usd", 0.0)) > 0.0,
        "positive_mean_net_pips": float(metrics.get("mean_net_pips") or -math.inf) > 0.0,
        "profit_factor_above_one": float(metrics.get("profit_factor") or 0.0) > 1.0,
        "max_drawdown_at_most_35pct": float(metrics.get("max_drawdown_pct", math.inf)) <= 35.0,
        "top_pair_share_at_most_40pct": (
            metrics.get("top_pair_trade_share") is not None
            and float(metrics["top_pair_trade_share"]) <= 0.40
        ),
        "at_least_half_months_positive": (
            months > 0 and int(metrics.get("positive_months", 0)) >= math.ceil(months / 2)
        ),
    }
    return {
        **checks,
        "passed_check_count": int(sum(checks.values())),
        "total_check_count": int(len(checks)),
        "viable": bool(all(checks.values())),
    }


def _evaluate_screen_predictions(
    predictions: pd.DataFrame,
    diagnostics: list[dict[str, Any]],
) -> dict[str, Any]:
    surface = _candidate_surface(predictions)
    calibration = surface[
        (surface["decision_time_utc"] >= CALIBRATION_START)
        & (surface["decision_time_utc"] < VALIDATION_START)
    ].copy()
    validation = surface[
        (surface["decision_time_utc"] >= VALIDATION_START)
        & (surface["decision_time_utc"] < FINAL_START)
    ].copy()
    threshold = _select_calibration_threshold(calibration, min_trades=60)
    validation_trades = _select_top1(validation, threshold["threshold_score_usd"])
    validation_metrics = _metrics(validation_trades, validation)
    successful = sum(
        bool(row.get("success")) and int(row.get("prediction_rows", 0)) >= 100
        for row in diagnostics
    )
    converged = sum(
        bool(row.get("converged")) and int(row.get("prediction_rows", 0)) >= 100
        for row in diagnostics
    )
    return {
        "threshold_quantile": threshold["quantile"],
        "frozen_threshold_score_usd": threshold["threshold_score_usd"],
        "calibration": threshold["calibration"],
        "validation": validation_metrics,
        "viability": _viability(validation_metrics, successful, converged),
        "successful_pair_fits": int(successful),
        "converged_pair_fits": int(converged),
        "mean_fit_seconds": float(
            np.mean([row.get("fit_seconds", 0.0) for row in diagnostics])
        ),
        "validation_surface_rows": int(len(validation)),
    }


def _screen_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    viability = row["viability"]
    validation = row["validation"]
    return (
        bool(viability["viable"]),
        int(viability["passed_check_count"]),
        float(validation["pnl_usd"]),
        float(validation.get("profit_factor") or -math.inf),
        -float(validation.get("max_drawdown_pct", math.inf)),
    )


def _fit_spec_all_pairs(
    pool: ProcessPoolExecutor,
    pairs: list[str],
    spec: SarimaSpec,
    fit_end: pd.Timestamp,
    predict_end: pd.Timestamp,
    maxiter: int,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    futures = {
        pool.submit(
            _fit_pair_task,
            pair,
            asdict(spec),
            fit_end.isoformat(),
            predict_end.isoformat(),
            int(maxiter),
        ): pair
        for pair in pairs
    }
    predictions = []
    diagnostics = []
    for future in as_completed(futures):
        pair = futures[future]
        try:
            result = future.result()
        except Exception as exc:
            diagnostics.append(
                {
                    "pair": pair,
                    "spec_id": spec.spec_id,
                    "success": False,
                    "error": f"worker failure: {type(exc).__name__}: {exc}",
                }
            )
            continue
        diagnostics.append(result["diagnostics"])
        if result["predictions"] is not None:
            predictions.append(result["predictions"])
    combined = pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame()
    return combined, diagnostics


def _simple_baseline_predictions(data_dir: Path, pairs: list[str]) -> dict[str, pd.DataFrame]:
    outputs: dict[str, list[pd.DataFrame]] = {
        "h2_momentum_continuation": [],
        "h2_snapback_reversal": [],
        "drift_12bar": [],
        "drift_60bar": [],
        "seasonal_naive_daily_s12": [],
        "seasonal_naive_weekly_s60": [],
    }
    for pair in pairs:
        frame = pd.read_parquet(data_dir / f"{pair}.parquet")
        for column in ["decision_time_utc", "entry_time_utc", "label_end_time_utc"]:
            frame[column] = pd.to_datetime(frame[column], utc=True)
        frame = frame.sort_values("decision_time_utc").reset_index(drop=True)
        log_close = frame["log_close"].astype(float)
        returns = log_close.diff()
        pip = frame["pip_size"].astype(float)
        close = frame["h2_close"].astype(float)
        forecast_map = {
            "h2_momentum_continuation": returns,
            "h2_snapback_reversal": -returns,
            "drift_12bar": returns.rolling(12, min_periods=6).mean(),
            "drift_60bar": returns.rolling(60, min_periods=30).mean(),
            "seasonal_naive_daily_s12": returns.shift(11),
            "seasonal_naive_weekly_s60": returns.shift(59),
        }
        next_time = frame["decision_time_utc"].shift(-1)
        valid_next = (next_time - frame["decision_time_utc"]) == pd.Timedelta(hours=2)
        keep = [
            "instrument",
            "decision_time_utc",
            "entry_time_utc",
            "label_end_time_utc",
            "pip_value_usd_per_unit",
            "fixed_units",
            "slippage_pips_round_trip",
            "estimated_round_trip_cost_pips",
            "forecast_cap_pips",
            "long_gross_endpoint_pips",
            "short_gross_endpoint_pips",
            "long_spread_drag_pips",
            "short_spread_drag_pips",
            "long_endpoint_net_pips",
            "short_endpoint_net_pips",
        ]
        for name, forecast_return in forecast_map.items():
            part = frame[keep].copy()
            part["forecast_pips"] = (
                close * np.expm1(forecast_return.astype(float)) / pip
            )
            part = part[valid_next & np.isfinite(part["forecast_pips"])].copy()
            outputs[name].append(part)
    return {
        name: pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        for name, parts in outputs.items()
    }


def _evaluate_baselines(data_dir: Path, pairs: list[str], end: pd.Timestamp) -> dict[str, Any]:
    reports = {}
    for name, predictions in _simple_baseline_predictions(data_dir, pairs).items():
        predictions = predictions[predictions["decision_time_utc"] < end]
        surface = _candidate_surface(predictions)
        calibration = surface[
            (surface["decision_time_utc"] >= CALIBRATION_START)
            & (surface["decision_time_utc"] < VALIDATION_START)
        ]
        validation = surface[
            (surface["decision_time_utc"] >= VALIDATION_START)
            & (surface["decision_time_utc"] < FINAL_START)
        ]
        final = surface[
            (surface["decision_time_utc"] >= FINAL_START)
            & (surface["decision_time_utc"] < end)
        ]
        threshold = _select_calibration_threshold(calibration, min_trades=60)
        validation_trades = _select_top1(validation, threshold["threshold_score_usd"])
        final_trades = _select_top1(final, threshold["threshold_score_usd"])
        reports[name] = {
            "threshold_quantile": threshold["quantile"],
            "frozen_threshold_score_usd": threshold["threshold_score_usd"],
            "calibration": threshold["calibration"],
            "validation": _metrics(validation_trades, validation),
            "diagnostic_evaluation": _metrics(final_trades, final),
        }
    best_name, best = max(
        reports.items(),
        key=lambda item: (
            float(item[1]["validation"]["pnl_usd"]),
            float(item[1]["validation"].get("profit_factor") or -math.inf),
        ),
    )
    return {"best": {"name": best_name, **best}, "all": reports}


def _cost_stress(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "trades": 0,
            "base_pnl_usd": 0.0,
            "stress": [],
            "breakeven_spread_multiplier": None,
            "breakeven_additional_slippage_pips": None,
            "edge_survives_mild_stress": False,
            "edge_exists_only_under_fantasy_costs": False,
        }
    usd_per_pip = trades["pip_value_usd_per_unit"].astype(float) * trades["fixed_units"].astype(float)
    gross = float((trades["selected_gross_pips"].astype(float) * usd_per_pip).sum())
    spread = float((trades["selected_spread_drag_pips"].astype(float) * usd_per_pip).sum())
    slippage = float((trades["slippage_pips_round_trip"].astype(float) * usd_per_pip).sum())
    base = gross - spread - slippage
    rows = []
    for name, multiplier in [
        ("zero_cost_fantasy", 0.0),
        ("spread_0p5x", 0.5),
        ("base_spread", 1.0),
        ("spread_1p25x", 1.25),
        ("spread_1p5x", 1.5),
        ("spread_2x", 2.0),
    ]:
        pnl = gross if multiplier == 0.0 else gross - spread * multiplier - slippage
        rows.append(
            {
                "stress": name,
                "pnl_usd": float(pnl),
                "return_pct": float(pnl / INITIAL_EQUITY * 100.0),
                "survives": bool(pnl > 0.0),
            }
        )
    per_pip = float(usd_per_pip.sum())
    for name, extra in [("slippage_plus_0p1", 0.1), ("slippage_plus_0p3", 0.3)]:
        pnl = base - extra * per_pip
        rows.append(
            {
                "stress": name,
                "pnl_usd": float(pnl),
                "return_pct": float(pnl / INITIAL_EQUITY * 100.0),
                "survives": bool(pnl > 0.0),
            }
        )
    by_name = {row["stress"]: row for row in rows}
    mild = all(
        by_name[name]["survives"]
        for name in ["base_spread", "spread_1p25x", "slippage_plus_0p1"]
    )
    return {
        "trades": int(len(trades)),
        "gross_pnl_usd": gross,
        "spread_drag_usd": spread,
        "slippage_drag_usd": slippage,
        "base_pnl_usd": base,
        "stress": rows,
        "breakeven_spread_multiplier": (
            float((gross - slippage) / spread) if spread > 0.0 else None
        ),
        "breakeven_additional_slippage_pips": (
            float(base / per_pip) if per_pip > 0.0 else None
        ),
        "edge_survives_mild_stress": bool(mild),
        "edge_exists_only_under_fantasy_costs": bool(
            by_name["zero_cost_fantasy"]["survives"] and base <= 0.0
        ),
    }


def _category_winners(screen_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    categories = {
        "overall": screen_rows,
        "nonseasonal_arima": [row for row in screen_rows if row["spec"]["seasonal_period"] == 0],
        "daily_sarima_s12": [row for row in screen_rows if row["spec"]["seasonal_period"] == 12],
        "weekly_sarima_s60": [row for row in screen_rows if row["spec"]["seasonal_period"] == 60],
    }
    return {
        name: max(rows, key=_screen_rank_key)
        for name, rows in categories.items()
        if rows
    }


def _refinement_shortlist(
    screen_rows: list[dict[str, Any]],
    per_category: int,
) -> list[dict[str, Any]]:
    canonical = [row for row in screen_rows if not row.get("fit_alias_of")]
    categories = {
        "overall": canonical,
        "nonseasonal_arima": [
            row for row in canonical if row["spec"]["seasonal_period"] == 0
        ],
        "daily_sarima_s12": [
            row for row in canonical if row["spec"]["seasonal_period"] == 12
        ],
        "weekly_sarima_s60": [
            row for row in canonical if row["spec"]["seasonal_period"] == 60
        ],
    }
    selected = {
        row["spec_id"]: row
        for row in canonical
        if bool(row.get("viability", {}).get("viable"))
    }
    count = max(1, int(per_category))
    for rows in categories.values():
        for row in sorted(rows, key=_screen_rank_key, reverse=True)[:count]:
            selected[row["spec_id"]] = row
        for row in sorted(
            rows,
            key=lambda item: float(item["validation"]["pnl_usd"]),
            reverse=True,
        )[:count]:
            selected[row["spec_id"]] = row
    return sorted(selected.values(), key=_screen_rank_key, reverse=True)


def run_sweep(args: argparse.Namespace) -> Path:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg = _load_config()
    if args.tier != "tier1":
        raise ValueError("SARIMA sweep is intentionally limited to tier1; all-tier is not automatic")
    pairs = list(cfg["tier1_pairs"])
    if args.pairs:
        requested = [value.strip() for value in args.pairs.split(",") if value.strip()]
        pairs = [pair for pair in pairs if pair in requested]
    if args.quick:
        pairs = pairs[:2]
    start = _utc(args.start)
    end = _exclusive_end(args.end)
    labels_path = _discover_corrected_labels(args.labels_path)
    data_dir, data_audit = _prepare_pair_data(
        output_dir,
        labels_path,
        pairs,
        start,
        end,
        cfg,
        bool(args.force_data),
    )
    specs = declared_specs()
    if args.max_specs:
        specs = specs[: int(args.max_specs)]
    elif args.quick:
        specs = specs[:6]
    cache_dir = output_dir / "screen_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    screen_rows: list[dict[str, Any]] = []
    all_diagnostics: list[dict[str, Any]] = []
    started = time.perf_counter()
    workers = max(1, int(args.workers))
    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_worker_init,
        initargs=(str(data_dir),),
    ) as pool:
        for index, spec in enumerate(specs, start=1):
            prediction_cache = cache_dir / f"{spec.spec_id}.parquet"
            diagnostics_cache = cache_dir / f"{spec.spec_id}.json"
            canonical = canonical_stationary_spec(spec)
            canonical_prediction_cache = cache_dir / f"{canonical.spec_id}.parquet"
            canonical_diagnostics_cache = cache_dir / f"{canonical.spec_id}.json"
            fit_alias_of = canonical.spec_id if canonical.spec_id != spec.spec_id else None
            if fit_alias_of and canonical_prediction_cache.exists() and canonical_diagnostics_cache.exists():
                predictions = pd.read_parquet(canonical_prediction_cache)
                for column in ["decision_time_utc", "entry_time_utc", "label_end_time_utc"]:
                    predictions[column] = pd.to_datetime(predictions[column], utc=True)
                source_diagnostics = json.loads(
                    canonical_diagnostics_cache.read_text(encoding="utf-8")
                )
                diagnostics = [
                    {
                        **row,
                        "spec_id": spec.spec_id,
                        "fit_alias_of": canonical.spec_id,
                        "endog_transform": "equivalent_log_level_d1_alias",
                    }
                    for row in source_diagnostics
                ]
                predictions.to_parquet(prediction_cache, index=False)
                _write_json(diagnostics_cache, diagnostics)
            elif prediction_cache.exists() and diagnostics_cache.exists() and not args.force_screen:
                predictions = pd.read_parquet(prediction_cache)
                for column in ["decision_time_utc", "entry_time_utc", "label_end_time_utc"]:
                    predictions[column] = pd.to_datetime(predictions[column], utc=True)
                diagnostics = json.loads(diagnostics_cache.read_text(encoding="utf-8"))
            else:
                predictions, diagnostics = _fit_spec_all_pairs(
                    pool,
                    pairs,
                    spec,
                    CALIBRATION_START,
                    FINAL_START,
                    int(args.maxiter_screen),
                )
                if not predictions.empty:
                    predictions.to_parquet(prediction_cache, index=False)
                _write_json(diagnostics_cache, diagnostics)
            all_diagnostics.extend({**row, "stage": "screen"} for row in diagnostics)
            if predictions.empty:
                screen = {
                    "spec_id": spec.spec_id,
                    "spec": asdict(spec),
                    "fit_alias_of": fit_alias_of,
                    "threshold_quantile": None,
                    "frozen_threshold_score_usd": None,
                    "calibration": _metrics(pd.DataFrame()),
                    "validation": _metrics(pd.DataFrame()),
                    "viability": {"viable": False, "passed_check_count": 0, "total_check_count": 9},
                    "successful_pair_fits": 0,
                    "converged_pair_fits": 0,
                }
            else:
                screen = {
                    "spec_id": spec.spec_id,
                    "spec": asdict(spec),
                    "fit_alias_of": fit_alias_of,
                    **_evaluate_screen_predictions(predictions, diagnostics),
                }
            screen_rows.append(screen)
            pd.DataFrame(screen_rows).to_json(
                output_dir / "SARIMA_GRID_CHECKPOINT.json",
                orient="records",
                indent=2,
                default_handler=_json_default,
            )
            elapsed = time.perf_counter() - started
            eta = elapsed / index * (len(specs) - index)
            print(
                f"[{index}/{len(specs)}] {spec.spec_id} "
                f"val_pnl={screen['validation']['pnl_usd']:.2f} "
                f"fits={screen.get('successful_pair_fits', 0)}/{len(pairs)} "
                f"eta_min={eta / 60.0:.1f}",
                flush=True,
            )

        screen_winners = _category_winners(screen_rows)
        refinement_cache_dir = output_dir / "refinement_cache"
        refinement_cache_dir.mkdir(parents=True, exist_ok=True)
        refinement_source_rows = _refinement_shortlist(
            screen_rows,
            int(args.refine_per_category),
        )
        refinement_rows: list[dict[str, Any]] = []
        for index, source in enumerate(refinement_source_rows, start=1):
            spec = SarimaSpec(**source["spec"])
            cache_stem = f"{spec.spec_id}_m{int(args.maxiter_final)}"
            prediction_cache = refinement_cache_dir / f"{cache_stem}.parquet"
            diagnostics_cache = refinement_cache_dir / f"{cache_stem}.json"
            if (
                prediction_cache.exists()
                and diagnostics_cache.exists()
                and not args.force_refinement
            ):
                predictions = pd.read_parquet(prediction_cache)
                for column in ["decision_time_utc", "entry_time_utc", "label_end_time_utc"]:
                    predictions[column] = pd.to_datetime(predictions[column], utc=True)
                diagnostics = json.loads(diagnostics_cache.read_text(encoding="utf-8"))
            else:
                predictions, diagnostics = _fit_spec_all_pairs(
                    pool,
                    pairs,
                    spec,
                    CALIBRATION_START,
                    FINAL_START,
                    int(args.maxiter_final),
                )
                if not predictions.empty:
                    predictions.to_parquet(prediction_cache, index=False)
                _write_json(diagnostics_cache, diagnostics)
            all_diagnostics.extend({**row, "stage": "refinement"} for row in diagnostics)
            if predictions.empty:
                refined = {
                    "spec_id": spec.spec_id,
                    "spec": asdict(spec),
                    "fit_alias_of": None,
                    "threshold_quantile": None,
                    "frozen_threshold_score_usd": None,
                    "calibration": _metrics(pd.DataFrame()),
                    "validation": _metrics(pd.DataFrame()),
                    "viability": {
                        "viable": False,
                        "passed_check_count": 0,
                        "total_check_count": 9,
                    },
                    "successful_pair_fits": 0,
                    "converged_pair_fits": 0,
                }
            else:
                refined = {
                    "spec_id": spec.spec_id,
                    "spec": asdict(spec),
                    "fit_alias_of": None,
                    **_evaluate_screen_predictions(predictions, diagnostics),
                }
            refined["screen_result"] = {
                "validation_pnl_usd": source["validation"]["pnl_usd"],
                "validation_trades": source["validation"]["trades"],
                "converged_pair_fits": source.get("converged_pair_fits", 0),
                "viable": source["viability"].get("viable", False),
            }
            refinement_rows.append(refined)
            print(
                f"[refine {index}/{len(refinement_source_rows)}] {spec.spec_id} "
                f"val_pnl={refined['validation']['pnl_usd']:.2f} "
                f"converged={refined.get('converged_pair_fits', 0)}/{len(pairs)}",
                flush=True,
            )

        winners = _category_winners(refinement_rows or screen_rows)
        final_evaluations: dict[str, Any] = {}
        overall_final_trades = pd.DataFrame()
        final_fit_cache: dict[str, tuple[pd.DataFrame, list[dict[str, Any]]]] = {}
        for category, winner in winners.items():
            spec = SarimaSpec(**winner["spec"])
            if spec.spec_id not in final_fit_cache:
                final_fit_cache[spec.spec_id] = _fit_spec_all_pairs(
                    pool,
                    pairs,
                    spec,
                    FINAL_START,
                    end,
                    int(args.maxiter_final),
                )
                all_diagnostics.extend(
                    {**row, "stage": "frozen_2026"}
                    for row in final_fit_cache[spec.spec_id][1]
                )
            predictions, diagnostics = final_fit_cache[spec.spec_id]
            surface = _candidate_surface(predictions) if not predictions.empty else pd.DataFrame()
            final_surface = surface[
                (surface["decision_time_utc"] >= FINAL_START)
                & (surface["decision_time_utc"] < end)
            ].copy() if not surface.empty else surface
            trades = _select_top1(final_surface, winner["frozen_threshold_score_usd"])
            final_evaluations[category] = {
                "spec_id": winner["spec_id"],
                "spec": winner["spec"],
                "selected_on_2025_validation_only": True,
                "selection_stage": "high_iteration_validation_refinement",
                "threshold_quantile": winner.get("threshold_quantile"),
                "frozen_threshold_score_usd": winner["frozen_threshold_score_usd"],
                "calibration": winner["calibration"],
                "validation": winner["validation"],
                "viability": winner["viability"],
                "refinement_successful_pair_fits": winner.get("successful_pair_fits", 0),
                "refinement_converged_pair_fits": winner.get("converged_pair_fits", 0),
                "diagnostic_evaluation": _metrics(trades, final_surface),
                "cost_stress": _cost_stress(trades),
                "fit_diagnostics": diagnostics,
            }
            safe_category = category.upper()
            final_surface.to_parquet(
                output_dir / f"sarima_frozen_{category}_surface.parquet",
                index=False,
            )
            trades.to_csv(
                output_dir / f"SARIMA_FINAL_TRADES_{safe_category}.csv",
                index=False,
            )
            if category == "overall":
                overall_final_trades = trades
                final_surface.to_parquet(output_dir / "sarima_frozen_evaluation_surface.parquet", index=False)
                trades.to_csv(output_dir / "SARIMA_FINAL_TRADES.csv", index=False)

    screen_table = []
    for row in sorted(screen_rows, key=_screen_rank_key, reverse=True):
        screen_table.append(
            {
                "spec_id": row["spec_id"],
                **row["spec"],
                "fit_alias_of": row.get("fit_alias_of"),
                "threshold_quantile": row.get("threshold_quantile"),
                "threshold_score_usd": row.get("frozen_threshold_score_usd"),
                "calibration_pnl_usd": row["calibration"]["pnl_usd"],
                "calibration_trades": row["calibration"]["trades"],
                "validation_pnl_usd": row["validation"]["pnl_usd"],
                "validation_trades": row["validation"]["trades"],
                "validation_profit_factor": row["validation"].get("profit_factor"),
                "validation_max_drawdown_pct": row["validation"]["max_drawdown_pct"],
                "validation_mean_net_pips": row["validation"].get("mean_net_pips"),
                "validation_forecast_rmse_pips": row["validation"].get("forecast_rmse_pips"),
                "validation_direction_accuracy": row["validation"].get("forecast_direction_accuracy"),
                "successful_pair_fits": row.get("successful_pair_fits", 0),
                "converged_pair_fits": row.get("converged_pair_fits", 0),
                "viability_passed_checks": row["viability"].get("passed_check_count", 0),
                "viable": row["viability"].get("viable", False),
            }
        )
    pd.DataFrame(screen_table).to_csv(output_dir / "SARIMA_GRID_RESULTS.csv", index=False)
    refinement_table = []
    for row in sorted(refinement_rows, key=_screen_rank_key, reverse=True):
        refinement_table.append(
            {
                "spec_id": row["spec_id"],
                **row["spec"],
                "threshold_quantile": row.get("threshold_quantile"),
                "threshold_score_usd": row.get("frozen_threshold_score_usd"),
                "calibration_pnl_usd": row["calibration"]["pnl_usd"],
                "calibration_trades": row["calibration"]["trades"],
                "validation_pnl_usd": row["validation"]["pnl_usd"],
                "validation_trades": row["validation"]["trades"],
                "validation_profit_factor": row["validation"].get("profit_factor"),
                "validation_max_drawdown_pct": row["validation"]["max_drawdown_pct"],
                "validation_mean_net_pips": row["validation"].get("mean_net_pips"),
                "successful_pair_fits": row.get("successful_pair_fits", 0),
                "converged_pair_fits": row.get("converged_pair_fits", 0),
                "viability_passed_checks": row["viability"].get("passed_check_count", 0),
                "viable": row["viability"].get("viable", False),
                "screen_validation_pnl_usd": row["screen_result"]["validation_pnl_usd"],
                "screen_converged_pair_fits": row["screen_result"]["converged_pair_fits"],
            }
        )
    pd.DataFrame(refinement_table).to_csv(
        output_dir / "SARIMA_REFINEMENT_RESULTS.csv",
        index=False,
    )
    pd.DataFrame(all_diagnostics).to_csv(output_dir / "SARIMA_FIT_DIAGNOSTICS.csv", index=False)
    baselines = _evaluate_baselines(data_dir, pairs, end)
    stress = _cost_stress(overall_final_trades)
    overall_winner = winners["overall"]
    overall_final = final_evaluations["overall"]["diagnostic_evaluation"]
    nonseasonal_final = final_evaluations.get("nonseasonal_arima", {}).get(
        "diagnostic_evaluation", {"pnl_usd": 0.0}
    )
    best_simple = baselines["best"]
    selected_is_seasonal = int(overall_winner["spec"]["seasonal_period"]) > 0
    signal_checks = {
        "validation_candidate_viable": bool(overall_winner["viability"]["viable"]),
        "positive_threshold_calibration_pnl": float(
            overall_winner["calibration"]["pnl_usd"]
        ) > 0.0,
        "at_least_12_refined_pair_fits_converged": int(
            overall_winner.get("converged_pair_fits", 0)
        ) >= MIN_CONVERGED_PAIR_FITS,
        "positive_2026_diagnostic_pnl": float(overall_final["pnl_usd"]) > 0.0,
        "beats_no_trade": float(overall_final["pnl_usd"]) > 0.0,
        "beats_best_simple_baseline": float(overall_final["pnl_usd"]) > float(
            best_simple["diagnostic_evaluation"]["pnl_usd"]
        ),
        "seasonal_winner_beats_nonseasonal_arima": (
            not selected_is_seasonal
            or float(overall_final["pnl_usd"]) > float(nonseasonal_final["pnl_usd"])
        ),
        "mild_cost_stress_survives": bool(stress["edge_survives_mild_stress"]),
        "diagnostic_drawdown_at_most_35pct": float(overall_final["max_drawdown_pct"]) <= 35.0,
        "diagnostic_top_pair_share_at_most_40pct": (
            overall_final.get("top_pair_trade_share") is not None
            and float(overall_final["top_pair_trade_share"]) <= 0.40
        ),
    }
    diagnostic_verdict = "PASS" if all(signal_checks.values()) else "FAIL"
    report = {
        "run_id": output_dir.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "grid_version": GRID_VERSION,
        "mode": "sarima-sweep",
        "evidence_status": "2025_validation_selected_2026_diagnostic_previously_inspected",
        "start": start,
        "end_exclusive": end,
        "tier": args.tier,
        "pairs": pairs,
        "runtime": {
            "python": os.sys.version,
            "statsmodels": statsmodels.__version__,
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "scipy": scipy.__version__,
            "legacy_site_packages": str(LEGACY_SITE_PACKAGES),
            "workers": workers,
            "elapsed_minutes": float((time.perf_counter() - started) / 60.0),
        },
        "grid": {
            "declared_full_grid_size": len(declared_specs()),
            "attempted_grid_size": len(specs),
            "unique_statistical_model_count": len(
                {canonical_stationary_spec(spec).spec_id for spec in specs}
            ),
            "p_values": list(P_VALUES),
            "d_values": list(D_VALUES),
            "q_values": list(Q_VALUES),
            "seasonal_p_values": list(SEASONAL_AR_VALUES),
            "seasonal_d_values": list(SEASONAL_DIFF_VALUES),
            "seasonal_q_values": list(SEASONAL_MA_VALUES),
            "seasonal_periods_h2_bars": list(SEASONAL_PERIODS),
            "seasonal_period_meanings": {"12": "24-hour FX day", "60": "120-hour FX trading week"},
            "trend": "n",
            "stationarity_contract": {
                "d_0": "fit stationary H2 log returns directly",
                "d_1": (
                    "mathematically equivalent log-price-level parameterization; "
                    "reported as an explicit alias of the matching d=0 return model"
                ),
            },
            "maxiter_screen": int(args.maxiter_screen),
            "maxiter_final": int(args.maxiter_final),
            "complete_declared_grid_attempted": len(specs) == len(declared_specs()),
        },
        "split": {
            "model_training": f"{start.isoformat()} through 2025-08-31",
            "threshold_calibration": "2025-09-01 through 2025-10-31",
            "model_selection_validation": "2025-11-01 through 2025-12-31",
            "frozen_diagnostic_evaluation": f"2026-01-01 through {args.end}",
            "configuration_selected_without_2026_outcomes": True,
            "2026_period_previously_inspected_elsewhere": True,
        },
        "data_audit": data_audit,
        "screen_category_winners": screen_winners,
        "category_winners": winners,
        "refinement": {
            "selection_uses_2026_outcomes": False,
            "shortlist_method": (
                "all screen-viable canonical models plus leading rank and raw-P/L "
                "models per overall/nonseasonal/daily/weekly category"
            ),
            "per_category": int(args.refine_per_category),
            "maxiter": int(args.maxiter_final),
            "candidate_count": len(refinement_rows),
            "minimum_converged_pair_fits_for_viability": MIN_CONVERGED_PAIR_FITS,
            "candidates": refinement_rows,
        },
        "category_final_evaluations": final_evaluations,
        "category_cost_stress": {
            category: evaluation["cost_stress"]
            for category, evaluation in final_evaluations.items()
        },
        "best_simple_baseline": best_simple,
        "all_simple_baselines": baselines["all"],
        "no_trade": {"pnl_usd": 0.0, "return_pct": 0.0, "trades": 0},
        "cost_stress": stress,
        "signal_checks": signal_checks,
        "diagnostic_verdict": diagnostic_verdict,
        "production_verdict": "FAIL",
        "production_blockers": [
            "2026 evaluation period has been inspected by prior research",
            "no post-2026-07-07 untouched candles are locally available",
        ],
        "top_25_grid_results": sorted(screen_rows, key=_screen_rank_key, reverse=True)[:25],
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
        "broker_placement_enabled": False,
    }
    _write_report_pair(output_dir, "SARIMA_FULL_SWEEP_REPORT", report)
    _write_report_pair(output_dir, "SARIMA_COST_STRESS", stress)
    final_summary = {
        "verdict": "FAIL",
        "diagnostic_verdict": diagnostic_verdict,
        "evidence_status": report["evidence_status"],
        "grid_declared": len(declared_specs()),
        "grid_attempted": len(specs),
        "refinement_candidates": len(refinement_rows),
        "refinement_maxiter": int(args.maxiter_final),
        "selected_model": final_evaluations["overall"],
        "best_nonseasonal_arima": final_evaluations.get("nonseasonal_arima"),
        "best_daily_sarima": final_evaluations.get("daily_sarima_s12"),
        "best_weekly_sarima": final_evaluations.get("weekly_sarima_s60"),
        "best_simple_baseline": best_simple,
        "no_trade": report["no_trade"],
        "cost_stress": stress,
        "category_cost_stress": report["category_cost_stress"],
        "signal_checks": signal_checks,
        "biggest_remaining_blocker": "no untouched candles after 2026-07-07",
        "next_recommended_step": (
            "freeze the validation-selected SARIMA configuration and evaluate once on post-2026-07-07 data"
            if diagnostic_verdict == "PASS"
            else "do not promote SARIMA; use the sweep to constrain or retire the seasonal family"
        ),
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "demo_execution_enabled": False,
        "broker_placement_enabled": False,
        "live_execution_confirmation": (
            "Research/backtest only; no OANDA bot, broker call, account file, credential, "
            "or order-placement path was invoked."
        ),
    }
    _write_report_pair(output_dir, "SARIMA_FINAL_SUMMARY", final_summary)
    _write_json(
        output_dir / "run_manifest.json",
        {
            "mode": "sarima-sweep",
            "run_dir": str(output_dir),
            "reports": [
                "SARIMA_FULL_SWEEP_REPORT.json",
                "SARIMA_COST_STRESS.json",
                "SARIMA_FINAL_SUMMARY.json",
            ],
            "live_execution_enabled": False,
            "oanda_execution_enabled": False,
            "demo_execution_enabled": False,
            "broker_placement_enabled": False,
        },
    )
    return output_dir / "SARIMA_FINAL_SUMMARY.json"


def _self_test() -> None:
    specs = declared_specs()
    assert len(specs) == 270, len(specs)
    assert len({spec.spec_id for spec in specs}) == len(specs)
    viable_metrics = {
        "trades": 60,
        "pnl_usd": 1.0,
        "mean_net_pips": 0.1,
        "profit_factor": 1.01,
        "max_drawdown_pct": 1.0,
        "top_pair_trade_share": 0.30,
        "months": 2,
        "positive_months": 1,
    }
    assert not _viability(viable_metrics, 15, 11)["viable"]
    assert _viability(viable_metrics, 15, 12)["viable"]
    rng = np.random.default_rng(42)
    y = np.cumsum(rng.normal(0.0, 0.01, 300))
    spec = SarimaSpec(1, 1, 1, 1, 0, 1, 12)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fitted = _sarimax_model(y, spec).fit(disp=False, maxiter=5)
    assert np.isfinite(np.asarray(fitted.params, dtype=float)).all()
    print(
        json.dumps(
            {
                "self_test": "PASS",
                "declared_grid_size": len(specs),
                "statsmodels": statsmodels.__version__,
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2026-07-07")
    parser.add_argument("--tier", default="tier1")
    parser.add_argument("--pairs")
    parser.add_argument("--labels-path")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--maxiter-screen", type=int, default=8)
    parser.add_argument("--maxiter-final", type=int, default=35)
    parser.add_argument("--refine-per-category", type=int, default=5)
    parser.add_argument("--max-specs", type=int)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--force-data", action="store_true")
    parser.add_argument("--force-screen", action="store_true")
    parser.add_argument("--force-refinement", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return
    if not args.output_dir:
        parser.error("--output-dir is required unless --self-test is used")
    print(run_sweep(args), flush=True)


if __name__ == "__main__":
    main()
