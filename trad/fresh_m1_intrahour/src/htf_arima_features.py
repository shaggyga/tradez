from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import pip_size_for


# AR(p) on differenced log price is the ARIMA(p,1,0) family. The rolling
# implementation keeps this feature block available in the Python 3.13 research
# runtime, where statsmodels is not installed.
ARIMA_ORDERS = (1, 2, 3, 5, 8, 13)
RETURN_LAGS = (1, 2, 3, 5, 8, 13)
DRIFT_WINDOWS = (6, 12, 24, 72, 120)
VOLATILITY_WINDOWS = (24, 72, 120)
ARIMA_REFIT_INTERVAL = 24
ARIMA_TRAIN_WINDOW = 720
ARIMA_MIN_TRAIN_SAMPLES = 96
ARIMA_RESIDUAL_WINDOW = 120
MAX_CLOSE_STALENESS_MINUTES = 20


def arima_feature_columns() -> list[str]:
    columns = [
        "arima_source_available",
        "arima_source_staleness_minutes",
    ]
    columns.extend(f"arima_return_lag_{lag}_pips" for lag in RETURN_LAGS)
    columns.extend(f"arima_drift_{window}h_pips" for window in DRIFT_WINDOWS)
    columns.extend(
        f"arima_realized_vol_{window}h_pips" for window in VOLATILITY_WINDOWS
    )
    for order in ARIMA_ORDERS:
        columns.extend([
            f"arima_p{order}_forecast_1h_pips",
            f"arima_p{order}_forecast_2h_pips",
        ])
    for horizon in (1, 2):
        columns.extend([
            f"arima_ensemble_mean_{horizon}h_pips",
            f"arima_ensemble_median_{horizon}h_pips",
            f"arima_ensemble_std_{horizon}h_pips",
        ])
    columns.extend([
        "arima_forecast_return_1h",
        "arima_forecast_return_2h",
        "arima_forecast_z_2h",
        "arima_forecast_slope_pips",
        "arima_direction",
        "arima_direction_flip",
        "arima_confidence",
        "arima_residual_pips",
        "arima_residual_z",
        "arima_residual_autocorr_5",
        "arima_residual_autocorr_20",
        "arima_order_id",
        "arima_selected_forecast_1h_pips",
        "arima_selected_forecast_2h_pips",
        "arima_selected_rolling_rmse_pips",
        "arima_fit_sample_count",
    ])
    return columns


ARIMA_FEATURE_COLUMNS = arima_feature_columns()


def _numeric_column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def _fit_ar_model(
    returns: np.ndarray,
    through_index: int,
    order: int,
    *,
    train_window: int,
    min_train_samples: int,
) -> dict[str, Any] | None:
    first_target = max(order, through_index - train_window + 1)
    target_indices = np.arange(first_target, through_index + 1, dtype=int)
    if target_indices.size < min_train_samples:
        return None
    x = np.column_stack(
        [returns[target_indices - lag] for lag in range(1, order + 1)]
    )
    y = returns[target_indices]
    valid = np.isfinite(y) & np.isfinite(x).all(axis=1)
    x = x[valid]
    y = y[valid]
    if len(y) < min_train_samples:
        return None

    x_mean = x.mean(axis=0)
    x_scale = x.std(axis=0, ddof=0)
    x_scale = np.where(x_scale > 1e-12, x_scale, 1.0)
    y_mean = float(y.mean())
    y_scale = float(y.std(ddof=0))
    standardized = (x - x_mean) / x_scale
    centered_y = y - y_mean
    gram = standardized.T @ standardized
    # A small ridge term stabilizes closely related return lags without changing
    # the AR model family or requiring sklearn/statsmodels at feature-build time.
    ridge = 1.0
    try:
        beta = np.linalg.solve(
            gram + ridge * np.eye(order, dtype=float),
            standardized.T @ centered_y,
        )
    except np.linalg.LinAlgError:
        beta = np.linalg.lstsq(
            gram + ridge * np.eye(order, dtype=float),
            standardized.T @ centered_y,
            rcond=None,
        )[0]
    return {
        "x_mean": x_mean,
        "x_scale": x_scale,
        "y_mean": y_mean,
        "y_scale": max(y_scale, 1e-12),
        "beta": beta,
        "sample_count": int(len(y)),
    }


def _predict_ar(model: dict[str, Any], lag_values: np.ndarray) -> float:
    if not np.isfinite(lag_values).all():
        return math_nan()
    standardized = (lag_values - model["x_mean"]) / model["x_scale"]
    forecast = float(model["y_mean"] + standardized @ model["beta"])
    limit = 8.0 * float(model["y_scale"])
    return float(np.clip(forecast, model["y_mean"] - limit, model["y_mean"] + limit))


def math_nan() -> float:
    return float("nan")


def _rolling_ar_forecasts(
    returns: np.ndarray,
    order: int,
    *,
    refit_interval: int,
    train_window: int,
    min_train_samples: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    count = len(returns)
    forecast_1h = np.full(count, np.nan, dtype=float)
    forecast_2h = np.full(count, np.nan, dtype=float)
    sample_count = np.full(count, np.nan, dtype=float)
    for refit_index in range(0, count, refit_interval):
        model = _fit_ar_model(
            returns,
            refit_index,
            order,
            train_window=train_window,
            min_train_samples=min_train_samples,
        )
        if model is None:
            continue
        block_end = min(count, refit_index + refit_interval)
        for index in range(refit_index, block_end):
            if index - order + 1 < 0:
                continue
            lags = np.asarray(
                [returns[index - lag + 1] for lag in range(1, order + 1)],
                dtype=float,
            )
            first = _predict_ar(model, lags)
            if not np.isfinite(first):
                continue
            recursive_lags = np.concatenate(([first], lags[:-1]))
            second = _predict_ar(model, recursive_lags)
            forecast_1h[index] = first
            forecast_2h[index] = first + second if np.isfinite(second) else np.nan
            sample_count[index] = float(model["sample_count"])
    return forecast_1h, forecast_2h, sample_count


def build_pair_causal_arima_features(
    closes: pd.Series | np.ndarray,
    pip_size: float,
    *,
    refit_interval: int = ARIMA_REFIT_INTERVAL,
    train_window: int = ARIMA_TRAIN_WINDOW,
    min_train_samples: int = ARIMA_MIN_TRAIN_SAMPLES,
) -> pd.DataFrame:
    close = pd.to_numeric(pd.Series(closes), errors="coerce").to_numpy(dtype=float)
    count = len(close)
    log_close = np.full(count, np.nan, dtype=float)
    positive = np.isfinite(close) & (close > 0.0)
    log_close[positive] = np.log(close[positive])
    returns = np.full(count, np.nan, dtype=float)
    if count > 1:
        valid_pair = positive[1:] & positive[:-1]
        changes = log_close[1:] - log_close[:-1]
        returns[1:] = np.where(valid_pair, changes, np.nan)

    pip_returns = np.full(count, np.nan, dtype=float)
    if count > 1:
        valid_pair = positive[1:] & positive[:-1]
        changes = close[1:] - close[:-1]
        pip_returns[1:] = np.where(valid_pair, changes / pip_size, np.nan)

    features = pd.DataFrame(index=np.arange(count))
    for lag in RETURN_LAGS:
        features[f"arima_return_lag_{lag}_pips"] = pd.Series(pip_returns).shift(lag - 1)
    pip_series = pd.Series(pip_returns)
    for window in DRIFT_WINDOWS:
        features[f"arima_drift_{window}h_pips"] = pip_series.rolling(
            window, min_periods=max(4, window // 3)
        ).mean()
    for window in VOLATILITY_WINDOWS:
        features[f"arima_realized_vol_{window}h_pips"] = pip_series.rolling(
            window, min_periods=max(8, window // 3)
        ).std(ddof=0)

    log_forecasts_1h: dict[int, np.ndarray] = {}
    log_forecasts_2h: dict[int, np.ndarray] = {}
    fit_counts: dict[int, np.ndarray] = {}
    pips_forecasts_1h: dict[int, np.ndarray] = {}
    pips_forecasts_2h: dict[int, np.ndarray] = {}
    for order in ARIMA_ORDERS:
        forecast_1h, forecast_2h, fitted_count = _rolling_ar_forecasts(
            returns,
            order,
            refit_interval=refit_interval,
            train_window=train_window,
            min_train_samples=min_train_samples,
        )
        log_forecasts_1h[order] = forecast_1h
        log_forecasts_2h[order] = forecast_2h
        fit_counts[order] = fitted_count
        pips_1h = close * np.expm1(forecast_1h) / pip_size
        pips_2h = close * np.expm1(forecast_2h) / pip_size
        pips_forecasts_1h[order] = pips_1h
        pips_forecasts_2h[order] = pips_2h
        features[f"arima_p{order}_forecast_1h_pips"] = pips_1h
        features[f"arima_p{order}_forecast_2h_pips"] = pips_2h

    log_1h = pd.DataFrame(log_forecasts_1h)
    log_2h = pd.DataFrame(log_forecasts_2h)
    pips_1h = pd.DataFrame(pips_forecasts_1h)
    pips_2h = pd.DataFrame(pips_forecasts_2h)
    for horizon, forecast_frame in ((1, pips_1h), (2, pips_2h)):
        features[f"arima_ensemble_mean_{horizon}h_pips"] = forecast_frame.mean(axis=1)
        features[f"arima_ensemble_median_{horizon}h_pips"] = forecast_frame.median(axis=1)
        features[f"arima_ensemble_std_{horizon}h_pips"] = forecast_frame.std(
            axis=1, ddof=0
        )

    ensemble_log_1h = log_1h.mean(axis=1)
    ensemble_log_2h = log_2h.mean(axis=1)
    features["arima_forecast_return_1h"] = ensemble_log_1h
    features["arima_forecast_return_2h"] = ensemble_log_2h
    mean_1h = features["arima_ensemble_mean_1h_pips"]
    mean_2h = features["arima_ensemble_mean_2h_pips"]
    std_2h = features["arima_ensemble_std_2h_pips"]
    vol_24h = features["arima_realized_vol_24h_pips"]
    features["arima_forecast_z_2h"] = mean_2h / (vol_24h * np.sqrt(2.0)).replace(0.0, np.nan)
    features["arima_forecast_slope_pips"] = mean_2h - mean_1h
    direction = np.sign(mean_2h).where(mean_2h.notna())
    features["arima_direction"] = direction
    previous_direction = direction.shift(1)
    features["arima_direction_flip"] = np.where(
        direction.notna() & previous_direction.notna(),
        (direction != previous_direction).astype(float),
        np.nan,
    )
    sign_votes = np.sign(pips_2h)
    vote_count = sign_votes.notna().sum(axis=1).replace(0, np.nan)
    vote_agreement = sign_votes.sum(axis=1).abs() / vote_count
    magnitude_confidence = mean_2h.abs() / (mean_2h.abs() + std_2h + 1e-12)
    features["arima_confidence"] = vote_agreement * magnitude_confidence

    residual_log = pd.Series(returns) - ensemble_log_1h.shift(1)
    residual_pips = residual_log * pd.Series(close) / pip_size
    features["arima_residual_pips"] = residual_pips
    residual_mean = residual_pips.rolling(
        ARIMA_RESIDUAL_WINDOW, min_periods=24
    ).mean().shift(1)
    residual_std = residual_pips.rolling(
        ARIMA_RESIDUAL_WINDOW, min_periods=24
    ).std(ddof=0).shift(1)
    features["arima_residual_z"] = (
        (residual_pips - residual_mean) / residual_std.replace(0.0, np.nan)
    )
    features["arima_residual_autocorr_5"] = residual_pips.rolling(
        5, min_periods=4
    ).corr(residual_pips.shift(1))
    features["arima_residual_autocorr_20"] = residual_pips.rolling(
        20, min_periods=10
    ).corr(residual_pips.shift(1))

    order_rmse: dict[int, pd.Series] = {}
    for order in ARIMA_ORDERS:
        order_residual_log = pd.Series(returns) - pd.Series(
            log_forecasts_1h[order]
        ).shift(1)
        order_residual_pips = order_residual_log * pd.Series(close) / pip_size
        order_rmse[order] = order_residual_pips.pow(2).rolling(
            ARIMA_RESIDUAL_WINDOW, min_periods=24
        ).mean().pow(0.5)
    rmse_frame = pd.DataFrame(order_rmse)
    selected_order = np.full(count, np.nan, dtype=float)
    selected_1h = np.full(count, np.nan, dtype=float)
    selected_2h = np.full(count, np.nan, dtype=float)
    selected_rmse = np.full(count, np.nan, dtype=float)
    rmse_values = rmse_frame.to_numpy(dtype=float)
    for index in range(count):
        finite = np.isfinite(rmse_values[index])
        if not finite.any():
            continue
        available_indices = np.flatnonzero(finite)
        best_position = available_indices[
            int(np.argmin(rmse_values[index, available_indices]))
        ]
        order = ARIMA_ORDERS[int(best_position)]
        selected_order[index] = float(order)
        selected_1h[index] = pips_forecasts_1h[order][index]
        selected_2h[index] = pips_forecasts_2h[order][index]
        selected_rmse[index] = rmse_values[index, best_position]
    features["arima_order_id"] = selected_order
    features["arima_selected_forecast_1h_pips"] = selected_1h
    features["arima_selected_forecast_2h_pips"] = selected_2h
    features["arima_selected_rolling_rmse_pips"] = selected_rmse
    features["arima_fit_sample_count"] = pd.DataFrame(fit_counts).median(axis=1)
    return features


def _align_completed_closes(
    decision_rows: pd.DataFrame,
    candles: pd.DataFrame,
) -> pd.DataFrame:
    left = decision_rows[["_arima_row_id", "decision_time_utc"]].copy()
    left["decision_time_utc"] = pd.to_datetime(
        left["decision_time_utc"], utc=True, errors="coerce"
    )
    left = left.sort_values("decision_time_utc")

    right = candles.copy()
    right["datetime"] = pd.to_datetime(right["datetime"], utc=True, errors="coerce")
    bid_close = _numeric_column(right, "bid_close")
    ask_close = _numeric_column(right, "ask_close")
    native_mid = (bid_close + ask_close) / 2.0
    right["_arima_close"] = native_mid.fillna(_numeric_column(right, "close"))
    # OANDA M1 timestamps mark bar starts. A close becomes usable one minute later.
    right["_arima_close_available_time"] = right["datetime"] + pd.Timedelta(minutes=1)
    right = (
        right.dropna(subset=["_arima_close_available_time", "_arima_close"])
        .sort_values("_arima_close_available_time")
        .drop_duplicates("_arima_close_available_time", keep="last")
    )
    if right.empty:
        left["_arima_close_available_time"] = pd.NaT
        left["_arima_close"] = np.nan
        return left
    return pd.merge_asof(
        left,
        right[["_arima_close_available_time", "_arima_close"]],
        left_on="decision_time_utc",
        right_on="_arima_close_available_time",
        direction="backward",
        tolerance=pd.Timedelta(minutes=MAX_CLOSE_STALENESS_MINUTES),
        allow_exact_matches=True,
    )


def _read_pair_candles(
    path: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    wanted = {"datetime", "close", "bid_close", "ask_close"}
    candles = pd.read_csv(path, usecols=lambda column: column in wanted)
    candles["datetime"] = pd.to_datetime(candles["datetime"], utc=True, errors="coerce")
    return candles[
        (candles["datetime"] >= start - pd.Timedelta(minutes=MAX_CLOSE_STALENESS_MINUTES))
        & (candles["datetime"] < end)
    ].copy()


def add_causal_h1_arima_features(
    frame: pd.DataFrame,
    candles_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = frame.reset_index(drop=True).copy()
    work["_arima_row_id"] = np.arange(len(work), dtype=int)
    pieces: list[pd.DataFrame] = []
    pair_audits: list[dict[str, Any]] = []
    for instrument, pair_rows in work.groupby("instrument", sort=True):
        pair_rows = pair_rows.sort_values("decision_time_utc")
        candle_path = candles_dir / f"{instrument}_M1.csv"
        candles = _read_pair_candles(
            candle_path,
            pair_rows["decision_time_utc"].min(),
            pair_rows["decision_time_utc"].max(),
        )
        aligned = _align_completed_closes(pair_rows, candles)
        causal_alignment = (
            aligned["_arima_close_available_time"].isna()
            | (
                aligned["_arima_close_available_time"]
                <= aligned["decision_time_utc"]
            )
        )
        if not bool(causal_alignment.all()):
            raise AssertionError(f"future candle used by ARIMA features for {instrument}")
        staleness = (
            aligned["decision_time_utc"]
            - aligned["_arima_close_available_time"]
        ).dt.total_seconds() / 60.0
        pair_features = build_pair_causal_arima_features(
            aligned["_arima_close"],
            pip_size_for(str(instrument)),
        )
        pair_features["arima_source_available"] = aligned[
            "_arima_close"
        ].notna().astype(float).to_numpy()
        pair_features["arima_source_staleness_minutes"] = staleness.to_numpy()
        pair_features["_arima_row_id"] = aligned["_arima_row_id"].to_numpy()
        pieces.append(pair_features[["_arima_row_id", *ARIMA_FEATURE_COLUMNS]])
        forecast_coverage = pair_features[
            "arima_ensemble_mean_2h_pips"
        ].notna().mean()
        pair_audits.append({
            "instrument": str(instrument),
            "rows": int(len(pair_rows)),
            "completed_close_coverage": float(aligned["_arima_close"].notna().mean()),
            "forecast_2h_coverage": float(forecast_coverage),
            "maximum_staleness_minutes": (
                float(staleness.max()) if staleness.notna().any() else None
            ),
            "future_close_count": int((~causal_alignment).sum()),
        })

    feature_frame = pd.concat(pieces, ignore_index=True)
    work = work.merge(feature_frame, on="_arima_row_id", how="left", validate="one_to_one")
    work = work.drop(columns=["_arima_row_id"])
    for column in ARIMA_FEATURE_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce").astype("float32")
    forecast_coverage = float(work["arima_ensemble_mean_2h_pips"].notna().mean())
    audit = {
        "implementation": "rolling AR(p) on completed H1 log returns; equivalent to ARIMA(p,1,0) log-price forecasts",
        "orders": list(ARIMA_ORDERS),
        "forecast_horizons_hours": [1, 2],
        "refit_interval_hours": ARIMA_REFIT_INTERVAL,
        "trailing_train_window_hours": ARIMA_TRAIN_WINDOW,
        "minimum_train_samples": ARIMA_MIN_TRAIN_SAMPLES,
        "feature_count": len(ARIMA_FEATURE_COLUMNS),
        "completed_close_contract": "last M1 close whose one-minute bar completion is at or before the H1 decision time",
        "uses_future_candles": False,
        "uses_labels_or_realized_forward_outcomes": False,
        "statsmodels_required": False,
        "overall_completed_close_coverage": float(work["arima_source_available"].mean()),
        "overall_forecast_2h_coverage": forecast_coverage,
        "pairs": pair_audits,
    }
    return work, audit
