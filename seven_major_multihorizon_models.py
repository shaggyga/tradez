"""Seven-major, multi-horizon FX forecasting with rolling windows and cost-aware metrics.

The pipeline intentionally separates:
1. causal M1 feature construction;
2. direct multi-output ML forecasts at 5/15/30/60 minutes;
3. ARIMA-family baselines on M5 bars derived from the same M1 feed;
4. chronological window stability analysis.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import warnings
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import (
    ExtraTreesRegressor,
    HistGradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge, SGDRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.multioutput import MultiOutputRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.arima.model import ARIMA


PAIRS = [
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
    "AUD_USD",
    "NZD_USD",
]
HORIZONS = [5, 15, 30, 60]
SAMPLE_MINUTES = 5
ROOT = Path("data") / "seven_major_research"
FEATURE_ROOT = ROOT / "features"
MODEL_ROOT = ROOT / "models"
REPORT_ROOT = ROOT / "reports"
BAM_ROOT = Path("data") / "oanda_training_manager" / "candles_bam"
PIPELINE_VERSION = "seven_major_multihorizon_v1"
WINDOW_OFFSETS_DAYS = [0, 60, 120, 180]
TRAIN_DAYS = 120
CALIBRATION_DAYS = 30
TEST_DAYS = 30
MAX_ROLLING_WINDOW = 240


def pip_multiplier(pair: str) -> float:
    return 100.0 if pair.endswith("JPY") else 10_000.0


def utc_timestamp(value: Any) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC") if pd.Timestamp(value).tzinfo is None else pd.Timestamp(value).tz_convert("UTC")


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Not JSON serializable: {type(value)!r}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=json_safe),
        encoding="utf-8",
    )


def load_bam(pair: str) -> pd.DataFrame:
    path = BAM_ROOT / f"{pair}_M1.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path)
    frame["time_utc"] = pd.to_datetime(
        frame.get("datetime", frame.get("time")), errors="coerce", utc=True
    )
    numeric = [
        "open",
        "high",
        "low",
        "close",
        "bid_close",
        "ask_close",
        "spread_pips",
        "volume",
    ]
    for column in numeric:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame = (
        frame.dropna(subset=["time_utc", "close", "bid_close", "ask_close", "spread_pips"])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .reset_index(drop=True)
    )
    frame = frame[frame["ask_close"] >= frame["bid_close"]]
    return frame


def exact_future(series: pd.Series, times: pd.Series, minutes: int) -> np.ndarray:
    indexed = pd.Series(series.to_numpy(), index=pd.DatetimeIndex(times))
    target_times = pd.DatetimeIndex(times) + pd.Timedelta(minutes=minutes)
    return indexed.reindex(target_times).to_numpy()


def rsi(close: pd.Series, window: int) -> pd.Series:
    change = close.diff()
    gain = change.clip(lower=0).rolling(window).mean()
    loss = (-change.clip(upper=0)).rolling(window).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def add_cross_pair_features(feature_frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    closes = {
        pair: frame.set_index("time_utc")["close"].astype(float)
        for pair, frame in feature_frames.items()
    }
    wide = pd.concat(closes, axis=1).sort_index()
    returns = np.log(wide).diff()
    usd_signed = returns.copy()
    for pair in PAIRS:
        usd_signed[pair] *= 1.0 if pair.startswith("USD_") else -1.0
    cross = pd.DataFrame(index=wide.index)
    cross["usd_strength_1"] = usd_signed.mean(axis=1)
    cross["cross_dispersion_1"] = returns.std(axis=1)
    for bars in [3, 6, 12, 24, 48]:
        cross[f"usd_strength_{bars}"] = usd_signed.rolling(bars).sum().mean(axis=1)
        cross[f"cross_dispersion_{bars}"] = returns.rolling(bars).sum().std(axis=1)
    signed_rank = usd_signed.rank(axis=1, pct=True)
    out: dict[str, pd.DataFrame] = {}
    for pair, frame in feature_frames.items():
        pair_cross = cross.copy()
        pair_cross["pair_return_rank_1"] = signed_rank[pair]
        joined = frame.join(pair_cross, on="time_utc")
        out[pair] = joined
    return out


def build_pair_features(pair: str) -> pd.DataFrame:
    raw = load_bam(pair)
    multiplier = pip_multiplier(pair)
    close = raw["close"].astype(float)
    log_close = np.log(close)
    gap = raw["time_utc"].diff().dt.total_seconds().div(60)
    raw["minutes_since_gap"] = (
        (~gap.fillna(999).le(10)).cumsum().groupby((~gap.fillna(999).le(10)).cumsum()).cumcount()
    )
    features = pd.DataFrame(
        {
            "time_utc": raw["time_utc"],
            "instrument": pair,
            "close": close,
            "bid_close": raw["bid_close"].astype(float),
            "ask_close": raw["ask_close"].astype(float),
            "spread_pips": raw["spread_pips"].astype(float),
            "volume": raw["volume"].astype(float),
            "gap_minutes": gap.clip(upper=10_000),
            "minutes_since_gap": raw["minutes_since_gap"],
        }
    )
    one_return = log_close.diff()
    for lag in [1, 2, 3, 5, 8, 13, 21, 30, 60, 120, 240]:
        features[f"log_return_lag_{lag}"] = one_return.shift(lag - 1)
        features[f"momentum_pips_{lag}"] = close.diff(lag) * multiplier
    ma_windows = [3, 5, 7, 8, 10, 13, 20, 30, 50, 60, 100, 120, 180, 240]
    ma: dict[int, pd.Series] = {}
    for window in ma_windows:
        ma[window] = close.rolling(window).mean()
        features[f"sma_gap_pips_{window}"] = (close - ma[window]) * multiplier
        features[f"sma_slope_pips_{window}_1"] = ma[window].diff() * multiplier
        features[f"sma_slope_pips_{window}_5"] = ma[window].diff(5) * multiplier
        ema = close.ewm(span=window, adjust=False).mean()
        features[f"ema_gap_pips_{window}"] = (close - ema) * multiplier
    for left, right in [(3, 5), (5, 8), (7, 8), (8, 13), (10, 20), (20, 30), (30, 50), (50, 100)]:
        features[f"sma_{left}_minus_{right}_pips"] = (ma[left] - ma[right]) * multiplier
        features[f"sma_{left}_gt_{right}"] = (ma[left] > ma[right]).astype(float)
    for change in [1, 2, 3, 5, 10, 15, 30]:
        features[f"sma30_change_{change}_pips"] = ma[30].diff(change) * multiplier
    for window in [5, 10, 15, 30, 60, 120, 240]:
        rolling_return = one_return.rolling(window)
        mean = rolling_return.mean()
        std = rolling_return.std()
        features[f"return_mean_{window}"] = mean
        features[f"return_vol_{window}"] = std
        features[f"return_z_{window}"] = (one_return - mean) / std.replace(0, np.nan)
        features[f"return_skew_{window}"] = rolling_return.skew()
        if window >= 30:
            features[f"return_kurt_{window}"] = rolling_return.kurt()
        rolling_min = close.rolling(window).min()
        rolling_max = close.rolling(window).max()
        features[f"range_position_{window}"] = (
            (close - rolling_min) / (rolling_max - rolling_min).replace(0, np.nan)
        )
    high = raw["high"].astype(float)
    low = raw["low"].astype(float)
    open_ = raw["open"].astype(float)
    true_range = pd.concat(
        [(high - low), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1
    ).max(axis=1)
    features["bar_range_pips"] = (high - low) * multiplier
    features["bar_body_pips"] = (close - open_) * multiplier
    for window in [5, 14, 30, 60]:
        features[f"atr_pips_{window}"] = true_range.rolling(window).mean() * multiplier
        features[f"rsi_{window}"] = rsi(close, window)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    features["macd_pips"] = macd * multiplier
    features["macd_signal_gap_pips"] = (macd - macd.ewm(span=9, adjust=False).mean()) * multiplier
    for window in [5, 15, 30, 60, 120]:
        spread_mean = features["spread_pips"].rolling(window).mean()
        spread_std = features["spread_pips"].rolling(window).std()
        volume_mean = features["volume"].rolling(window).mean()
        volume_std = features["volume"].rolling(window).std()
        features[f"spread_ratio_{window}"] = features["spread_pips"] / spread_mean.replace(0, np.nan)
        features[f"spread_z_{window}"] = (
            features["spread_pips"] - spread_mean
        ) / spread_std.replace(0, np.nan)
        features[f"volume_ratio_{window}"] = features["volume"] / volume_mean.replace(0, np.nan)
        features[f"volume_z_{window}"] = (
            features["volume"] - volume_mean
        ) / volume_std.replace(0, np.nan)
    minute_of_day = raw["time_utc"].dt.hour * 60 + raw["time_utc"].dt.minute
    day_of_week = raw["time_utc"].dt.dayofweek
    features["minute_sin"] = np.sin(2 * np.pi * minute_of_day / 1440)
    features["minute_cos"] = np.cos(2 * np.pi * minute_of_day / 1440)
    features["weekday_sin"] = np.sin(2 * np.pi * day_of_week / 7)
    features["weekday_cos"] = np.cos(2 * np.pi * day_of_week / 7)
    features["session_asia"] = ((raw["time_utc"].dt.hour >= 0) & (raw["time_utc"].dt.hour < 8)).astype(float)
    features["session_london"] = ((raw["time_utc"].dt.hour >= 7) & (raw["time_utc"].dt.hour < 16)).astype(float)
    features["session_new_york"] = ((raw["time_utc"].dt.hour >= 12) & (raw["time_utc"].dt.hour < 21)).astype(float)
    for horizon in HORIZONS:
        future_mid = exact_future(close, raw["time_utc"], horizon)
        future_bid = exact_future(raw["bid_close"], raw["time_utc"], horizon)
        future_ask = exact_future(raw["ask_close"], raw["time_utc"], horizon)
        features[f"target_return_pips_{horizon}"] = (future_mid - close.to_numpy()) * multiplier
        features[f"long_net_pips_{horizon}"] = (
            future_bid - raw["ask_close"].to_numpy()
        ) * multiplier
        features[f"short_net_pips_{horizon}"] = (
            raw["bid_close"].to_numpy() - future_ask
        ) * multiplier
    features = features[
        (features["time_utc"].dt.minute % SAMPLE_MINUTES == 0)
        & (features["minutes_since_gap"] >= MAX_ROLLING_WINDOW)
    ].copy()
    numeric_columns = features.select_dtypes(include=[np.number]).columns
    features[numeric_columns] = features[numeric_columns].astype("float32")
    return features.reset_index(drop=True)


def build_features() -> dict[str, Any]:
    FEATURE_ROOT.mkdir(parents=True, exist_ok=True)
    frames: dict[str, pd.DataFrame] = {}
    for number, pair in enumerate(PAIRS, 1):
        print(f"[features] pair={number}/{len(PAIRS)} {pair}", flush=True)
        frames[pair] = build_pair_features(pair)
        gc.collect()
    frames = add_cross_pair_features(frames)
    report: dict[str, Any] = {"pipeline_version": PIPELINE_VERSION, "pairs": {}}
    for pair, frame in frames.items():
        target_columns = [f"target_return_pips_{h}" for h in HORIZONS]
        frame = frame.dropna(subset=target_columns).replace([np.inf, -np.inf], np.nan)
        path = FEATURE_ROOT / f"{pair}_features.parquet"
        frame.to_parquet(path, index=False, compression="zstd")
        report["pairs"][pair] = {
            "rows": len(frame),
            "columns": len(frame.columns),
            "start": frame["time_utc"].min(),
            "end": frame["time_utc"].max(),
            "path": path,
        }
    report["feature_count"] = len(feature_columns(next(iter(frames.values()))))
    write_json(REPORT_ROOT / "latest_feature_report.json", report)
    return report


def feature_columns(frame: pd.DataFrame) -> list[str]:
    excluded_prefixes = ("target_", "long_net_", "short_net_")
    excluded = {
        "time_utc",
        "instrument",
        "close",
        "bid_close",
        "ask_close",
    }
    return [
        column
        for column in frame.columns
        if column not in excluded
        and not column.startswith(excluded_prefixes)
        and pd.api.types.is_numeric_dtype(frame[column])
    ]


@dataclass
class Window:
    window_id: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    calibration_end: pd.Timestamp
    test_end: pd.Timestamp


def windows_for(frame: pd.DataFrame) -> list[Window]:
    start = frame["time_utc"].min().ceil("D")
    windows: list[Window] = []
    for number, offset in enumerate(WINDOW_OFFSETS_DAYS, 1):
        train_start = start + pd.Timedelta(days=offset)
        train_end = train_start + pd.Timedelta(days=TRAIN_DAYS)
        calibration_end = train_end + pd.Timedelta(days=CALIBRATION_DAYS)
        test_end = calibration_end + pd.Timedelta(days=TEST_DAYS)
        if test_end <= frame["time_utc"].max():
            windows.append(Window(number, train_start, train_end, calibration_end, test_end))
    return windows


def model_specs(random_state: int = 19) -> dict[str, Any]:
    return {
        "ridge": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("model", Ridge(alpha=10.0)),
            ]
        ),
        "elastic_net_sgd": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                (
                    "model",
                    MultiOutputRegressor(
                        SGDRegressor(
                            loss="huber",
                            penalty="elasticnet",
                            alpha=0.0005,
                            l1_ratio=0.15,
                            max_iter=1200,
                            tol=1e-3,
                            early_stopping=True,
                            validation_fraction=0.1,
                            n_iter_no_change=10,
                            random_state=random_state,
                        ),
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
        "extra_trees": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    ExtraTreesRegressor(
                        n_estimators=100,
                        min_samples_leaf=12,
                        max_features=0.65,
                        n_jobs=-1,
                        random_state=random_state,
                    ),
                ),
            ]
        ),
        "random_forest": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestRegressor(
                        n_estimators=60,
                        min_samples_leaf=18,
                        max_features=0.5,
                        n_jobs=-1,
                        random_state=random_state,
                    ),
                ),
            ]
        ),
        "hist_gradient_boosting": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    MultiOutputRegressor(
                        HistGradientBoostingRegressor(
                            learning_rate=0.06,
                            max_iter=140,
                            max_leaf_nodes=31,
                            min_samples_leaf=30,
                            l2_regularization=1.0,
                            random_state=random_state,
                        ),
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
    }


def non_overlapping_indices(times: pd.Series, mask: np.ndarray, horizon: int) -> np.ndarray:
    chosen: list[int] = []
    next_allowed: pd.Timestamp | None = None
    for position in np.flatnonzero(mask):
        timestamp = times.iloc[position]
        if next_allowed is None or timestamp >= next_allowed:
            chosen.append(position)
            next_allowed = timestamp + pd.Timedelta(minutes=horizon)
    return np.asarray(chosen, dtype=int)


def trading_metrics(
    frame: pd.DataFrame,
    predictions: np.ndarray,
    horizon: int,
    threshold: float,
) -> dict[str, float | int]:
    predictions = np.asarray(predictions, dtype=float)
    signal_mask = (
        np.isfinite(predictions)
        & (np.abs(predictions) > 1e-9)
        & (np.abs(predictions) >= threshold)
    )
    selected = non_overlapping_indices(frame["time_utc"].reset_index(drop=True), signal_mask, horizon)
    if not len(selected):
        return {
            "trades": 0,
            "mean_net_pips": 0.0,
            "median_net_pips": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "total_net_pips": 0.0,
            "max_drawdown_pips": 0.0,
        }
    pred = predictions[selected]
    long_net = frame[f"long_net_pips_{horizon}"].to_numpy()[selected]
    short_net = frame[f"short_net_pips_{horizon}"].to_numpy()[selected]
    net = np.where(pred > 0, long_net, short_net)
    net = net[np.isfinite(net)]
    gains = net[net > 0].sum()
    losses = -net[net < 0].sum()
    curve = np.cumsum(net)
    drawdown = np.maximum.accumulate(np.r_[0.0, curve])[1:] - curve
    return {
        "trades": int(len(net)),
        "mean_net_pips": float(np.mean(net)) if len(net) else 0.0,
        "median_net_pips": float(np.median(net)) if len(net) else 0.0,
        "profit_factor": float(gains / losses) if losses > 0 else (float("inf") if gains > 0 else 0.0),
        "win_rate": float(np.mean(net > 0)) if len(net) else 0.0,
        "total_net_pips": float(np.sum(net)) if len(net) else 0.0,
        "max_drawdown_pips": float(np.max(drawdown)) if len(net) else 0.0,
    }


def choose_threshold(
    calibration: pd.DataFrame,
    predictions: np.ndarray,
    horizon: int,
) -> tuple[float, dict[str, Any]]:
    absolute = np.abs(np.asarray(predictions, dtype=float))
    finite = absolute[np.isfinite(absolute)]
    if not len(finite):
        return float("inf"), trading_metrics(calibration, predictions, horizon, float("inf"))
    candidates = sorted(
        set(float(np.quantile(finite, quantile)) for quantile in [0.0, 0.5, 0.7, 0.8, 0.9, 0.95])
    )
    rows = []
    for threshold in candidates:
        metrics = trading_metrics(calibration, predictions, horizon, threshold)
        rows.append((threshold, metrics))
    eligible = [row for row in rows if row[1]["trades"] >= 30]
    pool = eligible or rows
    threshold, metrics = max(
        pool,
        key=lambda row: (
            row[1]["mean_net_pips"],
            row[1]["profit_factor"],
            row[1]["total_net_pips"],
        ),
    )
    return threshold, metrics


def forecast_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    valid = np.isfinite(actual) & np.isfinite(predicted)
    actual, predicted = actual[valid], predicted[valid]
    if not len(actual):
        return {
            "mae_pips": float("nan"),
            "rmse_pips": float("nan"),
            "correlation": float("nan"),
            "direction_accuracy": float("nan"),
        }
    correlation = (
        float(np.corrcoef(actual, predicted)[0, 1])
        if len(actual) > 2 and np.std(actual) > 0 and np.std(predicted) > 0
        else 0.0
    )
    return {
        "mae_pips": float(mean_absolute_error(actual, predicted)),
        "rmse_pips": float(math.sqrt(mean_squared_error(actual, predicted))),
        "correlation": correlation,
        "direction_accuracy": float(np.mean(np.sign(actual) == np.sign(predicted))),
    }


def slice_window(frame: pd.DataFrame, window: Window, max_horizon: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    purge = pd.Timedelta(minutes=max_horizon)
    train = frame[
        (frame["time_utc"] >= window.train_start)
        & (frame["time_utc"] < window.train_end - purge)
    ].copy()
    calibration = frame[
        (frame["time_utc"] >= window.train_end)
        & (frame["time_utc"] < window.calibration_end - purge)
    ].copy()
    test = frame[
        (frame["time_utc"] >= window.calibration_end)
        & (frame["time_utc"] < window.test_end - purge)
    ].copy()
    return train, calibration, test


def evaluate_ml() -> list[dict[str, Any]]:
    MODEL_ROOT.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    target_columns = [f"target_return_pips_{h}" for h in HORIZONS]
    for pair_number, pair in enumerate(PAIRS, 1):
        frame = pd.read_parquet(FEATURE_ROOT / f"{pair}_features.parquet")
        frame["time_utc"] = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
        features = feature_columns(frame)
        for window in windows_for(frame):
            train, calibration, test = slice_window(frame, window, max(HORIZONS))
            if min(len(train), len(calibration), len(test)) < 500:
                continue
            x_train = train[features]
            y_train = train[target_columns].to_numpy(dtype=float)
            x_calibration = calibration[features]
            x_test = test[features]
            print(
                f"[ml] pair={pair_number}/{len(PAIRS)} {pair} window={window.window_id} "
                f"rows={len(train):,}/{len(calibration):,}/{len(test):,} features={len(features)}",
                flush=True,
            )
            for model_name, model in model_specs().items():
                print(
                    f"[ml-model] {pair} window={window.window_id} model={model_name}",
                    flush=True,
                )
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    model.fit(x_train, y_train)
                calibration_prediction = np.asarray(model.predict(x_calibration))
                test_prediction = np.asarray(model.predict(x_test))
                artifact_path = MODEL_ROOT / f"{pair}_w{window.window_id}_{model_name}.joblib"
                joblib.dump(
                    {
                        "pipeline_version": PIPELINE_VERSION,
                        "pair": pair,
                        "window": window.__dict__,
                        "features": features,
                        "horizons": HORIZONS,
                        "model": model,
                    },
                    artifact_path,
                    compress=3,
                )
                for column_number, horizon in enumerate(HORIZONS):
                    threshold, calibration_trading = choose_threshold(
                        calibration,
                        calibration_prediction[:, column_number],
                        horizon,
                    )
                    row = {
                        "family": "ML",
                        "model": model_name,
                        "pair": pair,
                        "window_id": window.window_id,
                        "train_start": window.train_start,
                        "train_end": window.train_end,
                        "calibration_end": window.calibration_end,
                        "test_end": window.test_end,
                        "horizon_minutes": horizon,
                        "train_rows": len(train),
                        "calibration_rows": len(calibration),
                        "test_rows": len(test),
                        "feature_count": len(features),
                        "threshold_pips": threshold,
                        **{
                            f"calibration_{key}": value
                            for key, value in calibration_trading.items()
                        },
                        **forecast_metrics(
                            test[f"target_return_pips_{horizon}"].to_numpy(),
                            test_prediction[:, column_number],
                        ),
                        **trading_metrics(
                            test,
                            test_prediction[:, column_number],
                            horizon,
                            threshold,
                        ),
                        "artifact": artifact_path,
                    }
                    results.append(row)
    return results


ARIMA_SPECS = [
    ((0, 1, 0), "n"),
    ((0, 1, 0), "t"),
    ((1, 1, 0), "n"),
    ((1, 1, 0), "t"),
    ((2, 1, 0), "n"),
    ((3, 1, 0), "n"),
    ((5, 1, 0), "n"),
    ((8, 1, 0), "n"),
    ((0, 1, 1), "n"),
    ((0, 1, 2), "n"),
    ((1, 1, 1), "n"),
    ((2, 1, 1), "n"),
    ((1, 1, 2), "n"),
    ((2, 1, 2), "n"),
    ((3, 1, 1), "n"),
]


def m5_frame(pair: str) -> pd.DataFrame:
    raw = load_bam(pair).set_index("time_utc")
    frame = pd.DataFrame(
        {
            "close": raw["close"].resample("5min").last(),
            "bid_close": raw["bid_close"].resample("5min").last(),
            "ask_close": raw["ask_close"].resample("5min").last(),
        }
    ).dropna()
    gap = frame.index.to_series().diff().dt.total_seconds().div(60)
    frame = frame[gap.fillna(999).le(10) | gap.isna()].copy()
    frame["time_utc"] = frame.index
    multiplier = pip_multiplier(pair)
    for horizon in HORIZONS:
        future_mid = exact_future(frame["close"], frame["time_utc"], horizon)
        future_bid = exact_future(frame["bid_close"], frame["time_utc"], horizon)
        future_ask = exact_future(frame["ask_close"], frame["time_utc"], horizon)
        frame[f"target_return_pips_{horizon}"] = (
            future_mid - frame["close"].to_numpy()
        ) * multiplier
        frame[f"long_net_pips_{horizon}"] = (
            future_bid - frame["ask_close"].to_numpy()
        ) * multiplier
        frame[f"short_net_pips_{horizon}"] = (
            frame["bid_close"].to_numpy() - future_ask
        ) * multiplier
    return frame.dropna().reset_index(drop=True)


def arima_predictions(
    train: pd.DataFrame,
    calibration: pd.DataFrame,
    test: pd.DataFrame,
    order: tuple[int, int, int],
    trend: str,
    pair: str,
) -> tuple[np.ndarray, np.ndarray]:
    multiplier = pip_multiplier(pair)
    train_series = np.log(train["close"].to_numpy(dtype=float)[-12_000:])
    continuation = np.log(
        pd.concat([calibration["close"], test["close"]], ignore_index=True).to_numpy(dtype=float)
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fitted = ARIMA(
            train_series,
            order=order,
            trend=trend,
            enforce_stationarity=False,
            enforce_invertibility=False,
        ).fit(method_kwargs={"maxiter": 60})
        applied = fitted.append(continuation, refit=False)
    train_length = len(train_series)
    calibration_length = len(calibration)
    max_steps = max(HORIZONS) // 5

    def predict_segment(start: int, length: int) -> np.ndarray:
        output = np.full((length, len(HORIZONS)), np.nan, dtype=float)
        origin_positions = range(0, length, 12)
        for local_position in origin_positions:
            absolute_origin = train_length + start + local_position - 1
            if absolute_origin < 0:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                predicted = applied.get_prediction(
                    start=absolute_origin + 1,
                    end=absolute_origin + max_steps,
                    dynamic=True,
                ).predicted_mean
            current_log = np.asarray(applied.model.endog, dtype=float).reshape(-1)[absolute_origin]
            current = math.exp(float(current_log))
            predicted_log = np.asarray(predicted, dtype=float)
            valid_forecast = np.isfinite(predicted_log) & (
                np.abs(predicted_log - current_log) <= 0.05
            )
            predicted_prices = np.full_like(predicted_log, np.nan, dtype=float)
            predicted_prices[valid_forecast] = np.exp(predicted_log[valid_forecast])
            for column, horizon in enumerate(HORIZONS):
                step = horizon // 5
                if step <= len(predicted_prices):
                    output[local_position, column] = (
                        predicted_prices[step - 1] - current
                    ) * multiplier
        return output

    return (
        predict_segment(0, calibration_length),
        predict_segment(calibration_length, len(test)),
    )


def evaluate_arima_pair(pair: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    frame = m5_frame(pair)
    for window in windows_for(frame):
        train, calibration, test = slice_window(frame, window, max(HORIZONS))
        if min(len(train), len(calibration), len(test)) < 500:
            continue
        for spec_number, (order, trend) in enumerate(ARIMA_SPECS, 1):
            model_name = f"ARIMA{order}_trend_{trend}"
            print(
                f"[arima] {pair} window={window.window_id} "
                f"spec={spec_number}/{len(ARIMA_SPECS)} {model_name}",
                flush=True,
            )
            try:
                calibration_prediction, test_prediction = arima_predictions(
                    train, calibration, test, order, trend, pair
                )
            except Exception as exc:
                results.append(
                    {
                        "family": "ARIMA",
                        "model": model_name,
                        "pair": pair,
                        "window_id": window.window_id,
                        "error": repr(exc),
                    }
                )
                continue
            for column, horizon in enumerate(HORIZONS):
                valid_cal = np.isfinite(calibration_prediction[:, column])
                valid_test = np.isfinite(test_prediction[:, column])
                calibration_subset = calibration.loc[valid_cal].reset_index(drop=True)
                test_subset = test.loc[valid_test].reset_index(drop=True)
                cal_pred = calibration_prediction[valid_cal, column]
                test_pred = test_prediction[valid_test, column]
                threshold, calibration_trading = choose_threshold(
                    calibration_subset, cal_pred, horizon
                )
                results.append(
                    {
                        "family": "ARIMA",
                        "model": model_name,
                        "pair": pair,
                        "window_id": window.window_id,
                        "train_start": window.train_start,
                        "train_end": window.train_end,
                        "calibration_end": window.calibration_end,
                        "test_end": window.test_end,
                        "horizon_minutes": horizon,
                        "train_rows": len(train),
                        "calibration_rows": len(calibration_subset),
                        "test_rows": len(test_subset),
                        "feature_count": 1,
                        "threshold_pips": threshold,
                        **{
                            f"calibration_{key}": value
                            for key, value in calibration_trading.items()
                        },
                        **forecast_metrics(
                            test_subset[f"target_return_pips_{horizon}"].to_numpy(),
                            test_pred,
                        ),
                        **trading_metrics(
                            test_subset,
                            test_pred,
                            horizon,
                            threshold,
                        ),
                    }
                )
    return results


def evaluate_arima() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=min(3, len(PAIRS))) as executor:
        for pair_results in executor.map(evaluate_arima_pair, PAIRS):
            results.extend(pair_results)
    return results


def evaluate_rules() -> list[dict[str, Any]]:
    rules = {
        "sma7_gt_sma8": lambda frame: np.sign(frame["sma_7_minus_8_pips"].to_numpy()),
        "sma30_change_2": lambda frame: np.sign(frame["sma30_change_2_pips"].to_numpy()),
        "sma30_change_5": lambda frame: np.sign(frame["sma30_change_5_pips"].to_numpy()),
        "sma20_gt_sma30": lambda frame: np.sign(frame["sma_20_minus_30_pips"].to_numpy()),
        "momentum_13": lambda frame: np.sign(frame["momentum_pips_13"].to_numpy()),
        "momentum_60": lambda frame: np.sign(frame["momentum_pips_60"].to_numpy()),
    }
    results: list[dict[str, Any]] = []
    for pair in PAIRS:
        frame = pd.read_parquet(FEATURE_ROOT / f"{pair}_features.parquet")
        frame["time_utc"] = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
        for window in windows_for(frame):
            _, calibration, test = slice_window(frame, window, max(HORIZONS))
            for rule_name, function in rules.items():
                calibration_prediction = function(calibration)
                test_prediction = function(test)
                for horizon in HORIZONS:
                    threshold, calibration_trading = choose_threshold(
                        calibration, calibration_prediction, horizon
                    )
                    results.append(
                        {
                            "family": "RULE",
                            "model": rule_name,
                            "pair": pair,
                            "window_id": window.window_id,
                            "train_start": window.train_start,
                            "train_end": window.train_end,
                            "calibration_end": window.calibration_end,
                            "test_end": window.test_end,
                            "horizon_minutes": horizon,
                            "train_rows": 0,
                            "calibration_rows": len(calibration),
                            "test_rows": len(test),
                            "feature_count": 1,
                            "threshold_pips": threshold,
                            **{
                                f"calibration_{key}": value
                                for key, value in calibration_trading.items()
                            },
                            **forecast_metrics(
                                test[f"target_return_pips_{horizon}"].to_numpy(),
                                test_prediction,
                            ),
                            **trading_metrics(
                                test, test_prediction, horizon, threshold
                            ),
                        }
                    )
    return results


def stability_report(results: pd.DataFrame) -> pd.DataFrame:
    valid = results[results.get("error").isna()] if "error" in results else results
    grouped = valid.groupby(["family", "model", "pair", "horizon_minutes"], dropna=False)
    rows: list[dict[str, Any]] = []
    for key, group in grouped:
        net = pd.to_numeric(group["mean_net_pips"], errors="coerce")
        accuracy = pd.to_numeric(group["direction_accuracy"], errors="coerce")
        correlation = pd.to_numeric(group["correlation"], errors="coerce")
        rows.append(
            {
                "family": key[0],
                "model": key[1],
                "pair": key[2],
                "horizon_minutes": key[3],
                "windows": group["window_id"].nunique(),
                "mean_net_pips_avg": net.mean(),
                "mean_net_pips_min": net.min(),
                "mean_net_pips_max": net.max(),
                "mean_net_pips_std": net.std(),
                "profitable_windows": int((net > 0).sum()),
                "net_sign_changed": bool((net > 0).any() and (net < 0).any()),
                "direction_accuracy_avg": accuracy.mean(),
                "direction_accuracy_range": accuracy.max() - accuracy.min(),
                "correlation_avg": correlation.mean(),
                "correlation_min": correlation.min(),
                "correlation_max": correlation.max(),
                "correlation_sign_changed": bool(
                    (correlation > 0).any() and (correlation < 0).any()
                ),
                "drastic_window_difference": bool(
                    ((net > 0).any() and (net < 0).any())
                    or (accuracy.max() - accuracy.min() >= 0.05)
                    or ((correlation > 0).any() and (correlation < 0).any())
                ),
            }
        )
    return pd.DataFrame(rows)


def summarize(results: pd.DataFrame, stability: pd.DataFrame) -> dict[str, Any]:
    valid = results[results.get("error").isna()] if "error" in results else results
    ranked = stability.sort_values(
        ["profitable_windows", "mean_net_pips_avg", "correlation_avg"],
        ascending=[False, False, False],
    )
    return {
        "pipeline_version": PIPELINE_VERSION,
        "pairs": PAIRS,
        "horizons_minutes": HORIZONS,
        "window_definition": {
            "training_days": TRAIN_DAYS,
            "calibration_days": CALIBRATION_DAYS,
            "test_days": TEST_DAYS,
            "offset_days": WINDOW_OFFSETS_DAYS,
            "purge_minutes": max(HORIZONS),
        },
        "result_rows": len(results),
        "successful_result_rows": len(valid),
        "models": sorted(valid["model"].dropna().unique().tolist()),
        "drastic_difference_rate": float(stability["drastic_window_difference"].mean()),
        "stable_positive_candidates": ranked[
            (ranked["profitable_windows"] == ranked["windows"])
            & (ranked["mean_net_pips_min"] > 0)
            & (~ranked["drastic_window_difference"])
        ]
        .head(30)
        .to_dict(orient="records"),
        "top_by_average_net": ranked.head(30).to_dict(orient="records"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["features", "rules", "ml", "arima", "all"],
        default="all",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    if args.mode in {"features", "all"}:
        feature_report = build_features()
        print(json.dumps(feature_report, indent=2, default=json_safe), flush=True)
    if args.mode == "features":
        return 0
    if not all((FEATURE_ROOT / f"{pair}_features.parquet").exists() for pair in PAIRS):
        build_features()
    result_frames: list[pd.DataFrame] = []
    existing_path = REPORT_ROOT / "model_window_results.csv"
    if existing_path.exists() and args.mode != "all":
        result_frames.append(pd.read_csv(existing_path))
    if args.mode in {"rules", "all"}:
        result_frames.append(pd.DataFrame(evaluate_rules()))
    if args.mode in {"ml", "all"}:
        result_frames.append(pd.DataFrame(evaluate_ml()))
    if args.mode in {"arima", "all"}:
        result_frames.append(pd.DataFrame(evaluate_arima()))
    results = pd.concat(result_frames, ignore_index=True)
    dedupe = ["family", "model", "pair", "window_id", "horizon_minutes"]
    results = results.drop_duplicates(dedupe, keep="last")
    results.to_csv(existing_path, index=False)
    stability = stability_report(results)
    stability.to_csv(REPORT_ROOT / "window_stability.csv", index=False)
    summary = summarize(results, stability)
    write_json(REPORT_ROOT / "latest_multihorizon_summary.json", summary)
    print(json.dumps(summary, indent=2, default=json_safe), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
