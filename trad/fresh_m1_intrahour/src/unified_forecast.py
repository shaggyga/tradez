from __future__ import annotations

import gc
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pandas.api.indexers import FixedForwardWindowIndexer
from scipy.stats import norm, spearmanr
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.multioutput import MultiOutputRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .common import pip_size_for, tier_for, utc_now_stamp, write_json


TIMEFRAME_MINUTES = {"M1": 1, "M30": 30, "H1": 60, "H4": 240}
FEATURE_SCHEMA_VERSION = "unified_intrahour_features_v5_opportunity"
WINDOWS = (2, 3, 5, 8, 13, 21, 30, 60)
STAT_WINDOWS = (5, 15, 30, 60)
BARRIER_PIPS = (1, 3, 5)
OPPORTUNITY_HORIZONS = (5, 15, 30, 60)
CATEGORICAL_FEATURES = ("instrument", "base_currency", "quote_currency")
IDENTITY_COLUMNS = {
    "decision_time_utc",
    "instrument",
    "base_currency",
    "quote_currency",
    "mid_close",
    "bid_close",
    "ask_close",
    "pip_size",
}


def _atomic_joblib(payload: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        joblib.dump(payload, temporary, compress=3)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _pip_fallback_spread(instrument: str, cfg: dict[str, Any]) -> float:
    tier = tier_for(instrument, cfg)
    return float(cfg["costs"].get(f"fallback_spread_pips_{tier}", 8.0))


def _slippage(instrument: str, cfg: dict[str, Any]) -> float:
    tier = tier_for(instrument, cfg)
    return float(cfg["costs"].get(f"slippage_pips_{tier}", 1.0))


def _source_files(source_dir: Path, pairs: list[str] | None) -> list[tuple[str, Path]]:
    wanted = {value.strip().upper().replace("/", "_") for value in pairs or []}
    found: dict[str, Path] = {}
    for path in sorted(source_dir.glob("*_M1.parquet")):
        instrument = path.name.removesuffix("_M1.parquet").upper()
        if not wanted or instrument in wanted:
            found[instrument] = path
    if wanted:
        missing = sorted(wanted - set(found))
        if missing:
            raise FileNotFoundError(
                f"missing M1 parquet for {len(missing)} requested pairs: "
                + ", ".join(missing)
            )
    if not found:
        raise FileNotFoundError(f"no *_M1.parquet files found under {source_dir}")
    return sorted(found.items())


def _numeric(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def infer_native_pip_size(
    frame: pd.DataFrame, instrument: str
) -> float:
    fallback = float(pip_size_for(instrument))
    required = {"ask_close", "bid_close", "spread_pips"}
    if not required.issubset(frame.columns):
        return fallback
    native_width = (
        pd.to_numeric(frame["ask_close"], errors="coerce")
        - pd.to_numeric(frame["bid_close"], errors="coerce")
    )
    spread = pd.to_numeric(frame["spread_pips"], errors="coerce")
    implied = (native_width / spread).replace([np.inf, -np.inf], np.nan)
    implied = implied[(implied > 0) & implied.notna()]
    if len(implied) < 10:
        return fallback
    median = float(implied.median())
    if not math.isfinite(median) or median <= 0:
        return fallback
    inferred = float(10.0 ** round(math.log10(median)))
    if inferred not in {1.0, 0.1, 0.01, 0.001, 0.0001, 0.00001}:
        return fallback
    if not 0.5 <= median / inferred <= 2.0:
        return fallback
    return inferred


def load_m1(
    path: Path,
    instrument: str,
    cfg: dict[str, Any],
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    time_name = "datetime" if "datetime" in frame else "time"
    frame["bar_start_utc"] = pd.to_datetime(
        frame[time_name], errors="coerce", utc=True
    )
    for name in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "spread_pips",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
    ):
        frame[name] = _numeric(frame, name)
    frame = (
        frame.dropna(subset=["bar_start_utc", "open", "high", "low", "close"])
        .sort_values("bar_start_utc")
        .drop_duplicates("bar_start_utc", keep="last")
    )
    if start:
        start_ts = pd.Timestamp(start)
        start_ts = (
            start_ts.tz_localize("UTC")
            if start_ts.tzinfo is None
            else start_ts.tz_convert("UTC")
        )
        frame = frame[frame["bar_start_utc"] >= start_ts]
    if end:
        end_ts = pd.Timestamp(end)
        end_ts = (
            end_ts.tz_localize("UTC")
            if end_ts.tzinfo is None
            else end_ts.tz_convert("UTC")
        )
        if len(str(end)) <= 10:
            end_ts += pd.Timedelta(days=1)
        frame = frame[frame["bar_start_utc"] < end_ts]

    # An OANDA candle is timestamped at bar start. The decision is made only
    # after that M1 bar has completed.
    frame["decision_time_utc"] = frame["bar_start_utc"] + pd.Timedelta(minutes=1)
    completed_before = pd.Timestamp.now(tz="UTC").floor("min")
    frame = frame[frame["decision_time_utc"] <= completed_before].copy()

    pip = infer_native_pip_size(frame, instrument)
    fallback = _pip_fallback_spread(instrument, cfg)
    native_spread = (frame["ask_close"] - frame["bid_close"]) / pip
    frame["spread_pips"] = frame["spread_pips"].where(
        frame["spread_pips"].gt(0), native_spread
    )
    frame["spread_pips"] = frame["spread_pips"].where(
        frame["spread_pips"].gt(0), fallback
    )
    half = frame["spread_pips"] * pip / 2.0
    for field in ("open", "high", "low", "close"):
        frame[f"bid_{field}"] = frame[f"bid_{field}"].where(
            frame[f"bid_{field}"].notna(), frame[field] - half
        )
        frame[f"ask_{field}"] = frame[f"ask_{field}"].where(
            frame[f"ask_{field}"].notna(), frame[field] + half
        )
    frame["volume"] = frame["volume"].clip(lower=0)
    frame["instrument"] = instrument
    frame["pip_size"] = pip
    keep = [
        "bar_start_utc",
        "decision_time_utc",
        "instrument",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "spread_pips",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "pip_size",
    ]
    return frame[keep].reset_index(drop=True)


def resample_completed(m1: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    minutes = TIMEFRAME_MINUTES[timeframe]
    if minutes == 1:
        return m1.copy()
    source = m1.set_index("decision_time_utc")
    rules: dict[str, str] = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "spread_pips": "last",
        "bid_open": "first",
        "bid_high": "max",
        "bid_low": "min",
        "bid_close": "last",
        "ask_open": "first",
        "ask_high": "max",
        "ask_low": "min",
        "ask_close": "last",
    }
    if "pip_size" in source:
        rules["pip_size"] = "last"
    grouped = source.resample(
        f"{minutes}min", label="right", closed="right", origin="epoch"
    )
    output = grouped.agg(rules)
    output["source_bar_count"] = grouped["close"].count()
    output = output[output["source_bar_count"] == minutes].dropna(
        subset=["open", "high", "low", "close"]
    )
    output["decision_time_utc"] = output.index
    output["bar_start_utc"] = output.index - pd.Timedelta(minutes=minutes)
    output["instrument"] = str(m1["instrument"].iloc[0])
    return output.reset_index(drop=True)


def _rsi(close: pd.Series, window: int) -> pd.Series:
    change = close.diff()
    gain = change.clip(lower=0).rolling(window, min_periods=window).mean()
    loss = (-change.clip(upper=0)).rolling(window, min_periods=window).mean()
    ratio = gain / loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + ratio))


def _session_code(stamps: pd.Series) -> pd.Series:
    hour = stamps.dt.hour
    return pd.Series(
        np.select(
            [
                (hour >= 0) & (hour < 7),
                (hour >= 7) & (hour < 12),
                (hour >= 12) & (hour < 16),
                (hour >= 16) & (hour < 21),
            ],
            [0, 1, 2, 3],
            default=4,
        ),
        index=stamps.index,
        dtype=float,
    )


def _same_session_baseline(
    values: pd.Series, sessions: pd.Series, window: int = 120
) -> pd.Series:
    return values.groupby(sessions, sort=False).transform(
        lambda group: group.shift(1).rolling(window, min_periods=20).median()
    )


def timeframe_features(
    candles: pd.DataFrame,
    instrument: str,
    timeframe: str,
    lag_count: int,
) -> pd.DataFrame:
    if "pip_size" in candles:
        observed_pip = pd.to_numeric(
            candles["pip_size"], errors="coerce"
        ).dropna()
        pip = (
            float(observed_pip.median())
            if len(observed_pip)
            else float(pip_size_for(instrument))
        )
    else:
        pip = float(pip_size_for(instrument))
    prefix = f"{timeframe.lower()}__"
    stamps = candles["decision_time_utc"]
    close = candles["close"].astype(float)
    open_ = candles["open"].astype(float)
    high = candles["high"].astype(float)
    low = candles["low"].astype(float)
    ret = close.diff() / pip
    true_range = pd.concat(
        [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
        axis=1,
    ).max(axis=1) / pip
    sessions = _session_code(stamps)
    columns: dict[str, Any] = {"decision_time_utc": stamps}
    expected_gap = float(TIMEFRAME_MINUTES[timeframe])
    gap_minutes = stamps.diff().dt.total_seconds().div(60.0)
    gap_break = gap_minutes.ne(expected_gap)
    columns[f"{prefix}source_gap_minutes"] = gap_minutes
    columns[f"{prefix}weekend_or_data_gap"] = gap_minutes.gt(expected_gap).astype(
        float
    )
    columns[f"{prefix}bars_since_gap"] = gap_break.groupby(
        gap_break.cumsum()
    ).cumcount()

    for lag in range(lag_count):
        columns[f"{prefix}return_lag_{lag:02d}_pips"] = ret.shift(lag)
    for window in WINDOWS:
        columns[f"{prefix}return_{window}_pips"] = close.diff(window) / pip
        absolute_path = ret.abs().rolling(window, min_periods=window).sum()
        columns[f"{prefix}path_efficiency_{window}"] = (
            (close.diff(window) / pip).abs() / absolute_path.replace(0.0, np.nan)
        )
        columns[f"{prefix}up_fraction_{window}"] = (
            ret.gt(0).astype(float).rolling(window, min_periods=window).mean()
        )
        rolling_high = high.rolling(window, min_periods=window).max()
        rolling_low = low.rolling(window, min_periods=window).min()
        columns[f"{prefix}range_position_{window}"] = (
            (close - rolling_low) / (rolling_high - rolling_low).replace(0.0, np.nan)
        )

    for window in STAT_WINDOWS:
        rolling = ret.rolling(window, min_periods=window)
        mean = rolling.mean()
        std = rolling.std()
        p_up = ret.gt(0).astype(float).rolling(window, min_periods=window).mean()
        columns[f"{prefix}return_mean_{window}_pips"] = mean
        columns[f"{prefix}return_vol_{window}_pips"] = std
        columns[f"{prefix}return_z_{window}"] = (ret - mean) / std.replace(0.0, np.nan)
        columns[f"{prefix}return_skew_{window}"] = rolling.skew()
        columns[f"{prefix}return_kurt_{window}"] = rolling.kurt()
        columns[f"{prefix}sign_entropy_{window}"] = -(
            p_up.clip(1e-6, 1 - 1e-6) * np.log(p_up.clip(1e-6, 1 - 1e-6))
            + (1 - p_up).clip(1e-6, 1 - 1e-6)
            * np.log((1 - p_up).clip(1e-6, 1 - 1e-6))
        )
        lagged = ret.shift(1)
        ar1 = rolling.cov(lagged) / lagged.rolling(
            window, min_periods=window
        ).var().replace(0.0, np.nan)
        columns[f"{prefix}ar1_coefficient_{window}"] = ar1.clip(-2.0, 2.0)
        columns[f"{prefix}ar1_forecast_{window}_pips"] = ar1.clip(-2.0, 2.0) * ret
        columns[f"{prefix}realized_up_semivol_{window}"] = (
            ret.clip(lower=0).pow(2).rolling(window, min_periods=window).mean().pow(0.5)
        )
        columns[f"{prefix}realized_down_semivol_{window}"] = (
            (-ret.clip(upper=0)).pow(2).rolling(window, min_periods=window).mean().pow(0.5)
        )
        bipower = (
            ret.abs() * ret.shift(1).abs()
        ).rolling(window, min_periods=window).mean()
        realized_var = ret.pow(2).rolling(window, min_periods=window).mean()
        columns[f"{prefix}jump_variation_ratio_{window}"] = (
            (realized_var - (math.pi / 2.0) * bipower).clip(lower=0)
            / realized_var.replace(0.0, np.nan)
        )

    ar_window = 60
    ar_r1 = ret.rolling(ar_window, min_periods=ar_window).corr(ret.shift(1))
    ar_r2 = ret.rolling(ar_window, min_periods=ar_window).corr(ret.shift(2))
    ar_denominator = (1.0 - ar_r1.pow(2)).replace(0.0, np.nan)
    ar_phi1 = (ar_r1 * (1.0 - ar_r2) / ar_denominator).clip(-3.0, 3.0)
    ar_phi2 = ((ar_r2 - ar_r1.pow(2)) / ar_denominator).clip(-3.0, 3.0)
    ar2_forecast = ar_phi1 * ret + ar_phi2 * ret.shift(1)
    ar2_prior_forecast = (
        ar_phi1.shift(1) * ret.shift(1) + ar_phi2.shift(1) * ret.shift(2)
    )
    innovation = ret - ar2_prior_forecast
    innovation_std = innovation.shift(1).rolling(60, min_periods=30).std()
    seasonal_lag = {"M1": 60, "M30": 48, "H1": 24, "H4": 6}[timeframe]
    seasonal_window = max(60, seasonal_lag * 4)
    columns[f"{prefix}arima_proxy_ar2_phi1_60"] = ar_phi1
    columns[f"{prefix}arima_proxy_ar2_phi2_60"] = ar_phi2
    columns[f"{prefix}arima_proxy_forecast_pips"] = ar2_forecast
    columns[f"{prefix}arima_proxy_innovation_pips"] = innovation
    columns[f"{prefix}arima_proxy_innovation_z"] = (
        innovation / innovation_std.replace(0.0, np.nan)
    )
    columns[f"{prefix}arima_proxy_forecast_uncertainty_pips"] = innovation_std
    columns[f"{prefix}arima_proxy_forecast_to_uncertainty"] = (
        ar2_forecast / innovation_std.replace(0.0, np.nan)
    )
    columns[f"{prefix}arima_proxy_seasonal_autocorr"] = ret.rolling(
        seasonal_window, min_periods=max(30, seasonal_lag * 2)
    ).corr(ret.shift(seasonal_lag))

    bar_range = (high - low) / pip
    body = (close - open_) / pip
    columns[f"{prefix}bar_range_pips"] = bar_range
    columns[f"{prefix}bar_body_pips"] = body
    columns[f"{prefix}body_to_range"] = body / bar_range.replace(0.0, np.nan)
    columns[f"{prefix}upper_wick_fraction"] = (
        (high - pd.concat([open_, close], axis=1).max(axis=1))
        / (high - low).replace(0.0, np.nan)
    )
    columns[f"{prefix}lower_wick_fraction"] = (
        (pd.concat([open_, close], axis=1).min(axis=1) - low)
        / (high - low).replace(0.0, np.nan)
    )
    for window in (5, 14, 30, 60):
        atr = true_range.rolling(window, min_periods=window).mean()
        columns[f"{prefix}atr_{window}_pips"] = atr
        columns[f"{prefix}range_to_atr_{window}"] = bar_range / atr.replace(0.0, np.nan)
    for window in (7, 14, 30):
        columns[f"{prefix}rsi_{window}"] = _rsi(close, window)
    typical = (high + low + close) / 3.0
    for window in (20, 60):
        sma = close.rolling(window, min_periods=window).mean()
        std_price = close.rolling(window, min_periods=window).std()
        ema = close.ewm(span=window, adjust=False).mean()
        typical_mean = typical.rolling(window, min_periods=window).mean()
        mean_deviation = (typical - typical_mean).abs().rolling(
            window, min_periods=window
        ).mean()
        columns[f"{prefix}sma_gap_{window}_pips"] = (close - sma) / pip
        columns[f"{prefix}ema_gap_{window}_pips"] = (close - ema) / pip
        columns[f"{prefix}bollinger_z_{window}"] = (
            (close - sma) / std_price.replace(0.0, np.nan)
        )
        columns[f"{prefix}cci_{window}"] = (
            (typical - typical_mean) / (0.015 * mean_deviation.replace(0.0, np.nan))
        )
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    columns[f"{prefix}macd_pips"] = macd / pip
    columns[f"{prefix}macd_signal_gap_pips"] = (
        macd - macd.ewm(span=9, adjust=False).mean()
    ) / pip
    high14 = high.rolling(14, min_periods=14).max()
    low14 = low.rolling(14, min_periods=14).min()
    columns[f"{prefix}stochastic_14"] = (
        (close - low14) / (high14 - low14).replace(0.0, np.nan)
    )
    columns[f"{prefix}williams_r_14"] = (
        (high14 - close) / (high14 - low14).replace(0.0, np.nan)
    )

    fast_level = close.ewm(span=8, adjust=False).mean()
    slow_level = close.ewm(span=32, adjust=False).mean()
    residual = (close - slow_level) / pip
    residual_std = residual.rolling(60, min_periods=30).std()
    columns[f"{prefix}state_fast_level_gap_pips"] = (close - fast_level) / pip
    columns[f"{prefix}state_slow_level_gap_pips"] = residual
    columns[f"{prefix}state_trend_pips"] = (fast_level - slow_level) / pip
    columns[f"{prefix}state_residual_z"] = residual / residual_std.replace(0.0, np.nan)
    columns[f"{prefix}state_gain_proxy"] = (
        close.diff().abs().ewm(span=8, adjust=False).mean()
        / close.diff().abs().ewm(span=32, adjust=False).mean().replace(0.0, np.nan)
    )

    volume = candles["volume"].astype(float)
    log_volume = np.log1p(volume)
    historical_median = volume.shift(1).rolling(120, min_periods=20).median()
    session_median = _same_session_baseline(volume, sessions)
    volume_std = log_volume.shift(1).rolling(120, min_periods=20).std()
    columns[f"{prefix}tick_activity_log1p"] = log_volume
    columns[f"{prefix}tick_activity_ratio_history"] = (
        volume / historical_median.replace(0.0, np.nan)
    )
    columns[f"{prefix}tick_activity_ratio_session"] = (
        volume / session_median.replace(0.0, np.nan)
    )
    columns[f"{prefix}tick_activity_z_history"] = (
        log_volume - log_volume.shift(1).rolling(120, min_periods=20).mean()
    ) / volume_std.replace(0.0, np.nan)
    columns[f"{prefix}tick_activity_change"] = log_volume.diff()
    columns[f"{prefix}return_activity_corr_60"] = ret.rolling(
        60, min_periods=30
    ).corr(log_volume)

    spread = candles["spread_pips"].astype(float)
    spread_median = spread.shift(1).rolling(120, min_periods=20).median()
    spread_std = spread.shift(1).rolling(120, min_periods=20).std()
    columns[f"{prefix}historical_spread_pips"] = spread
    columns[f"{prefix}historical_spread_ratio"] = (
        spread / spread_median.replace(0.0, np.nan)
    )
    columns[f"{prefix}historical_spread_z"] = (
        spread - spread.shift(1).rolling(120, min_periods=20).mean()
    ) / spread_std.replace(0.0, np.nan)
    columns[f"{prefix}movement_to_spread_14"] = (
        true_range.rolling(14, min_periods=14).mean() / spread.replace(0.0, np.nan)
    )
    columns[f"{prefix}source_session"] = sessions
    columns[f"{prefix}source_sample_count"] = (
        pd.Series(np.arange(len(candles), dtype=float), index=candles.index) + 1.0
    )

    output = pd.DataFrame(columns)
    numeric = output.columns.difference(["decision_time_utc"])
    output[numeric] = output[numeric].replace([np.inf, -np.inf], np.nan).astype(
        "float32"
    )
    return output


def _future_exact(series: pd.Series, stamps: pd.Series, minutes: int) -> np.ndarray:
    indexed = pd.Series(
        series.to_numpy(dtype=float), index=pd.DatetimeIndex(stamps)
    )
    return indexed.reindex(
        pd.DatetimeIndex(stamps) + pd.Timedelta(minutes=minutes)
    ).to_numpy()


def _first_future_barrier_hits(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    pip: float,
    barriers: Iterable[int],
    max_horizon: int,
) -> dict[int, tuple[np.ndarray, np.ndarray]]:
    current = close.to_numpy(dtype=float)
    hits = {
        int(barrier): (
            np.zeros(len(close), dtype=np.int16),
            np.zeros(len(close), dtype=np.int16),
        )
        for barrier in barriers
    }
    for minute in range(1, max_horizon + 1):
        future_high = high.shift(-minute).to_numpy(dtype=float)
        future_low = low.shift(-minute).to_numpy(dtype=float)
        for barrier, (first_up, first_down) in hits.items():
            distance = float(barrier) * pip
            new_up = (first_up == 0) & (future_high >= current + distance)
            new_down = (first_down == 0) & (future_low <= current - distance)
            first_up[new_up] = minute
            first_down[new_down] = minute
    return hits


def _first_future_return_reversal(
    close: pd.Series, max_horizon: int
) -> tuple[np.ndarray, np.ndarray]:
    current_direction = np.sign(close.diff().to_numpy(dtype=float))
    first_reversal = np.zeros(len(close), dtype=np.int16)
    for minute in range(1, max_horizon + 1):
        next_close = close.shift(-minute).to_numpy(dtype=float)
        previous_close = close.shift(-(minute - 1)).to_numpy(dtype=float)
        future_return = next_close - previous_close
        reversed_direction = (
            (first_reversal == 0)
            & (current_direction != 0)
            & np.isfinite(future_return)
            & (current_direction * future_return < 0)
        )
        first_reversal[reversed_direction] = minute
    return current_direction, first_reversal


def add_targets(
    base: pd.DataFrame,
    m1: pd.DataFrame,
    instrument: str,
    cfg: dict[str, Any],
    horizons: Iterable[int],
) -> pd.DataFrame:
    horizons = tuple(int(value) for value in horizons)
    output = base.copy()
    observed_pip = (
        pd.to_numeric(m1["pip_size"], errors="coerce").dropna()
        if "pip_size" in m1
        else pd.Series(dtype=float)
    )
    pip = (
        float(observed_pip.median())
        if len(observed_pip)
        else float(pip_size_for(instrument))
    )
    stamps = m1["decision_time_utc"]
    close = m1["close"].astype(float)
    high = m1["high"].astype(float)
    low = m1["low"].astype(float)
    current_close = close.to_numpy(dtype=float)
    current_spread = (
        pd.to_numeric(m1["spread_pips"], errors="coerce")
        .where(lambda values: values > 0)
        .to_numpy(dtype=float)
    )
    slippage = _slippage(instrument, cfg)
    settings = cfg.get("unified_intrahour_forecast", {})
    barriers = tuple(
        int(value) for value in settings.get("barrier_pips", BARRIER_PIPS)
    )
    max_horizon = max(horizons)
    barrier_hits = _first_future_barrier_hits(
        high, low, close, pip, barriers, max_horizon
    )
    current_direction, first_reversal = _first_future_return_reversal(
        close, max_horizon
    )
    by_time = pd.DataFrame({"decision_time_utc": stamps})
    by_time["mid_close"] = close.to_numpy()
    by_time["bid_close"] = m1["bid_close"].to_numpy(dtype=float)
    by_time["ask_close"] = m1["ask_close"].to_numpy(dtype=float)
    entry_ask = _future_exact(m1["ask_open"], stamps, 1)
    entry_bid = _future_exact(m1["bid_open"], stamps, 1)

    for horizon in horizons:
        future_mid = _future_exact(close, stamps, horizon)
        future_bid = _future_exact(m1["bid_close"], stamps, horizon)
        future_ask = _future_exact(m1["ask_close"], stamps, horizon)
        indexer = FixedForwardWindowIndexer(window_size=int(horizon))
        future_high = (
            high.shift(-1)
            .rolling(window=indexer, min_periods=int(horizon))
            .max()
            .to_numpy()
        )
        future_low = (
            low.shift(-1)
            .rolling(window=indexer, min_periods=int(horizon))
            .min()
            .to_numpy()
        )
        continuous_path = (
            m1["decision_time_utc"].shift(-int(horizon)).to_numpy()
            == (
                m1["decision_time_utc"]
                + pd.Timedelta(minutes=int(horizon))
            ).to_numpy()
        )
        future_mid[~continuous_path] = np.nan
        future_bid[~continuous_path] = np.nan
        future_ask[~continuous_path] = np.nan
        entry_ask[~continuous_path] = np.nan
        entry_bid[~continuous_path] = np.nan
        future_high[~continuous_path] = np.nan
        future_low[~continuous_path] = np.nan
        endpoint = (future_mid - current_close) / pip
        up_mfe = np.maximum((future_high - current_close) / pip, 0.0)
        down_mfe = np.maximum((current_close - future_low) / pip, 0.0)
        best_side_mfe = np.maximum(up_mfe, down_mfe)
        by_time[f"target_return_pips_{horizon}"] = endpoint
        by_time[f"target_abs_move_pips_{horizon}"] = np.abs(endpoint)
        by_time[f"target_direction_{horizon}"] = np.sign(endpoint)
        by_time[f"target_mfe_pips_{horizon}"] = (future_high - current_close) / pip
        by_time[f"target_mae_pips_{horizon}"] = (future_low - current_close) / pip
        by_time[f"target_up_mfe_pips_{horizon}"] = up_mfe
        by_time[f"target_down_mfe_pips_{horizon}"] = down_mfe
        by_time[f"target_best_side_mfe_pips_{horizon}"] = best_side_mfe
        by_time[f"target_current_side_mfe_pips_{horizon}"] = np.where(
            current_direction > 0,
            up_mfe,
            np.where(current_direction < 0, down_mfe, np.nan),
        )
        by_time[f"target_path_range_pips_{horizon}"] = (
            future_high - future_low
        ) / pip
        by_time[f"target_abs_move_to_spread_{horizon}"] = (
            np.abs(endpoint) / current_spread
        )
        by_time[f"target_path_range_to_spread_{horizon}"] = (
            (future_high - future_low) / pip / current_spread
        )
        by_time[f"target_best_side_mfe_to_spread_{horizon}"] = (
            best_side_mfe / current_spread
        )
        reversal_in_horizon = (first_reversal > 0) & (
            first_reversal <= int(horizon)
        )
        direction_available = current_direction != 0
        valid_continuation = continuous_path & direction_available
        continuation = np.where(
            reversal_in_horizon, first_reversal - 1, int(horizon)
        ).astype(float)
        time_to_reversal = np.where(
            reversal_in_horizon, first_reversal, int(horizon) + 1
        ).astype(float)
        by_time[f"target_continuation_minutes_{horizon}"] = np.where(
            valid_continuation, continuation, np.nan
        )
        by_time[f"target_time_to_reversal_minutes_{horizon}"] = np.where(
            valid_continuation, time_to_reversal, np.nan
        )
        by_time[f"target_reversal_observed_{horizon}"] = np.where(
            valid_continuation, reversal_in_horizon.astype(float), np.nan
        )
        for barrier, (first_up, first_down) in barrier_hits.items():
            up_hit = np.where(
                (first_up > 0) & (first_up <= int(horizon)), first_up, 0
            )
            down_hit = np.where(
                (first_down > 0) & (first_down <= int(horizon)), first_down, 0
            )
            up_first = (up_hit > 0) & (
                (down_hit == 0) | (up_hit < down_hit)
            )
            down_first = (down_hit > 0) & (
                (up_hit == 0) | (down_hit < up_hit)
            )
            ambiguous = (up_hit > 0) & (up_hit == down_hit)
            reached = (up_hit > 0) | (down_hit > 0)
            first_event = np.where(
                reached,
                np.where(
                    up_hit == 0,
                    down_hit,
                    np.where(
                        down_hit == 0, up_hit, np.minimum(up_hit, down_hit)
                    ),
                ),
                np.nan,
            )
            suffix = f"{barrier}pips_{horizon}"
            by_time[f"target_up_before_down_{suffix}"] = np.where(
                continuous_path, up_first.astype(float), np.nan
            )
            by_time[f"target_down_before_up_{suffix}"] = np.where(
                continuous_path, down_first.astype(float), np.nan
            )
            by_time[f"target_barrier_reached_{suffix}"] = np.where(
                continuous_path, reached.astype(float), np.nan
            )
            by_time[f"target_barrier_ambiguous_{suffix}"] = np.where(
                continuous_path, ambiguous.astype(float), np.nan
            )
            by_time[f"target_first_barrier_minutes_{suffix}"] = np.where(
                continuous_path, first_event, np.nan
            )
        by_time[f"diag_long_net_pips_{horizon}"] = (
            future_bid - entry_ask
        ) / pip - 2.0 * slippage
        by_time[f"diag_short_net_pips_{horizon}"] = (
            entry_bid - future_ask
        ) / pip - 2.0 * slippage
    return output.merge(by_time, on="decision_time_utc", how="left")


def build_pair_history(
    source: Path,
    output: Path,
    instrument: str,
    cfg: dict[str, Any],
    start: str | None,
    end: str | None,
) -> dict[str, Any]:
    settings = cfg["unified_intrahour_forecast"]
    horizons = [int(value) for value in settings["horizons_minutes"]]
    lag_count = int(settings["return_lags_per_timeframe"])
    stride = int(settings.get("materialization_stride_minutes", 5))
    m1 = load_m1(source, instrument, cfg, start, end)
    if len(m1) < 20_000:
        raise ValueError(f"{instrument} has only {len(m1):,} completed M1 rows")
    base = timeframe_features(m1, instrument, "M1", lag_count)
    base = add_targets(base, m1, instrument, cfg, horizons)
    base["instrument"] = instrument
    base["pip_size"] = float(m1["pip_size"].iloc[0])
    base["base_currency"], base["quote_currency"] = instrument.split("_", 1)
    base["time__hour_sin"] = np.sin(
        2 * np.pi
        * (base["decision_time_utc"].dt.hour * 60 + base["decision_time_utc"].dt.minute)
        / 1440.0
    )
    base["time__hour_cos"] = np.cos(
        2 * np.pi
        * (base["decision_time_utc"].dt.hour * 60 + base["decision_time_utc"].dt.minute)
        / 1440.0
    )
    base["time__weekday_sin"] = np.sin(
        2 * np.pi * base["decision_time_utc"].dt.dayofweek / 7.0
    )
    base["time__weekday_cos"] = np.cos(
        2 * np.pi * base["decision_time_utc"].dt.dayofweek / 7.0
    )
    max_horizon = max(horizons)
    target_columns = [f"target_return_pips_{value}" for value in horizons]
    base = base.dropna(
        subset=[
            *target_columns,
            f"target_path_range_pips_{max_horizon}",
        ]
    )
    if stride > 1:
        minute_number = (
            base["decision_time_utc"].astype("int64") // 60_000_000_000
        )
        base = base[minute_number.mod(stride).eq(0)].copy()

    for timeframe in ("M30", "H1", "H4"):
        aggregated = resample_completed(m1, timeframe)
        features = timeframe_features(
            aggregated, instrument, timeframe, lag_count
        ).rename(
            columns={
                "decision_time_utc": f"{timeframe.lower()}__source_bar_end_utc"
            }
        )
        base = pd.merge_asof(
            base.sort_values("decision_time_utc"),
            features.sort_values(f"{timeframe.lower()}__source_bar_end_utc"),
            left_on="decision_time_utc",
            right_on=f"{timeframe.lower()}__source_bar_end_utc",
            direction="backward",
            allow_exact_matches=True,
        )
        base[f"{timeframe.lower()}__source_age_minutes"] = (
            base["decision_time_utc"]
            - base[f"{timeframe.lower()}__source_bar_end_utc"]
        ).dt.total_seconds() / 60.0

    numeric = [
        name
        for name in base.select_dtypes(include=[np.number]).columns
        if name not in {"mid_close", "bid_close", "ask_close"}
    ]
    base[numeric] = base[numeric].replace([np.inf, -np.inf], np.nan).astype(
        "float32"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    try:
        base.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    feature_names = model_numeric_features(base)
    return {
        "instrument": instrument,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "status": "completed",
        "source": str(source.resolve()),
        "source_sha256": _sha256(source),
        "output": str(output.resolve()),
        "output_sha256": _sha256(output),
        "source_rows": len(m1),
        "rows": len(base),
        "feature_count": len(feature_names),
        "pip_size": float(m1["pip_size"].iloc[0]),
        "start_utc": base["decision_time_utc"].min().isoformat(),
        "end_utc": base["decision_time_utc"].max().isoformat(),
        "max_horizon_minutes": max_horizon,
        "completed_bars_only": True,
    }


def model_numeric_features(frame: pd.DataFrame) -> list[str]:
    return [
        name
        for name in frame.select_dtypes(include=[np.number]).columns
        if name not in IDENTITY_COLUMNS
        and not name.startswith("target_")
        and not name.startswith("diag_")
    ]


def build_histories(
    cfg: dict[str, Any],
    source_dir: Path,
    data_root: Path,
    start: str | None,
    end: str | None,
    pairs: list[str] | None,
    force: bool,
) -> dict[str, Any]:
    history_root = data_root / "history"
    history_root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    for number, (instrument, source) in enumerate(
        _source_files(source_dir, pairs), start=1
    ):
        output = history_root / f"{instrument}_unified.parquet"
        reusable = output.is_file() and not force
        frame: pd.DataFrame | None = None
        if reusable:
            try:
                frame = pd.read_parquet(
                    output,
                    columns=[
                        "decision_time_utc",
                        "instrument",
                        "m1__arima_proxy_forecast_pips",
                        "target_up_mfe_pips_60",
                        "target_up_before_down_3pips_60",
                    ],
                )
            except (KeyError, OSError):
                reusable = False
        if reusable and frame is not None:
            if "pip_size" in pq.ParquetFile(output).schema_arrow.names:
                pip_value = float(
                    pd.read_parquet(output, columns=["pip_size"])[
                        "pip_size"
                    ].dropna().iloc[0]
                )
            else:
                pip_value = float(pip_size_for(instrument))
            records.append(
                {
                    "instrument": instrument,
                    "feature_schema_version": FEATURE_SCHEMA_VERSION,
                    "status": "reused",
                    "source": str(source.resolve()),
                    "output": str(output.resolve()),
                    "rows": len(frame),
                    "pip_size": pip_value,
                    "start_utc": pd.to_datetime(
                        frame["decision_time_utc"], utc=True
                    ).min().isoformat(),
                    "end_utc": pd.to_datetime(
                        frame["decision_time_utc"], utc=True
                    ).max().isoformat(),
                }
            )
            continue
        print(
            f"[unified-build] {number} {instrument} source={source.name}",
            flush=True,
        )
        try:
            records.append(
                build_pair_history(
                    source, output, instrument, cfg, start, end
                )
            )
        except Exception as exc:
            records.append(
                {
                    "instrument": instrument,
                    "status": "failed",
                    "source": str(source.resolve()),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        write_json(
            data_root / "build_manifest.json",
            {
                "status": "running",
                "updated_utc": utc_now_stamp(),
                "records": records,
            },
        )
    failures = [row for row in records if row["status"] == "failed"]
    manifest = {
        "schema_version": 1,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "status": "complete" if not failures else "complete_with_errors",
        "built_utc": utc_now_stamp(),
        "source_dir": str(source_dir.resolve()),
        "data_root": str(data_root.resolve()),
        "records": records,
        "summary": {
            "pairs": len(records),
            "completed_or_reused": sum(
                row["status"] in {"completed", "reused"} for row in records
            ),
            "failed": len(failures),
            "rows": sum(int(row.get("rows", 0)) for row in records),
        },
        "contract": {
            "target": (
                "future midpoint return path, barrier order, favorable "
                "excursion, continuation lifetime, and spread-normalized "
                "opportunity"
            ),
            "tradability": "separate next-bar executable bid_ask diagnostic",
            "timeframes": list(TIMEFRAME_MINUTES),
            "tick_volume": "normalized OANDA price-update activity",
            "order_book_or_depth_used": False,
            "account_state_used": False,
            "execution_enabled": False,
        },
    }
    write_json(data_root / "build_manifest.json", manifest)
    if failures:
        raise RuntimeError(f"{len(failures)} pair histories failed to build")
    return manifest


def _even_sample(frame: pd.DataFrame, limit: int) -> pd.DataFrame:
    if limit <= 0 or len(frame) <= limit:
        return frame
    positions = np.unique(np.linspace(0, len(frame) - 1, limit, dtype=int))
    return frame.iloc[positions]


def add_cross_sectional_features(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for horizon in (1, 5, 15, 60):
        source = f"m1__return_{horizon}_pips"
        if source not in output:
            source = f"m1__return_lag_00_pips"
        atr = output["m1__atr_14_pips"].clip(lower=0.1)
        raw_return = pd.to_numeric(output[source], errors="coerce")
        normalized = (raw_return / atr).clip(-10.0, 10.0)
        timestamps = output["decision_time_utc"]
        direction = np.sign(raw_return)
        available = direction.notna().astype(float)
        up = direction.gt(0).astype(float).where(direction.notna())
        down = direction.lt(0).astype(float).where(direction.notna())
        active_count = available.groupby(timestamps, observed=True).transform(
            "sum"
        )
        denominator = (active_count - available).replace(0.0, np.nan)
        up_ex_self = (
            up.groupby(timestamps, observed=True).transform("sum") - up
        ) / denominator
        down_ex_self = (
            down.groupby(timestamps, observed=True).transform("sum") - down
        ) / denominator
        volatility_active = normalized.abs().ge(1.0).astype(float).where(
            normalized.notna()
        )
        volatility_ex_self = (
            volatility_active.groupby(timestamps, observed=True).transform(
                "sum"
            )
            - volatility_active
        ) / denominator
        output[f"cross__available_pair_count_{horizon}"] = active_count
        output[f"cross__pair_up_breadth_ex_self_{horizon}"] = up_ex_self
        output[f"cross__pair_down_breadth_ex_self_{horizon}"] = down_ex_self
        output[f"cross__same_direction_breadth_ex_self_{horizon}"] = np.where(
            direction > 0,
            up_ex_self,
            np.where(direction < 0, down_ex_self, np.nan),
        )
        output[f"cross__volatility_breadth_ex_self_{horizon}"] = (
            volatility_ex_self
        )
        legs = pd.concat(
            [
                pd.DataFrame(
                    {
                        "decision_time_utc": output["decision_time_utc"].to_numpy(),
                        "currency": output["base_currency"].to_numpy(),
                        "value": normalized.to_numpy(),
                        "positive": (
                            normalized.gt(0)
                            .astype(float)
                            .where(normalized.notna())
                            .to_numpy()
                        ),
                    }
                ),
                pd.DataFrame(
                    {
                        "decision_time_utc": output["decision_time_utc"].to_numpy(),
                        "currency": output["quote_currency"].to_numpy(),
                        "value": -normalized.to_numpy(),
                        "positive": (
                            normalized.lt(0)
                            .astype(float)
                            .where(normalized.notna())
                            .to_numpy()
                        ),
                    }
                ),
            ],
            ignore_index=True,
        )
        strength = legs.groupby(
            ["decision_time_utc", "currency"], observed=True
        )["value"].mean()
        base_key = pd.MultiIndex.from_arrays(
            [output["decision_time_utc"], output["base_currency"]]
        )
        quote_key = pd.MultiIndex.from_arrays(
            [output["decision_time_utc"], output["quote_currency"]]
        )
        base_strength = strength.reindex(base_key).to_numpy(dtype=float)
        quote_strength = strength.reindex(quote_key).to_numpy(dtype=float)
        breadth_stats = legs.groupby(
            ["decision_time_utc", "currency"], observed=True
        )["positive"].agg(["sum", "count"])
        base_breadth_stats = breadth_stats.reindex(base_key)
        quote_breadth_stats = breadth_stats.reindex(quote_key)
        base_own_positive = normalized.gt(0).astype(float).to_numpy()
        quote_own_positive = normalized.lt(0).astype(float).to_numpy()
        base_count = base_breadth_stats["count"].to_numpy(dtype=float)
        quote_count = quote_breadth_stats["count"].to_numpy(dtype=float)
        base_breadth = np.full(len(output), np.nan, dtype=float)
        quote_breadth = np.full(len(output), np.nan, dtype=float)
        np.divide(
            base_breadth_stats["sum"].to_numpy(dtype=float)
            - base_own_positive,
            base_count - 1.0,
            out=base_breadth,
            where=base_count > 1.0,
        )
        np.divide(
            quote_breadth_stats["sum"].to_numpy(dtype=float)
            - quote_own_positive,
            quote_count - 1.0,
            out=quote_breadth,
            where=quote_count > 1.0,
        )
        pair_breadth = base_breadth - quote_breadth
        current_direction_agreement = np.where(
            direction > 0,
            (base_breadth + (1.0 - quote_breadth)) / 2.0,
            np.where(
                direction < 0,
                ((1.0 - base_breadth) + quote_breadth) / 2.0,
                np.nan,
            ),
        )
        output[f"cross__base_strength_{horizon}"] = base_strength
        output[f"cross__quote_strength_{horizon}"] = quote_strength
        output[f"cross__base_minus_quote_strength_{horizon}"] = (
            base_strength - quote_strength
        )
        output[f"cross__base_strengthening_breadth_ex_self_{horizon}"] = (
            base_breadth
        )
        output[f"cross__quote_strengthening_breadth_ex_self_{horizon}"] = (
            quote_breadth
        )
        output[f"cross__base_minus_quote_breadth_ex_self_{horizon}"] = (
            pair_breadth
        )
        output[f"cross__current_direction_currency_agreement_{horizon}"] = (
            current_direction_agreement
        )
        output[f"cross__pair_return_rank_{horizon}"] = normalized.groupby(
            output["decision_time_utc"], observed=True
        ).rank(pct=True)
        output[f"cross__dispersion_{horizon}"] = normalized.groupby(
            output["decision_time_utc"], observed=True
        ).transform("std")
    cross_columns = [name for name in output if name.startswith("cross__")]
    output[cross_columns] = output[cross_columns].astype("float32")
    return output


def live_matrix_from_snapshot(
    snapshot: dict[str, Any],
    numeric_features: list[str],
    pip_sizes: dict[str, float] | None = None,
) -> pd.DataFrame:
    generated = pd.Timestamp(snapshot.get("generated_utc"))
    generated = (
        generated.tz_localize("UTC")
        if generated.tzinfo is None
        else generated.tz_convert("UTC")
    )
    rows: list[dict[str, Any]] = []
    for instrument, raw in sorted((snapshot.get("instruments") or {}).items()):
        structural = raw.get("structural_series") or {}
        row: dict[str, Any] = {
            "decision_time_utc": generated,
            "instrument": instrument,
        }
        try:
            row["base_currency"], row["quote_currency"] = instrument.split("_", 1)
        except ValueError:
            continue
        complete = True
        for timeframe, minutes in TIMEFRAME_MINUTES.items():
            values = structural.get(timeframe) or {}
            arrays = {
                "open": list(values.get("open") or []),
                "high": list(values.get("high") or []),
                "low": list(values.get("low") or []),
                "close": list(values.get("close") or []),
                "volume": list(values.get("tick_activity") or []),
            }
            length = min((len(value) for value in arrays.values()), default=0)
            if length < 60:
                complete = False
                break
            arrays = {name: value[-length:] for name, value in arrays.items()}
            spread = list(values.get("historical_spread_pips") or [])
            if len(spread) < length:
                spread = [np.nan] * (length - len(spread)) + spread
            else:
                spread = spread[-length:]
            raw_times = pd.to_datetime(
                list(values.get("bar_start_times_utc") or [])[-length:],
                errors="coerce",
                utc=True,
            )
            if len(raw_times) == length and not pd.isna(raw_times).any():
                stamps = raw_times + pd.Timedelta(minutes=minutes)
                source_end = stamps[-1]
            else:
                raw_start = pd.to_datetime(
                    values.get("bar_start_utc"), errors="coerce", utc=True
                )
                source_end = (
                    raw_start + pd.Timedelta(minutes=minutes)
                    if not pd.isna(raw_start)
                    else generated.floor(f"{minutes}min")
                )
                stamps = pd.date_range(
                    end=source_end,
                    periods=length,
                    freq=f"{minutes}min",
                )
            candles = pd.DataFrame(
                {
                    "decision_time_utc": stamps,
                    **arrays,
                    "spread_pips": spread,
                    "pip_size": float(
                        (pip_sizes or {}).get(
                            instrument, pip_size_for(instrument)
                        )
                    ),
                }
            )
            feature_row = timeframe_features(
                candles, instrument, timeframe, 60
            ).iloc[-1]
            row.update(
                {
                    name: value
                    for name, value in feature_row.items()
                    if name != "decision_time_utc"
                }
            )
            row[f"{timeframe.lower()}__source_age_minutes"] = max(
                0.0, (generated - source_end).total_seconds() / 60.0
            )
        if not complete:
            continue
        minute_of_day = generated.hour * 60 + generated.minute
        row["time__hour_sin"] = math.sin(2 * math.pi * minute_of_day / 1440)
        row["time__hour_cos"] = math.cos(2 * math.pi * minute_of_day / 1440)
        row["time__weekday_sin"] = math.sin(
            2 * math.pi * generated.dayofweek / 7
        )
        row["time__weekday_cos"] = math.cos(
            2 * math.pi * generated.dayofweek / 7
        )
        rows.append(row)
    if not rows:
        raise ValueError(
            "snapshot has no pair with 240 completed OHLCV bars on every structural timeframe"
        )
    matrix = add_cross_sectional_features(pd.DataFrame(rows))
    for name in numeric_features:
        if name not in matrix:
            matrix[name] = np.nan
    return matrix


def assemble_matrix(
    data_root: Path, max_rows: int, force: bool
) -> tuple[Path, dict[str, Any]]:
    output_path = data_root / "unified_training_matrix.parquet"
    manifest_path = data_root / "matrix_manifest.json"
    if output_path.is_file() and manifest_path.is_file() and not force:
        existing_manifest = _json_file(manifest_path)
        if (
            existing_manifest.get("feature_schema_version")
            == FEATURE_SCHEMA_VERSION
        ):
            return output_path, existing_manifest
    paths = sorted((data_root / "history").glob("*_unified.parquet"))
    if not paths:
        raise FileNotFoundError(f"no pair histories under {data_root / 'history'}")
    source_rows = sum(
        int(pq.ParquetFile(path).metadata.num_rows) for path in paths
    )
    row_stride = max(1, math.ceil(source_rows / max_rows))
    timestamp_cadence_minutes = 5 * row_stride
    parts: list[pd.DataFrame] = []
    for number, path in enumerate(paths, start=1):
        print(
            f"[unified-matrix] {number}/{len(paths)} {path.stem}",
            flush=True,
        )
        frame = pd.read_parquet(path)
        frame["decision_time_utc"] = pd.to_datetime(
            frame["decision_time_utc"], errors="coerce", utc=True
        )
        if "pip_size" not in frame:
            instrument = path.name.removesuffix("_unified.parquet")
            frame["pip_size"] = float(pip_size_for(instrument))
        continuity_target = "target_path_range_pips_60"
        if continuity_target not in frame:
            raise KeyError(
                f"{path.name} is missing required continuity target "
                f"{continuity_target}"
            )
        frame = frame.dropna(subset=[continuity_target])
        minute_number = (
            frame["decision_time_utc"].astype("int64") // 60_000_000_000
        )
        parts.append(
            frame[minute_number.mod(timestamp_cadence_minutes).eq(0)]
        )
    matrix = pd.concat(parts, ignore_index=True)
    parts.clear()
    gc.collect()
    matrix = matrix.sort_values(["decision_time_utc", "instrument"])
    if len(matrix) > max_rows:
        unique_times = np.sort(matrix["decision_time_utc"].unique())
        max_timestamps = max(1, max_rows // len(paths))
        selected_positions = np.unique(
            np.linspace(
                0,
                len(unique_times) - 1,
                min(len(unique_times), max_timestamps),
                dtype=int,
            )
        )
        selected_times = set(unique_times[selected_positions])
        matrix = matrix[matrix["decision_time_utc"].isin(selected_times)]
    matrix = add_cross_sectional_features(matrix)
    matrix = matrix.sort_values(
        ["decision_time_utc", "instrument"]
    ).reset_index(drop=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + f".{os.getpid()}.tmp")
    try:
        matrix.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    manifest = {
        "schema_version": 1,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "built_utc": utc_now_stamp(),
        "path": str(output_path.resolve()),
        "sha256": _sha256(output_path),
        "rows": len(matrix),
        "pairs": int(matrix["instrument"].nunique()),
        "start_utc": matrix["decision_time_utc"].min().isoformat(),
        "end_utc": matrix["decision_time_utc"].max().isoformat(),
        "numeric_feature_count": len(model_numeric_features(matrix)),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "source_histories": [str(path.resolve()) for path in paths],
        "sampling": "deterministic chronological coverage preserving sample",
        "sampling_shared_timestamp_grid": True,
        "source_rows_before_sampling": source_rows,
        "timestamp_cadence_minutes": timestamp_cadence_minutes,
        "cross_sectional_features_materialized": True,
    }
    registry_rows: list[dict[str, Any]] = []
    for name in model_numeric_features(matrix):
        prefix = name.split("__", 1)[0].upper() if "__" in name else "GLOBAL"
        if prefix in TIMEFRAME_MINUTES:
            family = "timeframe_market_state"
            availability = "last_completed_source_bar"
        elif prefix == "CROSS":
            family = "cross_sectional_currency_state"
            availability = "same_decision_timestamp_completed_bars"
        elif prefix == "TIME":
            family = "calendar_state"
            availability = "decision_timestamp"
        else:
            family = "structural_market_state"
            availability = "decision_timestamp_or_earlier"
        registry_rows.append(
            {
                "feature_id": hashlib.sha256(name.encode("utf-8")).hexdigest()[:16],
                "feature_name": name,
                "feature_family": family,
                "source_timeframe": prefix,
                "causal": True,
                "availability": availability,
                "completed_bars_only": True,
                "historically_materialized": True,
                "model_input": True,
                "forecast_horizons_minutes": ",".join(
                    str(value)
                    for value in (1, 2, 3, 5, 10, 15, 30, 60)
                ),
                "individual_trade_signal": False,
                "account_eligible": False,
                "deployment_status": "shadow_only_pending_untouched_final",
            }
        )
    registry_path = data_root / "unified_feature_registry.csv"
    pd.DataFrame(registry_rows).to_csv(registry_path, index=False)
    output_registry_rows: list[dict[str, Any]] = []
    for name in [
        value
        for value in matrix.columns
        if value.startswith("target_") or value.startswith("diag_")
    ]:
        if name.startswith("diag_"):
            family = "executable_tradability_diagnostic"
            role = "diagnostic_only"
        elif "before_" in name or "barrier_" in name:
            family = "barrier_first_probability_label"
            role = "structural_forecast_target"
        elif "mfe" in name:
            family = "favorable_excursion_label"
            role = "structural_forecast_target"
        elif "continuation" in name or "reversal" in name:
            family = "continuation_lifetime_label"
            role = "structural_forecast_target"
        elif "to_spread" in name:
            family = "spread_normalized_opportunity_label"
            role = "structural_forecast_target"
        else:
            family = "midpoint_path_label"
            role = "structural_forecast_target"
        output_registry_rows.append(
            {
                "output_id": hashlib.sha256(name.encode("utf-8")).hexdigest()[:16],
                "output_name": name,
                "output_family": family,
                "role": role,
                "future_label": True,
                "model_input": False,
                "completed_future_m1_path_required": True,
                "same_bar_barrier_policy": (
                    "explicit_ambiguous_neither_direction_wins"
                    if family == "barrier_first_probability_label"
                    else ""
                ),
                "account_eligible": False,
                "deployment_status": "research_shadow_only",
            }
        )
    output_registry_path = data_root / "unified_forecast_output_registry.csv"
    pd.DataFrame(output_registry_rows).to_csv(output_registry_path, index=False)
    manifest["feature_registry"] = str(registry_path.resolve())
    manifest["feature_registry_rows"] = len(registry_rows)
    manifest["forecast_output_registry"] = str(output_registry_path.resolve())
    manifest["forecast_output_registry_rows"] = len(output_registry_rows)
    write_json(manifest_path, manifest)
    return output_path, manifest


def chronological_split(
    frame: pd.DataFrame,
    validation_fraction: float,
    final_fraction: float,
    purge_minutes: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    times = np.sort(frame["decision_time_utc"].dropna().unique())
    if len(times) < 100:
        raise ValueError("insufficient unique timestamps for chronological split")
    final_position = int(len(times) * (1.0 - final_fraction))
    validation_position = int(
        len(times) * (1.0 - final_fraction - validation_fraction)
    )
    validation_start = pd.Timestamp(times[validation_position])
    final_start = pd.Timestamp(times[final_position])
    purge = pd.Timedelta(minutes=purge_minutes)
    train = frame[
        frame["decision_time_utc"] < validation_start - purge
    ].copy()
    validation = frame[
        (frame["decision_time_utc"] >= validation_start)
        & (frame["decision_time_utc"] < final_start - purge)
    ].copy()
    final = frame[frame["decision_time_utc"] >= final_start].copy()
    split = {
        "validation_start_utc": validation_start.isoformat(),
        "final_start_utc": final_start.isoformat(),
        "purge_minutes": purge_minutes,
        "train_rows": len(train),
        "validation_rows": len(validation),
        "final_rows": len(final),
        "train_target_latest_utc": (
            train["decision_time_utc"].max() + purge
        ).isoformat(),
        "validation_target_latest_utc": (
            validation["decision_time_utc"].max() + purge
        ).isoformat(),
        "train_purge_passed": bool(
            train["decision_time_utc"].max() + purge < validation_start
        ),
        "validation_purge_passed": bool(
            validation["decision_time_utc"].max() + purge < final_start
        ),
    }
    if not split["train_purge_passed"] or not split["validation_purge_passed"]:
        raise AssertionError("purged chronological split invariant failed")
    return train, validation, final, split


def _feature_family(all_numeric: list[str], family: str) -> list[str]:
    shared = [
        name
        for name in all_numeric
        if name.startswith("time__") or name.startswith("cross__")
    ]
    if family == "m1":
        return sorted(
            set(shared)
            | {name for name in all_numeric if name.startswith("m1__")}
        )
    if family == "mtf_no_activity":
        return [
            name
            for name in all_numeric
            if "tick_activity" not in name
            and "return_activity_corr" not in name
            and "historical_spread" not in name
            and "movement_to_spread" not in name
        ]
    return all_numeric


def _preprocessor(numeric: list[str], scale: bool) -> ColumnTransformer:
    numeric_steps: list[tuple[str, Any]] = [
        ("imputer", SimpleImputer(strategy="median", copy=False))
    ]
    if scale:
        numeric_steps.append(("scale", StandardScaler(copy=False)))
    return ColumnTransformer(
        [
            ("numeric", Pipeline(numeric_steps), numeric),
            (
                "categorical",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "one_hot",
                            OneHotEncoder(
                                handle_unknown="ignore",
                                sparse_output=False,
                                dtype=np.float32,
                            ),
                        ),
                    ]
                ),
                list(CATEGORICAL_FEATURES),
            ),
        ],
        sparse_threshold=0.0,
    )


def opportunity_target_groups(
    settings: dict[str, Any], columns: Iterable[str]
) -> dict[str, list[str]]:
    available = set(columns)
    horizons = [
        int(value)
        for value in settings.get(
            "opportunity_horizons_minutes", OPPORTUNITY_HORIZONS
        )
    ]
    barriers = [
        int(value) for value in settings.get("barrier_pips", BARRIER_PIPS)
    ]
    regression: list[str] = []
    probability: list[str] = []
    for horizon in horizons:
        regression.extend(
            [
                f"target_up_mfe_pips_{horizon}",
                f"target_down_mfe_pips_{horizon}",
                f"target_continuation_minutes_{horizon}",
                f"target_time_to_reversal_minutes_{horizon}",
                f"target_abs_move_to_spread_{horizon}",
                f"target_best_side_mfe_to_spread_{horizon}",
            ]
        )
        for barrier in barriers:
            probability.extend(
                [
                    f"target_up_before_down_{barrier}pips_{horizon}",
                    f"target_down_before_up_{barrier}pips_{horizon}",
                ]
            )
    return {
        "regression": [name for name in regression if name in available],
        "probability": [name for name in probability if name in available],
    }


def _opportunity_head(
    numeric: list[str], alpha: float
) -> Pipeline:
    return Pipeline(
        [
            ("prepare", _preprocessor(numeric, scale=True)),
            ("model", Ridge(alpha=float(alpha))),
        ]
    )


def _opportunity_metrics(
    actual: np.ndarray,
    predicted: np.ndarray,
    targets: list[str],
    baseline: np.ndarray,
    probability: bool,
) -> dict[str, Any]:
    prediction = np.asarray(predicted, dtype=float)
    observed = np.asarray(actual, dtype=float)
    reference = np.broadcast_to(
        np.asarray(baseline, dtype=float), observed.shape
    )
    if probability:
        prediction = np.clip(prediction, 0.0, 1.0)
        reference = np.clip(reference, 0.0, 1.0)
        errors = np.mean((observed - prediction) ** 2, axis=0)
        baseline_errors = np.mean((observed - reference) ** 2, axis=0)
        metric_name = "brier_score"
    else:
        prediction = np.maximum(prediction, 0.0)
        errors = np.mean(np.abs(observed - prediction), axis=0)
        baseline_errors = np.mean(np.abs(observed - reference), axis=0)
        metric_name = "mae"
    relative = errors / np.maximum(baseline_errors, 1e-9)
    return {
        "rows": int(len(observed)),
        "targets": len(targets),
        f"mean_{metric_name}": float(np.mean(errors)),
        f"mean_{metric_name}_relative_to_historical_baseline": float(
            np.mean(relative)
        ),
        "per_target": [
            {
                "target": target,
                metric_name: float(errors[position]),
                f"{metric_name}_relative_to_historical_baseline": float(
                    relative[position]
                ),
            }
            for position, target in enumerate(targets)
        ],
    }


def select_opportunity_heads(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    numeric: list[str],
    settings: dict[str, Any],
    max_train_rows: int,
) -> dict[str, Any]:
    groups = opportunity_target_groups(settings, train.columns)
    alphas = [
        float(value)
        for value in settings.get(
            "opportunity_ridge_alphas", (10.0, 100.0, 1000.0)
        )
    ]
    selection: dict[str, Any] = {
        "scope": "validation_only",
        "model_family": "multioutput_ridge",
        "heads": {},
    }
    input_columns = [*numeric, *CATEGORICAL_FEATURES]
    cap = min(max_train_rows, 50_000)
    for kind, targets in groups.items():
        if not targets:
            continue
        probability = kind == "probability"
        head_train = _even_sample(
            train.dropna(subset=targets), cap
        ).reset_index(drop=True)
        head_validation = _even_sample(
            validation.dropna(subset=targets), max(20_000, cap // 2)
        ).reset_index(drop=True)
        if len(head_train) < 1_000 or len(head_validation) < 500:
            selection["heads"][kind] = {
                "status": "skipped",
                "reason": "insufficient_complete_rows",
                "targets": targets,
            }
            continue
        train_actual = head_train[targets].to_numpy(dtype=float)
        validation_actual = head_validation[targets].to_numpy(dtype=float)
        baseline = (
            np.mean(train_actual, axis=0)
            if probability
            else np.median(train_actual, axis=0)
        )
        candidates: list[dict[str, Any]] = []
        for alpha in alphas:
            model = _opportunity_head(numeric, alpha)
            model.fit(head_train[input_columns], train_actual)
            predicted = model.predict(head_validation[input_columns])
            metrics = _opportunity_metrics(
                validation_actual,
                predicted,
                targets,
                baseline,
                probability,
            )
            candidates.append(
                {
                    "alpha": alpha,
                    "validation": metrics,
                }
            )
            del model
            gc.collect()
        relative_key = (
            "mean_brier_score_relative_to_historical_baseline"
            if probability
            else "mean_mae_relative_to_historical_baseline"
        )
        winner = min(
            candidates, key=lambda row: row["validation"][relative_key]
        )
        selection["heads"][kind] = {
            "status": "selected",
            "targets": targets,
            "train_rows": len(head_train),
            "validation_rows": len(head_validation),
            "historical_baseline": baseline.tolist(),
            "candidates": candidates,
            "winner": winner,
        }
    return selection


def fit_opportunity_heads(
    development: pd.DataFrame,
    numeric: list[str],
    selection: dict[str, Any],
    max_train_rows: int,
) -> dict[str, dict[str, Any]]:
    fitted: dict[str, dict[str, Any]] = {}
    input_columns = [*numeric, *CATEGORICAL_FEATURES]
    for kind, state in selection.get("heads", {}).items():
        if state.get("status") != "selected":
            continue
        targets = list(state["targets"])
        sample = _even_sample(
            development.dropna(subset=targets),
            min(max_train_rows, 50_000),
        ).reset_index(drop=True)
        model = _opportunity_head(
            numeric, float(state["winner"]["alpha"])
        )
        model.fit(
            sample[input_columns],
            sample[targets].to_numpy(dtype=float),
        )
        baseline = (
            sample[targets].mean().to_numpy(dtype=float)
            if kind == "probability"
            else sample[targets].median().to_numpy(dtype=float)
        )
        fitted[kind] = {
            "model": model,
            "targets": targets,
            "historical_baseline": baseline,
            "train_rows": len(sample),
        }
    return fitted


def evaluate_opportunity_heads(
    heads: dict[str, dict[str, Any]],
    frame: pd.DataFrame,
    numeric: list[str],
    max_rows: int,
) -> dict[str, Any]:
    report: dict[str, Any] = {}
    input_columns = [*numeric, *CATEGORICAL_FEATURES]
    for kind, state in heads.items():
        targets = list(state["targets"])
        sample = _even_sample(
            frame.dropna(subset=targets), max_rows
        ).reset_index(drop=True)
        prediction = state["model"].predict(sample[input_columns])
        report[kind] = _opportunity_metrics(
            sample[targets].to_numpy(dtype=float),
            prediction,
            targets,
            np.asarray(state["historical_baseline"], dtype=float),
            kind == "probability",
        )
    return report


def _candidate(name: str, numeric: list[str], seed: int) -> tuple[Pipeline, int]:
    if name.startswith("ridge"):
        model: Any = Ridge(
            alpha=10.0,
            solver="lsqr",
            max_iter=500,
            tol=1e-4,
        )
        scale = True
        cap = 100_000
    elif name.startswith("extra_trees"):
        model = ExtraTreesRegressor(
            n_estimators=60,
            max_depth=14,
            min_samples_leaf=40,
            max_features=0.65,
            n_jobs=2,
            random_state=seed,
        )
        scale = False
        cap = 80_000
    elif name.startswith("hist_gradient_boosting"):
        model = MultiOutputRegressor(
            HistGradientBoostingRegressor(
                learning_rate=0.05,
                max_iter=70,
                max_leaf_nodes=31,
                min_samples_leaf=50,
                l2_regularization=2.0,
                random_state=seed,
            ),
            n_jobs=1,
        )
        scale = False
        cap = 40_000
    else:
        raise ValueError(f"unsupported model candidate: {name}")
    return Pipeline([("prepare", _preprocessor(numeric, scale)), ("model", model)]), cap


def _candidate_family(name: str) -> str:
    if name.endswith("_m1"):
        return "m1"
    if name.endswith("_mtf_no_activity"):
        return "mtf_no_activity"
    return "mtf_full"


def _arrays(
    frame: pd.DataFrame, target_columns: list[str]
) -> tuple[pd.DataFrame, np.ndarray]:
    return (
        frame,
        frame[target_columns].to_numpy(dtype=float),
    )


def _safe_correlation(
    left: np.ndarray, right: np.ndarray, *, rank: bool
) -> float:
    if (
        len(left) <= 2
        or float(np.ptp(left)) <= 1e-12
        or float(np.ptp(right)) <= 1e-12
    ):
        return 0.0
    value = (
        float(spearmanr(left, right).statistic)
        if rank
        else float(np.corrcoef(left, right)[0, 1])
    )
    return value if math.isfinite(value) else 0.0


def forecast_metrics(
    actual: np.ndarray,
    predicted: np.ndarray,
    horizons: list[int],
    no_change_mae: list[float] | None = None,
) -> dict[str, Any]:
    per_horizon: list[dict[str, Any]] = []
    relative: list[float] = []
    direction_values: list[float] = []
    movement_relative: list[float] = []
    for column, horizon in enumerate(horizons):
        y = np.asarray(actual[:, column], dtype=float)
        p = np.asarray(predicted[:, column], dtype=float)
        valid = np.isfinite(y) & np.isfinite(p)
        y, p = y[valid], p[valid]
        if not len(y):
            continue
        mae = float(mean_absolute_error(y, p))
        baseline_mae = (
            float(no_change_mae[column])
            if no_change_mae is not None
            else float(np.mean(np.abs(y)))
        )
        ratio = mae / max(1e-12, baseline_mae)
        direction = float(np.mean(np.sign(y) == np.sign(p)))
        magnitude_mae = float(np.mean(np.abs(np.abs(y) - np.abs(p))))
        magnitude_baseline = float(np.mean(np.abs(y)))
        rank_ic = _safe_correlation(y, p, rank=True)
        correlation = _safe_correlation(y, p, rank=False)
        per_horizon.append(
            {
                "horizon_minutes": horizon,
                "rows": len(y),
                "mae_pips": mae,
                "rmse_pips": float(math.sqrt(mean_squared_error(y, p))),
                "no_change_mae_pips": baseline_mae,
                "mae_relative_to_no_change": ratio,
                "direction_accuracy": direction,
                "magnitude_mae_pips": magnitude_mae,
                "magnitude_mae_relative_to_zero": magnitude_mae
                / max(1e-12, magnitude_baseline),
                "pearson_correlation": correlation,
                "rank_ic": rank_ic,
                "actual_mean_abs_move_pips": float(np.mean(np.abs(y))),
                "predicted_mean_abs_move_pips": float(np.mean(np.abs(p))),
            }
        )
        relative.append(ratio)
        direction_values.append(direction)
        movement_relative.append(magnitude_mae / max(1e-12, magnitude_baseline))
    increments_actual = np.diff(
        np.column_stack([np.zeros(len(actual)), actual]), axis=1
    )
    increments_predicted = np.diff(
        np.column_stack([np.zeros(len(predicted)), predicted]), axis=1
    )
    valid_increment = np.isfinite(increments_actual) & np.isfinite(
        increments_predicted
    )
    curve_increment_mae = float(
        np.mean(
            np.abs(
                increments_actual[valid_increment]
                - increments_predicted[valid_increment]
            )
        )
    )
    return {
        "per_horizon": per_horizon,
        "mean_horizon_mae_relative_to_no_change": float(np.mean(relative)),
        "mean_direction_accuracy": float(np.mean(direction_values)),
        "mean_magnitude_mae_relative_to_zero": float(np.mean(movement_relative)),
        "curve_increment_mae_pips": curve_increment_mae,
    }


def _drift_prediction(frame: pd.DataFrame, horizons: list[int]) -> np.ndarray:
    drift = frame["m1__return_mean_15_pips"].fillna(0.0).to_numpy(dtype=float)
    return np.column_stack([drift * horizon for horizon in horizons])


def _ar2_prediction(frame: pd.DataFrame, horizons: list[int]) -> np.ndarray:
    phi1 = (
        frame["m1__arima_proxy_ar2_phi1_60"]
        .fillna(0.0)
        .to_numpy(dtype=float)
    )
    phi2 = (
        frame["m1__arima_proxy_ar2_phi2_60"]
        .fillna(0.0)
        .to_numpy(dtype=float)
    )
    previous_one = (
        frame["m1__return_lag_00_pips"].fillna(0.0).to_numpy(dtype=float)
    )
    previous_two = (
        frame["m1__return_lag_01_pips"].fillna(0.0).to_numpy(dtype=float)
    )
    cumulative = np.zeros(len(frame), dtype=float)
    outputs: dict[int, np.ndarray] = {}
    for step in range(1, max(horizons) + 1):
        forecast = phi1 * previous_one + phi2 * previous_two
        forecast = np.clip(forecast, -50.0, 50.0)
        cumulative = cumulative + forecast
        if step in horizons:
            outputs[step] = cumulative.copy()
        previous_two, previous_one = previous_one, forecast
    return np.column_stack([outputs[horizon] for horizon in horizons])


def _tradability_metrics(
    frame: pd.DataFrame,
    prediction: np.ndarray,
    horizons: list[int],
    thresholds: dict[int, float] | None = None,
    select_thresholds: bool = False,
) -> tuple[dict[str, Any], dict[int, float]]:
    rows: list[dict[str, Any]] = []
    chosen: dict[int, float] = {}
    for column, horizon in enumerate(horizons):
        predicted = prediction[:, column]
        finite = np.isfinite(predicted)
        quantiles = (
            [0.0, 0.5, 0.7, 0.8, 0.9, 0.95]
            if select_thresholds
            else []
        )
        candidates = (
            sorted(
                {
                    float(np.quantile(np.abs(predicted[finite]), value))
                    for value in quantiles
                }
            )
            if finite.any()
            else [float("inf")]
        )
        if not select_thresholds:
            candidates = [float((thresholds or {}).get(horizon, float("inf")))]
        attempts: list[tuple[float, dict[str, Any]]] = []
        for threshold in candidates:
            eligible = finite & (np.abs(predicted) >= threshold)
            selected = frame.loc[eligible].copy()
            selected["predicted_abs"] = np.abs(predicted[eligible])
            selected["predicted_direction"] = np.sign(predicted[eligible])
            # Forecasting is evaluated row-wise. This diagnostic separately
            # approximates a constant-rotation top-one selection per timestamp.
            selected = selected.sort_values(
                ["decision_time_utc", "predicted_abs"], ascending=[True, False]
            ).drop_duplicates("decision_time_utc", keep="first")
            net = np.where(
                selected["predicted_direction"].to_numpy() > 0,
                selected[f"diag_long_net_pips_{horizon}"].to_numpy(dtype=float),
                selected[f"diag_short_net_pips_{horizon}"].to_numpy(dtype=float),
            )
            net = net[np.isfinite(net)]
            gains = float(net[net > 0].sum())
            losses = float(-net[net < 0].sum())
            curve = np.cumsum(net)
            drawdown = (
                float(
                    np.max(
                        np.maximum.accumulate(np.r_[0.0, curve])[1:] - curve
                    )
                )
                if len(net)
                else 0.0
            )
            result = {
                "horizon_minutes": horizon,
                "threshold_pips": threshold,
                "trades": len(net),
                "mean_net_pips": float(np.mean(net)) if len(net) else 0.0,
                "total_net_pips": float(np.sum(net)) if len(net) else 0.0,
                "win_rate": float(np.mean(net > 0)) if len(net) else 0.0,
                "profit_factor": (
                    gains / losses
                    if losses > 0
                    else (float("inf") if gains > 0 else 0.0)
                ),
                "max_drawdown_pips": drawdown,
            }
            attempts.append((threshold, result))
        eligible_attempts = [
            value for value in attempts if value[1]["trades"] >= 50
        ]
        pool = eligible_attempts or attempts
        threshold, best = max(
            pool,
            key=lambda value: (
                value[1]["mean_net_pips"],
                value[1]["profit_factor"],
                value[1]["trades"],
            ),
        )
        chosen[horizon] = threshold
        rows.append(best)
    return {
        "selection_scope": "top_one_pair_by_timestamp_per_horizon",
        "training_target_used": False,
        "per_horizon": rows,
        "mean_net_pips": float(np.mean([row["mean_net_pips"] for row in rows])),
        "total_net_pips": float(np.sum([row["total_net_pips"] for row in rows])),
    }, chosen


def _pair_metrics(
    frame: pd.DataFrame,
    actual: np.ndarray,
    prediction: np.ndarray,
    horizons: list[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument, indexes in frame.groupby("instrument", observed=True).groups.items():
        positions = frame.index.get_indexer(indexes)
        metrics = forecast_metrics(
            actual[positions], prediction[positions], horizons
        )
        rows.append(
            {
                "instrument": instrument,
                "rows": len(positions),
                "mean_horizon_mae_relative_to_no_change": metrics[
                    "mean_horizon_mae_relative_to_no_change"
                ],
                "mean_direction_accuracy": metrics["mean_direction_accuracy"],
            }
        )
    return sorted(
        rows,
        key=lambda row: row["mean_horizon_mae_relative_to_no_change"],
    )


def _session_metrics(
    frame: pd.DataFrame,
    actual: np.ndarray,
    prediction: np.ndarray,
    horizons: list[int],
) -> list[dict[str, Any]]:
    hours = frame["decision_time_utc"].dt.hour
    sessions = pd.Series(
        np.select(
            [
                (hours >= 0) & (hours < 7),
                (hours >= 7) & (hours < 12),
                (hours >= 12) & (hours < 16),
                (hours >= 16) & (hours < 21),
            ],
            ["asia", "london", "london_new_york_overlap", "new_york_late"],
            default="rollover",
        ),
        index=frame.index,
    )
    rows: list[dict[str, Any]] = []
    for session, indexes in sessions.groupby(sessions).groups.items():
        positions = frame.index.get_indexer(indexes)
        metrics = forecast_metrics(
            actual[positions], prediction[positions], horizons
        )
        rows.append(
            {
                "session": session,
                "rows": len(positions),
                "mean_horizon_mae_relative_to_no_change": metrics[
                    "mean_horizon_mae_relative_to_no_change"
                ],
                "mean_direction_accuracy": metrics["mean_direction_accuracy"],
            }
        )
    return rows


def _model_feature_importance(model: Pipeline) -> list[dict[str, Any]]:
    try:
        names = list(model.named_steps["prepare"].get_feature_names_out())
        estimator = model.named_steps["model"]
        if hasattr(estimator, "feature_importances_"):
            values = np.asarray(estimator.feature_importances_, dtype=float)
            method = "tree_impurity"
        elif hasattr(estimator, "coef_"):
            coefficients = np.asarray(estimator.coef_, dtype=float)
            values = (
                np.abs(coefficients)
                if coefficients.ndim == 1
                else np.mean(np.abs(coefficients), axis=0)
            )
            method = "mean_absolute_standardized_coefficient"
        else:
            return []
        total = float(np.sum(values))
        if total > 0:
            values = values / total
        ranked = np.argsort(values)[::-1][:100]
        return [
            {
                "rank": rank + 1,
                "feature": names[position],
                "importance": float(values[position]),
                "method": method,
            }
            for rank, position in enumerate(ranked)
            if position < len(names) and np.isfinite(values[position])
        ]
    except (AttributeError, IndexError, TypeError, ValueError):
        return []


def train_validate(
    cfg: dict[str, Any],
    data_root: Path,
    run_dir: Path,
    matrix_path: Path,
    max_train_rows: int,
) -> dict[str, Any]:
    settings = cfg["unified_intrahour_forecast"]
    seed = int(settings.get("random_state", 42))
    horizons = [int(value) for value in settings["horizons_minutes"]]
    targets = [f"target_return_pips_{value}" for value in horizons]
    frame = pd.read_parquet(matrix_path)
    frame["decision_time_utc"] = pd.to_datetime(
        frame["decision_time_utc"], errors="coerce", utc=True
    )
    frame = frame.dropna(subset=targets).sort_values(
        ["decision_time_utc", "instrument"]
    ).reset_index(drop=True)
    all_numeric = model_numeric_features(frame)
    latest_rows = (
        frame.groupby("instrument", observed=True, as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    train, validation, final, split = chronological_split(
        frame,
        float(settings["validation_fraction"]),
        float(settings["final_fraction"]),
        int(settings["purge_minutes"]),
    )
    del frame
    gc.collect()
    train = _even_sample(train, max_train_rows)
    validation_for_selection = _even_sample(
        validation, max(50_000, max_train_rows // 2)
    ).reset_index(drop=True)
    baseline_mae = [
        float(np.mean(np.abs(validation_for_selection[target])))
        for target in targets
    ]
    validation_actual = validation_for_selection[targets].to_numpy(dtype=float)
    validation_no_change = forecast_metrics(
        validation_actual,
        np.zeros_like(validation_actual),
        horizons,
        baseline_mae,
    )
    validation_drift = forecast_metrics(
        validation_actual,
        _drift_prediction(validation_for_selection, horizons),
        horizons,
        baseline_mae,
    )
    validation_ar2 = forecast_metrics(
        validation_actual,
        _ar2_prediction(validation_for_selection, horizons),
        horizons,
        baseline_mae,
    )
    candidates: list[dict[str, Any]] = []
    selection_outputs: dict[str, tuple[np.ndarray, list[str]]] = {}

    for name in settings["model_candidates"]:
        family = _candidate_family(str(name))
        numeric = _feature_family(all_numeric, family)
        model, candidate_cap = _candidate(str(name), numeric, seed)
        candidate_train = _even_sample(
            train, min(max_train_rows, candidate_cap)
        )
        print(
            f"[unified-train] candidate={name} family={family} "
            f"train={len(candidate_train):,} validation={len(validation_for_selection):,} "
            f"features={len(numeric)}",
            flush=True,
        )
        model.fit(
            candidate_train[[*numeric, *CATEGORICAL_FEATURES]],
            candidate_train[targets].to_numpy(dtype=float),
        )
        predicted = np.asarray(
            model.predict(
                validation_for_selection[[*numeric, *CATEGORICAL_FEATURES]]
            ),
            dtype=float,
        )
        metrics = forecast_metrics(
            validation_actual, predicted, horizons, baseline_mae
        )
        candidates.append(
            {
                "model_candidate": name,
                "feature_family": family,
                "feature_count": len(numeric),
                "train_rows": len(candidate_train),
                "validation_rows": len(validation_for_selection),
                "validation": metrics,
            }
        )
        selection_outputs[str(name)] = (predicted, numeric)
        del model
        gc.collect()

    winner = min(
        candidates,
        key=lambda row: (
            row["validation"]["mean_horizon_mae_relative_to_no_change"],
            -row["validation"]["mean_direction_accuracy"],
        ),
    )
    candidate_by_name = {
        str(row["model_candidate"]): row for row in candidates
    }

    def ablation_delta(left: str, right: str) -> dict[str, Any]:
        left_row = candidate_by_name.get(left)
        right_row = candidate_by_name.get(right)
        if not left_row or not right_row:
            return {"available": False}
        left_metrics = left_row["validation"]
        right_metrics = right_row["validation"]
        return {
            "available": True,
            "left": left,
            "right": right,
            "relative_mae_delta_left_minus_right": float(
                left_metrics["mean_horizon_mae_relative_to_no_change"]
                - right_metrics["mean_horizon_mae_relative_to_no_change"]
            ),
            "direction_accuracy_delta_left_minus_right": float(
                left_metrics["mean_direction_accuracy"]
                - right_metrics["mean_direction_accuracy"]
            ),
            "negative_relative_mae_delta_favors_left": True,
        }

    feature_family_ablation = {
        "full_mtf_vs_m1_ridge": ablation_delta(
            "ridge_mtf_full", "ridge_m1"
        ),
        "activity_and_historical_spread_value": ablation_delta(
            "ridge_mtf_full", "ridge_mtf_no_activity"
        ),
    }
    winner_name = str(winner["model_candidate"])
    validation_prediction, winner_numeric = selection_outputs[winner_name]
    selection_outputs.clear()
    gc.collect()
    print(
        "[unified-train] selecting research-only opportunity heads on validation",
        flush=True,
    )
    opportunity_selection = select_opportunity_heads(
        train,
        validation_for_selection,
        winner_numeric,
        settings,
        max_train_rows,
    )
    validation_tradability, thresholds = _tradability_metrics(
        validation_for_selection,
        validation_prediction,
        horizons,
        select_thresholds=True,
    )
    residual = validation_actual - validation_prediction
    residual_quantiles = {
        str(horizon): {
            "q10": float(np.nanquantile(residual[:, column], 0.10)),
            "q50": float(np.nanquantile(residual[:, column], 0.50)),
            "q90": float(np.nanquantile(residual[:, column], 0.90)),
            "std": float(np.nanstd(residual[:, column])),
        }
        for column, horizon in enumerate(horizons)
    }
    del validation_for_selection
    del validation_actual
    del validation_prediction
    gc.collect()

    # Model/config selection and every threshold above are now frozen. Only
    # this selected configuration is refit before the final labels are opened.
    development = pd.concat([train, validation], ignore_index=True).sort_values(
        ["decision_time_utc", "instrument"]
    )
    selected_model, selected_cap = _candidate(
        winner_name, winner_numeric, seed
    )
    development = _even_sample(
        development, min(max_train_rows, selected_cap)
    )
    selected_model.fit(
        development[[*winner_numeric, *CATEGORICAL_FEATURES]],
        development[targets].to_numpy(dtype=float),
    )
    opportunity_heads = fit_opportunity_heads(
        development,
        winner_numeric,
        opportunity_selection,
        max_train_rows,
    )
    del development
    del train
    del validation
    gc.collect()
    feature_importance = _model_feature_importance(selected_model)
    final_evaluation = _even_sample(
        final, max(75_000, max_train_rows // 2)
    ).reset_index(drop=True)
    del final
    gc.collect()
    final_prediction = np.asarray(
        selected_model.predict(
            final_evaluation[[*winner_numeric, *CATEGORICAL_FEATURES]]
        ),
        dtype=float,
    )
    final_actual = final_evaluation[targets].to_numpy(dtype=float)
    final_baseline_mae = [
        float(np.mean(np.abs(final_actual[:, column])))
        for column in range(len(horizons))
    ]
    final_metrics = forecast_metrics(
        final_actual, final_prediction, horizons, final_baseline_mae
    )
    final_no_change = forecast_metrics(
        final_actual,
        np.zeros_like(final_actual),
        horizons,
        final_baseline_mae,
    )
    final_drift = forecast_metrics(
        final_actual,
        _drift_prediction(final_evaluation, horizons),
        horizons,
        final_baseline_mae,
    )
    final_ar2 = forecast_metrics(
        final_actual,
        _ar2_prediction(final_evaluation, horizons),
        horizons,
        final_baseline_mae,
    )
    final_opportunity = evaluate_opportunity_heads(
        opportunity_heads,
        final_evaluation,
        winner_numeric,
        max(50_000, max_train_rows // 2),
    )
    final_tradability, _ = _tradability_metrics(
        final_evaluation,
        final_prediction,
        horizons,
        thresholds=thresholds,
    )
    pair_metrics = _pair_metrics(
        final_evaluation,
        final_actual,
        final_prediction,
        horizons,
    )
    session_metrics = _session_metrics(
        final_evaluation,
        final_actual,
        final_prediction,
        horizons,
    )

    artifact_root = data_root / "models"
    artifact_path = artifact_root / "unified_intrahour_forecast_latest.joblib"
    bundle = {
        "schema_version": 1,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "contract_version": settings["contract_version"],
        "created_utc": utc_now_stamp(),
        "model_candidate": winner_name,
        "feature_family": winner["feature_family"],
        "numeric_features": winner_numeric,
        "categorical_features": list(CATEGORICAL_FEATURES),
        "targets": targets,
        "horizons_minutes": horizons,
        "pip_sizes": {
            str(row["instrument"]): float(row["pip_size"])
            for _, row in latest_rows.iterrows()
        },
        "residual_quantiles": residual_quantiles,
        "tradability_thresholds_pips": thresholds,
        "opportunity_heads": opportunity_heads,
        "opportunity_selection": opportunity_selection,
        "model": selected_model,
        "selection_scope": "validation_only",
        "final_test_opened_after_freeze": True,
        "account_eligible": False,
        "deployment_mode": "shadow_only",
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
    }
    _atomic_joblib(bundle, artifact_path)
    artifact_hash = _sha256(artifact_path)

    latest_prediction = np.asarray(
        selected_model.predict(
            latest_rows[[*winner_numeric, *CATEGORICAL_FEATURES]]
        )
    )
    latest_opportunity_predictions = {
        kind: np.asarray(
            state["model"].predict(
                latest_rows[[*winner_numeric, *CATEGORICAL_FEATURES]]
            ),
            dtype=float,
        )
        for kind, state in opportunity_heads.items()
    }
    curves: list[dict[str, Any]] = []
    for row_number, row in latest_rows.iterrows():
        curve: dict[str, Any] = {}
        for column, horizon in enumerate(horizons):
            point = float(latest_prediction[row_number, column])
            residual_state = residual_quantiles[str(horizon)]
            scale = max(1e-9, float(residual_state["std"]))
            curve[str(horizon * 60)] = {
                "predicted_return_pips": point,
                "predicted_signed_pips": point,
                "p10_return_pips": point + float(residual_state["q10"]),
                "p50_return_pips": point + float(residual_state["q50"]),
                "p90_return_pips": point + float(residual_state["q90"]),
                "quantile_low_pips": point + float(residual_state["q10"]),
                "quantile_high_pips": point + float(residual_state["q90"]),
                "probability_up": float(norm.cdf(point / scale)),
            }
        for kind, prediction in latest_opportunity_predictions.items():
            target_names = opportunity_heads[kind]["targets"]
            for column, target in enumerate(target_names):
                horizon = int(target.rsplit("_", 1)[-1])
                cell = curve.get(str(horizon * 60))
                if cell is None:
                    continue
                value = float(prediction[row_number, column])
                value = (
                    float(np.clip(value, 0.0, 1.0))
                    if kind == "probability"
                    else max(0.0, value)
                )
                output_name = target.removeprefix("target_").removesuffix(
                    f"_{horizon}"
                )
                cell[output_name] = value
        curves.append(
            {
                "instrument": row["instrument"],
                "input_timeframe": "MULTI",
                "context_timeframes": list(TIMEFRAME_MINUTES),
                "forecast_origin_utc": row["decision_time_utc"].isoformat(),
                "forecast_curve": curve,
                "artifact_sha256": artifact_hash,
                "account_eligible": False,
                "shadow_only": True,
            }
        )
    shadow_path = data_root / "latest_shadow_forecast_curves.json"
    write_json(
        shadow_path,
        {
            "schema_version": 1,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "generated_utc": utc_now_stamp(),
            "artifact": str(artifact_path.resolve()),
            "artifact_sha256": artifact_hash,
            "one_curve_per_pair": True,
            "account_eligible": False,
            "forecasts": curves,
        },
    )

    forecast_pass = bool(
        winner["validation"]["mean_horizon_mae_relative_to_no_change"] < 1.0
        and final_metrics["mean_horizon_mae_relative_to_no_change"] < 1.0
        and winner["validation"]["mean_horizon_mae_relative_to_no_change"]
        < validation_ar2["mean_horizon_mae_relative_to_no_change"]
        and final_metrics["mean_horizon_mae_relative_to_no_change"]
        < final_ar2["mean_horizon_mae_relative_to_no_change"]
        and split["train_purge_passed"]
        and split["validation_purge_passed"]
    )
    after_cost_pass = bool(final_tradability["mean_net_pips"] > 0.0)
    deployment_eligible = forecast_pass and after_cost_pass
    report = {
        "schema_version": 1,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "status": "complete",
        "verdict": "PASS" if deployment_eligible else "FAIL",
        "forecast_validation_pass": forecast_pass,
        "after_cost_tradability_pass": after_cost_pass,
        "deployment_eligible": deployment_eligible,
        "deployment_performed": False,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "matrix": str(matrix_path.resolve()),
        "matrix_sha256": _sha256(matrix_path),
        "split": split,
        "selection": {
            "scope": "validation_only",
            "candidates": candidates,
            "winner": winner,
            "fixed_baselines": {
                "no_change": validation_no_change,
                "drift": validation_drift,
                "rolling_differenced_ar2": validation_ar2,
            },
            "feature_family_ablation": feature_family_ablation,
            "thresholds_frozen_on_validation": thresholds,
            "opportunity_heads": opportunity_selection,
        },
        "untouched_final": {
            "opened_after_configuration_freeze": True,
            "rows": len(final_evaluation),
            "selected_model": final_metrics,
            "no_change": final_no_change,
            "drift": final_drift,
            "rolling_differenced_ar2": final_ar2,
            "opportunity_heads": final_opportunity,
            "pair_breakdown": pair_metrics,
            "session_breakdown": session_metrics,
        },
        "selected_model_feature_importance": feature_importance,
        "tradability_diagnostic": {
            "used_as_training_target": False,
            "validation": validation_tradability,
            "final": final_tradability,
        },
        "artifact": str(artifact_path.resolve()),
        "artifact_sha256": artifact_hash,
        "shadow_forecasts": str(shadow_path.resolve()),
        "promotion_hash_contract": {
            "namespace": artifact_hash,
            "old_promotion_state_reused": False,
            "new_hash_requires_new_prospective_shadow_evidence": True,
        },
        "excluded_inputs": {
            "account_state": True,
            "position_state": True,
            "order_book": True,
            "position_book": True,
            "live_depth": True,
            "s1_or_s5_history": True,
        },
    }
    write_json(run_dir / "UNIFIED_FORECAST_VALIDATION.json", report)
    write_json(data_root / "latest_validation.json", report)
    lines = [
        "# Unified Intrahour Forecast Validation",
        "",
        f"- Verdict: **{report['verdict']}**",
        f"- Selected on validation only: `{winner_name}` / `{winner['feature_family']}`",
        f"- Features: {winner['feature_count']}",
        f"- Validation relative MAE: {winner['validation']['mean_horizon_mae_relative_to_no_change']:.4f}",
        f"- Final relative MAE: {final_metrics['mean_horizon_mae_relative_to_no_change']:.4f}",
        f"- Final AR(2) relative MAE: {final_ar2['mean_horizon_mae_relative_to_no_change']:.4f}",
        f"- Final direction accuracy: {final_metrics['mean_direction_accuracy']:.4f}",
        f"- Final after-cost mean pips: {final_tradability['mean_net_pips']:.4f}",
        f"- Pairs: {final_evaluation['instrument'].nunique()}",
        f"- Deployment eligible: {deployment_eligible}",
        "- Deployment performed: False",
        "- Live/OANDA execution enabled: False / False",
        "",
        "The structural midpoint path is the model target. Executable bid/ask "
        "outcomes are a separate diagnostic and never enter model selection.",
    ]
    (run_dir / "UNIFIED_FORECAST_VALIDATION.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    write_json(
        run_dir / "UNIFIED_FORECAST_DEPLOYMENT_GATE.json",
        {
            "verdict": "PASS" if deployment_eligible else "FAIL",
            "deployment_eligible": deployment_eligible,
            "deployment_performed": False,
            "artifact_sha256": artifact_hash,
            "reason": (
                "untouched final forecast and after-cost gates passed"
                if deployment_eligible
                else "untouched final forecast and/or after-cost gate failed"
            ),
            "account_eligible": False,
            "live_execution_enabled": False,
            "oanda_execution_enabled": False,
        },
    )
    return report


def run_unified_forecast(
    cfg: dict[str, Any],
    run_dir: Path,
    *,
    source_dir: Path,
    data_root: Path,
    mode: str,
    start: str | None,
    end: str | None,
    pairs: list[str] | None,
    max_matrix_rows: int | None,
    max_train_rows: int | None,
    force: bool,
) -> Path:
    settings = cfg["unified_intrahour_forecast"]
    if cfg.get("execution", {}).get("live_execution_enabled"):
        raise RuntimeError("refusing research run: live_execution_enabled must be false")
    if settings.get("live_execution_enabled") or settings.get(
        "oanda_execution_enabled"
    ):
        raise RuntimeError("refusing research run: unified execution flags must be false")
    source_dir = source_dir.resolve()
    data_root = data_root.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)
    if mode in {"all", "build"}:
        build_histories(
            cfg, source_dir, data_root, start, end, pairs, force
        )
    if mode == "build":
        write_json(
            run_dir / "UNIFIED_FORECAST_BUILD_POINTER.json",
            {
                "build_manifest": str(
                    (data_root / "build_manifest.json").resolve()
                ),
                "live_execution_enabled": False,
                "oanda_execution_enabled": False,
            },
        )
        return run_dir
    matrix_path, matrix_manifest = assemble_matrix(
        data_root,
        int(max_matrix_rows or settings["max_matrix_rows"]),
        force,
    )
    report = train_validate(
        cfg,
        data_root,
        run_dir,
        matrix_path,
        int(max_train_rows or settings["max_train_rows"]),
    )
    write_json(
        run_dir / "UNIFIED_FORECAST_RUN_COMPLETE.json",
        {
            "run_dir": str(run_dir.resolve()),
            "data_root": str(data_root),
            "matrix_manifest": matrix_manifest,
            "validation_report": str(
                (run_dir / "UNIFIED_FORECAST_VALIDATION.json").resolve()
            ),
            "verdict": report["verdict"],
            "deployment_performed": False,
            "live_execution_enabled": False,
            "oanda_execution_enabled": False,
        },
    )
    return run_dir
