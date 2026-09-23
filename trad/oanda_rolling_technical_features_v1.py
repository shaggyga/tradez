"""One finite, causal M1 technical kernel for historical and live observations.

Inputs contain original UTC minute START epochs, mid OHLC, optional bid/ask
closes and tick activity.  A row is evaluated at that minute's END.  Callers
must pass only completed bars and record their separate receipt/publication
clocks; this module neither reads files nor knows the current time.

Formula lineage: retained/source/D_unified_forecast.py:timeframe_features,
oanda_practice_shadow_strategy_lab.py:build_features and the MA grid families.
This is a NEW schema, not a claim of compatibility with their fitted weights.
EMA has explicitly finite, normalized exponential weights; no hidden initial
state, expanding statistics, whole-history scale fallback or future inputs.
"""
from __future__ import annotations

from functools import lru_cache
import hashlib
import json
import math
import re
from typing import Mapping, Any

import numpy as np

SCHEMA_VERSION = "rolling_m1_technical_features_v1_20260915"
RET_WINDOWS = (1, 2, 3, 5, 8, 13, 15, 21, 30, 60)
STAT_WINDOWS = (5, 15, 30, 60)
TREND_PERIODS = (5, 13, 20, 50, 60, 100, 200)
INPUT_COLUMNS = ("time", "open", "high", "low", "close", "bid_close", "ask_close", "volume")
SOURCE_TECHNICAL = "feature_horizon_audit_20260908/retained/source/D_unified_forecast.py:timeframe_features"
SOURCE_STRUCTURAL = "oanda_practice_shadow_strategy_lab.py:build_features"
SOURCE_MA = "oanda_ma_feature_grid.py:build_ma_feature_matrix (family lineage; finite revised formulas)"


def _normalize(data: Mapping[str, Any], pair: str, pip: float) -> dict[str, np.ndarray]:
    if not isinstance(pair, str) or not re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", pair):
        raise ValueError("explicit_pair_required")
    if isinstance(pip, bool) or not isinstance(pip, (int, float, np.integer, np.floating)) or not math.isfinite(pip) or not 1e-8 <= pip <= .1:
        raise ValueError("explicit_positive_pip_required")
    raw = np.asarray(data["time"])
    if raw.ndim != 1 or raw.dtype.kind not in "iuf":
        raise ValueError("one_dimensional_numeric_time_required")
    if np.any(~np.isfinite(raw)) or np.any(raw < 0) or np.any(raw > 253402300799) or np.any(raw % 60):
        raise ValueError("original_aligned_utc_minute_start_required")
    times = raw.astype(np.int64)
    if np.any(np.diff(times) <= 0):
        raise ValueError("unique_sorted_minutes_required")
    result = {"time": times}
    for name in INPUT_COLUMNS[1:]:
        if name not in data:
            if name in ("bid_close", "ask_close", "volume"):
                result[name] = np.full(len(times), np.nan)
                continue
            raise ValueError("required_candle_column_missing:" + name)
        values = np.asarray(data[name], dtype=np.float64)
        if values.ndim != 1 or len(values) != len(times):
            raise ValueError("candle_array_shape_mismatch:" + name)
        values = values.copy()
        values[~np.isfinite(values)] = np.nan
        values[values < 0 if name == "volume" else values <= 0] = np.nan
        result[name] = values
    # Invalid OHLC affects OHLC-derived families, not valid close-only inputs.
    o, h, l, c = (result[k] for k in ("open", "high", "low", "close"))
    invalid = (h < np.maximum(o, c)) | (l > np.minimum(o, c)) | (h < l)
    for name in ("open", "high", "low"):
        result[name][invalid] = np.nan
    crossed = result["ask_close"] < result["bid_close"]
    result["bid_close"][crossed] = np.nan
    result["ask_close"][crossed] = np.nan
    return result


class _Windows:
    def __init__(self, times: np.ndarray):
        self.n = len(times)
        indexes = np.arange(self.n)
        breaks = np.r_[True, np.diff(times) != 60] if self.n else np.array([], dtype=bool)
        self.run = indexes - np.maximum.accumulate(np.where(breaks, indexes, 0)) + 1

    def shift(self, values: np.ndarray, lag: int = 1) -> np.ndarray:
        out = np.full(self.n, np.nan)
        if lag == 0:
            return values.copy()
        if lag < self.n:
            out[lag:] = values[:-lag]
        out[self.run < lag + 1] = np.nan
        return out

    def diff(self, values: np.ndarray, lag: int = 1) -> np.ndarray:
        result = values - self.shift(values, lag)
        if lag < self.n:
            supported = np.isfinite(np.lib.stride_tricks.sliding_window_view(values,lag+1)).all(axis=1)
            result[lag:] = np.where(supported,result[lag:],np.nan)
        return result

    def roll(self, values: np.ndarray, width: int, operation: str) -> np.ndarray:
        out = np.full(self.n, np.nan)
        if width > self.n:
            return out
        windows = np.lib.stride_tricks.sliding_window_view(values, width)
        valid = np.isfinite(windows).all(axis=1) & (self.run[width-1:] >= width)
        # Invalid rows are masked afterward; arithmetic never publishes fills.
        safe = np.where(np.isfinite(windows), windows, 0.)
        if operation == "mean":
            calc = safe.mean(axis=1)
        elif operation == "sum":
            calc = safe.sum(axis=1)
        elif operation == "std":
            calc = safe.std(axis=1, ddof=1)
        elif operation == "median":
            calc = np.median(safe, axis=1)
        elif operation == "min":
            calc = safe.min(axis=1)
        elif operation == "max":
            calc = safe.max(axis=1)
        elif operation == "skew":
            centered = safe - safe.mean(axis=1)[:, None]
            m2 = np.mean(centered**2, axis=1)
            calc = _divide(np.mean(centered**3, axis=1), m2**1.5)
        elif operation == "kurt":
            centered = safe - safe.mean(axis=1)[:, None]
            m2 = np.mean(centered**2, axis=1)
            calc = _divide(np.mean(centered**4, axis=1), m2**2) - 3.
        else:
            raise ValueError("unsupported_rolling_operation")
        out[width-1:] = np.where(valid, calc, np.nan)
        return out

    def corr(self, left: np.ndarray, right: np.ndarray, width: int) -> np.ndarray:
        out = np.full(self.n, np.nan)
        if self.n < width:
            return out
        a = np.lib.stride_tricks.sliding_window_view(left, width)
        b = np.lib.stride_tricks.sliding_window_view(right, width)
        valid = np.isfinite(a).all(axis=1) & np.isfinite(b).all(axis=1) & (self.run[width-1:] >= width)
        a = np.where(np.isfinite(a), a, 0.)
        b = np.where(np.isfinite(b), b, 0.)
        a = a - a.mean(axis=1)[:, None]
        b = b - b.mean(axis=1)[:, None]
        calc = _divide((a*b).sum(axis=1), np.sqrt((a*a).sum(axis=1)*(b*b).sum(axis=1)))
        out[width-1:] = np.where(valid, np.clip(calc, -1., 1.), np.nan)
        return out

    def ema(self, values: np.ndarray, period: int) -> np.ndarray:
        """Trailing 3*period observations; normalized alpha*(1-alpha)^age."""
        width = 3 * period
        out = np.full(self.n, np.nan)
        if self.n < width:
            return out
        windows = np.lib.stride_tricks.sliding_window_view(values, width)
        valid = np.isfinite(windows).all(axis=1) & (self.run[width-1:] >= width)
        alpha = 2. / (period + 1.)
        weights = (1.-alpha)**np.arange(width-1, -1, -1, dtype=float)
        weights /= weights.sum()
        # Fixed order reduction is independent of batch size/BLAS scheduling.
        calc = (np.where(np.isfinite(windows), windows, 0.) * weights).sum(axis=1)
        out[width-1:] = np.where(valid, calc, np.nan)
        return out


def _divide(numerator, denominator) -> np.ndarray:
    n, d = np.broadcast_arrays(np.asarray(numerator, dtype=float), np.asarray(denominator, dtype=float))
    out = np.full(n.shape, np.nan)
    np.divide(n, d, out=out, where=np.isfinite(n) & np.isfinite(d) & (d != 0))
    return out


def _calculate(data, pip):
    time = data["time"]
    w = _Windows(time)
    n = len(time)
    c, o, h, l, volume = (data[k] for k in ("close", "open", "high", "low", "volume"))
    columns, registry = {}, []

    def add(name, value, family, unit, lookback, formula, aliases=(), source=SOURCE_TECHNICAL):
        name = "m1__" + name
        if name in columns:
            raise AssertionError("duplicate_canonical_feature:" + name)
        value = np.asarray(value, dtype=np.float64)
        if value.shape != (n,):
            raise AssertionError("feature_shape:" + name)
        value = value.copy()
        value[~np.isfinite(value)] = np.nan
        columns[name] = value
        registry.append({"name": name, "family": family, "unit": unit,
            "lookback_bars": lookback, "lookback_seconds": lookback*60,
            "formula": formula, "source_origin": source,
            "aliases": list(aliases), "timeframe": "M1",
            "availability": "last_required_completed_bar_end",
            "gap_policy": "exact_60_second_support_for_each_feature",
            "missing_policy": "NaN; no imputation", "model_input": True})

    ret = w.diff(c)/pip
    # Current-bar values can be useful immediately; no all-family warm-up gate.
    spread = (data["ask_close"]-data["bid_close"])/pip
    add("historical_spread_pips", spread, "spread", "pips", 1, "(ask_close-bid_close)/pip", ("current_candle_spread_pips",))
    add("tick_activity_log1p", np.log1p(volume), "activity", "log_ticks", 1, "log1p(OANDA_tick_activity); not traded volume")
    add("tick_activity", volume, "activity", "ticks", 1, "completed_bar_tick_activity", ("current_volume",))
    for lag in range(1, 6):
        add(f"return_lag_{lag:02d}_pips", w.shift(ret, lag), "returns", "pips", lag+2, f"return_1_pips shifted {lag} elapsed minutes")
    for size in RET_WINDOWS:
        aliases = ("return_lag_00_pips", "r1_pips") if size == 1 else (f"r{size}_pips",) if size in (3, 5) else ()
        change = w.diff(c, size)
        add(f"return_{size}_pips", change/pip, "returns", "pips", size+1, f"(close[t]-close[t-{size}])/pip", aliases)
        add(f"return_{size}_bps", _divide(change, w.shift(c, size))*10000, "returns", "bps", size+1, f"10000*(close[t]/close[t-{size}]-1)")
        if size == 1:
            continue
        path = w.roll(np.abs(ret), size, "sum")
        efficiency = _divide(np.abs(change)/pip, path)
        efficiency[(path == 0) & np.isfinite(change)] = 0.
        add(f"path_efficiency_{size}", efficiency, "path", "fraction", size+1, f"abs(return_{size}_pips)/sum(abs(return_1),{size}); flat path=0")
        add(f"absolute_path_{size}_pips", path, "path", "pips", size+1, f"sum(abs(return_1_pips),{size})")
        ups = np.where(np.isfinite(ret), (ret > 0).astype(float), np.nan)
        add(f"up_fraction_{size}", w.roll(ups, size, "mean"), "path", "fraction", size+1, f"mean(return_1_pips>0,{size}); flat minutes included")
        high, low = w.roll(h, size, "max"), w.roll(l, size, "min")
        position = _divide(c-low, high-low)
        position[(high == low) & np.isfinite(c)] = .5
        add(f"range_position_{size}", position, "path", "fraction", size, f"(close-min(low,{size}))/(max(high,{size})-min(low,{size})); flat=.5")

    previous = w.shift(c)
    true_range = np.maximum.reduce([h-l, np.abs(h-previous), np.abs(l-previous)])/pip
    # True range requires the prior minute; never bridge an unknown/weekend gap.
    bar_range, body = (h-l)/pip, (c-o)/pip
    add("bar_range_pips", bar_range, "ohlc", "pips", 1, "(high-low)/pip")
    add("bar_body_pips", body, "ohlc", "pips", 1, "(close-open)/pip")
    for name, numerator in (("body_to_range", body), ("upper_wick_fraction", (h-np.maximum(o,c))/pip), ("lower_wick_fraction", (np.minimum(o,c)-l)/pip)):
        add(name, _divide(numerator, bar_range), "ohlc", "fraction", 1, name+"/bar_range; zero range is undefined")
    atrs = {}
    for size in (5, 14, 30, 60):
        atrs[size] = w.roll(true_range, size, "mean")
        add(f"atr_{size}_pips", atrs[size], "volatility", "pips", size+1, f"mean(true_range,{size}); requires prior close", ("m1_atr14_pips",) if size == 14 else ())
        add(f"range_to_atr_{size}", _divide(bar_range, atrs[size]), "volatility", "ratio", size+1, f"bar_range_pips/atr_{size}_pips")
    for size in (5, 15, 30, 60):
        add(f"momentum_{size}_atr", _divide(w.diff(c,size)/pip, atrs[14]), "returns", "ATR_units", max(size+1,15), f"return_{size}_pips/atr_14_pips")

    for size in STAT_WINDOWS:
        mean, std = w.roll(ret,size,"mean"), w.roll(ret,size,"std")
        add(f"return_mean_{size}_pips", mean, "volatility", "pips", size+1, f"mean(return_1_pips,{size})")
        add(f"return_vol_{size}_pips", std, "volatility", "pips", size+1, f"sample_std(return_1_pips,{size})")
        add(f"return_z_{size}", _divide(ret-mean,std), "volatility", "z_score", size+1, f"(return_1_pips-mean_{size})/sample_std_{size}; flat undefined")
        add(f"return_skew_{size}", w.roll(ret,size,"skew"), "volatility", "ratio", size+1, f"population central moment3 / moment2^1.5 over {size} returns")
        add(f"return_kurt_{size}", w.roll(ret,size,"kurt"), "volatility", "ratio", size+1, f"population excess kurtosis over {size} returns")
        entropy = np.zeros(n)
        for sign in (-1,0,1):
            probability = w.roll(np.where(np.isfinite(ret), (np.sign(ret)==sign).astype(float), np.nan),size,"mean")
            term = np.zeros(n)
            positive = probability > 0
            term[positive] = -probability[positive]*np.log(probability[positive])
            term[~np.isfinite(probability)] = np.nan
            entropy += term
        add(f"sign_entropy_{size}", entropy, "path", "nats", size+1, f"entropy of down/flat/up over {size} returns")
        add(f"return_autocorr1_{size}", w.corr(ret,w.shift(ret),size), "path", "correlation", size+2, f"corr(return_1,lag1_return_1,{size}); not a fitted ARIMA forecast")
        for label, signed in (("up",np.clip(ret,0,None)),("down",np.clip(-ret,0,None))):
            add(f"realized_{label}_semivol_{size}",np.sqrt(w.roll(signed*signed,size,"mean")),"volatility","pips",size+1,f"sqrt(mean({label} return squared,{size}))")
        variance = w.roll(ret*ret,size,"mean")
        bipower = w.roll(np.abs(ret*w.shift(ret)),size,"mean")
        add(f"jump_variation_ratio_{size}",_divide(np.maximum(0.,variance-math.pi/2*bipower),variance),"volatility","fraction",size+2,f"max(0,realized_var-pi/2*bipower)/realized_var over {size}")

    for size in (7,14,30):
        gain, loss = w.roll(np.maximum(ret,0),size,"mean"), w.roll(np.maximum(-ret,0),size,"mean")
        rsi = 100*_divide(gain,gain+loss)
        rsi[(gain == 0) & (loss == 0)] = 50.
        add(f"rsi_{size}",rsi,"oscillator","index_0_100",size+1,f"100*mean(gain)/mean(gain+loss) over {size}; flat=50, gain-only=100, loss-only=0")
    for size in (14,20):
        high, low = w.roll(h,size,"max"),w.roll(l,size,"min")
        position = _divide(c-low,high-low)
        position[(high == low) & np.isfinite(c)] = .5
        add(f"range_position_{size}",position,"oscillator","fraction",size,f"(close-min(low,{size}))/(max(high,{size})-min(low,{size})); flat=.5",("stochastic_14",) if size == 14 else ())
    # Legacy pos20 is CLOSE range, unlike high/low stochastic; not deduplicated.
    high, low = w.roll(c,20,"max"),w.roll(c,20,"min")
    position = _divide(c-low,high-low)
    position[(high == low) & np.isfinite(c)] = .5
    add("close_range_position_20",position,"path","fraction",20,"(close-min(close,20))/(max(close,20)-min(close,20)); flat=.5",("pos20",),SOURCE_STRUCTURAL)

    smas, emas = {}, {}
    for period in TREND_PERIODS:
        smas[period], emas[period] = w.roll(c,period,"mean"),w.ema(c,period)
        for kind, value, support in (("sma",smas[period],period),("finite_ema",emas[period],3*period)):
            add(f"{kind}_gap_{period}_pips",(c-value)/pip,"trend","pips",support,f"(close-{kind}({period}))/pip; finite_ema uses normalized trailing3p weights",source=SOURCE_MA)
            for lag in (1,3):
                add(f"{kind}_slope{lag}_{period}_pips",w.diff(value,lag)/pip,"trend","pips",support+lag,f"({kind}({period})[t]-{kind}({period})[t-{lag}])/pip",source=SOURCE_MA)
            add(f"{kind}_curvature_{period}_pips",w.diff(w.diff(value))/pip,"trend","pips",support+2,f"second finite difference of {kind}({period})/pip",source=SOURCE_MA)
        add(f"sma_finite_ema_gap_{period}_pips",(smas[period]-emas[period])/pip,"trend","pips",3*period,f"(sma({period})-finite_ema({period}))/pip",source=SOURCE_MA)
    typical = (h+l+c)/3
    for period in (20,60):
        std = w.roll(c,period,"std")
        add(f"bollinger_z_{period}",_divide(c-smas[period],std),"oscillator","z_score",period,f"(close-sma_{period})/sample_std(close,{period})")
        mean = w.roll(typical,period,"mean")
        # Original used a mean of historical deviations from varying means.
        # Retain that exact family under an honest, distinct name.
        deviation = w.roll(np.abs(typical-mean),period,"mean")
        add(f"cci_rolling_deviation_{period}",_divide(typical-mean,.015*deviation),"oscillator","index",2*period-1,f"(typical-mean_{period})/(.015*mean(abs(typical-historical_mean_{period}),{period}))")
    macd = w.ema(c,12)-w.ema(c,26)
    add("finite_macd_pips",macd/pip,"trend","pips",78,"(finite_ema12-close minus finite_ema26-close)/pip")
    add("finite_macd_signal_gap_pips",(macd-w.ema(macd,9))/pip,"trend","pips",104,"(finite_macd-finite_ema9(finite_macd))/pip")
    fast, slow = w.ema(c,8),w.ema(c,32)
    add("finite_state_fast_gap_pips",(c-fast)/pip,"trend","pips",24,"(close-finite_ema8)/pip; descriptive state, not fitted model")
    add("finite_state_slow_gap_pips",(c-slow)/pip,"trend","pips",96,"(close-finite_ema32)/pip")
    add("finite_state_trend_pips",(fast-slow)/pip,"trend","pips",96,"(finite_ema8-finite_ema32)/pip")

    logv = np.log1p(volume)
    for size in (12,30,120):
        previous_volume = w.shift(volume)
        median = w.roll(previous_volume,size,"median")
        add(f"tick_activity_ratio_{size}",_divide(volume,median),"activity","ratio",size+1,f"volume/median(strictly previous {size} ticks); zero baseline undefined",("volume_ratio_12",) if size == 12 else ("volume_ratio_30",) if size == 30 else ())
    prior_logv = w.shift(logv)
    add("tick_activity_z_120",_divide(logv-w.roll(prior_logv,120,"mean"),w.roll(prior_logv,120,"std")),"activity","z_score",121,"(log1p(volume)-mean(previous120))/sample_std(previous120)")
    add("tick_activity_change",w.diff(logv),"activity","log_ticks",2,"log1p(volume[t])-log1p(volume[t-1])")
    add("return_activity_corr_60",w.corr(ret,logv,60),"activity","correlation",61,"corr(return_1_pips,log1p(volume),60)")
    for size in (12,120):
        prior_spread = w.shift(spread)
        median = w.roll(prior_spread,size,"median")
        add(f"spread_median_prior_{size}_pips",median,"spread","pips",size+1,f"median(strictly previous {size} completed spreads)",("median_spread_12_pips",) if size == 12 else ())
        add(f"spread_ratio_prior_{size}",_divide(spread,median),"spread","ratio",size+1,f"spread/median(strictly previous {size} completed spreads)",("spread_ratio_12",) if size == 12 else ())
    add("spread_z_prior_120",_divide(spread-w.roll(w.shift(spread),120,"mean"),w.roll(w.shift(spread),120,"std")),"spread","z_score",121,"(spread-mean(previous120))/sample_std(previous120)")
    add("spread_drop_3_pips",w.shift(spread,3)-spread,"spread","pips",4,"spread[t-3]-spread[t]")
    add("movement_to_spread_14",_divide(atrs[14],spread),"spread","ratio",15,"atr_14_pips/current_completed_spread_pips")
    # Fixed UTC cycle encodings avoid mutable timezone databases/session clocks.
    end = time + 60
    hour = (end % 86400)/3600.
    weekday = ((end // 86400 + 3) % 7).astype(float)
    for name, values, period in (("hour",hour,24.),("weekday",weekday,7.)):
        for operation in ("sin","cos"):
            add(f"utc_{name}_{operation}",getattr(np,operation)(2*math.pi*values/period),"calendar","cycle",1,f"{operation}(2*pi*UTC_completed_bar_{name}/{period}); no price inputs",source="UTC completed M1 bar end clock")
    return columns, registry


def compute_features(data: Mapping[str, Any], pair: str, pip: float) -> dict[str, np.ndarray]:
    """Return ordered full-length float64 arrays; NaN means unsupported/undefined.

    To replay a chunk, provide at least ``max_lookback_bars()-1`` original bars
    preceding its first desired output. Live callers use this exact function.
    An absent optional source only affects its own dependent families.
    """
    normalized = _normalize(data,pair,pip)
    return _calculate(normalized,float(pip))[0]


def latest_features(data: Mapping[str, Any], pair: str, pip: float) -> dict[str, float | None]:
    features = compute_features(data,pair,pip)
    return {name: float(values[-1]) if len(values) and np.isfinite(values[-1]) else None for name,values in features.items()}


@lru_cache(maxsize=1)
def _registry_json() -> str:
    data = {k:np.array([],dtype=np.int64 if k == "time" else float) for k in INPUT_COLUMNS}
    return json.dumps(_calculate(data,.0001)[1],sort_keys=True,separators=(",",":"))


def feature_registry() -> list[dict[str, Any]]:
    return json.loads(_registry_json())


def max_lookback_bars() -> int:
    return max(row["lookback_bars"] for row in feature_registry())


def schema_metadata() -> dict[str, Any]:
    return {"schema_version":SCHEMA_VERSION,"feature_count":len(feature_registry()),
        "registry_sha256":hashlib.sha256(_registry_json().encode()).hexdigest(),
        "max_lookback_bars":max_lookback_bars(),"input_time":"original UTC minute START epoch",
        "feature_time":"completed minute END epoch; caller supplies completed rows only",
        "missing":"NaN in arrays / None in latest view; never zero imputed",
        "ema":"finite normalized exponential weights over trailing3*period completed minutes",
        "gap":"gaps invalidate only features whose exact elapsed support crosses them, including weekends",
        "historical_live_parity":"same pure generator with max_lookback_bars-1 overlap",
        "dedup":"one canonical column per exact formula; aliases in registry; related windows are retained",
        "retained_model_weight_compatibility":False,"can_place_orders":False}
