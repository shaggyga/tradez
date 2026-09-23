"""Causal control-window study for large-move technical setup hypotheses.

This module is research-only. It reads cached M1 bid/ask candles, constructs
features from completed M5 bars, enters on the next M1 bar, and measures the
following 60 wall-clock minutes using executable bid/ask prices.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PIPELINE_VERSION = "technical_setup_control_study_v1"
DEFAULT_DATA_ROOT = Path(
    r"C:\Users\zmoor\AppData\Local\Temp"
    r"\fx_continuous_runtime_20260721\trad\data"
    r"\oanda_training_manager\candles_m1_parquet"
)
DEFAULT_REPORT_ROOT = (
    Path(__file__).resolve().parents[1]
    / "reports"
    / "technical_setup_control_study_20260608_20260722"
)
ALERT_EXPORT_COLUMNS = [
    "decision_utc",
    "instrument",
    "rule",
    "period",
    "session",
    "forecast_side",
    "spread_pips",
    "atr240_pips",
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "momentum_60_atr",
    "ema8_minus_ema21_atr",
    "macd_hist_atr",
    "rsi14_centered",
    "range_position_60_centered",
    "atr15_to_atr240",
    "compression_30",
    "realized_vol_ratio_30_240",
    "volume_z_30",
    "cross_pair_trend_breadth",
    "forecast_mfe_pips",
    "forecast_terminal_pips",
    "forecast_mfe_bps",
    "forecast_terminal_bps",
    "any_side_mfe_bps",
]

# Match the restored project's instrument metadata used by the weekly study.
PIP_LOCATION_MINUS2 = {
    "AUD_JPY",
    "CAD_JPY",
    "CHF_JPY",
    "EUR_HUF",
    "EUR_JPY",
    "GBP_JPY",
    "NZD_JPY",
    "SGD_JPY",
    "TRY_JPY",
    "USD_HUF",
    "USD_JPY",
    "USD_THB",
    "ZAR_JPY",
}


@dataclass(frozen=True)
class RuleDefinition:
    name: str
    forecast_style: str
    description: str
    thresholds: dict[str, float]


RULES = [
    RuleDefinition(
        name="continuation_pullback",
        forecast_style="trend",
        description=(
            "Strong aligned M30/M60 trend, aligned EMA/RSI/range state, "
            "and an M5 pullback against that trend."
        ),
        thresholds={
            "abs_momentum_30_atr_min": 1.50,
            "abs_momentum_60_atr_min": 2.50,
            "ema8_minus_ema21_aligned_min": 0.35,
            "rsi14_aligned_min": 0.15,
            "range60_aligned_min": 0.35,
            "momentum_5_pullback_max": -0.15,
            "atr15_to_atr240_min": 1.00,
        },
    ),
    RuleDefinition(
        name="continuation_pullback_breadth",
        forecast_style="trend",
        description=(
            "Continuation-pullback rule plus agreement from other pairs "
            "sharing either currency."
        ),
        thresholds={"cross_pair_trend_breadth_min": 0.35},
    ),
    RuleDefinition(
        name="exhaustion_reversal",
        forecast_style="reversal",
        description=(
            "Extreme aligned M30/M60 extension with EMA, RSI, MACD, and "
            "range saturation; forecast the opposite direction."
        ),
        thresholds={
            "abs_momentum_30_atr_min": 2.50,
            "abs_momentum_60_atr_min": 2.50,
            "ema8_minus_ema21_aligned_min": 0.35,
            "macd_hist_aligned_min": 0.00,
            "rsi14_aligned_min": 0.20,
            "range60_aligned_min": 0.85,
        },
    ),
    RuleDefinition(
        name="exhaustion_reversal_breadth",
        forecast_style="reversal",
        description=(
            "Exhaustion-reversal rule plus broad agreement in the old, "
            "now-faded trend."
        ),
        thresholds={"cross_pair_old_trend_breadth_min": 0.35},
    ),
    RuleDefinition(
        name="event_vulnerability",
        forecast_style="trend",
        description=(
            "Broad, mature trend at a recent range edge while pre-move "
            "volume is no higher than mildly elevated."
        ),
        thresholds={
            "abs_momentum_30_atr_min": 1.50,
            "abs_momentum_60_atr_min": 2.50,
            "ema8_minus_ema21_aligned_min": 0.35,
            "rsi14_aligned_min": 0.15,
            "range60_aligned_min": 0.35,
            "cross_pair_trend_breadth_min": 0.35,
            "volume_z_30_max": 0.25,
        },
    ),
    RuleDefinition(
        name="low_volume_range_edge",
        forecast_style="trend",
        description=(
            "Trend-aligned range-edge positioning with compression and "
            "unusually low completed-bar volume."
        ),
        thresholds={
            "abs_momentum_60_atr_min": 2.00,
            "ema8_minus_ema21_aligned_min": 0.25,
            "rsi14_aligned_min": 0.10,
            "range60_aligned_min": 0.75,
            "cross_pair_trend_breadth_min": 0.25,
            "compression_30_max": 1.00,
            "volume_z_30_max": -0.50,
        },
    ),
]


def pip_multiplier(instrument: str) -> float:
    return 100.0 if instrument in PIP_LOCATION_MINUS2 else 10_000.0


def pair_parts(instrument: str) -> tuple[str, str]:
    return tuple(instrument.split("_", 1))  # type: ignore[return-value]


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0.0, np.nan)


def load_m1(path: Path) -> pd.DataFrame:
    columns = [
        "datetime",
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
    frame = pd.read_parquet(path, columns=columns)
    timestamp_source = frame["datetime"].fillna(frame["time"])
    frame["time_utc"] = pd.to_datetime(timestamp_source, errors="coerce", utc=True)
    numeric_columns = [column for column in columns if column not in {"datetime", "time"}]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.dropna(
            subset=[
                "time_utc",
                "open",
                "high",
                "low",
                "close",
                "bid_open",
                "bid_high",
                "bid_low",
                "bid_close",
                "ask_open",
                "ask_high",
                "ask_low",
                "ask_close",
            ]
        )
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )


def completed_m5_bars(m1: pd.DataFrame) -> pd.DataFrame:
    bars = m1.resample("5min", label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "bid_open": "first",
            "bid_high": "max",
            "bid_low": "min",
            "bid_close": "last",
            "ask_open": "first",
            "ask_high": "max",
            "ask_low": "min",
            "ask_close": "last",
            "spread_pips": "median",
        }
    )
    counts = m1["close"].resample("5min", label="left", closed="left").count()
    bars["source_m1_count"] = counts
    return bars.loc[counts >= 5].dropna(subset=["open", "high", "low", "close"])


def build_features(instrument: str, bars: pd.DataFrame) -> pd.DataFrame:
    multiplier = pip_multiplier(instrument)
    close = bars["close"].astype(float)
    previous = close.shift()
    true_range_pips = (
        pd.concat(
            [
                bars["high"] - bars["low"],
                (bars["high"] - previous).abs(),
                (bars["low"] - previous).abs(),
            ],
            axis=1,
        ).max(axis=1)
        * multiplier
    )
    atr240 = true_range_pips.rolling(48, min_periods=48).mean()
    atr15 = true_range_pips.rolling(3, min_periods=3).mean()
    output = bars.copy()
    output["instrument"] = instrument
    output["decision_utc"] = output.index + dt.timedelta(minutes=5)
    output["atr240_pips"] = atr240
    output["atr15_to_atr240"] = safe_divide(atr15, atr240)
    for minutes, periods in [(5, 1), (15, 3), (30, 6), (60, 12)]:
        output[f"momentum_{minutes}_atr"] = safe_divide(
            close.diff(periods) * multiplier,
            atr240,
        )

    ema8 = close.ewm(span=8, adjust=False, min_periods=8).mean()
    ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema21 = close.ewm(span=21, adjust=False, min_periods=21).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = ema12 - ema26
    macd_signal = macd.fillna(0.0).ewm(span=9, adjust=False, min_periods=9).mean()
    output["ema8_minus_ema21_atr"] = safe_divide(
        (ema8 - ema21) * multiplier,
        atr240,
    )
    output["macd_hist_atr"] = safe_divide(
        (macd - macd_signal) * multiplier,
        atr240,
    )

    change = close.diff()
    average_gain = change.clip(lower=0).rolling(14, min_periods=14).mean()
    average_loss = (-change.clip(upper=0)).rolling(14, min_periods=14).mean()
    relative_strength = safe_divide(average_gain, average_loss)
    rsi14 = 100.0 - 100.0 / (1.0 + relative_strength)
    rsi14 = rsi14.mask((average_loss <= 0) & (average_gain > 0), 100.0)
    output["rsi14_centered"] = (rsi14 - 50.0) / 50.0

    recent_60_low = close.rolling(12, min_periods=12).min()
    recent_60_high = close.rolling(12, min_periods=12).max()
    output["range_position_60_centered"] = (
        safe_divide(close - recent_60_low, recent_60_high - recent_60_low) - 0.5
    ) * 2.0
    output["compression_30"] = safe_divide(
        (close.rolling(6, min_periods=6).max() - close.rolling(6, min_periods=6).min())
        * multiplier,
        atr240 * math.sqrt(6.0),
    )

    log_return = np.log(close).diff()
    realized_vol_30 = log_return.rolling(6, min_periods=6).std()
    realized_vol_240 = log_return.rolling(48, min_periods=48).std()
    output["realized_vol_ratio_30_240"] = safe_divide(
        realized_vol_30,
        realized_vol_240,
    )
    volume_mean = output["volume"].rolling(6, min_periods=6).mean()
    volume_std = output["volume"].rolling(6, min_periods=6).std()
    output["volume_z_30"] = safe_divide(output["volume"] - volume_mean, volume_std)
    output["trend_sign"] = np.sign(output["momentum_60_atr"])
    return output.replace([np.inf, -np.inf], np.nan)


def add_executable_outcomes(
    features: pd.DataFrame,
    m1: pd.DataFrame,
    instrument: str,
    *,
    horizon_minutes: int,
) -> pd.DataFrame:
    multiplier = pip_multiplier(instrument)
    times = m1.index.asi8
    bid_high = m1["bid_high"].to_numpy(dtype=float)
    bid_close = m1["bid_close"].to_numpy(dtype=float)
    bid_open = m1["bid_open"].to_numpy(dtype=float)
    ask_low = m1["ask_low"].to_numpy(dtype=float)
    ask_close = m1["ask_close"].to_numpy(dtype=float)
    ask_open = m1["ask_open"].to_numpy(dtype=float)

    rows: list[dict[str, float | pd.Timestamp]] = []
    horizon_ns = horizon_minutes * 60 * 1_000_000_000
    tolerance_ns = 60 * 1_000_000_000
    for decision in pd.to_datetime(features["decision_utc"], utc=True):
        decision_ns = int(decision.value)
        entry_index = int(np.searchsorted(times, decision_ns, side="left"))
        end_index = int(
            np.searchsorted(times, decision_ns + horizon_ns, side="left")
        )
        if (
            entry_index >= len(times)
            or end_index <= entry_index
            or end_index > len(times)
            or times[entry_index] - decision_ns > tolerance_ns
            or times[end_index - 1] < decision_ns + horizon_ns - tolerance_ns
        ):
            rows.append({"decision_utc": decision})
            continue
        entry_mid = (ask_open[entry_index] + bid_open[entry_index]) / 2.0
        long_mfe_price = (
            float(np.nanmax(bid_high[entry_index:end_index]))
            - ask_open[entry_index]
        )
        short_mfe_price = (
            bid_open[entry_index]
            - float(np.nanmin(ask_low[entry_index:end_index]))
        )
        long_terminal_price = bid_close[end_index - 1] - ask_open[entry_index]
        short_terminal_price = bid_open[entry_index] - ask_close[end_index - 1]
        rows.append(
            {
                "decision_utc": decision,
                "entry_mid": entry_mid,
                "long_mfe_pips": long_mfe_price * multiplier,
                "short_mfe_pips": short_mfe_price * multiplier,
                "long_terminal_pips": long_terminal_price * multiplier,
                "short_terminal_pips": short_terminal_price * multiplier,
                "long_mfe_bps": long_mfe_price / entry_mid * 10_000.0,
                "short_mfe_bps": short_mfe_price / entry_mid * 10_000.0,
                "long_terminal_bps": long_terminal_price / entry_mid * 10_000.0,
                "short_terminal_bps": short_terminal_price / entry_mid * 10_000.0,
            }
        )
    outcomes = pd.DataFrame(rows).set_index("decision_utc")
    merged = features.set_index("decision_utc").join(outcomes, how="left")
    return merged.reset_index()


def build_pair_panel(
    path: Path,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    horizon_minutes: int,
) -> pd.DataFrame:
    instrument = path.stem.removesuffix("_M1")
    m1 = load_m1(path)
    bars = completed_m5_bars(m1)
    features = build_features(instrument, bars)
    features = features.loc[
        (features["decision_utc"] >= start)
        & (features["decision_utc"] < end)
    ]
    features = add_executable_outcomes(
        features,
        m1,
        instrument,
        horizon_minutes=horizon_minutes,
    )
    required = [
        "momentum_5_atr",
        "momentum_30_atr",
        "momentum_60_atr",
        "ema8_minus_ema21_atr",
        "macd_hist_atr",
        "rsi14_centered",
        "range_position_60_centered",
        "atr15_to_atr240",
        "compression_30",
        "realized_vol_ratio_30_240",
        "volume_z_30",
        "long_mfe_pips",
        "short_mfe_pips",
    ]
    return features.dropna(subset=required)


def add_cross_pair_breadth(panel: pd.DataFrame) -> pd.DataFrame:
    trend_wide = panel.pivot_table(
        index="decision_utc",
        columns="instrument",
        values="trend_sign",
        aggfunc="last",
    )
    currencies = sorted(
        {currency for pair in trend_wide.columns for currency in pair_parts(pair)}
    )
    currency_scores = pd.DataFrame(index=trend_wide.index)
    for currency in currencies:
        contributions = []
        for instrument in trend_wide.columns:
            base, quote = pair_parts(instrument)
            if base == currency:
                contributions.append(trend_wide[instrument])
            elif quote == currency:
                contributions.append(-trend_wide[instrument])
        currency_scores[currency] = pd.concat(contributions, axis=1).mean(axis=1)

    panel = panel.copy()
    panel["cross_pair_trend_breadth"] = np.nan
    for instrument, indexes in panel.groupby("instrument").groups.items():
        base, quote = pair_parts(instrument)
        times = pd.DatetimeIndex(panel.loc[indexes, "decision_utc"])
        base_scores = currency_scores[base].reindex(times).to_numpy()
        quote_scores = currency_scores[quote].reindex(times).to_numpy()
        trend = panel.loc[indexes, "trend_sign"].to_numpy()
        panel.loc[indexes, "cross_pair_trend_breadth"] = (
            trend * (base_scores - quote_scores) / 2.0
        )
    return panel


def apply_rules(panel: pd.DataFrame) -> pd.DataFrame:
    output = panel.copy()
    trend = output["trend_sign"].replace(0.0, np.nan)
    output["aligned_momentum_5_atr"] = trend * output["momentum_5_atr"]
    output["aligned_momentum_30_atr"] = trend * output["momentum_30_atr"]
    output["aligned_momentum_60_atr"] = trend * output["momentum_60_atr"]
    output["aligned_ema_atr"] = trend * output["ema8_minus_ema21_atr"]
    output["aligned_macd_hist_atr"] = trend * output["macd_hist_atr"]
    output["aligned_rsi14"] = trend * output["rsi14_centered"]
    output["aligned_range60"] = trend * output["range_position_60_centered"]

    common_alignment = (
        (output["aligned_momentum_30_atr"] > 0)
        & (output["aligned_momentum_60_atr"] > 0)
    )
    continuation = (
        common_alignment
        & (output["aligned_momentum_30_atr"] >= 1.50)
        & (output["aligned_momentum_60_atr"] >= 2.50)
        & (output["aligned_ema_atr"] >= 0.35)
        & (output["aligned_rsi14"] >= 0.15)
        & (output["aligned_range60"] >= 0.35)
        & (output["aligned_momentum_5_atr"] <= -0.15)
        & (output["atr15_to_atr240"] >= 1.00)
    )
    exhaustion = (
        common_alignment
        & (output["aligned_momentum_30_atr"] >= 2.50)
        & (output["aligned_momentum_60_atr"] >= 2.50)
        & (output["aligned_ema_atr"] >= 0.35)
        & (output["aligned_macd_hist_atr"] >= 0.00)
        & (output["aligned_rsi14"] >= 0.20)
        & (output["aligned_range60"] >= 0.85)
    )
    event_vulnerability = (
        common_alignment
        & (output["aligned_momentum_30_atr"] >= 1.50)
        & (output["aligned_momentum_60_atr"] >= 2.50)
        & (output["aligned_ema_atr"] >= 0.35)
        & (output["aligned_rsi14"] >= 0.15)
        & (output["aligned_range60"] >= 0.35)
        & (output["cross_pair_trend_breadth"] >= 0.35)
        & (output["volume_z_30"] <= 0.25)
    )
    low_volume_range_edge = (
        (output["aligned_momentum_60_atr"] >= 2.00)
        & (output["aligned_ema_atr"] >= 0.25)
        & (output["aligned_rsi14"] >= 0.10)
        & (output["aligned_range60"] >= 0.75)
        & (output["cross_pair_trend_breadth"] >= 0.25)
        & (output["compression_30"] <= 1.00)
        & (output["volume_z_30"] <= -0.50)
    )
    output["continuation_pullback"] = continuation
    output["continuation_pullback_breadth"] = continuation & (
        output["cross_pair_trend_breadth"] >= 0.35
    )
    output["exhaustion_reversal"] = exhaustion
    output["exhaustion_reversal_breadth"] = exhaustion & (
        output["cross_pair_trend_breadth"] >= 0.35
    )
    output["event_vulnerability"] = event_vulnerability
    output["low_volume_range_edge"] = low_volume_range_edge
    output["session"] = np.select(
        [
            output["decision_utc"].dt.hour.between(0, 6),
            output["decision_utc"].dt.hour.between(7, 11),
            output["decision_utc"].dt.hour.between(12, 15),
            output["decision_utc"].dt.hour.between(16, 20),
        ],
        ["asia", "london", "overlap", "new_york"],
        default="off_hours",
    )
    output["month"] = output["decision_utc"].dt.strftime("%Y-%m")
    return output


def attach_forecast_outcomes(frame: pd.DataFrame, style: str) -> pd.DataFrame:
    output = frame.copy()
    side = output["trend_sign"].to_numpy(dtype=float)
    if style == "reversal":
        side = -side
    output["forecast_side"] = np.where(side > 0, "LONG", "SHORT")
    for unit in ["pips", "bps"]:
        output[f"forecast_mfe_{unit}"] = np.where(
            side > 0,
            output[f"long_mfe_{unit}"],
            output[f"short_mfe_{unit}"],
        )
        output[f"forecast_terminal_{unit}"] = np.where(
            side > 0,
            output[f"long_terminal_{unit}"],
            output[f"short_terminal_{unit}"],
        )
    output["any_side_mfe_bps"] = output[
        ["long_mfe_bps", "short_mfe_bps"]
    ].max(axis=1)
    return output


def independent_alerts(frame: pd.DataFrame, minimum_gap_minutes: int = 60) -> pd.DataFrame:
    kept: list[int] = []
    for _, group in frame.sort_values("decision_utc").groupby("instrument"):
        last_time: pd.Timestamp | None = None
        for index, row in group.iterrows():
            timestamp = row["decision_utc"]
            if (
                last_time is None
                or timestamp - last_time
                >= dt.timedelta(minutes=minimum_gap_minutes)
            ):
                kept.append(index)
                last_time = timestamp
    return frame.loc[kept].sort_values(["decision_utc", "instrument"])


def wilson_interval(successes: int, total: int) -> tuple[float, float]:
    if total <= 0:
        return math.nan, math.nan
    z = 1.959963984540054
    proportion = successes / total
    denominator = 1.0 + z * z / total
    center = (proportion + z * z / (2.0 * total)) / denominator
    half = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / total
            + z * z / (4.0 * total * total)
        )
        / denominator
    )
    return center - half, center + half


def summarize(frame: pd.DataFrame) -> dict[str, Any]:
    count = len(frame)
    if not count:
        return {"alerts": 0}
    positive_terminal = int((frame["forecast_terminal_bps"] > 0).sum())
    lower, upper = wilson_interval(positive_terminal, count)
    result: dict[str, Any] = {
        "alerts": count,
        "pairs": int(frame["instrument"].nunique()),
        "terminal_direction_accuracy": positive_terminal / count,
        "terminal_direction_accuracy_wilson95": [lower, upper],
        "median_mfe_pips": float(frame["forecast_mfe_pips"].median()),
        "mean_mfe_pips": float(frame["forecast_mfe_pips"].mean()),
        "median_terminal_pips": float(frame["forecast_terminal_pips"].median()),
        "mean_terminal_pips": float(frame["forecast_terminal_pips"].mean()),
        "median_mfe_bps": float(frame["forecast_mfe_bps"].median()),
        "mean_mfe_bps": float(frame["forecast_mfe_bps"].mean()),
        "median_terminal_bps": float(frame["forecast_terminal_bps"].median()),
        "mean_terminal_bps": float(frame["forecast_terminal_bps"].mean()),
        "largest_pair_share": float(
            frame["instrument"].value_counts(normalize=True).iloc[0]
        ),
    }
    for threshold in [0.0, 1.0, 5.0, 10.0]:
        key = str(threshold).replace(".", "_")
        rate = float((frame["forecast_mfe_pips"] >= threshold).mean())
        result[f"mfe_at_least_{key}_pips_rate"] = rate
        result[f"false_positive_below_{key}_pips_rate"] = 1.0 - rate
    for threshold in [1.0, 2.5, 5.0, 10.0]:
        key = str(threshold).replace(".", "_")
        rate = float((frame["forecast_mfe_bps"] >= threshold).mean())
        result[f"mfe_at_least_{key}_bps_rate"] = rate
        result[f"false_positive_below_{key}_bps_rate"] = 1.0 - rate
    return result


def period_name(
    timestamp: pd.Timestamp,
    validation_start: pd.Timestamp,
    final_start: pd.Timestamp,
) -> str:
    if timestamp < validation_start:
        return "development"
    if timestamp < final_start:
        return "validation"
    return "final"


def evaluate_rules(
    panel: pd.DataFrame,
    *,
    validation_start: pd.Timestamp,
    final_start: pd.Timestamp,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    all_metrics: dict[str, Any] = {}
    alert_frames: list[pd.DataFrame] = []
    breakdown_rows: list[dict[str, Any]] = []
    for definition in RULES:
        styled = attach_forecast_outcomes(panel, definition.forecast_style)
        ordinary = independent_alerts(styled)
        selected = styled.loc[styled[definition.name]].copy()
        selected = independent_alerts(selected)
        selected["rule"] = definition.name
        selected["period"] = selected["decision_utc"].map(
            lambda value: period_name(value, validation_start, final_start)
        )
        alert_frames.append(selected)

        rule_metrics: dict[str, Any] = {
            "definition": asdict(definition),
            "ordinary_timestamp_baseline": summarize(ordinary),
            "ordinary_timestamp_baseline_periods": {},
            "all_periods": summarize(selected),
            "periods": {},
        }
        for period in ["development", "validation", "final"]:
            subset = selected.loc[selected["period"].eq(period)]
            rule_metrics["periods"][period] = summarize(subset)
            ordinary_subset = ordinary.loc[
                ordinary["decision_utc"].map(
                    lambda value: period_name(
                        value, validation_start, final_start
                    )
                ).eq(period)
            ]
            rule_metrics["ordinary_timestamp_baseline_periods"][period] = summarize(
                ordinary_subset
            )
        baseline_accuracy = rule_metrics["ordinary_timestamp_baseline"].get(
            "terminal_direction_accuracy", math.nan
        )
        rule_accuracy = rule_metrics["all_periods"].get(
            "terminal_direction_accuracy", math.nan
        )
        rule_metrics["terminal_accuracy_lift_vs_ordinary"] = (
            rule_accuracy - baseline_accuracy
            if math.isfinite(rule_accuracy) and math.isfinite(baseline_accuracy)
            else math.nan
        )
        all_metrics[definition.name] = rule_metrics

        for dimension in ["instrument", "session", "month", "period"]:
            for value, subset in selected.groupby(dimension):
                metrics = summarize(subset)
                breakdown_rows.append(
                    {
                        "rule": definition.name,
                        "dimension": dimension,
                        "value": value,
                        **metrics,
                    }
                )
    alerts = (
        pd.concat(alert_frames, ignore_index=True)
        if alert_frames
        else pd.DataFrame()
    )
    breakdown = pd.DataFrame(breakdown_rows)
    return all_metrics, alerts, breakdown


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def percent(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return "n/a" if not math.isfinite(numeric) else f"{numeric * 100.0:.1f}%"


def number(value: Any, decimals: int = 2) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return "n/a" if not math.isfinite(numeric) else f"{numeric:.{decimals}f}"


def write_report(
    payload: dict[str, Any],
    report_root: Path,
    alerts: pd.DataFrame,
    breakdown: pd.DataFrame,
) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    json_path = report_root / "TECHNICAL_SETUP_CONTROL_STUDY.json"
    markdown_path = report_root / "TECHNICAL_SETUP_CONTROL_STUDY.md"
    alerts_path = report_root / "technical_setup_independent_alerts.csv"
    breakdown_path = report_root / "technical_setup_breakdowns.csv"

    json_path.write_text(
        json.dumps(json_ready(payload), indent=2),
        encoding="utf-8",
    )
    alert_columns = [
        column for column in ALERT_EXPORT_COLUMNS if column in alerts.columns
    ]
    alerts.loc[:, alert_columns].to_csv(alerts_path, index=False)
    breakdown.to_csv(breakdown_path, index=False)

    lines = [
        "# Technical Setup Control-Window Study",
        "",
        "## Scope",
        "",
        f"- Pipeline: `{payload['pipeline_version']}`.",
        (
            f"- Data: `{payload['data_start_utc']}` through "
            f"`{payload['data_end_utc']}`, {payload['instruments']} instruments."
        ),
        (
            f"- Observations: {payload['eligible_m5_rows']:,} causal completed-M5 "
            "decision rows."
        ),
        (
            "- Entry/outcome: next available M1 open after the completed M5 bar; "
            "native bid/ask MFE and terminal return over 60 wall-clock minutes."
        ),
        (
            "- Alert independence: at least 60 minutes between retained alerts "
            "for the same rule and instrument."
        ),
        "- Spread is included. Slippage and financing are excluded.",
        (
            "- These fixed rules were motivated by hindsight-selected July moves. "
            "Earlier-date controls reduce narrative bias but do not make this a "
            "prospective validation."
        ),
        "",
        "## Chronology",
        "",
        f"- Development: before `{payload['validation_start_utc']}`.",
        (
            f"- Validation: `{payload['validation_start_utc']}` through just before "
            f"`{payload['final_start_utc']}`."
        ),
        f"- Latest segment: `{payload['final_start_utc']}` onward.",
        "- Thresholds were not optimized on any segment in this run.",
        "",
        "## Results",
        "",
        (
            "| Setup | Alerts | Terminal accuracy | Ordinary baseline | "
            "Accuracy lift | >=5 bps MFE | Ordinary >=5 bps | MFE lift | "
            ">=10 bps MFE | Median terminal bps | Largest-pair share |"
        ),
        (
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | "
            "---: | ---: | ---: |"
        ),
    ]
    for name, result in payload["rules"].items():
        total = result["all_periods"]
        baseline = result["ordinary_timestamp_baseline"]
        lines.append(
            "| "
            + " | ".join(
                [
                    name,
                    str(total.get("alerts", 0)),
                    percent(total.get("terminal_direction_accuracy")),
                    percent(baseline.get("terminal_direction_accuracy")),
                    percent(result.get("terminal_accuracy_lift_vs_ordinary")),
                    percent(total.get("mfe_at_least_5_0_bps_rate")),
                    percent(baseline.get("mfe_at_least_5_0_bps_rate")),
                    percent(
                        total.get("mfe_at_least_5_0_bps_rate", math.nan)
                        - baseline.get("mfe_at_least_5_0_bps_rate", math.nan)
                    ),
                    percent(total.get("mfe_at_least_10_0_bps_rate")),
                    number(total.get("median_terminal_bps")),
                    percent(total.get("largest_pair_share")),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Segment Stability",
            "",
            "| Setup | Segment | Alerts | Terminal accuracy | >=5 bps MFE | Median terminal bps |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for name, result in payload["rules"].items():
        for period, metrics in result["periods"].items():
            lines.append(
                "| "
                + " | ".join(
                    [
                        name,
                        period,
                        str(metrics.get("alerts", 0)),
                        percent(metrics.get("terminal_direction_accuracy")),
                        percent(metrics.get("mfe_at_least_5_0_bps_rate")),
                        number(metrics.get("median_terminal_bps")),
                    ]
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## Interpretation Guardrails",
            "",
            (
                "- `Terminal accuracy` asks whether the forecast side was profitable "
                "after spread exactly 60 minutes later."
            ),
            (
                "- `MFE` is the best executable price available during the hour. "
                "It is useful for movement detection but is not a realizable exit."
            ),
            (
                "- A high MFE rate with negative terminal returns indicates temporary "
                "movement, poor hold-to-60 behavior, or both."
            ),
            (
                "- A rule can therefore be retained as a continuous movement/ranking "
                "feature even when it fails as a standalone directional strategy."
            ),
            (
                "- Pair/session breakdowns and every retained independent alert are "
                "provided in the accompanying CSV files."
            ),
            "",
            "## Verdict",
            "",
            payload["verdict"],
            "",
            "No execution settings, credentials, account files, or bots were changed.",
        ]
    )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_verdict(rule_metrics: dict[str, Any]) -> str:
    stable: list[str] = []
    weak: list[str] = []
    for name, result in rule_metrics.items():
        validation = result["periods"]["validation"]
        final = result["periods"]["final"]
        validation_baseline = result["ordinary_timestamp_baseline_periods"][
            "validation"
        ]
        final_baseline = result["ordinary_timestamp_baseline_periods"]["final"]
        enough = (
            validation.get("alerts", 0) >= 20
            and final.get("alerts", 0) >= 20
        )
        val_edge = validation.get(
            "terminal_direction_accuracy", 0.0
        ) > validation_baseline.get(
            "terminal_direction_accuracy", 1.0
        )
        final_edge = final.get(
            "terminal_direction_accuracy", 0.0
        ) > final_baseline.get(
            "terminal_direction_accuracy", 1.0
        )
        final_positive = final.get("median_terminal_bps", -1.0) > 0.0
        if enough and val_edge and final_edge and final_positive:
            stable.append(name)
        else:
            weak.append(name)
    if stable:
        return (
            "DESCRIPTIVE PASS: "
            + ", ".join(stable)
            + " retained positive median terminal performance and exceeded its "
            "ordinary-timestamp side baseline in both later segments. This is "
            "candidate evidence only because the rule concepts came from "
            "hindsight-selected moves."
        )
    return (
        "FAIL: none of the case-motivated technical rules produced a sufficiently "
        "sampled, positive-median, segment-stable improvement over its corresponding "
        "ordinary-timestamp directional baseline. The selected large moves therefore "
        "do not establish a reusable standalone technical edge."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--start", default="2026-06-08")
    parser.add_argument("--end", default="2026-07-23")
    parser.add_argument("--validation-start", default="2026-06-29")
    parser.add_argument("--final-start", default="2026-07-13")
    parser.add_argument("--horizon-minutes", type=int, default=60)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC")
    validation_start = pd.Timestamp(args.validation_start, tz="UTC")
    final_start = pd.Timestamp(args.final_start, tz="UTC")
    paths = sorted(args.data_root.glob("*_M1.parquet"))
    if not paths:
        raise FileNotFoundError(f"No M1 parquet files under {args.data_root}")

    frames: list[pd.DataFrame] = []
    for index, path in enumerate(paths, 1):
        print(f"[{index:02d}/{len(paths):02d}] {path.stem}", flush=True)
        pair_frame = build_pair_panel(
            path,
            start=start,
            end=end,
            horizon_minutes=args.horizon_minutes,
        )
        frames.append(pair_frame)
    panel = pd.concat(frames, ignore_index=True)
    panel = add_cross_pair_breadth(panel)
    panel = apply_rules(panel)
    rule_metrics, alerts, breakdown = evaluate_rules(
        panel,
        validation_start=validation_start,
        final_start=final_start,
    )
    payload = {
        "pipeline_version": PIPELINE_VERSION,
        "source": str(args.data_root.resolve()),
        "data_start_utc": panel["decision_utc"].min().isoformat(),
        "data_end_utc": panel["decision_utc"].max().isoformat(),
        "validation_start_utc": validation_start.isoformat(),
        "final_start_utc": final_start.isoformat(),
        "horizon_minutes": args.horizon_minutes,
        "instruments": int(panel["instrument"].nunique()),
        "eligible_m5_rows": len(panel),
        "rules": rule_metrics,
    }
    payload["verdict"] = build_verdict(rule_metrics)
    write_report(payload, args.report_root, alerts, breakdown)
    print(json.dumps(json_ready(payload["verdict"])))
    print(f"report_root={args.report_root.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
