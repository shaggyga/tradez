from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from fresh_m1_intrahour.src.technical_setup_control_study import (
    add_executable_outcomes,
    apply_rules,
    build_features,
    completed_m5_bars,
    independent_alerts,
)


def synthetic_m1(periods: int = 420) -> pd.DataFrame:
    index = pd.date_range("2026-01-05", periods=periods, freq="1min", tz="UTC")
    mid = 1.10 + np.arange(periods) * 0.00001
    half_spread = 0.00005
    return pd.DataFrame(
        {
            "open": mid,
            "high": mid + 0.00002,
            "low": mid - 0.00002,
            "close": mid + 0.00001,
            "volume": 100.0 + np.arange(periods) % 11,
            "bid_open": mid - half_spread,
            "bid_high": mid + 0.00002 - half_spread,
            "bid_low": mid - 0.00002 - half_spread,
            "bid_close": mid + 0.00001 - half_spread,
            "ask_open": mid + half_spread,
            "ask_high": mid + 0.00002 + half_spread,
            "ask_low": mid - 0.00002 + half_spread,
            "ask_close": mid + 0.00001 + half_spread,
            "spread_pips": 1.0,
        },
        index=index,
    )


def test_outcome_enters_after_completed_m5_and_pays_spread() -> None:
    m1 = synthetic_m1()
    bars = completed_m5_bars(m1)
    features = build_features("EUR_USD", bars).dropna().iloc[[0]]
    result = add_executable_outcomes(
        features,
        m1,
        "EUR_USD",
        horizon_minutes=60,
    ).iloc[0]

    decision = pd.Timestamp(result["decision_utc"])
    entry = m1.loc[decision]
    expected_long_terminal = (
        m1.loc[decision + dt.timedelta(minutes=59), "bid_close"]
        - entry["ask_open"]
    ) * 10_000.0
    assert result["long_terminal_pips"] == expected_long_terminal
    assert decision == features.iloc[0]["decision_utc"]


def test_continuation_and_reversal_use_opposite_sides() -> None:
    row = {
        "decision_utc": pd.Timestamp("2026-01-05T12:00:00Z"),
        "instrument": "EUR_USD",
        "trend_sign": 1.0,
        "momentum_5_atr": -0.5,
        "momentum_30_atr": 3.0,
        "momentum_60_atr": 4.0,
        "ema8_minus_ema21_atr": 1.0,
        "macd_hist_atr": 0.2,
        "rsi14_centered": 0.5,
        "range_position_60_centered": 1.0,
        "atr15_to_atr240": 1.5,
        "compression_30": 0.8,
        "realized_vol_ratio_30_240": 1.2,
        "volume_z_30": -1.0,
        "cross_pair_trend_breadth": 0.8,
    }
    result = apply_rules(pd.DataFrame([row])).iloc[0]
    assert bool(result["continuation_pullback"])
    assert bool(result["exhaustion_reversal"])


def test_independent_alerts_enforce_one_hour_per_pair() -> None:
    frame = pd.DataFrame(
        {
            "decision_utc": pd.to_datetime(
                [
                    "2026-01-05T12:00:00Z",
                    "2026-01-05T12:30:00Z",
                    "2026-01-05T13:00:00Z",
                ]
            ),
            "instrument": ["EUR_USD"] * 3,
        }
    )
    result = independent_alerts(frame)
    assert result["decision_utc"].tolist() == [
        pd.Timestamp("2026-01-05T12:00:00Z"),
        pd.Timestamp("2026-01-05T13:00:00Z"),
    ]
