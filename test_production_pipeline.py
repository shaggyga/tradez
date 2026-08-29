from __future__ import annotations

import pandas as pd
import pytest

import oanda_event_meta_model_pipeline as event_pipeline
import oanda_depth_feature_collector as depth_collector


def test_fred_macro_history_fails_closed_without_vintage_data() -> None:
    with pytest.raises(RuntimeError, match="vintage-causal"):
        event_pipeline.fred_macro_frame()


def test_path_label_uses_stop_first_when_stop_and_target_share_bar() -> None:
    timestamp = pd.Timestamp("2026-01-05T12:00:00Z")
    candles = pd.DataFrame([
        {
            "dt": timestamp,
            "bid_close": 1.0000,
            "ask_close": 1.0002,
            "bid_high": 1.0000,
            "bid_low": 1.0000,
            "ask_high": 1.0002,
            "ask_low": 1.0002,
        },
        *[
            {
                "dt": timestamp + pd.Timedelta(minutes=minute),
                "bid_close": 1.0000,
                "ask_close": 1.0002,
                "bid_high": 1.0030 if minute == 1 else 1.0000,
                "bid_low": 0.9970 if minute == 1 else 1.0000,
                "ask_high": 1.0032 if minute == 1 else 1.0002,
                "ask_low": 0.9972 if minute == 1 else 1.0002,
            }
            for minute in range(1, event_pipeline.MAX_HOLD_MINUTES + 1)
        ],
    ])
    row = pd.Series({
        "time_utc": timestamp,
        "instrument": "EUR_USD",
        "direction": "LONG",
        "stop_pips": 14.0,
        "target_pips": 14.0,
    })

    label = event_pipeline._path_label(row, candles)

    assert label is not None
    assert label["exit_reason"] == "stop"
    assert label["target_before_stop"] == 0
    assert label["realized_pips"] == -14.0


def test_rolling_folds_purge_full_holding_window() -> None:
    frame = pd.DataFrame({
        "time_utc": pd.date_range("2025-01-01", periods=50_000, freq="5min", tz="UTC"),
    })

    folds = event_pipeline.rolling_folds(frame)

    assert folds
    for train, test in folds:
        assert train["time_utc"].max() < (
            test["time_utc"].min() - pd.Timedelta(minutes=event_pipeline.PURGE_MINUTES)
        )


def test_depth_price_row_calculates_imbalance_and_microprice() -> None:
    price = {
        "time": "2026-01-05T12:00:00Z",
        "instrument": "EUR_USD",
        "tradeable": True,
        "status": "tradeable",
        "bids": [
            {"price": "1.1000", "liquidity": 2_000_000},
            {"price": "1.0999", "liquidity": 1_000_000},
        ],
        "asks": [
            {"price": "1.1002", "liquidity": 1_000_000},
            {"price": "1.1003", "liquidity": 1_000_000},
        ],
        "closeoutBid": "1.0998",
        "closeoutAsk": "1.1004",
        "quoteHomeConversionFactors": {
            "positiveUnits": "1.0",
            "negativeUnits": "1.0",
        },
    }

    row = depth_collector.price_row(price, pd.Timestamp("2026-01-05T12:00:01Z").to_pydatetime())

    assert row is not None
    assert row["depth_imbalance"] == 0.2
    assert row["microprice"] > row["mid"]
    assert round(row["spread_pips"], 6) == 2.0
    assert round(row["closeout_spread_pips"], 6) == 6.0
