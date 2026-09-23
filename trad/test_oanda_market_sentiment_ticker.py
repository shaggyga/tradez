from __future__ import annotations

import math

import oanda_market_sentiment_ticker as ticker


def quote(instrument: str, minute: int, mid: float, pip: float = 0.0001) -> dict:
    spread = pip
    return {
        "instrument": instrument,
        "minute_epoch": minute,
        "first_epoch": minute,
        "last_epoch": minute + 59,
        "open_bid": mid - spread / 2,
        "open_ask": mid + spread / 2,
        "close_bid": mid - spread / 2,
        "close_ask": mid + spread / 2,
        "high_mid": mid,
        "low_mid": mid,
        "pip": pip,
    }


def moved(start: float, bps: float) -> float:
    return start * math.exp(bps / 10000.0)


def test_currency_solver_recovers_relative_strength_order() -> None:
    start = 1_000
    end = start + 60 * 60
    rows = [
        quote("EUR_USD", start, 1.10),
        quote("EUR_USD", end, moved(1.10, 10.0)),
        quote("GBP_USD", start, 1.25),
        quote("GBP_USD", end, moved(1.25, 5.0)),
        quote("EUR_GBP", start, 0.88),
        quote("EUR_GBP", end, moved(0.88, 5.0)),
    ]
    surface = ticker.build_market_surface(rows, end, now_epoch=end + 30)
    strengths = surface["horizons"]["60"]["currency_strength_bps"]

    assert strengths["EUR"] > strengths["GBP"] > strengths["USD"]
    assert surface["execution_eligible"] is False
    assert surface["fresh"] is True


def test_risk_off_theme_detects_haven_over_risk_currencies() -> None:
    strengths = {
        "JPY": 8.0,
        "CHF": 6.0,
        "AUD": -7.0,
        "NZD": -6.0,
        "CAD": -2.0,
        "NOK": -3.0,
        "USD": 1.0,
    }
    theme = ticker.theme_state(strengths)

    assert theme["regime"] == "RISK_OFF"
    assert theme["standardized"]["risk_off"] > 1.0


def test_pair_windows_uses_completed_bid_ask_bars() -> None:
    rows = [
        quote("EUR_USD", 1_000, 1.1000),
        quote("EUR_USD", 1_300, 1.1010),
    ]
    result = ticker.pair_windows(rows, 1_300)["EUR_USD"]

    assert result["windows"]["5"]["return_pips"] == 10.0
    assert result["spread_bps"] > 0.0


def test_pair_windows_does_not_use_future_intraminute_close() -> None:
    old = quote("EUR_USD", 1_000, 1.1000)
    current = quote("EUR_USD", 1_300, 1.1010)
    current.update(
        {
            "first_epoch": 1_305,
            "last_epoch": 1_350,
            "open_bid": 1.10095,
            "open_ask": 1.10105,
            "close_bid": 1.10495,
            "close_ask": 1.10505,
        }
    )

    before_first = ticker.pair_windows([old, current], 1_304)["EUR_USD"]
    after_first = ticker.pair_windows([old, current], 1_320)["EUR_USD"]
    after_close = ticker.pair_windows([old, current], 1_350)["EUR_USD"]

    assert before_first["latest_epoch"] == 1_059
    assert after_first["latest_epoch"] == 1_305
    assert after_first["mid"] == 1.101
    assert after_close["latest_epoch"] == 1_350
    assert after_close["mid"] == 1.105


def test_atomic_quote_snapshots_build_and_merge_minute_history() -> None:
    first = ticker.quote_snapshot_rows(
        {
            "generated_utc": "2026-07-29T14:00:30Z",
            "quotes": {
                "EUR_USD": {
                    "bid": 1.1000,
                    "ask": 1.1002,
                    "pip": 0.0001,
                    "time": "2026-07-29T14:00:30Z",
                }
            },
        }
    )
    second = ticker.quote_snapshot_rows(
        {
            "generated_utc": "2026-07-29T14:00:50Z",
            "quotes": {
                "EUR_USD": {
                    "bid": 1.1004,
                    "ask": 1.1006,
                    "pip": 0.0001,
                    "time": "2026-07-29T14:00:50Z",
                }
            },
        }
    )
    merged = ticker.merge_quote_history(first, second)

    assert len(merged) == 1
    assert merged[0]["open_bid"] == 1.1000
    assert merged[0]["close_bid"] == 1.1004
    assert merged[0]["high_mid"] == 1.1005
