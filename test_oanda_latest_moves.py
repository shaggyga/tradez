from __future__ import annotations

import datetime as dt
from unittest.mock import Mock, patch

import requests

from oanda_latest_moves import (
    _get_open_candle,
    build_payload,
    cached_open_payloads,
    directional_move_legs,
    latest_weekly_open,
    movement_row,
)


UTC = dt.timezone.utc


def _candles(open_time: str, bid: float, ask: float) -> dict:
    return {
        "open": {
            "time": open_time,
            "complete": True,
            "bid": {"o": str(bid)},
            "ask": {"o": str(ask)},
        },
        "recent": [
            {"time": "2026-08-17T15:00:00Z", "complete": True, "mid": {"c": "1.1010"}},
            {"time": "2026-08-17T15:45:00Z", "complete": True, "mid": {"c": "1.1020"}},
        ],
    }


def test_latest_weekly_open_respects_new_york_dst() -> None:
    assert latest_weekly_open(dt.datetime(2026, 8, 17, 17, tzinfo=UTC)) == dt.datetime(
        2026, 8, 16, 21, tzinfo=UTC
    )
    assert latest_weekly_open(dt.datetime(2026, 1, 5, 17, tzinfo=UTC)) == dt.datetime(
        2026, 1, 4, 22, tzinfo=UTC
    )


def test_movement_row_separates_mid_move_from_executable_cost() -> None:
    market_open = dt.datetime(2026, 8, 16, 21, tzinfo=UTC)
    row = movement_row(
        instrument="EUR_USD",
        pip=0.0001,
        market_open=market_open,
        price={
            "bid": 1.1014,
            "ask": 1.1016,
            "time": "2026-08-17T16:00:00Z",
            "tradeable": True,
        },
        candle_payload=_candles("2026-08-16T21:00:00Z", 1.1000, 1.1002),
    )
    assert row is not None
    assert row["since_open_move_pips"] == 14.0
    assert row["long_net_pips"] == 12.0
    assert row["best_executable_side"] == "long"
    assert row["movement_cleared_round_trip_spread"] is True


def test_wide_spread_move_does_not_become_executable_winner() -> None:
    market_open = dt.datetime(2026, 8, 16, 21, tzinfo=UTC)
    price = {
        "bid": 1.1008,
        "ask": 1.1022,
        "time": "2026-08-17T16:00:00Z",
        "tradeable": True,
    }
    row = movement_row(
        instrument="EUR_USD",
        pip=0.0001,
        market_open=market_open,
        price=price,
        candle_payload=_candles("2026-08-16T21:00:00Z", 1.1000, 1.1010),
    )
    assert row is not None
    assert row["since_open_abs_pips"] == 10.0
    assert row["best_executable_net_pips"] == -2.0
    assert row["movement_cleared_round_trip_spread"] is False


def test_build_payload_has_separate_normalized_and_executable_rankings() -> None:
    now = dt.datetime(2026, 8, 17, 16, tzinfo=UTC)
    market_open = dt.datetime(2026, 8, 16, 21, tzinfo=UTC)
    prices = {
        "EUR_USD": {"bid": 1.1014, "ask": 1.1016, "time": now.isoformat(), "tradeable": True},
        "USD_ZAR": {"bid": 18.01, "ask": 18.04, "time": now.isoformat(), "tradeable": True},
    }
    candles = {
        "EUR_USD": _candles("2026-08-16T21:00:00Z", 1.1000, 1.1002),
        "USD_ZAR": _candles("2026-08-16T21:00:00Z", 18.00, 18.03),
    }
    payload = build_payload(
        now=now,
        market_open=market_open,
        prices=prices,
        candles=candles,
        pip_by_instrument={"EUR_USD": 0.0001, "USD_ZAR": 0.0001},
        failures=[],
    )
    assert payload["instrument_count"] == 2
    assert payload["round_trip_cost_clear_count"] == 1
    assert payload["rankings"]["since_open_executable"][0]["instrument"] == "EUR_USD"


def test_delayed_open_is_retained_but_excluded_from_rankings() -> None:
    now = dt.datetime(2026, 8, 17, 16, tzinfo=UTC)
    market_open = dt.datetime(2026, 8, 16, 21, tzinfo=UTC)
    payload = build_payload(
        now=now,
        market_open=market_open,
        prices={"USD_TRY": {"bid": 48.0, "ask": 48.1, "time": now.isoformat(), "tradeable": True}},
        candles={"USD_TRY": _candles("2026-08-17T05:55:00Z", 47.9, 48.0)},
        pip_by_instrument={"USD_TRY": 0.0001},
        failures=[],
    )
    assert payload["instrument_count"] == 1
    assert payload["exact_market_open_count"] == 0
    assert payload["rankings"]["since_open_normalized"] == []


def test_cached_open_requires_same_weekly_open(tmp_path) -> None:
    path = tmp_path / "moves.json"
    path.write_text(
        '{"market_open_utc":"2026-08-16T21:00:00+00:00","rows":['
        '{"instrument":"EUR_USD","first_candle_utc":"2026-08-16T21:00:00+00:00",'
        '"open_bid":1.1,"open_ask":1.1002}]}',
        encoding="utf-8",
    )
    expected = dt.datetime(2026, 8, 16, 21, tzinfo=UTC)
    assert "EUR_USD" in cached_open_payloads(path, market_open=expected)
    assert cached_open_payloads(
        path,
        market_open=expected - dt.timedelta(days=7),
    ) == {}


def test_open_candle_retries_transient_transport_failure() -> None:
    response = Mock()
    response.ok = True
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "candles": [
            {
                "time": "2026-08-16T21:00:00Z",
                "complete": True,
                "bid": {"o": "1.1"},
                "ask": {"o": "1.1002"},
            }
        ]
    }
    with patch(
        "oanda_latest_moves.requests.get",
        side_effect=[requests.exceptions.SSLError("transient"), response],
    ) as get, patch("oanda_latest_moves.time.sleep"):
        payload = _get_open_candle(
            api_key="practice-token",
            instrument="EUR_USD",
            market_open=dt.datetime(2026, 8, 16, 21, tzinfo=UTC),
        )
    assert get.call_count == 2
    assert payload["open"]["time"] == "2026-08-16T21:00:00Z"


def _path_row(minute: int, mid: float, *, spread: float = 0.0002) -> dict:
    return {
        "time": f"2026-08-18T12:{minute:02d}:00Z",
        "complete": True,
        "bid": {"c": str(mid - spread / 2.0)},
        "ask": {"c": str(mid + spread / 2.0)},
        "mid": {"c": str(mid)},
    }


def test_directional_move_legs_preserve_rally_and_reversal_not_endpoint_only() -> None:
    rows = [
        _path_row(0, 1.00010),
        _path_row(1, 1.00015),
        _path_row(2, 1.00010),
        _path_row(3, 1.00015),
        _path_row(4, 1.00010),
        _path_row(5, 1.00060),
        _path_row(6, 1.00210),
        _path_row(7, 1.00150),
        _path_row(8, 1.00060),
    ]
    legs, diagnostics = directional_move_legs(
        rows,
        instrument="EUR_USD",
        pip=0.0001,
    )

    assert diagnostics["reversal_threshold_pips"] == 8.25
    assert [leg["direction"] for leg in legs] == ["increase", "decrease"]
    assert legs[0]["state"] == "confirmed"
    assert legs[0]["gross_pips"] == 20.0
    assert legs[0]["executable_net_pips"] == 18.0
    assert legs[0]["signed_move_bps"] > 0
    assert legs[0]["velocity_bps_per_hour"] > 0
    assert legs[1]["state"] == "active"
    assert legs[1]["gross_pips"] == 15.0
    assert legs[1]["executable_net_pips"] == 13.0
    assert legs[1]["signed_move_bps"] < 0
    assert legs[1]["velocity_bps_per_hour"] < 0
    assert len(legs[1]["chart_points"]) >= 2


def test_directional_move_legs_reject_spread_sized_noise() -> None:
    rows = [
        _path_row(0, 1.00010),
        _path_row(1, 1.00020),
        _path_row(2, 1.00005),
        _path_row(3, 1.00025),
        _path_row(4, 1.00010),
    ]
    legs, diagnostics = directional_move_legs(
        rows,
        instrument="EUR_USD",
        pip=0.0001,
    )

    assert diagnostics["reversal_threshold_pips"] >= 3.0
    assert legs == []


def test_live_velocity_excludes_active_leg_with_stale_candle_endpoint() -> None:
    now = dt.datetime(2026, 8, 18, 12, 30, tzinfo=UTC)
    market_open = dt.datetime(2026, 8, 16, 21, tzinfo=UTC)
    recent = [
        _path_row(0, 1.00010),
        _path_row(1, 1.00015),
        _path_row(2, 1.00010),
        _path_row(3, 1.00015),
        _path_row(4, 1.00010),
        _path_row(5, 1.00060),
        _path_row(6, 1.00210),
    ]
    payload = build_payload(
        now=now,
        market_open=market_open,
        prices={
            "EUR_USD": {
                "bid": 1.0020,
                "ask": 1.0022,
                "time": now.isoformat(),
                "tradeable": True,
            }
        },
        candles={
            "EUR_USD": {
                "open": {
                    "time": market_open.isoformat(),
                    "complete": True,
                    "bid": {"o": "1.0000"},
                    "ask": {"o": "1.0002"},
                },
                "recent": recent,
            }
        },
        pip_by_instrument={"EUR_USD": 0.0001},
        failures=[],
    )

    assert payload["directional_legs"][-1]["state"] == "active"
    assert payload["directional_legs"][-1]["live_velocity_fresh"] is False
    assert payload["directional_legs"][-1]["end_age_sec"] == 1380.0  # M1 close is available at bar end, not bar start.
    assert payload["rankings"]["live_velocity"] == []
