from __future__ import annotations

import datetime as dt

from trad.oanda_cftc_positioning_historical_discovery import (
    RULES,
    bh_qvalues,
    conservative_availability,
    evaluate,
    positioning_rows,
    summarise,
)


def _raw(day: str, lev_long: int, lev_short: int) -> dict:
    return {
        "report_date_as_yyyy_mm_dd": day,
        "open_interest_all": "1000",
        "lev_money_positions_long": str(lev_long),
        "lev_money_positions_short": str(lev_short),
        "asset_mgr_positions_long": "400",
        "asset_mgr_positions_short": "200",
        "dealer_positions_long_all": "100",
        "dealer_positions_short_all": "300",
    }


def test_availability_is_lagged_past_ordinary_friday_release() -> None:
    report = dt.datetime(2026, 8, 4, tzinfo=dt.timezone.utc)
    available = conservative_availability(report)
    assert available == dt.datetime(2026, 8, 11, tzinfo=dt.timezone.utc)
    assert available.weekday() == 1


def test_position_changes_use_only_prior_report() -> None:
    rows = positioning_rows(
        "EUR",
        [_raw("2026-07-28T00:00:00.000", 200, 100), _raw("2026-08-04T00:00:00.000", 230, 100)],
    )
    assert rows[0]["leveraged_change"] is None
    assert abs(rows[1]["leveraged_change"] - 0.03) < 1e-12
    assert RULES["leveraged_change_trend"](rows[1]) == 1


def test_executable_bid_ask_cost_is_charged_for_both_orientations() -> None:
    candles = [
        {"time": dt.datetime(2026, 8, 11, tzinfo=dt.timezone.utc), "bid_o": 1.0999, "ask_o": 1.1001, "mid_o": 1.1, "bid_h": 1.1009, "bid_l": 1.0990, "ask_h": 1.1011, "ask_l": 1.0992},
        {"time": dt.datetime(2026, 8, 12, tzinfo=dt.timezone.utc), "bid_o": 1.1009, "ask_o": 1.1011, "mid_o": 1.101, "bid_h": 1.1019, "bid_l": 1.1000, "ask_h": 1.1021, "ask_l": 1.1002},
    ]
    position = {"currency": "EUR", "availability_utc": "2026-08-11T00:00:00+00:00", "report_date": "2026-08-04"}
    long_row = evaluate(position, "EUR_USD", candles, "test", 1, 1)
    assert long_row is not None
    assert abs(long_row["gross_pips"] - 10.0) < 1e-9
    assert abs(long_row["net_pips"] - 8.0) < 1e-9
    assert abs(long_row["net_bps"] - (8.0 * 0.0001 / 1.1 * 10000.0)) < 1e-9

    inverse_position = {"currency": "USD", "availability_utc": "2026-08-11T00:00:00+00:00", "report_date": "2026-08-04"}
    short_row = evaluate(inverse_position, "EUR_USD", candles, "test", 1, 1)
    assert short_row is not None
    assert short_row["pair_direction"] == -1
    assert abs(short_row["net_pips"] + 12.0) < 1e-9


def test_bh_qvalues_are_monotone_in_rank() -> None:
    qvalues = bh_qvalues([0.001, 0.02, 0.8, 0.04])
    assert qvalues[0] <= qvalues[1] <= qvalues[3] <= qvalues[2]


def test_split_summary_does_not_claim_cross_period_confirmation() -> None:
    rows = [
        {
            "report_date": f"2026-01-{day:02d}",
            "rule": "r",
            "horizon_trading_days": 1,
            "currency": "EUR",
            "net_bps": 2.0,
            "net_pips": 2.0,
            "win_after_cost": True,
            "entry_spread_pips": 1.0,
        }
        for day in range(1, 29)
    ]
    summary = summarise(rows, "holdout")[0]
    assert "split_local_candidate" in summary
    assert "discovery_candidate" not in summary
