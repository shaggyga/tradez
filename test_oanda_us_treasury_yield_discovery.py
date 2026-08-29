from __future__ import annotations

from trad.oanda_us_treasury_yield_discovery import (
    apply_cross_period_gate,
    parse_yield_xml,
    rate_direction,
    yield_features,
)


def test_parse_official_treasury_xml_and_lag_features() -> None:
    xml = b'''<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
      <entry><updated>2026-08-08T12:00:00Z</updated><content><m:properties><d:NEW_DATE>2026-08-04T00:00:00</d:NEW_DATE><d:BC_2YEAR>3.50</d:BC_2YEAR><d:BC_10YEAR>4.20</d:BC_10YEAR></m:properties></content></entry>
      <entry><updated>2026-08-08T12:00:00Z</updated><content><m:properties><d:NEW_DATE>2026-08-05T00:00:00</d:NEW_DATE><d:BC_2YEAR>3.54</d:BC_2YEAR><d:BC_10YEAR>4.22</d:BC_10YEAR></m:properties></content></entry>
    </feed>'''
    rows = yield_features(parse_yield_xml(xml))
    assert len(rows) == 2
    assert rows[1]["availability_utc"] == "2026-08-06T00:00:00+00:00"
    assert abs(rows[1]["two_year_change_bps"] - 4.0) < 1e-9
    assert abs(rows[1]["ten_year_change_bps"] - 2.0) < 1e-9
    assert abs(rows[1]["front_end_relative_change_bps"] - 2.0) < 1e-9
    assert rate_direction(rows[1], "two_year_change_usd_trend") == 1
    assert rate_direction(rows[1], "ten_year_change_usd_trend") == 0


def test_cross_period_gate_requires_prior_stability() -> None:
    base = {
        "rule": "r",
        "horizon_trading_days": 1,
        "mean_without_best_currency_bps": 1.0,
        "mean_without_best_week_bps": 1.0,
        "split_local_candidate": False,
    }
    rows = [
        {**base, "split": "discovery", "mean_net_bps": -0.1},
        {**base, "split": "holdout", "mean_net_bps": 2.0, "split_local_candidate": True},
    ]
    output = apply_cross_period_gate(rows)
    assert output[1]["cross_period_stable"] is False
    assert output[1]["discovery_candidate"] is False
