from datetime import datetime, timezone

import numpy as np

from calendar_targets import endpoint_for_calendar_targets, utc_daily_close_targets


def epoch(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())


def test_weekend_is_skipped_for_trading_day_targets():
    friday = np.array([epoch("2024-07-05T12:01:00")])
    assert utc_daily_close_targets(friday, 1)[0] == epoch("2024-07-08T00:00:00")
    assert utc_daily_close_targets(friday, 2)[0] == epoch("2024-07-09T00:00:00")


def test_calendar_target_uses_previous_raw_bar_and_explicit_readiness():
    decision = np.array([epoch("2024-07-01T12:01:00")])
    data = {"time": np.array([epoch("2024-07-01T12:00:00"), epoch("2024-07-01T23:59:00")]), "close": np.array([100.0, 101.0])}
    outcome = endpoint_for_calendar_targets(data, decision, 1)
    assert outcome["state"][0] == "endpoint_available"
    assert np.isclose(outcome["midpoint_return_bps"][0], 100.0)
