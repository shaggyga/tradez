from all68_global_clock import BarEvent, global_minute_support, issue_no_change_tape, no_trade_ledger, ordered_events


UNIVERSE = ["USD_JPY", "EUR_HUF", "HKD_JPY"]
HORIZONS = [5 * 60, 15 * 60, 30 * 60, 60 * 60, 120 * 60, 240 * 60, 480 * 60, 1440 * 60]


def test_global_clock_is_timestamp_then_instrument_and_refuses_duplicate_pair_minutes():
    events = [BarEvent(120, "USD_JPY", 1.0), BarEvent(60, "USD_JPY", 1.0), BarEvent(60, "EUR_HUF", 1.0)]
    assert [(event.bar_end_epoch, event.instrument) for event in ordered_events(events)] == [(60, "EUR_HUF"), (60, "USD_JPY"), (120, "USD_JPY")]
    try:
        ordered_events([BarEvent(60, "USD_JPY", 1.0), BarEvent(60, "USD_JPY", 1.1)])
    except ValueError as exc:
        assert str(exc) == "duplicate_pair_minute"
    else:
        raise AssertionError("duplicate minute accepted")


def test_missing_support_is_explicit_and_never_forward_filled():
    support = global_minute_support([BarEvent(60, "USD_JPY", 1.0), BarEvent(120, "EUR_HUF", 2.0)], UNIVERSE)
    assert support[0][1] == {"EUR_HUF": "missing_support", "HKD_JPY": "missing_support", "USD_JPY": "supported"}
    assert support[1][1] == {"EUR_HUF": "supported", "HKD_JPY": "missing_support", "USD_JPY": "missing_support"}


def test_forecast_tape_has_all_required_horizons_and_availability_is_the_latest_gate():
    tape = issue_no_change_tape([BarEvent(60, "USD_JPY", 1.0)], UNIVERSE, HORIZONS, training_cutoff_epoch=500, fit_completed_epoch=1000)
    eligible = [item for item in tape if item.instrument == "USD_JPY" and item.status == "eligible"]
    assert len(eligible) == 8
    one_day = next(item for item in eligible if item.horizon_sec == 86400)
    assert one_day.target_epoch == 86460
    assert one_day.available_epoch == 1000
    blocked = [item for item in tape if item.instrument == "EUR_HUF"]
    assert {item.reason for item in blocked} == {"missing_support"}


def test_policy_arms_get_independent_capital_and_the_same_forecast_ids():
    tape = issue_no_change_tape([BarEvent(60, "USD_JPY", 1.0)], UNIVERSE, HORIZONS, training_cutoff_epoch=0, fit_completed_epoch=0)
    fixed = no_trade_ledger(tape, "fixed_reference", 1000.0)
    rotation = no_trade_ledger(tape, "recovered_rotation", 250.0)
    assert [row.forecast_id for row in fixed] == [row.forecast_id for row in rotation]
    assert {row.capital_after for row in fixed} == {1000.0}
    assert {row.capital_after for row in rotation} == {250.0}
