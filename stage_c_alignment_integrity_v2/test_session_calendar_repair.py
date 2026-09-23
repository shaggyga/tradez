"""TST32 hand-verifiable research-calendar fixtures; no venue history claims."""
from datetime import date, datetime, time, timezone

import pytest

from calendar_contract import (
    COUNTED_SESSION_TARGET_ID, DAILY_CLOSE_TARGET_ID, ELAPSED_TIME_TARGET_ID,
    NY_17_SESSION_CALENDAR_ID, CalendarMetadataUnavailable, NewYorkSessionCalendar,
    daily_close_target, elapsed_time_target, market_session_target, next_utc_weekday_close,
)


def epoch(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp())


def fixture_calendar(**kwargs) -> NewYorkSessionCalendar:
    return NewYorkSessionCalendar(metadata_start=date(2024, 1, 1), metadata_end=date(2024, 12, 31),
                                  metadata_id="synthetic_2024_calendar_fixture.v1", **kwargs)


@pytest.mark.parametrize("decision,expected", [
    ("2024-07-05T20:59:59+00:00", "2024-07-05T21:00:00+00:00"),
    ("2024-07-05T21:00:00+00:00", "2024-07-08T21:00:00+00:00"),
    ("2024-07-05T21:00:01+00:00", "2024-07-08T21:00:00+00:00"),
    ("2024-07-07T20:59:59+00:00", "2024-07-08T21:00:00+00:00"),
    ("2024-07-07T21:00:00+00:00", "2024-07-08T21:00:00+00:00"),
    ("2024-07-07T21:00:01+00:00", "2024-07-08T21:00:00+00:00"),
    ("2024-03-08T23:00:00+00:00", "2024-03-11T21:00:00+00:00"),
    ("2024-11-01T23:00:00+00:00", "2024-11-04T22:00:00+00:00"),
])
def test_friday_boundaries_sunday_open_and_dst(decision, expected):
    assert market_session_target(epoch(decision), 1, fixture_calendar()) == epoch(expected)


@pytest.mark.parametrize("decision,count,expected", [
    ("2024-07-05T20:00:00+00:00", 2, "2024-07-08T21:00:00+00:00"),
    ("2024-07-05T20:00:00+00:00", 5, "2024-07-11T21:00:00+00:00"),
    ("2024-07-05T21:00:00+00:00", 2, "2024-07-09T21:00:00+00:00"),
    ("2024-07-05T21:00:00+00:00", 5, "2024-07-12T21:00:00+00:00"),
    ("2024-03-08T23:00:00+00:00", 5, "2024-03-15T21:00:00+00:00"),
    ("2024-11-01T23:00:00+00:00", 5, "2024-11-08T22:00:00+00:00"),
])
def test_two_and_five_completed_closes(decision, count, expected):
    assert market_session_target(epoch(decision), count, fixture_calendar()) == epoch(expected)


def test_elapsed_daily_and_counted_sessions_stay_distinct():
    decision = epoch("2024-07-05T20:00:00+00:00")
    calendar = fixture_calendar()
    assert len({ELAPSED_TIME_TARGET_ID, DAILY_CLOSE_TARGET_ID, COUNTED_SESSION_TARGET_ID}) == 3
    assert elapsed_time_target(decision, 86400) == epoch("2024-07-06T20:00:00+00:00")
    assert daily_close_target(decision, calendar) == epoch("2024-07-05T21:00:00+00:00")
    assert market_session_target(decision, 2, calendar) == epoch("2024-07-08T21:00:00+00:00")
    assert next_utc_weekday_close(decision, 1) == epoch("2024-07-08T00:00:00+00:00")
    assert NY_17_SESSION_CALENDAR_ID == "fx_ny_1700_session_close.v2"


def test_declared_full_and_partial_closures():
    calendar = fixture_calendar(closure_dates=frozenset({date(2024, 12, 25)}),
                                partial_close_times=((date(2024, 12, 24), time(13)),))
    assert daily_close_target(epoch("2024-12-24T17:59:00+00:00"), calendar) == epoch("2024-12-24T18:00:00+00:00")
    assert daily_close_target(epoch("2024-12-24T18:00:00+00:00"), calendar) == epoch("2024-12-26T22:00:00+00:00")
    assert market_session_target(epoch("2024-12-24T17:59:00+00:00"), 2, calendar) == epoch("2024-12-26T22:00:00+00:00")
    with pytest.raises(ValueError, match="no completed-session"):
        calendar.close_epoch(date(2024, 12, 25))
    with pytest.raises(ValueError, match="no completed-session"):
        calendar.close_epoch(date(2024, 7, 7))


def test_missing_or_exhausted_holiday_metadata_fails_closed():
    decision = epoch("2024-07-05T20:00:00+00:00")
    with pytest.raises(CalendarMetadataUnavailable):
        market_session_target(decision, 1)
    with pytest.raises(CalendarMetadataUnavailable):
        market_session_target(decision, 1, NewYorkSessionCalendar())
    calendar = NewYorkSessionCalendar(metadata_start=date(2024, 7, 5), metadata_end=date(2024, 7, 7), metadata_id="synthetic")
    assert daily_close_target(decision, calendar) == epoch("2024-07-05T21:00:00+00:00")
    with pytest.raises(CalendarMetadataUnavailable, match="2024-07-08"):
        market_session_target(decision, 2, calendar)
    assert NewYorkSessionCalendar().contract()["holiday_coverage"] == "unknown"
    assert fixture_calendar().contract()["holiday_coverage"] == "caller_declared"


@pytest.mark.parametrize("bad", [True, False, 1.5, float("nan"), float("inf"), "2", None, 0, -1])
def test_invalid_counts_rejected(bad):
    decision = epoch("2024-07-05T20:00:00+00:00")
    with pytest.raises(ValueError):
        market_session_target(decision, bad, fixture_calendar())
    with pytest.raises(ValueError):
        elapsed_time_target(decision, bad)
    with pytest.raises(ValueError):
        next_utc_weekday_close(decision, bad)


@pytest.mark.parametrize("bad", [True, 1.5, float("nan"), float("inf"), "1700000000", None, 10**30])
def test_invalid_epochs_rejected(bad):
    with pytest.raises(ValueError):
        market_session_target(bad, 1, fixture_calendar())


@pytest.mark.parametrize("kwargs", [
    {"metadata_start": date(2024, 1, 1)},
    {"metadata_start": date(2024, 2, 1), "metadata_end": date(2024, 1, 1), "metadata_id": "x"},
    {"metadata_start": date(2024, 1, 1), "metadata_end": date(2024, 2, 1), "metadata_id": " "},
    {"closure_dates": frozenset({"2024-01-01"})},
    {"closure_dates": frozenset({datetime(2024, 1, 1)})},
    {"partial_close_times": ((date(2024, 7, 7), time(13)),)},
    {"partial_close_times": ((date(2024, 7, 8), time(18)),)},
    {"partial_close_times": ((date(2024, 7, 8), time(13, tzinfo=timezone.utc)),)},
    {"partial_close_times": ((date(2024, 7, 8), time(13)), (date(2024, 7, 8), time(14)))},
    {"closure_dates": frozenset({date(2024, 7, 8)}), "partial_close_times": ((date(2024, 7, 8), time(13)),)},
])
def test_invalid_calendar_metadata_rejected(kwargs):
    with pytest.raises(ValueError):
        NewYorkSessionCalendar(**kwargs)


def test_contract_records_every_declared_exception_and_coverage():
    contract = fixture_calendar(closure_dates=frozenset({date(2024, 12, 25)}),
                               partial_close_times=((date(2024, 12, 24), time(13)),)).contract()
    assert contract["close_weekdays"] == [0, 1, 2, 3, 4]
    assert contract["full_closures"] == ["2024-12-25"]
    assert contract["partial_closes"] == [["2024-12-24", "13:00:00"]]
    assert contract["metadata_start"] == "2024-01-01"
    assert contract["metadata_end"] == "2024-12-31"
