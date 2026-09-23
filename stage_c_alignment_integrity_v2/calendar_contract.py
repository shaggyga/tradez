"""Versioned elapsed-time, diagnostic-day, and declared NY-close conventions.

Mon-Fri 17:00 New York is a research target convention, not executable venue
hours. Sunday is the weekly opening, so it never completes a session. Venue
holiday and partial-close history must be declared before market-session use.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from numbers import Integral
from zoneinfo import ZoneInfo

UTC_CLOSE_CALENDAR_ID = "utc_weekday_close.v2"
NY_17_SESSION_CALENDAR_ID = "fx_ny_1700_session_close.v2"
ELAPSED_TIME_TARGET_ID = "elapsed_seconds.v2"
DAILY_CLOSE_TARGET_ID = "next_completed_ny_session_close.v2"
COUNTED_SESSION_TARGET_ID = "counted_completed_ny_sessions.v2"
NEW_YORK = ZoneInfo("America/New_York")


class CalendarMetadataUnavailable(ValueError):
    """The caller has not declared holiday/partial-session coverage at a date."""


def _integer(value: int, name: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{name} must be an integer")
    result = int(value)
    if positive and result < 1:
        raise ValueError(f"{name} must be positive")
    return result


def _utc_datetime(epoch: int) -> datetime:
    epoch = _integer(epoch, "decision_epoch")
    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("decision_epoch is outside the supported date range") from exc


def _date(value: date, name: str) -> date:
    if type(value) is not date:
        raise ValueError(f"{name} must be a date, not a datetime or string")
    return value


def elapsed_time_target(decision_epoch: int, elapsed_seconds: int) -> int:
    """Add exact elapsed UTC seconds; weekends and DST do not alter the horizon."""
    decision = _utc_datetime(decision_epoch)
    duration = _integer(elapsed_seconds, "elapsed_seconds", positive=True)
    try:
        return int((decision + timedelta(seconds=duration)).timestamp())
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("elapsed target is outside the supported date range") from exc


def next_utc_weekday_close(decision_epoch: int, trading_days: int) -> int:
    """Midnight-UTC weekday diagnostic; never a venue-session target."""
    value = _utc_datetime(decision_epoch)
    count = _integer(trading_days, "trading_days", positive=True)
    candidate = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    seen = 0
    try:
        while True:
            candidate += timedelta(days=1)
            if candidate.weekday() < 5:
                seen += 1
                if seen == count:
                    return int(candidate.timestamp())
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("UTC diagnostic target is outside the supported date range") from exc


@dataclass(frozen=True)
class NewYorkSessionCalendar:
    """Mon-Fri close-boundary convention plus caller-owned exceptions.

    ``next_close`` counts closes strictly after the decision. The current
    partial session's future close is the first boundary; at an exact close,
    counting starts at the following eligible close. This does not measure
    hours actively traded or assert fill availability at the target timestamp.

    Metadata coverage is inclusive in local NY dates. ``metadata_id`` identifies
    the caller's immutable source/fixture establishing full and partial closures
    for the whole interval, including dates without exceptions. Empty exception
    sets alone do not establish holiday coverage. The library validates the
    declaration, not the truth of external evidence.
    """
    closure_dates: frozenset[date] = frozenset()
    partial_close_times: tuple[tuple[date, time], ...] = ()
    metadata_start: date | None = None
    metadata_end: date | None = None
    metadata_id: str | None = None

    def __post_init__(self) -> None:
        closures = frozenset(_date(value, "closure date") for value in self.closure_dates)
        partials = tuple(self.partial_close_times)
        seen: set[date] = set()
        for entry in partials:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise ValueError("partial_close_times must contain (date, local time) tuples")
            local_date, local_time = entry
            _date(local_date, "partial-close date")
            if local_date.weekday() >= 5:
                raise ValueError("partial closes must be Monday-Friday")
            if type(local_time) is not time or local_time.tzinfo is not None or local_time >= time(17):
                raise ValueError("partial close must be a naive local time before 17:00")
            if local_time.fold:
                raise ValueError("partial-close fold must be zero")
            if local_date in closures or local_date in seen:
                raise ValueError("partial close conflicts with a closure or duplicate date")
            seen.add(local_date)
        supplied = (self.metadata_start is not None, self.metadata_end is not None, self.metadata_id is not None)
        if any(supplied) and not all(supplied):
            raise ValueError("metadata_start, metadata_end and metadata_id must be supplied together")
        if all(supplied):
            start = _date(self.metadata_start, "metadata_start")
            end = _date(self.metadata_end, "metadata_end")
            if start > end:
                raise ValueError("metadata coverage must be chronologically ordered")
            if not isinstance(self.metadata_id, str) or not self.metadata_id.strip():
                raise ValueError("metadata_id must identify the caller's calendar evidence")
            if any(not start <= value <= end for value in closures | seen):
                raise ValueError("calendar exceptions must lie within metadata coverage")
        object.__setattr__(self, "closure_dates", closures)
        object.__setattr__(self, "partial_close_times", tuple(sorted(partials)))

    @property
    def calendar_id(self) -> str:
        return NY_17_SESSION_CALENDAR_ID

    def contract(self) -> dict:
        """Serializable contract for inclusion in a run's dependency identity."""
        return {
            "calendar_id": self.calendar_id,
            "timezone": NEW_YORK.key,
            "normal_close_local": "17:00:00",
            "close_weekdays": [0, 1, 2, 3, 4],
            "boundary_rule": "count_closes_strictly_after_decision",
            "holiday_coverage": "caller_declared" if self.metadata_id else "unknown",
            "metadata_id": self.metadata_id,
            "metadata_start": self.metadata_start.isoformat() if self.metadata_start else None,
            "metadata_end": self.metadata_end.isoformat() if self.metadata_end else None,
            "full_closures": [value.isoformat() for value in sorted(self.closure_dates)],
            "partial_closes": [[value.isoformat(), close.isoformat()] for value, close in self.partial_close_times],
        }

    def require_metadata(self, local_date: date) -> None:
        _date(local_date, "local_date")
        if self.metadata_id is None or not self.metadata_start <= local_date <= self.metadata_end:
            raise CalendarMetadataUnavailable(f"holiday/partial-session metadata unavailable for {local_date.isoformat()}")

    def is_session_close_date(self, local_date: date) -> bool:
        _date(local_date, "local_date")
        return local_date.weekday() < 5 and local_date not in self.closure_dates

    def close_epoch(self, local_date: date) -> int:
        if not self.is_session_close_date(local_date):
            raise ValueError("date has no completed-session close")
        local_time = dict(self.partial_close_times).get(local_date, time(17))
        return int(datetime.combine(local_date, local_time, tzinfo=NEW_YORK).timestamp())

    def next_close(self, decision_epoch: int, trading_sessions: int, *, require_metadata: bool = False) -> int:
        local_date = _utc_datetime(decision_epoch).astimezone(NEW_YORK).date()
        count = _integer(trading_sessions, "trading_sessions", positive=True)
        seen = 0
        while True:
            if require_metadata:
                self.require_metadata(local_date)
            if self.is_session_close_date(local_date):
                close = self.close_epoch(local_date)
                if close > decision_epoch:
                    seen += 1
                    if seen == count:
                        return close
            try:
                local_date += timedelta(days=1)
            except OverflowError as exc:
                raise ValueError("session target is outside the supported date range") from exc


def market_session_target(decision_epoch: int, trading_sessions: int,
                          calendar: NewYorkSessionCalendar | None = None) -> int:
    """Count declared completed session closes; fail closed without metadata."""
    if calendar is None:
        raise CalendarMetadataUnavailable("an explicit calendar with holiday/partial-session metadata is required")
    return calendar.next_close(decision_epoch, trading_sessions, require_metadata=True)


def daily_close_target(decision_epoch: int, calendar: NewYorkSessionCalendar | None = None) -> int:
    """The next declared daily session close, distinct from 24 elapsed hours."""
    return market_session_target(decision_epoch, 1, calendar)
