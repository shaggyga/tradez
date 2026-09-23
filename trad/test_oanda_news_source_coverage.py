from __future__ import annotations

import datetime as dt

import oanda_news_source_coverage as coverage


NOW = dt.datetime(2026, 8, 16, 16, 30, tzinfo=dt.timezone.utc)
SOURCE = {"kind": "json_records", "poll_interval_sec": 300}


def test_source_health_requires_transport_and_parser_success() -> None:
    state = {
        "last_success_utc": "2026-08-16T16:29:00+00:00",
        "last_status": 200,
        "last_error": "parse_error: malformed payload",
        "consecutive_errors": 1,
    }
    assert not coverage.source_is_healthy(SOURCE, state, as_of=NOW)


def test_source_health_rejects_error_counter_even_without_message() -> None:
    state = {
        "last_success_utc": "2026-08-16T16:29:00+00:00",
        "last_status": 200,
        "last_error": "",
        "consecutive_errors": 1,
    }
    assert not coverage.source_is_healthy(SOURCE, state, as_of=NOW)


def test_source_health_accepts_recent_clean_success() -> None:
    state = {
        "last_success_utc": "2026-08-16T16:29:00+00:00",
        "last_status": 200,
        "last_error": "",
        "consecutive_errors": 0,
    }
    assert coverage.source_is_healthy(SOURCE, state, as_of=NOW)


def test_recent_transport_failure_gets_bounded_visible_grace() -> None:
    source = {"kind": "json_records", "poll_interval_sec": 900}
    state = {
        "last_success_utc": "2026-08-16T16:00:00+00:00",
        "last_status": 403,
        "last_error": "curl: requested URL returned error: 403",
        "consecutive_errors": 2,
        "parsed_items": 1,
    }
    usable, age = coverage.source_recent_success_state(source, state, as_of=NOW)
    assert usable is True
    assert age == 1800.0
    assert coverage.source_is_healthy(source, state, as_of=NOW) is False


def test_recent_success_grace_expires_and_never_masks_parse_errors() -> None:
    source = {"kind": "json_records", "poll_interval_sec": 300}
    stale = {
        "last_success_utc": "2026-08-16T15:00:00+00:00",
        "last_status": 403,
        "last_error": "http 403",
        "consecutive_errors": 1,
        "parsed_items": 1,
    }
    parse_error = {
        **stale,
        "last_success_utc": "2026-08-16T16:29:00+00:00",
        "last_status": 500,
        "last_error": "parse_error: malformed payload",
    }
    assert coverage.source_recent_success_state(source, stale, as_of=NOW)[0] is False
    assert coverage.source_recent_success_state(source, parse_error, as_of=NOW)[0] is False
