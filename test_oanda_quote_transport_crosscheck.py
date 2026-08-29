import datetime as dt

import oanda_quote_transport_crosscheck as crosscheck


UTC = dt.timezone.utc


def snapshot(
    now,
    producer,
    *,
    delta=0.0,
    missing_second=False,
    retained=(),
    retained_delta=0.0,
    retained_time=None,
):
    quotes = {
        "EUR_USD": {
            "bid": 1.1000 + delta,
            "ask": 1.1002 + delta,
            "pip": 0.0001,
            "time": now.isoformat(),
        },
        "USD_JPY": {
            "bid": 150.00 + delta,
            "ask": 150.02 + delta,
            "pip": 0.01,
            "time": now.isoformat(),
        },
    }
    if missing_second:
        quotes.pop("USD_JPY")
    for instrument in retained:
        quotes[instrument]["bid"] += retained_delta
        quotes[instrument]["ask"] += retained_delta
        if retained_time is not None:
            quotes[instrument]["time"] = retained_time.isoformat()
    return {
        "generated_utc": now.isoformat(),
        "producer": producer,
        "quotes": quotes,
        "coverage": {
            "retained_last_known_instruments": list(retained),
            "retained_quotes_execution_eligible": False,
        },
    }


def test_matching_independent_transports_are_healthy():
    now = dt.datetime(2026, 8, 4, 11, 0, tzinfo=UTC)
    result = crosscheck.compare_snapshots(
        snapshot(now, crosscheck.EXPECTED_CANONICAL_PRODUCER),
        snapshot(now, "practice_007_dedicated_quote_stream"),
        now=now,
    )
    assert result["status"] == "healthy"
    assert result["can_place_orders"] is False
    assert result["metrics"]["shared_instrument_count"] == 2
    assert result["metrics"]["maximum_mid_divergence_pips"] == 0.0


def test_wrong_canonical_owner_and_missing_coverage_degrade():
    now = dt.datetime(2026, 8, 4, 11, 0, tzinfo=UTC)
    result = crosscheck.compare_snapshots(
        snapshot(now, "practice_007_dedicated_quote_stream"),
        snapshot(now, "transport_check", missing_second=True),
        now=now,
    )
    assert result["status"] == "degraded"
    assert "unexpected_canonical_producer" in result["reasons"]
    assert "insufficient_shared_instruments" in result["reasons"]


def test_relative_divergence_is_measured_against_spread():
    now = dt.datetime(2026, 8, 4, 11, 0, tzinfo=UTC)
    result = crosscheck.compare_snapshots(
        snapshot(now, crosscheck.EXPECTED_CANONICAL_PRODUCER),
        snapshot(now, "transport_check", delta=0.0004),
        now=now,
        maximum_p95_divergence_to_spread=0.5,
    )
    assert result["status"] == "degraded"
    assert "transport_price_divergence" in result["reasons"]
    assert result["metrics"]["maximum_mid_divergence_pips"] == 4.0


def test_closed_market_staleness_is_context_not_transport_failure():
    now = dt.datetime(2026, 8, 8, 14, 0, tzinfo=UTC)
    friday = dt.datetime(2026, 8, 7, 20, 59, tzinfo=UTC)
    result = crosscheck.compare_snapshots(
        snapshot(friday, crosscheck.EXPECTED_CANONICAL_PRODUCER),
        snapshot(friday, "practice_007_dedicated_quote_stream"),
        now=now,
    )

    assert result["status"] == "healthy"
    assert result["market_state"] == "weekend_closed"
    assert "canonical_snapshot_stale_market_closed" in result["informational_reasons"]
    assert "comparison_snapshot_stale_market_closed" in result["informational_reasons"]


def test_market_close_boundary_tracks_new_york_daylight_saving_time():
    # 17:01 EDT is 21:01 UTC; the former fixed 22:00 UTC boundary was late.
    assert (
        crosscheck.forex_market_state(
            dt.datetime(2026, 8, 28, 21, 1, tzinfo=UTC)
        )
        == "weekend_closed"
    )
    # Sunday 16:59 EDT is still closed; 17:00 EDT is the regular reopen.
    assert (
        crosscheck.forex_market_state(
            dt.datetime(2026, 8, 30, 20, 59, tzinfo=UTC)
        )
        == "weekend_closed"
    )
    assert (
        crosscheck.forex_market_state(
            dt.datetime(2026, 8, 30, 21, 0, tzinfo=UTC)
        )
        == "open_or_transition"
    )


def test_market_close_boundary_tracks_new_york_standard_time():
    assert (
        crosscheck.forex_market_state(
            dt.datetime(2026, 1, 9, 21, 59, tzinfo=UTC)
        )
        == "open_or_transition"
    )
    assert (
        crosscheck.forex_market_state(
            dt.datetime(2026, 1, 9, 22, 0, tzinfo=UTC)
        )
        == "weekend_closed"
    )


def test_closed_market_restart_without_shared_quotes_is_informational():
    now = dt.datetime(2026, 8, 28, 21, 10, tzinfo=UTC)
    friday_close = dt.datetime(2026, 8, 28, 20, 59, tzinfo=UTC)
    canonical = snapshot(friday_close, crosscheck.EXPECTED_CANONICAL_PRODUCER)
    canonical["quotes"] = {}
    comparison = snapshot(
        friday_close,
        "practice_007_dedicated_quote_stream",
    )

    result = crosscheck.compare_snapshots(
        canonical,
        comparison,
        now=now,
    )

    assert result["status"] == "healthy"
    assert result["reasons"] == []
    assert (
        "insufficient_shared_instruments_market_closed"
        in result["informational_reasons"]
    )
    assert (
        "transport_price_comparison_unavailable_market_closed"
        in result["informational_reasons"]
    )


def test_retained_rows_are_excluded_from_current_skew_and_divergence():
    now = dt.datetime(2026, 8, 4, 11, 0, tzinfo=UTC)
    old = now - dt.timedelta(minutes=5)
    result = crosscheck.compare_snapshots(
        snapshot(now, crosscheck.EXPECTED_CANONICAL_PRODUCER),
        snapshot(
            now,
            "practice_007_dedicated_quote_stream",
            retained=("USD_JPY",),
            retained_delta=2.0,
            retained_time=old,
        ),
        now=now,
    )

    assert result["status"] == "degraded"
    assert result["reasons"] == ["insufficient_shared_instruments"]
    assert result["comparison"]["quote_count"] == 2
    assert result["comparison"]["current_quote_count"] == 1
    assert result["comparison"]["retained_last_known_instruments"] == ["USD_JPY"]
    assert result["metrics"]["shared_instrument_count"] == 1
    assert result["metrics"]["current_instrument_union_count"] == 2
    assert result["metrics"]["maximum_mid_divergence_pips"] == 0.0
    assert result["metrics"]["p95_quote_time_skew_sec"] == 0.0
    assert [row["instrument"] for row in result["worst_relative_divergences"]] == [
        "EUR_USD"
    ]
