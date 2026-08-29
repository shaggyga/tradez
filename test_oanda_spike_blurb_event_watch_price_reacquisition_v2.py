import sqlite3

import pytest

import oanda_spike_blurb_event_watch_price_reacquisition_v2 as prices


def test_request_jobs_requires_exact_68_universe():
    cases = [{"case_id": "c"}]
    instruments = [f"A{i}_B{i}" for i in range(68)]
    jobs = prices.request_jobs(cases, instruments)
    assert len(jobs) == 68
    with pytest.raises(ValueError, match="exact_68_instrument_universe_required"):
        prices.request_jobs(cases, instruments[:-1])


def test_summarize_classifies_exact_executable_window():
    job = {
        "instrument": "EUR_USD",
        "start_epoch": 1000,
        "end_epoch": 1120,
    }
    candles = [
        {
            "epoch": 1000, "time": "1970-01-01T00:16:40+00:00",
            "bid_c": 1.0, "ask_c": 1.0002, "mid_c": 1.0001,
        },
        {
            "epoch": 1060, "time": "1970-01-01T00:17:40+00:00",
            "bid_c": 1.0010, "ask_c": 1.0013, "mid_c": 1.00115,
        },
    ]
    result = prices.summarize(job, candles)
    assert result["coverage_state"] == "exact_window"
    assert result["average_spread_pips"] == pytest.approx(2.5)
    assert result["maximum_spread_pips"] == pytest.approx(3.0)


def test_empty_window_is_explicit_not_inferred():
    result = prices.summarize(
        {"instrument": "USD_JPY", "start_epoch": 1000, "end_epoch": 1120}, []
    )
    assert result["coverage_state"] == "no_candles"
    assert result["candle_count"] == 0
    assert result["average_spread_pips"] is None


def test_event_price_tables_are_immutable(tmp_path):
    connection = sqlite3.connect(tmp_path / "test.sqlite")
    prices.ensure_schema(connection)
    connection.execute(
        "INSERT INTO verified_event_price_contracts VALUES (?,?,?,?,?,?,?,?)",
        ("c", "s", "{}", "a" * 64, "b" * 64, "d" * 64, "https://example.test", "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("UPDATE verified_event_price_contracts SET contract_json='x' WHERE contract_id='c'")


def test_readonly_client_rejects_any_non_candle_surface():
    client = prices.ReadonlyPracticeCandleClient("test-token")
    with pytest.raises(RuntimeError, match="readonly_client_rejects_non_candle_request"):
        client.request("POST", "/v3/accounts/anything/orders")
    with pytest.raises(RuntimeError, match="readonly_client_rejects_non_candle_request"):
        client.request("GET", "/v3/accounts/anything/summary")
