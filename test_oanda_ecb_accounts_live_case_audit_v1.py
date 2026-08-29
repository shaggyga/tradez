import datetime as dt

import oanda_ecb_accounts_live_case_audit_v1 as audit


UTC = dt.timezone.utc


def _row(minute: int, bid_open: float, bid_close: float):
    return {
        "timestamp": dt.datetime(2026, 8, 27, 11, minute, tzinfo=UTC),
        "bid_open": bid_open,
        "ask_open": bid_open + 0.0002,
        "bid_close": bid_close,
        "ask_close": bid_close + 0.0002,
        "mid_open": bid_open + 0.0001,
        "mid_close": bid_close + 0.0001,
    }


def test_one_minute_path_uses_exactly_one_completed_candle():
    panel = {
        "EUR_USD": [
            _row(30, 1.1000, 1.1010),
            _row(31, 1.1010, 1.2010),
        ]
    }
    path = audit.exact_pair_path(
        panel,
        "EUR_USD",
        dt.datetime(2026, 8, 27, 11, 30, tzinfo=UTC),
        dt.datetime(2026, 8, 27, 11, 31, tzinfo=UTC),
    )
    assert path["last_completed_candle_utc"] == "2026-08-27T11:30:00+00:00"
    assert path["midpoint_move_pips"] == 10.0
    assert path["observed_after_cost_pips"] == 8.0


def test_factor_response_retains_pair_level_paths():
    panel = {
        "EUR_USD": [_row(30, 1.1000, 1.1010)],
        "GBP_USD": [_row(30, 1.3000, 1.3000)],
    }
    response = audit.exact_factor_response(
        panel,
        dt.datetime(2026, 8, 27, 11, 30, tzinfo=UTC),
        dt.datetime(2026, 8, 27, 11, 31, tzinfo=UTC),
    )
    assert response["usable_pair_count"] == 2
    assert response["expected_pair_count"] == 2
    assert response["missing_or_stale_pairs"] == []
    assert len(response["all_pair_paths"]) == 2
    assert response["scores_bps"]["EUR"] > response["scores_bps"]["USD"]


def test_factor_delta_is_post_minus_equal_pre_window():
    delta = audit.factor_delta_bps(
        {"scores_bps": {"EUR": -0.8}},
        {"scores_bps": {"EUR": 0.3}},
        "EUR",
    )
    assert delta == {
        "pre_event_equal_horizon_factor_bps": 0.3,
        "post_event_factor_bps": -0.8,
        "post_minus_pre_factor_bps": -1.1,
    }


def test_probe_jsonl_reader_ignores_partial_or_invalid_rows(tmp_path):
    path = tmp_path / "probe.jsonl"
    path.write_text(
        '{"observed_utc":"2026-08-27T11:30:34+00:00"}\ninvalid\n[]\n',
        encoding="utf-8",
    )
    assert audit._read_jsonl(path) == [
        {"observed_utc": "2026-08-27T11:30:34+00:00"}
    ]


def test_contract_is_fixed_research_only():
    assert audit.EVENT_UTC == dt.datetime(2026, 8, 27, 11, 30, tzinfo=UTC)
    assert audit.HORIZONS == (1, 5, 15, 30, 60)


def test_prior_ecb_statement_is_context_not_outcome_evidence():
    baseline = audit.prior_policy_baseline()
    assert baseline["available"] is True
    assert baseline["use_policy"] == "prior_context_only_not_outcome_evidence"
    assert "23 July 2026" in baseline["summary_excerpt"]
