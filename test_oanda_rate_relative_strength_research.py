import datetime as dt

import pytest

import oanda_rate_relative_strength_research as research


UTC = dt.timezone.utc


def test_aligned_rate_events_require_same_date_and_both_pair_legs():
    histories = {
        "AAA": [
            {"rate_date": "2026-08-01", "change_bps": 3.0, "source_id": "a"},
        ],
        "BBB": [
            {"rate_date": "2026-08-01", "change_bps": -1.0, "source_id": "b"},
        ],
        "CCC": [
            {"rate_date": "2026-08-02", "change_bps": 2.0, "source_id": "c"},
        ],
    }
    rows = research.aligned_rate_events(histories, ["AAA_BBB", "AAA_CCC"])
    assert len(rows) == 1
    assert rows[0]["pair"] == "AAA_BBB"
    assert rows[0]["change_differential_bps"] == 4.0
    assert rows[0]["predicted_side"] == "long"


def test_executable_long_and_flipped_results_include_spread():
    event = {
        "pair": "AAA_BBB",
        "rate_date": "2026-08-01",
        "predicted_side": "long",
        "change_differential_bps": 5.0,
    }
    candles = [
        {
            "time": dt.datetime(2026, 8, 2, 12, tzinfo=UTC),
            "bid_o": 1.0000,
            "ask_o": 1.0002,
            "mid_o": 1.0001,
        },
        {
            "time": dt.datetime(2026, 8, 2, 16, tzinfo=UTC),
            "bid_o": 1.0010,
            "ask_o": 1.0012,
            "mid_o": 1.0011,
        },
    ]
    rows = research.evaluate_event(event, candles, 36, [4])
    assert len(rows) == 1
    assert rows[0]["entry_spread_pips"] == pytest.approx(2.0)
    assert rows[0]["rule_after_cost_pips"] == pytest.approx(8.0)
    assert rows[0]["flipped_after_cost_pips"] == pytest.approx(-12.0)
    assert rows[0]["proof_eligible"] is False


def test_metrics_keep_raw_rows_and_independent_dates_separate():
    rows = [
        {"pair": "AAA_BBB", "rate_date": "2026-08-01", "net": 2.0},
        {"pair": "AAA_CCC", "rate_date": "2026-08-01", "net": -1.0},
        {"pair": "AAA_BBB", "rate_date": "2026-08-02", "net": 1.0},
    ]
    result = research.metrics(rows, "net")
    assert result["raw_n"] == 3
    assert result["independent_rate_dates"] == 2
    assert result["pair_count"] == 2
    assert result["proof_eligible"] is False


def test_robustness_exposes_best_pair_date_time_split_and_cost_stress():
    rows = [
        {
            "pair": "AAA_BBB",
            "base_currency": "AAA",
            "quote_currency": "BBB",
            "rate_date": "2026-08-01",
            "rule_after_cost_pips": 10.0,
            "entry_spread_pips": 2.0,
        },
        {
            "pair": "AAA_CCC",
            "base_currency": "AAA",
            "quote_currency": "CCC",
            "rate_date": "2026-08-01",
            "rule_after_cost_pips": -2.0,
            "entry_spread_pips": 2.0,
        },
        {
            "pair": "AAA_CCC",
            "base_currency": "AAA",
            "quote_currency": "CCC",
            "rate_date": "2026-08-02",
            "rule_after_cost_pips": -1.0,
            "entry_spread_pips": 2.0,
        },
    ]
    result = research.robustness(rows)
    assert result["best_pair"] == "AAA_BBB"
    assert result["best_date"] == "2026-08-01"
    assert result["mean_without_best_pair_pips"] == pytest.approx(-1.5)
    assert result["mean_without_best_date_pips"] == pytest.approx(-1.0)
    assert result["additional_full_spread_mean_pips"] == pytest.approx(1 / 3)
    assert result["date_factor_count"] == 2
    assert result["proof_eligible"] is False
