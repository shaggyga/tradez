import datetime as dt
import json
import sqlite3

import oanda_alfred_macro_relative_strength_research as research


UTC = dt.timezone.utc


def fixture(tmp_path):
    db_path = tmp_path / "alfred.sqlite"
    db = sqlite3.connect(db_path)
    db.execute(
        """CREATE TABLE vintage_observations (
            vintage_observation_id TEXT, cohort_id TEXT, source_contract_id TEXT,
            series_id TEXT, economic_family TEXT, currency TEXT,
            observation_date TEXT, value REAL, realtime_start TEXT,
            realtime_end TEXT, first_seen_utc TEXT, version INTEGER,
            supersedes_observation_id TEXT, observation_kind TEXT,
            bootstrap_current_view INTEGER, prospective_eligible INTEGER,
            same_day_intrahour_eligible INTEGER, direction_policy TEXT,
            substantive_sha256 TEXT, raw_payload_sha256 TEXT,
            cohort_contract_json TEXT
        )"""
    )
    cohort = "test.cohort"
    rows = []
    for series, currency, values in (
        ("CAD", "CAD", [("2025-01-01", 2.0), ("2025-02-01", 2.5)]),
        ("CPIAUCSL", "USD", [("2024-01-01", 100.0), ("2024-02-01", 100.0), ("2025-01-01", 103.0), ("2025-02-01", 104.0)]),
    ):
        for index, (date, value) in enumerate(values):
            rows.append((f"{series}-{index}", cohort, "source", series, "cpi", currency, date, value,
                         date, date, "2026-08-16T00:00:00Z", 1, None, "bootstrap_current_view",
                         1, 0, 0, "abstain", "x", "y", "{}"))
    db.executemany("INSERT INTO vintage_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    db.commit(); db.close()
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"cohort": {"cohort_id": cohort}}))
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"series": [
        {"series_id": "CAD", "currency": "CAD", "cross_currency_frequency_group": "monthly_cpi_yoy"},
        {"series_id": "CPIAUCSL", "currency": "USD", "economic_family": "headline_cpi"},
    ]}))
    return db_path, state, config


def test_panel_converts_us_index_to_yoy_without_treating_bootstrap_as_proof(tmp_path):
    panel, state = research.load_panel(*fixture(tmp_path))
    assert state["all_rows_bootstrap"] is True
    assert round(panel["USD"]["monthly_cpi_yoy"][-1]["macro_value"], 8) == 4.0
    assert panel["CAD"]["monthly_cpi_yoy"][-1]["macro_acceleration"] == 0.5


def test_aligned_events_keep_frequency_and_factor_episode_separate(tmp_path):
    panel, _ = research.load_panel(*fixture(tmp_path))
    rows = research.aligned_events(panel, ["USD_CAD"], {"monthly_cpi_yoy": 45})
    level = next(row for row in rows if row["reference_date"] == "2025-02-01" and row["signal_rule"] == "cpi_level_differential")
    assert level["predicted_side"] == "long"
    assert level["available_utc"] == dt.datetime(2025, 3, 18, 12, tzinfo=UTC)
    assert level["factor_episode_id"] == "cpi:monthly_cpi_yoy:2025-02-01"


def test_unemployment_differential_uses_lower_rate_as_stronger_currency():
    panel = {
        "EUR": {
            "monthly_unemployment_rate": [
                {
                    "reference_date": dt.date(2026, 5, 1),
                    "macro_value": 6.0,
                    "macro_acceleration": -0.1,
                }
            ]
        },
        "USD": {
            "monthly_unemployment_rate": [
                {
                    "reference_date": dt.date(2026, 5, 1),
                    "macro_value": 4.0,
                    "macro_acceleration": 0.1,
                }
            ]
        },
    }
    rows = research.aligned_events(
        panel,
        ["EUR_USD"],
        {"monthly_unemployment_rate": 45},
    )
    level = next(
        row for row in rows
        if row["signal_rule"] == "unemployment_level_differential"
    )
    change = next(
        row for row in rows
        if row["signal_rule"] == "unemployment_change_differential"
    )
    assert level["predicted_side"] == "short"
    assert level["signal_value"] == -2.0
    assert change["predicted_side"] == "long"
    assert change["signal_value"] == 0.2
    assert level["factor_episode_id"] == (
        "unemployment:monthly_unemployment_rate:2026-05-01"
    )


def test_short_rate_change_uses_higher_rate_as_stronger_currency():
    panel = {
        "EUR": {
            "monthly_short_term_interest_rate": [
                {
                    "reference_date": dt.date(2026, 5, 1),
                    "macro_value": 2.0,
                    "macro_acceleration": -0.1,
                }
            ]
        },
        "USD": {
            "monthly_short_term_interest_rate": [
                {
                    "reference_date": dt.date(2026, 5, 1),
                    "macro_value": 4.0,
                    "macro_acceleration": 0.1,
                }
            ]
        },
    }
    rows = research.aligned_events(
        panel,
        ["EUR_USD"],
        {"monthly_short_term_interest_rate": 45},
    )
    level = next(
        row for row in rows if row["signal_rule"] == "short_rate_level_differential"
    )
    change = next(
        row for row in rows if row["signal_rule"] == "short_rate_change_differential"
    )
    assert level["predicted_side"] == "short"
    assert level["signal_value"] == -2.0
    assert change["predicted_side"] == "short"
    assert change["signal_value"] == -0.2
    assert level["factor_episode_id"] == (
        "short_rate:monthly_short_term_interest_rate:2026-05-01"
    )


def test_point_in_time_availability_waits_for_both_currency_releases():
    panel = {
        "EUR": {
            "monthly_short_term_interest_rate": [
                {
                    "reference_date": dt.date(2026, 5, 1),
                    "macro_value": 2.0,
                    "macro_acceleration": 0.2,
                    "first_seen_utc": "2026-06-10T05:00:00Z",
                }
            ]
        },
        "USD": {
            "monthly_short_term_interest_rate": [
                {
                    "reference_date": dt.date(2026, 5, 1),
                    "macro_value": 4.0,
                    "macro_acceleration": 0.1,
                    "first_seen_utc": "2026-06-03T05:00:00Z",
                }
            ]
        },
    }
    rows = research.aligned_events(
        panel,
        ["EUR_USD"],
        {},
        "max_first_seen_utc",
    )
    assert rows
    assert all(
        row["available_utc"] == dt.datetime(2026, 6, 10, 5, tzinfo=UTC)
        for row in rows
    )


def test_executable_bid_ask_evaluation_keeps_flipped_control_costed():
    event = {
        "pair": "EUR_USD", "predicted_side": "long",
        "available_utc": dt.datetime(2026, 1, 1, tzinfo=UTC),
        "factor_episode_id": "factor", "signal_rule": "rule",
        "signal_value": 1.0,
    }
    candles = [
        {"time": dt.datetime(2026, 1, 1, tzinfo=UTC), "bid_o": 1.1000, "ask_o": 1.1002},
        {"time": dt.datetime(2026, 1, 2, tzinfo=UTC), "bid_o": 1.1012, "ask_o": 1.1014},
    ]
    rows = research.evaluate(event, candles, [24], {"liquid": 3.0, "moderate": 10.0})
    assert round(rows[0]["rule_after_cost_pips"], 8) == 10.0
    assert round(rows[0]["flipped_after_cost_pips"], 8) == -14.0
    assert rows[0]["proof_eligible"] is False
    assert rows[0]["liquidity_bucket"] == "liquid"
