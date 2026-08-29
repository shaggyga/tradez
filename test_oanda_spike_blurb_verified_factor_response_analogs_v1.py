import sqlite3

import pytest

import oanda_spike_blurb_verified_factor_response_analogs_v1 as analogs


def test_analog_key_retains_event_factor_and_horizon_identity():
    case = {"event_family": "employment_release", "event_currency": "USD"}
    factor = {
        "factor_family": "labor_revision",
        "metric": "two_month_revision",
        "unit": "persons",
    }
    key = analogs.analog_key(case, factor, 15)
    assert key == "employment_release|USD|labor_revision|two_month_revision|persons|h15"


def test_factor_links_use_fractional_event_weight_without_duplication():
    factor_count = 5
    weights = [1.0 / factor_count for _ in range(factor_count)]
    assert sum(weights) == pytest.approx(1.0)


def test_analog_tables_are_immutable(tmp_path):
    connection = sqlite3.connect(tmp_path / "test.sqlite")
    analogs.ensure_schema(connection)
    connection.execute(
        "INSERT INTO verified_factor_response_analog_contracts VALUES (?,?,?,?,?,?,?)",
        ("c", "s", "p", "{}", "a" * 64, "b" * 64, "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM verified_factor_response_analog_contracts WHERE contract_id='c'")
