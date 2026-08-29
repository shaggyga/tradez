from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import trad.oanda_spike_blurb_fomc_sep_factor_response_v1 as factor


def fixture_projection() -> dict:
    return {
        "federal_funds_rate": {
            "current": {"2026": 3.8, "2027": 3.6, "longer_run": 3.1},
            "prior": {"2026": 3.4, "2027": 3.1, "longer_run": 3.1},
        },
        "pce_inflation": {"current": {"2026": 3.1}, "prior": {"2026": 2.8}},
        "core_pce_inflation": {"current": {"2026": 3.3}, "prior": {"2026": 2.7}},
        "unemployment_rate": {"current": {"2026": 4.4}, "prior": {"2026": 4.3}},
        "real_gdp_growth": {"current": {"2026": 2.2}, "prior": {"2026": 2.4}},
    }


def test_projection_factor_vocabulary_and_direction_are_fixed() -> None:
    rows = {row["factor_name"]: row for row in factor.factor_values(fixture_projection(), 2026)}
    assert set(rows) == {name for name, _role, _mapping in factor.FACTOR_DEFINITIONS}
    assert rows["fed_funds_current_year_change_bps"]["signed_factor_score"] == pytest.approx(40.0)
    assert rows["fed_funds_next_year_change_bps"]["signed_factor_score"] == pytest.approx(50.0)
    assert rows["unemployment_current_year_change_pp"]["signed_factor_score"] == pytest.approx(-0.1)
    assert rows["policy_path_absolute_revision_bps"]["signed_factor_score"] == 0.0


def test_curve_slope_change_is_policy_path_not_price_outcome() -> None:
    rows = {row["factor_name"]: row for row in factor.factor_values(fixture_projection(), 2026)}
    assert rows["fed_funds_curve_slope_change_bps"]["raw_delta"] == pytest.approx(10.0)
    assert rows["fed_funds_curve_slope_change_bps"]["proof_state"] == "retrospective_official_projection_discovery_only"


def test_direction_and_policy_sets_remain_separate() -> None:
    assert factor.direction_sign("stronger") == 1
    assert factor.direction_sign("weaker") == -1
    assert factor.direction_sign(None) == 0
    assert "policy_path_absolute_revision_bps" not in factor.DIRECTIONAL_FACTORS
    assert set(factor.ARMS) == {"response_at_1m", "response_at_3m_persist_2", "response_at_5m_persist_3"}


def test_source_factor_tables_are_immutable(tmp_path: Path) -> None:
    database = tmp_path / "factor.sqlite"
    connection = sqlite3.connect(database)
    factor.ensure_schema(connection)
    connection.execute(
        "INSERT INTO fomc_sep_factor_contracts_v1 VALUES(?,?,?,?,?,?,?)",
        ("c", "s", "r", "{}", "b", "i", "2026-08-20T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("UPDATE fomc_sep_factor_contracts_v1 SET contract_json='x' WHERE contract_id='c'")
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM fomc_sep_factor_contracts_v1 WHERE contract_id='c'")
    connection.close()


def test_contract_has_no_execution_policy() -> None:
    assert factor.MINIMUM_SELECTED_EVENTS == 5
    assert factor.MINIMUM_COST_CLEARANCE == 0.55
    assert factor.RANDOMIZATION_DRAWS == 100_000
