from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import trad.oanda_spike_blurb_fomc_statement_factor_response_v2 as factor


def test_activity_does_not_cross_into_labor_sentence() -> None:
    state = factor.ordinal_state(
        "Economic activity has continued to expand at a solid pace. "
        "Job gains have slowed, and the unemployment rate remains low."
    )
    assert state["activity_strength"] == 1
    assert state["labor_tightness"] == -1


def test_moderated_activity_precedes_later_slowed_jobs() -> None:
    state = factor.ordinal_state(
        "Growth of economic activity moderated in the first half of the year. "
        "Job gains have slowed."
    )
    assert state["activity_strength"] == 0


def test_downside_employment_risk_accepts_rose() -> None:
    state = factor.ordinal_state(
        "The Committee is attentive to risks to both sides and judges that "
        "downside risks to employment rose in recent months."
    )
    assert state["risk_balance"] == -2
    assert state["labor_tightness"] == -2


def test_runoff_dissent_is_hawkish_tightness() -> None:
    state = factor.dissent_state(
        "Voting against this action was Christopher J. Waller, who supported no "
        "change for the federal funds target range but preferred to continue the "
        "current pace of decline in securities holdings.",
        0,
    )
    assert state["dissent_count"] == 1
    assert state["dissent_hawkish"] == 1
    assert state["dissent_tilt"] == 1


def test_v2_tables_are_immutable(tmp_path: Path) -> None:
    connection = sqlite3.connect(tmp_path / "factor-v2.sqlite")
    factor.ensure_schema(connection)
    connection.execute(
        "INSERT INTO fomc_statement_factor_v2_contracts VALUES(?,?,?,?,?,?,?,?,?)",
        ("c", "p", "s", "r", "{}", "b", "pb", "i", "t"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute(
            "UPDATE fomc_statement_factor_v2_contracts SET contract_json='x' WHERE contract_id='c'"
        )
    connection.close()
