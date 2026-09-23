from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import trad.oanda_spike_blurb_fomc_statement_factor_response_v1 as factor


def test_policy_text_strips_share_and_vote_material() -> None:
    value = (
        "Share The Committee decided to maintain the target range. "
        "Voting against this action was A. Person, who preferred to lower the rate."
    )
    assert factor.policy_text(value) == "The Committee decided to maintain the target range."
    new = (
        "Share The Federal Open Market Committee approved the following statement for "
        "release by a 12 - 0 vote: The Committee decided to maintain the target range."
    )
    assert factor.policy_text(new) == "The Committee decided to maintain the target range."


def test_ordinal_state_separates_inflation_labor_guidance_and_balance_sheet() -> None:
    hawkish = factor.ordinal_state(
        "Economic activity has expanded at a solid pace. Job gains remain strong and "
        "the unemployment rate remains low. Inflation remains elevated. The Committee "
        "remains highly attentive to inflation risks. Additional policy firming may be "
        "appropriate. The Committee will continue reducing its holdings."
    )
    assert hawkish == {
        "activity_strength": 1, "labor_tightness": 2, "inflation_pressure": 2,
        "inflation_progress": 0, "risk_balance": 2, "guidance_tilt": 2,
        "balance_sheet_tightness": 2,
    }
    easing = factor.ordinal_state(
        "Economic activity expanded at a moderate pace. Job gains have remained low. "
        "Inflation remains somewhat elevated. Downside risks to employment have risen. "
        "The Committee will initiate purchases to maintain an ample supply of reserves."
    )
    assert easing["labor_tightness"] == -2
    assert easing["inflation_pressure"] == 1
    assert easing["risk_balance"] == -2
    assert easing["balance_sheet_tightness"] == -2


@pytest.mark.parametrize(
    "text,actual,expected",
    [
        (
            "Voting against this action was Michelle W. Bowman, who preferred to lower "
            "the target range by 1/4 percentage point at this meeting.",
            -50,
            {"count": 1, "tilt": 1},
        ),
        (
            "Voting against this action were Michelle W. Bowman and Christopher J. Waller, "
            "who preferred to lower the target range by 1/4 percentage point at this meeting.",
            0,
            {"count": 2, "tilt": -1},
        ),
        (
            "Voting against this action were Stephen I. Miran, who preferred to lower the "
            "target range by 1/2 percentage point; and Jeffrey R. Schmid, who preferred no "
            "change to the target range at this meeting.",
            -25,
            {"count": 2, "tilt": 0},
        ),
        (
            "The Federal Open Market Committee approved the following statement for release "
            "by a 9 - 3 vote: The Committee held. Voting against the monetary policy action "
            "were Beth M. Hammack, Neel Kashkari, and Lorie K. Logan, who preferred to raise "
            "the target range by 1/4 percentage point at this meeting.",
            0,
            {"count": 3, "tilt": 1},
        ),
    ],
)
def test_dissent_orientation_is_relative_to_committee_action(
    text: str, actual: int, expected: dict[str, int]
) -> None:
    result = factor.dissent_state(text, actual)
    assert result["dissent_count"] == expected["count"]
    assert result["dissent_tilt"] == expected["tilt"]


def test_factor_mapping_does_not_treat_disinflation_as_hawkish() -> None:
    current = {
        "action_change_bps": 0, "activity_strength": 1, "labor_tightness": 0,
        "inflation_pressure": 1, "inflation_progress": 1, "risk_balance": 0,
        "guidance_tilt": 0, "balance_sheet_tightness": 1,
        "dissent_tilt": 0, "dissent_count": 0, "sep_release_flag": 1,
    }
    prior = {**current, "inflation_progress": 0}
    rows = {row["factor_name"]: row for row in factor.factor_values(
        current, prior,
        {"statement_novelty": 0.1, "added_token_fraction": 0.1, "removed_token_fraction": 0.1},
    )}
    assert rows["inflation_progress_change"]["raw_delta"] == 1
    assert rows["inflation_progress_change"]["signed_factor_score"] == -1
    assert rows["statement_novelty"]["signed_factor_score"] == 0


def test_factor_tables_are_immutable(tmp_path: Path) -> None:
    connection = sqlite3.connect(tmp_path / "factor.sqlite")
    factor.ensure_schema(connection)
    connection.execute(
        "INSERT INTO fomc_statement_factor_contracts VALUES(?,?,?,?,?,?,?,?)",
        ("c", "s", "r", "{}", "b", "{}", "i", "t"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute(
            "UPDATE fomc_statement_factor_contracts SET contract_json='x' WHERE contract_id='c'"
        )
    connection.close()


def test_directional_factor_registry_excludes_context_and_magnitude() -> None:
    assert "statement_novelty" not in factor.DIRECTIONAL_FACTORS
    assert "sep_release_flag" not in factor.DIRECTIONAL_FACTORS
    assert "action_change_bps" in factor.DIRECTIONAL_FACTORS
