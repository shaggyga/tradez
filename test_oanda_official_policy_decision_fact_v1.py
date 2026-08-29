from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import oanda_official_policy_decision_fact_v1 as facts


SEPTEMBER_HOLD = b"""
<html><head><title>Statement of the Monetary Policy Committee September 2026</title></head>
<body>
<p>The committee decided to keep the policy rate unchanged at 7%.</p>
<p>Four members preferred a hold, while two favoured an increase of 25 basis points.</p>
</body></html>
"""

SEPTEMBER_HIKE = b"""
<html><head><title>Statement of the Monetary Policy Committee September 2026</title></head>
<body>
<p>The committee decided to increase the policy rate by 25 basis points to 7.25%.</p>
<p>Three members preferred a hold, while three favoured an increase of 25 basis points.</p>
</body></html>
"""


def parsed_fixture(*, action: str = "hike") -> dict:
    delta = 25 if action != "hold" else 0
    current = 7.25 if action == "hike" else 6.75 if action == "cut" else 7.0
    votes = (
        {"count": 3, "preference": "hold", "basis_points": 0},
        {"count": 3, "preference": action if action != "hold" else "hike", "basis_points": 25},
    )
    return {
        "source_id": facts.sarb.SOURCE_ID,
        "source_contract_id": facts.sarb.CONTRACT_ID,
        "source_cohort_id": facts.sarb.COHORT_ID,
        "event_id": "sarb_mpc_decision:2026-09-23",
        "event_date": "2026-09-23",
        "currency": "ZAR",
        "action": action,
        "policy_rate_pct": current,
        "decision_basis_points": delta,
        "votes": votes,
        "vote_total_observed": 6,
        "scheduled_utc": "2026-09-23T13:00:00+00:00",
        "statement_first_seen_utc": "2026-09-23T13:00:11+00:00",
        "decision_causal_known_utc": "2026-09-23T13:00:11+00:00",
        "prospective_observation": True,
        "statement_source_url": (
            "https://www.resbank.co.za/en/home/publications/"
            "publication-detail-pages/statements/monetary-policy-statements/2026/september"
        ),
        "statement_source_sha256": "a" * 64,
        "decision_facts_sha256": "b" * 64,
        "independent_episode_key": "ZAR:sarb_mpc_decision:2026-09-23",
    }


def test_worker_contract_is_research_only_and_exact_source_bound() -> None:
    config = facts.read_config()
    manifest = facts.validate_source_contract(config)
    assert manifest["source_id"] == facts.sarb.SOURCE_ID
    assert config["broker_access"] is False
    assert config["execution_eligible"] is False
    assert config["can_authorize"] is False
    assert config["can_promote"] is False
    assert config["direction_policy"] == "abstain"
    assert config["fact_policy"]["generic_vote_regex_allowed"] is False


@pytest.mark.parametrize(
    ("action", "current", "prior", "signed_delta"),
    (
        ("hike", 7.25, 7.0, 25),
        ("hold", 7.0, 7.0, 0),
        ("cut", 6.75, 7.0, -25),
    ),
)
def test_normalized_fact_derives_explicit_prior_and_signed_delta(
    action: str,
    current: float,
    prior: float,
    signed_delta: int,
) -> None:
    row = facts.normalized_fact(parsed_fixture(action=action))
    assert row["action"] == action
    assert row["current_policy_rate_pct"] == current
    assert row["prior_policy_rate_pct"] == prior
    assert row["signed_decision_delta_bps"] == signed_delta
    assert row["prior_rate_derivation"] == "current_rate_minus_signed_decision_delta"
    assert row["publication_clock_basis"] == "collector_first_seen_floor"
    assert row["direction"] is None
    assert row["supported_decision"] == "abstain"


def test_incomplete_or_generic_vote_fact_is_rejected() -> None:
    row = parsed_fixture()
    row["votes"] = "6-3"
    with pytest.raises(facts.PolicyDecisionFactError, match="structured_votes_missing"):
        facts.normalized_fact(row)
    row = parsed_fixture()
    row.pop("decision_basis_points")
    with pytest.raises(facts.PolicyDecisionFactError, match="decision_basis_points_missing"):
        facts.normalized_fact(row)


def test_causal_clock_must_equal_schedule_first_seen_floor() -> None:
    row = parsed_fixture()
    row["decision_causal_known_utc"] = "2026-09-23T13:00:05+00:00"
    with pytest.raises(facts.PolicyDecisionFactError, match="causal_clock_mismatch"):
        facts.normalized_fact(row)


def test_append_only_schema_blocks_update_and_delete(tmp_path: Path) -> None:
    connection = sqlite3.connect(tmp_path / "facts.sqlite")
    facts.ensure_schema(connection)
    config = facts.read_config()
    facts.register_contract_and_schedule(
        connection, config, facts.parse_utc("2026-08-27T14:00:00+00:00")
    )
    normalized = facts.normalized_fact(parsed_fixture())
    assert facts.insert_fact(
        connection, normalized, facts.parse_utc("2026-09-23T13:00:12+00:00")
    )
    with pytest.raises(sqlite3.IntegrityError, match="append_only"):
        connection.execute(
            "UPDATE official_policy_decision_facts SET action='hold' WHERE fact_id=?",
            (normalized["fact_id"],),
        )
    connection.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="append_only"):
        connection.execute(
            "DELETE FROM official_policy_decision_facts WHERE fact_id=?",
            (normalized["fact_id"],),
        )
    connection.close()


def test_one_cycle_before_event_collects_schedule_but_never_fetches(
    tmp_path: Path,
) -> None:
    called = False

    def forbidden_fetch(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("not due")

    payload = facts.run_cycle(
        now=facts.parse_utc("2026-08-27T14:00:00+00:00"),
        database=tmp_path / "facts.sqlite",
        output=tmp_path / "state.json",
        report=tmp_path / "report.md",
        fetcher=forbidden_fetch,
    )
    assert called is False
    assert payload["counts"]["scheduled_events"] == 3
    assert payload["counts"]["facts"] == 0
    assert payload["cycle"]["due_events"] == 0
    assert payload["policy"]["supported_decision"] == "collect_structured_policy_facts_only"
    assert payload["generic_vote_regex_used"] is False
    assert json.loads((tmp_path / "state.json").read_text(encoding="utf-8")) == payload


def test_due_exact_sarb_decision_creates_one_complete_prospective_fact(
    tmp_path: Path,
) -> None:
    def fixture_fetch(url: str, **kwargs):
        assert url.endswith("/2026/september")
        return 200, SEPTEMBER_HIKE

    now = facts.parse_utc("2026-09-23T13:00:11+00:00")
    payload = facts.run_cycle(
        now=now,
        database=tmp_path / "facts.sqlite",
        output=tmp_path / "state.json",
        report=tmp_path / "report.md",
        fetcher=fixture_fetch,
    )
    assert payload["counts"]["facts"] == 1
    assert payload["counts"]["prospective_complete_facts"] == 1
    connection = sqlite3.connect(tmp_path / "facts.sqlite")
    row = connection.execute(
        "SELECT action,current_policy_rate_pct,prior_policy_rate_pct,"
        "signed_decision_delta_bps,votes_json,publication_clock_basis,"
        "research_only,execution_eligible,can_authorize,can_promote "
        "FROM official_policy_decision_facts"
    ).fetchone()
    connection.close()
    assert row[:4] == ("hike", 7.25, 7.0, 25)
    assert json.loads(row[4]) == [
        {"basis_points": 0, "count": 3, "preference": "hold"},
        {"basis_points": 25, "count": 3, "preference": "hike"},
    ]
    assert row[5:] == ("collector_first_seen_floor", 1, 0, 0, 0)


def test_source_contains_no_generic_news_classifier_or_execution_import() -> None:
    source = Path(facts.__file__).read_text(encoding="utf-8")
    assert "extract_policy_vote" not in source
    assert "oanda_local_news_sentiment" not in source
    assert "requests.post" not in source
    assert "create_order" not in source
    assert "close_trade" not in source
