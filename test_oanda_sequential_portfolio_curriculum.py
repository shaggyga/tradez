from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

import oanda_sequential_portfolio_curriculum as runner
import oanda_sequential_portfolio_curriculum_verifier as verifier
import oanda_sequential_portfolio_replay_verifier as source_verifier
from src.forex_system.research.sequential_portfolio_curriculum_v1 import (
    Memory,
    case_identity,
    choose_precommitted_action,
    exact_attempt_identity,
    option_identity,
    select_session_cases,
    update_memory,
)


ROOT = Path(__file__).resolve().parent


def curriculum() -> dict:
    return json.loads((ROOT / "config" / "sequential_portfolio_curriculum_v1.json").read_text(encoding="utf-8"))["curriculum"]


def action(label: str, name: str = "wait", instrument: str | None = None, side: int | None = None) -> dict:
    return {
        "action": name, "instrument": instrument, "side": side,
        "units": 0 if name == "wait" else 1, "branch_label": label,
    }


def cases(count: int = 20) -> list[dict]:
    return [
        {
            "case_id": f"case-{index:02d}",
            "market_episode_id": f"episode-{index // 5}",
            "options": [action("primary"), action("enter", "enter", "EUR_USD", 1)],
        }
        for index in range(count)
    ]


def test_first_session_is_novel_and_repetitions_are_zero_weight() -> None:
    cfg = curriculum()
    first = select_session_cases(
        cases(), {}, session_index=1, attempts_per_session=8, target_review_slots=3,
        seed=cfg["seed"], novelty_weight=2.0, due_error_weight=3.0,
        due_correct_weight=1.0, overdue_weight_per_session=0.25,
    )
    assert len(first) == 8
    assert all(row.selection_reason == "novel_case" for row in first)
    assert all(row.counts_as_market_repetition == 1 for row in first)
    memory = {
        row.case_id: Memory(
            attempt_count=1, last_session_index=1, next_due_session=2,
            last_correct=False, last_regret_pips=2.0,
        )
        for row in first
    }
    second = select_session_cases(
        cases(), memory, session_index=2, attempts_per_session=8, target_review_slots=3,
        seed=cfg["seed"], novelty_weight=2.0, due_error_weight=3.0,
        due_correct_weight=1.0, overdue_weight_per_session=0.25,
    )
    reviews = [row for row in second if row.attempt_ordinal_for_case == 2]
    assert len(reviews) == 3
    assert all(row.counts_as_market_repetition == 0 for row in reviews)
    assert all(row.counts_as_regime_repetition == 0 for row in reviews)


def test_action_is_precommitted_without_current_outcome() -> None:
    case = cases(1)[0]
    before = Memory()
    left = choose_precommitted_action(
        case, before, learner_id="learner", practice_session_id="session", assignment_id="assignment"
    )
    mutated = dict(case, hidden_future_outcome={"profit": 999999})
    right = choose_precommitted_action(
        mutated, before, learner_id="learner", practice_session_id="session", assignment_id="assignment"
    )
    assert left == right


def test_prior_feedback_can_guide_later_session_but_not_current_feedback() -> None:
    case = cases(1)[0]
    remembered = option_identity(case["options"][1])
    memory = Memory(attempt_count=1, last_best_option_id=remembered)
    selected = choose_precommitted_action(
        case, memory, learner_id="learner", practice_session_id="later", assignment_id="assignment"
    )
    assert option_identity(selected) == remembered


def test_attempt_identity_binds_action_and_attempt_ordinal() -> None:
    left = exact_attempt_identity("c", "l", "s", "a", "case", 1, action("wait"))
    changed_action = exact_attempt_identity("c", "l", "s", "a", "case", 1, action("enter", "enter", "EUR_USD", 1))
    changed_ordinal = exact_attempt_identity("c", "l", "s", "a", "case", 2, action("wait"))
    assert len({left, changed_action, changed_ordinal}) == 3


def test_case_identity_binds_causal_snapshot_and_legal_actions() -> None:
    options = [action("primary"), action("enter", "enter", "EUR_USD", 1)]
    left = case_identity("cohort", "session", "clock", "causal-a", options)
    changed_snapshot = case_identity("cohort", "session", "clock", "causal-b", options)
    changed_option = case_identity("cohort", "session", "clock", "causal-a", options + [action("short", "enter", "EUR_USD", -1)])
    assert len({left, changed_snapshot, changed_option}) == 3


def test_spacing_interval_expands_only_after_correct_recall() -> None:
    incorrect = update_memory(
        Memory(), session_index=1, regret_pips=2.0, correct=False,
        best_option_id="best", incorrect_delay=1, correct_base_interval=2, maximum_interval=8,
    )
    first_correct = update_memory(
        incorrect, session_index=2, regret_pips=0.0, correct=True,
        best_option_id="best", incorrect_delay=1, correct_base_interval=2, maximum_interval=8,
    )
    second_correct = update_memory(
        first_correct, session_index=4, regret_pips=0.0, correct=True,
        best_option_id="best", incorrect_delay=1, correct_base_interval=2, maximum_interval=8,
    )
    assert incorrect.next_due_session == 2
    assert first_correct.next_due_session == 4
    assert second_correct.next_due_session == 8


def test_append_only_curriculum_tables_reject_mutation(tmp_path: Path) -> None:
    connection = runner.output_connection(tmp_path / "curriculum.sqlite")
    try:
        connection.execute(
            "INSERT INTO spc_cohorts VALUES (?,?,?,?,?,?)",
            ("c", "now", "source", "session", "hash", "{}"),
        )
        with pytest.raises(sqlite3.DatabaseError, match="append_only"):
            connection.execute("UPDATE spc_cohorts SET created_utc='later' WHERE cohort_id='c'")
        with pytest.raises(sqlite3.DatabaseError, match="append_only"):
            connection.execute("DELETE FROM spc_cohorts WHERE cohort_id='c'")
    finally:
        connection.close()


def test_canonical_curriculum_is_idempotent_and_independently_verified() -> None:
    first = runner.run()
    second = runner.run()
    assert first["cohort_id"] == second["cohort_id"]
    assert first["session_seal_id"] == second["session_seal_id"]
    assert first["roots"] == second["roots"]
    assert first["attempt_count"] == 48
    assert first["distinct_case_count"] == 36
    assert first["repeated_attempt_count"] == 12
    assert first["distinct_market_repetition_count"] == 36
    assert first["structural_episode_count"] == 4
    assert first["proof_eligible"] is False
    receipt = verifier.verify()
    assert receipt["verified"] is True, receipt["failures"]
    assert receipt["failures"] == []


def test_verifier_has_no_producer_or_replay_import() -> None:
    assert verifier.forbidden_imports() == []


def test_upstream_verifier_timestamp_refresh_does_not_fork_cohort() -> None:
    before = runner.run()
    assert source_verifier.verify()["verified"] is True
    after = runner.run()
    assert after["cohort_id"] == before["cohort_id"]
    assert after["contract_sha256"] == before["contract_sha256"]


@pytest.mark.parametrize(
    ("key", "forged"),
    [
        ("research_only", False),
        ("broker_access", True),
        ("account_access", True),
        ("supported_decision", "trade"),
    ],
)
def test_verifier_rejects_forged_safety_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str, forged: object,
) -> None:
    state = runner.run()
    tampered = dict(state)
    tampered[key] = forged
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(tampered), encoding="utf-8")
    monkeypatch.setattr(verifier, "STATE", state_path)
    monkeypatch.setattr(verifier, "OUTPUT", tmp_path / "receipt.json")
    receipt = verifier.verify()
    assert receipt["verified"] is False
    assert f"unsafe_state_{key}" in receipt["failures"]


@pytest.mark.parametrize(
    ("key", "forged"),
    [
        ("attempt_count", 999999),
        ("correct_attempt_count", 999999),
        ("mean_regret_pips", 999.0),
        ("mistake_counts", {"forged": 999999}),
    ],
)
def test_verifier_rejects_forged_summary_statistics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str, forged: object,
) -> None:
    state = runner.run()
    tampered = dict(state)
    tampered[key] = forged
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(tampered), encoding="utf-8")
    monkeypatch.setattr(verifier, "STATE", state_path)
    monkeypatch.setattr(verifier, "OUTPUT", tmp_path / "receipt.json")
    receipt = verifier.verify()
    assert receipt["verified"] is False
    assert f"state_stat_{key}" in receipt["failures"]


def test_curriculum_timestamp_contract_is_content_bound_and_reproducible() -> None:
    state = runner.run()
    connection = sqlite3.connect(state["database"])
    try:
        contract = json.loads(connection.execute(
            "SELECT contract_json FROM spc_cohorts WHERE cohort_id=?",
            (state["cohort_id"],),
        ).fetchone()[0])
        timestamp = contract["timestamp_contract"]["value"]
        for table, column in (
            ("spc_sessions", "precommitted_utc"),
            ("spc_attempts", "committed_utc"),
            ("spc_feedback", "revealed_utc"),
            ("spc_session_seals", "created_utc"),
            ("spc_snapshots", "generated_utc"),
        ):
            values = {
                row[0] for row in connection.execute(
                    f"SELECT DISTINCT {column} FROM {table} WHERE cohort_id=?",
                    (state["cohort_id"],),
                )
            }
            assert values == {timestamp}
    finally:
        connection.close()
