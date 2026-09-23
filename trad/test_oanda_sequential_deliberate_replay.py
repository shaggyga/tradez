from __future__ import annotations

import sqlite3

import pytest

import oanda_sequential_deliberate_replay as runner

from src.forex_system.research.sequential_deliberate_replay_v1 import (
    Decision,
    PortfolioState,
    build_situation_fingerprint,
    effective_repetition_count,
    make_repetition_keys,
    proof_record_allowed,
    transition_portfolio,
    valid_action,
    validate_unique_primary_decisions,
)


def _repetition(
    *,
    clock: str,
    episode: str,
    factor: str,
    path: str,
    thesis: str = "thesis-1",
    archetype: str = "momentum",
    lineage: str = "lineage-1",
    **extra: object,
) -> dict[str, object]:
    return {
        "decision_clock_id": clock,
        "market_episode_id": episode,
        "currency_factor_id": factor,
        "price_path_id": path,
        "position_thesis_id": thesis,
        "strategy_archetype_id": archetype,
        "experiment_lineage_id": lineage,
        **extra,
    }


def test_situation_fingerprint_is_causal_and_future_independent() -> None:
    causal = {
        "decision_clock_id": "2026-08-28T12:00:00Z",
        "pair": "EUR_USD",
        "session": "new_york",
        "spread_pips": 1.2,
        "volatility_bucket": "normal",
        "support_distance_pips": 4.0,
        "resistance_distance_pips": 9.0,
        "trend_state": "up",
        "news_state": "official_release_fresh",
        "currency_factor": "USD:-",
        "recent_path": [0.0, 1.0, 1.5],
    }
    winner = {
        **causal,
        "future_return_pips": 25.0,
        "outcome": {"net_pips": 23.8, "mfe_pips": 28.0},
        "future_candles": [{"close": 1.2}],
    }
    loser = {
        **causal,
        "future_return_pips": -25.0,
        "outcome": {"net_pips": -26.2, "mfe_pips": 0.5},
        "future_candles": [{"close": 0.9}],
    }

    assert build_situation_fingerprint(winner) == build_situation_fingerprint(loser)
    assert build_situation_fingerprint(causal) != build_situation_fingerprint(
        {**causal, "spread_pips": 3.2}
    )


def test_exactly_one_primary_action_is_allowed_per_decision_clock() -> None:
    validate_unique_primary_decisions(
        [
            Decision(clock_id="clock-1", action="wait"),
            Decision(
                clock_id="clock-2",
                action="enter",
                pair="EUR_USD",
                side="long",
                units=1,
            ),
        ]
    )

    with pytest.raises(ValueError, match="clock-1"):
        validate_unique_primary_decisions(
            [
                Decision(clock_id="clock-1", action="wait"),
                Decision(
                    clock_id="clock-1",
                    action="enter",
                    pair="EUR_USD",
                    side="long",
                    units=1,
                ),
            ]
        )


def test_action_lifecycle_is_fail_closed() -> None:
    flat = PortfolioState()
    long_eurusd = PortfolioState(position_pair="EUR_USD", side="long", units=2)

    assert valid_action(flat, Decision(clock_id="c1", action="wait"))
    assert valid_action(
        flat,
        Decision(
            clock_id="c2",
            action="enter",
            pair="EUR_USD",
            side="long",
            units=1,
        ),
    )
    assert not valid_action(flat, Decision(clock_id="c3", action="hold"))
    assert not valid_action(flat, Decision(clock_id="c4", action="exit"))
    assert not valid_action(
        flat,
        Decision(
            clock_id="c5",
            action="rotate",
            pair="USD_JPY",
            side="short",
            units=1,
        ),
    )

    assert valid_action(long_eurusd, Decision(clock_id="c6", action="hold"))
    assert valid_action(long_eurusd, Decision(clock_id="c7", action="exit"))
    assert valid_action(
        long_eurusd,
        Decision(
            clock_id="c8",
            action="rotate",
            pair="USD_JPY",
            side="short",
            units=1,
        ),
    )
    assert not valid_action(
        long_eurusd,
        Decision(
            clock_id="c9",
            action="enter",
            pair="GBP_USD",
            side="long",
            units=1,
        ),
    )
    assert not valid_action(
        long_eurusd,
        Decision(
            clock_id="c10",
            action="rotate",
            pair="EUR_USD",
            side="long",
            units=2,
        ),
    )
    assert not valid_action(flat, Decision(clock_id="c11", action="add"))


def test_portfolio_transitions_count_both_rotation_legs() -> None:
    flat = PortfolioState()
    entered = transition_portfolio(
        flat,
        Decision(
            clock_id="c1",
            action="enter",
            pair="EUR_USD",
            side="long",
            units=2,
        ),
    )
    assert entered.execution_legs == 1
    assert entered.new_state == PortfolioState(
        position_pair="EUR_USD", side="long", units=2
    )

    held = transition_portfolio(
        entered.new_state,
        Decision(clock_id="c2", action="hold"),
    )
    assert held.execution_legs == 0
    assert held.new_state == entered.new_state

    rotated = transition_portfolio(
        held.new_state,
        Decision(
            clock_id="c3",
            action="rotate",
            pair="USD_JPY",
            side="short",
            units=3,
        ),
    )
    assert rotated.execution_legs == 2
    assert rotated.new_state == PortfolioState(
        position_pair="USD_JPY", side="short", units=3
    )

    exited = transition_portfolio(
        rotated.new_state,
        Decision(clock_id="c4", action="exit"),
    )
    assert exited.execution_legs == 1
    assert exited.new_state == PortfolioState()


def test_counterfactual_variants_do_not_increase_genuine_repetitions() -> None:
    base = _repetition(
        clock="clock-1",
        episode="episode-1",
        factor="USD:-",
        path="path-1",
    )
    records = [
        {**base, "counterfactual_id": "as_decided", "entry_delay_min": 0},
        {**base, "counterfactual_id": "flipped", "entry_delay_min": 0},
        {**base, "counterfactual_id": "delayed", "entry_delay_min": 5},
        {**base, "counterfactual_id": "alternate_exit", "horizon_min": 30},
    ]

    assert effective_repetition_count(records) == 1
    assert len({tuple(make_repetition_keys(row).items()) for row in records}) == 1


def test_repeat_entry_attempts_on_one_thesis_do_not_manufacture_reps() -> None:
    attempts = [
        _repetition(
            clock="clock-1",
            episode="episode-1",
            factor="JPY:+",
            path="path-1",
            thesis="long-jpy-episode-1",
            attempt_index=1,
        ),
        _repetition(
            clock="clock-2",
            episode="episode-1",
            factor="JPY:+",
            path="path-1",
            thesis="long-jpy-episode-1",
            attempt_index=2,
        ),
        _repetition(
            clock="clock-3",
            episode="episode-1",
            factor="JPY:+",
            path="path-1",
            thesis="long-jpy-episode-1",
            attempt_index=3,
        ),
    ]

    assert len({row["decision_clock_id"] for row in attempts}) == 3
    assert effective_repetition_count(attempts) == 1


def test_factor_episode_and_overlapping_paths_collapse_transitively() -> None:
    records = [
        _repetition(
            clock="clock-1",
            episode="episode-1",
            factor="JPY:+",
            path="path-a",
        ),
        # Same factor in the same episode: another pair is not independent.
        _repetition(
            clock="clock-2",
            episode="episode-1",
            factor="JPY:+",
            path="path-b",
            thesis="thesis-2",
        ),
        # Shared future path links this clock to the first collapsed component.
        _repetition(
            clock="clock-3",
            episode="episode-2",
            factor="EUR:-",
            path="path-b",
            thesis="thesis-3",
        ),
        # Fully independent episode, factor and path.
        _repetition(
            clock="clock-4",
            episode="episode-3",
            factor="AUD:+",
            path="path-c",
            thesis="thesis-4",
        ),
    ]

    assert effective_repetition_count(records) == 2


def test_training_review_quarantine_can_never_reenter_proof() -> None:
    clean = {
        "case_id": "clean-case",
        "partition": "confirmation",
        "reviewed": False,
        "used_for_training": False,
    }
    quarantined = {
        **clean,
        "case_id": "reviewed-case",
    }
    reviewed = {
        **clean,
        "case_id": "reviewed-flag-case",
        "reviewed": True,
    }
    training = {
        **clean,
        "case_id": "training-case",
        "partition": "training",
        "used_for_training": True,
    }

    quarantine = {"reviewed-case"}
    assert proof_record_allowed(clean, quarantine)
    assert not proof_record_allowed(quarantined, quarantine)
    assert not proof_record_allowed(reviewed, quarantine)
    assert not proof_record_allowed(training, quarantine)


def test_append_only_attempt_requires_full_precommitment_and_one_session_case(
    tmp_path,
) -> None:
    database = tmp_path / "replay.sqlite"
    connection = runner.output_connection(database)
    cohort_id = "cohort-1"
    connection.execute(
        "INSERT INTO replay_cohorts VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            cohort_id,
            "2026-08-29T00:00:00+00:00",
            "source-1",
            "a" * 64,
            "b" * 64,
            "c" * 64,
            "d" * 64,
            "e" * 64,
            "f" * 64,
            "{}",
        ),
    )
    connection.execute(
        "INSERT INTO replay_portfolio_cases VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "case-1",
            cohort_id,
            "source-1",
            "clock-1",
            1,
            1,
            "blind-1",
            "historical_training_discovery",
            0,
            "episode-1",
            "situation-1",
            "1" * 64,
            "{}",
            "2" * 64,
        ),
    )
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match="missing"):
        runner.record_attempt(
            database,
            cohort_id=cohort_id,
            session_id="session-1",
            case_id="case-1",
            action="enter",
            pair="EUR_USD",
            side="long",
            units=1,
            confidence=None,
            expected_move_pips=5.0,
            horizon_min=15,
            entry_condition="break",
            invalidation="level fails",
            rationale="test",
        )

    attempt_id = runner.record_attempt(
        database,
        cohort_id=cohort_id,
        session_id="session-1",
        case_id="case-1",
        action="enter",
        pair="EUR_USD",
        side="long",
        units=1,
        confidence=0.6,
        expected_move_pips=5.0,
        horizon_min=15,
        entry_condition="break",
        invalidation="level fails",
        rationale="precommitted fixture",
        committed_utc="2026-08-29T00:01:00+00:00",
    )
    assert attempt_id.startswith("replayattempt_")

    with pytest.raises(ValueError, match="immutable conflict"):
        runner.record_attempt(
            database,
            cohort_id=cohort_id,
            session_id="session-1",
            case_id="case-1",
            action="wait",
            pair=None,
            side=None,
            units=0,
            confidence=None,
            expected_move_pips=None,
            horizon_min=None,
            entry_condition="",
            invalidation="",
            rationale="cannot replace committed decision",
        )

    connection = sqlite3.connect(database)
    with pytest.raises(sqlite3.DatabaseError, match="append_only"):
        connection.execute("UPDATE replay_attempts SET action='wait'")
    connection.close()
