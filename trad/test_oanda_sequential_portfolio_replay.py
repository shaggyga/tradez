from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import sqlite3

import numpy as np
import pytest

import oanda_sequential_portfolio_replay as runner
import oanda_sequential_portfolio_replay_verifier as verifier
from src.forex_system.research.sequential_portfolio_replay_v1 import (
    Decision,
    MarketSeries,
    PortfolioState,
    apply_decision,
    build_global_clocks,
    candidate_set,
    legal_branches,
    liquidation_equity,
)


ROOT = Path(__file__).resolve().parent


def config() -> dict:
    return json.loads((ROOT / "config" / "sequential_portfolio_replay_v1.json").read_text(encoding="utf-8"))


def synthetic_market(
    instrument: str,
    mids: list[float],
    *,
    first_epoch: int = 0,
    spread_pips: float = 2.0,
) -> MarketSeries:
    pip = 0.01 if instrument.endswith("_JPY") else 0.0001
    mid = np.asarray(mids, dtype=float)
    half = spread_pips * pip / 2.0
    bid = mid - half
    ask = mid + half
    epochs = np.asarray([first_epoch + 60 * index for index in range(len(mids))], dtype=np.int64)
    return MarketSeries(
        instrument=instrument,
        pip=pip,
        epochs=epochs,
        bid_open=bid.copy(),
        bid_high=bid.copy(),
        bid_low=bid.copy(),
        bid_close=bid.copy(),
        ask_open=ask.copy(),
        ask_high=ask.copy(),
        ask_low=ask.copy(),
        ask_close=ask.copy(),
        mid_close=mid.copy(),
        source_sha256=f"source-{instrument}",
    )


def test_clock_schedule_is_causal_and_does_not_require_future_fill_quote() -> None:
    market = synthetic_market("EUR_USD", [1.10 + index * 0.00001 for index in range(65)])
    clocks = build_global_clocks(
        {"EUR_USD": market},
        start_epoch=3900,
        end_epoch=4200,
        cadence_min=5,
        feature_lookback_min=60,
        execution_delay_min=1,
        feedback_horizon_min=5,
    )
    assert clocks == [3900]
    assert market.exact_index(3960) is None


def test_future_mutation_cannot_change_candidate_at_cutoff() -> None:
    base = [1.10 + index * 0.00002 for index in range(70)]
    altered = list(base)
    altered[66:] = [1.50, 1.60, 1.70, 1.80]
    left = synthetic_market("EUR_USD", base)
    right = synthetic_market("EUR_USD", altered)
    decision_epoch = 65 * 60
    left_candidate = candidate_set({"EUR_USD": left}, decision_epoch, config())
    right_candidate = candidate_set({"EUR_USD": right}, decision_epoch, config())
    assert left_candidate == right_candidate


def test_long_and_short_use_executable_sides_and_per_leg_slippage() -> None:
    market = synthetic_market("EUR_USD", [1.1000, 1.1005, 1.1010], spread_pips=2.0)
    markets = {"EUR_USD": market}
    long_entry = Decision("enter", "EUR_USD", 1, 1, 0.6, 5.0, 5, "x", "y", "z", "c")
    opened = apply_decision(
        PortfolioState(), long_entry, markets, execution_epoch=60,
        slippage_per_leg_pips=0.125, decision_clock_id="clock-long",
        maximum_entry_spread_pips=5.0,
    )
    closed = apply_decision(
        opened.state,
        Decision("exit", "EUR_USD", None, 0, None, None, None, "", "", "", None),
        markets, execution_epoch=120, slippage_per_leg_pips=0.125,
        decision_clock_id="clock-close",
    )
    expected_long = ((1.1010 - 0.0001 - 0.0000125) - (1.1005 + 0.0001 + 0.0000125)) / 0.0001
    assert closed.state.realized_pips == pytest.approx(expected_long)
    short_entry = replace(long_entry, side=-1)
    short_open = apply_decision(
        PortfolioState(), short_entry, markets, execution_epoch=60,
        slippage_per_leg_pips=0.125, decision_clock_id="clock-short",
        maximum_entry_spread_pips=5.0,
    )
    short_close = apply_decision(
        short_open.state,
        Decision("exit", "EUR_USD", None, 0, None, None, None, "", "", "", None),
        markets, execution_epoch=120, slippage_per_leg_pips=0.125,
        decision_clock_id="clock-short-close",
    )
    expected_short = ((1.1005 - 0.0001 - 0.0000125) - (1.1010 + 0.0001 + 0.0000125)) / 0.0001
    assert short_close.state.realized_pips == pytest.approx(expected_short)


def test_rotation_is_atomic_two_leg_close_then_open() -> None:
    markets = {
        "EUR_USD": synthetic_market("EUR_USD", [1.10, 1.101, 1.102]),
        "GBP_CHF": synthetic_market("GBP_CHF", [1.08, 1.079, 1.078], spread_pips=3.0),
    }
    entered = apply_decision(
        PortfolioState(),
        Decision("enter", "EUR_USD", 1, 1, 0.6, 4.0, 5, "x", "y", "z", "a"),
        markets, execution_epoch=0, slippage_per_leg_pips=0.125,
        decision_clock_id="entry", maximum_entry_spread_pips=5.0,
    )
    rotated = apply_decision(
        entered.state,
        Decision("rotate", "GBP_CHF", -1, 1, 0.6, 4.0, 5, "x", "y", "z", "b"),
        markets, execution_epoch=60, slippage_per_leg_pips=0.125,
        decision_clock_id="rotate", maximum_entry_spread_pips=5.0,
    )
    assert [leg.leg_kind for leg in rotated.legs] == ["close", "open"]
    assert rotated.state.position.instrument == "GBP_CHF"
    assert rotated.state.position.side == -1


def test_wide_new_entry_rejects_without_mutation_but_exit_is_allowed() -> None:
    wide = synthetic_market("GBP_CHF", [1.08, 1.081], spread_pips=7.5)
    market = synthetic_market("EUR_USD", [1.10, 1.101], spread_pips=2.0)
    markets = {"GBP_CHF": wide, "EUR_USD": market}
    entered = apply_decision(
        PortfolioState(),
        Decision("enter", "EUR_USD", 1, 1, 0.6, 4.0, 5, "x", "y", "z", "a"),
        markets, execution_epoch=0, slippage_per_leg_pips=0.125,
        decision_clock_id="entry", maximum_entry_spread_pips=5.0,
    )
    rejected = apply_decision(
        entered.state,
        Decision("rotate", "GBP_CHF", -1, 1, 0.6, 4.0, 5, "x", "y", "z", "b"),
        markets, execution_epoch=60, slippage_per_leg_pips=0.125,
        decision_clock_id="rotation", maximum_entry_spread_pips=5.0,
    )
    assert rejected.status == "rejected"
    assert rejected.legs == ()
    assert rejected.state == entered.state
    exited = apply_decision(
        entered.state,
        Decision("exit", "EUR_USD", None, 0, None, None, None, "", "", "", None),
        markets, execution_epoch=60, slippage_per_leg_pips=0.125,
        decision_clock_id="exit", maximum_entry_spread_pips=0.1,
    )
    assert exited.status == "applied"
    assert exited.state.flat


def test_counterfactual_branches_do_not_mutate_primary_state() -> None:
    cfg = config()
    cfg["frozen_policy"]["minimum_score_cost_ratio"] = 0.0
    market = synthetic_market("EUR_USD", [1.10 + index * 0.00003 for index in range(70)])
    rows = candidate_set({"EUR_USD": market}, 65 * 60, cfg)
    primary = Decision("enter", "EUR_USD", 1, 1, 0.6, 4.0, 5, "x", "y", "z", rows[0].snapshot_id)
    state = PortfolioState()
    alternatives = legal_branches(state, primary, rows, cfg)
    before = state
    for branch in alternatives:
        result = apply_decision(
            state, branch, {"EUR_USD": market}, execution_epoch=66 * 60,
            slippage_per_leg_pips=0.125, decision_clock_id="clock",
            maximum_entry_spread_pips=5.0,
        )
        liquidation_equity(result.state, {"EUR_USD": market}, epoch=69 * 60, slippage_per_leg_pips=0.125)
    assert state == before


def test_append_only_tables_block_update_and_delete(tmp_path: Path) -> None:
    connection = runner.output_connection(tmp_path / "portfolio.sqlite")
    try:
        connection.execute(
            "INSERT INTO spr_cohorts VALUES (?,?,?,?,?)",
            ("c", "now", "s", "h", "{}"),
        )
        with pytest.raises(sqlite3.DatabaseError, match="append_only"):
            connection.execute("UPDATE spr_cohorts SET created_utc='later' WHERE cohort_id='c'")
        with pytest.raises(sqlite3.DatabaseError, match="append_only"):
            connection.execute("DELETE FROM spr_cohorts WHERE cohort_id='c'")
    finally:
        connection.close()


def test_sqlite_snapshot_sees_committed_wal_not_uncommitted(tmp_path: Path) -> None:
    path = tmp_path / "wal.sqlite"
    writer = sqlite3.connect(path)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("PRAGMA wal_autocheckpoint=0")
    writer.execute("CREATE TABLE sample(value INTEGER)")
    writer.commit()
    writer.execute("INSERT INTO sample VALUES (1)")
    writer.commit()
    writer.execute("BEGIN")
    writer.execute("INSERT INTO sample VALUES (2)")
    snapshot = verifier.sqlite_snapshot(path)
    try:
        assert [row[0] for row in snapshot.execute("SELECT value FROM sample")] == [1]
    finally:
        snapshot.close()
        writer.rollback()
        writer.close()


def test_canonical_pilot_is_idempotent_and_independently_verified() -> None:
    first = runner.run()
    second = runner.run()
    assert first["cohort_id"] == second["cohort_id"]
    assert first["roots"] == second["roots"]
    assert first["session_seal_id"] == second["session_seal_id"]
    assert first["global_clock_count"] == 48
    assert first["pair_context_count"] == 192
    assert set(first["action_counts"]) == {"wait", "enter", "hold", "exit", "rotate"}
    assert first["terminal_flat"] is True
    receipt = verifier.verify()
    assert receipt["verified"] is True, receipt["failures"]
    assert receipt["failures"] == []
