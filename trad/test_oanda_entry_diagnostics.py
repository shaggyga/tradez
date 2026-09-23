from types import SimpleNamespace
from unittest.mock import Mock

import oanda_entry_diagnostics as diagnostics
import oanda_practice_shadow_strategy_lab as lab


def test_empty_local_consumer_does_not_report_empty_shared_feed():
    fields = diagnostics.selection_fields([], 349, [{"direction_conflict": True}], {"id": "a"})
    assert fields["local_accepted_candidates"] == 0
    assert fields["shared_feed_candidate_observations"] == 349
    assert fields["accepted_candidates_scope"] == "local_candidates_only_excludes_shared_feed"
    assert fields["direction_conflicting_qualified_candidates"] == 1
    assert fields["final_entry_rules_evaluated"] is False


def test_final_counts_include_candidates_beyond_detail_limit():
    blocks = [{"reason": "direction_conflict_shadow_only"}] * 13 + [{"reason": "multihour_movement_to_cost"}]
    fields = diagnostics.blocked_fields(blocks)
    assert fields["candidate_blocks_total"] == 14
    assert fields["candidate_blocks_shown"] == 12
    assert fields["candidate_block_details_truncated"] is True
    assert fields["candidate_block_reason_counts"] == {
        "direction_conflict_shadow_only": 13, "multihour_movement_to_cost": 1}
    assert fields["authorization_stage_reached"] is False


def test_dashboard_marks_missing_historical_stage_unknown():
    fields = diagnostics.dashboard_fields({"accepted_candidates": 0, "signal_feed_candidates": 280}, {})
    assert fields["latest_shared_feed_candidate_observations"] == 280
    assert fields["latest_candidate_block_reason_counts"] is None
    assert fields["latest_entry_skip_stage"] is None
    assert fields["latest_entry_skip_reason"] is None


def fixture_executor(rows, monkeypatch):
    events = []
    monkeypatch.setattr(lab, "log_line", lambda path, event, **fields: events.append({"event": event, **fields}))
    args = SimpleNamespace(execute_top_signals=True, execution_min_samples=1, execution_top_lanes=10,
        execution_min_average_pips=0, execution_min_median_pips=0, execution_min_win_rate=.5,
        execution_min_lower_confidence_pips=0, execution_min_independent_blocks=1,
        execution_min_holdout_blocks=1, execution_min_pairs=1, execution_min_sessions=1)
    ex = SimpleNamespace(args=args, promotion=None, performance=SimpleNamespace(top=lambda *a: []),
        last_feed_candidate_count=350, last_qualified_candidates=rows, disabled_reason="",
        last_selection_log_monotonic=float("-inf"), log_path=None, last_rank_timings={}, last_feed_cache_stats={},
        ranked_candidate=lambda local: (rows[0] if rows else None, rows),
        write_signal_snapshot=lambda *a: None, compact_execution_log_rows=lambda x: x,
        compact_promotion_log_rows=lambda x: x, manage_open_trades=lambda **kw: [],
        portfolio_blocker=lambda *a: "", reentry_blocker=lambda row, trades: (row.get("block", ""), {}),
        cost_capture_blocker=lambda row: (row.get("cost_block", ""), {}),
        log_execution_skip=lambda **fields: events.append({"event": "execution_skipped", **fields}),
        submit_selected_locked=Mock(side_effect=AssertionError("unexpected order path")))
    return ex, events


def test_actual_executor_exposes_shared_feed_even_when_local_list_empty(monkeypatch):
    ex, events = fixture_executor([], monkeypatch)
    lab.PracticeExecutor.maybe_execute(ex, [])
    summary = events[0]
    assert summary["accepted_candidates"] == summary["local_accepted_candidates"] == 0
    assert summary["shared_feed_candidate_observations"] == 350
    assert summary["pre_final_selection_state"] == "no_qualified_candidate"
    ex.submit_selected_locked.assert_not_called()


def test_actual_executor_retains_all_blocks_and_never_routes(monkeypatch):
    rows = [{"id": str(i), "direction_conflict": True, "block": "direction_conflict_shadow_only"} for i in range(13)]
    rows.append({"id": "cost", "cost_block": "multihour_movement_to_cost"})
    ex, events = fixture_executor(rows, monkeypatch)
    lab.PracticeExecutor.maybe_execute(ex, [])
    skip = events[-1]
    assert skip["reason"] == "no_nonconflicting_signal_capacity"  # compatibility
    assert len(skip["candidate_blocks"]) == 12
    assert skip["candidate_blocks_total"] == 14
    assert skip["candidate_block_reason_counts"]["multihour_movement_to_cost"] == 1
    assert skip["authorization_stage_reached"] is False
    ex.submit_selected_locked.assert_not_called()


def test_passing_candidate_still_reaches_next_unchanged_entry_gate(monkeypatch):
    ex, events = fixture_executor([{"id": "passes", "instrument": "EUR_USD", "direction": "buy"}], monkeypatch)
    ex.args.execution_second_curve_entry_veto = True
    ex.second_curve_state = lambda *args: {"state": "opposed"}
    lab.PracticeExecutor.maybe_execute(ex, [])
    assert events[-1]["reason"] == "second_curve_opposed_entry"
    assert "candidate_block_reason_counts" not in events[-1]
    ex.submit_selected_locked.assert_not_called()
