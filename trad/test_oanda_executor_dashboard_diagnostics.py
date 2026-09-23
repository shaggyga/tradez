"""Offline dashboard receipt tests: synthetic log tails, no workers or databases."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

import oanda_practice_live_dashboard as dashboard


NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc).timestamp()
PREFIX = "practice_top_signal_executor_oanda_account_id_dum4_"
SESSION = PREFIX + "20260906_115900_123456"


def event(name, age=0, **fields):
    return {"event": name, "time": datetime.fromtimestamp(NOW - age, timezone.utc).isoformat(), **fields}


def write_log(directory, rows, *, stem=SESSION, mtime=NOW):
    path = directory / (stem + ".jsonl")
    path.write_bytes(b"".join(json.dumps(row).encode() + b"\n" for row in rows))
    os.utime(path, (mtime, mtime))
    return path


def read(directory):
    return dashboard.summarize_executor_entry_diagnostics(directory, now_epoch=NOW)


def test_missing_receipts_are_unknown_not_zero(tmp_path):
    result = read(tmp_path)
    assert result["status"] == "missing"
    for key in ("current_selection", "current_skip", "executor_running", "authorization_current", "order_attempt_count", "fill_count"):
        assert result[key] is None


def test_actual_shared_feed_shape_does_not_replace_it_with_empty_local_count(tmp_path):
    write_log(tmp_path, [event("execution_selection_summary", 10, accepted_candidates=0,
        signal_feed_candidates=349, qualified_candidates=5, nonconflicting_qualified_candidates=1)])
    result = read(tmp_path)
    selection = result["current_selection"]
    assert selection["shared_feed_candidate_observations"] == 349
    assert selection["local_accepted_candidates"] == 0
    assert selection["qualified_candidates"] == 5
    assert selection["pre_final_selection_state"] is None
    assert result["current_skip"] is None
    assert result["executor_running"] is None


def test_full_reason_counts_keep_total_beyond_truncated_details(tmp_path):
    write_log(tmp_path, [event("execution_selection_summary", 20, signal_feed_candidates=349),
        event("execution_skipped", 10, reason="no_nonconflicting_signal_capacity",
            entry_stage="final_candidate_rules", candidate_blocks=[{"reason": "direction_conflict_shadow_only"}] * 12,
            candidate_block_reason_counts={"direction_conflict_shadow_only": 13, "multihour_movement_to_cost": 1},
            candidate_blocks_total=14, candidate_block_details_truncated=True, authorization_stage_reached=False)])
    result = read(tmp_path)
    skip = result["current_skip"]
    assert skip["candidate_block_counts_scope"] == "all_candidate_blocks_in_this_skip"
    assert sum(skip["candidate_block_reason_counts"].values()) == 14
    assert skip["candidate_blocks_total"] == 14
    assert skip["entry_stage"] == "final_candidate_rules"
    assert skip["authorization_stage_reached"] is False
    assert result["authorization_current"] is None
    assert result["selection_skip_same_cycle"] is None


def test_legacy_nested_counts_are_shown_only_and_stage_remains_unknown(tmp_path):
    write_log(tmp_path, [event("execution_skipped", 10, reason="no_nonconflicting_signal_capacity",
        candidate_blocks=[{"reason": "direction_conflict_shadow_only"}] * 12)])
    skip = read(tmp_path)["current_skip"]
    assert skip["candidate_block_counts_scope"] == "shown_only_not_total"
    assert skip["candidate_block_reason_counts"] == {"direction_conflict_shadow_only": 12}
    for key in ("candidate_blocks_total", "candidate_block_details_truncated", "authorization_stage_reached", "entry_stage"):
        assert skip[key] is None


@pytest.mark.parametrize("new_event", ["execution_selection_summary", "execution_selected", "practice_order_filled", "practice_order_not_filled", "practice_order_error", "fast_executor_start"])
def test_newer_observation_invalidates_old_skip(tmp_path, new_event):
    write_log(tmp_path, [event("execution_skipped", 20, reason="old_block"), event(new_event, 10)])
    result = read(tmp_path)
    assert result["latest_skip"]["reason"] == "old_block"
    assert result["current_skip"] is None
    assert result["current_skip_state"] in {"superseded_by_newer_observation", "previous_session"}


def test_empty_new_session_does_not_carry_previous_skip_forward(tmp_path):
    old = PREFIX + "20260906_115800_000001"
    write_log(tmp_path, [event("execution_skipped", 10, reason="old_session")], stem=old, mtime=NOW - 5)
    write_log(tmp_path, [])
    result = read(tmp_path)
    assert result["status"] == "unknown"
    assert result["latest_skip"]["reason"] == "old_session"
    assert result["current_skip_state"] == "previous_session"
    assert result["current_skip"] is None


def test_rotation_keeps_session_but_new_selection_supersedes_prior_part_skip(tmp_path):
    write_log(tmp_path, [event("execution_skipped", 20, reason="old_part")],
        stem=SESSION + ".part_20260906T115950_000001Z_0001", mtime=NOW - 5)
    write_log(tmp_path, [event("execution_selection_summary", 10, signal_feed_candidates=349)])
    result = read(tmp_path)
    assert len(result["sources"]) == 2
    assert result["latest_skip"]["session"] == result["current_selection"]["session"]
    assert result["current_skip_state"] == "superseded_by_newer_observation"


@pytest.mark.parametrize("age,status", [(181, "stale"), (-1, "future_timestamp")])
def test_old_and_future_receipts_never_become_current(tmp_path, age, status):
    write_log(tmp_path, [event("execution_selection_summary", age, signal_feed_candidates=349),
        event("execution_skipped", age, reason="blocked")])
    result = read(tmp_path)
    assert result["status"] == status
    assert result["current_selection"] is None
    assert result["current_skip"] is None
    assert result["latest_skip"]["reason"] == "blocked"


def test_wrong_account_and_naive_clock_are_not_receipts(tmp_path):
    write_log(tmp_path, [event("execution_skipped", 1, account_suffix="-006"),
        {"event": "execution_skipped", "time": "2026-09-06T11:59:59"}])
    result = read(tmp_path)
    assert result["status"] == "unknown"
    assert result["latest_skip"] is None


def test_reader_opens_only_newest_two_matching_files_with_hard_byte_bound(tmp_path, monkeypatch):
    for index in range(4):
        path = write_log(tmp_path, [], stem=PREFIX + f"20260906_11590{index}_000001", mtime=NOW - 4 + index)
        path.write_bytes(b"x" * (2 * dashboard.EXECUTOR_DIAGNOSTIC_TAIL_BYTES)
                         + b"\n" + json.dumps(event("execution_skipped", 10, reason=str(index))).encode() + b"\n")
        os.utime(path, (NOW - 4 + index, NOW - 4 + index))
    other = tmp_path / "practice_top_signal_executor_oanda_account_id_dum3_20260906_120000_000001.jsonl"
    other.write_text("must not open", encoding="utf-8")
    real_open = Path.open
    opened, requests = [], []

    class ObservedReader:
        def __init__(self, handle):
            self.handle = handle
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.handle.close()
        def seek(self, *args):
            return self.handle.seek(*args)
        def tell(self):
            return self.handle.tell()
        def read(self, size=-1):
            requests.append(size)
            assert 0 <= size <= 1024 * 1024
            return self.handle.read(size)

    def observed_open(path, *args, **kwargs):
        assert path != other
        opened.append(path)
        return ObservedReader(real_open(path, *args, **kwargs))

    monkeypatch.setattr(Path, "open", observed_open)
    result = read(tmp_path)
    assert len(opened) == len(requests) == 2
    assert sum(row["bytes_read"] for row in result["sources"]) == 2 * 1024 * 1024
    assert all(row["tail_truncated"] for row in result["sources"])
    assert result["current_skip"]["reason"] == "3"


def test_partial_tail_and_invalid_utf8_do_not_create_receipts(tmp_path):
    path = write_log(tmp_path, [event("execution_selection_summary", 20)])
    with path.open("ab") as handle:
        handle.write(b"\xff\n")
        handle.write(json.dumps(event("execution_skipped", 1, reason="uncommitted")).encode())
    result = read(tmp_path)
    assert result["current_selection"] is not None
    assert result["latest_skip"] is None
    assert result["sources"][0]["invalid_lines"] == 1


def test_build_state_exposes_actual_executor_diagnostics_at_both_payload_paths(tmp_path, monkeypatch):
    write_log(tmp_path, [event("execution_skipped", 10, reason="actual_fast_executor")])
    monkeypatch.setattr(dashboard.time, "time", lambda: NOW)
    monkeypatch.setattr(dashboard, "discover_logs", lambda *args: [])
    monkeypatch.setattr(dashboard, "discover_lab_logs", lambda *args: [])
    for constant in ("MICRO_SNAPSHOT", "ACCOUNT_SNAPSHOT", "ACCOUNT_007_SNAPSHOT"):
        monkeypatch.setattr(dashboard, constant, tmp_path / "nonexistent")
    for function in ("load_json_dict", "summarize_second_forecast_matrix", "summarize_model_inventory",
                     "summarize_strategy_layers", "summarize_historical_calibration",
                     "summarize_post_gap_execution", "summarize_signal_feed", "live_research_counts"):
        monkeypatch.setattr(dashboard, function, lambda *args, **kwargs: {})
    result = dashboard.build_state(tmp_path, 1, 10)
    assert result["entry_diagnostics"] is result["primary_signal_system"]["entry_diagnostics"]
    assert result["entry_diagnostics"]["current_skip"]["reason"] == "actual_fast_executor"
    assert result["strategy_lab"] is None


def test_main_state_exposes_diagnostics_without_starting_or_reading_runtime(tmp_path, monkeypatch):
    expected = {"source_scope": "practice_007_fast_executor_log_tails", "status": "missing"}
    monkeypatch.setattr(dashboard, "summarize_executor_entry_diagnostics", lambda path: expected)
    monkeypatch.setattr(dashboard, "discover_lab_logs", lambda *args: [])
    for constant in ("ACCOUNT_SNAPSHOT", "ACCOUNT_007_SNAPSHOT", "SIGNAL_SNAPSHOT", "LOCAL_NEWS_SENTIMENT"):
        monkeypatch.setattr(dashboard, constant, tmp_path / "nonexistent")
    for function in ("heartbeat_status", "load_json_dict", "resolve_display_signals", "summarize_post_gap_execution",
                     "summarize_live_movers", "summarize_live_move_news", "summarize_continuous_narrative",
                     "summarize_adaptive_level_bands"):
        monkeypatch.setattr(dashboard, function, lambda *args, **kwargs: {})
    monkeypatch.setattr(dashboard, "summarize_primary_practice_account", lambda *args: {"ok": False, "snapshot_age_sec": None})
    result = dashboard.build_main_state(tmp_path)
    assert result["entry_diagnostics"] is expected
    assert result["active"] is False
