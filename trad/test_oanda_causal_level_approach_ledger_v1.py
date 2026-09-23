from __future__ import annotations

import ast
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import oanda_causal_level_approach_ledger_v1 as subject


UTC = timezone.utc


def bar(index: int, mid: float, *, high: float | None = None, low: float | None = None, spread: float = 0.0002) -> subject.Bar:
    half = spread / 2
    high = mid + 0.00015 if high is None else high
    low = mid - 0.00015 if low is None else low
    at = datetime(2026, 8, 1, tzinfo=UTC) + timedelta(minutes=5 * index)
    return subject.Bar(at, mid-half, high-half, low-half, mid-half, mid+half, high+half, low+half, mid+half)


def fixture_bars() -> list[subject.Bar]:
    values = [1.0000 + 0.00005 * i for i in range(45)]
    rows = [bar(i, value) for i, value in enumerate(values)]
    # Confirmed resistance pivot at 10; later approach occurs well after index 12.
    rows[10] = bar(10, 1.0010, high=1.0020, low=1.0008)
    rows[9] = bar(9, 1.0007, high=1.0012)
    rows[11] = bar(11, 1.0008, high=1.0013)
    return rows


def test_pivot_is_unavailable_until_right_confirmation_bar_is_complete() -> None:
    bars = fixture_bars()
    assert not any(p[0] == 10 for p in subject.confirmed_pivots(bars, 11))
    assert any(p[0] == 10 and p[3] == 12 for p in subject.confirmed_pivots(bars, 12))


def test_cluster_level_id_is_stable_when_a_new_touch_extends_cluster() -> None:
    bars = [bar(i, 1.0) for i in range(30)]
    bars[5] = bar(5, 1.0, high=1.0020)
    bars[4] = bar(4, 1.0, high=1.0010)
    bars[6] = bar(6, 1.0, high=1.0010)
    bars[12] = bar(12, 1.0, high=1.00205)
    bars[11] = bar(11, 1.0, high=1.0010)
    bars[13] = bar(13, 1.0, high=1.0010)
    first = [row for row in subject.build_levels(bars, 10, 0.001) if row.kind == "resistance"]
    extended = [row for row in subject.build_levels(bars, 16, 0.001) if row.kind == "resistance"]
    assert first and extended
    original = next(row for row in first if abs(row.price - 1.0020) < 1e-9)
    later = next(row for row in extended if row.touches == 2)
    assert later.level_id == original.level_id
    assert later.price != original.price


def test_completed_week_pivots_use_dst_aware_friday_1700_new_york_boundary() -> None:
    # Week ending Friday 2026-03-13 17:00 EDT == 21:00Z; DST changed Mar 8.
    start = datetime(2026, 3, 8, 21, 0, tzinfo=UTC)
    rows = []
    for index in range(5 * 24 * 12):
        at = start + timedelta(minutes=5 * index)
        one = bar(index, 1.10 + index * 0.000001)
        rows.append(subject.Bar(at, one.bid_open, one.bid_high, one.bid_low, one.bid_close, one.ask_open, one.ask_high, one.ask_low, one.ask_close))
    # Decision is after the completed Friday boundary; final bar after boundary
    # must not affect prior-week H/L/C.
    after = bar(20, 1.50, high=1.60, low=1.40)
    rows.append(subject.Bar(datetime(2026, 3, 16, 12, tzinfo=UTC), after.bid_open, after.bid_high, after.bid_low, after.bid_close, after.ask_open, after.ask_high, after.ask_low, after.ask_close))
    week_start, week_end = subject._last_completed_fx_week(rows[-1].known_at)
    assert week_start == datetime(2026, 3, 8, 21, tzinfo=UTC)
    assert week_end == datetime(2026, 3, 13, 21, tzinfo=UTC)
    levels = subject.prior_completed_week_pivot_levels("EUR_USD", rows, len(rows) - 1)
    assert {row.name for row in levels} == {"P", "R1", "R2", "S1", "S2"}
    assert {row.source for row in levels} == {"prior_completed_week_traditional_pivot"}
    assert all(row.price < 1.20 for row in levels)  # post-boundary 1.50 excluded


def completed_week_rows() -> list[subject.Bar]:
    start = datetime(2026, 3, 8, 21, tzinfo=UTC)
    rows = []
    for index in range(5 * 24 * 12):
        one = bar(index, 1.10)
        rows.append(subject.Bar(start + timedelta(minutes=5 * index), one.bid_open, one.bid_high, one.bid_low, one.bid_close, one.ask_open, one.ask_high, one.ask_low, one.ask_close))
    later = bar(2000, 1.11)
    rows.append(subject.Bar(datetime(2026, 3, 16, 12, tzinfo=UTC), later.bid_open, later.bid_high, later.bid_low, later.bid_close, later.ask_open, later.ask_high, later.ask_low, later.ask_close))
    return rows


def test_weekly_pivots_accept_natural_quote_gaps_and_persist_integrity() -> None:
    rows = completed_week_rows()
    # Sunday opens ten minutes late and three isolated bars are absent.
    del rows[:2]
    for index in sorted((900, 500, 100), reverse=True):
        del rows[index]
    levels = subject.prior_completed_week_pivot_levels("EUR_USD", rows, len(rows) - 1)
    assert len(levels) == 5
    assert {row.weekly_missing_bars for row in levels} == {5}
    assert {row.weekly_max_gap_minutes for row in levels} == {10.0}
    assert all(row.weekly_coverage_ratio == pytest.approx(1435 / 1440) for row in levels)
    assert {row.weekly_boundary_rule for row in levels} == {subject.WEEKLY_BOUNDARY_RULE}
    features = subject.frozen_features("EUR_USD", rows, len(rows) - 1, levels[0], 0)
    assert features["weekly_missing_bars"] == 5
    assert features["weekly_boundary_rule"] == subject.WEEKLY_BOUNDARY_RULE


@pytest.mark.parametrize("failure", ["late_open", "missing_close", "low_coverage", "large_gap", "partial_week"])
def test_weekly_pivots_reject_boundary_and_coverage_failures(failure: str) -> None:
    rows = completed_week_rows()
    if failure == "late_open":
        del rows[:4]  # Sunday 17:20 NY is too late.
    elif failure == "missing_close":
        del rows[-2]  # Preserve later decision row but remove Friday 16:55 open.
    elif failure == "low_coverage":
        del rows[100:150]  # Below 97% and also an excessive gap.
    elif failure == "large_gap":
        del rows[500:513]  # 70-minute gap, despite otherwise high coverage.
    else:
        # A Jul-9 partial week has no exact completed Friday boundary.
        start = datetime(2026, 7, 5, 21, tzinfo=UTC)
        partial = []
        for index in range(4 * 24 * 12):
            one = bar(index, 1.10)
            partial.append(subject.Bar(start + timedelta(minutes=5 * index), one.bid_open, one.bid_high, one.bid_low, one.bid_close, one.ask_open, one.ask_high, one.ask_low, one.ask_close))
        later = bar(2000, 1.11)
        partial.append(subject.Bar(datetime(2026, 7, 13, 12, tzinfo=UTC), later.bid_open, later.bid_high, later.bid_low, later.bid_close, later.ask_open, later.ask_high, later.ask_low, later.ask_close))
        rows = partial
    assert subject.prior_completed_week_pivot_levels("EUR_USD", rows, len(rows) - 1) == []


def test_features_are_frozen_before_next_entry_and_do_not_read_outcome() -> None:
    bars = fixture_bars()
    level = subject.Level("L", 1.0020, "resistance", 10, 10, 12, 1)
    first = subject.frozen_features("EUR_USD", bars, 25, level, 0)
    mutated = list(bars)
    mutated[26] = bar(26, 1.1000)
    second = subject.frozen_features("EUR_USD", mutated, 25, level, 0)
    assert first == second
    assert first["feature_cutoff_utc"] == subject.iso(bars[25].known_at)
    assert first["level_known_at_utc"] == subject.iso(bars[12].known_at)
    assert first["level_age_bars"] == 13


def test_bid_ask_paths_charge_spread_and_keep_both_sides() -> None:
    bars = [bar(i, 1.0000, spread=0.0002) for i in range(20)]
    level = subject.Level("L", 1.0010, "resistance", 1, 1, 3, 1)
    outcome = subject.mature_outcome("EUR_USD", bars, 1, level, 0.001, 5)
    assert outcome["long_path"]["terminal_pips"] == pytest.approx(-2.0)
    assert outcome["short_path"]["terminal_pips"] == pytest.approx(-2.0)
    assert outcome["selected_side"] is None
    assert outcome["long_path"]["cost_clear_time_observation"] == "m5_interval_censored"
    assert "time_to_cost_clear_seconds" not in outcome["long_path"]


@pytest.mark.parametrize(
    ("future", "expected"),
    [
        ([1.0000, 0.9992, 0.9990], "reject-away-before-break"),
        ([1.0013, 1.0014, 1.0015], "penetrate-and-hold"),
        ([1.0013, 1.0005, 1.0004], "false-break-reclaim"),
        ([1.0008, 1.0009, 1.0008], "neither"),
    ],
)
def test_labels_are_mutually_exclusive(future: list[float], expected: str) -> None:
    rows = [bar(i, 1.0008) for i in range(20)]
    for offset, value in enumerate(future):
        rows[5 + offset] = bar(5 + offset, value, high=value, low=value)
    level = subject.Level("L", 1.0010, "resistance", 1, 1, 3, 1)
    result = subject.mature_outcome("EUR_USD", rows, 5, level, 0.001, 15)
    assert result["label"] == expected
    assert result["label"] in {"reject-away-before-break", "penetrate-and-hold", "false-break-reclaim", "ambiguous_intrabar", "neither"}


def test_first_rejection_beats_a_later_break() -> None:
    rows = [bar(i, 1.0008) for i in range(20)]
    rows[5] = bar(5, 1.0000, high=1.0005, low=0.9999)
    rows[6] = bar(6, 1.0014, high=1.0015, low=1.0012)
    level = subject.Level("L", 1.0010, "resistance", 1, 1, 3, 1)
    result = subject.mature_outcome("EUR_USD", rows, 5, level, 0.001, 15)
    assert result["label"] == "reject-away-before-break"
    assert result["first_passage_bar_offset"] == 0


def test_same_bar_two_barriers_is_ambiguous_intrabar() -> None:
    rows = [bar(i, 1.0008) for i in range(20)]
    rows[5] = bar(5, 1.0010, high=1.0015, low=1.0000)
    level = subject.Level("L", 1.0010, "resistance", 1, 1, 3, 1)
    result = subject.mature_outcome("EUR_USD", rows, 5, level, 0.001, 5)
    assert result["label"] == "ambiguous_intrabar"


def test_barriers_are_level_anchored_and_clear_spread_floor() -> None:
    rows = [bar(i, 1.0, spread=0.0004) for i in range(20)]
    level = subject.Level("L", 1.0020, "resistance", 1, 1, 3, 1)
    result = subject.mature_outcome("EUR_USD", rows, 5, level, 0.0002, 5)
    assert result["barrier_scale_price"] == pytest.approx(0.0006)
    assert result["break_barrier_price"] == pytest.approx(1.0026)
    assert result["reject_barrier_price"] == pytest.approx(1.0014)


def test_outcome_requires_exact_contiguous_horizon_clock() -> None:
    rows = [bar(i, 1.0) for i in range(20)]
    broken = list(rows)
    original = broken[7]
    broken[7] = subject.Bar(original.timestamp + timedelta(minutes=5), original.bid_open, original.bid_high, original.bid_low, original.bid_close, original.ask_open, original.ask_high, original.ask_low, original.ask_close)
    level = subject.Level("L", 1.0010, "resistance", 1, 1, 3, 1)
    with pytest.raises(ValueError, match="outcome_not_mature"):
        subject.mature_outcome("EUR_USD", broken, 5, level, 0.001, 15)


def test_rearm_requires_leaving_level_zone(monkeypatch) -> None:
    bars = [bar(i, 1.0000) for i in range(80)]
    level = subject.Level("fixed", 1.0001, "resistance", 1, 1, 3, 1)
    monkeypatch.setattr(subject, "build_levels", lambda *args: [level])
    monkeypatch.setattr(subject, "atr_at", lambda *args: 0.001)
    monkeypatch.setattr(subject, "select_approached_level", lambda *args: level)
    # Always near and approaching: without the arm, this would emit every bar.
    episodes = subject.scan_instrument("EUR_USD", bars)
    assert len(episodes) == 1


def test_csv_replay_never_self_certifies_future_prospective_rows(monkeypatch) -> None:
    bars = [bar(i, 1.0000) for i in range(80)]
    level = subject.Level("fixed", 1.0001, "resistance", 1, 1, 3, 1)
    monkeypatch.setattr(subject, "build_levels", lambda *args: [level])
    monkeypatch.setattr(subject, "atr_at", lambda *args: 0.001)
    monkeypatch.setattr(subject, "select_approached_level", lambda *args: level)
    episodes = subject.scan_instrument("EUR_USD", bars)
    assert episodes
    assert {row["evidence_class"] for row in episodes} == {"historical_diagnostic"}
    assert not any(row["prospective_proof_eligible"] for row in episodes)
    assert "historical_diagnostic" in subject.COHORT_ID
    assert "prospective" not in subject.COHORT_ID
    with pytest.raises(TypeError):
        subject.scan_instrument("EUR_USD", bars, prospective_live_capture=True)


def test_m1_aggregation_uses_only_five_complete_bars() -> None:
    rows = []
    start = datetime(2026, 8, 1, tzinfo=UTC)
    for index in range(6):
        mid = 1.0 + index * 0.0001
        one = bar(index, mid)
        rows.append(subject.Bar(start + timedelta(minutes=index), one.bid_open, one.bid_high, one.bid_low, one.bid_close, one.ask_open, one.ask_high, one.ask_low, one.ask_close))
    aggregated = subject.aggregate_completed_m1_to_m5(rows)
    assert len(aggregated) == 1
    assert aggregated[0].timestamp == start
    assert aggregated[0].bid_open == rows[0].bid_open
    assert aggregated[0].bid_close == rows[4].bid_close


def test_append_only_and_idempotent(tmp_path) -> None:
    connection = subject.open_database(tmp_path / "ledger.sqlite")
    episode = {
        "episode_id": "e", "instrument": "EUR_USD", "decision_utc": "2026-08-01T00:00:00Z",
        "entry_utc": "2026-08-01T00:05:00Z", "evidence_class": "historical_diagnostic",
        "prospective_proof_eligible": False, "features": {"x": 1},
        "outcomes": [{"horizon_min": 5, "label": "neither", "entry_utc": "2026-08-01T00:05:00Z", "maturity_utc": "2026-08-01T00:10:00Z"}],
    }
    try:
        assert subject.insert_episodes(connection, [episode]) == {"episodes": 1, "outcomes": 1}
        assert subject.insert_episodes(connection, [episode]) == {"episodes": 0, "outcomes": 0}
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("UPDATE level_approach_episode SET instrument='X'")
    finally:
        connection.close()


def test_policy_and_imports_have_no_operational_surface() -> None:
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    assert not any(token in name.lower() for name in imports for token in ("broker", "execution", "authorization", "promotion", "watchlist"))
    assert subject.POLICY["research_only"] is True
    assert subject.POLICY["execution_eligible"] is False
    assert subject.POLICY["selected_pair_or_side"] is False
