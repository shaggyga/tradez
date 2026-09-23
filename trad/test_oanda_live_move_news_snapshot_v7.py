from __future__ import annotations

import copy
import sqlite3
from pathlib import Path

import pytest

from trad import oanda_live_move_news_snapshot_v6 as v6
from trad import oanda_live_move_news_snapshot_v7 as v7


def _row(
    instrument: str,
    start_utc: str,
    end_utc: str,
    *,
    primary: str,
    move_bps: float,
    net_pips: float,
) -> dict[str, object]:
    return {
        "instrument": instrument,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "move_direction": "up",
        "factor_primary_token": primary,
        "factor_primary_method": "causal_all68_currency_strength",
        "factor_primary_ambiguous": False,
        "move_bps": move_bps,
        "executable_net_pips": net_pips,
    }


def test_pln_rows_across_centered_boundary_share_one_episode() -> None:
    rows = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.439,
            net_pips=17.1,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:08:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=8.443,
            net_pips=17.0,
        ),
    ]
    count = v7.assign_overlap_factor_episodes(rows)
    assert count == 1
    assert rows[0]["factor_episode_id"] == rows[1]["factor_episode_id"]
    assert rows[0]["factor_episode_anchor_utc"].endswith("08:07:00+00:00")
    assert rows[0]["factor_episode_member_count"] == 2
    assert rows[1]["factor_representative"] is True
    assert rows[0]["factor_representative"] is False


def test_adjacent_same_primary_intervals_share_episode() -> None:
    rows = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:18:00+00:00",
            "2026-08-27T08:24:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
    ]
    assert v7.assign_overlap_factor_episodes(rows) == 1
    assert rows[0]["factor_episode_id"] == rows[1]["factor_episode_id"]


def test_truly_disjoint_same_primary_intervals_stay_separate() -> None:
    rows = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:20:00+00:00",
            "2026-08-27T08:24:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
    ]
    assert v7.assign_overlap_factor_episodes(rows) == 2
    assert rows[0]["factor_episode_id"] != rows[1]["factor_episode_id"]
    assert all(bool(row["factor_representative"]) for row in rows)


def test_overlapping_different_primary_factors_stay_separate() -> None:
    rows = [
        _row(
            "USD_PLN",
            "2026-08-27T08:08:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
        _row(
            "USD_ZAR",
            "2026-08-27T08:09:00+00:00",
            "2026-08-27T08:19:00+00:00",
            primary="ZAR-",
            move_bps=10.0,
            net_pips=100.0,
        ),
    ]
    assert v7.assign_overlap_factor_episodes(rows) == 2
    assert rows[0]["factor_episode_id"] != rows[1]["factor_episode_id"]


def test_interval_components_are_input_order_invariant() -> None:
    forward = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:08:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
        _row(
            "GBP_PLN",
            "2026-08-27T08:18:00+00:00",
            "2026-08-27T08:22:00+00:00",
            primary="PLN-",
            move_bps=6.0,
            net_pips=8.0,
        ),
    ]
    reverse = list(reversed(copy.deepcopy(forward)))
    assert v7.assign_overlap_factor_episodes(forward) == 1
    assert v7.assign_overlap_factor_episodes(reverse) == 1
    forward_ids = {
        str(row["instrument"]): str(row["factor_episode_id"]) for row in forward
    }
    reverse_ids = {
        str(row["instrument"]): str(row["factor_episode_id"]) for row in reverse
    }
    assert forward_ids == reverse_ids


def test_noncausal_primary_fallback_never_cross_pair_deduplicates() -> None:
    rows = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:08:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
    ]
    for row in rows:
        row["factor_primary_method"] = "time_bucket_token_recurrence_fallback"
    v7.normalize_primary_factor_assignments(rows)
    assert v7.assign_overlap_factor_episodes(rows) == 2
    assert all(
        row["factor_primary_method"] == "unresolved_without_causal_strength"
        for row in rows
    )
    assert rows[0]["factor_episode_id"] != rows[1]["factor_episode_id"]


def test_v7_history_is_separate_append_only_and_contract_bound(tmp_path: Path) -> None:
    history = tmp_path / "cases_v7.sqlite"
    meter = tmp_path / "meter.sqlite"
    rows = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:08:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
    ]
    v7.assign_overlap_factor_episodes(rows)
    result = v7.record_cases(
        history,
        rows,
        recorded_utc="2026-08-27T08:30:00+00:00",
        meter_database=meter,
    )
    assert result == {
        "inserted": 2,
        "total": 2,
        "new_membership_conflicts": 0,
        "membership_conflict_total": 0,
    }
    with sqlite3.connect(history) as connection:
        contract = connection.execute(
            "SELECT snapshot_contract_id,previous_snapshot_contract_id,"
            "grouping_method,maximum_gap_sec,research_only,execution_eligible "
            "FROM factor_episode_contract_registry"
        ).fetchone()
        assert contract == (
            v7.CONTRACT_ID,
            v6.CONTRACT_ID,
            "fixed_onset_primary_then_same_primary_interval_overlap_or_adjacency",
            60,
            1,
            0,
        )
        assert connection.execute(
            "SELECT COUNT(DISTINCT factor_episode_id) FROM factor_episode_membership"
        ).fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("DELETE FROM factor_episode_membership")


def test_membership_change_is_logged_and_fails_integrity_closed(tmp_path: Path) -> None:
    history = tmp_path / "cases_v7.sqlite"
    meter = tmp_path / "meter.sqlite"
    row = _row(
        "USD_PLN",
        "2026-08-27T08:08:00+00:00",
        "2026-08-27T08:17:00+00:00",
        primary="PLN-",
        move_bps=8.0,
        net_pips=12.0,
    )
    v7.assign_overlap_factor_episodes([row])
    first = v7.record_cases(
        history,
        [row],
        recorded_utc="2026-08-27T08:30:00+00:00",
        meter_database=meter,
    )
    assert first["membership_conflict_total"] == 0

    changed = copy.deepcopy(row)
    changed["factor_episode_id"] = "live_factor_v7_deliberate_conflict"
    second = v7.record_cases(
        history,
        [changed],
        recorded_utc="2026-08-27T08:31:00+00:00",
        meter_database=meter,
    )
    assert second == {
        "inserted": 0,
        "total": 1,
        "new_membership_conflicts": 1,
        "membership_conflict_total": 1,
    }
    assert changed["factor_episode_membership_conflict"] is True
    assert changed["persisted_factor_episode_id"] == row["factor_episode_id"]


def test_fixed_onset_primary_cutoff_does_not_follow_rolling_move_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_end_clocks: list[str] = []

    def fake_surfaces(
        rows: list[dict[str, object]], history: list[dict[str, object]]
    ) -> dict[tuple[int, int], dict[str, object]]:
        del history
        captured_end_clocks.extend(str(row["end_utc"]) for row in rows)
        return {}

    def fake_assign(
        rows: list[dict[str, object]], **_: object
    ) -> int:
        for row in rows:
            row.update(
                {
                    "factor_tokens": ["USD+", "PLN-"],
                    "factor_primary_token": "PLN-",
                    "factor_primary_method": "causal_all68_currency_strength",
                    "factor_primary_margin": 3.0,
                    "factor_primary_margin_unit": "bps",
                    "factor_primary_ambiguous": False,
                }
            )
        return 1

    monkeypatch.setattr(v7.v5, "build_causal_factor_strength_surfaces", fake_surfaces)
    monkeypatch.setattr(v7.v5, "assign_factor_episodes", fake_assign)
    rows = [
        _row(
            "USD_PLN",
            "2026-08-27T08:08:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
        _row(
            "EUR_PLN",
            "2026-08-27T08:08:00+00:00",
            "2026-08-27T08:35:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
    ]
    v7.assign_fixed_onset_primary_factors(rows, [])
    assert captured_end_clocks == [
        "2026-08-27T08:13:00+00:00",
        "2026-08-27T08:13:00+00:00",
    ]
    assert all(
        row["factor_primary_method"]
        == "causal_all68_currency_strength_fixed_onset_5m"
        for row in rows
    )
    assert all(row["factor_primary_token"] == "PLN-" for row in rows)


def test_existing_episode_root_is_reused_when_adjacent_case_arrives() -> None:
    rows = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:18:00+00:00",
            "2026-08-27T08:24:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
    ]
    rows[0]["case_id"] = "existing"
    rows[1]["case_id"] = "new"
    frozen = {
        "existing": {
            "factor_episode_id": "frozen_pln_root",
            "factor_primary_token": "PLN-",
        }
    }
    assert v7.assign_overlap_factor_episodes(
        rows, frozen_memberships=frozen
    ) == 1
    assert {row["factor_episode_id"] for row in rows} == {"frozen_pln_root"}
    assert not any(row["factor_episode_merge_conflict"] for row in rows)


def test_bridge_between_two_frozen_roots_fails_closed() -> None:
    rows = [
        _row(
            "EUR_PLN",
            "2026-08-27T08:07:00+00:00",
            "2026-08-27T08:17:00+00:00",
            primary="PLN-",
            move_bps=7.0,
            net_pips=10.0,
        ),
        _row(
            "USD_PLN",
            "2026-08-27T08:18:00+00:00",
            "2026-08-27T08:24:00+00:00",
            primary="PLN-",
            move_bps=8.0,
            net_pips=12.0,
        ),
    ]
    rows[0]["case_id"] = "left"
    rows[1]["case_id"] = "right"
    frozen = {
        "left": {
            "factor_episode_id": "root_a",
            "factor_primary_token": "PLN-",
        },
        "right": {
            "factor_episode_id": "root_b",
            "factor_primary_token": "PLN-",
        },
    }
    assert v7.assign_overlap_factor_episodes(
        rows, frozen_memberships=frozen
    ) == 1
    assert all(row["factor_episode_merge_conflict"] for row in rows)
    assert all(
        row["factor_episode_persisted_roots"] == ["root_a", "root_b"]
        for row in rows
    )


def test_v7_does_not_reuse_v6_outputs_or_broker_surface() -> None:
    assert v7.DEFAULT_OUTPUT != v6.DEFAULT_OUTPUT
    assert v7.DEFAULT_HISTORY != v6.DEFAULT_HISTORY
    assert v7.DEFAULT_REPORT != v6.DEFAULT_REPORT
    assert v7.CONTRACT_ID != v6.CONTRACT_ID
    source = Path(v7.__file__).read_text(encoding="utf-8").lower()
    assert "requests." not in source
    assert "oanda-api-v20" not in source
    assert "can_place_orders\": false" in source
