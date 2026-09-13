from __future__ import annotations

import sqlite3
from pathlib import Path

from trad import oanda_live_move_news_snapshot_v7 as v7r2
from trad import oanda_live_move_news_snapshot_v7r3 as v7r3


def _row(
    instrument: str,
    start_utc: str,
    end_utc: str,
    *,
    primary: str,
    move_bps: float = 5.0,
) -> dict[str, object]:
    row: dict[str, object] = {
        "instrument": instrument,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "move_direction": "up",
        "contract_id": v7r3.CONTRACT_ID,
        "factor_primary_token": primary,
        "factor_primary_method": (
            "causal_all68_currency_strength_fixed_onset_5m"
        ),
        "factor_primary_ambiguous": False,
        "move_bps": move_bps,
        "executable_net_pips": move_bps,
        "research_only": True,
        "execution_eligible": False,
    }
    row["case_id"] = v7r3.stable_case_id(row)
    return row


def _assign_and_record(
    database: Path,
    rows: list[dict[str, object]],
    observed: str,
) -> dict[str, object]:
    registry = v7r3.load_union_registry(database)
    v7r3.freeze_observed_primaries(rows, registry)
    assignment = v7r3.assign_transitive_factor_episodes(
        rows, registry, detected_utc=observed
    )
    history = v7r3.record_cases(
        database,
        rows,
        recorded_utc=observed,
        meter_database=database.with_name("meter.sqlite"),
        planned_merges=assignment["planned_merges"],
    )
    return {"assignment": assignment, "history": history}


def test_exact_chf_hkd_nzd_hkd_topn_fragmentation_unions_without_rewrite(
    tmp_path: Path,
) -> None:
    database = tmp_path / "cases_v7r3.sqlite"
    chf = _row(
        "CHF_HKD",
        "2026-08-27T08:35:00+00:00",
        "2026-08-27T08:48:00+00:00",
        primary="HKD-",
        move_bps=8.0,
    )
    nzd = _row(
        "NZD_HKD",
        "2026-08-27T08:36:00+00:00",
        "2026-08-27T08:43:00+00:00",
        primary="HKD-",
        move_bps=7.0,
    )
    first = _assign_and_record(
        database, [chf], "2026-08-27T08:42:48+00:00"
    )
    assert first["assignment"]["canonical_episode_count"] == 1
    chf_raw_root = str(chf["factor_episode_id"])

    second = _assign_and_record(
        database, [nzd], "2026-08-27T08:48:41+00:00"
    )
    assert second["assignment"]["canonical_episode_count"] == 1
    nzd_raw_root = str(nzd["factor_episode_id"])
    assert nzd_raw_root != chf_raw_root

    bridged = [dict(chf), dict(nzd)]
    third = _assign_and_record(
        database, bridged, "2026-08-27T08:50:21+00:00"
    )
    assert third["assignment"]["canonical_episode_count"] == 1
    assert third["assignment"]["planned_merge_count"] == 1
    assert third["history"]["inserted_root_merges"] == 1
    assert third["history"]["membership_conflict_total"] == 0
    assert third["history"]["graph_cycle"] is False
    assert len({str(row["factor_episode_id"]) for row in bridged}) == 1

    with sqlite3.connect(database) as connection:
        raw_memberships = connection.execute(
            "SELECT factor_episode_id FROM factor_episode_membership "
            "ORDER BY case_id"
        ).fetchall()
        assert len({str(row[0]) for row in raw_memberships}) == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM factor_episode_root_merges"
        ).fetchone()[0] == 1
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    registry = v7r3.load_union_registry(database)
    canonical = {
        str(value["canonical_root_id"])
        for value in registry["memberships"].values()
    }
    assert len(canonical) == 1
    assert registry["cycle"] is False


def test_transitive_root_merges_only_reduce_effective_n() -> None:
    rows = [
        _row(
            "CHF_HKD",
            "2026-08-27T08:35:00+00:00",
            "2026-08-27T08:48:00+00:00",
            primary="HKD-",
        ),
        _row(
            "NZD_HKD",
            "2026-08-27T08:36:00+00:00",
            "2026-08-27T08:43:00+00:00",
            primary="HKD-",
        ),
    ]
    memberships = {
        str(rows[0]["case_id"]): {
            "raw_root_id": "root-a",
            "canonical_root_id": "root-a",
            "factor_primary_token": "HKD-",
            "registered_utc": "2026-08-27T08:40:00+00:00",
        },
        str(rows[1]["case_id"]): {
            "raw_root_id": "root-c",
            "canonical_root_id": "root-b",
            "factor_primary_token": "HKD-",
            "registered_utc": "2026-08-27T08:42:00+00:00",
        },
    }
    registry = {
        "memberships": memberships,
        "edges": {"root-c": "root-b"},
        "root_registered_utc": {
            "root-a": "2026-08-27T08:40:00+00:00",
            "root-b": "2026-08-27T08:42:00+00:00",
        },
        "cycle": False,
    }
    assignment = v7r3.assign_transitive_factor_episodes(
        rows,
        registry,
        detected_utc="2026-08-27T08:50:00+00:00",
    )
    assert assignment["canonical_episode_count"] == 1
    assert assignment["planned_merges"] == [
        {
            "from_root_id": "root-b",
            "into_root_id": "root-a",
            "factor_primary_token": "HKD-",
            "bridge_case_ids": ",".join(
                sorted(str(row["case_id"]) for row in rows)
            ),
        }
    ]
    combined_edges = dict(registry["edges"])
    combined_edges.update(
        {
            merge["from_root_id"]: merge["into_root_id"]
            for merge in assignment["planned_merges"]
        }
    )
    assert v7r3.resolve_root("root-c", combined_edges) == ("root-a", False)
    raw_n = 3
    canonical_n = len(
        {v7r3.resolve_root(root, combined_edges)[0] for root in {"root-a", "root-b", "root-c"}}
    )
    assert canonical_n == 1
    assert canonical_n <= raw_n


def test_same_pass_cascading_unions_publish_only_final_canonical_roots() -> None:
    rows = [
        _row(
            "USD_HUF",
            "2026-08-27T00:00:00+00:00",
            "2026-08-27T00:01:00+00:00",
            primary="HUF-",
        ),
        _row(
            "EUR_HUF",
            "2026-08-27T00:00:30+00:00",
            "2026-08-27T00:02:00+00:00",
            primary="HUF-",
        ),
        _row(
            "GBP_HUF",
            "2026-08-27T00:10:00+00:00",
            "2026-08-27T00:11:00+00:00",
            primary="HUF-",
        ),
        _row(
            "CHF_HUF",
            "2026-08-27T00:10:30+00:00",
            "2026-08-27T00:12:00+00:00",
            primary="HUF-",
        ),
    ]
    raw_roots = ("root-a", "root-c", "root-a", "root-d")
    registered = {
        "root-a": "2026-08-27T01:00:00+00:00",
        "root-c": "2026-08-27T03:00:00+00:00",
        "root-d": "2026-08-27T00:00:00+00:00",
    }
    registry = {
        "memberships": {
            str(row["case_id"]): {
                "raw_root_id": root,
                "canonical_root_id": root,
                "factor_primary_token": "HUF-",
                "registered_utc": registered[root],
            }
            for row, root in zip(rows, raw_roots, strict=True)
        },
        "edges": {},
        "root_registered_utc": registered,
        "cycle": False,
    }

    assignment = v7r3.assign_transitive_factor_episodes(
        rows,
        registry,
        detected_utc="2026-08-27T04:00:00+00:00",
    )

    assert assignment["planned_merges"] == [
        {
            "from_root_id": "root-c",
            "into_root_id": "root-a",
            "factor_primary_token": "HUF-",
            "bridge_case_ids": ",".join(
                sorted(str(row["case_id"]) for row in rows[:2])
            ),
        },
        {
            "from_root_id": "root-a",
            "into_root_id": "root-d",
            "factor_primary_token": "HUF-",
            "bridge_case_ids": ",".join(
                sorted(str(row["case_id"]) for row in rows[2:])
            ),
        },
    ]
    assert assignment["canonical_episode_count"] == 1
    assert {str(row["factor_episode_canonical_root_id"]) for row in rows} == {
        "root-d"
    }
    assert {str(row["factor_episode_id"]) for row in rows} == {"root-d"}


def test_different_primary_and_disjoint_intervals_do_not_merge() -> None:
    different_primary = [
        _row(
            "CHF_HKD",
            "2026-08-27T08:35:00+00:00",
            "2026-08-27T08:48:00+00:00",
            primary="HKD-",
        ),
        _row(
            "USD_CHF",
            "2026-08-27T08:36:00+00:00",
            "2026-08-27T08:43:00+00:00",
            primary="USD-",
        ),
    ]
    result = v7r3.assign_transitive_factor_episodes(
        different_primary, {}, detected_utc="2026-08-27T08:50:00+00:00"
    )
    assert result["canonical_episode_count"] == 2
    assert result["planned_merge_count"] == 0

    disjoint = [
        _row(
            "CHF_HKD",
            "2026-08-27T08:35:00+00:00",
            "2026-08-27T08:40:00+00:00",
            primary="HKD-",
        ),
        _row(
            "NZD_HKD",
            "2026-08-27T08:43:00+00:00",
            "2026-08-27T08:48:00+00:00",
            primary="HKD-",
        ),
    ]
    result = v7r3.assign_transitive_factor_episodes(
        disjoint, {}, detected_utc="2026-08-27T08:50:00+00:00"
    )
    assert result["canonical_episode_count"] == 2
    assert result["planned_merge_count"] == 0


def test_v7r3_is_separate_inert_and_live_diagnostic_bound() -> None:
    assert v7r3.CONTRACT_ID != v7r2.CONTRACT_ID
    assert v7r3.DEFAULT_OUTPUT != v7r2.DEFAULT_OUTPUT
    assert v7r3.DEFAULT_HISTORY != v7r2.DEFAULT_HISTORY
    assert v7r3.DEFAULT_REPORT != v7r2.DEFAULT_REPORT
    source = Path(v7r3.__file__).read_text(encoding="utf-8").lower()
    assert "requests." not in source
    assert "oanda-api-v20" not in source
    assert '"can_place_orders": false' in source
    supervisor = (
        Path(v7r3.__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert '"oanda_live_move_news_snapshot_v7r3.py"' in supervisor
    assert '"live_move_news_snapshot_v7r3.json"' in supervisor
