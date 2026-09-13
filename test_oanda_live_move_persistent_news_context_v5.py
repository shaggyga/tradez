from __future__ import annotations

import ast
import copy
import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

from trad import oanda_live_move_news_snapshot_v7r3 as v7r3
from trad import oanda_live_move_persistent_news_context_v3 as v3
from trad import oanda_live_move_persistent_news_context_v4 as v4
from trad import oanda_live_move_persistent_news_context_v5 as v5


NOW = dt.datetime(2026, 8, 27, 9, 10, tzinfo=dt.timezone.utc)


def _row(
    instrument: str = "USD_HUF",
    start: str = "2026-08-27T08:35:00+00:00",
    root: str = "root-huf-a",
) -> dict[str, object]:
    row: dict[str, object] = {
        "instrument": instrument,
        "start_utc": start,
        "end_utc": "2026-08-27T08:43:00+00:00",
        "move_direction": "up",
        "contract_id": v7r3.CONTRACT_ID,
        "factor_primary_token": "HUF-",
        "factor_episode_id": root,
        "factor_episode_canonical_root_id": root,
        "factor_episode_contract_id": v7r3.FACTOR_EPISODE_CONTRACT_ID,
        "factor_representative": True,
        "factor_episode_membership_conflict": False,
    }
    row["case_id"] = v7r3.stable_case_id(row)
    return row


def _snapshot(rows: list[dict[str, object]], *, root_merge_total: int = 0):
    canonical = {
        str(row["factor_episode_canonical_root_id"]) for row in rows
    }
    representatives = sum(row.get("factor_representative") is True for row in rows)
    return {
        "schema_version": v7r3.SCHEMA_VERSION,
        "contract_id": v7r3.CONTRACT_ID,
        "previous_contract_id": v7r3.PREVIOUS_CONTRACT_ID,
        "baseline_contract_id": v7r3.BASELINE_CONTRACT_ID,
        "generated_utc": "2026-08-27T09:04:37+00:00",
        "factor_episode_contract": {
            "contract_id": v7r3.FACTOR_EPISODE_CONTRACT_ID,
            "previous_snapshot_contract_id": v7r3.PREVIOUS_CONTRACT_ID,
            "grouping_method": (
                "fixed_onset_same_primary_interval_overlap_adjacency_"
                "append_only_transitive_root_union"
            ),
            "maximum_gap_sec": v7r3.MAXIMUM_EPISODE_GAP_SEC,
            "primary_factor_classification_minutes": (
                v7r3.FACTOR_CLASSIFICATION_MINUTES
            ),
            "centered_time_buckets_used": False,
            "root_union_can_increase_effective_n": False,
            "membership_rewrites_allowed": False,
        },
        "factor_episode_integrity_ok": True,
        "factor_episode_membership_new_conflict_count": 0,
        "factor_episode_membership_conflict_total": 0,
        "factor_episode_graph_cycle": False,
        "planned_root_merge_count": 0,
        "inserted_root_merge_count": 0,
        "root_merge_total": root_merge_total,
        "factor_episode_count": len(canonical),
        "factor_representative_count": representatives,
        "narrative_join_contract": {
            "meter_contract_id": v7r3.v7r2.METER_CONTRACT_ID,
            "partial_live_excluded": True,
            "sealed_v12_database_only": True,
        },
        "mover_count": len(rows),
        "movers": rows,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "broker_access": False,
        "supported_decision": "diagnostic_only",
    }


def _record_upstream(path: Path, rows: list[dict[str, object]], *, merges=()):
    return v7r3.record_cases(
        path,
        rows,
        recorded_utc="2026-08-27T09:00:00+00:00",
        meter_database=path.parent / "meter.sqlite",
        planned_merges=merges,
    )


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    snapshot: dict[str, object],
    upstream_history: Path,
):
    snapshot = copy.deepcopy(snapshot)
    snapshot.setdefault("history_database", str(upstream_history.resolve()))
    if "retained_case_count" not in snapshot:
        retained = len(snapshot.get("movers") or [])
        if upstream_history.is_file():
            with sqlite3.connect(upstream_history) as connection:
                retained = int(
                    connection.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0]
                )
        snapshot["retained_case_count"] = retained
    snapshot_path = tmp_path / "v7r3.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    monkeypatch.setattr(
        v5.census,
        "load_source_index",
        lambda *args, **kwargs: ({}, "2026-08-27T08:00:00+00:00"),
    )
    monkeypatch.setattr(
        v5.base,
        "enrich_case",
        lambda row, index: {**row, "persistent_active_event_count": 0},
    )
    return v5.run(
        snapshot_path=snapshot_path,
        snapshot_history_path=upstream_history,
        source_database=tmp_path / "sources.sqlite",
        output_path=tmp_path / "context.json",
        history_path=tmp_path / "context.sqlite",
        report_path=tmp_path / "context.md",
        now=NOW,
    )


def test_valid_exact_v7r3_snapshot_and_history_are_consumed(
    monkeypatch, tmp_path: Path
) -> None:
    row = _row()
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [row])
    result = _run(tmp_path, monkeypatch, _snapshot([row]), upstream)
    assert result["input_integrity_ok"] is True
    assert result["input_rejection_reasons"] == []
    assert result["upstream_factor_history_status"]["ok"] is True
    assert result["upstream_factor_history_status"]["quick_check"] == "ok"
    assert result["upstream_factor_history_status"]["append_only_triggers_ok"] is True
    assert result["upstream_factor_history_status"]["semantic_sha256"]
    assert result["upstream_factor_history_fingerprint_method"] == (
        "canonical_registry_merges_memberships_conflicts_v1"
    )


def test_later_append_only_history_suffix_does_not_rewrite_snapshot(
    monkeypatch, tmp_path: Path
) -> None:
    first = _row()
    later = _row("EUR_HUF", "2026-08-27T09:05:00+00:00", "root-huf-b")
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [first])
    snapshot = _snapshot([first])
    snapshot["retained_case_count"] = 1
    _record_upstream(upstream, [later])

    result = _run(tmp_path, monkeypatch, snapshot, upstream)

    assert result["input_integrity_ok"] is True
    status = result["upstream_factor_history_status"]
    assert status["retained_case_count"] == 1
    assert status["current_retained_case_count"] == 2
    assert status["append_only_suffix"]["mover_cases"] == 1
    assert status["append_only_suffix"]["memberships"] == 1
    assert result["mover_count"] == 1
    assert result["supported_decision"] == "diagnostic_only"
    assert result["research_only"] is True
    assert result["execution_eligible"] is False
    assert result["can_place_orders"] is False
    assert result["broker_access"] is False


def test_real_append_only_union_is_verified_as_nonexpansive(
    monkeypatch, tmp_path: Path
) -> None:
    first = _row("USD_HUF", "2026-08-27T08:35:00+00:00", "root-huf-a")
    second = _row("EUR_HUF", "2026-08-27T08:36:00+00:00", "root-huf-b")
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [first, second])
    merged_first = {**first, "factor_episode_id": "root-huf-a", "factor_episode_canonical_root_id": "root-huf-a", "factor_representative": True}
    merged_second = {**second, "factor_episode_id": "root-huf-a", "factor_episode_canonical_root_id": "root-huf-a", "factor_representative": False}
    _record_upstream(
        upstream,
        [merged_first, merged_second],
        merges=(
            {
                "from_root_id": "root-huf-b",
                "into_root_id": "root-huf-a",
                "factor_primary_token": "HUF-",
                "bridge_case_ids": "bridge",
            },
        ),
    )
    result = _run(
        tmp_path,
        monkeypatch,
        _snapshot([merged_first, merged_second], root_merge_total=1),
        upstream,
    )
    status = result["upstream_factor_history_status"]
    assert result["input_integrity_ok"] is True
    assert status["raw_root_count"] == 2
    assert status["canonical_root_count"] == 1
    assert status["root_union_nonexpansive"] is True
    assert status["graph_cycle"] is False


def test_wrong_snapshot_contract_rejects_before_history_or_sources(
    monkeypatch, tmp_path: Path
) -> None:
    row = _row()
    snapshot = _snapshot([row])
    snapshot["contract_id"] = v4.EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID
    result = _run(tmp_path, monkeypatch, snapshot, tmp_path / "missing.sqlite")
    assert result["input_integrity_ok"] is False
    assert "snapshot_contract_mismatch" in result["input_rejection_reasons"]
    assert result["upstream_factor_history_status"]["not_checked_due_to_snapshot_rejection"] is True
    assert result["mover_count"] == 0


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (
            lambda value: value["factor_episode_contract"].update(
                {"root_union_can_increase_effective_n": True}
            ),
            "root_union_nonexpansion_contract_missing",
        ),
        (
            lambda value: value["factor_episode_contract"].update(
                {"membership_rewrites_allowed": True}
            ),
            "membership_rewrite_contract_not_disabled",
        ),
        (
            lambda value: value.update({"factor_episode_graph_cycle": True}),
            "factor_episode_graph_cycle",
        ),
        (
            lambda value: value.update({"broker_access": True}),
            "upstream_broker_access",
        ),
    ],
)
def test_snapshot_root_union_or_inertness_violation_fails_closed(
    monkeypatch, tmp_path: Path, mutate, reason: str
) -> None:
    row = _row()
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [row])
    snapshot = _snapshot([row])
    mutate(snapshot)
    result = _run(tmp_path, monkeypatch, snapshot, upstream)
    assert result["input_integrity_ok"] is False
    assert reason in result["input_rejection_reasons"]
    assert result["mover_count"] == 0


def test_missing_append_only_trigger_is_detected(monkeypatch, tmp_path: Path) -> None:
    row = _row()
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [row])
    with sqlite3.connect(upstream) as connection:
        connection.execute("DROP TRIGGER factor_episode_root_merges_no_update")
    result = _run(tmp_path, monkeypatch, _snapshot([row]), upstream)
    assert result["input_integrity_ok"] is False
    assert "upstream_append_only_triggers_missing" in result["input_rejection_reasons"]
    assert result["mover_count"] == 0


def test_independent_history_cycle_check_rejects_false_clean_snapshot(
    monkeypatch, tmp_path: Path
) -> None:
    row = _row(root="root-a")
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [row])
    with sqlite3.connect(upstream) as connection:
        for merge_id, source, target in (
            ("merge-ab", "root-a", "root-b"),
            ("merge-ba", "root-b", "root-a"),
        ):
            connection.execute(
                "INSERT INTO factor_episode_root_merges VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    merge_id,
                    v7r3.FACTOR_EPISODE_CONTRACT_ID,
                    source,
                    target,
                    "HUF-",
                    str(row["case_id"]),
                    NOW.isoformat(),
                    1,
                    0,
                ),
            )
    snapshot = _snapshot([row], root_merge_total=2)
    result = _run(tmp_path, monkeypatch, snapshot, upstream)
    assert result["input_integrity_ok"] is False
    assert "upstream_root_union_cycle" in result["input_rejection_reasons"]
    assert result["mover_count"] == 0


def test_snapshot_membership_must_exist_in_upstream_history(
    monkeypatch, tmp_path: Path
) -> None:
    history_row = _row()
    snapshot_row = _row("EUR_HUF", "2026-08-27T08:36:00+00:00")
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [history_row])
    result = _run(tmp_path, monkeypatch, _snapshot([snapshot_row]), upstream)
    assert result["input_integrity_ok"] is False
    assert any(
        reason.endswith("_missing_from_upstream_history")
        for reason in result["input_rejection_reasons"]
    )


def test_declared_upstream_history_path_must_match_bound_path(
    monkeypatch, tmp_path: Path
) -> None:
    row = _row()
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [row])
    snapshot = _snapshot([row])
    snapshot["history_database"] = str(tmp_path / "different.sqlite")
    result = _run(tmp_path, monkeypatch, snapshot, upstream)
    assert result["input_integrity_ok"] is False
    assert "upstream_history_path_mismatch" in result["input_rejection_reasons"]


def test_consumer_history_is_append_only_and_idempotent(
    monkeypatch, tmp_path: Path
) -> None:
    row = _row()
    upstream = tmp_path / "v7r3.sqlite"
    _record_upstream(upstream, [row])
    first = _run(tmp_path, monkeypatch, _snapshot([row]), upstream)
    second = _run(tmp_path, monkeypatch, _snapshot([row]), upstream)
    assert first["history_observation_id"] == second["history_observation_id"]
    consumer = tmp_path / "context.sqlite"
    with sqlite3.connect(consumer) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("SELECT COUNT(*) FROM context_observations").fetchone()[0] == 1
        registry = connection.execute(
            "SELECT required_snapshot_contract_id,required_factor_episode_contract_id,"
            "required_narrative_meter_contract_id,research_only,execution_eligible,"
            "can_place_orders,broker_access FROM context_contract_registry"
        ).fetchone()
        assert registry == (
            v7r3.CONTRACT_ID,
            v7r3.FACTOR_EPISODE_CONTRACT_ID,
            v7r3.v7r2.METER_CONTRACT_ID,
            1,
            0,
            0,
            0,
        )
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("DELETE FROM context_observations")


def test_v5r3_defaults_are_separate_and_live_diagnostic_bound() -> None:
    assert v5.DEFAULT_SNAPSHOT == v7r3.DEFAULT_OUTPUT
    assert v5.DEFAULT_SNAPSHOT_HISTORY == v7r3.DEFAULT_HISTORY
    assert v5.DEFAULT_OUTPUT != v4.DEFAULT_OUTPUT != v3.DEFAULT_OUTPUT
    assert v5.DEFAULT_HISTORY != v4.DEFAULT_HISTORY
    assert v5.DEFAULT_REPORT != v4.DEFAULT_REPORT != v3.DEFAULT_REPORT
    supervisor = (
        Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert "oanda_live_move_persistent_news_context_v5.py" in supervisor
    assert "oanda_live_move_persistent_news_context_v4.py" not in supervisor
    assert "live_move_persistent_news_context_v5r3.json" in supervisor


def test_module_has_no_broker_or_network_import_surface() -> None:
    tree = ast.parse(Path(v5.__file__).read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert not imports.intersection(
        {"requests", "httpx", "urllib", "socket", "oandapyV20"}
    )
