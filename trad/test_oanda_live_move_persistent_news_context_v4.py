from __future__ import annotations

import ast
import copy
import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

from trad import oanda_live_move_news_snapshot_v7 as v7
from trad import oanda_live_move_persistent_news_context_v3 as v3
from trad import oanda_live_move_persistent_news_context_v4 as v4


NOW = dt.datetime(2026, 8, 27, 9, 0, tzinfo=dt.timezone.utc)


def _mover() -> dict[str, object]:
    return {
        "instrument": "USD_PLN",
        "start_utc": "2026-08-27T08:08:00+00:00",
        "end_utc": "2026-08-27T08:17:00+00:00",
        "move_direction": "up",
        "contract_id": v7.CONTRACT_ID,
        "case_id": "case-usd-pln-0808",
        "factor_primary_token": "PLN-",
        "factor_episode_id": "episode-pln-0807",
        "factor_representative": True,
        "factor_episode_membership_conflict": False,
        "factor_episode_merge_conflict": False,
    }


def _snapshot() -> dict[str, object]:
    return {
        "schema_version": v7.SCHEMA_VERSION,
        "contract_id": v7.CONTRACT_ID,
        "previous_contract_id": v7.PREVIOUS_CONTRACT_ID,
        "generated_utc": "2026-08-27T08:43:02+00:00",
        "factor_episode_contract": {
            "contract_id": v7.FACTOR_EPISODE_CONTRACT_ID,
            "grouping_method": "same_primary_interval_overlap_or_adjacency",
            "maximum_gap_sec": v7.MAXIMUM_EPISODE_GAP_SEC,
            "primary_factor_classification_minutes": (
                v7.FACTOR_CLASSIFICATION_MINUTES
            ),
            "centered_time_buckets_used": False,
            "input_order_invariant": True,
        },
        "factor_episode_integrity_ok": True,
        "factor_episode_membership_conflict_total": 0,
        "factor_episode_merge_conflict_count": 0,
        "factor_episode_count": 1,
        "factor_representative_count": 1,
        "narrative_join_contract": {
            "meter_contract_id": v7.METER_CONTRACT_ID,
            "partial_live_excluded": True,
            "sealed_v12_database_only": True,
        },
        "mover_count": 1,
        "movers": [_mover()],
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "broker_access": False,
        "supported_decision": "diagnostic_only",
    }


def _write_snapshot(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    snapshot: dict[str, object],
):
    snapshot_path = tmp_path / "v7.json"
    _write_snapshot(snapshot_path, snapshot)
    monkeypatch.setattr(
        v4.census,
        "load_source_index",
        lambda *args, **kwargs: ({}, "2026-08-27T08:00:00+00:00"),
    )
    monkeypatch.setattr(
        v4.base,
        "enrich_case",
        lambda row, source_index: {**row, "persistent_active_event_count": 0},
    )
    return v4.run(
        snapshot_path=snapshot_path,
        source_database=tmp_path / "sources.sqlite",
        output_path=tmp_path / "context.json",
        history_path=tmp_path / "context.sqlite",
        report_path=tmp_path / "context.md",
        now=NOW,
    )


def test_valid_exact_v7r2_snapshot_is_consumed(monkeypatch, tmp_path: Path) -> None:
    result = _run(tmp_path, monkeypatch, _snapshot())
    assert result["input_integrity_ok"] is True
    assert result["input_rejection_reasons"] == []
    assert result["input_snapshot_contract_id"] == v7.CONTRACT_ID
    assert result["input_factor_episode_contract_id"] == v7.FACTOR_EPISODE_CONTRACT_ID
    assert result["mover_count"] == 1
    assert result["movers"][0]["persistent_active_event_count"] == 0
    assert result["supported_decision"] == "diagnostic_only"
    assert result["research_only"] is True
    assert result["execution_eligible"] is False
    assert result["can_place_orders"] is False
    assert result["broker_access"] is False
    assert (tmp_path / "context.json").is_file()
    assert (tmp_path / "context.md").is_file()


def test_wrong_snapshot_contract_fails_closed_before_source_load(
    monkeypatch, tmp_path: Path
) -> None:
    snapshot = _snapshot()
    snapshot["contract_id"] = "live_move_news_snapshot_v6r2"
    snapshot_path = tmp_path / "wrong.json"
    _write_snapshot(snapshot_path, snapshot)

    def forbidden(*args, **kwargs):
        raise AssertionError("source database must not be read")

    monkeypatch.setattr(v4.census, "load_source_index", forbidden)
    result = v4.run(
        snapshot_path=snapshot_path,
        source_database=tmp_path / "sources.sqlite",
        output_path=tmp_path / "context.json",
        history_path=tmp_path / "context.sqlite",
        report_path=tmp_path / "context.md",
        now=NOW,
    )
    assert result["input_integrity_ok"] is False
    assert "snapshot_contract_mismatch" in result["input_rejection_reasons"]
    assert result["mover_count"] == 0
    assert result["movers"] == []
    assert result["supported_decision"] == "reject_input"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            lambda value: value["factor_episode_contract"].update(
                {"contract_id": "wrong-factor-contract"}
            ),
            "factor_episode_contract_mismatch",
        ),
        (
            lambda value: value.update({"factor_episode_integrity_ok": False}),
            "factor_episode_integrity_failed",
        ),
        (
            lambda value: value.update(
                {"factor_episode_membership_conflict_total": 1}
            ),
            "factor_episode_membership_conflict",
        ),
        (
            lambda value: value.update({"broker_access": True}),
            "upstream_broker_access",
        ),
    ],
)
def test_factor_or_inertness_contract_violation_fails_closed(
    monkeypatch,
    tmp_path: Path,
    mutation,
    reason: str,
) -> None:
    snapshot = _snapshot()
    mutation(snapshot)
    result = _run(tmp_path, monkeypatch, snapshot)
    assert result["input_integrity_ok"] is False
    assert reason in result["input_rejection_reasons"]
    assert result["mover_count"] == 0
    assert result["supported_decision"] == "reject_input"


def test_source_database_failure_is_recorded_as_rejected_input(
    monkeypatch, tmp_path: Path
) -> None:
    snapshot_path = tmp_path / "v7.json"
    _write_snapshot(snapshot_path, _snapshot())

    def fail(*args, **kwargs):
        raise sqlite3.DatabaseError("broken")

    monkeypatch.setattr(v4.census, "load_source_index", fail)
    result = v4.run(
        snapshot_path=snapshot_path,
        source_database=tmp_path / "sources.sqlite",
        output_path=tmp_path / "context.json",
        history_path=tmp_path / "context.sqlite",
        report_path=tmp_path / "context.md",
        now=NOW,
    )
    assert result["input_integrity_ok"] is False
    assert result["input_rejection_reasons"] == [
        "source_database_load_failed:DatabaseError"
    ]
    assert result["mover_count"] == 0


def test_history_is_separate_append_only_integrity_checked_and_idempotent(
    monkeypatch, tmp_path: Path
) -> None:
    first = _run(tmp_path, monkeypatch, _snapshot())
    second = _run(tmp_path, monkeypatch, _snapshot())
    assert first["history_observation_id"] == second["history_observation_id"]
    history = tmp_path / "context.sqlite"
    with sqlite3.connect(history) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute(
            "SELECT COUNT(*) FROM context_observations"
        ).fetchone()[0] == 1
        contract = connection.execute(
            "SELECT required_snapshot_contract_id,"
            "required_factor_episode_contract_id,"
            "required_narrative_meter_contract_id,research_only,"
            "execution_eligible,can_place_orders,broker_access "
            "FROM context_contract_registry"
        ).fetchone()
        assert contract == (
            v7.CONTRACT_ID,
            v7.FACTOR_EPISODE_CONTRACT_ID,
            v7.METER_CONTRACT_ID,
            1,
            0,
            0,
            0,
        )
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("DELETE FROM context_observations")
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE context_contract_registry SET activated_utc='x'"
            )


def test_payload_identity_collision_fails_closed(tmp_path: Path) -> None:
    payload = {
        "generated_utc": NOW.isoformat(),
        "input_snapshot_generated_utc": "2026-08-27T08:43:02+00:00",
        "input_integrity_ok": True,
        "input_rejection_reasons": [],
        "mover_count": 0,
        "movers": [],
        "source_history_highwater_utc": None,
        "history_observation_id": "fixed-id",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "broker_access": False,
    }
    history = tmp_path / "context.sqlite"
    v4.record_observation(history, payload, input_snapshot_sha256="a" * 64)
    changed = copy.deepcopy(payload)
    changed["mover_count"] = 1
    with pytest.raises(
        sqlite3.IntegrityError, match="observation_identity_payload_mismatch"
    ):
        v4.record_observation(history, changed, input_snapshot_sha256="a" * 64)


def test_preexisting_wrong_contract_registry_fails_closed(tmp_path: Path) -> None:
    history = tmp_path / "context.sqlite"
    with sqlite3.connect(history) as connection:
        connection.execute(
            """CREATE TABLE context_contract_registry(
                contract_id TEXT PRIMARY KEY,
                activated_utc TEXT NOT NULL,
                required_snapshot_contract_id TEXT NOT NULL,
                required_factor_episode_contract_id TEXT NOT NULL,
                required_narrative_meter_contract_id TEXT NOT NULL,
                research_only INTEGER NOT NULL CHECK(research_only=1),
                execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
                can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
                broker_access INTEGER NOT NULL CHECK(broker_access=0),
                created_utc TEXT NOT NULL
            )"""
        )
        connection.execute(
            "INSERT INTO context_contract_registry VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                v4.CONTRACT_ID,
                v4.CONTRACT_ACTIVATED_UTC.isoformat(),
                "wrong-upstream",
                v7.FACTOR_EPISODE_CONTRACT_ID,
                v7.METER_CONTRACT_ID,
                1,
                0,
                0,
                0,
                NOW.isoformat(),
            ),
        )
    payload = {
        "generated_utc": NOW.isoformat(),
        "input_integrity_ok": False,
        "input_rejection_reasons": ["fixture"],
        "mover_count": 0,
        "history_observation_id": "fixture-id",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "broker_access": False,
    }
    with pytest.raises(
        sqlite3.IntegrityError, match="context_contract_registry_mismatch"
    ):
        v4.record_observation(history, payload, input_snapshot_sha256="b" * 64)


def test_future_snapshot_clock_fails_closed(monkeypatch, tmp_path: Path) -> None:
    snapshot = _snapshot()
    snapshot["generated_utc"] = "2026-08-27T10:00:00+00:00"
    result = _run(tmp_path, monkeypatch, snapshot)
    assert result["input_integrity_ok"] is False
    assert "snapshot_generated_in_future" in result["input_rejection_reasons"]
    assert result["mover_count"] == 0


def test_parallel_defaults_preserve_v3r2_and_are_not_live_bound() -> None:
    assert v4.DEFAULT_SNAPSHOT == v7.DEFAULT_OUTPUT
    assert v4.DEFAULT_OUTPUT != v3.DEFAULT_OUTPUT
    assert v4.DEFAULT_REPORT != v3.DEFAULT_REPORT
    assert "v4r2" in v4.DEFAULT_HISTORY.name
    assert v3.CONTRACT_ID == (
        "live_move_persistent_news_context_v3r2_precise_start_clock_20260827"
    )
    supervisor = (
        Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert "oanda_live_move_persistent_news_context_v3.py" in supervisor
    assert "oanda_live_move_persistent_news_context_v4.py" not in supervisor


def test_module_has_no_broker_or_network_import_surface() -> None:
    path = Path(v4.__file__).resolve()
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert not imports.intersection(
        {"requests", "httpx", "urllib", "socket", "oandapyV20"}
    )
