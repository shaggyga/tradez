import json
import sqlite3
from pathlib import Path

import pytest

import oanda_move_first_live_case_capture_v4 as capture


SNAPSHOT_CONTRACT = "snapshot_v1"
FACTOR_CONTRACT = "factor_v1"


def write_config(path: Path, start: str = "2026-09-01T07:05:00Z") -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "move_first_live_case_capture_config_v1",
                "cohort_id": "test_capture",
                "cohort_start_utc": start,
                "source_database_contract_id": SNAPSHOT_CONTRACT,
                "factor_episode_contract_id": FACTOR_CONTRACT,
                "historical_rows_imported": 0,
                "research_only": True,
                "execution_eligible": False,
                "can_place_orders": False,
            }
        ),
        encoding="utf-8",
    )


def build_source(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE mover_case_contract_registry(
            contract_id TEXT PRIMARY KEY,
            activated_utc TEXT NOT NULL,
            meter_contract_id TEXT NOT NULL,
            meter_database TEXT NOT NULL,
            narrative_join_semantics TEXT NOT NULL,
            partial_live_excluded INTEGER NOT NULL,
            research_only INTEGER NOT NULL,
            execution_eligible INTEGER NOT NULL,
            created_utc TEXT NOT NULL
        );
        CREATE TABLE factor_episode_contract_registry(
            factor_episode_contract_id TEXT PRIMARY KEY,
            snapshot_contract_id TEXT NOT NULL,
            previous_snapshot_contract_id TEXT NOT NULL,
            grouping_method TEXT NOT NULL,
            maximum_gap_sec INTEGER NOT NULL,
            unresolved_primary_policy TEXT NOT NULL,
            research_only INTEGER NOT NULL,
            execution_eligible INTEGER NOT NULL,
            created_utc TEXT NOT NULL
        );
        CREATE TABLE mover_cases(
            case_id TEXT PRIMARY KEY,
            first_recorded_utc TEXT NOT NULL,
            instrument TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            case_json TEXT NOT NULL
        );
        CREATE TABLE factor_episode_membership(
            case_id TEXT PRIMARY KEY,
            factor_episode_contract_id TEXT NOT NULL,
            factor_episode_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL,
            registered_utc TEXT NOT NULL
        );
        CREATE TABLE factor_episode_root_merges(
            merge_id TEXT PRIMARY KEY,
            factor_episode_contract_id TEXT NOT NULL,
            from_root_id TEXT NOT NULL,
            into_root_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL,
            bridge_case_ids TEXT NOT NULL,
            detected_utc TEXT NOT NULL,
            research_only INTEGER NOT NULL,
            execution_eligible INTEGER NOT NULL
        );
        """
    )
    connection.execute(
        "INSERT INTO mover_case_contract_registry VALUES(?,?,?,?,?,?,?,?,?)",
        (SNAPSHOT_CONTRACT, "2026-09-01T00:00:00Z", "meter", "meter.sqlite", "causal", 1, 1, 0, "2026-09-01T00:00:00Z"),
    )
    connection.execute(
        "INSERT INTO factor_episode_contract_registry VALUES(?,?,?,?,?,?,?,?,?)",
        (FACTOR_CONTRACT, SNAPSHOT_CONTRACT, "previous", "root_union", 60, "unresolved", 1, 0, "2026-09-01T00:00:00Z"),
    )
    connection.commit()
    return connection


def add_case(
    connection: sqlite3.Connection,
    case_id: str,
    first_recorded: str,
    episode: str,
    instrument: str = "EUR_USD",
) -> None:
    payload = json.dumps(
        {"case_id": case_id, "instrument": instrument, "research_only": True},
        sort_keys=True,
        separators=(",", ":"),
    )
    connection.execute(
        "INSERT INTO mover_cases VALUES(?,?,?,?,?,?)",
        (case_id, first_recorded, instrument, first_recorded, first_recorded, payload),
    )
    connection.execute(
        "INSERT INTO factor_episode_membership VALUES(?,?,?,?,?)",
        (case_id, FACTOR_CONTRACT, episode, "USD", first_recorded),
    )
    connection.commit()


def paths(tmp_path: Path):
    config = tmp_path / "config.json"
    source = tmp_path / "source.sqlite"
    database = tmp_path / "capture.sqlite"
    report = tmp_path / "report.json"
    write_config(config)
    return config, source, database, report


def test_direct_capture_excludes_history_and_is_idempotent(tmp_path: Path):
    config, source, database, report = paths(tmp_path)
    connection = build_source(source)
    add_case(connection, "old", "2026-09-01T07:04:59+00:00", "episode_old")
    add_case(connection, "new", "2026-09-01T07:05:01+00:00", "episode_new")
    connection.close()

    first = capture.run_once(config, source, database, report)
    second = capture.run_once(config, source, database, report)

    assert first["case_count"] == 1
    assert first["inserted_cases_this_cycle"] == 1
    assert first["factor_membership_count"] == 1
    assert second["inserted_cases_this_cycle"] == 0
    assert first["historical_rows_imported"] == "0"
    assert capture.verify(config, database, report)["verified"] is True


def test_late_source_case_is_captured_without_rebuilding_prior_rows(tmp_path: Path):
    config, source, database, report = paths(tmp_path)
    connection = build_source(source)
    add_case(connection, "one", "2026-09-01T07:06:00+00:00", "episode_one")
    capture.run_once(config, source, database, report)
    add_case(connection, "two", "2026-09-01T07:07:00+00:00", "episode_two")
    connection.close()

    payload = capture.run_once(config, source, database, report)

    assert payload["case_count"] == 2
    assert payload["inserted_cases_this_cycle"] == 1


def test_source_case_mutation_is_rejected(tmp_path: Path):
    config, source, database, report = paths(tmp_path)
    connection = build_source(source)
    add_case(connection, "one", "2026-09-01T07:06:00+00:00", "episode_one")
    capture.run_once(config, source, database, report)
    connection.execute(
        "UPDATE mover_cases SET case_json=? WHERE case_id='one'",
        (json.dumps({"case_id": "one", "mutated": True}),),
    )
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="append-only case mutation"):
        capture.run_once(config, source, database, report)


def test_root_merges_reduce_effective_episode_count_without_rewriting_cases(tmp_path: Path):
    config, source, database, report = paths(tmp_path)
    connection = build_source(source)
    add_case(connection, "one", "2026-09-01T07:06:00+00:00", "episode_one")
    add_case(connection, "two", "2026-09-01T07:06:30+00:00", "episode_two")
    connection.execute(
        "INSERT INTO factor_episode_root_merges VALUES(?,?,?,?,?,?,?,?,?)",
        ("merge", FACTOR_CONTRACT, "episode_two", "episode_one", "USD", "[]", "2026-09-01T07:07:00+00:00", 1, 0),
    )
    connection.commit()
    connection.close()

    payload = capture.run_once(config, source, database, report)

    assert payload["case_count"] == 2
    assert payload["resolved_factor_episode_count"] == 1
    assert payload["factor_root_merge_count"] == 1
    assert payload["factor_graph_cycle"] is False


def test_missing_factor_membership_fails_closed(tmp_path: Path):
    config, source, database, report = paths(tmp_path)
    connection = build_source(source)
    connection.execute(
        "INSERT INTO mover_cases VALUES(?,?,?,?,?,?)",
        ("orphan", "2026-09-01T07:06:00+00:00", "EUR_USD", "2026-09-01T07:06:00+00:00", "2026-09-01T07:07:00+00:00", "{}"),
    )
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="lacks factor membership"):
        capture.run_once(config, source, database, report)


def test_manifest_change_requires_new_database(tmp_path: Path):
    config, source, database, report = paths(tmp_path)
    connection = build_source(source)
    add_case(connection, "one", "2026-09-01T07:06:00+00:00", "episode_one")
    connection.close()
    capture.run_once(config, source, database, report)
    value = json.loads(config.read_text(encoding="utf-8"))
    value["cohort_id"] = "changed"
    config.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(RuntimeError, match="manifest mismatch"):
        capture.run_once(config, source, database, report)
