import json
import sqlite3
from pathlib import Path

import pytest

import oanda_move_first_calendar_episode_labels_v1 as calendar


def config_payload() -> dict:
    return {
        "schema_version": "move_first_calendar_episode_label_config_v1",
        "cohort_id": "calendar-test",
        "cohort_start_utc": "2026-09-01T04:15:00Z",
        "source_case_cohort_id": "source-test",
        "calendar_timezone": "America/New_York",
        "calendar_contract_id": "calendar-contract-test",
        "month_end_definition": "last_weekday_of_calendar_month",
        "quarter_end_definition": "last_weekday_of_quarter_end_month",
        "historical_rows_imported": 0,
        "execution_eligible": False,
        "can_place_orders": False,
    }


def source_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE manifest(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE cases(
            case_id TEXT PRIMARY KEY,
            factor_episode_id TEXT NOT NULL,
            start_utc TEXT NOT NULL
        );
        """
    )
    connection.execute("INSERT INTO manifest VALUES ('cohort_id', 'source-test')")
    connection.executemany(
        "INSERT INTO cases VALUES (?, ?, ?)",
        [
            ("old", "old-episode", "2026-09-01T04:14:59Z"),
            ("ordinary", "ordinary-episode", "2026-09-15T14:00:00Z"),
            ("month", "month-episode", "2026-09-30T14:00:00Z"),
            ("quarter", "quarter-episode", "2026-12-31T14:00:00Z"),
        ],
    )
    connection.commit()
    connection.close()


def test_calendar_labels_are_prospective_and_predeclared(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload()), encoding="utf-8")
    source_path = tmp_path / "source.sqlite"
    source_database(source_path)
    database = tmp_path / "labels.sqlite"
    report = tmp_path / "report.json"
    payload = calendar.run_once(config_path, source_path, database, report)
    assert payload["label_count"] == 3
    assert payload["month_end_count"] == 2
    assert payload["quarter_end_count"] == 2
    assert payload["execution_decision"] == "no_trade"
    connection = sqlite3.connect(database)
    rows = {
        row[0]: json.loads(row[1])
        for row in connection.execute(
            "SELECT source_case_id, label_json FROM labels"
        )
    }
    connection.close()
    assert "old" not in rows
    assert rows["ordinary"]["calendar_episode_labels"] == ["ordinary"]
    assert rows["month"]["calendar_episode_labels"] == [
        "ordinary", "month_end", "quarter_end"
    ]
    assert rows["quarter"]["calendar_episode_labels"] == [
        "ordinary", "month_end", "quarter_end"
    ]
    assert all(row["calendar_label_known_before_move"] for row in rows.values())
    assert calendar.verify(config_path, source_path, database, report)["verified"]


def test_calendar_label_mutation_fails_closed(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_payload()), encoding="utf-8")
    source_path = tmp_path / "source.sqlite"
    source_database(source_path)
    database = tmp_path / "labels.sqlite"
    report = tmp_path / "report.json"
    calendar.run_once(config_path, source_path, database, report)
    connection = sqlite3.connect(database)
    connection.execute(
        "UPDATE labels SET label_sha256 = 'mutated' WHERE source_case_id = 'month'"
    )
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="append-only calendar-label violation"):
        calendar.run_once(config_path, source_path, database, report)
