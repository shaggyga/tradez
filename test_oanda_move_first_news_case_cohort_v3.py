import csv
import json
import sqlite3
from pathlib import Path

import pytest

import oanda_move_first_news_case_cohort_v3 as cohort


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "move_first_news_case_cohort_config_v1",
                "cohort_id": "test_cohort",
                "cohort_start_utc": "2026-09-01T02:30:00Z",
                "source_classifier_contract_id": "classifier_v1",
                "pair_direction_contract_id": "pair_v1",
                "factor_episode_contract_id": "factor_v1",
                "historical_rows_imported": 0,
                "research_only": True,
                "execution_eligible": False,
                "can_place_orders": False,
            }
        ),
        encoding="utf-8",
    )


def case_row(net: str = "4.2") -> dict[str, str]:
    return {
        "move_id": "representative_1",
        "factor_episode_id": "episode_1",
        "start_utc": "2026-09-01T02:31:00Z",
        "end_utc": "2026-09-01T02:46:00Z",
        "instrument": "EUR_USD",
        "horizon_min": "15",
        "endpoint_after_cost_pips": net,
        "case_class": "no_directional_news_mapping",
    }


def move_rows() -> list[dict[str, str]]:
    base = {
        "factor_episode_id": "episode_1",
        "start_utc": "2026-09-01T02:31:00Z",
        "end_utc": "2026-09-01T02:46:00Z",
        "horizon_min": "15",
        "selected_side_label": "long",
        "gross_magnitude_pips": "6.0",
        "endpoint_after_cost_pips": "4.2",
        "modeled_cost_pips": "1.8",
        "liquidity_bucket": "liquid_major",
    }
    return [
        {**base, "move_id": "representative_1", "factor_representative": "1", "instrument": "EUR_USD"},
        {**base, "move_id": "related_2", "factor_representative": "0", "instrument": "EUR_JPY"},
    ]


def paths(tmp_path: Path):
    config = tmp_path / "config.json"
    cases = tmp_path / "cases.csv"
    moves = tmp_path / "moves.csv"
    database = tmp_path / "ledger.sqlite"
    report = tmp_path / "report.json"
    write_config(config)
    write_csv(cases, [case_row()])
    write_csv(moves, move_rows())
    return config, cases, moves, database, report


def test_append_only_cohort_retains_raw_pair_paths_and_verifies(tmp_path: Path):
    config, cases, moves, database, report = paths(tmp_path)
    first = cohort.run_once(config, cases, moves, database, report)
    second = cohort.run_once(config, cases, moves, database, report)

    assert first["inserted_this_cycle"] == 1
    assert first["case_count"] == 1
    assert first["raw_pair_path_count"] == 2
    assert first["execution_decision"] == "no_trade"
    assert second["inserted_this_cycle"] == 0
    assert cohort.verify(config, database, report)["verified"] is True

    connection = sqlite3.connect(database)
    stored = json.loads(connection.execute("SELECT raw_pair_paths_json FROM cases").fetchone()[0])
    connection.close()
    assert [row["instrument"] for row in stored] == ["EUR_JPY", "EUR_USD"]


def test_changed_inserted_case_is_append_only_violation(tmp_path: Path):
    config, cases, moves, database, report = paths(tmp_path)
    cohort.run_once(config, cases, moves, database, report)
    write_csv(cases, [case_row("9.9")])

    with pytest.raises(RuntimeError, match="append-only violation"):
        cohort.run_once(config, cases, moves, database, report)


def test_pre_cohort_rows_are_not_imported(tmp_path: Path):
    config, cases, moves, database, report = paths(tmp_path)
    older = case_row()
    older["start_utc"] = "2026-09-01T02:29:59Z"
    write_csv(cases, [older])
    write_csv(moves, [{**move_rows()[0], "start_utc": older["start_utc"]}])

    payload = cohort.run_once(config, cases, moves, database, report)

    assert payload["case_count"] == 0
    assert payload["historical_rows_imported"] == "0"


def test_manifest_change_requires_new_database(tmp_path: Path):
    config, cases, moves, database, report = paths(tmp_path)
    cohort.run_once(config, cases, moves, database, report)
    value = json.loads(config.read_text(encoding="utf-8"))
    value["pair_direction_contract_id"] = "pair_v2"
    config.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(RuntimeError, match="manifest mismatch"):
        cohort.run_once(config, cases, moves, database, report)
