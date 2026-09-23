from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import oanda_spike_blurb_rbnz_schedule_cohort_v1 as module


def test_schedule_is_complete_consecutive_and_not_move_selected() -> None:
    rows = module.validate_schedule()
    assert len(rows) == 12
    assert rows[0]["event_date"] == "2024-02-28"
    assert rows[-1]["event_date"] == "2025-08-20"
    assert [row["decision_action"] for row in rows].count("hold") == 5
    assert [row["decision_action"] for row in rows].count("cut") == 7
    assert {row["selection_rule"] for row in rows} == {
        "every_scheduled_rbnz_policy_decision_in_closed_date_range"
    }
    assert all(row["direction_interpretation"] == "not_assigned" for row in rows)
    assert all(row["release_url"].startswith("https://www.rbnz.govt.nz/") for row in rows)


def test_exact_release_clocks_preserve_new_zealand_dst() -> None:
    rows = {row["event_date"]: row for row in module.validate_schedule()}
    assert rows["2024-02-28"]["event_clock_utc"].startswith("2024-02-28T01:00:00")
    assert rows["2024-04-10"]["event_clock_utc"].startswith("2024-04-10T02:00:00")
    assert rows["2024-10-09"]["event_clock_utc"].startswith("2024-10-09T01:00:00")
    assert rows["2025-04-09"]["event_clock_utc"].startswith("2025-04-09T02:00:00")


def test_factor_value_is_part_of_analog_identity() -> None:
    cut_25 = module.analog_key(-0.25, 15)
    cut_50 = module.analog_key(-0.50, 15)
    hold = module.analog_key(0.0, 15)
    assert cut_25 != cut_50 != hold
    assert "number:-0.25000000" in cut_25
    assert "number:-0.50000000" in cut_50
    assert "number:+0.00000000" in hold


def test_prequential_builder_uses_only_three_or_more_prior_same_value_events() -> None:
    events = module.validate_schedule()
    event_map = {row["event_id"]: row for row in events}
    trajectories = []
    for index, event in enumerate(events):
        strength = float(index + 1) * (-1 if index % 2 else 1)
        trajectories.append({
            "event_id": event["event_id"],
            "horizon_minutes": 15,
            "currency_strength_bps": strength,
        })
    predictions = module.build_prequential_predictions(trajectories, event_map)
    assert len(predictions) == 3
    by_event = {row["event_id"]: row for row in predictions}
    assert module.event_id("2024-07-10") in by_event
    assert module.event_id("2025-07-09") in by_event
    assert module.event_id("2025-08-20") in by_event
    assert by_event[module.event_id("2024-07-10")]["prior_effective_n"] == 3
    assert by_event[module.event_id("2025-08-20")]["prior_effective_n"] == 3
    for row in predictions:
        prior = module.json.loads(row["prior_event_ids_json"])
        assert row["event_id"] not in prior
        assert len(prior) == row["prior_effective_n"]


def test_source_only_materialization_is_immutable_and_inert(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    first = module.run(database=database, report_root=tmp_path / "report", source_only=True)
    second = module.run(database=database, report_root=tmp_path / "report", source_only=True)
    assert first["event_count"] == second["event_count"] == 12
    assert first["factor_count"] == second["factor_count"] == 24
    assert first["execution_eligible"] is False
    connection = sqlite3.connect(database)
    assert connection.execute("SELECT count(*) FROM rbnz_schedule_events").fetchone()[0] == 12
    assert connection.execute("SELECT count(*) FROM rbnz_schedule_factors").fetchone()[0] == 24
    assert connection.execute("SELECT sum(execution_eligible) FROM rbnz_schedule_events").fetchone()[0] == 0
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("UPDATE rbnz_schedule_events SET decision_action='raise'")
    connection.close()


def test_jobs_cover_all_events_and_all_68_instruments() -> None:
    instruments = [f"A{i:02d}_B{i:02d}" for i in range(68)]
    jobs = module.event_jobs(module.validate_schedule(), instruments)
    assert len(jobs) == 816
    assert len({job["event_id"] for job in jobs}) == 12
    assert len({job["instrument"] for job in jobs}) == 68
    assert all(job["end_epoch"] - job["start_epoch"] == 130 * 60 for job in jobs)


def test_contract_has_no_execution_or_proof_surface() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "/orders" not in source
    assert "/trades" not in source
    assert "api-fxtrade.oanda.com" not in source
    assert "supported_execution_decision\": \"no_trade" in source
