from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import oanda_spike_blurb_rbnz_schedule_cohort_v1 as treatment
import oanda_spike_blurb_rbnz_schedule_placebo_v1 as module


def treatment_rows() -> list[dict[str, str]]:
    return [
        {
            "event_id": row["event_id"],
            "event_date": row["event_date"],
            "event_clock_utc": row["event_clock_utc"],
            "row_sha256": "0" * 64,
        }
        for row in treatment.validate_schedule()
    ]


def test_controls_are_fixed_same_weekday_and_not_policy_dates() -> None:
    events = treatment_rows()
    controls = module.build_control_clocks(events)
    assert len(controls) == 24
    policy_dates = {date.fromisoformat(row["event_date"]) for row in events}
    for control in controls:
        local = datetime.fromisoformat(control["control_clock_local"])
        source = next(row for row in events if row["event_id"] == control["source_event_id"])
        source_date = date.fromisoformat(source["event_date"])
        assert local.hour == 14 and local.minute == 0
        assert local.date() not in policy_dates
        assert local.date().weekday() == source_date.weekday()
        assert (local.date() - source_date).days == control["day_offset"]
        assert control["day_offset"] in {-7, 7}


def test_control_clock_conversion_handles_dst_at_14_local() -> None:
    controls = module.build_control_clocks(treatment_rows())
    april = [row for row in controls if row["source_event_id"] == treatment.event_id("2024-04-10")]
    by_offset = {row["day_offset"]: row for row in april}
    assert by_offset[-7]["control_clock_utc"].startswith("2024-04-03T01:00:00")
    assert by_offset[7]["control_clock_utc"].startswith("2024-04-17T02:00:00")


def test_control_jobs_use_only_fixed_inputs() -> None:
    instruments = [
        "AUD_NZD", "EUR_NZD", "GBP_NZD", "NZD_CAD", "NZD_CHF",
        "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD",
    ]
    jobs = module.control_jobs(module.build_control_clocks(treatment_rows()), instruments)
    assert len(jobs) == 24 * 9
    assert len({row["control_id"] for row in jobs}) == 24
    assert len({row["instrument"] for row in jobs}) == 9
    assert all(row["end_epoch"] - row["start_epoch"] == 130 * 60 for row in jobs)


def test_placebo_module_has_no_operational_surface() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "/orders" not in source
    assert "/trades" not in source
    assert "api-fxtrade.oanda.com" not in source
    assert "supported_execution_decision\": \"no_trade" in source
