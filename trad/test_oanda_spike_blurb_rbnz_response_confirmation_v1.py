from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import oanda_spike_blurb_rbnz_response_confirmation_v1 as module


def test_later_schedule_is_complete_official_and_timezone_exact() -> None:
    events = module.validate_schedule()
    assert len(events) == 6
    assert [row["event_date"] for row in events] == [
        "2025-10-08", "2025-11-26", "2026-02-18",
        "2026-04-08", "2026-05-27", "2026-07-08",
    ]
    assert [row["event_clock_utc"] for row in events] == [
        "2025-10-08T01:00:00+00:00", "2025-11-26T01:00:00+00:00",
        "2026-02-18T01:00:00+00:00", "2026-04-08T02:00:00+00:00",
        "2026-05-27T02:00:00+00:00", "2026-07-08T02:00:00+00:00",
    ]
    assert all(row["release_url"].startswith("https://www.rbnz.govt.nz/") for row in events)
    assert all(row["source_direction_assigned"] == 0 for row in events)


def test_matched_controls_are_predeclared_and_complete() -> None:
    clocks = module.build_clocks(module.validate_schedule())
    assert len(clocks) == 18
    by_event: dict[str, set[int]] = {}
    for row in clocks:
        by_event.setdefault(row["source_event_id"], set()).add(row["day_offset"])
        assert row["clock_local"][11:16] == "14:00"
    assert all(offsets == {-7, 0, 7} for offsets in by_event.values())


def test_candidate_is_one_fixed_first_minute_fifteen_minute_rule() -> None:
    assert module.LOCKED_ARM == {
        "start_min": 1,
        "end_min": 1,
        "minimum_strength_bps": 1.0,
        "persistence_min": 1,
        "technical_confirmation": False,
    }
    assert module.HOLD_MINUTES == 15
    assert len(module.DIRECT_INSTRUMENTS) == 9
    assert all("NZD" in instrument.split("_") for instrument in module.DIRECT_INSTRUMENTS)


def test_exact_within_trio_randomization() -> None:
    assert module.exact_randomization_pvalue([(1.0, 0.0, 0.0)]) == pytest.approx(1 / 3)
    assert module.exact_randomization_pvalue([(1.0, 0.0, 0.0)] * 2) == pytest.approx(1 / 9)
    assert module.exact_randomization_pvalue([(0.0, 0.0, 0.0)] * 6) == 1.0


def test_freeze_only_materializes_no_operational_rows(tmp_path: Path) -> None:
    database = tmp_path / "confirmation.sqlite"
    result = module.run(database=database, report_root=tmp_path / "report", freeze_only=True)
    assert result["event_count"] == 6
    assert result["clock_count"] == 18
    assert result["selection_state"] == "single_candidate_frozen_before_price_query"
    assert result["execution_eligible"] is False
    assert result["forecast_proof_eligible"] is False
    assert result["supported_execution_decision"] == "no_trade"
    connection = sqlite3.connect(database)
    assert connection.execute("select count(*) from rbnz_confirmation_events").fetchone()[0] == 6
    assert connection.execute("select count(*) from rbnz_confirmation_clocks").fetchone()[0] == 18
    contract = json.loads(connection.execute("select contract_json from rbnz_confirmation_contracts").fetchone()[0])
    assert contract["candidate"]["hold_minutes"] == 15
    assert contract["matched_controls"]["predeclared_with_candidate"] is True
    assert connection.execute("select coalesce(sum(execution_eligible),0) from rbnz_confirmation_events").fetchone()[0] == 0
    connection.close()


def test_module_has_no_order_trade_authorization_or_promotion_surface() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "/orders" not in source
    assert "/trades" not in source
    assert "api-fxtrade.oanda.com" not in source
    assert "authorization_id" not in source
    assert "promote_candidate" not in source
    assert '"supported_execution_decision": "no_trade"' in source
