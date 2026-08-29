import copy
import datetime as dt
import json

import pytest

import oanda_uncovered_factor_case_batch_v1 as audit


UTC = dt.timezone.utc


def test_relative_timing_state_is_boundary_exact() -> None:
    start = dt.datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    end = dt.datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    assert audit.relative_timing_state(None, start, end) == "clock_unverified"
    assert (
        audit.relative_timing_state(start - dt.timedelta(seconds=1), start, end)
        == "known_before_selected_interval"
    )
    assert audit.relative_timing_state(start, start, end) == "known_at_selected_start"
    assert (
        audit.relative_timing_state(start + dt.timedelta(seconds=1), start, end)
        == "known_during_selected_interval"
    )
    assert audit.relative_timing_state(end, start, end) == "known_during_selected_interval"
    assert (
        audit.relative_timing_state(end + dt.timedelta(seconds=1), start, end)
        == "known_after_selected_interval"
    )


def test_config_is_exactly_ten_unique_uncovered_cases() -> None:
    config = json.loads(audit.CONFIG.read_text(encoding="utf-8"))
    audit.validate_config(config)
    assert len(config["cases"]) == 10
    assert len({row["case_id"] for row in config["cases"]}) == 10
    assert len({row["episode_id"] for row in config["cases"]}) == 10
    duplicate = copy.deepcopy(config)
    duplicate["cases"][1]["episode_id"] = duplicate["cases"][0]["episode_id"]
    with pytest.raises(audit.CaseAuditError, match="episode_ids_duplicate"):
        audit.validate_config(duplicate)


def test_unverified_clock_cannot_become_exact() -> None:
    config = json.loads(audit.CONFIG.read_text(encoding="utf-8"))
    target = next(
        row for row in config["cases"] if row["case_id"].startswith("uncovered_20250120")
    )
    clock = target["source_clocks"][0]
    assert clock["at_utc"] is None
    assert clock["timestamp_quality"] == "reported_original_clock_unverified"


def test_current_batch_compiles_fail_closed() -> None:
    payload = audit.compile_batch()
    assert payload["case_count"] == 10
    assert payload["unique_episode_count"] == 10
    assert payload["unique_market_date_count"] == 10
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False
    assert payload["can_place_orders"] is False
    assert payload["execution_decision"] == "no_trade"
    assert all(row["forecast_proof_eligible"] is False for row in payload["cases"])
    assert all(row["execution_eligible"] is False for row in payload["cases"])
    assert all(row["factor_breadth_instruments"] >= 4 for row in payload["cases"])
    assert any(
        row["earliest_exact_clock_timing_state"] == "known_after_selected_interval"
        for row in payload["cases"]
    )
    assert any(
        row["earliest_exact_clock_timing_state"] == "known_during_selected_interval"
        for row in payload["cases"]
    )
    assert any(
        row["earliest_exact_clock_timing_state"] == "known_before_selected_interval"
        for row in payload["cases"]
    )


def test_already_covered_episode_is_rejected(tmp_path) -> None:
    config = json.loads(audit.CONFIG.read_text(encoding="utf-8"))
    config["cases"][0]["episode_id"] = "unmatched_episode_6ab705a9f5da110730b73255"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(audit.CaseAuditError, match="episode_already_covered"):
        audit.compile_batch(path, audit.DATABASE)
