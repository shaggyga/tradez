import oanda_signal_feed_availability_monitor as monitor
import datetime as dt
import json
import os
from pathlib import Path
import tempfile
from unittest import mock


def test_atomic_write_json_retries_transient_windows_replace_lock():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "state.json"
        real_replace = os.replace
        attempts = []

        def flaky_replace(source, destination):
            attempts.append((source, destination))
            if len(attempts) < 3:
                raise PermissionError("simulated Windows reader lock")
            return real_replace(source, destination)

        with mock.patch.object(monitor.os, "replace", side_effect=flaky_replace):
            monitor.atomic_write_json(path, {"status": "ok"})

        assert len(attempts) == 3
        assert json.loads(path.read_text(encoding="utf-8")) == {"status": "ok"}
        assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_atomic_write_json_cleans_temp_after_persistent_replace_lock():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "state.json"
        with mock.patch.object(
            monitor.os,
            "replace",
            side_effect=PermissionError("persistent Windows reader lock"),
        ), mock.patch.object(monitor.time, "sleep"):
            try:
                monitor.atomic_write_json(path, {"status": "blocked"})
            except PermissionError:
                pass
            else:
                raise AssertionError("persistent replacement failure must surface")

        assert not path.exists()
        assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def heartbeat(*, count=10, age=20.0, ttl=90.0, fresh=True):
    return {
        "details": {
            "qualified_candidates": 1,
            "nonconflicting_qualified_candidates": 0,
            "qualified_candidate_preview": [
                {
                    "id": "signal-1",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "direction_conflict": True,
                    "execution_permitted_after_conflict_gate": False,
                }
            ],
            "feed_cache": {
                "fresh": fresh,
                "fail_closed": False,
                "ttl_sec": ttl,
                "source_counts": {"strategy_lab": count},
                "source_newest_age_sec": {"strategy_lab": age},
            }
        }
    }


def test_source_observation_requires_present_fresh_source():
    assert monitor.source_observation(
        heartbeat(), source="strategy_lab", heartbeat_age_sec=1.0
    )["available"]
    expired = monitor.source_observation(
        heartbeat(age=90.0), source="strategy_lab", heartbeat_age_sec=1.0
    )
    assert not expired["available"]
    assert "source_expired" in expired["reasons"]
    scenarios = {row["ttl_sec"]: row for row in expired["shadow_ttl_scenarios"]}
    assert not scenarios[90.0]["available"]
    assert scenarios[120.0]["available"]
    assert all(row["research_only"] for row in scenarios.values())
    assert all(
        not row["execution_policy_changed"] for row in scenarios.values()
    )


def test_shadow_ttl_scenarios_preserve_fail_closed_and_heartbeat_constraints():
    observation = {
        "source_count": 10,
        "source_newest_age_sec": 95.0,
        "heartbeat_age_sec": 20.0,
        "feed_cache_fail_closed": True,
    }
    rows = monitor.shadow_ttl_scenarios(observation, (90.0, 120.0))
    assert not any(row["available"] for row in rows)
    assert all("feed_cache_fail_closed" in row["reasons"] for row in rows)

    observation["feed_cache_fail_closed"] = False
    observation["heartbeat_age_sec"] = 130.0
    rows = monitor.shadow_ttl_scenarios(observation, (120.0,))
    assert rows[0]["available"] is False
    assert "executor_heartbeat_stale_at_scenario_ttl" in rows[0]["reasons"]


def test_source_observation_can_measure_primary_or_shadow_union():
    payload = heartbeat(count=0, age=90.0)
    payload["details"]["feed_cache"]["source_counts"] = {
        "strategy_lab_partial_shadow": 12
    }
    payload["details"]["feed_cache"]["source_newest_age_sec"] = {
        "strategy_lab_partial_shadow": 4.0
    }
    result = monitor.source_observation(
        payload,
        source="strategy_lab|strategy_lab_partial_shadow",
        heartbeat_age_sec=1.0,
    )
    assert result["available"]
    assert result["source_count"] == 12
    assert result["source_newest_age_sec"] == 4.0


def test_gap_classification_excludes_executor_startup_from_cadence():
    assert monitor.gap_classification(
        {
            "executor_status": "running",
            "executor_uptime_sec": 4.0,
            "feed_generation": None,
            "reasons": ["source_absent"],
        }
    ) == "executor_startup_or_restart"
    assert monitor.gap_classification(
        {
            "executor_status": "running",
            "executor_uptime_sec": 400.0,
            "feed_generation": 10,
            "reasons": ["source_absent"],
        }
    ) == "source_publication_expiry"


def test_weekend_closure_is_not_an_operational_source_gap():
    assert monitor.market_state(
        dt.datetime(2026, 8, 8, 12, tzinfo=dt.timezone.utc)
    ) == "weekend_closed"
    observation = {
        "market_state": "weekend_closed", "executor_status": "running",
        "executor_uptime_sec": 400.0, "feed_generation": 10,
        "reasons": ["source_absent"],
    }
    assert monitor.gap_classification(observation) == "market_closed_expected"


def test_open_gap_is_reclassified_and_weekend_movement_suppressed():
    start = {"EUR_USD": {"bid": 1.1, "ask": 1.1002, "mid": 1.1001, "pip": 0.0001, "spread_pips": 2.0}}
    end = {"EUR_USD": {"bid": 1.2, "ask": 1.2002, "mid": 1.2001, "pip": 0.0001, "spread_pips": 2.0}}
    state, _ = monitor.update_state({}, {
        "available": False, "market_state": "open_or_transition",
        "executor_status": "running", "executor_uptime_sec": 400.0,
        "feed_generation": 10, "reasons": ["source_absent"],
    }, 100.0, start)
    state, _ = monitor.update_state(state, {
        "available": False, "market_state": "weekend_closed",
        "executor_status": "running", "executor_uptime_sec": 500.0,
        "feed_generation": 11, "reasons": ["source_absent"],
    }, 200.0, end)
    assert state["current_gap_classification"] == "market_closed_expected"
    assert state["current_gap_market_movement"] == []


def test_update_state_records_one_completed_gap():
    start_quotes = {
        "EUR_USD": {
            "bid": 1.099925,
            "ask": 1.100075,
            "mid": 1.1000,
            "pip": 0.0001,
            "spread_pips": 1.5,
        }
    }
    end_quotes = {
        "EUR_USD": {
            "bid": 1.10033,
            "ask": 1.10047,
            "mid": 1.1004,
            "pip": 0.0001,
            "spread_pips": 1.4,
        }
    }
    state, started = monitor.update_state(
        {}, {"available": False}, 100.0, start_quotes
    )
    state, quiet = monitor.update_state(
        state, {"available": False}, 103.0, start_quotes
    )
    state, ended = monitor.update_state(
        state, {"available": True}, 107.5, end_quotes
    )
    assert started["event"] == "availability_gap_started"
    assert quiet is None
    assert ended["event"] == "availability_gap_ended"
    assert state["gap_count"] == 1
    assert state["last_completed_gap_sec"] == 7.5
    assert state["current_gap_duration_sec"] == 0.0
    assert ended["market_movement"][0]["instrument"] == "EUR_USD"
    assert ended["market_movement"][0]["signed_mid_move_pips"] == 4.0


def test_gap_candidate_outcome_uses_executable_quote_sides():
    candidates = [
        {
            "id": "buy-1",
            "instrument": "EUR_USD",
            "direction": "buy",
            "execution_horizon_sec": 900,
            "execution_permitted_after_conflict_gate": True,
        }
    ]
    start = {
        "EUR_USD": {
            "bid": 1.1000,
            "ask": 1.1002,
            "mid": 1.1001,
            "pip": 0.0001,
            "spread_pips": 2.0,
        }
    }
    end = {
        "EUR_USD": {
            "bid": 1.1005,
            "ask": 1.1007,
            "mid": 1.1006,
            "pip": 0.0001,
            "spread_pips": 2.0,
        }
    }

    result = monitor.candidate_gap_outcomes(candidates, start, end)

    assert result[0]["gross_move_pips"] == 5.0
    assert result[0]["net_move_pips"] == 3.0
    assert result[0]["would_cover_round_trip_cost"] is True


def test_quote_points_and_gap_movement_are_cost_aware():
    points = monitor.quote_points(
        {
            "quotes": {
                "USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01},
                "BAD": {"bid": 2.0, "ask": 1.0, "pip": 0.0001},
            }
        }
    )
    assert set(points) == {"USD_JPY"}
    moved = monitor.gap_market_movement(
        points,
        {
            "USD_JPY": {
                "bid": points["USD_JPY"]["bid"] + 0.03,
                "ask": points["USD_JPY"]["ask"] + 0.03,
                "mid": points["USD_JPY"]["mid"] + 0.03,
                "pip": 0.01,
                "spread_pips": 1.5,
            }
        },
    )
    assert moved[0]["absolute_mid_move_pips"] == 3.0
    assert moved[0]["absolute_move_to_start_spread"] == 1.5
