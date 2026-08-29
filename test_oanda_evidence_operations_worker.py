from __future__ import annotations

import json
import time

from trad.oanda_evidence_operations_worker import (
    OperationsProgressHeartbeat,
    canary_authorization_payload,
    report_markdown,
)


def test_canary_authorization_never_auto_routes_confirmed_cells():
    payload = {
        "generated_utc": "2026-08-06T20:00:00+00:00",
        "lifecycle": {
            "lifecycle": {"states": {"confirmed_candidate": 2}}
        },
        "allocator": {"evidence": {"cohort_id": "allocator-1"}},
        "accounting": {
            "accounting": {
                "account_operational_continuity": {"account_state_current": True}
            },
            "routeability_sentinel": {"passed": True},
        },
    }

    authorization = canary_authorization_payload(payload)

    assert authorization["confirmed_candidate_count"] == 2
    assert authorization["entry_authorized"] is False
    assert authorization["authorized_entries"] == []
    assert authorization["auto_route"] is False
    assert authorization["reason"] == (
        "explicit_version_locked_practice_canary_activation_required"
    )


def test_canary_authorization_fails_closed_when_account_is_unavailable():
    payload = {
        "generated_utc": "2026-08-06T20:00:00+00:00",
        "lifecycle": {"lifecycle": {"states": {"confirmed_candidate": 2}}},
        "allocator": {"evidence": {"cohort_id": "allocator-1"}},
        "accounting": {
            "accounting": {
                "account_operational_continuity": {
                    "account_state_current": False,
                    "snapshot_state": "unavailable",
                }
            },
            "routeability_sentinel": {"passed": True},
        },
    }
    authorization = canary_authorization_payload(payload)
    assert authorization["entry_authorized"] is False
    assert authorization["account_state_current"] is False
    assert authorization["reason"] == "account_state_unavailable"


def test_operations_report_exposes_entry_authorization_state():
    payload = {
        "generated_utc": "2026-08-06T20:00:00+00:00",
        "lifecycle": {"lifecycle": {"states": {}}},
        "allocator": {"evidence": {}},
        "accounting": {"accounting": {}, "routeability_sentinel": {}},
        "opportunity_decision_level": {
            "collector_cohort_id": "collector",
            "decision_epochs": 4,
            "raw_pair_rows": 100,
            "top_one": {"matured_rows": 3, "cost_clearing_rows": 2,
                        "average_predicted_side_net_pips": -0.2},
            "exactly_three_currency_disjoint": {
                "decisions": 4, "matured_rows": 9,
                "average_predicted_side_net_pips": -0.1,
            },
        },
        "practice_canary_authorization": {
            "entry_authorized": False,
            "authorized_entries": [],
            "reason": "zero_confirmed_candidates",
        },
    }

    report = report_markdown(payload)

    assert "Governed new-entry authorization: **False**" in report
    assert "zero_confirmed_candidates" in report
    assert "Executable-opportunity decision level" in report


def test_progress_heartbeat_is_separate_and_fail_closed(tmp_path):
    path = tmp_path / "liveness.json"
    heartbeat = OperationsProgressHeartbeat(path, interval_sec=0.02)
    heartbeat.start()
    try:
        heartbeat.update("running_lifecycle", {"cycle": 1})
        time.sleep(0.04)
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["phase"] == "running_lifecycle"
        assert payload["progress_sequence"] == 1
        assert payload["research_only"] is True
        assert payload["can_place_orders"] is False
        assert payload["can_submit_orders"] is False
        assert payload["can_promote"] is False
        assert payload["real_money_routing"] is False
        assert "lifecycle" not in payload
        assert payload["progress_age_sec"] > 0
    finally:
        heartbeat.stop()


def test_progress_heartbeat_records_stage_completion_without_overwriting_report(tmp_path):
    path = tmp_path / "liveness.json"
    heartbeat = OperationsProgressHeartbeat(path, interval_sec=0.02)
    heartbeat.start()
    try:
        heartbeat.update("publishing_final_state")
        heartbeat.set_cycle_complete({"last_completed_utc": "2026-08-24T19:00:00+00:00"})
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["phase"] == "idle_between_cycles"
        assert payload["cycles"] == 1
        assert payload["details"]["last_completed_utc"] == "2026-08-24T19:00:00+00:00"
        assert payload["progress_sequence"] == 2
    finally:
        heartbeat.stop()
