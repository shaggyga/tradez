from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trad import oanda_scheduled_event_quote_capture_v1 as scheduled_capture
from trad import oanda_scheduled_event_quote_capture_v2 as scheduled_capture_v2
from trad.oanda_project_integrity_audit import (
    CONTINUOUS_NARRATIVE_BUCKET_MINUTES,
    CONTINUOUS_NARRATIVE_METER_CONTRACT,
    CONTINUOUS_NARRATIVE_SEAL_GRACE_SECONDS,
    CURRENT_NEWS_CLASSIFICATION_VERSION,
    EXPECTED_CONFIGURED_NEWS_SOURCES,
    _claim_audit_publication,
    _publish_owned_audit,
    audit_snapshot_publication_freshness,
    availability_market_state_consistent,
    collector_cohort_bound,
    continuous_narrative_meter_integrity,
    current_source_provenance_integrity,
    cross_pair_snapshot_routable,
    capture_current_quote_snapshot,
    event_technical_preflight_source_integrity,
    executable_move_census_v2e_integrity,
    executable_opportunity_proof_is_fail_closed,
    forex_market_state,
    historical_archive_gap_digest_preserved,
    independent_verifier_operationally_safe,
    integrity_runtime_contract,
    lifecycle_genealogy_sync_is_current,
    latest_sealable_clock,
    live_move_forward_proof_integrity,
    live_move_persistent_context_integrity,
    major_move_gap_census_progress_status,
    move_first_live_case_capture_is_current,
    move_first_live_arm_alignment_is_current,
    move_first_operational_mapping_alignment_is_current,
    move_first_operational_mapping_alignment_is_preserved_baseline,
    news_outcome_improvement_integrity,
    live_feature_coverage_diagnostic,
    move_first_news_audit_is_current,
    news_consumer_classification_bound,
    official_source_depth_integrity,
    official_release_fast_lane_integrity,
    official_release_fast_mapping_integrity,
    official_release_fast_response_integrity,
    official_detail_quality_integrity,
    persistent_policy_state_integrity,
    quote_tradeability_contract_current,
    read_json,
    rotate_jsonl_history_if_needed,
    scheduled_event_quote_capture_integrity,
    scheduled_event_quote_capture_v2_integrity,
    source_governance_collector_bound,
    source_governance_fast_lane_adapter_integrity,
    source_conditioned_rank_v6_inventory_integrity,
    source_conditioned_rank_v7_inventory_integrity,
    structured_numeric_currency_integrity,
    synchronized_quote_audit,
)


def test_integrity_runtime_contract_exposes_loaded_news_classification() -> None:
    assert integrity_runtime_contract() == {
        "classification_version": CURRENT_NEWS_CLASSIFICATION_VERSION
    }


def test_read_json_retries_a_transient_atomic_replace_race() -> None:
    class FlakySnapshot:
        def __init__(self) -> None:
            self.reads = 0

        def read_text(self, *, encoding: str) -> str:
            assert encoding == "utf-8"
            self.reads += 1
            if self.reads == 1:
                raise PermissionError("bounded Windows replace race")
            return '{"status":"ok"}'

    snapshot = FlakySnapshot()
    assert read_json(snapshot, retry_delay_sec=0.0) == {"status": "ok"}
    assert snapshot.reads == 2


def test_audit_snapshot_publication_freshness_is_fail_closed() -> None:
    fresh = audit_snapshot_publication_freshness(
        "2026-09-01T19:00:00Z",
        "2026-09-01T19:02:59Z",
    )
    stale = audit_snapshot_publication_freshness(
        "2026-09-01T19:00:00Z",
        "2026-09-01T19:03:01Z",
    )
    malformed = audit_snapshot_publication_freshness(
        "not-a-time",
        "2026-09-01T19:00:00Z",
    )
    assert fresh["ok"] is True
    assert fresh["age_sec"] == 179.0
    assert stale["ok"] is False
    assert stale["age_sec"] == 181.0
    assert malformed["ok"] is False
    assert malformed["age_sec"] is None


def test_lifecycle_genealogy_sync_requires_exact_inert_count_match() -> None:
    lifecycle = {
        "lifecycle": {"hypothesis_count": 52883},
        "genealogy_sync": {
            "ok": True,
            "status": "synchronized",
            "lifecycle_hypotheses": 52883,
            "genealogy_governed_cells": 52883,
            "research_only": True,
            "can_place_orders": False,
            "can_promote": False,
        },
    }
    assert lifecycle_genealogy_sync_is_current(lifecycle) is True
    assert lifecycle_genealogy_sync_is_current(
        {
            **lifecycle,
            "genealogy_sync": {
                **lifecycle["genealogy_sync"],
                "genealogy_governed_cells": 52882,
            },
        }
    ) is False
    assert lifecycle_genealogy_sync_is_current(
        {"lifecycle": {"hypothesis_count": 52883}}
    ) is False


def _scheduled_capture_state(generated_utc: str) -> dict[str, object]:
    return {
        "schema_version": scheduled_capture.SCHEMA_VERSION,
        "contract_id": scheduled_capture.CONTRACT_ID,
        "cohort_id": scheduled_capture.COHORT_ID,
        "activated_utc": scheduled_capture.iso_utc(scheduled_capture.ACTIVATED_UTC),
        "generated_utc": generated_utc,
        "status": "ok",
        "database_integrity": "ok",
        "counts": {
            "registered_event_clocks": 1,
            "terminal_capture_attempts": 0,
            "exact_all68_captures": 0,
            "invalid_terminal_captures": 0,
            "proof_quote_rows": 0,
        },
        "horizons_min": list(scheduled_capture.HORIZONS_MIN),
        "expected_instrument_count": scheduled_capture.EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": (
            scheduled_capture.EXPECTED_UNIVERSE_SHA256
        ),
        "clock_semantics": "scheduled_release_time_not_source_first_seen_time",
        "direction_policy": "abstain",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }


def _scheduled_capture_database(path: Path, *, valid_hash: bool = True) -> None:
    event = {
        "event_id": "event-rbnz",
        "event_series_id": "rbnz-ocr",
        "headline": "Reserve Bank of New Zealand Monetary Policy Decision",
        "scheduled_utc": "2026-09-02T02:00:00+00:00",
        "driver_currency": "NZD",
        "direct_currencies": ["NZD"],
        "affected_currencies": ["NZD"],
        "rows": [],
    }
    event_json = scheduled_capture.canonical_json(event)
    event_hash = scheduled_capture.sha256_text(event_json)
    if not valid_hash:
        event_hash = "0" * 64
    with scheduled_capture.open_database(path) as connection:
        connection.execute(
            """
            INSERT INTO scheduled_event_clock (
              clock_id,event_id,event_series_id,headline,scheduled_utc,
              driver_currency,direct_currencies_json,affected_currencies_json,
              registered_utc,registration_lead_sec,preflight_generated_utc,
              preflight_payload_sha256,event_payload_json,event_payload_sha256,
              research_only,execution_eligible,can_place_orders,can_authorize,
              can_promote,contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "clock-rbnz",
                "event-rbnz",
                "rbnz-ocr",
                event["headline"],
                event["scheduled_utc"],
                "NZD",
                '["NZD"]',
                '["NZD"]',
                "2026-09-02T01:00:00+00:00",
                3600.0,
                "2026-09-02T00:59:59+00:00",
                "1" * 64,
                event_json,
                event_hash,
                1,
                0,
                0,
                0,
                0,
                scheduled_capture.CONTRACT_ID,
                scheduled_capture.COHORT_ID,
            ),
        )
        connection.commit()


def test_scheduled_event_quote_capture_integrity_is_independent_and_fail_closed(
    tmp_path: Path,
) -> None:
    database = tmp_path / "scheduled.sqlite"
    _scheduled_capture_database(database)
    cutoff = datetime(2026, 9, 2, 1, 0, 5, tzinfo=timezone.utc).timestamp()
    state = _scheduled_capture_state("2026-09-02T01:00:04+00:00")
    valid = scheduled_event_quote_capture_integrity(
        state,
        database_path=database,
        cutoff_epoch=cutoff,
    )
    assert valid["ok"] is True
    assert valid["append_only_triggers_ok"] is True
    assert valid["database_counts"]["registered_event_clocks"] == 1

    count_mismatch = {
        **state,
        "counts": {**state["counts"], "registered_event_clocks": 2},
    }
    assert scheduled_event_quote_capture_integrity(
        count_mismatch,
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert scheduled_event_quote_capture_integrity(
        {**state, "can_place_orders": True},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert scheduled_event_quote_capture_integrity(
        {**state, "generated_utc": "2026-09-02T00:00:00+00:00"},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False


def test_scheduled_event_quote_capture_integrity_rejects_hash_tamper(
    tmp_path: Path,
) -> None:
    database = tmp_path / "scheduled.sqlite"
    _scheduled_capture_database(database, valid_hash=False)
    cutoff = datetime(2026, 9, 2, 1, 0, 5, tzinfo=timezone.utc).timestamp()
    result = scheduled_event_quote_capture_integrity(
        _scheduled_capture_state("2026-09-02T01:00:04+00:00"),
        database_path=database,
        cutoff_epoch=cutoff,
    )
    assert result["payload_hashes_ok"] is False
    assert result["ok"] is False


def _scheduled_capture_v2_state(generated_utc: str) -> dict[str, object]:
    return {
        "schema_version": scheduled_capture_v2.SCHEMA_VERSION,
        "contract_id": scheduled_capture_v2.CONTRACT_ID,
        "cohort_id": scheduled_capture_v2.COHORT_ID,
        "generated_utc": generated_utc,
        "status": "ok",
        "database_integrity": "ok",
        "counts": {
            "registered_event_clocks": 1,
            "terminal_capture_attempts": 0,
            "valid_current_snapshots": 0,
            "invalid_terminal_captures": 0,
            "observed_universe_rows": 0,
            "tradeable_proof_rows": 0,
            "nontradeable_observed_rows": 0,
            "direct_event_tradeable_rows": 0,
        },
        "horizons_min": list(scheduled_capture_v2.HORIZONS_MIN),
        "expected_instrument_count": 68,
        "expected_instrument_universe_sha256": (
            scheduled_capture_v2.EXPECTED_UNIVERSE_SHA256
        ),
        "quote_acquisition": "one_read_only_oanda_practice_pricing_request_all68",
        "currentness_clock": "pricing_response_observed_utc",
        "broker_price_time_role": "preserved_diagnostic_not_currentness_gate",
        "direction_policy": "abstain",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }


def _scheduled_capture_v2_database(path: Path) -> None:
    event = {
        "event_id": "future-boc",
        "event_series_id": "boc-policy",
        "headline": "Bank of Canada Monetary Policy Interest Rate Decision",
        "scheduled_utc": "2026-09-02T13:45:00+00:00",
        "driver_currency": "CAD",
        "direct_currencies": ["CAD"],
        "affected_currencies": ["CAD"],
        "rows": [],
    }
    event_json = scheduled_capture_v2.canonical_json(event)
    with scheduled_capture_v2.open_database(path) as connection:
        connection.execute(
            """
            INSERT INTO scheduled_event_clock_v2 (
              clock_id,event_id,event_series_id,headline,scheduled_utc,
              driver_currency,direct_currencies_json,affected_currencies_json,
              registered_utc,registration_lead_sec,preflight_generated_utc,
              preflight_payload_sha256,event_payload_json,event_payload_sha256,
              research_only,execution_eligible,can_place_orders,can_authorize,
              can_promote,contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "clock-boc", event["event_id"], event["event_series_id"],
                event["headline"], event["scheduled_utc"], "CAD", '["CAD"]',
                '["CAD"]', "2026-09-02T12:00:00+00:00", 6300.0,
                "2026-09-02T11:59:59+00:00", "1" * 64, event_json,
                scheduled_capture_v2.sha256_text(event_json), 1, 0, 0, 0, 0,
                scheduled_capture_v2.CONTRACT_ID,
                scheduled_capture_v2.COHORT_ID,
            ),
        )
        connection.commit()


def test_scheduled_event_quote_capture_v2_integrity_is_independent_and_inert(
    tmp_path: Path,
) -> None:
    database = tmp_path / "scheduled_v2.sqlite"
    _scheduled_capture_v2_database(database)
    cutoff = datetime(2026, 9, 2, 12, 0, 5, tzinfo=timezone.utc).timestamp()
    state = _scheduled_capture_v2_state("2026-09-02T12:00:04+00:00")
    result = scheduled_event_quote_capture_v2_integrity(
        state, database_path=database, cutoff_epoch=cutoff
    )
    assert result["ok"] is True
    assert result["append_only_triggers_ok"] is True
    assert result["database_counts"]["registered_event_clocks"] == 1
    assert scheduled_event_quote_capture_v2_integrity(
        {**state, "can_authorize": True},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert scheduled_event_quote_capture_v2_integrity(
        {**state, "generated_utc": "2026-09-02T11:00:00+00:00"},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False


def test_executable_move_census_current_requires_fresh_independent_agreement() -> None:
    cutoff = 1_788_282_000.0
    stamp = datetime.fromtimestamp(cutoff - 10, timezone.utc).isoformat()
    state = {
        "schema_version": "executable_move_census_latest_v1",
        "generated_utc": stamp,
        "cohort_id": "all68_executable_move_census_v3_20260902h",
        "status": "collecting_pre_activation",
        "instrument_count": 68,
        "side_count": 136,
        "frame_count": 0,
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
    }
    verifier = {
        "schema_version": "executable_move_census_verifier_v1",
        "generated_utc": stamp,
        "audit_finished_utc": stamp,
        "latest_generated_utc": stamp,
        "cohort_id": "all68_executable_move_census_v3_20260902h",
        "verified": True,
        "failure_count": 0,
        "failures": [],
        "counts": {"frames": 0, "frame_quotes": 0},
        "semantic_validity": {
            "valid_quote_rows": 0,
            "invalid_quote_rows": 0,
            "source_identity_mismatch_rows": 0,
            "zero_valid_open_market_frames": 0,
        },
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    result = executable_move_census_v2e_integrity(
        state, verifier, cutoff_epoch=cutoff
    )
    assert result["ok"]
    active_state = dict(state)
    active_state.pop("status")
    active_state.update({
        "frame_count": 4,
        "latest_frame_utc": stamp,
        "schedule_census": {
            "expected_open_frames": 4,
            "observed_frames": 4,
            "missing_open_frames": 0,
        },
    })
    active_verifier = dict(verifier)
    active_verifier["counts"] = {"frames": 4, "frame_quotes": 272}
    active_verifier["semantic_validity"] = {
        "valid_quote_rows": 224,
        "invalid_quote_rows": 48,
        "source_identity_mismatch_rows": 0,
        "zero_valid_open_market_frames": 0,
    }
    active_result = executable_move_census_v2e_integrity(
        active_state, active_verifier, cutoff_epoch=cutoff
    )
    assert active_result["ok"]
    assert active_result["active_state_ok"]
    assert active_result["semantic_ok"]
    database_suffix_verifier = dict(active_verifier)
    database_suffix_verifier["latest_frame_count"] = 4
    database_suffix_verifier["counts"] = {
        "frames": 8,
        "frame_quotes": 544,
    }
    database_suffix_verifier["semantic_validity"] = {
        "valid_quote_rows": 496,
        "invalid_quote_rows": 48,
        "source_identity_mismatch_rows": 0,
        "zero_valid_open_market_frames": 0,
    }
    database_suffix_result = executable_move_census_v2e_integrity(
        active_state, database_suffix_verifier, cutoff_epoch=cutoff
    )
    assert database_suffix_result["ok"]
    assert database_suffix_result["verified_latest_frames"] == 4
    assert database_suffix_result["verified_database_frames"] == 8
    assert database_suffix_result["database_append_suffix_frames"] == 4
    assert database_suffix_result["verification_lag_frames"] == 0
    lagged_state = dict(active_state)
    lagged_state["frame_count"] = 7
    lagged_result = executable_move_census_v2e_integrity(
        lagged_state, active_verifier, cutoff_epoch=cutoff
    )
    assert lagged_result["ok"]
    assert lagged_result["verification_lag_frames"] == 3
    slow_verifier = dict(active_verifier)
    slow_verifier["generated_utc"] = datetime.fromtimestamp(
        cutoff - 700, timezone.utc
    ).isoformat()
    slow_verifier["audit_finished_utc"] = stamp
    slow_verifier["latest_generated_utc"] = datetime.fromtimestamp(
        cutoff - 710, timezone.utc
    ).isoformat()
    slow_result = executable_move_census_v2e_integrity(
        active_state, slow_verifier, cutoff_epoch=cutoff
    )
    assert slow_result["ok"]
    assert slow_result["verifier_duration_sec"] == 690.0
    rollover_state = dict(active_state)
    rollover_state["frame_count"] = 9
    rollover_verifier = dict(active_verifier)
    rollover_verifier["counts"] = {"frames": 9, "frame_quotes": 612}
    rollover_verifier["semantic_validity"] = {
        "valid_quote_rows": 224,
        "invalid_quote_rows": 388,
        "source_identity_mismatch_rows": 0,
        "zero_valid_open_market_frames": 0,
    }
    rollover_result = executable_move_census_v2e_integrity(
        rollover_state, rollover_verifier, cutoff_epoch=cutoff
    )
    assert rollover_result["ok"]
    assert rollover_result["active_state_ok"]
    incomplete_open_state = dict(rollover_state)
    incomplete_open_state["schedule_census"] = {
        "expected_open_frames": 4,
        "observed_frames": 3,
        "missing_open_frames": 1,
    }
    assert not executable_move_census_v2e_integrity(
        incomplete_open_state, rollover_verifier, cutoff_epoch=cutoff
    )["ok"]
    invalid_semantic_verifier = dict(active_verifier)
    invalid_semantic_verifier["semantic_validity"] = {
        "valid_quote_rows": 0,
        "invalid_quote_rows": 272,
        "source_identity_mismatch_rows": 272,
        "zero_valid_open_market_frames": 4,
    }
    assert not executable_move_census_v2e_integrity(
        active_state, invalid_semantic_verifier, cutoff_epoch=cutoff
    )["ok"]
    active_state["status"] = "collecting_pre_activation"
    assert not executable_move_census_v2e_integrity(
        active_state, active_verifier, cutoff_epoch=cutoff
    )["ok"]
    verifier["counts"] = {"frames": 1, "frame_quotes": 68}
    assert not executable_move_census_v2e_integrity(
        state, verifier, cutoff_epoch=cutoff
    )["ok"]
    verifier["counts"] = {"frames": 0, "frame_quotes": 0}
    verifier["verified"] = False
    assert not executable_move_census_v2e_integrity(
        state, verifier, cutoff_epoch=cutoff
    )["ok"]


def _rank_inventory_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE source_factor_forecast(
          forecast_id TEXT PRIMARY KEY,
          factor_observation_id TEXT NOT NULL,
          canonical_event_id TEXT NOT NULL,
          currency TEXT NOT NULL,
          horizon_min INTEGER NOT NULL,
          issued_utc TEXT NOT NULL,
          forecast_state TEXT NOT NULL,
          abstain_reason TEXT NOT NULL,
          prospective_proof_eligible INTEGER NOT NULL,
          probability_strengthening REAL,
          predicted_currency_factor_bps REAL,
          predicted_absolute_factor_bps REAL,
          contract_id TEXT NOT NULL
        )
        """
    )
    rows = [
        (
            "abstain", "factor_a", "event_a", "JPY", 60,
            "2026-09-01T06:59:00+00:00", "abstain", "low_effective_n:1<8",
            0, None, None, None,
            "causal_source_factor_response_map_v7_v152_subject_bound_release_policy_targets_20260901",
        ),
        (
            "forecast", "factor_b", "event_b", "NOK", 60,
            "2026-09-01T06:59:30+00:00", "forecast", "",
            1, 0.7, 2.0, 2.0,
            "causal_source_factor_response_map_v7_v152_subject_bound_release_policy_targets_20260901",
        ),
        (
            "future", "factor_c", "event_c", "SEK", 30,
            "2026-09-01T07:01:00+00:00", "abstain", "future_row",
            0, None, None, None,
            "causal_source_factor_response_map_v7_v152_subject_bound_release_policy_targets_20260901",
        ),
    ]
    connection.executemany(
        "INSERT INTO source_factor_forecast VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    connection.commit()
    connection.close()


def test_source_rank_v6_inventory_reconciles_abstentions_at_snapshot_cutoff(
    tmp_path: Path,
) -> None:
    database = tmp_path / "source_v7.sqlite"
    _rank_inventory_database(database)
    state = {
        "generated_utc": "2026-09-01T07:00:00+00:00",
        "contract_id": (
            "source_conditioned_currency_rank_v6_v7_input_explicit_no_trade_20260901"
        ),
        "required_source_contract_id": (
            "causal_source_factor_response_map_v7_v152_subject_bound_release_policy_targets_20260901"
        ),
        "source_input_status": "ready",
        "source_forecast_rows": 1,
        "source_forecast_rows_semantics": (
            "rank_eligible_non_abstaining_rows_loaded_by_adapter"
        ),
        "supported_execution_decision": "no_trade",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "policy": {
            "abstain_inventory_is_diagnostic_only": True,
            "abstain_rows_can_trigger_rank_decisions": False,
        },
        "source_forecast_inventory": {
            "count_basis": "issued_utc_at_or_before_adapter_generated_utc",
            "total_rows": 2,
            "rank_eligible_rows": 1,
            "abstain_rows": 1,
            "excluded_from_rank_rows": 1,
            "distinct_event_count": 2,
            "distinct_factor_observation_count": 2,
            "forecast_state_counts": {"abstain": 1, "forecast": 1},
            "abstain_reason_counts": {"low_effective_n:1<8": 1},
            "currency_counts": {"JPY": 1, "NOK": 1},
            "horizon_counts": {"60": 2},
            "contract_counts": {
                "causal_source_factor_response_map_v7_v152_subject_bound_release_policy_targets_20260901": 2
            },
            "status": "rank_eligible_rows_available",
            "database_integrity": "ok",
        },
    }
    result = source_conditioned_rank_v6_inventory_integrity(
        state,
        source_database=database,
        cutoff_epoch=datetime(2026, 9, 1, 7, 0, 5, tzinfo=timezone.utc).timestamp(),
    )
    assert result["ok"] is True
    assert result["counts_ok"] is True
    assert result["total_rows"] == 2
    assert result["rank_eligible_rows"] == 1
    assert result["abstain_rows"] == 1

    state["source_forecast_inventory"]["abstain_rows"] = 2
    rejected = source_conditioned_rank_v6_inventory_integrity(
        state,
        source_database=database,
        cutoff_epoch=datetime(2026, 9, 1, 7, 0, 5, tzinfo=timezone.utc).timestamp(),
    )
    assert rejected["ok"] is False
    assert rejected["counts_ok"] is False


def test_source_rank_v7_wrapper_rejects_v7_contract_state(tmp_path: Path) -> None:
    database = tmp_path / "source_v7.sqlite"
    _rank_inventory_database(database)
    state = {
        "generated_utc": "2026-09-01T07:00:00+00:00",
        "contract_id": (
            "source_conditioned_currency_rank_v6_v7_input_explicit_no_trade_20260901"
        ),
        "required_source_contract_id": (
            "causal_source_factor_response_map_v7_"
            "v152_subject_bound_release_policy_targets_20260901"
        ),
        "source_input_status": "ready",
        "source_forecast_rows": 0,
        "source_forecast_rows_semantics": (
            "rank_eligible_non_abstaining_rows_loaded_by_adapter"
        ),
        "supported_execution_decision": "no_trade",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "policy": {
            "abstain_inventory_is_diagnostic_only": True,
            "abstain_rows_can_trigger_rank_decisions": False,
        },
        "source_forecast_inventory": {},
    }
    result = source_conditioned_rank_v7_inventory_integrity(
        state,
        source_database=database,
        cutoff_epoch=datetime(2026, 9, 1, 7, 0, 5, tzinfo=timezone.utc).timestamp(),
    )
    assert result["ok"] is False
    assert result["contract_ok"] is False


def test_direct_move_first_capture_requires_fresh_inert_equal_counts() -> None:
    cutoff = datetime(2026, 9, 1, 7, 10, tzinfo=timezone.utc).timestamp()
    state = {
        "schema_version": "move_first_live_case_capture_report_v1",
        "cohort_id": "move_first_live_case_capture_v4_prospective_20260901T070500Z",
        "source_distance": "direct_append_only_live_mover_database",
        "append_only": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
        "historical_rows_imported": "0",
        "sqlite_integrity": "ok",
        "factor_graph_cycle": False,
        "case_count": 3,
        "factor_membership_count": 3,
        "generated_utc": "2026-09-01T07:09:00Z",
    }
    assert move_first_live_case_capture_is_current(state, cutoff_epoch=cutoff)
    assert not move_first_live_case_capture_is_current(
        {**state, "factor_membership_count": 2}, cutoff_epoch=cutoff
    )
    assert not move_first_live_case_capture_is_current(
        {**state, "execution_decision": "trade"}, cutoff_epoch=cutoff
    )
    assert not move_first_live_case_capture_is_current(
        {**state, "generated_utc": "2026-09-01T07:00:00Z"}, cutoff_epoch=cutoff
    )


def test_move_first_arm_alignment_separates_pre_move_from_label_derived() -> None:
    cutoff = datetime(2026, 9, 1, 7, 12, tzinfo=timezone.utc).timestamp()
    capture = {
        "generated_utc": "2026-09-01T07:11:30Z",
        "case_count": 12,
        "resolved_factor_episode_count": 7,
    }
    pre_names = [f"pre_{index}" for index in range(10)]
    control_names = [f"control_{index}" for index in range(2)]
    arm_names = pre_names + control_names
    signature_payload = [[f"episode_{index}", 0] for index in range(7)]
    signature = hashlib.sha256(
        json.dumps(
            signature_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    common_metric = {
        "effective_episode_count": 7,
        "signaled_episode_count": 0,
        "abstained_episode_count": 7,
        "correct_direction_count": 0,
        "wrong_direction_count": 0,
        "directional_coverage_pct": 0.0,
        "directional_accuracy_pct": None,
        "aligned_observed_net_room_pips": 0.0,
        "opposed_observed_net_room_pips": 0.0,
        "signed_alignment_room_pips": 0.0,
        "decision_signature_sha256": signature,
    }
    pre = {
        **common_metric,
        "clock_classification": "available_at_or_before_move_start",
        "eligible_as_pre_move_directional_diagnostic": True,
        "interpretation": "move_conditioned_alignment_diagnostic_not_trade_pnl",
    }
    control = {
        **common_metric,
        "clock_classification": "label_derived_after_move_detection",
        "eligible_as_pre_move_directional_diagnostic": False,
        "interpretation": "move_conditioned_alignment_diagnostic_not_trade_pnl",
    }
    support_pre = {
        **pre,
        "interpretation": (
            "factor_support_qualified_move_conditioned_alignment_"
            "diagnostic_not_trade_pnl"
        ),
    }
    support_control = {
        **control,
        "interpretation": (
            "factor_support_qualified_move_conditioned_alignment_"
            "diagnostic_not_trade_pnl"
        ),
    }
    gap_contract = (
        "move_first_causal_gap_taxonomy_v4_narrative_family_concentration_exact_retained_clock_20260901"
    )
    narrative_contract = (
        "retained_story_narrative_family_v1_conservative_time_bounded_headline_component_20260901"
    )
    factor_support_contract = (
        "move_first_factor_support_v1_positive_aligned_primary_and_unambiguous_20260901"
    )
    episode_rows = [
        {
            "resolved_factor_episode_id": f"episode_{index}",
            "actual_direction": 1,
            "executable_net_room_pips": 2.0,
            "arms": {
                arm_name: {"direction": 0, "raw_value": None}
                for arm_name in arm_names
            },
            "factor_support_qualified": True,
            "factor_support": {
                "contract_id": factor_support_contract,
                "status": "supported",
                "support_qualified": True,
                "membership_rewritten": False,
            },
            "causal_gap": {
                "contract_id": gap_contract,
                "state": "strict_abstain_no_direction",
                "strict_abstention_reasons": {},
                "strict_forward_independent_story_count": 0,
                "retained_forward_research_story_count": 0,
                "retained_forward_aligned_story_count": 0,
                "retained_forward_opposed_story_count": 0,
                "retained_forward_research_stories": [],
                "interpretation": (
                    "move_conditioned_source_gap_diagnostic_not_prediction_or_trade_pnl"
                ),
            },
        }
        for index in range(7)
    ]
    state = {
        "schema_version": "move_first_live_arm_alignment_report_v1",
        "audit_id": "move_first_live_arm_alignment_v1_20260901",
        "source_cohort_id":
            "move_first_live_case_capture_v4_prospective_20260901T070500Z",
        "generated_utc": "2026-09-01T07:11:00Z",
        "source_case_count": 11,
        "resolved_factor_episode_count": 7,
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
        "arm_count": 12,
        "pre_move_arm_count": 10,
        "label_derived_control_arm_count": 2,
        "unique_decision_vector_count": 1,
        "pre_move_unique_decision_vector_count": 1,
        "decision_equivalence_groups": [
            {
                "decision_signature_sha256": signature,
                "arms": sorted(arm_names),
                "arm_count": 12,
            }
        ],
        "pre_move_decision_equivalence_groups": [
            {
                "decision_signature_sha256": signature,
                "arms": sorted(pre_names),
                "arm_count": 10,
            }
        ],
        "factor_support_audit": {
            "contract_id": factor_support_contract,
            "source_case_status_counts": {"supported": 11},
            "selected_episode_status_counts": {"supported": 7},
            "support_qualified_source_case_count": 11,
            "support_qualified_selected_episode_count": 7,
            "unsupported_source_case_count": 0,
            "unsupported_selected_episode_count": 0,
            "existing_membership_bytes_preserved": True,
            "changes_existing_arm_metrics": False,
            "promotion_eligible": False,
            "execution_eligible": False,
        },
        "causal_gap_taxonomy_contract_id": gap_contract,
        "causal_gap_taxonomy": {
            "episode_count": 7,
            "state_counts": {"strict_abstain_no_direction": 7},
            "strict_abstention_reason_episode_counts": {},
            "retained_forward_research_story_count": 0,
            "retained_forward_aligned_story_count": 0,
            "retained_forward_opposed_story_count": 0,
            "factor_episodes_with_retained_forward_story_count": 0,
            "factor_episodes_with_strict_independent_story_count": 0,
            "unique_retained_forward_story_count": 0,
            "unique_retained_forward_source_count": 0,
            "unique_retained_forward_story_cluster_count": 0,
            "factor_episodes_with_multiple_retained_story_clusters_count": 0,
            "narrative_family_contract_id": narrative_contract,
            "unique_retained_forward_narrative_family_count": 0,
            "factor_episodes_with_multiple_retained_narrative_families_count": 0,
            "dominant_retained_story_factor_episode_count": 0,
            "dominant_retained_story_factor_episode_pct": 0.0,
            "retained_story_concentration_top20": [],
            "dominant_retained_story_cluster_factor_episode_count": 0,
            "dominant_retained_story_cluster_factor_episode_pct": 0.0,
            "retained_story_cluster_concentration_top20": [],
            "dominant_retained_narrative_family_factor_episode_count": 0,
            "dominant_retained_narrative_family_factor_episode_pct": 0.0,
            "retained_narrative_family_concentration_top20": [],
            "interpretation": (
                "retained_pre_move_source_gap_and_syndication_concentration_diagnostic_on_move_conditioned_cases"
            ),
        },
        "episode_rows": episode_rows,
        "arm_metrics": {
            **{name: dict(pre) for name in pre_names},
            **{name: dict(control) for name in control_names},
        },
        "support_qualified_factor_episode_count": 7,
        "support_qualified_unique_decision_vector_count": 1,
        "support_qualified_pre_move_unique_decision_vector_count": 1,
        "support_qualified_decision_equivalence_groups": [
            {
                "decision_signature_sha256": signature,
                "arms": sorted(arm_names),
                "arm_count": 12,
            }
        ],
        "support_qualified_pre_move_decision_equivalence_groups": [
            {
                "decision_signature_sha256": signature,
                "arms": sorted(pre_names),
                "arm_count": 10,
            }
        ],
        "support_qualified_arm_metrics": {
            **{name: dict(support_pre) for name in pre_names},
            **{name: dict(support_control) for name in control_names},
        },
    }
    assert move_first_live_arm_alignment_is_current(
        state, capture, cutoff_epoch=cutoff
    )
    unsafe = json.loads(json.dumps(state))
    unsafe["arm_metrics"]["control_0"][
        "eligible_as_pre_move_directional_diagnostic"
    ] = True
    assert not move_first_live_arm_alignment_is_current(
        unsafe, capture, cutoff_epoch=cutoff
    )
    assert not move_first_live_arm_alignment_is_current(
        {**state, "predictive_backtest_eligible": True},
        capture,
        cutoff_epoch=cutoff,
    )
    assert not move_first_live_arm_alignment_is_current(
        {**state, "causal_gap_taxonomy_contract_id": "wrong"},
        capture,
        cutoff_epoch=cutoff,
    )
    bad_count = json.loads(json.dumps(state))
    bad_count["causal_gap_taxonomy"]["episode_count"] = 6
    assert not move_first_live_arm_alignment_is_current(
        bad_count, capture, cutoff_epoch=cutoff
    )
    bad_factor_support = json.loads(json.dumps(state))
    bad_factor_support["factor_support_audit"][
        "unsupported_selected_episode_count"
    ] = 1
    assert not move_first_live_arm_alignment_is_current(
        bad_factor_support, capture, cutoff_epoch=cutoff
    )
    bad_support_metric = json.loads(json.dumps(state))
    bad_support_metric["support_qualified_arm_metrics"]["pre_0"][
        "correct_direction_count"
    ] = 1
    assert not move_first_live_arm_alignment_is_current(
        bad_support_metric, capture, cutoff_epoch=cutoff
    )
    bad_support_group = json.loads(json.dumps(state))
    bad_support_group["support_qualified_decision_equivalence_groups"][0][
        "arm_count"
    ] = 11
    assert not move_first_live_arm_alignment_is_current(
        bad_support_group, capture, cutoff_epoch=cutoff
    )


def test_operational_mapping_alignment_requires_pre_move_receipts_and_inertness() -> None:
    now = datetime.now(timezone.utc)
    generated = now.isoformat()
    move_start = now.replace(microsecond=0).isoformat()
    before_move = (now.replace(microsecond=0) - timedelta(seconds=30)).isoformat()
    arm_names = {
        "legacy_strict",
        "legacy_broad",
        "receipt_backed_strict",
        "receipt_backed_broad",
        "technical_continuation_control",
    }
    metrics = {
        name: {
            "episode_count": 1,
            "signaled_episode_count": 1,
            "aligned_episode_count": 1,
            "opposed_episode_count": 0,
            "abstained_episode_count": 0,
        }
        for name in arm_names
    }
    row = {
        "move_start_utc": move_start,
        "operational_event_receipts": [
            {
                "mapping_receipt_id": "receipt",
                "mapping_available_utc": before_move,
                "operational_effective_from_utc": before_move,
                "mapping_contract_id": "news_source_governance_fast_lane_v2_first_seen_prospective_20260901",
                "mapping_kind": "general_news_fast_lane",
            }
        ],
        "operational_pre_move_event_count": 1,
        "operational_independent_story_count": 1,
        "legacy_recent_story_mapping_status_counts": {"admitted": 1},
        "directions": {name: 1 for name in arm_names},
        "alignments": {name: "aligned" for name in arm_names},
        "source_case_bytes_rewritten": False,
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }
    state = {
        "schema_version": "move_first_operational_mapping_alignment_report_v1",
        "generated_utc": generated,
        "contract_id": "move_first_operational_mapping_alignment_v1_receipt_backed_prospective_20260901T140000Z",
        "cohort_id": "move_first_operational_mapping_alignment_v1_prospective_20260901T140000Z",
        "ledger_cutoff_utc": generated,
        "factor_merge_cutoff_utc": generated,
        "source_case_count": 1,
        "resolved_factor_episode_count": 1,
        "arm_metrics": metrics,
        "episode_rows": [row],
        "source_case_bytes_rewritten": False,
        "historical_rows_imported": 0,
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }
    capture = {"generated_utc": generated, "case_count": 1}
    cutoff = now.timestamp()
    assert move_first_operational_mapping_alignment_is_current(
        state, capture, cutoff_epoch=cutoff
    )
    assert move_first_operational_mapping_alignment_is_preserved_baseline(
        state, capture
    )

    retired_cutoff = cutoff + 3600.0
    assert not move_first_operational_mapping_alignment_is_current(
        state, capture, cutoff_epoch=retired_cutoff
    )
    assert move_first_operational_mapping_alignment_is_preserved_baseline(
        state,
        {"generated_utc": (now + timedelta(hours=1)).isoformat(), "case_count": 2},
    )

    late = json.loads(json.dumps(state))
    late["episode_rows"][0]["operational_event_receipts"][0][
        "mapping_available_utc"
    ] = (now + timedelta(seconds=1)).isoformat()
    assert not move_first_operational_mapping_alignment_is_current(
        late, capture, cutoff_epoch=cutoff
    )
    subsecond_late = json.loads(json.dumps(state))
    subsecond_late["episode_rows"][0]["operational_event_receipts"][0][
        "mapping_available_utc"
    ] = (now.replace(microsecond=0) + timedelta(microseconds=757212)).isoformat()
    assert not move_first_operational_mapping_alignment_is_current(
        subsecond_late, capture, cutoff_epoch=cutoff
    )
    unsafe = json.loads(json.dumps(state))
    unsafe["episode_rows"][0]["execution_eligible"] = True
    assert not move_first_operational_mapping_alignment_is_current(
        unsafe, capture, cutoff_epoch=cutoff
    )
    assert not move_first_operational_mapping_alignment_is_preserved_baseline(
        unsafe, capture
    )
    wrong_contract = json.loads(json.dumps(state))
    wrong_contract["episode_rows"][0]["operational_event_receipts"][0][
        "mapping_contract_id"
    ] = "obsolete"
    assert not move_first_operational_mapping_alignment_is_current(
        wrong_contract, capture, cutoff_epoch=cutoff
    )

    v2 = json.loads(json.dumps(state))
    v2_contract = (
        "move_first_operational_mapping_alignment_v2_receipt_backed_"
        "syndication_dedup_prospective_20260901T150000Z"
    )
    v2_cohort = (
        "move_first_operational_mapping_alignment_v2_prospective_"
        "20260901T150000Z"
    )
    v2_rule = "publisher_suffix_stripped_normalized_headline_v2"
    v2["contract_id"] = v2_contract
    v2["cohort_id"] = v2_cohort
    v2["story_deduplication_rule"] = v2_rule
    v2["operational_syndicated_duplicate_count"] = 0
    v2["episode_rows"][0]["story_deduplication_rule"] = v2_rule
    v2["episode_rows"][0]["operational_syndicated_duplicate_count"] = 0
    assert move_first_operational_mapping_alignment_is_current(
        v2,
        capture,
        cutoff_epoch=cutoff,
        expected_contract_id=v2_contract,
        expected_cohort_id=v2_cohort,
        expected_story_deduplication_rule=v2_rule,
    )

    v3 = json.loads(json.dumps(v2))
    v3_contract = (
        "move_first_operational_mapping_alignment_v3_receipt_backed_"
        "narrative_family_age_decay_prospective_20260901T170000Z"
    )
    v3_cohort = (
        "move_first_operational_mapping_alignment_v3_prospective_"
        "20260901T170000Z"
    )
    v3_rule = "conservative_time_bounded_narrative_family_v3"
    v3["contract_id"] = v3_contract
    v3["cohort_id"] = v3_cohort
    v3["story_deduplication_rule"] = v3_rule
    v3["broad_age_decay_half_life_minutes"] = 30.0
    v3["operational_story_family_duplicate_count"] = 0
    v3["episode_rows"][0]["story_deduplication_rule"] = v3_rule
    v3["episode_rows"][0]["broad_age_decay_half_life_minutes"] = 30.0
    v3["episode_rows"][0]["operational_story_family_duplicate_count"] = 0
    assert move_first_operational_mapping_alignment_is_current(
        v3,
        capture,
        cutoff_epoch=cutoff,
        expected_contract_id=v3_contract,
        expected_cohort_id=v3_cohort,
        expected_story_deduplication_rule=v3_rule,
        expected_story_duplicate_count_field=(
            "operational_story_family_duplicate_count"
        ),
        expected_broad_age_decay_half_life_minutes=30.0,
    )
    v4 = json.loads(json.dumps(v3))
    v4["contract_id"] = (
        "move_first_operational_mapping_alignment_v4_receipt_backed_"
        "subsecond_causal_narrative_family_prospective_20260902T121500Z"
    )
    v4["cohort_id"] = (
        "move_first_operational_mapping_alignment_v4_prospective_"
        "20260902T121500Z"
    )
    v4["operational_clock_rule"] = (
        "max_source_effective_detail_available_and_mapping_receipt_"
        "available_utc_subsecond_precision"
    )
    assert move_first_operational_mapping_alignment_is_current(
        v4,
        capture,
        cutoff_epoch=cutoff,
        expected_contract_id=v4["contract_id"],
        expected_cohort_id=v4["cohort_id"],
        expected_story_deduplication_rule=v3_rule,
        expected_story_duplicate_count_field=(
            "operational_story_family_duplicate_count"
        ),
        expected_broad_age_decay_half_life_minutes=30.0,
        expected_operational_clock_rule=v4["operational_clock_rule"],
    )

    family_duplicate_v3 = json.loads(json.dumps(v3))
    duplicate_receipt = json.loads(
        json.dumps(family_duplicate_v3["episode_rows"][0]["operational_event_receipts"][0])
    )
    duplicate_receipt["mapping_receipt_id"] = "receipt-family-duplicate"
    family_duplicate_v3["episode_rows"][0]["operational_event_receipts"].append(
        duplicate_receipt
    )
    family_duplicate_v3["episode_rows"][0]["operational_pre_move_event_count"] = 2
    family_duplicate_v3["episode_rows"][0][
        "operational_story_family_duplicate_count"
    ] = 1
    family_duplicate_v3["operational_story_family_duplicate_count"] = 1
    assert move_first_operational_mapping_alignment_is_current(
        family_duplicate_v3,
        capture,
        cutoff_epoch=cutoff,
        expected_contract_id=v3_contract,
        expected_cohort_id=v3_cohort,
        expected_story_deduplication_rule=v3_rule,
        expected_story_duplicate_count_field=(
            "operational_story_family_duplicate_count"
        ),
        expected_broad_age_decay_half_life_minutes=30.0,
    )
    assert not move_first_operational_mapping_alignment_is_current(
        family_duplicate_v3,
        capture,
        cutoff_epoch=cutoff,
        expected_contract_id=v3_contract,
        expected_cohort_id=v3_cohort,
        expected_story_deduplication_rule=v3_rule,
        expected_broad_age_decay_half_life_minutes=30.0,
    )
    wrong_v3_half_life = json.loads(json.dumps(v3))
    wrong_v3_half_life["episode_rows"][0][
        "broad_age_decay_half_life_minutes"
    ] = 60.0
    assert not move_first_operational_mapping_alignment_is_current(
        wrong_v3_half_life,
        capture,
        cutoff_epoch=cutoff,
        expected_contract_id=v3_contract,
        expected_cohort_id=v3_cohort,
        expected_story_deduplication_rule=v3_rule,
        expected_story_duplicate_count_field=(
            "operational_story_family_duplicate_count"
        ),
        expected_broad_age_decay_half_life_minutes=30.0,
    )

    preactivation = json.loads(json.dumps(v2))
    preactivation_start = now + timedelta(minutes=5)
    preactivation["cohort_start_utc"] = preactivation_start.isoformat()
    preactivation["ledger_cutoff_utc"] = preactivation_start.isoformat()
    preactivation["source_case_count"] = 0
    preactivation["resolved_factor_episode_count"] = 0
    preactivation["receipt_backed_event_count"] = 0
    preactivation["receipt_backed_independent_story_count"] = 0
    preactivation["episode_rows"] = []
    for metric in preactivation["arm_metrics"].values():
        metric.update(
            episode_count=0,
            signaled_episode_count=0,
            aligned_episode_count=0,
            opposed_episode_count=0,
            abstained_episode_count=0,
        )
    assert move_first_operational_mapping_alignment_is_current(
        preactivation,
        {"generated_utc": generated, "case_count": 1},
        cutoff_epoch=cutoff,
        expected_contract_id=v2_contract,
        expected_cohort_id=v2_cohort,
        expected_story_deduplication_rule=v2_rule,
    )

    contaminated_preactivation = json.loads(json.dumps(preactivation))
    contaminated_preactivation["receipt_backed_event_count"] = 1
    assert not move_first_operational_mapping_alignment_is_current(
        contaminated_preactivation,
        {"generated_utc": generated, "case_count": 1},
        cutoff_epoch=cutoff,
        expected_contract_id=v2_contract,
        expected_cohort_id=v2_cohort,
        expected_story_deduplication_rule=v2_rule,
    )

    inconsistent_v2 = json.loads(json.dumps(v2))
    inconsistent_v2["episode_rows"][0][
        "operational_syndicated_duplicate_count"
    ] = 1
    assert not move_first_operational_mapping_alignment_is_current(
        inconsistent_v2,
        capture,
        cutoff_epoch=cutoff,
        expected_contract_id=v2_contract,
        expected_cohort_id=v2_cohort,
        expected_story_deduplication_rule=v2_rule,
    )


from trad.oanda_official_release_fast_lane_contract import (
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
)
from trad.oanda_source_governance import connect_registry
from trad.oanda_news_classification_contract import NEWS_CLASSIFICATION_VERSION
from trad import oanda_official_currency_source_depth as source_depth
from trad import oanda_news_outcome_improvement_audit as news_record
from trad import oanda_live_move_news_snapshot_v7r3 as live_v7r3


def test_stale_integrity_pass_cannot_overwrite_newer_publication(tmp_path) -> None:
    guard = tmp_path / "guard.sqlite"
    output = tmp_path / "state.json"
    report = tmp_path / "report.md"
    history = tmp_path / "history.jsonl"
    older = _claim_audit_publication(guard)
    newer = _claim_audit_publication(guard)
    assert not _publish_owned_audit(
        guard=guard,
        ownership=older,
        output=output,
        report=report,
        history=history,
        payload={"status": "older"},
        report_text="older",
    )
    assert _publish_owned_audit(
        guard=guard,
        ownership=newer,
        output=output,
        report=report,
        history=history,
        payload={"status": "newer"},
        report_text="newer",
    )
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "newer"
    assert report.read_text(encoding="utf-8") == "newer"
    assert len(history.read_text(encoding="utf-8").splitlines()) == 1


def test_integrity_history_rotation_preserves_every_byte(tmp_path) -> None:
    history = tmp_path / "project_integrity_audit_v1.jsonl"
    original = b'{"generation":1}\n{"generation":2}\n'
    history.write_bytes(original)
    rotated = rotate_jsonl_history_if_needed(
        history,
        maximum_bytes=len(original),
        now=datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc),
    )
    assert rotated is not None
    assert rotated.name.startswith(
        "project_integrity_audit_v1.part_20260902T090000_000000Z_"
    )
    assert rotated.name.endswith(".jsonl")
    assert rotated.read_bytes() == original
    assert not history.exists()


def test_owned_publication_rotates_oversized_history_before_append(
    tmp_path,
    monkeypatch,
) -> None:
    from trad import oanda_project_integrity_audit as audit

    monkeypatch.setattr(audit, "PROJECT_INTEGRITY_HISTORY_ROTATE_BYTES", 1)
    guard = tmp_path / "guard.sqlite"
    output = tmp_path / "state.json"
    report = tmp_path / "report.md"
    history = tmp_path / "project_integrity_audit_v1.jsonl"
    history.write_text('{"status":"prior"}\n', encoding="utf-8")
    ownership = _claim_audit_publication(guard)
    assert _publish_owned_audit(
        guard=guard,
        ownership=ownership,
        output=output,
        report=report,
        history=history,
        payload={"status": "current"},
        report_text="current",
    )
    rotated = list(tmp_path.glob("project_integrity_audit_v1.part_*.jsonl"))
    assert len(rotated) == 1
    assert rotated[0].read_text(encoding="utf-8") == '{"status":"prior"}\n'
    assert json.loads(history.read_text(encoding="utf-8"))["status"] == "current"


def test_integrity_audit_uses_the_producer_classification_contract() -> None:
    assert CURRENT_NEWS_CLASSIFICATION_VERSION == NEWS_CLASSIFICATION_VERSION


def test_narrative_seal_boundary_keeps_just_closed_bucket_open_during_grace() -> None:
    cutoff = datetime(2026, 8, 27, 8, 15, 17, tzinfo=timezone.utc).timestamp()
    assert latest_sealable_clock(
        cutoff, bucket_minutes=5, grace_seconds=60
    ) == datetime(2026, 8, 27, 8, 10, tzinfo=timezone.utc)


def test_narrative_seal_boundary_advances_after_full_grace() -> None:
    at_grace = datetime(2026, 8, 27, 8, 16, 0, tzinfo=timezone.utc).timestamp()
    after_grace = datetime(2026, 8, 27, 8, 16, 1, tzinfo=timezone.utc).timestamp()
    expected = datetime(2026, 8, 27, 8, 15, tzinfo=timezone.utc)
    assert latest_sealable_clock(
        at_grace, bucket_minutes=5, grace_seconds=60
    ) == expected
    assert latest_sealable_clock(
        after_grace, bucket_minutes=5, grace_seconds=60
    ) == expected


def test_narrative_integrity_uses_snapshot_knowledge_time_not_later_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "narrative.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE meter_contract_registry(
            meter_contract_id TEXT,
            bucket_minutes INTEGER,
            seal_grace_seconds INTEGER,
            research_only INTEGER,
            execution_eligible INTEGER
        );
        CREATE TABLE bucket_seals(
            meter_contract_id TEXT,
            clock_utc TEXT,
            sealed_at_utc TEXT,
            currency_row_count INTEGER
        );
        CREATE TABLE seal_integrity_events(
            meter_contract_id TEXT,
            detected_utc TEXT
        );
        CREATE TABLE seal_gap_events(
            meter_contract_id TEXT,
            detected_utc TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO meter_contract_registry VALUES(?,?,?,?,?)",
        (
            CONTINUOUS_NARRATIVE_METER_CONTRACT,
            CONTINUOUS_NARRATIVE_BUCKET_MINUTES,
            CONTINUOUS_NARRATIVE_SEAL_GRACE_SECONDS,
            1,
            0,
        ),
    )
    connection.executemany(
        "INSERT INTO bucket_seals VALUES(?,?,?,?)",
        [
            (
                CONTINUOUS_NARRATIVE_METER_CONTRACT,
                "2026-09-01T06:25:00+00:00",
                "2026-09-01T06:30:05+00:00",
                21,
            ),
            (
                CONTINUOUS_NARRATIVE_METER_CONTRACT,
                "2026-09-01T06:30:00+00:00",
                "2026-09-01T06:31:05+00:00",
                21,
            ),
        ],
    )
    connection.executemany(
        "INSERT INTO seal_integrity_events VALUES(?,?)",
        [
            (CONTINUOUS_NARRATIVE_METER_CONTRACT, "2026-09-01T06:29:00+00:00"),
            (CONTINUOUS_NARRATIVE_METER_CONTRACT, "2026-09-01T06:31:00+00:00"),
        ],
    )
    connection.executemany(
        "INSERT INTO seal_gap_events VALUES(?,?)",
        [
            (CONTINUOUS_NARRATIVE_METER_CONTRACT, "2026-09-01T06:29:00+00:00"),
            (CONTINUOUS_NARRATIVE_METER_CONTRACT, "2026-09-01T06:31:00+00:00"),
        ],
    )
    connection.commit()
    connection.close()
    state = {
        "schema_version": "continuous_narrative_meter_latest_v12",
        "meter_contract_id": CONTINUOUS_NARRATIVE_METER_CONTRACT,
        "generated_utc": "2026-09-01T06:30:08+00:00",
        "seal_grace_seconds": CONTINUOUS_NARRATIVE_SEAL_GRACE_SECONDS,
        "bucket_minutes": CONTINUOUS_NARRATIVE_BUCKET_MINUTES,
        "sealed_clock_utc": "2026-09-01T06:25:00+00:00",
        "clock_utc": "2026-09-01T06:25:00+00:00",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "integrity_status": "late_arrival_and_seal_gap_incident",
        "persistence": {
            "total_late_arrival_incidents": 1,
            "total_seal_gap_incidents": 1,
        },
        "partial_live": {
            "bucket_end_utc": "2026-09-01T06:30:00+00:00",
            "state_kind": "unsealed_provisional_live_view",
            "complete": False,
            "persisted": False,
            "proof_eligible": False,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
        },
    }
    cutoff = datetime(2026, 9, 1, 6, 32, tzinfo=timezone.utc).timestamp()

    result = continuous_narrative_meter_integrity(
        state,
        database_path=database,
        cutoff_epoch=cutoff,
    )

    assert result["ok"] is True
    assert result["count_basis"] == (
        "database_seals_and_incidents_at_or_before_snapshot_generated_utc"
    )
    assert result["sealed_clock_utc"] == "2026-09-01T06:25:00+00:00"
    assert result["expected_sealed_clock_utc"] == (
        "2026-09-01T06:25:00+00:00"
    )
    assert result["bucket_seal_count"] == 1
    assert result["late_arrival_incident_count"] == 1
    assert result["seal_gap_incident_count"] == 1


def test_official_source_depth_must_be_current_complete_and_zero_weight() -> None:
    report = source_depth.build_report()
    report["generated_utc"] = "2026-08-25T04:00:00+00:00"
    cutoff = datetime.fromisoformat("2026-08-25T04:30:00+00:00").timestamp()
    assert official_source_depth_integrity(report, cutoff_epoch=cutoff)["ok"] is True
    assert official_source_depth_integrity(
        {**report, "currency_strength_weight": True}, cutoff_epoch=cutoff
    )["ok"] is False
    assert official_source_depth_integrity(
        {**report, "generated_utc": "2026-08-24T04:00:00+00:00"},
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert official_source_depth_integrity(
        {
            **report,
            "category_currency_counts": {
                **report["category_currency_counts"],
                "inflation": 20,
            },
        },
        cutoff_epoch=cutoff,
    )["ok"] is False


def test_move_first_news_audit_must_be_fresh_fail_closed_and_within_census_cadence() -> None:
    audit = {
        "schema_version": 2,
        "research_id": "move_first_news_case_audit_v2",
        "generated_utc": "2026-08-19T03:00:00+00:00",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
        "case_count": 12,
    }
    census = {"generated_utc": "2026-08-19T03:30:00+00:00"}
    cutoff = datetime(2026, 8, 19, 4, 0, tzinfo=timezone.utc).timestamp()
    assert move_first_news_audit_is_current(
        audit, census, cutoff_epoch=cutoff
    ) is True
    assert move_first_news_audit_is_current(
        {**audit, "research_only": False}, census, cutoff_epoch=cutoff
    ) is False
    assert move_first_news_audit_is_current(
        {**audit, "generated_utc": "2026-08-18T19:00:00+00:00"},
        census,
        cutoff_epoch=cutoff,
    ) is False
    assert move_first_news_audit_is_current(
        audit,
        {"generated_utc": "2026-08-18T19:00:00+00:00"},
        cutoff_epoch=cutoff,
    ) is False


def test_live_census_rebuild_never_refreshes_stale_completed_evidence() -> None:
    cutoff = datetime(2026, 9, 1, 20, 0, tzinfo=timezone.utc).timestamp()
    report = {
        "generated_utc": "2026-09-01T10:00:00+00:00",
        "publication_utc": "2026-09-01T11:00:00+00:00",
    }
    heartbeat = {
        "worker": "oanda_major_move_gap_census",
        "status": "running",
        "phase": "recovering_legacy_executable_paths",
        "updated_at": "2026-09-01T19:59:55+00:00",
        "phase_age_sec": 120.0,
        "progress_sequence": 12,
        "progress_age_sec": 15.0,
        "details": {"completed_instruments": 31, "total_instruments": 68},
    }

    status = major_move_gap_census_progress_status(
        report,
        heartbeat,
        cutoff_epoch=cutoff,
    )

    assert status["status"] == "stale_rebuild_in_progress"
    assert status["worker_live"] is True
    assert status["rebuild_in_progress"] is True
    assert status["evidence_fresh"] is False
    assert status["fresh_heartbeat_does_not_refresh_completed_evidence"] is True


def test_census_progress_requires_exact_worker_identity_and_recent_heartbeat() -> None:
    cutoff = datetime(2026, 9, 1, 20, 0, tzinfo=timezone.utc).timestamp()
    report = {"generated_utc": "2026-09-01T19:00:00+00:00"}
    heartbeat = {
        "worker": "another_worker",
        "status": "running",
        "phase": "recovering_legacy_executable_paths",
        "updated_at": "2026-09-01T19:59:55+00:00",
    }

    status = major_move_gap_census_progress_status(
        report,
        heartbeat,
        cutoff_epoch=cutoff,
    )

    assert status["status"] == "current_completed_evidence"
    assert status["evidence_fresh"] is True
    assert status["worker_live"] is False
    assert status["rebuild_in_progress"] is False


def test_independent_verifier_progress_is_safe_only_with_recent_match_and_closed_routing() -> None:
    cutoff = datetime(2026, 8, 28, 16, 30, tzinfo=timezone.utc).timestamp()
    progress = {
        "status": "verification_in_progress",
        "authorization_safe": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "no_trade",
        "verified_confirmed_candidates": [],
        "previous_completed_state_utc": "2026-08-28T16:24:00+00:00",
        "checks": [{"name": "prior_check", "passed": True}],
    }
    lifecycle = {"confirmed_candidate": 0}
    assert independent_verifier_operationally_safe(
        progress, lifecycle, cutoff_epoch=cutoff
    ) is True
    assert independent_verifier_operationally_safe(
        {**progress, "authorization_safe": True},
        lifecycle,
        cutoff_epoch=cutoff,
    ) is False
    assert independent_verifier_operationally_safe(
        {**progress, "previous_completed_state_utc": "2026-08-28T15:00:00+00:00"},
        lifecycle,
        cutoff_epoch=cutoff,
    ) is False
    assert independent_verifier_operationally_safe(
        {**progress, "checks": [{"name": "prior_check", "passed": False}]},
        lifecycle,
        cutoff_epoch=cutoff,
    ) is False
    assert independent_verifier_operationally_safe(
        progress,
        {"confirmed_candidate": 1},
        cutoff_epoch=cutoff,
    ) is False


def test_completed_independent_match_requires_current_lifecycle_fingerprint() -> None:
    cutoff = datetime(2026, 9, 2, 1, 45, tzinfo=timezone.utc).timestamp()
    lifecycle = {
        "continue_collecting": 43,
        "futility_rejected": 9,
        "confirmed_candidate": 0,
    }
    verifier = {
        "status": "match",
        "generated_utc": "2026-09-02T01:44:30+00:00",
        "completed_verification_utc": "2026-09-02T01:44:30+00:00",
        "can_place_orders": False,
        "can_promote": False,
        "real_money_routing": False,
        "research_only": True,
        "independent_implementation": True,
        "production_calculator_imports": [],
        "failed_checks": [],
        "input_fingerprints": {"lifecycle_state": "sha256:current"},
        "rebuilt_lifecycle": {
            "hypothesis_count": 52,
            "states": lifecycle,
        },
    }
    assert independent_verifier_operationally_safe(
        verifier,
        lifecycle,
        cutoff_epoch=cutoff,
        current_lifecycle_fingerprint="sha256:current",
    ) is True
    assert independent_verifier_operationally_safe(
        verifier,
        lifecycle,
        cutoff_epoch=cutoff,
        current_lifecycle_fingerprint="sha256:newer",
    ) is False
    assert independent_verifier_operationally_safe(
        {
            **verifier,
            "rebuilt_lifecycle": {
                "hypothesis_count": 52,
                "states": {**lifecycle, "continue_collecting": 42},
            },
        },
        lifecycle,
        cutoff_epoch=cutoff,
        current_lifecycle_fingerprint="sha256:current",
    ) is False


def test_completed_independent_match_accepts_only_exact_semantic_republication() -> None:
    cutoff = datetime(2026, 9, 2, 12, 36, tzinfo=timezone.utc).timestamp()
    lifecycle = {
        "continue_collecting": 43,
        "futility_rejected": 9,
        "confirmed_candidate": 0,
    }
    integrity = {
        "contract": "evidence_lifecycle_publication_integrity_v1",
        "current_state_count": 52,
        "current_state_sha256": "exact-current-state-sha256",
        "current_states": lifecycle,
        "futility_retirement_count": 9,
        "futility_retirement_highwater_rowid": 9,
        "hypothesis_count": 52,
        "hypothesis_highwater_rowid": 52,
        "lifecycle_event_count": 57,
        "lifecycle_event_highwater_rowid": 57,
        "reconsideration_count": 0,
        "reconsideration_highwater_rowid": 0,
    }
    verifier = {
        "status": "match",
        "generated_utc": "2026-09-02T12:35:30+00:00",
        "completed_verification_utc": "2026-09-02T12:35:30+00:00",
        "can_place_orders": False,
        "can_promote": False,
        "real_money_routing": False,
        "research_only": True,
        "independent_implementation": True,
        "production_calculator_imports": [],
        "failed_checks": [],
        "input_fingerprints": {"lifecycle_state": "sha256:prior-bytes"},
        "rebuilt_lifecycle": {
            "hypothesis_count": 52,
            "states": lifecycle,
        },
        "checks": [
            {
                "name": "lifecycle_publication_integrity",
                "passed": True,
                "evidence": {"published": integrity, "rebuilt": integrity},
            }
        ],
    }

    assert independent_verifier_operationally_safe(
        verifier,
        lifecycle,
        cutoff_epoch=cutoff,
        current_lifecycle_fingerprint="sha256:republished-bytes",
        current_lifecycle_integrity=integrity,
    ) is True
    assert independent_verifier_operationally_safe(
        verifier,
        lifecycle,
        cutoff_epoch=cutoff,
        current_lifecycle_fingerprint="sha256:republished-bytes",
        current_lifecycle_integrity={
            **integrity,
            "lifecycle_event_highwater_rowid": 58,
        },
    ) is False
    assert independent_verifier_operationally_safe(
        {
            **verifier,
            "checks": [
                {
                    "name": "lifecycle_publication_integrity",
                    "passed": False,
                    "evidence": {"published": integrity},
                }
            ],
        },
        lifecycle,
        cutoff_epoch=cutoff,
        current_lifecycle_fingerprint="sha256:republished-bytes",
        current_lifecycle_integrity=integrity,
    ) is False


def test_live_move_forward_proof_binds_exact_inert_contracts(tmp_path: Path) -> None:
    database = tmp_path / "cases_v7r3.sqlite"
    live_v7r3.record_cases(
        database,
        [],
        recorded_utc="2026-08-27T07:50:00+00:00",
        meter_database=tmp_path / "meter.sqlite",
        planned_merges=[],
    )
    cutoff = datetime(2026, 8, 27, 8, 0, tzinfo=timezone.utc).timestamp()
    snapshot = {
        "schema_version": 7,
        "contract_id": live_v7r3.CONTRACT_ID,
        "generated_utc": "2026-08-27T07:59:00+00:00",
        "mover_count": 0,
        "factor_episode_count": 0,
        "retained_case_count": 0,
        "movers": [],
        "narrative_join_contract": {
            "meter_contract_id": "continuous_currency_narrative_meter_v12_sealed_20260827",
            "partial_live_excluded": True,
            "sealed_v12_database_only": True,
            "research_only": True,
            "execution_eligible": False,
        },
        "factor_episode_contract": {
            "contract_id": live_v7r3.FACTOR_EPISODE_CONTRACT_ID,
            "grouping_method": (
                "fixed_onset_same_primary_interval_overlap_adjacency_"
                "append_only_transitive_root_union"
            ),
            "root_union_can_increase_effective_n": False,
            "membership_rewrites_allowed": False,
        },
        "factor_episode_integrity_ok": True,
        "factor_episode_membership_conflict_total": 0,
        "factor_episode_graph_cycle": False,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "broker_access": False,
        "supported_decision": "diagnostic_only",
    }
    outcome = {
        "schema_version": 4,
        "contract_id": (
            "live_move_news_forward_outcomes_v4r3_v7r3_"
            "transitive_factor_root_union_20260827"
        ),
        "case_contract_id": snapshot["contract_id"],
        "factor_episode_contract_id": live_v7r3.FACTOR_EPISODE_CONTRACT_ID,
        "generated_utc": "2026-08-27T07:58:00+00:00",
        "case_count": 0,
        "retained_outcome_count": 0,
        "sqlite_integrity": "ok",
        "upstream_integrity": {
            "ok": True,
            "failures": [],
            "quick_check": "ok",
            "case_contract_id": live_v7r3.CONTRACT_ID,
            "factor_episode_contract_id": live_v7r3.FACTOR_EPISODE_CONTRACT_ID,
            "membership_conflict_count": 0,
            "membership_mismatch_count": 0,
            "invalid_root_merge_count": 0,
            "root_union_cycle": False,
            "raw_root_count": 0,
            "canonical_root_count": 0,
        },
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "collect_forward_outcomes",
    }
    assert live_move_forward_proof_integrity(
        snapshot, outcome, cutoff_epoch=cutoff, case_database_path=database
    )["ok"] is True
    assert live_move_forward_proof_integrity(
        {**snapshot, "contract_id": "stale"}, outcome, cutoff_epoch=cutoff,
        case_database_path=database,
    )["ok"] is False
    assert live_move_forward_proof_integrity(
        snapshot, {**outcome, "execution_eligible": True}, cutoff_epoch=cutoff,
        case_database_path=database,
    )["ok"] is False
    inflated_outcome = json.loads(json.dumps(outcome))
    inflated_outcome["upstream_integrity"]["raw_root_count"] = 1
    inflated_outcome["upstream_integrity"]["canonical_root_count"] = 2
    assert live_move_forward_proof_integrity(
        snapshot,
        inflated_outcome,
        cutoff_epoch=cutoff,
        case_database_path=database,
    )["ok"] is False
    with sqlite3.connect(database) as connection:
        for merge_id, source, target in (
            ("cycle-a", "root-a", "root-b"),
            ("cycle-b", "root-b", "root-a"),
        ):
            connection.execute(
                "INSERT INTO factor_episode_root_merges VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    merge_id,
                    live_v7r3.FACTOR_EPISODE_CONTRACT_ID,
                    source,
                    target,
                    "USD+",
                    "[]",
                    "2026-08-27T07:59:30+00:00",
                    1,
                    0,
                ),
            )
        connection.commit()
    cycle_status = live_move_forward_proof_integrity(
        snapshot,
        outcome,
        cutoff_epoch=cutoff,
        case_database_path=database,
    )
    assert cycle_status["case_database_root_union_cycle"] is True
    assert cycle_status["ok"] is False


def test_persistent_move_context_binds_exact_inert_contract() -> None:
    cutoff = datetime(2026, 8, 27, 8, 0, tzinfo=timezone.utc).timestamp()
    state = {
        "schema_version": 5,
        "contract_id": (
            "live_move_persistent_news_context_v5r3_exact_v7r3_"
            "transitive_root_union_binding_20260827"
        ),
        "input_snapshot_contract_id": live_v7r3.CONTRACT_ID,
        "expected_input_snapshot_contract_id": live_v7r3.CONTRACT_ID,
        "input_factor_episode_contract_id": live_v7r3.FACTOR_EPISODE_CONTRACT_ID,
        "expected_factor_episode_contract_id": live_v7r3.FACTOR_EPISODE_CONTRACT_ID,
        "input_integrity_ok": True,
        "input_rejection_reasons": [],
        "generated_utc": "2026-08-27T07:59:00+00:00",
        "mover_count": 1,
        "movers": [{
            "factor_episode_contract_id": live_v7r3.FACTOR_EPISODE_CONTRACT_ID,
            "factor_episode_id": "raw-root",
            "factor_episode_canonical_root_id": "canonical-root",
            "factor_episode_membership_conflict": False,
            "persistent_active_event_count": 3,
            "persistent_independent_story_count": 2,
            "persistent_direct_authority_side": "neutral",
            "persistent_direct_authority_alignment": "neutral",
        }],
        "upstream_factor_history_status": {
            "ok": True,
            "quick_check": "ok",
            "contract_registry_ok": True,
            "append_only_triggers_ok": True,
            "current_mover_membership_ok": True,
            "membership_conflict_total": 0,
            "graph_cycle": False,
            "root_union_nonexpansive": True,
            "raw_root_count": 2,
            "canonical_root_count": 1,
        },
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "diagnostic_only",
    }
    assert live_move_persistent_context_integrity(
        state, cutoff_epoch=cutoff
    )["ok"] is True
    assert live_move_persistent_context_integrity(
        {**state, "can_place_orders": True}, cutoff_epoch=cutoff
    )["ok"] is False


def test_news_outcome_canonical_record_requires_exact_inert_append_only_state(
    tmp_path: Path,
) -> None:
    database = tmp_path / "audit.sqlite"
    with news_record.open_output_database(database) as connection:
        connection.execute(
            "INSERT INTO diagnoses VALUES (?,?,?,?,?,?)",
            ("one", "2026-08-26T16:09:00+00:00", "accepted_news_decision", "g1", "direction_wrong", "{}"),
        )
        connection.execute(
            "INSERT INTO audit_snapshots VALUES (?,?,?,?,?)",
            ("fingerprint", "2026-08-26T16:09:00+00:00", 1, 1, "{}"),
        )
        connection.commit()
    cutoff = datetime(2026, 8, 26, 16, 10, tzinfo=timezone.utc).timestamp()
    payload = {
        "schema_version": 1,
        "contract_id": "news_outcome_improvement_audit_v2_20260826",
        "generated_utc": "2026-08-26T16:09:00+00:00",
        "recorded_utc": "2026-08-26T16:09:00+00:00",
        "refresh_mode": "on_demand_append_only",
        "recurring_polling": False,
        "status": "ok",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_modify_execution_policy": False,
        "broker_access": False,
        "supported_execution_decision": "no_change_diagnostic_only",
        "input_contracts": {
            "signal_news": "practice_007_signal_news_monitor_v7",
            "mover_cases": "live_move_news_snapshot_v5_causal_factor_strength_20260827",
            "mover_outcomes": "live_move_news_forward_outcomes_v1_20260824",
        },
        "sqlite_integrity": "ok",
        "source_fingerprint": "fingerprint",
        "canonical_snapshot": {
            "source_fingerprint": "fingerprint",
            "diagnosis_count": 1,
            "queue_count": 1,
        },
        "summary": {
            "retained_diagnoses": 1,
            "current_effective_theses": 1,
            "reason_counts": {"direction_wrong": 1},
        },
        "known_reason_codes": ["direction_wrong"],
        "queue": {"queue_count": 1, "items": [{}]},
    }
    assert news_outcome_improvement_integrity(
        payload, database_path=database, cutoff_epoch=cutoff
    )["ok"] is True
    assert news_outcome_improvement_integrity(
        {**payload, "can_place_orders": True},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert news_outcome_improvement_integrity(
        {**payload, "generated_utc": "2026-08-26T15:00:00+00:00"},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is True
    assert news_outcome_improvement_integrity(
        {**payload, "generated_utc": "2026-08-26T17:00:00+00:00"},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert news_outcome_improvement_integrity(
        {**payload, "queue": {"queue_count": 2, "items": [{}]}},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False


def test_official_release_fast_lane_requires_exact_fresh_inert_ledger(
    tmp_path: Path,
) -> None:
    database = tmp_path / "fast.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE official_release_observation (
                prospective_observation INTEGER NOT NULL,
                first_seen_utc TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE TRIGGER trg_fast_lane_observation_no_update "
            "BEFORE UPDATE ON official_release_observation BEGIN "
            "SELECT RAISE(ABORT,'append_only'); END"
        )
        connection.execute(
            "CREATE TRIGGER trg_fast_lane_observation_no_delete "
            "BEFORE DELETE ON official_release_observation BEGIN "
            "SELECT RAISE(ABORT,'append_only'); END"
        )
        connection.execute(
            "INSERT INTO official_release_observation VALUES (?, ?)",
            (0, "2026-08-25T01:58:00+00:00"),
        )
        connection.commit()
    cutoff = datetime(2026, 8, 25, 2, 0, tzinfo=timezone.utc).timestamp()
    policy = {
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_promote": False,
        "broker_access": False,
    }
    snapshot = {
        "schema_version": "official_release_fast_lane_v4",
        "collector_contract_id": OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
        "configured_currency_count": 21,
        "configured_release_source_count": 23,
        "generated_utc": "2026-08-25T01:59:30+00:00",
        "counts": {"observations": 1, "prospective_observations": 0},
        "policy": policy,
    }
    heartbeat = {
        "schema_version": "official_release_fast_lane_v4",
        "collector_contract_id": snapshot["collector_contract_id"],
        "heartbeat_utc": "2026-08-25T01:59:30+00:00",
        "status": "cycle_complete",
        "policy": policy,
    }
    result = official_release_fast_lane_integrity(
        snapshot, heartbeat, database_path=database, cutoff_epoch=cutoff
    )
    assert result["ok"] is True
    assert result["count_basis"] == (
        "database_rows_first_seen_at_or_before_snapshot_generated_utc"
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO official_release_observation VALUES (?, ?)",
            (1, "2026-08-25T01:59:45+00:00"),
        )
        connection.commit()
    raced = official_release_fast_lane_integrity(
        snapshot, heartbeat, database_path=database, cutoff_epoch=cutoff
    )
    assert raced["ok"] is True
    assert raced["observations"] == 1
    assert raced["current_database_observations"] == 2
    assert official_release_fast_lane_integrity(
        {**snapshot, "policy": {**policy, "execution_eligible": True}},
        heartbeat,
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False


def test_official_release_fast_mapping_requires_exact_current_inert_contract(
    tmp_path: Path,
) -> None:
    database = tmp_path / "mapping.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE official_release_mapping (
                input_prospective_observation INTEGER NOT NULL,
                semantic_direction_available INTEGER NOT NULL,
                prospective_semantic_candidate INTEGER NOT NULL,
                publish_eligible_forward_candidate INTEGER NOT NULL,
                forward_shadow_candidate INTEGER NOT NULL,
                classification_version TEXT NOT NULL,
                mapper_contract_id TEXT NOT NULL,
                mapped_utc TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE TRIGGER trg_fast_mapping_no_update "
            "BEFORE UPDATE ON official_release_mapping BEGIN "
            "SELECT RAISE(ABORT,'append_only'); END"
        )
        connection.execute(
            "CREATE TRIGGER trg_fast_mapping_no_delete "
            "BEFORE DELETE ON official_release_mapping BEGIN "
            "SELECT RAISE(ABORT,'append_only'); END"
        )
        connection.execute(
            "INSERT INTO official_release_mapping VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                0,
                1,
                0,
                0,
                0,
                CURRENT_NEWS_CLASSIFICATION_VERSION,
                "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824",
                "2026-08-25T03:08:00+00:00",
            ),
        )
        connection.commit()
    cutoff = datetime(2026, 8, 25, 3, 10, tzinfo=timezone.utc).timestamp()
    policy = {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "broker_access": False,
        "watchlist_mutation": False,
    }
    identity = {
        "schema_version": "official_release_fast_mapping_v3",
        "mapper_contract_id": "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824",
        "required_input_contract_id": OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
        "required_classification_version": CURRENT_NEWS_CLASSIFICATION_VERSION,
    }
    snapshot = {
        **identity,
        "generated_utc": "2026-08-25T03:09:00+00:00",
        "counts": {
            "mappings": 1,
            "prospective_inputs": 0,
            "semantic_direction_available": 1,
            "prospective_semantic_candidates": 0,
            "publish_eligible_forward_candidates": 0,
            "forward_shadow_candidates": 0,
        },
        "policy": policy,
    }
    heartbeat = {
        **identity,
        "heartbeat_utc": "2026-08-25T03:09:30+00:00",
        "status": "cycle_complete",
        "policy": {key: value for key, value in policy.items() if key != "watchlist_mutation" and key != "can_place_orders"},
    }
    assert official_release_fast_mapping_integrity(
        snapshot, heartbeat, database_path=database, cutoff_epoch=cutoff
    )["ok"] is True
    assert official_release_fast_mapping_integrity(
        {**snapshot, "required_classification_version": "stale"},
        heartbeat,
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False


def test_fast_mapping_counts_are_bound_to_snapshot_cutoff_during_live_growth(
    tmp_path: Path,
) -> None:
    database = tmp_path / "mapping.sqlite"
    contract = "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """CREATE TABLE official_release_mapping(
               input_prospective_observation INTEGER NOT NULL,
               semantic_direction_available INTEGER NOT NULL,
               prospective_semantic_candidate INTEGER NOT NULL,
               publish_eligible_forward_candidate INTEGER NOT NULL,
               forward_shadow_candidate INTEGER NOT NULL,
               classification_version TEXT NOT NULL,
               mapper_contract_id TEXT NOT NULL,
               mapped_utc TEXT NOT NULL
               )"""
        )
        connection.execute(
            "CREATE TRIGGER trg_fast_mapping_no_update "
            "BEFORE UPDATE ON official_release_mapping BEGIN "
            "SELECT RAISE(ABORT,'append_only'); END"
        )
        connection.execute(
            "CREATE TRIGGER trg_fast_mapping_no_delete "
            "BEFORE DELETE ON official_release_mapping BEGIN "
            "SELECT RAISE(ABORT,'append_only'); END"
        )
        connection.executemany(
            "INSERT INTO official_release_mapping VALUES(?,?,?,?,?,?,?,?)",
            [
                (0, 1, 0, 0, 0, CURRENT_NEWS_CLASSIFICATION_VERSION,
                 contract, "2026-08-25T03:08:00+00:00"),
                # This row was committed after the already-published snapshot.
                (0, 0, 0, 0, 0, CURRENT_NEWS_CLASSIFICATION_VERSION,
                 contract, "2026-08-25T03:09:30+00:00"),
            ],
        )
    identity = {
        "schema_version": "official_release_fast_mapping_v3",
        "mapper_contract_id": contract,
        "required_input_contract_id": (
            OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
        ),
        "required_classification_version": CURRENT_NEWS_CLASSIFICATION_VERSION,
    }
    policy = {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "broker_access": False,
        "watchlist_mutation": False,
    }
    snapshot = {
        **identity,
        "generated_utc": "2026-08-25T03:09:00+00:00",
        "counts": {
            "mappings": 1,
            "prospective_inputs": 0,
            "semantic_direction_available": 1,
            "prospective_semantic_candidates": 0,
            "publish_eligible_forward_candidates": 0,
            "forward_shadow_candidates": 0,
        },
        "policy": policy,
    }
    heartbeat = {
        **identity,
        "heartbeat_utc": "2026-08-25T03:09:45+00:00",
        "status": "cycle_complete",
        "policy": {
            key: value
            for key, value in policy.items()
            if key not in {"watchlist_mutation", "can_place_orders"}
        },
    }
    cutoff = datetime(2026, 8, 25, 3, 10, tzinfo=timezone.utc).timestamp()
    result = official_release_fast_mapping_integrity(
        snapshot, heartbeat, database_path=database, cutoff_epoch=cutoff
    )
    assert result["ok"] is True
    assert result["mappings"] == 1
    assert result["current_database_mappings"] == 2
    assert result["count_basis"].endswith("snapshot_generated_utc")


def test_official_release_fast_response_requires_exact_quote_inert_contract(
    tmp_path: Path,
) -> None:
    database = tmp_path / "response.sqlite"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE response_watch (
                watch_id TEXT PRIMARY KEY,mapping_id TEXT,event_factor_id TEXT,
                instrument TEXT
            );
            CREATE TABLE response_outcome (
                watch_id TEXT PRIMARY KEY,maturity_state TEXT
            );
            """
        )
    cutoff = datetime.fromisoformat("2026-08-25T03:30:00+00:00").timestamp()
    policy = {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "broker_access": False,
        "canonical_watchlist_mutation": False,
        "supported_decision": "shadow_observation_only",
    }
    snapshot = {
        "schema_version": "official_release_fast_response_watch_v3",
        "contract_id": "official_release_fast_response_watch_v3_multi_horizon_exact_quote_20260825",
        "cohort_id": "official_release_fast_response_watch_v3_20260825",
        "required_mapper_contract_id": "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824",
        "required_classification_version": CURRENT_NEWS_CLASSIFICATION_VERSION,
        "horizons_min": [1, 5, 15, 30, 60, 120],
        "counts": {
            "watches": 0,
            "mapping_events": 0,
            "factor_episodes": 0,
            "instruments": 0,
            "outcomes": 0,
            "valid_outcomes": 0,
        },
        "policy": policy,
    }
    heartbeat = {
        "schema_version": snapshot["schema_version"],
        "contract_id": snapshot["contract_id"],
        "cohort_id": snapshot["cohort_id"],
        "heartbeat_utc": "2026-08-25T03:29:30+00:00",
        "status": "cycle_complete",
        "policy": policy,
    }
    assert official_release_fast_response_integrity(
        snapshot, heartbeat, database_path=database, cutoff_epoch=cutoff
    )["ok"] is True
    concurrent_heartbeat = {
        **heartbeat,
        "heartbeat_utc": "2026-08-25T03:30:05.480288+00:00",
    }
    concurrent_result = official_release_fast_response_integrity(
        snapshot,
        concurrent_heartbeat,
        database_path=database,
        cutoff_epoch=cutoff,
    )
    assert concurrent_result["ok"] is True
    assert concurrent_result["heartbeat_age_sec"] == -5.480288028717041
    implausibly_future_heartbeat = {
        **heartbeat,
        "heartbeat_utc": "2026-08-25T03:31:00+00:00",
    }
    future_result = official_release_fast_response_integrity(
        snapshot,
        implausibly_future_heartbeat,
        database_path=database,
        cutoff_epoch=cutoff,
    )
    assert future_result["ok"] is False
    assert future_result["freshness_ok"] is False
    assert official_release_fast_response_integrity(
        {**snapshot, "horizons_min": [1, 15]},
        heartbeat,
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert official_release_fast_response_integrity(
        {**snapshot, "cohort_id": "forged"},
        heartbeat,
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert official_release_fast_response_integrity(
        {**snapshot, "policy": {**policy, "can_place_orders": True}},
        heartbeat,
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False
    assert official_release_fast_lane_integrity(
        snapshot,
        {**heartbeat, "heartbeat_utc": "2026-08-25T01:50:00+00:00"},
        database_path=database,
        cutoff_epoch=cutoff,
    )["ok"] is False


def test_live_feature_coverage_requires_explicit_fail_closed_exclusions() -> None:
    feature_state = {
        "instrument_count": 67,
        "coverage": {
            "expected_feature_instrument_count": 68,
            "accepted_instrument_count": 67,
            "excluded_instrument_count": 1,
            "fail_closed_on_invalid_quote": True,
            "quote_exclusions": [
                {"instrument": "USD_TRY", "reason": "stale_quote"}
            ],
        },
    }
    result = live_feature_coverage_diagnostic(feature_state, 68)
    assert result["status"] == "fail_closed_quote_exclusions"
    feature_state["coverage"]["quote_exclusions"][0]["reason"] = "ignored"
    assert live_feature_coverage_diagnostic(feature_state, 68)["status"] == "unexpected"


def test_expected_news_inventory_tracks_governed_source_addition() -> None:
    assert EXPECTED_CONFIGURED_NEWS_SOURCES == 103


def test_structured_numeric_currency_integrity_rejects_fanout(tmp_path: Path) -> None:
    database=tmp_path / "news.sqlite"
    connection=sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE articles(event_id TEXT,source_id TEXT,headline TEXT,currencies_json TEXT,payload_json TEXT)"
    )
    payload={
        "classification_version":NEWS_CLASSIFICATION_VERSION,
        "source_native_currency_bound":True,
        "source_contract_id":"test_source_contract_v1",
        "source_cohort_id":"test_source_cohort_v1",
    }
    connection.execute(
        "INSERT INTO articles VALUES(?,?,?,?,?)",
        ("good","ons","GDP",json.dumps(["GBP"]),json.dumps(payload)),
    )
    connection.commit()
    assert structured_numeric_currency_integrity(database)["ok"] is True
    connection.execute(
        "INSERT INTO articles VALUES(?,?,?,?,?)",
        ("bad","ons","GDP",json.dumps(["GBP","JPY"]),json.dumps(payload)),
    )
    connection.commit();connection.close()
    result=structured_numeric_currency_integrity(database)
    assert result["ok"] is False
    assert result["invalid_row_count"]==1


def test_structured_numeric_currency_integrity_requires_source_identity(
    tmp_path: Path,
) -> None:
    database = tmp_path / "news.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE articles(event_id TEXT,source_id TEXT,headline TEXT,currencies_json TEXT,payload_json TEXT)"
    )
    connection.execute(
        "INSERT INTO articles VALUES(?,?,?,?,?)",
        (
            "missing-contract",
            "official",
            "Official actual",
            json.dumps(["USD"]),
            json.dumps(
                {
                    "classification_version": NEWS_CLASSIFICATION_VERSION,
                    "source_native_currency_bound": True,
                }
            ),
        ),
    )
    connection.commit()
    connection.close()
    result = structured_numeric_currency_integrity(database)
    assert result["ok"] is False
    assert result["invalid_row_count"] == 1
    assert result["invalid_rows"][0]["reasons"] == [
        "missing_source_contract_id",
        "missing_source_cohort_id",
    ]


def test_structured_numeric_currency_integrity_preserves_pre_adoption_legacy(
    tmp_path: Path,
) -> None:
    database = tmp_path / "news.sqlite"
    source_state = tmp_path / "collector_state.json"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE articles(event_id TEXT,source_id TEXT,headline TEXT,"
        "currencies_json TEXT,first_seen_utc TEXT,payload_json TEXT)"
    )
    payload = {
        "classification_version": NEWS_CLASSIFICATION_VERSION,
        "source_native_currency_bound": True,
    }
    connection.executemany(
        "INSERT INTO articles VALUES(?,?,?,?,?,?)",
        [
            (
                "legacy", "boj", "Legacy decision", json.dumps(["JPY"]),
                "2026-08-26T23:59:00+00:00", json.dumps(payload),
            ),
            (
                "prospective", "boj", "New decision", json.dumps(["JPY"]),
                "2026-08-27T00:02:00+00:00", json.dumps(payload),
            ),
        ],
    )
    connection.commit()
    connection.close()
    source_state.write_text(
        json.dumps(
            {
                "sources": {
                    "boj": {
                        "source_contract_derived": True,
                        "source_lineage_adopted_utc": "2026-08-27T00:00:00+00:00",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = structured_numeric_currency_integrity(
        database, source_state_path=source_state
    )
    assert result["legacy_pre_lineage_count"] == 1
    assert result["legacy_pre_lineage_rows"][0]["event_id"] == "legacy"
    assert result["invalid_row_count"] == 1
    assert result["invalid_rows"][0]["event_id"] == "prospective"
    assert result["ok"] is False


def test_official_detail_quality_integrity_rejects_enriched_error_shell(
    tmp_path: Path,
) -> None:
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "generated_utc": "2026-08-19T01:00:00+00:00",
                "articles": [
                    {
                        "event_id": "bad",
                        "source_id": "mas",
                        "headline": "Policy statement",
                        "detail_enriched": True,
                        "summary": (
                            "Maintenance Sorry, this service is currently unavailable. "
                            "Try accessing it again later."
                        ),
                    },
                    {
                        "event_id": "good",
                        "source_id": "boj",
                        "headline": "Policy statement",
                        "detail_enriched": True,
                        "summary": "The policy board voted to raise the policy rate.",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    result = official_detail_quality_integrity(context)
    assert result["ok"] is False
    assert result["enriched_detail_count"] == 2
    assert result["invalid_row_count"] == 1
    assert result["invalid_rows"][0]["source_id"] == "mas"


def test_official_detail_quality_integrity_accepts_rejected_error_shell(
    tmp_path: Path,
) -> None:
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "articles": [
                    {
                        "event_id": "rejected",
                        "source_id": "mas",
                        "detail_enriched": False,
                        "detail_quality_state": "rejected",
                        "summary": "",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    result = official_detail_quality_integrity(context)
    assert result["ok"] is True
    assert result["enriched_detail_count"] == 0
    assert result["invalid_row_count"] == 0


def test_current_source_provenance_rejects_unverified_direct_label(
    tmp_path: Path,
) -> None:
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "articles": [
                    {
                        "event_id": "bad",
                        "source_id": "aggregator",
                        "headline": "Syndicated headline",
                        "source_direct": True,
                        "source_verified": False,
                        "source_contract_id": "aggregator-contract-v1",
                        "source_cohort_id": "aggregator-cohort-v1",
                    },
                    {
                        "event_id": "good",
                        "source_id": "official",
                        "headline": "Official release",
                        "source_direct": True,
                        "source_verified": True,
                        "source_contract_id": "official-contract-v1",
                        "source_cohort_id": "official-cohort-v1",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    result = current_source_provenance_integrity(context)
    assert result["ok"] is False
    assert result["direct_article_count"] == 2
    assert result["invalid_row_count"] == 1


def test_current_source_provenance_accepts_indirect_unverified_source(
    tmp_path: Path,
) -> None:
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "articles": [
                    {
                        "event_id": "ok",
                        "source_id": "aggregator",
                        "source_direct": False,
                        "source_verified": False,
                        "source_contract_id": "aggregator-contract-v1",
                        "source_cohort_id": "aggregator-cohort-v1",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    result = current_source_provenance_integrity(context)
    assert result["ok"] is True
    assert result["direct_article_count"] == 0
    assert result["lineage_bound_article_count"] == 1
    assert result["missing_lineage_count"] == 0


def test_current_source_provenance_rejects_missing_indirect_lineage(
    tmp_path: Path,
) -> None:
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "articles": [
                    {
                        "event_id": "missing-lineage",
                        "source_id": "aggregator",
                        "headline": "Current research-only observation",
                        "source_direct": False,
                        "source_verified": False,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = current_source_provenance_integrity(context)

    assert result["ok"] is False
    assert result["status"] == "missing_source_lineage"
    assert result["missing_lineage_count"] == 1
    assert result["invalid_rows"][0]["reason"] == (
        "missing_source_contract_or_cohort"
    )


def test_news_consumer_must_bind_shared_classification_contract() -> None:
    current = {
        "required_news_classification_version": NEWS_CLASSIFICATION_VERSION,
        "diagnostics": {
            "required_news_classification_version": NEWS_CLASSIFICATION_VERSION
        }
    }
    stale = {
        "required_news_classification_version": "obsolete",
        "diagnostics": {"required_news_classification_version": "obsolete"}
    }
    assert news_consumer_classification_bound(current) is True
    assert news_consumer_classification_bound(stale) is False
    del current["required_news_classification_version"]
    assert news_consumer_classification_bound(current) is False


def test_retired_executable_opportunity_drain_is_fail_closed() -> None:
    retired = {
        "status": "draining_pending_outcomes",
        "runtime_mode": "mature_only",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "artifact_failures": [],
        "supported_execution_decision": "no_trade",
        "contract": {
            "future_forecast_production": False,
            "pending_forecasts_preserved_to_maturity": True,
        },
    }
    assert executable_opportunity_proof_is_fail_closed(
        retired, market_state="open_or_transition"
    )
    retired["status"] = "drained_retired"
    assert executable_opportunity_proof_is_fail_closed(
        retired, market_state="open_or_transition"
    )


def test_executable_opportunity_rejects_unsafe_or_ambiguous_retirement() -> None:
    retired = {
        "status": "draining_pending_outcomes",
        "runtime_mode": "mature_only",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "artifact_failures": [],
        "supported_execution_decision": "no_trade",
        "contract": {
            "future_forecast_production": False,
            "pending_forecasts_preserved_to_maturity": True,
        },
    }
    retired["contract"]["future_forecast_production"] = True
    assert not executable_opportunity_proof_is_fail_closed(
        retired, market_state="open_or_transition"
    )
    retired["contract"]["future_forecast_production"] = False
    retired["execution_eligible"] = True
    assert not executable_opportunity_proof_is_fail_closed(
        retired, market_state="open_or_transition"
    )


def test_source_governance_must_bind_shared_collector_contract() -> None:
    from trad.oanda_news_collector_contract import NEWS_COLLECTOR_CONTRACT_ID

    current = {
        "status": "ok",
        "research_only": True,
        "can_place_orders": False,
        "real_money_routing": False,
        "news_collector_contract_id": NEWS_COLLECTOR_CONTRACT_ID,
        "news_collector_cohort_id": NEWS_COLLECTOR_CONTRACT_ID,
    }
    assert source_governance_collector_bound(current) is True
    current["status"] = "building_governance"
    assert source_governance_collector_bound(current) is True
    current["can_place_orders"] = True
    assert source_governance_collector_bound(current) is False
    current["can_place_orders"] = False
    current["status"] = "unknown"
    assert source_governance_collector_bound(current) is False
    current["status"] = "ok"
    current["news_collector_contract_id"] = "stale"
    assert source_governance_collector_bound(current) is False


def test_source_governance_fast_lane_adapter_is_prospective_append_only_and_inert(
    tmp_path: Path,
) -> None:
    database = tmp_path / "source-governance.sqlite"
    connection = connect_registry(database)
    connection.close()
    activated = OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC.isoformat()
    state = {
        "status": "ok",
        "research_only": True,
        "can_place_orders": False,
        "real_money_routing": False,
        "official_fast_lane_governance_adapter_contract_id": (
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
        ),
        "official_fast_lane_governance_adapter_activated_utc": activated,
        "official_fast_lane_source_governance_adapter": {
            "status": "ok",
            "adapter_contract_id": (
                OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
            ),
            "adapter_activated_utc": activated,
            "upstream_collector_contract_id": (
                OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
            ),
            "upstream_collector_cohort_id": (
                OFFICIAL_RELEASE_FAST_LANE_COHORT_ID
            ),
            "allowed_official_source_count": 24,
            "active_source_lineage_count": 24,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_authorize": False,
        },
    }
    valid = source_governance_fast_lane_adapter_integrity(
        state, database_path=database
    )
    assert valid["ok"] is True
    assert valid["receipt_count"] == 0
    assert valid["append_only_triggers_present"] is True
    assert valid["adapter_table_integrity"] == "ok"
    assert valid["full_database_integrity"] == "ok"
    assert valid["full_database_check_performed"] is True
    attestation = valid["full_database_integrity_attestation"]
    checked_epoch = datetime.fromisoformat(
        str(attestation["checked_utc"]).replace("Z", "+00:00")
    ).timestamp()
    reused = source_governance_fast_lane_adapter_integrity(
        state,
        database_path=database,
        full_database_integrity_attestation=attestation,
        cutoff_epoch=checked_epoch + 1.0,
    )
    assert reused["ok"] is True
    assert reused["full_database_check_performed"] is False
    assert reused["full_database_integrity_attestation"][
        "legacy_completed_scan_migrated"
    ] is False
    assert reused["database_integrity_scope"] == (
        "bounded_full_quick_check_attestation_plus_current_adapter_table_"
        "and_cross_table_receipt_validation"
    )
    expired_attestation = dict(attestation)
    expired_attestation["checked_utc"] = "2000-01-01T00:00:00+00:00"
    expired = source_governance_fast_lane_adapter_integrity(
        state,
        database_path=database,
        full_database_integrity_attestation=expired_attestation,
        cutoff_epoch=datetime.now(timezone.utc).timestamp(),
        maximum_full_integrity_age_sec=1.0,
    )
    assert expired["ok"] is True
    assert expired["full_database_check_performed"] is True

    stale = json.loads(json.dumps(state))
    stale["official_fast_lane_source_governance_adapter"][
        "adapter_contract_id"
    ] = "stale"
    assert source_governance_fast_lane_adapter_integrity(
        stale, database_path=database
    )["ok"] is False

    connection = sqlite3.connect(database)
    connection.execute(
        "INSERT INTO official_fast_lane_governance_imports VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "receipt", "observation", "boj_updates", "provider-event",
            "listing_observation", "a" * 64, "missing-source-event",
            "2026-08-27T03:57:07.220000+00:00",
            "2026-08-27T03:57:07.220000+00:00",
            "2026-08-27T07:00:00+00:00",
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
            activated,
            OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
            OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
            "source-contract", "source-cohort", 1, 0, 0,
        ),
    )
    connection.commit()
    connection.close()
    rejected = source_governance_fast_lane_adapter_integrity(
        state, database_path=database
    )
    assert rejected["ok"] is False
    assert rejected["preactivation_receipts"] == 1
    assert rejected["orphan_or_mismatched_source_events"] == 1


def test_persistent_policy_state_rejects_calendar_as_stance(tmp_path: Path) -> None:
    state_path = tmp_path / "policy.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "persistent_policy_state_v3_stance_bearing_only",
                "document_count": 1,
                "currencies": {
                    "JPY": {
                        "source_id": "boj_policy_decision_calendar_2026",
                        "event_id": "clock",
                        "known_utc": "2026-08-16T05:40:55+00:00",
                        "policy_stance_bearing_eligible": True,
                        "headline": "Bank of Japan Monetary Policy Decision",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    result = persistent_policy_state_integrity(state_path)
    assert result["ok"] is False
    assert result["invalid_row_count"] == 1
    assert "schedule_used_as_policy_state" in result["invalid_rows"][0]["reasons"]


def test_persistent_policy_state_accepts_completed_decision(tmp_path: Path) -> None:
    state_path = tmp_path / "policy.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "persistent_policy_state_v3_stance_bearing_only",
                "document_count": 1,
                "excluded_policy_clock_count": 2,
                "currencies": {
                    "JPY": {
                        "source_id": "boj_updates",
                        "event_id": "decision",
                        "known_utc": "2026-08-09T23:50:00+00:00",
                        "policy_stance_bearing_eligible": True,
                        "headline": "Summary of Opinions at the Monetary Policy Meeting",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    result = persistent_policy_state_integrity(state_path)
    assert result["ok"] is True
    assert result["currency_count"] == 1


def test_market_state_classifies_weekend_without_using_strategy_results() -> None:
    assert forex_market_state(datetime(2026, 8, 8, 12, tzinfo=timezone.utc)) == "weekend_closed"
    assert forex_market_state(datetime(2026, 8, 10, 12, tzinfo=timezone.utc)) == "open_or_transition"
    assert forex_market_state(datetime(2026, 8, 7, 21, tzinfo=timezone.utc)) == "weekend_closed"
    assert forex_market_state(datetime(2026, 8, 9, 20, tzinfo=timezone.utc)) == "weekend_closed"
    assert forex_market_state(datetime(2026, 8, 9, 21, tzinfo=timezone.utc)) == "open_or_transition"


def test_weekend_availability_suppresses_gap_movement() -> None:
    state = {
        "latest": {"market_state": "weekend_closed"},
        "current_gap_start_epoch": 1.0,
        "current_gap_classification": "market_closed_expected",
        "current_gap_market_movement": [],
        "comparison": {
            "latest": {"market_state": "weekend_closed"},
            "current_gap_start_epoch": 1.0,
            "current_gap_classification": "market_closed_expected",
            "current_gap_market_movement": [],
        },
    }
    assert availability_market_state_consistent(state, "weekend_closed")
    state["current_gap_market_movement"] = [{"instrument": "EUR_USD"}]
    assert not availability_market_state_consistent(state, "weekend_closed")


def test_cross_pair_snapshot_is_ingestion_order_invariant() -> None:
    first = {
        "EUR_USD": {"bid": 1.1, "ask": 1.1002, "time": "2026-08-08T12:00:00Z", "tradeable": True},
        "USD_JPY": {"bid": 150.0, "ask": 150.02, "time": "2026-08-08T12:00:01Z", "tradeable": False},
    }
    second = dict(reversed(list(first.items())))
    left = synchronized_quote_audit(first, cutoff_epoch=1786190460.0)
    right = synchronized_quote_audit(second, cutoff_epoch=1786190460.0)
    assert left["snapshot_sha256"] == right["snapshot_sha256"]
    assert left["cross_pair_event_time_dispersion_sec"] == 1.0


def test_routability_uses_fresh_snapshot_not_last_change_dispersion() -> None:
    cutoff = 1786190460.0
    quotes = {
        f"PAIR_{index:02d}": {
            "bid": 1.0 + index / 1000.0,
            "ask": 1.0001 + index / 1000.0,
            "time": "2026-08-08T11:55:00Z" if index == 0 else "2026-08-08T12:00:00Z",
            "tradeable": index < 65,
        }
        for index in range(68)
    }
    audit = synchronized_quote_audit(quotes, cutoff_epoch=cutoff)
    assert audit["cross_pair_event_time_dispersion_sec"] == 300.0
    state = {
        "schema_version": 3,
        "generated_utc": datetime.fromtimestamp(cutoff - 2.0, timezone.utc).isoformat(),
        "quote_count": 68,
        "coverage": {
            "current_quote_count": 68,
            "retained_last_known_count": 0,
            "current_tradeable_quote_count": 65,
            "current_non_tradeable_quote_count": 3,
            "current_tradeability_unknown_count": 0,
            "current_non_tradeable_instruments": [
                "PAIR_65", "PAIR_66", "PAIR_67"
            ],
            "tradeability_contract": "oanda_client_price_status_boolean_v1",
        },
    }
    assert quote_tradeability_contract_current(audit, state)
    assert cross_pair_snapshot_routable(
        audit, state, cutoff_epoch=cutoff, market_state="open_or_transition"
    )
    state["coverage"]["tradeability_contract"] = "missing_status_contract"
    assert not quote_tradeability_contract_current(audit, state)
    assert not cross_pair_snapshot_routable(
        audit, state, cutoff_epoch=cutoff, market_state="open_or_transition"
    )
    state["coverage"]["tradeability_contract"] = (
        "oanda_client_price_status_boolean_v1"
    )
    state["generated_utc"] = datetime.fromtimestamp(cutoff - 20.0, timezone.utc).isoformat()
    assert not cross_pair_snapshot_routable(
        audit, state, cutoff_epoch=cutoff, market_state="open_or_transition"
    )


def test_quote_snapshot_cutoff_is_bound_to_atomic_read(tmp_path) -> None:
    quote_path = tmp_path / "quotes.json"
    quotes = {
        f"PAIR_{index:02d}": {
            "bid": 1.0 + index / 1000.0,
            "ask": 1.0001 + index / 1000.0,
            "time": datetime.now(timezone.utc).isoformat(),
            "tradeable": index < 65,
        }
        for index in range(68)
    }
    state = {
        "schema_version": 3,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "quote_count": 68,
        "coverage": {
            "current_quote_count": 68,
            "retained_last_known_count": 0,
            "current_tradeable_quote_count": 65,
            "current_non_tradeable_quote_count": 3,
            "current_tradeability_unknown_count": 0,
            "current_non_tradeable_instruments": [
                "PAIR_65", "PAIR_66", "PAIR_67"
            ],
            "tradeability_contract": "oanda_client_price_status_boolean_v1",
        },
        "quotes": quotes,
    }
    quote_path.write_text(json.dumps(state), encoding="utf-8")

    captured, cutoff = capture_current_quote_snapshot(quote_path)
    audit = synchronized_quote_audit(
        captured["quotes"], cutoff_epoch=cutoff.timestamp()
    )

    assert captured == state
    assert cross_pair_snapshot_routable(
        audit,
        captured,
        cutoff_epoch=cutoff.timestamp(),
        market_state="open_or_transition",
    )


def test_collector_cohort_must_bind_code_and_definition_hash() -> None:
    valid = {
        "cohort": {
            "cohort_id": "source.discovery.abcdef0123456789",
            "cohort_definition_sha256": "abcdef0123456789" + "0" * 48,
            "collector_sha256": "c" * 64,
            "material_change_requires_new_cohort": True,
        }
    }
    assert collector_cohort_bound(valid)
    assert not collector_cohort_bound({
        "cohort": {**valid["cohort"], "cohort_id": "source.discovery.config_only"}
    })
    assert not collector_cohort_bound({
        "cohort": {**valid["cohort"], "collector_sha256": ""}
    })


def test_historical_archive_gap_requires_matching_frozen_digest() -> None:
    assert historical_archive_gap_digest_preserved(
        {"detail_reconciliation": {"detail_gap_vs_rollup": 0}}
    )
    contained = {
        "detail_reconciliation": {
            "detail_gap_vs_rollup": 332,
            "gap_dates_have_matching_frozen_snapshot": True,
            "nonzero_gap_dates": [
                {
                    "detail_gap": 332,
                    "snapshot_matches_rollup": True,
                    "frozen_snapshot_sha256": "abc",
                }
            ],
        }
    }
    assert historical_archive_gap_digest_preserved(contained)
    contained["detail_reconciliation"]["nonzero_gap_dates"][0][
        "snapshot_matches_rollup"
    ] = False
    assert not historical_archive_gap_digest_preserved(contained)


def test_rollup_lag_does_not_invalidate_preserved_historical_gap() -> None:
    state = {
        "detail_reconciliation": {
            "detail_gap_vs_rollup": -2408,
            "historical_missing_detail_rows": 332,
            "positive_gap_dates": [{
                "detail_gap": 332,
                "snapshot_matches_rollup": True,
                "frozen_snapshot_sha256": "abc",
            }],
            "rollup_lag_dates": [{
                "detail_gap": -2740,
                "snapshot_matches_rollup": None,
            }],
            "gap_dates_have_matching_frozen_snapshot": True,
        }
    }
    assert historical_archive_gap_digest_preserved(state)


def test_event_preflight_integrity_reconciles_direct_and_fallback_transport() -> None:
    now = datetime(2026, 9, 2, 0, 15, tzinfo=timezone.utc)
    state = {
        "schema_version": "event_technical_preflight_v1",
        "contract_id": (
            "neutral_event_clock_to_technical_preflight_v6_"
            "source_transport_readiness_20260901"
        ),
        "generated_utc": now.isoformat(),
        "policy_direct_release_ready_rows": 1,
        "policy_fallback_only_rows": 1,
        "policy_release_transport_blocked_rows": 0,
        "events": [
            {
                "policy_event": True,
                "policy_release_transport_state": "direct_release_ready",
                "policy_direct_release_transport_available": True,
                "policy_fallback_transport_available": False,
                "execution_eligible": False,
            },
            {
                "policy_event": True,
                "policy_release_transport_state": (
                    "fallback_only_direct_release_unavailable"
                ),
                "policy_direct_release_transport_available": False,
                "policy_fallback_transport_available": True,
                "execution_eligible": False,
            },
        ],
        "policy": {
            "can_place_orders": False,
            "can_promote": False,
            "calendar_readiness_is_not_release_transport_readiness": True,
            "publisher_search_fallback_is_not_direct_release_transport": True,
        },
        "research_only": True,
        "execution_eligible": False,
    }
    result = event_technical_preflight_source_integrity(
        state, cutoff_epoch=now.timestamp()
    )
    assert result["ok"] is True
    assert result["direct_release_ready_rows"] == 1
    assert result["fallback_only_rows"] == 1
    state["policy_direct_release_ready_rows"] = 2
    assert event_technical_preflight_source_integrity(
        state, cutoff_epoch=now.timestamp()
    )["ok"] is False
