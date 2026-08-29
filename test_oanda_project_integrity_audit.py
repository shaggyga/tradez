from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from trad.oanda_project_integrity_audit import (
    CURRENT_NEWS_CLASSIFICATION_VERSION,
    EXPECTED_CONFIGURED_NEWS_SOURCES,
    availability_market_state_consistent,
    collector_cohort_bound,
    current_source_provenance_integrity,
    cross_pair_snapshot_routable,
    executable_opportunity_proof_is_fail_closed,
    forex_market_state,
    historical_archive_gap_digest_preserved,
    independent_verifier_operationally_safe,
    latest_sealable_clock,
    live_move_forward_proof_integrity,
    live_move_persistent_context_integrity,
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
    source_governance_collector_bound,
    source_governance_fast_lane_adapter_integrity,
    structured_numeric_currency_integrity,
    synchronized_quote_audit,
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


def test_move_first_news_audit_must_be_fresh_fail_closed_and_newer_than_census() -> None:
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
    census = {"generated_utc": "2026-08-19T02:00:00+00:00"}
    cutoff = datetime(2026, 8, 19, 4, 0, tzinfo=timezone.utc).timestamp()
    assert move_first_news_audit_is_current(
        audit, census, cutoff_epoch=cutoff
    ) is True
    assert move_first_news_audit_is_current(
        {**audit, "research_only": False}, census, cutoff_epoch=cutoff
    ) is False
    assert move_first_news_audit_is_current(
        {**audit, "generated_utc": "2026-08-19T01:00:00+00:00"},
        census,
        cutoff_epoch=cutoff,
    ) is False


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
                prospective_observation INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO official_release_observation VALUES (0)"
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
    assert EXPECTED_CONFIGURED_NEWS_SOURCES == 102


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
                    },
                    {
                        "event_id": "good",
                        "source_id": "official",
                        "headline": "Official release",
                        "source_direct": True,
                        "source_verified": True,
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
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    result = current_source_provenance_integrity(context)
    assert result["ok"] is True
    assert result["direct_article_count"] == 0


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
        "EUR_USD": {"bid": 1.1, "ask": 1.1002, "time": "2026-08-08T12:00:00Z"},
        "USD_JPY": {"bid": 150.0, "ask": 150.02, "time": "2026-08-08T12:00:01Z"},
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
        }
        for index in range(68)
    }
    audit = synchronized_quote_audit(quotes, cutoff_epoch=cutoff)
    assert audit["cross_pair_event_time_dispersion_sec"] == 300.0
    state = {
        "generated_utc": datetime.fromtimestamp(cutoff - 2.0, timezone.utc).isoformat(),
        "coverage": {"current_quote_count": 68, "retained_last_known_count": 0},
    }
    assert cross_pair_snapshot_routable(
        audit, state, cutoff_epoch=cutoff, market_state="open_or_transition"
    )
    state["generated_utc"] = datetime.fromtimestamp(cutoff - 20.0, timezone.utc).isoformat()
    assert not cross_pair_snapshot_routable(
        audit, state, cutoff_epoch=cutoff, market_state="open_or_transition"
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
