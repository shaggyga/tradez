from __future__ import annotations

import sqlite3
from pathlib import Path

import gzip
import hashlib
import json
import shutil
import pytest

import trad.oanda_research_genealogy as genealogy

from trad.oanda_research_genealogy import (
    connect,
    import_external_discovery_report,
    import_gdelt_mapping_report,
    import_prospective_source_state,
    import_treasury_source_state,
    import_alfred_source_state,
    import_shadow_runtime_state,
    import_model_artifact_manifest,
    import_currency_state_after_cost_state,
    observe_currency_state_after_cost_supersession,
    import_internal_discovery_artifact,
    import_macro_point_in_time_validation,
    import_counterfactual_sim_gym,
    import_lifecycle,
    import_zero_output_adapter_retirement,
    insert_experiment,
    observe,
)


def insert_parent_fixture(
    connection: sqlite3.Connection, hypothesis_id: str, experiment_kind: str,
    *, definition_sha256: str | None = None,
) -> None:
    definition = {
        "hypothesis_id": hypothesis_id,
        "parent_hypothesis_id": None,
        "experiment_kind": experiment_kind,
        "research_generation": "fixture",
        "idea_origin": "fixture",
        "pre_registered": 1,
        "data_sources_json": "[]",
        "feature_contract_json": "{}",
        "label_contract_json": "{}",
        "model_contract_json": "{}",
        "cost_contract_json": "{}",
        "allocator_contract_json": "{}",
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": "{}",
        "all_parameters_tried_json": "{}",
        "selection_rule": "fixture",
        "holdouts_touched_json": "[]",
        "source_code_hash": "fixture",
        "data_snapshot_hash": "fixture",
        "definition_sha256": definition_sha256 or genealogy.stable_hash(hypothesis_id),
        "created_at": "2026-08-29T00:00:00+00:00",
        "definition_json": "{}",
    }
    assert genealogy.insert_experiment(connection, definition)


def write_predecessor_registry_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    predecessor_path = tmp_path / "predecessor.sqlite"
    baseline_path = tmp_path / "baseline.sqlite"
    predecessor = connect(predecessor_path)
    baseline = connect(baseline_path)
    try:
        insert_parent_fixture(predecessor, "common.exact", "runtime")
        insert_parent_fixture(baseline, "common.exact", "runtime")
        insert_parent_fixture(predecessor, "legacy.only", "legacy_runtime")
        insert_parent_fixture(predecessor, "definition.conflict", "legacy_runtime")
        insert_parent_fixture(
            baseline, "definition.conflict", "hardened_runtime",
            definition_sha256=genealogy.stable_hash("hardened-definition"),
        )
        insert_parent_fixture(baseline, "hardened.only", "hardened_runtime")
        assert observe(
            predecessor, hypothesis_id="common.exact",
            observed_at="2026-08-01T00:00:00+00:00", source_system="old",
            result="collecting", evidence={"value": 1},
        )
        assert observe(
            predecessor, hypothesis_id="legacy.only",
            observed_at="2026-08-01T00:01:00+00:00", source_system="old",
            result="collecting", evidence={"value": 2},
        )
        assert observe(
            predecessor, hypothesis_id="definition.conflict",
            observed_at="2026-08-01T00:02:00+00:00", source_system="old",
            result="collecting", evidence={"value": 3},
        )
        predecessor.commit(); baseline.commit()
    finally:
        predecessor.close(); baseline.close()
    return predecessor_path, baseline_path, tmp_path / "predecessor-ledger"


def write_curriculum_fixture(root: Path, parent_id: str) -> tuple[str, Path, Path]:
    root.mkdir(parents=True)
    policy = dict(genealogy.FULL_RESEARCH_ONLY_SAFETY)
    timestamp = "2026-08-30T01:00:00+00:00"
    contract = {
        "policy": policy,
        "source_cohort_id": parent_id,
        "source_session_id": "source-session",
        "source_session_seal_id": "source-seal",
        "timestamp_contract": {
            "mode": "source_state_generated_utc", "value": timestamp,
        },
        "curriculum": {
            "learner_id": "learner.v1",
            "scheduler_id": "scheduler.v1",
            "session_count": 1,
        },
        "core_code_sha256": "b" * 64,
        "producer_code_sha256": "c" * 64,
    }
    contract_sha = genealogy._variadic_hash(contract)
    cohort_id = "sequential_portfolio_curriculum_v1." + contract_sha[:20]
    cohort_root = root / "cohorts" / cohort_id
    cohort_root.mkdir(parents=True)
    database = cohort_root / "curriculum.sqlite"
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE spc_cohorts(cohort_id TEXT PRIMARY KEY,created_utc TEXT,"
        "source_cohort_id TEXT,source_session_id TEXT,contract_sha256 TEXT,"
        "contract_json TEXT)"
    )
    connection.execute(
        "INSERT INTO spc_cohorts VALUES (?,?,?,?,?,?)",
        (
            cohort_id, timestamp, parent_id,
            "source-session", contract_sha, json.dumps(contract),
        ),
    )
    table_names = {
        "cases": "spc_cases", "source_outcomes": "spc_source_outcomes",
        "sessions": "spc_sessions", "assignments": "spc_assignments",
        "attempts": "spc_attempts", "feedback": "spc_feedback",
        "memory_transitions": "spc_memory_transitions",
    }
    roots = {}
    for ordinal, (name, table) in enumerate(table_names.items(), start=1):
        connection.execute(
            f"CREATE TABLE {table}(row_id TEXT PRIMARY KEY,cohort_id TEXT,"
            "value TEXT,row_sha256 TEXT)"
        )
        payload = {"row_id": f"{name}-1", "cohort_id": cohort_id, "value": str(ordinal)}
        row_sha = genealogy._variadic_hash(payload)
        connection.execute(
            f"INSERT INTO {table} VALUES (?,?,?,?)",
            (payload["row_id"], cohort_id, payload["value"], row_sha),
        )
        roots[name] = {"count": 1, "set_sha256": genealogy._variadic_hash([row_sha])}
    statistics = {
        "practice_session_count": 1, "attempt_count": 1,
        "distinct_case_count": 1, "repeated_attempt_count": 0,
        "distinct_market_repetition_count": 1, "structural_episode_count": 1,
        "independent_regime_count": None,
        "independent_regime_count_state": "unknown_inspected_historical_window",
        "correct_attempt_count": 1, "mean_regret_pips": 0.0,
        "mistake_counts": {"none": 1}, "roots": roots,
    }
    seal_payload = {"cohort_id": cohort_id, "contract_sha256": contract_sha, **statistics}
    seal_sha = genealogy._variadic_hash(seal_payload)
    seal_id = "spcseal_" + seal_sha[:28]
    connection.execute(
        "CREATE TABLE spc_session_seals(seal_id TEXT,cohort_id TEXT,created_utc TEXT,seal_sha256 TEXT,seal_json TEXT)"
    )
    connection.execute(
        "INSERT INTO spc_session_seals VALUES (?,?,?,?,?)",
        (seal_id, cohort_id, timestamp, seal_sha, json.dumps(seal_payload)),
    )
    snapshot_id = "spcsnapshot_" + genealogy._variadic_hash(cohort_id, statistics)[:28]
    connection.execute(
        "CREATE TABLE spc_snapshots(snapshot_id TEXT,cohort_id TEXT,generated_utc TEXT,statistics_sha256 TEXT,statistics_json TEXT)"
    )
    connection.execute(
        "INSERT INTO spc_snapshots VALUES (?,?,?,?,?)",
        (
            snapshot_id, cohort_id, timestamp, genealogy._variadic_hash(statistics),
            json.dumps(statistics),
        ),
    )
    connection.commit(); connection.close()
    state = {
        **policy,
        "evidence_role": "historical_training_discovery",
        "cohort_id": cohort_id,
        "source_cohort_id": parent_id,
        "source_session_id": "source-session",
        "source_session_seal_id": "source-seal",
        "contract_sha256": contract_sha,
        "database": str(database),
        "generated_utc": timestamp,
        "session_seal_id": seal_id,
        "counterfactual_options_count_as_repetitions": False,
        "repeat_attempts_count_as_repetitions": False,
        **statistics,
    }
    verifier = {
        **policy,
        "evidence_role": "historical_training_discovery",
        "cohort_id": cohort_id,
        "verified": True,
        "failures": [],
        "checks": {
            "verifier_forbidden_imports": [],
            "precommitted_session_count": 1, "attempt_count": 1,
            "distinct_case_count": 1, "repeated_attempt_count": 0,
            "distinct_market_repetition_count": 1, "structural_episode_count": 1,
            "correct_attempt_count": 1, "mean_regret_pips": 0.0,
            "mistake_counts": {"none": 1},
        },
    }
    state_path = root / "sequential_portfolio_curriculum_v1.json"
    verifier_path = root / "sequential_portfolio_curriculum_verifier_v1.json"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    verifier_path.write_text(json.dumps(verifier), encoding="utf-8")
    return cohort_id, state_path, verifier_path


def write_mistake_fixture(root: Path, parent_id: str) -> tuple[str, Path]:
    root.mkdir(parents=True)
    session_id = "replay-session"
    created = "2026-08-30T01:02:00+00:00"
    database_root = root / "cohorts" / parent_id
    database_root.mkdir(parents=True)
    database = database_root / "replay.sqlite"
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    root_tables = {
        "administrative_events": "spr_administrative_events",
        "clocks": "spr_clocks", "counterfactuals": "spr_counterfactuals",
        "decisions": "spr_decisions", "execution_legs": "spr_execution_legs",
        "execution_outcomes": "spr_execution_outcomes", "feedback": "spr_feedback",
        "pair_contexts": "spr_pair_contexts",
    }
    roots = {}
    for ordinal, (name, table) in enumerate(root_tables.items(), start=1):
        connection.execute(
            f"CREATE TABLE {table}(row_id TEXT PRIMARY KEY,cohort_id TEXT,value TEXT,row_sha256 TEXT)"
        )
        if name == "administrative_events":
            hashes = []
        else:
            payload = {"row_id": name + "-1", "cohort_id": parent_id, "value": str(ordinal)}
            row_sha = genealogy._variadic_hash(payload)
            connection.execute(
                f"INSERT INTO {table} VALUES (?,?,?,?)",
                (payload["row_id"], parent_id, payload["value"], row_sha),
            )
            hashes = [row_sha]
        roots[name] = {"count": len(hashes), "set_sha256": genealogy._variadic_hash(hashes)}
    seal_payload = {
        "cohort_id": parent_id, "session_id": session_id,
        "previous_seal_id": None, "roots": roots,
    }
    seal_sha = genealogy._variadic_hash(seal_payload)
    seal_id = "sprseal_" + seal_sha[:28]
    connection.execute(
        "CREATE TABLE spr_session_seals(seal_id TEXT,cohort_id TEXT,session_id TEXT,created_utc TEXT,seal_sha256 TEXT,seal_json TEXT)"
    )
    connection.execute(
        "INSERT INTO spr_session_seals VALUES (?,?,?,?,?,?)",
        (seal_id, parent_id, session_id, created, seal_sha, json.dumps(seal_payload)),
    )
    connection.commit(); connection.close()
    replay_state = {
        "research_only": True,
        "execution_eligible": False,
        "proof_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "supported_decision": "no_trade",
        "cohort_id": parent_id,
        "session_id": session_id,
        "session_seal_id": seal_id,
        "database": str(database),
        "roots": roots,
    }
    replay_verifier = {
        "cohort_id": parent_id,
        "session_id": session_id,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "no_trade",
        "verified": True,
        "failures": [],
        "checks": {"roots": roots},
    }
    (root / "sequential_portfolio_replay_v1.json").write_text(
        json.dumps(replay_state), encoding="utf-8"
    )
    verifier_path = root / "sequential_portfolio_replay_verifier_v1.json"
    verifier_path.write_text(json.dumps(replay_verifier), encoding="utf-8")
    state_path = root / "sequential_portfolio_replay_v1.json"
    source_state_sha = hashlib.sha256(state_path.read_bytes()).hexdigest()
    source_verifier_sha = hashlib.sha256(verifier_path.read_bytes()).hexdigest()
    source_database_sha = hashlib.sha256(database.read_bytes()).hexdigest()
    mistake_root = root.parent / ".mistake_v1"
    policy = {
        **genealogy.FULL_RESEARCH_ONLY_SAFETY,
        "evidence_role": "historical_training_curriculum",
        "lifecycle_write": False, "signal_feed_write": False,
    }
    classification = {
        "raw_observations_are_not_independent": True,
        "structural_clusters_are_not_independent_regimes": True,
        "categories_are_nonexclusive": True,
    }
    material = {
        "schema_version": 1, "contract_id": "mistake.contract.v1",
        "classification": classification, "policy": policy,
        "source_cohort_id": parent_id, "source_session_id": session_id,
        "source_session_seal_id": seal_id,
        "source_session_seal_roots_sha256": genealogy._variadic_hash(roots),
        "source_database_sha256": source_database_sha,
        "source_state_sha256": source_state_sha,
        "source_verifier_receipt_sha256": source_verifier_sha,
        "core_code_sha256": "1" * 64, "producer_code_sha256": "2" * 64,
        "verifier_code_sha256": "3" * 64,
    }
    material_sha = genealogy._variadic_hash(material)
    cohort_id = "sequential_portfolio_mistake_curriculum_v1." + material_sha[:20]
    report_core = {
        "schema_version": 1,
        "contract_id": "mistake.contract.v1",
        **policy,
        "cohort_id": cohort_id,
        "material_contract": material,
        "material_contract_sha256": material_sha,
        "source_binding": {
            "cohort_id": parent_id,
            "session_id": session_id, "session_seal_id": seal_id,
            "session_seal_sha256": seal_sha, "as_of_seal_utc": created,
            "source_database_sha256": source_database_sha,
            "source_state_sha256": source_state_sha,
            "source_verifier_receipt_sha256": source_verifier_sha,
            "verified_roots": roots,
        },
        "classification_contract": classification,
        "source_counts": {"feedback_rows": 2},
        "summary": {
            "raw_observation_count": 2,
            "structural_cluster_count": 1,
            "independent_regime_count": None,
        },
        "categories": {"direction": {"raw_observation_count": 2}},
        "curriculum_priorities": [{"category": "direction"}],
        "limitations": ["historical only"],
    }
    report_sha = genealogy._variadic_hash(report_core)
    report = {
        **report_core, "report_sha256": report_sha,
        "report_id": "sprmistakecurriculum_" + report_sha[:28],
    }
    cohort_root = mistake_root / "cohorts" / cohort_id
    cohort_root.mkdir(parents=True)
    report_path = cohort_root / "SEQUENTIAL_PORTFOLIO_MISTAKE_CURRICULUM_V1.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    (cohort_root / "material_contract_v1.json").write_text(
        json.dumps(material), encoding="utf-8"
    )
    report_file_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
    receipt = {
        **policy, "schema_version": 1, "generated_utc": created,
        "verified": True, "failures": [], "cohort_id": cohort_id,
        "report_id": report["report_id"], "report_sha256": report_file_sha,
        "report_content_sha256": report_sha,
        "material_contract_sha256": material_sha,
        "checks": {
            "verifier_forbidden_imports": [], "source_cohort_id": parent_id,
            "source_session_id": session_id, "source_session_seal_id": seal_id,
            "source_session_seal_sha256": seal_sha,
            "source_database_sha256": source_database_sha,
            "source_state_sha256": source_state_sha,
            "source_verifier_receipt_sha256": source_verifier_sha,
            "source_roots": roots, "raw_observation_count": 2,
            "structural_cluster_count": 1,
        },
    }
    receipt_path = cohort_root / "sequential_portfolio_mistake_curriculum_verifier_v1.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    mistake_root.mkdir(parents=True, exist_ok=True)
    (mistake_root / "sequential_portfolio_mistake_curriculum_v1.json").write_bytes(
        report_path.read_bytes()
    )
    (mistake_root / "sequential_portfolio_mistake_curriculum_verifier_v1.json").write_bytes(
        receipt_path.read_bytes()
    )
    return cohort_id, verifier_path


def write_source_pack_fixture(root: Path) -> tuple[str, Path, Path]:
    root.mkdir(parents=True)
    policy = dict(genealogy.FULL_RESEARCH_ONLY_SAFETY)
    source_manifest = [
        {"session_key": "fixture", "instrument": "EUR_USD", "gzip_sha256": "a" * 64},
        {"session_key": "fixture", "instrument": "USD_JPY", "gzip_sha256": "b" * 64},
    ]
    material = {
        "schema_version": 1,
        "experiment_key": "sequential_replay_source_pack_v1",
        "config_sha256": "1" * 64,
        "core_sha256": "2" * 64,
        "runner_sha256": "3" * 64,
        "slice_identity_manifest": source_manifest,
        "safety": policy,
    }
    material_sha = genealogy.stable_hash(material)
    pack_id = "sequential_replay_source_pack_v1." + material_sha[:20]
    source_manifest_sha = genealogy.stable_hash(source_manifest)
    state = {
        **policy,
        "evidence_role": "historical_training_discovery",
        "pack_id": pack_id,
        "material_sha256": material_sha,
        "material_contract": material,
        "source_manifest": source_manifest,
        "source_manifest_sha256": source_manifest_sha,
        "contracts": {"core": {"sha256": "2" * 64}},
        "instrument_count": 2,
        "session_count": 1,
        "slice_count": 2,
        "scheduled_global_clock_count": 2,
        "scheduled_pair_context_count": 4,
        "market_repetition_count": 2,
        "independent_regime_count": None,
        "independent_regime_state": "unknown_structural_weekday_blocks_only",
        "selection_rule": "weekday and UTC clock only",
        "sessions": [{
            "session_key": "fixture",
            "start_utc": "2026-08-29T12:00:00Z",
            "end_utc_exclusive": "2026-08-29T13:00:00Z",
        }],
        "coverage": {"fully_ready_pair_context_count": 4},
    }
    verifier = {
        "verified": True,
        "failure_count": 0,
        "failures": [],
        "pack_id": pack_id,
        "material_sha256": material_sha,
        "source_manifest_sha256": source_manifest_sha,
        "reconstructed_slice_count": 2,
        "reconstructed_instrument_count": 2,
        "reconstructed_global_clock_count": 2,
        "reconstructed_pair_context_count": 4,
    }
    state_path = root / "sequential_replay_source_pack_v1.json"
    verifier_path = root / "sequential_replay_source_pack_verifier_v1.json"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    immutable_manifest = root / "packs" / pack_id / "manifest.json"
    immutable_manifest.parent.mkdir(parents=True)
    immutable_manifest.write_bytes(state_path.read_bytes())
    verifier_path.write_text(json.dumps(verifier), encoding="utf-8")
    return pack_id, state_path, verifier_path


def write_all68_batch_fixture(
    root: Path, parent_pack_id: str,
) -> tuple[str, dict[str, str], Path]:
    policy = dict(genealogy.FULL_RESEARCH_ONLY_SAFETY)
    source_material = {"schema_version": 1, "fixture_parent": parent_pack_id, "safety": policy}
    source_material_sha = genealogy.stable_hash(source_material)
    source_receipt = {
        "verified": True,
        "failures": [],
        "pack_id": parent_pack_id,
        "material_sha256": source_material_sha,
        "source_manifest_sha256": genealogy.stable_hash([]),
    }
    source_manifest = {
        **policy,
        "pack_id": parent_pack_id,
        "material_contract": source_material,
        "material_sha256": source_material_sha,
        "source_manifest": [],
        "source_manifest_sha256": genealogy.stable_hash([]),
        "sessions": [{
            "session_key": "fixture",
            "end_utc_exclusive": "2026-08-30T00:00:00+00:00",
        }],
    }
    source_receipt_path = root / "source_pack_clean_verifier_receipt.json"
    source_manifest_path = root / "source_pack_manifest.json"
    root.mkdir(parents=True)
    source_receipt_path.write_text(json.dumps(source_receipt), encoding="utf-8")
    source_manifest_path.write_text(json.dumps(source_manifest), encoding="utf-8")
    binding = {
        "pack_id": parent_pack_id,
        "archive_count": 2,
        "archive_binding_sha256": "5" * 64,
        "clean_verifier_receipt_sha256": hashlib.sha256(
            source_receipt_path.read_bytes()
        ).hexdigest(),
        "pack_manifest_sha256": hashlib.sha256(
            source_manifest_path.read_bytes()
        ).hexdigest(),
    }
    material = {
        "schema_version": 1,
        "config_sha256": "6" * 64,
        "core_sha256": "7" * 64,
        "runner_sha256": "8" * 64,
        "verifier_sha256": "9" * 64,
        "source_binding": binding,
        "safety": policy,
        "independence": {
            "independent_regime_count": None,
            "independent_regime_state": "unknown_structural_sessions_only",
            "pair_contexts_count_as_market_repetitions": False,
            "counterfactuals_count_as_market_repetitions": False,
            "replays_count_as_market_repetitions": False,
        },
        "frozen_policy": {"policy_id": "fixture"},
        "costs": {"round_trip_slippage_pips": 0.25},
    }
    material_sha = hashlib.sha256(
        genealogy.canonical_json([material]).encode("utf-8")
    ).hexdigest()
    cohort_id = "sequential_all68_portfolio_batch_replay_v1." + material_sha[:20]
    common = {
        "cohort_id": cohort_id,
        "proof_eligible": False,
        "counts_as_regime_repetition": 0,
    }
    dataset_rows = {
        "clocks": [
            {**common, "clock_id": "clock-1", "counts_as_market_repetition": 1},
            {**common, "clock_id": "clock-2", "counts_as_market_repetition": 1},
        ],
        "pair_contexts": [
            {
                **common,
                "context_id": f"context-{index}",
                "counts_as_market_repetition": 0,
                "causal_ready": True,
                "fully_ready": True,
                "missing_reasons": [],
                "candidate": ({"candidate_id": "candidate-1"} if index == 1 else None),
            }
            for index in range(1, 5)
        ],
        "decisions": [
            {
                **common, "decision_id": "decision-1",
                "counts_as_market_repetition": 1,
                "decision": {"action": "enter"},
                "execution": {"legs": [{"leg": "open"}]},
            },
            {
                **common, "decision_id": "decision-2",
                "counts_as_market_repetition": 1,
                "decision": {"action": "exit"},
                "execution": {"legs": [{"leg": "close"}]},
            },
        ],
        "feedback": [
            {**common, "feedback_id": "feedback-1", "counts_as_market_repetition": 0},
            {**common, "feedback_id": "feedback-2", "counts_as_market_repetition": 0},
        ],
        "counterfactuals": [{
            **common, "counterfactual_id": "counterfactual-1",
            "counts_as_market_repetition": 0,
        }],
        "terminals": [{
            **common, "terminal_id": "terminal-1",
            "counts_as_market_repetition": 0,
            "terminal_epoch": 1,
            "terminal_flat": True,
            "state_after": {"position": None, "realized_pips": -1.0},
        }],
    }
    dataset_files = {
        "clocks": "global_clocks.jsonl.gz",
        "pair_contexts": "pair_contexts.jsonl.gz",
        "decisions": "portfolio_decisions.jsonl.gz",
        "feedback": "feedback.jsonl.gz",
        "counterfactuals": "counterfactuals.jsonl.gz",
        "terminals": "session_terminals.jsonl.gz",
    }
    datasets = {}
    for name, rows in dataset_rows.items():
        sealed_rows = []
        for row in rows:
            sealed_rows.append({**row, "row_sha256": genealogy._variadic_hash(row)})
        raw = "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
            for row in sealed_rows
        ).encode("utf-8")
        payload = gzip.compress(raw, mtime=0)
        path = root / dataset_files[name]
        path.write_bytes(payload)
        hashes = [row["row_sha256"] for row in sealed_rows]
        datasets[name] = {
            "relative_path": dataset_files[name], "gzip_bytes": len(payload),
            "gzip_sha256": hashlib.sha256(payload).hexdigest(),
            "row_count": len(sealed_rows),
            "row_set_sha256": genealogy._variadic_hash(sorted(hashes)),
            "ordered_row_sha256": genealogy._variadic_hash(hashes),
        }
    state = {
        **policy,
        "cohort_id": cohort_id,
        "material_contract": material,
        "material_sha256": material_sha,
        "source_binding": binding,
        "generated_utc": "2026-08-30T02:00:00+00:00",
        "instrument_count": 2,
        "session_count": 1,
        "global_clock_count": 2,
        "pair_context_count": 4,
        "causal_ready_context_count": 4,
        "candidate_context_count": 1,
        "execution_leg_count": 2,
        "counterfactual_count": 1,
        "realized_pips": -1.0,
        "terminal_flat": True,
        "market_repetition_count": 2,
        "independent_regime_count": None,
        "independent_regime_state": "unknown_structural_sessions_only",
        "action_counts": {"enter": 1, "exit": 1},
        "context_availability_counts": {"fully_ready": 4},
        "datasets": datasets,
    }
    verifier = {
        "cohort_id": cohort_id,
        "verified": True,
        "failures": [],
        "generated_utc": "2026-08-30T02:01:00+00:00",
        "independent_regime_count": None,
        "reconstructed_global_clocks": 2,
        "reconstructed_pair_contexts": 4,
    }
    state_path = root / "state.json"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    (root / "verifier_receipt.json").write_text(
        json.dumps(verifier), encoding="utf-8"
    )
    specification = {
        "source_pack_id": parent_pack_id,
        "result": "fixture_historical_diagnostic",
        "source_pack_state": "fixture",
        "acceptance_schema": "strict_v2",
        "state_semantic_sha256": genealogy._variadic_hash({
            key: value for key, value in state.items() if key != "generated_utc"
        }),
        "verifier_semantic_sha256": genealogy._variadic_hash({
            key: value for key, value in verifier.items()
            if key != "generated_utc"
        }),
        "dataset_roots_sha256": genealogy._variadic_hash(datasets),
        "fixture_parent_definition_sha256": source_material_sha,
    }
    return cohort_id, specification, state_path


def write_all68_mistake_fixture(
    root: Path, replay_root: Path, *, strict_raw: bool,
    require_current: bool, config_tag: str,
) -> tuple[str, dict[str, object], Path, Path]:
    """Write a small independently sealed all-68 curriculum fixture."""
    root.mkdir(parents=True, exist_ok=True)
    state_path = replay_root / "state.json"
    verifier_path = replay_root / "verifier_receipt.json"
    pack_receipt_path = replay_root / "source_pack_clean_verifier_receipt.json"
    pack_manifest_path = replay_root / "source_pack_manifest.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    verifier = json.loads(verifier_path.read_text(encoding="utf-8"))
    datasets = genealogy._dataset_manifest_binding(replay_root, state["datasets"])
    assert datasets is not None
    rows = genealogy._read_verified_dataset_rows(replay_root, datasets)
    assert rows is not None
    parent_id = state["cohort_id"]
    pack_id = state["source_binding"]["pack_id"]
    state_semantic_sha = genealogy._variadic_hash({
        key: value for key, value in state.items() if key != "generated_utc"
    })
    verifier_semantic_sha = genealogy._variadic_hash({
        key: value for key, value in verifier.items() if key != "generated_utc"
    })
    state_sha = hashlib.sha256(state_path.read_bytes()).hexdigest()
    verifier_sha = hashlib.sha256(verifier_path.read_bytes()).hexdigest()
    binding = {
        "cohort_id": parent_id,
        "source_material_sha256": state["material_sha256"],
        "source_pack_id": pack_id,
        "source_dataset_specs": datasets,
        "source_dataset_roots_sha256": genealogy._variadic_hash(datasets),
        "source_state_semantic_sha256": state_semantic_sha,
        "source_verifier_semantic_sha256": verifier_semantic_sha,
        "source_pack_verifier_receipt_sha256": hashlib.sha256(
            pack_receipt_path.read_bytes()
        ).hexdigest(),
        "source_pack_manifest_sha256": hashlib.sha256(
            pack_manifest_path.read_bytes()
        ).hexdigest(),
    }
    if strict_raw:
        binding.update({
            "source_state_sha256": state_sha,
            "source_verifier_sha256": verifier_sha,
        })
    policy = {
        **genealogy.FULL_RESEARCH_ONLY_SAFETY,
        "evidence_role": "historical_training_curriculum",
        "lifecycle_write": False,
        "signal_feed_write": False,
    }
    classification = {
        "raw_rows_are_not_independent": True,
        "structural_clusters_are_not_independent_regimes": True,
        "categories_are_nonexclusive": True,
    }
    material = {
        "schema_version": 1,
        "contract_id": "all68.mistake.fixture.v1",
        "config_sha256": hashlib.sha256(config_tag.encode()).hexdigest(),
        "config_semantic_sha256": hashlib.sha256(
            ("semantic:" + config_tag).encode()
        ).hexdigest(),
        "core_code_sha256": "1" * 64,
        "producer_code_sha256": "2" * 64,
        "verifier_code_sha256": "3" * 64,
        "classification": classification,
        "safety": policy,
        "source_cohort_id": parent_id,
        "source_pack_id": pack_id,
        "source_dataset_specs": datasets,
        "source_dataset_roots_sha256": binding["source_dataset_roots_sha256"],
        "source_state_semantic_sha256": state_semantic_sha,
        "source_verifier_semantic_sha256": verifier_semantic_sha,
        "source_pack_verifier_receipt_sha256": binding[
            "source_pack_verifier_receipt_sha256"
        ],
        "source_pack_manifest_sha256": binding["source_pack_manifest_sha256"],
    }
    if strict_raw:
        material.update({
            "source_state_sha256": state_sha,
            "source_verifier_sha256": verifier_sha,
        })
    material_sha = genealogy._variadic_hash(material)
    cohort_id = "sequential_all68_mistake_curriculum_v1." + material_sha[:20]

    clocks = rows["clocks"]
    decisions = rows["decisions"]
    feedback = rows["feedback"]
    counterfactual = rows["counterfactuals"][0]
    curriculum_rows = []
    resource_components = []
    for index in range(2):
        curriculum_id = f"curriculum-{config_tag}-primary-{index}"
        cluster_id = f"resource-{config_tag}-{index}"
        core = {
            "curriculum_row_id": curriculum_id,
            "row_role": "primary_feedback",
            "curriculum_weight": 1,
            "proof_eligible": False,
            "execution_eligible": False,
            "counts_as_regime_repetition": 0,
            "counts_as_market_repetition": 1,
            "source_cohort_id": parent_id,
            "clock_id": clocks[index]["clock_id"],
            "decision_id": decisions[index]["decision_id"],
            "feedback_id": feedback[index]["feedback_id"],
            "counterfactual_id": None,
            "source_clock_row_sha256": clocks[index]["row_sha256"],
            "source_decision_row_sha256": decisions[index]["row_sha256"],
            "source_feedback_row_sha256": feedback[index]["row_sha256"],
            "source_counterfactual_row_sha256": None,
            "structural_cluster_id": cluster_id,
        }
        curriculum_rows.append({
            **core, "row_sha256": genealogy._variadic_hash(core),
        })
        resource = {
            "cluster_id": cluster_id,
            "curriculum_row_ids": [curriculum_id],
            "proof_eligible": False,
            "counts_as_regime_repetition": 0,
        }
        resource_components.append({
            **resource, "row_sha256": genealogy._variadic_hash(resource),
        })
    review_id = f"curriculum-{config_tag}-review"
    review_cluster_id = f"resource-{config_tag}-review"
    review = {
        "curriculum_row_id": review_id,
        "row_role": "depth_one_review",
        "curriculum_weight": 0,
        "proof_eligible": False,
        "execution_eligible": False,
        "counts_as_regime_repetition": 0,
        "counts_as_market_repetition": 0,
        "source_cohort_id": parent_id,
        "clock_id": clocks[0]["clock_id"],
        "decision_id": decisions[0]["decision_id"],
        "feedback_id": feedback[0]["feedback_id"],
        "counterfactual_id": counterfactual["counterfactual_id"],
        "source_clock_row_sha256": clocks[0]["row_sha256"],
        "source_decision_row_sha256": decisions[0]["row_sha256"],
        "source_feedback_row_sha256": feedback[0]["row_sha256"],
        "source_counterfactual_row_sha256": counterfactual["row_sha256"],
        "structural_cluster_id": review_cluster_id,
    }
    curriculum_rows.append({
        **review, "row_sha256": genealogy._variadic_hash(review),
    })
    review_resource = {
        "cluster_id": review_cluster_id,
        "curriculum_row_ids": [review_id],
        "proof_eligible": False,
        "counts_as_regime_repetition": 0,
    }
    resource_components.append({
        **review_resource,
        "row_sha256": genealogy._variadic_hash(review_resource),
    })
    all_ids = [row["curriculum_row_id"] for row in curriculum_rows]
    structural = {
        "cluster_id": f"structural-{config_tag}",
        "curriculum_row_ids": all_ids,
        "proof_eligible": False,
        "counts_as_regime_repetition": 0,
    }
    structural_clusters = [{
        **structural, "row_sha256": genealogy._variadic_hash(structural),
    }]
    summary = {
        "curriculum_row_count": 3,
        "feedback_resource_component_count": 3,
        "structural_cluster_count": 1,
        "source_global_clock_count": 2,
        "source_primary_feedback_count": 2,
        "source_depth_one_review_count": 1,
        "primary_weight_sum": 2,
        "review_weight_sum": 0,
        "deduplicated_primary_weight": 2,
        "independent_regime_count": None,
    }
    report_core = {
        **policy,
        "schema_version": 1,
        "generated_utc": "2026-08-30T02:00:00+00:00",
        "cohort_id": cohort_id,
        "contract_id": material["contract_id"],
        "material_contract": material,
        "material_contract_sha256": material_sha,
        "source_binding": binding,
        "classification_contract": classification,
        "curriculum_rows": curriculum_rows,
        "feedback_resource_components": resource_components,
        "structural_clusters": structural_clusters,
        "summary": summary,
        "categories": {},
        "category_definitions": {},
        "reason_counts": {},
        "curriculum_priorities": [],
        "limitations": ["historical training only"],
    }
    report_sha = genealogy._variadic_hash(report_core)
    report = {
        **report_core,
        "report_id": "a68mistakecurriculum_" + report_sha[:28],
        "report_sha256": report_sha,
    }
    cohort_root = root / "cohorts" / cohort_id
    cohort_root.mkdir(parents=True)
    report_path = cohort_root / "SEQUENTIAL_ALL68_MISTAKE_CURRICULUM_V1.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    (cohort_root / "material_contract_v1.json").write_text(
        json.dumps(material), encoding="utf-8"
    )
    checks = {
        "verifier_forbidden_imports": [],
        "source_cohort_id": parent_id,
        "source_pack_id": pack_id,
        "source_state_semantic_sha256": state_semantic_sha,
        "source_verifier_semantic_sha256": verifier_semantic_sha,
        "source_dataset_roots_sha256": binding["source_dataset_roots_sha256"],
        "curriculum_row_count": 3,
        "structural_cluster_count": 1,
    }
    if strict_raw:
        checks.update({
            "source_state_sha256": state_sha,
            "source_verifier_sha256": verifier_sha,
        })
    receipt = {
        **policy,
        "schema_version": 1,
        "generated_utc": "2026-08-30T02:00:01+00:00",
        "verified": True,
        "failures": [],
        "cohort_id": cohort_id,
        "report_id": report["report_id"],
        "material_contract_sha256": material_sha,
        "report_content_sha256": report_sha,
        "report_file_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
        "checks": checks,
    }
    receipt_path = cohort_root / "verifier_receipt.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    if require_current:
        (root / "sequential_all68_mistake_curriculum_v1.json").write_bytes(
            report_path.read_bytes()
        )
        (
            root / "sequential_all68_mistake_curriculum_verifier_v1.json"
        ).write_bytes(receipt_path.read_bytes())
    specification: dict[str, object] = {
        "result": f"fixture-{config_tag}",
        "require_current": require_current,
        "parent_cohort_id": parent_id,
        "report_id": report["report_id"],
        "report_content_sha256": report_sha,
        "report_file_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
        "acceptance_schema": (
            "strict_raw_parent_binding_v3"
            if strict_raw else "semantic_parent_binding_v2"
        ),
    }
    return cohort_id, specification, report_path, receipt_path


def test_sequential_curricula_and_source_pack_have_closed_correct_lineage(
    tmp_path: Path,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    replay_parent = "sequential_portfolio_replay_v1.parent"
    source_parent = "counterfactual_sim_gym_v1.parent"
    insert_parent_fixture(
        target, replay_parent, "historical_sequential_portfolio_training"
    )
    insert_parent_fixture(target, source_parent, "historical_counterfactual_sim")

    curriculum_id, _, _ = write_curriculum_fixture(
        tmp_path / "curriculum", replay_parent
    )
    mistake_id, _ = write_mistake_fixture(tmp_path / "replay", replay_parent)
    source_pack_id, _, _ = write_source_pack_fixture(tmp_path / "source-pack")

    assert genealogy.import_sequential_portfolio_curriculum(
        target, tmp_path / "curriculum"
    ) == (1, 1)
    assert genealogy.import_sequential_portfolio_mistake_curriculum(
        target, tmp_path / "replay"
    ) == (1, 1)
    assert genealogy.import_sequential_replay_source_pack(
        target, tmp_path / "source-pack", parent_hypothesis_id=source_parent
    ) == (1, 1)
    assert genealogy.import_sequential_portfolio_curriculum(
        target, tmp_path / "curriculum"
    ) == (0, 0)
    assert genealogy.import_sequential_portfolio_mistake_curriculum(
        target, tmp_path / "replay"
    ) == (0, 0)
    assert genealogy.import_sequential_replay_source_pack(
        target, tmp_path / "source-pack", parent_hypothesis_id=source_parent
    ) == (0, 0)

    rows = target.execute(
        "SELECT hypothesis_id,parent_hypothesis_id,experiment_kind "
        "FROM experiments WHERE hypothesis_id IN (?,?,?) ORDER BY hypothesis_id",
        (curriculum_id, mistake_id, source_pack_id),
    ).fetchall()
    assert [tuple(row) for row in rows] == sorted([
        (
            curriculum_id, replay_parent,
            "historical_sequential_portfolio_curriculum",
        ),
        (
            mistake_id, replay_parent,
            "historical_sequential_portfolio_mistake_curriculum",
        ),
        (
            source_pack_id, source_parent,
            "historical_replay_source_infrastructure",
        ),
    ])
    evidence = [
        json.loads(row[0]) for row in target.execute(
            "SELECT evidence_json FROM experiment_observations "
            "WHERE hypothesis_id IN (?,?,?)",
            (curriculum_id, mistake_id, source_pack_id),
        )
    ]
    assert all(row["execution_eligible"] is False for row in evidence)
    assert all(row["proof_eligible"] is False for row in evidence)
    assert all(row["supported_decision"] == "no_trade" for row in evidence)
    assert all(
        (
            row.get("independent_regime_count") is None
            if "independent_regime_count" in row else
            row["summary"]["independent_regime_count"] is None
        )
        for row in evidence
    )
    target.close()


def test_sequential_genealogy_importers_fail_closed_on_attestation_drift(
    tmp_path: Path,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    replay_parent = "sequential_portfolio_replay_v1.parent"
    source_parent = "counterfactual_sim_gym_v1.parent"
    insert_parent_fixture(
        target, replay_parent, "historical_sequential_portfolio_training"
    )
    insert_parent_fixture(target, source_parent, "historical_counterfactual_sim")
    _, _, curriculum_verifier = write_curriculum_fixture(
        tmp_path / "curriculum", replay_parent
    )
    _, replay_verifier = write_mistake_fixture(
        tmp_path / "replay", replay_parent
    )
    _, source_state, _ = write_source_pack_fixture(tmp_path / "source-pack")

    value = json.loads(curriculum_verifier.read_text(encoding="utf-8"))
    value["failures"] = ["tampered"]
    curriculum_verifier.write_text(json.dumps(value), encoding="utf-8")
    assert genealogy.import_sequential_portfolio_curriculum(
        target, tmp_path / "curriculum"
    ) == (0, 0)

    value = json.loads(replay_verifier.read_text(encoding="utf-8"))
    value["verified"] = False
    replay_verifier.write_text(json.dumps(value), encoding="utf-8")
    assert genealogy.import_sequential_portfolio_mistake_curriculum(
        target, tmp_path / "replay"
    ) == (0, 0)

    value = json.loads(source_state.read_text(encoding="utf-8"))
    value["independent_regime_count"] = 3
    source_state.write_text(json.dumps(value), encoding="utf-8")
    assert genealogy.import_sequential_replay_source_pack(
        target, tmp_path / "source-pack", parent_hypothesis_id=source_parent
    ) == (0, 0)
    assert target.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == 2
    target.close()


def test_all68_batch_replays_keep_exact_pack_parent_and_null_independence(
    tmp_path: Path,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    old_pack = "sequential_replay_source_pack_v1.old"
    corrected_pack = "sequential_replay_source_pack_v1.corrected"
    replay_root = tmp_path / "all68"
    old_root = replay_root / "cohorts" / "placeholder-old"
    corrected_root = replay_root / "cohorts" / "placeholder-corrected"
    old_id, old_spec, _ = write_all68_batch_fixture(old_root, old_pack)
    corrected_id, corrected_spec, _ = write_all68_batch_fixture(
        corrected_root, corrected_pack
    )
    insert_parent_fixture(
        target, old_pack, "historical_replay_source_infrastructure",
        definition_sha256=old_spec.pop("fixture_parent_definition_sha256"),
    )
    insert_parent_fixture(
        target, corrected_pack, "historical_replay_source_infrastructure",
        definition_sha256=corrected_spec.pop("fixture_parent_definition_sha256"),
    )
    old_root.rename(replay_root / "cohorts" / old_id)
    corrected_root.rename(replay_root / "cohorts" / corrected_id)
    specs = {old_id: old_spec, corrected_id: corrected_spec}

    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs=specs
    ) == (2, 2)
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs=specs
    ) == (0, 0)
    rows = target.execute(
        "SELECT hypothesis_id,parent_hypothesis_id,experiment_kind "
        "FROM experiments WHERE hypothesis_id IN (?,?) ORDER BY hypothesis_id",
        (old_id, corrected_id),
    ).fetchall()
    assert {tuple(row) for row in rows} == {
        (old_id, old_pack, "historical_all68_portfolio_batch_replay"),
        (
            corrected_id, corrected_pack,
            "historical_all68_portfolio_batch_replay",
        ),
    }
    observations = [
        json.loads(row[0]) for row in target.execute(
            "SELECT evidence_json FROM experiment_observations "
            "WHERE hypothesis_id IN (?,?)", (old_id, corrected_id),
        )
    ]
    assert all(row["independent_regime_count"] is None for row in observations)
    assert all(row["proof_eligible"] is False for row in observations)
    assert all(row["execution_eligible"] is False for row in observations)
    target.close()


def test_all68_batch_replay_rejects_parent_or_independence_drift(
    tmp_path: Path,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    pack_id = "sequential_replay_source_pack_v1.fixture"
    replay_root = tmp_path / "all68"
    placeholder = replay_root / "cohorts" / "placeholder"
    cohort_id, specification, state_path = write_all68_batch_fixture(
        placeholder, pack_id
    )
    insert_parent_fixture(
        target, pack_id, "historical_replay_source_infrastructure",
        definition_sha256=specification.pop("fixture_parent_definition_sha256"),
    )
    cohort_root = replay_root / "cohorts" / cohort_id
    placeholder.rename(cohort_root)
    state_path = cohort_root / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["independent_regime_count"] = 1
    state_path.write_text(json.dumps(state), encoding="utf-8")
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs={cohort_id: specification}
    ) == (0, 0)
    state["independent_regime_count"] = None
    state_path.write_text(json.dumps(state), encoding="utf-8")
    wrong = dict(specification)
    wrong["source_pack_id"] = "sequential_replay_source_pack_v1.wrong"
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs={cohort_id: wrong}
    ) == (0, 0)
    assert target.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == 1
    target.close()


@pytest.mark.parametrize(
    "tamper",
    ("receipt_stats", "database_row", "session_seal", "state_safety"),
)
def test_curriculum_genealogy_rebuilds_receipt_database_and_seal_binding(
    tmp_path: Path, tamper: str,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    parent = "sequential_portfolio_replay_v1.parent"
    insert_parent_fixture(target, parent, "historical_sequential_portfolio_training")
    _, state_path, receipt_path = write_curriculum_fixture(tmp_path / "curriculum", parent)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    if tamper == "receipt_stats":
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["checks"]["attempt_count"] += 1
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    elif tamper == "database_row":
        connection = sqlite3.connect(state["database"])
        connection.execute("UPDATE spc_attempts SET value='forged'")
        connection.commit(); connection.close()
    elif tamper == "session_seal":
        connection = sqlite3.connect(state["database"])
        connection.execute("UPDATE spc_session_seals SET seal_sha256=?", ("f" * 64,))
        connection.commit(); connection.close()
    else:
        state["broker_access"] = True
        state_path.write_text(json.dumps(state), encoding="utf-8")
    assert genealogy.import_sequential_portfolio_curriculum(
        target, tmp_path / "curriculum"
    ) == (0, 0)
    target.close()


@pytest.mark.parametrize(
    "field", ("proof_eligible", "can_authorize", "broker_access", "account_access"),
)
def test_source_pack_requires_every_explicit_safety_field(
    tmp_path: Path, field: str,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    parent = "counterfactual_sim_gym_v1.parent"
    insert_parent_fixture(target, parent, "historical_counterfactual_sim")
    _, state_path, _ = write_source_pack_fixture(tmp_path / "source-pack")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.pop(field)
    state_path.write_text(json.dumps(state), encoding="utf-8")
    assert genealogy.import_sequential_replay_source_pack(
        target, tmp_path / "source-pack", parent_hypothesis_id=parent
    ) == (0, 0)
    target.close()


def test_source_pack_registers_byte_pinned_legacy_lineage_in_fresh_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    parent = "counterfactual_sim_gym_v1.parent"
    insert_parent_fixture(target, parent, "historical_counterfactual_sim")
    current_id, state_path, _ = write_source_pack_fixture(tmp_path / "source-pack")
    legacy = json.loads(state_path.read_text(encoding="utf-8"))
    legacy_material = {
        "schema_version": 0,
        "legacy_fixture": True,
        "safety": {
            "research_only": True,
            "execution_eligible": False,
            "can_promote": False,
            "can_place_orders": False,
            "can_authorize": False,
            "supported_decision": "no_trade",
        },
    }
    legacy_material_sha = genealogy.stable_hash(legacy_material)
    legacy_id = (
        "sequential_replay_source_pack_v1." + legacy_material_sha[:20]
    )
    legacy.update({
        "pack_id": legacy_id,
        "material_contract": legacy_material,
        "material_sha256": legacy_material_sha,
    })
    for field in ("proof_eligible", "broker_access", "account_access"):
        legacy.pop(field, None)
    legacy_path = (
        tmp_path / "source-pack" / "packs" / legacy_id / "manifest.json"
    )
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(json.dumps(legacy), encoding="utf-8")
    legacy_file_sha = hashlib.sha256(legacy_path.read_bytes()).hexdigest()
    monkeypatch.setattr(
        genealogy,
        "SEQUENTIAL_LEGACY_SOURCE_PACK_SPECS",
        {legacy_id: {
            "manifest_sha256": legacy_file_sha,
            "lineage_state": "test_byte_pinned_pre_safety_schema",
        }},
    )

    assert genealogy.import_sequential_replay_source_pack(
        target, tmp_path / "source-pack", parent_hypothesis_id=parent
    ) == (2, 2)
    assert genealogy.import_sequential_replay_source_pack(
        target, tmp_path / "source-pack", parent_hypothesis_id=parent
    ) == (0, 0)
    rows = target.execute(
        "SELECT hypothesis_id,parent_hypothesis_id FROM experiments "
        "WHERE hypothesis_id IN (?,?) ORDER BY hypothesis_id",
        (current_id, legacy_id),
    ).fetchall()
    assert {tuple(row) for row in rows} == {
        (current_id, parent), (legacy_id, parent),
    }
    evidence = json.loads(target.execute(
        "SELECT evidence_json FROM experiment_observations "
        "WHERE hypothesis_id=?", (legacy_id,),
    ).fetchone()[0])
    assert evidence["legacy_explicit_safety_schema_incomplete"] is True
    assert evidence["proof_eligible"] is False
    assert evidence["execution_eligible"] is False
    target.close()


@pytest.mark.parametrize(
    "tamper",
    (
        "receipt", "dataset", "terminal", "state_summary",
        "source_receipt", "source_manifest",
    ),
)
def test_all68_genealogy_binds_verifier_state_and_every_dataset(
    tmp_path: Path, tamper: str,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    parent = "sequential_replay_source_pack_v1.fixture"
    replay_root = tmp_path / "all68"
    placeholder = replay_root / "cohorts" / "placeholder"
    cohort_id, specification, _ = write_all68_batch_fixture(placeholder, parent)
    insert_parent_fixture(
        target, parent, "historical_replay_source_infrastructure",
        definition_sha256=specification.pop("fixture_parent_definition_sha256"),
    )
    cohort_root = replay_root / "cohorts" / cohort_id
    placeholder.rename(cohort_root)
    if tamper == "receipt":
        path = cohort_root / "verifier_receipt.json"
        receipt = json.loads(path.read_text(encoding="utf-8"))
        receipt["reconstructed_global_clocks"] += 1
        path.write_text(json.dumps(receipt), encoding="utf-8")
    elif tamper == "dataset":
        path = cohort_root / "feedback.jsonl.gz"
        payload = bytearray(path.read_bytes()); payload[-1] ^= 1
        path.write_bytes(bytes(payload))
    elif tamper == "terminal":
        path = cohort_root / "state.json"
        state = json.loads(path.read_text(encoding="utf-8"))
        state["terminal_flat"] = False
        path.write_text(json.dumps(state), encoding="utf-8")
    elif tamper == "state_summary":
        path = cohort_root / "state.json"
        state = json.loads(path.read_text(encoding="utf-8"))
        state["action_counts"]["enter"] += 1
        path.write_text(json.dumps(state), encoding="utf-8")
    elif tamper == "source_receipt":
        path = cohort_root / "source_pack_clean_verifier_receipt.json"
        receipt = json.loads(path.read_text(encoding="utf-8"))
        receipt["failures"] = ["forged"]
        path.write_text(json.dumps(receipt), encoding="utf-8")
    else:
        path = cohort_root / "source_pack_manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["source_manifest"].append({"forged": True})
        path.write_text(json.dumps(manifest), encoding="utf-8")
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs={cohort_id: specification}
    ) == (0, 0)
    target.close()


def test_all68_generated_timestamp_refresh_is_idempotent_but_semantic_drift_rejects(
    tmp_path: Path,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    parent = "sequential_replay_source_pack_v1.fixture"
    replay_root = tmp_path / "all68"
    placeholder = replay_root / "cohorts" / "placeholder"
    cohort_id, specification, _ = write_all68_batch_fixture(placeholder, parent)
    insert_parent_fixture(
        target, parent, "historical_replay_source_infrastructure",
        definition_sha256=specification.pop("fixture_parent_definition_sha256"),
    )
    cohort_root = replay_root / "cohorts" / cohort_id
    placeholder.rename(cohort_root)
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs={cohort_id: specification}
    ) == (1, 1)
    state_path = cohort_root / "state.json"
    receipt_path = cohort_root / "verifier_receipt.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    state["generated_utc"] = "2026-09-01T00:00:00+00:00"
    receipt["generated_utc"] = "2026-09-01T00:01:00+00:00"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs={cohort_id: specification}
    ) == (0, 0)
    receipt["extra_semantic_claim"] = "forged"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, replay_root, cohort_specs={cohort_id: specification}
    ) == (0, 0)
    target.close()


def test_all68_mistake_curricula_preserve_parent_lineage_and_strict_current(
    tmp_path: Path,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    pack_id = "sequential_replay_source_pack_v1.fixture"
    all68_root = tmp_path / "sequential_all68_portfolio_batch_replay_v1"
    placeholder = all68_root / "cohorts" / "placeholder"
    replay_id, replay_spec, _ = write_all68_batch_fixture(placeholder, pack_id)
    insert_parent_fixture(
        target, pack_id, "historical_replay_source_infrastructure",
        definition_sha256=replay_spec.pop("fixture_parent_definition_sha256"),
    )
    replay_final = placeholder.parent / replay_id
    placeholder.rename(replay_final)
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, all68_root, cohort_specs={replay_id: replay_spec}
    ) == (1, 1)
    mistake_root = tmp_path / "sequential_all68_mistake_curriculum_v1"
    old_id, old_spec, _, _ = write_all68_mistake_fixture(
        mistake_root, replay_final, strict_raw=False,
        require_current=False, config_tag="old",
    )
    final_id, final_spec, _, _ = write_all68_mistake_fixture(
        mistake_root, replay_final, strict_raw=True,
        require_current=True, config_tag="final",
    )
    specs = {old_id: old_spec, final_id: final_spec}

    assert genealogy.import_sequential_all68_mistake_curriculum(
        target, mistake_root, cohort_specs=specs
    ) == (2, 2)
    assert genealogy.import_sequential_all68_mistake_curriculum(
        target, mistake_root, cohort_specs=specs
    ) == (0, 0)
    rows = target.execute(
        "SELECT hypothesis_id,parent_hypothesis_id,experiment_kind FROM experiments "
        "WHERE hypothesis_id IN (?,?) ORDER BY hypothesis_id", (old_id, final_id),
    ).fetchall()
    assert {tuple(row) for row in rows} == {
        (old_id, replay_id, "historical_all68_mistake_curriculum"),
        (final_id, replay_id, "historical_all68_mistake_curriculum"),
    }
    results = dict(target.execute(
        "SELECT hypothesis_id,result FROM experiment_observations "
        "WHERE hypothesis_id IN (?,?)", (old_id, final_id),
    ))
    assert results == {old_id: "fixture-old", final_id: "fixture-final"}
    target.close()


@pytest.mark.parametrize(
    "tamper",
    (
        "raw_source_state", "raw_source_verifier", "dataset",
        "source_pack_receipt", "current_alias", "curriculum_reference",
        "receipt_raw_binding",
    ),
)
def test_all68_mistake_curriculum_fails_closed_on_parent_or_report_drift(
    tmp_path: Path, tamper: str,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    pack_id = "sequential_replay_source_pack_v1.fixture"
    all68_root = tmp_path / "sequential_all68_portfolio_batch_replay_v1"
    placeholder = all68_root / "cohorts" / "placeholder"
    replay_id, replay_spec, _ = write_all68_batch_fixture(placeholder, pack_id)
    insert_parent_fixture(
        target, pack_id, "historical_replay_source_infrastructure",
        definition_sha256=replay_spec.pop("fixture_parent_definition_sha256"),
    )
    replay_final = placeholder.parent / replay_id
    placeholder.rename(replay_final)
    assert genealogy.import_sequential_all68_portfolio_batch_replays(
        target, all68_root, cohort_specs={replay_id: replay_spec}
    ) == (1, 1)
    mistake_root = tmp_path / "sequential_all68_mistake_curriculum_v1"
    cohort_id, specification, report_path, receipt_path = (
        write_all68_mistake_fixture(
            mistake_root, replay_final, strict_raw=True,
            require_current=True, config_tag="final",
        )
    )
    current_report = mistake_root / "sequential_all68_mistake_curriculum_v1.json"
    current_receipt = (
        mistake_root / "sequential_all68_mistake_curriculum_verifier_v1.json"
    )
    if tamper == "raw_source_state":
        path = replay_final / "state.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["generated_utc"] = "2026-09-01T00:00:00+00:00"
        path.write_text(json.dumps(value), encoding="utf-8")
    elif tamper == "raw_source_verifier":
        path = replay_final / "verifier_receipt.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["generated_utc"] = "2026-09-01T00:00:00+00:00"
        path.write_text(json.dumps(value), encoding="utf-8")
    elif tamper == "dataset":
        path = replay_final / "feedback.jsonl.gz"
        payload = bytearray(path.read_bytes())
        payload[-1] ^= 1
        path.write_bytes(bytes(payload))
    elif tamper == "source_pack_receipt":
        path = replay_final / "source_pack_clean_verifier_receipt.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["verified"] = False
        path.write_text(json.dumps(value), encoding="utf-8")
    elif tamper == "current_alias":
        current_report.write_bytes(current_report.read_bytes() + b"\n")
    elif tamper == "receipt_raw_binding":
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["checks"]["source_state_sha256"] = "f" * 64
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        current_receipt.write_bytes(receipt_path.read_bytes())
    else:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        row = report["curriculum_rows"][0]
        row["source_clock_row_sha256"] = "f" * 64
        row["row_sha256"] = genealogy._variadic_hash({
            key: value for key, value in row.items() if key != "row_sha256"
        })
        semantic = dict(report)
        semantic.pop("report_id")
        semantic.pop("report_sha256")
        report_sha = genealogy._variadic_hash(semantic)
        report["report_id"] = "a68mistakecurriculum_" + report_sha[:28]
        report["report_sha256"] = report_sha
        report_path.write_text(json.dumps(report), encoding="utf-8")
        current_report.write_bytes(report_path.read_bytes())
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["report_id"] = report["report_id"]
        receipt["report_content_sha256"] = report_sha
        receipt["report_file_sha256"] = hashlib.sha256(
            report_path.read_bytes()
        ).hexdigest()
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        current_receipt.write_bytes(receipt_path.read_bytes())
        specification["report_id"] = report["report_id"]
        specification["report_content_sha256"] = report_sha
        specification["report_file_sha256"] = hashlib.sha256(
            report_path.read_bytes()
        ).hexdigest()
    assert genealogy.import_sequential_all68_mistake_curriculum(
        target, mistake_root, cohort_specs={cohort_id: specification}
    ) == (0, 0)
    assert target.execute(
        "SELECT COUNT(*) FROM experiments WHERE hypothesis_id=?", (cohort_id,),
    ).fetchone()[0] == 0
    target.close()


@pytest.mark.parametrize(
    "tamper", ("receipt_roots", "report", "database_row", "database_seal"),
)
def test_mistake_genealogy_requires_verified_report_and_source_database_seal(
    tmp_path: Path, tamper: str,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    parent = "sequential_portfolio_replay_v1.parent"
    insert_parent_fixture(target, parent, "historical_sequential_portfolio_training")
    cohort_id, _ = write_mistake_fixture(tmp_path / "replay", parent)
    mistake_root = tmp_path / ".mistake_v1"
    state = json.loads(
        (tmp_path / "replay" / "sequential_portfolio_replay_v1.json").read_text(
            encoding="utf-8"
        )
    )
    if tamper == "receipt_roots":
        paths = [
            mistake_root / "sequential_portfolio_mistake_curriculum_verifier_v1.json",
            mistake_root / "cohorts" / cohort_id /
            "sequential_portfolio_mistake_curriculum_verifier_v1.json",
        ]
        for path in paths:
            receipt = json.loads(path.read_text(encoding="utf-8"))
            receipt["checks"]["source_roots"] = {}
            path.write_text(json.dumps(receipt), encoding="utf-8")
    elif tamper == "report":
        path = mistake_root / "sequential_portfolio_mistake_curriculum_v1.json"
        report = json.loads(path.read_text(encoding="utf-8"))
        report["summary"]["raw_observation_count"] += 1
        path.write_text(json.dumps(report), encoding="utf-8")
    elif tamper == "database_row":
        connection = sqlite3.connect(state["database"])
        connection.execute("UPDATE spr_feedback SET value='forged'")
        connection.commit(); connection.close()
    else:
        connection = sqlite3.connect(state["database"])
        connection.execute("UPDATE spr_session_seals SET seal_sha256=?", ("f" * 64,))
        connection.commit(); connection.close()
    assert genealogy.import_sequential_portfolio_mistake_curriculum(
        target, tmp_path / "replay"
    ) == (0, 0)
    assert target.execute(
        "SELECT COUNT(*) FROM experiments WHERE hypothesis_id=?", (cohort_id,)
    ).fetchone()[0] == 0
    target.close()


def test_curriculum_receipt_timestamp_refresh_is_idempotent_but_semantic_drift_conflicts(
    tmp_path: Path,
) -> None:
    target = connect(tmp_path / "genealogy.sqlite")
    parent = "sequential_portfolio_replay_v1.parent"
    insert_parent_fixture(target, parent, "historical_sequential_portfolio_training")
    _, _, receipt_path = write_curriculum_fixture(tmp_path / "curriculum", parent)
    assert genealogy.import_sequential_portfolio_curriculum(
        target, tmp_path / "curriculum"
    ) == (1, 1)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["generated_utc"] = "2026-08-31T00:00:00+00:00"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assert genealogy.import_sequential_portfolio_curriculum(
        target, tmp_path / "curriculum"
    ) == (0, 0)
    receipt["extra_semantic_claim"] = "forged"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="immutable registration conflict"):
        genealogy.import_sequential_portfolio_curriculum(
            target, tmp_path / "curriculum"
        )
    target.close()


def test_counterfactual_sim_gym_lineage_is_registered_as_historical_only(
    tmp_path: Path,
):
    target = connect(tmp_path / "genealogy.sqlite")
    source = tmp_path / "counterfactual_sim_gym_v1.sqlite"
    sim = sqlite3.connect(source)
    sim.executescript(
        """
        CREATE TABLE sim_cohorts(
          cohort_id TEXT PRIMARY KEY,experiment_key TEXT NOT NULL,
          created_utc TEXT NOT NULL,material_contract_sha256 TEXT NOT NULL,
          config_sha256 TEXT NOT NULL,runner_sha256 TEXT NOT NULL,
          core_sha256 TEXT NOT NULL,material_contract_json TEXT NOT NULL
        );
        CREATE TABLE sim_cohort_transitions(
          transition_id TEXT PRIMARY KEY,experiment_key TEXT NOT NULL,
          previous_cohort_id TEXT,next_cohort_id TEXT NOT NULL,
          observed_utc TEXT NOT NULL,reason TEXT NOT NULL
        );
        """
    )
    base_contract = {
        "contract_id": "sim_contract_v1",
        "window": {"start_epoch": 1, "end_epoch": 2},
        "source_manifest": [{"instrument": "EUR_USD", "sha256": "a" * 64}],
        "effective_config": {
            "research_only": True,
            "execution_eligible": False,
            "supported_decision": "no_trade",
            "source": {
                "kind": "oanda_completed_m1_bid_ask_csv",
                "knowledge_time": "completed only",
                "entry_quote": "exact next quote",
                "path_quote": "executable sides",
            },
            "sampling": {"cadence_min": 60},
            "historical_partitions": {
                "labels": ["early", "late"],
                "purge_overlapping_outcomes": True,
            },
            "signal_rules": [{"id": "momentum_5m", "kind": "momentum"}],
            "execution_grid": {"horizon_min": [5]},
            "comparators": {"no_trade": {"enabled": True}},
            "costs": {"round_trip_slippage_pips": 0.25},
        },
    }
    second_contract = dict(base_contract)
    second_contract["contract_id"] = "sim_contract_v2"
    sim.execute(
        "INSERT INTO sim_cohorts VALUES (?,?,?,?,?,?,?,?)",
        (
            "sim.cohort.1", "sim", "2026-08-29T10:00:00+00:00", "1" * 64,
            "2" * 64, "3" * 64, "4" * 64,
            json.dumps(base_contract, sort_keys=True),
        ),
    )
    sim.execute(
        "INSERT INTO sim_cohorts VALUES (?,?,?,?,?,?,?,?)",
        (
            "sim.cohort.2", "sim", "2026-08-29T11:00:00+00:00", "5" * 64,
            "2" * 64, "3" * 64, "6" * 64,
            json.dumps(second_contract, sort_keys=True),
        ),
    )
    sim.execute(
        "INSERT INTO sim_cohort_transitions VALUES (?,?,?,?,?,?)",
        (
            "transition.1", "sim", None, "sim.cohort.1",
            "2026-08-29T10:00:00+00:00", "initial_registration",
        ),
    )
    sim.execute(
        "INSERT INTO sim_cohort_transitions VALUES (?,?,?,?,?,?)",
        (
            "transition.2", "sim", "sim.cohort.1", "sim.cohort.2",
            "2026-08-29T11:00:00+00:00", "material_contract_change",
        ),
    )
    sim.commit()
    sim.close()

    assert import_counterfactual_sim_gym(target, source) == (2, 2)
    assert import_counterfactual_sim_gym(target, source) == (0, 0)
    experiments = target.execute(
        "SELECT hypothesis_id,parent_hypothesis_id,experiment_kind "
        "FROM experiments ORDER BY created_at"
    ).fetchall()
    assert [tuple(row) for row in experiments] == [
        ("sim.cohort.1", None, "historical_counterfactual_sim"),
        ("sim.cohort.2", "sim.cohort.1", "historical_counterfactual_sim"),
    ]
    results = target.execute(
        "SELECT hypothesis_id,result FROM experiment_observations "
        "ORDER BY observed_at"
    ).fetchall()
    assert [tuple(row) for row in results] == [
        ("sim.cohort.1", "engineering_superseded_material_contract"),
        ("sim.cohort.2", "historical_diagnostic_current"),
    ]
    target.close()


def make_after_cost_v4_fixture(root: Path) -> tuple[Path, dict, dict]:
    """Build a complete miniature copy of the v4 byte-attested closure."""
    paths = genealogy.AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS
    contract_id = "currency_state_after_cost_counterfactual_v4_test"
    cohort_id = "currency_state_after_cost_counterfactual_cohort_v4_test"
    manifest_id = "currency_state_after_cost_manifest_v4_test"
    for label, relative_path in paths.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if label != "counterfactual_v4_config":
            path.write_bytes(f"fixture:{label}".encode("utf-8"))
    config_path = root / paths["counterfactual_v4_config"]
    config_path.write_text(json.dumps({
        "contract_id": contract_id,
        "counterfactual_cohort_id": cohort_id,
        "required_frozen_manifest_id": manifest_id,
        "required_manifest_artifacts": list(paths),
    }, sort_keys=True), encoding="utf-8")
    artifacts = {
        label: {
            "relative_path": relative_path,
            "sha256": hashlib.sha256((root / relative_path).read_bytes()).hexdigest(),
        }
        for label, relative_path in paths.items()
    }
    manifest = {
        "manifest_id": manifest_id,
        "contract_id": contract_id,
        "cohort_id": cohort_id,
        "artifacts": artifacts,
    }
    manifest_path = root / genealogy.AFTER_COST_V4_MANIFEST_RELATIVE_PATH
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    normalized_manifest = {
        "manifest_id": manifest_id,
        "contract_id": contract_id,
        "cohort_id": cohort_id,
        "artifacts": {label: artifacts[label] for label in sorted(artifacts)},
    }
    normalized_manifest["manifest_sha256"] = genealogy.stable_hash(
        normalized_manifest
    )
    expected_anchor = {
        "anchor_schema": "currency_state_after_cost_v4_external_trust_anchor_v1",
        "manifest_relative_path": genealogy.AFTER_COST_V4_MANIFEST_RELATIVE_PATH,
        "manifest_file_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "manifest_id": manifest_id,
        "manifest_sha256": normalized_manifest["manifest_sha256"],
        "artifacts": normalized_manifest["artifacts"],
        "counterfactual_v4_module_sha256": artifacts["counterfactual_v4_module"]["sha256"],
        "counterfactual_v4_config_sha256": artifacts["counterfactual_v4_config"]["sha256"],
        "counterfactual_v4_cli_sha256": artifacts["counterfactual_v4_cli"]["sha256"],
    }
    payload = {
        "generated_utc": "2026-08-17T06:00:00+00:00",
        "snapshot_id": "after_cost_v4_snapshot_test",
        "snapshot_schema": "currency_state_after_cost_counterfactual_snapshot_v4",
        "counterfactual_contract_id": contract_id,
        "counterfactual_cohort_id": cohort_id,
        "supersedes_contract_id": "v3_contract",
        "supersedes_cohort_id": "v3_cohort",
        "response_arm_contract_id": "response_contract",
        "response_snapshot_id": "response_snapshot",
        "response_snapshot_sha256": "b" * 64,
        "decision_cutoff_utc": "2026-08-17T05:59:00+00:00",
        "horizons_sec": [60, 300],
        "arm_ids": ["official_context_only", "no_trade"],
        "summary": {
            "submitted_envelope_count": 0,
            "submitted_record_count": 0,
            "submitted_canonical_record_count": 0,
            "submitted_account_state_record_count": 0,
            "admissible_canonical_record_count": 0,
            "admissible_account_state_record_count": 0,
            "admissible_quote_count": 0,
            "admissible_verifier_count": 0,
            "venue_quote_input_count": 0,
            "verifier_input_count": 0,
            "economics_input_count": 0,
            "economics_admissible_count": 0,
            "ranked_count": 0,
            "selectable_count": 0,
            "hold_switch_count": 0,
            "row_count": 4,
        },
        "allocations": {},
        "hold_switch_counterfactuals": [],
        "input_rejections": [],
        "frozen_manifest": normalized_manifest,
        "expected_external_trust_anchor": expected_anchor,
        "registration_status": "engineering_blocked_zero_evidence",
        "producer_integration_state": "producer_integration_missing",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    state_path = root / "state" / "after_cost_v4.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    return state_path, payload, manifest


def patch_after_cost_v4_frozen_hashes(
    monkeypatch: pytest.MonkeyPatch, payload: dict,
) -> None:
    anchor = payload["expected_external_trust_anchor"]
    monkeypatch.setattr(
        genealogy, "AFTER_COST_V4_FROZEN_MANIFEST_FILE_SHA256",
        anchor["manifest_file_sha256"],
    )
    monkeypatch.setattr(
        genealogy, "AFTER_COST_V4_FROZEN_MANIFEST_SEMANTIC_SHA256",
        anchor["manifest_sha256"],
    )
    monkeypatch.setattr(
        genealogy, "AFTER_COST_V4_FROZEN_MODULE_SHA256",
        anchor["counterfactual_v4_module_sha256"],
    )
    monkeypatch.setattr(
        genealogy, "AFTER_COST_V4_FROZEN_CONFIG_SHA256",
        anchor["counterfactual_v4_config_sha256"],
    )
    monkeypatch.setattr(
        genealogy, "AFTER_COST_V4_FROZEN_CLI_SHA256",
        anchor["counterfactual_v4_cli_sha256"],
    )


def make_lifecycle_fixture(path: Path, rows: int) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE hypotheses (
          hypothesis_id TEXT PRIMARY KEY,evidence_contract_id TEXT NOT NULL,
          cohort_id TEXT,cell_id TEXT NOT NULL,family TEXT NOT NULL,
          instrument TEXT NOT NULL,horizon_sec INTEGER NOT NULL,
          session_bucket TEXT NOT NULL,liquidity_bucket TEXT NOT NULL,
          definition_sha256 TEXT NOT NULL,definition_json TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL);
        CREATE TABLE lifecycle_events (
          event_id TEXT PRIMARY KEY,hypothesis_id TEXT NOT NULL,
          previous_state TEXT,next_state TEXT NOT NULL,observed_utc TEXT NOT NULL,
          source_run_id TEXT NOT NULL,evidence_json TEXT NOT NULL);
        CREATE TABLE futility_retirements (
          hypothesis_id TEXT PRIMARY KEY,retired_utc TEXT NOT NULL,
          source_run_id TEXT NOT NULL,boundary_method TEXT NOT NULL,
          minimum_economic_edge_pips REAL NOT NULL,upper_bound_pips REAL NOT NULL,
          evidence_json TEXT NOT NULL);
        """
    )
    for index in range(rows):
        hypothesis_id = f"cell-{index}"
        definition_sha = f"sha-{index}"
        connection.execute(
            "INSERT INTO hypotheses VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (hypothesis_id, "contract", "cohort", f"cell-id-{index}", "family",
             "EUR_USD", 900, "overlap", "liquid", definition_sha, "{}",
             "2026-08-14T00:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO lifecycle_events VALUES (?,?,?,?,?,?,?)",
            (f"event-{index}", hypothesis_id, None, "continue_collecting",
             "2026-08-14T00:00:00+00:00", "run", '{"n":1}'),
        )
    connection.commit()
    connection.close()


def test_lifecycle_genealogy_incrementally_skips_unchanged_rows(tmp_path: Path) -> None:
    source = tmp_path / "evidence_lifecycle_v1.sqlite"
    make_lifecycle_fixture(source, 25)
    target = connect(tmp_path / "genealogy.sqlite")

    assert import_lifecycle(target, source, imported_at="unused") == (25, 25)
    target.commit()
    assert import_lifecycle(target, source, imported_at="unused") == (0, 0)

    source_connection = sqlite3.connect(source)
    source_connection.execute(
        "INSERT INTO hypotheses VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        ("cell-25", "contract", "cohort", "cell-id-25", "family", "EUR_USD",
         900, "overlap", "liquid", "sha-25", "{}", "2026-08-14T01:00:00+00:00"),
    )
    source_connection.execute(
        "INSERT INTO lifecycle_events VALUES (?,?,?,?,?,?,?)",
        ("event-25", "cell-25", None, "continue_collecting",
         "2026-08-14T01:00:00+00:00", "run", '{"n":1}'),
    )
    source_connection.commit()
    source_connection.close()

    assert import_lifecycle(target, source, imported_at="unused") == (1, 1)
    target.close()


def test_genealogy_connection_has_bounded_busy_timeout(tmp_path: Path) -> None:
    connection = connect(tmp_path / "genealogy.sqlite")
    try:
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
    finally:
        connection.close()


def test_internal_discovery_and_adapter_retirement_are_explicit(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({
        "research_id": "cost_clearance_test", "generated_utc": "2026-08-09T00:00:00+00:00",
        "evidence_class": "archive_discovery", "config_sha256": "a", "source_manifest_sha256": "b",
        "source_code_sha256": "c", "results": [{"proof_eligible": False}],
    }))
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_internal_discovery_artifact(connection, artifact) == (1, 1)
    assert import_internal_discovery_artifact(connection, artifact) == (0, 0)
    assert import_zero_output_adapter_retirement(connection) == (1, 1)
    result = connection.execute(
        "SELECT result,retirement_reason FROM experiment_observations WHERE hypothesis_id LIKE 'intrasecond_ridge.%'"
    ).fetchone()
    assert tuple(result) == ("engineering_retired_zero_valid_outputs", "zero valid outputs and no active structural-entry role")
    connection.close()


def test_internal_discovery_can_use_a_registry_assigned_id(tmp_path: Path) -> None:
    artifact = tmp_path / "legacy_artifact.json"
    artifact.write_text(json.dumps({
        "generated_utc": "2026-08-16T21:00:00+00:00",
        "evidence_class": "availability_counterfactual_discovery",
        "matched_control_comparison": [{"horizon_sec": 60}],
        "technical_confirmation_by_horizon": {"300": {"proof_eligible": False}},
    }))
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_internal_discovery_artifact(
        connection,
        artifact,
        research_id_override="direct_source_test",
    ) == (1, 1)
    hypothesis = connection.execute(
        "SELECT hypothesis_id FROM experiments WHERE idea_origin='direct_source_test'"
    ).fetchone()
    assert hypothesis is not None
    connection.close()


def test_macro_point_in_time_null_is_separate_from_prospective_cohorts(tmp_path: Path) -> None:
    artifact = tmp_path / "macro_validation.json"
    artifact.write_text(json.dumps({
        "generated_utc": "2026-08-16T20:30:00+00:00",
        "conclusion": "simple_relative_macro_generation_not_supported_by_initial_release_replay",
        "prospective_cohorts_remain_unchanged": True,
        "candidates": [{
            "candidate": {
                "candidate_id": "short_rate_change_h24_liquid_threshold_025",
                "signal_rule": "short_rate_change_differential",
                "threshold": 0.25,
                "horizon_hours": 24,
                "liquidity_bucket": "liquid",
            },
            "current_view_counterfactual": {"raw_n": 42, "average_net_pips": 23.852},
            "initial_release_point_in_time": {"raw_n": 55, "average_net_pips": -1.829},
            "point_in_time_technical_aligned": {"raw_n": 20},
            "robustness_state": "failed_negative_point_estimate",
        }],
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_macro_point_in_time_validation(connection, artifact) == (1, 1)
    assert import_macro_point_in_time_validation(connection, artifact) == (0, 0)
    row = connection.execute(
        "SELECT experiment_kind,result,retirement_reason FROM experiments "
        "JOIN experiment_observations USING(hypothesis_id)"
    ).fetchone()
    assert tuple(row) == (
        "historical_point_in_time_robustness",
        "failed_negative_point_estimate",
        "historical_current_view_candidate_failed_initial_release_replay",
    )
    assert connection.execute(
        "SELECT COUNT(*) FROM experiments WHERE experiment_kind='external_source_prospective_collection'"
    ).fetchone()[0] == 0
    connection.close()


def definition() -> dict:
    return {
        "hypothesis_id": "h1", "parent_hypothesis_id": None,
        "experiment_kind": "proof_model", "research_generation": "g1",
        "idea_origin": "ridge", "pre_registered": 1,
        "data_sources_json": "[]", "feature_contract_json": "{}",
        "label_contract_json": "{}", "model_contract_json": "{}",
        "cost_contract_json": "{}", "allocator_contract_json": "{}",
        "training_period_json": "{}", "selection_period_json": "{}",
        "confirmation_period_json": "{}", "all_parameters_tried_json": "{}",
        "selection_rule": "frozen", "holdouts_touched_json": "[]",
        "source_code_hash": "code", "data_snapshot_hash": "data",
        "definition_sha256": "definition", "created_at": "2026-08-08T00:00:00+00:00",
        "definition_json": "{}",
    }


def test_genealogy_is_append_only_and_results_are_observations(tmp_path: Path) -> None:
    connection = connect(tmp_path / "genealogy.sqlite")
    assert insert_experiment(connection, definition())
    assert not insert_experiment(connection, definition())
    assert observe(
        connection, hypothesis_id="h1", observed_at="2026-08-08T01:00:00+00:00",
        source_system="test", result="continue_collecting", evidence={"n": 1},
    )
    assert observe(
        connection, hypothesis_id="h1", observed_at="2026-08-08T02:00:00+00:00",
        source_system="test", result="futility_rejected", evidence={"n": 2},
        retirement_reason="upper_bound_below_edge", retired_at="2026-08-08T02:00:00+00:00",
    )
    connection.commit()
    assert connection.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == 1
    assert connection.execute("SELECT COUNT(*) FROM experiment_observations").fetchone()[0] == 2
    try:
        connection.execute("UPDATE experiments SET idea_origin='changed'")
    except sqlite3.IntegrityError as exc:
        assert "append_only" in str(exc)
    else:
        raise AssertionError("experiment mutation was not blocked")
    connection.close()


def test_external_discovery_rejections_are_permanent_genealogy_rows(tmp_path: Path) -> None:
    report = tmp_path / "discovery.json"
    report.write_text(json.dumps({
        "generated_utc": "2026-08-08T22:00:00+00:00",
        "availability_contract": "next day",
        "evidence_class": "historical_discovery",
        "date_range": {"start": "2024-01-01", "end": "2026-08-04"},
        "source_hashes": {"EUR": "a"},
        "candle_hashes": {"EUR_USD": "b"},
        "supported_action": "reject_tested_rules",
        "limitations": ["not prospective"],
        "summaries": [
            {"split": "discovery", "rule": "simple", "horizon_trading_days": 1, "mean_net_bps": -1.0, "discovery_candidate": False},
            {"split": "holdout", "rule": "simple", "horizon_trading_days": 1, "mean_net_bps": -2.0, "discovery_candidate": False},
        ],
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    definitions, observations = import_external_discovery_report(
        connection, report, source_family="test_source"
    )
    connection.commit()
    assert definitions == observations == 1
    assert connection.execute(
        "SELECT result FROM experiment_observations"
    ).fetchone()[0] == "historical_discovery_rejected"
    again = import_external_discovery_report(
        connection, report, source_family="test_source"
    )
    assert again == (0, 0)
    connection.close()


def test_gdelt_direction_and_magnitude_remain_distinct_genealogy_rows(tmp_path: Path) -> None:
    report = tmp_path / "gdelt.json"
    report.write_text(json.dumps({
        "generated_utc": "2026-08-08T22:30:00+00:00",
        "source_ids": ["gdelt_fx"],
        "candle_hashes": {"EUR_USD": "candle"},
        "market_days": 7,
        "mapping_quality": {"raw_articles": 100},
        "limitations": ["short history"],
        "arm_summaries": [{
            "arm": "news_only", "horizon_minutes": 60,
            "mean_net_bps": -2.0, "confirmation_eligible": False,
        }],
        "attention_magnitude_summaries": [{
            "horizon_minutes": 60, "mean_absolute_move_bps": 7.0,
            "confirmation_eligible": False,
        }],
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_gdelt_mapping_report(connection, report) == (2, 2)
    results = {
        row[0] for row in connection.execute(
            "SELECT result FROM experiment_observations"
        )
    }
    assert results == {
        "historical_discovery_rejected",
        "historical_magnitude_discovery_requires_prospective_cohort",
    }
    assert import_gdelt_mapping_report(connection, report) == (0, 0)
    connection.close()


def test_prospective_source_cohort_is_registered_as_collecting(tmp_path: Path) -> None:
    state = tmp_path / "prospective.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-08T22:46:00+00:00",
        "status": "market_or_quote_stale",
        "latest_quote_utc": "2026-08-07T20:59:00+00:00",
        "totals": {"forecasts": 0, "matured": 0},
        "cycle": {"new_forecasts": 0, "new_outcomes": 0},
        "supported_execution_decision": "no_trade",
        "cohort": {
            "cohort_id": "gdelt_attention_magnitude_v1.discovery.test",
            "config_sha256": "config",
            "collector_sha256": "collector",
            "direction_policy": "abstain",
            "forecasts_written_before_outcomes": True,
            "late_backfill_refused": True,
        },
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_prospective_source_state(connection, state) == (1, 1)
    row = connection.execute(
        "SELECT experiment_kind FROM experiments"
    ).fetchone()
    assert row[0] == "external_source_prospective_collection"
    assert connection.execute(
        "SELECT result FROM experiment_observations"
    ).fetchone()[0] == "continue_collecting"
    connection.close()


def test_treasury_source_collection_is_not_a_directional_hypothesis(tmp_path: Path) -> None:
    state = tmp_path / "treasury.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-08T23:10:00+00:00",
        "status": "ok", "database_integrity": "ok",
        "totals": {"bootstrap_rows": 151, "prospective_eligible_rows": 0},
        "cycle": {"inserted_rows": 151},
        "cohort": {
            "cohort_id": "treasury.discovery.test", "config_sha256": "config",
            "collector_sha256": "collector", "source": "treasury",
            "first_seen_contract": "local_retrieval", "direction_policy": "abstain",
        },
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_treasury_source_state(connection, state) == (1, 1)
    row = connection.execute(
        "SELECT idea_origin,label_contract_json FROM experiments"
    ).fetchone()
    assert row[0] == "us_treasury_daily_yield_curve"
    assert json.loads(row[1])["target"] == "source_integrity_only"
    assert connection.execute(
        "SELECT result FROM experiment_observations"
    ).fetchone()[0] == "continue_collecting"
    connection.close()


def test_alfred_missing_credential_is_registered_not_fabricated(tmp_path: Path) -> None:
    state = tmp_path / "alfred.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-08T23:20:00+00:00",
        "status": "blocked_missing_fred_api_key",
        "configured_series": 9,
        "credential_environment": "FRED_API_KEY",
        "credential_present": False,
        "cohort": {
            "cohort_id": "alfred.discovery.test",
            "config_sha256": "config",
            "collector_sha256": "collector",
            "source_contract_id": "alfred_test",
            "first_seen_contract": "local_retrieval",
            "direction_policy": "abstain",
        },
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_alfred_source_state(connection, state) == (1, 1)
    row = connection.execute(
        "SELECT idea_origin FROM experiments"
    ).fetchone()
    assert row[0] == "fred_alfred_point_in_time_macro_vintages"
    result = connection.execute(
        "SELECT result,evidence_json FROM experiment_observations"
    ).fetchone()
    assert result[0] == "blocked_external_credential"
    assert json.loads(result[1])["credential_present"] is False
    connection.close()


def test_shadow_runtime_state_is_registered_research_only(tmp_path: Path) -> None:
    state = tmp_path / "runtime.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-09T00:00:00+00:00",
        "status": "market_or_source_stale",
        "cohort_id": "runtime.collector.test",
        "model_cohort_id": "model.test",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "totals": {"forecasts": 0, "matured": 0},
        "collection_cohort": {"collector_sha256": "collector"},
        "contract": {"exact_horizon": True},
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_shadow_runtime_state(
        connection, state, idea_origin="test_runtime",
        data_sources=["source"], label_target="after_cost",
        selection_rule="frozen",
    ) == (1, 1)
    row = connection.execute(
        "SELECT parent_hypothesis_id,experiment_kind FROM experiments"
    ).fetchone()
    assert row == ("model.test", "prospective_shadow_runtime")
    assert connection.execute(
        "SELECT result FROM experiment_observations"
    ).fetchone()[0] == "closed_market_collecting"
    connection.close()


def test_shadow_runtime_registers_material_child_as_separate_cohort(tmp_path: Path) -> None:
    state = tmp_path / "runtime.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-14T10:43:00+00:00",
        "status": "ok", "cohort_id": "runtime.base", "research_only": True,
        "policy": {"arms": ["base"]},
        "additional_shadow_cohorts": [{
            "cohort_id": "runtime.child.h15", "parent_cohort_id": "runtime.base",
            "created_at": "2026-08-14T10:43:00+00:00",
            "idea_origin": "reconfirmation", "data_sources": ["news", "quotes"],
            "feature_contract": {"trigger": "reconfirm"},
            "label_contract": {"horizon_minutes": 15},
            "selection_rule": "first transition", "execution_eligible": False,
        }],
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_shadow_runtime_state(
        connection, state, idea_origin="base", data_sources=["news"],
        label_target="after_cost", selection_rule="frozen",
    ) == (2, 2)
    rows = connection.execute(
        "SELECT hypothesis_id,parent_hypothesis_id FROM experiments ORDER BY hypothesis_id"
    ).fetchall()
    assert rows == [("runtime.base", None), ("runtime.child.h15", "runtime.base")]
    connection.close()


def test_shadow_runtime_frozen_child_parent_cannot_change_in_place(tmp_path: Path) -> None:
    state = tmp_path / "runtime.json"
    base = {
        "generated_utc": "2026-08-18T12:00:00+00:00",
        "status": "ok", "cohort_id": "runtime.base.v2", "research_only": True,
        "additional_shadow_cohorts": [{
            "cohort_id": "runtime.child.h15.v1",
            "parent_cohort_id": "runtime.base.v1",
            "created_at": "2026-08-18T12:00:00+00:00",
            "feature_contract": {"trigger": "official_release"},
            "label_contract": {"horizon_minutes": 15},
        }],
    }
    state.write_text(json.dumps(base), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_shadow_runtime_state(
        connection, state, idea_origin="base", data_sources=["official"],
        label_target="after_cost", selection_rule="frozen",
    ) == (2, 2)
    base["cohort_id"] = "runtime.base.v3"
    base["additional_shadow_cohorts"][0]["parent_cohort_id"] = "runtime.base.v2"
    state.write_text(json.dumps(base), encoding="utf-8")
    with pytest.raises(ValueError, match="hypothesis_id definition conflict"):
        import_shadow_runtime_state(
            connection, state, idea_origin="base", data_sources=["official"],
            label_target="after_cost", selection_rule="frozen",
        )
    connection.close()


def test_frozen_model_artifact_is_parent_genealogy_node(tmp_path: Path) -> None:
    folder = tmp_path / "artifact"
    folder.mkdir()
    (folder / "models.joblib").write_bytes(b"model")
    manifest = folder / "manifest.json"
    manifest.write_text(json.dumps({
        "cohort_id": "model.test", "research_only": True,
        "execution_eligible": False, "created_utc": "2026-08-08T00:00:00+00:00",
        "feature_contract": ["spread"], "config_sha256": "config",
        "source_code_sha256": "code", "prospective_start_utc": "2026-08-09T00:00:00+00:00",
        "source_instrument_count": 68,
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_model_artifact_manifest(
        connection, manifest, idea_origin="test_model"
    ) == (1, 1)
    row = connection.execute(
        "SELECT experiment_kind,result FROM experiments JOIN experiment_observations USING(hypothesis_id)"
    ).fetchone()
    assert row == ("frozen_model_artifact", "frozen_awaiting_prospective_evidence")
    connection.close()


def test_currency_state_after_cost_is_one_frozen_policy_hypothesis(tmp_path: Path) -> None:
    state = tmp_path / "after_cost.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-17T04:00:00+00:00",
        "snapshot_id": "after_cost_snapshot",
        "counterfactual_contract_id": "after_cost_contract",
        "counterfactual_contract_sha256": "a" * 64,
        "counterfactual_cohort_id": "after_cost_cohort",
        "response_arm_contract_id": "response_contract",
        "response_snapshot_id": "response_snapshot",
        "response_snapshot_sha256": "b" * 64,
        "currency_state_snapshot_id": "state_snapshot",
        "decision_cutoff_utc": "2026-08-17T03:40:00+00:00",
        "horizons_sec": [60, 300],
        "arm_ids": ["official_context_only", "no_trade"],
        "summary": {"economics_admissible_count": 0, "row_count": 4},
        "input_rejections": [],
        "artifact_fingerprints": {
            "counterfactual_module_sha256": "c" * 64,
            "counterfactual_config_file_sha256": "d" * 64,
        },
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_currency_state_after_cost_state(connection, state) == (1, 1)
    row = connection.execute(
        "SELECT hypothesis_id,experiment_kind FROM experiments"
    ).fetchone()
    assert row == ("after_cost_cohort", "frozen_policy_level_counterfactual")
    observation = connection.execute(
        "SELECT result,evidence_json FROM experiment_observations"
    ).fetchone()
    assert observation[0] == "frozen_awaiting_admissible_inputs"
    assert json.loads(observation[1])["summary"]["row_count"] == 4
    connection.close()


def test_currency_state_after_cost_v2_uses_manifest_and_parent_lineage(tmp_path: Path) -> None:
    state = tmp_path / "after_cost_v2.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-17T04:14:21+00:00",
        "snapshot_id": "after_cost_v2_snapshot",
        "snapshot_schema": "currency_state_after_cost_counterfactual_snapshot_v2",
        "counterfactual_contract_id": "after_cost_v2_contract",
        "counterfactual_cohort_id": "after_cost_v2_cohort",
        "supersedes_contract_id": "after_cost_v1_contract",
        "supersedes_cohort_id": "after_cost_v1_cohort",
        "response_arm_contract_id": "response_contract",
        "response_snapshot_id": "response_snapshot",
        "response_snapshot_sha256": "b" * 64,
        "decision_cutoff_utc": "2026-08-17T03:40:00+00:00",
        "horizons_sec": [60, 300],
        "arm_ids": ["official_context_only", "no_trade"],
        "summary": {"economics_admissible_count": 0, "row_count": 4},
        "input_rejections": [],
        "frozen_manifest": {
            "manifest_id": "after_cost_v2_manifest",
            "contract_id": "after_cost_v2_contract",
            "cohort_id": "after_cost_v2_cohort",
            "manifest_sha256": "a" * 64,
            "artifacts": {
                "counterfactual_module": {"sha256": "c" * 64},
                "counterfactual_config": {"sha256": "d" * 64},
                "counterfactual_cli": {"sha256": "e" * 64},
                "independent_verifier_producer": {"sha256": "f" * 64},
            },
        },
        "quote_envelope": None,
        "verifier_envelope": None,
        "economics_envelope": None,
        "hold_switch_envelope": None,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_currency_state_after_cost_state(
        connection, state, registration_status="engineering_blocked"
    ) == (1, 1)
    row = connection.execute(
        "SELECT hypothesis_id,parent_hypothesis_id,experiment_kind,definition_json FROM experiments"
    ).fetchone()
    assert row[:2] == ("after_cost_v2_cohort", "after_cost_v1_cohort")
    assert row[2] == "engineering_policy_level_counterfactual"
    definition = json.loads(row[3])
    assert definition["frozen_manifest"]["manifest_id"] == "after_cost_v2_manifest"
    evidence = json.loads(connection.execute(
        "SELECT evidence_json FROM experiment_observations"
    ).fetchone()[0])
    assert evidence["frozen_manifest_sha256"] == "a" * 64
    assert connection.execute(
        "SELECT result FROM experiment_observations"
    ).fetchone()[0] == "engineering_blocked_zero_evidence"
    connection.close()


def test_currency_state_after_cost_v3_prefers_v3_manifest_labels(tmp_path: Path) -> None:
    state = tmp_path / "after_cost_v3.json"
    state.write_text(json.dumps({
        "generated_utc": "2026-08-17T05:00:00+00:00",
        "snapshot_id": "after_cost_v3_snapshot",
        "snapshot_schema": "currency_state_after_cost_counterfactual_snapshot_v3",
        "counterfactual_contract_id": "after_cost_v3_contract",
        "counterfactual_cohort_id": "after_cost_v3_cohort",
        "supersedes_contract_id": "after_cost_v2_contract",
        "supersedes_cohort_id": "after_cost_v2_cohort",
        "response_arm_contract_id": "response_contract",
        "response_snapshot_id": "response_snapshot",
        "response_snapshot_sha256": "b" * 64,
        "decision_cutoff_utc": "2026-08-17T04:59:00+00:00",
        "horizons_sec": [60, 300], "arm_ids": ["official_context_only", "no_trade"],
        "summary": {"economics_admissible_count": 0, "row_count": 4},
        "input_rejections": [],
        "frozen_manifest": {
            "manifest_id": "after_cost_v3_manifest",
            "contract_id": "after_cost_v3_contract",
            "cohort_id": "after_cost_v3_cohort",
            "manifest_sha256": "a" * 64,
            "artifacts": {
                "counterfactual_v3_module": {"sha256": "c" * 64},
                "counterfactual_v3_config": {"sha256": "d" * 64},
                "counterfactual_v3_cli": {"sha256": "e" * 64},
                "independent_verifier_producer": {"sha256": "f" * 64},
            },
        },
        "canonical_input_hashes": {"envelopes": {}, "records": {}},
        "quote_envelope": None, "verifier_envelope": None,
        "economics_envelope": None, "hold_switch_envelope": None,
        "research_only": True, "execution_eligible": False,
        "can_place_orders": False, "supported_execution_decision": "no_trade",
    }), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_currency_state_after_cost_state(connection, state) == (1, 1)
    row = connection.execute(
        "SELECT parent_hypothesis_id,source_code_hash,definition_json FROM experiments"
    ).fetchone()
    assert row[0] == "after_cost_v2_cohort"
    assert row[1] == "c" * 64
    assert json.loads(row[2])["frozen_manifest"]["manifest_id"] == "after_cost_v3_manifest"
    connection.close()


def test_currency_state_after_cost_v4_independently_attests_exact_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    state, payload, _ = make_after_cost_v4_fixture(root)
    monkeypatch.setattr(genealogy, "ROOT", root)
    patch_after_cost_v4_frozen_hashes(monkeypatch, payload)
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_currency_state_after_cost_state(
        connection, state, registration_status="engineering_blocked"
    ) == (1, 1)
    row = connection.execute(
        "SELECT source_code_hash,model_contract_json,definition_sha256,definition_json "
        "FROM experiments"
    ).fetchone()
    v4_sha = payload["expected_external_trust_anchor"][
        "counterfactual_v4_module_sha256"
    ]
    v3_sha = payload["frozen_manifest"]["artifacts"][
        "counterfactual_v3_module"
    ]["sha256"]
    assert v4_sha != v3_sha
    assert row[0] == v4_sha
    model_contract = json.loads(row[1])
    assert model_contract["module_sha256"] == v4_sha
    definition_contract = json.loads(row[3])
    assert definition_contract["v4_external_trust_anchor"] == payload[
        "expected_external_trust_anchor"
    ]
    assert row[2] == genealogy.stable_hash(definition_contract)
    result = connection.execute(
        "SELECT result FROM experiment_observations"
    ).fetchone()[0]
    assert result == "engineering_blocked_zero_evidence"
    connection.close()


@pytest.mark.parametrize(
    "tamper",
    ("file", "path", "hash", "anchor", "manifest_file_hash", "nonzero_evidence"),
)
def test_currency_state_after_cost_v4_tampering_hard_blocks_registration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper: str,
) -> None:
    root = tmp_path / "root"
    state, payload, manifest = make_after_cost_v4_fixture(root)
    monkeypatch.setattr(genealogy, "ROOT", root)
    patch_after_cost_v4_frozen_hashes(monkeypatch, payload)
    manifest_path = root / genealogy.AFTER_COST_V4_MANIFEST_RELATIVE_PATH
    if tamper == "file":
        module = root / genealogy.AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS[
            "counterfactual_v4_module"
        ]
        module.write_bytes(module.read_bytes() + b"tamper")
    elif tamper == "path":
        manifest["artifacts"]["counterfactual_v4_module"][
            "relative_path"
        ] = "src/forex_system/research/not_the_frozen_v4.py"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    elif tamper == "hash":
        manifest["artifacts"]["counterfactual_v4_module"]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    elif tamper == "anchor":
        payload["expected_external_trust_anchor"][
            "counterfactual_v4_module_sha256"
        ] = "0" * 64
        state.write_text(json.dumps(payload), encoding="utf-8")
    elif tamper == "manifest_file_hash":
        payload["expected_external_trust_anchor"]["manifest_file_sha256"] = "0" * 64
        state.write_text(json.dumps(payload), encoding="utf-8")
    elif tamper == "nonzero_evidence":
        payload["summary"]["economics_admissible_count"] = 1
        state.write_text(json.dumps(payload), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_currency_state_after_cost_state(
        connection, state, registration_status="engineering_blocked"
    ) == (0, 0)
    assert connection.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == 0
    assert connection.execute(
        "SELECT COUNT(*) FROM experiment_observations"
    ).fetchone()[0] == 0
    connection.close()


def test_currency_state_after_cost_v4_rejects_frozen_registration_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    state, payload, _ = make_after_cost_v4_fixture(root)
    monkeypatch.setattr(genealogy, "ROOT", root)
    patch_after_cost_v4_frozen_hashes(monkeypatch, payload)
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_currency_state_after_cost_state(connection, state) == (0, 0)
    assert connection.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == 0
    connection.close()


def test_after_cost_definition_drift_is_hard_blocked_without_overwrite(
    tmp_path: Path,
) -> None:
    state = tmp_path / "after_cost_v3.json"
    payload = {
        "generated_utc": "2026-08-17T05:00:00+00:00",
        "snapshot_id": "after_cost_v3_snapshot_original",
        "snapshot_schema": "currency_state_after_cost_counterfactual_snapshot_v3",
        "counterfactual_contract_id": "after_cost_v3_contract",
        "counterfactual_cohort_id": "after_cost_v3_cohort",
        "supersedes_contract_id": "after_cost_v2_contract",
        "supersedes_cohort_id": "after_cost_v2_cohort",
        "response_arm_contract_id": "response_contract",
        "response_snapshot_id": "response_snapshot",
        "response_snapshot_sha256": "b" * 64,
        "decision_cutoff_utc": "2026-08-17T04:59:00+00:00",
        "horizons_sec": [60, 300],
        "arm_ids": ["official_context_only", "no_trade"],
        "summary": {"economics_admissible_count": 0, "row_count": 4},
        "input_rejections": [],
        "frozen_manifest": {
            "manifest_id": "after_cost_v3_manifest",
            "contract_id": "after_cost_v3_contract",
            "cohort_id": "after_cost_v3_cohort",
            "manifest_sha256": "a" * 64,
            "artifacts": {
                "counterfactual_v3_module": {"sha256": "c" * 64},
                "counterfactual_v3_config": {"sha256": "d" * 64},
                "counterfactual_v3_cli": {"sha256": "e" * 64},
                "independent_verifier_producer": {"sha256": "f" * 64},
            },
        },
        "canonical_input_hashes": {"envelopes": {}, "records": {}},
        "quote_envelope": None,
        "verifier_envelope": None,
        "economics_envelope": None,
        "hold_switch_envelope": None,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    state.write_text(json.dumps(payload), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    assert import_currency_state_after_cost_state(connection, state) == (1, 1)
    original = connection.execute(
        "SELECT definition_sha256,source_code_hash,definition_json FROM experiments "
        "WHERE hypothesis_id='after_cost_v3_cohort'"
    ).fetchone()

    payload["generated_utc"] = "2026-08-17T05:10:00+00:00"
    payload["snapshot_id"] = "after_cost_v3_snapshot_drifted"
    payload["frozen_manifest"]["manifest_sha256"] = "1" * 64
    payload["frozen_manifest"]["artifacts"]["counterfactual_v3_module"][
        "sha256"
    ] = "2" * 64
    state.write_text(json.dumps(payload), encoding="utf-8")
    assert import_currency_state_after_cost_state(connection, state) == (0, 1)
    assert import_currency_state_after_cost_state(connection, state) == (0, 0)

    preserved = connection.execute(
        "SELECT definition_sha256,source_code_hash,definition_json FROM experiments "
        "WHERE hypothesis_id='after_cost_v3_cohort'"
    ).fetchone()
    assert tuple(preserved) == tuple(original)
    latest = connection.execute(
        "SELECT result,evidence_json FROM experiment_observations "
        "WHERE hypothesis_id='after_cost_v3_cohort' ORDER BY observed_at DESC LIMIT 1"
    ).fetchone()
    assert latest[0] == "engineering_quarantined_artifact_drift_zero_evidence"
    incident = json.loads(latest[1])
    assert incident["hard_blocked"] is True
    assert incident["definition_merged_or_overwritten"] is False
    assert incident["registered_source_code_hash"] == "c" * 64
    assert incident["proposed_source_code_hash"] == "2" * 64

    payload["generated_utc"] = "2026-08-17T05:20:00+00:00"
    payload["summary"]["economics_admissible_count"] = 1
    state.write_text(json.dumps(payload), encoding="utf-8")
    assert import_currency_state_after_cost_state(connection, state) == (0, 1)
    latest_result = connection.execute(
        "SELECT result FROM experiment_observations "
        "WHERE hypothesis_id='after_cost_v3_cohort' ORDER BY observed_at DESC LIMIT 1"
    ).fetchone()[0]
    assert latest_result == "artifact_definition_conflict_hard_blocked_nonzero_evidence"
    connection.close()


def test_after_cost_supersession_requires_zero_prior_evidence(tmp_path: Path) -> None:
    prior = tmp_path / "v1.json"
    replacement = tmp_path / "v2.json"
    prior_payload = {
        "generated_utc": "2026-08-17T03:54:10+00:00",
        "snapshot_id": "v1_snapshot",
        "counterfactual_cohort_id": "v1_cohort",
        "research_only": True,
        "execution_eligible": False,
        "summary": {
            "economics_admissible_count": 0,
            "ranked_count": 0,
            "selectable_count": 0,
            "hold_switch_count": 0,
        },
    }
    replacement_payload = {
        "generated_utc": "2026-08-17T04:14:21+00:00",
        "snapshot_id": "v2_snapshot",
        "counterfactual_contract_id": "v2_contract",
        "counterfactual_cohort_id": "v2_cohort",
        "supersedes_cohort_id": "v1_cohort",
        "research_only": True,
        "execution_eligible": False,
        "frozen_manifest": {
            "manifest_id": "v2_manifest", "manifest_sha256": "a" * 64,
        },
    }
    prior.write_text(json.dumps(prior_payload), encoding="utf-8")
    replacement.write_text(json.dumps(replacement_payload), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    connection.execute(
        "INSERT INTO experiments VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "v1_cohort", None, "fixture", "fixture", "fixture", 1,
            "[]", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}",
            "{}", "fixture", "[]", "", "", "fixture_hash",
            "2026-08-17T03:54:10+00:00", "{}",
        ),
    )
    assert observe_currency_state_after_cost_supersession(
        connection, prior, replacement
    ) == 1
    result = connection.execute(
        "SELECT result FROM experiment_observations"
    ).fetchone()[0]
    assert result == "engineering_superseded_zero_evidence"
    assert observe_currency_state_after_cost_supersession(
        connection, prior, replacement
    ) == 0
    prior_payload["summary"]["economics_admissible_count"] = 1
    prior.write_text(json.dumps(prior_payload), encoding="utf-8")
    assert observe_currency_state_after_cost_supersession(
        connection, prior, replacement
    ) == 0
    prior_payload["summary"]["economics_admissible_count"] = 0
    prior_payload["summary"]["economics_input_count"] = 1
    prior.write_text(json.dumps(prior_payload), encoding="utf-8")
    assert observe_currency_state_after_cost_supersession(
        connection, prior, replacement
    ) == 0
    connection.close()


def test_predecessor_snapshot_losslessly_merges_compatible_history(
    tmp_path: Path,
) -> None:
    predecessor_path, baseline_path, ledger = write_predecessor_registry_fixture(tmp_path)
    predecessor_state = tmp_path / "research_genealogy_v1.json"
    predecessor_report = tmp_path / "RESEARCH_GENEALOGY_CURRENT.md"
    predecessor_state.write_text('{"status":"pre_hardening"}', encoding="utf-8")
    predecessor_report.write_text("# Pre-hardening genealogy\n", encoding="utf-8")
    manifest = genealogy.stage_genealogy_predecessor_snapshot(
        ledger, predecessor_database=predecessor_path,
        predecessor_state=predecessor_state,
        predecessor_report=predecessor_report,
        baseline_database=baseline_path,
        captured_at="2026-08-29T05:00:00+00:00",
    )
    census = manifest["material_contract"]["reconciliation"]
    assert census["predecessor_only_definition_count"] == 1
    assert census["common_exact_definition_count"] == 1
    assert census["definition_conflict_count"] == 1
    assert census["merge_eligible_observation_count"] == 2
    assert census["quarantined_observation_count"] == 1
    immutable = ledger / "snapshots" / manifest["snapshot_id"] / "manifest.json"
    assert immutable.read_bytes() == (
        ledger / "research_genealogy_predecessor_v1.json"
    ).read_bytes()
    repeated = genealogy.stage_genealogy_predecessor_snapshot(
        ledger, predecessor_database=predecessor_path,
        predecessor_state=predecessor_state,
        predecessor_report=predecessor_report,
        baseline_database=baseline_path,
        captured_at="2099-01-01T00:00:00+00:00",
    )
    assert repeated == manifest

    target = connect(baseline_path)
    try:
        assert genealogy.import_genealogy_predecessor_snapshot(target, ledger) == (2, 3)
        assert target.execute(
            "SELECT COUNT(*) FROM experiments WHERE hypothesis_id='legacy.only'"
        ).fetchone()[0] == 1
        assert target.execute(
            "SELECT COUNT(*) FROM experiment_observations WHERE hypothesis_id='common.exact'"
        ).fetchone()[0] == 1
        assert target.execute(
            "SELECT COUNT(*) FROM experiment_observations WHERE hypothesis_id='legacy.only'"
        ).fetchone()[0] == 1
        assert target.execute(
            "SELECT COUNT(*) FROM experiment_observations WHERE hypothesis_id='definition.conflict'"
        ).fetchone()[0] == 0
        archived = target.execute(
            "SELECT COUNT(*) FROM experiments WHERE experiment_kind='archived_genealogy_snapshot'"
        ).fetchone()[0]
        assert archived == 1
        registered = genealogy._registered_predecessor_snapshots(target)
        assert registered[0]["snapshot_id"] == manifest["snapshot_id"]
        assert registered[0]["quarantined_observation_count"] == 1
        assert genealogy.import_genealogy_predecessor_snapshot(target, ledger) == (0, 0)
    finally:
        target.close()


def test_predecessor_snapshot_tamper_fails_before_merge(tmp_path: Path) -> None:
    predecessor_path, baseline_path, ledger = write_predecessor_registry_fixture(tmp_path)
    state = tmp_path / "state.json"; report = tmp_path / "report.md"
    state.write_text("{}", encoding="utf-8"); report.write_text("old", encoding="utf-8")
    manifest = genealogy.stage_genealogy_predecessor_snapshot(
        ledger, predecessor_database=predecessor_path, predecessor_state=state,
        predecessor_report=report, baseline_database=baseline_path,
    )
    archived_database = (
        ledger / "snapshots" / manifest["snapshot_id"] /
        manifest["files"]["database"]["filename"]
    )
    archived_database.write_bytes(archived_database.read_bytes() + b"tamper")
    target = connect(baseline_path)
    try:
        before = target.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
        with pytest.raises(ValueError, match="archive bytes mismatch"):
            genealogy.import_genealogy_predecessor_snapshot(target, ledger)
        assert target.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == before
    finally:
        target.close()


def test_predecessor_snapshot_rejects_changed_baseline_census(tmp_path: Path) -> None:
    predecessor_path, baseline_path, ledger = write_predecessor_registry_fixture(tmp_path)
    state = tmp_path / "state.json"; report = tmp_path / "report.md"
    state.write_text("{}", encoding="utf-8"); report.write_text("old", encoding="utf-8")
    genealogy.stage_genealogy_predecessor_snapshot(
        ledger, predecessor_database=predecessor_path, predecessor_state=state,
        predecessor_report=report, baseline_database=baseline_path,
    )
    target = connect(baseline_path)
    try:
        insert_parent_fixture(target, "late.unreviewed", "fixture")
        with pytest.raises(ValueError, match="census changed"):
            genealogy.import_genealogy_predecessor_snapshot(target, ledger)
        assert target.execute(
            "SELECT COUNT(*) FROM experiments WHERE hypothesis_id='legacy.only'"
        ).fetchone()[0] == 0
    finally:
        target.close()


def test_predecessor_snapshot_rejects_noncheckpointed_wal(tmp_path: Path) -> None:
    predecessor_path, baseline_path, ledger = write_predecessor_registry_fixture(tmp_path)
    state = tmp_path / "state.json"; report = tmp_path / "report.md"
    state.write_text("{}", encoding="utf-8"); report.write_text("old", encoding="utf-8")
    predecessor_path.with_name(predecessor_path.name + "-wal").write_bytes(b"live")
    with pytest.raises(ValueError, match="checkpoint predecessor WAL"):
        genealogy.stage_genealogy_predecessor_snapshot(
            ledger, predecessor_database=predecessor_path,
            predecessor_state=state, predecessor_report=report,
            baseline_database=baseline_path,
        )


def test_all68_policy_challenger_registers_exact_three_cohort_lineage(
    tmp_path: Path,
) -> None:
    connection = connect(tmp_path / "genealogy.sqlite")
    try:
        insert_parent_fixture(
            connection,
            "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
            "historical_all68_portfolio_batch_replay",
            definition_sha256=(
                "9d6e7e0f27ad289c4c8368679bdc350058e61750387ccb94d9a98dcb26ecbb04"
            ),
        )
        insert_parent_fixture(
            connection,
            "sequential_all68_mistake_curriculum_v1.98755c2c2f9bd26b014a",
            "historical_all68_mistake_curriculum",
            definition_sha256=(
                "98755c2c2f9bd26b014aad140754c771482de69d0c418bceb1003e8313d7b83c"
            ),
        )
        root = (
            genealogy.ROOT / "data" / "oanda_training_manager" /
            "research_ledgers" / "sequential_all68_policy_challenger_v1"
        )
        assert genealogy.import_sequential_all68_policy_challengers(
            connection, root
        ) == (3, 3)
        assert genealogy.import_sequential_all68_policy_challengers(
            connection, root
        ) == (0, 0)
        rows = connection.execute(
            "SELECT hypothesis_id,parent_hypothesis_id FROM experiments "
            "WHERE experiment_kind='historical_sequential_all68_policy_challenger' "
            "ORDER BY hypothesis_id"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            (
                "sequential_all68_policy_challenger_v1.a02365972fb81cba1f5f",
                "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
            ),
            (
                "sequential_all68_policy_challenger_v1.ebb8b63d36ada93dca64",
                "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
            ),
            (
                "sequential_all68_policy_challenger_v1.efafd05f9f14a83ac946",
                "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
            ),
        ]
        evidence = json.loads(connection.execute(
            "SELECT evidence_json FROM experiment_observations WHERE hypothesis_id=?",
            ("sequential_all68_policy_challenger_v1.a02365972fb81cba1f5f",),
        ).fetchone()[0])
        assert evidence["mistake_curriculum_binding"]["cohort_id"] == (
            "sequential_all68_mistake_curriculum_v1.98755c2c2f9bd26b014a"
        )
        assert evidence["market_repetition_count"] == 144
        assert evidence["independent_regime_count"] is None
        assert evidence["supported_decision"] == "no_trade"
    finally:
        connection.close()


def test_all68_policy_challenger_rejects_parent_name_without_exact_definition(
    tmp_path: Path,
) -> None:
    connection = connect(tmp_path / "genealogy.sqlite")
    try:
        insert_parent_fixture(
            connection,
            "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
            "historical_all68_portfolio_batch_replay",
            definition_sha256="0" * 64,
        )
        insert_parent_fixture(
            connection,
            "sequential_all68_mistake_curriculum_v1.98755c2c2f9bd26b014a",
            "historical_all68_mistake_curriculum",
            definition_sha256="1" * 64,
        )
        root = (
            genealogy.ROOT / "data" / "oanda_training_manager" /
            "research_ledgers" / "sequential_all68_policy_challenger_v1"
        )
        assert genealogy.import_sequential_all68_policy_challengers(
            connection, root
        ) == (0, 0)
    finally:
        connection.close()


def test_all68_policy_challenger_rejects_tampered_dataset_bytes(
    tmp_path: Path,
) -> None:
    source = (
        genealogy.ROOT / "data" / "oanda_training_manager" /
        "research_ledgers" / "sequential_all68_policy_challenger_v1"
    )
    copied = tmp_path / "policy"
    shutil.copytree(source, copied)
    cohort_id = "sequential_all68_policy_challenger_v1.a02365972fb81cba1f5f"
    dataset = copied / "cohorts" / cohort_id / "global_clocks.jsonl.gz"
    dataset.write_bytes(dataset.read_bytes() + b"tamper")
    connection = connect(tmp_path / "genealogy.sqlite")
    try:
        insert_parent_fixture(
            connection,
            "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
            "historical_all68_portfolio_batch_replay",
            definition_sha256=(
                "9d6e7e0f27ad289c4c8368679bdc350058e61750387ccb94d9a98dcb26ecbb04"
            ),
        )
        insert_parent_fixture(
            connection,
            "sequential_all68_mistake_curriculum_v1.98755c2c2f9bd26b014a",
            "historical_all68_mistake_curriculum",
            definition_sha256=(
                "98755c2c2f9bd26b014aad140754c771482de69d0c418bceb1003e8313d7b83c"
            ),
        )
        assert genealogy.import_sequential_all68_policy_challengers(
            connection, copied,
            cohort_specs={
                cohort_id: genealogy.SEQUENTIAL_ALL68_POLICY_CHALLENGER_GENEALOGY_COHORT_SPECS[cohort_id]
            },
        ) == (0, 0)
    finally:
        connection.close()


def test_all68_policy_expansion_registers_exact_sealed_parent_chain(
    tmp_path: Path,
) -> None:
    connection = connect(tmp_path / "genealogy.sqlite")
    root = (genealogy.ROOT / "data" / "oanda_training_manager" /
            "research_ledgers" / "sequential_all68_policy_expansion_v1")
    try:
        old_spec = genealogy.SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC
        assert genealogy.import_sequential_all68_policy_expansion(connection, root, specification=old_spec) == (3, 3)
        assert genealogy.import_sequential_all68_policy_expansion(connection, root, specification=old_spec) == (0, 0)
        rows = connection.execute(
            "SELECT hypothesis_id,parent_hypothesis_id,definition_sha256 "
            "FROM experiments WHERE hypothesis_id IN (?,?,?) ORDER BY hypothesis_id",
            ("sequential_replay_source_pack_v1.5cfcd2011d3b34fe4bb2",
             "seq_a68_wed_exp_v1.92bb0e964d2639b7e897",
             "sequential_all68_policy_expansion_v1.bd9d43742d8ee85ba52b"),
        ).fetchall()
        by_id = {row[0]: tuple(row[1:]) for row in rows}
        assert by_id["sequential_replay_source_pack_v1.5cfcd2011d3b34fe4bb2"] == (
            None, "5cfcd2011d3b34fe4bb2ff1da6d62fba54d87b68f70fbf9212a51b98dc0624bb")
        assert by_id["seq_a68_wed_exp_v1.92bb0e964d2639b7e897"] == (
            "sequential_replay_source_pack_v1.5cfcd2011d3b34fe4bb2",
            "92bb0e964d2639b7e8976e1dbe439d60f1221630db5dc7149bb2d5358738d4d2")
        assert by_id["sequential_all68_policy_expansion_v1.bd9d43742d8ee85ba52b"] == (
            "seq_a68_wed_exp_v1.92bb0e964d2639b7e897",
            "bd9d43742d8ee85ba52b9f18d49ba3ce67fcb09fee8e6e9ae47e45e7df0427a5")
        evidence = json.loads(connection.execute(
            "SELECT evidence_json FROM experiment_observations WHERE hypothesis_id=?",
            ("sequential_all68_policy_expansion_v1.bd9d43742d8ee85ba52b",),
        ).fetchone()[0])
        assert evidence["global_clock_count"] == 336
        assert evidence["market_repetition_count"] == 336
        assert evidence["independent_regime_count"] is None
        assert evidence["proof_eligible"] is False
        assert evidence["execution_eligible"] is False
        assert evidence["can_authorize"] is False
        assert evidence["broker_access"] is False
        assert evidence["account_access"] is False
        assert evidence["supported_decision"] == "no_trade"
    finally:
        connection.close()


def test_all68_policy_expansion_rejects_tampered_dataset_bytes(tmp_path: Path) -> None:
    source = (genealogy.ROOT / "data" / "oanda_training_manager" /
              "research_ledgers" / "sequential_all68_policy_expansion_v1")
    copied = tmp_path / "expansion"
    shutil.copytree(source, copied)
    cohort = copied / "cohorts" / genealogy.SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC["cohort_id"]
    dataset = cohort / "arm_decisions.jsonl.gz"
    dataset.write_bytes(dataset.read_bytes() + b"tamper")
    connection = connect(tmp_path / "genealogy.sqlite")
    try:
        assert genealogy.import_sequential_all68_policy_expansion(
            connection, copied, specification=genealogy.SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC
        ) == (0, 0)
        assert connection.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == 0
    finally:
        connection.close()


def test_all68_policy_expansion_rejects_name_only_or_wrong_bound_parent(
    tmp_path: Path,
) -> None:
    source = (genealogy.ROOT / "data" / "oanda_training_manager" /
              "research_ledgers" / "sequential_all68_policy_expansion_v1")
    copied = tmp_path / "expansion"
    shutil.copytree(source, copied)
    cohort = copied / "cohorts" / genealogy.SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC["cohort_id"]
    bound = cohort / "bound_state.json"
    value = json.loads(bound.read_text(encoding="utf-8"))
    value["material_sha256"] = "0" * 64
    bound.write_text(json.dumps(value), encoding="utf-8")
    connection = connect(tmp_path / "genealogy.sqlite")
    try:
        assert genealogy.import_sequential_all68_policy_expansion(
            connection, copied, specification=genealogy.SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC
        ) == (0, 0)
        assert connection.execute("SELECT COUNT(*) FROM experiments").fetchone()[0] == 0
    finally:
        connection.close()


def test_canonical_gzip_rebuild_registers_new_chain_without_replacing_prior_ids(
    tmp_path: Path,
) -> None:
    connection = connect(tmp_path / "genealogy.sqlite")
    ledgers = genealogy.ROOT / "data" / "oanda_training_manager" / "research_ledgers"
    try:
        insert_parent_fixture(
            connection, "sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d",
            "historical_replay_source_infrastructure",
            definition_sha256="b9d1526f3dfa057bd06d16d70dfa1a377065337833e186faad52448bf1e1d20f",
        )
        assert genealogy.import_sequential_all68_portfolio_batch_replays(
            connection, ledgers / "sequential_all68_portfolio_batch_replay_v1",
            cohort_specs={
                "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262":
                genealogy.SEQUENTIAL_ALL68_GENEALOGY_COHORT_SPECS[
                    "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262"
                ]
            },
        ) == (1, 1)
        assert genealogy.import_sequential_all68_mistake_curriculum(
            connection, ledgers / "sequential_all68_mistake_curriculum_v1",
            cohort_specs={
                "sequential_all68_mistake_curriculum_v1.26b13486af3803244125":
                genealogy.SEQUENTIAL_ALL68_MISTAKE_GENEALOGY_COHORT_SPECS[
                    "sequential_all68_mistake_curriculum_v1.26b13486af3803244125"
                ]
            },
        ) == (1, 1)
        assert genealogy.import_sequential_all68_policy_challengers(
            connection, ledgers / "sequential_all68_policy_challenger_v1",
            cohort_specs={
                "sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf":
                genealogy.SEQUENTIAL_ALL68_POLICY_CHALLENGER_GENEALOGY_COHORT_SPECS[
                    "sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf"
                ]
            },
        ) == (1, 1)
        new_expansion = genealogy.SEQUENTIAL_ALL68_POLICY_EXPANSION_SPECS[
            "sequential_all68_policy_expansion_v1.15a3aa5d039df7df9a7d"
        ]
        assert genealogy.import_sequential_all68_policy_expansion(
            connection, ledgers / "sequential_all68_policy_expansion_v1",
            specification=new_expansion,
        ) == (3, 3)
        assert genealogy.import_sequential_all68_policy_expansion(
            connection, ledgers / "sequential_all68_policy_expansion_v1",
            specification=new_expansion,
        ) == (0, 0)
        for child, parent in [
            ("sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262",
             "sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d"),
            ("sequential_all68_mistake_curriculum_v1.26b13486af3803244125",
             "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262"),
            ("sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf",
             "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262"),
            ("seq_a68_wed_exp_v1.e7de4ecdd0255eb13306",
             "sequential_replay_source_pack_v1.554c8f74212202aa9b86"),
            ("sequential_all68_policy_expansion_v1.15a3aa5d039df7df9a7d",
             "seq_a68_wed_exp_v1.e7de4ecdd0255eb13306"),
        ]:
            assert connection.execute(
                "SELECT parent_hypothesis_id FROM experiments WHERE hypothesis_id=?",
                (child,),
            ).fetchone()[0] == parent
    finally:
        connection.close()
