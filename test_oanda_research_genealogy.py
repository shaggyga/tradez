from __future__ import annotations

import sqlite3
from pathlib import Path

import hashlib
import json
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
