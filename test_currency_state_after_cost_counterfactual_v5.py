from __future__ import annotations

import copy
import inspect
import json
import sqlite3
from pathlib import Path

import pytest

try:
    from forex_system.research.currency_state_after_cost_counterfactual_v5 import (
        AfterCostV5Error,
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v5,
        initialize_identity_registry_v5,
    )
    from forex_system.research.currency_state_after_cost_registrar_v5 import (
        EXPERIMENT_COLUMNS,
        registrar_definition_v5,
    )
except ModuleNotFoundError:
    from src.forex_system.research.currency_state_after_cost_counterfactual_v5 import (
        AfterCostV5Error,
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v5,
        initialize_identity_registry_v5,
    )
    from src.forex_system.research.currency_state_after_cost_registrar_v5 import (
        EXPERIMENT_COLUMNS,
        registrar_definition_v5,
    )

import oanda_currency_state_after_cost_counterfactual_v5 as cli
from test_currency_state_after_cost_counterfactual_v2 import response
from test_currency_state_after_cost_counterfactual_v3 import (
    H,
    economics_record,
    envelope,
    flat_account_records,
    forecast_record,
    manifest as v3_manifest,
    quote_record,
    verifier_record,
    contract as v3_contract,
)


ROOT = Path(__file__).resolve().parent


def contract():
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v5.json").read_text()
    )


def v4_contract():
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v4.json").read_text()
    )


def v4_manifest():
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v4_manifest.json").read_text()
    )


def manifest():
    c = contract()
    return {
        "manifest_id": c["required_frozen_manifest_id"],
        "contract_id": c["contract_id"],
        "cohort_id": c["counterfactual_cohort_id"],
        "artifacts": {
            label: {"relative_path": path, "sha256": H}
            for label, path in EXPECTED_ARTIFACT_PATHS.items()
        },
    }


def build(r, q=None, v=None, e=None, h=None, a=None, *, registry=None, replay=None):
    return build_after_cost_counterfactual_v5(
        r,
        contract=contract(), frozen_manifest=manifest(),
        upstream_v4_contract=v4_contract(), upstream_v4_manifest=v4_manifest(),
        upstream_v3_contract=v3_contract(), upstream_v3_manifest=v3_manifest(),
        venue_quotes=q, verifier_evidence=v, economics_inputs=e,
        hold_switch_inputs=h, account_state=a,
        identity_registry_path=registry, replay_identity=replay,
    )


def valid_inputs(r):
    cutoff = r["decision_cutoff_utc"]
    q = envelope("venue_quotes", cutoff, [quote_record()])
    forecast = forecast_record(r)
    v = envelope("verifier_evidence", cutoff, [verifier_record(r, forecast)])
    a = envelope("account_state", cutoff, flat_account_records())
    econ = economics_record(r, forecast, q["records"][0], v["records"][0], a["records"])
    e = envelope("economics_inputs", cutoff, [econ])
    return q, v, e, a


def init_registry(path: Path) -> None:
    initialize_identity_registry_v5(
        path, contract=contract(), manifest_id=manifest()["manifest_id"],
    )


def create_genealogy(path: Path, definition: dict) -> None:
    connection = sqlite3.connect(path)
    try:
        column_types = {
            "pre_registered": "INTEGER",
        }
        columns = ",".join(
            f"{name} {column_types.get(name, 'TEXT')}"
            + (" PRIMARY KEY" if name == "hypothesis_id" else "")
            for name in EXPERIMENT_COLUMNS
        )
        connection.execute(f"CREATE TABLE experiments({columns})")
        connection.execute(
            f"INSERT INTO experiments({','.join(EXPERIMENT_COLUMNS)}) VALUES({','.join('?' for _ in EXPERIMENT_COLUMNS)})",
            tuple(definition[name] for name in EXPERIMENT_COLUMNS),
        )
        connection.commit()
    finally:
        connection.close()


def sample_anchor() -> dict:
    return {
        "anchor_schema": "currency_state_after_cost_v5_external_trust_anchor_v1",
        "manifest_relative_path": "config/currency_state_after_cost_counterfactual_v5_manifest.json",
        "manifest_file_sha256": "1" * 64,
        "manifest_id": contract()["required_frozen_manifest_id"],
        "manifest_sha256": "2" * 64,
        "artifacts": {
            label: {"relative_path": path, "sha256": H}
            for label, path in EXPECTED_ARTIFACT_PATHS.items()
        },
        "counterfactual_v5_module_sha256": H,
        "counterfactual_v5_registrar_sha256": H,
        "counterfactual_v5_config_sha256": H,
        "counterfactual_v5_cli_sha256": H,
    }


def test_zero_input_is_v5_zero_evidence_research_only_no_trade():
    result = build(response())
    assert result["schema_version"] == 5
    assert result["counterfactual_cohort_id"].endswith("20260817e")
    assert result["summary"]["row_count"] == 2720
    assert result["summary"]["submitted_envelope_count"] == 0
    assert result["summary"]["economics_admissible_count"] == 0
    assert result["identity_registry_state"]["state"] == "not_opened_for_zero_input_initialization"
    assert result["supported_execution_decision"] == "no_trade"
    assert all(row["paper_decision"] == "no_trade" for row in result["records"])


def test_nonempty_core_requires_persistent_registry_and_replay_identity(tmp_path):
    r = response(); q, v, e, a = valid_inputs(r)
    with pytest.raises(AfterCostV5Error, match="persistent identity registry"):
        build(r, q, v, e, a=a)
    registry = tmp_path / "identity.sqlite"
    init_registry(registry)
    with pytest.raises(AfterCostV5Error, match="replay_identity"):
        build(r, q, v, e, a=a, registry=registry)


def test_identical_cross_run_replay_is_append_only_and_idempotently_bound(tmp_path):
    registry = tmp_path / "identity.sqlite"; init_registry(registry)
    r = response(); q, v, e, a = valid_inputs(r)
    first = build(r, q, v, e, a=a, registry=registry, replay="replay-1")
    second = build(r, q, v, e, a=a, registry=registry, replay="replay-1")
    assert first["identity_registry_state"]["canonical_submission_sha256"] == second["identity_registry_state"]["canonical_submission_sha256"]
    connection = sqlite3.connect(registry)
    try:
        assert connection.execute("SELECT COUNT(*) FROM replay_bindings").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM replay_observations").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM identity_conflicts").fetchone()[0] == 0
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE replay_bindings SET canonical_submission_sha256='x'")
    finally:
        connection.close()


@pytest.mark.parametrize("identity_scope", ["replay", "payload", "record"])
def test_cross_run_identity_equivocation_is_logged_and_rejected(tmp_path, identity_scope):
    registry = tmp_path / "identity.sqlite"; init_registry(registry)
    r = response(); q, v, e, a = valid_inputs(r)
    build(r, q, v, e, a=a, registry=registry, replay="replay-1")
    bad_q = copy.deepcopy(q)
    raw = copy.deepcopy(bad_q["records"][0])
    raw.pop("canonical_record_sha256")
    raw["bid"] += 0.0001
    # Keep or change identities selectively, while resealing valid canonical bytes.
    if identity_scope == "record":
        raw_id = raw["quote_record_id"]
        bad_q = envelope("venue_quotes", r["decision_cutoff_utc"], [raw])
        assert bad_q["records"][0]["quote_record_id"] == raw_id
        bad_q["payload_id"] = "new-payload"
        from test_currency_state_after_cost_counterfactual_v3 import seal_envelope
        bad_q = seal_envelope({k: val for k, val in bad_q.items() if k != "canonical_envelope_sha256"})
        replay = "replay-2"
    else:
        bad_q = envelope("venue_quotes", r["decision_cutoff_utc"], [raw])
        replay = "replay-1" if identity_scope == "replay" else "replay-2"
    with pytest.raises(AfterCostV5Error, match="equivocation"):
        build(r, bad_q, registry=registry, replay=replay)
    connection = sqlite3.connect(registry)
    try:
        assert connection.execute("SELECT COUNT(*) FROM identity_conflicts").fetchone()[0] >= 1
    finally:
        connection.close()


def test_wrong_identity_registry_contract_and_missing_append_only_triggers_fail(tmp_path):
    wrong = tmp_path / "wrong.sqlite"
    init_registry(wrong)
    connection = sqlite3.connect(wrong)
    try:
        connection.execute("DROP TRIGGER no_update_registry_meta")
        connection.commit()
    finally:
        connection.close()
    r = response(); q, _, _, _ = valid_inputs(r)
    with pytest.raises(AfterCostV5Error, match="wrong or non-append-only"):
        build(r, q, registry=wrong, replay="replay-1")


def test_genealogy_path_is_not_caller_selectable():
    assert "genealogy_path" not in inspect.signature(cli.run).parameters
    assert "identity_registry_path" not in inspect.signature(cli.run).parameters
    with pytest.raises(TypeError):
        cli.run(genealogy_path=Path("alternate.sqlite"))


def test_exact_registrar_definition_and_sha_are_verified(tmp_path):
    expected = registrar_definition_v5(contract=contract(), external_anchor=sample_anchor())
    db = tmp_path / "genealogy.sqlite"; create_genealogy(db, expected)
    assert cli._read_exact_genealogy_definition(db, expected) == (
        True, "v5_genealogy_exact_registrar_definition_verified",
    )
    for field, value, expected_state in (
        ("selection_rule", "modified", "v5_genealogy_exact_registrar_definition_mismatch"),
        ("definition_sha256", "f" * 64, "v5_genealogy_definition_sha256_recompute_mismatch"),
    ):
        modified = dict(expected); modified[field] = value
        other = tmp_path / f"bad-{field}.sqlite"; create_genealogy(other, modified)
        trusted, state = cli._read_exact_genealogy_definition(other, expected)
        assert trusted is False and state == expected_state


def test_definition_json_change_with_rehashed_value_still_fails_exact_registrar_match(tmp_path):
    expected = registrar_definition_v5(contract=contract(), external_anchor=sample_anchor())
    changed = dict(expected)
    payload = json.loads(changed["definition_json"]); payload["can_place_orders"] = True
    from src.forex_system.contracts.currency_state import stable_hash
    changed["definition_json"] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    changed["definition_sha256"] = stable_hash(payload)
    db = tmp_path / "changed.sqlite"; create_genealogy(db, changed)
    assert cli._read_exact_genealogy_definition(db, expected) == (
        False, "v5_genealogy_exact_registrar_definition_mismatch",
    )


def test_wrong_genealogy_registry_schema_fails_closed(tmp_path):
    db = tmp_path / "wrong.sqlite"
    connection = sqlite3.connect(db)
    try:
        connection.execute("CREATE TABLE experiments(hypothesis_id TEXT PRIMARY KEY,definition_json TEXT)")
        connection.commit()
    finally:
        connection.close()
    expected = registrar_definition_v5(contract=contract(), external_anchor=sample_anchor())
    assert cli._read_exact_genealogy_definition(db, expected) == (
        False, "wrong_genealogy_registry_schema",
    )


def test_v4_frozen_artifact_hashes_are_unchanged():
    assert cli.file_sha256(ROOT / "src/forex_system/research/currency_state_after_cost_counterfactual_v4.py") == "98010415f01fb49d82d1be1bcc873e8070a46627bc5c43ae0198df4e2c496f5e"
    assert cli.file_sha256(ROOT / "config/currency_state_after_cost_counterfactual_v4.json") == "72c7cf64e2eaa5510e981239801393a1bb512cdb1feb1ae3c4ae2daa1745a03d"
    assert cli.file_sha256(ROOT / "oanda_currency_state_after_cost_counterfactual_v4.py") == "971a8d94502d5f23b7c43b299c491b3c295f63fe5583265fc569dcee7d9de825"
    assert cli.file_sha256(ROOT / "config/currency_state_after_cost_counterfactual_v4_manifest.json") == "3a2fe88ba8e330c92082324de307db075ce63b69b7c7b7e08a1e7dfca464b906"
