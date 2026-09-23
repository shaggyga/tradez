from __future__ import annotations

import concurrent.futures
import copy
import hashlib
import inspect
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

try:
    import forex_system.research.currency_state_after_cost_counterfactual_v6 as core
    from forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from forex_system.research.currency_state_after_cost_counterfactual_v6 import (
        AfterCostV6Error,
        EXPECTED_ARTIFACT_PATHS,
        admit_persistent_identities_v6,
        build_after_cost_counterfactual_v6,
        initialize_identity_registry_v6,
    )
    from forex_system.research.currency_state_after_cost_registrar_v6 import (
        EXPERIMENT_COLUMNS,
        canonical_json,
        registrar_definition_v6,
    )
except ModuleNotFoundError:
    import src.forex_system.research.currency_state_after_cost_counterfactual_v6 as core
    from src.forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from src.forex_system.research.currency_state_after_cost_counterfactual_v6 import (
        AfterCostV6Error,
        EXPECTED_ARTIFACT_PATHS,
        admit_persistent_identities_v6,
        build_after_cost_counterfactual_v6,
        initialize_identity_registry_v6,
    )
    from src.forex_system.research.currency_state_after_cost_registrar_v6 import (
        EXPERIMENT_COLUMNS,
        canonical_json,
        registrar_definition_v6,
    )

import oanda_currency_state_after_cost_counterfactual_v6 as cli
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


def contract() -> dict:
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v6.json").read_text()
    )


def v5_contract() -> dict:
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v5.json").read_text()
    )


def v4_contract() -> dict:
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v4.json").read_text()
    )


def v4_manifest() -> dict:
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v4_manifest.json").read_text()
    )


def manifest() -> dict:
    value = contract()
    return {
        "manifest_id": value["required_frozen_manifest_id"],
        "contract_id": value["contract_id"],
        "cohort_id": value["counterfactual_cohort_id"],
        "artifacts": {
            label: {"relative_path": path, "sha256": H}
            for label, path in EXPECTED_ARTIFACT_PATHS.items()
        },
    }


def raw_manifest_sha(value: dict) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def manifest_semantic_sha(value: dict | None = None) -> str:
    value = value or manifest()
    return _validate_manifest(value, contract())["manifest_sha256"]


def init_registry(path: Path, value: dict | None = None, *, raw_sha: str | None = None) -> None:
    value = value or manifest()
    initialize_identity_registry_v6(
        path,
        contract=contract(),
        manifest_id=value["manifest_id"],
        manifest_file_sha256=raw_sha or raw_manifest_sha(value),
        manifest_semantic_sha256=manifest_semantic_sha(value),
    )


def admit(path: Path, replay: str, envelopes: dict, value: dict | None = None, *, raw_sha: str | None = None):
    value = value or manifest()
    return admit_persistent_identities_v6(
        path,
        contract=contract(),
        manifest_id=value["manifest_id"],
        manifest_file_sha256=raw_sha or raw_manifest_sha(value),
        manifest_semantic_sha256=manifest_semantic_sha(value),
        replay_identity=replay,
        envelopes=envelopes,
    )


def build(r, q=None, v=None, e=None, h=None, a=None, *, registry=None, replay=None, value=None, raw_sha=None):
    value = value or manifest()
    return build_after_cost_counterfactual_v6(
        r,
        contract=contract(),
        frozen_manifest=value,
        frozen_manifest_file_sha256=raw_sha or raw_manifest_sha(value),
        upstream_v5_contract=v5_contract(),
        upstream_v4_contract=v4_contract(),
        upstream_v4_manifest=v4_manifest(),
        upstream_v3_contract=v3_contract(),
        upstream_v3_manifest=v3_manifest(),
        venue_quotes=q,
        verifier_evidence=v,
        economics_inputs=e,
        hold_switch_inputs=h,
        account_state=a,
        identity_registry_path=registry,
        replay_identity=replay,
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


def only_quotes(value: dict) -> dict:
    return {
        "venue_quotes": value,
        "verifier_evidence": None,
        "economics_inputs": None,
        "hold_switch_inputs": None,
        "account_state": None,
    }


def registry_counts(path: Path) -> dict[str, int]:
    connection = sqlite3.connect(path)
    try:
        return {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in (
                "registry_meta", "identity_bindings", "replay_bindings",
                "replay_observations", "identity_conflicts",
            )
        }
    finally:
        connection.close()


def sample_anchor() -> dict:
    return {
        "anchor_schema": "currency_state_after_cost_v6_external_trust_anchor_v1",
        "manifest_relative_path": "config/currency_state_after_cost_counterfactual_v6_manifest.json",
        "manifest_file_sha256": "1" * 64,
        "manifest_id": contract()["required_frozen_manifest_id"],
        "manifest_semantic_sha256": "2" * 64,
        "artifacts": {
            label: {"relative_path": path, "sha256": H}
            for label, path in EXPECTED_ARTIFACT_PATHS.items()
        },
        "counterfactual_v6_module_sha256": H,
        "counterfactual_v6_registrar_sha256": H,
        "counterfactual_v6_config_sha256": H,
        "counterfactual_v6_cli_sha256": H,
        "counterfactual_v6_test_sha256": H,
        "counterfactual_v6_proposal_sha256": H,
    }


def create_genealogy(
    path: Path,
    rows: list[dict] | None = None,
    *,
    experiments_sql: str | None = None,
    trigger_overrides: dict[str, str] | None = None,
) -> None:
    connection = sqlite3.connect(path)
    try:
        tables = dict(cli.GENEALOGY_TABLE_SQL)
        if experiments_sql is not None:
            tables["experiments"] = experiments_sql
        for sql in tables.values():
            connection.execute(sql)
        for sql in cli.GENEALOGY_INDEX_SQL.values():
            connection.execute(sql)
        triggers = dict(cli.GENEALOGY_TRIGGER_SQL)
        triggers.update(trigger_overrides or {})
        for sql in triggers.values():
            connection.execute(sql)
        for row in rows or []:
            connection.execute(
                f"INSERT INTO experiments({','.join(EXPERIMENT_COLUMNS)}) "
                f"VALUES({','.join('?' for _ in EXPERIMENT_COLUMNS)})",
                tuple(row[name] for name in EXPERIMENT_COLUMNS),
            )
        connection.commit()
    finally:
        connection.close()


def test_zero_input_is_isolated_zero_evidence_no_trade():
    result = build(response())
    assert result["schema_version"] == 6
    assert result["counterfactual_cohort_id"].endswith("20260817f")
    assert result["summary"]["row_count"] == 2720
    assert result["summary"]["submitted_envelope_count"] == 0
    for field in (
        "submitted_record_count", "economics_admissible_count", "ranked_count",
        "selectable_count", "hold_switch_count",
    ):
        assert result["summary"][field] == 0
    assert result["identity_registry_state"]["state"] == "not_opened_for_zero_input_initialization"
    assert result["supported_execution_decision"] == "no_trade"
    assert all(row["paper_decision"] == "no_trade" for row in result["records"])


@pytest.mark.parametrize("equivocal", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_first_batch_duplicate_or_equivocal_key_is_rejected_before_any_insert(
    tmp_path, equivocal, reverse,
):
    registry = tmp_path / f"batch-{equivocal}-{reverse}.sqlite"
    init_registry(registry)
    first = quote_record("EUR_USD")
    second = copy.deepcopy(first)
    if equivocal:
        second["bid"] += 0.0001
    records = [first, second]
    if reverse:
        records.reverse()
    q = envelope("venue_quotes", response()["decision_cutoff_utc"], records)
    expected_word = "equivocal" if equivocal else "duplicate"
    with pytest.raises(AfterCostV6Error, match=expected_word):
        admit(registry, "first-replay", only_quotes(q))
    assert registry_counts(registry) == {
        "registry_meta": 1,
        "identity_bindings": 0,
        "replay_bindings": 0,
        "replay_observations": 0,
        "identity_conflicts": 0,
    }


def test_distinct_record_reversal_has_same_semantic_replay_binding(tmp_path):
    registry = tmp_path / "order.sqlite"
    init_registry(registry)
    cutoff = response()["decision_cutoff_utc"]
    records = [quote_record("EUR_USD"), quote_record("GBP_USD")]
    forward = envelope("venue_quotes", cutoff, records)
    reversed_envelope = envelope("venue_quotes", cutoff, list(reversed(records)))
    first = admit(registry, "same-replay", only_quotes(forward))
    second = admit(registry, "same-replay", only_quotes(reversed_envelope))
    assert first["semantic_submission_sha256"] == second["semantic_submission_sha256"]
    assert first["raw_submission_sha256"] != second["raw_submission_sha256"]
    counts = registry_counts(registry)
    assert counts["replay_bindings"] == 1
    assert counts["identity_bindings"] == 3
    assert counts["replay_observations"] == 2
    assert counts["identity_conflicts"] == 0


def test_exact_registry_schema_and_trigger_sql_are_checked_on_every_replay(tmp_path):
    registry = tmp_path / "inert-trigger.sqlite"
    init_registry(registry)
    q = envelope("venue_quotes", response()["decision_cutoff_utc"], [quote_record()])
    admit(registry, "replay-1", only_quotes(q))
    connection = sqlite3.connect(registry)
    try:
        connection.execute("DROP TRIGGER no_update_registry_meta")
        connection.execute(
            "CREATE TRIGGER no_update_registry_meta BEFORE UPDATE ON registry_meta "
            "BEGIN SELECT 1; END"
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(AfterCostV6Error, match="exact SQL schema mismatch"):
        admit(registry, "replay-1", only_quotes(q))
    assert registry_counts(registry)["replay_observations"] == 1


def test_registry_metadata_binds_same_id_to_raw_and_semantic_manifest_hashes(tmp_path):
    original = manifest()
    registry = tmp_path / "manifest.sqlite"
    init_registry(registry, original)
    q = envelope("venue_quotes", response()["decision_cutoff_utc"], [quote_record()])
    admit(registry, "replay-1", only_quotes(q), original)

    # Same semantic manifest and ID, different raw-file bytes.
    alternate_raw = "f" * 64
    with pytest.raises(AfterCostV6Error, match="manifest/cohort metadata binding"):
        admit(registry, "replay-1", only_quotes(q), original, raw_sha=alternate_raw)

    # Same ID, changed artifact definition and therefore changed semantic hash.
    changed = copy.deepcopy(original)
    changed["artifacts"]["quote_producer"]["sha256"] = "e" * 64
    with pytest.raises(AfterCostV6Error, match="manifest/cohort metadata binding"):
        admit(registry, "replay-2", only_quotes(q), changed)
    assert registry_counts(registry)["replay_observations"] == 1


def test_missing_registry_and_genealogy_paths_are_noncreating(tmp_path):
    missing_registry = tmp_path / "missing-parent" / "missing.sqlite"
    q = envelope("venue_quotes", response()["decision_cutoff_utc"], [quote_record()])
    with pytest.raises(AfterCostV6Error, match="missing or not a regular file"):
        admit(missing_registry, "replay-1", only_quotes(q))
    assert not missing_registry.exists()
    assert not missing_registry.parent.exists()

    missing_genealogy = tmp_path / "missing-genealogy-parent" / "missing.sqlite"
    expected = registrar_definition_v6(contract=contract(), external_anchor=sample_anchor())
    assert cli._read_exact_genealogy_definition(missing_genealogy, expected) == (
        False, "canonical_append_only_genealogy_database_missing",
    )
    assert not missing_genealogy.exists()
    assert not missing_genealogy.parent.exists()


def test_interrupted_initialization_leaves_no_target_and_retry_succeeds(tmp_path, monkeypatch):
    registry = tmp_path / "retry.sqlite"
    original = core._atomic_install_no_replace

    def interrupted(_temporary, _target):
        raise RuntimeError("injected interruption before publish")

    monkeypatch.setattr(core, "_atomic_install_no_replace", interrupted)
    with pytest.raises(RuntimeError, match="injected interruption"):
        init_registry(registry)
    assert not registry.exists()
    assert list(tmp_path.glob(f".{registry.name}.*.init.sqlite")) == []

    monkeypatch.setattr(core, "_atomic_install_no_replace", original)
    init_registry(registry)
    assert registry_counts(registry)["registry_meta"] == 1


def test_concurrent_initialization_has_one_atomic_winner_and_valid_target(tmp_path):
    registry = tmp_path / "concurrent.sqlite"

    def initialize():
        try:
            init_registry(registry)
            return "created"
        except AfterCostV6Error as exc:
            return str(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _value: initialize(), range(2)))
    assert results.count("created") == 1
    assert len([value for value in results if "already exists" in value]) == 1
    assert registry_counts(registry) == {
        "registry_meta": 1,
        "identity_bindings": 0,
        "replay_bindings": 0,
        "replay_observations": 0,
        "identity_conflicts": 0,
    }


def test_crash_orphan_cannot_publish_partial_target_and_retry_is_safe(tmp_path):
    registry = tmp_path / "crash.sqlite"
    script = r"""
import json, os, sys
from pathlib import Path
from src.forex_system.research.currency_state_after_cost_counterfactual_v6 import _create_registry_temp_database
root = Path.cwd()
contract = json.loads((root / 'config/currency_state_after_cost_counterfactual_v6.json').read_text())
_create_registry_temp_database(
    Path(sys.argv[1]), contract=contract,
    manifest_id=contract['required_frozen_manifest_id'],
    manifest_file_sha256='1' * 64,
    manifest_semantic_sha256='2' * 64,
)
os._exit(23)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(registry)], cwd=ROOT, check=False,
    )
    assert completed.returncode == 23
    assert not registry.exists()
    orphans = list(tmp_path.glob(f".{registry.name}.*.init.sqlite"))
    assert len(orphans) == 1
    try:
        init_registry(registry)
        assert registry_counts(registry)["registry_meta"] == 1
    finally:
        for orphan in orphans:
            orphan.unlink(missing_ok=True)


def test_exact_canonical_genealogy_schema_pk_unique_cardinality_and_row(tmp_path):
    expected = registrar_definition_v6(contract=contract(), external_anchor=sample_anchor())
    genealogy = tmp_path / "genealogy.sqlite"
    create_genealogy(genealogy, [expected])
    assert cli._read_exact_genealogy_definition(genealogy, expected) == (
        True, "v6_genealogy_exact_schema_cardinality_and_definition_verified",
    )


def test_genealogy_missing_hypothesis_has_exact_zero_cardinality_state(tmp_path):
    expected = registrar_definition_v6(contract=contract(), external_anchor=sample_anchor())
    genealogy = tmp_path / "empty-genealogy.sqlite"
    create_genealogy(genealogy)
    assert cli._read_exact_genealogy_definition(genealogy, expected) == (
        False, "v6_genealogy_hypothesis_cardinality_not_exactly_one",
    )


def test_inert_same_name_genealogy_trigger_fails_exact_sql_check(tmp_path):
    expected = registrar_definition_v6(contract=contract(), external_anchor=sample_anchor())
    genealogy = tmp_path / "inert-genealogy.sqlite"
    create_genealogy(
        genealogy,
        [expected],
        trigger_overrides={
            "experiments_no_update": (
                "CREATE TRIGGER experiments_no_update BEFORE UPDATE ON experiments "
                "BEGIN SELECT 1; END"
            ),
        },
    )
    assert cli._read_exact_genealogy_definition(genealogy, expected) == (
        False, "wrong_genealogy_registry_exact_sql_schema",
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_duplicate_genealogy_rows_cannot_win_by_row_order(tmp_path, reverse):
    expected = registrar_definition_v6(contract=contract(), external_anchor=sample_anchor())
    changed = dict(expected)
    changed["selection_rule"] = "attacker-selected-first-row"
    rows = [expected, changed]
    if reverse:
        rows.reverse()
    genealogy = tmp_path / f"duplicates-{reverse}.sqlite"
    no_primary_key_sql = cli.GENEALOGY_TABLE_SQL["experiments"].replace(
        "hypothesis_id TEXT PRIMARY KEY", "hypothesis_id TEXT",
    )
    create_genealogy(genealogy, rows, experiments_sql=no_primary_key_sql)
    trusted, state = cli._read_exact_genealogy_definition(genealogy, expected)
    assert trusted is False
    assert state in {
        "wrong_genealogy_registry_exact_sql_schema",
        "wrong_genealogy_registry_pk_or_unique_index",
        "v6_genealogy_hypothesis_cardinality_not_exactly_one",
    }


def test_changed_canonical_definition_rehashed_still_fails_exact_23_column_row(tmp_path):
    expected = registrar_definition_v6(contract=contract(), external_anchor=sample_anchor())
    changed = dict(expected)
    decoded = json.loads(changed["definition_json"])
    decoded["can_place_orders"] = True
    changed["definition_json"] = canonical_json(decoded)
    from src.forex_system.contracts.currency_state import stable_hash
    changed["definition_sha256"] = stable_hash(decoded)
    genealogy = tmp_path / "changed-definition.sqlite"
    create_genealogy(genealogy, [changed])
    assert cli._read_exact_genealogy_definition(genealogy, expected) == (
        False, "v6_genealogy_exact_23_column_registrar_definition_mismatch",
    )


def test_cli_paths_are_hard_pinned_and_nonempty_operational_input_is_unconditional(tmp_path):
    signature = inspect.signature(cli.run)
    assert "genealogy_path" not in signature.parameters
    assert "identity_registry_path" not in signature.parameters
    with pytest.raises(TypeError):
        cli.run(genealogy_path=tmp_path / "alternate.sqlite")
    with pytest.raises(RuntimeError, match="unconditionally blocked"):
        cli.run(
            quote_path=tmp_path / "not-even-read.json",
            output_path=tmp_path / "output.json",
            report_path=tmp_path / "report.md",
        )
    assert not (tmp_path / "not-even-read.json").exists()


def test_real_manifest_pins_all_v6_and_immutable_ancestry_artifacts():
    frozen = json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v6_manifest.json").read_text()
    )
    cli.verify_manifest_files(frozen)
    assert frozen["artifacts"]["counterfactual_v5_manifest"]["sha256"] == (
        "ee7935d981357096f6a19470c7b3de49aa5e10085c60e8e02478a1e24d2a846c"
    )
    for label in (
        "counterfactual_v3_module", "counterfactual_v3_config",
        "counterfactual_v3_cli", "counterfactual_v3_manifest",
        "counterfactual_v4_module", "counterfactual_v4_config",
        "counterfactual_v4_cli", "counterfactual_v4_manifest",
        "counterfactual_v5_module", "counterfactual_v5_registrar",
        "counterfactual_v5_config", "counterfactual_v5_cli",
        "counterfactual_v5_manifest",
    ):
        row = frozen["artifacts"][label]
        assert cli.file_sha256(ROOT / row["relative_path"]) == row["sha256"]


def test_nonempty_core_requires_registry_and_replay_identity(tmp_path):
    r = response()
    q, v, e, a = valid_inputs(r)
    with pytest.raises(AfterCostV6Error, match="persistent identity registry"):
        build(r, q, v, e, a=a)
    registry = tmp_path / "identity.sqlite"
    init_registry(registry)
    with pytest.raises(AfterCostV6Error, match="replay_identity"):
        build(r, q, v, e, a=a, registry=registry)


def test_registry_update_delete_triggers_are_effective_not_merely_named(tmp_path):
    registry = tmp_path / "immutable.sqlite"
    init_registry(registry)
    connection = sqlite3.connect(registry)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only v6"):
            connection.execute("UPDATE registry_meta SET manifest_id='changed'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only v6"):
            connection.execute("DELETE FROM registry_meta")
    finally:
        connection.close()
