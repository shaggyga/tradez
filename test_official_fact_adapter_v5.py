from __future__ import annotations

import ast
import copy
import hashlib
import json
import math
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

try:
    import forex_system.features.currency_state_official_context_v5 as context_v5
    import forex_system.ingestion.official_fact_adapter_v5 as official_v5
    from forex_system.contracts.currency_state import stable_hash
    from forex_system.features.currency_state_official_context_v5 import (
        attach_official_fact_context_v5,
        validate_currency_state_official_context_v5_snapshot,
    )
    from forex_system.ingestion.official_fact_adapter_v5 import (
        OfficialFactAdapterV5,
        OfficialFactAdapterV5Error,
        PinnedArtifactSpec,
        read_pinned_artifact,
        validate_official_fact_v5_snapshot,
        verify_official_fact_v5_inputs,
    )
except ModuleNotFoundError:
    import src.forex_system.features.currency_state_official_context_v5 as context_v5
    import src.forex_system.ingestion.official_fact_adapter_v5 as official_v5
    from src.forex_system.contracts.currency_state import stable_hash
    from src.forex_system.features.currency_state_official_context_v5 import (
        attach_official_fact_context_v5,
        validate_currency_state_official_context_v5_snapshot,
    )
    from src.forex_system.ingestion.official_fact_adapter_v5 import (
        OfficialFactAdapterV5,
        OfficialFactAdapterV5Error,
        PinnedArtifactSpec,
        read_pinned_artifact,
        validate_official_fact_v5_snapshot,
        verify_official_fact_v5_inputs,
    )

from test_oanda_currency_state_official_context import base_snapshot


ROOT = Path(__file__).resolve().parent
CUTOFF = "2026-08-17T08:00:00+00:00"
REJECTED_PREDECESSOR_HASHES = {
    "src/forex_system/ingestion/official_fact_adapter_v3.py":
        "3a3e588ed2f71ec9d6f2b83a0ce0103d0b15bc45e4f542a38a09d17974597874",
    "src/forex_system/features/currency_state_official_context_v3.py":
        "1fa9a13a22eadc3aee3d8788099bba47b6af3f44ca29bdbe732af2c82eee4866",
    "src/forex_system/ingestion/official_fact_adapter_v4.py":
        "5254578ba5d53c14499f91b97f1fa3c370c17b762f3a5aea28df2c1406c0fba3",
    "src/forex_system/features/currency_state_official_context_v4.py":
        "5bd24fdc5776dded82291d10da62de30d0cbf274ca9586b8dff33a8c61cc0daf",
    "test_official_fact_adapter_v4.py":
        "52d33e287aa740c56245479741957e7989db5be7f935125de709d996d6d97ce1",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _aligned_base() -> dict:
    _, base = base_snapshot()
    base["decision_cutoff_utc"] = CUTOFF
    for horizon in base["horizons"].values():
        horizon["decision_cutoff_utc"] = CUTOFF
    identity = {
        key: base[key]
        for key in (
            "contract_id",
            "contract_sha256",
            "decision_cutoff_utc",
            "completed_bar_cutoff_utc",
            "input_identity",
            "horizons",
        )
    }
    base["snapshot_id"] = "currency_state_snapshot_" + stable_hash(identity)[:24]
    return base


def _rehash_official(snapshot: dict) -> None:
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    try:
        digest = stable_hash(material)[:24]
    except ValueError:
        # Non-finite mutations are intentionally not canonically hashable;
        # retain a syntactically valid ID so the strict numeric guard decides.
        digest = "0" * 24
    snapshot["snapshot_id"] = "official_fact_v5_snapshot_" + digest


def _rehash_context(snapshot: dict) -> None:
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    snapshot["snapshot_id"] = "currency_state_official_v5_" + stable_hash(material)[:24]


def _rehash_base(snapshot: dict) -> None:
    identity = {
        key: snapshot[key]
        for key in (
            "contract_id",
            "contract_sha256",
            "decision_cutoff_utc",
            "completed_bar_cutoff_utc",
            "input_identity",
            "horizons",
        )
    }
    snapshot["snapshot_id"] = "currency_state_snapshot_" + stable_hash(identity)[:24]


def _first_observable_edge(snapshot: dict) -> dict:
    return next(
        edge
        for horizon in snapshot["horizons"].values()
        for edge in horizon["pair_edges"].values()
        if edge["research_observable"] is True
    )


@pytest.fixture(scope="module")
def governed() -> dict:
    official = OfficialFactAdapterV5().as_of(CUTOFF)
    base = _aligned_base()
    context = attach_official_fact_context_v5(base, official)
    return {"official": official, "base": base, "context": context}


def test_v5_has_no_rejected_runtime_imports_and_imports_with_poisoned_modules() -> None:
    for module_path in (Path(official_v5.__file__), Path(context_v5.__file__)):
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
        assert all(not name.endswith(("official_fact_adapter_v3", "official_fact_adapter_v4")) for name in imported)
        assert all(not name.endswith(("currency_state_official_context_v3", "currency_state_official_context_v4")) for name in imported)
        assert all(not name.startswith(("requests", "httpx", "oandapy", "oandapyV20")) for name in imported)

    script = """
import sys
for name in (
    'src.forex_system.ingestion.official_fact_adapter_v3',
    'src.forex_system.ingestion.official_fact_adapter_v4',
    'src.forex_system.features.currency_state_official_context_v3',
    'src.forex_system.features.currency_state_official_context_v4',
):
    sys.modules[name] = None
import src.forex_system.ingestion.official_fact_adapter_v5
import src.forex_system.features.currency_state_official_context_v5
"""
    completed = subprocess.run(
        [sys.executable, "-c", script], cwd=ROOT, capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stderr


def test_rejected_v3_v4_bytes_are_preserved() -> None:
    for relative, expected in REJECTED_PREDECESSOR_HASHES.items():
        assert _sha256(ROOT / relative) == expected


def test_fixed_inputs_are_content_addressed_and_mutable_inputs_are_eliminated() -> None:
    evidence = verify_official_fact_v5_inputs()
    assert evidence["canonical_mutable_files_opened"] is False
    assert tuple(evidence["eliminated_mutable_inputs"]) == official_v5.ELIMINATED_MUTABLE_INPUTS
    assert set(evidence["structurally_eliminated_path_fields"]) == official_v5.ELIMINATED_PATH_FIELDS
    assert evidence["clock_v2"]["sha256"] == official_v5.ARCHIVED_CLOCK_V2_DATABASE_SHA256
    assert evidence["clock_v2"]["byte_length"] == official_v5.ARCHIVED_CLOCK_V2_DATABASE_BYTES
    assert evidence["clock_v2"]["row_counts"] == official_v5.ARCHIVED_CLOCK_V2_ROW_COUNTS
    assert evidence["clock_v2"]["quick_check"] == "ok"
    assert evidence["clock_v2"]["read_only"] is True
    assert evidence["clock_v2"]["link_count"] == 1
    assert evidence["policy_baseline"]["sha256"] == official_v5.FIXED_POLICY_BASELINE_SHA256
    assert evidence["intraday_rate_contract"]["sha256"] == official_v5.FIXED_INTRADAY_RATE_CONTRACT_SHA256


def test_adapter_paths_structurally_eliminate_all_mutable_and_fallback_inputs() -> None:
    adapter = OfficialFactAdapterV5()
    path_values = vars(adapter.paths)
    assert set(path_values).intersection(official_v5.ELIMINATED_PATH_FIELDS) == official_v5.ELIMINATED_PATH_FIELDS
    assert all(path_values[field] is None for field in official_v5.ELIMINATED_PATH_FIELDS)
    assert path_values["policy_baselines_json"] == official_v5.FIXED_POLICY_BASELINE
    assert path_values["immutable_event_clock_db"] == official_v5.ARCHIVED_CLOCK_V2_DATABASE
    assert path_values["intraday_rates_config_json"] == official_v5.FIXED_INTRADAY_RATE_CONTRACT


def test_inherited_readers_and_connect_are_unreachable(monkeypatch) -> None:
    def poison(*args, **kwargs):
        raise AssertionError("an inherited mutable reader was reached")

    for method in (
        "_connect",
        "_quarantined_source_events",
        "_macro_facts",
        "_rate_facts",
        "_expectation_facts",
        "_policy_facts",
        "_upcoming_events",
        "_source_health",
        "_clock_state",
        "_intraday_rate_state",
    ):
        monkeypatch.setattr(official_v5.OfficialFactAdapterV2, method, poison, raising=False)
    snapshot = OfficialFactAdapterV5().as_of(CUTOFF)
    assert snapshot["adapter_contract_id"] == official_v5.ADAPTER_CONTRACT_ID


def test_normal_build_cannot_attempt_any_eliminated_path(monkeypatch) -> None:
    banned = set(official_v5.ELIMINATED_MUTABLE_INPUTS)
    attempted: list[str] = []
    original_open = Path.open
    original_read_text = Path.read_text
    original_connect = official_v5.sqlite3.connect

    def check(value) -> None:
        text = str(value)
        attempted.append(text)
        assert all(name not in text for name in banned)

    def guarded_open(self, *args, **kwargs):
        check(self)
        return original_open(self, *args, **kwargs)

    def guarded_read_text(self, *args, **kwargs):
        check(self)
        return original_read_text(self, *args, **kwargs)

    def guarded_connect(database, *args, **kwargs):
        check(database)
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    monkeypatch.setattr(official_v5.sqlite3, "connect", guarded_connect)
    snapshot = OfficialFactAdapterV5().as_of(CUTOFF)
    assert snapshot["fact_count"] == 11
    assert attempted


def test_official_snapshot_is_deterministic_complete_and_valid(governed: dict) -> None:
    snapshot = governed["official"]
    assert snapshot == OfficialFactAdapterV5().as_of(CUTOFF)
    validate_official_fact_v5_snapshot(snapshot)
    assert set(snapshot) == set(official_v5.OFFICIAL_FACT_V5_TOP_LEVEL_FIELDS)
    assert snapshot["schema_version"] == 5
    assert snapshot["currency_count"] == 21
    assert tuple(snapshot["currency_evidence"]) == tuple(official_v5.CANONICAL_CURRENCIES)
    assert snapshot["fact_count"] == len(snapshot["facts"]) == 11
    assert snapshot["upcoming_event_count"] == len(snapshot["upcoming_events"]) == 25
    assert snapshot["macro_fact_record_count"] == 0
    assert snapshot["distinct_macro_observation_count"] == 0
    assert snapshot["causal_consensus_count"] == 0
    assert snapshot["quarantined_source_event_count"] == 0
    assert {item["fact_type"] for item in snapshot["facts"]} == {"official_policy_document_context"}
    assert snapshot["execution_eligible"] is False
    assert snapshot["can_place_orders"] is False
    assert snapshot["can_promote"] is False
    assert snapshot["can_authorize"] is False
    assert snapshot["supported_execution_decision"] == "no_trade"


def test_official_requires_exact_full_currency_universe() -> None:
    with pytest.raises(OfficialFactAdapterV5Error):
        OfficialFactAdapterV5().as_of(CUTOFF, currencies=["USD", "EUR"])


@pytest.mark.parametrize(
    "mutator",
    [
        lambda row: row.__setitem__("currency_count", True),
        lambda row: row.__setitem__("fact_count", row["fact_count"] + 1),
        lambda row: row.__setitem__("maximum_clock_observation_age_sec", math.nan),
        lambda row: row.__setitem__("can_authorize", True),
        lambda row: row.__setitem__("unexpected", "field"),
        lambda row: row["facts"].append(copy.deepcopy(row["facts"][0])),
        lambda row: row["upcoming_events"].append(copy.deepcopy(row["upcoming_events"][0])),
        lambda row: row["global_gaps"].append(copy.deepcopy(row["global_gaps"][0])),
    ],
)
def test_official_rejects_type_count_identity_and_schema_tampering(governed: dict, mutator) -> None:
    tampered = copy.deepcopy(governed["official"])
    mutator(tampered)
    _rehash_official(tampered)
    with pytest.raises(OfficialFactAdapterV5Error):
        validate_official_fact_v5_snapshot(tampered)


@pytest.mark.parametrize(
    "field,value",
    [
        ("published_at_utc", "2026-08-18T00:00:00+00:00"),
        ("first_seen_at_utc", "2026-08-01T00:00:00+00:00"),
        ("retrieved_at_utc", "2026-08-01T00:00:00+00:00"),
        ("effective_from_utc", "2026-08-18T00:00:00+00:00"),
    ],
)
def test_official_rejects_rehashed_fact_chronology_tampering(governed: dict, field: str, value: str) -> None:
    tampered = copy.deepcopy(governed["official"])
    tampered["facts"][0][field] = value
    _rehash_official(tampered)
    with pytest.raises(OfficialFactAdapterV5Error):
        validate_official_fact_v5_snapshot(tampered)


@pytest.mark.parametrize(
    "field,value",
    [
        ("known_from_snapshot_utc", "2026-08-18T00:00:00+00:00"),
        ("ledger_effective_known_utc", "2026-08-18T00:00:00+00:00"),
        ("schedule_window_end_utc", "2026-08-01T00:00:00+00:00"),
        ("execution_eligible", True),
        ("consensus_causal", True),
    ],
)
def test_official_rejects_rehashed_event_chronology_or_claim_tampering(governed: dict, field: str, value) -> None:
    tampered = copy.deepcopy(governed["official"])
    tampered["upcoming_events"][0][field] = value
    _rehash_official(tampered)
    with pytest.raises(OfficialFactAdapterV5Error):
        validate_official_fact_v5_snapshot(tampered)


def test_official_rejects_rehashed_clock_age_and_provenance_tampering(governed: dict) -> None:
    for field, value in (
        ("observation_age_sec", 0.0),
        ("snapshot_id", "event_clock_snapshot_" + "0" * 32),
        ("events_sha256", "0" * 64),
    ):
        tampered = copy.deepcopy(governed["official"])
        tampered["event_clock_provenance"][field] = value
        _rehash_official(tampered)
        with pytest.raises(OfficialFactAdapterV5Error):
            validate_official_fact_v5_snapshot(tampered)


def test_strict_json_rejects_duplicate_keys_and_nonfinite_values() -> None:
    with pytest.raises(OfficialFactAdapterV5Error, match="duplicate_json_key"):
        official_v5._strict_json(b'{"a": 1, "a": 2}', label="adversarial")
    with pytest.raises(OfficialFactAdapterV5Error, match="nonfinite_json"):
        official_v5._strict_json(b'{"a": NaN}', label="adversarial")


def test_pinned_reader_rejects_writable_hardlink_symlink_and_content_mismatch(tmp_path: Path) -> None:
    payload = b"fixed-evidence"
    source = tmp_path / "artifact.bin"
    source.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    spec = PinnedArtifactSpec(source, tmp_path, len(payload), digest, require_read_only=True)
    with pytest.raises(OfficialFactAdapterV5Error, match="not_read_only"):
        read_pinned_artifact(spec)

    os.chmod(source, stat.S_IREAD)
    try:
        assert read_pinned_artifact(spec)[0] == payload
        hardlink = tmp_path / "hardlink.bin"
        os.link(source, hardlink)
        with pytest.raises(OfficialFactAdapterV5Error, match="hardlink_count"):
            read_pinned_artifact(spec)
        os.chmod(hardlink, stat.S_IWRITE)
        hardlink.unlink()

        symlink = tmp_path / "symlink.bin"
        try:
            os.symlink(source, symlink)
        except OSError:  # pragma: no cover - Windows policy dependent
            symlink = None
        if symlink is not None:
            link_spec = PinnedArtifactSpec(symlink, tmp_path, len(payload), digest, require_read_only=False)
            with pytest.raises(OfficialFactAdapterV5Error):
                read_pinned_artifact(link_spec)

        bad_spec = PinnedArtifactSpec(source, tmp_path, len(payload), "0" * 64, require_read_only=False)
        with pytest.raises(OfficialFactAdapterV5Error, match="content_mismatch"):
            read_pinned_artifact(bad_spec)
    finally:
        os.chmod(source, stat.S_IWRITE)


def test_pinned_reader_rejects_simulated_identity_replacement(tmp_path: Path, monkeypatch) -> None:
    payload = b"stable-file"
    source = tmp_path / "artifact.bin"
    source.write_bytes(payload)
    spec = PinnedArtifactSpec(
        source, tmp_path, len(payload), hashlib.sha256(payload).hexdigest(), require_read_only=False
    )
    original = official_v5._stat_identity
    calls = 0

    def unstable(value):
        nonlocal calls
        calls += 1
        identity = original(value)
        if calls == 4:
            return (*identity[:-2], identity[-2] + 1, identity[-1])
        return identity

    monkeypatch.setattr(official_v5, "_stat_identity", unstable)
    with pytest.raises(OfficialFactAdapterV5Error, match="identity_changed"):
        read_pinned_artifact(spec)


def test_context_snapshot_is_deterministic_complete_and_valid(governed: dict) -> None:
    output = governed["context"]
    validate_currency_state_official_context_v5_snapshot(
        output, base_snapshot=governed["base"], official_snapshot=governed["official"]
    )
    assert output == attach_official_fact_context_v5(governed["base"], governed["official"])
    assert set(output) == set(context_v5.CONTEXT_V5_TOP_LEVEL_FIELDS) - {"generated_utc"}
    assert output["currency_count"] == 21
    assert output["instrument_count"] == 68
    assert tuple(output["horizons"]) == ("60", "300", "900", "1800", "3600")
    assert output["execution_eligible"] is False
    assert output["can_place_orders"] is False
    assert output["can_promote"] is False
    assert output["can_authorize"] is False
    assert output["supported_execution_decision"] == "no_trade"
    assert output["horizons"] == governed["base"]["horizons"] or all(
        set(output["horizons"][key]["currencies"]) == set(governed["base"]["horizons"][key]["currencies"])
        for key in output["horizons"]
    )
    for horizon in output["horizons"].values():
        assert len(horizon["currencies"]) == 21
        assert len(horizon["pair_edges"]) == 68


def test_context_preserves_every_base_field_except_explicit_official_attachments(governed: dict) -> None:
    base = governed["base"]
    output = governed["context"]
    for horizon_key, base_horizon in base["horizons"].items():
        out_horizon = output["horizons"][horizon_key]
        assert out_horizon["solver"] == base_horizon["solver"]
        assert out_horizon["rejected_observation_counts"] == base_horizon["rejected_observation_counts"]
        assert out_horizon["pair_edges"] == base_horizon["pair_edges"]
        for currency, base_currency in base_horizon["currencies"].items():
            stripped = copy.deepcopy(out_horizon["currencies"][currency])
            stripped.pop("official_fact_context")
            for field in context_v5._BASE_CURRENCY_FIELDS - {"components"}:
                assert stripped[field] == base_currency[field]
            base_components = {item["component_id"]: item for item in base_currency["components"]}
            output_components = {item["component_id"]: item for item in stripped["components"]}
            for component_id in ("semantic_analog", "positioning", "media_context", "latent_price_response"):
                assert output_components[component_id] == base_components[component_id]


def test_context_contains_no_forecast_ev_rank_or_basis_claim(governed: dict) -> None:
    output = governed["context"]
    basis = output["component_context"]["basis_eligible_fact_counts"]
    assert basis == {
        "causal_numeric_surprise": 0,
        "causal_rate_repricing": 0,
        "frozen_policy_statement_delta": 0,
        "versioned_semantic_thesis": 0,
    }
    assert output["component_context"]["direction_policy"] == "abstain"
    for summary in output["component_context"]["currency_summary"].values():
        assert summary["direction_policy"] == "abstain"
        assert summary["forecast_mean_bps"] is None
        assert summary["forecast_absolute_move_bps"] is None
        assert summary["cost_clear_probability"] is None
    for horizon in output["horizons"].values():
        for currency in horizon["currencies"].values():
            assert currency["forecast_mean_bps"] is None
            assert currency["forecast_absolute_move_bps"] is None
            assert currency["cost_clear_probability"] is None
            for component in currency["components"]:
                assert component["forecast_mean_bps"] is None
                assert component["forecast_absolute_move_bps"] is None
                assert component["cost_clear_probability"] is None
        for edge in horizon["pair_edges"].values():
            assert edge["forecast_mean_bps"] is None
            assert edge["forecast_absolute_move_bps"] is None
            assert edge["cost_clear_probability"] is None
            assert edge["expected_net_pips"] is None
            assert edge["allocator_rank"] is None
            assert edge["execution_eligible"] is False


@pytest.mark.parametrize(
    "mutator",
    [
        lambda row: row.__setitem__("currency_count", True),
        lambda row: row.__setitem__("can_authorize", True),
        lambda row: row.__setitem__("unexpected", "field"),
        lambda row: row["component_context"]["basis_eligible_fact_counts"].__setitem__("causal_numeric_surprise", 1),
        lambda row: next(iter(row["horizons"].values()))["pair_edges"]["AUD_CAD"].__setitem__("expected_net_pips", 1.0),
        lambda row: next(iter(row["horizons"].values()))["currencies"]["AUD"].__setitem__("forecast_mean_bps", 1.0),
        lambda row: row["component_context"]["currency_summary"]["AUD"].__setitem__("direction_policy", "buy"),
    ],
)
def test_context_rejects_rehashed_schema_type_basis_forecast_and_execution_tampering(governed: dict, mutator) -> None:
    tampered = copy.deepcopy(governed["context"])
    mutator(tampered)
    _rehash_context(tampered)
    with pytest.raises((ValueError, OfficialFactAdapterV5Error)):
        validate_currency_state_official_context_v5_snapshot(
            tampered, base_snapshot=governed["base"], official_snapshot=governed["official"]
        )


def test_context_rejects_wrong_or_mutated_base_and_official_identity(governed: dict) -> None:
    changed_base = copy.deepcopy(governed["base"])
    changed_base["horizons"]["60"]["currencies"]["AUD"]["observed_currency_return_bps"] = 999.0
    with pytest.raises((ValueError, OfficialFactAdapterV5Error)):
        validate_currency_state_official_context_v5_snapshot(
            governed["context"], base_snapshot=changed_base, official_snapshot=governed["official"]
        )
    changed_official = copy.deepcopy(governed["official"])
    changed_official["facts"][0]["text_excerpt"] += " forged"
    _rehash_official(changed_official)
    with pytest.raises((ValueError, OfficialFactAdapterV5Error)):
        validate_currency_state_official_context_v5_snapshot(
            governed["context"], base_snapshot=governed["base"], official_snapshot=changed_official
        )


@pytest.mark.parametrize("field", ["completed_bar_cutoff_utc", "event_time_watermark_utc"])
def test_base_rejects_knowledge_times_after_decision_cutoff(governed: dict, field: str) -> None:
    tampered = copy.deepcopy(governed["base"])
    future = "2026-08-18T08:00:00+00:00"
    tampered[field] = future
    if field == "completed_bar_cutoff_utc":
        for horizon in tampered["horizons"].values():
            horizon["completed_bar_cutoff_utc"] = future
    _rehash_base(tampered)
    with pytest.raises(ValueError, match="knowledge-time"):
        context_v5.validate_base_currency_state_identity(
            tampered, contract=context_v5.load_fixed_currency_state_contract()
        )


@pytest.mark.parametrize(
    "refs,identity,error",
    [
        (
            {"quote_history_sha256": "1" * 64},
            {"quote_history_sha256": "2" * 64},
            "input_refs and input_identity",
        ),
        (
            {"contract_sha256": "1" * 64},
            {"contract_sha256": "1" * 64},
            "fixed contract",
        ),
    ],
)
def test_base_rejects_input_identity_or_fixed_artifact_hash_mismatch(
    governed: dict, refs: dict, identity: dict, error: str
) -> None:
    tampered = copy.deepcopy(governed["base"])
    tampered["input_refs"] = refs
    tampered["input_identity"] = identity
    _rehash_base(tampered)
    with pytest.raises(ValueError, match=error):
        context_v5.validate_base_currency_state_identity(
            tampered, contract=context_v5.load_fixed_currency_state_contract()
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda edge: edge.__setitem__("start_epoch", 9_999_999_999),
        lambda edge: edge.__setitem__("end_epoch", 1),
        lambda edge: edge.__setitem__("actual_observation_duration_sec", -1),
        lambda edge: edge.__setitem__(
            "actual_observation_duration_sec", edge["actual_observation_duration_sec"] + 1
        ),
        lambda edge: edge.__setitem__("endpoint_age_sec", edge["endpoint_age_sec"] + 1.0),
        lambda edge: edge.__setitem__("start_alignment_sec", edge["start_alignment_sec"] + 1.0),
        lambda edge: edge.__setitem__("exact_horizon_observation", not edge["exact_horizon_observation"]),
        lambda edge: edge.__setitem__("research_observable", False),
    ],
)
def test_context_rejects_rehashed_edge_chronology_duration_and_horizon_tampering(
    governed: dict, mutator
) -> None:
    tampered = copy.deepcopy(governed["context"])
    mutator(_first_observable_edge(tampered))
    _rehash_context(tampered)
    with pytest.raises((ValueError, OfficialFactAdapterV5Error)):
        validate_currency_state_official_context_v5_snapshot(
            tampered, base_snapshot=governed["base"], official_snapshot=governed["official"]
        )


def test_context_rejects_mutually_consistent_1000_second_endpoint_age(governed: dict) -> None:
    tampered = copy.deepcopy(governed["context"])
    edge = next(
        item
        for item in tampered["horizons"]["3600"]["pair_edges"].values()
        if item["research_observable"] is True
    )
    completed = context_v5._timestamp(
        tampered["completed_bar_cutoff_utc"], path="test.completed_bar_cutoff_utc"
    )
    assert completed is not None
    completed_epoch = int(completed.timestamp())
    start_epoch = completed_epoch - edge["declared_horizon_sec"]
    end_epoch = completed_epoch - 1000
    assert start_epoch <= end_epoch
    edge["start_epoch"] = start_epoch
    edge["end_epoch"] = end_epoch
    edge["actual_observation_duration_sec"] = end_epoch - start_epoch
    edge["endpoint_age_sec"] = 1000.0
    edge["start_alignment_sec"] = 0.0
    edge["exact_horizon_observation"] = False
    _rehash_context(tampered)
    with pytest.raises(ValueError, match="endpoint age exceeds fixed contract ceiling"):
        validate_currency_state_official_context_v5_snapshot(
            tampered, base_snapshot=governed["base"], official_snapshot=governed["official"]
        )


def test_context_rejects_mutually_consistent_1000_second_start_alignment(governed: dict) -> None:
    tampered = copy.deepcopy(governed["context"])
    edge = _first_observable_edge(tampered)
    completed = context_v5._timestamp(
        tampered["completed_bar_cutoff_utc"], path="test.completed_bar_cutoff_utc"
    )
    assert completed is not None
    completed_epoch = int(completed.timestamp())
    end_epoch = completed_epoch
    start_epoch = completed_epoch - edge["declared_horizon_sec"] - 1000
    edge["start_epoch"] = start_epoch
    edge["end_epoch"] = end_epoch
    edge["actual_observation_duration_sec"] = end_epoch - start_epoch
    edge["endpoint_age_sec"] = 0.0
    edge["start_alignment_sec"] = 1000.0
    edge["exact_horizon_observation"] = False
    _rehash_context(tampered)
    with pytest.raises(ValueError, match="start alignment exceeds fixed contract ceiling"):
        validate_currency_state_official_context_v5_snapshot(
            tampered, base_snapshot=governed["base"], official_snapshot=governed["official"]
        )


def test_v5_contracts_and_manifests_are_disabled_unstarted_and_hash_complete() -> None:
    contract_paths = (
        ROOT / "config/official_fact_adapter_contract_v5.json",
        ROOT / "config/currency_state_official_context_contract_v5.json",
    )
    for path in contract_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        state = payload["state"]
        assert state["enabled"] is False
        assert state["registered"] is False
        assert state["research_worker_enabled"] is False
        assert state["collector_enabled"] is False
        assert state["candidate_frozen_utc"] is None
        assert state["independent_review_started_utc"] is None
        assert state["independent_review_completed_utc"] is None
        assert state["runtime_started_utc"] is None
        assert state["canonical_output_initialized"] is False
        assert payload["supported_execution_decision"] == "no_trade"
        assert payload["execution_eligible"] is False
        assert payload["can_place_orders"] is False
        assert payload["can_promote"] is False
        assert payload["can_authorize"] is False

    manifest_paths = (
        ROOT / "config/official_fact_adapter_v5_manifest.json",
        ROOT / "config/currency_state_official_context_v5_manifest.json",
    )
    for path in manifest_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["candidate_frozen_utc"] is None
        assert payload["runtime_started_utc"] is None
        state = payload["state"]
        assert state["enabled"] is False
        assert state["registered"] is False
        assert state["independent_review_started_utc"] is None
        assert state["independent_review_completed_utc"] is None
        assert state["canonical_output_initialized"] is False
        assert payload["policy"]["research_worker_enabled"] is False
        assert payload["policy"]["collector_enabled"] is False
        assert payload["policy"]["supported_execution_decision"] == "no_trade"
        artifact_paths = {
            artifact["relative_path"] for artifact in payload["artifacts"].values()
        }
        assert "src/forex_system/__init__.py" in artifact_paths
        assert "test_oanda_currency_state_official_context.py" in artifact_paths
        for artifact in payload["artifacts"].values():
            artifact_path = ROOT / artifact["relative_path"]
            assert artifact_path.stat().st_size == artifact["byte_length"]
            assert _sha256(artifact_path) == artifact["sha256"]
            if artifact_path.suffix == ".py":
                assert not artifact_path.stem.endswith(("_v3", "_v4"))
    official_manifest = json.loads(manifest_paths[0].read_text(encoding="utf-8"))
    assert official_manifest["policy"]["clock_archive_read_only_is_verified"] is True
    assert official_manifest["policy"]["fixed_json_read_only_is_not_claimed"] is True
    context_manifest = json.loads(manifest_paths[1].read_text(encoding="utf-8"))
    assert context_manifest["policy"]["fixed_contract_json_read_only_is_not_claimed"] is True
