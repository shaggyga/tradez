from __future__ import annotations

import copy
import hashlib
import inspect
import json
import math
import os
import stat
import types
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

try:
    import forex_system.features.currency_state_official_context_v4 as context_v4
    import forex_system.ingestion.official_fact_adapter_v4 as official_v4
    from forex_system.contracts.currency_state import stable_hash
    from forex_system.features.currency_state_official_context_v4 import (
        attach_official_fact_context_v4,
        load_fixed_currency_state_contract,
        validate_currency_state_official_context_v4_snapshot,
    )
    from forex_system.ingestion.official_fact_adapter_v4 import (
        ADAPTER_CONTRACT_ID,
        ARCHIVED_CLOCK_V1_DATABASE,
        ARCHIVED_CLOCK_V1_DATABASE_SHA256,
        ARCHIVED_CLOCK_V1_RETIREMENT,
        ARCHIVED_CLOCK_V1_RETIREMENT_SHA256,
        ARCHIVED_CLOCK_V2_DATABASE,
        ARCHIVED_CLOCK_V2_DATABASE_SHA256,
        OfficialFactAdapterV4,
        OfficialFactAdapterV4Error,
        PinnedArtifactSpec,
        read_pinned_artifact,
        validate_official_fact_v4_snapshot,
        verify_official_fact_v4_archives,
    )
except ModuleNotFoundError:
    import src.forex_system.features.currency_state_official_context_v4 as context_v4
    import src.forex_system.ingestion.official_fact_adapter_v4 as official_v4
    from src.forex_system.contracts.currency_state import stable_hash
    from src.forex_system.features.currency_state_official_context_v4 import (
        attach_official_fact_context_v4,
        load_fixed_currency_state_contract,
        validate_currency_state_official_context_v4_snapshot,
    )
    from src.forex_system.ingestion.official_fact_adapter_v4 import (
        ADAPTER_CONTRACT_ID,
        ARCHIVED_CLOCK_V1_DATABASE,
        ARCHIVED_CLOCK_V1_DATABASE_SHA256,
        ARCHIVED_CLOCK_V1_RETIREMENT,
        ARCHIVED_CLOCK_V1_RETIREMENT_SHA256,
        ARCHIVED_CLOCK_V2_DATABASE,
        ARCHIVED_CLOCK_V2_DATABASE_SHA256,
        OfficialFactAdapterV4,
        OfficialFactAdapterV4Error,
        PinnedArtifactSpec,
        read_pinned_artifact,
        validate_official_fact_v4_snapshot,
        verify_official_fact_v4_archives,
    )

from test_oanda_currency_state_official_context import base_snapshot


ROOT = Path(__file__).resolve().parent
CUTOFF = "2026-08-17T23:59:59+00:00"


def _brand_official(snapshot: dict) -> None:
    official_v4._brand(snapshot, version=4)


def _brand_context(snapshot: dict) -> None:
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    snapshot["snapshot_id"] = "currency_state_official_v4_" + stable_hash(material)[:24]


def _aligned_base() -> dict:
    _, base = base_snapshot()
    base["decision_cutoff_utc"] = CUTOFF
    for horizon in base["horizons"].values():
        horizon["decision_cutoff_utc"] = CUTOFF
    identity = {
        "contract_id": base["contract_id"],
        "contract_sha256": base["contract_sha256"],
        "decision_cutoff_utc": base["decision_cutoff_utc"],
        "completed_bar_cutoff_utc": base["completed_bar_cutoff_utc"],
        "input_identity": base["input_identity"],
        "horizons": base["horizons"],
    }
    base["snapshot_id"] = "currency_state_snapshot_" + stable_hash(identity)[:24]
    return base


class _FrozenParent:
    def __init__(self, parent: dict) -> None:
        self.parent = copy.deepcopy(parent)

    def as_of(self, cutoff, *, currencies):
        assert cutoff == CUTOFF
        return copy.deepcopy(self.parent)


@contextmanager
def _frozen_parent(official: dict):
    parent = official_v4._project(official, version=2)
    with patch.object(
        official_v4, "_new_v2_parent", return_value=_FrozenParent(parent)
    ):
        yield


@pytest.fixture(scope="module")
def governed() -> dict:
    last_error: Exception | None = None
    for _ in range(8):
        try:
            official = OfficialFactAdapterV4().as_of(CUTOFF)
            base = _aligned_base()
            with _frozen_parent(official):
                context = attach_official_fact_context_v4(base, official)
            return {"official": official, "base": base, "context": context}
        except OfficialFactAdapterV4Error as exc:
            # Live non-clock diagnostics can advance between two independent
            # full reads.  V4 correctly fails closed; retry selects a stable
            # instant without weakening any comparison.
            last_error = exc
    raise AssertionError(f"unable to obtain stable V4 fixture: {last_error}")


def test_archive_copies_are_distinct_read_only_content_addressed_evidence() -> None:
    evidence = verify_official_fact_v4_archives()
    assert evidence["canonical_files_opened"] is False
    assert evidence["clock_v1_database"]["sha256"] == ARCHIVED_CLOCK_V1_DATABASE_SHA256
    assert evidence["clock_v1_retirement"]["sha256"] == ARCHIVED_CLOCK_V1_RETIREMENT_SHA256
    assert evidence["clock_v2_database"]["sha256"] == ARCHIVED_CLOCK_V2_DATABASE_SHA256
    for item in (
        evidence["clock_v1_database"],
        evidence["clock_v1_retirement"],
        evidence["clock_v2_database"],
    ):
        assert item["read_only"] is True
        assert item["link_count"] == 1
        assert "source_archives" in item["resolved_path"]
    assert ARCHIVED_CLOCK_V1_DATABASE != ROOT / "data/oanda_training_manager/research_ledgers/immutable_event_clock_v1.sqlite"
    assert ARCHIVED_CLOCK_V2_DATABASE != ROOT / "data/oanda_training_manager/research_ledgers/immutable_event_clock_v2.sqlite"


def test_canonical_clock_bytes_remain_unchanged() -> None:
    expected = {
        ROOT / "data/oanda_training_manager/research_ledgers/immutable_event_clock_v1.sqlite": ARCHIVED_CLOCK_V1_DATABASE_SHA256,
        ROOT / "config/immutable_event_clock_v1_retirement.json": ARCHIVED_CLOCK_V1_RETIREMENT_SHA256,
        ROOT / "data/oanda_training_manager/research_ledgers/immutable_event_clock_v2.sqlite": ARCHIVED_CLOCK_V2_DATABASE_SHA256,
    }
    for path, digest in expected.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest


def test_official_v4_valid_snapshot_is_full_universe_and_no_trade(governed) -> None:
    official = governed["official"]
    with _frozen_parent(official):
        validate_official_fact_v4_snapshot(official)
    assert official["adapter_contract_id"] == ADAPTER_CONTRACT_ID
    assert official["currency_count"] == 21
    assert official["causal_consensus_count"] == 0
    assert official["can_promote"] is False
    assert official["can_authorize"] is False
    assert official["supported_execution_decision"] == "no_trade"


def test_context_v4_valid_snapshot_recomputes_all_basis_false(governed) -> None:
    with _frozen_parent(governed["official"]):
        validate_currency_state_official_context_v4_snapshot(
            governed["context"],
            base_snapshot=governed["base"],
            official_snapshot=governed["official"],
        )
    context = governed["context"]["component_context"]
    assert set(context["basis_eligible_fact_counts"].values()) == {0}
    assert all(
        metadata["proof_eligible_for_any_direction_basis"] is False
        and set(metadata["basis_eligibility"].values()) == {False}
        for metadata in context["fact_metadata_by_id"].values()
    )


def test_fact_timestamps_after_cutoff_are_rejected_even_when_rebranded(governed) -> None:
    snapshot = copy.deepcopy(governed["official"])
    fact = snapshot["facts"][0]
    fact["first_seen_at_utc"] = "2030-01-01T00:00:00+00:00"
    _brand_official(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises(OfficialFactAdapterV4Error):
        validate_official_fact_v4_snapshot(snapshot)


def test_forged_ready_clock_future_timestamps_ages_and_hashes_are_rejected(governed) -> None:
    snapshot = copy.deepcopy(governed["official"])
    provenance = snapshot["event_clock_provenance"]
    provenance.update(
        {
            "state": "immutable_v2_fresh_attested_at_cutoff",
            "clock_ready_for_cutoff": True,
            "complete_snapshot_at_cutoff": True,
            "snapshot_captured_utc": "2027-01-01T00:00:00+00:00",
            "clock_observed_utc": "2030-01-01T00:00:00+00:00",
            "events_sha256": "a" * 64,
            "manifest_sha256": "b" * 64,
            "observation_age_sec": 0.0,
        }
    )
    provenance.pop("degradation_reason", None)
    provenance["clock_attestation"]["age_at_capture_sec"] = 999999.0
    _brand_official(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises((OfficialFactAdapterV4Error, ValueError)):
        validate_official_fact_v4_snapshot(snapshot)


def test_causal_consensus_without_pre_release_proof_is_rejected(governed) -> None:
    snapshot = copy.deepcopy(governed["official"])
    fact = next(row for row in snapshot["facts"] if row["fact_type"] == "official_macro_actual")
    fact["consensus_causal"] = True
    fact["consensus_value"] = None
    snapshot["causal_consensus_count"] = 1
    snapshot["currency_evidence"][fact["currency"]]["causal_consensus_count"] = 1
    _brand_official(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises(OfficialFactAdapterV4Error):
        validate_official_fact_v4_snapshot(snapshot)


@pytest.mark.parametrize("invalid", [True, float("nan"), {"x": 1}, ("x",)])
def test_official_recursive_strict_closure_rejects_bool_nonfinite_and_containers(
    governed, invalid
) -> None:
    snapshot = copy.deepcopy(governed["official"])
    if invalid is True:
        snapshot["fact_count"] = invalid
    elif isinstance(invalid, float):
        fact = next(row for row in snapshot["facts"] if row["fact_type"] == "official_macro_actual")
        fact["actual_value"] = invalid
        with pytest.raises((OfficialFactAdapterV4Error, RuntimeError, ValueError)):
            _brand_official(snapshot)
        return
    else:
        snapshot["facts"][0]["degradation_reasons"] = invalid
    _brand_official(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises((OfficialFactAdapterV4Error, ValueError)):
        validate_official_fact_v4_snapshot(snapshot)


def test_quarantine_count_cannot_be_caller_supplied(governed) -> None:
    snapshot = copy.deepcopy(governed["official"])
    snapshot["quarantined_source_event_count"] = 999
    _brand_official(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises(OfficialFactAdapterV4Error):
        validate_official_fact_v4_snapshot(snapshot)


def test_context_forged_basis_flags_are_recomputed_and_rejected(governed) -> None:
    snapshot = copy.deepcopy(governed["context"])
    metadata = next(iter(snapshot["component_context"]["fact_metadata_by_id"].values()))
    metadata["basis_eligibility"]["versioned_semantic_thesis"] = True
    metadata["proof_eligible_for_any_direction_basis"] = True
    snapshot["component_context"]["basis_eligible_fact_counts"]["versioned_semantic_thesis"] = 1
    _brand_context(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises(ValueError):
        validate_currency_state_official_context_v4_snapshot(
            snapshot,
            base_snapshot=governed["base"],
            official_snapshot=governed["official"],
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("base_currency_state_snapshot_sha256", "a" * 64),
        ("base_currency_state_horizons_sha256", "b" * 64),
        ("official_fact_snapshot_id", "official_fact_v4_snapshot_forged"),
    ],
)
def test_context_arbitrary_dependency_lineage_is_rejected(
    governed, field, value
) -> None:
    snapshot = copy.deepcopy(governed["context"])
    snapshot[field] = value
    _brand_context(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises(ValueError):
        validate_currency_state_official_context_v4_snapshot(
            snapshot,
            base_snapshot=governed["base"],
            official_snapshot=governed["official"],
        )


def test_context_caller_contract_and_missing_dependencies_are_forbidden(governed) -> None:
    with pytest.raises(TypeError):
        attach_official_fact_context_v4(
            governed["base"], governed["official"], contract={"schema_version": True}
        )
    with pytest.raises(TypeError):
        validate_currency_state_official_context_v4_snapshot(governed["context"])


def test_context_upcoming_event_identity_and_count_are_rebuilt(governed) -> None:
    snapshot = copy.deepcopy(governed["context"])
    currency = "AUD"
    summary = snapshot["component_context"]["currency_summary"][currency]
    summary["upcoming_event_count"] = 1
    summary["upcoming_event_ids"] = ["forged_event"]
    snapshot["component_context"]["upcoming_event_count"] = 1
    for horizon in snapshot["horizons"].values():
        horizon["currencies"][currency]["official_fact_context"] = copy.deepcopy(summary)
    _brand_context(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises(ValueError):
        validate_currency_state_official_context_v4_snapshot(
            snapshot,
            base_snapshot=governed["base"],
            official_snapshot=governed["official"],
        )


def test_context_strict_bool_int_closure(governed) -> None:
    snapshot = copy.deepcopy(governed["context"])
    snapshot["currency_count"] = True
    _brand_context(snapshot)
    with _frozen_parent(governed["official"]), pytest.raises(ValueError):
        validate_currency_state_official_context_v4_snapshot(
            snapshot,
            base_snapshot=governed["base"],
            official_snapshot=governed["official"],
        )


def test_fixed_currency_state_contract_is_exact_and_cannot_inject_execution() -> None:
    contract = load_fixed_currency_state_contract()
    assert contract["contract_sha256"] == context_v4.FIXED_CURRENCY_STATE_CONTRACT_MATERIAL_SHA256
    assert contract["schema_version"] == 1 and type(contract["schema_version"]) is int
    assert contract["research_only"] is True
    assert contract["execution_eligible"] is False
    assert contract["can_place_orders"] is False
    assert not {"trade", "side", "units"}.intersection(contract)


def _readonly(path: Path) -> None:
    os.chmod(path, stat.S_IREAD)


def _spec(path: Path, payload: bytes, *, readonly: bool = True) -> PinnedArtifactSpec:
    return PinnedArtifactSpec(
        path=path.resolve(),
        root=path.parent.resolve(),
        byte_length=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        require_read_only=readonly,
    )


def test_pinned_reader_rejects_writable_and_hardlinked_files(tmp_path: Path) -> None:
    payload = b"immutable-proof"
    writable = tmp_path / "writable.bin"
    writable.write_bytes(payload)
    with pytest.raises(OfficialFactAdapterV4Error, match="not_read_only"):
        read_pinned_artifact(_spec(writable, payload))

    origin = tmp_path / "origin.bin"
    linked = tmp_path / "linked.bin"
    origin.write_bytes(payload)
    os.link(origin, linked)
    _readonly(origin)
    _readonly(linked)
    with pytest.raises(OfficialFactAdapterV4Error, match="hardlink"):
        read_pinned_artifact(_spec(linked, payload))


def test_pinned_reader_rejects_symlink_or_reparse_path(tmp_path: Path) -> None:
    payload = b"immutable-proof"
    target = tmp_path / "target.bin"
    linked = tmp_path / "alias.bin"
    target.write_bytes(payload)
    _readonly(target)
    try:
        linked.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation unavailable on this Windows host")
    with pytest.raises(OfficialFactAdapterV4Error):
        read_pinned_artifact(_spec(linked, payload))


def test_pinned_reader_detects_fstat_race(tmp_path: Path, monkeypatch) -> None:
    payload = b"immutable-proof"
    path = tmp_path / "race.bin"
    path.write_bytes(payload)
    _readonly(path)
    real_fstat = official_v4.os.fstat
    calls = 0

    def racing_fstat(fd):
        nonlocal calls
        calls += 1
        value = real_fstat(fd)
        if calls != 2:
            return value
        fields = {
            name: getattr(value, name)
            for name in (
                "st_dev",
                "st_ino",
                "st_mode",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_file_attributes",
            )
        }
        fields["st_size"] += 1
        return types.SimpleNamespace(**fields)

    monkeypatch.setattr(official_v4.os, "fstat", racing_fstat)
    with pytest.raises(OfficialFactAdapterV4Error, match="identity_changed"):
        read_pinned_artifact(_spec(path, payload))


def test_windows_guard_denies_path_replacement_for_consumer_lifetime(tmp_path: Path) -> None:
    if os.name != "nt":
        pytest.skip("Windows share-mode proof lock is platform-specific")
    payload = b"immutable-proof"
    path = tmp_path / "guarded.bin"
    replacement = tmp_path / "replacement.bin"
    path.write_bytes(payload)
    replacement.write_bytes(payload)
    spec = _spec(path, payload, readonly=False)
    with official_v4.hold_pinned_artifact(spec):
        with pytest.raises(OSError):
            os.replace(replacement, path)
        assert path.read_bytes() == payload


def test_v4_modules_expose_no_registration_authorization_or_execution_api() -> None:
    forbidden = {"register", "promote", "authorize", "execute", "submit_order"}
    for module in (official_v4, context_v4):
        public = {name.lower() for name in module.__all__}
        assert not public.intersection(forbidden)
        source = inspect.getsource(module)
        assert "oandapy" not in source.lower()


def test_v4_contracts_are_disabled_unregistered_and_review_timestamps_null() -> None:
    for relative in (
        "config/official_fact_adapter_contract_v4.json",
        "config/currency_state_official_context_contract_v4.json",
    ):
        payload = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        state = payload["state"]
        assert state["enabled"] is False
        assert state["registered"] is False
        assert state["canonical_output_initialized"] is False
        assert state["independent_review_state"] == "pending"
        assert state["independent_review_started_utc"] is None
        assert state["independent_review_completed_utc"] is None
        assert payload["research_only"] is True
        assert payload["execution_eligible"] is False
        assert payload["can_place_orders"] is False
        assert payload["can_promote"] is False
        assert payload["can_authorize"] is False
        assert payload["supported_execution_decision"] == "no_trade"


def test_v4_manifests_are_disabled_and_pin_every_declared_artifact() -> None:
    for relative in (
        "config/official_fact_adapter_v4_manifest.json",
        "config/currency_state_official_context_v4_manifest.json",
    ):
        payload = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        state = payload["state"]
        assert state["enabled"] is False
        assert state["registered"] is False
        assert state["canonical_output_initialized"] is False
        assert state["independent_review_state"] == "pending"
        assert state["independent_review_started_utc"] is None
        assert state["independent_review_completed_utc"] is None
        for artifact in payload["artifacts"].values():
            path = ROOT / artifact["relative_path"]
            body = path.read_bytes()
            assert len(body) == artifact["byte_length"], artifact["relative_path"]
            assert hashlib.sha256(body).hexdigest() == artifact["sha256"], artifact[
                "relative_path"
            ]


def test_v4_has_no_canonical_output_database_or_live_wiring() -> None:
    forbidden_outputs = (
        ROOT / "data/oanda_training_manager/research_ledgers/official_fact_adapter_v4.sqlite",
        ROOT / "data/oanda_training_manager/state/official_fact_adapter_v4.sqlite",
        ROOT / "data/oanda_training_manager/state/currency_state_official_context_v4.sqlite",
    )
    assert not any(path.exists() for path in forbidden_outputs)
    with pytest.raises(TypeError):
        OfficialFactAdapterV4(paths={})
    with pytest.raises(OfficialFactAdapterV4Error):
        OfficialFactAdapterV4().as_of(CUTOFF, currencies=["USD", "JPY"])
