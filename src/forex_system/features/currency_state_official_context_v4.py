"""Fixed-contract CurrencyState + OfficialFact V4 research composition.

The caller supplies the two data snapshots, but never a contract or a proof
claim.  V4 reopens the one approved CurrencyState contract by exact bytes,
independently validates the exact base snapshot and OfficialFact V4 snapshot,
rebuilds the entire composition, and accepts only exact equality with that
rebuild.  Official context remains unscored and cannot promote, authorize, or
execute.
"""

from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any, Mapping

from ..contracts.currency_state import stable_hash, validate_contract
from ..ingestion.immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
)
from ..ingestion.official_fact_adapter_v4 import (
    ADAPTER_CONTRACT_ID as OFFICIAL_FACT_ADAPTER_CONTRACT_ID,
    FACT_BASIS_ELIGIBILITY_CONTRACT_ID,
    PinnedArtifactSpec,
    _strict_json,
    read_pinned_artifact,
    validate_official_fact_v4_snapshot,
)
from .currency_state_official_context import (
    attach_official_fact_context as attach_official_fact_context_v1,
)
from .currency_state_official_context_v2 import OFFICIAL_BASIS_FIELDS
from .currency_state_official_context_v3 import (
    CONTEXT_CONTRACT_ID as V3_CONTEXT_CONTRACT_ID,
    CONTEXT_V3_TOP_LEVEL_FIELDS,
    FACT_BASIS_ELIGIBILITY_CONTRACT_ID as V3_FACT_BASIS_CONTRACT_ID,
    PARENT_CONTEXT_CONTRACT_ID as V2_CONTEXT_CONTRACT_ID,
    SNAPSHOT_SCHEMA as V3_SNAPSHOT_SCHEMA,
    STATUS as V3_STATUS,
    _validate_preserved_horizons,
    validate_base_currency_state_identity,
    validate_currency_state_official_context_v3_snapshot,
)


CONTEXT_CONTRACT_ID = "currency_state_official_context_v4_20260817"
PARENT_CONTEXT_CONTRACT_ID = V3_CONTEXT_CONTRACT_ID
PARENT_FACT_BASIS_ELIGIBILITY_CONTRACT_ID = V3_FACT_BASIS_CONTRACT_ID
SNAPSHOT_SCHEMA = "currency_state_official_context_snapshot_v4"
STATUS = "clock_v2_official_fact_v4_context_attached_unscored"
CONTEXT_V4_TOP_LEVEL_FIELDS = CONTEXT_V3_TOP_LEVEL_FIELDS

SOURCE_ROOT = Path(__file__).resolve().parents[3]
FIXED_CURRENCY_STATE_CONTRACT_PATH = (
    SOURCE_ROOT / "config" / "currency_state_engine_v2.json"
)
FIXED_CURRENCY_STATE_CONTRACT_BYTES = 3_272
FIXED_CURRENCY_STATE_CONTRACT_FILE_SHA256 = (
    "20138fe4328ee88b47b8edce2b3bd6081191cda35dd4124a9a457ea9b2efa400"
)
FIXED_CURRENCY_STATE_CONTRACT_MATERIAL_SHA256 = (
    "79ae9b93cd8facc16fd69586993d84235458fc35b1bfeda6ad7df6d047389a72"
)
_FIXED_CONTRACT_SPEC = PinnedArtifactSpec(
    path=FIXED_CURRENCY_STATE_CONTRACT_PATH,
    root=SOURCE_ROOT / "config",
    byte_length=FIXED_CURRENCY_STATE_CONTRACT_BYTES,
    sha256=FIXED_CURRENCY_STATE_CONTRACT_FILE_SHA256,
    require_read_only=False,
)


def load_fixed_currency_state_contract() -> dict[str, Any]:
    """Reopen exactly the one approved dependency contract."""

    payload_bytes, metadata = read_pinned_artifact(_FIXED_CONTRACT_SPEC)
    payload = _strict_json(payload_bytes, label="currency_state_engine_v2_contract")
    validate_contract(payload)
    material_hash = stable_hash(payload)
    if material_hash != FIXED_CURRENCY_STATE_CONTRACT_MATERIAL_SHA256:
        raise ValueError("fixed CurrencyState contract material hash mismatch")
    result = dict(payload)
    result["contract_sha256"] = material_hash
    result["contract_path"] = metadata["resolved_path"]
    return result


def _brand_v3(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(snapshot))
    result["snapshot_schema"] = V3_SNAPSHOT_SCHEMA
    result["component_context_contract_id"] = V3_CONTEXT_CONTRACT_ID
    result["parent_component_context_contract_id"] = V2_CONTEXT_CONTRACT_ID
    result["official_fact_adapter_contract_id"] = (
        "official_fact_adapter_v3_20260817"
    )
    result["status"] = V3_STATUS
    context = result["component_context"]
    context["fact_basis_eligibility_contract_id"] = V3_FACT_BASIS_CONTRACT_ID
    context["parent_fact_basis_eligibility_contract_id"] = (
        "official_fact_basis_eligibility_v2_20260817"
    )
    material = {key: value for key, value in result.items() if key != "snapshot_id"}
    result["snapshot_id"] = "currency_state_official_v3_" + stable_hash(material)[:24]
    return result


def _disable_unproved_basis(context: Mapping[str, Any]) -> None:
    metadata_by_id = context["fact_metadata_by_id"]
    for metadata in metadata_by_id.values():
        metadata["basis_eligibility"] = {
            basis: False for basis in sorted(OFFICIAL_BASIS_FIELDS)
        }
        metadata["proof_eligible_for_any_direction_basis"] = False
    context["basis_eligible_fact_counts"] = {
        basis: 0 for basis in sorted(OFFICIAL_BASIS_FIELDS)
    }


def _build_expected(
    base_snapshot: Mapping[str, Any], official_snapshot: Mapping[str, Any]
) -> dict[str, Any]:
    contract = load_fixed_currency_state_contract()
    base_identity = validate_base_currency_state_identity(
        base_snapshot, contract=contract
    )
    validate_official_fact_v4_snapshot(official_snapshot)

    output = attach_official_fact_context_v1(
        base_snapshot, official_snapshot, contract=contract
    )
    _validate_preserved_horizons(base_snapshot, output)
    output["snapshot_schema"] = SNAPSHOT_SCHEMA
    output["component_context_contract_id"] = CONTEXT_CONTRACT_ID
    output["parent_component_context_contract_id"] = PARENT_CONTEXT_CONTRACT_ID
    output["official_fact_adapter_contract_id"] = OFFICIAL_FACT_ADAPTER_CONTRACT_ID
    output["immutable_event_clock_schema_version"] = (
        IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
    )
    output["immutable_event_clock_contract_id"] = (
        IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    )
    output["base_currency_state_snapshot_sha256"] = base_identity[
        "snapshot_sha256"
    ]
    output["base_currency_state_horizons_sha256"] = base_identity[
        "horizons_sha256"
    ]
    context = output["component_context"]
    context["fact_basis_eligibility_contract_id"] = (
        FACT_BASIS_ELIGIBILITY_CONTRACT_ID
    )
    context["parent_fact_basis_eligibility_contract_id"] = (
        PARENT_FACT_BASIS_ELIGIBILITY_CONTRACT_ID
    )
    context["immutable_event_clock_contract_id"] = (
        IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    )
    context["event_clock_provenance"] = copy.deepcopy(
        official_snapshot["event_clock_provenance"]
    )
    _disable_unproved_basis(context)
    output["can_promote"] = False
    output["can_authorize"] = False
    output["status"] = STATUS
    material = {key: value for key, value in output.items() if key != "snapshot_id"}
    output["snapshot_id"] = "currency_state_official_v4_" + stable_hash(material)[:24]
    return output


def attach_official_fact_context_v4(
    base_snapshot: Mapping[str, Any], official_snapshot: Mapping[str, Any]
) -> dict[str, Any]:
    """Build the exact disabled composition; there is no contract parameter."""

    output = _build_expected(base_snapshot, official_snapshot)
    validate_currency_state_official_context_v4_snapshot(
        output,
        base_snapshot=base_snapshot,
        official_snapshot=official_snapshot,
    )
    return output


def validate_currency_state_official_context_v4_snapshot(
    snapshot: Mapping[str, Any],
    *,
    base_snapshot: Mapping[str, Any],
    official_snapshot: Mapping[str, Any],
) -> None:
    """Require separately supplied exact dependencies and deterministic rebuild."""

    contract = load_fixed_currency_state_contract()
    validate_base_currency_state_identity(base_snapshot, contract=contract)
    validate_official_fact_v4_snapshot(official_snapshot)
    if (
        type(snapshot) is not dict
        or set(snapshot) - set(CONTEXT_V4_TOP_LEVEL_FIELDS)
        or (set(CONTEXT_V4_TOP_LEVEL_FIELDS) - {"generated_utc"}) - set(snapshot)
    ):
        raise ValueError("CurrencyState Official Context V4 top level is not exact")
    literals = {
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "component_context_contract_id": CONTEXT_CONTRACT_ID,
        "parent_component_context_contract_id": PARENT_CONTEXT_CONTRACT_ID,
        "official_fact_adapter_contract_id": OFFICIAL_FACT_ADAPTER_CONTRACT_ID,
        "immutable_event_clock_schema_version": (
            IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
        ),
        "immutable_event_clock_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
        "status": STATUS,
        "supported_execution_decision": "no_trade",
    }
    for field, expected in literals.items():
        if type(snapshot.get(field)) is not str or snapshot.get(field) != expected:
            raise ValueError(f"CurrencyState Official Context V4 {field} mismatch")
    for field, expected in (
        ("research_only", True),
        ("execution_eligible", False),
        ("can_place_orders", False),
        ("can_promote", False),
        ("can_authorize", False),
    ):
        if type(snapshot.get(field)) is not bool or snapshot.get(field) is not expected:
            raise ValueError(f"CurrencyState Official Context V4 {field} mismatch")
    if snapshot.get("base_currency_state_snapshot_id") != base_snapshot.get(
        "snapshot_id"
    ) or snapshot.get("official_fact_snapshot_id") != official_snapshot.get(
        "snapshot_id"
    ):
        raise ValueError("CurrencyState Official Context V4 dependency ID mismatch")
    context = snapshot.get("component_context")
    if type(context) is not dict:
        raise ValueError("CurrencyState Official Context V4 context is not exact")
    if (
        context.get("fact_basis_eligibility_contract_id")
        != FACT_BASIS_ELIGIBILITY_CONTRACT_ID
        or context.get("parent_fact_basis_eligibility_contract_id")
        != PARENT_FACT_BASIS_ELIGIBILITY_CONTRACT_ID
        or context.get("immutable_event_clock_contract_id")
        != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    ):
        raise ValueError("CurrencyState Official Context V4 nested contract mismatch")

    # Reuse the complete V3 recursive bool/int/container/nonfinite closure after
    # projection, then require an independent exact V4 rebuild.
    v3_projection = _brand_v3(snapshot)
    validate_currency_state_official_context_v3_snapshot(
        v3_projection, contract=contract
    )
    expected = _build_expected(base_snapshot, official_snapshot)
    if dict(snapshot) != expected:
        raise ValueError(
            "CurrencyState Official Context V4 independent rebuild mismatch"
        )


__all__ = [
    "CONTEXT_CONTRACT_ID",
    "CONTEXT_V4_TOP_LEVEL_FIELDS",
    "FACT_BASIS_ELIGIBILITY_CONTRACT_ID",
    "FIXED_CURRENCY_STATE_CONTRACT_FILE_SHA256",
    "FIXED_CURRENCY_STATE_CONTRACT_MATERIAL_SHA256",
    "FIXED_CURRENCY_STATE_CONTRACT_PATH",
    "SNAPSHOT_SCHEMA",
    "attach_official_fact_context_v4",
    "load_fixed_currency_state_contract",
    "validate_currency_state_official_context_v4_snapshot",
]
