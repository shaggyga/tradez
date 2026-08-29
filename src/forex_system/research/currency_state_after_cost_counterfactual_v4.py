"""Atomic proof-integrity wrapper for the frozen after-cost v3 engine.

V4 is a new research cohort.  It never mutates or reinterprets v3 evidence.
Every supplied input envelope is either admitted whole or withheld whole before
the frozen v3 calculation engine sees it.  Identity collisions, any malformed
account constituent, mixed-validity records, and never-forward hold arms fail
closed without an input-order winner.
"""

from __future__ import annotations

import copy
import math
import re
from collections import Counter
from typing import Any, Mapping, Sequence

from .currency_state_after_cost_counterfactual_v2 import (
    AfterCostV2Error,
    _response_grid,
    _validate_manifest,
    validate_contract,
)
from .currency_state_after_cost_counterfactual_v3 import (
    AfterCostV3Error,
    ENVELOPES,
    _account_records,
    _record_id,
    _strict_envelope,
    build_after_cost_counterfactual_v3,
)
from ..contracts.currency_state import stable_hash
from ..features.currency_state_engine import parse_epoch


SHA256 = re.compile(r"^[0-9a-f]{64}$")


class AfterCostV4Error(ValueError):
    pass


EXPECTED_ARTIFACT_PATHS = {
    "counterfactual_v4_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v4.py",
    "counterfactual_v4_config": "config/currency_state_after_cost_counterfactual_v4.json",
    "counterfactual_v4_cli": "oanda_currency_state_after_cost_counterfactual_v4.py",
    "counterfactual_v3_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v3.py",
    "counterfactual_v3_config": "config/currency_state_after_cost_counterfactual_v3.json",
    "counterfactual_v3_cli": "oanda_currency_state_after_cost_counterfactual_v3.py",
    "counterfactual_v3_manifest": "config/currency_state_after_cost_counterfactual_v3_manifest.json",
    "counterfactual_v2_engine_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v2.py",
    "input_envelope_module": "src/forex_system/research/after_cost_input_envelopes_v3.py",
    "input_envelope_config": "config/after_cost_input_envelopes_v3.json",
    "response_module": "src/forex_system/research/currency_state_response_timing_arms.py",
    "response_config": "config/currency_state_response_timing_arms_v1.json",
    "response_cli": "oanda_currency_state_response_timing_arms.py",
    "currency_state_module": "src/forex_system/features/currency_state_engine.py",
    "currency_state_config": "config/currency_state_engine_v2.json",
    "currency_state_cli": "oanda_currency_state_engine.py",
    "official_context_module": "src/forex_system/features/currency_state_official_context.py",
    "official_context_cli": "oanda_currency_state_official_context.py",
    "official_fact_adapter_module": "src/forex_system/ingestion/official_fact_adapter.py",
    "official_fact_adapter_cli": "oanda_official_fact_adapter.py",
    "official_fact_adapter_contract_manifest": "config/official_fact_adapter_contract_v1.json",
    "signed_exposure_module": "src/forex_system/contracts/signed_currency_exposure.py",
    "independent_verifier_producer": "oanda_independent_evidence_verifier.py",
    "quote_producer": "oanda_practice_quote_stream.py",
    "account_producer": "oanda_account_snapshot_writer.py",
}


def _validate_contracts(
    contract: Mapping[str, Any],
    upstream_contract: Mapping[str, Any],
) -> None:
    try:
        validate_contract(contract)
    except AfterCostV2Error as exc:
        raise AfterCostV4Error(str(exc)) from exc
    if (
        contract.get("contract_id") != "currency_state_after_cost_counterfactual_v4_20260817"
        or contract.get("counterfactual_cohort_id") != "currency_state_after_cost_counterfactual_cohort_20260817d"
        or contract.get("supersedes_contract_id") != contract.get("required_upstream_v3_contract_id")
        or contract.get("supersedes_cohort_id") != contract.get("required_upstream_v3_cohort_id")
    ):
        raise AfterCostV4Error("v4 contract/cohort/parent identity mismatch")
    if (
        upstream_contract.get("contract_id") != contract["required_upstream_v3_contract_id"]
        or upstream_contract.get("counterfactual_cohort_id") != contract["required_upstream_v3_cohort_id"]
        or upstream_contract.get("required_frozen_manifest_id") != contract["required_upstream_v3_manifest_id"]
    ):
        raise AfterCostV4Error("frozen upstream v3 identity mismatch")
    shared = (
        "expected_instruments", "instrument_universe_sha256", "expected_horizons_sec",
        "expected_arm_ids", "forward_admissible_arm_ids", "never_forward_arm_ids",
        "required_response_contract_id", "required_response_snapshot_schema",
        "required_state_contract_id", "required_basis_eligibility_contract_id",
    )
    if any(contract.get(field) != upstream_contract.get(field) for field in shared):
        raise AfterCostV4Error("v4 changed a frozen upstream grid or response contract")
    if contract.get("account_policy") != {
        "maximum_open_positions_supported": 1,
        "multiple_open_positions_fail_closed": True,
        "position_and_ledger_semantic_inventory_must_match_exactly": True,
    }:
        raise AfterCostV4Error("v4 single-position account policy changed")
    if contract.get("operational_input_policy") != {
        "mode": "zero_input_engineering_initialization_only",
        "producer_integration_state": "producer_integration_missing",
        "nonempty_operational_input_requires_separately_reviewed_new_cohort": True,
        "core_fixture_inputs_are_test_only": True,
    }:
        raise AfterCostV4Error("v4 engineering-blocked operational input policy changed")
    rotation = contract.get("rotation_close_contract") or {}
    if (
        rotation.get("required_when_open_position_count_is_one") is not True
        or rotation.get("missing_or_invalid_close_economics_fail_closed") is not True
        or rotation.get("recomputed_cost_formula")
        != "executable_half_spread_bps_plus_nonnegative_slippage_bps_plus_nonnegative_latency_bps"
        or float(rotation.get("numeric_tolerance_bps", -1)) != 1e-9
    ):
        raise AfterCostV4Error("v4 executable rotation-close policy changed")


def _validate_v4_manifest(
    manifest: Mapping[str, Any], contract: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        normalized = _validate_manifest(manifest, contract)
    except AfterCostV2Error as exc:
        raise AfterCostV4Error(str(exc)) from exc
    actual_paths = {
        label: str(row.get("relative_path") or "").replace("\\", "/")
        for label, row in (manifest.get("artifacts") or {}).items()
    }
    if actual_paths != EXPECTED_ARTIFACT_PATHS:
        raise AfterCostV4Error("v4 manifest label-to-path map is not the externally specified map")
    return normalized


def _submitted_lineage(
    envelopes: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Any]:
    envelope_rows: dict[str, Any] = {}
    record_rows: dict[str, Any] = {}
    for name in ENVELOPES:
        envelope = envelopes.get(name)
        if envelope is None:
            continue
        envelope_rows[name] = {
            "producer_contract_id": envelope["producer_contract_id"],
            "cohort_id": envelope["cohort_id"],
            "payload_id": envelope["payload_id"],
            "canonical_envelope_sha256": envelope["canonical_envelope_sha256"],
            "payload_id_authoritative": False,
        }
        record_rows[name] = [
            {
                "index": index,
                "record_id": _record_id(record, name),
                "canonical_record_sha256": record["canonical_record_sha256"],
            }
            for index, record in enumerate(envelope.get("records") or [])
        ]
    return {"envelopes": envelope_rows, "records": record_rows}


def _identity_values(name: str, record: Mapping[str, Any]) -> list[tuple[str, Any]]:
    if name == "venue_quotes":
        return [("quote_record_id", str(record.get("quote_record_id") or "")), ("instrument", str(record.get("instrument") or ""))]
    if name == "verifier_evidence":
        return [
            ("verifier_evidence_id", str(record.get("verifier_evidence_id") or "")),
            ("forecast_record_id", str(record.get("forecast_record_id") or "")),
            ("arm_instrument_horizon", (record.get("arm_id"), record.get("instrument"), record.get("horizon_sec"))),
        ]
    if name == "economics_inputs":
        return [
            ("economics_record_id", str(record.get("economics_record_id") or "")),
            ("forecast_record_id", str(record.get("forecast_record_id") or "")),
            ("arm_instrument_horizon", (record.get("arm_id"), record.get("instrument"), record.get("horizon_sec"))),
        ]
    if name == "hold_switch_inputs":
        return [
            ("hold_record_id", str(record.get("hold_record_id") or "")),
            ("position_bound_arm_horizon", (
                record.get("position_record_id"), record.get("ledger_record_id"),
                record.get("arm_id"), record.get("horizon_sec"),
            )),
        ]
    values: list[tuple[str, Any]] = [("account_state_record_id", str(record.get("account_state_record_id") or ""))]
    if record.get("record_type") in {"position", "ledger"}:
        values.append((
            f"{record.get('record_type')}_position_trade_instrument",
            (record.get("position_id"), record.get("trade_id"), record.get("instrument")),
        ))
    return values


def _collision_rejections(
    name: str, records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    values: dict[tuple[str, str], list[tuple[int, Mapping[str, Any]]]] = {}
    for index, record in enumerate(records):
        for identity, value in _identity_values(name, record):
            if value in {"", None}:
                continue
            encoded = stable_hash(value)
            values.setdefault((identity, encoded), []).append((index, record))
    rejected: list[dict[str, Any]] = []
    for (identity, identity_hash), members in values.items():
        if len(members) < 2:
            continue
        for index, record in members:
            rejected.append({
                "kind": f"{name}_v4", "envelope": name,
                "record_id": _record_id(record, name), "record_index": index,
                "canonical_record_sha256": record["canonical_record_sha256"],
                "reason": f"atomic_{identity}_collision",
                "collision_identity_sha256": identity_hash,
            })
    return sorted(rejected, key=lambda row: (
        row["reason"], row["collision_identity_sha256"],
        row["canonical_record_sha256"], row["record_index"],
    ))


def _strict_number(value: Any, *, integer: bool = False) -> bool:
    if isinstance(value, bool):
        return False
    if integer:
        return isinstance(value, int)
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _account_rejections(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not records:
        return []
    _, upstream_rejections = _account_records(records)
    reasons = [str(row["reason"]) for row in upstream_rejections]
    by_type = Counter(str(row.get("record_type") or "") for row in records)
    if by_type["account"] != 1 or by_type["capacity"] != 1 or by_type["conflict"] != 1:
        reasons.append("account_snapshot_cardinality_invalid")
    accounts = [row for row in records if row.get("record_type") == "account"]
    positions = [row for row in records if row.get("record_type") == "position"]
    ledgers = [row for row in records if row.get("record_type") == "ledger"]
    if accounts:
        count = accounts[0].get("open_position_count")
        if not _strict_number(count, integer=True) or int(count) < 0:
            reasons.append("open_position_count_must_be_nonnegative_nonbool_integer")
        elif int(count) > 1:
            reasons.append("multiple_open_positions_outside_v4_single_position_policy")
        elif int(count) != len(positions) or int(count) != len(ledgers):
            reasons.append("open_position_count_inventory_mismatch")
    position_keys = [(row.get("position_id"), row.get("trade_id"), row.get("instrument")) for row in positions]
    ledger_keys = [(row.get("position_id"), row.get("trade_id"), row.get("instrument")) for row in ledgers]
    if len(position_keys) != len(set(position_keys)) or len(ledger_keys) != len(set(ledger_keys)):
        reasons.append("duplicate_semantic_position_or_ledger_identity")
    if sorted(position_keys, key=str) != sorted(ledger_keys, key=str):
        reasons.append("position_ledger_semantic_inventory_mismatch")
    for row in records:
        if row.get("record_type") == "position":
            if not _strict_number(row.get("units")) or float(row["units"]) == 0 or not _strict_number(row.get("entry_price")) or float(row["entry_price"]) <= 0:
                reasons.append("position_units_or_entry_must_be_finite_nonbool")
        if row.get("record_type") == "capacity":
            if not _strict_number(row.get("maximum_switch_units")) or float(row["maximum_switch_units"]) < 0:
                reasons.append("maximum_switch_units_must_be_finite_nonbool")
        for field in ("balance", "NAV", "nav", "margin_available", "margin_used", "unrealized_pl", "realized_pl"):
            if field in row and not _strict_number(row.get(field)):
                reasons.append(f"{field}_must_be_finite_nonbool")
    if not reasons:
        return []
    return [{
        "kind": "account_state_v4", "envelope": "account_state",
        "record_id": "account-envelope", "reason": reason,
    } for reason in sorted(set(reasons))]


def _economics_type_rejections(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    numeric = (
        "proposed_units", "expected_favorable_move_bps", "expected_adverse_move_bps",
        "expected_exit_half_spread_bps", "entry_slippage_bps",
        "expected_exit_slippage_bps", "entry_latency_bps",
        "expected_exit_latency_bps", "rotation_close_cost_bps",
    )
    rejected = []
    for record in records:
        bad = [field for field in numeric if not _strict_number(record.get(field))]
        if bad:
            rejected.append({
                "kind": "economics_inputs_v4", "envelope": "economics_inputs",
                "record_id": str(record.get("economics_record_id") or ""),
                "reason": "economics_numeric_fields_must_be_finite_nonbool",
                "fields": bad,
            })
    return rejected


def _cross_forecast_equivocation(
    verifier_records: Sequence[Mapping[str, Any]],
    economics_records: Sequence[Mapping[str, Any]],
) -> tuple[set[str], list[dict[str, Any]]]:
    """Permit the intended verifier/economics join only when it is exact.

    A forecast identifier may appear once in each role.  If the two roles bind
    that identifier to different immutable forecast bytes or different exact
    cells, neither envelope is allowed to survive as an apparently admissible
    half of an equivocated identity.
    """
    verifier_by_forecast = {
        str(row.get("forecast_record_id") or ""): row for row in verifier_records
        if str(row.get("forecast_record_id") or "")
    }
    economics_by_forecast = {
        str(row.get("forecast_record_id") or ""): row for row in economics_records
        if str(row.get("forecast_record_id") or "")
    }
    rejected: list[dict[str, Any]] = []
    invalid: set[str] = set()
    for forecast_id in sorted(set(verifier_by_forecast) & set(economics_by_forecast)):
        verifier = verifier_by_forecast[forecast_id]
        economics = economics_by_forecast[forecast_id]
        verifier_identity = (
            verifier.get("forecast_record_sha256"), verifier.get("arm_id"),
            verifier.get("instrument"), verifier.get("horizon_sec"),
            verifier.get("research_direction"),
        )
        economics_identity = (
            economics.get("forecast_record_sha256"), economics.get("arm_id"),
            economics.get("instrument"), economics.get("horizon_sec"),
            economics.get("forecast_direction"),
        )
        if verifier_identity != economics_identity:
            invalid.update(("verifier_evidence", "economics_inputs"))
            rejected.extend([
                {
                    "kind": f"{name}_v4", "envelope": name,
                    "record_id": forecast_id,
                    "reason": "cross_envelope_forecast_id_equivocation",
                    "forecast_identity_sha256": stable_hash({
                        "verifier": verifier_identity, "economics": economics_identity,
                    }),
                }
                for name in ("verifier_evidence", "economics_inputs")
            ])
    return invalid, rejected


def _rotation_close_rejections(
    economics: Sequence[Mapping[str, Any]],
    *,
    account_records: Sequence[Mapping[str, Any]],
    quotes: Sequence[Mapping[str, Any]],
    tolerance_bps: float,
) -> list[dict[str, Any]]:
    """Recompute open-position close economics from an exact executable quote.

    V3 only required a positive caller-supplied aggregate close cost.  V4 does
    not permit that value to enter a ranking unless the currently open
    position, its exact close-side quote, and the decomposed slippage/latency
    costs are all bound and independently recomputable.
    """
    accounts = [row for row in account_records if row.get("record_type") == "account"]
    if len(accounts) != 1 or accounts[0].get("open_position_count") != 1:
        return []
    positions = [row for row in account_records if row.get("record_type") == "position"]
    if len(positions) != 1:
        return []  # account envelope is rejected separately and never admitted.
    position = positions[0]
    quote_by_id = {str(row.get("quote_record_id") or ""): row for row in quotes}
    rejected: list[dict[str, Any]] = []
    for record in economics:
        reason = None
        quote = quote_by_id.get(str(record.get("rotation_close_quote_record_id") or ""))
        try:
            position_units = float(position.get("units"))
            bid = float(quote.get("bid")) if quote is not None else float("nan")
            ask = float(quote.get("ask")) if quote is not None else float("nan")
            slippage = float(record.get("rotation_close_slippage_bps"))
            latency = float(record.get("rotation_close_latency_bps"))
            reported = float(record.get("rotation_close_cost_bps"))
        except (TypeError, ValueError):
            position_units = bid = ask = slippage = latency = reported = float("nan")
        expected_side = "sell" if position_units > 0 else "buy" if position_units < 0 else ""
        if quote is None:
            reason = "rotation_close_exact_executable_quote_missing"
        elif (
            quote.get("instrument") != position.get("instrument")
            or record.get("rotation_close_quote_record_hash") != quote.get("canonical_record_sha256")
        ):
            reason = "rotation_close_quote_position_or_hash_binding_mismatch"
        elif record.get("rotation_close_side") != expected_side:
            reason = "rotation_close_side_not_derived_from_signed_position_units"
        elif not all(math.isfinite(value) for value in (position_units, bid, ask, slippage, latency, reported)):
            reason = "rotation_close_economics_must_be_finite_nonbool"
        elif any(isinstance(record.get(field), bool) for field in (
            "rotation_close_slippage_bps", "rotation_close_latency_bps", "rotation_close_cost_bps",
        )):
            reason = "rotation_close_economics_must_be_finite_nonbool"
        elif bid <= 0 or ask <= bid or slippage < 0 or latency < 0:
            reason = "rotation_close_quote_or_cost_components_invalid"
        else:
            midpoint = (bid + ask) / 2.0
            half_spread_bps = (ask - bid) / (2.0 * midpoint) * 10_000.0
            recomputed = half_spread_bps + slippage + latency
            if abs(reported - recomputed) > tolerance_bps:
                reason = "rotation_close_cost_not_recomputed_from_executable_quote"
            quote_known = parse_epoch(quote.get("retrieved_at_utc"))
            cost_known = parse_epoch(record.get("rotation_close_cost_known_at_utc"))
            evaluated = parse_epoch(record.get("economics_evaluated_at_utc"))
            if reason is None and (
                quote_known is None or cost_known is None or evaluated is None
                or quote_known > cost_known or cost_known > evaluated
            ):
                reason = "rotation_close_cost_clock_not_after_quote_and_before_evaluation"
        if reason:
            rejected.append({
                "kind": "economics_inputs_v4", "envelope": "economics_inputs",
                "record_id": str(record.get("economics_record_id") or ""),
                "reason": reason,
            })
    return rejected


def _hold_rejections(
    records: Sequence[Mapping[str, Any]],
    *,
    response_index: Mapping[tuple[str, str, int], Mapping[str, Any]],
    forward_arms: set[str],
) -> list[dict[str, Any]]:
    hashes = (
        "model_contract_sha256", "model_cohort_sha256", "feature_contract_sha256",
        "magnitude_model_sha256", "cost_model_contract_sha256",
    )
    rejected = []
    for record in records:
        try:
            key = (str(record.get("arm_id") or ""), str(record.get("instrument") or ""), int(record.get("horizon_sec")))
        except (TypeError, ValueError):
            key = (str(record.get("arm_id") or ""), str(record.get("instrument") or ""), 0)
        response = response_index.get(key)
        reason = None
        if key[0] not in forward_arms or response is None or response.get("paper_candidate") is not True or response.get("forward_hypothesis") is not True:
            reason = "hold_arm_not_forward_admissible_or_response_not_forward_candidate"
        forecast = record.get("hold_forecast_record")
        if reason is None and not isinstance(forecast, Mapping):
            reason = "hold_forecast_record_missing"
        if reason is None and any(not SHA256.fullmatch(str(forecast.get(field) or "").lower()) for field in hashes):
            reason = "hold_model_feature_magnitude_or_cost_sha256_invalid"
        if reason:
            rejected.append({
                "kind": "hold_switch_inputs_v4", "envelope": "hold_switch_inputs",
                "record_id": str(record.get("hold_record_id") or ""),
                "reason": reason,
            })
    return rejected


def _rejection_envelope(kind: str) -> str | None:
    if kind.startswith("venue_quote"):
        return "venue_quotes"
    if kind.startswith("verifier"):
        return "verifier_evidence"
    if kind.startswith("economics"):
        return "economics_inputs"
    if kind.startswith("hold"):
        return "hold_switch_inputs"
    if kind.startswith("account"):
        return "account_state"
    return None


def build_after_cost_counterfactual_v4(
    response_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    frozen_manifest: Mapping[str, Any],
    upstream_v3_contract: Mapping[str, Any],
    upstream_v3_manifest: Mapping[str, Any],
    venue_quotes: Mapping[str, Any] | None = None,
    verifier_evidence: Mapping[str, Any] | None = None,
    economics_inputs: Mapping[str, Any] | None = None,
    hold_switch_inputs: Mapping[str, Any] | None = None,
    account_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_contracts(contract, upstream_v3_contract)
    manifest = _validate_v4_manifest(frozen_manifest, contract)
    try:
        upstream_manifest = _validate_manifest(upstream_v3_manifest, upstream_v3_contract)
        _, _, response_index = _response_grid(response_snapshot, upstream_v3_contract)
    except (AfterCostV2Error, AfterCostV3Error) as exc:
        raise AfterCostV4Error(str(exc)) from exc
    if upstream_manifest["manifest_id"] != contract["required_upstream_v3_manifest_id"]:
        raise AfterCostV4Error("upstream v3 manifest id mismatch")
    supplied = {
        "venue_quotes": venue_quotes, "verifier_evidence": verifier_evidence,
        "economics_inputs": economics_inputs, "hold_switch_inputs": hold_switch_inputs,
        "account_state": account_state,
    }
    strict: dict[str, Mapping[str, Any] | None] = {}
    records: dict[str, list[dict[str, Any]]] = {}
    for name in ENVELOPES:
        try:
            payload, normalized, _ = _strict_envelope(
                supplied[name], name=name, contract=upstream_v3_contract,
                manifest=upstream_manifest,
            )
        except AfterCostV3Error as exc:
            raise AfterCostV4Error(str(exc)) from exc
        strict[name], records[name] = payload, normalized
    submitted = _submitted_lineage(strict)
    rejections: list[dict[str, Any]] = []
    invalid: set[str] = set()
    for name in ENVELOPES:
        collision = _collision_rejections(name, records[name])
        if collision:
            invalid.add(name); rejections.extend(collision)
    equivocated_envelopes, equivocation_errors = _cross_forecast_equivocation(
        records["verifier_evidence"], records["economics_inputs"],
    )
    invalid.update(equivocated_envelopes); rejections.extend(equivocation_errors)
    account_errors = _account_rejections(records["account_state"])
    if account_errors:
        invalid.add("account_state"); rejections.extend(account_errors)
    economics_errors = _economics_type_rejections(records["economics_inputs"])
    if economics_errors:
        invalid.add("economics_inputs"); rejections.extend(economics_errors)
    rotation_errors = _rotation_close_rejections(
        records["economics_inputs"], account_records=records["account_state"],
        quotes=records["venue_quotes"],
        tolerance_bps=float(contract["rotation_close_contract"]["numeric_tolerance_bps"]),
    )
    if rotation_errors:
        invalid.add("economics_inputs"); rejections.extend(rotation_errors)
    hold_errors = _hold_rejections(
        records["hold_switch_inputs"], response_index=response_index,
        forward_arms=set(contract["forward_admissible_arm_ids"]),
    )
    if hold_errors:
        invalid.add("hold_switch_inputs"); rejections.extend(hold_errors)
    base: dict[str, Any] | None = None
    for _ in range(len(ENVELOPES) + 1):
        try:
            base = build_after_cost_counterfactual_v3(
                response_snapshot, contract=upstream_v3_contract,
                frozen_manifest=upstream_v3_manifest,
                venue_quotes=None if "venue_quotes" in invalid else strict["venue_quotes"],
                verifier_evidence=None if "verifier_evidence" in invalid else strict["verifier_evidence"],
                economics_inputs=None if "economics_inputs" in invalid else strict["economics_inputs"],
                hold_switch_inputs=None if "hold_switch_inputs" in invalid else strict["hold_switch_inputs"],
                account_state=None if "account_state" in invalid else strict["account_state"],
            )
        except AfterCostV3Error as exc:
            raise AfterCostV4Error(str(exc)) from exc
        newly_invalid: dict[str, set[str]] = {}
        for item in base.get("input_rejections") or []:
            name = _rejection_envelope(str(item.get("kind") or ""))
            if name and name not in invalid and strict.get(name) is not None:
                newly_invalid.setdefault(name, set()).add(str(item.get("reason") or "upstream_rejection"))
        if not newly_invalid:
            break
        for name, reasons in newly_invalid.items():
            invalid.add(name)
            rejections.append({
                "kind": f"{name}_v4", "envelope": name,
                "record_id": "whole-envelope", "reason": "atomic_envelope_invalidated_after_any_constituent_rejection",
                "upstream_reasons": sorted(reasons),
            })
    else:
        raise AfterCostV4Error("atomic envelope validation did not converge")
    assert base is not None
    final_upstream_rejections = list(base.get("input_rejections") or [])
    rejections = sorted(rejections, key=lambda row: (
        str(row.get("envelope") or ""), str(row.get("reason") or ""),
        str(row.get("record_id") or ""), str(row.get("canonical_record_sha256") or ""),
    ))
    dispositions = {
        name: {
            "submitted": strict[name] is not None,
            "admitted_whole": strict[name] is not None and name not in invalid,
            "invalidated_whole": name in invalid,
            "authoritative_submitted_envelope_sha256": None if strict[name] is None else strict[name]["canonical_envelope_sha256"],
        }
        for name in ENVELOPES
    }
    base.update({
        "schema_version": 4, "snapshot_schema": contract["snapshot_schema"],
        "counterfactual_contract_id": contract["contract_id"],
        "counterfactual_cohort_id": contract["counterfactual_cohort_id"],
        "supersedes_contract_id": contract["supersedes_contract_id"],
        "supersedes_cohort_id": contract["supersedes_cohort_id"],
        "frozen_manifest": manifest,
        "upstream_v3_contract_id": upstream_v3_contract["contract_id"],
        "upstream_v3_cohort_id": upstream_v3_contract["counterfactual_cohort_id"],
        "upstream_v3_manifest": upstream_manifest,
        "submitted_input_hashes": submitted,
        "canonical_input_hashes": submitted,
        "input_envelope_dispositions": dispositions,
        "v4_input_rejections": rejections,
        "strict_input_rejections": rejections + final_upstream_rejections,
    })
    economics_by_id = {
        str(row.get("economics_record_id") or ""): row
        for row in records["economics_inputs"]
    }
    rotation_lineage_by_economics: dict[str, dict[str, Any]] = {}
    for row in base["records"]:
        if row.get("economics_admissible") is not True:
            continue
        raw = economics_by_id.get(str(row.get("economics_record_id") or ""))
        if raw is None or not str(raw.get("rotation_close_quote_record_id") or ""):
            continue
        lineage = {
            "rotation_close_quote_record_id": raw["rotation_close_quote_record_id"],
            "rotation_close_quote_record_hash": raw["rotation_close_quote_record_hash"],
            "rotation_close_side": raw["rotation_close_side"],
            "rotation_close_slippage_bps": raw["rotation_close_slippage_bps"],
            "rotation_close_latency_bps": raw["rotation_close_latency_bps"],
            "recomputed_rotation_close_cost_bps": raw["rotation_close_cost_bps"],
        }
        row.update(lineage)
        rotation_lineage_by_economics[str(row["economics_record_id"])] = lineage
    for arm_payload in base["allocations"].values():
        for allocation in arm_payload.values():
            candidates = list(allocation.get("disjoint_basket") or [])
            if allocation.get("top_one"):
                candidates.append(allocation["top_one"])
            for candidate in candidates:
                candidate.update(rotation_lineage_by_economics.get(
                    str(candidate.get("economics_record_id") or ""), {}
                ))
    base["input_rejections"] = rejections + final_upstream_rejections
    base["summary"]["v4_input_rejection_count"] = len(rejections)
    base["summary"]["input_rejection_count"] = len(base["input_rejections"])
    base["summary"]["submitted_envelope_count"] = sum(value is not None for value in strict.values())
    base["summary"]["atomically_invalidated_envelope_count"] = len(invalid)
    base["summary"]["submitted_record_count"] = sum(len(value) for value in records.values())
    base["decision_fingerprint"] = stable_hash({
        "records": base["records"], "allocations": base["allocations"],
        "holds": base["hold_switch_counterfactuals"],
        "supported_execution_decision": base["supported_execution_decision"],
    })
    base["payload_identity_policy"] = copy.deepcopy(contract["payload_identity_policy"])
    base["limitations"] = [
        "v3 remains immutable engineering_blocked_zero_evidence",
        "any record collision or constituent rejection invalidates its full input envelope",
        "payload_id is diagnostic and non-authoritative; canonical envelope SHA-256 is authoritative",
        "canonical integrity is not producer authenticity; the operational CLI requires an append-only genealogy trust anchor before nonempty inputs",
        "all results remain research-only no_trade",
    ]
    base["snapshot_id"] = "currency_state_after_cost_v4_" + stable_hash({
        key: value for key, value in base.items() if key != "snapshot_id"
    })[:24]
    if (
        len(base["records"]) != 8 * 68 * 5
        or any(row["paper_decision"] != "no_trade" for row in base["records"])
        or base.get("supported_execution_decision") != "no_trade"
    ):
        raise AfterCostV4Error("v4 grid or safety invariant failed")
    return base


__all__ = [
    "AfterCostV4Error", "EXPECTED_ARTIFACT_PATHS",
    "build_after_cost_counterfactual_v4",
]
