"""After-cost v3: exact-cell evidence and canonical-envelope hardening.

This is a pure research wrapper around the frozen v2 calculation engine.  It
adds consumer-recomputed full-record/full-envelope hashes, exact verifier-cell
and forecast bindings, and recomputed Practice-007 account/position/portfolio
bindings before any v2 arithmetic is allowed.  No operational imports exist.
"""

from __future__ import annotations

import copy
from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from .after_cost_input_envelopes_v3 import (
    CanonicalEnvelopeError,
    canonical_sha256,
    verify_envelope,
    verify_record,
)
from .currency_state_after_cost_counterfactual_v2 import (
    AfterCostV2Error,
    _response_grid,
    _validate_manifest,
    build_after_cost_counterfactual_v2,
)
from ..contracts.currency_state import stable_hash
from ..contracts.signed_currency_exposure import canonical_factor_ids
from ..features.currency_state_engine import parse_epoch


class AfterCostV3Error(ValueError):
    pass


ENVELOPES = ("venue_quotes", "verifier_evidence", "economics_inputs", "hold_switch_inputs", "account_state")


def _validate_v3_contract(contract: Mapping[str, Any]) -> None:
    canonical = contract.get("canonical_envelope_contract") or {}
    if tuple(canonical.get("required_envelopes") or ()) != ENVELOPES:
        raise AfterCostV3Error("v3 canonical envelope set/order mismatch")
    bindings = canonical.get("producer_bindings") or {}
    if set(bindings) != set(ENVELOPES):
        raise AfterCostV3Error("v3 producer binding set mismatch")
    for name in ENVELOPES:
        binding = bindings[name]
        expected = canonical_sha256({
            "producer_contract_id": binding.get("producer_contract_id"),
            "producer_cohort_id": binding.get("producer_cohort_id"),
        })
        if binding.get("producer_cohort_sha256") != expected:
            raise AfterCostV3Error(f"{name} producer cohort hash is not canonical")
    if canonical.get("canonical_json") != "utf8_sorted_keys_compact_no_nan":
        raise AfterCostV3Error("v3 canonical JSON contract mismatch")


def _strict_envelope(
    payload: Mapping[str, Any] | None,
    *,
    name: str,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
    if payload is None:
        return None, [], []
    if not isinstance(payload, Mapping):
        raise AfterCostV3Error(f"{name} envelope must be a mapping")
    binding = contract["canonical_envelope_contract"]["producer_bindings"][name]
    artifact_label = binding["manifest_artifact"]
    artifact = manifest["artifacts"].get(artifact_label) or {}
    if payload.get("producer_contract_id") != binding["producer_contract_id"]:
        raise AfterCostV3Error(f"{name} producer contract id mismatch")
    if payload.get("contract_id") != binding["producer_contract_id"]:
        raise AfterCostV3Error(f"{name} envelope contract must equal producer contract")
    if payload.get("cohort_id") != binding["producer_cohort_id"]:
        raise AfterCostV3Error(f"{name} producer cohort id mismatch")
    if payload.get("cohort_sha256") != binding["producer_cohort_sha256"]:
        raise AfterCostV3Error(f"{name} producer cohort hash mismatch")
    if payload.get("producer_module_id") != artifact_label:
        raise AfterCostV3Error(f"{name} producer module id mismatch")
    if payload.get("producer_module_sha256") != artifact.get("sha256"):
        raise AfterCostV3Error(f"{name} producer module hash is not frozen manifest bytes")
    expected_contract_hash = manifest["artifacts"]["input_envelope_config"]["sha256"]
    if payload.get("contract_sha256") != expected_contract_hash:
        raise AfterCostV3Error(f"{name} producer contract hash mismatch")
    if not str(payload.get("payload_id") or ""):
        raise AfterCostV3Error(f"{name} payload_id required")
    try:
        envelope_hash = verify_envelope(payload)
    except CanonicalEnvelopeError as exc:
        raise AfterCostV3Error(f"{name} canonical envelope hash mismatch") from exc
    records = payload.get("records") or []
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise AfterCostV3Error(f"{name}.records invalid")
    try:
        record_count = int(payload.get("record_count"))
    except (TypeError, ValueError):
        raise AfterCostV3Error(f"{name}.record_count required") from None
    if record_count != len(records):
        raise AfterCostV3Error(f"{name}.record_count mismatch")
    normalized, hashes = [], []
    for index, raw in enumerate(records):
        if not isinstance(raw, Mapping):
            raise AfterCostV3Error(f"{name}.records[{index}] invalid")
        try:
            record_hash = verify_record(raw)
        except CanonicalEnvelopeError as exc:
            raise AfterCostV3Error(f"{name} canonical record hash mismatch at index {index}") from exc
        normalized.append(copy.deepcopy(dict(raw)))
        hashes.append({"record_index": index, "canonical_record_sha256": record_hash})
    result = copy.deepcopy(dict(payload))
    return result, normalized, [{"canonical_envelope_sha256": envelope_hash, "records": hashes}]


def _record_id(row: Mapping[str, Any], name: str) -> str:
    field = {"venue_quotes": "quote_record_id", "verifier_evidence": "verifier_evidence_id", "economics_inputs": "economics_record_id", "hold_switch_inputs": "hold_record_id", "account_state": "account_state_record_id"}[name]
    return str(row.get(field) or "")


def _canonical_maps(envelopes: Mapping[str, Mapping[str, Any] | None]) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    env_hashes, records = {}, {}
    for name, payload in envelopes.items():
        if payload is None:
            continue
        env_hashes[name] = str(payload["canonical_envelope_sha256"])
        records[name] = {_record_id(row, name): str(row["canonical_record_sha256"]) for row in payload.get("records") or []}
    return env_hashes, records


def _response_ref(response_index: Mapping[tuple[str, str, int], Mapping[str, Any]], raw: Mapping[str, Any]) -> tuple[tuple[str, str, int], Mapping[str, Any] | None]:
    try:
        key = (str(raw.get("arm_id") or ""), str(raw.get("instrument") or ""), int(raw.get("horizon_sec")))
    except (TypeError, ValueError):
        key = (str(raw.get("arm_id") or ""), str(raw.get("instrument") or ""), 0)
    return key, response_index.get(key)


def _validate_quotes(
    records: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Require the purported immutable venue payload to actually reproduce the quote."""
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    seen_instruments: set[str] = set()
    bound_fields = (
        "quote_record_id", "instrument", "venue", "environment", "account_id",
        "bid", "ask", "pip", "quote_time_utc", "broker_time_utc",
        "observed_at_utc", "retrieved_at_utc",
    )
    for raw in records:
        record_id = str(raw.get("quote_record_id") or "")
        instrument = str(raw.get("instrument") or "")
        reason = None
        immutable = raw.get("immutable_payload")
        if not record_id or record_id in by_id:
            reason = "duplicate_or_missing_quote_record_id"
        elif instrument in seen_instruments:
            reason = "duplicate_quote_instrument"
        elif not isinstance(immutable, Mapping):
            reason = "immutable_quote_payload_missing"
        elif raw.get("immutable_payload_sha256") != canonical_sha256(immutable):
            reason = "immutable_quote_payload_hash_mismatch"
        elif any(raw.get(field) != immutable.get(field) for field in bound_fields):
            reason = "immutable_quote_payload_field_mismatch"
        if reason:
            rejected.append({
                "kind": "venue_quote_v3", "record_id": record_id,
                "arm_id": "", "instrument": instrument, "horizon_sec": 0,
                "reason": reason,
            })
            continue
        normalized = copy.deepcopy(dict(raw))
        valid.append(normalized)
        by_id[record_id] = normalized
        seen_instruments.add(instrument)
    return valid, rejected, by_id


def _same_epoch(left: Any, right: Any) -> bool:
    a, b = parse_epoch(left), parse_epoch(right)
    return a is not None and b is not None and abs(a - b) <= 1e-6


def _validate_verifier_bindings(
    records: Sequence[Mapping[str, Any]],
    *,
    response_id: str,
    response_sha: str,
    response_cutoff: str,
    response_index: Mapping[tuple[str, str, int], Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    valid, rejected, by_id, cells = [], [], {}, set()
    exact_fields = ("model_contract_id", "model_contract_sha256", "model_cohort_id", "model_cohort_sha256", "feature_contract_id", "feature_contract_sha256", "magnitude_model_id", "magnitude_model_sha256", "cost_model_contract_id", "cost_model_contract_sha256")
    for raw in records:
        evidence_id = str(raw.get("verifier_evidence_id") or "")
        key, response = _response_ref(response_index, raw)
        reason = None
        if not evidence_id or evidence_id in by_id:
            reason = "duplicate_or_missing_verifier_evidence_id"
        elif key in cells:
            reason = "verifier_evidence_reused_for_cell"
        elif response is None:
            reason = "verifier_cell_outside_response_grid"
        elif raw.get("response_snapshot_id") != response_id or raw.get("response_snapshot_sha256") != response_sha or raw.get("response_record_hash") != stable_hash(response):
            reason = "verifier_exact_response_binding_mismatch"
        elif (
            parse_epoch(raw.get("response_bound_at_utc")) is None
            or parse_epoch(raw.get("verified_at_utc")) is None
            or parse_epoch(raw.get("response_bound_at_utc")) < parse_epoch(response_cutoff)
            or parse_epoch(raw.get("verified_at_utc")) < parse_epoch(raw.get("response_bound_at_utc"))
            or parse_epoch(raw.get("verified_at_utc")) > parse_epoch(response_cutoff)
        ):
            reason = "verifier_claimed_response_before_response_existed"
        elif str(raw.get("research_direction") or "") != str(response.get("research_direction") or "") or not response.get("research_direction"):
            reason = "verifier_research_direction_mismatch"
        elif not str(raw.get("forecast_record_id") or "") or not str(raw.get("forecast_record_sha256") or ""):
            reason = "verifier_forecast_record_binding_missing"
        elif any(not str(raw.get(field) or "") for field in exact_fields):
            reason = "verifier_model_feature_magnitude_or_cost_binding_missing"
        if reason:
            rejected.append({"kind": "verifier_v3", "record_id": evidence_id, "arm_id": key[0], "instrument": key[1], "horizon_sec": key[2], "reason": reason})
            continue
        normalized = copy.deepcopy(dict(raw)); normalized["forecast_id"] = str(raw["forecast_record_id"])
        valid.append(normalized); by_id[evidence_id] = normalized; cells.add(key)
    return valid, rejected, by_id


def _bound_account_record(
    raw: Mapping[str, Any],
    accounts: Mapping[str, Mapping[str, Any]],
    prefix: str,
) -> Mapping[str, Any] | None:
    record = accounts.get(str(raw.get(f"bound_{prefix}_record_id") or ""))
    if record is None:
        return None
    if raw.get(f"bound_{prefix}_record_sha256") != record.get("canonical_record_sha256"):
        return None
    return record


def _rotation_binding_error(
    raw: Mapping[str, Any],
    *,
    accounts: Mapping[str, Mapping[str, Any]],
    instrument: str,
    direction: str,
    evaluated_epoch: float,
) -> str | None:
    account = _bound_account_record(raw, accounts, "account")
    capacity = _bound_account_record(raw, accounts, "capacity")
    conflict = _bound_account_record(raw, accounts, "conflict")
    if account is None or capacity is None or conflict is None:
        return "economics_flat_or_rotation_account_capacity_conflict_binding_missing"
    if account.get("record_type") != "account" or capacity.get("record_type") != "capacity" or conflict.get("record_type") != "conflict":
        return "economics_bound_account_record_type_mismatch"
    if any(item.get("account_id") != account.get("account_id") for item in (capacity, conflict)):
        return "economics_bound_account_identity_mismatch"
    if any((parse_epoch(item.get("known_at_utc")) or float("inf")) > evaluated_epoch for item in (account, capacity, conflict)):
        return "economics_bound_account_state_known_after_evaluation"
    try:
        proposed_units = abs(float(raw.get("proposed_units")))
        maximum_units = float(capacity.get("maximum_switch_units"))
    except (TypeError, ValueError):
        return "economics_proposed_or_capacity_units_invalid"
    if proposed_units <= 0 or proposed_units > maximum_units:
        return "economics_proposed_units_exceed_recomputed_capacity"
    if instrument not in set(str(x) for x in capacity.get("switch_allowed_instruments") or []):
        return "economics_instrument_not_in_recomputed_capacity"
    if set(canonical_factor_ids(instrument, direction)) & set(str(x) for x in conflict.get("conflicted_factor_ids") or []):
        return "economics_recomputed_factor_conflict"
    try:
        open_count = int(account.get("open_position_count"))
        rotation_cost = float(raw.get("rotation_close_cost_bps"))
    except (TypeError, ValueError):
        return "economics_open_count_or_rotation_cost_invalid"
    rotation_state = str(raw.get("rotation_state_id") or "")
    if open_count == 0:
        expected = f"flat:{account.get('account_snapshot_id')}"
        if rotation_state != expected or str(raw.get("rotation_position_id") or "") or rotation_cost != 0.0:
            return "arbitrary_flat_rotation_state_or_cost_rejected"
        return None
    position = _bound_account_record(raw, accounts, "position")
    ledger = _bound_account_record(raw, accounts, "ledger")
    if position is None or ledger is None:
        return "economics_open_rotation_position_or_ledger_binding_missing"
    if position.get("record_type") != "position" or ledger.get("record_type") != "ledger":
        return "economics_open_rotation_record_type_mismatch"
    if (
        position.get("account_id") != account.get("account_id")
        or ledger.get("account_id") != account.get("account_id")
        or ledger.get("position_id") != position.get("position_id")
        or ledger.get("trade_id") != position.get("trade_id")
        or ledger.get("instrument") != position.get("instrument")
    ):
        return "economics_open_rotation_position_trade_ledger_mismatch"
    expected = f"rotate:{position.get('position_id')}:{ledger.get('ledger_snapshot_id')}"
    if rotation_state != expected or raw.get("rotation_position_id") != position.get("position_id") or rotation_cost <= 0:
        return "arbitrary_open_rotation_state_or_zero_close_cost_rejected"
    if any((parse_epoch(item.get("known_at_utc")) or float("inf")) > evaluated_epoch for item in (position, ledger)):
        return "economics_open_rotation_state_known_after_evaluation"
    return None


def _validate_economics_bindings(
    records: Sequence[Mapping[str, Any]],
    *,
    verifiers: Mapping[str, Mapping[str, Any]],
    verifier_hashes: Mapping[str, str],
    quotes: Mapping[str, Mapping[str, Any]],
    accounts: Mapping[str, Mapping[str, Any]],
    response_id: str,
    response_sha: str,
    response_cutoff: str,
    response_index: Mapping[tuple[str, str, int], Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    cells: set[tuple[str, str, int]] = set()
    used_forecasts: set[tuple[str, str]] = set()
    used_verifiers: set[str] = set()
    exact_fields = (
        "model_contract_id", "model_contract_sha256", "model_cohort_id",
        "model_cohort_sha256", "feature_contract_id", "feature_contract_sha256",
        "magnitude_model_id", "magnitude_model_sha256", "cost_model_contract_id",
        "cost_model_contract_sha256",
    )
    for raw in records:
        record_id = str(raw.get("economics_record_id") or "")
        key, response = _response_ref(response_index, raw)
        reason = None
        evidence_id = str(raw.get("verifier_evidence_id") or "")
        evidence = verifiers.get(evidence_id)
        forecast = raw.get("forecast_record")
        quote = quotes.get(str(raw.get("entry_quote_record_id") or ""))
        evaluated_epoch = parse_epoch(raw.get("economics_evaluated_at_utc"))
        known_epoch = parse_epoch(raw.get("known_at_utc"))
        cutoff_epoch = parse_epoch(response_cutoff)
        if key in cells:
            reason = "duplicate_economics_cell"
        elif response is None:
            reason = "economics_cell_outside_response_grid"
        elif raw.get("response_snapshot_id") != response_id or raw.get("response_snapshot_sha256") != response_sha or raw.get("response_record_hash") != stable_hash(response):
            reason = "economics_exact_response_binding_mismatch"
        elif evidence is None:
            reason = "economics_missing_exact_cell_verifier"
        elif evidence_id in used_verifiers:
            reason = "verifier_evidence_reused_across_economics_cells"
        elif (str(evidence.get("arm_id")), str(evidence.get("instrument")), int(evidence.get("horizon_sec"))) != key:
            reason = "v2_cross_pair_verifier_reuse_exploit_rejected"
        elif str(evidence.get("research_direction")) != str(raw.get("forecast_direction")) or str(raw.get("forecast_direction")) != str(response.get("research_direction")):
            reason = "economics_verifier_response_direction_mismatch"
        elif raw.get("verifier_evidence_hash") != verifier_hashes.get(evidence_id):
            reason = "economics_verifier_canonical_hash_binding_mismatch"
        elif quote is None or quote.get("instrument") != key[1] or raw.get("entry_quote_record_hash") != quote.get("canonical_record_sha256"):
            reason = "economics_canonical_entry_quote_binding_mismatch"
        elif (
            evaluated_epoch is None
            or known_epoch is None
            or evaluated_epoch < (parse_epoch(quote.get("retrieved_at_utc")) or float("inf"))
            or evaluated_epoch < (parse_epoch(evidence.get("verified_at_utc")) or float("inf"))
            or evaluated_epoch > cutoff_epoch
            or known_epoch < evaluated_epoch
            or known_epoch > cutoff_epoch
        ):
            reason = "economics_evaluated_before_quote_or_verifier_or_after_cutoff"
        elif not isinstance(forecast, Mapping):
            reason = "economics_forecast_record_missing"
        else:
            forecast_hash = canonical_sha256(forecast)
            forecast_key = (str(raw.get("forecast_record_id") or ""), str(raw.get("forecast_record_sha256") or ""))
            forecast_fields = {
                "forecast_record_id": raw.get("forecast_record_id"),
                "arm_id": key[0], "instrument": key[1], "horizon_sec": key[2],
                "research_direction": raw.get("forecast_direction"),
                "response_snapshot_id": response_id,
                "response_snapshot_sha256": response_sha,
                "response_record_hash": stable_hash(response),
                **{field: raw.get(field) for field in exact_fields},
            }
            if raw.get("forecast_record_id") != forecast.get("forecast_record_id") or raw.get("forecast_record_sha256") != forecast_hash:
                reason = "invented_or_repackaged_forecast_record_rejected"
            elif any(forecast.get(field) != value for field, value in forecast_fields.items()):
                reason = "forecast_record_exact_cell_response_or_model_binding_mismatch"
            elif not _same_epoch(forecast.get("forecast_decision_cutoff_utc"), response_cutoff):
                reason = "forecast_record_decision_cutoff_mismatch"
            elif forecast_key in used_forecasts:
                reason = "forecast_record_reused_across_cells"
            elif evidence.get("forecast_record_id") != raw.get("forecast_record_id") or evidence.get("forecast_record_sha256") != raw.get("forecast_record_sha256"):
                reason = "verifier_forecast_binding_mismatch"
            elif any(raw.get(field) != evidence.get(field) for field in exact_fields):
                reason = "economics_verifier_model_feature_magnitude_or_cost_mismatch"
            else:
                reason = _rotation_binding_error(
                    raw, accounts=accounts, instrument=key[1],
                    direction=str(raw.get("forecast_direction") or ""),
                    evaluated_epoch=float(evaluated_epoch),
                )
        if reason:
            rejected.append({
                "kind": "economics_v3", "record_id": record_id,
                "arm_id": key[0], "instrument": key[1], "horizon_sec": key[2],
                "reason": reason,
            })
            continue
        normalized = copy.deepcopy(dict(raw))
        normalized["forecast_id"] = str(raw["forecast_record_id"])
        valid.append(normalized)
        cells.add(key)
        used_forecasts.add((str(raw["forecast_record_id"]), str(raw["forecast_record_sha256"])))
        used_verifiers.add(evidence_id)
    return valid, rejected


def _account_records(
    records: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], list[dict[str, Any]]]:
    result: dict[str, Mapping[str, Any]] = {}
    rejected: list[dict[str, Any]] = []
    allowed_types = {"account", "position", "ledger", "capacity", "conflict"}
    for raw in records:
        record_id = str(raw.get("account_state_record_id") or "")
        record_type = str(raw.get("record_type") or "")
        reason = None
        if not record_id or record_id in result:
            reason = "duplicate_or_missing_account_state_record_id"
        elif record_type not in allowed_types:
            reason = "account_state_record_type_invalid"
        elif raw.get("account_id") != "101-001-37981792-007" or raw.get("environment") != "practice":
            reason = "account_state_not_practice_007"
        elif parse_epoch(raw.get("known_at_utc")) is None:
            reason = "account_state_known_clock_missing"
        elif record_type == "account":
            try:
                open_count = int(raw.get("open_position_count"))
            except (TypeError, ValueError):
                open_count = -1
            if not str(raw.get("account_snapshot_id") or "") or open_count < 0:
                reason = "account_snapshot_identity_or_open_count_missing"
        elif record_type == "position":
            try:
                units, entry = float(raw.get("units")), float(raw.get("entry_price"))
            except (TypeError, ValueError):
                units, entry = 0.0, 0.0
            signed = "buy" if units > 0 else "sell" if units < 0 else None
            if (
                not str(raw.get("position_id") or "")
                or not str(raw.get("trade_id") or "")
                or not str(raw.get("instrument") or "")
                or entry <= 0
                or signed != raw.get("direction")
            ):
                reason = "position_identity_instrument_signed_units_or_entry_invalid"
        elif record_type == "ledger":
            if any(not str(raw.get(field) or "") for field in (
                "ledger_snapshot_id", "position_ledger_record_id", "position_id",
                "trade_id", "instrument",
            )):
                reason = "ledger_position_trade_identity_missing"
        elif record_type == "capacity":
            allowed = raw.get("switch_allowed_instruments")
            try:
                maximum_units = float(raw.get("maximum_switch_units"))
            except (TypeError, ValueError):
                maximum_units = -1.0
            if (
                not str(raw.get("capacity_state_id") or "")
                or not isinstance(allowed, Sequence)
                or isinstance(allowed, (str, bytes))
                or maximum_units < 0
            ):
                reason = "capacity_state_invalid"
        elif record_type == "conflict":
            factors = raw.get("conflicted_factor_ids")
            if (
                not str(raw.get("conflict_state_id") or "")
                or not isinstance(factors, Sequence)
                or isinstance(factors, (str, bytes))
            ):
                reason = "conflict_state_invalid"
        if reason:
            rejected.append({
                "kind": "account_v3", "record_id": record_id,
                "arm_id": "", "instrument": str(raw.get("instrument") or ""),
                "horizon_sec": 0, "reason": reason,
            })
            continue
        result[record_id] = raw
    typed: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in result.values():
        typed[str(row["record_type"])].append(row)
    inventory_reason = None
    if result and (
        len(typed["account"]) != 1
        or len(typed["capacity"]) != 1
        or len(typed["conflict"]) != 1
    ):
        inventory_reason = "account_envelope_requires_one_account_capacity_and_conflict_record"
    elif result:
        account = typed["account"][0]
        try:
            expected_positions = int(account["open_position_count"])
        except (TypeError, ValueError):
            expected_positions = -1
        positions = {
            (str(row["position_id"]), str(row["trade_id"]), str(row["instrument"]))
            for row in typed["position"]
        }
        ledgers = {
            (str(row["position_id"]), str(row["trade_id"]), str(row["instrument"]))
            for row in typed["ledger"]
        }
        if (
            expected_positions != len(typed["position"])
            or expected_positions != len(typed["ledger"])
            or positions != ledgers
        ):
            inventory_reason = "account_open_position_count_or_ledger_inventory_mismatch"
    if inventory_reason:
        rejected.append({
            "kind": "account_v3", "record_id": "account-envelope-inventory",
            "arm_id": "", "instrument": "", "horizon_sec": 0,
            "reason": inventory_reason,
        })
        return {}, rejected
    return result, rejected


def _validate_hold_bindings(
    records: Sequence[Mapping[str, Any]],
    *,
    accounts: Mapping[str, Mapping[str, Any]],
    verifiers: Mapping[str, Mapping[str, Any]],
    verifier_hashes: Mapping[str, str],
    quotes: Mapping[str, Mapping[str, Any]],
    response_id: str,
    response_sha: str,
    response_cutoff: str,
    response_index: Mapping[tuple[str, str, int], Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen_groups: set[tuple[str, int]] = set()
    for raw in records:
        record_id = str(raw.get("hold_record_id") or ""); key, response = _response_ref(response_index, raw); reason = None
        account = accounts.get(str(raw.get("account_record_id") or "")); position = accounts.get(str(raw.get("position_record_id") or "")); ledger = accounts.get(str(raw.get("ledger_record_id") or "")); capacity = accounts.get(str(raw.get("capacity_record_id") or "")); conflict = accounts.get(str(raw.get("conflict_record_id") or ""))
        verifier_id = str(raw.get("hold_verifier_evidence_id") or "")
        verifier = verifiers.get(verifier_id)
        forecast = raw.get("hold_forecast_record")
        quote = quotes.get(str(raw.get("current_close_quote_record_id") or ""))
        evaluated_epoch = parse_epoch(raw.get("hold_evaluated_at_utc"))
        cutoff_epoch = parse_epoch(response_cutoff)
        if (key[0], key[2]) in seen_groups:
            reason = "duplicate_hold_arm_horizon_group"
        elif response is None: reason = "hold_cell_outside_response_grid"
        elif raw.get("response_snapshot_id") != response_id or raw.get("response_snapshot_sha256") != response_sha or raw.get("response_record_hash") != stable_hash(response): reason = "hold_exact_response_binding_mismatch"
        elif any(value is None for value in (account, position, ledger, capacity, conflict)): reason = "hold_recomputed_account_position_ledger_capacity_or_conflict_missing"
        elif any(raw.get(field) != value.get("canonical_record_sha256") for field, value in (("account_record_sha256", account), ("position_record_sha256", position), ("ledger_record_sha256", ledger), ("capacity_record_sha256", capacity), ("conflict_record_sha256", conflict))): reason = "hold_account_state_canonical_hash_binding_mismatch"
        elif (
            account.get("record_type") != "account"
            or position.get("record_type") != "position"
            or ledger.get("record_type") != "ledger"
            or capacity.get("record_type") != "capacity"
            or conflict.get("record_type") != "conflict"
        ): reason = "hold_account_state_record_type_mismatch"
        elif account.get("account_id") != "101-001-37981792-007" or account.get("environment") != "practice": reason = "hold_practice_account_binding_mismatch"
        elif (
            position.get("account_id") != account.get("account_id")
            or ledger.get("account_id") != account.get("account_id")
            or capacity.get("account_id") != account.get("account_id")
            or conflict.get("account_id") != account.get("account_id")
            or ledger.get("position_id") != position.get("position_id")
            or ledger.get("trade_id") != position.get("trade_id")
            or ledger.get("instrument") != position.get("instrument")
            or position.get("instrument") != key[1]
        ): reason = "hold_position_instrument_trade_ledger_binding_mismatch"
        elif quote is None or quote.get("instrument") != key[1] or raw.get("current_close_quote_record_hash") != quote.get("canonical_record_sha256"): reason = "hold_canonical_current_close_quote_binding_mismatch"
        elif raw.get("hold_verifier_evidence_hash") != verifier_hashes.get(verifier_id): reason = "hold_canonical_verifier_hash_binding_mismatch"
        elif (
            evaluated_epoch is None
            or evaluated_epoch < (parse_epoch(quote.get("retrieved_at_utc")) or float("inf"))
            or evaluated_epoch < (parse_epoch(None if verifier is None else verifier.get("verified_at_utc")) or float("inf"))
            or evaluated_epoch > cutoff_epoch
            or any((parse_epoch(item.get("known_at_utc")) or float("inf")) > evaluated_epoch for item in (account, position, ledger, capacity, conflict))
        ): reason = "hold_evaluated_before_quote_verifier_state_or_after_cutoff"
        else:
            units = float(position.get("units") or 0); signed_direction = "buy" if units > 0 else "sell" if units < 0 else None
            response_direction = str(response.get("research_direction") or ""); hold_direction = str(raw.get("hold_direction") or "")
            verifier_direction = None if verifier is None else str(verifier.get("research_direction") or "")
            if signed_direction is None or not (signed_direction == str(position.get("direction") or "") == hold_direction == response_direction == verifier_direction):
                reason = "v2_sell_position_buy_response_direction_exploit_rejected"
            elif verifier is None or (str(verifier.get("arm_id")), str(verifier.get("instrument")), int(verifier.get("horizon_sec"))) != key:
                reason = "hold_exact_cell_verifier_missing"
            elif raw.get("hold_forecast_id") != verifier.get("forecast_record_id") or raw.get("hold_forecast_sha256") != verifier.get("forecast_record_sha256"):
                reason = "hold_forecast_verifier_binding_mismatch"
            elif not isinstance(forecast, Mapping) or canonical_sha256(forecast) != raw.get("hold_forecast_sha256"):
                reason = "hold_forecast_record_missing_or_hash_mismatch"
            elif any(
                forecast.get(field) != verifier.get(field)
                for field in (
                    "forecast_record_id", "arm_id", "instrument", "horizon_sec",
                    "research_direction", "response_snapshot_id",
                    "response_snapshot_sha256", "response_record_hash",
                    "model_contract_id", "model_contract_sha256",
                    "model_cohort_id", "model_cohort_sha256",
                    "feature_contract_id", "feature_contract_sha256",
                    "magnitude_model_id", "magnitude_model_sha256",
                    "cost_model_contract_id", "cost_model_contract_sha256",
                )
            ):
                reason = "hold_forecast_exact_verifier_binding_mismatch"
            elif not _same_epoch(forecast.get("forecast_decision_cutoff_utc"), response_cutoff):
                reason = "hold_forecast_decision_cutoff_mismatch"
        if reason:
            rejected.append({"kind": "hold_v3", "record_id": record_id, "arm_id": key[0], "instrument": key[1], "horizon_sec": key[2], "reason": reason}); continue
        normalized = copy.deepcopy(dict(raw))
        normalized.update({"account_id": account["account_id"], "environment": account["environment"], "account_snapshot_id": account["account_snapshot_id"], "account_snapshot_sha256": account["canonical_record_sha256"], "position_ledger_snapshot_id": ledger["ledger_snapshot_id"], "position_ledger_snapshot_sha256": ledger["canonical_record_sha256"], "position_id": position["position_id"], "trade_id": position["trade_id"], "position_ledger_record_id": ledger["position_ledger_record_id"], "position_record_sha256": position["canonical_record_sha256"], "units": position["units"], "entry_price": position["entry_price"], "capacity_state_id": capacity["capacity_state_id"], "capacity_state_sha256": capacity["canonical_record_sha256"], "conflict_state_id": conflict["conflict_state_id"], "conflict_state_sha256": conflict["canonical_record_sha256"], "account_snapshot_at_utc": account["known_at_utc"], "position_ledger_at_utc": ledger["known_at_utc"], "portfolio_state_known_at_utc": max(capacity["known_at_utc"], conflict["known_at_utc"]), "switch_allowed_instruments": capacity["switch_allowed_instruments"], "maximum_switch_units": capacity["maximum_switch_units"], "conflicted_factor_ids": conflict["conflicted_factor_ids"], "known_at_utc": raw["hold_evaluated_at_utc"]})
        valid.append(normalized)
        seen_groups.add((key[0], key[2]))
    return valid, rejected


def _reseal_envelope(payload: Mapping[str, Any] | None, records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    if payload is None: return None
    result = copy.deepcopy(dict(payload)); result["records"] = [copy.deepcopy(dict(row)) for row in records]
    # v2 ignores canonical hashes, but filtering changes the envelope. Remove the
    # original v3 hash rather than pretending the filtered view is producer output.
    result.pop("canonical_envelope_sha256", None)
    return result


def _v2_compatibility_views(
    *,
    quotes: Sequence[Mapping[str, Any]],
    verifiers: Sequence[Mapping[str, Any]],
    economics: Sequence[Mapping[str, Any]],
    holds: Sequence[Mapping[str, Any]],
    accounts: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Translate canonical v3 references only inside the frozen v2 engine view.

    The producer-signed v3 records remain unchanged.  v2 historically hashes
    the whole supplied mapping, including the v3 canonical hash field, so its
    private compatibility references are derived here and never exposed as v3
    evidence identities.
    """
    q_rows = [copy.deepcopy(dict(row)) for row in quotes]
    v_rows = [copy.deepcopy(dict(row)) for row in verifiers]
    q_by_id = {str(row["quote_record_id"]): row for row in q_rows}
    v_by_id = {str(row["verifier_evidence_id"]): row for row in v_rows}
    e_rows: list[dict[str, Any]] = []
    for source in economics:
        row = copy.deepcopy(dict(source))
        quote = q_by_id[str(row["entry_quote_record_id"])]
        verifier = v_by_id[str(row["verifier_evidence_id"])]
        row["entry_quote_record_hash"] = stable_hash(quote)
        row["verifier_evidence_hash"] = stable_hash(verifier)
        row["forecast_id"] = str(row["forecast_record_id"])
        account = _bound_account_record(row, accounts, "account")
        ledger = _bound_account_record(row, accounts, "ledger")
        row["bound_account_snapshot_id"] = None if account is None else account.get("account_snapshot_id")
        row["bound_position_ledger_snapshot_id"] = None if ledger is None else ledger.get("ledger_snapshot_id")
        e_rows.append(row)
    h_rows: list[dict[str, Any]] = []
    for source in holds:
        row = copy.deepcopy(dict(source))
        quote = q_by_id[str(row["current_close_quote_record_id"])]
        verifier = v_by_id[str(row["hold_verifier_evidence_id"])]
        row["current_close_quote_record_hash"] = stable_hash(quote)
        row["hold_verifier_evidence_hash"] = stable_hash(verifier)
        h_rows.append(row)
    return q_rows, v_rows, e_rows, h_rows


def build_after_cost_counterfactual_v3(response_snapshot: Mapping[str, Any], *, contract: Mapping[str, Any], frozen_manifest: Mapping[str, Any], venue_quotes: Mapping[str, Any] | None = None, verifier_evidence: Mapping[str, Any] | None = None, economics_inputs: Mapping[str, Any] | None = None, hold_switch_inputs: Mapping[str, Any] | None = None, account_state: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if contract.get("contract_id") != "currency_state_after_cost_counterfactual_v3_20260817" or contract.get("counterfactual_cohort_id") != "currency_state_after_cost_counterfactual_cohort_20260817c":
        raise AfterCostV3Error("v3 contract/cohort identity mismatch")
    _validate_v3_contract(contract)
    try:
        manifest = _validate_manifest(frozen_manifest, contract)
        cutoff, response_sha, response_index = _response_grid(response_snapshot, contract)
    except AfterCostV2Error as exc:
        raise AfterCostV3Error(str(exc)) from exc
    response_id = str(response_snapshot.get("snapshot_id") or "")
    supplied = {"venue_quotes": venue_quotes, "verifier_evidence": verifier_evidence, "economics_inputs": economics_inputs, "hold_switch_inputs": hold_switch_inputs, "account_state": account_state}
    strict, hash_meta, records = {}, {}, {}
    for name in ENVELOPES:
        payload, normalized, meta = _strict_envelope(supplied[name], name=name, contract=contract, manifest=manifest)
        strict[name], records[name] = payload, normalized
        if meta: hash_meta[name] = meta[0]
    quote_valid, quote_rejections, quote_by_id = _validate_quotes(records["venue_quotes"])
    verifier_valid, verifier_rejections, verifier_by_id = _validate_verifier_bindings(
        records["verifier_evidence"], response_id=response_id,
        response_sha=response_sha, response_cutoff=cutoff,
        response_index=response_index,
    )
    verifier_hashes = {_record_id(row, "verifier_evidence"): row["canonical_record_sha256"] for row in verifier_valid}
    account_by_id, account_rejections = _account_records(records["account_state"])
    economics_valid, economics_rejections = _validate_economics_bindings(
        records["economics_inputs"], verifiers=verifier_by_id,
        verifier_hashes=verifier_hashes, quotes=quote_by_id,
        accounts=account_by_id, response_id=response_id,
        response_sha=response_sha, response_cutoff=cutoff,
        response_index=response_index,
    )
    hold_valid, hold_rejections = _validate_hold_bindings(
        records["hold_switch_inputs"], accounts=account_by_id,
        verifiers=verifier_by_id, verifier_hashes=verifier_hashes,
        quotes=quote_by_id, response_id=response_id,
        response_sha=response_sha, response_cutoff=cutoff,
        response_index=response_index,
    )
    q_v2, v_v2, e_v2, h_v2 = _v2_compatibility_views(
        quotes=quote_valid, verifiers=verifier_valid,
        economics=economics_valid, holds=hold_valid,
        accounts=account_by_id,
    )
    try:
        base = build_after_cost_counterfactual_v2(
            response_snapshot, contract=contract, frozen_manifest=frozen_manifest,
            venue_quotes=_reseal_envelope(strict["venue_quotes"], q_v2),
            verifier_evidence=_reseal_envelope(strict["verifier_evidence"], v_v2),
            economics_inputs=_reseal_envelope(strict["economics_inputs"], e_v2),
            hold_switch_inputs=_reseal_envelope(strict["hold_switch_inputs"], h_v2),
        )
    except AfterCostV2Error as exc:
        raise AfterCostV3Error(str(exc)) from exc
    pre_rejections = quote_rejections + verifier_rejections + economics_rejections + account_rejections + hold_rejections
    rejection_lookup = defaultdict(list)
    for item in economics_rejections: rejection_lookup[(item["arm_id"], item["instrument"], item["horizon_sec"])].append(item["reason"])
    econ_by_id = {str(row["economics_record_id"]): row for row in economics_valid}
    for row in base["records"]:
        key = (row["arm_id"], row["instrument"], row["horizon_sec"])
        if not row["economics_admissible"] and rejection_lookup.get(key): row["blockers"] = rejection_lookup[key]
        economics_id = row.get("economics_record_id")
        econ = econ_by_id.get(str(economics_id or ""))
        if econ:
            row["economics_canonical_record_sha256"] = econ["canonical_record_sha256"]
            row["verifier_canonical_record_sha256"] = verifier_hashes.get(str(econ.get("verifier_evidence_id") or ""))
            row["forecast_record_sha256"] = econ["forecast_record_sha256"]
            quote = quote_by_id.get(str(econ.get("entry_quote_record_id") or ""))
            row["quote_canonical_record_sha256"] = None if quote is None else quote["canonical_record_sha256"]
    row_hashes_by_economics = {
        str(row.get("economics_record_id") or ""): {
            "economics_canonical_record_sha256": row.get("economics_canonical_record_sha256"),
            "verifier_canonical_record_sha256": row.get("verifier_canonical_record_sha256"),
            "forecast_record_sha256": row.get("forecast_record_sha256"),
            "quote_canonical_record_sha256": row.get("quote_canonical_record_sha256"),
        }
        for row in base["records"] if row.get("economics_record_id")
    }
    for arm_payload in base["allocations"].values():
        for allocation in arm_payload.values():
            for candidate in allocation.get("disjoint_basket") or []:
                candidate.update(row_hashes_by_economics.get(str(candidate.get("economics_record_id") or ""), {}))
            if allocation.get("top_one"):
                allocation["top_one"].update(row_hashes_by_economics.get(str(allocation["top_one"].get("economics_record_id") or ""), {}))
    hold_by_id = {str(row["hold_record_id"]): row for row in hold_valid}
    for comparison in base["hold_switch_counterfactuals"]:
        hold = hold_by_id.get(str(comparison.get("hold_record_id") or ""))
        if hold is None:
            continue
        comparison.update({
            "hold_canonical_record_sha256": hold["canonical_record_sha256"],
            "account_canonical_record_sha256": hold["account_record_sha256"],
            "position_canonical_record_sha256": hold["position_record_sha256"],
            "ledger_canonical_record_sha256": hold["ledger_record_sha256"],
            "capacity_canonical_record_sha256": hold["capacity_record_sha256"],
            "conflict_canonical_record_sha256": hold["conflict_record_sha256"],
            "verifier_canonical_record_sha256": verifier_hashes.get(str(hold.get("hold_verifier_evidence_id") or "")),
            "quote_canonical_record_sha256": quote_by_id[str(hold["current_close_quote_record_id"])]["canonical_record_sha256"],
        })
        if comparison.get("best_switch_candidate"):
            comparison["best_switch_candidate"].update(row_hashes_by_economics.get(str(comparison["best_switch_candidate"].get("economics_record_id") or ""), {}))
    env_hashes, record_hashes = _canonical_maps(strict)
    admissible_records = {
        "venue_quotes": {_record_id(row, "venue_quotes"): row["canonical_record_sha256"] for row in quote_valid},
        "verifier_evidence": {_record_id(row, "verifier_evidence"): row["canonical_record_sha256"] for row in verifier_valid},
        "economics_inputs": {_record_id(row, "economics_inputs"): row["canonical_record_sha256"] for row in economics_valid},
        "hold_switch_inputs": {_record_id(row, "hold_switch_inputs"): row["canonical_record_sha256"] for row in hold_valid},
        "account_state": {_record_id(row, "account_state"): row["canonical_record_sha256"] for row in account_by_id.values()},
    }
    base.update({"schema_version": 3, "snapshot_schema": contract["snapshot_schema"], "counterfactual_contract_id": contract["contract_id"], "counterfactual_cohort_id": contract["counterfactual_cohort_id"], "supersedes_contract_id": contract["supersedes_contract_id"], "supersedes_cohort_id": contract["supersedes_cohort_id"], "canonical_input_hashes": {"envelopes": env_hashes, "records": record_hashes}, "admissible_input_hashes": {"envelopes": {name: value for name, value in env_hashes.items() if admissible_records.get(name)}, "records": admissible_records}, "strict_input_rejections": pre_rejections})
    base["input_rejections"] = pre_rejections + list(base.get("input_rejections") or [])
    base["summary"]["strict_input_rejection_count"] = len(pre_rejections)
    base["summary"]["input_rejection_count"] = len(base["input_rejections"])
    base["summary"]["submitted_account_state_record_count"] = len(records["account_state"])
    base["summary"]["admissible_account_state_record_count"] = len(account_by_id)
    base["summary"]["submitted_canonical_record_count"] = sum(len(value) for value in record_hashes.values())
    base["summary"]["admissible_canonical_record_count"] = sum(len(value) for value in admissible_records.values())
    base["summary"]["blocker_counts"] = dict(sorted(Counter(reason for row in base["records"] for reason in row["blockers"]).items()))
    base["limitations"] = ["v1 and v2 remain zero-evidence engineering-superseded cohorts", "canonical consumer-recomputed record and envelope hashes are mandatory", "verifier evidence is single-use and exact-cell/forecast/model bound", "Practice-007 hold state is recomputed from separate immutable account records", "all outputs remain research-only no_trade"]
    base["snapshot_id"] = "currency_state_after_cost_v3_" + stable_hash({key: value for key, value in base.items() if key != "snapshot_id"})[:24]
    return base


__all__ = ["AfterCostV3Error", "build_after_cost_counterfactual_v3"]
