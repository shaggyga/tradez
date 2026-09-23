"""Fail-closed after-cost counterfactuals for CurrencyState response arms.

This module is a pure research adapter.  It has no filesystem, database,
network, broker, lifecycle, authorization, supervisor, or execution imports.
It refuses to calculate expected value unless every probability, magnitude,
and cost component is frozen and point-in-time valid at the CurrencyState
decision cutoff.  Observed price response is explicitly ineligible as a
forward forecast.
"""

from __future__ import annotations

import copy
import datetime as dt
import re
import math
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from ..contracts.currency_state import stable_hash
from ..contracts.signed_currency_exposure import canonical_factor_ids, split_currency_pair
from ..features.currency_state_engine import parse_epoch


UTC = dt.timezone.utc
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_ARTIFACT_FINGERPRINTS = (
    "counterfactual_module_sha256",
    "counterfactual_config_file_sha256",
    "response_module_sha256",
    "response_config_file_sha256",
    "currency_state_module_sha256",
    "currency_state_contract_file_sha256",
    "official_context_module_sha256",
)


class AfterCostCounterfactualError(ValueError):
    """Raised when a frozen counterfactual contract is structurally invalid."""


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _utc_text(value: Any, *, field: str) -> str:
    epoch = parse_epoch(value)
    if epoch is None:
        raise AfterCostCounterfactualError(f"{field} must be a valid timestamp")
    return dt.datetime.fromtimestamp(epoch, tz=UTC).isoformat()


def _same_time(left: Any, right: Any) -> bool:
    left_epoch = parse_epoch(left)
    right_epoch = parse_epoch(right)
    return (
        left_epoch is not None
        and right_epoch is not None
        and math.isclose(left_epoch, right_epoch, abs_tol=1e-6)
    )


def _strings(value: Any, *, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise AfterCostCounterfactualError(f"{field} must be a sequence")
    return sorted({str(item) for item in value if str(item)})


def validate_counterfactual_contract(contract: Mapping[str, Any]) -> None:
    required = {
        "schema_version",
        "contract_id",
        "snapshot_schema",
        "counterfactual_cohort_id",
        "required_response_contract_id",
        "required_response_snapshot_schema",
        "required_state_contract_id",
        "expected_instrument_count",
        "expected_horizons_sec",
        "expected_arm_ids",
        "forward_admissible_arm_ids",
        "never_forward_arm_ids",
        "economics_input",
        "response_basis_gate",
        "selection",
        "hold_switch",
        "cost_accounting",
    }
    missing = sorted(required - set(contract))
    if missing:
        raise AfterCostCounterfactualError(f"counterfactual contract missing keys: {missing}")
    if contract.get("research_only") is not True:
        raise AfterCostCounterfactualError("counterfactual contract must be research-only")
    if contract.get("execution_eligible") is not False:
        raise AfterCostCounterfactualError("counterfactual contract cannot be execution eligible")
    if contract.get("can_place_orders") is not False:
        raise AfterCostCounterfactualError("counterfactual contract cannot place orders")
    if contract.get("supported_execution_decision") != "no_trade":
        raise AfterCostCounterfactualError("only no_trade may be supported")
    if not str(contract.get("counterfactual_cohort_id") or ""):
        raise AfterCostCounterfactualError("counterfactual cohort id is required")
    if contract.get("material_logic_cost_or_schema_change_requires_new_cohort") is not True:
        raise AfterCostCounterfactualError("material changes must require a new cohort")
    expected = [str(value) for value in contract["expected_arm_ids"]]
    forward = {str(value) for value in contract["forward_admissible_arm_ids"]}
    never = {str(value) for value in contract["never_forward_arm_ids"]}
    if set(expected) != forward | never or forward & never:
        raise AfterCostCounterfactualError("forward and never-forward arms must partition the arm set")
    if "observed_price_only" not in never:
        raise AfterCostCounterfactualError("observed_price_only must remain never-forward")
    basis_gate = contract["response_basis_gate"]
    if any(
        basis_gate.get(field) is not True
        for field in (
            "require_basis_eligible_for_after_cost",
            "require_proof_eligible_lineage",
            "require_basis_eligibility_contract_id",
            "mere_source_fact_presence_is_insufficient",
        )
    ):
        raise AfterCostCounterfactualError("response basis gate must remain fully fail-closed")
    if not str(basis_gate.get("required_basis_eligibility_contract_id") or ""):
        raise AfterCostCounterfactualError("required basis-eligibility contract id is missing")
    economics = contract["economics_input"]
    if float(economics.get("minimum_calibration_effective_n") or 0) <= 0:
        raise AfterCostCounterfactualError("calibration effective-N floor must be positive")
    if not economics.get("allowed_calibration_states"):
        raise AfterCostCounterfactualError("calibration state allowlist cannot be empty")
    selection = contract["selection"]
    if int(selection.get("maximum_disjoint_positions") or 0) <= 0:
        raise AfterCostCounterfactualError("maximum disjoint positions must be positive")


def _validated_fingerprints(value: Mapping[str, Any] | None) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise AfterCostCounterfactualError("artifact_fingerprints are required")
    normalized: dict[str, str] = {}
    for field in REQUIRED_ARTIFACT_FINGERPRINTS:
        digest = str(value.get(field) or "").lower()
        if not SHA256_PATTERN.fullmatch(digest):
            raise AfterCostCounterfactualError(f"artifact_fingerprints.{field} must be SHA-256")
        normalized[field] = digest
    normalized["fingerprint_manifest_id"] = "counterfactual_manifest_" + stable_hash(
        normalized
    )[:24]
    return normalized


def _validate_response_snapshot(
    snapshot: Mapping[str, Any], contract: Mapping[str, Any]
) -> tuple[str, list[int], dict[tuple[str, str, int], Mapping[str, Any]]]:
    if snapshot.get("snapshot_schema") != contract["required_response_snapshot_schema"]:
        raise AfterCostCounterfactualError("response snapshot schema is not the required version")
    if snapshot.get("arm_contract_id") != contract["required_response_contract_id"]:
        raise AfterCostCounterfactualError("response-arm contract is not the required version")
    if snapshot.get("input_contract_id") != contract["required_state_contract_id"]:
        raise AfterCostCounterfactualError("CurrencyState input contract is not the required version")
    if snapshot.get("research_only") is not True:
        raise AfterCostCounterfactualError("response snapshot must be research-only")
    if snapshot.get("execution_eligible") is not False or snapshot.get("can_place_orders") is not False:
        raise AfterCostCounterfactualError("response snapshot cannot be execution eligible")
    if snapshot.get("supported_execution_decision") != "no_trade":
        raise AfterCostCounterfactualError("response snapshot must remain no_trade")
    cutoff = _utc_text(snapshot.get("decision_cutoff_utc"), field="decision_cutoff_utc")
    horizons = sorted(int(value) for value in snapshot.get("horizons_sec") or [])
    if horizons != sorted(int(value) for value in contract["expected_horizons_sec"]):
        raise AfterCostCounterfactualError("response horizon grid differs from the contract")
    if int(snapshot.get("instrument_count") or 0) != int(contract["expected_instrument_count"]):
        raise AfterCostCounterfactualError("response instrument count differs from the contract")
    arms = snapshot.get("arms") or {}
    if list(arms) != list(contract["expected_arm_ids"]):
        raise AfterCostCounterfactualError("response arm order differs from the frozen contract")
    expected_rows = int(contract["expected_instrument_count"]) * len(horizons)
    index: dict[tuple[str, str, int], Mapping[str, Any]] = {}
    for arm_id in contract["expected_arm_ids"]:
        arm = arms.get(arm_id) or {}
        records = arm.get("records") or []
        if len(records) != expected_rows:
            raise AfterCostCounterfactualError(f"arm {arm_id} does not retain the full edge grid")
        if arm.get("execution_eligible") is not False or arm.get("can_place_orders") is not False:
            raise AfterCostCounterfactualError(f"arm {arm_id} became execution eligible")
        for row in records:
            if row.get("execution_eligible") is not False or row.get("can_place_orders") is not False:
                raise AfterCostCounterfactualError("response record became execution eligible")
            if row.get("supported_execution_decision") != "no_trade":
                raise AfterCostCounterfactualError("response record supports a non-no_trade decision")
            instrument = str(row.get("instrument") or "")
            horizon = int(row.get("horizon_sec") or 0)
            key = (str(arm_id), instrument, horizon)
            if horizon not in horizons or key in index:
                raise AfterCostCounterfactualError("response grid contains an invalid or duplicate key")
            split_currency_pair(instrument)
            index[key] = row
    if len(index) != len(contract["expected_arm_ids"]) * expected_rows:
        raise AfterCostCounterfactualError("response grid is incomplete")
    return cutoff, horizons, index


def _validate_envelope(
    payload: Mapping[str, Any] | None,
    *,
    name: str,
    cutoff_utc: str,
) -> tuple[dict[str, Any] | None, list[Mapping[str, Any]]]:
    if payload is None:
        return None, []
    if not isinstance(payload, Mapping):
        raise AfterCostCounterfactualError(f"{name} must be a mapping")
    for field in (
        "contract_id",
        "contract_sha256",
        "cohort_id",
        "cohort_sha256",
        "frozen_at_utc",
        "decision_cutoff_utc",
    ):
        if not str(payload.get(field) or ""):
            raise AfterCostCounterfactualError(f"{name}.{field} is required")
    for field in ("contract_sha256", "cohort_sha256"):
        if not SHA256_PATTERN.fullmatch(str(payload[field]).lower()):
            raise AfterCostCounterfactualError(f"{name}.{field} must be SHA-256")
    if not _same_time(payload["decision_cutoff_utc"], cutoff_utc):
        raise AfterCostCounterfactualError(f"{name} cutoff must equal the response cutoff")
    frozen_epoch = parse_epoch(payload["frozen_at_utc"])
    cutoff_epoch = parse_epoch(cutoff_utc)
    if frozen_epoch is None or frozen_epoch > cutoff_epoch:
        raise AfterCostCounterfactualError(f"{name} was frozen after the response cutoff")
    if payload.get("research_only") is not True:
        raise AfterCostCounterfactualError(f"{name} must be research-only")
    if payload.get("execution_eligible") is not False or payload.get("can_place_orders", False) is not False:
        raise AfterCostCounterfactualError(f"{name} cannot be execution eligible")
    if payload.get("supported_execution_decision") != "no_trade":
        raise AfterCostCounterfactualError(f"{name} must remain no_trade")
    records = payload.get("records") or []
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise AfterCostCounterfactualError(f"{name}.records must be a sequence")
    if any(not isinstance(row, Mapping) for row in records):
        raise AfterCostCounterfactualError(f"{name}.records must contain mappings")
    return (
        {
            "contract_id": str(payload["contract_id"]),
            "contract_sha256": str(payload["contract_sha256"]).lower(),
            "cohort_id": str(payload["cohort_id"]),
            "cohort_sha256": str(payload["cohort_sha256"]).lower(),
            "frozen_at_utc": _utc_text(payload["frozen_at_utc"], field=f"{name}.frozen_at_utc"),
            "payload_id": str(payload.get("payload_id") or ""),
        },
        list(records),
    )


def _response_lineage_sets(row: Mapping[str, Any]) -> tuple[list[str], list[str], list[str]]:
    lineage = row.get("lineage") or {}
    official = lineage.get("official_thesis") or {}
    return (
        _strings(official.get("source_ids"), field="response.official_thesis.source_ids"),
        _strings(official.get("source_fact_ids"), field="response.official_thesis.source_fact_ids"),
        _strings(
            official.get("independent_factor_ids"),
            field="response.official_thesis.independent_factor_ids",
        ),
    )


def _response_basis_blocker(
    row: Mapping[str, Any], contract: Mapping[str, Any]
) -> str | None:
    """Require an explicit upstream eligibility proof; never infer it from fact IDs."""

    official = ((row.get("lineage") or {}).get("official_thesis") or {})
    if official.get("basis_eligible_for_after_cost") is not True:
        return "response_basis_not_explicitly_eligible_for_after_cost"
    if official.get("proof_eligible_lineage") is not True:
        return "response_lineage_not_explicitly_proof_eligible"
    actual_contract_id = str(official.get("basis_eligibility_contract_id") or "")
    required_contract_id = str(
        contract["response_basis_gate"]["required_basis_eligibility_contract_id"]
    )
    if actual_contract_id != required_contract_id:
        return "missing_or_incompatible_basis_eligibility_contract_id"
    return None


def _reject(
    rejections: list[dict[str, Any]],
    *,
    kind: str,
    record_id: str,
    arm_id: str,
    instrument: str,
    horizon_sec: int,
    reason: str,
) -> None:
    rejections.append(
        {
            "kind": kind,
            "record_id": record_id,
            "arm_id": arm_id,
            "instrument": instrument,
            "horizon_sec": horizon_sec,
            "reason": reason,
        }
    )


def _valid_probability(value: Any, *, minimum: float, maximum: float) -> float | None:
    number = _finite(value)
    if number is None or number < minimum or number > maximum:
        return None
    return number


def _validate_component_clocks(
    raw: Mapping[str, Any],
    *,
    fields: Iterable[str],
    known_epoch: float,
    frozen_epoch: float,
    cutoff_epoch: float,
) -> str | None:
    for field in fields:
        value = parse_epoch(raw.get(field))
        if value is None:
            return f"missing_{field}"
        if value > known_epoch:
            return f"{field}_after_record_knowledge"
        if value > frozen_epoch:
            return f"{field}_after_envelope_freeze"
        if value > cutoff_epoch:
            return f"{field}_after_decision_cutoff"
    return None


def _validate_executable_spread(
    row: Mapping[str, Any], contract: Mapping[str, Any]
) -> tuple[float | None, float | None, float | None, str | None]:
    edge = row.get("source_edge") or {}
    bid = _finite(edge.get("bid"))
    ask = _finite(edge.get("ask"))
    pip = _finite(edge.get("pip"))
    spread = _finite((row.get("cost_context") or {}).get("observed_entry_spread_bps"))
    if bid is None or ask is None or pip is None or spread is None:
        return None, None, None, "missing_executable_bid_ask_spread_or_pip"
    if bid <= 0 or ask < bid or pip <= 0 or spread < 0:
        return None, None, None, "invalid_executable_bid_ask_spread_or_pip"
    mid = (bid + ask) / 2.0
    recalculated = (ask - bid) / mid * 10_000.0
    tolerance = float(contract["economics_input"]["spread_recalculation_tolerance_bps"])
    if not math.isclose(spread, recalculated, rel_tol=0.0, abs_tol=tolerance):
        return None, None, None, "spread_not_consistent_with_executable_bid_ask"
    return spread, mid, pip, None


def _index_economics(
    records: Iterable[Mapping[str, Any]],
    *,
    identity: Mapping[str, Any] | None,
    cutoff_utc: str,
    response_index: Mapping[tuple[str, str, int], Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> tuple[dict[tuple[str, str, int], dict[str, Any]], list[dict[str, Any]]]:
    index: dict[tuple[str, str, int], dict[str, Any]] = {}
    seen: set[tuple[str, str, int]] = set()
    rejections: list[dict[str, Any]] = []
    economics = contract["economics_input"]
    forward = set(contract["forward_admissible_arm_ids"])
    cutoff_epoch = parse_epoch(cutoff_utc)
    frozen_epoch = parse_epoch(None if identity is None else identity["frozen_at_utc"])
    for raw in records:
        record_id = str(raw.get("economics_record_id") or "")
        arm_id = str(raw.get("arm_id") or "")
        instrument = str(raw.get("instrument") or "")
        try:
            horizon = int(raw.get("horizon_sec"))
        except (TypeError, ValueError):
            horizon = 0
        key = (arm_id, instrument, horizon)

        def reject(reason: str) -> None:
            _reject(
                rejections,
                kind="economics",
                record_id=record_id,
                arm_id=arm_id,
                instrument=instrument,
                horizon_sec=horizon,
                reason=reason,
            )

        if not record_id:
            reject("missing_economics_record_id")
            continue
        if key in seen:
            raise AfterCostCounterfactualError(f"duplicate economics record for {key}")
        seen.add(key)
        response_row = response_index.get(key)
        if response_row is None:
            reject("outside_response_grid")
            continue
        if arm_id not in forward:
            reject("arm_not_forward_admissible")
            continue
        if response_row.get("paper_candidate") is not True or response_row.get("forward_hypothesis") is not True:
            reject("response_arm_has_no_forward_candidate")
            continue
        basis_blocker = _response_basis_blocker(response_row, contract)
        if basis_blocker:
            reject(basis_blocker)
            continue
        if str(response_row.get("direction_origin") or "").startswith("observed_price"):
            reject("observed_price_hindsight_forbidden")
            continue
        direction = str(raw.get("forecast_direction") or "").lower()
        if direction not in {"buy", "sell"} or direction != response_row.get("research_direction"):
            reject("forecast_direction_does_not_match_frozen_response_direction")
            continue
        known_epoch = parse_epoch(raw.get("known_at_utc"))
        if known_epoch is None:
            reject("missing_known_at_utc")
            continue
        if frozen_epoch is None or known_epoch > frozen_epoch:
            reject("record_known_after_envelope_freeze")
            continue
        if known_epoch > cutoff_epoch:
            reject("record_known_after_decision_cutoff")
            continue
        clock_reason = _validate_component_clocks(
            raw,
            fields=economics["required_component_clock_fields"],
            known_epoch=known_epoch,
            frozen_epoch=frozen_epoch,
            cutoff_epoch=cutoff_epoch,
        )
        if clock_reason:
            reject(clock_reason)
            continue
        missing_identity = [
            field for field in economics["required_identity_fields"] if not str(raw.get(field) or "")
        ]
        if missing_identity:
            reject("missing_identity_fields:" + ",".join(missing_identity))
            continue
        invalid_hash_fields = [
            field
            for field in economics["required_identity_fields"]
            if field.endswith("_sha256")
            and not SHA256_PATTERN.fullmatch(str(raw.get(field) or "").lower())
        ]
        if invalid_hash_fields:
            reject("invalid_identity_hash_fields:" + ",".join(invalid_hash_fields))
            continue
        if raw.get("point_in_time_validated") is not True:
            reject("point_in_time_validation_not_proven")
            continue
        calibration_state = str(raw.get("calibration_state") or "")
        if calibration_state not in economics["allowed_calibration_states"]:
            reject("calibration_state_not_allowed")
            continue
        effective_n = _finite(raw.get("calibration_effective_n"))
        if effective_n is None or effective_n < float(economics["minimum_calibration_effective_n"]):
            reject("calibration_effective_n_below_floor")
            continue
        calibration_cutoff = parse_epoch(raw.get("calibration_data_cutoff_utc"))
        probability_known = parse_epoch(raw.get("probability_known_at_utc"))
        if calibration_cutoff is None:
            reject("missing_calibration_data_cutoff_utc")
            continue
        if calibration_cutoff > probability_known:
            reject("calibration_data_cutoff_after_probability_knowledge")
            continue
        p_direction = _valid_probability(
            raw.get("calibrated_direction_probability"),
            minimum=float(economics["minimum_direction_probability"]),
            maximum=float(economics["maximum_probability"]),
        )
        p_clear = _valid_probability(
            raw.get("calibrated_cost_clear_probability"),
            minimum=0.0,
            maximum=float(economics["maximum_probability"]),
        )
        if p_direction is None or p_clear is None:
            reject("invalid_calibrated_probability")
            continue
        numeric_fields = (
            "expected_favorable_move_bps",
            "expected_adverse_move_bps",
            "modeled_slippage_bps",
            "latency_cost_bps",
            "rotation_cost_bps",
        )
        values = {field: _finite(raw.get(field)) for field in numeric_fields}
        if any(value is None or value < 0 for value in values.values()):
            reject("missing_or_negative_magnitude_or_cost")
            continue
        response_sources, response_facts, response_factors = _response_lineage_sets(response_row)
        source_ids = _strings(raw.get("source_ids"), field="economics.source_ids")
        source_fact_ids = _strings(raw.get("source_fact_ids"), field="economics.source_fact_ids")
        factor_ids = _strings(raw.get("independent_factor_ids"), field="economics.independent_factor_ids")
        if not source_ids or not set(response_sources).issubset(source_ids):
            reject("source_lineage_does_not_cover_response_thesis")
            continue
        if not set(response_facts).issubset(source_fact_ids):
            reject("source_fact_lineage_does_not_cover_response_thesis")
            continue
        if not response_factors or factor_ids != response_factors:
            reject("factor_lineage_does_not_exactly_match_response_thesis")
            continue
        spread_bps, mid, pip, spread_reason = _validate_executable_spread(response_row, contract)
        if spread_reason:
            reject(spread_reason)
            continue
        gross = p_direction * values["expected_favorable_move_bps"] - (
            1.0 - p_direction
        ) * values["expected_adverse_move_bps"]
        total_cost = spread_bps + values["modeled_slippage_bps"] + values["latency_cost_bps"] + values["rotation_cost_bps"]
        net = gross - total_cost
        bps_to_pips = mid * 0.0001 / pip
        normalized = copy.deepcopy(dict(raw))
        normalized.update(
            {
                "known_at_utc": _utc_text(raw["known_at_utc"], field="economics.known_at_utc"),
                "forecast_direction": direction,
                "calibrated_direction_probability": p_direction,
                "calibrated_cost_clear_probability": p_clear,
                "calibration_effective_n": effective_n,
                "expected_favorable_move_bps": values["expected_favorable_move_bps"],
                "expected_adverse_move_bps": values["expected_adverse_move_bps"],
                "modeled_slippage_bps": values["modeled_slippage_bps"],
                "latency_cost_bps": values["latency_cost_bps"],
                "rotation_cost_bps": values["rotation_cost_bps"],
                "source_ids": source_ids,
                "source_fact_ids": source_fact_ids,
                "independent_factor_ids": factor_ids,
                "executable_spread_bps": spread_bps,
                "expected_gross_ev_bps": gross,
                "expected_total_cost_bps": total_cost,
                "expected_after_cost_ev_bps": net,
                "expected_after_cost_ev_pips": net * bps_to_pips,
                "bps_to_pips_at_cutoff": bps_to_pips,
            }
        )
        index[key] = normalized
    rejections.sort(
        key=lambda row: (
            row["kind"], row["arm_id"], row["horizon_sec"], row["instrument"], row["record_id"], row["reason"]
        )
    )
    return index, rejections


def _compact_lineage(row: Mapping[str, Any]) -> dict[str, Any]:
    lineage = copy.deepcopy(dict(row.get("lineage") or {}))
    return {
        "input_snapshot_id": lineage.get("input_snapshot_id"),
        "base_currency_state_snapshot_id": lineage.get("base_currency_state_snapshot_id"),
        "official_fact_snapshot_id": lineage.get("official_fact_snapshot_id"),
        "official_fact_adapter_contract_id": lineage.get("official_fact_adapter_contract_id"),
        "component_context_contract_id": lineage.get("component_context_contract_id"),
        "base_official_fact_ids": lineage.get("base_official_fact_ids") or [],
        "quote_official_fact_ids": lineage.get("quote_official_fact_ids") or [],
        "source_contract_ids": lineage.get("source_contract_ids") or [],
        "source_cohort_ids": lineage.get("source_cohort_ids") or [],
        "official_thesis": lineage.get("official_thesis"),
        "technical_state": lineage.get("technical_state"),
        "magnitude_estimate": lineage.get("magnitude_estimate"),
    }


def _row_blockers(
    response_row: Mapping[str, Any],
    *,
    arm_id: str,
    economics: Mapping[str, Any] | None,
    rejection_reasons: Sequence[str],
    contract: Mapping[str, Any],
) -> list[str]:
    if arm_id == "observed_price_only":
        return ["observed_price_hindsight_forbidden_as_forward_forecast"]
    if arm_id in set(contract["never_forward_arm_ids"]):
        return ["arm_not_directional_forward_hypothesis"]
    if response_row.get("paper_candidate") is not True or response_row.get("forward_hypothesis") is not True:
        return ["response_arm_has_no_forward_candidate"]
    basis_blocker = _response_basis_blocker(response_row, contract)
    if basis_blocker:
        return [basis_blocker]
    if economics is None:
        return list(rejection_reasons) or ["missing_admissible_economics_record"]
    return []


def _selection_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "arm_id": row["arm_id"],
        "instrument": row["instrument"],
        "horizon_sec": row["horizon_sec"],
        "research_direction": row["research_direction"],
        "expected_after_cost_ev_bps": row["expected_after_cost_ev_bps"],
        "expected_after_cost_ev_pips": row["expected_after_cost_ev_pips"],
        "calibrated_cost_clear_probability": row["calibrated_cost_clear_probability"],
        "rank_within_arm_horizon": row["rank_within_arm_horizon"],
        "independent_factor_ids": list(row["independent_factor_ids"]),
        "lineage_id": row["lineage_id"],
        "economics_record_id": row["economics_record_id"],
    }


def _allocate(
    rows: list[dict[str, Any]],
    *,
    horizons: Sequence[int],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    selection = contract["selection"]
    forward = set(contract["forward_admissible_arm_ids"])
    allocations: dict[str, Any] = {}
    for arm_id in contract["expected_arm_ids"]:
        per_horizon: dict[str, Any] = {}
        for horizon in horizons:
            group = [
                row
                for row in rows
                if row["arm_id"] == arm_id
                and row["horizon_sec"] == horizon
                and row["economics_admissible"]
            ]
            group.sort(
                key=lambda row: (-float(row["expected_after_cost_ev_bps"]), row["instrument"])
            )
            for rank, row in enumerate(group, start=1):
                row["rank_within_arm_horizon"] = rank
                row["selectable_counterfactual"] = bool(
                    row["expected_after_cost_ev_bps"]
                    >= float(selection["minimum_after_cost_ev_bps"])
                    and row["calibrated_cost_clear_probability"]
                    >= float(selection["minimum_cost_clear_probability"])
                )
            selectable = [row for row in group if row["selectable_counterfactual"]]
            if arm_id not in forward:
                state = "unavailable"
                reason = "arm_not_forward_admissible"
                top = None
                basket: list[dict[str, Any]] = []
            elif not selectable:
                state = "no_admissible_positive_after_cost_candidate"
                reason = "no_row_clears_frozen_ev_and_probability_thresholds"
                top = None
                basket = []
            else:
                state = "paper_counterfactual_available"
                reason = None
                top = _selection_payload(selectable[0])
                basket = []
                used_currencies: set[str] = set()
                used_factors: set[str] = set()
                for row in selectable:
                    base, quote = split_currency_pair(row["instrument"])
                    currencies = {base, quote}
                    factors = set(row["independent_factor_ids"])
                    if selection["disallow_shared_currency"] and currencies & used_currencies:
                        continue
                    if selection["disallow_shared_signed_factor"] and factors & used_factors:
                        continue
                    basket.append(_selection_payload(row))
                    used_currencies.update(currencies)
                    used_factors.update(factors)
                    if len(basket) >= int(selection["maximum_disjoint_positions"]):
                        break
            per_horizon[str(horizon)] = {
                "state": state,
                "reason": reason,
                "admissible_ranked_count": len(group),
                "selectable_count": len(selectable),
                "top_one": top,
                "disjoint_basket": basket,
                "research_only": True,
                "execution_eligible": False,
                "can_place_orders": False,
                "supported_execution_decision": "no_trade",
            }
        allocations[arm_id] = per_horizon
    return allocations


def _index_holds(
    records: Iterable[Mapping[str, Any]],
    *,
    identity: Mapping[str, Any] | None,
    cutoff_utc: str,
    response_index: Mapping[tuple[str, str, int], Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    economics = contract["economics_input"]
    forward = set(contract["forward_admissible_arm_ids"])
    cutoff_epoch = parse_epoch(cutoff_utc)
    frozen_epoch = parse_epoch(None if identity is None else identity["frozen_at_utc"])
    clock_fields = (
        "probability_known_at_utc",
        "magnitude_known_at_utc",
        "exit_cost_known_at_utc",
    )
    for raw in records:
        record_id = str(raw.get("hold_record_id") or "")
        arm_id = str(raw.get("arm_id") or "")
        instrument = str(raw.get("instrument") or "")
        try:
            horizon = int(raw.get("horizon_sec"))
        except (TypeError, ValueError):
            horizon = 0

        def reject(reason: str) -> None:
            _reject(
                rejections,
                kind="hold",
                record_id=record_id,
                arm_id=arm_id,
                instrument=instrument,
                horizon_sec=horizon,
                reason=reason,
            )

        if not record_id:
            reject("missing_hold_record_id")
            continue
        arm_horizon = (arm_id, horizon)
        if arm_horizon in seen:
            raise AfterCostCounterfactualError(f"duplicate hold record for {arm_horizon}")
        seen.add(arm_horizon)
        response_row = response_index.get((arm_id, instrument, horizon))
        if response_row is None:
            reject("outside_response_grid")
            continue
        if arm_id not in forward:
            reject("arm_not_forward_admissible")
            continue
        if contract["hold_switch"]["require_current_forward_arm_record"] and (
            response_row.get("paper_candidate") is not True
            or response_row.get("forward_hypothesis") is not True
        ):
            reject("current_response_arm_has_no_forward_candidate")
            continue
        basis_blocker = _response_basis_blocker(response_row, contract)
        if basis_blocker:
            reject(basis_blocker)
            continue
        direction = str(raw.get("hold_direction") or "").lower()
        if direction not in {"buy", "sell"}:
            reject("invalid_hold_direction")
            continue
        known_epoch = parse_epoch(raw.get("known_at_utc"))
        if known_epoch is None:
            reject("missing_known_at_utc")
            continue
        if frozen_epoch is None or known_epoch > frozen_epoch or known_epoch > cutoff_epoch:
            reject("hold_known_after_freeze_or_cutoff")
            continue
        clock_reason = _validate_component_clocks(
            raw,
            fields=clock_fields,
            known_epoch=known_epoch,
            frozen_epoch=frozen_epoch,
            cutoff_epoch=cutoff_epoch,
        )
        if clock_reason:
            reject(clock_reason)
            continue
        if raw.get("point_in_time_validated") is not True:
            reject("point_in_time_validation_not_proven")
            continue
        if str(raw.get("calibration_state") or "") not in economics["allowed_calibration_states"]:
            reject("calibration_state_not_allowed")
            continue
        effective_n = _finite(raw.get("calibration_effective_n"))
        if effective_n is None or effective_n < float(economics["minimum_calibration_effective_n"]):
            reject("calibration_effective_n_below_floor")
            continue
        p_direction = _valid_probability(
            raw.get("calibrated_direction_probability"),
            minimum=float(economics["minimum_direction_probability"]),
            maximum=float(economics["maximum_probability"]),
        )
        if p_direction is None:
            reject("invalid_calibrated_probability")
            continue
        numeric_fields = (
            "expected_favorable_move_bps",
            "expected_adverse_move_bps",
            "modeled_exit_spread_bps",
            "modeled_exit_slippage_bps",
            "modeled_exit_latency_bps",
        )
        values = {field: _finite(raw.get(field)) for field in numeric_fields}
        if any(value is None or value < 0 for value in values.values()):
            reject("missing_or_negative_hold_magnitude_or_cost")
            continue
        for field in (
            "hold_model_contract_id",
            "hold_model_contract_sha256",
            "calibration_contract_id",
            "calibration_contract_sha256",
            "cost_model_contract_id",
            "cost_model_contract_sha256",
        ):
            if not str(raw.get(field) or ""):
                reject(f"missing_{field}")
                break
        else:
            invalid_hashes = [
                field
                for field in (
                    "hold_model_contract_sha256",
                    "calibration_contract_sha256",
                    "cost_model_contract_sha256",
                )
                if not SHA256_PATTERN.fullmatch(str(raw.get(field) or "").lower())
            ]
            if invalid_hashes:
                reject("invalid_hold_contract_hashes:" + ",".join(invalid_hashes))
                continue
            factors = _strings(raw.get("independent_factor_ids"), field="hold.independent_factor_ids")
            allowed_factors = set(canonical_factor_ids(instrument, direction))
            if not factors or not set(factors).issubset(allowed_factors):
                reject("hold_factor_lineage_is_not_pair_consistent")
                continue
            sources = _strings(raw.get("source_ids"), field="hold.source_ids")
            if not sources:
                reject("missing_hold_source_ids")
                continue
            gross = p_direction * values["expected_favorable_move_bps"] - (
                1.0 - p_direction
            ) * values["expected_adverse_move_bps"]
            cost = values["modeled_exit_spread_bps"] + values["modeled_exit_slippage_bps"] + values["modeled_exit_latency_bps"]
            valid.append(
                {
                    "hold_record_id": record_id,
                    "arm_id": arm_id,
                    "instrument": instrument,
                    "horizon_sec": horizon,
                    "hold_direction": direction,
                    "known_at_utc": _utc_text(raw["known_at_utc"], field="hold.known_at_utc"),
                    "expected_remaining_gross_ev_bps": gross,
                    "expected_remaining_cost_bps": cost,
                    "expected_remaining_after_cost_ev_bps": gross - cost,
                    "independent_factor_ids": factors,
                    "source_ids": sources,
                    "hold_record_hash": stable_hash(raw),
                }
            )
    rejections.sort(
        key=lambda row: (
            row["kind"], row["arm_id"], row["horizon_sec"], row["instrument"], row["record_id"], row["reason"]
        )
    )
    return valid, rejections


def _hold_switch_comparisons(
    holds: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    minimum_increment = float(contract["hold_switch"]["minimum_incremental_switch_ev_bps"])
    for hold in holds:
        alternatives = [
            row
            for row in rows
            if row["arm_id"] == hold["arm_id"]
            and row["horizon_sec"] == hold["horizon_sec"]
            and row["selectable_counterfactual"]
            and not (
                row["instrument"] == hold["instrument"]
                and row["research_direction"] == hold["hold_direction"]
            )
        ]
        alternatives.sort(
            key=lambda row: (-float(row["expected_after_cost_ev_bps"]), row["instrument"])
        )
        best = alternatives[0] if alternatives else None
        incremental = None if best is None else (
            float(best["expected_after_cost_ev_bps"])
            - float(hold["expected_remaining_after_cost_ev_bps"])
        )
        switch = best is not None and incremental >= minimum_increment
        result.append(
            {
                **copy.deepcopy(dict(hold)),
                "state": "paper_switch" if switch else "paper_hold",
                "best_switch_candidate": None if best is None else _selection_payload(best),
                "incremental_switch_ev_bps": incremental,
                "minimum_incremental_switch_ev_bps": minimum_increment,
                "reason": (
                    "best_switch_clears_frozen_incremental_value"
                    if switch
                    else "no_switch_clears_frozen_incremental_value"
                ),
                "research_only": True,
                "execution_eligible": False,
                "can_place_orders": False,
                "supported_execution_decision": "no_trade",
            }
        )
    return result


def build_after_cost_counterfactual(
    response_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    artifact_fingerprints: Mapping[str, Any],
    economics_inputs: Mapping[str, Any] | None = None,
    hold_states: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a complete fail-closed arm/pair/horizon counterfactual snapshot."""

    validate_counterfactual_contract(contract)
    fingerprints = _validated_fingerprints(artifact_fingerprints)
    cutoff, horizons, response_index = _validate_response_snapshot(response_snapshot, contract)
    economics_identity, economics_records = _validate_envelope(
        economics_inputs, name="economics_inputs", cutoff_utc=cutoff
    )
    hold_identity, hold_records = _validate_envelope(
        hold_states, name="hold_states", cutoff_utc=cutoff
    )
    economics_index, economics_rejections = _index_economics(
        economics_records,
        identity=economics_identity,
        cutoff_utc=cutoff,
        response_index=response_index,
        contract=contract,
    )
    hold_index, hold_rejections = _index_holds(
        hold_records,
        identity=hold_identity,
        cutoff_utc=cutoff,
        response_index=response_index,
        contract=contract,
    )
    rejection_lookup: dict[tuple[str, str, int], list[str]] = defaultdict(list)
    for rejection in economics_rejections:
        rejection_lookup[
            (rejection["arm_id"], rejection["instrument"], rejection["horizon_sec"])
        ].append(rejection["reason"])

    lineage_registry: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for key in sorted(response_index, key=lambda value: (value[0], value[2], value[1])):
        arm_id, instrument, horizon = key
        response_row = response_index[key]
        economics = economics_index.get(key)
        lineage = _compact_lineage(response_row)
        lineage_id = "counterfactual_lineage_" + stable_hash(lineage)[:24]
        lineage_registry.setdefault(lineage_id, lineage)
        response_sources, response_facts, response_factors = _response_lineage_sets(response_row)
        blockers = _row_blockers(
            response_row,
            arm_id=arm_id,
            economics=economics,
            rejection_reasons=rejection_lookup.get(key, []),
            contract=contract,
        )
        row = {
            "arm_id": arm_id,
            "instrument": instrument,
            "base_currency": str(response_row.get("base_currency") or ""),
            "quote_currency": str(response_row.get("quote_currency") or ""),
            "horizon_sec": horizon,
            "research_direction": response_row.get("research_direction"),
            "direction_origin": response_row.get("direction_origin"),
            "source_response_state": response_row.get("state"),
            "source_response_record_hash": stable_hash(response_row),
            "lineage_id": lineage_id,
            "source_ids": response_sources,
            "source_fact_ids": response_facts,
            "independent_factor_ids": response_factors,
            "economics_record_id": None if economics is None else economics["economics_record_id"],
            "economics_record_hash": None if economics is None else stable_hash(economics),
            "economics_admissible": economics is not None and not blockers,
            "blockers": blockers,
            "calibrated_direction_probability": None if economics is None else economics["calibrated_direction_probability"],
            "calibrated_cost_clear_probability": None if economics is None else economics["calibrated_cost_clear_probability"],
            "expected_favorable_move_bps": None if economics is None else economics["expected_favorable_move_bps"],
            "expected_adverse_move_bps": None if economics is None else economics["expected_adverse_move_bps"],
            "expected_gross_ev_bps": None if economics is None else economics["expected_gross_ev_bps"],
            "expected_costs_bps": {
                "executable_spread": None if economics is None else economics["executable_spread_bps"],
                "modeled_slippage": None if economics is None else economics["modeled_slippage_bps"],
                "latency": None if economics is None else economics["latency_cost_bps"],
                "rotation": None if economics is None else economics["rotation_cost_bps"],
                "total": None if economics is None else economics["expected_total_cost_bps"],
            },
            "expected_after_cost_ev_bps": None if economics is None else economics["expected_after_cost_ev_bps"],
            "expected_after_cost_ev_pips": None if economics is None else economics["expected_after_cost_ev_pips"],
            "rank_within_arm_horizon": None,
            "selectable_counterfactual": False,
            "paper_decision": "no_trade",
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "supported_execution_decision": "no_trade",
        }
        output_rows.append(row)

    allocations = _allocate(output_rows, horizons=horizons, contract=contract)
    hold_switch = _hold_switch_comparisons(hold_index, output_rows, contract)
    blocker_counts = Counter(blocker for row in output_rows for blocker in row["blockers"])
    summary = {
        "row_count": len(output_rows),
        "economics_input_record_count": len(economics_records),
        "economics_admissible_count": sum(row["economics_admissible"] for row in output_rows),
        "ranked_count": sum(row["rank_within_arm_horizon"] is not None for row in output_rows),
        "selectable_counterfactual_count": sum(row["selectable_counterfactual"] for row in output_rows),
        "top_one_available_count": sum(
            allocation["top_one"] is not None
            for arm in allocations.values()
            for allocation in arm.values()
        ),
        "disjoint_basket_member_count": sum(
            len(allocation["disjoint_basket"])
            for arm in allocations.values()
            for allocation in arm.values()
        ),
        "hold_input_record_count": len(hold_records),
        "hold_admissible_count": len(hold_index),
        "hold_switch_comparison_count": len(hold_switch),
        "economics_rejection_count": len(economics_rejections),
        "hold_rejection_count": len(hold_rejections),
        "blocker_counts": dict(sorted(blocker_counts.items())),
    }
    output = {
        "schema_version": int(contract["schema_version"]),
        "snapshot_schema": str(contract["snapshot_schema"]),
        "counterfactual_contract_id": str(contract["contract_id"]),
        "counterfactual_cohort_id": str(contract["counterfactual_cohort_id"]),
        "counterfactual_contract_sha256": stable_hash(contract),
        "artifact_fingerprints": fingerprints,
        "decision_cutoff_utc": cutoff,
        "response_snapshot_id": str(response_snapshot.get("snapshot_id") or ""),
        "response_snapshot_sha256": stable_hash(response_snapshot),
        "response_arm_contract_id": str(response_snapshot.get("arm_contract_id") or ""),
        "currency_state_snapshot_id": str(response_snapshot.get("input_snapshot_id") or ""),
        "economics_input_envelope": economics_identity,
        "hold_state_envelope": hold_identity,
        "instrument_count": int(contract["expected_instrument_count"]),
        "horizons_sec": horizons,
        "arm_ids": list(contract["expected_arm_ids"]),
        "edge_horizon_count": int(contract["expected_instrument_count"]) * len(horizons),
        "row_count": len(output_rows),
        "lineage_registry": lineage_registry,
        "records": output_rows,
        "allocations": allocations,
        "hold_switch_counterfactuals": hold_switch,
        "input_rejections": economics_rejections + hold_rejections,
        "summary": summary,
        "limitations": [
            "observed-price response is hindsight context and is never admitted as a forward forecast",
            "missing calibrated probability, magnitude, spread, slippage, latency, or rotation cost makes EV and rank unavailable rather than zero",
            "top-one, disjoint-basket, and hold-switch outputs are paper counterfactuals only",
            "all rows remain no_trade and cannot authorize or place an order",
        ],
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    output["snapshot_id"] = "currency_state_after_cost_" + stable_hash(
        {
            "contract_id": output["counterfactual_contract_id"],
            "cohort_id": output["counterfactual_cohort_id"],
            "contract_sha256": output["counterfactual_contract_sha256"],
            "artifact_fingerprints": fingerprints,
            "decision_cutoff_utc": cutoff,
            "response_snapshot_id": output["response_snapshot_id"],
            "economics_input_envelope": economics_identity,
            "hold_state_envelope": hold_identity,
            "records": output_rows,
            "allocations": allocations,
            "hold_switch_counterfactuals": hold_switch,
            "input_rejections": output["input_rejections"],
        }
    )[:24]
    if len(output_rows) != len(contract["expected_arm_ids"]) * output["edge_horizon_count"]:
        raise AfterCostCounterfactualError("counterfactual output lost arm/pair/horizon rows")
    if any(
        row["execution_eligible"] is not False
        or row["can_place_orders"] is not False
        or row["paper_decision"] != "no_trade"
        for row in output_rows
    ):
        raise AfterCostCounterfactualError("counterfactual output crossed the execution boundary")
    return output


__all__ = [
    "AfterCostCounterfactualError",
    "build_after_cost_counterfactual",
    "validate_counterfactual_contract",
]
