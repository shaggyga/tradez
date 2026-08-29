"""Pure point-in-time response/timing arms for CurrencyState research.

This module has no filesystem, database, network, broker, lifecycle,
authorization, supervisor, or execution imports.  It produces parallel paper
research arms while keeping ``no_trade`` as the only supported execution
decision.  In particular, a technical record may confirm, veto, delay, or
identify conflict with an explicit frozen official thesis, but it cannot
create or reverse that thesis direction.
"""

from __future__ import annotations

import copy
import datetime as dt
import math
from collections import Counter
from typing import Any, Iterable, Mapping, Sequence

from ..contracts.currency_state import stable_hash, validate_contract as validate_state_contract
from ..contracts.signed_currency_exposure import canonical_factor_ids
from ..features.currency_state_engine import parse_epoch, validate_snapshot


UTC = dt.timezone.utc


class ResponseTimingArmError(ValueError):
    """Raised when an input violates the frozen point-in-time contract."""


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _utc_text(value: Any, *, field: str) -> str:
    epoch = parse_epoch(value)
    if epoch is None:
        raise ResponseTimingArmError(f"{field} must be a valid timestamp")
    return dt.datetime.fromtimestamp(epoch, tz=UTC).isoformat()


def _same_time(left: Any, right: Any) -> bool:
    left_epoch = parse_epoch(left)
    right_epoch = parse_epoch(right)
    return (
        left_epoch is not None
        and right_epoch is not None
        and math.isclose(left_epoch, right_epoch, abs_tol=1e-6)
    )


def validate_arm_contract(contract: Mapping[str, Any]) -> None:
    required = {
        "schema_version",
        "contract_id",
        "snapshot_schema",
        "required_input_contract_id",
        "required_input_context_contract_id",
        "required_fact_basis_eligibility_contract_id",
        "expected_instrument_count",
        "expected_horizons_sec",
        "allowed_directions",
        "allowed_technical_directions",
        "allowed_timing_states",
        "allowed_official_direction_bases",
        "official_thesis_grounding",
        "arms",
    }
    missing = sorted(required - set(contract))
    if missing:
        raise ResponseTimingArmError(f"arm contract missing keys: {missing}")
    if contract.get("research_only") is not True:
        raise ResponseTimingArmError("arm contract must be research-only")
    if contract.get("execution_eligible") is not False:
        raise ResponseTimingArmError("arm contract cannot be execution eligible")
    if contract.get("can_place_orders") is not False:
        raise ResponseTimingArmError("arm contract cannot place orders")
    if contract.get("supported_execution_decision") != "no_trade":
        raise ResponseTimingArmError("arm contract must support only no_trade")
    if list(contract.get("allowed_directions") or []) != ["buy", "sell"]:
        raise ResponseTimingArmError("official direction contract must be exactly buy/sell")
    allowed_bases = list(contract.get("allowed_official_direction_bases") or [])
    if not allowed_bases or any(not str(value).strip() for value in allowed_bases):
        raise ResponseTimingArmError("official direction bases must be explicit and nonempty")
    if len(set(str(value) for value in allowed_bases)) != len(allowed_bases):
        raise ResponseTimingArmError("official direction bases must be unique")
    grounding = contract.get("official_thesis_grounding")
    if not isinstance(grounding, Mapping):
        raise ResponseTimingArmError("official thesis grounding contract is required")
    required_grounding = (
        "require_source_ids",
        "require_source_fact_ids_present_in_context_snapshot",
        "require_source_fact_currencies_match_pair_legs",
        "require_basis_specific_fact_eligibility",
        "require_pair_consistent_signed_currency_factor",
        "require_versioned_semantic_lineage",
    )
    if any(grounding.get(field) is not True for field in required_grounding):
        raise ResponseTimingArmError("official thesis grounding requirements cannot be disabled")
    if grounding.get("factor_id_contract") != "currency:AAA:long|short":
        raise ResponseTimingArmError("official thesis factor-id contract is not canonical")
    arm_ids = [str(row.get("id") or "") for row in contract["arms"]]
    expected = [
        "official_context_only",
        "observed_price_only",
        "technical_timing_verification",
        "aligned",
        "direction_conflicted_aggressive_paper",
        "delayed_reconfirmed",
        "magnitude_only",
        "no_trade",
    ]
    if arm_ids != expected:
        raise ResponseTimingArmError("arm contract must retain the canonical arm order")
    if len(set(arm_ids)) != len(arm_ids):
        raise ResponseTimingArmError("arm ids must be unique")


def _validate_source_envelope(
    payload: Mapping[str, Any] | None,
    *,
    name: str,
    cutoff_utc: str,
) -> tuple[dict[str, Any] | None, list[Mapping[str, Any]]]:
    if payload is None:
        return None, []
    if not isinstance(payload, Mapping):
        raise ResponseTimingArmError(f"{name} must be a mapping")
    required = ("contract_id", "cohort_id", "frozen_at_utc", "decision_cutoff_utc")
    for field in required:
        if not str(payload.get(field) or ""):
            raise ResponseTimingArmError(f"{name}.{field} is required")
    if not _same_time(payload["decision_cutoff_utc"], cutoff_utc):
        raise ResponseTimingArmError(f"{name} cutoff must equal CurrencyState cutoff")
    frozen_epoch = parse_epoch(payload["frozen_at_utc"])
    if frozen_epoch is None:
        raise ResponseTimingArmError(f"{name}.frozen_at_utc must be a valid timestamp")
    if frozen_epoch > parse_epoch(cutoff_utc):
        raise ResponseTimingArmError(f"{name} was frozen after the decision cutoff")
    if payload.get("research_only") is not True:
        raise ResponseTimingArmError(f"{name} must be research-only")
    if payload.get("execution_eligible") is not False:
        raise ResponseTimingArmError(f"{name} cannot be execution eligible")
    if payload.get("can_place_orders", False) is not False:
        raise ResponseTimingArmError(f"{name} cannot place orders")
    if payload.get("supported_execution_decision") != "no_trade":
        raise ResponseTimingArmError(f"{name} must remain no_trade")
    records = payload.get("records") or []
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise ResponseTimingArmError(f"{name}.records must be a sequence")
    if any(not isinstance(row, Mapping) for row in records):
        raise ResponseTimingArmError(f"{name}.records must contain mappings")
    identity = {
        "contract_id": str(payload["contract_id"]),
        "cohort_id": str(payload["cohort_id"]),
        "frozen_at_utc": _utc_text(payload["frozen_at_utc"], field=f"{name}.frozen_at_utc"),
        "payload_id": str(payload.get("payload_id") or ""),
    }
    return identity, list(records)


def _normalize_source_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ResponseTimingArmError("source_ids must be a sequence")
    return sorted({str(item) for item in value if str(item)})


def _index_records(
    records: Iterable[Mapping[str, Any]],
    *,
    kind: str,
    cutoff_utc: str,
    grid: set[tuple[str, int]],
    contract: Mapping[str, Any],
    envelope_frozen_utc: str | None,
) -> tuple[dict[tuple[str, int], dict[str, Any]], list[dict[str, Any]]]:
    index: dict[tuple[str, int], dict[str, Any]] = {}
    rejections: list[dict[str, Any]] = []
    id_field = {
        "official_thesis": "thesis_id",
        "technical_state": "technical_state_id",
        "magnitude_estimate": "magnitude_estimate_id",
    }[kind]
    cutoff_epoch = parse_epoch(cutoff_utc)
    envelope_frozen_epoch = parse_epoch(envelope_frozen_utc)
    for raw in records:
        record_id = str(raw.get(id_field) or "")
        instrument = str(raw.get("instrument") or "")
        try:
            horizon = int(raw.get("horizon_sec"))
        except (TypeError, ValueError):
            horizon = 0
        key = (instrument, horizon)
        if not record_id:
            rejections.append({"kind": kind, "record_id": "", "reason": f"missing_{id_field}"})
            continue
        if key not in grid:
            rejections.append({"kind": kind, "record_id": record_id, "reason": "outside_snapshot_grid"})
            continue
        known_epoch = parse_epoch(raw.get("known_at_utc"))
        if known_epoch is None:
            rejections.append({"kind": kind, "record_id": record_id, "reason": "missing_known_at_utc"})
            continue
        if known_epoch > cutoff_epoch:
            rejections.append({"kind": kind, "record_id": record_id, "reason": "known_after_decision_cutoff"})
            continue
        if envelope_frozen_epoch is None or known_epoch > envelope_frozen_epoch:
            rejections.append(
                {
                    "kind": kind,
                    "record_id": record_id,
                    "reason": "record_known_after_envelope_freeze",
                }
            )
            continue
        if key in index:
            raise ResponseTimingArmError(f"duplicate {kind} record for {instrument}/{horizon}")
        normalized = copy.deepcopy(dict(raw))
        normalized["known_at_utc"] = _utc_text(raw["known_at_utc"], field=f"{kind}.known_at_utc")
        normalized["source_ids"] = _normalize_source_ids(raw.get("source_ids"))
        if kind == "official_thesis":
            direction = str(raw.get("direction") or "").lower()
            if direction not in contract["allowed_directions"]:
                rejections.append({"kind": kind, "record_id": record_id, "reason": "invalid_direction"})
                continue
            normalized["direction"] = direction
            direction_basis = str(raw.get("direction_basis") or "").strip()
            if direction_basis not in contract["allowed_official_direction_bases"]:
                rejections.append(
                    {
                        "kind": kind,
                        "record_id": record_id,
                        "reason": "invalid_or_missing_direction_basis",
                    }
                )
                continue
            source_ids = normalized["source_ids"]
            source_fact_ids = _normalize_source_ids(raw.get("source_fact_ids"))
            factor_ids = _normalize_source_ids(raw.get("independent_factor_ids"))
            if not source_ids:
                rejections.append(
                    {"kind": kind, "record_id": record_id, "reason": "missing_source_ids"}
                )
                continue
            if not source_fact_ids:
                rejections.append(
                    {
                        "kind": kind,
                        "record_id": record_id,
                        "reason": "missing_source_fact_ids",
                    }
                )
                continue
            allowed_factor_ids = set(canonical_factor_ids(instrument, direction))
            if not factor_ids or not set(factor_ids).issubset(allowed_factor_ids):
                rejections.append(
                    {
                        "kind": kind,
                        "record_id": record_id,
                        "reason": "missing_or_pair_inconsistent_signed_currency_factor",
                    }
                )
                continue
            normalized["direction_basis"] = direction_basis
            normalized["source_fact_ids"] = source_fact_ids
            normalized["independent_factor_ids"] = factor_ids
            if direction_basis == "versioned_semantic_thesis":
                semantic_fields = (
                    "semantic_contract_id",
                    "semantic_contract_sha256",
                    "semantic_model_version",
                    "semantic_prompt_sha256",
                    "semantic_known_at_utc",
                )
                if any(not str(raw.get(field) or "").strip() for field in semantic_fields):
                    rejections.append(
                        {
                            "kind": kind,
                            "record_id": record_id,
                            "reason": "missing_versioned_semantic_lineage",
                        }
                    )
                    continue
                semantic_known_epoch = parse_epoch(raw.get("semantic_known_at_utc"))
                if (
                    semantic_known_epoch is None
                    or semantic_known_epoch > known_epoch
                    or semantic_known_epoch > envelope_frozen_epoch
                ):
                    rejections.append(
                        {
                            "kind": kind,
                            "record_id": record_id,
                            "reason": "semantic_lineage_not_known_by_record_freeze",
                        }
                    )
                    continue
                for field in semantic_fields[:-1]:
                    normalized[field] = str(raw[field])
                normalized["semantic_known_at_utc"] = _utc_text(
                    raw["semantic_known_at_utc"],
                    field="official_thesis.semantic_known_at_utc",
                )
        elif kind == "technical_state":
            direction = str(raw.get("technical_direction") or "neutral").lower()
            timing = str(raw.get("timing_state") or "unavailable").lower()
            if direction not in contract["allowed_technical_directions"]:
                rejections.append({"kind": kind, "record_id": record_id, "reason": "invalid_technical_direction"})
                continue
            if timing not in contract["allowed_timing_states"]:
                rejections.append({"kind": kind, "record_id": record_id, "reason": "invalid_timing_state"})
                continue
            normalized["technical_direction"] = direction
            normalized["timing_state"] = timing
            if raw.get("reconfirmed_at_utc"):
                reconfirmed_epoch = parse_epoch(raw["reconfirmed_at_utc"])
                if reconfirmed_epoch is None or reconfirmed_epoch > cutoff_epoch:
                    rejections.append({"kind": kind, "record_id": record_id, "reason": "reconfirmation_not_known_at_cutoff"})
                    continue
                if reconfirmed_epoch > known_epoch:
                    rejections.append({"kind": kind, "record_id": record_id, "reason": "reconfirmation_after_record_knowledge_time"})
                    continue
                normalized["reconfirmed_at_utc"] = _utc_text(
                    raw["reconfirmed_at_utc"], field="technical_state.reconfirmed_at_utc"
                )
        else:
            magnitude = _finite(raw.get("expected_absolute_move_bps"))
            if magnitude is None or magnitude < 0:
                rejections.append({"kind": kind, "record_id": record_id, "reason": "invalid_expected_absolute_move_bps"})
                continue
            normalized["expected_absolute_move_bps"] = magnitude
        index[key] = normalized
    rejections.sort(key=lambda row: (row["kind"], row["record_id"], row["reason"]))
    return index, rejections


def _fact_ids(currency_row: Mapping[str, Any]) -> list[str]:
    context = currency_row.get("official_fact_context") or {}
    return sorted({str(value) for value in context.get("fact_ids") or [] if str(value)})


def _direction_from_observed(
    edge: Mapping[str, Any], contract: Mapping[str, Any]
) -> tuple[str | None, str]:
    if edge.get("research_observable") is not True:
        return None, "source_edge_not_research_observable"
    move = _finite(edge.get("observed_pair_return_bps"))
    if move is None:
        return None, "observed_pair_return_unavailable"
    if contract["observed_price_baseline"].get("requires_exact_horizon_observation") and not bool(
        edge.get("exact_horizon_observation")
    ):
        return None, "inexact_horizon_observation"
    threshold = float(contract["observed_price_baseline"]["minimum_absolute_move_bps"])
    if abs(move) <= threshold:
        return None, "observed_move_below_frozen_threshold"
    return ("buy" if move > 0 else "sell"), "frozen_observed_price_continuation_rule"


def _technical_relation(
    official_direction: str | None,
    technical: Mapping[str, Any] | None,
) -> str:
    if technical is None or technical.get("timing_state") == "unavailable":
        return "unavailable"
    if technical.get("timing_state") == "veto":
        return "veto"
    if technical.get("timing_state") == "wait":
        return "wait"
    if official_direction is None:
        return "no_official_direction"
    technical_direction = technical.get("technical_direction")
    if technical_direction == "neutral":
        return "neutral"
    if technical_direction == official_direction:
        return "aligned"
    return "conflicted"


def _lineage(
    snapshot: Mapping[str, Any],
    base_row: Mapping[str, Any],
    quote_row: Mapping[str, Any],
    *,
    official_identity: Mapping[str, Any] | None,
    technical_identity: Mapping[str, Any] | None,
    magnitude_identity: Mapping[str, Any] | None,
    official: Mapping[str, Any] | None,
    technical: Mapping[str, Any] | None,
    magnitude: Mapping[str, Any] | None,
) -> dict[str, Any]:
    def record_ref(record: Mapping[str, Any] | None, id_field: str) -> dict[str, Any]:
        if record is None:
            return {"record_id": None, "known_at_utc": None, "source_ids": [], "record_hash": None}
        return {
            "record_id": str(record.get(id_field) or ""),
            "known_at_utc": str(record.get("known_at_utc") or ""),
            "source_ids": list(record.get("source_ids") or []),
            "source_fact_ids": list(record.get("source_fact_ids") or []),
            "independent_factor_ids": list(record.get("independent_factor_ids") or []),
            "direction_basis": str(record.get("direction_basis") or "") or None,
            "basis_eligibility_contract_id": record.get(
                "basis_eligibility_contract_id"
            ),
            "basis_eligible_for_after_cost": record.get(
                "basis_eligible_for_after_cost"
            ),
            "proof_eligible_lineage": record.get("proof_eligible_lineage"),
            "semantic_contract_id": record.get("semantic_contract_id"),
            "semantic_contract_sha256": record.get("semantic_contract_sha256"),
            "semantic_model_version": record.get("semantic_model_version"),
            "semantic_prompt_sha256": record.get("semantic_prompt_sha256"),
            "semantic_known_at_utc": record.get("semantic_known_at_utc"),
            "record_hash": stable_hash(record),
        }

    official_ref = record_ref(official, "thesis_id")
    technical_ref = record_ref(technical, "technical_state_id")
    magnitude_ref = record_ref(magnitude, "magnitude_estimate_id")
    source_contract_ids = sorted(
        {
            str(identity.get("contract_id") or "")
            for identity in (official_identity, technical_identity, magnitude_identity)
            if identity and str(identity.get("contract_id") or "")
        }
    )
    source_cohort_ids = sorted(
        {
            str(identity.get("cohort_id") or "")
            for identity in (official_identity, technical_identity, magnitude_identity)
            if identity and str(identity.get("cohort_id") or "")
        }
    )
    return {
        "input_snapshot_id": str(snapshot["snapshot_id"]),
        "base_currency_state_snapshot_id": str(snapshot.get("base_currency_state_snapshot_id") or ""),
        "official_fact_snapshot_id": str(snapshot.get("official_fact_snapshot_id") or ""),
        "official_fact_adapter_contract_id": str(snapshot.get("official_fact_adapter_contract_id") or ""),
        "component_context_contract_id": str(snapshot.get("component_context_contract_id") or ""),
        "base_official_fact_ids": _fact_ids(base_row),
        "quote_official_fact_ids": _fact_ids(quote_row),
        "source_contract_ids": source_contract_ids,
        "source_cohort_ids": source_cohort_ids,
        "official_thesis": None if official_identity is None else {
            **copy.deepcopy(dict(official_identity)),
            **official_ref,
        },
        "technical_state": None if technical_identity is None else {
            **copy.deepcopy(dict(technical_identity)),
            **technical_ref,
        },
        "magnitude_estimate": None if magnitude_identity is None else {
            **copy.deepcopy(dict(magnitude_identity)),
            **magnitude_ref,
        },
    }


def _common_record(
    arm_id: str,
    edge: Mapping[str, Any],
    *,
    lineage: Mapping[str, Any],
    official: Mapping[str, Any] | None,
    technical: Mapping[str, Any] | None,
    magnitude: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "arm_id": arm_id,
        "instrument": str(edge["instrument"]),
        "base_currency": str(edge["base_currency"]),
        "quote_currency": str(edge["quote_currency"]),
        "horizon_sec": int(edge["horizon_sec"]),
        "research_direction": None,
        "direction_origin": None,
        "paper_action": "abstain",
        "paper_candidate": False,
        "forward_hypothesis": False,
        "state": "abstain",
        "reasons": [],
        "technical_relation": _technical_relation(
            None if official is None else str(official.get("direction") or ""), technical
        ),
        "official_direction": None if official is None else official.get("direction"),
        "technical_direction": None if technical is None else technical.get("technical_direction"),
        "timing_state": None if technical is None else technical.get("timing_state"),
        "expected_signed_move_bps": None,
        "expected_absolute_move_bps": None,
        "prediction_uncertainty_bps": None,
        "cost_clear_probability": None,
        "magnitude_vs_observed_spread": "unavailable",
        "cost_context": {
            "bid": edge.get("bid"),
            "ask": edge.get("ask"),
            "pip": edge.get("pip"),
            "observed_entry_spread_bps": edge.get("spread_bps"),
            "upstream_cost_clear_probability": edge.get("cost_clear_probability"),
            "upstream_expected_net_pips": edge.get("expected_net_pips"),
            "slippage_bps": None,
            "latency_cost_bps": None,
        },
        "uncertainty_context": {
            "observed_edge_uncertainty_bps": edge.get("observed_edge_uncertainty_bps"),
            "official_thesis_uncertainty_bps": None if official is None else official.get("uncertainty_bps"),
            "technical_uncertainty_bps": None if technical is None else technical.get("uncertainty_bps"),
            "magnitude_uncertainty_bps": None if magnitude is None else magnitude.get("uncertainty_bps"),
        },
        "lineage": copy.deepcopy(dict(lineage)),
        "source_edge": copy.deepcopy(dict(edge)),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }


def _apply_direction(
    row: dict[str, Any],
    direction: str,
    *,
    origin: str,
    reason: str,
    official: Mapping[str, Any] | None,
) -> None:
    row.update(
        {
            "research_direction": direction,
            "direction_origin": origin,
            "paper_action": direction,
            "paper_candidate": True,
            "forward_hypothesis": True,
            "state": "paper_candidate",
            "reasons": [reason],
        }
    )
    if official is not None:
        magnitude = _finite(official.get("expected_absolute_move_bps"))
        if magnitude is not None and magnitude >= 0:
            row["expected_absolute_move_bps"] = magnitude
            row["expected_signed_move_bps"] = magnitude if direction == "buy" else -magnitude
        row["prediction_uncertainty_bps"] = _finite(official.get("uncertainty_bps"))


def _set_magnitude(
    row: dict[str, Any], magnitude: Mapping[str, Any] | None, contract: Mapping[str, Any]
) -> None:
    if magnitude is None:
        row["state"] = "unavailable"
        row["reasons"] = ["no_point_in_time_magnitude_estimate"]
        return
    value = float(magnitude["expected_absolute_move_bps"])
    spread = _finite(row["cost_context"].get("observed_entry_spread_bps"))
    row.update(
        {
            "state": "magnitude_available",
            "reasons": ["point_in_time_directionless_magnitude_estimate_present"],
            "forward_hypothesis": True,
            "expected_absolute_move_bps": value,
            "prediction_uncertainty_bps": _finite(magnitude.get("uncertainty_bps")),
        }
    )
    if spread is not None:
        required = spread * float(contract["magnitude_cost_diagnostic"]["clearance_multiple"])
        row["magnitude_vs_observed_spread"] = (
            "clears_observed_spread" if value > required else "does_not_clear_observed_spread"
        )


def build_response_timing_arms(
    official_context_snapshot: Mapping[str, Any],
    *,
    state_contract: Mapping[str, Any],
    arm_contract: Mapping[str, Any],
    official_theses: Mapping[str, Any] | None = None,
    technical_states: Mapping[str, Any] | None = None,
    magnitude_estimates: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build all eight research arms over the exact CurrencyState edge grid."""

    validate_state_contract(state_contract)
    validate_arm_contract(arm_contract)
    validate_snapshot(official_context_snapshot, contract=state_contract)
    if official_context_snapshot.get("snapshot_schema") != "currency_state_official_context_snapshot_v1":
        raise ResponseTimingArmError("input must be a CurrencyState official-context snapshot")
    if official_context_snapshot.get("contract_id") != arm_contract["required_input_contract_id"]:
        raise ResponseTimingArmError("input CurrencyState contract is not the required version")
    if official_context_snapshot.get("component_context_contract_id") != arm_contract[
        "required_input_context_contract_id"
    ]:
        raise ResponseTimingArmError("input official-context contract is not the required version")
    if official_context_snapshot.get("research_only") is not True:
        raise ResponseTimingArmError("input snapshot must be research-only")
    if official_context_snapshot.get("execution_eligible") is not False:
        raise ResponseTimingArmError("input snapshot cannot be execution eligible")
    if official_context_snapshot.get("supported_execution_decision") != "no_trade":
        raise ResponseTimingArmError("input snapshot must remain no_trade")

    cutoff = _utc_text(
        official_context_snapshot.get("decision_cutoff_utc"), field="decision_cutoff_utc"
    )
    horizon_values = sorted(int(value) for value in official_context_snapshot["horizons"])
    if horizon_values != sorted(int(value) for value in arm_contract["expected_horizons_sec"]):
        raise ResponseTimingArmError("input horizon grid differs from arm contract")
    grid: set[tuple[str, int]] = set()
    edges_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
    currencies_by_horizon: dict[int, Mapping[str, Any]] = {}
    for horizon_text, payload in official_context_snapshot["horizons"].items():
        horizon = int(horizon_text)
        edges = payload.get("pair_edges") or {}
        if len(edges) != int(arm_contract["expected_instrument_count"]):
            raise ResponseTimingArmError(f"horizon {horizon} does not contain all 68 edges")
        currencies_by_horizon[horizon] = payload.get("currencies") or {}
        for instrument, edge in edges.items():
            if str(edge.get("instrument")) != str(instrument) or int(edge.get("horizon_sec")) != horizon:
                raise ResponseTimingArmError("pair-edge identity does not match its grid key")
            key = (str(instrument), horizon)
            grid.add(key)
            edges_by_key[key] = edge

    official_identity, official_records = _validate_source_envelope(
        official_theses, name="official_theses", cutoff_utc=cutoff
    )
    technical_identity, technical_records = _validate_source_envelope(
        technical_states, name="technical_states", cutoff_utc=cutoff
    )
    magnitude_identity, magnitude_records = _validate_source_envelope(
        magnitude_estimates, name="magnitude_estimates", cutoff_utc=cutoff
    )
    official_index, rejections_a = _index_records(
        official_records, kind="official_thesis", cutoff_utc=cutoff, grid=grid,
        contract=arm_contract,
        envelope_frozen_utc=None if official_identity is None else official_identity["frozen_at_utc"],
    )
    technical_index, rejections_b = _index_records(
        technical_records, kind="technical_state", cutoff_utc=cutoff, grid=grid,
        contract=arm_contract,
        envelope_frozen_utc=None if technical_identity is None else technical_identity["frozen_at_utc"],
    )
    magnitude_index, rejections_c = _index_records(
        magnitude_records, kind="magnitude_estimate", cutoff_utc=cutoff, grid=grid,
        contract=arm_contract,
        envelope_frozen_utc=None if magnitude_identity is None else magnitude_identity["frozen_at_utc"],
    )
    component_context = official_context_snapshot.get("component_context") or {}
    eligibility_contract_id = str(
        component_context.get("fact_basis_eligibility_contract_id") or ""
    )
    if eligibility_contract_id != str(
        arm_contract["required_fact_basis_eligibility_contract_id"]
    ):
        raise ResponseTimingArmError(
            "fact basis-eligibility contract is missing or incompatible"
        )
    raw_fact_metadata = component_context.get("fact_metadata_by_id") or {}
    if not isinstance(raw_fact_metadata, Mapping):
        raise ResponseTimingArmError("fact metadata index must be a mapping")
    fact_metadata = {
        str(fact_id): dict(metadata)
        for fact_id, metadata in raw_fact_metadata.items()
        if isinstance(metadata, Mapping)
    }
    available_fact_ids = set(fact_metadata)
    for key, record in list(official_index.items()):
        source_fact_ids = set(record["source_fact_ids"])
        if not source_fact_ids.issubset(available_fact_ids):
            rejections_a.append(
                {
                    "kind": "official_thesis",
                    "record_id": str(record["thesis_id"]),
                    "reason": "source_fact_not_present_in_context_snapshot",
                }
            )
            del official_index[key]
            continue
        instrument, _ = key
        pair_currencies = set(instrument.split("_"))
        cited_metadata = [fact_metadata[fact_id] for fact_id in sorted(source_fact_ids)]
        if any(
            str(metadata.get("currency") or "") not in pair_currencies
            for metadata in cited_metadata
        ):
            rejections_a.append(
                {
                    "kind": "official_thesis",
                    "record_id": str(record["thesis_id"]),
                    "reason": "source_fact_currency_not_a_pair_leg",
                }
            )
            del official_index[key]
            continue
        basis = str(record.get("direction_basis") or "")
        if any(
            (metadata.get("basis_eligibility") or {}).get(basis) is not True
            for metadata in cited_metadata
        ):
            rejections_a.append(
                {
                    "kind": "official_thesis",
                    "record_id": str(record["thesis_id"]),
                    "reason": "source_fact_not_eligible_for_direction_basis",
                }
            )
            del official_index[key]
            continue
        record["basis_eligibility_contract_id"] = eligibility_contract_id
        record["basis_eligible_for_after_cost"] = True
        record["proof_eligible_lineage"] = True
    rejections = sorted(
        rejections_a + rejections_b + rejections_c,
        key=lambda row: (row["kind"], row["record_id"], row["reason"]),
    )

    arm_definitions = {str(row["id"]): dict(row) for row in arm_contract["arms"]}
    arm_records: dict[str, list[dict[str, Any]]] = {arm_id: [] for arm_id in arm_definitions}
    for key in sorted(grid, key=lambda value: (value[1], value[0])):
        instrument, horizon = key
        edge = edges_by_key[key]
        official = official_index.get(key)
        technical = technical_index.get(key)
        magnitude = magnitude_index.get(key)
        base_row = currencies_by_horizon[horizon][str(edge["base_currency"])]
        quote_row = currencies_by_horizon[horizon][str(edge["quote_currency"])]
        lineage = _lineage(
            official_context_snapshot,
            base_row,
            quote_row,
            official_identity=official_identity,
            technical_identity=technical_identity,
            magnitude_identity=magnitude_identity,
            official=official,
            technical=technical,
            magnitude=magnitude,
        )
        relation = _technical_relation(
            None if official is None else str(official["direction"]), technical
        )

        row = _common_record(
            "official_context_only", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        if official is None:
            row["reasons"] = ["no_explicit_frozen_official_thesis"]
        else:
            _apply_direction(
                row, str(official["direction"]), origin="frozen_official_thesis",
                reason="explicit_frozen_official_thesis_present", official=official,
            )
        arm_records[row["arm_id"]].append(row)

        row = _common_record(
            "observed_price_only", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        observed_direction, observed_reason = _direction_from_observed(edge, arm_contract)
        if observed_direction is None:
            row["reasons"] = [observed_reason]
        else:
            _apply_direction(
                row, observed_direction, origin="observed_price_signed_continuation_control",
                reason=observed_reason, official=None,
            )
            row["observed_response_only_warning"] = True
        arm_records[row["arm_id"]].append(row)

        row = _common_record(
            "technical_timing_verification", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        row["state"] = "diagnostic_only"
        row["reasons"] = [f"technical_relation:{relation}"]
        arm_records[row["arm_id"]].append(row)

        row = _common_record(
            "aligned", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        if official is not None and relation == "aligned" and technical.get("timing_state") in {"ready", "reconfirmed"}:
            _apply_direction(
                row, str(official["direction"]), origin="frozen_official_thesis",
                reason="technical_state_aligned_without_changing_macro_direction", official=official,
            )
        else:
            row["reasons"] = [f"aligned_arm_abstains:{relation}"]
        arm_records[row["arm_id"]].append(row)

        row = _common_record(
            "direction_conflicted_aggressive_paper", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        if official is not None and relation == "conflicted":
            _apply_direction(
                row, str(official["direction"]), origin="frozen_official_thesis",
                reason="technical_conflict_scored_separately_following_macro_only", official=official,
            )
            row["aggressive_paper_only"] = True
        else:
            row["reasons"] = [f"conflict_arm_abstains:{relation}"]
        arm_records[row["arm_id"]].append(row)

        row = _common_record(
            "delayed_reconfirmed", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        delay = None
        if official is not None and technical is not None and technical.get("reconfirmed_at_utc"):
            delay = parse_epoch(technical["reconfirmed_at_utc"]) - parse_epoch(official["known_at_utc"])
        row["reconfirmation_delay_sec"] = delay
        minimum_delay = float(arm_contract["delayed_reconfirmation"]["minimum_delay_sec"])
        if (
            official is not None
            and relation == "aligned"
            and technical is not None
            and technical.get("timing_state") == "reconfirmed"
            and delay is not None
            and delay >= minimum_delay
        ):
            _apply_direction(
                row, str(official["direction"]), origin="frozen_official_thesis",
                reason="delayed_same_direction_reconfirmation_known_at_cutoff", official=official,
            )
        else:
            row["reasons"] = ["delayed_reconfirmation_requirements_not_met"]
        arm_records[row["arm_id"]].append(row)

        row = _common_record(
            "magnitude_only", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        _set_magnitude(row, magnitude, arm_contract)
        arm_records[row["arm_id"]].append(row)

        row = _common_record(
            "no_trade", edge, lineage=lineage, official=official,
            technical=technical, magnitude=magnitude,
        )
        row["state"] = "no_trade_control"
        row["reasons"] = ["explicit_no_trade_control"]
        arm_records[row["arm_id"]].append(row)

    arms: dict[str, Any] = {}
    for definition in arm_contract["arms"]:
        arm_id = str(definition["id"])
        records = arm_records[arm_id]
        action_counts = Counter(str(row["paper_action"]) for row in records)
        arms[arm_id] = {
            **copy.deepcopy(dict(definition)),
            "record_count": len(records),
            "paper_action_counts": dict(sorted(action_counts.items())),
            "records": records,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "supported_execution_decision": "no_trade",
        }

    stable_lineage = {
        "input_snapshot_id": str(official_context_snapshot["snapshot_id"]),
        "base_currency_state_snapshot_id": str(official_context_snapshot.get("base_currency_state_snapshot_id") or ""),
        "official_fact_snapshot_id": str(official_context_snapshot.get("official_fact_snapshot_id") or ""),
        "official_thesis_envelope": official_identity,
        "technical_state_envelope": technical_identity,
        "magnitude_estimate_envelope": magnitude_identity,
    }
    output = {
        "schema_version": int(arm_contract["schema_version"]),
        "snapshot_schema": str(arm_contract["snapshot_schema"]),
        "arm_contract_id": str(arm_contract["contract_id"]),
        "arm_contract_sha256": stable_hash(arm_contract),
        "decision_cutoff_utc": cutoff,
        "input_snapshot_id": str(official_context_snapshot["snapshot_id"]),
        "input_snapshot_schema": str(official_context_snapshot["snapshot_schema"]),
        "input_contract_id": str(official_context_snapshot["contract_id"]),
        "fact_basis_eligibility_contract_id": str(
            component_context.get("fact_basis_eligibility_contract_id") or ""
        ),
        "basis_eligible_fact_counts": copy.deepcopy(
            dict(component_context.get("basis_eligible_fact_counts") or {})
        ),
        "lineage": stable_lineage,
        "horizons_sec": horizon_values,
        "instrument_count": int(arm_contract["expected_instrument_count"]),
        "edge_horizon_count": len(grid),
        "arm_count": len(arms),
        "arms": arms,
        "input_rejections": rejections,
        "limitations": [
            "technical state can only confirm, veto, time, or identify conflict with a frozen official direction",
            "the observed-price arm is a frozen price-only paper control and does not establish causal direction",
            "magnitude cost clearance is diagnostic and has no calibrated probability",
            "all arms are shadow research and cannot authorize or place an order",
        ],
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    output["snapshot_id"] = "currency_state_response_timing_" + stable_hash(
        {
            "contract_id": output["arm_contract_id"],
            "contract_sha256": output["arm_contract_sha256"],
            "decision_cutoff_utc": cutoff,
            "lineage": stable_lineage,
            "rejections": rejections,
            "arms": arms,
        }
    )[:24]

    expected_records = int(arm_contract["expected_instrument_count"]) * len(horizon_values)
    for arm_id, arm in arms.items():
        if arm["record_count"] != expected_records:
            raise ResponseTimingArmError(f"arm {arm_id} lost pair/horizon cells")
        for row in arm["records"]:
            if row["execution_eligible"] is not False or row["can_place_orders"] is not False:
                raise ResponseTimingArmError("an arm record became execution eligible")
            if row["supported_execution_decision"] != "no_trade":
                raise ResponseTimingArmError("an arm record supports a non-no_trade decision")
            if arm_id in {
                "official_context_only",
                "aligned",
                "direction_conflicted_aggressive_paper",
                "delayed_reconfirmed",
            } and row["paper_candidate"] and row["research_direction"] != row["official_direction"]:
                raise ResponseTimingArmError("technical state changed a frozen official direction")
    return output


__all__ = [
    "ResponseTimingArmError",
    "build_response_timing_arms",
    "validate_arm_contract",
]
