"""Attach point-in-time official facts to a CurrencyState snapshot.

The adapter enriches raw component diagnostics only.  It never converts an
official fact, internal expectation, daily rate, or future clock into a
currency direction.  Pair forecasts and execution fields therefore remain
null/false.
"""

from __future__ import annotations

import copy
import math
from collections import Counter, defaultdict
from typing import Any, Mapping

from ..contracts.currency_state import stable_hash
from .currency_state_engine import parse_epoch, validate_snapshot


CONTEXT_CONTRACT_ID = "currency_state_official_context_v1_20260817"
FACT_BASIS_ELIGIBILITY_CONTRACT_ID = "official_fact_basis_eligibility_v1_20260817"


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _known_by(value: Any, cutoff_epoch: float) -> bool:
    epoch = parse_epoch(value)
    return epoch is not None and epoch <= cutoff_epoch


def _first_finite(*values: Any) -> float | None:
    for value in values:
        result = _finite(value)
        if result is not None:
            return result
    return None


def _fact_basis_metadata(raw: Mapping[str, Any], cutoff_epoch: float) -> dict[str, Any]:
    """Classify what a fact may support without assigning a direction."""

    fact_type = str(raw.get("fact_type") or "")
    evidence_class = str(raw.get("evidence_class") or "")
    degradation = sorted({str(value) for value in raw.get("degradation_reasons") or []})
    disqualifying = {
        "bootstrap_or_revision_context_only",
        "published_time_inferred",
        "prior_policy_context_only",
        "late_observation_context",
        "unverified_source_context",
        "source_listing_bootstrap",
    }
    effective_known = _known_by(raw.get("effective_from_utc"), cutoff_epoch)
    provenance_present = bool(str(raw.get("source_id") or "")) and bool(
        str(raw.get("raw_payload_sha256") or "")
    )
    common = effective_known and provenance_present and not disqualifying.intersection(
        degradation
    )

    scheduled = parse_epoch(raw.get("scheduled_utc"))
    actual_known = parse_epoch(raw.get("actual_known_utc"))
    surprise_known = parse_epoch(raw.get("standardized_surprise_known_utc"))
    scale_known = parse_epoch(raw.get("surprise_scale_known_utc"))
    numeric_surprise = bool(
        common
        and fact_type == "official_macro_actual"
        and evidence_class == "prospective_causal"
        and raw.get("consensus_causal") is True
        and _finite(raw.get("actual_value")) is not None
        and _finite(raw.get("consensus_value")) is not None
        and _finite(raw.get("standardized_surprise")) is not None
        and scheduled is not None
        and actual_known is not None
        and surprise_known is not None
        and scale_known is not None
        and scheduled <= actual_known <= surprise_known <= cutoff_epoch
        and scale_known < scheduled
    )

    delta_known = parse_epoch(
        raw.get("statement_delta_known_utc") or raw.get("delta_known_utc")
    )
    policy_delta = bool(
        common
        and fact_type
        in {"official_policy_statement_delta", "frozen_policy_statement_delta"}
        and evidence_class == "prospective_causal"
        and _first_finite(raw.get("statement_delta_score"), raw.get("delta_score"))
        is not None
        and delta_known is not None
        and delta_known <= cutoff_epoch
        and bool(str(raw.get("baseline_document_fact_id") or ""))
        and bool(str(raw.get("delta_contract_id") or ""))
        and bool(str(raw.get("delta_contract_sha256") or ""))
    )

    rate_known = parse_epoch(
        raw.get("rate_repricing_known_utc") or raw.get("repricing_known_utc")
    )
    rate_repricing = bool(
        common
        and fact_type == "intraday_rate_repricing"
        and evidence_class == "prospective_causal"
        and raw.get("connected") is True
        and _first_finite(raw.get("rates_repricing_bps"), raw.get("repricing_bps"))
        is not None
        and rate_known is not None
        and rate_known <= cutoff_epoch
        and bool(str(raw.get("source_contract_id") or ""))
    )

    semantic_source = bool(
        common
        and fact_type
        in {
            "official_macro_actual",
            "official_policy_document_context",
            "official_policy_statement_delta",
            "frozen_policy_statement_delta",
        }
        and evidence_class in {"prospective_causal", "policy_context_only"}
        and (
            raw.get("published_at_utc") is None
            or _known_by(raw.get("published_at_utc"), cutoff_epoch)
        )
        and (
            raw.get("first_seen_at_utc") is None
            or _known_by(raw.get("first_seen_at_utc"), cutoff_epoch)
        )
    )

    eligibility = {
        "causal_numeric_surprise": numeric_surprise,
        "frozen_policy_statement_delta": policy_delta,
        "causal_rate_repricing": rate_repricing,
        "versioned_semantic_thesis": semantic_source,
    }
    return {
        "fact_id": str(raw.get("fact_id") or ""),
        "currency": str(raw.get("currency") or ""),
        "fact_type": fact_type,
        "evidence_class": evidence_class,
        "effective_from_utc": raw.get("effective_from_utc"),
        "consensus_causal": raw.get("consensus_causal") is True,
        "source_id": str(raw.get("source_id") or ""),
        "source_contract_id": str(raw.get("source_contract_id") or ""),
        "raw_payload_sha256": str(raw.get("raw_payload_sha256") or ""),
        "degradation_reasons": degradation,
        "basis_eligibility": eligibility,
        "proof_eligible_for_any_direction_basis": any(eligibility.values()),
    }


def _component(row: Mapping[str, Any], component_id: str) -> dict[str, Any]:
    for component in row.get("components") or []:
        if component.get("component_id") == component_id:
            return component
    raise ValueError(f"currency row is missing component {component_id!r}")


def _same_cutoff(left: Any, right: Any) -> bool:
    left_epoch = parse_epoch(left)
    right_epoch = parse_epoch(right)
    return (
        left_epoch is not None
        and right_epoch is not None
        and math.isclose(left_epoch, right_epoch, abs_tol=1e-6)
    )


def attach_official_fact_context(
    currency_snapshot: Mapping[str, Any],
    official_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a new immutable research snapshot with unscored fact context."""

    validate_snapshot(currency_snapshot, contract=contract)
    if not _same_cutoff(
        currency_snapshot.get("decision_cutoff_utc"),
        official_snapshot.get("decision_cutoff_utc"),
    ):
        raise ValueError("currency and official-fact knowledge cutoffs must match")
    if official_snapshot.get("research_only") is not True:
        raise ValueError("official-fact snapshot must be research-only")
    if official_snapshot.get("execution_eligible") is not False:
        raise ValueError("official-fact snapshot cannot be execution eligible")
    if official_snapshot.get("supported_execution_decision") != "no_trade":
        raise ValueError("official-fact snapshot must remain no_trade")

    output = copy.deepcopy(dict(currency_snapshot))
    cutoff_epoch = parse_epoch(currency_snapshot.get("decision_cutoff_utc"))
    if cutoff_epoch is None:
        raise ValueError("currency snapshot cutoff must be a valid timestamp")
    facts_by_currency: dict[str, list[dict[str, Any]]] = defaultdict(list)
    fact_metadata_by_id: dict[str, dict[str, Any]] = {}
    for raw in official_snapshot.get("facts") or []:
        if isinstance(raw, Mapping) and raw.get("currency"):
            facts_by_currency[str(raw["currency"])].append(dict(raw))
            fact_id = str(raw.get("fact_id") or "")
            if not fact_id:
                raise ValueError("official fact is missing fact_id")
            if fact_id in fact_metadata_by_id:
                raise ValueError(f"duplicate official fact_id {fact_id!r}")
            fact_metadata_by_id[fact_id] = _fact_basis_metadata(raw, cutoff_epoch)
    events_by_currency: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in official_snapshot.get("upcoming_events") or []:
        if isinstance(raw, Mapping) and raw.get("currency"):
            events_by_currency[str(raw["currency"])].append(dict(raw))
    official_evidence = official_snapshot.get("currency_evidence") or {}

    component_summary: dict[str, Any] = {}
    for currency in contract["currencies"]:
        facts = sorted(
            facts_by_currency.get(currency, []),
            key=lambda row: (
                str(row.get("fact_type") or ""),
                str(row.get("effective_from_utc") or ""),
                str(row.get("fact_id") or ""),
            ),
        )
        events = sorted(
            events_by_currency.get(currency, []),
            key=lambda row: (
                str(row.get("scheduled_utc") or ""),
                str(row.get("event_id") or ""),
            ),
        )
        type_counts = Counter(str(row.get("fact_type") or "unknown") for row in facts)
        class_counts = Counter(str(row.get("evidence_class") or "unknown") for row in facts)
        macro_facts = [row for row in facts if row.get("fact_type") == "official_macro_actual"]
        policy_facts = [
            row
            for row in facts
            if row.get("fact_type") == "official_policy_document_context"
        ]
        causal_consensus = sum(bool(row.get("consensus_causal")) for row in facts)
        summary = {
            "currency": currency,
            "fact_count": len(facts),
            "fact_type_counts": dict(sorted(type_counts.items())),
            "evidence_class_counts": dict(sorted(class_counts.items())),
            "fact_ids": [str(row.get("fact_id") or "") for row in facts],
            "upcoming_event_count": len(events),
            "upcoming_event_ids": [str(row.get("event_id") or "") for row in events],
            "causal_consensus_count": causal_consensus,
            "source_health": dict(
                (official_evidence.get(currency) or {}).get("source_health") or {}
            ),
            "missing_or_degraded": list(
                (official_evidence.get(currency) or {}).get("missing_or_degraded") or []
            ),
            "direction_policy": "abstain",
            "forecast_mean_bps": None,
            "forecast_absolute_move_bps": None,
            "cost_clear_probability": None,
        }
        component_summary[currency] = summary

        for horizon_payload in output["horizons"].values():
            currency_row = horizon_payload["currencies"][currency]
            currency_row["official_fact_context"] = copy.deepcopy(summary)

            structured = _component(currency_row, "structured_official_fact")
            structured.update(
                {
                    "state": "available_unscored" if macro_facts else "unavailable",
                    "fact_count": len(macro_facts),
                    "fact_ids": [str(row.get("fact_id") or "") for row in macro_facts],
                    "causal_consensus_count": sum(
                        bool(row.get("consensus_causal")) for row in macro_facts
                    ),
                    "forecast_mean_bps": None,
                    "forecast_absolute_move_bps": None,
                    "cost_clear_probability": None,
                    "reason": (
                        "official_facts_available_but_direction_and_magnitude_unproved"
                        if macro_facts
                        else "no_official_numeric_fact_at_cutoff"
                    ),
                }
            )
            policy = _component(currency_row, "policy_statement_delta")
            policy.update(
                {
                    "state": "context_available_unscored" if policy_facts else "unavailable",
                    "fact_count": len(policy_facts),
                    "fact_ids": [str(row.get("fact_id") or "") for row in policy_facts],
                    "forecast_mean_bps": None,
                    "forecast_absolute_move_bps": None,
                    "cost_clear_probability": None,
                    "reason": (
                        "policy_document_context_available_but_no_frozen_delta_score"
                        if policy_facts
                        else "no_policy_document_context_at_cutoff"
                    ),
                }
            )
            intraday = _component(currency_row, "intraday_rate_repricing")
            intraday.update(
                {
                    "state": "unavailable",
                    "forecast_mean_bps": None,
                    "forecast_absolute_move_bps": None,
                    "cost_clear_probability": None,
                    "reason": "intraday_rate_repricing_not_connected",
                }
            )

    output["base_currency_state_snapshot_id"] = str(currency_snapshot["snapshot_id"])
    output["official_fact_snapshot_id"] = str(official_snapshot["snapshot_id"])
    output["official_fact_adapter_contract_id"] = str(
        official_snapshot.get("adapter_contract_id") or ""
    )
    output["component_context_contract_id"] = CONTEXT_CONTRACT_ID
    output["component_context"] = {
        "currency_summary": component_summary,
        "fact_count": int(official_snapshot.get("fact_count") or 0),
        "upcoming_event_count": int(official_snapshot.get("upcoming_event_count") or 0),
        "causal_consensus_count": int(official_snapshot.get("causal_consensus_count") or 0),
        "intraday_rates_connected": bool(
            (official_snapshot.get("intraday_rates") or {}).get("connected")
        ),
        "fact_basis_eligibility_contract_id": FACT_BASIS_ELIGIBILITY_CONTRACT_ID,
        "fact_metadata_by_id": dict(sorted(fact_metadata_by_id.items())),
        "basis_eligible_fact_counts": {
            basis: sum(
                bool((row.get("basis_eligibility") or {}).get(basis))
                for row in fact_metadata_by_id.values()
            )
            for basis in (
                "causal_numeric_surprise",
                "frozen_policy_statement_delta",
                "causal_rate_repricing",
                "versioned_semantic_thesis",
            )
        },
        "global_gaps": copy.deepcopy(list(official_snapshot.get("global_gaps") or [])),
        "direction_policy": "abstain",
    }
    output["snapshot_schema"] = "currency_state_official_context_snapshot_v1"
    output["snapshot_id"] = "currency_state_context_" + stable_hash(
        {
            "context_contract_id": CONTEXT_CONTRACT_ID,
            "base_snapshot_id": currency_snapshot["snapshot_id"],
            "official_fact_snapshot_id": official_snapshot["snapshot_id"],
            "component_context": output["component_context"],
        }
    )[:24]
    output["status"] = "context_attached_unscored"
    output["limitations"] = list(output.get("limitations") or []) + [
        "official facts are attached as unscored context and supply no calibrated direction",
        "causal consensus and intraday rate repricing remain unavailable",
    ]

    for horizon_payload in output["horizons"].values():
        for edge in horizon_payload["pair_edges"].values():
            if edge.get("forecast_mean_bps") is not None:
                raise ValueError("official context cannot create a pair forecast")
            if edge.get("expected_net_pips") is not None:
                raise ValueError("official context cannot create pair expected value")
            if edge.get("execution_eligible") is not False:
                raise ValueError("official context cannot create execution eligibility")
    validate_snapshot(output, contract=contract)
    return output


__all__ = [
    "CONTEXT_CONTRACT_ID",
    "FACT_BASIS_ELIGIBILITY_CONTRACT_ID",
    "attach_official_fact_context",
]
