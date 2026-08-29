"""Deterministic, point-in-time source-response analog selection.

This module is deliberately pure and research-only.  It performs no I/O and has
no knowledge of databases, brokers, supervisors, GPT, or execution.  Selection
is completed and cryptographically frozen from an allowlisted feature view
before any matured outcomes may be attached.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence


UTC = timezone.utc


class AnalogSelectorError(ValueError):
    """Raised when an input would violate the selector contract."""


class IneligibleEvent(AnalogSelectorError):
    """Raised when an event was not knowable at the requested cutoff."""


def canonical_json(value: Any) -> str:
    """Return the stable JSON representation used by every selector hash."""

    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise AnalogSelectorError(f"value is not canonical-JSON safe: {exc}") from exc


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parse_utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise AnalogSelectorError(f"{field} is not a valid ISO-8601 timestamp") from exc
    else:
        raise AnalogSelectorError(f"{field} is required")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AnalogSelectorError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _optional_utc(value: Any, *, field: str) -> datetime | None:
    if value is None or value == "":
        return None
    return parse_utc(value, field=field)


def _finite_float(value: Any, *, field: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise AnalogSelectorError(f"{field} must be numeric, not boolean")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise AnalogSelectorError(f"{field} must be numeric") from exc
    if not math.isfinite(parsed):
        raise AnalogSelectorError(f"{field} must be finite")
    return parsed


def _lookup(record: Mapping[str, Any], field: str) -> Any:
    if field in record:
        return record[field]
    features = record.get("features")
    if isinstance(features, Mapping):
        return features.get(field)
    return None


def _event_id(record: Mapping[str, Any]) -> str:
    raw = record.get("event_id")
    if not isinstance(raw, str) or not raw.strip():
        raise AnalogSelectorError("event_id is required and must be a non-empty string")
    return raw.strip()


def _category(value: Any, *, field: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise AnalogSelectorError(f"{field} must be a string when present")
    normalized = " ".join(value.strip().split())
    if not normalized:
        return None
    return normalized.upper() if field == "currency" else normalized.lower()


def validate_contract(contract: Mapping[str, Any]) -> None:
    if int(contract.get("schema_version", 0)) != 1:
        raise AnalogSelectorError("unsupported selector contract schema_version")
    if not contract.get("selector_contract_id"):
        raise AnalogSelectorError("selector_contract_id is required")
    if contract.get("research_only") is not True:
        raise AnalogSelectorError("selector must remain research_only")
    if contract.get("execution_eligible") is not False:
        raise AnalogSelectorError("selector must remain execution-ineligible")
    if contract.get("can_place_orders") is not False:
        raise AnalogSelectorError("selector cannot place orders")
    if contract.get("supported_execution_decision") != "no_trade":
        raise AnalogSelectorError("selector supports only no_trade")

    allowlist = list(contract.get("ranking_field_allowlist") or [])
    prohibited = set(contract.get("prohibited_ranking_fields") or [])
    if not allowlist or len(set(allowlist)) != len(allowlist):
        raise AnalogSelectorError("ranking_field_allowlist must be non-empty and unique")
    overlap = prohibited.intersection(allowlist)
    if overlap:
        raise AnalogSelectorError(f"outcome fields appear in ranking allowlist: {sorted(overlap)}")

    configured: list[str] = list(contract.get("required_exact_fields") or [])
    for group in ("categorical_features", "numeric_features"):
        rows = contract.get(group)
        if not isinstance(rows, Sequence):
            raise AnalogSelectorError(f"{group} must be a sequence")
        for row in rows:
            if not isinstance(row, Mapping) or not row.get("field"):
                raise AnalogSelectorError(f"every {group} entry needs a field")
            configured.append(str(row["field"]))
            for key in ("weight", "mismatch_penalty", "missing_penalty", "both_missing_penalty", "scale"):
                if key in row:
                    number = _finite_float(row[key], field=f"{group}.{row['field']}.{key}")
                    if number is None or number < 0.0:
                        raise AnalogSelectorError(f"{group}.{row['field']}.{key} must be non-negative")
            if group == "numeric_features" and float(row.get("scale", 0.0)) <= 0.0:
                raise AnalogSelectorError(f"numeric feature {row['field']} requires scale > 0")
    if len(configured) != len(set(configured)):
        raise AnalogSelectorError("ranking fields may only be configured once")
    if not set(configured).issubset(set(allowlist)):
        raise AnalogSelectorError("all configured ranking fields must be allowlisted")
    if int(contract.get("default_top_k", 0)) <= 0:
        raise AnalogSelectorError("default_top_k must be positive")


def _causal_consensus_features(
    record: Mapping[str, Any],
    cutoff: datetime,
    feature_known: datetime,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    scheduled = _optional_utc(_lookup(record, "scheduled_release_time_utc"), field="scheduled_release_time_utc")
    consensus_observed = _optional_utc(
        _lookup(record, "consensus_observed_at_utc"), field="consensus_observed_at_utc"
    )
    consensus_value = _finite_float(_lookup(record, "consensus_value"), field="consensus_value")
    source_type = _category(_lookup(record, "consensus_source_type"), field="consensus_source_type")
    market_flag = _lookup(record, "market_consensus") is True

    actual_value = _finite_float(_lookup(record, "initial_actual_value"), field="initial_actual_value")
    if actual_value is None:
        actual_value = _finite_float(_lookup(record, "actual_value"), field="actual_value")
    actual_known = _optional_utc(_lookup(record, "actual_known_utc"), field="actual_known_utc")
    actual_is_known = bool(
        actual_value is not None
        and actual_known is not None
        and actual_known <= cutoff
        and (scheduled is None or actual_known >= scheduled)
    )

    accepted_types = {
        str(value).strip().lower()
        for value in contract.get("market_consensus_source_types", [])
        if str(value).strip()
    }
    causal_market = bool(
        consensus_value is not None
        and consensus_observed is not None
        and scheduled is not None
        and consensus_observed < scheduled
        and consensus_observed <= cutoff
        and (market_flag or source_type in accepted_types)
    )

    internal_value = _finite_float(
        _lookup(record, "internal_expectation_value"), field="internal_expectation_value"
    )
    internal_issued = _optional_utc(
        _lookup(record, "internal_expectation_issued_at_utc"),
        field="internal_expectation_issued_at_utc",
    )
    causal_internal = bool(
        internal_value is not None
        and internal_issued is not None
        and internal_issued <= cutoff
        and scheduled is not None
        and internal_issued < scheduled
    )

    surprise_known = _optional_utc(
        _lookup(record, "standardized_surprise_known_utc"),
        field="standardized_surprise_known_utc",
    )
    surprise_scale_known = _optional_utc(
        _lookup(record, "surprise_scale_known_utc"),
        field="surprise_scale_known_utc",
    )
    causal_standardization = bool(
        actual_known is not None
        and surprise_known is not None
        and surprise_scale_known is not None
        and surprise_known >= actual_known
        and surprise_known <= cutoff
        and scheduled is not None
        and surprise_scale_known < scheduled
    )
    standardized_surprise = None
    if causal_market and actual_is_known and causal_standardization:
        standardized_surprise = _finite_float(
            _lookup(record, "standardized_surprise"), field="standardized_surprise"
        )
    internal_error_known = _optional_utc(
        _lookup(record, "internal_expectation_error_known_utc"),
        field="internal_expectation_error_known_utc",
    )
    internal_scale_known = _optional_utc(
        _lookup(record, "internal_expectation_scale_known_utc"),
        field="internal_expectation_scale_known_utc",
    )
    causal_internal_standardization = bool(
        actual_known is not None
        and internal_error_known is not None
        and internal_scale_known is not None
        and internal_error_known >= actual_known
        and internal_error_known <= cutoff
        and scheduled is not None
        and internal_scale_known < scheduled
    )
    internal_error = None
    if causal_internal and actual_is_known and causal_internal_standardization:
        internal_error = _finite_float(
            _lookup(record, "internal_expectation_error_standardized"),
            field="internal_expectation_error_standardized",
        )

    if causal_market:
        consensus_state = "causal_market_consensus"
        causality_reason = "observed_before_scheduled_release"
    elif causal_internal:
        consensus_state = "internal_expectation_only"
        causality_reason = "internal_expectation_is_not_market_consensus"
    else:
        consensus_state = "missing"
        if consensus_value is not None:
            causality_reason = "market_consensus_missing_provenance_or_not_observed_pre_release"
        else:
            causality_reason = "no_market_consensus"

    return {
        "scheduled_release_time_utc": utc_text(scheduled) if scheduled else None,
        "consensus_state": consensus_state,
        "consensus_causality_reason": causality_reason,
        "actual_known_causally": actual_is_known,
        "standardized_surprise_causal": bool(
            causal_market and actual_is_known and causal_standardization
        ),
        "standardized_surprise": standardized_surprise,
        "internal_expectation_available": causal_internal,
        "internal_expectation_error_causal": bool(
            causal_internal and actual_is_known and causal_internal_standardization
        ),
        "internal_expectation_error_standardized": internal_error,
    }


def normalize_point_in_time_event(
    record: Mapping[str, Any],
    *,
    decision_cutoff_utc: str | datetime,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the only feature view that ranking is allowed to inspect."""

    validate_contract(contract)
    cutoff = parse_utc(decision_cutoff_utc, field="decision_cutoff_utc")
    event_id = _event_id(record)
    event_time = parse_utc(record.get("event_time_utc"), field="event_time_utc")
    feature_known = parse_utc(record.get("feature_known_utc"), field="feature_known_utc")
    if feature_known > cutoff:
        raise IneligibleEvent(f"{event_id}: features were not known by decision cutoff")

    consensus = _causal_consensus_features(record, cutoff, feature_known, contract)
    rates_known = _optional_utc(_lookup(record, "rates_known_utc"), field="rates_known_utc")
    rates_state = _category(_lookup(record, "rates_state"), field="rates_state")
    rates_value = _finite_float(_lookup(record, "rates_repricing_bps"), field="rates_repricing_bps")
    if rates_known is None or rates_known > cutoff:
        rates_state = "missing"
        rates_value = None
    elif rates_state is None:
        rates_state = "point_in_time_available"

    special: dict[str, Any] = {
        "consensus_state": consensus["consensus_state"],
        "standardized_surprise": consensus["standardized_surprise"],
        "internal_expectation_error_standardized": consensus[
            "internal_expectation_error_standardized"
        ],
        "rates_state": rates_state,
        "rates_repricing_bps": rates_value,
    }

    view: dict[str, Any] = {
        "event_id": event_id,
        "event_time_utc": utc_text(event_time),
        "feature_known_utc": utc_text(feature_known),
        "scheduled_release_time_utc": consensus["scheduled_release_time_utc"],
        "consensus_causality_reason": consensus["consensus_causality_reason"],
        "actual_known_causally": consensus["actual_known_causally"],
        "standardized_surprise_causal": consensus[
            "standardized_surprise_causal"
        ],
        "internal_expectation_available": consensus["internal_expectation_available"],
        "internal_expectation_error_causal": consensus[
            "internal_expectation_error_causal"
        ],
    }
    categorical_fields = {
        str(row["field"]) for row in contract.get("categorical_features", [])
    }.union(str(field) for field in contract.get("required_exact_fields", []))
    numeric_fields = {str(row["field"]) for row in contract.get("numeric_features", [])}
    for field in contract.get("ranking_field_allowlist", []):
        if field in special:
            view[field] = special[field]
        elif field in categorical_fields:
            view[field] = _category(_lookup(record, field), field=field)
        elif field in numeric_fields:
            view[field] = _finite_float(_lookup(record, field), field=field)

    # The allowlist is the leakage boundary. Outcome keys and all unlisted input
    # keys are intentionally absent from this view and therefore from its hash.
    return view


def _rounded(value: float) -> float:
    return round(float(value), 12)


def compute_distance(
    query_view: Mapping[str, Any],
    candidate_view: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a fully decomposed deterministic feature distance."""

    validate_contract(contract)
    contributions: list[dict[str, Any]] = []
    for field in contract.get("required_exact_fields", []):
        query_value = query_view.get(field)
        candidate_value = candidate_view.get(field)
        match = query_value is not None and query_value == candidate_value
        contributions.append(
            {
                "field": field,
                "kind": "required_exact",
                "query_value": query_value,
                "candidate_value": candidate_value,
                "state": "match" if match else "mismatch",
                "weighted_distance": 0.0,
            }
        )
        if not match:
            return {
                "eligible": False,
                "exclusion_reason": f"required_exact_mismatch:{field}",
                "distance": None,
                "contributions": contributions,
            }

    for spec in contract.get("categorical_features", []):
        field = str(spec["field"])
        weight = float(spec.get("weight", 1.0))
        query_value = query_view.get(field)
        candidate_value = candidate_view.get(field)
        if query_value is None and candidate_value is None:
            state = "both_missing"
            raw = 0.0
        elif query_value is None or candidate_value is None:
            state = "one_missing"
            raw = float(spec.get("missing_penalty", 0.0))
        elif query_value == candidate_value:
            state = "match"
            raw = 0.0
        else:
            state = "mismatch"
            raw = float(spec.get("mismatch_penalty", 0.0))
        weighted = _rounded(raw * weight)
        contributions.append(
            {
                "field": field,
                "kind": "categorical",
                "query_value": query_value,
                "candidate_value": candidate_value,
                "state": state,
                "raw_distance": _rounded(raw),
                "weight": _rounded(weight),
                "weighted_distance": weighted,
            }
        )

    max_numeric = float(contract.get("max_normalized_numeric_distance", 5.0))
    for spec in contract.get("numeric_features", []):
        field = str(spec["field"])
        weight = float(spec.get("weight", 1.0))
        scale = float(spec["scale"])
        query_value = query_view.get(field)
        candidate_value = candidate_view.get(field)
        if query_value is None and candidate_value is None:
            state = "both_missing"
            raw = float(spec.get("both_missing_penalty", 0.0))
        elif query_value is None or candidate_value is None:
            state = "one_missing"
            raw = float(spec.get("missing_penalty", 0.0))
        else:
            state = "observed"
            raw = min(abs(float(query_value) - float(candidate_value)) / scale, max_numeric)
        weighted = _rounded(raw * weight)
        contributions.append(
            {
                "field": field,
                "kind": "numeric",
                "query_value": query_value,
                "candidate_value": candidate_value,
                "state": state,
                "raw_distance": _rounded(raw),
                "scale": _rounded(scale),
                "weight": _rounded(weight),
                "weighted_distance": weighted,
            }
        )

    distance = _rounded(sum(float(row["weighted_distance"]) for row in contributions))
    return {
        "eligible": True,
        "exclusion_reason": None,
        "distance": distance,
        "contributions": contributions,
    }


def _feature_hash(view: Mapping[str, Any], contract_fingerprint: str) -> str:
    return stable_hash(
        {
            "hash_contract": "point_in_time_feature_view_v1",
            "selector_contract_fingerprint": contract_fingerprint,
            "feature_view": view,
        }
    )


def _selection_material(selection: Mapping[str, Any]) -> dict[str, Any]:
    frozen_selected_fields = (
        "event_id",
        "candidate_feature_hash",
        "distance",
        "distance_contributions",
        "point_in_time_features",
    )
    return {
        "selection_schema_version": selection["selection_schema_version"],
        "selector_contract_id": selection["selector_contract_id"],
        "selector_contract_fingerprint": selection["selector_contract_fingerprint"],
        "decision_cutoff_utc": selection["decision_cutoff_utc"],
        "query_event_id": selection["query_event_id"],
        "query_feature_hash": selection["query_feature_hash"],
        "candidate_universe_hash": selection["candidate_universe_hash"],
        "top_k": selection["top_k"],
        "selected": [
            {field: row[field] for field in frozen_selected_fields}
            for row in selection["selected"]
        ],
    }


def verify_frozen_selection(selection: Mapping[str, Any]) -> bool:
    contract_fingerprint = str(selection.get("selector_contract_fingerprint") or "")
    query_view = selection.get("query_point_in_time_features")
    if not isinstance(query_view, Mapping):
        raise AnalogSelectorError("frozen query point-in-time feature view is missing")
    if _feature_hash(query_view, contract_fingerprint) != selection.get("query_feature_hash"):
        raise AnalogSelectorError("query_feature_hash does not match frozen query features")
    for row in selection.get("selected", []):
        candidate_view = row.get("point_in_time_features")
        if not isinstance(candidate_view, Mapping):
            raise AnalogSelectorError("frozen candidate point-in-time feature view is missing")
        if _feature_hash(candidate_view, contract_fingerprint) != row.get(
            "candidate_feature_hash"
        ):
            raise AnalogSelectorError(
                f"{row.get('event_id')}: candidate feature hash does not match frozen features"
            )
    expected = stable_hash(_selection_material(selection))
    if selection.get("selection_hash") != expected:
        raise AnalogSelectorError("selection_hash does not match frozen feature-only selection")
    return True


def select_analogs(
    query: Mapping[str, Any],
    candidates: Iterable[Mapping[str, Any]],
    *,
    decision_cutoff_utc: str | datetime,
    contract: Mapping[str, Any],
    top_k: int | None = None,
) -> dict[str, Any]:
    """Rank historical analogs without reading or hashing any outcome label."""

    validate_contract(contract)
    cutoff = parse_utc(decision_cutoff_utc, field="decision_cutoff_utc")
    cutoff_text = utc_text(cutoff)
    contract_fingerprint = stable_hash(contract)
    query_view = normalize_point_in_time_event(
        query, decision_cutoff_utc=cutoff, contract=contract
    )
    query_event_time = parse_utc(query_view["event_time_utc"], field="query.event_time_utc")
    query_hash = _feature_hash(query_view, contract_fingerprint)

    resolved_top_k = int(top_k if top_k is not None else contract["default_top_k"])
    if resolved_top_k <= 0:
        raise AnalogSelectorError("top_k must be positive")

    candidate_rows = list(candidates)
    ids = [_event_id(record) for record in candidate_rows]
    if len(ids) != len(set(ids)):
        raise AnalogSelectorError("candidate event_id values must be unique")

    ranked: list[dict[str, Any]] = []
    rejections: list[dict[str, str]] = []
    universe_rows: list[dict[str, Any]] = []
    max_distance = float(contract.get("maximum_distance", math.inf))
    for record in candidate_rows:
        candidate_id = _event_id(record)
        if candidate_id == query_view["event_id"]:
            rejections.append({"event_id": candidate_id, "reason": "same_event_as_query"})
            universe_rows.append({"event_id": candidate_id, "state": "same_event_as_query"})
            continue
        raw_candidate_event_time = parse_utc(
            record.get("event_time_utc"), field="candidate.event_time_utc"
        )
        if raw_candidate_event_time >= query_event_time or raw_candidate_event_time > cutoff:
            reason = "candidate_event_not_historical"
            rejections.append({"event_id": candidate_id, "reason": reason})
            universe_rows.append({"event_id": candidate_id, "state": reason})
            continue
        try:
            view = normalize_point_in_time_event(
                record, decision_cutoff_utc=cutoff, contract=contract
            )
        except IneligibleEvent:
            rejections.append({"event_id": candidate_id, "reason": "features_not_known_at_cutoff"})
            universe_rows.append({"event_id": candidate_id, "state": "features_not_known_at_cutoff"})
            continue
        candidate_feature_hash = _feature_hash(view, contract_fingerprint)
        result = compute_distance(query_view, view, contract=contract)
        if not result["eligible"]:
            reason = str(result["exclusion_reason"])
            rejections.append({"event_id": candidate_id, "reason": reason})
            universe_rows.append(
                {
                    "event_id": candidate_id,
                    "state": reason,
                    "candidate_feature_hash": candidate_feature_hash,
                }
            )
            continue
        if float(result["distance"]) > max_distance:
            reason = "distance_above_maximum"
            rejections.append({"event_id": candidate_id, "reason": reason})
            universe_rows.append(
                {
                    "event_id": candidate_id,
                    "state": reason,
                    "candidate_feature_hash": candidate_feature_hash,
                }
            )
            continue
        universe_rows.append(
            {
                "event_id": candidate_id,
                "state": "eligible",
                "candidate_feature_hash": candidate_feature_hash,
            }
        )
        ranked.append(
            {
                "event_id": candidate_id,
                "candidate_feature_hash": candidate_feature_hash,
                "distance": result["distance"],
                "distance_contributions": result["contributions"],
                "point_in_time_features": view,
            }
        )

    ranked.sort(key=lambda row: (float(row["distance"]), str(row["event_id"])))
    selected = ranked[:resolved_top_k]
    universe_rows.sort(key=lambda row: (str(row["event_id"]), str(row["state"])))
    rejections.sort(key=lambda row: (row["event_id"], row["reason"]))
    candidate_universe_hash = stable_hash(
        {
            "hash_contract": "candidate_universe_v1",
            "selector_contract_fingerprint": contract_fingerprint,
            "rows": universe_rows,
        }
    )
    selection: dict[str, Any] = {
        "selection_schema_version": 1,
        "selector_contract_id": contract["selector_contract_id"],
        "selector_contract_fingerprint": contract_fingerprint,
        "decision_cutoff_utc": cutoff_text,
        "query_event_id": query_view["event_id"],
        "query_feature_hash": query_hash,
        "query_point_in_time_features": query_view,
        "candidate_universe_hash": candidate_universe_hash,
        "top_k": resolved_top_k,
        "selected": selected,
        "rejections": rejections,
        "eligible_candidate_count": len(ranked),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
        "outcomes_attached": False,
    }
    selection["selection_hash"] = stable_hash(_selection_material(selection))
    return selection


def _effective_outcome_known_time(
    outcome: Mapping[str, Any], contract: Mapping[str, Any]
) -> datetime | None:
    time_contract = contract.get("outcome_time_contract") or {}
    known_field = str(time_contract.get("known_timestamp_field", "outcome_known_utc"))
    matured_field = str(time_contract.get("matured_timestamp_field", "matured_utc"))
    known = _optional_utc(outcome.get(known_field), field=known_field)
    matured = _optional_utc(outcome.get(matured_field), field=matured_field)
    timestamps = [stamp for stamp in (known, matured) if stamp is not None]
    return max(timestamps) if timestamps else None


def attach_known_outcomes(
    selection: Mapping[str, Any],
    candidate_records: Iterable[Mapping[str, Any]],
    *,
    decision_cutoff_utc: str | datetime,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach only outcomes knowable at cutoff without reranking candidates."""

    validate_contract(contract)
    verify_frozen_selection(selection)
    cutoff = parse_utc(decision_cutoff_utc, field="decision_cutoff_utc")
    if utc_text(cutoff) != selection.get("decision_cutoff_utc"):
        raise AnalogSelectorError("attachment cutoff must equal the frozen selection cutoff")
    if stable_hash(contract) != selection.get("selector_contract_fingerprint"):
        raise AnalogSelectorError("attachment contract differs from frozen selection contract")

    records = list(candidate_records)
    ids = [_event_id(record) for record in records]
    if len(ids) != len(set(ids)):
        raise AnalogSelectorError("outcome attachment event_id values must be unique")
    by_id = {_event_id(record): record for record in records}

    attached_rows: list[dict[str, Any]] = []
    withheld_count = 0
    attached_count = 0
    for frozen in selection.get("selected", []):
        row = deepcopy(frozen)
        record = by_id.get(str(frozen["event_id"]))
        known_outcomes: list[dict[str, Any]] = []
        if record is not None:
            view = normalize_point_in_time_event(
                record, decision_cutoff_utc=cutoff, contract=contract
            )
            feature_hash = _feature_hash(view, str(selection["selector_contract_fingerprint"]))
            if feature_hash != frozen.get("candidate_feature_hash"):
                raise AnalogSelectorError(
                    f"{frozen['event_id']}: attachment record does not match frozen feature hash"
                )
            outcomes = record.get("outcomes") or []
            if not isinstance(outcomes, Sequence) or isinstance(outcomes, (str, bytes)):
                raise AnalogSelectorError("outcomes must be a sequence of mappings")
            for outcome in outcomes:
                if not isinstance(outcome, Mapping):
                    raise AnalogSelectorError("each outcome must be a mapping")
                effective_known = _effective_outcome_known_time(outcome, contract)
                if effective_known is None or effective_known > cutoff:
                    withheld_count += 1
                    continue
                payload = deepcopy(dict(outcome))
                payload["effective_outcome_known_utc"] = utc_text(effective_known)
                # Force canonical validation before the payload becomes evidence.
                outcome_hash = stable_hash(payload)
                payload["outcome_hash"] = outcome_hash
                known_outcomes.append(payload)
                attached_count += 1
        known_outcomes.sort(
            key=lambda outcome: (
                str(outcome.get("effective_outcome_known_utc", "")),
                str(outcome.get("horizon", outcome.get("horizon_sec", ""))),
                str(outcome.get("outcome_hash", "")),
            )
        )
        row["known_outcomes"] = known_outcomes
        row["outcome_attachment_state"] = (
            "known_outcomes_attached" if known_outcomes else "no_known_outcome_at_cutoff"
        )
        attached_rows.append(row)

    result = deepcopy(dict(selection))
    result["selected"] = attached_rows
    result["outcomes_attached"] = True
    result["known_outcome_count"] = attached_count
    result["withheld_outcome_count"] = withheld_count
    result["attachment_hash"] = stable_hash(
        {
            "hash_contract": "known_outcome_attachment_v1",
            "selection_hash": selection["selection_hash"],
            "decision_cutoff_utc": selection["decision_cutoff_utc"],
            "attached": [
                {
                    "event_id": row["event_id"],
                    "outcome_hashes": [
                        outcome["outcome_hash"] for outcome in row["known_outcomes"]
                    ],
                }
                for row in attached_rows
            ],
        }
    )
    # The feature-only selection hash remains byte-for-byte frozen.
    if result["selection_hash"] != selection["selection_hash"]:
        raise AnalogSelectorError("outcome attachment changed selection hash")
    verify_frozen_selection(result)
    return result


__all__ = [
    "AnalogSelectorError",
    "IneligibleEvent",
    "attach_known_outcomes",
    "canonical_json",
    "compute_distance",
    "normalize_point_in_time_event",
    "parse_utc",
    "select_analogs",
    "stable_hash",
    "validate_contract",
    "verify_frozen_selection",
]
