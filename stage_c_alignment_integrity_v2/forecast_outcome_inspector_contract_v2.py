"""Read-only forecast/outcome inspection and reconciliation contract.

This module does not fit models, score trades, contact services or reveal outcomes
by default.  It binds forecast-only rows, explicit coverage rows and outcome rows
so a reviewer can inspect what was issued, what was missing/WAIT, and what would
be revealable only under an explicit as-of request.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable

from contracts import fingerprint, validate_forecast

OUTCOME_STATES = {"PENDING", "MATURED", "RIGHT_CENSORED", "AMBIGUOUS", "INVALID_INPUT"}


def validate_outcome(row: dict[str, Any]) -> None:
    required = {"schema_version", "forecast_id", "outcome_ready_epoch", "state", "value"}
    missing = required.difference(row)
    if missing:
        raise ValueError(f"outcome record missing fields: {sorted(missing)}")
    unexpected = set(row).difference(required)
    if unexpected:
        raise ValueError(f"outcome record contains unsupported fields: {sorted(unexpected)}")
    if row["schema_version"] != "outcome.v2":
        raise ValueError("unsupported outcome schema")
    if row["state"] not in OUTCOME_STATES:
        raise ValueError("unknown outcome state")
    if row["state"] == "MATURED" and row["value"] is None:
        raise ValueError("matured outcome requires a value")
    if row["state"] != "MATURED" and row["value"] is not None:
        raise ValueError("unmatured or censored outcome must not carry a value")
    int(row["outcome_ready_epoch"])


def _forecast_map(forecasts: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in forecasts:
        validate_forecast(row)
        fid = row["forecast_id"]
        if fid in result:
            raise ValueError("duplicate_forecast_id")
        result[fid] = row
    return result


def _outcome_map(outcomes: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in outcomes:
        validate_outcome(row)
        fid = row["forecast_id"]
        if fid in result:
            raise ValueError("duplicate_outcome_id")
        result[fid] = row
    return result


def _coverage_identity(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("instrument"),
        row.get("decision_epoch"),
        row.get("target_id"),
        row.get("model_id"),
        row.get("forecast_id"),
    )


def reconcile_forecast_publication(*, forecasts: Iterable[dict[str, Any]],
                                   outcomes: Iterable[dict[str, Any]],
                                   coverage: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Return explicit publication support without revealing outcome values.

    Coverage rows are intentionally simple and publication-agnostic.  Each row must
    include `forecast_id`, `instrument`, `decision_epoch`, `target_id`, `model_id`,
    `status` and `reason`.  `eligible` rows require exactly one forecast and one
    outcome placeholder.  Non-eligible rows must not have a forecast or outcome.
    """
    forecast_by_id = _forecast_map(forecasts)
    outcome_by_id = _outcome_map(outcomes)
    seen_coverage: set[str] = set()
    support: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    outcome_counts: dict[str, int] = {}
    errors: list[dict[str, Any]] = []

    for row in coverage:
        required = {"forecast_id", "instrument", "decision_epoch", "target_id", "model_id", "status", "reason"}
        missing = required.difference(row)
        if missing:
            raise ValueError(f"coverage record missing fields: {sorted(missing)}")
        fid = row["forecast_id"]
        if fid in seen_coverage:
            raise ValueError("duplicate_coverage_forecast_id")
        seen_coverage.add(fid)
        status = row["status"]
        reason = row["reason"]
        status_counts[status] = status_counts.get(status, 0) + 1
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        forecast = forecast_by_id.get(fid)
        outcome = outcome_by_id.get(fid)
        if status == "eligible":
            if forecast is None:
                errors.append({"forecast_id": fid, "error": "eligible_missing_forecast"})
            if outcome is None:
                errors.append({"forecast_id": fid, "error": "eligible_missing_outcome_placeholder"})
            if forecast is not None:
                for key in ("instrument", "decision_epoch", "target_id", "model_id"):
                    if forecast[key] != row[key]:
                        errors.append({"forecast_id": fid, "error": "forecast_coverage_identity_mismatch", "field": key})
            if outcome is not None:
                outcome_counts[outcome["state"]] = outcome_counts.get(outcome["state"], 0) + 1
        else:
            if forecast is not None:
                errors.append({"forecast_id": fid, "error": "noneligible_has_forecast"})
            if outcome is not None:
                errors.append({"forecast_id": fid, "error": "noneligible_has_outcome"})
        support.append({
            "forecast_id": fid,
            "coverage_identity": fingerprint(_coverage_identity(row)),
            "status": status,
            "reason": reason,
            "has_forecast": forecast is not None,
            "has_outcome_placeholder": outcome is not None,
        })

    unregistered_forecasts = sorted(set(forecast_by_id).difference(seen_coverage))
    unregistered_outcomes = sorted(set(outcome_by_id).difference(seen_coverage))
    for fid in unregistered_forecasts:
        errors.append({"forecast_id": fid, "error": "forecast_without_coverage"})
    for fid in unregistered_outcomes:
        errors.append({"forecast_id": fid, "error": "outcome_without_coverage"})

    return {
        "schema_version": "forecast_outcome_reconciliation.v1",
        "coverage_rows": len(support),
        "forecast_rows": len(forecast_by_id),
        "outcome_rows": len(outcome_by_id),
        "status_counts": dict(sorted(status_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "outcome_state_counts": dict(sorted(outcome_counts.items())),
        "support": support,
        "errors": errors,
        "accepted": not errors,
        "outcomes_revealed": False,
    }


def inspect_forecast(*, forecast_id: str, forecasts: Iterable[dict[str, Any]],
                     outcomes: Iterable[dict[str, Any]], asof_epoch: int,
                     reveal_outcome: bool = False) -> dict[str, Any]:
    if type(reveal_outcome) is not bool:
        raise ValueError("explicit_boolean_outcome_reveal_required")
    forecast_by_id = _forecast_map(forecasts)
    outcome_by_id = _outcome_map(outcomes)
    if forecast_id not in forecast_by_id:
        raise ValueError("unregistered_forecast_id")
    forecast = forecast_by_id[forecast_id]
    if asof_epoch < forecast["available_epoch"]:
        return {"schema_version": "forecast_outcome_inspection.v1", "forecast_id": forecast_id,
                "status": "not_yet_available", "forecast": None,
                "outcomes_revealed": False, "outcome": None}
    result = {"schema_version": "forecast_outcome_inspection.v1", "forecast_id": forecast_id,
              "status": "available", "forecast": deepcopy(forecast),
              "forecast_source_sha256": fingerprint(forecast),
              "outcomes_revealed": False, "outcome": None}
    if not reveal_outcome:
        return result
    outcome = outcome_by_id.get(forecast_id)
    result["outcomes_revealed"] = True
    if outcome is None:
        result["outcome"] = {"status": "missing_outcome_placeholder"}
    elif asof_epoch < outcome["outcome_ready_epoch"]:
        result["outcome"] = {"status": "not_yet_available", "outcome_ready_epoch": outcome["outcome_ready_epoch"]}
    else:
        result["outcome"] = deepcopy(outcome)
        result["outcome_source_sha256"] = fingerprint(outcome)
    return result
