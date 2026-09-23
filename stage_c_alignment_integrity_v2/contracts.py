"""Small dependency-free contracts shared by later all-68 research runners."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from math import isfinite
from typing import Any

import numpy as np

SCHEMA_VERSION = "all68_integrity_contract.v2"


def canonical_bytes(value: Any) -> bytes:
    """Stable content representation for settings and dependency identities."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


@dataclass(frozen=True)
class TrainingView:
    """Explicitly restrict a fitting population to information known at cutoff."""

    origin_start_epoch: int
    origin_end_epoch: int
    fit_cutoff_epoch: int
    protected_eval_start_epoch: int
    protected_eval_end_epoch: int

    def __post_init__(self) -> None:
        if not (self.origin_start_epoch < self.origin_end_epoch <= self.fit_cutoff_epoch <= self.protected_eval_start_epoch < self.protected_eval_end_epoch):
            raise ValueError("training view must order origin, fit cutoff and protected evaluation chronologically")

    def eligible(self, origin: np.ndarray, outcome_ready: np.ndarray, label_end: np.ndarray) -> np.ndarray:
        origin = np.asarray(origin, dtype=np.int64)
        outcome_ready = np.asarray(outcome_ready, dtype=np.int64)
        label_end = np.asarray(label_end, dtype=np.int64)
        if not (origin.shape == outcome_ready.shape == label_end.shape):
            raise ValueError("training arrays must have matching shapes")
        in_origin_range = (origin >= self.origin_start_epoch) & (origin < self.origin_end_epoch)
        matured_by_fit = outcome_ready <= self.fit_cutoff_epoch
        no_interval_overlap = (label_end <= self.protected_eval_start_epoch) | (origin >= self.protected_eval_end_epoch)
        return in_origin_range & matured_by_fit & no_interval_overlap

    def identity(self) -> dict[str, int | str]:
        return {"schema": SCHEMA_VERSION, "origin_start_epoch": self.origin_start_epoch, "origin_end_epoch": self.origin_end_epoch, "fit_cutoff_epoch": self.fit_cutoff_epoch, "protected_eval_start_epoch": self.protected_eval_start_epoch, "protected_eval_end_epoch": self.protected_eval_end_epoch}


def forecast_coverage(feature_ready: np.ndarray, model_ready_epoch: int, decision_epoch: np.ndarray) -> np.ndarray:
    """Forecast issuance uses present readiness only, never its future endpoint."""
    feature_ready = np.asarray(feature_ready, dtype=bool)
    decision_epoch = np.asarray(decision_epoch, dtype=np.int64)
    if feature_ready.shape != decision_epoch.shape:
        raise ValueError("feature readiness and decision times must match")
    return feature_ready & (decision_epoch >= int(model_ready_epoch))


def forecast_record(*, forecast_id: str, instrument: str, decision_epoch: int, available_epoch: int,
                    model_id: str, model_ready_epoch: int, training_view: TrainingView,
                    target_id: str, prediction: float, coverage_reason: str = "eligible") -> dict[str, Any]:
    if available_epoch < decision_epoch:
        raise ValueError("forecast cannot be available before its decision")
    if model_ready_epoch > decision_epoch:
        raise ValueError("model was not ready by decision time")
    if not isfinite(prediction):
        raise ValueError("forecast prediction must be finite")
    row = {"schema_version": "forecast.v2", "forecast_id": forecast_id, "instrument": instrument,
           "decision_epoch": int(decision_epoch), "available_epoch": int(available_epoch),
           "model_id": model_id, "model_ready_epoch": int(model_ready_epoch),
           "training_view_fingerprint": fingerprint(training_view.identity()), "target_id": target_id,
           "prediction": float(prediction), "coverage_reason": coverage_reason}
    validate_forecast(row)
    return row


def validate_forecast(row: dict[str, Any]) -> None:
    forbidden = {"actual", "actual_bps", "outcome", "outcome_value", "outcome_ready_epoch", "settled_at"}
    found = forbidden.intersection(row)
    if found:
        raise ValueError(f"forecast record contains later outcome fields: {sorted(found)}")
    required = {"schema_version", "forecast_id", "instrument", "decision_epoch", "available_epoch", "model_id", "model_ready_epoch", "training_view_fingerprint", "target_id", "prediction", "coverage_reason"}
    missing = required.difference(row)
    if missing:
        raise ValueError(f"forecast record missing fields: {sorted(missing)}")
    unexpected = set(row).difference(required)
    if unexpected:
        raise ValueError(f"forecast record contains unsupported fields: {sorted(unexpected)}")
    if row["schema_version"] != "forecast.v2":
        raise ValueError("unsupported forecast schema")
    if int(row["available_epoch"]) < int(row["decision_epoch"]):
        raise ValueError("forecast availability precedes decision")
    if int(row["model_ready_epoch"]) > int(row["decision_epoch"]):
        raise ValueError("forecast uses a model that was not ready")
    if not isfinite(float(row["prediction"])):
        raise ValueError("forecast prediction must be finite")


def outcome_record(*, forecast_id: str, outcome_ready_epoch: int, state: str, value: float | None) -> dict[str, Any]:
    if state not in {"PENDING", "MATURED", "RIGHT_CENSORED", "AMBIGUOUS", "INVALID_INPUT"}:
        raise ValueError("unknown outcome state")
    return {"schema_version": "outcome.v2", "forecast_id": forecast_id,
            "outcome_ready_epoch": int(outcome_ready_epoch), "state": state, "value": value}
