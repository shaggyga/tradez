"""Pure, forecast-time baselines for an offline prediction assessment.

The caller supplies realized labels and the time at which a forecast was issued.
A label can train a baseline only after both its target and its actual label
availability time.  This module never infers availability from input row order.
It neither fits the project models nor starts any project runtime.
"""

from __future__ import annotations

import math
from typing import Any


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    return number


def _nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _horizon(value: Any, field: str) -> float:
    result = _finite_number(value, field)
    if result <= 0:
        raise ValueError(f"{field} must be positive")
    return result


def _positive_integer(value: Any, field: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def decide_baselines(
    issue_epoch: float,
    labels: list[dict[str, Any]],
    *,
    lookback: int = 100,
    min_labels: int = 20,
    instrument: str = "EUR_USD",
    horizon_sec: float = 3600,
) -> dict[str, Any]:
    """Return fixed reference predictions using strictly prior known labels.

    Required label fields are ``event_id``, ``instrument``, ``horizon_sec``,
    ``target_epoch``, ``available_epoch`` and an exact boolean ``up``.  Extra
    metadata is ignored.  All supplied labels are validated before filtering,
    including labels from other instruments, horizons or future target times.
    Repeated identical canonical labels count once; conflicting repetitions of
    an event ID fail closed.  Availability before the target is invalid.

    Eligible unique labels must match the requested instrument and horizon and
    satisfy both ``target_epoch < issue_epoch`` and
    ``available_epoch < issue_epoch``.  The latest ``lookback`` labels, sorted
    by (target, availability, event ID), form the training subset.  Its smoothed
    up probability is (up count + 1) / (label count + 2) once ``min_labels`` are
    available, otherwise 0.5.  A probability tie always abstains (side 0).

    Training event IDs are returned oldest first.  The last clock fields are
    the maxima of each clock independently across the selected subset, making
    forecast-time availability auditable even when labels arrived out of order.
    ``eligible_training_count`` counts all eligible unique labels before the
    lookback limit; ``n_training_labels`` counts the selected subset, including
    during warmup.  Inputs are not modified.
    """
    issue = _finite_number(issue_epoch, "issue_epoch")
    selected_instrument = _nonempty_string(instrument, "instrument")
    selected_horizon = _horizon(horizon_sec, "horizon_sec")
    limit = _positive_integer(lookback, "lookback")
    minimum = _positive_integer(min_labels, "min_labels")
    if not isinstance(labels, list):
        raise ValueError("labels must be a list of dictionaries")

    required = {
        "event_id", "instrument", "horizon_sec", "target_epoch",
        "available_epoch", "up",
    }
    unique: dict[str, tuple[str, float, float, float, bool]] = {}
    for index, label in enumerate(labels):
        if not isinstance(label, dict) or not required.issubset(label):
            raise ValueError(f"label {index} is missing required fields")
        event_id = _nonempty_string(label["event_id"], f"label {index} event_id")
        label_instrument = _nonempty_string(
            label["instrument"], f"label {index} instrument"
        )
        label_horizon = _horizon(label["horizon_sec"], f"label {index} horizon_sec")
        target = _finite_number(label["target_epoch"], f"label {index} target_epoch")
        available = _finite_number(
            label["available_epoch"], f"label {index} available_epoch"
        )
        if type(label["up"]) is not bool:
            raise ValueError(f"label {index} up must be a boolean")
        if available < target:
            raise ValueError(f"label {index} availability precedes its target")
        canonical = (label_instrument, label_horizon, target, available, label["up"])
        previous = unique.get(event_id)
        if previous is not None and previous != canonical:
            raise ValueError(f"conflicting labels for event_id {event_id!r}")
        unique[event_id] = canonical

    eligible = [
        (target, available, event_id, up)
        for event_id, (label_instrument, label_horizon, target, available, up)
        in unique.items()
        if label_instrument == selected_instrument
        and label_horizon == selected_horizon
        and target < issue
        and available < issue
    ]
    eligible.sort(key=lambda row: (row[0], row[1], row[2]))
    training = eligible[-limit:]
    count = len(training)
    fallback = count < minimum
    probability = 0.5 if fallback else (sum(row[3] for row in training) + 1) / (count + 2)
    side = 1 if probability > 0.5 else -1 if probability < 0.5 else 0
    return {
        "fair_coin": {"probability_up": 0.5, "side": 0},
        "zero_move": {"predicted_return_bps": 0.0},
        "no_trade": {"side": 0},
        "rolling_class_rate": {
            "probability_up": probability,
            "side": side,
            "n_training_labels": count,
            "eligible_training_count": len(eligible),
            "warmup_fallback": fallback,
            "last_training_target_epoch": max((row[0] for row in training), default=None),
            "last_training_available_epoch": max((row[1] for row in training), default=None),
            "training_event_ids": [row[2] for row in training],
        },
    }
