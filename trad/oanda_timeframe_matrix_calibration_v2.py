"""Pure availability-aware calibration evaluation, with no runtime integration.

This contract consumes separately recorded forecasts and committed label receipts.
It does not accept the legacy outcomes database, infer issue/availability from row
order, attest supplied clocks, or authorize trading. Old calibration bins are not
inputs. The report evaluates calibration conditional on the supplied raw forecasts;
it does not certify the base model's feature or training provenance.
"""

from __future__ import annotations

from bisect import insort
from dataclasses import dataclass
import math
from typing import Any

CONTRACT_ID = "timeframe_matrix_calibration_v2_original_issue_availability_20260906"
BIN_COUNT = 10
PRIOR_STRENGTH = 20.0
WARMUP_LABELS = 40
MAX_TARGET_QUOTE_DELAY_SEC = 60.0


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{name}: finite number required")
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{name}: finite number required") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name}: finite number required")
    return result


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name}: nonempty string required")
    return value


@dataclass(frozen=True)
class CalibrationScope:
    instrument: str
    lane_id: str
    cohort_id: str
    horizon_sec: int

    def validate(self) -> None:
        for name in ("instrument", "lane_id", "cohort_id"):
            _text(getattr(self, name), name)
        if type(self.horizon_sec) is not int or self.horizon_sec <= 0:
            raise ValueError("horizon_sec: positive integer required")

    def matches(self, row: dict) -> bool:
        return all(row[name] == getattr(self, name) for name in self.__dataclass_fields__)


def _identity(row: Any, *, forecast: bool) -> dict:
    if not isinstance(row, dict) or row.get("contract_id") != CONTRACT_ID:
        raise ValueError("separate V2 contract required; historical rows cannot be imported")
    result = {name: _text(row.get(name), name) for name in ("event_id", "instrument")}
    horizon = row.get("horizon_sec")
    if type(horizon) is not int or horizon <= 0:
        raise ValueError("horizon_sec: positive integer required")
    result["horizon_sec"] = horizon
    for name in ("reference_epoch", "target_epoch", "reference_mid"):
        result[name] = _number(row.get(name), name)
    if result["reference_mid"] <= 0:
        raise ValueError("reference_mid: positive price required")
    if result["target_epoch"] != result["reference_epoch"] + horizon:
        raise ValueError("target must remain reference_epoch + horizon_sec")
    if forecast:
        for name in ("forecast_id", "lane_id", "cohort_id"):
            result[name] = _text(row.get(name), name)
        for name in ("issued_epoch", "committed_available_epoch", "raw_probability_up"):
            result[name] = _number(row.get(name), name)
        if not 0 <= result["raw_probability_up"] <= 1:
            raise ValueError("raw_probability_up must be between zero and one")
        if not result["reference_epoch"] <= result["issued_epoch"] <= result["committed_available_epoch"] < result["target_epoch"]:
            raise ValueError("forecast clocks must preserve reference <= original issue <= publication < target")
    else:
        for name in ("target_mid", "target_quote_epoch", "committed_available_epoch"):
            result[name] = _number(row.get(name), name)
        if result["target_mid"] <= 0:
            raise ValueError("target_mid: positive price required")
        if not result["target_epoch"] <= result["target_quote_epoch"] <= result["committed_available_epoch"]:
            raise ValueError("label availability must follow target and actual quote maturity")
        if result["target_quote_epoch"] - result["target_epoch"] > MAX_TARGET_QUOTE_DELAY_SEC:
            raise ValueError("label quote exceeds the fixed target quote delay tolerance")
    return result


def _market_key(row: dict) -> tuple:
    return tuple(row[name] for name in ("instrument", "horizon_sec", "reference_epoch", "target_epoch"))


def _side(probability: float) -> int:
    return 1 if probability > 0.5 else -1 if probability < 0.5 else 0


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def evaluate_calibration(
    forecasts: list[dict], labels: list[dict], *, scope: CalibrationScope,
) -> dict:
    """Evaluate one fixed scope using all unique labels known at original issue.

    Forecast and label identity is the market event (instrument, horizon,
    reference, original target), not its arbitrary event ID. Repeated identical
    copies count once even under aliases; conflicting copies fail closed. A scope
    fixes instrument, lane, cohort and horizon. Labels may be shared by different
    forecast scopes, but each market event trains a selected scope at most once.

    Prior forecasts' raw probabilities locate the ten calibration bins. A label
    trains only when its original target, actual target quote maturity and actual
    committed availability are all strictly earlier than the scored forecast's
    original issue. Availability between issue and publication is excluded. Each
    forecast is evaluated independently, so shuffled rows, simultaneous forecasts,
    and labels arriving out of order cannot change its training set.

    Bin posteriors use the legacy fixed 20-observation bin-center prior; until 40
    unique prior labels exist, the raw forecast is returned and the row is not in
    the after-warmup comparison. Exactly 0.5 abstains. Realized flat moves count as
    false for the explicitly defined P(strictly positive midpoint return) Brier
    target and are excluded from directional accuracy. Target quotes may be at
    most 60 seconds after the unchanged target. No P/L is inferred here.
    """
    if not isinstance(scope, CalibrationScope):
        raise ValueError("CalibrationScope required")
    scope.validate()
    if not isinstance(forecasts, list) or not isinstance(labels, list):
        raise ValueError("forecasts and labels must be lists")

    # Validate every supplied item before filtering: unrelated malformed input
    # cannot silently disappear. Bind arbitrary event IDs to one market identity.
    event_ids: dict[str, tuple] = {}

    def bind_event(row: dict) -> tuple:
        key = _market_key(row)
        previous = event_ids.setdefault(row["event_id"], key)
        if previous != key:
            raise ValueError("event_id reused for conflicting market events")
        return key

    unique_labels: dict[tuple, dict] = {}
    for supplied in labels:
        label = _identity(supplied, forecast=False)
        key = bind_event(label)
        label = {k: v for k, v in label.items() if k != "event_id"}
        if key in unique_labels and label != unique_labels[key]:
            raise ValueError("conflicting label copies for one market event")
        unique_labels[key] = label

    forecast_ids: dict[str, dict] = {}
    unique_forecasts: dict[tuple, dict] = {}
    outside_scope = 0
    for supplied in forecasts:
        forecast = _identity(supplied, forecast=True)
        market_key = bind_event(forecast)
        forecast_id = forecast["forecast_id"]
        if forecast_id in forecast_ids and forecast_ids[forecast_id] != forecast:
            raise ValueError("forecast_id reused for conflicting forecasts")
        forecast_ids[forecast_id] = forecast
        if not scope.matches(forecast):
            outside_scope += 1
            continue
        label = unique_labels.get(market_key)
        if label is not None and label["reference_mid"] != forecast["reference_mid"]:
            raise ValueError("forecast and label reference_mid disagree")
        previous = unique_forecasts.get(market_key)
        semantic = {k: v for k, v in forecast.items() if k not in {"event_id", "forecast_id"}}
        if previous is not None:
            previous_semantic = {k: v for k, v in previous.items() if k not in {"event_id", "forecast_id"}}
            if semantic != previous_semantic:
                raise ValueError("conflicting forecasts for one market event in a scope")
            # Canonical alias is deterministic, independent of input order.
            forecast = min((previous, forecast), key=lambda row: (row["forecast_id"], row["event_id"]))
        unique_forecasts[market_key] = forecast

    ordered = sorted(unique_forecasts.items(), key=lambda item: (item[1]["issued_epoch"], item[0]))
    pending = []
    for market_key, forecast in ordered:
        label = unique_labels.get(market_key)
        if label is not None:
            available = max(label["target_epoch"], label["target_quote_epoch"],
                            label["committed_available_epoch"], forecast["committed_available_epoch"])
            pending.append((available, market_key, forecast, label))
    pending.sort(key=lambda item: (item[0], item[1]))
    pending_index = 0
    # Each unique label updates the bins once at actual availability. The full
    # per-forecast evidence list intentionally scales with the reported training
    # sets; bins do not rescan the complete input on every forecast.
    available_rows = []
    bins = [[0, 0] for _ in range(BIN_COUNT)]
    rows = []
    for market_key, forecast in ordered:
        issue = forecast["issued_epoch"]
        while pending_index < len(pending) and pending[pending_index][0] < issue:
            _, prior_key, prior, label = pending[pending_index]
            pending_index += 1
            insort(available_rows, (label["target_epoch"], label["committed_available_epoch"], prior_key, prior, label))
            index = min(BIN_COUNT - 1, int(prior["raw_probability_up"] * BIN_COUNT))
            bins[index][0] += 1
            bins[index][1] += int(label["target_mid"] > label["reference_mid"])
        eligible = [(item[2], item[3], item[4]) for item in available_rows]
        raw = forecast["raw_probability_up"]
        index = min(BIN_COUNT - 1, int(raw * BIN_COUNT))
        count, up_count = bins[index]
        warmup = len(eligible) < WARMUP_LABELS
        calibrated = raw if warmup else (up_count + PRIOR_STRENGTH * ((index + 0.5) / BIN_COUNT)) / (count + PRIOR_STRENGTH)
        label = unique_labels.get(market_key)
        actual = None if label is None else int(label["target_mid"] > label["reference_mid"])
        actual_side = None if label is None else (1 if label["target_mid"] > label["reference_mid"] else -1 if label["target_mid"] < label["reference_mid"] else 0)
        rows.append({
            "forecast_id": forecast["forecast_id"], "event_id": forecast["event_id"],
            "issued_epoch": issue, "committed_available_epoch": forecast["committed_available_epoch"],
            "reference_epoch": forecast["reference_epoch"], "target_epoch": forecast["target_epoch"],
            "training_forecast_ids": [item[1]["forecast_id"] for item in eligible],
            "training_market_events": [list(item[0]) for item in eligible],
            "n_training_labels": len(eligible), "bin_training_labels": count,
            "last_training_target_epoch": max((item[2]["target_epoch"] for item in eligible), default=None),
            "last_training_maturity_epoch": max((item[2]["target_quote_epoch"] for item in eligible), default=None),
            "last_training_available_epoch": max((item[2]["committed_available_epoch"] for item in eligible), default=None),
            "warmup_fallback": warmup, "scored_after_warmup": label is not None and not warmup,
            "outcome_available": label is not None, "actual_up": actual, "actual_side": actual_side,
            "raw_probability_up": raw, "calibrated_probability_up": calibrated,
            "raw_side": _side(raw), "calibrated_side": _side(calibrated),
            "raw_brier": None if actual is None else (raw - actual) ** 2,
            "calibrated_brier": None if actual is None else (calibrated - actual) ** 2,
        })
    scored = [row for row in rows if row["scored_after_warmup"]]
    directional = [row for row in scored if row["actual_side"] != 0]
    return {
        "contract_id": CONTRACT_ID, "schema_version": 2,
        "scope": {name: getattr(scope, name) for name in scope.__dataclass_fields__},
        "evaluation_kind": "offline_original_issue_availability_calibration",
        "availability_evidence": "caller_supplied_clock_assertions_not_independent_attestation",
        "base_forecast_provenance_verified": False,
        "binary_target": "strictly_positive_midpoint_return",
        "maximum_target_quote_delay_sec": MAX_TARGET_QUOTE_DELAY_SEC,
        "collection_enabled": False, "proof_eligible": False, "account_eligible": False,
        "validation_ready": False, "independent_sample_count": None,
        "input_forecast_count": len(forecasts), "outside_scope_forecast_count": outside_scope,
        "unique_scope_forecast_count": len(rows),
        "missing_outcome_count": sum(not row["outcome_available"] for row in rows),
        "scored_after_warmup_count": len(scored),
        "raw_brier": _mean([row["raw_brier"] for row in scored]),
        "calibrated_brier": _mean([row["calibrated_brier"] for row in scored]),
        "fair_coin_brier": 0.25 if scored else None,
        "raw_directional_accuracy": _mean([float(row["raw_side"] == row["actual_side"]) for row in directional if row["raw_side"] != 0]),
        "calibrated_directional_accuracy": _mean([float(row["calibrated_side"] == row["actual_side"]) for row in directional if row["calibrated_side"] != 0]),
        "directional_target_count": len(directional),
        "raw_directional_decision_count": sum(row["raw_side"] != 0 for row in directional),
        "calibrated_directional_decision_count": sum(row["calibrated_side"] != 0 for row in directional),
        "raw_abstention_count": sum(row["raw_side"] == 0 for row in scored),
        "calibrated_abstention_count": sum(row["calibrated_side"] == 0 for row in scored),
        "flat_outcome_count": sum(row["actual_side"] == 0 for row in scored),
        "rows": rows,
    }
