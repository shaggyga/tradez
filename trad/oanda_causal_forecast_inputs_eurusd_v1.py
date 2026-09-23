"""Observed-now EURUSD-only inputs for a separate two-family research study.

Peer-pair availability is irrelevant to this adapter. Missing EURUSD minutes
remain missing; retained historical bars acquire availability only at capture.
No broker, database, worker, account or execution operation is performed here.
"""
from __future__ import annotations

import base64
import csv
import hashlib
import math
from pathlib import Path
import time
from types import ModuleType

from oanda_causal_forecast_inputs import _read_tail, _clock, _digest, dependency_versions
from oanda_causal_forecast_inputs_gap_v2 import _parse_exact_source


PAIRS = ("EUR_USD",)
FAMILIES = ("ridge_return_repaired", "probabilistic_state_space")
SCHEMA = "causal_forecast_observed_m1_inputs_eurusd_v1_20260907"
NUMERICAL_MODULE = "oanda_eurusd_local_models_v1"
NUMERICAL_SOURCE_SHA256 = "8cd57cb20d26a220dcc1328b949d5e380d31e9d63ece12af743a010ac4b263d7"
MIN_CURRENT_COMMON_BARS = 61
MAX_COMMON_BAR_AGE_SEC = 900
MAX_SOURCE_ROWS = 1024
MAX_TAIL_BYTES = 1024 * 1024
AVAILABILITY = "conservative_consumer_observation_after_stable_read"
TRAINING_POLICY = "Actual EURUSD timestamp-addressed windows and exact H1 labels from valid retained segments; no imputation."


def _source_rows(source, observed):
    # Revalidate exact evidence bounds and byte geometry as well as its digest.
    if len(source["tail_base64"]) > 4 * ((MAX_TAIL_BYTES + 2) // 3) or len(source["header_base64"]) > 10924:
        raise ValueError("captured_source_exceeds_bounds")
    header = base64.b64decode(source["header_base64"], validate=True)
    tail = base64.b64decode(source["tail_base64"], validate=True)
    if not 0 < len(header) <= 8192 or not 0 < len(tail) <= MAX_TAIL_BYTES:
        raise ValueError("captured_source_exceeds_bounds")
    if not header.endswith(b"\n") or not tail.endswith(b"\n"):
        raise ValueError("partial_captured_source")
    if any(type(source.get(key)) is not int for key in ("file_size_bytes", "tail_byte_offset", "tail_byte_length")):
        raise ValueError("invalid_captured_byte_geometry")
    if (source["tail_byte_length"] != len(tail) or source["tail_byte_offset"] < len(header)
            or source["tail_byte_offset"] + len(tail) != source["file_size_bytes"]):
        raise ValueError("invalid_captured_byte_geometry")
    rows = _parse_exact_source(source, "EUR_USD", observed)
    if not 1 <= len(rows) <= MAX_SOURCE_ROWS:
        raise ValueError("captured_source_exceeds_bounds")
    return rows


def _derived(rows, observed):
    if not rows:
        raise ValueError("no_eurusd_reference")
    reference = max(rows)
    if not 0 <= observed - reference - 60 <= MAX_COMMON_BAR_AGE_SEC:
        raise ValueError("stale_or_future_eurusd_reference")
    consecutive = 0
    while reference - consecutive * 60 in rows:
        consecutive += 1
    if consecutive < MIN_CURRENT_COMMON_BARS:
        raise ValueError(f"current_common_warmup:{consecutive}<{MIN_CURRENT_COMMON_BARS}")
    return {
        "series": {"EUR_USD": [[epoch, price] for epoch, price in sorted(rows.items())]},
        "current_common_start_epochs": [reference - lag * 60 for lag in reversed(range(MIN_CURRENT_COMMON_BARS))],
        "current_common_bars": consecutive, "required_current_common_bars": MIN_CURRENT_COMMON_BARS,
        "retained_real_rows_by_pair": {"EUR_USD": len(rows)},
        "reference_start_epoch": reference, "max_bar_close_epoch": reference + 60,
        "common_bar_age_sec": observed - reference - 60,
        "training_label_maturity_max_epoch": reference + 60,
        "training_labels_available_max_epoch": observed,
        "feature_cutoff_epoch": reference + 60, "features_available_epoch": observed,
    }


def _seal(result):
    result["source_capture_sha256"] = _digest(result)
    return result


def capture_inputs(candle_root, *, clock=time.time):
    result = {
        "schema_version": SCHEMA, "status": "abstain", "reasons": [],
        "pairs": list(PAIRS), "output_instrument": "EUR_USD", "sources": {}, "series": {},
        "model_source_sha256": NUMERICAL_SOURCE_SHA256, "dependency_versions": dependency_versions(),
        "availability_semantics": AVAILABILITY, "training_policy": TRAINING_POLICY,
        "research_only": True, "account_eligible": False, "can_place_orders": False,
    }
    try:
        begun = _clock(clock)
        source = _read_tail(Path(candle_root) / "EUR_USD_M1.csv")
        first = _clock(clock)
        source["first_observed_epoch"] = first
        if first < begun:
            raise ValueError("observation_clock_moved_backwards")
        result["sources"]["EUR_USD"] = source
        observed = _clock(clock)
        result["first_observed_epoch"] = observed
        if observed < first:
            raise ValueError("observation_clock_moved_backwards")
        rows = _source_rows(source, first)
        result.update(_derived(rows, observed))
        result["status"] = "ready"
    except (OSError, ValueError, KeyError, UnicodeError, csv.Error) as exc:
        reason = "source_unreadable" if isinstance(exc, OSError) else str(exc)
        result["reasons"].append(f"EUR_USD:{type(exc).__name__}:{reason}")
    return _seal(result)


def _verified_rows(capture):
    if capture.get("schema_version") != SCHEMA or capture.get("status") != "ready":
        raise ValueError("input_not_ready")
    if _digest({key: value for key, value in capture.items() if key != "source_capture_sha256"}) != capture.get("source_capture_sha256"):
        raise ValueError("input_capture_hash_mismatch")
    if capture.get("model_source_sha256") != NUMERICAL_SOURCE_SHA256 or capture.get("dependency_versions") != dependency_versions():
        raise ValueError("model_or_dependency_binding_changed")
    if capture.get("pairs") != list(PAIRS) or set(capture.get("sources", {})) != set(PAIRS):
        raise ValueError("fixed_eurusd_input_universe_required")
    if (capture.get("output_instrument") != "EUR_USD" or capture.get("availability_semantics") != AVAILABILITY
            or capture.get("training_policy") != TRAINING_POLICY or capture.get("reasons") != []):
        raise ValueError("fixed_output_and_availability_required")
    if capture.get("research_only") is not True or any(capture.get(key) is not False for key in ("account_eligible", "can_place_orders")):
        raise ValueError("inert_capture_required")
    observed = _clock(lambda: capture["first_observed_epoch"])
    source = capture["sources"]["EUR_USD"]
    first = _clock(lambda: source["first_observed_epoch"])
    if first > observed:
        raise ValueError("source_observed_after_capture")
    rows = _source_rows(source, first)
    for name, value in _derived(rows, observed).items():
        if capture.get(name) != value:
            raise ValueError("derived_input_binding_mismatch:" + name)
    return rows


def validate_capture(capture):
    _verified_rows(capture)


def _numerical_module():
    path = Path(__file__).with_name(NUMERICAL_MODULE + ".py")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != NUMERICAL_SOURCE_SHA256:
        raise ValueError("numerical_source_binding_changed")
    module = ModuleType(NUMERICAL_MODULE)
    module.__file__ = str(path)
    exec(compile(raw, str(path), "exec"), module.__dict__)
    return module


def compute_predictions(capture, *, clock=time.time):
    result = {
        "status": "abstain", "reasons": [], "predictions": {},
        "model_source_sha256": NUMERICAL_SOURCE_SHA256,
        "source_capture_sha256": capture.get("source_capture_sha256"),
        "dependency_versions": dependency_versions(),
    }
    try:
        rows = _verified_rows(capture)
        predictions = _numerical_module().predict_all(rows, capture["reference_start_epoch"])
        if set(predictions) - set(FAMILIES):
            raise ValueError("unexpected_family_prediction")
        for family in FAMILIES:
            if family not in predictions:
                raise ValueError("family_prediction_unavailable:" + family)
            expected, probability, diagnostics = predictions[family]
            if isinstance(expected, bool) or isinstance(probability, bool) or not math.isfinite(expected) or not math.isfinite(probability) or not 0 <= probability <= 1:
                raise ValueError("invalid_family_prediction:" + family)
            result["predictions"][family] = {
                "expected_signed_pips": float(expected), "probability_up": float(probability),
                "side": 1 if expected >= 0 else -1, "diagnostics": diagnostics,
            }
        completed = _clock(clock)
        if completed < capture["first_observed_epoch"]:
            raise ValueError("computation_clock_moved_backwards")
        result.update(status="ready", computed_epoch=completed)
    except Exception as exc:
        result["predictions"] = {}
        result["reasons"].append(f"{type(exc).__name__}:{exc}")
    return result
