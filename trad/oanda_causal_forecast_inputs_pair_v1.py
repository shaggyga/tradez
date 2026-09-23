"""Observed-now independent-pair inputs for separate two-family research studies.

Peer-pair availability is irrelevant to this adapter. Missing own-pair minutes
remain missing; retained historical bars acquire availability only at capture.
No broker, database, worker, account or execution operation is performed here.
"""
from __future__ import annotations

import base64
import csv
import hashlib
import math
import re
from pathlib import Path
import time
from types import ModuleType

from oanda_causal_forecast_inputs import _read_tail, _clock, _digest, dependency_versions
from oanda_causal_forecast_inputs_gap_v2 import _parse_exact_source


FAMILIES = ("ridge_return_repaired", "probabilistic_state_space")
SCHEMA = "causal_forecast_observed_m1_inputs_pair_v1_20260907"
NUMERICAL_MODULE = "oanda_pair_local_models_v1"
NUMERICAL_SOURCE_SHA256 = "ecd259fd66d74930ee23c715d29a5dba788d56ab2f7198cd1f521b912637ff26"
MIN_CURRENT_COMMON_BARS = 61
MAX_COMMON_BAR_AGE_SEC = 900
MAX_SOURCE_ROWS = 1024
MAX_TAIL_BYTES = 1024 * 1024
AVAILABILITY = "conservative_consumer_observation_after_stable_read"
TRAINING_POLICY = "Actual own-pair timestamp-addressed windows and exact H1 labels from valid retained segments; no imputation."


def _identity(instrument, pip_size):
    if (type(instrument) is not str or not re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", instrument)
            or instrument[:3] == instrument[4:]):
        raise ValueError("valid_distinct_currency_pair_required")
    if (type(pip_size) not in (int, float) or pip_size not in (.0001, .001, .01)
            or not math.isfinite(pip_size)):
        raise ValueError("bounded_positive_finite_pip_size_required")
    return instrument, float(pip_size)


def _source_rows(source, observed, instrument):
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
    rows = _parse_exact_source(source, instrument, observed)
    if not 1 <= len(rows) <= MAX_SOURCE_ROWS:
        raise ValueError("captured_source_exceeds_bounds")
    return rows


def _derived(rows, observed, instrument):
    if not rows:
        raise ValueError("no_pair_reference")
    reference = max(rows)
    if not 0 <= observed - reference - 60 <= MAX_COMMON_BAR_AGE_SEC:
        raise ValueError("stale_or_future_pair_reference")
    consecutive = 0
    while reference - consecutive * 60 in rows:
        consecutive += 1
    if consecutive < MIN_CURRENT_COMMON_BARS:
        raise ValueError(f"current_common_warmup:{consecutive}<{MIN_CURRENT_COMMON_BARS}")
    return {
        "series": {instrument: [[epoch, price] for epoch, price in sorted(rows.items())]},
        "current_common_start_epochs": [reference - lag * 60 for lag in reversed(range(MIN_CURRENT_COMMON_BARS))],
        "current_common_bars": consecutive, "required_current_common_bars": MIN_CURRENT_COMMON_BARS,
        "retained_real_rows_by_pair": {instrument: len(rows)},
        "reference_start_epoch": reference, "max_bar_close_epoch": reference + 60,
        "common_bar_age_sec": observed - reference - 60,
        "training_label_maturity_max_epoch": reference + 60,
        "training_labels_available_max_epoch": observed,
        "feature_cutoff_epoch": reference + 60, "features_available_epoch": observed,
    }


def _seal(result):
    result["source_capture_sha256"] = _digest(result)
    return result


def capture_inputs(candle_root, instrument, *, pip_size, clock=time.time):
    instrument, pip_size = _identity(instrument, pip_size)
    result = {
        "schema_version": SCHEMA, "status": "abstain", "reasons": [],
        "pairs": [instrument], "output_instrument": instrument, "pip_size": pip_size, "sources": {}, "series": {},
        "model_source_sha256": NUMERICAL_SOURCE_SHA256, "dependency_versions": dependency_versions(),
        "availability_semantics": AVAILABILITY, "training_policy": TRAINING_POLICY,
        "research_only": True, "account_eligible": False, "can_place_orders": False,
    }
    try:
        begun = _clock(clock)
        source = _read_tail(Path(candle_root) / f"{instrument}_M1.csv")
        first = _clock(clock)
        source["first_observed_epoch"] = first
        if first < begun:
            raise ValueError("observation_clock_moved_backwards")
        result["sources"][instrument] = source
        observed = _clock(clock)
        result["first_observed_epoch"] = observed
        if observed < first:
            raise ValueError("observation_clock_moved_backwards")
        rows = _source_rows(source, first, instrument)
        result.update(_derived(rows, observed, instrument))
        result["status"] = "ready"
    except (OSError, ValueError, KeyError, UnicodeError, csv.Error) as exc:
        reason = "source_unreadable" if isinstance(exc, OSError) else str(exc)
        result["reasons"].append(f"{instrument}:{type(exc).__name__}:{reason}")
    return _seal(result)


def _verified_rows(capture, *, instrument=None, pip_size=None):
    if capture.get("schema_version") != SCHEMA or capture.get("status") != "ready":
        raise ValueError("input_not_ready")
    if _digest({key: value for key, value in capture.items() if key != "source_capture_sha256"}) != capture.get("source_capture_sha256"):
        raise ValueError("input_capture_hash_mismatch")
    if capture.get("model_source_sha256") != NUMERICAL_SOURCE_SHA256 or capture.get("dependency_versions") != dependency_versions():
        raise ValueError("model_or_dependency_binding_changed")
    captured_instrument, captured_pip = _identity(capture.get("output_instrument"), capture.get("pip_size"))
    if instrument is not None and captured_instrument != instrument:
        raise ValueError("registered_instrument_binding_mismatch")
    if pip_size is not None and captured_pip != _identity(captured_instrument, pip_size)[1]:
        raise ValueError("registered_pip_size_binding_mismatch")
    instrument = captured_instrument
    if capture.get("pairs") != [instrument] or set(capture.get("sources", {})) != {instrument}:
        raise ValueError("fixed_pair_input_universe_required")
    if (capture.get("availability_semantics") != AVAILABILITY
            or capture.get("training_policy") != TRAINING_POLICY or capture.get("reasons") != []):
        raise ValueError("fixed_output_and_availability_required")
    if capture.get("research_only") is not True or any(capture.get(key) is not False for key in ("account_eligible", "can_place_orders")):
        raise ValueError("inert_capture_required")
    observed = _clock(lambda: capture["first_observed_epoch"])
    source = capture["sources"][instrument]
    first = _clock(lambda: source["first_observed_epoch"])
    if first > observed:
        raise ValueError("source_observed_after_capture")
    rows = _source_rows(source, first, instrument)
    for name, value in _derived(rows, observed, instrument).items():
        if capture.get(name) != value:
            raise ValueError("derived_input_binding_mismatch:" + name)
    return rows


def validate_capture(capture, *, instrument=None, pip_size=None):
    _verified_rows(capture, instrument=instrument, pip_size=pip_size)


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
        "output_instrument": capture.get("output_instrument"), "pip_size": capture.get("pip_size"),
        "dependency_versions": dependency_versions(),
    }
    try:
        rows = _verified_rows(capture)
        predictions = _numerical_module().predict_all(rows, capture["reference_start_epoch"],
            instrument=capture["output_instrument"], pip_size=capture["pip_size"])
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


def coverage_probe(candle_root, instruments, *, pip_sizes, clock=time.time):
    """Read at most 68 local archives; report eligibility without fitting.

    Pip sizes must be supplied from a separately frozen instrument registry.
    They are never inferred from currency suffixes. This diagnostic does not
    publish forecasts, register a study, write files or authorize orders.
    """
    pairs = []
    for pair in instruments:
        if len(pairs) >= 68:
            raise ValueError("coverage_probe_maximum_68_pairs")
        if pair in pairs:
            raise ValueError("coverage_probe_duplicate_pair")
        _identity(pair, pip_sizes.get(pair))
        pairs.append(pair)
    if not pairs:
        raise ValueError("coverage_probe_nonempty_pairs_required")
    begun = _clock(clock)
    reports = []
    for instrument in pairs:
        capture = capture_inputs(candle_root, instrument, pip_size=pip_sizes[instrument], clock=clock)
        report = {"instrument": instrument, "pip_size": capture["pip_size"],
            "status": "abstain", "reasons": list(capture["reasons"]),
            "current_pair_bars": None, "required_current_pair_bars": MIN_CURRENT_COMMON_BARS,
            "mature_ridge_training_rows": None, "required_ridge_training_rows": 24,
            "retained_real_rows": None, "source_capture_sha256": capture["source_capture_sha256"],
            "first_observed_epoch": capture.get("first_observed_epoch")}
        try:
            source = capture["sources"][instrument]
            rows = _source_rows(source, source["first_observed_epoch"], instrument)
            reference = max(rows)
            run = 0
            previous = None
            runs = {}
            for epoch in sorted(rows):
                run = run + 1 if previous is not None and epoch - previous == 60 else 1
                runs[epoch] = run
                previous = epoch
            training_count = sum(epoch // 60 % 3 == 0 and epoch + 3600 <= reference
                and runs.get(epoch + 3600, 0) >= 121 for epoch in rows)
            report.update(current_pair_bars=runs[reference], mature_ridge_training_rows=training_count,
                retained_real_rows=len(rows), reference_start_epoch=reference,
                source_sha256=source["captured_bytes_sha256"],
                latest_bar_age_sec=capture["first_observed_epoch"] - reference - 60)
            if capture["status"] == "ready" and training_count >= 24:
                report["status"] = "ready"
            elif capture["status"] == "ready":
                report["reasons"].append(f"mature_ridge_training_rows:{training_count}<24")
        except (OSError, ValueError, KeyError, UnicodeError, csv.Error) as exc:
            if not report["reasons"]:
                report["reasons"].append(f"{type(exc).__name__}:{exc}")
        reports.append(report)
    completed = _clock(clock)
    if completed < begun:
        raise ValueError("observation_clock_moved_backwards")
    return {"schema_version": "independent_pair_input_coverage_v1_20260907",
        "begun_epoch": begun, "completed_epoch": completed,
        "pair_count": len(reports), "ready_pair_count": sum(row["status"] == "ready" for row in reports),
        "rows": reports, "research_only": True, "can_place_orders": False, "can_promote": False}
