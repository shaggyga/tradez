"""Bounded, observed-now M1 inputs for a separate four-family research cohort.

No historical availability is inferred. Captured legacy candles become known
to this consumer only after a successful stable read. No worker, database,
network, broker, forecast-row builder or execution function is called here.
"""
from __future__ import annotations

import base64
import csv
from datetime import datetime
import hashlib
from importlib import metadata
import importlib.util
import json
import math
from pathlib import Path
import platform
import time
from typing import Callable

PAIRS = ("EUR_USD", "GBP_USD", "AUD_USD", "NZD_USD", "USD_JPY", "USD_CHF", "USD_CAD")
FAMILIES = ("ridge_return_repaired", "modern_tabular_probabilistic_repaired",
            "cross_pair_graph_transfer", "probabilistic_state_space")
SCHEMA = "causal_forecast_observed_m1_inputs_v1"
NUMERICAL_SOURCE_SHA256 = "51c3f945eadb3f26166cfddd37d97451ef21e2b69ece6664c57ee06c145ff417"
MAX_TAIL_BYTES = 1024 * 1024
MAX_SOURCE_ROWS = 1024
MAX_MODEL_ROWS = 512
MIN_MODEL_ROWS = 335
MAX_COMMON_BAR_AGE_SEC = 900


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _digest(value: object) -> str:
    return _hash(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())


def dependency_versions() -> dict:
    versions = {"python": platform.python_version()}
    for package in ("numpy", "scikit-learn"):
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = "unavailable"
    return versions


def _clock(clock: Callable[[], float]) -> float:
    value = clock()
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("invalid_observation_clock")
    return float(value)


def _signature(stat) -> tuple:
    # Windows path.stat() and fstat() can expose different ctime semantics.
    # File identity, exact byte size and last-write time are common to both.
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _read_tail(path: Path) -> dict:
    # An append, truncation, or atomic replacement during capture invalidates
    # this attempt. Filesystem timestamps are consistency checks, never clocks.
    with path.open("rb") as handle:
        before = path.stat()
        header = handle.readline(8193)
        if not header.endswith(b"\n") or len(header) > 8192:
            raise ValueError("invalid_or_oversized_header")
        handle.seek(0, 2)
        size = handle.tell()
        offset = max(len(header), size - MAX_TAIL_BYTES)
        handle.seek(offset)
        raw = handle.read(MAX_TAIL_BYTES)
        if offset > len(header):
            split = raw.find(b"\n")
            if split < 0:
                raise ValueError("oversized_csv_row")
            offset += split + 1
            raw = raw[split + 1:]
        after = path.stat()
        # fstat prevents accepting an old open handle after atomic replacement.
        import os
        if _signature(before) != _signature(after) or _signature(after) != _signature(os.fstat(handle.fileno())):
            raise ValueError("source_changed_during_read")
    if not raw or not raw.endswith(b"\n"):
        raise ValueError("empty_or_partial_csv_tail")
    lines = raw.splitlines(keepends=True)
    if len(lines) > MAX_SOURCE_ROWS:
        discarded = lines[:-MAX_SOURCE_ROWS]
        offset += sum(map(len, discarded))
        raw = b"".join(lines[-MAX_SOURCE_ROWS:])
    return {"path": str(path.resolve()), "file_size_bytes": size,
            "tail_byte_offset": offset, "tail_byte_length": len(raw),
            "header_base64": base64.b64encode(header).decode("ascii"),
            "header_sha256": _hash(header),
            "tail_base64": base64.b64encode(raw).decode("ascii"),
            "tail_sha256": _hash(raw),
            "captured_bytes_sha256": _hash(header + raw)}


def _parse_epoch(value: str) -> float:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("bar_timezone_required")
    epoch = parsed.timestamp()
    if not math.isfinite(epoch) or epoch % 60 != 0:
        raise ValueError("bar_start_not_minute_aligned")
    return epoch


def _parse_source(source: dict, pair: str, observed: float) -> dict[float, float]:
    header = base64.b64decode(source["header_base64"], validate=True)
    tail = base64.b64decode(source["tail_base64"], validate=True)
    if (_hash(header) != source["header_sha256"] or _hash(tail) != source["tail_sha256"]
            or _hash(header + tail) != source["captured_bytes_sha256"]):
        raise ValueError("captured_source_hash_mismatch")
    fields = next(csv.reader([header.decode("utf-8-sig").strip()]))
    if len(set(fields)) != len(fields) or "close" not in fields:
        raise ValueError("invalid_csv_columns")
    time_fields = [name for name in ("datetime", "time") if name in fields]
    if not time_fields:
        raise ValueError("missing_bar_timestamp")
    rows: dict[float, float] = {}
    previous = None
    for line in tail.decode("utf-8").splitlines():
        cells = next(csv.reader([line], strict=True))
        if len(cells) != len(fields):
            raise ValueError("invalid_csv_row_width")
        row = dict(zip(fields, cells))
        clocks = [_parse_epoch(row[field]) for field in time_fields]
        if len(set(clocks)) != 1:
            raise ValueError("conflicting_bar_timestamps")
        epoch = clocks[0]
        if previous is not None and epoch <= previous:
            raise ValueError("duplicate_or_unordered_bar")
        previous = epoch
        if epoch + 60 > observed:
            raise ValueError("bar_not_complete_at_observation")
        if "instrument" in row and row["instrument"] != pair:
            raise ValueError("wrong_bar_instrument")
        if "granularity" in row and row["granularity"] != "M1":
            raise ValueError("wrong_bar_granularity")
        if "complete" in row and row["complete"].lower() not in ("true", "1"):
            raise ValueError("incomplete_bar_flag")
        close = float(row["close"])
        if not math.isfinite(close) or close <= 0:
            raise ValueError("invalid_bar_close")
        rows[epoch] = close
    if not rows:
        raise ValueError("no_captured_bars")
    return rows


def _model_window(rows: dict[str, dict[float, float]], observed: float) -> tuple[list[float], dict]:
    common = sorted(set.intersection(*(set(rows[pair]) for pair in PAIRS)))
    suffix: list[float] = []
    for epoch in reversed(common):
        if suffix and suffix[-1] - epoch != 60:
            break
        suffix.append(epoch)
        if len(suffix) == MAX_MODEL_ROWS:
            break
    suffix.reverse()
    if len(suffix) < MIN_MODEL_ROWS:
        raise ValueError(f"contiguous_common_warmup:{len(suffix)}<{MIN_MODEL_ROWS}")
    if observed - (suffix[-1] + 60) > MAX_COMMON_BAR_AGE_SEC:
        raise ValueError("stale_common_bar_window")
    return suffix, {pair: [rows[pair][epoch] for epoch in suffix] for pair in PAIRS}


def capture_inputs(candle_root: Path, *, clock: Callable[[], float] = time.time) -> dict:
    """Capture one observed-now snapshot, with explicit abstention on bad input.

    Exact bounded CSV evidence is retained. Past input bars are NOT evidence
    that a forecast or label was available historically. The latest bar close
    conservatively bounds every label endpoint used by the numerical models.
    """
    result = {"schema_version": SCHEMA, "status": "abstain", "reasons": [],
              "pairs": list(PAIRS), "output_instrument": "EUR_USD", "series": {},
              "common_start_epochs": [], "sources": {},
              "model_source_sha256": NUMERICAL_SOURCE_SHA256,
              "dependency_versions": dependency_versions(),
              "availability_semantics": "conservative_consumer_observation_after_stable_read",
              "research_only": True, "account_eligible": False, "can_place_orders": False}
    try:
        begun = _clock(clock)
        previous_observed = begun
        for pair in PAIRS:
            try:
                source = _read_tail(Path(candle_root) / f"{pair}_M1.csv")
                source["first_observed_epoch"] = _clock(clock)
                if source["first_observed_epoch"] < previous_observed:
                    raise ValueError("observation_clock_moved_backwards")
                previous_observed = source["first_observed_epoch"]
                result["sources"][pair] = source
            except (OSError, ValueError) as exc:
                result["reasons"].append(f"{pair}:{type(exc).__name__}:{exc.args[0] if isinstance(exc, ValueError) else 'source_unreadable'}")
        observed = _clock(clock)
        result["first_observed_epoch"] = observed
        if observed < begun or any(s["first_observed_epoch"] > observed for s in result["sources"].values()):
            raise ValueError("observation_clock_moved_backwards")
        if result["reasons"]:
            return _seal(result)
        rows = {pair: _parse_source(result["sources"][pair], pair, result["sources"][pair]["first_observed_epoch"])
                for pair in PAIRS}
        epochs, series = _model_window(rows, observed)
        result.update(status="ready", series=series, common_start_epochs=epochs,
                      max_bar_close_epoch=epochs[-1] + 60,
                      common_bar_age_sec=observed - epochs[-1] - 60,
                      pooled_training_rows=7 * len(range(60, len(epochs) - 60, 5)),
                      training_label_maturity_max_epoch=epochs[-1] + 60,
                      training_labels_available_max_epoch=observed,
                      feature_cutoff_epoch=epochs[-1] + 60,
                      features_available_epoch=observed)
    except (OSError, ValueError, KeyError, UnicodeError, csv.Error) as exc:
        result["reasons"].append(f"{type(exc).__name__}:{exc}")
    return _seal(result)


def _seal(result: dict) -> dict:
    result["source_capture_sha256"] = _digest(result)
    return result


def _verified_series(capture: dict) -> dict:
    if capture.get("schema_version") != SCHEMA or capture.get("status") != "ready":
        raise ValueError("input_not_ready")
    if _digest({k: v for k, v in capture.items() if k != "source_capture_sha256"}) != capture.get("source_capture_sha256"):
        raise ValueError("input_capture_hash_mismatch")
    if capture.get("model_source_sha256") != NUMERICAL_SOURCE_SHA256 or capture.get("dependency_versions") != dependency_versions():
        raise ValueError("model_or_dependency_binding_changed")
    if capture.get("pairs") != list(PAIRS) or set(capture.get("sources", {})) != set(PAIRS):
        raise ValueError("fixed_input_universe_required")
    observed = capture["first_observed_epoch"]
    rows = {}
    for pair in PAIRS:
        source = capture["sources"][pair]
        if source["first_observed_epoch"] > observed:
            raise ValueError("source_observed_after_capture")
        rows[pair] = _parse_source(source, pair, source["first_observed_epoch"])
    epochs, series = _model_window(rows, observed)
    if epochs != capture["common_start_epochs"] or series != capture["series"]:
        raise ValueError("derived_model_window_mismatch")
    if any(capture.get(name) != epochs[-1] + 60 for name in (
            "max_bar_close_epoch", "training_label_maturity_max_epoch", "feature_cutoff_epoch")):
        raise ValueError("bar_maturity_binding_mismatch")
    if any(capture.get(name) != observed for name in (
            "training_labels_available_max_epoch", "features_available_epoch")):
        raise ValueError("input_availability_binding_mismatch")
    if capture.get("pooled_training_rows") != 7 * len(range(60, len(epochs) - 60, 5)):
        raise ValueError("pooled_training_count_mismatch")
    return series


def _numerical_module():
    # Verify actual source bytes, not legacy code_hash(), which intentionally
    # returns an older frozen semantic identifier.
    path = Path(__file__).with_name("oanda_proof_shadow_predictors.py")
    if _hash(path.read_bytes()) != NUMERICAL_SOURCE_SHA256:
        raise ValueError("numerical_source_binding_changed")
    spec = importlib.util.spec_from_file_location("_causal_bound_numerical_models", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if _hash(path.read_bytes()) != NUMERICAL_SOURCE_SHA256:
        raise ValueError("numerical_source_changed_during_import")
    return module


def validate_capture(capture: dict) -> None:
    """Verify captured bytes, clocks and derived rows without running models."""
    _verified_series(capture)


def compute_predictions(capture: dict, *, clock: Callable[[], float] = time.time) -> dict:
    """Return all four fixed numerical EURUSD predictions, or one abstention.

    Issuance and publication clocks belong to the caller AFTER this function.
    This binding describes source, inputs and dependencies, not saved weights.
    """
    output = {"status": "abstain", "reasons": [], "predictions": {},
              "model_source_sha256": NUMERICAL_SOURCE_SHA256,
              "source_capture_sha256": capture.get("source_capture_sha256"),
              "dependency_versions": dependency_versions()}
    try:
        series = _verified_series(capture)
        import numpy as np
        module = _numerical_module()
        arrays = {pair: np.asarray(series[pair], dtype=float) for pair in PAIRS}
        pips = {pair: 0.01 if pair.endswith("_JPY") else 0.0001 for pair in PAIRS}
        functions = (module.ridge_predictions, module.pooled_tabular_predictions,
                     module.graph_predictions, module.state_space_predictions)
        for family, function in zip(FAMILIES, functions):
            predictions = function(arrays, pips)
            if "EUR_USD" not in predictions:
                raise ValueError("family_prediction_unavailable:" + family)
            expected, probability, diagnostics = predictions["EUR_USD"]
            if not math.isfinite(expected) or not math.isfinite(probability) or not 0 <= probability <= 1:
                raise ValueError("invalid_family_prediction:" + family)
            output["predictions"][family] = {"expected_signed_pips": float(expected),
                "probability_up": float(probability),
                "side": 1 if expected >= 0 else -1, "diagnostics": diagnostics}
        completed = _clock(clock)
        if completed < capture["first_observed_epoch"]:
            raise ValueError("computation_clock_moved_backwards")
        output["computed_epoch"] = completed
        output["status"] = "ready"
    except Exception as exc:
        # Numerical/library failures produce an explicit whole-batch abstention.
        # Interrupts and process termination remain outside Exception.
        output["predictions"] = {}
        output["reasons"].append(f"{type(exc).__name__}:{exc}")
    return output
