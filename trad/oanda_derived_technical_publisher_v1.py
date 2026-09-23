"""Publish a current all-68 derived technical feature view.

This publisher keeps the immutable rolling M1 store unchanged. It reads the
native completed candle files, computes the separately named derived M1/M5
feature families, and writes one bounded JSON snapshot for downstream research.
Existing fitted model weights are not compatible with this schema.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import io
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time

import numpy as np

import oanda_derived_technical_features_v1 as derived
import oanda_feature_candle_inputs_v2 as candles

ROOT = Path(__file__).resolve().parent
SCHEMA = "derived_technical_publisher_v1_20260916"
DEFAULT_CONFIG = ROOT / "config/derived_technical_features_v1_20260916.json"
DEFAULT_OBSERVATION_CONFIG = ROOT / "config/rolling_technical_dataset_v1_20260915.json"
DEFAULT_OUTPUT = ROOT / "data/oanda_training_manager/state/all68_derived_technical_features_v1.json"
FLAGS = {
    "research_only": True,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "model_training_performed": False,
}
TIMEFRAME_SECONDS = {"M1": 60, "M5": 300}
MINIMAL_SUFFIXES = (
    "historical_spread_pips",
    "tick_activity_log1p",
    "tick_activity",
    "bar_range_pips",
    "bar_body_pips",
    "utc_hour_sin",
    "utc_hour_cos",
    "utc_weekday_sin",
    "utc_weekday_cos",
)
MOVEMENT_SUFFIXES = ("return_1_pips", "return_1_bps")


def utc(epoch=None):
    return datetime.fromtimestamp(time.time() if epoch is None else epoch, timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp." + str(os.getpid()))
    temp.write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temp, path)


def _no_reparse(path):
    path = Path(path)
    return not any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (path, *path.parents))


def _number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("finite_" + name + "_required")
    return float(value)


def _load_observation_config(path):
    path = Path(path).resolve()
    body = json.loads(path.read_text(encoding="utf-8"))
    pairs = body.get("pairs")
    if body.get("schema") != "rolling_technical_dataset_v1_20260915" or not isinstance(pairs, dict) or len(pairs) != 68:
        raise ValueError("explicit_observation_68_pair_config_required")
    for pair, pip in pairs.items():
        if not isinstance(pair, str) or len(pair) != 7 or pair[3] != "_" or not 0 < _number(pip, "pip") <= 0.1:
            raise ValueError("valid_pair_pip_required")
    return body, {"path": str(path), "sha256": sha(path)}


def read_config(path=DEFAULT_CONFIG):
    path = Path(path).resolve()
    config = json.loads(path.read_text(encoding="utf-8"))
    if config.get("schema") != SCHEMA or any(config.get(key) != value for key, value in FLAGS.items()):
        raise ValueError("derived_research_only_config_required")
    timeframe = config.get("timeframe")
    if timeframe not in TIMEFRAME_SECONDS:
        raise ValueError("explicit_m1_or_m5_timeframe_required")
    observation_config, observation_reference = _load_observation_config(config.get("observation_config", DEFAULT_OBSERVATION_CONFIG))
    if config.get("observation_config_sha256") != observation_reference["sha256"]:
        raise ValueError("observation_config_binding_changed")
    if config.get("pairs") != observation_config["pairs"]:
        raise ValueError("pair_metadata_binding_changed")
    for name, expected in config.get("source_bindings", {}).items():
        if Path(name).name != name or sha(ROOT / name) != expected:
            raise ValueError("derived_source_binding_changed:" + name)
    if set(config.get("source_bindings", {})) != {"oanda_derived_technical_features_v1.py", "oanda_feature_candle_inputs_v2.py"}:
        raise ValueError("complete_derived_source_bindings_required")
    candle_root = Path(config["candle_root"]).resolve()
    output = Path(config.get("output_path", DEFAULT_OUTPUT)).resolve()
    if not _no_reparse(candle_root) or not _no_reparse(output.parent):
        raise ValueError("derived_reparse_path_refused")
    if not candle_root.is_relative_to((ROOT / "data").resolve()):
        raise ValueError("derived_candle_root_must_be_project_data")
    state_root = (ROOT / "data/oanda_training_manager/state").resolve()
    if output.parent != state_root:
        raise ValueError("derived_output_must_be_state_file")
    max_age = _number(config.get("maximum_anchor_age_seconds"), "maximum_anchor_age_seconds")
    max_actual_lag = _number(config.get("maximum_actual_lag_seconds"), "maximum_actual_lag_seconds")
    if not TIMEFRAME_SECONDS[timeframe] <= max_age <= 3600:
        raise ValueError("bounded_anchor_age_required")
    if not TIMEFRAME_SECONDS[timeframe] <= max_actual_lag <= 3600:
        raise ValueError("bounded_actual_lag_required")
    fill = config.get("max_consecutive_fill")
    if type(fill) is not int or not 0 <= fill <= derived.MAX_CONSECUTIVE_FILL:
        raise ValueError("bounded_consecutive_fill_required")
    neutral_shape = config.get("neutralize_undefined_shape_fallbacks", False)
    if type(neutral_shape) is not bool:
        raise ValueError("boolean_neutral_shape_fallback_required")
    return config, observation_reference


def rows_to_arrays(rows):
    result = {"time": [], "open": [], "high": [], "low": [], "close": [], "bid_close": [], "ask_close": [], "volume": []}
    for row in rows:
        result["time"].append(int(candles.stamp(row["time"]).timestamp()))
        result["open"].append(float(row["mid"]["o"]))
        result["high"].append(float(row["mid"]["h"]))
        result["low"].append(float(row["mid"]["l"]))
        result["close"].append(float(row["mid"]["c"]))
        result["bid_close"].append(float(row["bid"]["c"]))
        result["ask_close"].append(float(row["ask"]["c"]))
        result["volume"].append(float(row["volume"]))
    return {name: np.asarray(values, dtype=np.int64 if name == "time" else np.float64) for name, values in result.items()}



def read_tail_with_internal_gaps(path, pair, timeframe, *, observed_utc):
    """Read the same bounded source tail but retain short intraday gaps for causal fill.

    The shared candle reader returns only the contiguous suffix after an unknown
    intraday gap. That is correct for consumers that require strict continuity,
    but this derived publisher has its own explicit short-gap assumption masks.
    Keep the bounded validated tail here so the derived kernel can mark short
    gaps as imputed instead of discarding all prior history.
    """
    path = Path(path).absolute()
    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (path, *path.parents)):
        raise ValueError("candle_source_link_refused")
    before = path.stat()
    with path.open("rb") as handle:
        header = handle.readline(4097)
        if len(header) > 4096 or not header.endswith(b"\n"):
            raise ValueError("candle_header_bound")
        offset = max(len(header), before.st_size - candles.MAX_BYTES)
        handle.seek(offset)
        raw = handle.read(candles.MAX_BYTES + 1)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or len(raw) > candles.MAX_BYTES:
        raise ValueError("candle_source_changed_or_exceeded_bound")
    if raw and not raw.endswith(b"\n"):
        raise ValueError("candle_partial_last_record")
    if offset > len(header):
        raw = raw.split(b"\n", 1)[-1]
    fields = next(csv.reader([header.decode("utf-8-sig").strip()]))
    if len(fields) != len(set(fields)):
        raise ValueError("duplicate_candle_columns")
    parsed = []
    observed = candles.stamp(observed_utc)
    seconds = candles.SECONDS[timeframe]
    for source in list(csv.DictReader(io.StringIO(raw.decode("utf-8")), fieldnames=fields))[-candles.MAX_ROWS:]:
        if None in source or any(v is None for v in source.values()) or source.get("instrument") != pair or source.get("granularity") != timeframe:
            raise ValueError("candle_row_identity_or_width_invalid")
        times = [candles.stamp(source[k]) for k in ("time", "datetime") if k in source]
        if not times or len(set(times)) != 1:
            raise ValueError("candle_original_clock_invalid")
        if "complete" in source and source["complete"].lower() not in ("true", "1"):
            raise ValueError("explicit_complete_candle_required")
        row = {"time": times[0].isoformat(), "complete": True, "volume": float(source["volume"])}
        for side, prefix in (("mid", ""), ("bid", "bid_"), ("ask", "ask_")):
            row[side] = {short: float(source[prefix + long]) for short, long in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"))}
        candles.validate_candle(row, seconds, observed)
        if parsed and candles.stamp(parsed[-1]["time"]) >= times[0]:
            raise ValueError("candle_duplicate_or_unsorted")
        parsed.append(row)
    suffix, suffix_receipt = candles.supported_suffix(parsed, seconds, allow_weekend=timeframe != "M1")
    return parsed, {"status": "available", "source_name": path.name, "source_path": str(path),
        "source_bytes": len(header) + len(raw), "source_tail_sha256": hashlib.sha256(header + raw).hexdigest(),
        "source_read_completed_utc": observed.isoformat(), "parsed_rows": len(parsed),
        "retained_rows": len(parsed), "contiguous_suffix_rows": len(suffix),
        "complete_basis": "explicit_if_present_and_original_bar_end_before_read",
        "feature_row_semantics": "bounded_validated_tail_with_intraday_gaps_retained_for_derived_causal_fill",
        **suffix_receipt}

def _read_histories(config, now):
    root = Path(config["candle_root"])
    timeframe = config["timeframe"]
    histories, receipts, errors = {}, {}, {}
    observed = utc(now)
    for pair in sorted(config["pairs"]):
        try:
            rows, receipt = read_tail_with_internal_gaps(root / f"{pair}_{timeframe}.csv", pair, timeframe, observed_utc=observed)
            histories[pair] = rows
            receipts[pair] = receipt
        except (OSError, ValueError, TypeError, KeyError) as exc:
            errors[pair] = type(exc).__name__ + ":" + str(exc)[:200]
    return histories, receipts, errors


def _choose_anchor(histories, step, now, maximum_anchor_age):
    eligible = []
    for rows in histories.values():
        latest = None
        for row in rows:
            end = candles.stamp(row["time"]).timestamp() + step
            if 0 <= now - end <= maximum_anchor_age:
                latest = end if latest is None else max(latest, end)
        if latest is not None:
            eligible.append(latest)
    if not eligible:
        return math.floor(now / step) * step
    counts = Counter(eligible)
    return int(max(counts, key=lambda item: (counts[item], item)))


def _quality_summary(view):
    finite = sum(value is not None for value in view["values"].values())
    buckets = Counter()
    for item in view["feature_quality"].values():
        if item["missing_grid_count"]:
            buckets["missing_grid_support"] += 1
        elif item["imputed_count"]:
            buckets["partly_imputed_window"] += 1
        else:
            buckets["observed_window"] += 1
    return {"finite_count": finite, "expected_count": len(view["values"]), "quality_counts": dict(buckets)}


def minimal_feature_names(timeframe):
    prefix = timeframe.lower() + "_derived__"
    return tuple(prefix + suffix for suffix in MINIMAL_SUFFIXES)


def movement_feature_names(timeframe):
    prefix = timeframe.lower() + "_derived__"
    return tuple(prefix + suffix for suffix in MOVEMENT_SUFFIXES)


def _pair_status(view, anchor, max_actual_lag):
    if view["bar_end_epoch"] is None or view["latest_actual_bar_end_epoch"] is None:
        return "unavailable"
    if view["trailing_unfilled_bars"]:
        return "stale_gap"
    if anchor - view["latest_actual_bar_end_epoch"] > max_actual_lag:
        return "stale_actual"
    if view["imputed"]:
        return "derived_current"
    return "observed_current"


def build_report(config, histories, receipts, errors, *, now=None, observation_reference=None):
    now = time.time() if now is None else _number(now, "now")
    step = TIMEFRAME_SECONDS[config["timeframe"]]
    anchor = _choose_anchor(histories, step, now, config["maximum_anchor_age_seconds"])
    pairs, statuses = {}, Counter()
    minimal_names = minimal_feature_names(config["timeframe"])
    movement_names = movement_feature_names(config["timeframe"])
    for pair, pip in sorted(config["pairs"].items()):
        try:
            source_rows = [row for row in histories.get(pair, []) if candles.stamp(row["time"]).timestamp() + step <= anchor]
            data = rows_to_arrays(source_rows)
            view = derived.latest_features(
                data,
                pair,
                pip,
                bar_seconds=step,
                as_of_epoch=anchor,
                max_consecutive_fill=config["max_consecutive_fill"],
                neutralize_undefined_shape_fallbacks=config.get("neutralize_undefined_shape_fallbacks", False),
            )
            status = _pair_status(view, anchor, config["maximum_actual_lag_seconds"])
            summary = _quality_summary(view)
            view["publication_epoch"] = now
            view["publication_utc"] = utc(now)
            pairs[pair] = {
                "status": status,
                "readable": status != "unavailable",
                "minimal_feature_ready": all(view["values"].get(name) is not None for name in minimal_names),
                "movement_feature_ready": all(view["values"].get(name) is not None for name in movement_names),
                "feature_family": view["metadata"]["schema_version"],
                "feature_timeframe": view["metadata"]["actual_bar_seconds"],
                "feature_count": summary["expected_count"],
                "finite_count": summary["finite_count"],
                "quality_counts": summary["quality_counts"],
                "bar_start_epoch": view["bar_start_epoch"],
                "bar_end_epoch": view["bar_end_epoch"],
                "latest_actual_bar_end_epoch": view["latest_actual_bar_end_epoch"],
                "latest_actual_bar_age_seconds": view["latest_actual_bar_age_seconds"],
                "trailing_unfilled_bars": view["trailing_unfilled_bars"],
                "observed": view["observed"],
                "imputed": view["imputed"],
                "consecutive_imputed_bars": view["consecutive_imputed_bars"],
                "values": view["values"],
                "value_fallbacks": view.get("value_fallbacks", {}),
                "feature_quality": view["feature_quality"],
                "source_receipt": receipts.get(pair),
                "source_error": errors.get(pair),
            }
        except (ValueError, TypeError, KeyError, OSError) as exc:
            status = "unavailable"
            pairs[pair] = {
                "status": status,
                "readable": False,
                "minimal_feature_ready": False,
                "movement_feature_ready": False,
                "reason": type(exc).__name__ + ":" + str(exc)[:240],
                "source_error": errors.get(pair),
            }
        statuses[status] += 1
    registry = derived.feature_registry(step)
    report = {
        "schema": SCHEMA,
        "schema_version": SCHEMA,
        "generated_utc": utc(now),
        "generated_epoch": now,
        "analysis_anchor_utc": utc(anchor),
        "analysis_anchor_epoch": anchor,
        "analysis_age_seconds": now - anchor,
        "timeframe": config["timeframe"],
        "bar_seconds": step,
        "coverage": {
            "registered_pairs": len(config["pairs"]),
            "histories_read": len(histories),
            "status_counts": dict(statuses),
            "current_pairs": statuses["observed_current"] + statuses["derived_current"],
            "fully_finite_pairs": sum(row.get("finite_count") == len(registry) for row in pairs.values()),
            "minimal_feature_count": len(minimal_names),
            "minimal_feature_finite_pairs": sum(row.get("minimal_feature_ready") is True for row in pairs.values()),
            "minimal_feature_current_pairs": sum(row.get("minimal_feature_ready") is True and row["status"] in ("observed_current", "derived_current") for row in pairs.values()),
            "movement_feature_count": len(movement_names),
            "movement_feature_finite_pairs": sum(row.get("movement_feature_ready") is True for row in pairs.values()),
            "movement_feature_current_pairs": sum(row.get("movement_feature_ready") is True and row["status"] in ("observed_current", "derived_current") for row in pairs.values()),
            "source_errors": len(errors),
        },
        "metadata": {
            **FLAGS,
            "feature_schema": derived.SCHEMA,
            "feature_registry_sha256": hashlib.sha256(encoded(registry)).hexdigest(),
            "minimal_feature_names": list(minimal_names),
            "movement_feature_names": list(movement_names),
            "source_bindings": dict(config["source_bindings"]),
            "observation_config": observation_reference,
            "original_model_weight_compatibility": False,
            "publication_scope": "Derived technical features only; no forecasts, fitting, orders or promotion.",
            "gap_policy": "Short missing candle tails use strictly prior flat assumptions up to the configured bound; longer gaps stay explicit.",
        },
        "pairs": pairs,
    }
    report["publication_generation"] = hashlib.sha256(encoded({
        "schema": SCHEMA,
        "schema_version": SCHEMA,
        "anchor": anchor,
        "pairs": {pair: {"status": row["status"], "finite": row.get("finite_count"),
                         "minimal": row.get("minimal_feature_ready"),
                         "movement": row.get("movement_feature_ready")} for pair, row in pairs.items()},
    })).hexdigest()
    return report


def capture(config_path=DEFAULT_CONFIG, *, now=None):
    config, observation_reference = read_config(config_path)
    now = time.time() if now is None else now
    histories, receipts, errors = _read_histories(config, now)
    return build_report(config, histories, receipts, errors, now=now, observation_reference=observation_reference)


def publish_once(config, output, observation_reference):
    now = time.time()
    report = build_report(config, *_read_histories(config, now), now=now, observation_reference=observation_reference)
    atomic_json(output, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--once", action="store_true", help="Run one publication cycle, the default.")
    parser.add_argument("--interval-sec", type=int, default=60)
    parser.add_argument("--duration-sec", type=int, default=0, help="Loop for this many seconds; zero means one cycle.")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if not 15 <= args.interval_sec <= 300:
        raise ValueError("bounded_interval_required")
    if not 0 <= args.duration_sec <= 604800:
        raise ValueError("bounded_duration_required")
    config, observation_reference = read_config(args.config)
    output = args.output.resolve() if args.output else Path(config["output_path"]).resolve()
    state_root = (ROOT / "data/oanda_training_manager/state").resolve()
    if output.parent != state_root or not _no_reparse(output.parent):
        raise ValueError("derived_output_must_be_state_file")
    started = time.monotonic()
    while True:
        cycle = time.monotonic()
        report = publish_once(config, output, observation_reference)
        print(json.dumps({"output": str(output), "coverage": report["coverage"],
                          "analysis_anchor_utc": report["analysis_anchor_utc"],
                          "publication_generation": report["publication_generation"]},
                         sort_keys=True, separators=(",", ":"), allow_nan=False), flush=True)
        if args.once or args.duration_sec == 0 or time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1, args.interval_sec - (time.monotonic() - cycle)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
