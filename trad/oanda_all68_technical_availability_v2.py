"""Read-only availability of the 68-pair rolling technical feed.

This report explains original published features; it never computes a feature,
repairs a candle, promotes an observation, or treats an absent value as zero.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
import zlib

import oanda_rolling_technical_features_v1 as kernel
import oanda_rolling_technical_continuity_v2 as continuity

ROOT = Path(__file__).resolve().parent
SCHEMA = "all68_technical_availability_v2_20260930"
DEFAULT_CONFIG = ROOT / "config/rolling_technical_dataset_runtime_20260930.json"
DEFAULT_OPERATIONS_CONFIG = ROOT / "config/rolling_technical_operations_runtime_20260930.json"
ORIGINAL_CONFIG_SHA256 = "9a60230d41ad5b3ea140cdfa58ee2fb9bc00e727b7ad01d56d641a58c97b5de9"
OPERATIONS_CONFIG_SHA256 = "aac2ed345c95a9aa65451d249c5924e6927e43417cd99fbaaec753af3c57e038"
IMPORTED_SOURCE_BINDINGS = {
    "oanda_rolling_technical_features_v1.py": "074a7b4fc138ef25a1e99b503ea693e0c31bc2e51a2e8f3753aab7556a6fb657",
    "oanda_rolling_technical_inputs_v1.py": "0609b9562e4a528a5e44269af0135be5520418ff772305a5ee302ed3ee38f8cf",
    "oanda_rolling_technical_continuity_v2.py": "e0a1fcff4bfec76021c360444702fdbb953a6e1ddfd062db343c0c8fb9730f64",
}
STATE = ROOT / "data/oanda_training_manager/state"
DEFAULT_OUTPUT = STATE / "all68_technical_availability_v2.json"
DEFAULT_QUOTES = STATE / "practice_007_market_quotes_v1.json"
DEFAULT_HEARTBEAT = STATE / "practice_007_quote_stream_heartbeat_v1.json"
MAX_ENVELOPE_BYTES = 32 * 1024**2
MAX_SMALL_BYTES = 4 * 1024**2
ZERO_RANGE_FEATURES = frozenset(("m1__body_to_range", "m1__upper_wick_fraction", "m1__lower_wick_fraction"))
FLAGS = {"research_only": True, "can_place_orders": False, "can_authorize": False,
         "can_promote": False, "model_training_performed": False}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("finite_number_required")
    return float(value)


def epoch(value):
    if not isinstance(value, str):
        raise ValueError("aware_original_timestamp_required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("aware_original_timestamp_required")
    return number(parsed.timestamp())


def failure(exc):
    return type(exc).__name__ + ":" + str(exc)[:240]


def read_json(path, limit):
    with Path(path).open("rb") as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("source_byte_bound")
    def invalid(value):
        raise ValueError("nonfinite_json_constant:" + value)
    value = json.loads(raw, parse_constant=invalid)
    if not isinstance(value, dict):
        raise ValueError("source_object_required")
    return value, {"path": str(Path(path).absolute()), "sha256": digest(raw), "bytes": len(raw)}


def read_config(path):
    config, reference = read_json(path, 65536)
    pairs = config.get("pairs")
    if (config.get("schema") != "rolling_technical_dataset_v1_20260915" or
            not isinstance(pairs, dict) or len(pairs) != 68 or
            any(not re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", pair) for pair in pairs)):
        raise ValueError("explicit_original_68_pair_config_required")
    if config.get("research_only") is not True or config.get("can_place_orders") is not False:
        raise ValueError("research_only_config_required")
    if not 1 <= number(config.get("maximum_bar_age_seconds")) <= 600:
        raise ValueError("bounded_original_bar_age_required")
    for value in pairs.values():
        if not 0 < number(value) <= .1:
            raise ValueError("valid_original_pip_required")
    return config, reference


def verify_dependencies(config, reference, operations_path=DEFAULT_OPERATIONS_CONFIG):
    operations, operations_reference = read_json(operations_path, 65536)
    if operations_reference["sha256"] != OPERATIONS_CONFIG_SHA256:
        raise ValueError("frozen_operations_config_binding_changed")
    if reference is None or reference["sha256"] != ORIGINAL_CONFIG_SHA256:
        raise ValueError("frozen_observation_config_binding_required")
    current_config, current_reference = read_json(reference["path"], 65536)
    if current_reference["sha256"] != reference["sha256"] or current_config != config:
        raise ValueError("observation_config_changed")
    if operations.get("observation_config_sha256") != reference["sha256"]:
        raise ValueError("operations_observation_config_mismatch")
    for name, expected in operations["source_bindings"].items():
        if Path(name).name != name or digest((ROOT/name).read_bytes()) != expected:
            raise ValueError("frozen_source_binding_changed:" + name)
    for name, expected in IMPORTED_SOURCE_BINDINGS.items():
        if operations["source_bindings"].get(name) != expected:
            raise ValueError("imported_helper_binding_changed:" + name)
    for module in (kernel, continuity, continuity.inputs):
        if Path(module.__file__).resolve() != (ROOT/Path(module.__file__).name).resolve():
            raise ValueError("imported_helper_path_changed")
    return operations, {"operations_config": operations_reference,
        "imported_sources": dict(IMPORTED_SOURCE_BINDINGS),
        "report_source_sha256": digest(Path(__file__).read_bytes()),
        "feature_registry_sha256": digest(encoded(kernel.feature_registry()))}


def _unpack(raw, limit=131072):
    decoder = zlib.decompressobj()
    result = decoder.decompress(raw, limit + 1)
    if len(result) > limit or not decoder.eof or decoder.unused_data:
        raise ValueError("observation_blob_bound_or_shape")
    return result


def read_store_evidence(path, pairs, envelope, *, config=None, operations=None):
    """One indexed original observation/bar/receipt join per configured pair."""
    result, errors = {}, {}
    db = sqlite3.connect(Path(path).absolute().as_uri() + "?mode=ro", uri=True, timeout=1)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA query_only=ON")
        if config is not None:
            row = db.execute("SELECT value FROM metadata WHERE key='contract'").fetchone()
            if row is None or len(row[0]) > 1024**2:
                raise ValueError("original_store_contract_required")
            contract = json.loads(row[0])
            body = contract.get("contract", {})
            if (contract.get("schema") != config["schema"] or body.get("pairs") != config["pairs"] or
                    body.get("feature_registry") != kernel.feature_registry() or
                    body.get("feature_names") != [item["name"] for item in kernel.feature_registry()] or
                    body.get("source_bindings") != config["source_bindings"]):
                raise ValueError("original_store_registry_or_contract_mismatch")
            runtime_id = envelope.get("operations_runtime_id")
            if not isinstance(runtime_id, str) or not re.fullmatch(r"[0-9a-f]{64}", runtime_id):
                raise ValueError("original_operations_runtime_id_required")
            runtime = db.execute("SELECT value FROM metadata WHERE key=?", ("operations_run:"+runtime_id,)).fetchone()
            if runtime is None or len(runtime[0]) > 65536 or digest(bytes(runtime[0])) != runtime_id:
                raise ValueError("original_operations_runtime_hash_mismatch")
            runtime_value = json.loads(runtime[0])
            if (runtime_value.get("schema") != operations["schema"] or
                    runtime_value.get("config_sha256") != OPERATIONS_CONFIG_SHA256 or
                    runtime_value.get("source_bindings") != operations["source_bindings"] or
                    envelope.get("operations_schema") != operations["schema"]):
                raise ValueError("original_operations_runtime_binding_changed")
        for pair in pairs:
            try:
                state = envelope.get("pairs", {}).get(pair, {})
                availability = state.get("availability", {})
                original = state.get("observation") or {}
                at = original.get("bar_start_epoch")
                if at is None and availability.get("bar_start_utc"):
                    at = epoch(availability["bar_start_utc"])
                if at is None:
                    continue
                if number(at) % 60:
                    raise ValueError("unaligned_original_minute")
                row = db.execute("""SELECT o.t,o.feature_hash,o.values_blob,o.feature_count,
                    o.available_count,o.published,b.body AS bar_body,b.input_hash,
                    b.first_observed,b.receipt_id,r.pair AS receipt_pair,r.observed,r.body AS receipt_body
                    FROM observations o JOIN bars b ON b.pair=o.pair AND b.t=o.t
                    JOIN receipts r ON r.id=b.receipt_id
                    WHERE o.pair=? AND o.t=? AND o.published>0""", (pair, int(at))).fetchone()
                if row is None:
                    raise ValueError("original_observation_receipt_not_found")
                raw = bytes(row["receipt_body"])
                bar_raw = bytes(row["bar_body"])
                if len(raw) > 65536 or len(bar_raw) > 16384:
                    raise ValueError("original_receipt_or_bar_bound")
                if digest(raw) != row["receipt_id"] or digest(bar_raw) != row["input_hash"]:
                    raise ValueError("original_receipt_or_input_hash_mismatch")
                values_raw = _unpack(row["values_blob"])
                if digest(values_raw) != row["feature_hash"]:
                    raise ValueError("original_feature_hash_mismatch")
                receipt = json.loads(raw)
                if row["receipt_pair"] != pair or receipt.get("instrument") != pair:
                    raise ValueError("original_receipt_pair_mismatch")
                if not (at + 60 <= number(row["first_observed"]) <= number(row["published"])):
                    raise ValueError("original_receipt_clock_order")
                if number(receipt["observed_epoch"]) != row["first_observed"] or row["observed"] != row["first_observed"]:
                    raise ValueError("original_receipt_observation_clock_mismatch")
                result[pair] = {"t": row["t"], "feature_hash": row["feature_hash"],
                    "values": json.loads(values_raw), "feature_count": row["feature_count"],
                    "available_count": row["available_count"], "published_epoch": row["published"],
                    "bar": json.loads(bar_raw), "input_hash": row["input_hash"],
                    "receipt_id": row["receipt_id"], "first_observed_epoch": row["first_observed"],
                    "receipt": receipt}
            except (ValueError, TypeError, KeyError, AttributeError, OSError, zlib.error) as exc:
                errors[pair] = failure(exc)
    finally:
        db.close()
    return result, errors


def _quote(pair, quotes, heartbeat, now, maximum_quote_age):
    result = {"tradeable": None, "status": "unavailable", "age_seconds": None,
              "connected": None, "current_connection": False, "reason": None}
    try:
        if quotes is None or heartbeat is None:
            raise ValueError("quote_snapshot_or_heartbeat_unreadable")
        snapshot_epoch = epoch(quotes["generated_utc"])
        heartbeat_epoch = epoch(heartbeat["updated_at"])
        stream = heartbeat["details"]["stream"]
        quote = quotes["quotes"][pair]
        quote_epoch = epoch(quote["time"])
        if not 0 <= now - snapshot_epoch <= maximum_quote_age:
            raise ValueError("quote_snapshot_stale_or_future")
        if not 0 <= now - heartbeat_epoch <= maximum_quote_age:
            raise ValueError("quote_heartbeat_stale_or_future")
        if quote_epoch > now or quote_epoch > snapshot_epoch:
            raise ValueError("underlying_quote_clock_future")
        bid, ask = number(quote["bid"]), number(quote["ask"])
        if not 0 < bid <= ask:
            raise ValueError("invalid_original_bid_ask")
        tradeable = quote.get("tradeable")
        if type(tradeable) is not bool:
            raise ValueError("explicit_tradeability_missing")
        generation = quotes.get("connection_generation")
        current = (type(generation) is int and generation == stream.get("connection_generation") and
                   pair not in quotes.get("coverage", {}).get("retained_last_known_instruments", []))
        connected = stream.get("connected") is True
        result.update(tradeable=tradeable, connected=connected, current_connection=current,
                      quote_epoch=quote_epoch, quote_utc=quote["time"], age_seconds=now-quote_epoch,
                      snapshot_epoch=snapshot_epoch, heartbeat_epoch=heartbeat_epoch)
        if not connected or not current:
            result.update(status="unavailable", reason="disconnected_or_retained_connection_quote")
        elif tradeable is False:
            result.update(status="not_tradeable", reason="original_broker_tradeability_false")
        elif now - quote_epoch > maximum_quote_age:
            result.update(status="stale", reason="underlying_quote_age_exceeds_report_bound")
        else:
            result.update(status="current", reason=None)
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        result["reason"] = failure(exc)
    return result


def _feature_row(pair, state, retained, registry, now, envelope_epoch, maximum_bar_age):
    availability = state["availability"]
    expected_names = [definition["name"] for definition in registry]
    if availability.get("status") == "unavailable":
        raise ValueError("rolling_source_unavailable:" + str(availability.get("reason", "unspecified")))
    if retained is None:
        raise ValueError("bound_original_store_receipt_unavailable")
    t = number(retained["t"])
    published = number(retained["published_epoch"])
    if not (t % 60 == 0 and t + 60 <= published <= envelope_epoch <= now):
        raise ValueError("original_feature_publication_clock_order")
    values_list = retained["values"]
    if not isinstance(values_list, list) or len(values_list) != len(expected_names):
        raise ValueError("exact_original_feature_count_required")
    values = dict(zip(expected_names, values_list))
    if any(value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value))
           for value in values.values()):
        raise ValueError("invalid_original_feature_value")
    finite_count = sum(value is not None for value in values_list)
    if (availability.get("feature_hash") != retained["feature_hash"] or
            availability.get("available_features") != finite_count or
            availability.get("feature_count") != len(expected_names) or
            retained["available_count"] != finite_count or retained["feature_count"] != len(expected_names)):
        raise ValueError("envelope_original_observation_binding_mismatch")
    if epoch(availability["bar_start_utc"]) != t or epoch(availability["bar_end_utc"]) != t + 60:
        raise ValueError("envelope_original_bar_clock_mismatch")
    if abs(epoch(availability["published_utc"]) - published) > .000002:
        raise ValueError("envelope_original_publication_clock_mismatch")
    observation = state.get("observation")
    if observation is not None and (observation.get("values") != values or
            observation.get("feature_hash") != retained["feature_hash"] or
            observation.get("bar_start_epoch") != t or observation.get("published_epoch") != published):
        raise ValueError("envelope_values_differ_from_original_observation")
    diagnostic = state["continuity"]
    if diagnostic.get("schema") != continuity.SCHEMA:
        raise ValueError("original_continuity_schema_required")
    support = diagnostic["feature_support"]
    features = support["features"]
    if set(features) != set(expected_names):
        raise ValueError("exact_feature_support_registry_required")
    missing = defaultdict(list)
    allowed = {"available", "finite_with_support_conflict", "missing_elapsed_support",
               "insufficient_bounded_history", "insufficient_source_history", "source_input_unavailable",
               "no_completed_observation", "undefined_or_numerical"}
    for definition in registry:
        name = definition["name"]
        detail = features[name]
        category = detail.get("category")
        if category not in allowed or detail.get("lookback_bars") != definition["lookback_bars"]:
            raise ValueError("invalid_original_feature_support")
        if detail.get("required_start_epoch") != t - (definition["lookback_bars"]-1)*60:
            raise ValueError("feature_support_original_clock_mismatch")
        if values[name] is None:
            if category in {"available", "finite_with_support_conflict"}:
                raise ValueError("missing_value_marked_finite")
            missing[category].append(name)
        elif category != "available":
            raise ValueError("finite_value_support_conflict")
    c = diagnostic["continuity"]
    if c.get("latest_observed_bar_start_epoch") != t:
        raise ValueError("continuity_original_minute_mismatch")
    bound_start = t - (kernel.max_lookback_bars()-1)*60
    gap_minutes, gap_intervals = Counter(), Counter()
    for gap in c.get("missing_intervals", []):
        if gap["location"] != "interior_gap" or gap["end_epoch"] < bound_start:
            continue
        left, right = max(bound_start, gap["start_epoch"]), min(t, gap["end_epoch"])
        if right >= left:
            gap_minutes[gap["kind"]] += int((right-left)//60)+1
            gap_intervals[gap["kind"]] += 1
    zero_range = (retained["bar"]["high"] == retained["bar"]["low"])
    confirmed_zero = sorted(ZERO_RANGE_FEATURES.intersection(missing.get("undefined_or_numerical", []))) if zero_range else []
    peer = state.get("aligned_state")
    peer_result = {"status": "unavailable", "finite_count": None, "expected_count": 12,
                   "reason": state.get("peer_alignment", {}).get("reason")}
    if peer is not None:
        peer_at = number(peer["bar_start_epoch"])
        peer_pub = number(peer["panel_published_epoch"])
        if peer["observation"]["bar_start_epoch"] != peer_at or peer["peer"]["bar_start_epoch"] != peer_at:
            raise ValueError("aligned_peer_local_clock_mismatch")
        peer_values = peer["peer"]["values"]
        peer_current = 0 <= now - peer_at - 60 <= maximum_bar_age and peer_at+60 <= peer_pub <= now
        peer_result = {"status": peer["peer"]["status"] if peer_current else "stale",
                       "finite_count": sum(value is not None for value in peer_values.values()),
                       "expected_count": len(peer_values), "bar_start_epoch": peer_at,
                       "bar_age_seconds": now-peer_at-60, "published_epoch": peer_pub,
                       "panel_id": peer["panel_id"], "local_feature_hash": peer["observation"]["feature_hash"],
                       "must_use_aligned_local_vector": True}
    age = now-t-60
    return {"status": "stale" if not 0 <= age <= maximum_bar_age else "complete" if finite_count == len(expected_names) else "partial",
            "finite_count": finite_count, "expected_count": len(expected_names),
            "missing_count": len(expected_names)-finite_count,
            "missing_counts_by_reason": {reason: len(names) for reason, names in missing.items()},
            "missing_features_by_reason": dict(missing), "confirmed_zero_range_denominator_features": confirmed_zero,
            "bar_start_epoch": int(t), "bar_end_epoch": int(t+60), "bar_end_utc": utc(t+60),
            "bar_age_seconds": age, "publication_epoch": published, "publication_age_seconds": now-published,
            "feature_hash": retained["feature_hash"], "original_source_receipt_id": retained["receipt_id"],
            "original_source_first_observed_epoch": retained["first_observed_epoch"],
            "original_input_hash": retained["input_hash"],
            "source_path": retained["receipt"].get("source_path"),
            "source_read_completed_epoch": retained["receipt"].get("source_read_completed_epoch", retained["receipt"].get("read_completed_epoch")),
            "source_tail_sha256": retained["receipt"].get("source_tail_sha256"),
            "consecutive_suffix_minutes": c.get("consecutive_suffix_rows"),
            "maximum_required_support_minutes": kernel.max_lookback_bars(),
            "support_gap_minutes_by_evidence": dict(gap_minutes),
            "support_gap_intervals_by_evidence": dict(gap_intervals),
            "gap_evidence_scope": "Retained worker evidence only; unknown is not a broker omission, and an omission is not permanent unavailability.",
            "peer": peer_result, "reason": None}


def build_report(config, envelope, rolling_status, quotes, heartbeat, retained, receipt_errors,
                 *, now, source_errors=None, source_references=None, maximum_quote_age=60):
    now = number(now)
    registry = kernel.feature_registry()
    errors = dict(source_errors or {})
    envelope_epoch = None
    try:
        if envelope is None or rolling_status is None:
            raise ValueError("rolling_envelope_or_status_unreadable")
        if "identity_binding" in errors:
            raise ValueError(errors["identity_binding"])
        envelope_epoch = number(envelope["generated_epoch"])
        if not 0 <= now-envelope_epoch <= config["maximum_bar_age_seconds"]:
            raise ValueError("rolling_envelope_stale_or_future")
        if abs(epoch(envelope["generated_utc"])-envelope_epoch) > .000002:
            raise ValueError("rolling_envelope_clock_mismatch")
        if not envelope.get("publication_generation") or envelope["publication_generation"] != rolling_status.get("publication_generation"):
            raise ValueError("rolling_publication_generation_mismatch")
        if envelope.get("operations_runtime_id") != rolling_status.get("operations_runtime_id"):
            raise ValueError("rolling_operations_runtime_mismatch")
        if set(envelope["pairs"]) != set(config["pairs"]):
            raise ValueError("rolling_universe_mismatch")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        errors["rolling_binding"] = failure(exc)
    pairs, states, feature_states, quote_states = {}, Counter(), Counter(), Counter()
    missing_totals = Counter()
    for pair in sorted(config["pairs"]):
        quote = _quote(pair, quotes, heartbeat, now, maximum_quote_age)
        features = {"status": "unavailable", "finite_count": None, "expected_count": len(registry),
                    "missing_count": None, "reason": None}
        try:
            if "rolling_binding" in errors:
                raise ValueError(errors["rolling_binding"])
            if pair in receipt_errors:
                raise ValueError("original_receipt_read_failed:" + receipt_errors[pair])
            features = _feature_row(pair, envelope["pairs"][pair], retained.get(pair), registry,
                                    now, envelope_epoch, config["maximum_bar_age_seconds"])
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
            features["reason"] = failure(exc)
        status = "not_tradeable" if quote["status"] == "not_tradeable" else features["status"]
        if status in {"complete", "partial"} and quote["status"] != "current":
            status = "quote_" + quote["status"]
        pairs[pair] = {"instrument": pair, "status": status, "quote": quote, "features": features,
                       "readable": features["status"] != "unavailable"}
        states[status] += 1
        feature_states[features["status"]] += 1
        quote_states[quote["status"]] += 1
        missing_totals.update(features.get("missing_counts_by_reason", {}))
    return {"schema_version": SCHEMA, "generated_epoch": now, "generated_utc": utc(now),
            "status": "readable" if all(row["readable"] for row in pairs.values()) else "partial_source_failure",
            "configured_pair_count": len(config["pairs"]), "reported_pair_count": len(pairs),
            "all_configured_pairs_reported": set(pairs) == set(config["pairs"]),
            "counts": dict(states), "feature_status_counts": dict(feature_states),
            "quote_status_counts": dict(quote_states), "missing_feature_reason_counts": dict(missing_totals),
            "source_errors": errors, "source_references": source_references or {},
            "original_receipt_errors": receipt_errors,
            "rolling_generated_epoch": envelope_epoch,
            "rolling_publication_generation": (envelope or {}).get("publication_generation"),
            "bar_age_bound_seconds": config["maximum_bar_age_seconds"],
            "quote_age_reporting_bound_seconds": maximum_quote_age,
            "quote_age_bound_is_not_trading_authorization": True,
            "feature_registry_sha256": digest(encoded(registry)), "continuity_schema": continuity.SCHEMA,
            "semantic_scope": "Availability only; current connection, actual quote age, feature support, and original publication clocks remain separate.",
            "numerical_recomputation_performed": False, "missing_values_filled": False,
            "source_or_original_store_mutated": False, **FLAGS, "pairs": pairs}


def capture(config, *, quote_path=DEFAULT_QUOTES, heartbeat_path=DEFAULT_HEARTBEAT,
            maximum_quote_age=60, maximum_bar_age=None, config_reference=None, operations_path=DEFAULT_OPERATIONS_CONFIG):
    started = time.monotonic()
    root = Path(config["output_root"])
    refs, errors, values = {}, {}, {}
    operations = None
    try:
        operations, refs["identity_binding"] = verify_dependencies(config, config_reference, operations_path)
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        errors["identity_binding"] = failure(exc)
    if maximum_bar_age is not None:
        config = {**config, "maximum_bar_age_seconds": float(maximum_bar_age)}
    paths = {"rolling_status_before": (root/"status.json", MAX_SMALL_BYTES),
             "rolling_envelope": (root/"latest_features.json", MAX_ENVELOPE_BYTES),
             "rolling_status_after": (root/"status.json", MAX_SMALL_BYTES),
             "quotes": (quote_path, MAX_SMALL_BYTES), "quote_heartbeat": (heartbeat_path, MAX_SMALL_BYTES)}
    for name, (path, limit) in paths.items():
        try:
            values[name], refs[name] = read_json(path, limit)
        except (OSError, ValueError, TypeError) as exc:
            values[name] = None
            errors[name] = failure(exc)
            refs[name] = {"path": str(Path(path).absolute()), "read_error": errors[name]}
    envelope = values["rolling_envelope"]
    statuses = [values["rolling_status_before"], values["rolling_status_after"]]
    status = next((value for value in statuses if value and envelope and value.get("publication_generation") == envelope.get("publication_generation")), None)
    retained, receipt_errors = {}, {}
    if envelope is not None and operations is not None:
        try:
            retained, receipt_errors = read_store_evidence(root/"technical.sqlite", config["pairs"], envelope,
                                                          config=config, operations=operations)
            refs["original_store"] = {"path": str(root/"technical.sqlite"), "mode": "read_only",
                                      "rows_bound": len(retained), "binding": "Original immutable per-row hashes; no whole changing database hash."}
        except (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error) as exc:
            errors["original_store"] = failure(exc)
            receipt_errors = {pair: errors["original_store"] for pair in config["pairs"]}
    if config_reference is not None:
        refs["observation_config"] = config_reference
    result = build_report(config, envelope, status, values["quotes"], values["quote_heartbeat"],
        retained, receipt_errors, now=time.time(), source_errors=errors, source_references=refs,
        maximum_quote_age=maximum_quote_age)
    result["capture_seconds"] = time.monotonic()-started
    result["pid"] = os.getpid()
    return result


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded(value)); handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observation-config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--operations-config", type=Path, default=DEFAULT_OPERATIONS_CONFIG)
    parser.add_argument("--quote-path", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--quote-heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--interval-sec", type=float, default=60)
    parser.add_argument("--duration-sec", type=float, default=604800)
    parser.add_argument("--maximum-quote-age-sec", type=float, default=60)
    parser.add_argument("--maximum-bar-age-sec", type=float, default=None)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if not (10 <= args.interval_sec <= 3600 and 1 <= args.duration_sec <= 604800 and 1 <= args.maximum_quote_age_sec <= 180):
        raise ValueError("bounded_report_runtime_required")
    if args.maximum_bar_age_sec is not None and not (60 <= args.maximum_bar_age_sec <= 900):
        raise ValueError("bounded_bar_age_required")
    config, config_reference = read_config(args.observation_config)
    forbidden = {Path(args.observation_config).resolve(), Path(args.operations_config).resolve(), Path(args.quote_path).resolve(), Path(args.quote_heartbeat).resolve(),
                 *(Path(config["output_root"])/name for name in ("latest_features.json", "status.json", "technical.sqlite"))}
    if args.output.resolve() in {path.resolve() for path in forbidden}:
        raise ValueError("report_output_must_not_replace_source")
    started = time.monotonic()
    while time.monotonic()-started < args.duration_sec:
        cycle = time.monotonic()
        report = capture(config, quote_path=args.quote_path, heartbeat_path=args.quote_heartbeat,
                         maximum_quote_age=args.maximum_quote_age_sec,
                         maximum_bar_age=args.maximum_bar_age_sec,
                         config_reference=config_reference,
                         operations_path=args.operations_config)
        atomic_json(args.output, report)
        print(json.dumps({key: report[key] for key in ("generated_utc", "status", "counts", "capture_seconds")}), flush=True)
        if args.once:
            break
        remaining = args.duration_sec-(time.monotonic()-started)
        if remaining > 0:
            time.sleep(min(remaining, max(.01, args.interval_sec-(time.monotonic()-cycle))))


if __name__ == "__main__":
    main()
