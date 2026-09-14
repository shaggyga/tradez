"""Neutral, versioned feature observations; no models, accounts or services."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import numbers
import os
import statistics
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

FRAME_SCHEMA = "feature_observation_frame_v1"
ARCHIVE_SCHEMA = "feature_observation_archive_v1"
PRODUCER_CONTRACT = "live_feature_observation_capture_v1_20260913"
DEFAULT_ARCHIVE_ROOT = Path(__file__).resolve().parent / "data" / "oanda_training_manager" / "feature_observations_v1"
MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
NONFINITE_TAG = "__feature_observation_nonfinite_v1__"
TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M10": 600, "M15": 900, "M30": 1800, "H1": 3600, "H2": 7200, "H3": 10800, "H4": 14400, "D": 86400}
PURE_QUOTE_FEATURES_V1 = frozenset(("bid", "ask", "live_spread_pips", "bid_top_liquidity", "ask_top_liquidity", "bid_total_liquidity", "ask_total_liquidity", "depth_imbalance", "bid_levels", "ask_levels", "depth_total_liquidity", "depth_log_total_liquidity", "depth_top_imbalance", "microprice", "microprice_offset_pips", "quote_receive_age_sec"))


def parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def normalize_json(value: Any, *, _depth: int = 0) -> Any:
    """Preserve JSON data, tagging nonfinite numbers instead of inventing zero."""
    if _depth > 40:
        raise ValueError("observation nesting limit exceeded")
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        numeric = float(value)
        if math.isfinite(numeric):
            return numeric
        return {NONFINITE_TAG: "nan" if math.isnan(numeric) else "positive_infinity" if numeric > 0 else "negative_infinity"}
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("observation mapping keys must be strings")
        return {key: normalize_json(item, _depth=_depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_json(item, _depth=_depth + 1) for item in value]
    raise TypeError(f"unsupported observation value type: {type(value).__name__}")


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(normalize_json(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def payload_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def observation_source_sha256() -> str:
    """Pin this exact capture/normalization implementation in producer lineage."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, bool, numbers.Real)) or (isinstance(value, Mapping) and set(value) == {NONFINITE_TAG})


def capture_feature_group(values: Mapping[str, Any], *, input_timeframe: str, clock: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Capture every scalar; keep nested forecasts distinct from feature IDs."""
    scalars, states, nested, nonfinite = {}, {}, {}, {}
    for name, value in values.items():
        if _scalar(value):
            normalized = normalize_json(value)
            state = "missing" if value is None else "nonfinite" if isinstance(normalized, dict) else "observed"
            if state == "observed" and name.startswith("supervised_") and name != "supervised_ready" and values.get("supervised_ready") is False:
                state = "default"
            if state == "observed" and name in {"depth_top_imbalance", "depth_total_liquidity", "depth_log_total_liquidity", "microprice_offset_pips", "quote_receive_age_sec"} and value == 0:
                state = "unknown_or_default"
            ratio_sources = {"volume_ratio_12": ("volumes", 12), "volume_ratio_30": ("volumes", 30), "spread_ratio_12": ("spread_history_pips", 12)}
            if state == "observed" and name in ratio_sources:
                history_key, lookback = ratio_sources[name]
                history = values.get(history_key)
                if isinstance(history, (list, tuple)) and (len(history) < lookback + 1 or statistics.median(history[-lookback - 1:-1]) <= 0):
                    state = "default"
            spread_lengths = {"current_candle_spread_pips": 1, "median_spread_12_pips": 13, "spread_drop_3_pips": 4}
            if state == "observed" and name in spread_lengths and isinstance(values.get("spread_history_pips"), (list, tuple)) and len(values["spread_history_pips"]) < spread_lengths[name]:
                state = "default"
            if state == "observed" and name == "pos20":
                closes = values.get("closes")
                if isinstance(closes, (list, tuple)) and closes and math.isclose(max(closes[-20:]), min(closes[-20:])):
                    state = "default_flat_range"
            if state == "nonfinite":
                nonfinite[name] = normalized
            scalars[name] = None if state == "nonfinite" else normalized
            states[name] = state
        else:
            normalized = normalize_json(value)
            item = {"source_field": name, "payload_sha256": payload_sha256(normalized), "kind": "mapping" if isinstance(normalized, dict) else "sequence", "item_count": len(normalized)}
            # Model-output dictionaries and clock/quality lineage remain inspectable;
            # large price series already have bounded, explicit legacy envelope fields.
            if isinstance(normalized, dict) and ("forecast" in name or "quality" in name or "origin" in name):
                item.update(storage="retained_payload", payload=normalized)
            else:
                item["storage"] = "metadata_only_not_scalar_feature"
            nested[name] = item
    return {"input_timeframe": input_timeframe, "feature_origin_utc": str(values.get("candle_time") or ""), "values": scalars, "value_states": states, "nonfinite_values": nonfinite, "nested_lineage": nested, "clock": normalize_json(dict(clock or {}))}


def _group(pair: str, group_id: str, captured: Mapping[str, Any], source_schema_id: str, *, quote_component_clocks_v1: bool = False) -> dict[str, Any]:
    timeframe = str(captured.get("input_timeframe") or "UNKNOWN")
    origin = parse_utc(captured.get("feature_origin_utc"))
    clock = captured.get("clock") or {}
    observed = parse_utc(clock.get("observed_utc"))
    completion = parse_utc(clock.get("bar_complete_utc"))
    if completion is None and origin is not None and timeframe in TIMEFRAME_SECONDS:
        completion = origin + timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
    values = dict(captured.get("values") or {})
    states = dict(captured.get("value_states") or {})
    feature_clocks = {}
    feature_ids = {name: f"{source_schema_id}:{pair}:{timeframe}:{name}" for name in values}
    component_clocks = normalize_json(dict(clock.get("component_clocks") or {}))
    origins = ((captured.get("nested_lineage") or {}).get("series_origins") or {}).get("payload") or {}
    for name in values:
        if quote_component_clocks_v1 and name in PURE_QUOTE_FEATURES_V1:
            source_quote = parse_utc(component_clocks.get("quote_feature_source_utc"))
            feature_clocks[name] = {"observed_utc": observed.isoformat() if observed else "", "bar_complete_utc": source_quote.isoformat() if source_quote else "", "input_timeframe": "QUOTE", "clock_basis": str(clock.get("clock_basis") or "unknown") if source_quote else "unknown_quote_source"}
            feature_ids[name] = f"{source_schema_id}:{pair}:QUOTE:{name}"
        if name in {"m5_r1_pips", "m5_r3_pips", "m5_atr14_pips"}:
            m5_origin = parse_utc(origins.get("M5"))
            complete = m5_origin + timedelta(minutes=5) if m5_origin else None
            feature_clocks[name] = {"observed_utc": observed.isoformat() if observed else "", "bar_complete_utc": complete.isoformat() if complete else "", "input_timeframe": "M5", "clock_basis": str(clock.get("clock_basis") or "unknown") if complete else "unknown_component_completion"}
            canonical_name = "m1_atr14_pips" if name == "m5_atr14_pips" else name[3:]
            feature_ids[name] = f"{source_schema_id}:{pair}:M5:{canonical_name}"
        if name.startswith(("order_book_", "position_book_")):
            prefix = "order_book" if name.startswith("order_book_") else "position_book"
            source = parse_utc(component_clocks.get(prefix + "_source_utc"))
            feature_clocks[name] = {"observed_utc": source.isoformat() if source else "", "bar_complete_utc": source.isoformat() if source else "", "input_timeframe": "BOOK", "clock_basis": "source_book" if source else "unknown_book_source"}
            feature_ids[name] = f"{source_schema_id}:{pair}:BOOK:{name}"
    return {
        "group_id": group_id, "input_timeframe": timeframe,
        "group_kind": "microstructure" if group_id == "microstructure" else "indicator",
        "feature_origin_utc": origin.isoformat() if origin else "",
        "bar_complete_utc": completion.isoformat() if completion else "",
        "observed_utc": observed.isoformat() if observed else "",
        "clock_basis": str(clock.get("clock_basis") or "unknown"),
        "status": "available" if observed is not None and completion is not None else "clock_unavailable",
        "component_clocks": component_clocks, "feature_clocks": feature_clocks,
        "values": values, "value_states": states,
        "value_kinds": {name: "model_output" if name.startswith(("supervised_", "pattern_count_")) else "metadata" if name in {"candle_time", "instrument", "input_timeframe", "input_timeframe_seconds", "pip", "pip_size", "history_rows", "bar_count", "received_monotonic"} or name.endswith(("_time", "_utc")) else "indicator" for name in values},
        "feature_ids": feature_ids,
        "aliases": {}, "conflicts": {},
        "nested_lineage": {name: {**{key: value for key, value in item.items() if key != "payload"}, "envelope_pointer": f"observation_inputs.instruments.{pair}.groups.{group_id}.nested_lineage.{name}"} for name, item in (captured.get("nested_lineage") or {}).items()},
        "nonfinite_values": normalize_json(dict(captured.get("nonfinite_values") or {})),
        "coverage": {"scalar_count": len(values), "state_counts": {state: sum(item == state for item in states.values()) for state in sorted(set(states.values()))}},
    }


def _separate_model_outputs(groups: dict[str, Any]) -> None:
    """Preserve model outputs in a labelled group outside indicator values."""
    scalar_maps = ("values", "value_states", "value_kinds", "feature_ids", "feature_clocks", "nonfinite_values")
    for group_id, group in list(groups.items()):
        names = {name for name, kind in group["value_kinds"].items() if kind == "model_output"}
        if not names:
            continue
        model_id = "model_output:" + group_id
        model = {**group, "group_id": model_id, "group_kind": "model_output", "aliases": {}, "conflicts": {}, "nested_lineage": {}}
        for key in scalar_maps:
            model[key] = {name: value for name, value in group[key].items() if name in names}
            group[key] = {name: value for name, value in group[key].items() if name not in names}
        for item in (group, model):
            states = item["value_states"]
            item["coverage"] = {"scalar_count": len(item["values"]), "state_counts": {state: sum(value == state for value in states.values()) for state in sorted(set(states.values()))}}
        groups[model_id] = model


def _deduplicate_feature_ids(groups: dict[str, Any]) -> None:
    seen = {}
    for group_id, group in sorted(groups.items()):
        # Canonical archive JSON sorts keys. Resolve aliases in that same order
        # so the original in-memory snapshot and its replay build exact frames.
        for name, identity in sorted(group["feature_ids"].items()):
            clock = group["feature_clocks"].get(name, group)
            value = group["values"][name]
            evidence = (type(value).__name__, value, group["value_states"].get(name), clock.get("bar_complete_utc"))
            if identity not in seen:
                seen[identity] = (group_id, name, evidence)
                continue
            other_group, other_name, other_evidence = seen[identity]
            if evidence == other_evidence:
                group["aliases"][name] = {"canonical_group": other_group, "canonical_feature_name": other_name, "feature_id": identity}
            else:
                group["feature_ids"][name] = identity + ":conflict:" + group_id + ":" + name
                group["conflicts"][name] = {"other_group": other_group, "other_feature_name": other_name, "reason": "same_identity_different_value_state_or_completion"}
                groups[other_group]["conflicts"][other_name] = {"other_group": group_id, "other_feature_name": name, "reason": "same_identity_different_value_state_or_completion"}


def build_observation_frame(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize an original snapshot without importing its producer."""
    raw = normalize_json(snapshot)
    source = dict(raw.get("observation_source") or {})
    source.setdefault("producer_id", "legacy_live_model_feature_snapshot")
    source.setdefault("producer_contract_id", "unknown")
    source_schema_id = payload_sha256({"frame_schema": FRAME_SCHEMA, "source": source})[:24]
    captured_pairs = (raw.get("observation_inputs") or {}).get("instruments") or {}
    legacy_pairs = raw.get("instruments") or {}
    instruments = {}
    for pair in sorted(set(captured_pairs) | set(legacy_pairs)):
        legacy = legacy_pairs.get(pair) or {}
        captured = captured_pairs.get(pair) or {}
        captures = captured.get("groups") or {}
        if not captures:
            captures = {"primary": capture_feature_group(legacy.get("features") or {}, input_timeframe="M1")}
            captures.update({f"timeframe:{tf}": capture_feature_group(values, input_timeframe=tf) for tf, values in (legacy.get("timeframe_features") or {}).items()})
            captures["microstructure"] = capture_feature_group(legacy.get("microstructure") or {}, input_timeframe="QUOTE")
        groups = {group_id: _group(pair, group_id, value, source_schema_id, quote_component_clocks_v1=(raw.get("observation_inputs") or {}).get("quote_component_clocks_v1") is True) for group_id, value in sorted(captures.items())}
        _separate_model_outputs(groups)
        primary = groups.get("primary")
        m1 = groups.get("timeframe:M1")
        if primary and m1:
            for name in set(primary["values"]) & set(m1["values"]):
                equal = type(primary["values"][name]) is type(m1["values"][name]) and primary["values"][name] == m1["values"][name] and primary["value_states"].get(name) == m1["value_states"].get(name) and primary["feature_origin_utc"] == m1["feature_origin_utc"]
                if equal:
                    m1["aliases"][name] = {"canonical_group": "primary", "feature_id": primary["feature_ids"][name]}
                else:
                    # The producer can enrich only its primary cache. Do not let a
                    # same-name stale structural value silently replace that field.
                    m1["feature_ids"][name] += ":structural_conflict"
                    detail = {"other_group": "primary", "reason": "same_identity_different_value_state_or_origin"}
                    m1["conflicts"][name] = detail
                    primary["conflicts"][name] = {**detail, "other_group": "timeframe:M1"}
        _deduplicate_feature_ids(groups)
        aliases = {}
        for name in (legacy.get("unified_forecast_features") or {}):
            if "__" in name:
                prefix, feature = name.split("__", 1)
                target = groups.get(f"timeframe:{prefix.upper()}")
                if target and feature in target["feature_ids"]:
                    aliases[name] = {"group_id": target["group_id"], "feature_name": feature, "feature_id": target["feature_ids"][feature]}
                else:
                    aliases[name] = {"state": "unresolved_source_group"}
        instruments[pair] = {"quote": normalize_json(captured.get("quote", legacy.get("quote") or {})), "groups": groups, "unified_aliases": aliases, "coverage": normalize_json(captured.get("coverage") or {"quote_accepted": pair in legacy_pairs}), "nested_lineage": {group_id: group["nested_lineage"] for group_id, group in groups.items() if group["nested_lineage"]}}
    return {"schema_version": FRAME_SCHEMA, "snapshot_id": str(raw.get("snapshot_id") or ""), "generated_utc": str(raw.get("generated_utc") or ""), "generated_epoch": raw.get("generated_epoch"), "source": source, "source_schema_id": source_schema_id, "source_payload_sha256": payload_sha256(raw), "coverage": normalize_json(raw.get("coverage") or {}), "instruments": instruments}


def archive_observation_snapshot(snapshot: Mapping[str, Any], root: Path, *, before_publish=None) -> Path:
    """Write one immutable, bounded full-envelope archive; collisions are errors."""
    original = normalize_json(snapshot)
    generated = parse_utc(original.get("generated_utc"))
    if generated is None or not isinstance(original.get("snapshot_id"), str) or not 1 <= len(original["snapshot_id"]) <= 512:
        raise ValueError("observation requires snapshot identity and timezone-aware generation clock")
    frame = build_observation_frame(original)
    identity = {"schema_version": ARCHIVE_SCHEMA, "snapshot_id": frame["snapshot_id"], "source_schema_id": frame["source_schema_id"]}
    envelope = {"schema_version": ARCHIVE_SCHEMA, "payload_sha256": frame["source_payload_sha256"], "source_identity": identity, "original_snapshot": original, "frame": frame}
    encoded = canonical_bytes(envelope)
    if len(encoded) > MAX_ARCHIVE_BYTES:
        raise ValueError("observation archive exceeds bounded envelope size")
    directory = Path(root) / f"date={generated:%Y%m%d}" / f"hour={generated:%H}"
    identity_hash = payload_sha256(identity)[:32]
    path = directory / f"obs_{identity_hash}.json.gz"
    identity_directory = Path(root) / ".identities"
    identity_directory.mkdir(parents=True, exist_ok=True)
    identity_path = identity_directory / f"{identity_hash}.json"
    identity_record = canonical_bytes({"source_identity": identity, "payload_sha256": envelope["payload_sha256"], "archive_path": path.relative_to(root).as_posix()})
    if identity_path.exists() and _read_identity(identity_path) != identity_record:
        raise ValueError("observation identity reused with different payload or generation partition")
    # Reserve identity before publishing any archive, so conflicting concurrent
    # payloads cannot both become visible in different hour partitions.
    if before_publish is None:
        _publish_identity(identity_path, identity_record)
    directory.mkdir(parents=True, exist_ok=True)
    if path.exists():
        with gzip.open(path, "rb") as stream:
            existing_bytes = stream.read(MAX_ARCHIVE_BYTES + 1)
        if len(existing_bytes) > MAX_ARCHIVE_BYTES:
            raise ValueError("existing observation archive exceeds size limit")
        existing = json.loads(existing_bytes)
        if existing.get("payload_sha256") != envelope["payload_sha256"] or existing.get("source_identity") != identity or existing_bytes != encoded:
            raise ValueError("observation identity collision or archive integrity mismatch")
        if before_publish is not None:
            before_publish()
        _publish_identity(identity_path, identity_record)
        return path
    temporary = path.with_name(f".{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as output:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as stream:
                stream.write(encoded)
        try:
            # Hard-link publication cannot replace a concurrently written archive.
            if before_publish is not None:
                before_publish()
                _publish_identity(identity_path, identity_record)
            os.link(temporary, path)
        except FileExistsError:
            return archive_observation_snapshot(original, root, before_publish=before_publish)
    finally:
        temporary.unlink(missing_ok=True)
    _publish_identity(identity_path, identity_record)
    return path


def _publish_identity(path: Path, encoded: bytes) -> None:
    temporary = path.with_name(f".{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_bytes(encoded)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if _read_identity(path) != encoded:
                raise ValueError("observation identity collision")
    finally:
        temporary.unlink(missing_ok=True)


def _read_identity(path: Path) -> bytes:
    with path.open("rb") as stream:
        value = stream.read(8193)
    if len(value) > 8192:
        raise ValueError("observation identity record exceeds limit")
    return value
