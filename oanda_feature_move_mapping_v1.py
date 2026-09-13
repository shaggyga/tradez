"""Bounded descriptive feature-change / pair-movement join. No forecasts or orders.

Only observation archives with explicit availability clocks are accepted. A
requested window and its actual endpoint spacing are always reported separately.
"""
from __future__ import annotations

import bisect
import gzip
import hashlib
import json
import math
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = "feature_move_mapping_v1"
WINDOWS = (300, 900, 3600)
MAX_FRAMES = 256
MAX_FEATURE_VALUES = 2_000_000
MAX_FILES = 256
MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_READ_BYTES = 256 * 1024 * 1024
PAIR = re.compile(r"^[A-Z]{3}_[A-Z]{3}$")


def _epoch(value: Any) -> float | None:
    try:
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return None
            result = parsed.timestamp()
        elif type(value) in (int, float):
            result = float(value)
        else:
            return None
        return result if math.isfinite(result) and result > 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _iso(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _numeric(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _seconds(timeframe: Any) -> int | None:
    if timeframe == "D":
        return 86400
    match = re.fullmatch(r"([SMHD])(\d+)", str(timeframe).upper())
    if not match:
        return None
    return {"S": 1, "M": 60, "H": 3600, "D": 86400}[match[1]] * int(match[2])


def _quote(row: Mapping[str, Any], cutoff: float, tolerance: float, *, frame_cutoff=None):
    quote = row.get("quote") or {}
    if not isinstance(quote, Mapping):
        return None, "quote_missing"
    stamp = _epoch(quote.get("time") or quote.get("observed_utc"))
    bid, ask = quote.get("bid"), quote.get("ask")
    if stamp is None:
        return None, "quote_clock_missing"
    if stamp > cutoff:
        return None, "quote_from_future"
    if frame_cutoff is not None and stamp > frame_cutoff:
        return None, "quote_after_snapshot"
    if cutoff - stamp > tolerance:
        return None, "quote_stale"
    if not (_numeric(bid) and _numeric(ask) and 0 < bid <= ask):
        return None, "quote_invalid"
    # Nontradeable quotes can still be observed; executable status is separate.
    return {"time": stamp, "mid": bid + (ask - bid) / 2,
            "bid": bid, "ask": ask, "tradeable": quote.get("tradeable")}, None


def _entries(frame: Mapping[str, Any], instrument: str):
    pair = (frame.get("instruments") or {}).get(instrument) or {}
    result = {}
    for group_name, group in sorted((pair.get("groups") or {}).items()):
        if not isinstance(group, Mapping):
            continue
        for name, value in (group.get("values") or {}).items():
            identity = (group.get("feature_ids") or {}).get(name)
            if not isinstance(identity, str) or not identity:
                continue
            item = {"feature_id": identity, "feature_name": name, "group": group_name,
                    "input_timeframe": group.get("input_timeframe"), "value": value,
                    "value_state": (group.get("value_states") or {}).get(name, "unknown"),
                    "observed_utc": group.get("observed_utc"),
                    "bar_complete_utc": group.get("bar_complete_utc"),
                    "clock_basis": group.get("clock_basis"),
                    "component_clocks": group.get("component_clocks") or {},
                    "value_kind": (group.get("value_kinds") or {}).get(name, group.get("group_kind", "observed_feature")),
                    "units": (group.get("units") or {}).get(name),
                    "aliases": []}
            override = (group.get("feature_clocks") or {}).get(name) or {}
            for key in ("input_timeframe", "observed_utc", "bar_complete_utc", "clock_basis", "component_clocks"):
                if key in override:
                    item[key] = override[key]
            if str(name).startswith(("m5_", "order_book_", "position_book_")) and not override:
                item["clock_basis"] = "unknown_component_clock"
            old = result.get(identity)
            if old is None:
                result[identity] = item
            elif old["value_state"] == "conflicting_alias":
                # A third alias cannot erase an already observed disagreement.
                old["aliases"].append(group_name)
            elif type(old["value"]) is type(value) and old["value"] == value:
                # Use the later availability clock for an alias, never an earlier one.
                if (_epoch(item["observed_utc"]) or 0) > (_epoch(old["observed_utc"]) or 0):
                    item["aliases"] = old["aliases"] + [old["group"]]
                    result[identity] = item
                else:
                    old["aliases"].append(group_name)
            else:
                # Conflicts must survive even if an upstream writer reused an ID.
                old["value_state"] = "conflicting_alias"
                old["aliases"].append(group_name)
    return result


def _entry_reason(item, cutoff: float, tolerance: float, *, frame_cutoff=None,
                  parse_epoch=_epoch, parse_seconds=_seconds):
    if item is None:
        return "feature_missing"
    if item["value_state"] != "observed":
        return "feature_" + str(item["value_state"])
    observed = parse_epoch(item["observed_utc"])
    if observed is None or not item["clock_basis"] or "unknown" in str(item["clock_basis"]).lower():
        return "feature_clock_unknown"
    if observed > cutoff:
        return "feature_from_future"
    if frame_cutoff is not None and observed > frame_cutoff:
        return "feature_after_snapshot"
    if cutoff - observed > tolerance:
        return "feature_observation_stale"
    book_value = str(item["feature_name"]).startswith(("order_book_", "position_book_"))
    quote_sensitive = (not book_value and (item["group"] == "microstructure" or
                       str(item["feature_name"]).startswith(("depth_", "microprice", "live_spread", "quote_receive", "bid_", "ask_")))
                       )
    if quote_sensitive:
        source_quote = parse_epoch(item["component_clocks"].get("quote_feature_source_utc"))
        if source_quote is None:
            return "feature_source_quote_clock_unknown"
        if source_quote > observed or source_quote > cutoff:
            return "feature_source_quote_from_future"
        if cutoff - source_quote > tolerance:
            return "feature_source_quote_stale"
    completed = parse_epoch(item["bar_complete_utc"])
    interval = parse_seconds(item["input_timeframe"])
    if interval is None and item["input_timeframe"] not in ("QUOTE", "BOOK"):
        return "feature_timeframe_unknown"
    if interval:
        if completed is None:
            return "bar_completion_unknown"
        if completed > observed:
            return "bar_incomplete_at_observation"
        if observed - completed > interval + tolerance:
            return "feature_bar_stale"
    return None


def _units_and_percent(name, before, after, explicit=None):
    lower = str(name).lower()
    unit = explicit or ("pips" if "pips" in lower else "bps" if "bps" in lower
                        else "points" if "rsi" in lower else "feature units")
    # A signed oscillator's percentage is unstable around zero. Only known
    # nonnegative level statistics receive this optional secondary display.
    level = any(token in lower for token in ("atr", "spread", "volume", "liquidity", "range_pips"))
    signed = any(token in lower for token in ("imbalance", "offset", "difference", "minus", "change", "return"))
    percent = 100 * (after - before) / before if level and not signed and before > 1e-12 and after >= 0 else None
    return str(unit), percent if percent is None or math.isfinite(percent) else None


def _prepare_frames(frames, now, *, recreated_digests=None):
    """Validate a call-owned frame set once; never cache between observations."""
    if len(frames) > MAX_FRAMES:
        raise ValueError("frame_count_bound")
    indexed, ids, invalid = [], {}, Counter()
    values_count = 0
    for frame in frames:
        if not isinstance(frame, Mapping) or frame.get("schema_version") != "feature_observation_frame_v1":
            invalid["unsupported_frame"] += 1
            continue
        stamp = _epoch(frame.get("generated_utc"))
        if stamp is None or stamp > now:
            invalid["invalid_or_future_frame_clock"] += 1
            continue
        identity = (str(frame.get("source_schema_id")), str(frame.get("snapshot_id")))
        digest = (recreated_digests or {}).get(id(frame))
        if digest is None:
            digest = hashlib.sha256(_canonical(frame)).hexdigest()
        if identity in ids:
            if ids[identity] != digest:
                raise ValueError("conflicting_snapshot_identity")
            continue
        ids[identity] = digest
        instruments = frame.get("instruments") or {}
        if not isinstance(instruments, Mapping) or len(instruments) > 68:
            raise ValueError("instrument_bound_or_shape")
        for pair in instruments.values():
            if not isinstance(pair, Mapping) or not isinstance(pair.get("groups") or {}, Mapping):
                raise ValueError("feature_group_shape")
            for group in (pair.get("groups") or {}).values():
                if not isinstance(group, Mapping) or not isinstance(group.get("values") or {}, Mapping):
                    raise ValueError("feature_value_shape")
                if any(value is not None and type(value) not in (bool, int, float, str)
                       for value in (group.get("values") or {}).values()):
                    raise ValueError("feature_scalar_required")
                values_count += len(group.get("values") or {})
        if values_count > MAX_FEATURE_VALUES:
            raise ValueError("feature_value_bound")
        indexed.append((stamp, digest, frame))
    indexed.sort(key=lambda value: (value[0], value[1]))
    return indexed, invalid


def build_feature_move_map(frames: Sequence[Mapping[str, Any]], *, as_of_utc,
                           window_sec=300, endpoint_tolerance_sec=75,
                           max_frame_age_sec=75, min_history=5, top_limit=50,
                           instrument=None, include_all_comparisons=False):
    return _build_feature_move_map(
        frames, as_of_utc=as_of_utc, window_sec=window_sec,
        endpoint_tolerance_sec=endpoint_tolerance_sec,
        max_frame_age_sec=max_frame_age_sec, min_history=min_history,
        top_limit=top_limit, instrument=instrument,
        include_all_comparisons=include_all_comparisons)


def _build_feature_move_map(frames, *, as_of_utc, window_sec=300,
                            endpoint_tolerance_sec=75, max_frame_age_sec=75,
                            min_history=5, top_limit=50, instrument=None,
                            include_all_comparisons=False, prepared=None,
                            entry_cache=None, parser_caches=None):
    now = _epoch(as_of_utc)
    if now is None or window_sec not in WINDOWS:
        raise ValueError("valid_as_of_and_5_15_60_minute_window_required")
    if type(include_all_comparisons) is not bool:
        raise ValueError("boolean_full_comparison_option_required")
    if instrument is not None and (not isinstance(instrument, str) or not PAIR.fullmatch(instrument)
                                   or instrument[:3] == instrument[4:]):
        raise ValueError("valid_pair_filter_required")
    if not (0 <= endpoint_tolerance_sec <= 120 and 0 < max_frame_age_sec <= 120
            and 2 <= min_history <= 30 and 1 <= top_limit <= 200):
        raise ValueError("bounded_mapping_parameters_required")
    indexed, invalid = _prepare_frames(frames, now) if prepared is None else prepared
    payload = {"schema_version": SCHEMA_VERSION, "generated_utc": _iso(now),
               "window_sec": window_sec, "instrument_filter": instrument, "status": "waiting_for_observations",
               "summary": {"frames": len(indexed), "rejected_frames": dict(invalid)},
               "feature_changes": [], "pairs": [], "source_snapshots": [],
               "limitations": ["Observed association, not a forecast or causal attribution.",
                               "History counts are overlapping comparisons, not independent events.",
                               "Unavailable fields remain visible; ranking is a display limit only."],
               "research_only": True, "can_place_orders": False, "can_promote": False}
    if include_all_comparisons:
        # Evaluation retains controls and unavailable rows before any display
        # truncation. Existing frame/value bounds still apply to this payload.
        payload["all_feature_changes"] = []
    if not indexed:
        return payload
    clocks = [item[0] for item in indexed]
    end_time, _, latest = indexed[-1]
    if now - end_time > max_frame_age_sec:
        payload.update(status="stale_observations", latest_observed_utc=_iso(end_time))
        return payload
    target = end_time - window_sec
    base_index = bisect.bisect_right(clocks, target) - 1
    base = indexed[base_index][2] if base_index >= 0 and target - clocks[base_index] <= endpoint_tolerance_sec else None
    names = set((latest.get("instruments") or {}))
    if base:
        names.update(base.get("instruments") or {})
    for exclusion in (latest.get("coverage") or {}).get("quote_exclusions", []):
        if isinstance(exclusion, Mapping) and PAIR.fullmatch(str(exclusion.get("instrument", ""))):
            names.add(exclusion["instrument"])
    if any(not PAIR.fullmatch(pair) or pair[:3] == pair[4:] for pair in names):
        raise ValueError("invalid_pair_identity")
    counts, reasons, all_changes = Counter(), Counter(), []
    if entry_cache is None:
        entry_cache = {}
    epoch_cache, seconds_cache = parser_caches if parser_caches is not None else ({}, {})

    def parsed(value, cache, parser, bound):
        # Parsing a string is pure. Keep small call-owned caches, not evidence
        # or freshness results that could carry into another decision.
        if not isinstance(value, str) or len(value) > 128:
            return parser(value)
        if value not in cache:
            if len(cache) >= bound:
                cache.clear()
            cache[value] = parser(value)
        return cache[value]

    def epoch_once(value):
        return parsed(value, epoch_cache, _epoch, 4096)

    def seconds_once(value):
        return parsed(value, seconds_cache, _seconds, 64)

    def entry_reason(item, cutoff, tolerance, *, frame_cutoff=None):
        return _entry_reason(item, cutoff, tolerance, frame_cutoff=frame_cutoff,
                             parse_epoch=epoch_once, parse_seconds=seconds_once)

    def entries(index, pair):
        key = (index, pair)
        if key not in entry_cache:
            entry_cache[key] = _entries(indexed[index][2], pair)
        return entry_cache[key]

    used_indexes = {len(indexed)-1}
    if base:
        used_indexes.add(base_index)
    for pair in sorted(names):
        current_pair = (latest.get("instruments") or {}).get(pair) or {}
        before_pair = ((base or {}).get("instruments") or {}).get(pair) or {}
        q1, r1 = _quote(current_pair, end_time, endpoint_tolerance_sec)
        q0, r0 = _quote(before_pair, target, endpoint_tolerance_sec,
                        frame_cutoff=clocks[base_index] if base else None)
        reason = r1 or ("baseline_snapshot_missing" if base is None else r0)
        market = {"instrument": pair, "status": "unavailable" if reason else "available",
                  "reason": reason, "move_pct": None, "move_bps": None,
                  "start_utc": None, "end_utc": None, "actual_window_sec": None,
                  "feature_count": 0, "base_currency": pair[:3], "quote_currency": pair[4:]}
        if not reason:
            pct = 100 * (q1["mid"] / q0["mid"] - 1)
            if not math.isfinite(pct) or not math.isfinite(pct*100):
                market.update(status="unavailable", reason="nonfinite_derived_price_move")
            else:
                market.update(move_pct=pct, move_bps=pct*100, start_utc=_iso(q0["time"]),
                          end_utc=_iso(q1["time"]), actual_window_sec=q1["time"]-q0["time"],
                          current_spread_bps=(q1["ask"]-q1["bid"])/q1["mid"]*10000,
                          tradeable=q1["tradeable"])
                counts["pairs_with_no_price_change" if pct == 0 else "pairs_with_price_change"] += 1
        payload["pairs"].append(market)
        if instrument is not None and pair != instrument:
            market["feature_count"] = None
            continue
        after_entries = entries(len(indexed)-1, pair)
        before_entries = entries(base_index, pair) if base else {}
        history = {}
        # Only comparison windows fully preceding the current window enter
        # the descriptive reference distribution. No future/current deltas.
        for index in range(max(0, base_index)+1):
            h_end = clocks[index]
            previous = bisect.bisect_right(clocks, h_end-window_sec) - 1
            if previous < 0 or h_end-window_sec-clocks[previous] > endpoint_tolerance_sec:
                continue
            if (indexed[index][2].get("source_schema_id") != latest.get("source_schema_id")
                    or indexed[previous][2].get("source_schema_id") != latest.get("source_schema_id")):
                continue
            e1, e0 = entries(index, pair), entries(previous, pair)
            used_indexes.update((index, previous))
            for identity in after_entries.keys() & e1.keys() & e0.keys():
                v1, v0 = e1[identity], e0[identity]
                if (entry_reason(v1, h_end, endpoint_tolerance_sec)
                        or entry_reason(v0, h_end-window_sec, endpoint_tolerance_sec,
                                         frame_cutoff=clocks[previous])
                        or not _numeric(v1["value"]) or not _numeric(v0["value"])):
                    continue
                delta = abs(v1["value"] - v0["value"])
                if math.isfinite(delta):
                    history.setdefault(identity, []).append(delta)
        for identity in sorted(after_entries.keys() | before_entries.keys()):
            v1, v0 = after_entries.get(identity), before_entries.get(identity)
            exemplar = v1 or v0
            if exemplar.get("value_kind") == "metadata":
                counts["metadata_fields_retained_outside_ranking"] += 1
                continue
            feature_reason = ("baseline_snapshot_missing" if base is None else None)
            if base and base.get("source_schema_id") != latest.get("source_schema_id"):
                feature_reason = "feature_schema_changed"
            feature_reason = feature_reason or entry_reason(v1, end_time, endpoint_tolerance_sec) or entry_reason(
                v0, target, endpoint_tolerance_sec, frame_cutoff=clocks[base_index] if base else None)
            row = {"instrument": pair, "feature_id": identity,
                   "feature_name": exemplar["feature_name"], "group": exemplar["group"],
                   "feature_kind": "model_output" if exemplar.get("value_kind") == "model_output" or str(exemplar["feature_name"]).startswith(("supervised_", "forecast_", "predicted_")) else exemplar.get("value_kind", "observed_feature"),
                   "input_timeframe": exemplar["input_timeframe"], "units": exemplar.get("units") or "feature units",
                   "before": v0["value"] if v0 else None, "after": v1["value"] if v1 else None,
                   "change": None, "change_pct": None, "unusual_percentile": None,
                   "history_count": 0, "ranking_reason": None,
                   "status": "unavailable" if feature_reason else "available", "reason": feature_reason,
                   "before_observed_utc": v0["observed_utc"] if v0 else None,
                   "after_observed_utc": v1["observed_utc"] if v1 else None,
                   "actual_window_sec": None, "pair_move_pct": market["move_pct"],
                   "pair_move_status": market["status"], "pair_move_reason": market["reason"],
                   "relationship": "contemporaneous_window", "aliases": list(exemplar["aliases"])}
            if feature_reason:
                counts["unavailable"] += 1
                reasons[feature_reason] += 1
            else:
                counts["available"] += 1
                market["feature_count"] += 1
                row["actual_window_sec"] = epoch_once(v1["observed_utc"]) - epoch_once(v0["observed_utc"])
                if _numeric(row["before"]) and _numeric(row["after"]):
                    change = row["after"] - row["before"]
                    if not math.isfinite(change):
                        row.update(status="unavailable", reason="nonfinite_derived_feature_change")
                        counts["available"] -= 1
                        counts["unavailable"] += 1
                        market["feature_count"] -= 1
                        reasons[row["reason"]] += 1
                        all_changes.append(row)
                        continue
                    row["change"] = change
                    row["units"], row["change_pct"] = _units_and_percent(row["feature_name"], row["before"], row["after"], exemplar.get("units"))
                    prior = history.get(identity, [])
                    row["history_count"] = len(prior)
                    if len(prior) < min_history:
                        row["ranking_reason"] = "insufficient_past_comparisons"
                    elif max(prior) == min(prior):
                        row["ranking_reason"] = "constant_past_changes"
                    else:
                        row["unusual_percentile"] = 100 * sum(value <= abs(change) for value in prior) / len(prior)
                        counts["rankable"] += 1
                    counts["unchanged" if change == 0 else "changed"] += 1
                else:
                    row["status"] = "state_transition" if type(row["before"]) is not type(row["after"]) or row["before"] != row["after"] else "unchanged_state"
                    row["units"] = "state"
                    row["ranking_reason"] = "categorical_value"
                    counts["state_changes" if row["status"] == "state_transition" else "unchanged"] += 1
            all_changes.append(row)
    all_changes.sort(key=lambda row: (row["status"] == "unavailable", row["unusual_percentile"] is None,
                                     -(row["unusual_percentile"] or 0),
                                     row["change"] == 0, row["instrument"], row["feature_id"]))
    payload["feature_changes"] = all_changes[:top_limit]
    if include_all_comparisons:
        payload["all_feature_changes"] = all_changes
    payload["pairs"].sort(key=lambda row: (row["move_pct"] is None, -abs(row["move_pct"] or 0), row["instrument"]))
    # Shared-currency links are descriptive orientations, not independent votes.
    for row in payload["feature_changes"]:
        currencies = {row["instrument"][:3], row["instrument"][4:]}
        row["related_pair_moves"] = [
            {"instrument": market["instrument"], "move_pct": market["move_pct"],
             "status": market["status"], "shared_currencies": sorted(currencies & {market["base_currency"], market["quote_currency"]}),
             "base_currency": market["base_currency"], "quote_currency": market["quote_currency"]}
            for market in payload["pairs"]
            if currencies & {market["base_currency"], market["quote_currency"]}]
    payload["source_snapshots"] = [{"snapshot_id": indexed[index][2].get("snapshot_id"),
                                    "generated_utc": _iso(clocks[index]),
                                    "source_schema_id": indexed[index][2].get("source_schema_id"),
                                    "source_payload_sha256": indexed[index][2].get("source_payload_sha256")}
                                   for index in sorted(used_indexes)]
    payload["summary"].update(dict(counts), total_feature_comparisons=len(all_changes),
                              visible_feature_comparisons=len(payload["feature_changes"]),
                              unavailable_reasons=dict(reasons), pair_count=len(names),
                              display_truncated=len(all_changes) > top_limit,
                              coverage=latest.get("coverage") or {})
    payload["status"] = "available" if counts["available"] else "waiting_for_comparable_features"
    payload["latest_observed_utc"] = _iso(end_time)
    return payload


def read_feature_move_map(archive_root, *, as_of_utc, window_sec=300, instrument=None,
                         include_all_comparisons=False):
    return read_feature_move_maps(
        archive_root, as_of_utc=as_of_utc, window_secs=(window_sec,),
        instrument=instrument, include_all_comparisons=include_all_comparisons)[window_sec]


def read_feature_move_maps(archive_root, *, as_of_utc, window_secs=WINDOWS, instrument=None,
                          include_all_comparisons=False):
    """Read only the new append-only archive, with explicit IO/shape bounds.

    This routine never starts a producer or opens a database. Sampling caused by
    a bound is reported, and unsupported/missing baselines remain unavailable.
    """
    now = _epoch(as_of_utc)
    if (now is None or not isinstance(window_secs, (list, tuple))
            or not 1 <= len(window_secs) <= len(WINDOWS)
            or any(type(value) is not int or value not in WINDOWS for value in window_secs)
            or len(set(window_secs)) != len(window_secs)):
        raise ValueError("valid_as_of_and_window_required")
    if type(include_all_comparisons) is not bool:
        raise ValueError("boolean_full_comparison_option_required")
    if instrument is not None and (not isinstance(instrument, str) or not PAIR.fullmatch(instrument)
                                   or instrument[:3] == instrument[4:]):
        raise ValueError("valid_pair_filter_required")
    root = Path(archive_root)
    stats = {"files_considered": 0, "files_read": 0, "bytes_read": 0,
             "errors": [], "bounded_sample": False}
    files = []
    if root.is_symlink() or root.is_junction():
        raise ValueError("archive_root_symlink_refused")
    # Eight recent UTC hours cover a one-hour baseline and prior reference
    # windows. Never recursively enumerate the older market archive.
    for offset in range(9):
        stamp = datetime.fromtimestamp(now, timezone.utc) - timedelta(hours=offset)
        day = root / stamp.strftime("date=%Y%m%d")
        hour = day / stamp.strftime("hour=%H")
        if day.is_symlink() or hour.is_symlink() or day.is_junction() or hour.is_junction():
            stats["errors"].append("archive_directory_symlink_refused")
            continue
        if not hour.is_dir():
            continue
        for path in hour.glob("obs_*.json.gz"):
            if path.is_symlink() or path.is_junction():
                stats["errors"].append("archive_file_symlink_refused")
                continue
            meta = path.stat()
            files.append((meta.st_mtime_ns, path, meta.st_size))
            if len(files) >= MAX_FILES * 8:
                stats["bounded_sample"] = True
                break
        if len(files) >= MAX_FILES * 8:
            break
    stats["files_considered"] = len(files)
    files.sort(key=lambda value: (value[0], value[1].name))
    if len(files) > MAX_FILES:
        # Keep recent snapshots plus a uniform older sample so long windows
        # are not displaced entirely by high-frequency current observations.
        old = files[:-32]
        indices = {round(i * (len(old)-1) / (MAX_FILES-33)) for i in range(MAX_FILES-32)}
        files = [old[i] for i in sorted(indices)] + files[-32:]
        stats["bounded_sample"] = True
    frames, recreated_digests = [], {}
    # Prioritize baseline/reference-window candidates before dense current
    # samples consume the byte budget. File metadata only selects IO order;
    # actual acceptance and windows still use validated payload clocks.
    priority = []
    if files:
        mtimes = [item[0] / 1e9 for item in files]
        # Share one validated read across requested windows. Interleave their
        # reference candidates so the shortest window cannot consume the
        # complete byte budget before another window's baseline is inspected.
        targets = [mtimes[-1] - window_sec * offset
                   for offset in range(8) for window_sec in window_secs]
        priority = list(dict.fromkeys(max(0, bisect.bisect_right(mtimes, target)-1) for target in targets))
    order = priority + [index for index in reversed(range(len(files))) if index not in priority]
    for index in order:
        _, path, size = files[index]
        if size > MAX_FILE_BYTES:
            stats["errors"].append("compressed_file_bound")
            continue
        remaining = MAX_READ_BYTES - stats["bytes_read"]
        if remaining <= 0:
            stats["bounded_sample"] = True
            break
        try:
            with gzip.open(path, "rb") as handle:
                raw = handle.read(min(remaining, MAX_FILE_BYTES) + 1)
            stats["bytes_read"] += len(raw)
            if len(raw) > min(remaining, MAX_FILE_BYTES):
                stats["errors"].append("expanded_file_or_total_bound")
                stats["bounded_sample"] = True
                continue
            envelope = json.loads(raw)
            if not isinstance(envelope, dict):
                raise ValueError("archive_object_required")
            if envelope.get("schema_version") != "feature_observation_archive_v1":
                raise ValueError("unsupported_archive_schema")
            original = envelope["original_snapshot"]
            if not isinstance(original, dict):
                raise ValueError("snapshot_object_required")
            if hashlib.sha256(_canonical(original)).hexdigest() != envelope["payload_sha256"]:
                raise ValueError("snapshot_hash_mismatch")
            # Recreate the normalized frame from the retained source payload;
            # editing only the derived frame cannot invent feature observations.
            try:
                from oanda_feature_observations_v1 import build_observation_frame
            except ModuleNotFoundError:
                from trad.oanda_feature_observations_v1 import build_observation_frame
            frame = build_observation_frame(original)
            recreated_bytes = _canonical(frame)
            if recreated_bytes != _canonical(envelope["frame"]):
                raise ValueError("frame_recreation_mismatch")
            recreated_digests[id(frame)] = hashlib.sha256(recreated_bytes).hexdigest()
            del recreated_bytes
            frames.append(frame)
            stats["files_read"] += 1
        except (OSError, EOFError, ValueError, KeyError, TypeError, AttributeError) as exc:
            stats["errors"].append(type(exc).__name__ + ":" + str(exc)[:100])
    results = {}
    prepared, preparation_error = None, None
    try:
        prepared = _prepare_frames(frames, now, recreated_digests=recreated_digests)
    except ValueError as exc:
        preparation_error = str(exc)
    entry_cache, parser_caches = {}, ({}, {})
    for window_sec in window_secs:
        window_stats = {**stats, "errors": list(stats["errors"])}
        try:
            if preparation_error is not None:
                raise ValueError(preparation_error)
            payload = _build_feature_move_map(frames, as_of_utc=as_of_utc, window_sec=window_sec,
                                              instrument=instrument, include_all_comparisons=include_all_comparisons,
                                              prepared=prepared, entry_cache=entry_cache,
                                              parser_caches=parser_caches)
        except ValueError as exc:
            payload = build_feature_move_map([], as_of_utc=as_of_utc, window_sec=window_sec,
                                             instrument=instrument, include_all_comparisons=include_all_comparisons)
            payload["status"] = "observation_validation_failed"
            window_stats["errors"].append(str(exc))
        payload["archive"] = window_stats
        results[window_sec] = payload
    return results
